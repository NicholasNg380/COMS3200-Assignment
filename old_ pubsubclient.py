import socket
import sys
import re
import threading
import time
from sys import stdout,stdin,argv,exit

CLIENT_HANDSHAKE = b'\x01' 
CLIENT_HANDSHAKE_OK = b'\x02' 
CLIENT_HANDSHAKE_DUP = b'\x03' 
CLIENT_PUBLISH = b'\x04' 
CLIENT_SUBSCRIBE = b'\x05' 
CLIENT_UNSUBSCRIBE = b'\x06'
CLIENT_INCOMING = b'\x07' 
CLIENT_RATE_LIMIT = b'\x08' 
CLIENT_SERVER_QUIT = b'\x09' 
CLIENT_SENDFILE = b'\x0A' 
CLIENT_INCOMING_FILE = b'\x0B'

SECRET = b'PUBS1'

state_lock = threading.Lock()

state = {
    'default_topic': None,   
    'subscriptions': [],     
    'rate_limits': {},       
    'file_count': 0,         
    'exiting': False,        
}

class Parameters:
    def __init__(self):
        self.topic = None
        self.server_port = None
        self.clientid = None
        self.message = None

sock_lock = threading.Lock()

def decode(b: bytes) -> str:
    return b.decode('utf-8')

def send_frame(s, msg_type: bytes, *fields):
    """Encode and send a framed message over socket s."""
    payload = b'\x00'.join(
        f.encode('utf-8') if isinstance(f, str) else f
        for f in fields
    )
    header = msg_type + len(payload).to_bytes(4, 'big')
    with sock_lock:
        try:
            s.sendall(header + payload)
        except OSError:
            pass

def recv_exactly(s, n):
    """Read exactly n bytes from s, return None on EOF/error."""
    buf = b''
    while len(buf) < n:
        try:
            chunk = s.recv(n - len(buf))
        except OSError:
            return None
        if not chunk:
            return None
        buf += chunk
    return buf

def recv_frame(s):
    """Returns (msg_type_byte, list_of_field_bytes) or (None, None)."""
    header = recv_exactly(s, 5)
    if header is None:
        return None, None
    msg_type = header[0:1]
    length = int.from_bytes(header[1:5], byteorder='big')
    if length == 0:
        return msg_type, []
    payload = recv_exactly(s, length)
    if payload is None:
        return None, None
    return msg_type, payload.split(b'\x00')

def usage():
    print("Usage: pubsubclient [--topic topic] [server]:port clientid [message]", file=sys.stderr)
    sys.exit(1)
    
def parse(argv):
    params = Parameters()
    args = argv[1:]
    
    # Pull --topic if present
    if args and args[0] == '--topic':
        if len(args) < 2:
            usage()
        params.topic = args[1]
        args = args[2:]

    # Check for unexpected option args
    # [server]:port must contain ':'
    if not args or ':' not in args[0]:
        usage()
    
    colon = args[0].index(':')
    port_str = args[0][colon + 1:]

    if port_str == '':
        usage()
    
    params.server_port = args[0]
    args = args[1:]
    
    if not args:
        usage()

    params.clientid = args[0]
    args = args[1:]
    
    if args:
        params.message = args[0]
        args = args[1:]
    
    if args:  # leftover args
        usage()
    
    if params.message and not params.topic:
        usage()
    
    return params

def validate_clientid(cid):
    if not (2 <= len(cid) <= 32) or not re.fullmatch(r'[A-Za-z0-9]+', cid):
        print(f'pubsubclient: bad client ID "{cid}"', file=sys.stderr)
        sys.exit(4)


def validate_topic(topic, exit_on_fail=True):
    """Returns True if valid, or prints error and exits/returns False."""
    if re.fullmatch(r'[A-Za-z][A-Za-z0-9 /]*', topic):
        return True
    if exit_on_fail:
        print(f'pubsubclient: invalid topic string "{topic}"', file=sys.stderr)
        sys.exit(5)
    else:
        print(f'pubsubclient: invalid topic string "{topic}"', file=sys.stderr)
        return False


def validate_message(msg, exit_on_fail=True):
    if msg.isprintable():
        return True
    if exit_on_fail:
        print('pubsubclient: messages must only contain printable characters', file=sys.stderr)
        sys.exit(6)
    else:
        print('pubsubclient: messages must only contain printable characters', file=sys.stderr)
        return False


def validate_filter(filter_str):
    """
    A filter is 'operator value' where operator in {<,<=,>,>=,==,!=}
    and value is a numeric (int or float).
    Returns (operator, float_value) or None if invalid.
    """
    if filter_str is None:
        return True  # no filter = valid
    m = re.fullmatch(r'(<=|>=|==|!=|<|>)\s*([+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?)', filter_str.strip())
    if not m:
        return None
    op = m.group(1)
    try:
        val = float(m.group(2))
    except ValueError:
        return None
    return (op, val)

def resolve_server_port(server_port_str):
    """
    Split 'server:port' into (host, port_int, display_str).
    display_str uses 'localhost' if no host was given.
    """
    colon = server_port_str.index(':')
    host = server_port_str[:colon] or 'localhost'
    port_str = server_port_str[colon + 1:]
    # Resolve service name
    try:
        if port_str.isdigit():
            port = int(port_str)
        else:
            port = socket.getservbyname(port_str)
    except (OSError, ValueError):
        port = None
    return host, port

def connect_to_server(host, port):
    """
    Connect to the server.  Returns (socket, display_str).
    Exits with status 7 if unable to connect.
    """
    display = f'{host}:{port}'
    if port is None:
        print(f'pubsubclient: unable to connect to "{display}"', file=sys.stderr)
        sys.exit(7)
    try:
        s = socket.create_connection((host, port), timeout=5)
        s.settimeout(None)  # back to blocking after connect
        return s, display
    except OSError:
        print(f'pubsubclient: unable to connect to "{display}"', file=sys.stderr)
        sys.exit(7)

def handshake_client(sock, client_id, display):
    # Send your custom protocol "hello" to server
    # Read response - is it your server? Is ID unique?
    # Exit with code 8 or 9 if not

        # Send: MAGIC + null + clientid
    payload = SECRET + b'\x00' + client_id.encode('utf-8')
    header = CLIENT_HANDSHAKE + len(payload).to_bytes(4, 'big')
    try:
        sock.sendall(header + payload)
    except OSError:
        print(f'pubsubclient: unable to connect to "{display}"', file=sys.stderr)
        sys.exit(7)

    # Give server 1 second to respond
    sock.settimeout(1.0)
    try:
        msg_type, fields = recv_frame(sock)
    except socket.timeout:
        msg_type, fields = None, None
    finally:
        sock.settimeout(None)

    if msg_type is None:
        print(f'pubsubclient: server at "{display}" is not a valid server', file=sys.stderr)
        sys.exit(8)

    if msg_type == CLIENT_HANDSHAKE_DUP:
        print(f'pubsubclient: client ID "{client_id}" is not unique', file=sys.stderr) 
        sys.exit(9)

    if msg_type != CLIENT_HANDSHAKE_OK:
        print(f'pubsubclient: server at "{display}" is not a valid server', file=sys.stderr)
        sys.exit(8)

    # Verify magic in response payload
    if not fields or fields[0] != SECRET:
        print(f'pubsubclient: server at "{display}" is not a valid server', file=sys.stderr)
        sys.exit(8)

def stdin_thread(sock):
    for line in sys.stdin:
        line = line.rstrip('\n')
        # strip leading/trailing whitespace for command detection
        stripped = line.strip()
        
        if not stripped:
            continue  # ignore empty/whitespace-only lines
        
        if stripped.startswith('/'):
            handle_command(stripped, sock)
        else:
            # publish with default topic
            msg = line  # keep original (not stripped) per spec
            if not state['default_topic']:
                print('pubsubclient: no default topic set', file=sys.stderr, flush=True)
            elif not validate_message(msg, exit_on_fail=False):
                pass  # error already printed
            else:
                send_frame(sock, CLIENT_PUBLISH, state['default_topic'], msg)
    
    # EOF
    sock.close()
    sys.exit(0)

def tokenise(line):
    # Count quotes – must be even
    if line.count('"') % 2 != 0:
        return None

    tokens = []
    i = 0
    n = len(line)

    while i < n:
        # Skip whitespace
        while i < n and line[i] != '\n' and line[i] in ' \t\r\f\v':
            i += 1
        if i >= n:
            break

        if line[i] == '"':
            # Quoted token
            i += 1  # skip opening quote
            start = i
            while i < n and line[i] != '"':
                i += 1
            if i >= n:
                return None  # unclosed quote
            token = line[start:i]
            i += 1  # skip closing quote
            # After closing quote must be whitespace or end of string
            if i < n and line[i] not in ' \t\r\f\v':
                return None
            tokens.append(token)
        else:
            # Unquoted token – read until whitespace or quote
            start = i
            while i < n and line[i] not in ' \t\r\f\v':
                if line[i] == '"':
                    # quote inside unquoted token is invalid
                    return None
                i += 1
            tokens.append(line[start:i])

    return tokens

def check_rate_limit(topic):
    """
    Returns True if we are allowed to publish on topic right now.
    Updates last-sent time if allowed.
    Call while holding state_lock.
    """
    if topic not in state['rate_limits']:
        return True
    n_seconds, last_sent = state['rate_limits'][topic]
    if n_seconds == 0:
        return True
    if last_sent is None:
        state['rate_limits'][topic] = (n_seconds, time.monotonic())
        return True
    elapsed = time.monotonic() - last_sent
    if elapsed >= n_seconds:
        state['rate_limits'][topic] = (n_seconds, time.monotonic())
        return True
    return False


def record_sent(topic):
    """Record that we just sent on topic. Call while holding state_lock."""
    if topic in state['rate_limits']:
        n_seconds, _ = state['rate_limits'][topic]
        state['rate_limits'][topic] = (n_seconds, time.monotonic())


def publish_message(s, topic, message):
    """Send a publish frame to the server. Returns False if rate limited."""
    with state_lock:
        if not check_rate_limit(topic):
            print('pubsubclient: message publication failed due to rate limit')
            return False
        record_sent(topic)
    send_frame(s, CLIENT_PUBLISH, topic, message)
    return True

def handle_command(line, sock):
    try:
        tokens = tokenise(line)
    except ValueError:
        print('pubsubclient: unknown argument(s) - usage: ...', file=sys.stderr, flush=True)
        return
    
    if not tokens:
        return
    
    cmd = tokens[0]
    args = tokens[1:]
    
    if cmd == '/subscribe':
        handle_subscribe(args, sock)
    elif cmd == '/unsubscribe':
        handle_unsubscribe(args, sock)
    elif cmd == '/topic':
        handle_topic(args)
    elif cmd == '/publish':
        handle_publish(args, sock)
        '''
    elif cmd == '/sendfile':
        handle_sendfile(args, sock)
    elif cmd == '/listsubs':
        handle_listsubs(args)
    elif cmd == '/listlimits':
        handle_listlimits(args)
    elif cmd == '/quit':
        handle_quit(args, sock)'''
    else:
        print('pubsubclient: unknown command', file=sys.stderr, flush=True)

def handle_subscribe(arg, sock):
    # /subscribe topic [filter]
    if len(arg) < 1 or len(arg) > :
        print('pubsubclient: unknown argument(s) - usage: /subscribe topic [filter]', file=sys.stderr)
        return

    topic = arg[0]
    # Validate topic
    if not validate_topic(topic, exit_on_fail=False):
        return
    
    filter_str = arg[1] if len(arg) == 2 else None

    # Validate filter
    if filter_str is not None:
        parsed = validate_filter(filter_str)
        if parsed is None:
            print(f'pubsubclient: invalid filter string "{filter_str}"', file=sys.stderr)
            return

    with state_lock:
        # Check for identical subscription
        for (sub_topic, sub_filter) in state['subscriptions']:
            if sub_topic == topic:
                # Compare filters
                if sub_filter is None and filter_str is None:
                    print('pubsubclient: identical subscription ignored', file=sys.stderr)
                    return
                if sub_filter is not None and filter_str is not None:
                    p1 = validate_filter(sub_filter)
                    p2 = validate_filter(filter_str)
                    if p1 and p2 and p1 is not True and p2 is not True:
                        if p1[0] == p2[0] and p1[1] == p2[1]:
                            print('pubsubclient: identical subscription ignored', file=sys.stderr)
                            return

        state['subscriptions'].append((topic, filter_str))

    # Tell server
    filter_payload = filter_str if filter_str is not None else ''
    send_frame(sock, CLIENT_SUBSCRIBE, topic, filter_payload)

def handle_unsubscribe(topic, s):
    # /unsubscribe topic
    if len(topic) != 1:
        print('pubsubclient: unknown argument(s) - usage: /unsubscribe topic', file=sys.stderr)
        return

    if not validate_topic(topic, exit_on_fail=False):
        return

    with state_lock:
        before = len(state['subscriptions'])
        state['subscriptions'] = [(t, f) for (t, f) in state['subscriptions'] if t != topic]
        after = len(state['subscriptions'])

    if before == after:
        print(f'pubsubclient: not subscribed to messages about "{topic}"', file=sys.stderr)
    else:
        print(f'pubsubclient: unsubscribed from messages about "{topic}"', file=sys.stdout)
        send_frame(s, CLIENT_UNSUBSCRIBE, topic)

def handle_topic(topic):
    # /topic topic
    if len(topic) != 1:
        print('pubsubclient: unknown argument(s) - usage: /topic topic', file=sys.stderr)
        return

    if not validate_topic(topic, exit_on_fail=False):
        return

    with state_lock:
        state['default_topic'] = topic

def handle_publish(arg, s):
    # /publish topic message
    if len(arg) < 2:
        print('pubsubclient: unknown argument(s) - usage: /publish topic message', file=sys.stderr)
        return

    topic = arg[0]
    message = ' '.join(arg[1:])  # remainder is the message

    if not validate_topic(topic, exit_on_fail=False):
        return
    if not validate_message(message, exit_on_fail=False):
        return

    publish_message(s, topic, message)

def main():
    params = parse(sys.argv)


if __name__ == "__main__":
    main()