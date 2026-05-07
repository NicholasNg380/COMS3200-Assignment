"""! @file pubsubclient.py
"""

import socket
import sys
import re
import threading
import time
from sys import stdout,stdin,argv,exit

# Client message types for sending and recieving
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

# Key to verify server
SECRET = b'PUBS1'

state_lock = threading.Lock()

# initital parameters from argument line
class Parameters:
    def __init__(self):
        self.topic = None
        self.sp = None
        self.cid = None
        self.msg = None

class State:
    def __init__(self):
        self.d_topic = None
        self.sub = []      # list of (topic, filter_str)
        self.limits = {}             # {topic: N_seconds}
        self.last_sent = {}          # {topic: timestamp}
        self.files_received = 0

# Usage error
def usage():
    print("Usage: pubsubclient [--topic topic] [server]:port clientid [message]", file=sys.stderr)
    sys.exit(1)

def parse(argv):
    params = Parameters()
    args = argv[1:]
    
    if args and args[0] == '--topic':
        if len(args) < 2:
            usage()
        params.topic = args[1]
        args = args[2:]

    if not args or ':' not in args[0]:
        usage()
    
    colon = args[0].index(':')
    port_str = args[0][colon + 1:]

    if port_str == '':
        usage()
    
    params.sp = args[0]
    args = args[1:]
    
    if not args:
        usage()

    params.cid = args[0]
    args = args[1:]
    
    if args:
        params.msg = args[0]
        args = args[1:]
    
    if args:
        usage()
    
    if params.msg and not params.topic:
        usage()
    
    return params

def clientid_check(cid):
    if not re.fullmatch(r'[A-Za-z0-9]{2,32}', cid):
        print(f'pubsubclient: bad client ID "{cid}"', file=sys.stderr)
        sys.exit(4)

def topic_check(topic, exit=True):
    if re.fullmatch(r'[A-Za-z][A-Za-z0-9 /]*', topic):
        return True
    if exit:
        print(f'pubsubclient: invalid topic string "{topic}"', file=sys.stderr)
        sys.exit(5)
    else:
        print(f'pubsubclient: invalid topic string "{topic}"', file=sys.stderr)
        return False
    
def message_check(msg, exit):
    if msg.isprintable():
        return True
    if exit:
        print('pubsubclient: messages must only contain printable characters', file=sys.stderr)
        sys.exit(6)
    else:
        print('pubsubclient: messages must only contain printable characters', file=sys.stderr)
        return False

def connect_server(srv_port):
    colon = srv_port.index(':')
    host = srv_port[:colon] or 'localhost'
    port_str = srv_port[colon + 1:]
    try:
        if port_str.isdigit():
            port = int(port_str)
        else:
            port = socket.getservbyname(port_str)
    except (OSError, ValueError):
        port = None

    if port is None:
        print(f'pubsubclient: unable to connect to "{host}:{port_str}"', file=sys.stderr)
        sys.exit(7)
    try:
        sock = socket.create_connection((host, port), timeout=5)
        sock.settimeout(None)
        return sock
    except OSError:
        print(f'pubsubclient: unable to connect to "{host}:{port_str}"', file=sys.stderr)
        sys.exit(7)

def segment_read(sock, size):
    buffer = b''
    while len(buffer) < size:
        try:
            data = sock.recv(size - len(buffer))
        except OSError:
            return None
        if not data:
            return None
        buffer += data
    return buffer

def recv_layer(sock):
    header = segment_read(sock, 5)
    if header is None:
        return None, None
    type = header[0:1]
    length = int.from_bytes(header[1:5], byteorder='big')
    if length == 0:
        return type, []
    data = segment_read(sock, length)
    if data is None:
        return None, None
    return type, data.split(b'\x00')

def send_layer(sock, type, *information):
    parts = []

    for info in information:
        if isinstance(info, str):
            parts.append(info.encode('utf-8'))
        else:
            parts.append(info)

    data = b'\x00'.join(parts)
    header = type + len(data).to_bytes(4, 'big')
    try:
        sock.sendall(header + data)
    except OSError:
        pass


def handshake(sock, cid, svr_port):
    # Header Format [message type: 1 byte][payload length: 4 byte][data]
    data = SECRET + b'\x00' + cid.encode('utf-8')
    header = CLIENT_HANDSHAKE + len(data).to_bytes(4, 'big')
    try:
        sock.sendall(header + data)
    except OSError:
        print(f'pubsubclient: unable to connect to "{svr_port}"', file=sys.stderr)
        sys.exit(7)

    sock.settimeout(1.0)
    try:
        msg_type, data = recv_layer(sock)
    except socket.timeout:
        msg_type, data = None, None
    finally:
        sock.settimeout(None)

    if msg_type is None:
        print(f'pubsubclient: server at "{svr_port}" is not a valid server', file=sys.stderr)
        sys.exit(8)

    if not data or data[0] != SECRET:
        print(f'pubsubclient: server at "{svr_port}" is not a valid server', file=sys.stderr)
        sys.exit(8)

    if msg_type == CLIENT_HANDSHAKE_DUP:
        print(f'pubsubclient: client ID "{cid}" is not unique', file=sys.stderr) 
        sys.exit(9)
    elif msg_type != CLIENT_HANDSHAKE_OK:
        print(f'pubsubclient: server at "{svr_port}" is not a valid server', file=sys.stderr)
        sys.exit(8)

def recv(sock, server_port, state):
    while True:
        type, data = recv_layer(sock)

        if type is None:
            print('pubsubclient: server disconnected - exiting', file=sys.stderr, flush=True)
            sys.exit(10)

        if type == CLIENT_INCOMING:
            sid = data[0].decode('utf-8')
            cid  = data[1].decode('utf-8')
            topic = data[2].decode('utf-8')
            message  = data[3].decode('utf-8')
            print(f'{topic}: {message} ({sid}:{cid})', file=sys.stdout, flush=True)

        elif type == CLIENT_RATE_LIMIT:
            topic = data[0].decode('utf-8')
            N = int(data[1].decode('utf-8'))
            with state_lock:
                state.limits[topic] = n
            print(f'pubsubclient: you are rate limited on topic "{topic}" to {N} seconds between messages', file=sys.stdout, flush=True)
        
        elif type == CLIENT_SERVER_QUIT:
            print('pubsubclient: exiting due to server shutdown', file=sys.stdout, flush=True)
            sys.exit(0)

def stdin(sock, state: State):
    while True:
        try:
            line = sys.stdin.readline()
        except (EOFError, OSError):
            sys.exit(0)

        if line == '': 
            sys.exit(0)
        
        line = line.rstrip('\n')
        if not line.strip():
            continue
        
        stripped = line.lstrip()
        if stripped.startswith('/'):
            cmd_functions(sock, line, state)  
        else:
            with state_lock:
                topic = state.d_topic 
            if not topic:
                print('pubsubclient: no default topic set', file=sys.stderr, flush=True)
                continue
            msg = line
            if not message_check(msg, False):
                continue
            with state_lock:
                limit = state.limits.get(topic)
                last = state.last_sent.get(topic)
                now = time.time()
            if limit and last and (now - last) > limit:
                print('pubsubclient: message publication failed due to rate limit', file=sys.stderr, flush=True)
                continue
            with state_lock:
                state.last_sent[topic] = time.time()
            send_layer(sock, CLIENT_PUBLISH, topic, msg)
        
def cmd_functions(sock, cmd, state: State):
    tokens = parse_line(cmd)
    
    if tokens is None or not tokens:
        print('pubsubclient: unknown command', file=sys.stderr, flush=True)
        return
    
    cmd  = tokens[0]
    args = tokens[1:]

    if cmd == '/subscribe':
        subscribe(sock, args, state)
    elif cmd == '/unsubscribe':
        unsubscribe(sock, args, state)
    elif cmd == '/topic':
        topic(args, state)
    elif cmd == '/publish':
        publish(sock, args, state)
    elif cmd == '/sendfile':
        sendfile(sock, args, state)
    elif cmd == '/listsubs':
        listsubs(args, state)
    elif cmd == '/listlimits':
        listlimits(args, state)
    elif cmd == '/quit':
        if args:
            print('pubsubclient: unknown argument(s) - usage: /quit', file=sys.stderr, flush=True)
        else:
            sys.exit(0)
    else:
        print('pubsubclient: unknown command', file=sys.stderr, flush=True)

def parse_line(line):
    line = line.strip()
    tokens = []
    current = ''
    in_quotes = False
    i = 0
    
    # check for odd number of quotes
    if line.count('"') % 2 != 0:
        return None
    
    while i < len(line):
        c = line[i]
        if c == '"':
            if in_quotes:
                # closing quote — must be end of line or followed by whitespace
                if i + 1 < len(line) and not line[i + 1].isspace():
                    return None
                in_quotes = False
            else:
                # opening quote — must be start of token
                if current:
                    return None
                in_quotes = True
        elif c.isspace() and not in_quotes:
            if current:
                tokens.append(current)
                current = ''
        else:
            current += c
        i += 1
    
    if in_quotes:
        return None
    if current:
        tokens.append(current)
    
    return tokens

def subscribe(sock, arg, state):
    if not arg or len(arg) > 2:
        print('pubsubclient: unknown argument(s) - usage: /subscribe topic [filter]', file=sys.stderr, flush=True)
        return
    
    topic = arg[0]
    if not topic_check(topic, False):
        return
    
    filter = None
    if len(arg) == 2:
        filter = arg[1]
        if not filter_check(filter):
            return
        
    with state_lock:
        for top, fil in state.sub:
            if top == topic and filter_equal(fil, filter):
                print('pubsubclient: identical subscription ignored', file=sys.stderr, flush=True)
                return
        state.sub.append((topic, filter))
    send_layer(sock, CLIENT_SUBSCRIBE, topic, filter or '')

def unsubscribe(sock, args, state: State):
    if len(args) != 1:
        print('pubsubclient: unknown argument(s) - usage: /unsubscribe topic', file=sys.stderr, flush=True)
        return
    
    topic = args[0]
    if not topic_check(topic, False):
        return
    
    with state_lock:
        total_before = len(state.sub)
        state.sub = [tup for tup in state.sub if tup[0] != topic]
        total_after = len(state.sub)
    
    if total_before == total_after:
        print(f'pubsubclient: not subscribed to messages about "{topic}"', file=sys.stderr, flush=True)
    else:
        print(f'pubsubclient: unsubscribed from messages about "{topic}"', file=sys.stdout, flush=True)
        send_layer(sock, CLIENT_UNSUBSCRIBE, topic)

def topic(args, state: State):
    if len(args) != 1:
        print('pubsubclient: unknown argument(s) - usage: /topic topic', file=sys.stderr, flush=True)
        return
    topic = args[0]
    if not topic_check(topic, False):
        return
    with state_lock:
        state.d_topic = topic

def listsubs(args, state: State):
    if args:
        print('pubsubclient: unknown argument(s) - usage: /listsubs', file=sys.stderr, flush=True)
        return
    with state_lock:
        subs = state.sub
    if not subs:
        print('No subscriptions', file=sys.stdout, flush=True)
        return
    for topic, filter in subs:
        show_t = ''
        if ' ' in topic:
            show_t = f'"{topic}"'
        else:
            show_t = topic
        if filter:
            if ' ' in filter:
                print(f'/subscribe {show_t} "{filter}"', file=sys.stdout, flush=True)
            else:
                print(f'/subscribe {show_t} {filter}', file=sys.stdout, flush=True)
        else:
            print(f'/subscribe {show_t}', file=sys.stdout, flush=True)

def listlimits(args, state: State):
    if args:
        print('pubsubclient: unknown argument(s) - usage: /listlimits', file=sys.stderr, flush=True)
        return
    with state_lock:
        limits = dict(state.limits)
    if not limits:
        print('No limits', flush=True)
        return
    # limits stored in order they were received
    for topic, N in limits.items():
        if ' ' in topic:
            show_t = f'"{topic}"'
        else:
            show_t = topic
        print(f'/limit {state.cid} {show_t} {N}', flush=True)


def publish(sock, args, state: State):
    if len(args) != 2:
        print('pubsubclient: unknown argument(s) - usage: /publish topic message', file=sys.stderr, flush=True)
        return
    topic, msg = args[0], args[1]
    if not topic_check(topic, False):
        return
    if not message_check(msg, False):
        return
    with state_lock:
        limit = state.limits.get(topic)
        last  = state.last_sent.get(topic)
        now   = time.time()
    if limit and last and (now - last) < limit:
        print('pubsubclient: message publication failed due to rate limit', file=sys.stderr, flush=True)
        return
    with state_lock:
        state.last_sent[topic] = time.time()
    send_layer(sock, CLIENT_PUBLISH, topic, msg)

def filter_check(filter):
    parts = filter.split(None, 1)
    if len(parts) != 2:
        print(f'pubsubclient: invalid filter string "{filter}"', file=sys.stderr, flush=True)
        return False
    op, val = parts
    if op not in ('<', '<=', '>', '>=', '==', '!='):
        print(f'pubsubclient: invalid filter string "{filter}"', file=sys.stderr, flush=True)
        return False
    try:
        float(val)
    except ValueError:
        print(f'pubsubclient: invalid filter string "{filter}"', file=sys.stderr, flush=True)
        return False
    return True

def filter_equal(fil1, fil2):
    if fil1 is None and fil2 is None:
        return True
    if fil1 is None or fil2 is None:
        return False
    # compare operator and float value
    op1, val1 = fil1.split(None, 1)
    op2, val2 = fil2.split(None, 1)
    return op1 == op2 and float(val1) == float(val2)

def main():
    params = parse(sys.argv)

    clientid_check(params.cid)

    if params.topic is not None:
            topic_check(params.topic, True)

    if params.msg is not None:
            message_check(params.msg, True)

    conn = connect_server(params.sp)
    sock = conn

    handshake(sock, params.cid, params.sp)

    if params.msg:
        send_layer(sock, CLIENT_PUBLISH, params.topic, params.msg)
        time.sleep(0.1)
        sys.exit(0)

    print('Welcome to pubsubclient!', file=sys.stdout, flush=True)

    state = State()

    if params.topic:
        state.d_topic = params.topic

    recv_thread = threading.Thread(target=recv, args=(sock, params.sp, state), daemon=True)
    recv_thread.start()

    stdin(sock, state)




if __name__ == "__main__":
    main()