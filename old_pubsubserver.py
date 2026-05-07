import sys
import socket
import threading
import re

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

# Server-to-server only messages
SRV_HANDSHAKE = b'\x0C'   # peer intro: magic + server_id + known_server_ids
SRV_HANDSHAKE_OK = b'\x0D'
SRV_HANDSHAKE_DUP = b'\x0E'   # duplicate server ID in federation
SRV_HANDSHAKE_SELF = b'\x0F'   # connected to self
SRV_QUIT = b'\x10'   # server shutting down
SRV_PUBLISH  = b'\x11'   # federated publish (text)
SRV_PUBLISH_FILE = b'\x12'   # federated publish (file)
SRV_CLIENT_JOINED = b'\x13'   # inform peers a new client joined
SRV_CLIENT_LEFT = b'\x14'   # inform peers a client left
SRV_RATE_LIMIT = b'\x15'   # propagate rate-limit to target server
SRV_PEER_LIST = b'\x16'   # exchange known server IDs during handshake

SECRET = b'PUBS1'

state_lock = threading.Lock()

state = {
    'server_id': '...',
    'clients': {},          # {client_id: {'sock': ..., 'subs': [...]}}
    'peers': {},            # {peer_id: {'sock': ..., 'thread': ...}}
    'rate_limits': {},      # {client_id: {topic: N_seconds}}
    'last_published': {},   # {client_id: {topic: timestamp}} for rate limiting
}

def usage():
    print('Usage: pubsubserver [--server [server]:port]... [--listenon port] serverid', file=sys.stderr)
    sys.exit(1)

def send_frame(s, msg_type: bytes, *fields):
    """Encode and send a framed message. fields may be str or bytes."""
    payload = b'\x00'.join(
        f.encode('utf-8') if isinstance(f, str) else f
        for f in fields
    )
    header = msg_type + len(payload).to_bytes(4, byteorder='big')
    try:
        s.sendall(header + payload)
    except OSError:
        pass


def recv_exactly(s, n):
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


def decode(b: bytes) -> str:
    return b.decode('utf-8')

def parse(argv):
    """
    Returns (server_list, listenport_or_None, serverid)
    server_list is a list of raw '[server]:port' strings.
    """
    args = argv[1:]
    server_list = []
    listenport = None
    seen_listenon = False

    while args and args[0].startswith('--'):
        opt = args[0]
        if opt == '--server':
            if len(args) < 2:
                usage()
            sp = args[1]
            if sp == '' or ':' not in sp:
                usage()
            colon = sp.index(':')
            if sp[colon + 1:] == '':
                usage()
            server_list.append(sp)
            args = args[2:]
        elif opt == '--listenon':
            if seen_listenon:
                usage()
            seen_listenon = True
            if len(args) < 2:
                usage()
            listenport = args[1]
            if listenport == '':
                usage()
            args = args[2:]
        else:
            usage()

    # remaining must be exactly: serverid
    if len(args) != 1:
        usage()
    serverid = args[0]
    if serverid == '':
        usage()

    return server_list, listenport, serverid

def serverid_check(sid): 
    if not (2 <= len(sid) <= 32) or not re.fullmatch(r'[A-Za-z0-9]+', sid):
        print(f'pubsubserver: bad server ID "{sid}"', file=sys.stderr)
        sys.exit(2)

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

def start_listening(port):
    # socket.socket(), setsockopt, bind, listen
    # If port given but can't bind: exit 3
    # Print "listening on port N" to stderr
    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    if port is not None:
        try: 
            if port.isdigit():
                lport = int(port)
            else:
                lport = socket.getservbyname(port)
            sock.bind(('', lport))
        except OSError:
            print(f'pubsubserver: can\'t listen on port "{port}"', file=sys.stderr)
            sys.exit(3)
    else:
        sock.bind(('', 0))
    
    sock.listen(128)
    actual_port = sock.getsockname()[1]
    print(f'pubsubserver: listening on port {actual_port}', file=sys.stderr)

    return sock


def connect_to_peer(host, port, arg_value, state):
    # Try connecting
    # Handshake to verify it's your server
    # Check: not self, not already connected, no duplicate IDs
    # Print success/failure messages
    try:
        sock = socket.create_connection((host, port), timeout=5)
    except OSError:
        print(f'pubsubserver: can\'t connect to peer "{arg_value}"',
              file=sys.stderr, flush=True)
        return
    
    send_frame(sock, SRV_HANDSHAKE, SECRET, state['server_id'])

    sock.settimeout(1.0)
    msg_type, fields = recv_frame(sock)
    sock.settimeout(None)

    if msg_type != SRV_HANDSHAKE_OK or not fields or fields[0] != SECRET:
        print(f'pubsubserver: server at "{arg_value}" is not valid',
              file=sys.stderr, flush=True)
        sock.close()
        return

def accept_connections_thread(sock, state):
    # Loop: sock, addr = listen_sock.accept()
    # Spawn new thread: handle_connection(sock, addr, state)
    while True:
        try:
            conn, addr = sock.accept()
        except OSError:
            return

        threading.Thread(
            target=handle_connection,
            args=(conn, addr, state),
            daemon=True
        ).start()

def handle_connection(sock, addr, state):
    # Within 1 second, determine: client or peer server?
    # If client: check unique ID, add to state, start client thread
    # If peer: do peer handshake
    # If unknown: close within 1 second
    try:
        sock.settimeout(1.0)
        msg_type, fields = recv_frame(sock)
        sock.settimeout(None)

        if msg_type is None:
                print('pubsubserver: Connection with unknown client aborted', file=sys.stderr, flush=True)
                sock.close()
                return
        
        if msg_type == CLIENT_HANDSHAKE:
            if len(fields) < 2:
                print('pubsubserver: Connection with unknown client aborted', file=sys.stderr, flush=True)
                sock.close()
                return

            secret = fields[0]
            client_id = decode(fields[1])

            if secret != SECRET:
                sock.close()
                return
            
            with state_lock:
                if client_id in state['clients']:
                    send_frame(sock, CLIENT_HANDSHAKE_DUP)
                    print(f'pubsubserver: Client ID "{client_id}" would be duplicated - aborting connection', file=sys.stdout, flush=True)
                    sock.close()
                    return

                state['clients'][client_id] = {
                    'sock': sock,
                    'subs': []
                }

            send_frame(sock, CLIENT_HANDSHAKE_OK, SECRET)

            threading.Thread(
                target=handle_client_thread,
                args=(sock, client_id, state),
                daemon=True
            ).start()
            return
        
        elif msg_type == SRV_HANDSHAKE:
            if len(fields) < 2:
                sock.close()
                return

            secret = fields[0]
            peer_id = decode(fields[1])

            if secret != SECRET:
                sock.close()
                return
            
            if peer_id == state['server_id']:
                print('pubsubserver: Can\'t connect to self as peer', file=sys.stderr, flush=True)
                sock.close()
                return
            

            with state_lock:
                if peer_id in state['peers']:
                    send_frame(sock, SRV_HANDSHAKE_DUP)
                    sock.close()
                    return

                state['peers'][peer_id] = {
                    'sock': sock
                }

            # send OK back (you should define this)
            send_frame(sock, SRV_HANDSHAKE_OK, SECRET)
            print(f'pubsubserver: Connection received from peer "{peer_id}"', file=sys.stdout, flush=True)


            threading.Thread(
                target=handle_peer_thread,
                args=(sock, peer_id, state),
                daemon=True
            ).start()
            return

        else:
            sock.close()

    except OSError:
        sock.close()



def handle_client_thread(sock, client_id, state):
    # Loop: receive message from client
    # Process: subscribe, unsubscribe, publish, sendfile, quit
    # On publish: find matching subscriptions, forward message
    # On disconnect: remove subscriptions, print message
    try:
        while True:
            msg_type, fields = recv_frame(sock)
            if msg_type is None:
                break

            if msg_type == CLIENT_SUBSCRIBE:
                # fields: [topic, filter_or_empty]
                topic = decode(fields[0])
                filt = decode(fields[1]) if len(fields) > 1 and fields[1] else None
                with state_lock:
                    state['clients'][client_id]['subs'].append((topic, filt))

            elif msg_type == CLIENT_UNSUBSCRIBE:
                topic = decode(fields[0])
                with state_lock:
                    subs = state['clients'][client_id]['subs']
                    state['clients'][client_id]['subs'] = [
                        s for s in subs if s[0] != topic
                    ]

            elif msg_type == CLIENT_PUBLISH:
                topic = decode(fields[0])
                message = decode(fields[1])
                route_message(message, topic, client_id, 
                              state['server_id'], None, state)

    finally:
        with state_lock:
            state['clients'].pop(client_id, None)
        print(f'pubsubserver: Client "{client_id}" has disconnected', file=sys.stdout, flush=True)
        try:
            sock.close()
        except OSError:
            pass

def handle_peer_thread(sock, peer_id, state):
    # Loop: receive message from peer server
    # Forward publishes to local clients if they match
    # Forward to other peers (but not back to sender!)
    # On disconnect: print message, clean up

def stdin_thread(state):
    # Loop: read line from stdin
    # Dispatch: listclients, listpeers, peer, limit, quit
    try:
        for line in sys.stdin:
            line = line.rstrip('\n')
            stripped = line.strip()
            if not stripped:
                print('pubsubserver: unknown command', file=sys.stderr, flush=True)
                continue
            handle_server_command(stripped)
    except (EOFError, KeyboardInterrupt):
        pass
    finally:
        handle_quit()

def handle_listclients(args, state): ...
def handle_listpeers(args, state): ...
def handle_peer_command(args, state): ...  # Same as connect_to_peer
def handle_limit(args, state): ...
def handle_quit(state): # Notify all clients & peers, then exit
    with state_lock:
        client_socks = [(cid, c['sock']) for cid, c in state['clients'].item]
        peer_items   = list(state['peers'].item)

    for cid, s in client_socks:
        try:
            send_frame(s, SRV_QUIT)
        except OSError:
            pass

    for pid, pdata in peer_items:
        try:
            with pdata['thread']:
                send_frame(pdata['sock'], SRV_QUIT, state['server_id'])
        except OSError:
            pass

    sys.exit(0)


def route_message(msg, topic, sender_client_id, sender_server_id, 
                  source_peer, state):
    # Find all local clients with matching subscription
    # Apply filters (numeric comparison)
    # Apply rate limits
    # Send to matching local clients
    # Forward to all peer servers EXCEPT source_peer
    with state_lock:
        local_clients = dict(state['clients'])

    for cid, cdata in local_clients.items():
        for sub_topic, sub_filter in cdata['subs']:
            if sub_topic != topic:
                continue
            # Apply numeric filter if present
            if sub_filter is not None:
                op, val = validate_filter(sub_filter)
                if op is None or val is None:
                    continue
                try:
                    num = float(msg.strip())
                except ValueError:
                    continue  # message isn't numeric, skip
                if not apply_op(op, num, val):
                    continue
            send_frame(cdata['sock'], CLIENT_INCOMING,
                       topic, sender_server_id, sender_client_id, msg)
            break  # only send once per client even if multiple matching subs

def apply_op(op, num, val):
    return {
        '<':  num < val,
        '<=': num <= val,
        '>':  num > val,
        '>=': num >= val,
        '==': num == val,
        '!=': num != val,
    }[op]

def main():
    server_list, listenport, serverid = parse(sys.argv)
    validate_server_id(serverid)
    state['server_id'] = serverid

    listen_sock = start_listening(listenport)

    threading.Thread(
        target=accept_connections_thread,
        args=(listen_sock, state),
        daemon=True
    ).start()

    threading.Thread(
        target=stdin_thread,
        args=(state,),
        daemon=True
    ).start()

    for sp in server_list:
        colon = sp.index(':')
        host = sp[:colon] or 'localhost'
        port_str = sp[colon+1:]
        try:
            port = int(port_str) if port_str.isdigit() else socket.getservbyname(port_str)
        except OSError:
            print(f'pubsubserver: can\'t connect to peer "{sp}"', file=sys.stderr, flush=True)
            continue
        connect_to_peer(host, port, sp, state)

    threading.Event().wait()

if __name__ == '__main__':
    main()