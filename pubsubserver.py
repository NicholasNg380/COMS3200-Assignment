"""! @file pubsubserver.py
"""

import sys
import socket
import threading
import re
import time

# Client message types for sending and receiving
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

# Server message types for sending and receiving
SERVER_HANDSHAKE = b'\x0C'
SERVER_HANDSHAKE_OK = b'\x0D'
SERVER_HANDSHAKE_DUP = b'\x0E'
SERVER_HANDSHAKE_SELF = b'\x0F'
SERVER_QUIT = b'\x10'
SERVER_PUBLISH  = b'\x11'
SERVER_PUBLISH_FILE = b'\x12'
SERVER_CLIENT_JOINED = b'\x13'
SERVER_CLIENT_LEFT = b'\x14'
SERVER_RATE_LIMIT = b'\x15'
SERVER_PEER_LIST = b'\x16'

SECRET = b'PUBS1'

state_lock = threading.Lock()

class Parameters:
    def __init__(self):
        self.servers = []
        self.listen = None
        self.sid = None

class State:
    def __init__(self, sid):
        self.sid = sid
        self.servers = set()
        self.clients = {}            # {client_id: ClientConn}
        self.peers = {}              # {peer_id: PeerConn}
        self.fed_clients = {}     # {client_id: server_id}
        self.limits = {}        # {client_id: {topic: N_seconds}}
        self.last_published = {}     # {client_id: {topic: timestamp}}

class ClientConn:
    def __init__(self, cid, sock):
        self.cid = cid
        self.sock = sock
        self.sub = []      # list of (topic, filter_str)

class PeerConn:
    def __init__(self, pid, sock):
        self.pid = pid
        self.sock = sock

def usage():
    print('Usage: pubsubserver [--server [server]:port]... [--listenon port] serverid', file=sys.stderr)
    sys.exit(1)

def parse(argv):
    params = Parameters()
    args = argv[1:]

    while args and args[0].startswith('--'):
        param = args[0]
        if param == '--server':
            if len(args) < 2:
                usage()
            sp = args[1]
            if sp == '' or ':' not in sp:
                usage()
            colon = sp.index(':')
            if sp[colon + 1:] == '':
                usage()
            params.servers.append(sp)
            args = args[2:]
        elif param == '--listenon':
            if params.listen:
                usage()
            if len(args) < 2:
                usage()
            listen = args[1]
            if listen == '':
                usage()
            params.listen = listen
            args = args[2:]
        else:
            usage()

    if len(args) != 1:
        usage()
    sid = args[0]
    if sid == '':
        usage()
    params.sid = sid

    return params

def serverid_check(sid):
    if not re.fullmatch(r'[A-Za-z0-9]{2,32}', sid):
        print(f'pubsubserver: bad server ID "{sid}"', file=sys.stderr)
        sys.exit(2)

def clientid_check(cid):
    if re.fullmatch(r'[A-Za-z0-9]{2,32}', cid):
        return True
    return False

def topic_check(topic, exit=True):
    if re.fullmatch(r'[A-Za-z][A-Za-z0-9 /]*', topic):
        return True
    return False

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

def server_connect(server, state: State):
    colon = server.index(':')
    host = server[:colon] or 'localhost'
    port = server[colon + 1:]

    try:
        if port.isdigit():
            port = int(port)
        else:
            port = socket.getservbyname(port)
        conn = socket.create_connection((host, port), timeout=5)
        conn.settimeout(None)
    except (OSError, ValueError):
        print(f'pubsubserver: can\'t connect to peer "{server}"', file=sys.stderr, flush=True)
        return

    with state_lock:
        known = list(state.servers)

    part = [SECRET, state.sid.encode('utf-8')]
    parts = []
    for sid in known:
        if isinstance(sid, str):
            parts.append(sid.encode('utf-8'))
        else:
            parts.append(sid)
    data = b'\x00'.join(part + parts)
    header = SERVER_HANDSHAKE + len(data).to_bytes(4, 'big')
    # Sending [Header][Secret][sid][peer sids]
    try:
        conn.sendall(header + data)
    except OSError:
        print(f'pubsubserver: can\'t connect to peer "{server}"', file=sys.stderr, flush=True)
        conn.close()
        return
    
    conn.settimeout(1.0)
    try:
        type, data = recv_layer(conn)
    except socket.timeout:
        type, data = None, None
    finally:
        conn.settimeout(None)

    if type is None or not data:
        print(f'pubsubserver: Peer server not found at "{server}"', file=sys.stderr, flush=True)
        conn.close()
        return

    if type == SERVER_HANDSHAKE_SELF:
        print('pubsubserver: Can\'t connect to self as peer', file=sys.stderr, flush=True)
        conn.close()
        return

    if type == SERVER_HANDSHAKE_DUP:
        print(f'pubsubserver: Unable to connect to server "{server}" due to common server IDs', file=sys.stderr, flush=True)
        conn.close()
        return

    if type != SERVER_HANDSHAKE_OK or data[0] != SECRET:
        print(f'pubsubserver: Peer server not found at "{server}"', file=sys.stderr, flush=True)
        conn.close()
        return
    
    pid = ''
    if len(data) > 1:
        pid = data[1].decode('utf-8')

    if pid == state.sid:
        print('pubsubserver: Can\'t connect to self as peer', file=sys.stderr, flush=True)
        conn.close()
        return
    
    with state_lock:
        if pid in state.peers:
            print(f'pubsubserver: Already connected to peer server at "{server}"', file=sys.stderr, flush=True)
            conn.close()
            return

        peer_known = {d.decode('utf-8') for d in data[2:]}
        conflicts = peer_known & (state.servers - {state.sid})
        if conflicts:
            print(f'pubsubserver: Unable to connect to server "{server}" due to common server IDs', file=sys.stderr, flush=True)
            conn.close()
            return
        
        state.peers[pid] = PeerConn(pid, conn)
        state.servers.update(peer_known)
        state.servers.add(pid)

    print(f'pubsubserver: Connected to peer "{pid}" at "{server}"', file=sys.stdout, flush=True)
    
    t = threading.Thread(target=peer_functions, args=(pid, conn, state), daemon=True)
    t.start()

def peer_functions(pid, sock, state: State):
    while True:
        type, data = recv_layer(sock)
        if type is None:
            with state_lock:
                if pid in state.peers:
                    del state.peers[pid]
                    state.servers.discard(pid)
            print(f'pubsubserver: Peer server "{pid}" disconnected', file=sys.stderr, flush=True)
            return
        
        if type == SERVER_QUIT:
            peer_quit(pid, state)
            return
        elif type == SERVER_PUBLISH:
            peer_publish(data, state)
        elif type == SERVER_PUBLISH_FILE:
            peer_file(data, state)
        elif type == SERVER_CLIENT_JOINED:
            peer_client_joined(data, state)
        elif type == SERVER_CLIENT_LEFT:
            peer_client_left(data, state)
        elif type == SERVER_RATE_LIMIT:
            peer_rate_limit(data, state)

def peer_quit(pid, state: State):
    with state_lock:
        if pid in state.peers:
            del state.peers[pid]
        state.servers.discard(pid)
        # remove any remote clients that were on that peer
        remove = []
        for cid, sid in state.fed_clients.items():
            if sid == pid:
                remove.append(cid)
        for cid in remove:
            del state.fed_clients[cid]
    print(f'pubsubserver: Peer server "{pid}" shutting down', file=sys.stdout, flush=True)

def peer_publish(data, state: State):
    server_id   = data[0].decode('utf-8')
    client_id   = data[1].decode('utf-8')
    topic   = data[2].decode('utf-8')
    message = data[3].decode('utf-8')
    
    with state_lock:
        clients = list(state.clients.values())
        peers = list(state.peers.values())

    for client in clients:
        if filter_match(client.sub, topic, message):
            send_layer(client.sock, CLIENT_INCOMING, server_id, client_id, topic, message)

    for peer in peers:
        if peer.pid != server_id:
            send_layer(peer.sock, SERVER_PUBLISH, server_id, client_id, topic, message)

def peer_client_joined(data, state: State):
    sid = data[0].decode()
    cid = data[1].decode()
    with state_lock:
        state.fed_clients[cid] = sid

def peer_client_left(data, state: State):
    sid = data[0].decode()
    cid = data[1].decode()
    with state_lock:
        state.fed_clients.pop(cid, None)

def peer_rate_limit(data, state):
    cid  = data[0].decode()
    topic = data[1].decode()
    N = int(data[2].decode())
    with state_lock:
        state.limits.setdefault(cid, {})[topic] = N

def accept(sock, state):
    while True:
        try:
            conn, addr = sock.accept()
        except OSError:
            return
        
        thread = threading.Thread(target=client_server_conn, args=(conn, state), daemon=True)
        thread.start()

def client_server_conn(conn, state):
    conn.settimeout(1.0)
    try:
        type, data = recv_layer(conn)
    except socket.timeout:
        type, data = None, None
    finally:
        conn.settimeout(None)
    
    if type is None or not data:
        print('pubsubserver: Connection with unknown client aborted', file=sys.stderr, flush=True)
        conn.close()
        return
    
    if type == CLIENT_HANDSHAKE:
        client_handshake(conn, data, state)
    elif type == SERVER_HANDSHAKE:
        peer_handshake(conn, data, state)
    else:
        print('pubsubserver: Connection with unknown client aborted', file=sys.stderr, flush=True)
        conn.close()

def client_handshake(conn, data, state):
    if not data or data[0] != SECRET:
        print('pubsubserver: Connection with unknown client aborted', file=sys.stderr, flush=True)
        conn.close()
        return

    cid = data[1].decode('utf-8')

    with state_lock:
        if cid in state.clients or cid in state.fed_clients:
            send_layer(conn, CLIENT_HANDSHAKE_DUP, SECRET)
            print(f'pubsubserver: Client ID "{cid}" would be duplicated - aborting connection', file=sys.stdout, flush=True)
            conn.close()
            return
        state.clients[cid] = ClientConn(cid, conn)

    send_layer(conn, CLIENT_HANDSHAKE_OK, SECRET)
    print(f'pubsubserver: Client "{cid}" has connected', file=sys.stdout, flush=True)
    for peer in list(state.peers.values()):
        send_layer(peer.sock, SERVER_CLIENT_JOINED, state.sid, cid)

    thread = threading.Thread(target=client_functions, args=(cid, conn, state), daemon=True)
    thread.start()

def peer_handshake(conn, data, state: State):
    if not data or data[0] != SECRET:
        print('pubsubserver: Connection with unknown client aborted', file=sys.stderr, flush=True)
        conn.close()
        return
    
    pid = data[1].decode('utf-8')
    known = {d.decode('utf-8') for d in data[2:]}
    known.add(pid)

    if pid == state.sid:
        send_layer(conn, SERVER_HANDSHAKE_SELF)
        conn.close()
        return
    
    with state_lock:
        if pid in state.peers:
            send_layer(conn, SERVER_HANDSHAKE_DUP)
            conn.close()
            return
        
        federation_known = known & state.servers
        if federation_known:
            send_layer(conn, SERVER_HANDSHAKE_DUP)
            conn.close()
            return
        
        state.peers[pid] = PeerConn(pid, conn)
        state.servers.update(known)
        state.servers.add(pid)

        servers = list(state.servers)

    part = [SECRET, state.sid.encode('utf-8')]
    parts = []
    for sid in servers:
        if isinstance(sid, str):
            parts.append(sid.encode('utf-8'))
        else:
            parts.append(sid)
    data = b'\x00'.join(part + parts)
    header = SERVER_HANDSHAKE + len(data).to_bytes(4, 'big')
    try:
        conn.sendall(header + data)
    except OSError:
        print(f'pubsubserver: can\'t connect to peer "{pid}"', file=sys.stderr, flush=True)
        conn.close()
        return
    
    with state_lock:
        clients = list(state.clients.keys())

    for cid in clients:
        send_layer(conn, SERVER_CLIENT_JOINED, state.sid, cid)

    t = threading.Thread(target=peer_functions, args=(pid, conn, state), daemon=True)
    t.start()
    

def client_functions(cid, conn, state):
    while True:
        msg_type, data = recv_layer(conn)

        if msg_type is None:
            with state_lock:
                if cid in state.clients:
                    del state.clients[cid]
            for peer in list(state.peers.values()):
                send_layer(peer.sock, SERVER_CLIENT_LEFT, state.sid, cid)
            print(f'pubsubserver: Client "{cid}" has disconnected', file=sys.stdout, flush=True)
            return
        
        if msg_type == CLIENT_PUBLISH:
            client_publish(cid, data, state)

        elif msg_type == CLIENT_SUBSCRIBE:
            client_subscribe(cid, data, state)

        elif msg_type == CLIENT_UNSUBSCRIBE:
            client_unsubscribe(cid, data, state)

        elif msg_type == CLIENT_SENDFILE:
            client_sendfile(cid, data, state)

        elif msg_type == CLIENT_SERVER_QUIT:
            with state_lock:
                if cid in state.clients:
                    del state.clients[cid]
            for peer in list(state.peers.values()):
                send_layer(peer.sock, SERVER_CLIENT_LEFT, state.sid, cid)
            print(f'pubsubserver: Client "{cid}" has disconnected', file=sys.stdout, flush=True)
            return
        
def client_publish(cid, data, state: State):
    topic   = data[0].decode('utf-8')
    message = data[1].decode('utf-8')
    
    with state_lock:
        limit = state.limits.get(cid, {}).get(topic)
        now = time.time()
        last = state.last_published.get(cid, {}).get(topic)
    
    if limit and last and (now - last) < limit:
        # notify client their message was dropped
        with state_lock:
            client = state.clients.get(cid)
        if client:
            send_layer(client.sock, CLIENT_RATE_LIMIT, topic, str(limit))
        return
    
    with state_lock:
        state.last_published.setdefault(cid, {})[topic] = now
        clients = list(state.clients.values())
    
    seen = set()
    for client in clients:
        if filter_match(client.sub, topic, message):
            if client.cid not in seen:
                seen.add(client.cid)
                send_layer(client.sock, CLIENT_INCOMING, state.sid, cid, topic, message)
    
    for peer in list(state.peers.values()):
        send_layer(peer.sock, SERVER_PUBLISH, state.sid, cid, topic, message)

def client_subscribe(cid, data, state:State):
    topic      = data[0].decode('utf-8')
    filter_str = data[1].decode('utf-8') if len(data) > 1 else None

    with state_lock:
        client = state.clients.get(cid)
        if client:
            client.sub.append((topic, filter_str))

def client_unsubscribe(cid, data, state: State):
    topic = data[0].decode('utf-8')
    filter_str = data[1].decode('utf-8') if len(data) > 1 else None
    with state_lock:
        client = state.clients.get(cid)
        if client:
            client.sub.remove((topic, filter_str))

def client_sendfile(cid, data, state: State):
    topic    = data[0].decode('utf-8')
    filename = data[1].decode('utf-8')
    content  = data[2]
    
    with state_lock:
        clients = list(state.clients.values())
    
    seen = set()
    for client in clients:
        if any(t == topic and f is None for t, f in client.sub):
            if client.cid not in seen:
                seen.add(client.cid)
                send_layer(client.sock, CLIENT_INCOMING_FILE, state.sid, cid, topic, filename, content)
    
    for peer in list(state.peers.values()):
        send_layer(peer.sock, SERVER_PUBLISH_FILE, state.sid, cid, topic, filename, content)

def filter_match(sub, topic, message):
    match = False
    for top, filter in sub:
        if top != topic:
            continue
        if filter is None:
            match = True
            break
        try:
            val = float(message.strip())
        except ValueError:
            continue
        op, num = filter.split(None, 1)
        num = float(num)
        if op == '<' and val < num: 
            match = True
        elif op == '<=' and val <= num: 
            match = True
        elif op == '>'  and val >  num: 
            match = True
        elif op == '>=' and val >= num: 
            match = True
        elif op == '==' and val == num: 
            match = True
        elif op == '!=' and val != num: 
            match = True
        if match:
            break
    return match

def stdin(state: State):
    while True:
        try:
            line = sys.stdin.readline()
        except (EOFError, OSError):
            quit(state)
            return
        
        if line == '': 
            quit(state)
            return
        
        line = line.rstrip('\n')
        tokens = parse_line(line)
        
        if tokens is None:  # bad quotes
            print('pubsubserver: unknown command', file=sys.stderr, flush=True)
            continue
        
        if not tokens:  # empty line
            continue
        
        if not tokens[0].startswith('/'):
            print('pubsubserver: unknown command', file=sys.stderr, flush=True)
            continue

        cmd = tokens[0]
        args = tokens[1:]

        if cmd == '/listclients':
            listclients(args, state)
        elif cmd == '/listpeers':
            listpeers(args, state)
        elif cmd == '/peer':
            peer(args, state)
        elif cmd == '/limit':
            limit(args, state)
        elif cmd == '/quit':
            if args:
                print('pubsubserver: unknown argument(s) - usage: /quit', file=sys.stderr, flush=True)
            else:
                quit(state)
                return
        else:
            print('pubsubserver: unknown command', file=sys.stderr, flush=True)

#
#
#
#
#
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

def quit(state: State):
    with state_lock:
        clients = list(state.clients.values())
        peers = list(state.peers.values())

    for client in clients:
        send_layer(client.sock, CLIENT_SERVER_QUIT)
        client.sock.close()

    for peer in peers:
        send_layer(peer.sock, SERVER_QUIT)
        peer.sock.close()

    sys.exit(0)

def listclients(arg, state: State):
    if arg and arg[0] == '--all':
        if len(arg) > 1:
            print('pubsubserver: unknown argument(s) - usage: /listclients [--all]', file=sys.stderr, flush=True)
            return
        with state_lock:
            client = [f'{state.sid}:{cid}' for cid in state.clients]
            federation_client = [f'{sid}:{cid}' for cid, sid in state.fed_clients.items()]
        clients = sorted(client + federation_client)
        if not clients:
            print('pubsubserver: No clients connected', file=sys.stdout, flush=True)
        else:
            for c in client:
                print(c, file=sys.stdout, flush=True)
    elif not arg:
        with state_lock:
            client = [f'{state.sid}:{cid}' for cid in state.clients]
        if not client:
            print('pubsubserver: No clients connected', file=sys.stdout, flush=True)
        else:
            for c in client:
                print(c, file=sys.stdout, flush=True)
    else:
        print('pubsubserver: unknown argument(s) - usage: /listclients [--all]', file=sys.stderr, flush=True)

def listpeers(arg, state: State):
    if arg and arg[0] == '--all':
        if len(arg) > 1:
            print('pubsubserver: unknown argument(s) - usage: /listpeers [--all]', file=sys.stderr, flush=True)
            return
        with state_lock:
            federation_peers = sorted(state.servers - {state.sid})
        if not federation_peers:
            print('pubsubserver: No peer servers connected', file=sys.stdout, flush=True)
        else:
            for p in federation_peers:
                print(p, file=sys.stdout, flush=True)
    elif not arg:
        with state_lock:
            peers = sorted(state.peers.keys())
        if not peers:
            print('pubsubserver: No peer servers connected', file=sys.stdout, flush=True)
        else:
            for p in peers:
                print(p, file=sys.stdout, flush=True)
    else:
        print('pubsubserver: unknown argument(s) - usage: /listpeers [--all]', file=sys.stderr, flush=True)

def peer(arg, state: State):
    if len(arg) != 1:
        print('pubsubserver: unknown argument(s) - usage: /peer [server]:port', file=sys.stderr, flush=True)
        return
    server = arg[0]
    if ':' not in server or server[server.index(':') + 1:] == '':
        print('pubsubserver: unknown argument(s) - usage: /peer [server]:port', file=sys.stderr, flush=True)
        return
    server_connect(server, state)

def limit(args, state: State):
    if len(args) != 3:
        print('pubsubserver: unknown argument(s) - usage: /limit clientid topic N', file=sys.stderr, flush=True)
        return
    
    cid, topic, N = args

    with state_lock:
        exists = cid in state.clients
    
    if not clientid_check(cid) or not exists:
        print(f'pubsubserver: Client "{cid}" is unknown', file=sys.stderr, flush=True)
        return
        
    if not topic_check(topic):
        print(f'pubsubserver: Topic "{topic}" is not valid', file=sys.stderr, flush=True)
        return
        
    if not N.isdigit() or not (0 <= int(N) <= 3600):
        print(f'pubsubserver: Rate limit must be 0 to 3600 seconds inclusive', file=sys.stderr, flush=True)
        return
        
    limit = int(N)

    with state_lock:
        state.limits.setdefault(cid, {})[topic] = limit
        client_conn = state.clients.get(cid)

    if client_conn:
        send_layer(client_conn.sock, CLIENT_RATE_LIMIT, topic, str(limit))

    for peer in list(state.peers.values()):
        send_layer(peer.sock, SERVER_RATE_LIMIT, cid, topic, str(limit))

def main():
    params = parse(sys.argv)

    serverid_check(params.sid)

    state = State(params.sid)
    state.servers.add(params.sid)

    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    if params.listen:
        try:
            if params.listen.isdigit():
                port = int(params.listen)
            else:
                port = socket.getservbyname(params.listen)
            sock.bind(('', port))
        except (OSError, ValueError):
            print(f'pubsubserver: can\'t listen on port "{port}"', file=sys.stderr, flush=True)
            sys.exit(3)
    else:
        sock.bind(('', 0))

    sock.listen(128)
    actual_port = sock.getsockname()[1]
    print(f'pubsubserver: listening on port {actual_port}', file=sys.stderr)

    for server in params.servers:
        server_connect(server, state)

    accept_t = threading.Thread(target=accept, args=(sock, state))
    accept_t.start()

    stdin(state)

if __name__ == '__main__':
    main()