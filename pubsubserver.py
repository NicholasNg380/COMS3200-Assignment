"""! @file pubsubserver.py
"""

import sys
import socket
import threading
import re

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

# Server message types for sending and recieving
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
        self.remote_clients = {}     # {client_id: server_id}
        self.limits = {}        # {client_id: {topic: N_seconds}}
        self.last_published = {}     # {client_id: {topic: timestamp}}

class ClientConn:
    def __init__(self, cid, sock):
        self.cid = cid
        self.sock = sock
        self.subscriptions = []      # list of (topic, filter_str)

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

    # remaining must be exactly: serverid
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

    print(f'pubsubserver: Connected to peer "{pid}" at "{server}"', flush=True)
    
    t = threading.Thread(target=handle_peer, args=(pid, conn, state), daemon=True)
    t.start()


def main():
    params = parse(sys.argv)
    state = State(params.sid)
    state.servers.add(params.sid)

    serverid_check(params.sid)

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

if __name__ == '__main__':
    main()