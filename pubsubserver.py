import sys
import socket
import threading
import re

MSG_HANDSHAKE_CLIENT  = b'\x01'
MSG_HANDSHAKE_OK      = b'\x02'
MSG_HANDSHAKE_DUP     = b'\x03'
MSG_PUBLISH           = b'\x04'
MSG_SUBSCRIBE         = b'\x05'
MSG_UNSUBSCRIBE       = b'\x06'
MSG_INCOMING          = b'\x07'
MSG_RATE_LIMIT        = b'\x08'
MSG_SERVER_QUIT       = b'\x09'
MSG_SENDFILE          = b'\x0A'
MSG_INCOMING_FILE     = b'\x0B'

# Server-to-server only messages
MSG_SRV_HANDSHAKE     = b'\x20'   # peer intro: magic + server_id + known_server_ids
MSG_SRV_HANDSHAKE_OK  = b'\x21'
MSG_SRV_HANDSHAKE_DUP = b'\x22'   # duplicate server ID in federation
MSG_SRV_HANDSHAKE_SELF= b'\x23'   # connected to self
MSG_SRV_QUIT          = b'\x24'   # server shutting down
MSG_SRV_PUBLISH       = b'\x25'   # federated publish (text)
MSG_SRV_PUBLISH_FILE  = b'\x26'   # federated publish (file)
MSG_SRV_CLIENT_JOINED = b'\x27'   # inform peers a new client joined
MSG_SRV_CLIENT_LEFT   = b'\x28'   # inform peers a client left
MSG_SRV_RATE_LIMIT    = b'\x29'   # propagate rate-limit to target server
MSG_SRV_PEER_LIST     = b'\x2A'   # exchange known server IDs during handshake

SECRET = b'PUBS1'

state = {
    'server_id': '...',
    'clients': {},          # {client_id: {'sock': ..., 'subs': [...]}}
    'peers': {},            # {peer_id: {'sock': ..., 'thread': ...}}
    'rate_limits': {},      # {client_id: {topic: N_seconds}}
    'last_published': {},   # {client_id: {topic: timestamp}} for rate limiting
    'lock': threading.Lock()
}

def usage():
    print('Usage: pubsubserver [--server [server]:port]... [--listenon port] serverid\n', file=sys.stderr)
    sys.exit(1)

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
            if sp == '' or ':' not in val:
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

def validate_server_id(sid):  # Same rules as client ID
    if not (2 <= len(sid) <= 32) or not re.fullmatch(r'[A-Za-z0-9]+', sid):
        print(f'pubsubclient: bad server ID "{sid}"', file=sys.stderr)
        sys.exit(2)

def start_listening(port):
    # socket.socket(), setsockopt, bind, listen
    # If port given but can't bind: exit 3
    # Print "listening on port N" to stderr
    sock = socket.socket()
    socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

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

def accept_connections_thread(listen_sock, state):
    # Loop: sock, addr = listen_sock.accept()
    # Spawn new thread: handle_connection(sock, addr, state)

def handle_connection(sock, addr, state):
    # Within 1 second, determine: client or peer server?
    # If client: check unique ID, add to state, start client thread
    # If peer: do peer handshake
    # If unknown: close within 1 second

def handle_client_thread(sock, client_id, state):
    # Loop: receive message from client
    # Process: subscribe, unsubscribe, publish, sendfile, quit
    # On publish: find matching subscriptions, forward message
    # On disconnect: remove subscriptions, print message

def handle_peer_thread(sock, peer_id, state):
    # Loop: receive message from peer server
    # Forward publishes to local clients if they match
    # Forward to other peers (but not back to sender!)
    # On disconnect: print message, clean up

def stdin_thread(state):
    # Loop: read line from stdin
    # Dispatch: listclients, listpeers, peer, limit, quit

def handle_listclients(args, state): ...
def handle_listpeers(args, state): ...
def handle_peer_command(args, state): ...  # Same as connect_to_peer
def handle_limit(args, state): ...
def handle_quit(state): ...  # Notify all clients & peers, then exit

def route_message(msg, topic, sender_client_id, sender_server_id, 
                  source_peer, state):
    # Find all local clients with matching subscription
    # Apply filters (numeric comparison)
    # Apply rate limits
    # Send to matching local clients
    # Forward to all peer servers EXCEPT source_peer