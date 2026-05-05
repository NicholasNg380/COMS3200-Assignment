from socket import *
import sys
import re
import threading
from sys import stdout,stdin,argv,exit

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