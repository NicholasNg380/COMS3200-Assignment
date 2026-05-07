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
CLIENT_MSG_PUBLISH = b'\x04' 
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
        send_layer(sock, CLIENT_MSG_PUBLISH, params.topic, params.msg)
        time.sleep(0.1)
        sys.exit(0)




if __name__ == "__main__":
    main()