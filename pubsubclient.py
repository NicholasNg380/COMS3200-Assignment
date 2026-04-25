from socket import *
import sys
from sys import stdout,stdin,argv,exit


USAGE_MSG = "Usage: pubsubclient [--topic topic] [server]:port clientid [message]\n"

class Parameters:
    def __init__(self):
        self.topic = None
        self.server = None
        self.port = None
        self.clientid = None
        self.message = None

def parse(argv):
    params = Parameters()
    i = 1

    # checks if topic is first
    if i < len(argv) and argv[i] == "--topic":
        if i + 1 >= len(argv):
            usage_err()
        params.topic = argv[i + 1]
        if not params.topic:  # empty string check
            usage_err()
        i += 2

    if i < len(argv) and argv[i].startswith("--"):
        usage_err()    

    if i >= len(argv):
        usage_err()
        serverport = argv[i]
        if ":" not in serverport:
            usage_err()
        colon_idx = serverport.index(":")
        params.server = serverport[:colon_idx] if serverport[:colon_idx] else "localhost"
        params.port = serverport[colon_idx + 1:]
        if not params.port:  
            usage_err()
        i += 1

        if i >= len(argv):
            usage_err()
        params.clientid = argv[i]
        if not params.clientid:
            usage_err()
        i += 1

        if i < len(argv):
            if not params.topic:  # message given but no --topic
                usage_err()
            params.message = argv[i]
            if not params.message:
                usage_err()
            i += 1

        if i < len(argv):
            usage_err()

    return params

def usage_err():
    print(USAGE_MSG, file=sys.stderr)
    sys.exit(1)

def main():
    params = parse(sys.argv)


if __name__ == "__main__":
    main()