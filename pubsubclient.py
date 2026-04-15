from socket import *
import sys
from sys import stdout,stdin,argv,exit

BUFSIZE = 1024

def handle_input(*arg):
    len = len(arg)
    if len == 0:
        print("Usage: python pubsubclient.py <hostname> <port>")
        exit(1)
    

if __name__ == "__main__":
    args = sys.argv[1:]
    handle_input(*args)
    
    