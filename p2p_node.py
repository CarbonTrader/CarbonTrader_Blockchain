import sys
import socket as Socket
import concurrent.futures as Futures

def establish_connection(peer_address, src_port, dst_port):
    print(f'Establishing connection with peer with IP address {peer_address}')
    sock = Socket.socket(Socket.AF_INET, Socket.SOCK_DGRAM)
    sock.bind((b'0', src_port))
    sock.sendto(b'0', (peer_address, dst_port))

SRC_PORT = '5555'
DST_PORT = '5556'

# Thread Pool Executor
with Futures.ThreadPoolExecutor() as peer_conn_executor:
    peer_addresses = ['10.244.220.176']
    for peer_address in peer_addresses:
        peer_conn_executor.map(establish_connection, peer_address, SRC_PORT, DST_PORT)