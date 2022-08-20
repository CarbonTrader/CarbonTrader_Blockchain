import socket as Socket
import concurrent.futures

# The following constants are standard for the whole network.
SRC_PORT = 5556
DST_PORT = 5555
PEER_ADDRS = ['10.244.220.176', '10.244.169.146']

class Node:
    """
    A node in CarbonTrader's Blockchain P2P network.
    It functions both as server and client, simultaneously, and is used to perform validation of blocks
    given the [consensus] protocol.
    """

    def __init__(self, machine_ip_address):
        self.ip_address = machine_ip_address
        pass
        # TODO: Define whatever else the constructor should instantiate.

    """""
    The following function is used for establishing a connection with each of the peers on the network.
    """""
    def establish_connection(self, peer_address, src_port, dst_port):
        sock = Socket.socket(Socket.AF_INET, Socket.SOCK_DGRAM)
        sock.bind(('0.0.0.0', src_port))
        sock.sendto(b'0', (peer_address, dst_port))
        return f'Message sent to peer with address {peer_address}'

    """""
    The following function is used to receive the incoming data on the dst_port.
    'dst_port' is used to listen to incoming data on *any* of the nodes.
    """""
    def listen(self, dst_port):
        sock = Socket.socket(Socket.AF_INET, Socket.SOCK_DGRAM)
        sock.bind(('0.0.0.0', dst_port))
        while True:
            # TODO: Find out what the 'recv()' argument 1024 (I assume bits) does.
            data = sock.recv(1024)
            print(data.decode())

    """""
    The following function triggers multiple threads:
    One of them listens on the DST_PORT.
    The remaining number, which is linearly dependent on the number of peers (the current not included) on the network, establishes connection with them.
    """""

    def execute(self):
        with concurrent.futures.ThreadPoolExecutor() as thread_executor:
            thread_executor.submit(self.listen, DST_PORT)
            for peer_address in PEER_ADDRS:
                if peer_address == self.ip_address:
                    continue
                future = thread_executor.submit(self.establish_connection, peer_address, SRC_PORT, DST_PORT)
                print(future.result())
