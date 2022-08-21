import socket as Socket
import concurrent.futures

# The following constants are standard for the whole network.
SRC_PORT = 5556
DST_PORT = 5555
MTCAST_ADDR_GROUP = '10.244.1.1'
PEER_ADDRS = ['10.244.169.146', '10.244.220.176']

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
    The following function is used to receive the incoming data on the dst_port.
    DST_PORT constant is used to listen to incoming data on *any* of the nodes.
    """""
    def listen(self, dst_port):
        no_data = True
        sock = Socket.socket(Socket.AF_INET, Socket.SOCK_DGRAM)
        sock.bind(('0.0.0.0', dst_port))
        while no_data:
            # TODO: Find out what the 'recv()' argument 1024 (I assume bits) does.
            data = sock.recv(1024)
            if(data is not None):
                no_data = False
                print(data.decode())
        return data.decode()

    """""
    The following function sends multiple messages.
    The number of messages sent is identical to the number of peers a node has on the network:
    i.e. number of nodes in the network - 1.
    """""
    def heartbeat(self):
        sock = Socket.socket(Socket.AF_INET, Socket.SOCK_DGRAM)
        sock.bind(('0.0.0.0', SRC_PORT))
        for peer_address in PEER_ADDRS:
            if peer_address == self.ip_address:
                continue
            sock.sendto(b'0', (peer_address, DST_PORT))
            print(f'Message sent to peer with address {peer_address}')

    """""
    The following function is similar to the one above. 
    However, this one uses the multicast option to multicast to a group of addresses on the network, rather than sending the message one by one.
    """""
    def heartbeat_multicast(self):
        sock = Socket.socket(Socket.AF_INET, Socket.SOCK_DGRAM)
        sock.bind(('0.0.0.0', SRC_PORT))
        # TODO: Define the number of hops for the message.
        sock.setsockopt(Socket.IPPROTO_IP, Socket.IP_MULTICAST_TTL, hops=2)
        sock.sendto(b'0', (MTCAST_ADDR_GROUP, DST_PORT))

    """""
    The following function triggers two concurrent threads:
    One of them listens on the DST_PORT.
    The remaining, sends a heartbeat message to each of this node's peers. 
    """""
    def execute(self):
        with concurrent.futures.ThreadPoolExecutor() as thread_executor:
            port_listener = thread_executor.submit(self.listen, DST_PORT)
            
            thread_executor.submit(self.heartbeat)
            # thread_executor.submit(self.heartbeat_multicast)

            # 'timeout' parameter sets a timer which, when finishing the countdown, if no data has been received, the thread raises a TimeOutError exception.
            # TODO: Handle TimeOutError for the following function callback.
            port_listener.result(timeout=10)
            thread_executor.shutdown(wait=True)
