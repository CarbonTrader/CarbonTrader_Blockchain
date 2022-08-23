import socket as Socket
import concurrent.futures
from threading import current_thread, enumerate
import time
import struct

# The following constants are standard for the whole network.
SRC_PORT = 5556
DST_PORT = 5555
MCAST_PORT = 5557
MCAST_ADDR_GROUP = '224.1.1.1'
PEER_ADDRS = ['10.244.169.146', '10.244.220.176']

class Node:
    """
    A node in CarbonTrader's Blockchain P2P network.
    It functions both as server and client, simultaneously, and is used to perform validation of blocks
    given the [consensus] protocol.
    """

    node_is_alive = True

    def __init__(self, machine_ip_address):
        self.ip_address = machine_ip_address
        pass
        # TODO: Define whatever else the constructor should instantiate.

    """""
    The following function is used to receive the incoming data on the dst_port.
    DST_PORT constant is used to listen to incoming data on *any* of the nodes.
    """""
    def listen(self, dst_port):

        print(f'Listener thread: {current_thread().name}')

        data = None
        sock = Socket.socket(Socket.AF_INET, Socket.SOCK_DGRAM)
        sock.bind(('0.0.0.0', dst_port))
        while data is None:
            # TODO: Find out what the 'recv()' argument 1024 (I assume bits) does.
            data = sock.recv(1024)
        return print('Data received: ' + data.decode())

    """""
    The following function is used to receive incoming data sent to the multicast group (MTCAST_ADDR_GROUP).
    """""
    def listen_multicast(self, mcast_group, mcast_port):

        print('Listener thread launched...')

        sock = Socket.socket(Socket.AF_INET, Socket.SOCK_DGRAM, Socket.IPPROTO_UDP)
        sock.setsockopt(Socket.SOL_SOCKET, Socket.SO_REUSEADDR, 1)
        sock.bind((MCAST_ADDR_GROUP, mcast_port))
        mreq = struct.pack("4sl", Socket.inet_aton(mcast_group), Socket.INADDR_ANY)
        sock.setsockopt(Socket.IPPROTO_IP, Socket.IP_ADD_MEMBERSHIP, mreq)
        while True:
            print(sock.recv(10240))


    # TODO: For the heartbeat functions, define which kind of loop do they have to be inside of, as well as the frequency of the signal.
    """""
    The following function sends multiple messages.
    The number of messages sent is identical to the number of peers a node has on the network:
    i.e. number of nodes in the network - 1.
    """""
    def heartbeat(self, dst_port, src_port):

        print(f'Heartbeat thread: {current_thread().name}')

        sock = Socket.socket(Socket.AF_INET, Socket.SOCK_DGRAM)
        sock.bind(('0.0.0.0', src_port))
        while self.node_is_alive:
            for peer_address in PEER_ADDRS:
                if peer_address == self.ip_address:
                    continue
                sock.sendto(f'{self.ip_address}'.encode(), (peer_address, dst_port))
                print(f'Message sent to peer with address {peer_address}')
                time.sleep(1)
        return None

    """""
    The following function is similar to the one above. 
    However, this one uses the multicast option to multicast to a group of addresses on the network, rather
     than sending the message one by one.
    """""
    def heartbeat_multicast(self, mcast_group, dst_port, src_port):

        print('Heartbeat thread launched...')

        sock = Socket.socket(Socket.AF_INET, Socket.SOCK_DGRAM)
        sock.bind((MCAST_ADDR_GROUP, src_port))
        # TODO: Define the number of hops for the message.
        sock.setsockopt(Socket.IPPROTO_IP, Socket.IP_MULTICAST_TTL, hops=2)
        while True:
            sock.sendto(f'{self.ip_address}'.encode(), (mcast_group, dst_port))
            print(f'Message sent to multicast group address {mcast_group}')
            time.sleep(5)

    """""
    The following function triggers two concurrent threads:
    One of them listens on the DST_PORT.
    The remaining, sends a heartbeat message to each of this node's peers. 
    """""
    def execute(self):
        with concurrent.futures.ThreadPoolExecutor() as thread_executor:
            try:
                while self.node_is_alive:
                    port_listener_thread = thread_executor.submit(self.listen, DST_PORT)
                    # thread_executor.submit(self.listen_multicast, MCAST_ADDR_GROUP, DST_PORT)
                    
                    thread_executor.submit(self.heartbeat, DST_PORT, SRC_PORT)
                    # thread_executor.submit(self.heartbeat_multicast, MCAST_ADDR_GROUP, DST_PORT, SRC_PORT)

                    # 'timeout' parameter sets a timer which, when finishing the countdown, 
                    # if no data has been received, the thread raises a TimeoutError exception.
                    port_listener_thread.result(timeout = 10)
            except concurrent.futures.TimeoutError:
                self.node_is_alive = False
                print('Node is dead. Stopping all tasks...')
                thread_executor.shutdown(wait = False)
                
                # TODO: Find a way of stopping all currently active threads.
                print('Currently active threads:')
                for thread in enumerate():
                    print(thread.name)