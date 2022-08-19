import zmq

TEST_NODE_ADDRESSES = ['']

class Node: 
    """
    A node in CarbonTrader's Blockchain P2P network.
    It functions both as server and client, simultaneously, and is used to perform validation of blocks
    given the [consensus] protocol.
    """
    def __init__(self, machine_ip_address):
        self.ip_address = machine_ip_address
        self.zmq_context = zmq.Context()
        # TODO: Define what else the constructor should do.

    def connection_test(self, peer_addresses):
        for peer_address in peer_addresses:
            if(peer_address == self.ip_address):
                continue
            else:
                # TODO: Send a message to each server and await a response.
                print(peer_address)

    def request(self):
        socket = self.zmq_context.socket(zmq.REQ)
        socket.connect("tcp://10.244.220.176:5556")
        for i in range(1,3):
            socket.send_string(f'Hola {i}')
            print(socket.recv())

    def reply(self):
        socket = self.zmq_context.socket(zmq.REP)
        socket.bind("tcp://*:5556")
        while True:
            req = socket.recv()
            socket.send(req) 
            print(req)