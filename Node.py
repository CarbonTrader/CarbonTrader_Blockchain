import zmq

TEST_NODE_ADDRESSES = ['']

class Node: 
    """
    A node in CarbonTrader's Blockchain P2P network.
    It functions both as server and client, simultaneously, and is used to perform validation of blocks
    given the [consensus] protocol.
    """
    def __init__(self, machine_ip_address) -> None:
        self.ip_address = machine_ip_address
        # TODO: Define what else the constructor should do.
        pass

    def connection_test(self, peer_addresses):
        for peer_address in peer_addresses:
            if(peer_address == self.ip_address):
                continue
            else:
                # TODO: Send a message to each server and await a response.
                print(peer_address)

    def request():
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect("tcp://10.244.220.176")
        socket.send("Hola")
        print(socket.recv())

    def reply():
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        socket.bind("tcp://*:5556")
        req = socket.recv()
        socket.send(req) 
