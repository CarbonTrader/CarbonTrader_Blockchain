from DataIntegraton import DataIntegrator
import threading
import random
import time
import json
NODE_ID = 'Node1'
MAX_TRANSACTIONS_PER_BLOCK = 3

class ConsensusController:
    @staticmethod
    def consensus_algorithm(api_publisher, api_topic_path):
        ConsensusController.broadcast_number(api_publisher,api_topic_path)
        # TODO: Death nodes
        nodes = DataIntegrator.read_json("nodes.json")
        timeout = time.time() + 60 * 1  # 5 minutes from now
        while not ConsensusController.is_nodes_done(nodes) or time.time() > timeout:
            time.sleep(1)
            print("Consensus: wating for nodes")
            nodes = DataIntegrator.read_json("nodes.json")
        print("Recibí todos los mensajes")
        print("Gano: {}".format(ConsensusController.get_consensus_winner()))
        DataIntegrator.reset_consensus_nodes()
        DataIntegrator.write_json("transactions_to_mine.json", [])

    @staticmethod
    def broadcast_number(api_publisher, api_topic_path):
        number = ConsensusController.generate_random_number()
        print('Genere:{}'.format(number))
        data = {
            'type': 'consensus_message',
            'sender': NODE_ID,
            'number': number,
        }
        message_to_send = json.dumps(data, ensure_ascii=False).encode('utf8')
        future1 = api_publisher.publish(api_topic_path, message_to_send)
        future1.result()



    @staticmethod
    def generate_random_number():
        number = random.uniform(0, 1)
        nodes = DataIntegrator.read_json("nodes.json")
        nodes[NODE_ID] = number
        DataIntegrator.write_json("nodes.json", nodes)
        return number

    @staticmethod
    def get_consensus_winner():
        nodes = DataIntegrator.read_json("nodes.json")
        max = -2
        winner = "winner"
        for k, v in nodes.items():
            if v > max:
                max = v
                winner = k
        return winner

    @staticmethod
    def is_nodes_done(data):
        for k, v in data.items():
            if v == -1:
                return False
        return True

    @staticmethod
    def handle_consensus_message(message):
        number = message['number']
        sender = message['sender']
        nodes = DataIntegrator.read_json("nodes.json")

        if sender != NODE_ID:
            nodes[sender] = number
            print('Emisor: {}, numero: {}'.format(sender, number))
            print('cant-nodes:{}'.format(len(nodes)))
            DataIntegrator.write_json("nodes.json", nodes)