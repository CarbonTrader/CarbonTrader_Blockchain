from blockchain.Blockchain import Blockchain
from integrators.DataIntegraton import DataIntegrator
import random
import time
import json

from integrators.Parameters import Parameters

TIME_OUT = 0.2
class ConsensusController:
    @staticmethod
    def consensus_algorithm(api_publisher, api_topic_path):
        ConsensusController.broadcast_number(api_publisher,api_topic_path)
        # TODO: Death nodes
        nodes = DataIntegrator.read_json("db/nodes.json")
        timeout = time.time() + 60 * TIME_OUT  # 5 minutes from now
        while not ConsensusController.is_nodes_done(nodes):
            time.sleep(1)
            print("Consensus: wating for nodes")
            nodes = DataIntegrator.read_json("db/nodes.json")
            if time.time() > timeout:
                break
        winner = ConsensusController.get_consensus_winner()
        print("Gano: {}".format(winner))
        DataIntegrator.reset_consensus_nodes()
        ConsensusController.notify_winner(api_publisher, api_topic_path, winner)
        verify_winner = ConsensusController.establish_winner()
        print(f"Verify winner is : {verify_winner}")
        return verify_winner


    #TODO: REFACTOR
    @staticmethod
    def notify_winner(api_publisher, api_topic_path, winner):
        winner_nodes = DataIntegrator.read_json("db/winner.json")
        winner_nodes[Parameters.get_node_id()] = winner
        DataIntegrator.write_json("db/winner.json", winner_nodes)
        data = {
            'type': 'consensus_winner',
            'sender': Parameters.get_node_id(),
            'winner': winner,
        }
        message_to_send = json.dumps(data, ensure_ascii=False).encode('utf8')
        future1 = api_publisher.publish(api_topic_path, message_to_send)
        future1.result()
    @staticmethod
    def broadcast_number(api_publisher, api_topic_path):
        number = ConsensusController.generate_random_number()
        print('Genere:{}'.format(number))
        data = {
            'type': 'consensus_message',
            'sender': Parameters.get_node_id(),
            'number': number,
        }
        message_to_send = json.dumps(data, ensure_ascii=False).encode('utf8')
        future1 = api_publisher.publish(api_topic_path, message_to_send)
        future1.result()

    @staticmethod
    def establish_winner():
        nodes = DataIntegrator.read_json("db/winner.json")
        timeout = time.time() + 60 * TIME_OUT  # 5 minutes from now
        while not ConsensusController.is_winner_done(nodes):
            time.sleep(1)
            print("Consensus: wating for nodes")
            nodes = DataIntegrator.read_json("db/winner.json")
            if time.time() > timeout:
                break
        if ConsensusController.verify_consensus_winner():
            print("Se verifico que gano: {}".format(nodes[Parameters.get_node_id()]))
            DataIntegrator.reset_consensus_winners()
            return nodes[Parameters.get_node_id()]
        return None



    @staticmethod
    def is_winner_done(data):
        winner = data[Parameters.get_node_id()]
        for k, v in data.items():
            if v == "":
                return False
        return True

    @staticmethod
    def verify_consensus_winner():
        nodes = DataIntegrator.read_json("db/winner.json")
        winner = nodes[Parameters.get_node_id()]
        for k, v in nodes.items():
            if v != winner and v != "":
                return False
        return True

    @staticmethod
    def generate_random_number():
        number = random.uniform(0, 1)
        nodes = DataIntegrator.read_json("db/nodes.json")
        nodes[Parameters.get_node_id()] = number
        DataIntegrator.write_json("db/nodes.json", nodes)
        return number

    @staticmethod
    def get_consensus_winner():
        nodes = DataIntegrator.read_json("db/nodes.json")
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
        nodes = DataIntegrator.read_json("db/nodes.json")

        if sender != Parameters.get_node_id():
            nodes[sender] = number
            print('Emisor: {}, numero: {}'.format(sender, number))
            print('cant-nodes:{}'.format(len(nodes)))
            DataIntegrator.write_json("db/nodes.json", nodes)

    @staticmethod
    def handle_consensus_verifier_message(message):
        winner = message['winner']
        sender = message['sender']
        nodes = DataIntegrator.read_json("db/winner.json")

        if sender != Parameters.get_node_id():
            nodes[sender] = winner
            DataIntegrator.write_json("db/winner.json", nodes)
