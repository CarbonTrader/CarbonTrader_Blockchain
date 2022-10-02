from blockchain.Blockchain import Blockchain
from integrators.DataIntegrator import DataIntegrator
import random
import time
import json
import logging
from integrators.Parameters import Parameters

#Initialize logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s, %(name)s %(levelname)s : %(message)s')
file_handler = logging.FileHandler('db/blockchain.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)
class ConsensusController:
    @staticmethod
    def consensus_algorithm(api_publisher, api_topic_path):
        logger.info("Starting consensus algorithm.")
        ConsensusController.broadcast_number(api_publisher, api_topic_path)
        # TODO: Death nodes
        nodes = DataIntegrator.read_json("db/nodes.json")
        timeout = time.time() + 60 * Parameters.get_time_out()  # 5 minutes from now
        while not ConsensusController.is_nodes_done(nodes):
            time.sleep(1)
            logger.info("Waiting for random number from nodes.")
            nodes = DataIntegrator.read_json("db/nodes.json")
            if time.time() > timeout:
                break
        winner = ConsensusController.get_consensus_winner()
        logger.info("Unverified winner {}.".format(winner))
        DataIntegrator.reset_consensus_nodes()
        ConsensusController.notify_winner(
            api_publisher, api_topic_path, winner)
        verify_winner = ConsensusController.establish_winner()
        logger.info(f"Verify winner {verify_winner}.")
        return verify_winner

    #TODO: REFACTOR

    @staticmethod
    def notify_winner(api_publisher, api_topic_path, winner):
        logger.info("Notifying nodes about the local winner.")
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
        logger.info('Local number generated for consensus algorithm {}.'.format(number))
        logger.info("Broadcasting local number.")
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
        logger.info("Establishing with the global winner nodes.")
        nodes = DataIntegrator.read_json("db/winner.json")
        timeout = time.time() + 60 * Parameters.get_time_out()  # 5 minutes from now
        while not ConsensusController.is_winner_done(nodes):
            time.sleep(1)
            logger.info("Waiting for response from nodes about winner.")
            nodes = DataIntegrator.read_json("db/winner.json")
            if time.time() > timeout:
                break
        if ConsensusController.verify_consensus_winner():
            DataIntegrator.reset_consensus_winners()
            return nodes[Parameters.get_node_id()]
        return None

    @staticmethod
    def is_winner_done(data):
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
            logger.info('Sending node {}, consensus number {}'.format(sender, number))
            DataIntegrator.write_json("db/nodes.json", nodes)

    @staticmethod
    def handle_consensus_verifier_message(message):
        winner = message['winner']
        sender = message['sender']
        nodes = DataIntegrator.read_json("db/winner.json")

        if sender != Parameters.get_node_id():
            nodes[sender] = winner
            DataIntegrator.write_json("db/winner.json", nodes)
