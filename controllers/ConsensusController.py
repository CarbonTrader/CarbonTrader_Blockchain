from integrators.DataIntegrator import DataIntegrator
import random
import time
import json
import logging
from config.Parameters import Parameters

# Initialize logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s, %(name)s %(levelname)s : %(message)s')
file_handler = logging.FileHandler('db/blockchain.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)

NODES = {}

WINNER = {}

# TODO:Borrar prints


class ConsensusController:
    @staticmethod
    def consensus_algorithm(api_publisher, api_topic_path):
        global NODES
        ConsensusController.init_consensus_objects()
        logger.info("Starting consensus algorithm.")
        ConsensusController.broadcast_number(api_publisher, api_topic_path)
        timeout = time.time() + 60 * Parameters.get_time_out()
        while not ConsensusController.is_nodes_done():
            time.sleep(1)
            logger.info("Waiting for random number from nodes.")
            if time.time() > timeout:
                break
        ConsensusController.update_log_death_nodes_random_nunmber()
        winner = ConsensusController.get_consensus_winner()
        logger.info("Unverified winner {}.".format(winner))
        ConsensusController.notify_winner(
            api_publisher, api_topic_path, winner)
        verify_winner = ConsensusController.establish_winner()
        logger.info(f"Verify winner {verify_winner}.")
        return verify_winner

    # TODO: REFACTOR

    @staticmethod
    def notify_winner(api_publisher, api_topic_path, winner):
        global WINNER
        logger.info("Notifying nodes about the local winner.")
        WINNER[Parameters.get_node_id()] = winner
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

        logger.info(
            'Local number generated for consensus algorithm {}.'.format(number))
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
        global WINNER
        logger.info("Establishing the global winner nodes.")
        timeout = time.time() + 60 * Parameters.get_time_out()
        while not ConsensusController.is_winner_done():
            time.sleep(1)
            logger.info("Waiting for response from nodes about winner.")
            if time.time() > timeout:
                break
        ConsensusController.update_log_death_nodes_winner()
        if ConsensusController.verify_consensus_winner():
            return WINNER[Parameters.get_node_id()]
        return None

    @staticmethod
    def is_winner_done():
        for k, v in WINNER.items():
            if v == "":
                return False
        return True

    @staticmethod
    def verify_consensus_winner():
        winner = WINNER[Parameters.get_node_id()]
        for k, v in WINNER.items():
            if v != winner and v != "":
                return False
        return True

    @staticmethod
    def generate_random_number():
        global NODES
        number = random.uniform(0, 1)
        NODES[Parameters.get_node_id()] = number
        return number

    @staticmethod
    def get_consensus_winner():
        global NODES
        max = -2
        winner = "winner"
        for k, v in NODES.items():
            if v > max:
                max = v
                winner = k
        return winner

    @staticmethod
    def is_nodes_done():
        global NODES

        for k, v in NODES.items():
            if v == -1:
                return False
        return True

    @staticmethod
    def init_consensus_objects():
        global NODES, WINNER
        NODES = DataIntegrator.reset_consensus_nodes()
        WINNER = DataIntegrator.reset_consensus_winners()

    @staticmethod
    def update_log_death_nodes_random_nunmber():
        for k, v in NODES.items():
            if v == -1:
                logger.warning(
                    f"There was no random number response from node {k}.")

    @staticmethod
    def update_log_death_nodes_winner():
        for k, v in WINNER.items():
            if v == "":
                logger.warning(f"There was no winner response from node {k}.")

    @staticmethod
    def handle_consensus_message(message):
        global NODES

        number = message['number']
        sender = message['sender']

        if sender != Parameters.get_node_id():
            NODES[sender] = number
            logger.info(
                'Sending node {}, consensus number {}'.format(sender, number))

    @staticmethod
    def handle_consensus_verifier_message(message):
        global WINNER
        winner = message['winner']
        sender = message['sender']

        if sender != Parameters.get_node_id():
            WINNER[sender] = winner
