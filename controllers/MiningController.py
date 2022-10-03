import json

from blockchain.Block import Block
from blockchain.Blockchain import Blockchain
from integrators.DataIntegrator import DataIntegrator
from integrators.Parameters import Parameters
import time
import logging
import requests

# Initialize logger
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


class MiningController:
    @staticmethod
    def begin_mining(mining_publisher, mining_topic_path, winner):
        logger.info("Starting mining.")
        blockchain = Blockchain()
        blockchain.upload_blockchain()
        try:
            if not blockchain.chain:
                logger.error('The blockchain is not initialize!')
                raise ValueError('The blockchain is not initialize!')
        # TODO: Check if thread is gone
        except ValueError as err:
            logger.error(str(err.args))
            exit()
        transactions_to_mine = DataIntegrator.fetch_transactions_to_mine()
        is_agreed_valid = False
        if winner == Parameters.get_node_id():
            logger.info("This node is the miner.")
            is_agreed_valid = MiningController.mine_new_block(mining_publisher, mining_topic_path, blockchain,
                                                              transactions_to_mine)
        else:
            logger.info("This node is a validator.")
            is_agreed_valid = MiningController.validate_new_block(blockchain, transactions_to_mine, mining_publisher,
                                                                  mining_topic_path)
        DataIntegrator.reset_mining()
        logger.info("Mining is done.")
        return is_agreed_valid

    @staticmethod
    def mine_new_block(mining_publisher, mining_topic_path, blockchain, transactions_to_mine):
        transactions_hashes = Blockchain.obtain_transactions_hashes(transactions_to_mine)
        logger.info("Generating new block.")
        new_block = blockchain.create_not_verify_block(transactions_hashes)
        logger.info("Broadcasting new block.")
        MiningController.broadcast_new_block(mining_publisher, mining_topic_path, new_block)
        is_agreed_valid = MiningController.validate_new_block(blockchain, transactions_to_mine, mining_publisher,
                                                              mining_topic_path)
        if is_agreed_valid:
            logger.info("Updating local blockchain.")
            blockchain = DataIntegrator.read_json("db/blockchain.json")
            logger.info("Updating API blockchain.")
            MiningController.broadcast_chain_to_alter_nodes(mining_publisher,mining_topic_path)
            r = requests.put(url=Parameters.get_url_backup(), json=blockchain)
        else:
            # TODO: Get info from backup
            blockchain = requests.get(Parameters.get_url_backup())
            print(blockchain)
            DataIntegrator.update_blockchain(blockchain)
            logger.info("Asking API for replacement")

        return is_agreed_valid

    @staticmethod
    def validate_new_block(blockchain, transactions_to_mine, mining_publisher, mining_topic_path):
        transactions_hashes = Blockchain.obtain_transactions_hashes(transactions_to_mine)
        new_block_to_verify = DataIntegrator.read_json("db/new_block.json")
        is_valid = False
        timeout = time.time() + 60 * Parameters.get_time_out()  # 5 minutes from now
        while not new_block_to_verify:
            time.sleep(1)
            new_block_to_verify = DataIntegrator.read_json("db/new_block.json")
            if time.time() > timeout:
                break
                # TODO: Restart
        if Block.is_valid_block(new_block_to_verify, blockchain.chain[-1], transactions_to_mine, transactions_hashes):
            logger.info("New block determined to be valid")
            is_valid = True
        else:
            logger.warning("New block determined to be invalid")
        DataIntegrator.persist_validation(is_valid, Parameters.get_node_id())
        logger.info("Broadcasting local validation.")
        MiningController.broadcast_validation(mining_publisher, mining_topic_path, is_valid)
        logger.info("Receiving validations from nodes.")
        is_agreed_valid = MiningController.fetch_nodes_validation()
        if is_agreed_valid:
            MiningController.update_blockchain(blockchain, new_block_to_verify, is_valid)
            logger.info("Nodes agreed valid block.")
        else:
            logger.warning("Nodes agreed invalid block.")
            # TODO: Restart consensus algo.
        return is_agreed_valid

    @staticmethod
    def update_blockchain(blockchain, new_block, is_valid):
        if is_valid:
            blockchain.add_block(new_block)
            DataIntegrator.update_blockchain(blockchain.chain)
            logger.info("Blockchain updated.")
        else:
            logger.warning("Blockchain alter, must receive new.")
            # TODO: Ask for back up

    @staticmethod
    def broadcast_chain_to_alter_nodes(mining_publisher, mining_topic_path):
        nodes = DataIntegrator.read_json("db/validation.json")
        alter_nodes = MiningController.get_alter_nodes(nodes)
        blockchain = DataIntegrator.read_json("db/blockchain.json")
        if alter_nodes:
            data = {
                'type': 'recovery',
                'sender': Parameters.get_node_id(),
                'alter_nodes': alter_nodes,
                'blockchain': blockchain
            }
            message_to_send = json.dumps(data, ensure_ascii=False).encode('utf8')
            future1 = mining_publisher.publish(mining_topic_path, message_to_send)
            future1.result()


    @staticmethod
    def get_alter_nodes(nodes):
        alter_nodes = []
        for k, v in nodes.items():
            if not v and v != "":
                alter_nodes.append(k)
        return alter_nodes

    @staticmethod
    def fetch_nodes_validation():
        nodes = DataIntegrator.read_json("db/validation.json")
        timeout = time.time() + 60 * Parameters.get_time_out()  # 5 minutes from now
        while not MiningController.is_validation_done(nodes):
            time.sleep(1)
            logger.info("Waiting for validation results from nodes.")
            nodes = DataIntegrator.read_json("db/validation.json")
            if time.time() > timeout:
                break
        is_valid = MiningController.validate_fifty_one_acceptance()
        if is_valid:
            return True
        else:
            return False

    @staticmethod
    def validate_fifty_one_acceptance():
        logger.info("Validating 51% acceptance.")
        nodes = DataIntegrator.read_json("db/validation.json")
        alive_nodes = [v for _, v in nodes.items() if v != '']
        fifty_one_percent = len(alive_nodes) * 0.51
        if alive_nodes.count(True) >= fifty_one_percent:
            return True
        else:
            return False

    @staticmethod
    def is_validation_done(nodes):
        for k, v in nodes.items():
            if v == "":
                return False
        return True

    @staticmethod
    def handle_new_block_message(message):
        new_block = message['new_block']
        sender = message['sender']

        if sender != Parameters.get_node_id():
            DataIntegrator.write_json("db/new_block.json", new_block)

    @staticmethod
    def handle_validation_message(message):
        validation = message['validation']
        sender = message['sender']

        if sender != Parameters.get_node_id():
            DataIntegrator.persist_validation(validation, sender)

    @staticmethod
    def broadcast_validation(mining_publisher, mining_topic_path, validation):
        data = {
            'type': 'validation',
            'sender': Parameters.get_node_id(),
            'validation': validation,
        }
        message_to_send = json.dumps(data, ensure_ascii=False).encode('utf8')
        future1 = mining_publisher.publish(mining_topic_path, message_to_send)
        future1.result()

    @staticmethod
    def broadcast_new_block(mining_publisher, mining_topic_path, new_block):
        data = {
            'type': 'new_block',
            'sender': Parameters.get_node_id(),
            'new_block': new_block.__dict__,
        }
        message_to_send = json.dumps(data, ensure_ascii=False).encode('utf8')
        future1 = mining_publisher.publish(mining_topic_path, message_to_send)
        future1.result()

    @staticmethod
    def handle_recovery_message(message):
        alter_nodes = message['alter_nodes']
        blockchain = message['blockchain']

        if Parameters.get_node_id() in alter_nodes:
            logger.info("Replacing blockchain.")
            DataIntegrator.update_blockchain(blockchain)

