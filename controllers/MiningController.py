import json

from blockchain.Block import Block
from blockchain.Blockchain import Blockchain
from integrators.DataIntegraton import DataIntegrator
from integrators.Parameters import Parameters
import time
import requests

class MiningController:
    @staticmethod
    def begin_mining(mining_publisher, mining_topic_path, winner):
        print("Begin mining...")
        blockchain = Blockchain()
        blockchain.upload_blockchain()
        try:
            if not blockchain.chain:
                raise ValueError('The blockchain is not initialize!')
        # TODO: Check if thread is gone
        except ValueError as err:
            print("Error:" + str(err.args))
            exit()
        transactions_to_mine = DataIntegrator.fetch_transactions_to_mine()
        print("winner: "+ winner)
        is_agreed_valid = False
        if winner == Parameters.get_node_id():
            print("Mining....")
            is_agreed_valid = MiningController.mine_new_block(mining_publisher, mining_topic_path, blockchain, transactions_to_mine)
        else:
            is_agreed_valid = MiningController.validate_new_block(blockchain,transactions_to_mine, mining_publisher, mining_topic_path)
        return is_agreed_valid

    @staticmethod
    def mine_new_block(mining_publisher, mining_topic_path, blockchain, transactions_to_mine):
        transactions_hashes = Blockchain.obtain_transactions_hashes(transactions_to_mine)
        print("Generating new block...")
        new_block = blockchain.create_not_verify_block(transactions_hashes)
        print("Broadcasting new block...")
        MiningController.broadcast_new_block(mining_publisher, mining_topic_path, new_block)
        is_agreed_valid = MiningController.validate_new_block(blockchain, transactions_to_mine, mining_publisher, mining_topic_path)
        if is_agreed_valid:
            #TODO: Update backup
            blockchain = DataIntegrator.read_json("db/blockchain.json")
            r = requests.post(url=Parameters.get_url_backup(), json=blockchain)
        else:
            #TODO: Get info from backup
            blockchain = requests.get(Parameters.get_url_backup())
            print(blockchain)
            pass
        return is_agreed_valid


    @staticmethod
    def validate_new_block(blockchain, transactions_to_mine, mining_publisher, mining_topic_path):
        print("Validating...")
        transactions_hashes = Blockchain.obtain_transactions_hashes(transactions_to_mine)
        new_block_to_verify = DataIntegrator.read_json("db/new_block.json")
        is_valid = False
        #TODO: Set timeout
        while not new_block_to_verify:
            time.sleep(1)
            new_block_to_verify = DataIntegrator.read_json("db/new_block.json")
        if Block.is_valid_block(new_block_to_verify, blockchain.chain[-1], transactions_to_mine, transactions_hashes):
            print("Block is valid")
            is_valid = True
        else:
            print("Block is invalid")
        DataIntegrator.persist_validation(is_valid, Parameters.get_node_id())
        print("Broadcasting my validation...")
        MiningController.broadcast_validation(mining_publisher, mining_topic_path, is_valid)
        print("Reciving validations from nodes...")
        is_agreed_valid = MiningController.fetch_nodes_validation()
        if is_agreed_valid:
            MiningController.update_blockchain(blockchain,new_block_to_verify, is_valid)
            print("Nodes agreed valid block.")
        else:
            print("Nodes agreed invalid block.")
            #TODO: Restart consensus algo.
        DataIntegrator.reset_mining()
        return is_agreed_valid

    @staticmethod
    def update_blockchain(blockchain, new_block, is_valid):
        if is_valid:
            blockchain.add_block(new_block)
            DataIntegrator.update_blockchain(blockchain.chain)
            print("Blockchain updated")
        else:
            print("Blockchain alter, must receive new.")
            #TODO: Ask for back up


    @staticmethod
    def fetch_nodes_validation():
        nodes = DataIntegrator.read_json("db/validation.json")
        timeout = time.time() + 60 * Parameters.get_time_out()  # 5 minutes from now
        while not MiningController.is_validation_done(nodes):
            time.sleep(1)
            print("Validation: wating for nodes")
            nodes = DataIntegrator.read_json("db/validation.json")
            if time.time() > timeout:
                break
        is_valid = MiningController.validate_fifty_one_acceptance()
        if is_valid:
            print("Unir al blockchain!")
            return True

        else:
            print("Pedir al API nuevo blockchain")
            return False
    @staticmethod
    def validate_fifty_one_acceptance():
        print("Validating 51% acceptance...")
        nodes = DataIntegrator.read_json("db/validation.json")
        alive_nodes = [v for _, v in nodes.items() if v != '']
        print(alive_nodes)
        fifty_one_percent = len(alive_nodes)*0.51
        print(fifty_one_percent)
        print(alive_nodes.count(True))
        if alive_nodes.count(True) >= fifty_one_percent:
            print("We are valid")
            return True
        else:
            print("we are invalid")
            return False

    @staticmethod
    def is_validation_done(nodes):
        print(nodes.items())
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
