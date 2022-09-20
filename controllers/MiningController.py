import json

from blockchain.Block import Block
from blockchain.Blockchain import Blockchain
from integrators.DataIntegraton import DataIntegrator
from integrators.Parameters import Parameters
import time

class MiningController:
    @staticmethod
    def begin_mining(mining_publisher, mining_topic_path, winner):
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
        if winner == Parameters.get_node_id():
            MiningController.mine_new_block(mining_publisher, mining_topic_path, blockchain, transactions_to_mine)
        else:
            MiningController.validate_new_block(blockchain,transactions_to_mine, mining_publisher, mining_topic_path)
        return "done"

    @staticmethod
    def mine_new_block(mining_publisher, mining_topic_path, blockchain, transactions_to_mine):
        transactions_hashes = Blockchain.obtain_transactions_hashes(transactions_to_mine)
        new_block = blockchain.create_not_verify_block(transactions_hashes)
        MiningController.broadcast_new_block(mining_publisher, mining_topic_path, new_block)
        print("a")
        #TODO:
        DataIntegrator.write_json("db/new_block.json", {})
        DataIntegrator.write_json("db/transactions_to_mine.json", [])

    @staticmethod
    def validate_new_block(blockchain, transactions_to_mine, mining_publisher, mining_topic_path):
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
        MiningController.broadcast_validation(mining_publisher, mining_topic_path, is_valid)

        #TODO:
        DataIntegrator.write_json("db/new_block.json", {})
        DataIntegrator.write_json("db/transactions_to_mine.json",[])


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
