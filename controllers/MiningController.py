from blockchain.Blockchain import Blockchain
from integrators.DataIntegraton import DataIntegrator
from integrators.Parameters import Parameters


class MiningController:
    @staticmethod
    def begin_mining(winner):
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
        if winner  == Parameters.get_node_id():
            MiningController.mine_new_block(blockchain,transactions_to_mine)
        else:
            MiningController.validate_new_block(blockchain)
        return "done"
    @staticmethod
    def mine_new_block(blockchain, transactions_to_mine):
        transactions_hashes = Blockchain.obtain_transactions_hashes(transactions_to_mine)
        new_block = blockchain.create_not_verify_block(transactions_hashes)
    @staticmethod
    def validate_new_block(blockchain):
        while new_block:
            new_block = DataIntegrator.read_json("db/new_block2.json")
        print("Empece a validar")

    @staticmethod
    def handle_new_block_message(message):
        new_block = message['new_block']
        sender = message['sender']

        if sender != Parameters.get_node_id():
            DataIntegrator.write_json("db/new_block2.json", new_block)