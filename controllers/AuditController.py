from blockchain.Blockchain import Blockchain
import time


class AuditController:
    @staticmethod
    def audit_full_blockchain():
        blockchain = Blockchain()
        blockchain.upload_blockchain()
        # Block.is_valid_block(block_from_file, blockchain.chain[-1], transactions, transactions_hashes)
        AuditController.check_every_block(blockchain.chain)

    """
    [1,2,3,4]"""

    @staticmethod
    def check_every_block(chain):
        for i in range(len(chain)-1, -1, -1):
            print(chain[i].__dict__)

