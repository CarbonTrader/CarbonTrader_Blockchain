from blockchain.Block import Block
from blockchain.Blockchain import Blockchain
import time

from blockchain.Merkle import Merkle


class AuditController:
    """
    TODO: - Volver a generar cada bloque : Donzo motha faka
    TODO: - revisar los hashes : Donzooooooooo
    TODO: - Revisar firma: hacer en API
    """
    @staticmethod
    def audit_full_blockchain():
        blockchain = Blockchain()
        blockchain.upload_blockchain()
        print(AuditController.check_every_block_hash(blockchain.chain))
        print(AuditController.validate_transaction_in_block(blockchain.chain,2,"cdd1fc8c847174cdafe601b4f2a158e654d7b0b1703aa8c18ac8bcd0b73e30c6"))
    
    @staticmethod
    def check_every_block_hash(chain) -> bool:
        tainted_block = []
        for i in range(len(chain)-1, 0, -1):
            if not Block.is_valid_audit_block(chain[i],chain[i-1]):
                tainted_block.append(i)
        return len(tainted_block) == 0
    
    @staticmethod
    def validate_transaction_in_block(chain,block_number: int,transaction_hash: str) -> bool:
        trans = chain[block_number].get("transactions_hashes")
        merkle_proof_ = Merkle.merkle_proof(trans, transaction_hash)
        return Merkle.merkle_proof_root(merkle_proof_,transaction_hash) == chain[block_number].get("merkle_root")
            
 
