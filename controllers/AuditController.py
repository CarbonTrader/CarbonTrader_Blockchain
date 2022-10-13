from blockchain.Block import Block
from blockchain.Blockchain import Blockchain
import time

from blockchain.Merkle import Merkle
from config.Parameters import Parameters
import random

class AuditController:
    @staticmethod
    def audit_full_blockchain(message):
        response = {
            "sende_id" : Parameters.get_node_id(),
            "test_results" : {
                "test_audit" : None,
                "test_signatures" : None,
                "test_transactions_in_block" : None
            }
        }
        parameters = message["parameters"]
        blockchain = Blockchain()
        blockchain.upload_blockchain()
        print(parameters)
        
        if parameters.get("audit_type") == 2:
            response["test_results"]["test_audit"] = AuditController.deep_block_hash_test(blockchain.chain)
        elif parameters.get("audit_type") == 1:
            response["test_results"]["test_audit"] = AuditController.light_block_hash_test(blockchain.chain)
        
        if  parameters.get("merkle_search"):
             response["test_results"]["merkle_search"] = AuditController.validate_transaction_in_block(blockchain.chain, parameters.get("merkle_search"))
            
        print(response)
    
    @staticmethod
    def deep_block_hash_test(chain) -> bool:
        tainted_block = []
        for i in range(len(chain)-1, 0, -1):
            if not Block.is_valid_audit_block(chain[i],chain[i-1]):
                return False
        return True
    
    
    @staticmethod
    def light_block_hash_test(chain) -> bool:
        len_half_chain = len(chain) // 2
        random_blocks = set()
        while len(random_blocks) < len_half_chain:
            random_blocks.add(random.randint(1,len(chain)-1))

        for chain_index in random_blocks:
            if not Block.is_valid_audit_block(chain[chain_index],chain[chain_index-1]):
               return False
            
        return True
    
    @staticmethod
    def validate_transaction_in_block(chain,search_items) -> list:
        search_items_response = []
        for item in search_items:
            block_number = item[0]
            transaction_hash = item[1]
            trans = chain[block_number].get("transactions_hashes")
            merkle_proof_ = Merkle.merkle_proof(trans, transaction_hash,[])
            search_items_response.append(Merkle.merkle_proof_root(merkle_proof_,transaction_hash) == chain[block_number].get("merkle_root"))
        return search_items_response
            
 
