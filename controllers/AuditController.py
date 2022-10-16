from distutils import log
from fcntl import F_SEAL_SEAL
from blockchain.Block import Block
from blockchain.Blockchain import Blockchain
import time
import json
from blockchain.Merkle import Merkle
from config.Parameters import Parameters
import random
import logging

project_id = 'flash-ward-360216'


# Initialize logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s, %(name)s %(levelname)s : %(message)s')
file_handler = logging.FileHandler('db/audit.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)


class AuditController:
    @staticmethod
    def audit_full_blockchain(api_publisher, api_topic_path, api_subscriber, message):
        response = {
            "type": "audit_response",
            "sender_id": Parameters.get_node_id(),
            "test_results": {
                "test_audit": None,
                "test_signatures": None,
                "test_transactions_in_block": None
            }
        }
        parameters = message["parameters"]
        blockchain = Blockchain()
        blockchain.upload_blockchain()

        if parameters.get("audit_type") == 2:
            response["test_results"]["test_audit"] = AuditController.deep_block_hash_test(
                blockchain.chain)
        elif parameters.get("audit_type") == 1:
            response["test_results"]["test_audit"] = AuditController.light_block_hash_test(
                blockchain.chain)

        if parameters.get("merkle_search"):
            response["test_results"]["test_transactions_in_block"] = AuditController.validate_transaction_in_block(
                blockchain.chain, parameters.get("merkle_search"))
        AuditController.return_response(
            api_publisher, api_topic_path, response)
        logger.info("Audit results." + str(response))

    @staticmethod
    def deep_block_hash_test(chain) -> bool:
        for i in range(len(chain)-1, 0, -1):
            if not Block.is_valid_audit_block(chain[i], chain[i-1]):
                return False
        return True

    @staticmethod
    def light_block_hash_test(chain) -> bool:
        len_half_chain = len(chain) // 2
        random_blocks = set()
        while len(random_blocks) < len_half_chain:
            random_blocks.add(random.randint(1, len(chain)-1))

        for chain_index in random_blocks:
            if not Block.is_valid_audit_block(chain[chain_index], chain[chain_index-1]):
                return False

        return True

    @staticmethod
    def validate_transaction_in_block(chain, search_items) -> list:
        search_items_response = []
        for item in search_items:
            block_number = item[0]
            transaction_hash = item[1]
            trans = chain[block_number].get("transactions_hashes")
            merkle_proof_ = Merkle.merkle_proof(trans, transaction_hash, [])
            search_items_response.append(Merkle.merkle_proof_root(
                merkle_proof_, transaction_hash) == chain[block_number].get("merkle_root"))
        return search_items_response

    @staticmethod
    def return_response(api_publisher, api_topic_path, response):
        message_to_send = json.dumps(
            response, ensure_ascii=False).encode('utf8')
        future1 = api_publisher.publish(
            api_topic_path, message_to_send)
        future1.result()
