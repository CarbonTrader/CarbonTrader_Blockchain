import json
import logging
import requests
import datetime

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


class DataIntegrator:
    @staticmethod
    def read_json(filename):
        try:
            with open(filename) as json_file:
                data = json.load(json_file)
                return data
        except:
            logger.error(
                f"There was a problem fetching {filename} the json file")
            return None

    @staticmethod
    def write_json(filename, data):
        try:
            with open(filename, 'w') as fp:
                json.dump(data, fp, sort_keys=False,
                          indent=4, separators=(',', ': '))
        except:
            logger.error(
                f"There was a problem writing the {filename} json file")

    @staticmethod
    def reset_consensus_nodes():
        nodes = DataIntegrator.read_json("config/blockchain_nodes.json")
        for k, _ in nodes.items():
            nodes[k] = -1
        return nodes

    @staticmethod
    def reset_consensus_winners():
        nodes = DataIntegrator.read_json("config/blockchain_nodes.json")
        for k, _ in nodes.items():
            nodes[k] = ""
        return nodes

    @staticmethod
    def reset_validation():
        nodes = DataIntegrator.read_json("config/blockchain_nodes.json")
        for k, _ in nodes.items():
            nodes[k] = ""
        return nodes

    @staticmethod
    def update_blockchain(chain):
        DataIntegrator.write_json("db/blockchain.json", chain)

    @staticmethod
    def reset_all():
        DataIntegrator.write_json("db/local_transactions.json", [])
        DataIntegrator.write_json("db/transactions_to_mine.json", [])
        DataIntegrator.write_json("db/new_block.json", {})
        DataIntegrator.fetch_blockchain_recovery()

    @staticmethod
    def reset_mining():
        DataIntegrator.write_json("db/new_block.json", {})
        DataIntegrator.write_json("db/transactions_to_mine.json", [])

    @staticmethod
    def update_transactions_to_mine(number_transactions: int):
        transactions = DataIntegrator.read_json("db/local_transactions.json")
        DataIntegrator.write_json(
            "db/local_transactions.json", transactions[number_transactions:])
        DataIntegrator.write_json(
            "db/transactions_to_mine.json", transactions[:number_transactions])

    @staticmethod
    def fetch_transactions_to_mine():
        return DataIntegrator.read_json('db/transactions_to_mine.json')

    @staticmethod
    def fetch_blockchain_recovery():
        logger.info("Fetching copy of blockchain.")
        data = DataIntegrator.read_json("config/config.json")
        blockchain = requests.get(data["URL_BACKUP"])
        DataIntegrator.update_blockchain(blockchain.json())

    @staticmethod
    def save_time(test_name, time, death_nodes):
        info = DataIntegrator.read_json("db/load_test.json")
        starter_info = {
            "test_name": test_name,
            "time": time,
            "death_nodes": death_nodes
        }
        info.append(starter_info)
        DataIntegrator.write_json("db/load_test.json", info)
