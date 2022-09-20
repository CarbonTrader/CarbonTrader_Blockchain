import json

from integrators.Parameters import Parameters


class DataIntegrator:
    @staticmethod
    def read_json(filename):
        try:
            with open(filename) as json_file:
                data = json.load(json_file)
                return data
        except:
            print("There was a problem fetching the json file")
            return None
    @staticmethod
    def write_json(filename, data):
        try:
            with open(filename, 'w') as fp:
                json.dump(data, fp, sort_keys=False, indent=4, separators=(',', ': '))
        except:
            print("There was a problem writing the json file")
    @staticmethod
    def reset_consensus_nodes():
        nodes = DataIntegrator.read_json("db/nodes.json")
        for k, _ in nodes.items():
            nodes[k] = -1
        DataIntegrator.write_json("db/nodes.json", nodes)
    @staticmethod
    def reset_consensus_winners():
        nodes = DataIntegrator.read_json("db/winner.json")
        for k, _ in nodes.items():
            nodes[k] = ""
        DataIntegrator.write_json("db/winner.json", nodes)

    @staticmethod
    def reset_validation():
        nodes = DataIntegrator.read_json("db/validation.json")
        for k, _ in nodes.items():
            nodes[k] = False
        DataIntegrator.write_json("db/validation.json", nodes)

    @staticmethod
    def reset_all():
        DataIntegrator.reset_consensus_winners()
        DataIntegrator.reset_consensus_nodes()
        DataIntegrator.write_json("db/local_transactions.json", [])
        DataIntegrator.write_json("db/transactions_to_mine.json", [])
        DataIntegrator.write_json("db/new_block.json", {})
        DataIntegrator.reset_validation()

    @staticmethod
    def update_transactions_to_mine():
        transactions = DataIntegrator.read_json("db/local_transactions.json")
        trans_to_mine = transactions[:3]
        DataIntegrator.write_json("db/local_transactions.json", transactions[3:])
        DataIntegrator.write_json("db/transactions_to_mine.json", transactions[:3])
    @staticmethod
    def persist_validation(validation, node_id):
        nodes = DataIntegrator.read_json("db/validation.json")
        nodes[node_id] = validation
        DataIntegrator.write_json("db/validation.json",nodes)
    @staticmethod
    def fetch_transactions_to_mine():
        return DataIntegrator.read_json('db/transactions_to_mine.json')