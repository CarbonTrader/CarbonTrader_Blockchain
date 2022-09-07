import json

class DataIntegrator:
    @staticmethod
    def read_json(filename):
        with open(filename) as json_file:
            data = json.load(json_file)
            return data

    @staticmethod
    def write_json(filename, data):
        with open(filename, 'w') as fp:
            json.dump(data, fp, sort_keys=False, indent=4, separators=(',', ': '))

    @staticmethod
    def reset_consensus_nodes():
        nodes = DataIntegrator.read_json("db/nodes.json")
        for k, _ in nodes.items():
            nodes[k] = -1
        DataIntegrator.write_json("db/nodes.json", nodes)

    @staticmethod
    def reset_all():
        DataIntegrator.reset_consensus_nodes()
        DataIntegrator.write_json("db/local_transactions.json", [])
        DataIntegrator.write_json("db/transactions_to_mine.json", [])

    @staticmethod
    def update_transactions_to_mine():
        transactions = DataIntegrator.read_json("db/local_transactions.json")
        trans_to_mine = transactions[:3]
        DataIntegrator.write_json("db/local_transactions.json", transactions[3:])
        DataIntegrator.write_json("db/transactions_to_mine.json", transactions[:3])