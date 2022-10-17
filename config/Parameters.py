from integrators.DataIntegrator import DataIntegrator


class Parameters:
    @staticmethod
    def get_node_id():
        data = DataIntegrator.read_json("config/config.json")
        return data["NODE_ID"]

    @staticmethod
    def get_max_transactions_per_block():
        data = DataIntegrator.read_json("config/config.json")
        return data["MAX_TRANSACTIONS_PER_BLOCK"]

    @staticmethod
    def get_time_out():
        data = DataIntegrator.read_json("config/config.json")
        return data["TIME_OUT"]

    @staticmethod
    def get_url_backup():
        data = DataIntegrator.read_json("config/config.json")
        return data["URL_BACKUP"]

    @staticmethod
    def get_test_name():
        data = DataIntegrator.read_json("config/config.json")
        return data["TEST_NAME"]

