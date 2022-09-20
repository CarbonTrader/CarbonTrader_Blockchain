NODE_ID = 'Node1'
MAX_TRANSACTIONS_PER_BLOCK = 3
TIME_OUT = 0.2

class Parameters:
    @staticmethod
    def get_node_id():
        return NODE_ID

    @staticmethod
    def get_max_transactions_per_block():
        return MAX_TRANSACTIONS_PER_BLOCK

    @staticmethod
    def get_time_out():
        return TIME_OUT

