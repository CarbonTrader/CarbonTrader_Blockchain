import concurrent.futures
import json
import os
import threading
from concurrent import futures

from google.cloud import pubsub_v1
from pydantic import BaseSettings
import logging

from controllers.AuditController import AuditController
from controllers.ConsensusController import ConsensusController
from controllers.MiningController import MiningController
from integrators.DataIntegrator import DataIntegrator
from config.Parameters import Parameters

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


class Settings(BaseSettings):
    CREDENTIALS_PATH: str
    project_id = 'flash-ward-360216'
    api_topic_subscription_id = 'vocero-sub-' + Parameters.get_node_id()
    mining_topic_sub_id = 'mining-sub-' + Parameters.get_node_id()
    node_topic_id = 'nodes_info'
    api_topic_id = 'vocero'
    mining_topic_id = "mining"


settings = Settings()

# GCP Auth
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = settings.CREDENTIALS_PATH

api_subscriber = pubsub_v1.SubscriberClient()
api_topic_subscription_path = api_subscriber.subscription_path(
    settings.project_id, settings.api_topic_subscription_id)
api_publisher = pubsub_v1.PublisherClient()
api_topic_path = api_publisher.topic_path(
    settings.project_id, settings.api_topic_id)

# Mining init
mining_subscriber = pubsub_v1.SubscriberClient()
mining_sub_path = mining_subscriber.subscription_path(
    settings.project_id, settings.mining_topic_sub_id)
mining_publisher = pubsub_v1.PublisherClient()
mining_topic_path = mining_publisher.topic_path(
    settings.project_id, settings.mining_topic_id)


def handle_transaction_message(message):
    transaction = message['transactions']
    logger.info(f'Transaction received {transaction}')
    transactions = DataIntegrator.read_json("db/local_transactions.json")
    transactions.append(transaction)
    DataIntegrator.write_json("db/local_transactions.json", transactions)
    if len(transactions) >= Parameters.get_max_transactions_per_block():
        DataIntegrator.update_transactions_to_mine(
            Parameters.get_max_transactions_per_block())
        start_consensus_process()


def start_consensus_process():
    winner = begin_consensus_thread()
    while not winner:
        winner = begin_consensus_thread()
        logger.warning("There was no agreed winner.")
        logger.warning("Restarting consensus algorithm.")
    begin_mining_thread(winner)


def begin_consensus_thread():
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(
            ConsensusController.consensus_algorithm, api_publisher, api_topic_path)
        winner = future.result()
        return winner


def begin_mining_thread(winner):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(
            MiningController.begin_mining, api_publisher, api_topic_path, winner)
        valid_mining = future.result()
        if valid_mining:
            logger.info("Mining is done.")
        else:
            start_consensus_process()


def handle_message(message):
    message_type = message['type']

    if message_type == 'api_message':
        handle_transaction_message(message)
    elif message_type == 'consensus_message':
        ConsensusController.handle_consensus_message(message)
    elif message_type == 'consensus_winner':
        ConsensusController.handle_consensus_verifier_message(message)
    elif message_type == 'new_block':
        MiningController.handle_new_block_message(message)
    elif message_type == 'validation':
        MiningController.handle_validation_message(message)
    elif message_type == 'recovery':
        MiningController.handle_recovery_message(message)
    elif message_type == "audit":
        AuditController.audit_full_blockchain(message)

def create_subscription(subscriber, topic_sub_path, topic_path):
    subscriber.create_subscription(
        request={"name": topic_sub_path,
                 "topic": topic_path}
    )


def listener_transactions_messages():
    with api_subscriber, mining_subscriber:
        subscriptions = []
        for sub in api_subscriber.list_subscriptions(request={"project": 'projects/' + settings.project_id}):
            subscriptions.append(sub.name)

        if api_topic_subscription_path in subscriptions:
            api_subscriber.delete_subscription(
                request={"subscription": api_topic_subscription_path})

        if mining_sub_path in subscriptions:
            mining_subscriber.delete_subscription(
                request={"subscription": mining_sub_path})

        create_subscription(
            api_subscriber, api_topic_subscription_path, api_topic_path)
        create_subscription(mining_subscriber,
                            mining_sub_path, mining_topic_path)

        logger.info('Waiting for transactions.')
        future = api_subscriber.subscribe(
            api_topic_subscription_path, callback=callback)
        future2 = mining_subscriber.subscribe(
            api_topic_subscription_path, callback=callback)
        try:
            future.result()
            future2.result()
        except futures.TimeoutError:
            future.result()
            future2.result()
            future.cancel()
            future2.cancel()
            api_subscriber.delete_subscription(
                request={"subscription": api_topic_subscription_path})
            mining_subscriber.delete_subscription(
                request={"subscription": mining_sub_path})


def callback(message):
    message.ack()
    data = json.loads(message.data.decode('utf-8'))
    handle_message(data)



def main():
    logger.info("Starting server.")
    listener_transactions_messages()

if __name__ == "__main__":
    logger.info("Resetting mining values.")
    DataIntegrator.reset_all()
    main()