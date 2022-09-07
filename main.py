import json
import os
import threading
from concurrent import futures
import time
from google.cloud import pubsub_v1
from pydantic import BaseSettings

from ConsensusController import ConsensusController
from DataIntegraton import DataIntegrator

NODE_ID = 'Node1'
MAX_TRANSACTIONS_PER_BLOCK = 3
class Settings(BaseSettings):
    CREDENTIALS_PATH: str
    project_id = 'flash-ward-360216'
    api_topic_subscription_id = 'vocero-sub-' + NODE_ID
    node_topic_id = 'nodes_info'
    api_topic_id = 'vocero'

settings = Settings()

# GCP Auth
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = settings.CREDENTIALS_PATH

# Creando el suscriptor
subscriber = pubsub_v1.SubscriberClient()
api_topic_subscription_path = subscriber.subscription_path(
    settings.project_id, settings.api_topic_subscription_id)

# Se inicializa el publisher
api_publisher = pubsub_v1.PublisherClient()
api_topic_path = api_publisher.topic_path(settings.project_id, settings.api_topic_id)


#TODO:Think of what happens when there are enogh transactions for 2 blocks
def handle_transaction_message(message):
    idtransaction = message['idTransaction']
    transaction = message['transactions']
    print('id: {}'.format(idtransaction))
    print('transaction: {}'.format(transaction))
    transactions = DataIntegrator.read_json("local_transactions.json")
    transactions.append(transaction)
    DataIntegrator.write_json("local_transactions.json",transactions)
    if len(transactions) >= MAX_TRANSACTIONS_PER_BLOCK:
        DataIntegrator.update_transactions_to_mine()
        consensus_thread = threading.Thread(target=ConsensusController.consensus_algorithm(api_publisher,api_topic_path))
        consensus_thread.start()

def handle_message(message):
    message_type = message['type']

    if message_type == 'api_message':
        handle_transaction_message(message)
    elif message_type == 'consensus_message':
        ConsensusController.handle_consensus_message(message)


def listener_api_messages():
    with subscriber:
        subscriptions = []
        for sub in subscriber.list_subscriptions(request={"project": 'projects/' + settings.project_id}):
            subscriptions.append(sub.name)

        if api_topic_subscription_path not in subscriptions:
            subscriber.create_subscription(
                request={"name": api_topic_subscription_path,
                         "topic": api_topic_path}
            )
        print('Esperando mensajes de API')
        future = subscriber.subscribe(
            api_topic_subscription_path, callback=callback)
        try:
            future.result()
        except futures.TimeoutError:
            future.result()
            future.cancel()
            subscriber.delete_subscription(
                request={"subscription": api_topic_subscription_path})


def callback(message):
    message.ack()
    data = json.loads(message.data.decode('utf-8'))
    handle_message(data)


def main():
    node_messages_thread = threading.Thread(target=listener_api_messages)
    node_messages_thread.start()

if __name__ == "__main__":
    DataIntegrator.reset_all()
    main()