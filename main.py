import concurrent.futures
import json
import os
import threading
from concurrent import futures

from google.cloud import pubsub_v1
from pydantic import BaseSettings

from controllers.ConsensusController import ConsensusController
from controllers.MiningController import MiningController
from integrators.DataIntegraton import DataIntegrator
from integrators.Parameters import Parameters


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

# Creando el suscriptor
subscriber = pubsub_v1.SubscriberClient()
api_topic_subscription_path = subscriber.subscription_path(
    settings.project_id, settings.api_topic_subscription_id)
mining_sub = subscriber.subscription_path(settings.project_id, settings.mining_topic_sub_id)

# Se inicializa el publisher
api_publisher = pubsub_v1.PublisherClient()
api_topic_path = api_publisher.topic_path(settings.project_id, settings.api_topic_id)
mining_pub = api_publisher.topic_path(settings.project_id, settings.mining_topic_id)


# TODO:Think of what happens when there are enogh transactions for 2 blocks
def handle_transaction_message(message):
    idtransaction = message['idTransaction']
    transaction = message['transactions']
    print('id: {}'.format(idtransaction))
    print('transaction: {}'.format(transaction))
    transactions = DataIntegrator.read_json("db/local_transactions.json")
    transactions.append(transaction)
    DataIntegrator.write_json("db/local_transactions.json", transactions)
    if len(transactions) >= Parameters.get_max_transactions_per_block():
        DataIntegrator.update_transactions_to_mine()
        winner = begin_consensus_thread()
        if winner:
            begin_mining_thread(winner)
        # TODO: what happens if there is no winner


def begin_consensus_thread():
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(ConsensusController.consensus_algorithm, api_publisher, api_topic_path)
        winner = future.result()
        return winner


def begin_mining_thread(winner):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(MiningController.begin_mining, winner)
        result = future.result()
        print(result)


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


def listener_api_messages():
    with subscriber:
        subscriptions = []


        for sub in subscriber.list_subscriptions(request={"project": 'projects/' + settings.project_id}):
            subscriptions.append(sub.name)

        if api_topic_subscription_path in subscriptions:
            subscriber.delete_subscription(request={"subscription": api_topic_subscription_path})

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
