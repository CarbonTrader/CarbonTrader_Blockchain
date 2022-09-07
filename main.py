import json
import os
import random
import threading
from concurrent import futures
import time
from google.cloud import pubsub_v1
from pydantic import BaseSettings

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


def handle_consensus_reception(sender, number):
    print('Mensajero: {}, Numero: {}'.format(sender, number))


def handle_api_message(message):
    idtransaction = message['idTransaction']
    transaction = message['transactions']
    print('id: {}'.format(idtransaction))
    print('transaction: {}'.format(transaction))
    data = read_json("local_transactions.json")
    data.append(transaction)
    write_json("local_transactions.json",data)
    validate_number_transactions(data)



def validate_number_transactions(data):
    if len(data) >= MAX_TRANSACTIONS_PER_BLOCK:
        consensus_thread = threading.Thread(target=consensus_algorithm())
        consensus_thread.start()

def consensus_algorithm():
    number = generate_random_number()
    print('Genere:{}'.format(number))
    data = {
        'type': 'node_message',
        'sender': NODE_ID,
        'number': number,
    }
    message_to_send = json.dumps(data, ensure_ascii=False).encode('utf8')
    future1 = api_publisher.publish(api_topic_path, message_to_send)
    future1.result()
    # TODO: Death nodes
    nodes = read_json("nodes.json")
    while not is_nodes_done(nodes):
        print("waiting")
        time.sleep(5)
    print("Recibí todos los mensajes")
    print("Gano: {}".format(get_consensus_winner()))
    reset_consensus_nodes()
    write_json("local_transactions.json",[])

def generate_random_number():
    number = random.uniform(0, 1)
    nodes = read_json("nodes.json")
    nodes[NODE_ID] = number
    write_json("nodes.json", nodes)
    return number


def read_json(filename):
    with open(filename) as json_file:
        data = json.load(json_file)
        return data


def write_json(filename, data):
    with open(filename, 'w') as fp:
        json.dump(data, fp, sort_keys=False, indent=4, separators=(',', ': '))


def get_consensus_winner():
    nodes = read_json("nodes.json")
    max = -2
    winner = "winner"
    for k, v in nodes.items():
        if v > max:
            max = v
            winner = k
    return winner


def is_nodes_done(data):
    for k, v in data.items():
        if v == -1:
            return False
    return True


def handle_node_message(message):
    number = message['number']
    sender = message['sender']
    nodes = read_json("nodes.json")

    if sender != NODE_ID:
        nodes[sender] = number
        print('Emisor: {}, numero: {}'.format(sender, number))
        print('cant-nodes:{}'.format(len(nodes)))
        write_json("nodes.json", nodes)


def reset_consensus_nodes():
    nodes = read_json("nodes.json")
    for k, _ in nodes.items():
        nodes[k] = -1
    write_json("nodes.json", nodes)


def handle_message(message):
    message_type = message['type']

    if message_type == 'api_message':
        handle_api_message(message)
    elif message_type == 'node_message':
        handle_node_message(message)


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
    reset_consensus_nodes()
    write_json("local_transactions.json",[])
    main()