from asyncore import read
import json
import random
import threading
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor

import os
from google.auth import jwt
from google.cloud import pubsub_v1
import math
import json

NODE_ID = 'Node1'

count = 1


def handle_consensus_reception(sender, number):
    global count
    print('Mensajero: {}, Numero: {}'.format(sender, number))


"""  count += 1
  #  if count < size:
  #      if number > current_winner['number']:
  #          current_winner['number'] = number
  #          current_winner['sender'] = sender

    if count == size - 1:
        count = 0
        data = {
            'type': 'result_message',
            'winner_2': current_winner['sender'],
            'number': current_winner['number']
        }
        print('Gano {} con {}'.format(
            current_winner['sender'], current_winner['number']))
        message_to_send = json.dumps(data, ensure_ascii=False).encode('utf8')
        future1 = api_publisher.publish(api_topic_path, message_to_send)
        future1.result()"""


def handle_api_message(message):
    print('-----------------------------------------------------------------------------------------------------------')
    global count
    idtransaction = message['idTransaction']
    transaction = message['transactions']
    print('id: {}'.format(idtransaction))
    print('transaction: {}'.format(transaction))
    print(count)
    if count == 3:
        print("Llegaron 3 transacciones")
        number = generate_random_number()
        print('Genere:{}'.format(number))
        count = 1
        data = {
            'type': 'node_message',
            'sender': NODE_ID,
            'number': number,
        }
        message_to_send = json.dumps(data, ensure_ascii=False).encode('utf8')
        future1 = api_publisher.publish(api_topic_path, message_to_send)
        future1.result()

    else:
        count += 1


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
        json.dump(data, fp,  sort_keys=False, indent=4, separators=(',', ': '))


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

    print(is_nodes_done(nodes))
    # TODO: Death nodes
    if is_nodes_done(nodes):
        print("Recibí todos los mensajes")
        print("Gano: {}".format(get_consensus_winner()))
        reset_consensus_nodes()


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
        for sub in subscriber.list_subscriptions(request={"project": 'projects/' + project_id}):
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


if __name__ == "__main__":
    # node_id = sys.argv[1]
    winner_1 = ''
    count = 1
    size = 0

    # Autenticación
    credentials_path = "service-account-info.json"
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

    # Estas variables se deben mover a variables de entorno
    project_id = 'flash-ward-360216'
    api_topic_subscription_id = 'vocero-sub-' + NODE_ID
    node_topic_id = 'nodes_info'
    api_topic_id = 'vocero'

    # Creando el suscriptor
    subscriber = pubsub_v1.SubscriberClient()
    api_topic_subscription_path = subscriber.subscription_path(
        project_id, api_topic_subscription_id)

    # Se inicializa el publisher
    node_publisher = pubsub_v1.PublisherClient()
    node_topic_path = node_publisher.topic_path(project_id, node_topic_id)
    api_publisher = pubsub_v1.PublisherClient()
    api_topic_path = api_publisher.topic_path(project_id, api_topic_id)

   #future = subscriber.subscribe(api_topic_subscription_path, callback=callback)

   #executor = ThreadPoolExecutor(max_workers=2)

    node_messages_thread = threading.Thread(target=listener_api_messages)
    node_messages_thread.start()

   # executor.submit(listener_api_messages)
    # listener_api_messages()
