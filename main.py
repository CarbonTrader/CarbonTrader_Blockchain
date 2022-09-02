import json
import random
import threading
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor

import os
from google.auth import jwt
from google.cloud import pubsub_v1
import math


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
    global count, size
    idtransaction = message['idTransaction']
    transaction = message['transactions']
    size = message['tam']
    print('id: {}'.format(idtransaction))
    print('transaction: {}'.format(transaction))
    print(count)
    if count == 3:
        print("Llegaron 3 transacciones")
        number = random.uniform(0, 1)
        print('Genere:{}'.format(number))
        count = 1
        data = {
            'type': 'node_message',
            'sender': node_id,
            'number': number,
        }
        message_to_send = json.dumps(data, ensure_ascii=False).encode('utf8')
        future1 = api_publisher.publish(api_topic_path, message_to_send)
        future1.result()

    else:
        count += 1

def handle_node_message(message):
    number = message['number']
    sender = message['sender']
    consensusNumbers = []

    if sender != node_id:
        consensusNumbers.append({sender: number})
        print('Emisor: {}, numero: {}'.format(sender, number))
        print('tam:{}'.format(size))
        print('cant-nodes:{}'.format(len(consensusNumbers)))
    if len(consensusNumbers) == size:
        print("Recibí todos los mensajes")
        max = -math.inf
        winner = ""
        for k,v in consensusNumbers.items():
            if v > max:
                winner = k
                max = v
        print("Gano: {}".format(winner))

def handle_result_message(message):
    # number = message['number']
    winner = message['winner_2']

    if winner == node_id:
        print('Yo soy el winner 2')


def handle_message(message):
    message_type = message['type']

    if message_type == 'api_message':
        handle_api_message(message)
    elif message_type == 'node_message':
        handle_node_message(message)
    elif message_type == 'result_message':
        handle_result_message(message)


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


def f():
    print("hello")


if __name__ == "__main__":
    # node_id = sys.argv[1]
    node_id = 'nodeF'
    winner_1 = ''
    count = 1
    size = 0

    # Autenticación
    credentials_path = "service-account-info.json"
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

    # Estas variables se deben mover a variables de entorno
    project_id = 'flash-ward-360216'
    api_topic_subscription_id = 'vocero-sub-' + node_id
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

    node_me = threading.Thread(target=f)
    node_me.start()
   # executor.submit(listener_api_messages)
    # listener_api_messages()
