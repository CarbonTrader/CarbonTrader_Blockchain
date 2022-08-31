import json
import random
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor

from google.auth import jwt
from google.cloud import pubsub_v1


def handle_consensus_reception(sender, number):
    global count
    print('Mensajero: {}, Numero: {}'.format(sender, number))
    count += 1
    if count < size:
        if number > current_winner['number']:
            current_winner['number'] = number
            current_winner['sender'] = sender

    if count == size - 1:
        count = 0
        data = {
            'type': 'result_message',
            'winner_2': current_winner['sender'],
            'number': current_winner['number']
        }
        print('Gano {} con {}'.format(current_winner['sender'], current_winner['number']))
        message_to_send = json.dumps(data, ensure_ascii=False).encode('utf8')
        future1 = api_publisher.publish(api_topic_path, message_to_send)
        future1.result()


def handle_api_message(message):
    print('-----------------------------------------------------------------------------------------------------------')
    idtransaction = message['idTransaction']
    transaction = message['transactions']
    global size
    size_result = message['size']

    print('id: {}'.format(idtransaction))
    print('transaction: {}'.format(transaction))
    print(size)
    if size == 3:
        print("Llegaron 3 transacciones")
        number = random.uniform(0, 1)
        print('Genere:{}'.format(number))
        size = 1
        data = {
            'type': 'node_message',
            'sender': node_id,
            'number': number
        }
        message_to_send = json.dumps(data, ensure_ascii=False).encode('utf8')
        future1 = api_publisher.publish(api_topic_path, message_to_send)
        future1.result()

    else:
        size+=1
    """ global winner_1
    winner_1 = winner_id

    if winner_id != node_id:
        n = random.randint(0, number_range)
        print('Genere {}'.format(n))

        data = {
            'type': 'node_message',
            'sender': node_id,
            'number': n
        }
        print('Enviando mensaje a {}'.format(winner_id))
        message_to_send = json.dumps(data, ensure_ascii=False).encode('utf8')
        future1 = api_publisher.publish(api_topic_path, message_to_send)
        future1.result()

    else:
        global size
        size = size_result
        print('Soy winner 1')
        print('Esperando numeros de los otros nodos') """       


def handle_node_message(message):
    number = message['number']
    sender = message['sender']
    consensusNumbers = []

    if sender!= node_id:
        node = {
            'id': sender,
            'number':number
        }
        consensusNumbers.append(node)
        print('Emisor: {}, numero: {}'.format(sender,number))

    #if winner_1 == node_id:
    #   handle_consensus_reception(sender, number)


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
    print('Esperando mensajes de API')
    with subscriber:
        subscriptions = []
        for sub in subscriber.list_subscriptions(request={"project": 'projects/' + project_id}):
            subscriptions.append(sub.name)

        if api_topic_subscription_path not in subscriptions:
            subscriber.create_subscription(
                request={"name": api_topic_subscription_path, "topic": api_topic_path}
            )

        future = subscriber.subscribe(api_topic_subscription_path, callback=callback)
        try:
            future.result()
        except futures.TimeoutError:
            future.result()
            future.cancel()
            subscriber.delete_subscription(request={"subscription": api_topic_subscription_path})


def callback(message):
    message.ack()

    data = json.loads(message.data.decode('utf-8'))
    handle_message(data)


if __name__ == "__main__":
    # node_id = sys.argv[1]
    node_id = 'node1'
    current_winner = {
        'sender': '',
        'number': 0
    }
    winner_1 = ''
    size = 1

    # Autenticación
    service_account_info = json.load(open("service-account-info.json"))
    audience = "https://pubsub.googleapis.com/google.pubsub.v1.Subscriber"

    credentials = jwt.Credentials.from_service_account_info(
        service_account_info, audience=audience
    )

    # Estas variables se deben mover a variables de entorno
    project_id = 'flash-ward-360216'
    api_topic_subscription_id = 'vocero-sub-' + node_id
    node_topic_id = 'nodes_info'
    api_topic_id = 'vocero'

    # Creando el suscriptor
    subscriber = pubsub_v1.SubscriberClient()
    api_topic_subscription_path = subscriber.subscription_path(project_id, api_topic_subscription_id)

    # Se inicializa el publisher
    node_publisher = pubsub_v1.PublisherClient()
    node_topic_path = node_publisher.topic_path(project_id, node_topic_id)
    api_publisher = pubsub_v1.PublisherClient()
    api_topic_path = api_publisher.topic_path(project_id, api_topic_id)

    # future = subscriber.subscribe(api_topic_subscription_path, callback=callback)

    executor = ThreadPoolExecutor(max_workers=2)
    count = 0

    """ node_messages_thread = threading.Thread(target=listener_api_messages)
    node_messages_thread.start() """
    # executor.submit(listener_api_messages)
    listener_api_messages()
