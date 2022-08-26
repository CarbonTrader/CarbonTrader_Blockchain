import json
import random
import socket
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor

from google.auth import jwt
from google.cloud import pubsub_v1


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
    print('Received %s', message)
    message.ack()

    data = json.loads(message.data.decode('utf-8'))
    number_range = data['range']
    limit = data['limit']

    n = random.randint(0, number_range)
    print(n)

    if n > limit:
        message_to_send = str(n).encode("utf-8")
        future1 = node_publisher.publish(node_topic_path, message_to_send)
        future1.result()


if __name__ == "__main__":
    # Autenticación
    service_account_info = json.load(open("service-account-info.json"))
    audience = "https://pubsub.googleapis.com/google.pubsub.v1.Subscriber"

    credentials = jwt.Credentials.from_service_account_info(
        service_account_info, audience=audience
    )

    # Estas variables se deben mover a variables de entorno
    project_id = 'flash-ward-360216'
    hostname = socket.gethostname()
    ip = socket.gethostbyname(hostname)
    api_topic_subscription_id = 'vocero-sub-' + ip
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

    """ node_messages_thread = threading.Thread(target=listener_api_messages)
    node_messages_thread.start() """
    # executor.submit(listener_api_messages)
    listener_api_messages()
