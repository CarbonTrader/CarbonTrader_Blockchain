import json
import logging
import random
import threading
from concurrent import futures

from fastapi import FastAPI
from google.auth import jwt
from google.cloud import pubsub_v1


def node_callback(message):
    print('Received %s', message)
    message.ack()


def listener_node_messages():
    print('Esperando mensajes de Nodos')
    future = subscriber.subscribe(node_topic_subscription_path, callback=node_callback)
    with subscriber:
        try:
            future.result()
        except futures.TimeoutError:
            future.cancel()
            future.result()


app = FastAPI()
service_account_info = json.load(open("service-account-info.json"))
audience = "https://pubsub.googleapis.com/google.pubsub.v1.Subscriber"

credentials = jwt.Credentials.from_service_account_info(
    service_account_info, audience=audience
)

# Estas variables se deben mover a variables de entorno
project_id = 'flash-ward-360216'
api_topic_id = 'vocero'
node_topic_subscription_id = 'nodes_info-sub'

# Se inicializa el publisher
publisher = pubsub_v1.PublisherClient()
api_topic_path = publisher.topic_path(project_id, api_topic_id)

# Creando el suscriptor
subscriber = pubsub_v1.SubscriberClient()
node_topic_subscription_path = subscriber.subscription_path(project_id, node_topic_subscription_id)

node_messages_thread = threading.Thread(target=listener_node_messages)
node_messages_thread.start()
nodes_list = ['node1', 'node2']


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/hello/{range_value}")
async def say_hello(range_value: int):
    publish_futures = []
    n = random.randint(0, range_value)
    position = random.randint(0, len(nodes_list) - 1)
    print(position)

    data = {
        "type": 'api_message',
        "range": range_value,
        "limit": n,
        "node": nodes_list[position]
    }

    message = json.dumps(data, ensure_ascii=False).encode('utf8')

    future1 = publisher.publish(api_topic_path, message)
    future1.add_done_callback(get_callback(future1, message))
    publish_futures.append(future1)

    futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)

    return {"message": f"Hello {range_value}"}


def get_callback(future, message):
    def callback(future):
        try:
            logging.info("Published message %s.", future.result(timeout=1))
        except futures.TimeoutError as exc:
            print("Publishing %s timeout: %r", message, exc)

    return callback
