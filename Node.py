import os
from google.cloud import pubsub_v1

credentials_path = 'carbontrader.privatekey.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

publisher = pubsub_v1.PublisherClient()
topic_path = 'projects/carbontrader-1111/topics/api_2_blockchain'

data = {
    "leader_node_id" : "1",
    "transactions" : [
        "1t",
        "2t"
    ]
}

future = publisher.publish(topic_path, data)
print(f'publisher message published with id {future.result()}')