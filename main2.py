from config.Parameters import Parameters
from google.cloud import pubsub_v1
from pydantic import BaseSettings
import json
import os

class Settings(BaseSettings):
    CREDENTIALS_PATH: str
    project_id = 'flash-ward-360216'
    api_topic_subscription_id = 'vocero-sub-' + Parameters.get_node_id() + "S"
    mining_topic_sub_id = 'mining-sub-' + Parameters.get_node_id() +"S"
    node_topic_id = 'nodes_info'
    api_topic_id = 'vocero'
    mining_topic_id = "mining"

settings = Settings()

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = settings.CREDENTIALS_PATH

api_publisher = pubsub_v1.PublisherClient()
api_topic_path = api_publisher.topic_path(
    settings.project_id, settings.api_topic_id)

data = {
        'type': 'audit',
        'parameters': {
            "audit_type" : 1,
            "merkle_search": [[1,"4267438e03fee933fbc15fd74411c00575e11785773862e0751d74cf15efed1f"]]
        }
    }
message_to_send = json.dumps(
    data, ensure_ascii=False).encode('utf8')
future1 = api_publisher.publish(
    api_topic_path, message_to_send)
future1.result()
print("XXX")

