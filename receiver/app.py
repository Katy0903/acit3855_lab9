import connexion
from connexion import NoContent
import json
import os
from datetime import datetime
import httpx
import yaml
import logging
import logging.config
import uuid
from pykafka import KafkaClient
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import time


# STORAGE_SERVICE_URL = "http://localhost:8090"

with open('./config/app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open("./config/log_conf.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger('basicLogger')


kafka_config = app_config['events']
kafka_host = kafka_config['hostname']
kafka_port = kafka_config['port']
kafka_topic = kafka_config['topic']


# KAFKA_CLIENT = KafkaClient(hosts=f'{kafka_host}:{kafka_port}')
# KAFKA_TOPIC = KAFKA_CLIENT.topics[str.encode(kafka_topic)]
# KAFKA_PRODUCER = KAFKA_TOPIC.get_sync_producer()


# def send_to_kafka(event_type, data):
#     try:

#         # client = KafkaClient(hosts=f'{kafka_host}:{kafka_port}')
#         # topic = client.topics[str.encode(kafka_topic)]
#         # producer = topic.get_sync_producer()
        
#         logger.info(f"Received event {event_type} with a trace id of {data.get('trace_id')}")
#         data["trace_id"] = str(uuid.uuid4())

#         msg = {
#             "type": event_type,
#             "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
#             "payload": data
#         }

        
#         msg_str = json.dumps(msg)
#         KAFKA_PRODUCER.produce(msg_str.encode('utf-8'))  

#         logger.info(f"Message for event {event_type} has been sent to Kafka.")
    
#     except Exception as e:
#         logger.error(f"Error sending to Kafka: {e}")
#         return None
    



# Retry logic on Kafka client connection
@retry(stop=stop_after_attempt(5), wait=wait_fixed(2), retry=retry_if_exception_type(KafkaException))
def get_kafka_producer():
    client = KafkaClient(hosts=f'{kafka_host}:{kafka_port}')
    topic = client.topics[str.encode(kafka_topic)]
    producer = topic.get_sync_producer()
    return producer

# Kafka producer instance
KAFKA_PRODUCER = get_kafka_producer()

def send_to_kafka(event_type, data):
    try:
        logger.info(f"Received event {event_type} with a trace id of {data.get('trace_id')}")
        data["trace_id"] = str(uuid.uuid4())

        msg = {
            "type": event_type,
            "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "payload": data
        }

        msg_str = json.dumps(msg)
        KAFKA_PRODUCER.produce(msg_str.encode('utf-8'))  

        logger.info(f"Message for event {event_type} has been sent to Kafka.")
    
    except Exception as e:
        logger.error(f"Error sending to Kafka: {e}")
        return None
    

def post_clientcase(body):

    send_to_kafka("clientcase", body)

    return NoContent, 201  


def post_survey(body):
   
    send_to_kafka("survey", body)

    return NoContent, 201  


app = connexion.FlaskApp(__name__, specification_dir=".")
app.add_api("openapi.yml", base_path="/receiver", strict_validation=True, validate_responses=True)



if __name__ == "__main__":
    app.run(port=8080, host="0.0.0.0")


