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
import time
import random
from pykafka import KafkaClient
from pykafka.exceptions import KafkaException



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


KAFKA_CLIENT = KafkaClient(hosts=f'{kafka_host}:{kafka_port}')
KAFKA_TOPIC = KAFKA_CLIENT.topics[str.encode(kafka_topic)]
KAFKA_PRODUCER = KAFKA_TOPIC.get_sync_producer()



class KafkaWrapper:
    def __init__(self, hostname, topic):
        self.hostname = hostname
        self.topic = topic
        self.client = None
        self.producer = None
        self.connect()

    def connect(self):
        """Infinite loop: will keep trying to reconnect to Kafka"""
        while True:
            logger.debug("Trying to connect to Kafka...")
            if self.make_client() and self.make_producer():
                break
            # Sleep for a random amount of time (0.5 to 1.5s)
            time.sleep(random.randint(500, 1500) / 1000)

    def make_client(self):
        """Runs once, makes a client and sets it on the instance."""
        if self.client is not None:
            return True
        try:
            self.client = KafkaClient(hosts=f'{self.hostname}:{self.port}')
            logger.info("Kafka client created!")
            return True
        except KafkaException as e:
            msg = f"Kafka error when making client: {e}"
            logger.warning(msg)
            self.client = None
            self.producer = None
            return False

    def make_producer(self):
        """Runs once, makes a producer and sets it on the instance."""
        if self.producer is not None:
            return True
        if self.client is None:
            return False
        try:
            topic = self.client.topics[str.encode(self.topic)]
            self.producer = topic.get_sync_producer()
            return True
        except KafkaException as e:
            msg = f"Error when making producer: {e}"
            logger.warning(msg)
            self.client = None
            self.producer = None
            return False

    def send_message(self, message):
        """Sends a message to Kafka"""
        if self.producer is None:
            self.connect()
        try:
            self.producer.produce(message.encode('utf-8'))
            logger.info("Message has been sent to Kafka.")
        except KafkaException as e:
            msg = f"Kafka issue when sending message: {e}"
            logger.warning(msg)
            self.client = None
            self.producer = None
            self.connect()


# Initialize KafkaWrapper globally
kafka_wrapper = KafkaWrapper(kafka_host, kafka_topic)


def send_to_kafka(event_type, data):
    try:

        # client = KafkaClient(hosts=f'{kafka_host}:{kafka_port}')
        # topic = client.topics[str.encode(kafka_topic)]
        # producer = topic.get_sync_producer()
        
        logger.info(f"Received event {event_type} with a trace id of {data.get('trace_id')}")
        data["trace_id"] = str(uuid.uuid4())

        msg = {
            "type": event_type,
            "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "payload": data
        }

        
        msg_str = json.dumps(msg)
        # KAFKA_PRODUCER.produce(msg_str.encode('utf-8'))  
        kafka_wrapper.send_message(msg_str)  # Use the KafkaWrapper to send the message

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


