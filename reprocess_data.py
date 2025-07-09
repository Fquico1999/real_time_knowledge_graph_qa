import os
import logging
import time
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError


KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "articles")
KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL", "localhost:29092")
MAX_CONNECTION_ATTEMPTS = int(os.environ.get("MAX_CONNECTION_ATTEMPTS"))
RAW_ARTICLE_DIR = os.environ.get("RAW_ARTICLE_DIR")


# Better log formatting for module clarity
log_format = '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format)
logger = logging.getLogger(__name__)

def create_producer():
    """Create a Kafka producer."""
    attempts = 0
    while attempts < MAX_CONNECTION_ATTEMPTS:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER_URL,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info("Kafka producer created successfully.")
            return producer
        except KafkaError as e:
            attempts += 1
            logger.error(f"Failed to create Kafka producer: {e}. Attempt {attempts}/{MAX_CONNECTION_ATTEMPTS}")
            time.sleep(2)
    raise Exception("Could not create Kafka producer after multiple attempts.") 


def main():
    producer = create_producer()
    count = 0
    for filename in os.listdir(RAW_ARTICLE_DIR):
        if filename.endswith(".json"):
            article_id = filename.split(".")[0]
            message = {
                "id": article_id, 
                "storage_path": os.path.join(RAW_ARTICLE_DIR, filename)
            }
            producer.send(KAFKA_TOPIC, message)
            count+=1
            logger.info(f"Sent notification for article: {article_id}")
    producer.flush()
    logger.info(f"Finished. Sent {count} reprocessing notifications to Kafka.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Encountered error reprocessing data: {e}")