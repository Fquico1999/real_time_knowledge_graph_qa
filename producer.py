import logging
import json
import time
import random
from kafka import KafkaProducer
from kafka.errors import KafkaError

from news_data import articles

# Better log formatting for module clarity
log_format = '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format)
logger = logging.getLogger(__name__)


KAFKA_TOPIC = 'articles'
MAX_CONNECTION_ATTEMPTS = 5

def create_producer():
    """Create a Kafka producer."""
    attempts = 0
    while attempts < MAX_CONNECTION_ATTEMPTS:
        try:
            producer = KafkaProducer(
                bootstrap_servers='localhost:29092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info("Kafka producer created successfully.")
            return producer
        except KafkaError as e:
            attempts += 1
            logger.error(f"Failed to create Kafka producer: {e}. Attempt {attempts}/{MAX_CONNECTION_ATTEMPTS}")
            time.sleep(2)
    raise Exception("Could not create Kafka producer after multiple attempts.") 


producer = create_producer()

try:
    for article in articles:
        # Send each article to the Kafka topic
        producer.send(KAFKA_TOPIC, article)
        logger.info(f"Sent article {article['id']} to topic {KAFKA_TOPIC}")
        # Simulate a random delay between messages
        time.sleep(random.uniform(0.5, 3.0))
finally:
    producer.flush()
    producer.close()
    logger.info("All articles sent to Kafka topic '%s'", KAFKA_TOPIC)
