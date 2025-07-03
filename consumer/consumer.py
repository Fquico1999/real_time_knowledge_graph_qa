import json
import logging
import os
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import redis
from redis.exceptions import ConnectionError as RedisConnectionError
from typing import List

from langchain_ollama import ChatOllama
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.globals import set_debug
from pydantic import BaseModel, Field

log_format = '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format)
logger = logging.getLogger(__name__)

class ExtractedEntity(BaseModel):
    """Model to represent a single extracted entity."""
    name: str = Field(description="The full name of the entity.")
    type: str = Field(description="The type of the entity (e.g., Person, Organization, Topic).")

class ExtractedRelationship(BaseModel):
    """Model to represent a relationship between entities."""
    subject: str = Field(description="The name of the subject entity.")
    relationship: str = Field(description="The type of relationship (e.g., MENTIONS, IS_ABOUT).")
    object: str = Field(description="The name of the object entity.")

class InitialExtraction(BaseModel):
    """Raw, unverified data extracted from an article."""
    entities: List[ExtractedEntity] = Field(
        description="List of entities extracted from the article.",
    )
    relationships: List[ExtractedRelationship] = Field(
        description="List of relationships between entities extracted from the article."
    )

OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")

# Setup LLM and Extraction Chain
extractor_llm = ChatOllama(model="mistral", base_url=OLLAMA_HOST, temperature=0)
set_debug(True)  # Enable debug mode for LangChain

prompt = ChatPromptTemplate.from_template(
    "You are an expert at extracting entities and relationships. "
    "From the article content below, extract all people, organizations, and key topics. "
    "Also, extract any clear relationships between them in the format [subject, relationship, object]. "
    "Respond with a JSON object containing 'entities' and 'relationships'.\n\n"
    "CONTENT: {article_content}"
)
extraction_chain = prompt | extractor_llm | JsonOutputParser()

def get_kafka_consumer(topic, servers):
    """Create a Kafka consumer for the specified topic."""
    while True:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=servers,
                auto_offset_reset='earliest',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            logger.info(f"Kafka consumer created for topic '{topic}' on servers '{servers}'.")
            return consumer
        except NoBrokersAvailable:
            logger.warning(f"Could not connect to Kafka brokers. Retrying in 3 seconds...")
            time.sleep(3)
        except Exception as e:
            logger.error(f"Error creating Kafka consumer: {e}")
            raise

def get_redis_client(host, port):
    """Create a Redis client."""
    while True:
        try:
            client = redis.Redis(host=host, port=port, db=0, decode_responses=True)
            client.ping()  # Test connection
            logger.info(f"Connected to Redis at {host}:{port}.")
            return client
        except RedisConnectionError:
            logger.warning(f"Could not connect to Redis at {host}:{port}. Retrying in 3 seconds...")
            time.sleep(3)
        except Exception as e:
            logger.error(f"Error connecting to Redis: {e}")
            raise

# Database connections
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = os.getenv("REDIS_PORT", 6379)
redis_client = get_redis_client(REDIS_HOST, REDIS_PORT)
REDIS_QUEUE_KEY = "raw_extractions_queue"

# Kafka Consumer setup
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "articles")
KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "localhost:29092")
consumer = get_kafka_consumer(KAFKA_TOPIC, KAFKA_BROKER_URL)


def main():
    logger.info("Fast consumer service is running... waiting for articles.")
    for message in consumer:
        article = message.value
        article_id = article.get('id')
        logger.info(f"--- Processing article: {article_id} ---")
        
        try:
            # Call the fast LLM for initial extraction
            raw_extraction = extraction_chain.invoke({"article_content": article['content']})
            
            # Add article ID to the extraction data for context
            raw_extraction['article_id'] = article_id
            
            # Push the raw JSON data to the Redis queue for the curator
            redis_client.rpush(REDIS_QUEUE_KEY, json.dumps(raw_extraction))
            logger.info(f"Pushed raw extraction for article {article_id} to Redis queue.")

        except Exception as e:
            logger.error(f"Failed to process article {article_id}: {e}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user. Shutting down...")

