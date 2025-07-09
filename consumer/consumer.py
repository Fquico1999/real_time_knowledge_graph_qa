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
from pydantic import BaseModel, Field, ValidationError

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

# prompt = ChatPromptTemplate.from_template(
#     "You are an expert at extracting entities and relationships. "
#     "From the article content below, extract all people, organizations, and key topics. "
#     "Your response MUST be a JSON object containing 'entities' and 'relationships' keys. "
#     "Each item in the 'entities' list must be an object with 'name' and 'type' keys. "
#     "Each item in the 'relationships' list must be an object with 'subject', 'relationship', and 'object' keys. "
#     "Do not add any explanatory text before or after the JSON object.\n\n"
#     "CONTENT: {article_content}"
# )

prompt = ChatPromptTemplate.from_template(
    "You are an expert at extracting knowledge triplets from text with the goal of building an informative and useful world knoweldge graph."
    "Your goal is to identify entities and their relationships. "
    "From the article content below, extract as many knowledge triplets as you can. "
    "Format EACH triplet as a JSON array (a list in Python) with exactly three string elements: [\"SUBJECT\", \"RELATIONSHIP\", \"OBJECT\"].\n"
    "Respond ONLY with a single JSON object containing one key, \"triplets\", which is a list of these JSON arrays.\n\n"
    "EXAMPLE:\n"
    "CONTENT: After leaving Google, Geoffrey Hinton now works for a lab at the University of Toronto.\n"
    "RESPONSE: {\"triplets\": [[\"Geoffrey Hinton\", \"LEFT_COMPANY\", \"Google\"], [\"Geoffrey Hinton\", \"WORKS_FOR\", \"University of Toronto\"]]}\n\n"
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

        kafka_data = message.value
        article_id = kafka_data.get("id")
        storage_path = kafka_data.get("storage_path")

        if not all([article_id, storage_path]):
            logger.warning(f"Skipping malformed message: {kafka_data}")
            continue

        logger.info(f"--- Processing notification for article: {article_id} ---")
        
        try:
            # Read article content from mounted file
            with open(storage_path, "r", encoding="utf-8") as f:
                article_data = json.load(f)
        
            article_content = article_data.get('content_summary', '')
            if not article_content:
                logger.warning(f"Article file {storage_path} is missing content. Skipping.")
                continue

            # Call the fast LLM for initial extraction
            llm_response = extraction_chain.invoke({"article_content": article_content})
            
            # Extract the list of triplets
            triplets = llm_response.get("triplets",[])

            if not triplets:
                logger.info("Could not extract any triplets")
                continue

            # Create simple object to push to Redis
            data_to_push = {
                "article_data": article_data,
                "triplets": triplets
            }

            # Push to Redis Queue
            redis_client.rpush(REDIS_QUEUE_KEY, json.dumps(data_to_push))
            logger.info(f"Pushed {len(triplets)} raw triplets for article {article_id} to Redis queue.")

        except json.JSONDecodeError as e:
            logger.error(f"JSON DECODE FAILED for article {article_id}. LLM likely produced non-JSON text. Output: '{llm_output_str}'. Error: {e}")
        except ValidationError as e:
            logger.error(f"PYDANTIC VALIDATION FAILED for article {article_id}. LLM output did not match schema. Output: '{llm_output_str}'. Error: {e}")
        except FileNotFoundError:
            logger.error(f"Could not find article file at path: {storage_path}. Skipping.")
        except Exception as e:
            logger.error(f"Failed to process article {article_id}: {e}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user. Shutting down...")

