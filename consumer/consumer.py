import json
import logging
import os
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
import redis
from redis.exceptions import ConnectionError as RedisConnectionError
from typing import List

from langchain_ollama import ChatOllama
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.globals import set_debug
from pydantic import BaseModel, Field

log_format = '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format)
logger = logging.getLogger(__name__)

class Entities(BaseModel):
    """Model to hold entities extracted from the article."""
    people: List[str] = Field(
        description="All the people's full names mentioned in the article.",
    )
    organizations: List[str] = Field(
        description="All the organizations, companies, or groups mentioned in the article."
    )
    topics: List[str] = Field(
        description="Key topics, technologies, or concepts discussed in the article."
    )

# Setup LLM and Extraction Chain
llm = ChatOllama(model="mistral", temperature=0)
set_debug(True)  # Enable debug mode for LangChain

# Create prompt template
prompt = ChatPromptTemplate.from_messages([
    ("system", "You are an expert at extracting information from text. You must extract the named entities from the following article content. Respond with a valid JSON object."),
    ("human", "{article_content}")
    ])

entity_extraction_chain = prompt | llm.with_structured_output(Entities)

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

def get_neo4j_driver(uri, user, password):
    """Create a Neo4j driver."""
    while True:
        try:
            driver = GraphDatabase.driver(uri, auth=(user, password))
            driver.verify_connectivity()
            logger.info(f"Connected to Neo4j at {uri} as user '{user}'.")
            return driver
        except ServiceUnavailable:
            logger.warning(f"Could not connect to Neo4j at {uri}. Retrying in 3 seconds...")
            time.sleep(3)
        except Exception as e:
            logger.error(f"Error connecting to Neo4j: {e}")
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
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password123")
neo4j_driver = get_neo4j_driver(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = os.getenv("REDIS_PORT", 6379)
redis_client = get_redis_client(REDIS_HOST, REDIS_PORT)

# Kafka Consumer setup
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "articles")
KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "localhost:29092")
consumer = get_kafka_consumer(KAFKA_TOPIC, KAFKA_BROKER_URL)


def add_to_graph(tx, article_id:str, title:str, entities:Entities):
    """Function to add article and its entities to the Neo4j graph."""
    tx.run("MERGE (a:Article {id: $id, title: $title})", id=article_id, title=title)

    for person in entities.people:
        tx.run("""
               MERGE (p:Person {name: $name})
               WITH p
               MATCH (a:Article {id: $id})
               MERGE (a)-[:MENTIONS]->(p)
               """, name=person, id=article_id)

    for org in entities.organizations:
        tx.run("""
               MERGE (o:Organization {name: $name})
               WITH o
               MATCH (a:Article {id: $id})
               MERGE (a)-[:MENTIONS]->(o)
               """, name=org, id=article_id)
    for topic in entities.topics:
        tx.run("""
               MERGE (t:Topic {name: $name})
               WITH t
               MATCH (a:Article {id: $id})
               MERGE (a)-[:IS_ABOUT]->(t)
               """, name=topic, id=article_id)

def main():
    """Main function to consume messages from Kafka and process them."""
    logger.info(f"Consumer is running. Listening to topic '{KAFKA_TOPIC}' on broker '{KAFKA_BROKER_URL}'.")
    
    for message in consumer:
        article = message.value
        article_id = article.get('id')
        article_content = article.get('content')

        if not all([article_id, article_content]):
            logger.warning(f"Received message without required fields: {message.value}")
            continue
        

        logger.info(f"--- Received article: {article['id']} ---")

        cache_key = f"article:{article_id}:entities"
        cached_entities = redis_client.get(cache_key)

        if cached_entities:
            logger.info(f"Found cached entities for article {article_id}.")
            entities = Entities.parse_raw(cached_entities)
        else:
            logger.info(f"No cached entities found for article {article_id}. Extracting entities...")
            
            try:
                entities = entity_extraction_chain.invoke({"article_content": article_content})
                redis_client.set(cache_key, entities.json(), ex=3600)  # Cache for 1 hour
                logger.info(f"Cached LLM results in Redis.")
            except Exception as e:
                logger.error(f"Error extracting entities for article {article_id}: {e}")
                continue
            
        logger.info(f"Extracted entities for article {article_id}: {entities.json()}")

        try:
            with neo4j_driver.session() as session:
                session.execute_write(add_to_graph, article_id, article["title"], entities)
            logger.info(f"Added article {article_id} and its entities to Neo4j graph.")
        except Exception as e:
            logger.error(f"Error adding article {article_id} to Neo4j: {e}")



if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user. Shutting down...")
    finally:
        if 'neo4j_driver' in locals():
            neo4j_driver.close()
            logger.info("Closed Neo4j driver connection.")
