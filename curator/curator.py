import json
import logging
import os
import time
from typing import List

import redis
from redis.exceptions import ConnectionError as RedisConnectionError
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
from langchain_ollama import ChatOllama
from langchain_core.runnables import ConfigurableField
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.globals import set_debug

log_format = '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format)
logger = logging.getLogger(__name__)

set_debug(True) # For Langchain

BATCH_SIZE = 100
CURATION_INTERVAL_SECONDS = 300 # Run every 5 minutes

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

def get_existing_entities(tx, entity_names: List[str]):
    """Get existing entities from the graph."""
    query = """
    UNWIND $names AS entity_name
    MATCH (e)
    WHERE e.name = entity_name
    RETURN e.name AS name, labels(e)[0] AS type"""
    result = tx.run(query, names=entity_names)
    return [dict(record) for record in result]

def add_refined_to_graph(tx, refined_data: dict):
    """Writes only new, validated entities and relationships to the graph."""

    for entity in refined_data.get("new_entities", []):
        tx.run(f"MERGE (e:{entity['type']} {{name: $name}})", name=entity['name'])

    for rel in refined_data.get("new_relationships", []):
        tx.run(f"""
        MATCH (a {{name: $subj}}), (b {{name: $obj}})
        MERGE (a)-[r:{rel['relationship'].replace(' ','_').upper()}]->(b)
        """, subj=rel['subject'], obj=rel['object'])


REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
redis_client = get_redis_client(REDIS_HOST, REDIS_PORT)
REDIS_QUEUE_KEY = "raw_extractions_queue"

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password123")
neo4j_driver = get_neo4j_driver(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)


OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")
refiner_llm = ChatOllama(
    model="mixtral",
    base_url=OLLAMA_HOST,
    temperature=0
).configurable_fields(
    options = ConfigurableField(
        id="llm_options", 
        name="LLM Options", 
        description="Ollama generation options")
)
refinement_prompt = ChatPromptTemplate.from_template(
    """
You are a master Knowledge Graph builder. Your goal is to convert messy, extracted data into a clean, canonical graph.
You will be given a batch of raw data packets. Each packet contains raw triplets extracted by a weaker AI, along with the original article content that the triplets were extracted from.
You will also be given a list of entities that already exist in the main graph for context.

Your task is to perform high-quality reasoning to build a clean set of new entities and relationships.

**EXISTING GRAPH ENTITIES (for cross-referencing):**
{graph_context}

---

**RAW DATA BATCH (the new information to process):**
{new_data_batch}

---

**YOUR INSTRUCTIONS:**

1.  **Read the Triplets in Context:** For each item in the 'RAW DATA BATCH', use the 'content_summary' to understand the context and meaning of the extracted 'triplets'. The triplets may be noisy or incorrect; the content is the source of truth.

2.  **Classify and Standardize Entities:** For every subject and object in the triplets, determine its correct type (e.g., Person, Organization, Topic, Location, Product). Standardize names where possible (e.g., "Google" and "Google LLC" should be treated as the same "Google" entity).

3.  **Validate and Refine Relationships:** Analyze the relationship type from the triplet. If it's vague or nonsensical given the article content, either discard it or refine it to a more standard type (e.g., change "is with" to "WORKS_FOR" or "PARTNERED_WITH").

4.  **De-duplicate:** Compare your refined entities and relationships against the 'EXISTING GRAPH ENTITIES'. Do not create entities or relationships that already exist. An entity is a duplicate if its name and type match.

5.  **Final Output:** Your response MUST be a single, valid JSON object with two keys: "new_entities" and "new_relationships".
    - 'new_entities': A list of objects, each with 'name' and 'type'.
    - 'new_relationships': A list of objects, each with 'subject', 'relationship', and 'object'.
    If there is nothing new to add, return empty lists.
"""
)
refinement_chain = refinement_prompt | refiner_llm | JsonOutputParser()


def main():
    logger.info("Starting curator service...")
    while True:
        try:
            logger.info("Starting new curation cycle...")

            queue_size = redis_client.llen(REDIS_QUEUE_KEY)
            
            if queue_size == 0:
                logger.info("No raw extractions to process. Sleeping...")
                time.sleep(CURATION_INTERVAL_SECONDS)
                continue

            logger.info(f"{queue_size} items found in queue. Starting batch processing.")

            while redis_client.llen(REDIS_QUEUE_KEY) > 0:
                
                # Fetch batch of raw extractions from Redis
                raw_extractions_json = redis_client.lrange(REDIS_QUEUE_KEY, 0, BATCH_SIZE-1)

                if not raw_extractions_json:
                    break

                batch_extractions = [json.loads(item) for item in raw_extractions_json]

                # Aggregate entities for batch
                all_entity_names = set()
                for extraction in batch_extractions:
                    for triplet in extraction.get("triplets", []):
                        if isinstance(triplet, list) and len(triplet) == 3:
                            all_entity_names.add(triplet[0]) # Subject
                            all_entity_names.add(triplet[2]) # Object
                
                if not all_entity_names:
                    logger.info("Batch contained no entities. Skipping...")
                    continue
                    
                # Fetch context from graph
                with neo4j_driver.session() as session:
                    existing_graph_data = session.execute_read(get_existing_entities, list(all_entity_names))
                
                # Call ollama to refine entire batch
                logger.info("Calling LLM to refine raw extractions...")
                refined_data = refinement_chain.with_config(
                    configurable = {
                        "llm_options": {
                            "num_ctx": 8192, 
                            "num_predict": 4096
                        }
                    }
                ).invoke({
                    "graph_context": json.dumps(existing_graph_data, indent=2),
                    "new_data_batch": json.dumps(batch_extractions, indent=2)
                })

                logger.info("--- DEBUGGING LLM OUTPUT ---")
                logger.info(f"RAW REFINED DATA: {refined_data}")
                logger.info(f"TYPE OF REFINED DATA: {type(refined_data)}")
                logger.info("--- END DEBUGGING ---")

                # Write validated knowledge to Graph
                if refined_data and (refined_data.get("new_entities") or refined_data.get("new_relationships")):
                    with neo4j_driver.session() as session:
                        session.execute_write(add_refined_to_graph, refined_data)
                    logger.info(f"Added {len(refined_data.get('new_entities', []))} new entities and "
                                f"{len(refined_data.get('new_relationships', []))} new relationships to the graph.")
                else:
                    logger.info("Refinement complete. No new entities or relationships to add.")
                
                # Remove items from the queue
                redis_client.ltrim(REDIS_QUEUE_KEY, len(raw_extractions_json), -1) 

                # Brief pause between batches
                time.sleep(1)

            logger.info("Finished processing all batches in the queue.")
        
        except Exception as e:
            logger.error(f"Error during curation cycle: {e}")
        
        logger.info(f"Curation cycle complete. Sleeping for {CURATION_INTERVAL_SECONDS} seconds...")
        time.sleep(CURATION_INTERVAL_SECONDS)
    
if __name__ == "__main__":
    try:
        # Allow other services to spin up. 
        time.sleep(10)
        main()
    except KeyboardInterrupt:
        logger.info("Curation service stopped by user.")
    except Exception as e:
        logger.error(f"Fatal error in curation service: {e}")
        raise