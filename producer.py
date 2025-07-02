import logging
import os
import json
import time
import hashlib
import feedparser

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError

load_dotenv(verbose=True)

KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "articles")
KAFKA_BROKER_URL = "localhost:29092"
MAX_CONNECTION_ATTEMPTS = int(os.environ.get("MAX_CONNECTION_ATTEMPTS"))
PROCESSED_ARTICLES_LOG = 'processed_articles.log'

# Better log formatting for module clarity
log_format = '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format)
logger = logging.getLogger(__name__)


def load_processed_articles():
    """Load already processed articles from a log file."""
    if not os.path.exists(PROCESSED_ARTICLES_LOG):
        return set()
    
    with open(PROCESSED_ARTICLES_LOG, 'r') as f:
        return {line.strip() for line in f.readlines()}

def save_processed_article(article_id):
    """Save an article ID to the processed articles log."""
    with open(PROCESSED_ARTICLES_LOG, 'a') as f:
        f.write(f"{article_id}\n")

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
    processed_ids = load_processed_articles()

    rss_url = 'http://feeds.wired.com/wired/index'
    logger.info(f"Fetching articles from RSS feed: {rss_url}")
    
    feed = feedparser.parse(rss_url)

    new_articles_found = 0

    try:
        for entry in feed.entries:
            article_id = hashlib.sha1(entry.id.encode()).hexdigest()
            if article_id in processed_ids:
                continue
            
            new_articles_found += 1

            article = {
                "id": article_id,
                "title": entry.title,
                "content": entry.summary,
                "source_url": entry.link,
            }

            producer.send(KAFKA_TOPIC, article)
            logger.info(f"Sent new article {article_id} ('{entry.title[:50]}...') to topic {KAFKA_TOPIC}")

            save_processed_article(article_id)
            time.sleep(0.5)
    except Exception as e:
        logger.error(f"Error processing articles: {e}")
    finally:
        producer.flush()
        producer.close()
        logger.info(f"Finished processing articles. Total new articles found: {new_articles_found}")

if __name__ == "__main__":
    main()