import asyncio
import logging
import os
import json
import time
import hashlib

import aiohttp
import feedparser
from kafka import KafkaProducer
from kafka.errors import KafkaError

KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "articles")
KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL", "localhost:29092")
MAX_CONNECTION_ATTEMPTS = int(os.environ.get("MAX_CONNECTION_ATTEMPTS"))
FEEDS_CONFIG_PATH = 'feeds.json'
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


async def poll_feed(feed_config, producer):
    """Poll a single RSS feed and send new articles to Kafka."""
    rss_url = feed_config['url']
    interval = feed_config.get('interval_seconds', 600)
    feed_name = rss_url.split('//')[1].split('/')[0]  # Extract feed name from URL
    logger.info(f"[{feed_name}] Starting polling task. Interval: {interval}s")

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                logger.info(f"[{feed_name}] Fetching feed...")

                async with session.get(rss_url) as response:
                    if response.status != 200:
                        logger.error(f"[{feed_name}] Failed to fetch feed: {response.status}")
                        await asyncio.sleep(interval)
                        continue
                    
                    content = await response.text()

                feed = feedparser.parse(content)
                processed_ids = load_processed_articles()
                new_articles_found = 0

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
                    logger.info(f"[{feed_name}] Sent new article {article_id} ('{entry.title[:50]}...') to topic {KAFKA_TOPIC}")

                    save_processed_article(article_id)
                if new_articles_found > 0:
                    logger.info(f"[{feed_name}] Found {new_articles_found} new articles.") 
                    producer.flush()
            
            except Exception as e:
                logger.error(f"[{feed_name}] Error processing feed: {e}")
            
            logger.info(f"[{feed_name}] Waiting for {interval} seconds before next poll...")
            await asyncio.sleep(interval)
            

async def main():
    """Main function to create and manage polling tasks for all feeds."""
    logger.info("Starting RSS feed polling...")

    with open(FEEDS_CONFIG_PATH, 'r') as f:
        feeds = json.load(f)

    producer = create_producer()

    # Create async task for each feed
    tasks = [asyncio.create_task(poll_feed(feed, producer)) for feed in feeds]

    logger.info(f"Initialized {len(tasks)} feed polling tasks.")


    # RUn tasks concurrently and forever
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Polling interrupted by user. Shutting down...")
