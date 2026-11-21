"""
Runner principal : lance tous les scrapers et publie vers Kafka (streaming)
ou sauvegarde en JSON (batch).

Usage:
  python run_scrapers.py --mode batch
  python run_scrapers.py --mode streaming
"""

import argparse
import json
import logging
import os
from datetime import datetime
from typing import Callable

from hespress_scraper import scrape_hespress
from bbc_scraper import scrape_bbc
from aljazeera_scraper import scrape_aljazeera

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

SCRAPERS: dict[str, Callable] = {
    "hespress":  lambda: scrape_hespress(max_articles_per_category=20),
    "bbc_news":  lambda: scrape_bbc(max_articles_per_category=20),
    "aljazeera": lambda: scrape_aljazeera(max_articles_per_category=20),
}


# ──────────────────────────────────────────────
# MODE BATCH : sauvegarde JSON local
# ──────────────────────────────────────────────

def run_batch():
    """Collecte tous les articles et les sauvegarde en JSON partitionné."""
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_dir = f"data/raw/{timestamp}"
    os.makedirs(output_dir, exist_ok=True)

    all_articles = []
    for source, scraper in SCRAPERS.items():
        logger.info(f"=== Running scraper: {source} ===")
        try:
            articles = scraper()
            all_articles.extend(articles)

            # Fichier par source
            path = f"{output_dir}/{source}.json"
            with open(path, "w", encoding="utf-8") as f:
                json.dump(articles, f, ensure_ascii=False, indent=2)
            logger.info(f"  Saved {len(articles)} articles -> {path}")
        except Exception as e:
            logger.error(f"  Scraper {source} failed: {e}")

    # Fichier consolidé
    consolidated_path = f"{output_dir}/all_articles.json"
    with open(consolidated_path, "w", encoding="utf-8") as f:
        json.dump(all_articles, f, ensure_ascii=False, indent=2)

    logger.info(f"\n=== Batch complete: {len(all_articles)} total articles -> {output_dir} ===")
    return all_articles


# ──────────────────────────────────────────────
# MODE STREAMING : publication vers Kafka
# ──────────────────────────────────────────────

def run_streaming():
    """Scrape et publie chaque article immédiatement vers Kafka."""
    try:
        from kafka import KafkaProducer
    except ImportError:
        logger.error("kafka-python not installed. Run: pip install kafka-python")
        return

    producer = KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=3,
    )

    TOPIC = "news.raw"
    total = 0

    for source, scraper in SCRAPERS.items():
        logger.info(f"=== Streaming scraper: {source} ===")
        try:
            articles = scraper()
            for article in articles:
                producer.send(TOPIC, value=article, key=article["id"].encode())
                total += 1
            producer.flush()
            logger.info(f"  Published {len(articles)} events from {source}")
        except Exception as e:
            logger.error(f"  Error streaming {source}: {e}")

    producer.close()
    logger.info(f"\n=== Streaming complete: {total} events published to topic '{TOPIC}' ===")


# ──────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="News scraper runner")
    parser.add_argument(
        "--mode", choices=["batch", "streaming"], default="batch",
        help="batch = save JSON locally | streaming = publish to Kafka"
    )
    args = parser.parse_args()

    if args.mode == "batch":
        run_batch()
    else:
        run_streaming()
