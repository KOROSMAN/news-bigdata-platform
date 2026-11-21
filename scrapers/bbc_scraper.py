"""
Scraper for BBC News - International source
Collecte: titre, auteur, date, catégorie, contenu, source, url
"""

import requests
from bs4 import BeautifulSoup
from datetime import datetime
import json
import hashlib
import logging
import time
import random
import feedparser

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}

# BBC expose des flux RSS — méthode plus stable que le scraping direct
RSS_FEEDS = {
    "world":       "http://feeds.bbci.co.uk/news/world/rss.xml",
    "technology":  "http://feeds.bbci.co.uk/news/technology/rss.xml",
    "business":    "http://feeds.bbci.co.uk/news/business/rss.xml",
    "science":     "http://feeds.bbci.co.uk/news/science_and_environment/rss.xml",
    "health":      "http://feeds.bbci.co.uk/news/health/rss.xml",
}


def generate_article_id(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()


def fetch_article_content(url: str) -> str:
    """Récupère le contenu complet depuis la page BBC."""
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        # BBC structure: article > div[data-component="text-block"]
        content_blocks = soup.select(
            "article [data-component='text-block'] p, "
            "article .ssrcss-11r1m41-RichTextComponentWrapper p, "
            "article p"
        )
        content = " ".join(b.get_text(strip=True) for b in content_blocks)
        return content if content else ""
    except Exception as e:
        logger.warning(f"Could not fetch content from {url}: {e}")
        return ""


def parse_rss_entry(entry: dict, category: str) -> dict:
    """Transforme une entrée RSS en article structuré."""
    url = entry.get("link", "")

    # Auteur depuis RSS
    author = "BBC News"
    if hasattr(entry, "author"):
        author = entry.author

    # Date
    published = entry.get("published", datetime.utcnow().isoformat())
    try:
        from email.utils import parsedate_to_datetime
        published = parsedate_to_datetime(published).isoformat()
    except Exception:
        pass

    # Résumé depuis RSS (description courte)
    summary = entry.get("summary", "")
    if summary:
        summary_soup = BeautifulSoup(summary, "html.parser")
        summary = summary_soup.get_text(strip=True)

    return {
        "id":           generate_article_id(url),
        "title":        entry.get("title", ""),
        "author":       author,
        "published_at": published,
        "category":     category,
        "content":      summary,  # Sera enrichi avec fetch_article_content si nécessaire
        "source":       "bbc_news",
        "url":          url,
        "language":     "en",
        "scraped_at":   datetime.utcnow().isoformat(),
    }


def scrape_bbc(max_articles_per_category: int = 20, fetch_full_content: bool = True) -> list[dict]:
    """Point d'entrée principal du scraper BBC."""
    all_articles = []

    for category, rss_url in RSS_FEEDS.items():
        logger.info(f"Fetching RSS feed: {category} -> {rss_url}")
        feed = feedparser.parse(rss_url)

        if feed.bozo:
            logger.warning(f"RSS parse warning for {category}: {feed.bozo_exception}")

        entries = feed.entries[:max_articles_per_category]
        logger.info(f"  Found {len(entries)} entries")

        for entry in entries:
            article = parse_rss_entry(entry, category)
            if not article["title"] or not article["url"]:
                continue

            # Récupération optionnelle du contenu complet
            if fetch_full_content and article["url"]:
                full_content = fetch_article_content(article["url"])
                if full_content:
                    article["content"] = full_content
                time.sleep(random.uniform(0.8, 1.8))

            all_articles.append(article)
            logger.info(f"  OK: {article['title'][:60]}...")

    logger.info(f"Total articles scraped: {len(all_articles)}")
    return all_articles


if __name__ == "__main__":
    articles = scrape_bbc(max_articles_per_category=5, fetch_full_content=False)
    output_path = f"data/raw/bbc_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(articles)} articles to {output_path}")
