"""
Scraper for Al Jazeera English - International source
Utilise les flux RSS officiels pour une collecte stable
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
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

RSS_FEEDS = {
    "top_news":    "https://www.aljazeera.com/xml/rss/all.xml",
    "middle_east": "https://www.aljazeera.com/xml/rss/middleeast.xml",
    "africa":      "https://www.aljazeera.com/xml/rss/africa.xml",
    "economy":     "https://www.aljazeera.com/xml/rss/economy.xml",
}


def generate_article_id(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()


def fetch_article_content(url: str) -> tuple[str, str]:
    """Retourne (content, author) depuis la page Al Jazeera."""
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        # Al Jazeera: article body dans .wysiwyg ou .article-p-wrapper
        content_div = (
            soup.find(class_="wysiwyg")
            or soup.find(class_="article-p-wrapper")
            or soup.find("article")
        )
        content = ""
        if content_div:
            for tag in content_div(["script", "style", "figure"]):
                tag.decompose()
            content = " ".join(p.get_text(strip=True) for p in content_div.find_all("p"))

        author_tag = soup.find(class_=["article-author-name", "author-name"])
        author = author_tag.get_text(strip=True) if author_tag else "Al Jazeera"

        return content, author
    except Exception as e:
        logger.warning(f"Could not fetch content from {url}: {e}")
        return "", "Al Jazeera"


def scrape_aljazeera(max_articles_per_category: int = 20, fetch_full_content: bool = True) -> list[dict]:
    all_articles = []

    for category, rss_url in RSS_FEEDS.items():
        logger.info(f"Fetching RSS: {category}")
        feed = feedparser.parse(rss_url)
        entries = feed.entries[:max_articles_per_category]
        logger.info(f"  {len(entries)} entries found")

        for entry in entries:
            url = entry.get("link", "")
            if not url:
                continue

            published = entry.get("published", datetime.utcnow().isoformat())
            try:
                from email.utils import parsedate_to_datetime
                published = parsedate_to_datetime(published).isoformat()
            except Exception:
                pass

            summary = entry.get("summary", "")
            if summary:
                summary = BeautifulSoup(summary, "html.parser").get_text(strip=True)

            content = summary
            author = "Al Jazeera"

            if fetch_full_content:
                full_content, author = fetch_article_content(url)
                if full_content:
                    content = full_content
                time.sleep(random.uniform(1.0, 2.0))

            article = {
                "id":           generate_article_id(url),
                "title":        entry.get("title", ""),
                "author":       author,
                "published_at": published,
                "category":     category,
                "content":      content,
                "source":       "aljazeera",
                "url":          url,
                "language":     "en",
                "scraped_at":   datetime.utcnow().isoformat(),
            }

            if article["title"] and article["content"]:
                all_articles.append(article)
                logger.info(f"  OK: {article['title'][:60]}...")

    logger.info(f"Total: {len(all_articles)} articles")
    return all_articles


if __name__ == "__main__":
    articles = scrape_aljazeera(max_articles_per_category=5, fetch_full_content=False)
    output_path = f"data/raw/aljazeera_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(articles)} articles to {output_path}")
