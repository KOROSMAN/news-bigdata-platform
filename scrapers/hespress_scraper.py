"""
Scraper for Hespress.com - Moroccan news site
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ar,fr;q=0.9,en;q=0.8",
}

BASE_URL = "https://www.hespress.com"

CATEGORIES = {
    "politique": f"{BASE_URL}/politique",
    "societe":   f"{BASE_URL}/societe",
    "economie":  f"{BASE_URL}/economie",
    "sport":     f"{BASE_URL}/sport",
    "monde":     f"{BASE_URL}/monde",
}


def generate_article_id(url: str) -> str:
    """Génère un ID unique basé sur l'URL."""
    return hashlib.md5(url.encode()).hexdigest()


def fetch_page(url: str, retries: int = 3) -> BeautifulSoup | None:
    """Récupère et parse une page HTML avec retry."""
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=HEADERS, timeout=15)
            response.raise_for_status()
            return BeautifulSoup(response.text, "html.parser")
        except requests.RequestException as e:
            logger.warning(f"Attempt {attempt+1}/{retries} failed for {url}: {e}")
            time.sleep(2 ** attempt + random.uniform(0, 1))
    logger.error(f"All retries failed for {url}")
    return None


def parse_article_list(soup: BeautifulSoup, category: str) -> list[dict]:
    """Extrait les liens d'articles depuis la page de catégorie."""
    articles = []
    # Hespress utilise des articles avec classe 'overlay'
    for card in soup.select("article.card, div.overlay, .post-item"):
        link_tag = card.find("a", href=True)
        title_tag = card.find(["h2", "h3", "h4"])
        if not link_tag:
            continue
        url = link_tag["href"]
        if not url.startswith("http"):
            url = BASE_URL + url
        articles.append({
            "url": url,
            "category": category,
            "title_preview": title_tag.get_text(strip=True) if title_tag else "",
        })
    return articles


def parse_article_detail(url: str, category: str) -> dict | None:
    """Scrape le détail complet d'un article."""
    soup = fetch_page(url)
    if not soup:
        return None

    # Titre
    title_tag = soup.find("h1")
    title = title_tag.get_text(strip=True) if title_tag else ""

    # Auteur
    author_tag = soup.find(class_=["author", "post-author", "writer"])
    author = author_tag.get_text(strip=True) if author_tag else "Hespress"

    # Date
    date_tag = soup.find("time") or soup.find(class_=["date", "post-date", "published"])
    pub_date = None
    if date_tag:
        pub_date = date_tag.get("datetime") or date_tag.get_text(strip=True)

    # Contenu (paragraphes du corps de l'article)
    content_div = (
        soup.find(class_=["article-content", "post-content", "entry-content"])
        or soup.find("article")
    )
    if content_div:
        # Supprimer les scripts et styles inclus
        for tag in content_div(["script", "style", "aside", "nav"]):
            tag.decompose()
        content = " ".join(p.get_text(strip=True) for p in content_div.find_all("p"))
    else:
        content = ""

    return {
        "id":           generate_article_id(url),
        "title":        title,
        "author":       author,
        "published_at": pub_date or datetime.utcnow().isoformat(),
        "category":     category,
        "content":      content,
        "source":       "hespress",
        "url":          url,
        "language":     "ar",
        "scraped_at":   datetime.utcnow().isoformat(),
        "raw_html":     "",  # On ne stocke pas le HTML brut ici (stocké en Bronze)
    }


def scrape_hespress(max_articles_per_category: int = 20) -> list[dict]:
    """Point d'entrée principal du scraper Hespress."""
    all_articles = []

    for category, cat_url in CATEGORIES.items():
        logger.info(f"Scraping category: {category} -> {cat_url}")
        soup = fetch_page(cat_url)
        if not soup:
            continue

        article_links = parse_article_list(soup, category)
        logger.info(f"  Found {len(article_links)} article links")

        for item in article_links[:max_articles_per_category]:
            article = parse_article_detail(item["url"], item["category"])
            if article and article["title"] and article["content"]:
                all_articles.append(article)
                logger.info(f"  OK: {article['title'][:60]}...")
            time.sleep(random.uniform(1.0, 2.5))  # Politeness delay

    logger.info(f"Total articles scraped: {len(all_articles)}")
    return all_articles


if __name__ == "__main__":
    articles = scrape_hespress(max_articles_per_category=5)
    output_path = f"data/raw/hespress_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(articles)} articles to {output_path}")
