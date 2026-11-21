"""
Module de Qualité des Données — News Platform
Dimensions: Complétude, Cohérence, Validité
Tests: Article sans titre, Date manquante, Contenu trop court, URL invalide
"""

import json
import re
import logging
from datetime import datetime
from typing import Any
from urllib.parse import urlparse

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Seuils et règles
# ──────────────────────────────────────────────

MIN_CONTENT_LENGTH = 100   # Caractères minimum pour le contenu
MIN_TITLE_LENGTH   = 5     # Caractères minimum pour le titre
MAX_FUTURE_DAYS    = 1     # Jours max dans le futur (tolérance)
VALID_SOURCES      = {"hespress", "bbc_news", "aljazeera", "reuters", "cnn"}
VALID_LANGUAGES    = {"ar", "en", "fr", "es", "unknown"}


# ──────────────────────────────────────────────
# Tests individuels
# ──────────────────────────────────────────────

def check_titre_absent(article: dict) -> dict:
    """Complétude: le titre est présent et non vide."""
    title = article.get("title", "")
    passed = bool(title and len(title.strip()) >= MIN_TITLE_LENGTH)
    return {
        "test":    "titre_absent",
        "passed":  passed,
        "message": None if passed else f"Titre manquant ou trop court: '{title[:30]}'",
        "dimension": "complétude",
    }


def check_date_manquante(article: dict) -> dict:
    """Complétude: la date de publication est présente et parsable."""
    date_str = article.get("published_at", "")
    if not date_str:
        return {"test": "date_manquante", "passed": False, "message": "Date absente", "dimension": "complétude"}

    formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d",
    ]
    parsed = None
    for fmt in formats:
        try:
            parsed = datetime.strptime(date_str[:19], fmt)
            break
        except ValueError:
            continue

    if not parsed:
        return {"test": "date_manquante", "passed": False, "message": f"Date non parsable: {date_str}", "dimension": "validité"}

    # Vérifier que la date n'est pas dans le futur (sauf tolérance)
    delta = (parsed - datetime.utcnow()).days
    if delta > MAX_FUTURE_DAYS:
        return {"test": "date_manquante", "passed": False, "message": f"Date dans le futur: {date_str}", "dimension": "cohérence"}

    return {"test": "date_manquante", "passed": True, "message": None, "dimension": "complétude"}


def check_contenu_court(article: dict) -> dict:
    """Validité: le contenu est assez long pour être exploitable."""
    content = article.get("content", "")
    length = len(content.strip())
    passed = length >= MIN_CONTENT_LENGTH
    return {
        "test":    "contenu_court",
        "passed":  passed,
        "message": None if passed else f"Contenu trop court: {length} caractères (min {MIN_CONTENT_LENGTH})",
        "dimension": "validité",
    }


def check_url_invalide(article: dict) -> dict:
    """Validité: l'URL est présente et syntaxiquement valide."""
    url = article.get("url", "")
    if not url:
        return {"test": "url_invalide", "passed": False, "message": "URL absente", "dimension": "complétude"}

    try:
        result = urlparse(url)
        valid = all([result.scheme in ("http", "https"), result.netloc])
    except Exception:
        valid = False

    return {
        "test":    "url_invalide",
        "passed":  valid,
        "message": None if valid else f"URL invalide: {url[:80]}",
        "dimension": "validité",
    }


def check_source_inconnue(article: dict) -> dict:
    """Cohérence: la source appartient à la liste des sources reconnues."""
    source = article.get("source", "").lower().strip()
    passed = source in VALID_SOURCES
    return {
        "test":    "source_inconnue",
        "passed":  passed,
        "message": None if passed else f"Source non reconnue: '{source}'",
        "dimension": "cohérence",
    }


def check_langue_inconnue(article: dict) -> dict:
    """Cohérence: la langue est renseignée et reconnue."""
    lang = article.get("language", "").lower().strip()
    passed = lang in VALID_LANGUAGES
    return {
        "test":    "langue_inconnue",
        "passed":  passed,
        "message": None if passed else f"Langue non reconnue: '{lang}'",
        "dimension": "cohérence",
    }


def check_doublon_url(articles: list[dict]) -> dict:
    """Cohérence: pas de doublons d'URL dans le lot."""
    urls = [a.get("url", "") for a in articles]
    seen = set()
    duplicates = []
    for url in urls:
        if url in seen:
            duplicates.append(url)
        seen.add(url)

    passed = len(duplicates) == 0
    return {
        "test":    "doublon_url",
        "passed":  passed,
        "message": None if passed else f"{len(duplicates)} URL(s) en doublon",
        "dimension": "cohérence",
        "details": duplicates[:10],
    }


# ──────────────────────────────────────────────
# Runner principal
# ──────────────────────────────────────────────

ARTICLE_CHECKS = [
    check_titre_absent,
    check_date_manquante,
    check_contenu_court,
    check_url_invalide,
    check_source_inconnue,
    check_langue_inconnue,
]


def check_articles(articles: list[dict]) -> dict:
    """
    Lance tous les tests sur un lot d'articles.
    Retourne un rapport structuré.
    """
    results = {
        "total_articles":     len(articles),
        "checked_at":         datetime.utcnow().isoformat(),
        "passed":             True,
        "summary_by_test":    {},
        "summary_by_dimension": {"complétude": 0, "cohérence": 0, "validité": 0},
        "failed_articles":    [],
    }

    for article in articles:
        article_issues = []
        for check_fn in ARTICLE_CHECKS:
            result = check_fn(article)
            if not result["passed"]:
                article_issues.append(result)
                dim = result.get("dimension", "autre")
                results["summary_by_dimension"][dim] = results["summary_by_dimension"].get(dim, 0) + 1
                test_name = result["test"]
                results["summary_by_test"][test_name] = results["summary_by_test"].get(test_name, 0) + 1

        if article_issues:
            results["passed"] = False
            results["failed_articles"].append({
                "id":     article.get("id", "?"),
                "url":    article.get("url", "?")[:80],
                "issues": article_issues,
            })

    # Test de lot (doublons)
    dedup_result = check_doublon_url(articles)
    if not dedup_result["passed"]:
        results["passed"] = False
        results["summary_by_test"]["doublon_url"] = 1

    # Taux de qualité global
    total_checks = len(articles) * len(ARTICLE_CHECKS)
    failed_checks = sum(results["summary_by_test"].values())
    results["quality_score"] = round((1 - failed_checks / max(total_checks, 1)) * 100, 1)

    return results


def run_quality_checks(bronze_paths: list[str]) -> list[dict]:
    """
    Lance les contrôles qualité sur une liste de fichiers Bronze.
    Utilisé par le DAG Airflow.
    """
    import boto3
    from botocore.client import Config
    import os

    client = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    all_results = []
    for path in bronze_paths:
        # path = "s3://bronze/source=hespress/..."
        path_clean = path.replace("s3://", "").replace("s3a://", "")
        bucket, key = path_clean.split("/", 1)

        try:
            response = client.get_object(Bucket=bucket, Key=key)
            payload  = json.loads(response["Body"].read().decode("utf-8"))
            articles = payload.get("articles", [])

            report = check_articles(articles)
            report["source_path"] = path
            all_results.append(report)

            status = "✅ PASS" if report["passed"] else "⚠️ ISSUES"
            logger.info(f"Quality [{status}] {path} — Score: {report['quality_score']}%")

        except Exception as e:
            logger.error(f"Could not check {path}: {e}")
            all_results.append({"source_path": path, "passed": False, "error": str(e)})

    return all_results


# ──────────────────────────────────────────────
# Test local avec données mock
# ──────────────────────────────────────────────

if __name__ == "__main__":
    sample = [
        {
            "id": "1", "title": "Maroc : hausse du PIB", "author": "Hassan",
            "published_at": "2025-01-15T10:00:00", "category": "economie",
            "content": "L'économie marocaine a connu une hausse significative de son PIB au dernier trimestre. Les secteurs du tourisme et de l'agriculture ont été les principaux moteurs de cette croissance remarquable selon le HCP.",
            "source": "hespress", "url": "https://hespress.com/pib-2025-123.html", "language": "fr",
        },
        {
            "id": "2", "title": "", "author": "",  # Titre manquant
            "published_at": "bad-date", "category": "sport",
            "content": "Trop court.",
            "source": "inconnu", "url": "not-a-url", "language": "xyz",
        },
    ]

    report = check_articles(sample)
    print(json.dumps(report, ensure_ascii=False, indent=2))
