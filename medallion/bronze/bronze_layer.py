"""
Bronze Layer — Ingestion des données brutes vers MinIO
Stocke les articles tels quels (JSON) dans le bucket bronze/
Partitionnement: bronze/source=hespress/year=2025/month=01/day=15/HH_MM.json
"""

import json
import logging
import os
from datetime import datetime
from io import BytesIO

import boto3
from botocore.client import Config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Configuration MinIO
# ──────────────────────────────────────────────

MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS    = os.getenv("MINIO_ACCESS_KEY",  "minioadmin")
MINIO_SECRET    = os.getenv("MINIO_SECRET_KEY",  "minioadmin")
BRONZE_BUCKET   = "bronze"
SILVER_BUCKET   = "silver"
GOLD_BUCKET     = "gold"


def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_buckets():
    """Crée les buckets s'ils n'existent pas encore."""
    client = get_minio_client()
    for bucket in [BRONZE_BUCKET, SILVER_BUCKET, GOLD_BUCKET]:
        try:
            client.head_bucket(Bucket=bucket)
        except Exception:
            client.create_bucket(Bucket=bucket)
            logger.info(f"Created bucket: {bucket}")


def build_bronze_key(source: str, dt: datetime) -> str:
    """
    Construit la clé S3 partitionnée par source/année/mois/jour.
    Ex: source=hespress/year=2025/month=01/day=15/14_30.json
    """
    return (
        f"source={source}/"
        f"year={dt.year}/"
        f"month={dt.month:02d}/"
        f"day={dt.day:02d}/"
        f"{dt.hour:02d}_{dt.minute:02d}.json"
    )


def save_to_bronze(articles: list[dict], source: str) -> str:
    """
    Sauvegarde une liste d'articles bruts dans le bucket Bronze.
    Retourne la clé S3 du fichier créé.
    """
    client = get_minio_client()
    ensure_buckets()

    now = datetime.utcnow()
    key = build_bronze_key(source, now)

    payload = {
        "metadata": {
            "source":      source,
            "ingested_at": now.isoformat(),
            "count":       len(articles),
            "layer":       "bronze",
        },
        "articles": articles,
    }

    body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")

    client.put_object(
        Bucket=BRONZE_BUCKET,
        Key=key,
        Body=BytesIO(body),
        ContentType="application/json",
        ContentLength=len(body),
        Metadata={
            "source":  source,
            "layer":   "bronze",
            "records": str(len(articles)),
        },
    )

    logger.info(f"Bronze: saved {len(articles)} articles -> s3://{BRONZE_BUCKET}/{key}")
    return f"s3://{BRONZE_BUCKET}/{key}"


def list_bronze_files(source: str | None = None) -> list[str]:
    """Liste les fichiers dans le bucket Bronze, filtrés par source optionnelle."""
    client = get_minio_client()
    prefix = f"source={source}/" if source else ""

    response = client.list_objects_v2(Bucket=BRONZE_BUCKET, Prefix=prefix)
    keys = [obj["Key"] for obj in response.get("Contents", [])]
    return keys


def read_from_bronze(key: str) -> list[dict]:
    """Lit et retourne les articles depuis un fichier Bronze."""
    client = get_minio_client()
    response = client.get_object(Bucket=BRONZE_BUCKET, Key=key)
    payload = json.loads(response["Body"].read().decode("utf-8"))
    return payload.get("articles", [])


# ──────────────────────────────────────────────
# Test local
# ──────────────────────────────────────────────

if __name__ == "__main__":
    # Exemple avec données mock
    sample_articles = [
        {
            "id": "abc123",
            "title": "Test Article",
            "author": "Test Author",
            "published_at": datetime.utcnow().isoformat(),
            "category": "politique",
            "content": "Contenu de test...",
            "source": "hespress",
            "url": "https://hespress.com/test-123.html",
            "language": "ar",
            "scraped_at": datetime.utcnow().isoformat(),
        }
    ]

    path = save_to_bronze(sample_articles, "hespress")
    print(f"Saved to: {path}")
