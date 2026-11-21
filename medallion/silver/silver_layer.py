"""
Silver Layer — Nettoyage, normalisation et enrichissement des articles
Bronze (JSON brut) → Silver (Parquet propre)

Transformations appliquées:
  - Suppression balises HTML
  - Normalisation texte (espaces, casse)
  - Détection de langue
  - Déduplication par ID
  - Validation des champs obligatoires
  - Normalisation des dates
"""

import re
import logging
import os
from datetime import datetime
from html import unescape

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField, TimestampType

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Spark Session
# ──────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("NewsPlatform-Silver")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

# ──────────────────────────────────────────────
# UDFs (User Defined Functions)
# ──────────────────────────────────────────────

def _clean_html(text: str) -> str:
    """Supprime les balises HTML et normalise les espaces."""
    if not text:
        return ""
    text = unescape(text)
    text = re.sub(r"<[^>]+>", " ", text)          # Balises HTML
    text = re.sub(r"&\w+;", " ", text)             # Entités HTML résiduelles
    text = re.sub(r"\s+", " ", text)               # Espaces multiples
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text)  # Caractères de contrôle
    return text.strip()


def _detect_language(text: str) -> str:
    """
    Détection simple de la langue basée sur les caractères Unicode.
    En production: utiliser langdetect ou fastText.
    """
    if not text or len(text) < 20:
        return "unknown"
    arabic_chars = len(re.findall(r"[\u0600-\u06FF]", text))
    latin_chars  = len(re.findall(r"[a-zA-Z]", text))
    french_chars = len(re.findall(r"[àâäéèêëïîôùûüç]", text, re.IGNORECASE))

    if arabic_chars > latin_chars:
        return "ar"
    elif french_chars > 5:
        return "fr"
    elif latin_chars > 0:
        return "en"
    return "unknown"


def _normalize_date(date_str: str) -> str:
    """Normalise différents formats de date vers ISO 8601."""
    if not date_str:
        return datetime.utcnow().isoformat()
    formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%a, %d %b %Y %H:%M:%S %z",
        "%Y-%m-%d",
        "%d/%m/%Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(date_str[:25], fmt).isoformat()
        except ValueError:
            continue
    return datetime.utcnow().isoformat()


# Enregistrement des UDFs Spark
clean_html_udf       = F.udf(_clean_html, StringType())
detect_language_udf  = F.udf(_detect_language, StringType())
normalize_date_udf   = F.udf(_normalize_date, StringType())


# ──────────────────────────────────────────────
# Pipeline Silver
# ──────────────────────────────────────────────

def bronze_to_silver(spark: SparkSession, bronze_path: str, silver_path: str) -> DataFrame:
    """
    Lit les données Bronze, applique les transformations et écrit en Parquet Silver.
    """
    logger.info(f"Reading bronze: {bronze_path}")

    # Lecture du JSON (Bronze)
    df_raw = spark.read.option("multiLine", True).json(bronze_path)

    # Si structure imbriquée {metadata, articles[]}
    if "articles" in df_raw.columns:
        df = df_raw.select(F.explode("articles").alias("article")).select("article.*")
    else:
        df = df_raw

    logger.info(f"Raw records: {df.count()}")

    # ── Nettoyage ──────────────────────────────
    df_clean = (
        df
        # Nettoyage HTML sur titre et contenu
        .withColumn("title",   clean_html_udf(F.col("title")))
        .withColumn("content", clean_html_udf(F.col("content")))
        .withColumn("author",  clean_html_udf(F.col("author")))

        # Normalisation date
        .withColumn("published_at", normalize_date_udf(F.col("published_at")))

        # Détection langue (si non renseignée ou inconnue)
        .withColumn(
            "language",
            F.when(
                F.col("language").isNull() | (F.col("language") == "unknown"),
                detect_language_udf(F.col("content"))
            ).otherwise(F.col("language"))
        )

        # Normalisation de la source (lowercase, strip)
        .withColumn("source", F.lower(F.trim(F.col("source"))))
        .withColumn("category", F.lower(F.trim(F.col("category"))))

        # Longueur du contenu (utile pour la qualité)
        .withColumn("content_length", F.length(F.col("content")))

        # Timestamp de traitement
        .withColumn("silver_processed_at", F.lit(datetime.utcnow().isoformat()))

        # Partition helpers
        .withColumn("pub_date", F.to_date(F.col("published_at")))
        .withColumn("pub_year",  F.year(F.col("pub_date")))
        .withColumn("pub_month", F.month(F.col("pub_date")))
    )

    # ── Filtres qualité de base ─────────────────
    df_valid = (
        df_clean
        .filter(F.col("title").isNotNull() & (F.length("title") > 5))
        .filter(F.col("content").isNotNull() & (F.length("content") > 50))
        .filter(F.col("url").isNotNull())
    )

    # ── Déduplication par ID ────────────────────
    df_dedup = df_valid.dropDuplicates(["id"])

    logger.info(f"Silver records (after cleaning + dedup): {df_dedup.count()}")

    # ── Écriture Parquet partitionné ────────────
    (
        df_dedup
        .write
        .mode("append")
        .partitionBy("source", "pub_year", "pub_month")
        .parquet(silver_path)
    )

    logger.info(f"Silver written to: {silver_path}")
    return df_dedup


# ──────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────

if __name__ == "__main__":
    spark = create_spark_session()

    bronze_path = "s3a://bronze/"
    silver_path = "s3a://silver/"

    df_silver = bronze_to_silver(spark, bronze_path, silver_path)
    df_silver.printSchema()
    df_silver.show(5, truncate=80)

    spark.stop()
