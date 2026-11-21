"""
Gold Layer — Tables analytiques prêtes pour le Data Warehouse
Silver (Parquet) → Gold (Parquet optimisé + PostgreSQL DWH)

Tables produites:
  - articles_par_jour        : volume quotidien par source
  - articles_par_categorie   : distribution thématique
  - articles_par_langue      : répartition linguistique
  - top_mots_cles            : fréquence des termes significatifs
  - tendances_news           : score de tendance par catégorie/source
"""

import logging
import os
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

STOPWORDS_FR = {
    "le","la","les","de","du","des","un","une","et","en","au","aux","par","sur",
    "pour","dans","avec","est","sont","a","à","se","ce","qui","que","ou","si",
    "ne","pas","plus","il","elle","ils","elles","nous","vous","on","mais","donc",
    "or","ni","car","je","tu","y","cela","cette","ces","son","sa","ses","leur",
    "leurs","dont","tout","tous","aussi","comme","bien","très","même","encore",
}
STOPWORDS_EN = {
    "the","a","an","and","in","of","to","is","it","for","on","at","by","from",
    "with","as","was","are","be","this","that","or","but","not","have","had",
    "has","its","they","he","she","we","you","said","been","were","also","more",
    "after","before","about","when","all","some","into","than","their","which",
}
STOPWORDS = STOPWORDS_FR | STOPWORDS_EN


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("NewsPlatform-Gold")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.6.0")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


# ──────────────────────────────────────────────
# Table 1 : Articles par jour
# ──────────────────────────────────────────────

def build_articles_par_jour(df_silver: DataFrame) -> DataFrame:
    """Nombre d'articles publiés par jour et par source."""
    return (
        df_silver
        .groupBy("pub_date", "source")
        .agg(
            F.count("*").alias("nb_articles"),
            F.avg("content_length").alias("avg_content_length"),
            F.countDistinct("category").alias("nb_categories"),
        )
        .orderBy("pub_date", "source")
        .withColumn("computed_at", F.lit(datetime.utcnow().isoformat()))
    )


# ──────────────────────────────────────────────
# Table 2 : Articles par catégorie
# ──────────────────────────────────────────────

def build_articles_par_categorie(df_silver: DataFrame) -> DataFrame:
    """Distribution des articles par thème/catégorie."""
    total = df_silver.count()
    return (
        df_silver
        .groupBy("category", "source")
        .agg(F.count("*").alias("nb_articles"))
        .withColumn("pct_total", F.round(F.col("nb_articles") / total * 100, 2))
        .orderBy(F.desc("nb_articles"))
        .withColumn("computed_at", F.lit(datetime.utcnow().isoformat()))
    )


# ──────────────────────────────────────────────
# Table 3 : Articles par langue
# ──────────────────────────────────────────────

def build_articles_par_langue(df_silver: DataFrame) -> DataFrame:
    """Répartition linguistique du corpus."""
    return (
        df_silver
        .groupBy("language", "source")
        .agg(F.count("*").alias("nb_articles"))
        .orderBy(F.desc("nb_articles"))
        .withColumn("computed_at", F.lit(datetime.utcnow().isoformat()))
    )


# ──────────────────────────────────────────────
# Table 4 : Top mots-clés
# ──────────────────────────────────────────────

def build_top_mots_cles(df_silver: DataFrame, top_n: int = 200) -> DataFrame:
    """
    Fréquence des mots significatifs (stopwords exclus).
    Utilise explode sur les tokens du titre + contenu.
    """
    stopwords_broadcast = list(STOPWORDS)

    df_words = (
        df_silver
        .select("source", "category", "pub_date",
                F.concat_ws(" ", F.col("title"), F.col("content")).alias("full_text"))
        .withColumn("words", F.split(F.lower(F.col("full_text")), r"[\s\W]+"))
        .withColumn("word", F.explode("words"))
        .filter(F.length("word") > 3)
        .filter(~F.col("word").isin(stopwords_broadcast))
        .filter(F.col("word").rlike(r"^[a-zà-ÿ\u0600-\u06FF]+$"))  # Lettres seulement
    )

    return (
        df_words
        .groupBy("word", "source")
        .agg(F.count("*").alias("frequency"))
        .orderBy(F.desc("frequency"))
        .limit(top_n)
        .withColumn("computed_at", F.lit(datetime.utcnow().isoformat()))
    )


# ──────────────────────────────────────────────
# Table 5 : Score de tendance
# ──────────────────────────────────────────────

def build_tendances(df_silver: DataFrame) -> DataFrame:
    """
    Score de tendance: catégories les plus actives sur les 7 derniers jours
    vs la moyenne historique. Un score > 1.5 = tendance montante.
    """
    from pyspark.sql.functions import datediff, current_date

    df_recent = df_silver.filter(datediff(current_date(), F.col("pub_date")) <= 7)
    df_hist   = df_silver.filter(datediff(current_date(), F.col("pub_date")) <= 30)

    recent_counts = (
        df_recent.groupBy("category", "source")
        .agg(F.count("*").alias("count_recent"))
    )

    hist_counts = (
        df_hist.groupBy("category", "source")
        .agg((F.count("*") / 4).alias("count_avg_week"))  # moyenne hebdo sur 4 semaines
    )

    return (
        recent_counts.join(hist_counts, on=["category", "source"], how="left")
        .withColumn(
            "trend_score",
            F.round(
                F.col("count_recent") / F.greatest(F.col("count_avg_week"), F.lit(1)),
                2
            )
        )
        .withColumn(
            "trend_label",
            F.when(F.col("trend_score") >= 1.5, "🔥 Trending")
             .when(F.col("trend_score") >= 1.0, "📈 Stable")
             .otherwise("📉 Declining")
        )
        .orderBy(F.desc("trend_score"))
        .withColumn("computed_at", F.lit(datetime.utcnow().isoformat()))
    )


# ──────────────────────────────────────────────
# Écriture Gold (Parquet + PostgreSQL)
# ──────────────────────────────────────────────

def write_gold(df: DataFrame, table_name: str, gold_path: str):
    """Écrit une table Gold en Parquet ET en PostgreSQL."""
    # Parquet (Data Lake)
    path = f"{gold_path}/{table_name}"
    df.write.mode("overwrite").parquet(path)
    logger.info(f"Gold Parquet: {path}")

    # PostgreSQL (Data Warehouse)
    try:
        pg_url  = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/newsdb")
        pg_user = os.getenv("POSTGRES_USER", "newsuser")
        pg_pass = os.getenv("POSTGRES_PASSWORD", "newspass")
        (
            df.write
            .format("jdbc")
            .option("url", pg_url)
            .option("dbtable", f"gold.{table_name}")
            .option("user", pg_user)
            .option("password", pg_pass)
            .option("driver", "org.postgresql.Driver")
            .mode("overwrite")
            .save()
        )
        logger.info(f"Gold PostgreSQL: gold.{table_name}")
    except Exception as e:
        logger.warning(f"PostgreSQL write failed (skipped): {e}")


# ──────────────────────────────────────────────
# Pipeline principal
# ──────────────────────────────────────────────

def silver_to_gold(spark: SparkSession, silver_path: str, gold_path: str):
    logger.info(f"Reading silver: {silver_path}")
    df_silver = spark.read.parquet(silver_path)
    logger.info(f"Silver records: {df_silver.count()}")
    df_silver.cache()

    tables = {
        "articles_par_jour":       build_articles_par_jour(df_silver),
        "articles_par_categorie":  build_articles_par_categorie(df_silver),
        "articles_par_langue":     build_articles_par_langue(df_silver),
        "top_mots_cles":           build_top_mots_cles(df_silver),
        "tendances_news":          build_tendances(df_silver),
    }

    for name, df in tables.items():
        logger.info(f"Building table: {name}")
        write_gold(df, name, gold_path)

    df_silver.unpersist()
    logger.info("Gold layer complete.")


if __name__ == "__main__":
    spark = create_spark_session()
    silver_to_gold(spark, "s3a://silver/", "s3a://gold/")
    spark.stop()
