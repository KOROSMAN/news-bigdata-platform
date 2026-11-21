"""
DAG Airflow — Pipeline Streaming (Kafka → Bronze)
Fréquence: toutes les 5 minutes (consomme les événements Kafka)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner":            "data-team",
    "depends_on_past":  False,
    "start_date":       datetime(2025, 1, 1),
    "retries":          1,
    "retry_delay":      timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=8),
}

dag = DAG(
    dag_id="news_pipeline_streaming",
    description="Consommation des événements Kafka et ingestion Bronze (micro-batch toutes les 5 min)",
    schedule_interval="*/5 * * * *",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["news", "streaming", "kafka"],
)


def consume_kafka_to_bronze(**context):
    """
    Consomme les messages du topic Kafka 'news.raw' et les écrit en Bronze.
    Polling limité à max_records ou max_seconds.
    """
    import os
    import json
    from kafka import KafkaConsumer
    from medallion.bronze.bronze_layer import save_to_bronze

    KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    TOPIC         = "news.raw"
    MAX_RECORDS   = 500
    MAX_SECONDS   = 240  # 4 minutes max de consommation

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id="news-bronze-consumer",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=5000,
    )

    articles_by_source: dict[str, list] = {}
    count = 0
    start = datetime.utcnow()

    for message in consumer:
        article = message.value
        source  = article.get("source", "unknown")
        articles_by_source.setdefault(source, []).append(article)
        count += 1

        if count >= MAX_RECORDS:
            break
        if (datetime.utcnow() - start).seconds >= MAX_SECONDS:
            break

    consumer.commit()
    consumer.close()

    # Sauvegarde par source
    paths = []
    for source, articles in articles_by_source.items():
        path = save_to_bronze(articles, source)
        paths.append(path)

    context["ti"].xcom_push(key="consumed_count", value=count)
    context["ti"].xcom_push(key="bronze_paths", value=paths)
    return f"Consumed {count} messages from Kafka"


def micro_batch_to_silver(**context):
    """Déclenche une transformation Silver rapide sur les nouveaux fichiers Bronze."""
    from pyspark.sql import SparkSession
    from medallion.silver.silver_layer import create_spark_session, bronze_to_silver

    ti = context["ti"]
    paths = ti.xcom_pull(key="bronze_paths") or []

    if not paths:
        return "No new Bronze files to process"

    spark = create_spark_session()
    try:
        for path in paths:
            bronze_to_silver(spark, path, "s3a://silver/")
    finally:
        spark.stop()

    return f"Processed {len(paths)} Bronze files to Silver"


with dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    consume = PythonOperator(
        task_id="consume_kafka_to_bronze",
        python_callable=consume_kafka_to_bronze,
        provide_context=True,
    )

    to_silver = PythonOperator(
        task_id="micro_batch_to_silver",
        python_callable=micro_batch_to_silver,
        provide_context=True,
    )

    start >> consume >> to_silver >> end
