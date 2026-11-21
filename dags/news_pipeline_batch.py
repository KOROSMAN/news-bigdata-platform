"""
DAG Airflow — Pipeline Batch News Platform
Fréquence: toutes les heures
Ordre: Scraping → Bronze → Silver → Gold → Quality Check → Notify
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# ──────────────────────────────────────────────
# Configuration du DAG
# ──────────────────────────────────────────────

default_args = {
    "owner":            "data-team",
    "depends_on_past":  False,
    "start_date":       datetime(2025, 1, 1),
    "email_on_failure": True,
    "email":            ["data-alerts@emsi.ma"],
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

dag = DAG(
    dag_id="news_pipeline_batch",
    description="Pipeline de collecte et traitement des articles de presse (batch horaire)",
    schedule_interval="0 * * * *",  # Toutes les heures
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["news", "batch", "medallion"],
)

# ──────────────────────────────────────────────
# Fonctions Python
# ──────────────────────────────────────────────

def run_hespress_scraper(**context):
    """Lance le scraper Hespress et pousse le résultat dans XCom."""
    import sys
    sys.path.insert(0, "/opt/airflow/scrapers")
    from hespress_scraper import scrape_hespress
    from medallion.bronze.bronze_layer import save_to_bronze

    articles = scrape_hespress(max_articles_per_category=20)
    path = save_to_bronze(articles, "hespress")
    context["ti"].xcom_push(key="bronze_path_hespress", value=path)
    return f"Scraped {len(articles)} articles"


def run_bbc_scraper(**context):
    import sys
    sys.path.insert(0, "/opt/airflow/scrapers")
    from bbc_scraper import scrape_bbc
    from medallion.bronze.bronze_layer import save_to_bronze

    articles = scrape_bbc(max_articles_per_category=20)
    path = save_to_bronze(articles, "bbc_news")
    context["ti"].xcom_push(key="bronze_path_bbc", value=path)
    return f"Scraped {len(articles)} articles"


def run_aljazeera_scraper(**context):
    import sys
    sys.path.insert(0, "/opt/airflow/scrapers")
    from aljazeera_scraper import scrape_aljazeera
    from medallion.bronze.bronze_layer import save_to_bronze

    articles = scrape_aljazeera(max_articles_per_category=20)
    path = save_to_bronze(articles, "aljazeera")
    context["ti"].xcom_push(key="bronze_path_aljazeera", value=path)
    return f"Scraped {len(articles)} articles"


def check_data_quality(**context):
    """Vérifie la qualité avant de passer à Silver."""
    # Importé depuis quality/data_quality.py
    import sys
    sys.path.insert(0, "/opt/airflow")
    from quality.data_quality import run_quality_checks

    ti = context["ti"]
    paths = [
        ti.xcom_pull(key="bronze_path_hespress"),
        ti.xcom_pull(key="bronze_path_bbc"),
        ti.xcom_pull(key="bronze_path_aljazeera"),
    ]
    paths = [p for p in paths if p]

    results = run_quality_checks(paths)
    failed = [r for r in results if not r["passed"]]

    if failed:
        raise ValueError(f"Quality checks failed: {failed}")

    return f"All quality checks passed for {len(paths)} files"


def send_pipeline_report(**context):
    """Génère et envoie un rapport d'exécution."""
    ti = context["ti"]
    run_id = context["run_id"]
    execution_date = context["execution_date"]

    report = {
        "run_id":         run_id,
        "execution_date": str(execution_date),
        "status":         "SUCCESS",
        "sources": {
            "hespress":  ti.xcom_pull(key="bronze_path_hespress"),
            "bbc_news":  ti.xcom_pull(key="bronze_path_bbc"),
            "aljazeera": ti.xcom_pull(key="bronze_path_aljazeera"),
        },
    }

    import json
    print(f"Pipeline Report:\n{json.dumps(report, indent=2)}")
    return report


# ──────────────────────────────────────────────
# Tâches
# ──────────────────────────────────────────────

with dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    # ── Scraping (parallèle) ──────────────────
    scrape_hespress_task = PythonOperator(
        task_id="scrape_hespress",
        python_callable=run_hespress_scraper,
        provide_context=True,
    )

    scrape_bbc_task = PythonOperator(
        task_id="scrape_bbc",
        python_callable=run_bbc_scraper,
        provide_context=True,
    )

    scrape_aljazeera_task = PythonOperator(
        task_id="scrape_aljazeera",
        python_callable=run_aljazeera_scraper,
        provide_context=True,
    )

    # ── Qualité Bronze ────────────────────────
    quality_bronze = PythonOperator(
        task_id="quality_check_bronze",
        python_callable=check_data_quality,
        provide_context=True,
    )

    # ── Bronze → Silver (Spark) ───────────────
    silver_transform = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            "spark-submit "
            "--master spark://spark-master:7077 "
            "--conf spark.executor.memory=2g "
            "/opt/airflow/medallion/silver/silver_layer.py"
        ),
    )

    # ── Silver → Gold (Spark) ─────────────────
    gold_transform = BashOperator(
        task_id="silver_to_gold",
        bash_command=(
            "spark-submit "
            "--master spark://spark-master:7077 "
            "--conf spark.executor.memory=2g "
            "/opt/airflow/medallion/gold/gold_layer.py"
        ),
    )

    # ── Rapport ───────────────────────────────
    report = PythonOperator(
        task_id="send_pipeline_report",
        python_callable=send_pipeline_report,
        provide_context=True,
    )

    # ── Dépendances ───────────────────────────
    start >> [scrape_hespress_task, scrape_bbc_task, scrape_aljazeera_task]
    [scrape_hespress_task, scrape_bbc_task, scrape_aljazeera_task] >> quality_bronze
    quality_bronze >> silver_transform >> gold_transform >> report >> end
