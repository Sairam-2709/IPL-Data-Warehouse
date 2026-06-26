# dags/ipl_pipeline_dag.py
# -----------------------------------------------
# Apache Airflow DAG — IPL Cricket Stats ELT Pipeline
# Orchestrates: Ingest → Transform → Load → Validate
# -----------------------------------------------

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

import sys
sys.path.insert(0, "/opt/airflow/project")  # Adjust to your project root

from scripts.ingest          import ingest_raw_files
from scripts.transform       import run_transformations
from scripts.load_to_bigquery import load_all_tables

# ── Default Arguments ───────────────────────────────────────────
default_args = {
    "owner":            "sairam",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

# ── DAG Definition ──────────────────────────────────────────────
with DAG(
    dag_id="ipl_elt_pipeline",
    description="End-to-end ELT pipeline for IPL Cricket Stats Data Warehouse on GCP",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@weekly",   # Run every week
    catchup=False,
    tags=["ipl", "gcp", "pyspark", "bigquery", "data-warehouse"],
) as dag:

    # ── Task: Start ─────────────────────────────────────────────
    start = EmptyOperator(task_id="pipeline_start")

    # ── Task 1: Ingest ──────────────────────────────────────────
    ingest_task = PythonOperator(
        task_id="ingest_raw_to_gcs",
        python_callable=ingest_raw_files,
        doc_md="""
        **Ingest Raw Files**
        Uploads raw IPL CSV files (matches.csv, deliveries.csv)
        from local storage into the GCS raw zone.
        """,
    )

    # ── Task 2: Transform ───────────────────────────────────────
    transform_task = PythonOperator(
        task_id="transform_with_pyspark",
        python_callable=run_transformations,
        doc_md="""
        **PySpark Transformation**
        Reads raw CSVs from GCS, performs:
        - Data cleansing & type casting
        - Deduplication & null handling
        - Derived metrics (strike rate, economy, etc.)
        - Builds star schema tables (fact + dimensions)
        Writes Parquet output to GCS processed zone.
        """,
        execution_timeout=timedelta(minutes=30),
    )

    # ── Task 3: Load ────────────────────────────────────────────
    load_task = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_all_tables,
        doc_md="""
        **Load to BigQuery**
        Loads all Parquet tables from GCS processed zone
        into BigQuery with partitioning and clustering applied.
        """,
        execution_timeout=timedelta(minutes=20),
    )

    # ── Task 4: Validate ────────────────────────────────────────
    def validate_pipeline(**kwargs):
        """Basic row-count validation after load."""
        from google.cloud import bigquery
        from config.config import PROJECT_ID, BQ_DATASET

        client = bigquery.Client(project=PROJECT_ID)
        tables_to_check = [
            "fact_deliveries", "dim_matches", "dim_players",
            "dim_teams", "dim_seasons",
        ]
        for table in tables_to_check:
            query  = f"SELECT COUNT(*) as cnt FROM `{PROJECT_ID}.{BQ_DATASET}.{table}`"
            result = client.query(query).result()
            count  = list(result)[0].cnt
            if count == 0:
                raise ValueError(f"Validation failed: {table} has 0 rows!")
            print(f"✅ {table}: {count:,} rows")

    validate_task = PythonOperator(
        task_id="validate_pipeline",
        python_callable=validate_pipeline,
        doc_md="""
        **Validation**
        Checks row counts for all BigQuery tables.
        Raises an error if any table is empty.
        """,
    )

    # ── Task: End ───────────────────────────────────────────────
    end = EmptyOperator(task_id="pipeline_complete")

    # ── Task Dependencies ───────────────────────────────────────
    start >> ingest_task >> transform_task >> load_task >> validate_task >> end
