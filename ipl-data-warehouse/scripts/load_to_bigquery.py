# scripts/load_to_bigquery.py
# -----------------------------------------------
# Loads processed Parquet files from GCS into BigQuery
# Applies partitioning and clustering for query optimization
# -----------------------------------------------

import logging
from google.cloud import bigquery
from config.config import (
    PROJECT_ID,
    BQ_DATASET,
    BQ_LOCATION,
    GCS_BUCKET_NAME,
    GCS_PROCESSED_PREFIX,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

client = bigquery.Client(project=PROJECT_ID)

PROCESSED_BASE = f"gs://{GCS_BUCKET_NAME}/{GCS_PROCESSED_PREFIX}"

# ── Table Load Configurations ───────────────────────────────────
# Each entry: (table_name, partition_field, cluster_fields)
TABLE_CONFIGS = [
    ("dim_matches",     "match_date",  ["season_year", "match_winner"]),
    ("dim_teams",       None,          ["team_name"]),
    ("dim_players",     None,          ["player_name"]),
    ("dim_seasons",     None,          ["season_year"]),
    ("fact_deliveries", None,          ["batting_team", "bowling_team"]),
    ("batting_stats",   None,          ["batsman", "season"]),
    ("bowling_stats",   None,          ["bowler", "season"]),
]


def ensure_dataset_exists() -> None:
    """Creates the BigQuery dataset if it doesn't exist."""
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{BQ_DATASET}")
    dataset_ref.location = BQ_LOCATION
    try:
        client.get_dataset(dataset_ref)
        logger.info(f"Dataset already exists: {BQ_DATASET}")
    except Exception:
        client.create_dataset(dataset_ref, exists_ok=True)
        logger.info(f"Created dataset: {BQ_DATASET}")


def load_table(
    table_name: str,
    partition_field: str | None,
    cluster_fields: list[str] | None,
) -> None:
    """Loads a Parquet file from GCS into a BigQuery table."""
    gcs_uri   = f"{PROCESSED_BASE}{table_name}/*.parquet"
    table_ref = f"{PROJECT_ID}.{BQ_DATASET}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )

    if partition_field:
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field,
        )
        logger.info(f"Partitioning {table_name} by '{partition_field}'")

    if cluster_fields:
        job_config.clustering_fields = cluster_fields
        logger.info(f"Clustering {table_name} by {cluster_fields}")

    load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    load_job.result()  # Wait for job to complete

    table = client.get_table(table_ref)
    logger.info(f"Loaded {table.num_rows:,} rows into {table_ref}")


def load_all_tables() -> None:
    ensure_dataset_exists()
    for table_name, partition_field, cluster_fields in TABLE_CONFIGS:
        logger.info(f"Loading: {table_name}")
        load_table(table_name, partition_field, cluster_fields)
    logger.info("All tables loaded into BigQuery successfully.")


if __name__ == "__main__":
    load_all_tables()
