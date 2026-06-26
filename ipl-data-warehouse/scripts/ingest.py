# scripts/ingest.py
# -----------------------------------------------
# Ingests raw IPL CSV files into Google Cloud Storage (GCS)
# -----------------------------------------------

import os
import logging
from google.cloud import storage
from config.config import (
    GCS_BUCKET_NAME,
    GCS_RAW_PREFIX,
    RAW_DATA_PATH,
    MATCHES_FILE,
    DELIVERIES_FILE,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def upload_file_to_gcs(bucket_name: str, source_path: str, destination_blob: str) -> None:
    """Uploads a local file to a GCS bucket."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)

    blob.upload_from_filename(source_path)
    logger.info(f"Uploaded: {source_path} → gs://{bucket_name}/{destination_blob}")


def ingest_raw_files() -> None:
    """Uploads all raw IPL CSV files to GCS raw zone."""
    files = [MATCHES_FILE, DELIVERIES_FILE]

    for file_name in files:
        local_path = os.path.join(RAW_DATA_PATH, file_name)
        gcs_path = GCS_RAW_PREFIX + file_name

        if not os.path.exists(local_path):
            logger.error(f"File not found: {local_path}")
            raise FileNotFoundError(f"Raw file missing: {local_path}")

        upload_file_to_gcs(GCS_BUCKET_NAME, local_path, gcs_path)

    logger.info("All raw files successfully ingested to GCS.")


if __name__ == "__main__":
    ingest_raw_files()
