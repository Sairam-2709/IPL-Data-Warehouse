# config/config.py
# -----------------------------------------------
# GCP Configuration for IPL Data Warehouse Project
# -----------------------------------------------

# GCP Project
PROJECT_ID = "your-gcp-project-id"

# Google Cloud Storage
GCS_BUCKET_NAME = "your-bucket-name"
GCS_RAW_PREFIX = "ipl/raw/"
GCS_PROCESSED_PREFIX = "ipl/processed/"

# BigQuery
BQ_DATASET = "ipl_data_warehouse"
BQ_LOCATION = "US"

# BigQuery Table Names
BQ_FACT_DELIVERIES = f"{PROJECT_ID}.{BQ_DATASET}.fact_deliveries"
BQ_DIM_MATCHES     = f"{PROJECT_ID}.{BQ_DATASET}.dim_matches"
BQ_DIM_PLAYERS     = f"{PROJECT_ID}.{BQ_DATASET}.dim_players"
BQ_DIM_TEAMS       = f"{PROJECT_ID}.{BQ_DATASET}.dim_teams"
BQ_DIM_SEASONS     = f"{PROJECT_ID}.{BQ_DATASET}.dim_seasons"

# Local data paths (for development/testing)
RAW_DATA_PATH = "data/raw/"
PROCESSED_DATA_PATH = "data/processed/"

# Source files
MATCHES_FILE = "matches.csv"
DELIVERIES_FILE = "deliveries.csv"
