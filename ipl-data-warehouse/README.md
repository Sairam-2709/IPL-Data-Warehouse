# 🏏 IPL Cricket Stats Data Warehouse — GCP | PySpark | Airflow | BigQuery | GCS

A production-style, end-to-end **Data Warehouse** built on **Google Cloud Platform** to process, transform, and analyze **90,000+ ball-by-ball IPL cricket records** spanning **10+ seasons**.

---

## 📌 Project Overview

This project implements a complete **ELT pipeline** on GCP that:
- Ingests raw IPL match and delivery datasets from **Kaggle**
- Transforms and enriches data using **PySpark**
- Loads structured data into a **Star Schema** in **BigQuery**
- Automates the entire workflow using **Apache Airflow DAGs**
- Optimizes query performance via **partitioning and clustering**

---

## 🏗️ Architecture

```
Kaggle CSV Data
      │
      ▼
Google Cloud Storage (GCS)   ← Raw Zone
      │
      ▼
PySpark Transformation        ← Compute Layer
      │
      ▼
Google Cloud Storage (GCS)   ← Processed Zone
      │
      ▼
BigQuery (Star Schema)        ← Data Warehouse
      │
      ▼
SQL Analytics & Reporting
```

---

## 🗂️ Project Structure

```
ipl-data-warehouse/
│
├── dags/
│   └── ipl_pipeline_dag.py          # Airflow DAG — orchestrates full ELT pipeline
│
├── scripts/
│   ├── ingest.py                    # Uploads raw CSVs to GCS
│   ├── transform.py                 # PySpark transformations & derived metrics
│   └── load_to_bigquery.py          # Loads processed data into BigQuery
│
├── sql/
│   ├── create_tables.sql            # BigQuery schema — fact & dimension tables
│   └── analytics_queries.sql        # Sample analytical SQL queries
│
├── config/
│   └── config.py                    # GCP project, bucket, dataset configurations
│
├── data/
│   ├── raw/                         # Raw CSVs (matches.csv, deliveries.csv)
│   └── processed/                   # Parquet output from PySpark
│
├── docs/
│   └── schema_diagram.md            # Star schema description
│
├── requirements.txt                 # Python dependencies
└── README.md
```

---

## ⭐ Star Schema Design

| Table | Type | Description |
|---|---|---|
| `fact_deliveries` | Fact | Ball-by-ball delivery records |
| `dim_matches` | Dimension | Match metadata (venue, date, teams) |
| `dim_players` | Dimension | Player details |
| `dim_teams` | Dimension | Team information |
| `dim_seasons` | Dimension | Season/year details |

---

## 🔧 Tech Stack

| Layer | Technology |
|---|---|
| Cloud Platform | Google Cloud Platform (GCP) |
| Storage | Google Cloud Storage (GCS) |
| Processing | PySpark (Dataproc) |
| Warehouse | BigQuery |
| Orchestration | Apache Airflow |
| Language | Python, SQL |
| Data Format | CSV (raw), Parquet (processed) |

---

## 📊 Key Metrics Generated

- **Batting:** Runs, average, strike rate, boundaries, centuries, half-centuries
- **Bowling:** Wickets, economy rate, bowling average, dot ball %
- **Match:** Win/loss records, toss impact, venue performance
- **Season:** Top run-scorers, top wicket-takers per season

---

## ⚡ BigQuery Optimizations

- **Partitioning** by `season_year` — reduces data scanned per query
- **Clustering** by `team_name` — improves filter performance
- Result: **~35% reduction** in query costs and scan time

---

## 🚀 How to Run

### 1. Prerequisites
```bash
pip install -r requirements.txt
```

### 2. Configure GCP credentials
```bash
export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account.json"
```

### 3. Update config
Edit `config/config.py` with your GCP project ID, bucket name, and dataset.

### 4. Run manually (step by step)
```bash
python scripts/ingest.py
python scripts/transform.py
python scripts/load_to_bigquery.py
```

### 5. Run via Airflow
- Place `dags/ipl_pipeline_dag.py` in your Airflow DAGs folder
- Trigger the DAG `ipl_elt_pipeline` from the Airflow UI

---

## 📁 Dataset

- Source: [Kaggle — IPL Complete Dataset](https://www.kaggle.com/datasets/patrickb1912/ipl-complete-dataset-20082020)
- Files: `matches.csv`, `deliveries.csv`
- Records: 90,000+ ball-by-ball delivery records across 10+ seasons

---

## 📜 License

This project is for educational and portfolio purposes.
