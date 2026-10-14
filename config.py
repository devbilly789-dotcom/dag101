"""Central place for paths and pipeline settings."""
import os

DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data")

BRONZE = f"{DATA_DIR}/bronze"            # raw JSON lines, one file per region per day
SILVER = f"{DATA_DIR}/silver/orders"     # cleaned Parquet, partitioned by ingest_date
GOLD = f"{DATA_DIR}/gold/daily_revenue"  # aggregated Parquet
REJECTED = f"{DATA_DIR}/rejected"        # rows that failed validation
QUARANTINE = f"{DATA_DIR}/quarantine"    # silver partitions pulled out after a failed quality gate
METRICS = f"{DATA_DIR}/metrics"          # per-run counts written by the silver job
REPORTS = f"{DATA_DIR}/reports"

# Dataset URI that links the pipeline DAG to the reporting DAG
GOLD_DATASET_URI = "file:///opt/airflow/data/gold/daily_revenue"

REGIONS = ["nairobi", "mombasa", "kisumu", "eldoret"]

# If more than this share of a day's rows are rejected, the gold step is skipped
BAD_ROW_THRESHOLD = 0.10
