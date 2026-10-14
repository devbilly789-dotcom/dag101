"""Runs whenever the gold table is refreshed (Dataset-aware scheduling, no cron)."""
from datetime import datetime

from airflow.datasets import Dataset
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

from src.config import GOLD_DATASET_URI


@dag(
    dag_id="revenue_report",
    schedule=[Dataset(GOLD_DATASET_URI)],
    start_date=datetime(2026, 10, 5),
    catchup=False,
    tags=["portfolio", "reporting"],
)
def revenue_report():
    BashOperator(task_id="build_report", bash_command="cd /opt/airflow && python -m src.report")


revenue_report()
