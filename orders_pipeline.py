"""Orders pipeline: synthetic ingestion -> PySpark silver -> quality gate -> gold.

generate (one mapped task per region) -> silver_clean -> quality_gate
    -> gold_aggregate (updates the gold Dataset)   |   quarantine
"""
from datetime import datetime, timedelta

from airflow.datasets import Dataset
from airflow.decorators import dag, task, task_group
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from src.config import BAD_ROW_THRESHOLD, BRONZE, GOLD_DATASET_URI, REGIONS
from src.generator import write_orders
from src.quality import bad_row_ratio, quarantine_partition

GOLD = Dataset(GOLD_DATASET_URI)


@dag(
    dag_id="orders_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 10, 5),
    catchup=True,          # backfills every day since start_date when unpaused
    max_active_runs=1,     # gold is rebuilt from all silver data, so runs go in date order
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["portfolio", "pyspark", "medallion"],
    doc_md=__doc__,
)
def orders_pipeline():

    @task_group(group_id="ingest")
    def ingest():
        @task
        def generate(region: str, ds=None) -> str:
            """Bronze: one raw JSON-lines file per region per day."""
            return write_orders(region, ds, out=BRONZE)

        generate.expand(region=REGIONS)  # dynamic task mapping: runs in parallel

    silver = BashOperator(
        task_id="silver_clean",
        bash_command="spark-submit --master local[*] --driver-memory 1g "
                     "/opt/airflow/src/silver.py --date {{ ds }}",
    )

    @task.branch
    def quality_gate(ds=None) -> str:
        ratio = bad_row_ratio(ds)
        print(f"rejected ratio for {ds}: {ratio:.1%} (threshold {BAD_ROW_THRESHOLD:.0%})")
        return "gold_aggregate" if ratio <= BAD_ROW_THRESHOLD else "quarantine"

    gold = BashOperator(
        task_id="gold_aggregate",
        bash_command="spark-submit --master local[*] --driver-memory 1g "
                     "/opt/airflow/src/gold.py --date {{ ds }}",
        outlets=[GOLD],
    )

    @task
    def quarantine(ds=None) -> str:
        """Pull the bad day out of silver so it can't reach gold."""
        return quarantine_partition(ds)

    done = EmptyOperator(task_id="done", trigger_rule="none_failed_min_one_success")

    ingest() >> silver >> quality_gate() >> [gold, quarantine()] >> done


orders_pipeline()
