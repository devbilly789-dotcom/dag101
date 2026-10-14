"""Gold job: silver -> daily revenue per region with a 7-row rolling average.

Recomputed from all silver data up to the run date, so late-arriving orders
automatically revise earlier days' revenue. Output is overwritten, which keeps
the job idempotent.
"""
import argparse
import os

from pyspark.sql import SparkSession, Window, functions as F

DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data")
SILVER = f"{DATA_DIR}/silver/orders"
GOLD = f"{DATA_DIR}/gold/daily_revenue"


def main(ds: str) -> None:
    spark = SparkSession.builder.appName(f"gold_{ds}").getOrCreate()

    silver = spark.read.parquet(SILVER).filter(F.col("ingest_date") <= F.lit(ds).cast("date"))

    daily = silver.groupBy("order_date", "region").agg(
        F.round(F.sum("amount"), 2).alias("revenue"),
        F.count("*").alias("orders"),
    ).withColumn("avg_basket", F.round(F.col("revenue") / F.col("orders"), 2))

    w = Window.partitionBy("region").orderBy("order_date").rowsBetween(-6, 0)
    gold = daily.withColumn("rolling_7d_avg_revenue", F.round(F.avg("revenue").over(w), 2))

    gold.write.mode("overwrite").parquet(GOLD)
    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    main(p.parse_args().date)
