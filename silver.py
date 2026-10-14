"""Silver job: bronze JSON -> cleaned, deduplicated, typed Parquet.

Usage: spark-submit silver.py --date 2026-10-05
Writes one partition (ingest_date=<date>) using dynamic partition overwrite,
so re-running a day replaces that day only.
"""
import argparse
import json
import os
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data")
BRONZE = f"{DATA_DIR}/bronze"
SILVER = f"{DATA_DIR}/silver/orders"
REJECTED = f"{DATA_DIR}/rejected"
METRICS = f"{DATA_DIR}/metrics"

# Explicit schema (no inference). Both `amount` and `total_amount` are listed
# so schema drift from one region does not break the job.
SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("region", StringType()),
    StructField("product", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("amount", DoubleType()),
    StructField("total_amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("status", StringType()),
    StructField("order_ts", StringType()),
])


def main(ds: str) -> None:
    spark = SparkSession.builder.appName(f"silver_{ds}").getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    raw = (
        spark.read.schema(SCHEMA).json(f"{BRONZE}/dt={ds}/*.jsonl")
        .withColumn("amount", F.coalesce("amount", "total_amount"))
        .drop("total_amount")
        .withColumn("order_ts", F.to_timestamp("order_ts"))
        .cache()
    )

    invalid = (
        F.col("order_id").isNull()
        | F.col("amount").isNull()
        | (F.col("amount") <= 0)
        | F.col("order_ts").isNull()
    )
    rejected = raw.filter(invalid).withColumn(
        "reason",
        F.when(F.col("order_id").isNull(), "null_order_id")
        .when(F.col("amount").isNull() | (F.col("amount") <= 0), "bad_amount")
        .otherwise("bad_timestamp"),
    )
    valid = raw.filter(~invalid)
    silver = (
        valid.dropDuplicates(["order_id"])
        .withColumn("order_date", F.to_date("order_ts"))
        .withColumn("ingest_date", F.lit(ds).cast("date"))
    )

    rejected.write.mode("overwrite").json(f"{REJECTED}/dt={ds}")
    silver.write.mode("overwrite").partitionBy("ingest_date").parquet(SILVER)

    total, n_rejected = raw.count(), rejected.count()
    n_valid, n_clean = valid.count(), silver.count()
    metrics = {
        "date": ds,
        "total_rows": total,
        "rejected_rows": n_rejected,
        "duplicate_rows": n_valid - n_clean,
        "clean_rows": n_clean,
    }
    Path(METRICS).mkdir(parents=True, exist_ok=True)
    Path(f"{METRICS}/dt={ds}.json").write_text(json.dumps(metrics))
    print("silver metrics:", metrics)
    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    main(p.parse_args().date)
