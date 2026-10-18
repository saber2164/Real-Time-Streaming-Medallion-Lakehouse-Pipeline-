"""
scripts/inspect_delta.py

Inspect the Medallion Delta tables: row counts, file counts/sizes, schema, and
the last few transaction-log entries (DESCRIBE HISTORY).

Purpose
-------
A single read-only command to answer "what is actually in the lakehouse right
now?" for the docs, the web page, and the Measured Results section. It also
produces the small-file before/after numbers used to justify OPTIMIZE.

How to run (inside the Spark container)
---------------------------------------
    docker-compose run --rm spark spark-submit \
        --packages io.delta:delta-spark_2.12:3.2.0 \
        /opt/spark/scripts/inspect_delta.py
"""

import os
import sys

# --- locate config.py (mounted at /opt/spark/app in the container) -----------
HERE = os.path.dirname(os.path.abspath(__file__))
for candidate in (os.path.join(HERE, "..", "spark"), "/opt/spark/app"):
    if os.path.exists(os.path.join(candidate, "config.py")):
        sys.path.insert(0, candidate)
        break

from pyspark.sql import SparkSession  # noqa: E402
from config import BRONZE_PATH, SILVER_PATH, GOLD_PATH  # noqa: E402


def build_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def is_delta_table(path: str) -> bool:
    return os.path.exists(os.path.join(path, "_delta_log"))


def file_stats(path: str):
    """Return (file_count, total_bytes, avg_bytes) of parquet data files."""
    files = []
    for root, _dirs, names in os.walk(path):
        if "_delta_log" in root:
            continue
        for name in names:
            if name.endswith(".parquet"):
                files.append(os.path.join(root, name))
    total = sum(os.path.getsize(f) for f in files)
    avg = (total / len(files)) if files else 0
    return len(files), total, avg


def report(spark: SparkSession, label: str, path: str) -> None:
    print("\n" + "=" * 72)
    print(f"{label.upper():8s} {path}")
    print("=" * 72)
    if not is_delta_table(path):
        print("  (no Delta table here)")
        return
    df = spark.read.format("delta").load(path)
    print(f"  rows            : {df.count()}")

    # Active files per the Delta transaction log (the number that matters for
    # read cost). Old files stay on disk until VACUUM, so the physical count
    # below can be *higher* after OPTIMIZE -- that is expected and explained in
    # RUNBOOK.md / CONCEPTS_AND_DESIGN_DECISIONS.md.
    detail = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").collect()[0]
    num_files = detail["numFiles"]
    size_bytes = detail["sizeInBytes"]
    avg_active = (size_bytes / num_files) if num_files else 0
    print(f"  active files    : {num_files}   (from _delta_log)")
    print(f"  active bytes    : {size_bytes:,}")
    print(f"  avg active file : {avg_active:,.0f} bytes")

    fc, total, avg = file_stats(path)
    print(f"  physical files  : {fc}   (incl. tombstoned, pre-VACUUM)")
    print(f"  physical bytes  : {total:,}")
    print(f"  avg physical    : {avg:,.0f} bytes")

    print("  schema          :")
    for field in df.schema.fields:
        print(f"      {field.name:16s} {field.dataType}")

    print("  last history (version op metrics):")
    history = spark.sql(f"DESCRIBE HISTORY delta.`{path}`").collect()
    for row in history[:5]:
        metrics = row["operationMetrics"] or {}
        keep = {
            k: metrics[k]
            for k in (
                "numFiles",
                "numOutputRows",
                "numAddedFiles",
                "numRemovedFiles",
            )
            if k in metrics
        }
        print(f"      v{row.version} op={row.operation} ts={row.timestamp} {keep}")


def main():
    spark = build_spark_session("inspect-delta")
    spark.sparkContext.setLogLevel("ERROR")
    report(spark, "bronze", BRONZE_PATH)
    report(spark, "silver", SILVER_PATH)
    report(spark, "gold", GOLD_PATH)
    spark.stop()


if __name__ == "__main__":
    main()
