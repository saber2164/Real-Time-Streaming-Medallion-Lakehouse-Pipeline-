"""
tests/verify_bronze_offsets.py

Assert the Bronze table captured every Kafka offset exactly once per partition
(no gaps, no repeats) — the evidence that the append-only landing layer is
complete and that checkpoint recovery did not skip or double-consume.

Checks (per partition)
----------------------
1. max(offset) - min(offset) + 1 == count(*)       (no gaps)
2. count(*) == count(distinct offset)              (no repeats)

How to run (inside the Spark container)
---------------------------------------
    docker-compose run --rm spark spark-submit \
        --packages io.delta:delta-spark_2.12:3.2.0 \
        /opt/spark/tests/verify_bronze_offsets.py
"""

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
for candidate in (os.path.join(HERE, "..", "spark"), "/opt/spark/app"):
    if os.path.exists(os.path.join(candidate, "config.py")):
        sys.path.insert(0, candidate)
        break

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.functions import (  # noqa: E402
    col,
    count,
    countDistinct,
    min as _min,
    max as _max,
)
from config import BRONZE_PATH  # noqa: E402


def build_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def main() -> int:
    spark = build_spark_session("verify-bronze-offsets")
    spark.sparkContext.setLogLevel("ERROR")

    bronze = spark.read.format("delta").load(BRONZE_PATH)
    stats = (
        bronze.groupBy("kafka_partition")
        .agg(
            _min("kafka_offset").alias("min_off"),
            _max("kafka_offset").alias("max_off"),
            count("*").alias("rows"),
            countDistinct("kafka_offset").alias("distinct_off"),
        )
        .orderBy("kafka_partition")
        .collect()
    )

    print("partition | min   | max   | rows | distinct | contiguous")
    failures = []
    for r in stats:
        expected = r["max_off"] - r["min_off"] + 1
        contiguous = expected == r["rows"] and r["rows"] == r["distinct_off"]
        print(
            f"{r['kafka_partition']:^9} | {r['min_off']:<5} | {r['max_off']:<5} "
            f"| {r['rows']:<4} | {r['distinct_off']:<8} | {contiguous}"
        )
        if r["rows"] != r["distinct_off"]:
            failures.append(
                f"partition {r['kafka_partition']}: {r['rows']} rows but only "
                f"{r['distinct_off']} distinct offsets (repeats)"
            )
        if expected != r["rows"]:
            failures.append(
                f"partition {r['kafka_partition']}: offsets not contiguous — "
                f"expected {expected} rows for [{r['min_off']}..{r['max_off']}] "
                f"but found {r['rows']} (gap)"
            )

    spark.stop()

    if failures:
        print("\nFAIL verify_bronze_offsets:")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("\nPASS verify_bronze_offsets: contiguous offsets, no gaps or repeats")
    return 0


if __name__ == "__main__":
    sys.exit(main())
