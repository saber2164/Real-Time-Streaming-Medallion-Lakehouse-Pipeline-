"""
tests/verify_silver_dedup.py

Assert the Silver table has zero duplicate events.

Checks
------
1. COUNT(*) == COUNT(DISTINCT event_id)               (no duplicate identity)
2. COUNT(*) == COUNT(DISTINCT (event_id, event_time)) (no duplicate dedup key)
3. derived column sanity: total_amount == price + freight_value for all rows

Prints the real counts (feeds PROJECT_MEMORY.md Measured Results) and exits
non-zero if any assertion fails.

How to run (inside the Spark container)
---------------------------------------
    docker-compose run --rm spark spark-submit \
        --packages io.delta:delta-spark_2.12:3.2.0 \
        /opt/spark/tests/verify_silver_dedup.py
"""

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
for candidate in (os.path.join(HERE, "..", "spark"), "/opt/spark/app"):
    if os.path.exists(os.path.join(candidate, "config.py")):
        sys.path.insert(0, candidate)
        break

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.functions import abs as _abs, col  # noqa: E402
from config import SILVER_PATH  # noqa: E402


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
    spark = build_spark_session("verify-silver-dedup")
    spark.sparkContext.setLogLevel("ERROR")

    silver = spark.read.format("delta").load(SILVER_PATH)
    total = silver.count()
    distinct_ids = silver.select("event_id").distinct().count()
    distinct_keys = silver.select("event_id", "event_time").distinct().count()
    bad_amount = silver.filter(
        _abs(col("total_amount") - (col("price") + col("freight_value"))) > 1e-6
    ).count()

    print(f"Silver rows                 : {total}")
    print(f"Distinct event_id           : {distinct_ids}")
    print(f"Distinct (event_id, time)   : {distinct_keys}")
    print(f"Rows with bad total_amount  : {bad_amount}")

    failures = []
    if total != distinct_ids:
        failures.append(f"duplicate event_id: {total} rows vs {distinct_ids} distinct ids")
    if total != distinct_keys:
        failures.append(
            f"duplicate dedup key: {total} rows vs {distinct_keys} distinct (id,time)"
        )
    if bad_amount != 0:
        failures.append(f"{bad_amount} rows have total_amount != price + freight_value")

    spark.stop()

    if failures:
        print("\nFAIL verify_silver_dedup:")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("\nPASS verify_silver_dedup: zero duplicate event_id; total_amount correct")
    return 0


if __name__ == "__main__":
    sys.exit(main())
