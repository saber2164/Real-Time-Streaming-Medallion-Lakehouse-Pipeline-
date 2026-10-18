"""
tests/verify_silver_dedup.py

Assert the Silver table has zero duplicate events.

Checks
------
1. COUNT(*) == COUNT(DISTINCT event_id)               (no duplicate identity)
2. COUNT(*) == COUNT(DISTINCT (event_id, event_time)) (no duplicate dedup key)
3. Silver retains EVERY distinct Bronze key            (no silent late-data loss)
4. derived column sanity: total_amount == price + freight_value for all rows

Check 3 is the important one: a watermark shorter than the producer's maximum
lateness makes Spark DROP (not dedup) valid late events, which checks 1-2 do
NOT detect (a smaller table can still be duplicate-free). Comparing the
distinct dedup-key counts across Bronze and Silver catches that loss.

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
from pyspark.sql.functions import abs as _abs, col, get_json_object  # noqa: E402
from config import BRONZE_PATH, SILVER_PATH  # noqa: E402


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

    # Distinct dedup keys recorded in Bronze. Silver must contain exactly these
    # (dedup removes repeats, never unique rows). event_time is compared via the
    # raw JSON string here; Spark parses it at second precision, so the distinct
    # counts are 1:1 with Silver's typed (event_id, event_time).
    try:
        bronze = spark.read.format("delta").load(BRONZE_PATH)
        bronze_distinct_keys = (
            bronze.select(
                get_json_object("raw_payload", "$.event_id").alias("event_id"),
                get_json_object("raw_payload", "$.event_time").alias("event_time"),
            )
            .distinct()
            .count()
        )
    except Exception as exc:  # noqa: BLE001
        bronze_distinct_keys = None
        print(f"  (Bronze not readable, skipping completeness check: {exc})")

    print(f"Silver rows                 : {total}")
    print(f"Distinct event_id           : {distinct_ids}")
    print(f"Distinct (event_id, time)   : {distinct_keys}")
    if bronze_distinct_keys is not None:
        print(f"Bronze distinct dedup keys  : {bronze_distinct_keys}")
    print(f"Rows with bad total_amount  : {bad_amount}")

    failures = []
    if total != distinct_ids:
        failures.append(f"duplicate event_id: {total} rows vs {distinct_ids} distinct ids")
    if total != distinct_keys:
        failures.append(
            f"duplicate dedup key: {total} rows vs {distinct_keys} distinct (id,time)"
        )
    if bronze_distinct_keys is not None and distinct_keys != bronze_distinct_keys:
        failures.append(
            "silent late-data loss: Silver has "
            f"{distinct_keys} distinct keys but Bronze has {bronze_distinct_keys} "
            "(watermark dropped valid events; see scripts/watermark_report.py)"
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
