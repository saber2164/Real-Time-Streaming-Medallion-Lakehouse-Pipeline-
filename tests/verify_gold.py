"""
tests/verify_gold.py

Independent verification that the Gold aggregate matches a fresh recomputation
from Silver.

What it asserts
---------------
1. Gold row count == the number of distinct (hour_start, seller_id,
   payment_type) groups in Silver filtered to `order_created`.
2. The grand totals of `hourly_order_count` and `total_revenue` in Gold equal
   the independently recomputed totals from Silver (to floating-point tolerance).
3. There are no duplicate grain keys in Gold.

This is deliberately computed with plain SQL over Silver (not by reusing any
Gold code path), so it is a genuine cross-check.

How to run (inside the Spark container)
---------------------------------------
    docker-compose run --rm spark spark-submit \
        --packages io.delta:delta-spark_2.12:3.2.0 \
        /opt/spark/tests/verify_gold.py

Exit code 0 = PASS, 1 = FAIL.
"""

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
for candidate in (os.path.join(HERE, "..", "spark"), "/opt/spark/app"):
    if os.path.exists(os.path.join(candidate, "config.py")):
        sys.path.insert(0, candidate)
        break

from pyspark.sql import SparkSession  # noqa: E402
from config import SILVER_PATH, GOLD_PATH  # noqa: E402


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("verify-gold")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def main() -> int:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    failures = []

    # --- Independently recompute the Gold aggregate from Silver -------------
    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW silver_v AS
        SELECT * FROM delta.`{SILVER_PATH}`
        """
    )
    independent = spark.sql(
        """
        SELECT
            date_trunc('hour', event_time) AS hour_start,
            seller_id,
            payment_type,
            COUNT(*)                       AS hourly_order_count,
            SUM(price)                     AS total_revenue
        FROM silver_v
        WHERE event_type = 'order_created'
        GROUP BY 1, 2, 3
        """
    )
    independent.createOrReplaceTempView("independent")

    gold = spark.read.format("delta").load(GOLD_PATH)
    gold.createOrReplaceTempView("gold")

    ind_rows = independent.count()
    gold_rows = gold.count()
    print(f"[verify_gold] independent groups = {ind_rows}")
    print(f"[verify_gold] gold rows          = {gold_rows}")
    if ind_rows != gold_rows:
        failures.append(f"row count mismatch: independent={ind_rows} gold={gold_rows}")

    # --- Duplicate grain key check -----------------------------------------
    dup = spark.sql(
        """
        SELECT COUNT(*) AS c FROM (
            SELECT hour_start, seller_id, payment_type
            FROM gold
            GROUP BY 1, 2, 3
            HAVING COUNT(*) > 1
        )
        """
    ).collect()[0]["c"]
    print(f"[verify_gold] duplicate grain keys = {dup}")
    if dup != 0:
        failures.append(f"gold has {dup} duplicate grain keys")

    # --- Grand-total cross-check -------------------------------------------
    tot = spark.sql(
        """
        SELECT
            (SELECT SUM(hourly_order_count) FROM gold)       AS gold_count,
            (SELECT SUM(hourly_order_count) FROM independent) AS ind_count,
            (SELECT ROUND(SUM(total_revenue), 6) FROM gold)        AS gold_rev,
            (SELECT ROUND(SUM(total_revenue), 6) FROM independent) AS ind_rev
        """
    ).collect()[0]
    print(f"[verify_gold] order_count  gold={tot['gold_count']} independent={tot['ind_count']}")
    print(f"[verify_gold] total_revenue gold={tot['gold_rev']} independent={tot['ind_rev']}")
    if tot["gold_count"] != tot["ind_count"]:
        failures.append(
            f"order_count mismatch: gold={tot['gold_count']} independent={tot['ind_count']}"
        )
    if tot["gold_rev"] != tot["ind_rev"]:
        failures.append(
            f"revenue mismatch: gold={tot['gold_rev']} independent={tot['ind_rev']}"
        )

    # --- Anti-join: any grain key in independent missing/changed in gold ----
    mismatched = spark.sql(
        """
        SELECT COUNT(*) AS c FROM (
            SELECT i.*, g.hourly_order_count AS g_count, g.total_revenue AS g_rev
            FROM independent i
            LEFT JOIN gold g
              ON i.hour_start = g.hour_start
             AND i.seller_id  = g.seller_id
             AND i.payment_type = g.payment_type
        )
        WHERE g_count IS NULL
           OR g_count != hourly_order_count
           OR ABS(g_rev - total_revenue) > 0.000001
        """
    ).collect()[0]["c"]
    print(f"[verify_gold] mismatched groups = {mismatched}")
    if mismatched != 0:
        failures.append(f"{mismatched} groups differ between gold and independent aggregation")

    spark.stop()

    if failures:
        print("\n[verify_gold] FAIL")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("\n[verify_gold] PASS: gold matches independent recomputation from silver")
    return 0


if __name__ == "__main__":
    sys.exit(main())
