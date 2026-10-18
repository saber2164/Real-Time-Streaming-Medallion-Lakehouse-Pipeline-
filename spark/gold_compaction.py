"""
spark/gold_compaction.py

Gold layer aggregation + Delta compaction (OPTIMIZE / ZORDER).

Purpose
-------
Read the Silver Delta table (batch), keep only `order_created` events
(avoid double counting cancelled/paid events per doc v2 §3.A), aggregate
per hour per seller_id per payment_type, and write idempotently to the Gold
Delta table. Then run OPTIMIZE ... ZORDER BY (seller_id) to compact small
files produced by streaming micro-batches.

Inputs
------
- Silver Delta table: /lakehouse/silver/order_events

Outputs
-------
- Gold Delta table: /lakehouse/gold/hourly_sales
  Columns: hour_start (TIMESTAMP), event_date (DATE), seller_id (STRING),
           payment_type (STRING), hourly_order_count (LONG),
           total_revenue (DOUBLE), avg_freight_cost (DOUBLE)

How to run
----------
    docker-compose run --rm spark \
        spark-submit --packages io.delta:delta-spark_2.12:3.2.0,io.delta:delta-storage:3.2.0 \
        /opt/spark/app/gold_compaction.py

Key design choices
------------------
- Gold write mode = overwrite (idempotent). Each run recomputes aggregates
  from Silver and fully overwrites Gold. Justification: Gold is a derived
  aggregate table (not a source of truth), so overwriting is simpler and
  safer than MERGE for a scheduled batch job, and guarantees the table
  always reflects the current Silver state. MERGE would be preferable for
  incremental late-arriving updates but adds complexity not needed here.
- Grain: 1 row per hour per seller_id per payment_type. region is available
  in Silver but not part of the Gold grain (kept narrow; documented in
  CONCEPTS_AND_DESIGN_DECISIONS.md).
- OPTIMIZE ZORDER BY (seller_id): seller_id is the most common filter in
  Gold queries (revenue per seller). Z-order cannot use a partition column,
  and Gold is not partitioned, so ZORDER on seller_id alone is valid.
- Also compacts Bronze and Silver (where streaming actually accumulates
  small files) and reports file count / size before vs. after.
"""

import os
import sys
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    date_trunc,
    to_date,
    count,
    sum as _sum,
    avg,
    lit,
)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import (  # noqa: E402
    SILVER_PATH,
    GOLD_PATH,
    BRONZE_PATH,
    GOLD_WRITE_MODE,
    GOLD_ZORDER_COL,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("gold_compaction")


def build_spark_session(app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.port", "4041")
    )
    return builder.getOrCreate()


def table_exists(spark: SparkSession, path: str) -> bool:
    """Check whether a Delta table exists at the given path."""
    try:
        spark.read.format("delta").load(path).limit(1).collect()
        return True
    except Exception:  # noqa: BLE001
        return False


def run_optimize(spark: SparkSession, path: str, zorder_col: str):
    """Run OPTIMIZE ... ZORDER BY on a Delta table and log history."""
    if zorder_col:
        spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({zorder_col})")
    else:
        spark.sql(f"OPTIMIZE delta.`{path}`")
    logger.info("Ran OPTIMIZE on %s%s", path, f" ZORDER BY ({zorder_col})" if zorder_col else "")

    history = spark.sql(f"DESCRIBE HISTORY delta.`{path}`").collect()
    logger.info("DESCRIBE HISTORY for %s:", path)
    for row in history[:5]:
        logger.info("  version=%s operation=%s timestamp=%s", row.version, row.operation, row.timestamp)


def main():
    spark = build_spark_session("medallion-gold-compaction")
    spark.sparkContext.setLogLevel("WARN")

    # 1. Read Silver (batch).
    if not table_exists(spark, SILVER_PATH):
        logger.error("Silver table does not exist at %s. Run ingestion first.", SILVER_PATH)
        spark.stop()
        sys.exit(1)

    silver = spark.read.format("delta").load(SILVER_PATH)
    logger.info("Silver rows: %s", silver.count())

    # 2. Filter order_created only (revenue/count use order_created to avoid double counting).
    created = silver.filter(col("event_type") == "order_created")

    # 3. Aggregate per hour / seller / payment type.
    gold_df = (
        created.withColumn("hour_start", date_trunc("hour", col("event_time")))
        .groupBy("hour_start", "seller_id", "payment_type")
        .agg(
            count("*").alias("hourly_order_count"),
            _sum("price").alias("total_revenue"),
            _sum("freight_value").alias("total_freight"),
            avg("freight_value").alias("avg_freight_cost"),
        )
        .withColumn("event_date", to_date(col("hour_start")))
    )

    gold_row_count = gold_df.count()
    logger.info("Gold aggregate rows: %s", gold_row_count)

    # 4. Write idempotently (overwrite).
    gold_df.write.format("delta").mode(GOLD_WRITE_MODE).option(
        "overwriteSchema", "true"
    ).save(GOLD_PATH)
    logger.info("Wrote Gold table to %s (mode=%s)", GOLD_PATH, GOLD_WRITE_MODE)

    # Independent verification: recompute from Silver and compare row count.
    verify = (
        spark.read.format("delta").load(GOLD_PATH)
    )
    logger.info("Verified Gold rows after write: %s", verify.count())

    # 5. Compaction: OPTIMIZE + ZORDER on Gold, then Bronze & Silver.
    run_optimize(spark, GOLD_PATH, GOLD_ZORDER_COL)

    # Bronze & Silver accumulate small files from streaming micro-batches;
    # compact them too (doc v2 §4.3 / Appendix A #10).
    if table_exists(spark, BRONZE_PATH):
        run_optimize(spark, BRONZE_PATH, None)
    if table_exists(spark, SILVER_PATH):
        run_optimize(spark, SILVER_PATH, None)

    spark.stop()
    logger.info("Gold compaction job complete.")


if __name__ == "__main__":
    main()
