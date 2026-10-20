"""
spark/gold_refresh.py

Live Gold refresher + dashboard serving-metrics exporter.

Purpose
-------
Recompute the Gold aggregate from Silver on a fixed cadence (default every
10 seconds) and publish a small JSON snapshot that the read-only stakeholder
dashboard serves. This is the "serving layer" for the lakehouse: the dashboard
never reads Delta or re-implements business logic, it only renders the numbers
this job writes.

Modes
-----
Continuous (default, used by the live demo):
    docker-compose --profile dashboard up -d gold-refresh

One-shot (used by CI to verify the dashboard against a finished run):
    docker-compose run --rm spark spark-submit \
        --packages io.delta:delta-spark_2.12:3.2.0 \
        /opt/spark/app/gold_refresh.py --once

Read-only one-shot (used by CI AFTER compaction; Gold is not rewritten, so
OPTIMIZE/ZORDER files and the Gold version are preserved):
    ... /opt/spark/app/gold_refresh.py --once --metrics-only

Outputs
-------
- Gold Delta table: /lakehouse/gold/hourly_sales (overwritten each cycle)
- Metrics JSON:     /lakehouse/dashboard/metrics.json (atomic replace)

Design notes
------------
- Reuses ``gold_logic.build_gold_dataframe`` so the live view and the batch
  ``gold_compaction.py`` job can never diverge.
- A full recompute is acceptable at this project's scale (thousands of rows)
  and makes late-arriving events correct without MERGE bookkeeping. At larger
  scale the refresh would move to incremental/foreachBatch MERGE.
- No OPTIMIZE in the loop: that would rewrite files every 10 s. The normal
  ``gold_compaction.py`` run owns compaction.
- JSON is written to a temp file then ``os.replace``d, so the dashboard can
  never read a half-written snapshot.
"""

import argparse
import json
import logging
import os
import signal
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    date_trunc,
    max as _max,
    percentile_approx,
    sum as _sum,
    unix_timestamp,
    when,
)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import (  # noqa: E402
    BRONZE_PATH,
    DASHBOARD_METRICS_PATH,
    DEDUP_KEYS,
    GOLD_PATH,
    GOLD_REFRESH_MAX_SECONDS,
    GOLD_REFRESH_SECONDS,
    QUARANTINE_PATH,
    SILVER_PATH,
    SILVER_WAIT_TIMEOUT_SECONDS,
    WATERMARK,
)
from gold_logic import build_gold_dataframe, write_gold  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("gold_refresh")


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def iso_utc(value):
    """Render a datetime as an ISO-8601 UTC string (or None)."""
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def round2(value):
    """Round a numeric value to 2 dp, preserving None (JSON null)."""
    if value is None:
        return None
    return round(float(value), 2)


def table_exists(path: str) -> bool:
    """A Delta table exists iff its transaction log directory exists."""
    return os.path.exists(os.path.join(path, "_delta_log"))


def write_json_atomic(path: str, payload: dict) -> None:
    """Write JSON atomically so readers only ever see a complete snapshot."""
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        dir=directory, prefix=".metrics-", suffix=".json"
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=False)
            handle.write("\n")
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def count_table(spark: SparkSession, path: str):
    """Row count of a Delta table, or None when it does not exist yet."""
    if not table_exists(path):
        return None
    return spark.read.format("delta").load(path).count()


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("medallion-gold-refresh")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
        # Deterministic timestamps in the JSON contract.
        .config("spark.sql.session.timeZone", "UTC")
        # Avoid clashing with the ingestion job's Spark UI on 4040.
        .config("spark.ui.port", os.getenv("SPARK_UI_PORT", "4042"))
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------


def _events_by_minute(silver):
    """Arrival throughput: events and order_created metrics per minute."""
    rows = (
        silver.withColumn("minute", date_trunc("minute", col("kafka_timestamp")))
        .groupBy("minute")
        .agg(
            count("*").alias("events"),
            _sum(when(col("event_type") == "order_created", 1).otherwise(0)).alias(
                "orders"
            ),
            _sum(
                when(
                    col("event_type") == "order_created",
                    col("price") + col("freight_value"),
                ).otherwise(0.0)
            ).alias("revenue"),
        )
        .orderBy("minute")
        .collect()
    )
    series = [
        {
            "minute": iso_utc(row["minute"]),
            "events": int(row["events"] or 0),
            "orders": int(row["orders"] or 0),
            "revenue": round2(row["revenue"] or 0.0),
        }
        for row in rows
        if row["minute"] is not None
    ]
    return series[-60:]


def _revenue_by_hour(gold_df):
    rows = (
        gold_df.groupBy("hour_start")
        .agg(
            _sum("total_revenue").alias("revenue"),
            _sum("hourly_order_count").alias("orders"),
        )
        .orderBy("hour_start")
        .collect()
    )
    return [
        {
            "hour_start": iso_utc(row["hour_start"]),
            "revenue": round2(row["revenue"] or 0.0),
            "orders": int(row["orders"] or 0),
        }
        for row in rows
    ]


def _top_sellers(gold_df, limit: int = 10):
    rows = (
        gold_df.groupBy("seller_id")
        .agg(
            _sum("total_revenue").alias("revenue"),
            _sum("hourly_order_count").alias("orders"),
        )
        .orderBy(col("revenue").desc())
        .limit(limit)
        .collect()
    )
    return [
        {
            "seller_id": row["seller_id"],
            "revenue": round2(row["revenue"] or 0.0),
            "orders": int(row["orders"] or 0),
        }
        for row in rows
    ]


def _payment_mix(gold_df):
    rows = (
        gold_df.groupBy("payment_type")
        .agg(
            _sum("total_revenue").alias("revenue"),
            _sum("hourly_order_count").alias("orders"),
        )
        .orderBy(col("revenue").desc())
        .collect()
    )
    return [
        {
            "payment_type": row["payment_type"],
            "revenue": round2(row["revenue"] or 0.0),
            "orders": int(row["orders"] or 0),
        }
        for row in rows
    ]


def _lateness(silver):
    """How late events arrived: kafka_timestamp - event_time, in seconds."""
    delta_seconds = unix_timestamp(col("kafka_timestamp")) - unix_timestamp(
        col("event_time")
    )
    row = (
        silver.filter(
            col("kafka_timestamp").isNotNull() & col("event_time").isNotNull()
        )
        .agg(
            avg(delta_seconds).alias("avg_lateness"),
            _max(delta_seconds).alias("max_lateness"),
            percentile_approx(delta_seconds, 0.95).alias("p95_lateness"),
        )
        .collect()[0]
    )
    return (
        round2(row["avg_lateness"]),
        round2(row["max_lateness"]),
        round2(row["p95_lateness"]),
    )


def build_metrics(spark: SparkSession, silver, gold_df) -> dict:
    """Compute the dashboard snapshot from the current Silver/Gold state.

    All inputs are already materialised (or cached) by ``refresh_once`` so this
    function is a set of cheap aggregations over the cached DataFrames.
    """
    now = datetime.now(timezone.utc)

    kpi_row = gold_df.agg(
        _sum("hourly_order_count").alias("total_orders"),
        _sum("total_revenue").alias("total_revenue"),
        countDistinct("seller_id").alias("sellers"),
        countDistinct("payment_type").alias("payment_types"),
        count("*").alias("gold_rows"),
    ).collect()[0]

    total_orders = int(kpi_row["total_orders"] or 0)
    total_revenue = float(kpi_row["total_revenue"] or 0.0)
    sellers = int(kpi_row["sellers"] or 0)
    payment_types = int(kpi_row["payment_types"] or 0)
    gold_rows = int(kpi_row["gold_rows"] or 0)

    silver_rows = int(silver.count())
    bronze_rows = count_table(spark, BRONZE_PATH)
    quarantine_rows = count_table(spark, QUARANTINE_PATH)

    duplicates_removed = None
    dedup_rate_pct = None
    if bronze_rows is not None:
        duplicates_removed = max(
            bronze_rows - silver_rows - (quarantine_rows or 0), 0
        )
        if bronze_rows > 0:
            dedup_rate_pct = round2(100.0 * duplicates_removed / bronze_rows)

    max_kafka_ts = silver.agg(_max("kafka_timestamp").alias("m")).collect()[0]["m"]
    freshness_seconds = None
    if max_kafka_ts is not None:
        if max_kafka_ts.tzinfo is None:
            max_kafka_ts = max_kafka_ts.replace(tzinfo=timezone.utc)
        freshness_seconds = round2(
            max((now - max_kafka_ts).total_seconds(), 0.0)
        )

    avg_lateness, max_lateness, p95_lateness = _lateness(silver)

    history = spark.sql(f"DESCRIBE HISTORY delta.`{GOLD_PATH}`").collect()
    latest = history[0]

    return {
        "schema_version": 1,
        "status": "live",
        "generated_at": iso_utc(now),
        "refresh_seconds": GOLD_REFRESH_SECONDS,
        "kpis": {
            "total_revenue": round2(total_revenue),
            "total_orders": total_orders,
            "avg_order_value": round2(total_revenue / total_orders)
            if total_orders
            else None,
            "sellers": sellers,
            "payment_types": payment_types,
            "gold_rows": gold_rows,
            "silver_rows": silver_rows,
            "bronze_rows": bronze_rows,
            "quarantine_rows": quarantine_rows,
            "duplicates_removed": duplicates_removed,
            "dedup_rate_pct": dedup_rate_pct,
            "data_freshness_seconds": freshness_seconds,
            "avg_lateness_seconds": avg_lateness,
            "max_lateness_seconds": max_lateness,
            "p95_lateness_seconds": p95_lateness,
        },
        "pipeline": {
            "watermark": WATERMARK,
            "dedup_keys": DEDUP_KEYS,
            "gold_version": int(latest["version"]),
            "gold_updated_at": iso_utc(latest["timestamp"]),
            "bronze_path": BRONZE_PATH,
            "silver_path": SILVER_PATH,
            "gold_path": GOLD_PATH,
        },
        "charts": {
            "events_by_minute": _events_by_minute(silver),
            "revenue_by_hour": _revenue_by_hour(gold_df),
            "top_sellers": _top_sellers(gold_df),
            "payment_mix": _payment_mix(gold_df),
        },
    }


def refresh_once(spark: SparkSession, metrics_only: bool = False) -> dict:
    """Read Silver, optionally overwrite Gold, and write the metrics JSON.

    ``metrics_only=True`` reads the *existing* Gold table instead of rebuilding
    it. CI uses this after ``gold_compaction.py`` so the metrics step cannot
    undo OPTIMIZE/ZORDER or bump the Gold version.
    """
    started = time.time()
    silver = spark.read.format("delta").load(SILVER_PATH).cache()
    if metrics_only:
        gold_df = spark.read.format("delta").load(GOLD_PATH).cache()
    else:
        gold_df = build_gold_dataframe(silver).cache()

    try:
        if not metrics_only:
            write_gold(gold_df, GOLD_PATH)
        metrics = build_metrics(spark, silver, gold_df)
        write_json_atomic(DASHBOARD_METRICS_PATH, metrics)
    finally:
        gold_df.unpersist()
        silver.unpersist()

    kpis = metrics["kpis"]
    logger.info(
        "REFRESH %.1fs gold_rows=%s silver=%s bronze=%s revenue=%.2f orders=%s "
        "version=%s -> %s",
        time.time() - started,
        kpis["gold_rows"],
        kpis["silver_rows"],
        kpis["bronze_rows"],
        kpis["total_revenue"] or 0.0,
        kpis["total_orders"],
        metrics["pipeline"]["gold_version"],
        DASHBOARD_METRICS_PATH,
    )
    return metrics


def waiting_payload(message: str) -> dict:
    return {
        "schema_version": 1,
        "status": "waiting",
        "generated_at": iso_utc(datetime.now(timezone.utc)),
        "refresh_seconds": GOLD_REFRESH_SECONDS,
        "message": message,
        "kpis": {},
        "pipeline": {
            "watermark": WATERMARK,
            "dedup_keys": DEDUP_KEYS,
            "bronze_path": BRONZE_PATH,
            "silver_path": SILVER_PATH,
            "gold_path": GOLD_PATH,
        },
        "charts": {
            "events_by_minute": [],
            "revenue_by_hour": [],
            "top_sellers": [],
            "payment_mix": [],
        },
    }


def wait_for_silver(stop_event: threading.Event, timeout_seconds: int):
    """Block until the Silver Delta table exists.

    In one-shot mode a finite timeout is used. In continuous mode
    ``timeout_seconds`` is 0 and the job waits until signalled, which is what a
    long-lived dashboard wants.
    """
    deadline = (
        time.time() + timeout_seconds if timeout_seconds and timeout_seconds > 0 else None
    )
    announced = False
    while not stop_event.is_set():
        if table_exists(SILVER_PATH):
            logger.info("Silver table found at %s", SILVER_PATH)
            return True
        if not announced:
            message = (
                "Waiting for the Silver table at "
                f"{SILVER_PATH}. Start the streaming ingestion job."
            )
            logger.info(message)
            write_json_atomic(DASHBOARD_METRICS_PATH, waiting_payload(message))
            announced = True
        if deadline is not None and time.time() >= deadline:
            logger.error(
                "Silver table did not appear within %ss", timeout_seconds
            )
            return False
        stop_event.wait(5)
    return False


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Live Gold refresh + dashboard metrics")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single refresh and exit (CI verification mode).",
    )
    parser.add_argument(
        "--metrics-only",
        action="store_true",
        help=(
            "Read the existing Gold table and only write the metrics JSON. "
            "Does not rewrite Gold (preserves OPTIMIZE/ZORDER)."
        ),
    )
    args = parser.parse_args(argv)

    stop_event = threading.Event()

    def _handle_signal(signum, _frame):
        logger.info("Received signal %s; stopping after the current cycle.", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    if args.metrics_only and not table_exists(GOLD_PATH):
        logger.error(
            "Gold table not found at %s; run gold_compaction.py first or omit "
            "--metrics-only.",
            GOLD_PATH,
        )
        return 1
    logger.info(
        "Mode: %s",
        "metrics-only (Gold is read, not rewritten)"
        if args.metrics_only
        else "write-gold",
    )

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        timeout = SILVER_WAIT_TIMEOUT_SECONDS if args.once else 0
        if not wait_for_silver(stop_event, timeout):
            message = (
                f"Silver table not found at {SILVER_PATH}. "
                "Start stream_ingestion.py first."
            )
            write_json_atomic(DASHBOARD_METRICS_PATH, waiting_payload(message))
            return 1

        if args.once:
            refresh_once(spark, metrics_only=args.metrics_only)
            logger.info("One-shot Gold refresh complete.")
            return 0

        started = time.time()
        while not stop_event.is_set():
            try:
                refresh_once(spark, metrics_only=args.metrics_only)
            except Exception:  # noqa: BLE001
                # A transient failure (e.g. a concurrent Delta commit) must not
                # kill a long-lived dashboard refresher.
                logger.exception("Gold refresh failed; retrying in %ss", GOLD_REFRESH_SECONDS)

            if (
                GOLD_REFRESH_MAX_SECONDS > 0
                and (time.time() - started) >= GOLD_REFRESH_MAX_SECONDS
            ):
                logger.info(
                    "Runtime cap (%ss) reached.", GOLD_REFRESH_MAX_SECONDS
                )
                break
            stop_event.wait(GOLD_REFRESH_SECONDS)

        logger.info("Gold refresher stopped.")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
