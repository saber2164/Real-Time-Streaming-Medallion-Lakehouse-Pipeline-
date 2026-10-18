"""Quantify watermark / late-data behaviour for the Bronze + Silver tables.

Run this after a streaming run to check the question the design doc asks
(v2, "Watermarking & Late Data Strategy"):

    "the producer delays events by 2-15 minutes while the watermark is 10
     minutes, so some late events (>10 min) fall outside tolerance -- show
     what happens to them"

Answer (measured): Spark *drops* them (append dedup), which is why the
watermark is now 20 minutes -- above the producer's 15-min maximum lateness.
This script is the evidence tool for that decision.

It prints, for the current dataset:

  * Bronze / Silver / quarantine row counts
  * distinct dedup-key counts on each layer
  * duplicate keys in Bronze (what dedup must remove)
  * max(event_time) and the watermark cutoff = max(event_time) - WATERMARK
  * Bronze rows OLDER than the cutoff (i.e. outside the watermark window)
  * Silver rows OLDER than the cutoff

If Bronze rows outside the watermark window are present in Silver, late
unique events are retained. If they are absent, the watermark dropped them
(Spark drops records whose event time is below the watermark in an append
dedup), i.e. the pipeline *lost* valid events.

Usage (inside the spark container):

    spark-submit --packages io.delta:delta-spark_2.12:3.2.0 \
        /opt/spark/scripts/watermark_report.py

Environment: honours BRONZE_PATH, SILVER_PATH, QUARANTINE_PATH, DEDUP_KEYS
and WATERMARK from spark/config.py.
"""

import os
import re
import sys

sys.path.insert(0, "/opt/spark/app")
from config import (  # noqa: E402
    BRONZE_PATH,
    DEDUP_KEYS,
    QUARANTINE_PATH,
    SILVER_PATH,
    WATERMARK,
)

from pyspark.sql import SparkSession, functions as F  # noqa: E402


def _watermark_seconds(spec):
    """'10 minutes' / '30 seconds' / '2 hours' -> seconds (int)."""
    match = re.match(r"\s*(\d+)\s*(second|minute|hour)s?", spec.strip(), re.I)
    if not match:
        raise ValueError(f"unparsable watermark interval: {spec!r}")
    value, unit = int(match.group(1)), match.group(2).lower()
    return value * {"second": 1, "minute": 60, "hour": 3600}[unit]


def _read(spark, path):
    return spark.read.format("delta").load(path)


def main():
    spark = (
        SparkSession.builder.appName("watermark-report")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    wm_secs = _watermark_seconds(WATERMARK)

    bronze = _read(spark, BRONZE_PATH)
    b = bronze.select(
        F.get_json_object("raw_payload", "$.event_id").alias("event_id"),
        F.to_timestamp(
            F.get_json_object("raw_payload", "$.event_time"),
            "yyyy-MM-dd'T'HH:mm:ss'Z'",
        ).alias("event_time"),
    )

    bronze_rows = b.count()
    bronze_distinct = b.select(*DEDUP_KEYS).distinct().count()
    max_et = b.agg(F.max("event_time")).first()[0]

    sep = "=" * 68
    print(f"\n{sep}")
    print("WATERMARK / LATE-DATA REPORT")
    print(f"  watermark setting        : {WATERMARK} ({wm_secs}s)")
    print(f"  dedup keys               : {DEDUP_KEYS}")
    print(sep)
    print(f"  Bronze rows              : {bronze_rows}")
    print(f"  Bronze distinct keys     : {bronze_distinct}")
    print(f"  Bronze duplicate rows    : {bronze_rows - bronze_distinct}")
    print(f"  max(event_time)          : {max_et}")

    try:
        silver_rows = _read(spark, SILVER_PATH).count()
        print(f"  Silver rows              : {silver_rows}")
    except Exception as exc:  # noqa: BLE001
        silver_rows = None
        print(f"  Silver rows              : <unreadable: {type(exc).__name__}>")

    try:
        q_rows = _read(spark, QUARANTINE_PATH).count()
        print(f"  Quarantine rows          : {q_rows}")
    except Exception as exc:  # noqa: BLE001
        q_rows = None
        print(f"  Quarantine rows          : <none/unreadable: {type(exc).__name__}>")

    if max_et is not None:
        cutoff = F.lit(max_et).cast("timestamp") - F.expr(
            f"INTERVAL {wm_secs} SECONDS"
        )
        outside = b.filter(F.col("event_time") < cutoff)
        outside_rows = outside.count()
        outside_distinct = outside.select(*DEDUP_KEYS).distinct().count()
        print(sep)
        print(f"  watermark cutoff         : {max_et} - {wm_secs}s")
        print(f"  Bronze rows OUTSIDE (older than cutoff) : {outside_rows}")
        print(f"  Bronze DISTINCT keys outside            : {outside_distinct}")
        if silver_rows is not None:
            silver = _read(spark, SILVER_PATH)
            s_outside = silver.filter(F.col("event_time") < cutoff).count()
            print(f"  Silver rows outside the watermark window: {s_outside}")
            lost = outside_distinct - s_outside
            print(sep)
            if s_outside >= outside_distinct:
                print("  RESULT: late unique events are RETAINED (no data loss).")
            else:
                print(
                    f"  RESULT: ~{lost} late unique events were DROPPED by the "
                    "watermark (data loss!)."
                )
    print(sep + "\n")
    spark.stop()


if __name__ == "__main__":
    main()
