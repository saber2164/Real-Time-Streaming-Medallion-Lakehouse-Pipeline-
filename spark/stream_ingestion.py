"""
spark/stream_ingestion.py

Bronze + Silver streaming ingestion for the Medallion Lakehouse.

Purpose
-------
Consume raw order events from the Redpanda `order-events` topic, land them
verbatim (plus Kafka metadata) into the Bronze Delta table, then parse,
deduplicate, and write cleansed events to the Silver Delta table by reading
Bronze back as a Delta *stream*.

Inputs
------
- Kafka topic `order-events` at `redpanda:9092` (internal listener).
  JSON payload per doc v2 §3.A:
    event_id, order_id, customer_id, seller_id, region, event_type,
    price, freight_value, payment_type, event_time
- Bronze Delta table (streamed) at BRONZE_PATH for the Silver query.

Outputs
-------
- Bronze Delta table:  /lakehouse/bronze/order_events
  Columns: raw_payload (STRING), kafka_partition (INT), kafka_offset (LONG),
           kafka_timestamp (TIMESTAMP), ingest_date (DATE)
- Silver Delta table: /lakehouse/silver/order_events
  Columns: event_id, order_id, customer_id, seller_id, region, event_type,
           price, freight_value, payment_type, event_time (TIMESTAMP),
           event_date (DATE), total_amount (= price + freight_value)
- Quarantine: /lakehouse/silver/_quarantine (unparseable rows)

How to run
----------
    docker-compose run --rm spark \
        spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,io.delta:delta-spark_2.12:3.2.0 \
        /opt/spark/app/stream_ingestion.py

Key design choices
------------------
- Bronze is append-only and never mutated (replayable source of truth).
- Layer wiring (doc v2 Part 2): **Silver reads the Bronze Delta stream**, not
  Kafka directly. One Kafka consumption feeds Bronze only; Silver is derived
  from Bronze. This keeps Kafka offsets owned by a single query (no double
  consumption), and lets Silver be rebuilt by replaying Bronze.
- Dedup key = ["event_id", "event_time"] with a 10-minute watermark. event_id
  is the true identity (producer deliberately duplicates it); event_time must
  be in the key so Spark can expire state (otherwise state grows unbounded).
- Unparseable JSON rows are routed to a quarantine path instead of being
  dropped silently.
- Separate checkpointLocation per query (Bronze vs Silver vs quarantine).
- Graceful stop on SIGTERM.
"""

import os
import sys
import signal
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_date,
    current_date,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    LongType,
    TimestampType,
    DateType,
)

# Add the app dir to path so `config` imports when run via spark-submit.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import (  # noqa: E402
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_STARTING_OFFSETS,
    BRONZE_PATH,
    SILVER_PATH,
    QUARANTINE_PATH,
    BRONZE_CHECKPOINT,
    SILVER_CHECKPOINT,
    WATERMARK,
    DEDUP_KEYS,
    TRIGGER_PROCESSING_TIME,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("stream_ingestion")

# Global handle to the active streaming queries so the signal handler can stop
# them gracefully.
ACTIVE_QUERIES = []


def build_spark_session(app_name: str) -> SparkSession:
    """Build a SparkSession with Delta Lake extensions enabled.

    Delta Lake requires the Spark extensions and the delta catalog to be
    registered so that `format("delta")` reads/writes work and `OPTIMIZE`/
    `DESCRIBE HISTORY` are available.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Shuffle partitions kept low for a local single-node demo.
        .config("spark.sql.shuffle.partitions", "4")
        # Spark UI port (exposed on host as 4040).
        .config("spark.ui.port", "4040")
    )
    return builder.getOrCreate()


def define_event_schema() -> StructType:
    """Explicit StructType for the order event payload.

    No schema inference is used (inference is unreliable for streaming and
    can silently widen/narrow types). Every field is typed explicitly per
    doc v2 §3.A / §3.B.2.
    """
    return StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), True),
            StructField("seller_id", StringType(), True),
            StructField("region", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("freight_value", DoubleType(), True),
            StructField("payment_type", StringType(), True),
            StructField("event_time", TimestampType(), True),
        ]
    )


def define_bronze_schema() -> StructType:
    """Explicit StructType for the Bronze table (raw payload + Kafka metadata)."""
    return StructType(
        [
            StructField("raw_payload", StringType(), True),
            StructField("kafka_partition", IntegerType(), True),
            StructField("kafka_offset", LongType(), True),
            StructField("kafka_timestamp", TimestampType(), True),
            StructField("ingest_date", DateType(), True),
        ]
    )


def read_kafka_stream(spark: SparkSession):
    """Read the Kafka topic as an unbounded DataFrame shaped like Bronze."""
    logger.info("Reading Kafka topic %s from %s", KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS)
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )
    return kafka_df.selectExpr(
        "CAST(value AS STRING) AS raw_payload",
        "partition AS kafka_partition",
        "offset AS kafka_offset",
        "CAST(timestamp AS TIMESTAMP) AS kafka_timestamp",
    ).withColumn("ingest_date", current_date())


def ensure_bronze_table(spark: SparkSession) -> None:
    """Create an empty Bronze Delta table if it does not exist yet.

    Silver reads Bronze as a Delta *stream*, so Bronze must be a valid Delta
    table before the Silver query starts. Creating it empty first makes the
    two queries independent of start order and is idempotent across restarts.
    """
    if os.path.exists(os.path.join(BRONZE_PATH, "_delta_log")):
        logger.info("Bronze table already exists at %s", BRONZE_PATH)
        return
    empty = spark.createDataFrame([], define_bronze_schema())
    empty.write.format("delta").mode("overwrite").save(BRONZE_PATH)
    logger.info("Created empty Bronze Delta table at %s", BRONZE_PATH)


def write_bronze_stream(bronze_df):
    """Write the Kafka stream to Bronze Delta (append mode, own checkpoint)."""
    bronze_query = (
        bronze_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", BRONZE_CHECKPOINT)
        .option("path", BRONZE_PATH)
        .trigger(processingTime=TRIGGER_PROCESSING_TIME)
        .queryName("bronze_order_events")
        .start()
    )
    logger.info(
        "Bronze query started: path=%s checkpoint=%s",
        BRONZE_PATH,
        BRONZE_CHECKPOINT,
    )
    return bronze_query


def read_bronze_delta_stream(spark: SparkSession):
    """Read the Bronze Delta table as a streaming source (Silver's input)."""
    logger.info("Reading Bronze Delta stream from %s", BRONZE_PATH)
    return (
        spark.readStream.format("delta")
        .option("startingVersion", "0")  # replay all Bronze history on a fresh start
        .option("ignoreDeletes", "true")
        .load(BRONZE_PATH)
    )


def build_silver_stream(bronze_df):
    """Parse the Bronze stream, dedup, and write to Silver Delta (+ quarantine).

    Steps:
      1. Parse raw_payload JSON with an explicit StructType.
      2. Split rows into good (parsed) vs quarantine (unparseable).
      3. withWatermark("event_time", "10 minutes") — bounds state for dedup.
      4. dropDuplicates(["event_id", "event_time"]) — stateful dedup.
      5. Add derived columns: event_date, total_amount = price + freight_value.
      6. Write good rows to Silver; write bad rows to the quarantine path.

    Returns (silver_query, quarantine_query).
    """
    schema = define_event_schema()

    parsed = (
        bronze_df.withColumn("parsed", from_json(col("raw_payload"), schema))
        .selectExpr(
            "parsed.event_id",
            "parsed.order_id",
            "parsed.customer_id",
            "parsed.seller_id",
            "parsed.region",
            "parsed.event_type",
            "parsed.price",
            "parsed.freight_value",
            "parsed.payment_type",
            "parsed.event_time",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "ingest_date",
        )
    )

    # Quarantine: rows whose JSON failed to parse (event_id is null after parse).
    quarantine_df = parsed.filter(col("event_id").isNull())
    good_df = parsed.filter(col("event_id").isNotNull())

    # Derived columns.
    good_df = good_df.withColumn("event_date", to_date(col("event_time"))).withColumn(
        "total_amount", col("price") + col("freight_value")
    )

    # --- Silver streaming query (deduped) ---
    silver_query = (
        good_df.withWatermark("event_time", WATERMARK)
        .dropDuplicates(DEDUP_KEYS)
        .writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", SILVER_CHECKPOINT)
        .option("path", SILVER_PATH)
        .trigger(processingTime=TRIGGER_PROCESSING_TIME)
        .queryName("silver_order_events")
        .start()
    )

    # --- Quarantine stream (unparseable rows) ---
    quarantine_query = (
        quarantine_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", os.path.join(SILVER_CHECKPOINT, "_quarantine"))
        .option("path", QUARANTINE_PATH)
        .trigger(processingTime=TRIGGER_PROCESSING_TIME)
        .queryName("silver_quarantine")
        .start()
    )

    logger.info(
        "Silver query started: path=%s checkpoint=%s watermark=%s dedup_keys=%s",
        SILVER_PATH,
        SILVER_CHECKPOINT,
        WATERMARK,
        DEDUP_KEYS,
    )
    return silver_query, quarantine_query


def log_progress(query):
    """Log lastProgress for a query (input rows/s, state rows, watermark).

    Called periodically so the docs and web page capture real metrics.
    """
    progress = query.lastProgress
    if progress is None:
        return
    logger.info(
        "PROGRESS %s: inputRows=%s inputRowsPerSecond=%s "
        "stateOperators=%s batchDurationMs=%s",
        query.name,
        progress.get("numInputRows"),
        progress.get("inputRowsPerSecond"),
        len(progress.get("stateOperators") or []),
        (progress.get("durationMs") or {}).get("addBatch"),
    )
    for so in progress.get("stateOperators") or []:
        logger.info(
            "STATE %s: numRowsTotal=%s numUpdatedRows=%s numRowsRemoved=%s",
            so.get("operatorName"),
            so.get("numRowsTotal"),
            so.get("numUpdatedRows"),
            so.get("numRowsRemoved"),
        )


def handle_signal(signum, frame):
    """Gracefully stop all active streaming queries on SIGTERM/SIGINT."""
    logger.info("Received signal %s, stopping streaming queries gracefully...", signum)
    for q in ACTIVE_QUERIES:
        try:
            q.stop()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Error stopping query %s: %s", q.name, exc)
    logger.info("All streaming queries stopped.")
    sys.exit(0)


def main():
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    spark = build_spark_session("medallion-bronze-silver-ingestion")
    spark.sparkContext.setLogLevel("WARN")

    # 1. Make sure Bronze exists so Silver can read it as a Delta stream.
    ensure_bronze_table(spark)

    # 2. One Kafka consumption -> Bronze.
    bronze_df = read_kafka_stream(spark)
    bronze_query = write_bronze_stream(bronze_df)
    ACTIVE_QUERIES.append(bronze_query)

    # 3. Silver is derived from the Bronze Delta stream (not Kafka).
    bronze_delta_stream = read_bronze_delta_stream(spark)
    silver_query, quarantine_query = build_silver_stream(bronze_delta_stream)
    ACTIVE_QUERIES.extend([silver_query, quarantine_query])

    # Log progress every 30 seconds while streaming.
    import threading
    import time

    def progress_logger():
        while True:
            for q in ACTIVE_QUERIES:
                try:
                    log_progress(q)
                except Exception as exc:  # noqa: BLE001
                    logger.debug("Progress log skipped for %s: %s", q.name, exc)
            time.sleep(30)

    t = threading.Thread(target=progress_logger, daemon=True)
    t.start()

    logger.info("Streaming ingestion running. Awaiting termination...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
