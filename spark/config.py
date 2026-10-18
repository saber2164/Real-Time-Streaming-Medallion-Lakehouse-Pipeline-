"""Central configuration for the PySpark streaming ingestion and Gold jobs.

All paths, topic names, watermark, and trigger settings live here so the
Bronze, Silver, and Gold components stay consistent and are easy to tune.

Environment variables (set in docker-compose.yml) override these defaults.
"""

import os

# ---------------------------------------------------------------------------
# Kafka / Redpanda
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "order-events")
KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")

# ---------------------------------------------------------------------------
# Lakehouse paths (bind-mounted volumes in docker-compose)
# ---------------------------------------------------------------------------
BRONZE_PATH = os.getenv("BRONZE_PATH", "/lakehouse/bronze/order_events")
SILVER_PATH = os.getenv("SILVER_PATH", "/lakehouse/silver/order_events")
GOLD_PATH = os.getenv("GOLD_PATH", "/lakehouse/gold/hourly_sales")
CHECKPOINT_BASE = os.getenv("CHECKPOINT_BASE", "/checkpoints")

BRONZE_CHECKPOINT = os.path.join(CHECKPOINT_BASE, "bronze")
SILVER_CHECKPOINT = os.path.join(CHECKPOINT_BASE, "silver")
# Quarantine lives as a *sibling* of the Silver table (not nested inside it) so
# Delta does not treat it as a sub-directory of the Silver table.
QUARANTINE_PATH = os.getenv("QUARANTINE_PATH", "/lakehouse/silver/_quarantine")

# ---------------------------------------------------------------------------
# Streaming / watermark settings
# ---------------------------------------------------------------------------
# 10-minute event-time watermark. Producer delays events by 2-15 minutes, so
# late events beyond ~10 minutes fall outside watermark tolerance (see design
# doc v2 §4.1 and CONCEPTS_AND_DESIGN_DECISIONS.md).
WATERMARK = os.getenv("WATERMARK", "10 minutes")

# Dedup key per doc v2 §3.B.2: event_id is the true identity; event_time must
# be in the key so Spark can expire state. Do NOT dedup on order_id alone.
DEDUP_KEYS = ["event_id", "event_time"]

# Trigger for Bronze + Silver streaming queries.
TRIGGER_PROCESSING_TIME = os.getenv("TRIGGER_PROCESSING_TIME", "5 seconds")

# Optional runtime cap for the streaming job, in seconds. 0 (default) = run
# until signalled, the normal always-on mode. Set to a positive value for a
# bounded CI / cloud run: the job drains the topic and then stops itself
# gracefully, so a runner exits cleanly instead of hanging.
STREAM_MAX_SECONDS = int(os.getenv("STREAM_MAX_SECONDS", "0"))

# ---------------------------------------------------------------------------
# Gold layer
# ---------------------------------------------------------------------------
# Grain: 1 row per hour per seller_id per payment_type. region is an optional
# extra dimension (decided: not partitioned by region to keep Gold narrow;
# region is not part of the grain).
GOLD_GRAIN_COLUMNS = ["hour_start", "seller_id", "payment_type"]

# Idempotent write mode for Gold: overwrite (justified in concepts doc).
# Each full run recomputes aggregates from Silver and overwrites Gold,
# which is idempotent and simple for a scheduled batch job.
GOLD_WRITE_MODE = "overwrite"

# ZORDER column for compaction. seller_id only (event_date would be a
# partition column and cannot be Z-ordered; customer_id is not in Gold).
GOLD_ZORDER_COL = "seller_id"
