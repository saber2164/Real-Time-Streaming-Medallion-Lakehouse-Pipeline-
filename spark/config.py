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
# 20-minute event-time watermark: sized ABOVE the producer's maximum lateness
# (2-15 min) so no valid event is ever beyond the watermark.
#
# Why 20 and not 10 (measured, not theoretical): with a 10-minute watermark in
# an append dedup, Spark DROPS any record whose event_time is below the
# watermark. A live run (stream first, producer second) of the canonical
# dataset therefore lost 137 legitimate, unique orders: Bronze 3277 rows /
# 3018 distinct keys -> Silver 2881 (Bronze "producer-first" runs hid this,
# because the whole backlog landed before the watermark advanced). See
# scripts/watermark_report.py and CONCEPTS_AND_DESIGN_DECISIONS.md §1.4.
WATERMARK = os.getenv("WATERMARK", "20 minutes")

# Dedup key per doc v2 §3.B.2: event_id is the true identity; event_time must
# be in the key so Spark can expire state. Do NOT dedup on order_id alone.
DEDUP_KEYS = ["event_id", "event_time"]

# Trigger for Bronze + Silver streaming queries.
TRIGGER_PROCESSING_TIME = os.getenv("TRIGGER_PROCESSING_TIME", "5 seconds")

# Optional runtime cap for the streaming job, in seconds. 0 (default) = run
# until signalled, the normal always-on mode. Set to a positive value for a
# bounded CI / cloud run as a SAFETY BACKSTOP (hard upper bound), so a runner
# can never hang. Prefer STREAM_IDLE_TIMEOUT_SECONDS for correctness: a fixed
# cap can cut the run off before the Bronze -> Silver cascade has drained.
STREAM_MAX_SECONDS = int(os.getenv("STREAM_MAX_SECONDS", "0"))

# Drain-aware graceful stop, in seconds. 0 (default) = disabled. When > 0, once
# the queries have processed at least one batch and then produce no new input
# rows for this many seconds, the job stops itself. This lets a bounded run
# fully drain (all Kafka rows -> Bronze -> Silver) before exiting, instead of
# racing a wall-clock cap. Safe because the producer emits continuously, so a
# real gap of this length means the source is genuinely finished.
STREAM_IDLE_TIMEOUT_SECONDS = int(os.getenv("STREAM_IDLE_TIMEOUT_SECONDS", "0"))

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

# ---------------------------------------------------------------------------
# Live dashboard / serving layer
# ---------------------------------------------------------------------------
# The Gold refresher recomputes Gold from Silver on this cadence (seconds) and
# writes a small JSON snapshot for the read-only dashboard to serve. 10 s is
# near-real-time for humans; the pipeline itself still micro-batches every 5 s.
GOLD_REFRESH_SECONDS = int(os.getenv("GOLD_REFRESH_SECONDS", "10"))

# Optional wall-clock cap for the refresher loop (0 = run until signalled).
# Useful as a safety backstop for scripted demos; normal use is 0.
GOLD_REFRESH_MAX_SECONDS = int(os.getenv("GOLD_REFRESH_MAX_SECONDS", "0"))

# How long to wait for the Silver table to appear before writing a "waiting"
# snapshot (e.g. when the dashboard starts before the streaming job).
SILVER_WAIT_TIMEOUT_SECONDS = int(os.getenv("SILVER_WAIT_TIMEOUT_SECONDS", "600"))

# JSON contract consumed by dashboard/server.py. Lives inside the (gitignored)
# lakehouse volume: it is data, not source code.
DASHBOARD_METRICS_PATH = os.getenv(
    "DASHBOARD_METRICS_PATH", "/lakehouse/dashboard/metrics.json"
)
