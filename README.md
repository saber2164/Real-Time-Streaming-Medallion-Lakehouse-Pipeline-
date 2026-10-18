# Real-Time Streaming & Medallion Lakehouse Pipeline

**Redpanda + PySpark Structured Streaming + Delta Lake**

A Medallion Lakehouse (Bronze → Silver → Gold) for real-time e-commerce order events, running 100% locally in Docker Compose with no cloud account.

> **Source of truth:** `project_2_architecture_design_doc_v2.md` contains the full design, requirements, and roadmap.

## Architecture

```
Synthetic Event Producer (Python)
        | KAFKA (produce, key=order_id)
        v
Redpanda Broker (topic: order-events, 3 partitions)
        | KAFKA (consume)
        v
PySpark Structured Streaming (+ Delta Lake)
  BRONZE  /lakehouse/bronze/order_events   (raw JSON + Kafka metadata)
    | Delta streaming read
    v
  SILVER  /lakehouse/silver/order_events   (parsed, typed, 10-min watermark, dedup)
    | batch
    v
  GOLD    /lakehouse/gold/hourly_sales     (hourly aggregates + OPTIMIZE/ZORDER)
```

## Quickstart

```bash
# 1. Start the stack
docker-compose up -d

# 2. Verify Redpanda + topic
# Redpanda Console: http://localhost:8080

# 3. Run the producer (emits ~10% duplicates, ~10% late events)
docker-compose run --rm producer python event_producer.py --duration 60

# 4. Run streaming ingestion (Bronze + Silver)
docker-compose run --rm spark spark-submit /opt/spark/app/stream_ingestion.py

# 5. Run Gold aggregation + compaction
docker-compose run --rm spark spark-submit /opt/spark/app/gold_compaction.py
```

## Medallion Layers

| Layer | Path | Purpose |
|-------|------|---------|
| Bronze | `/lakehouse/bronze/order_events` | Append-only raw JSON + Kafka metadata (partition, offset, timestamp) |
| Silver | `/lakehouse/silver/order_events` | Parsed, typed, 10-min watermark, stateful dedup on (`event_id`, `event_time`) |
| Gold | `/lakehouse/gold/hourly_sales` | Hourly revenue / order count / avg freight per seller & payment type |

## Ports

| Service | Port | Note |
|---------|------|------|
| Kafka API (internal) | `redpanda:9092` | Containers (Spark, producer) |
| Kafka API (external) | `localhost:19092` | Host-side tools |
| Redpanda Console | `8080` | Web UI |
| Spark UI | `4040` | Watermark / state-store demo |

## Testing

```bash
# Unit tests for producer ratios / schema
pytest tests/test_producer_logic.py

# Verify Silver dedup (zero duplicate event_id)
python tests/verify_silver_dedup.py

# Verify Bronze offsets contiguous (no gaps/repeats per partition)
python tests/verify_bronze_offsets.py

# Fault recovery: stop Spark mid-stream, restart, assertions
bash tests/verify_recovery.sh
```

## Documentation

- `docs/ARCHITECTURE.md` — schemas, layers, grain, diagram
- `docs/CONCEPTS_AND_DESIGN_DECISIONS.md` — decisions, alternatives, interview Q&A
- `docs/RUNBOOK.md` — run / verify / recover steps
- `docs/index.html` — showcase web page (GitHub Pages)
- `PROJECT_MEMORY.md` — persistent agent memory (current status, measured results, bugs)

## License

MIT
