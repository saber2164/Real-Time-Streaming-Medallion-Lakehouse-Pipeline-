# Architecture

**Project:** Real-Time Streaming & Medallion Lakehouse Pipeline
**Stack:** Redpanda (Kafka API) → PySpark Structured Streaming → Delta Lake (Bronze/Silver/Gold)
**Doc:** `../project_2_architecture_design_doc_v2.md` is the source of truth. This file is
the implementation-level architecture reference and must stay in sync with the code.

---

## 1. Component overview

```
┌─────────────────────────────┐        JSON events (key = order_id)
│  Synthetic Producer (Py)    │        ~10% duplicates, ~10% delayed 2-15 min
│  producer/event_producer.py │
└──────────────┬──────────────┘
               │ produce
               ▼
┌─────────────────────────────┐        topic: order-events (3 partitions, 1 replica)
│  Redpanda broker v24.3.10   │        internal redpanda:9092 / external localhost:19092
│  (+ Redpanda Console :8080) │
└──────────────┬──────────────┘
               │ readStream (kafka)   ← ONLY Kafka consumer in the pipeline
               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  PySpark Structured Streaming (local[*], Spark 3.5.3, delta-spark 3.2.0)     │
│                                                                             │
│   BRONZE query        raw_payload + kafka metadata, append, own checkpoint   │
│   /lakehouse/bronze/order_events                                            │
│               │ readStream (delta)   ← Silver replays Bronze, not Kafka      │
│               ▼                                                             │
│   SILVER query        from_json → watermark 10m → dropDuplicates             │
│   /lakehouse/silver/order_events          (["event_id","event_time"])        │
│                       unparseable → /lakehouse/silver/_quarantine           │
└──────────────┬──────────────────────────────────────────────────────────────┘
               │ batch read
               ▼
┌─────────────────────────────┐
│  Gold batch job (PySpark)   │        filter order_created → hour×seller×payment
│  spark/gold_compaction.py   │        → OPTIMIZE ZORDER BY (seller_id)
│  /lakehouse/gold/hourly_sales│
└─────────────────────────────┘
```

## 2. Data flow and layer wiring

The **single most important wiring decision**: only the **Bronze** query consumes Kafka.
The **Silver** query reads the Bronze Delta table as a *stream* (`readStream.format("delta")`).

```
Kafka ──readStream──► Bronze (Delta, append) ──readStream(delta)──► Silver (Delta, append)
```

Why: exactly one component owns Kafka offsets (no double consumption, no consumer-group
coordination between two Spark queries); Silver can be **rebuilt by replaying Bronze**
without re-reading the broker; and Bronze stays the immutable, replayable source of truth.
See `CONCEPTS_AND_DESIGN_DECISIONS.md` → "Layer wiring".

## 3. Schemas

### 3.1 Event payload (Kafka value, JSON)

| Field | Type | Notes |
|-------|------|-------|
| `event_id` | string | **true identity**; the producer deliberately re-sends it for duplicates |
| `order_id` | string | Kafka **message key** (all events of an order share a partition) |
| `customer_id` | string | |
| `seller_id` | string | Gold grain + Z-order key |
| `region` | string | one of west/east/north/south |
| `event_type` | string | `order_created` / `order_paid` / `order_cancelled` |
| `price` | double | |
| `freight_value` | double | |
| `payment_type` | string | credit_card / paypal / bank_transfer |
| `event_time` | timestamp | **event time**, ISO-8601 UTC (`...Z`) |

Parsed with an **explicit `StructType`** (no schema inference).

### 3.2 Bronze — `/lakehouse/bronze/order_events`

| Column | Type | Notes |
|--------|------|-------|
| `raw_payload` | string | the raw JSON value, verbatim |
| `kafka_partition` | int | source partition |
| `kafka_offset` | long | source offset |
| `kafka_timestamp` | timestamp | broker timestamp |
| `ingest_date` | date | processing date (`current_date()`) |

Append-only, never mutated. Columns `kafka_partition`/`kafka_offset` are what make the
"no gaps / no repeats" recovery proof possible.

### 3.3 Silver — `/lakehouse/silver/order_events`

Parsed event columns (`event_id` … `event_time`) **plus**:

| Column | Type | Derivation |
|--------|------|-----------|
| `kafka_partition`, `kafka_offset`, `kafka_timestamp`, `ingest_date` | (carried through) | lineage back to Bronze |
| `event_date` | date | `to_date(event_time)` |
| `total_amount` | double | `price + freight_value` |

Deduplicated with `withWatermark("event_time", "10 minutes")` +
`dropDuplicates(["event_id", "event_time"])`.

### 3.4 Quarantine — `/lakehouse/silver/_quarantine`

Rows whose `raw_payload` fails `from_json` (null `event_id` after parse) are written here
instead of being silently dropped. It is a **sibling** of the Silver table, not nested
inside it (a nested directory would be treated as part of the Silver Delta table).

### 3.5 Gold — `/lakehouse/gold/hourly_sales`

| Column | Type | Notes |
|--------|------|-------|
| `hour_start` | timestamp | `date_trunc('hour', event_time)` |
| `event_date` | date | `to_date(hour_start)` |
| `seller_id` | string | |
| `payment_type` | string | |
| `hourly_order_count` | long | count of `order_created` events |
| `total_revenue` | double | `sum(price)` |
| `avg_freight_cost` | double | `avg(freight_value)` |
| `total_freight` | double | `sum(freight_value)` (kept for completeness) |

**Grain: one row per `(hour_start, seller_id, payment_type)`.** Only `order_created`
events contribute counts/revenue, so cancelled/paid events in Silver do not
double-count an order.

## 4. Streaming semantics

| Concern | Setting | Rationale |
|---------|---------|-----------|
| Trigger | `processingTime = 5 seconds` | small micro-batches, low local latency |
| Output mode | `append` | Bronze and Silver are append-only facts |
| Watermark | `10 minutes` on `event_time` | bounds dedup state; deliberately **shorter** than the producer's 2-15 min delay window so late-data behaviour is observable |
| Dedup key | `["event_id", "event_time"]` | `event_id` alone cannot bound state; `order_id` would collapse legitimately different events |
| Checkpoints | `checkpointLocation` per query (`bronze`, `silver`, `silver/_quarantine`) | one checkpoint per query; never shared |
| Delivery guarantee | **effectively-once into Delta** (idempotent writes + checkpoint); source/producer are at-least-once → dedup layer makes Silver effectively unique | precise wording matters (see concepts doc) |

## 5. Gold job and compaction

`spark/gold_compaction.py`:
1. Batch-reads Silver, filters `event_type = 'order_created'`.
2. Aggregates to the Gold grain, writes Gold with **`mode("overwrite")`** (idempotent for a
   derived aggregate; simpler and safer than MERGE for a full recompute).
3. `OPTIMIZE delta.\`/lakehouse/gold/hourly_sales\` ZORDER BY (seller_id)`.
4. Also `OPTIMIZE`s Bronze and Silver — that is where streaming micro-batches actually
   accumulate small files (Gold is produced by one batch write).
5. Logs active-file count/size before vs after (from `DESCRIBE DETAIL`) and
   `DESCRIBE HISTORY` operation metrics as evidence.

## 6. Ports and endpoints

| Endpoint | Address | Used by |
|----------|---------|---------|
| Kafka internal | `redpanda:9092` | Spark, producer (containers) |
| Kafka external | `localhost:19092` | host-side `rpk` |
| Redpanda Console | http://localhost:8080 | humans |
| Spark UI | http://localhost:4040 | humans (streaming metrics) |

## 7. Container topology (`docker-compose.yml`)

| Service | Image / build | Role |
|---------|---------------|------|
| `redpanda` | `redpandadata/redpanda:v24.3.10` | broker, dual listeners, healthcheck |
| `console` | `redpandadata/console:v2.8.4` | web UI on 8080 |
| `topic-init` | redpanda image | one-shot: create-and-verify topic `order-events` (idempotent), exits 0 |
| `spark` | build `Dockerfile.spark` (pyspark 3.5.3 + delta-spark 3.2.0, JDK 21) | runs ingestion + gold + tests; mounts `./lakehouse`, `./checkpoints`, `./spark`, `./scripts`, `./tests`, `./.ivy2` |
| `producer` | build `./producer` (confluent-kafka) | optional profile; synthetic load |

`spark` and `producer` use `depends_on` with `service_healthy` (redpanda) and
`service_completed_successfully` (topic-init). No secrets are stored anywhere in the stack.

## 8. Repository map

```
.
├── docker-compose.yml          # the whole stack
├── Dockerfile.spark            # Spark + Delta image
├── console.yaml                # Redpanda Console config
├── producer/
│   ├── event_producer.py       # synthetic events (dup/late injection)
│   ├── requirements.txt
│   └── Dockerfile
├── spark/
│   ├── config.py               # paths, topic, watermark, dedup keys, trigger
│   ├── stream_ingestion.py     # Bronze + Silver streaming queries
│   └── gold_compaction.py      # Gold aggregate + OPTIMIZE/ZORDER
├── tests/
│   ├── test_producer_logic.py  # unit tests (dup/late ratios, schema)
│   ├── verify_silver_dedup.py  # zero duplicate event_id
│   ├── verify_bronze_offsets.py# contiguous offsets per partition
│   ├── verify_gold.py          # Gold == independent recompute from Silver
│   └── verify_recovery.sh      # ungraceful crash → restart → assertions
├── scripts/
│   └── inspect_delta.py        # rows, active/physical files, schema, history
├── docs/                       # this file, concepts, runbook, web page, assets
├── PROJECT_MEMORY.md           # agent memory: status, decisions, measured results
├── CHANGELOG.md
├── README.md
└── LICENSE                     # MIT
```
