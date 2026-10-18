# Concepts & Design Decisions

The "everything explained" companion to this project: what each technology is, why it is
used here, every alternative we considered, what actually went wrong, and interview-style
Q&A grounded in the **real measured numbers** (all from
[`../PROJECT_MEMORY.md`](../PROJECT_MEMORY.md); raw evidence in `assets/`).

Written for a reader who is new to streaming but wants to reach interview depth.

---

## Part 1 — Concepts explained

### 1.1 Event streaming, brokers, topics, partitions, offsets, consumer groups

**What.** A broker stores an append-only, ordered **log** per **topic**. A topic is split
into **partitions** for parallelism; each record in a partition gets a monotonically
increasing **offset**. Producers append; consumers read at their own pace and track their
position (offset). A **consumer group** lets several consumers split a topic's partitions
between them, each partition owned by exactly one consumer in the group.

**Why it matters here.** Partition count = maximum read parallelism, and it defines the
unit of ordering. We created `order-events` with **3 partitions, 1 replica**.

**How it's used.** The producer keys every message by `order_id`, so all events for one
order land in the **same partition** (same key → same partition hash) and therefore keep a
per-order ordering. `verify_bronze_offsets.py` proves offsets are contiguous per partition.

**Example (real).** After the measured run: partition 0 offsets `0..1055` (1056 records),
partition 1 `0..1169` (1170), partition 2 `0..1050` (1051); total **3277** = exactly what
the producer emitted.

### 1.2 Redpanda vs. Kafka internals

**What.** Redpanda is a Kafka-API-compatible broker written in C++ with a **Raft**
consensus engine built in. Apache Kafka (and Kafka in **KRaft** mode) is Java-based; older
Kafka needed ZooKeeper for metadata.

**Why it matters here.** Because Redpanda speaks the Kafka wire protocol, **unmodified**
Kafka clients work against it: PySpark's `spark-sql-kafka-0-10` connector and the Python
`confluent-kafka` producer both talk to `redpanda:9092` with no code changes.

**How it's used.** Redpanda single node, dual listeners (internal `9092`, external `19092`),
healthchecked via `rpk topic list`.

**Example.** The whole pipeline (Spark, producer) points at `redpanda:9092`; the host uses
`localhost:19092` for `rpk`. Swap in Apache Kafka and only the image/address change.

### 1.3 Structured Streaming model (unbounded table, micro-batch, triggers, output modes)

**What.** Structured Streaming treats a stream as an **unbounded table** to which rows are
continuously appended. Each **trigger** fires a **micro-batch**: Spark reads the new rows,
runs the same DataFrame plan, and writes the result. **Output modes**: `append` (only new
rows), `update`, `complete`.

**Why it matters here.** Bronze and Silver are append-only facts, so `append` mode is
correct and cheapest.

**How it's used.** `writeStream … .trigger(processingTime="5 seconds").outputMode("append")`
for both Bronze and Silver, each named (`bronze_order_events`, `silver_order_events`) so the
Spark UI is readable.

**Example.** Every 5 s Spark reads new Kafka/Bronze records and appends a Delta commit; on
the Spark UI → *Structured Streaming* you can watch `numInputRows` per micro-batch.

### 1.4 Event time vs. processing time; watermarks; state store; late data

**What.** **Event time** = when the event actually happened (`event_time` in the payload).
**Processing time** = when Spark saw it. These differ because of network/queue/producer
delays. A **watermark** is a moving bound: "I have (probably) seen everything with
`event_time ≥ max_event_time_seen − watermark`". Spark uses the watermark to **evict older
state**, which is what keeps stateful operations from growing forever. Data that arrives
after the watermark for its event time is **late data**.

**Why it matters here.** Dedup is stateful, so without a watermark the state store would
grow without bound. We set the watermark to **20 minutes**.

**How it's used.** `withWatermark("event_time", "20 minutes")` on the Silver query.

**Why 20 minutes, not 10 (measured).** In an `append` dedup, Spark does **not** merely evict
state for records below the watermark — it **drops** them. That distinction cost us real
data: the producer delays events by **2-15 minutes**, so with the original **10-minute**
watermark a *live* run (stream started before the producer) silently lost **137 valid,
unique orders** — Bronze 3277 rows / 3018 distinct keys, but Silver only **2879-2882**, with
`numRowsDroppedByWatermark > 0` on the `dedupe` state operator. Locally the same code lost
nothing, because the producer was run to completion *before* the stream, so the whole backlog
arrived in early batches while the watermark was still 0 — a dangerous way to be "correct".

Sizing the watermark **above the maximum lateness (15 min → 20 min)** removes the loss
entirely: a live run now keeps all 3018 distinct keys with `numRowsDroppedByWatermark = 0`.
Reproduce with `scripts/watermark_report.py`.

**The residual trade-off.** The watermark still bounds state, and it remains a *tuning knob*.
Lowering it shrinks state but begins dropping valid late events and lets late *duplicates*
slip through; raising it retains more late data at the cost of a larger state store. We chose
20 min because it is the smallest round value safely above the producer's 15-min maximum
lateness. **Late-data completeness and bounded state are in direct tension; set the
watermark from the workload's measured lateness.**

**Example.** Event with `event_time = 12:00` arriving at 12:09 → within watermark, deduped
normally. The same event arriving at 12:40 → beyond a 20-minute watermark → dropped (which is
exactly why the watermark must cover the latency you actually see).

### 1.5 Stateful deduplication and why `event_time` must be in the key

**What.** `dropDuplicates(keys)` keeps a **state store** entry per key it has seen, to
remember and drop repeats. The keys define identity **and** the state footprint.

**Why not just `event_id`?** With only `event_id`, Spark has no time dimension to **expire**
state — the store grows forever. Adding `event_time` to the key lets the watermark evict
entries older than the watermark.

**Why not `order_id`?** An order legitimately produces several *different* events
(`order_created`, `order_paid`, `order_cancelled`); deduping on `order_id` would wrongly
collapse them.

**How it's used.** `dropDuplicates(["event_id", "event_time"])`.

**Example (real).** Bronze 3277 records → Silver **3018** rows: exactly **259** duplicate
sends removed, matching the producer's `duplicates=259`.

### 1.6 Checkpointing, write-ahead log, offset tracking, recovery

**What.** A **checkpoint** is durable bookkeeping for a streaming query: which offsets /
source versions each micro-batch consumed, and the progress of stateful operators. Spark
writes it atomically (write then rename via a **write-ahead log**), so a crash leaves either
"batch N committed" or "batch N not committed", never a half state.

**Why it matters here.** This is what makes recovery correct: after a crash, the query
resumes from the **last committed** offsets and reprocesses only the in-flight batch.

**How it's used.** A **separate** `checkpointLocation` per query
(`/checkpoints/bronze`, `/checkpoints/silver`, `/checkpoints/silver/_quarantine`). Sharing
a checkpoint between queries corrupts both — so we never do.

**Example (real, measured).** `verify_recovery.sh` `docker kill`s the stream mid-batch,
restarts it, and Bronze offsets remain contiguous (p0 `0..1681`, p1 `0..1808`, p2 `0..1423`)
with Silver showing **zero duplicate `event_id`**.

### 1.7 Delivery semantics: at-most-once / at-least-once / exactly-once

**What.**

- **At-most-once**: may lose messages.
- **At-least-once**: may duplicate, never loses.
- **Exactly-once**: neither loses nor duplicates, end to end.

**What *this* pipeline actually guarantees.**

- The **producer** is **at-least-once** (it even injects duplicates on purpose). Delivery
  errors are logged; `enable.idempotence=true` prevents *broker-side* retry duplicates, but
  the app-level duplicates remain by design.
- The **Delta sink + checkpoint** give **idempotent, effectively-once** writes: a
  reprocessed micro-batch is deduplicated by Delta's transaction log / this query's
  checkpoint, so a retry does not double-write.
- The **Silver dedup layer** turns the at-least-once input into **effectively unique**
  Silver rows.

So: *the source is at-least-once; the lakehouse writes are effectively-once; the dedup layer
makes Silver unique.* We deliberately avoid the loose phrase "exactly-once", because true
end-to-end exactly-once also depends on the replayable source and producer.

**Example.** A micro-batch commits to Delta but the process dies before the checkpoint
advances → on restart Spark replays it → Delta's idempotent write prevents a duplicate row.

### 1.8 Delta Lake: transaction log, ACID, schema enforcement, time travel, VACUUM

**What.** Delta Lake adds a **transaction log** (`_delta_log/*.json`, plus periodic
Parquet checkpoints) on top of plain Parquet. Every write is one log entry; the set of
"active" Parquet files is derived from the log. This gives ACID transactions, schema
enforcement, `time travel`, and metadata operations like `OPTIMIZE`.

**Why it matters here.** The `_delta_log` is *why* `numFiles` (active files) can differ
from the physical files on disk — tombstoned files still exist until `VACUUM`.

**How it's used.** Bronze, Silver, quarantine, and Gold are all Delta tables; extension
`DeltaSparkSessionExtension` + `DeltaCatalog` are configured in every job.

**Example (real).** After `OPTIMIZE`, `DESCRIBE DETAIL` reports Silver **1 active file
(128,692 bytes)**, while `ls` shows **7 physical** Parquet files (the replaced ones await
`VACUUM`).

### 1.9 Medallion architecture; why Bronze is immutable

**What.** Bronze = raw landing, Silver = cleansed/conformed, Gold = business aggregates.

**Why Bronze immutable.** Replayability and auditability: Bronze is the raw record of what
the broker delivered (plus partition/offset/timestamp lineage). If Silver logic changes,
you rebuild Silver by replaying Bronze — no need to re-read the broker, and you can prove
exactly which source record produced which Silver row.

**How it's used.** Bronze stores `raw_payload` verbatim + Kafka metadata and is never
updated. Silver reads Bronze as a **stream** and derives typed, deduped rows.

**Example.** `raw_payload` is the literal JSON string; `kafka_partition`/`kafka_offset`
let `verify_bronze_offsets.py` prove completeness.

### 1.10 The small file problem, OPTIMIZE, Z-ordering, file sizing, VACUUM

**What.** Each micro-batch writes one or more files per partition per task. At 5-second
triggers that means **many tiny Parquet files**, which hurts read planning and per-file
overhead. `OPTIMIZE` **compacts** small files into larger ones; `ZORDER BY (col)` also
co-locates related rows so queries filtering on `col` read fewer files. `VACUUM` physically
deletes files the log no longer references, after a **retention** window (default 7 days).

**Why it matters here.** Bronze and Silver are the layers that accumulate small files
(streaming writers). Gold is written by a single batch job, so it starts well-sized.

**How it's used.** `gold_compaction.py` runs `OPTIMIZE … ZORDER BY (seller_id)` on Gold and
plain `OPTIMIZE` on Bronze and Silver, logging before/after active-file counts.

**Example (real).**

| Table | Active files before | Active files after | bytes (after) |
|-------|--------------------:|-------------------:|--------------:|
| Bronze | 3 | **1** | 210,156 |
| Silver | 4 | **1** | 128,692 |
| Gold | 1 | **1** | 4,142 |

**Why we do NOT set `VACUUM` retention to 0.** Old files are what make **time travel** and
readers on older snapshots work; aggressive vacuuming breaks concurrent readers and removes
your safety net. Ship with the default (7 days) and only lower it deliberately.

### 1.11 Data grain and why Gold's grain was chosen

**What.** **Grain** = what one row represents. Gold's grain is **one row per
`(hour_start, seller_id, payment_type)`**.

**Why.** The business questions are "revenue and order volume per seller per hour, split by
payment type" — that is exactly the grain. It is coarse enough to be small
(**60 rows** from **3018** Silver rows) and fine enough to slice by seller/payment.

**Why only `order_created`?** Counting `order_paid`/`order_cancelled` too would
double-count an order; revenue is realised at creation in this dataset. (Measured: 3018
Silver rows → **1055** `order_created` → 60 Gold groups.)

**Example.** `(2026-10-18 12:00, seller_007, credit_card) → count=3, revenue=412.50`.

---

## Part 2 — Alternatives and "why not"

Each row: **what we chose**, what else was viable, **why not here**, and **when the
alternative wins**.

### Broker — Redpanda (chosen)
- **Alternatives:** Apache Kafka (KRaft), Apache Pulsar, RabbitMQ, cloud (AWS Kinesis, Azure Event Hubs).
- **Why not Kafka:** identical API but needs a JVM and (historically) ZooKeeper; heavier for a
  single-node local demo.
- **Why not Pulsar/RabbitMQ:** different APIs/tooling; overkill for a Kafka-ecosystem project.
- **Why not Kinesis/Event Hubs:** no cloud account allowed here; also cloud-lock.
- **When the alternative wins:** Kafka for a huge, battle-tested ecosystem and managed
  offerings (Confluent/MSK/Confluent Cloud); Pulsar for multi-tenancy + tiered storage;
  RabbitMQ for task queues rather than event logs; cloud services when you want zero ops and
  are already on that cloud.

### Stream engine — Spark Structured Streaming (chosen)
- **Alternatives:** Apache Flink, Kafka Streams, ksqlDB, Materialize, plain consumer + batch.
- **Why not Flink:** excellent true-streaming engine, but a separate runtime/skillset; here we
  wanted one engine for streaming **and** batch (the Gold job is the same PySpark).
- **Why not Kafka Streams/ksqlDB:** JVM/DSL-bound, no Delta integration, weaker for the
  batch re-aggregation.
- **Why not plain consumer + cron batch:** loses the streaming demonstration and low latency.
- **When the alternative wins:** Flink for millisecond latency, event-time-heavy CEP, or
  very large keyed state; Kafka Streams/ksqlDB for JVM shops wanting minimal infra; plain
  batch when minutes of latency are fine (see the trade-off section in the README).

### Table format — Delta Lake (chosen)
- **Alternatives:** Apache Iceberg, Apache Hudi, plain Parquet.
- **Why not Iceberg:** strong (arguably better) catalog/partition-evolution story, but Delta's
  `OPTIMIZE ZORDER`, `DESCRIBE HISTORY`, and Spark integration are simplest for this demo.
- **Why not Hudi:** more involved ingest tooling for a small project.
- **Why not plain Parquet:** no ACID, no `_delta_log`, no time travel, no idempotent sink.
- **When the alternative wins:** Iceberg for multi-engine (Trino/Flink/Spark) catalogs and
  hidden partitioning; Hudi for heavy upsert/CDC workloads; plain Parquet for append-only
  data with no correctness requirements.

### Dedup approach — `dropDuplicates` + watermark (chosen)
- **Alternatives:** `dropDuplicatesWithinWatermark`, Delta `MERGE` per micro-batch
  (`foreachBatch`), dedup only at Gold.
- **Why not `dropDuplicatesWithinWatermark`:** cleaner semantics, but requires Spark 3.5+ and
  was chosen against to keep the classic, widely-understood `dropDuplicates` in play.
- **Why not per-batch `MERGE`:** correct and flexible, but heavier (a MERGE per micro-batch)
  and more code; `dropDuplicates` is native and cheaper here.
- **Watch the watermark vs. lateness:** with `append` dedup, records below the watermark are
  *dropped*, not just evicted. A watermark shorter than the producer's 2-15 min lateness lost
  137 valid rows on a live run; we size it at 20 min instead (see §1.4).
- **Why not Gold-only dedup:** would leave duplicates in Silver (the cleanse layer) and still
  require stateful dedup somewhere.
- **When the alternative wins:** `MERGE` when dedup keys are updated (upserts), when you must
  dedup against the *existing* table (not just the stream), or when you need SCD logic.

### Bronze content — raw string + metadata (chosen)
- **Alternatives:** parsed Bronze; Avro/Protobuf + Schema Registry.
- **Why not parsed Bronze:** parsing in Bronze throws away the raw record and couples landing
  to a schema; raw Bronze lets you re-parse later.
- **Why not Avro/Protobuf + registry:** adds infra and dependencies for a demo; JSON is fine.
- **When the alternative wins:** schema-registry + binary formats at high volume or when you
  need strict cross-team contracts and compatibility enforcement.

### Layer wiring — Silver reads the Bronze Delta stream (chosen)
- **Alternatives:** Silver reads Kafka directly; a single query fan-out to both layers.
- **Why not Silver-direct-from-Kafka:** two Kafka consumers → duplicated offset ownership and
  coordination; and Silver could not be rebuilt from Bronze alone.
- **Why not single-query fan-out:** couples rate-limiting and replays, harder to reason about.
- **When the alternative wins:** Silver-direct when you want the lowest possible end-to-end
  latency and do not need a replayable Bronze.

### Compaction — scheduled `OPTIMIZE` (chosen)
- **Alternatives:** Delta auto-compaction / optimized writes; Liquid Clustering; larger
  trigger intervals.
- **Why not auto-compaction only:** it helps opportunistically but is not a substitute for a
  deliberate compaction job with metrics.
- **Why not Liquid Clustering:** newer feature; `OPTIMIZE ZORDER` is the well-understood
  choice here.
- **Why not just bigger triggers:** fewer files but higher latency; doesn't fix historical
  small files.
- **When the alternative wins:** auto-compaction/optimized writes to reduce ops burden;
  Liquid Clustering when clustering keys change over time.

### Clustering — Z-ORDER on `seller_id` (chosen)
- **Alternatives:** partitioning only; Liquid Clustering; none.
- **Why `seller_id`:** it is the most common Gold filter; `customer_id` isn't in Gold and
  `event_date` would be a partition column (Z-order can't use partition columns).
- **When the alternative wins:** partitioning when a low-cardinality column is filtered
  constantly; Liquid Clustering for evolving keys.

### Orchestration — manual/scheduled scripts (chosen)
- **Alternatives:** Airflow, Dagster, Prefect, cron.
- **Why not here:** a weekend single-node demo; the Gold job is idempotent so a cron entry is
  enough.
- **When the alternative wins:** multi-job DAGs, backfills, retries, and SLAs → Airflow/
  Dagster/Prefect; a single box → cron.

### Gold transformation — PySpark script (chosen)
- **Alternatives:** dbt, Spark SQL jobs, materialized views.
- **Why not dbt:** dbt shines with a warehouse + SQL models + tests/lineage; here Spark's
  Delta writer is the natural batch engine.
- **When the alternative wins:** dbt when analysts own transformation logic on a warehouse and
  you want lineage/testing/docs out of the box.

### Storage — local disk (chosen)
- **Alternatives:** MinIO/S3-compatible, cloud object storage.
- **Why not object storage:** no cloud account; local bind mounts keep it runnable anywhere.
- **When the alternative wins:** anything production — object storage + a catalog for
  durability, separation of compute/storage, and scale.

### Processing mode — continuous streaming (chosen)
- **Alternatives:** `trigger(availableNow=True)` scheduled micro-batches.
- **Why not availableNow:** the point of the project is to demonstrate continuous streaming
  semantics (watermarks, state, micro-batches).
- **When the alternative wins:** when minutes of latency are acceptable and you'd rather pay
  only for scheduled runs (see "Why streaming is a batch problem in disguise").

### Local environment — Docker Compose (chosen)
- **Alternatives:** Kubernetes/Helm, local installs.
- **Why not K8s:** huge overhead for one box; Compose starts the whole stack in one command.
- **When the alternative wins:** real deployments (K8s/Helm), or managed services.

---

## Part 3 — What went wrong and what we learned

Pulled from the Bugs & Fixes log. The *bugs are the interview material*.

| # | Symptom | Root cause | Fix / lesson |
|---|---------|-----------|--------------|
| 1 | Host port 9092 conflict on `up` | a single `9092:9092` mapping | dual listeners: `redpanda:9092` internal, `localhost:19092` external. *Advertised addresses must match the network each client is on.* |
| 2 | `docker.redpanda.com` unreachable | registry blocked from the host | switched to Docker Hub `redpandadata/*` images |
| 3 | Redpanda marked unhealthy | `rpk cluster health --dry-until-ready` failing | healthcheck = `rpk topic list --brokers redpanda:9092` |
| 4 | `openjdk-11/17` missing in the base image | Debian trixie package set | used `openjdk-21-jre-headless` |
| 5 | `delta-storage==3.2.0` not on PyPI | it is bundled inside `delta-spark`, not a standalone PyPI package | dropped the separate pip install. *Rely on the resolved transitive deps.* |
| 6 | Console: *you must specify at least one broker* | Console `latest` (v3) rejects the `kafka.brokers` YAML key | pinned `redpandadata/console:v2.8.4`. *Pin versions; `latest` is a moving target.* |
| 7 | Console: *config filepath is not set* | the image ENTRYPOINT is already `./console`; compose `command` re-invoked the binary and Go's flag parser stopped at the first positional arg | `command: ["-config.filepath", "/config/console.yaml"]` only |
| 8 | Producer image build failed | build context was `./producer` but the Dockerfile did `COPY producer/ /app/`; also `pip install -r requirements.txt` ran before the file was copied | copy `requirements.txt` first, install, then `COPY . /app/`. *Docker contexts are relative to the context root, and layer order matters for cache.* |
| 9 | `topic-init` failed on the second `up` | `rpk topic create` errors if the topic exists (no `--if-not-exists` here) | `sh -c '… create … || … describe … >/dev/null'` → idempotent init |
| 10 | Producer never stopped with `--duration` | the duration check wasn't enforced in the emission loop | break when `(now − start) ≥ duration` |
| 11 | Producer `delayed` double-counted | late events counted at enqueue *and* at emit | `late_queued` at enqueue; `delayed` only at emit |
| 12 | Env-var defaults ignored | CLI `default=None` overrode the env value | defaults read `os.getenv(...)`; flags still win |
| 13 | Quarantine nested inside the Silver table | default path was `<SILVER>/…/_quarantine` | moved to sibling `/lakehouse/silver/_quarantine`. *Never nest a table inside another table's directory.* |
| 14 | Silver read from Kafka, not Bronze | two Kafka read streams | Silver `readStream`s the Bronze Delta table — one Kafka consumer only |
| 15 | Confusing file counts after `OPTIMIZE` | physical files persist until `VACUUM` | report **active** files from `_delta_log` (`DESCRIBE DETAIL`), not `ls` |

---

## Part 4 — Known limitations & what production would add

This is a **single-node, local, demonstration** build. To productionise:

- **Monitoring/alerting:** Prometheus + Grafana for micro-batch latency, input rows/s, state
  size, watermark lag; Spark's `StreamingQueryProgress` → metrics exporter. (Today: logs only.)
- **Dead-letter queue:** the quarantine Delta path is file-based; production wants a proper
  DLQ topic + reprocessing tooling + alerting on quarantine growth.
- **Schema registry + contract enforcement:** Avro/Protobuf with compatibility checks instead
  of untyped JSON.
- **CI/CD:** run `tests/test_producer_logic.py` + the verifiers in CI on every push.
- **Data quality:** Great Expectations / Deequ expectations (null rates, ranges, referential
  checks) as gates, not just post-hoc counts.
- **Security:** TLS/SASL on the broker, secrets management, network policies, least-privilege
  IAM for object storage. (Today: no auth — acceptable only because it's local.)
- **Multi-broker replication:** 3+ brokers, replication factor ≥ 3, rack awareness.
- **Backfill strategy:** since Bronze is immutable and replayable, backfill Silver/Gold by
  replaying Bronze from a chosen version (`startingVersion`) — productionise as a rebuild job.
- **Cost tracking:** bytes scanned/written, cluster-hours, storage growth.
- **Orchestration:** Airflow/Dagster DAG with retries, SLAs, and lineage for the Gold refresh.
- **True exactly-once framing:** document the producer's at-least-once contract and keep the
  dedup layer; if the producer can be made transactional, revisit.

---

## Part 5 — Interview Q&A (grounded in this project's numbers)

**Q1. Why is Bronze raw and immutable?**
It is the replayable source of truth: JSON landed verbatim plus `kafka_partition`,
`kafka_offset`, `kafka_timestamp`. Silver can be rebuilt from Bronze without re-reading the
broker — e.g. our Silver 3018 rows are derived from Bronze 3277 rows by a dedup rule that we
can change and replay.

**Q2. What is the difference between event time and processing time here?**
`event_time` is when the order happened; processing time is when Spark saw it. The producer
emits delayed events 2-15 minutes after their true time, which is exactly the gap between the
two clocks that watermarks have to absorb.

**Q3. Why `["event_id","event_time"]` and not just `event_id`?**
`event_id` is the true identity, but `event_time` must be in the key so the **watermark can
expire** dedup state; otherwise the state store grows unbounded. Deduping on `order_id` would
wrongly collapse `order_created`/`order_paid`/`order_cancelled`.

**Q4. What does your watermark actually guarantee?**
With a 20-minute watermark, everything with `event_time ≥ max_seen − 20min` is presumed seen.
In an `append` dedup Spark **drops** records below the watermark, so the real guarantee is:
an event arriving more than 20 minutes after its `event_time` is **lost**. We measured this —
a 10-minute watermark lost 137 valid orders on a live run — and set 20 min to exceed the
producer's 15-min maximum lateness (`numRowsDroppedByWatermark = 0`).

**Q5. How many duplicates did dedup remove, and how do you know?**
Bronze 3277 → Silver 3018 = **259** removed, exactly the producer's `duplicates=259`.
`verify_silver_dedup.py` asserts `COUNT(*) = COUNT(DISTINCT event_id) = COUNT(DISTINCT
(event_id, event_time))` = 3018, that **Silver retains every distinct Bronze key** (the check
that catches silent late-data loss), and zero bad `total_amount`.

**Q6. What delivery guarantee does the pipeline give?**
The producer/broker are **at-least-once**; the Delta sink + checkpoint give **idempotent,
effectively-once** writes; the Silver dedup layer makes Silver **unique on `event_id`**. I
avoid saying "exactly-once end to end" because that also depends on the producer.

**Q7. How did you prove fault tolerance?**
`verify_recovery.sh` `docker kill`s the stream mid-batch, restarts it, and then: Silver =
4315 rows = 4315 distinct `event_id`, and Bronze offsets are contiguous per partition
(p0 `0..1681`, p1 `0..1808`, p2 `0..1423`) — no gaps, no repeats.

**Q8. Why separate checkpoints per query?**
Sharing a checkpoint between the Bronze and Silver queries corrupts both because each expects
its own source-progress and state layout. We use `/checkpoints/bronze`,
`/checkpoints/silver`, and `/checkpoints/silver/_quarantine`.

**Q9. What is the small file problem and did you fix it?**
Every micro-batch writes small files; over a run that fragments the table. `OPTIMIZE` compacted
Bronze from 3 → 1 active file and Silver from 4 → 1 (one run each), and we Z-order Gold on
`seller_id` for the most common filter.

**Q10. Why do physical file counts go *up* after `OPTIMIZE`?**
`OPTIMIZE` rewrites into new files and **tombstones** the old ones; they are only removed by
`VACUUM` after the retention window (default 7 days). So `ls` showed Silver 7 files while
`DESCRIBE DETAIL` showed **1 active file** (128,692 bytes). Active files from `_delta_log` are
what govern read cost.

**Q11. Why `mode("overwrite")` for Gold instead of `MERGE`?**
Gold is a fully derived aggregate recomputed from Silver, so overwrite is idempotent and
simple. `MERGE` is better when you need incremental updates or late-arriving corrections.

**Q12. Why Z-order on `seller_id` and not `event_date`/`customer_id`?**
`customer_id` isn't in Gold, and `event_date` would be a partition column — and Z-ordering on a
partition column is invalid. `seller_id` is the top Gold filter and is not a partition column.

**Q13. Why does the Gold job filter `event_type = 'order_created'`?**
To avoid double-counting an order: 3018 Silver rows contain 1055 `order_created` events, which
aggregate to 60 Gold groups; including paid/cancelled would inflate counts/revenue.

**Q14. Why does Silver read Bronze instead of Kafka directly?**
One Kafka consumer (Bronze) avoids duplicated offset ownership; Silver replays Bronze, which
is replayable and auditable, and Kafka rate-limiting/replays stay in one place.

**Q15. When would you *not* use streaming here?**
When minutes of latency are acceptable: `trigger(availableNow=True)` every 15 minutes gives
most of the freshness at a fraction of the cost/complexity. We chose continuous streaming to
demonstrate watermarks, state, and micro-batches.

**Q16. How would you catch a late duplicate that slipped past the watermark?**
Raise the watermark (more state, catches more late dups), or dedup against the *table* with a
`MERGE`/`dropDuplicatesWithinWatermark` rather than only within the stream. Note the flip side
we hit: making the watermark *smaller* than the actual lateness silently **drops** valid
events, so always size it from measured lateness.

**Q17. What's your state store doing, concretely?**
It holds one entry per `(event_id, event_time)` seen within the watermark window so repeats can
be dropped. The Spark UI *Structured Streaming* tab exposes `numRowsTotal`/`numRowsRemoved`
per state operator — the pruning you'd want to watch in production.

**Q18. Why Redpanda and not Kafka?**
Same Kafka API (our PySpark/`confluent-kafka` code is unchanged), but a single C++ binary with
built-in Raft — no JVM/ZooKeeper — which is ideal for a one-command local stack. On a large
managed deployment I'd happily use Kafka (MSK/Confluent) instead.

**Q19. How do you know the Bronze layer is complete (no missing offsets)?**
`verify_bronze_offsets.py` asserts, per partition, `max(offset) − min(offset) + 1 = count(*)`
and `count(*) = count(distinct offset)`. Measured: p0 1056, p1 1170, p2 1051 = 3277.
