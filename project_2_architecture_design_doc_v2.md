# Project 2 Architecture & Design Doc (v2): Real-Time Streaming & Medallion Lakehouse Pipeline (Redpanda + PySpark + Delta Lake)

**Target repository:** https://github.com/saber2164/Real-Time-Streaming-Medallion-Lakehouse-Pipeline-
**Doc version:** 2.0 | **Status:** Ready for execution

> **What changed from v1 (summary):** see [Appendix A: Change Log from v1](#appendix-a-change-log-from-v1). Short version: fixed a port conflict, a dedup-key inconsistency, a broker networking gap, a Gold-layer Z-ORDER mistake, and a filename mismatch; added persistent agent memory, a documentation-sync rule, a concepts/decision-record doc, a showcase web page, and a Git/GitHub workflow.

---

## 0. Operating Rules for the Execution Agent (READ FIRST, EVERY SESSION)

These rules override convenience. They apply from the first command to the final push.

### 0.1 Persistent Memory Protocol

The agent must retain context across the entire build, including across restarts, context resets, and new sessions. The project's memory lives **in the repo**, not in the agent's head.

1. **Create `PROJECT_MEMORY.md` at the repo root in Phase 0, before any other file.**
2. **At the start of every session / before every phase:** read `PROJECT_MEMORY.md` fully, then `git log --oneline -20`, then state in one short paragraph where the project stands and what comes next.
3. **After every meaningful action** (file created, command run, bug found, decision made, test result), append to `PROJECT_MEMORY.md`. Do not batch these up for later.
4. `PROJECT_MEMORY.md` must contain these sections, kept current:

| Section | Contents |
|---|---|
| **Current Status** | Phase, step, what is done / in progress / blocked |
| **Next Actions** | Ordered checklist of the next 3-5 steps |
| **Decisions Log** | Date, decision, alternatives considered, reason (feeds the concepts doc) |
| **Environment Facts** | Versions actually installed (Spark, Delta, Redpanda, Python), ports, paths, OS quirks |
| **Commands That Worked** | Exact verified commands, copy-pasteable |
| **Bugs & Fixes** | Symptom, root cause, fix (feeds the README troubleshooting section) |
| **Measured Results** | Real numbers only: event counts, duplicate counts, file counts before/after `OPTIMIZE`, latencies |
| **Open Questions** | Anything needing the user's input |
| **Doc Sync Checklist** | Which docs are stale relative to the code (must be empty before each commit) |

5. **Never fabricate results.** Every number in any doc or on the web page must come from a command the agent actually ran, recorded under *Measured Results*. If something was not run, write "not yet measured".

### 0.2 Document-Everything Rule

* Every code file starts with a header docstring: purpose, inputs, outputs, how to run, and key design choices.
* Every non-obvious line of Spark/Delta logic gets an inline comment explaining *why*, not just *what*.
* Every decision with a credible alternative gets an entry in the Decisions Log (and later in the concepts doc).
* Every bug hit during the build is logged, because the bugs are interview material.

### 0.3 Doc-Sync Rule ("No Stale Docs")

**After ANY change to code, config, schema, ports, file names, or behavior, update every affected doc in the same commit.** Before each commit, the agent runs this checklist and records the result in `PROJECT_MEMORY.md`:

- [ ] `README.md` (commands, ports, structure, diagram still correct?)
- [ ] `docs/ARCHITECTURE.md` (schemas, layers, grain, diagram)
- [ ] `docs/CONCEPTS_AND_DESIGN_DECISIONS.md` (new decision or alternative?)
- [ ] `docs/RUNBOOK.md` (run / verify / recover steps still valid?)
- [ ] `docs/index.html` (web page claims and numbers still true?)
- [ ] `CHANGELOG.md` (entry added?)
- [ ] `PROJECT_MEMORY.md` (status + next actions updated?)

A commit that changes behavior without touching the relevant docs is not allowed.

### 0.4 Git Rules

* Initialize git in Phase 0; remote = the target repository above.
* **Commit after every completed step**, using Conventional Commits (`feat:`, `fix:`, `docs:`, `test:`, `chore:`).
* Push to `main` at the end of every phase (not only at the very end), so work is never lost.
* `.gitignore` must exclude: `lakehouse/`, `checkpoints/`, `__pycache__/`, `.env`, `*.parquet`, `.DS_Store`, Spark/Ivy caches.
* **Never commit credentials or tokens.** If `git push` fails on authentication, stop, log it under *Open Questions*, and ask the user to authenticate (SSH key, `gh auth login`, or a credential helper). Do not ask the user to paste a token into chat.
* Never force-push or rewrite history without explicit user approval.

### 0.5 Honesty & Verification Rule

A task is "done" only when its verification command has been run and its real output recorded. If a step fails, log it, fix it, and document the fix; do not silently work around it.

---

## 1. Project Overview & Business Value

### Project Title
**Real-Time E-Commerce Clickstream & Order Events Ingestion Lakehouse**

### Business Problem / Value Proposition
Traditional batch ETL creates an operational data lag of hours or days. Fraud Prevention, Inventory Logistics, and Customer Retention teams need low-latency data to detect checkout anomalies, prevent overselling during traffic spikes, and monitor conversion drops.

Streaming introduces specific engineering challenges:
* **Network & Mobile Retries:** Clients resend payloads, producing duplicate events.
* **Late-Arriving Events:** Network latency causes out-of-order arrival, disrupting chronological processing and aggregates.
* **The "Small File Problem":** Continuous micro-batch writes create thousands of tiny files that degrade query performance.

### The Solution & Impact
A **Medallion Lakehouse (Bronze → Silver → Gold)** built with **Docker Compose**, **Redpanda** (Kafka-API-compatible broker, no JVM), **PySpark Structured Streaming**, and **Delta Lake**.

1. **Low-latency ingestion** of order events into a Bronze landing layer.
2. **Event-time watermarking & deduplication** (10-minute watermark) to handle late and duplicate events.
3. **Lakehouse compaction** (`OPTIMIZE` / `ZORDER`) in the Gold layer to merge small files.
4. **100% free & open-source**, runs locally in Docker with no cloud account.

---

## 2. Technical Stack & Architecture

```
+---------------------------------------------------------------------------------------------------+
| LOCAL DOCKER COMPOSE ENVIRONMENT                                                                  |
|                                                                                                   |
|  +-----------------------+         +-----------------------+                                      |
|  | Synthetic Event       |  KAFKA  | Redpanda Broker       |                                      |
|  | Producer (Python)     | ------> | (Kafka API Compatible)|                                      |
|  | - Emits order events  | PRODUCE | Topic: `order-events` |                                      |
|  | - ~10% duplicates     |         | Console UI: :8080     |                                      |
|  | - ~10% late events    |         +-----------------------+                                      |
|  +-----------------------+                     |                                                  |
|                                                | KAFKA CONSUME                                    |
|                                                v                                                  |
|  +---------------------------------------------------------------------------------------------+  |
|  | PySpark Structured Streaming Engine (+ Delta Lake)                                          |  |
|  |                                                                                             |  |
|  |  BRONZE  /lakehouse/bronze/order_events                                                     |  |
|  |   - Append-only raw JSON + Kafka metadata (partition, offset, timestamp)                    |  |
|  |                              |                                                              |  |
|  |                              v  (Delta streaming read)                                      |  |
|  |  SILVER  /lakehouse/silver/order_events                                                     |  |
|  |   - Parsed, typed, 10-min watermark, stateful dedup on (event_id, event_time)               |  |
|  +---------------------------------------------------------------------------------------------+  |
|                                                |                                                  |
|                                                v  BATCH (scheduled / manual)                      |
|  +---------------------------------------------------------------------------------------------+  |
|  | GOLD  /lakehouse/gold/hourly_sales                                                          |  |
|  |  - Hourly revenue / order count / avg freight per seller & payment type                     |  |
|  |  - OPTIMIZE + ZORDER BY (seller_id) for small-file compaction                               |  |
|  +---------------------------------------------------------------------------------------------+  |
+---------------------------------------------------------------------------------------------------+
```

### Core Stack Components
| Component | Choice | Notes |
|---|---|---|
| Containerization | Docker & Docker Compose | Single `docker compose up -d` |
| Event broker | Redpanda (pin a specific version) | Kafka API compatible, single container |
| Stream engine | PySpark 3.5.x | Pin exact version; record in `PROJECT_MEMORY.md` |
| Table format | Delta Lake (`delta-spark` 3.x compatible with the chosen Spark) | Check the Delta/Spark compatibility matrix before pinning |
| Kafka connector | `org.apache.spark:spark-sql-kafka-0-10_2.12:<same as Spark>` | Must match the Spark version |
| Producer | Python 3.10+, `confluent-kafka` | |
| Storage | Local bind-mounted volume (`./lakehouse`, `./checkpoints`) | |

### Networking Requirement (fixes a gap in v1)
Redpanda must expose **two listeners**: an **internal** one (`redpanda:9092`) for containers (Spark, producer) and an **external** one (e.g. `localhost:19092`) for host-side tools. Advertised addresses must match the listener, or clients connect and then fail on metadata redirects. Document this in the compose file and README.

### Port Map (fixes a conflict in v1)
| Service | Port | Note |
|---|---|---|
| Kafka API (internal) | `redpanda:9092` | Containers |
| Kafka API (external) | `localhost:19092` | Host |
| Redpanda Console | `8080` | **v1 used 8081, which collides with Redpanda's built-in Schema Registry** |
| Spark UI | `4040` | Useful for the watermark / state-store demo |

---

## 3. Data Schema & Medallion Layer Definitions

### A. Event Payload Schema (JSON)
```json
{
  "event_id": "evt_9841a32d-32a2-4a0b-bf88-e9231f28b211",
  "order_id": "ord_10842",
  "customer_id": "cust_5501",
  "seller_id": "seller_201",
  "region": "west",
  "event_type": "order_created",
  "price": 149.99,
  "freight_value": 12.50,
  "payment_type": "credit_card",
  "event_time": "2026-10-18T10:15:30Z"
}
```
* `region` was added in v2 because v1 promised "revenue per region" in the business case but had no region field.
* `event_type` ∈ {`order_created`, `order_paid`, `order_cancelled`}. **Revenue and order counts in Gold use `order_created` only** (documented rule, avoids double counting).
* A *duplicate* is a re-send of the **same `event_id` with the same `event_time`** (simulating a client retry). A *late* event keeps its true (old) `event_time` but is produced 2-15 minutes later.
* Use the Kafka message key = `order_id` so all events for an order land in the same partition.

### B. Medallion Table Architecture

1. **Bronze** `/lakehouse/bronze/order_events`
   * Delta, append stream, never mutated (it is the replayable source of truth).
   * Schema: `raw_payload` STRING, `kafka_partition` INT, `kafka_offset` LONG, `kafka_timestamp` TIMESTAMP, `ingest_date` DATE.
   * Partition by `ingest_date` only if volume justifies it; document the choice.

2. **Silver** `/lakehouse/silver/order_events`
   * Delta, append stream with stateful dedup.
   * Schema: `event_id`, `order_id`, `customer_id`, `seller_id`, `region`, `event_type`, `price`, `freight_value`, `payment_type`, `event_time` (TIMESTAMP), `event_date` (DATE), `total_amount` (= `price + freight_value`).
   * **Dedup key: `["event_id", "event_time"]`** with `withWatermark("event_time", "10 minutes")`.
     * v1 deduped on `order_id + event_time`, which wrongly collapses *different* events of the same order that share a timestamp, and ignores the `event_id` the producer deliberately duplicates. `event_id` is the true identity.
     * `event_time` must be in the dedup subset, otherwise Spark cannot expire the state and it grows without bound.
     * Optional comparison: Spark 3.5's `dropDuplicatesWithinWatermark` (handles duplicates whose event_time differs slightly). Document as an alternative, and why the main path doesn't use it, in the concepts doc.
   * Malformed JSON: route unparseable rows to a quarantine path (`/lakehouse/silver/_quarantine`) rather than dropping silently.

3. **Gold** `/lakehouse/gold/hourly_sales`
   * Delta, batch overwrite (idempotent) or MERGE; pick one and justify it.
   * Grain: **1 row per hour per seller_id per payment_type** (region optional as an extra dimension; decide and document).
   * Columns: `hour_start`, `event_date`, `seller_id`, `payment_type`, `hourly_order_count`, `total_revenue`, `avg_freight_cost`.
   * **Optimization:** `OPTIMIZE` with `ZORDER BY (seller_id)`. v1 listed `ZORDER BY (event_date, customer_id)`: `customer_id` does not exist in Gold, and Z-ordering on a partition column is invalid. If partitioning by `event_date`, Z-order on `seller_id` only.

---

## 4. Key Engineering Deliverables & Design Patterns

1. **Watermarking & Late Data Strategy**
   * Distinguish **event time** (when the purchase happened) from **processing time** (when Spark saw it).
   * Demonstrate state-store pruning under `withWatermark()` (screenshot/metrics from the Spark UI Structured Streaming tab or `query.lastProgress`).
   * **Be explicit that the producer delays events by 2-15 minutes while the watermark is 10 minutes**, so some late events (>10 min) fall outside tolerance. Show what happens to them: for dedup-only logic the late row is still written, but its dedup state may already be expired, so a late *duplicate* can slip through. Quantify this with a real measurement and discuss tuning the watermark vs. state size.

2. **Fault Tolerance & Checkpointing**
   * Separate `checkpointLocation` per query (Bronze, Silver), never shared.
   * Verify end-to-end behavior by `docker stop` mid-stream and restart. Assert: no duplicate `event_id` in Silver, Bronze offsets contiguous with no gaps/repeats per partition, query resumes from the checkpoint.
   * Be precise in wording: Delta sink + checkpoint gives **idempotent, effectively-once** writes into Delta; true end-to-end exactly-once also depends on the replayable source and the producer (the producer here is at-least-once, hence the dedup layer).

3. **Small File Problem & Compaction**
   * Measure file count and average file size in each table before and after `OPTIMIZE` (real numbers into `Measured Results`).
   * Explain why micro-batches create small files (one or more files per micro-batch per partition per task).
   * Note `VACUUM` and retention (default 7 days) and why you shouldn't set retention to 0 casually.

4. **Trade-off Narrative: "Why Streaming is a Batch Problem in Disguise"**
   * When streaming adds unnecessary cost vs. when scheduled micro-batches (e.g. `trigger(availableNow=True)` every 15 minutes) are the better fit.
   * Include a cost/complexity comparison table and a decision rubric (latency requirement, volume, team size, failure-recovery needs).

---

## 5. Agent Prompt & Execution Requirements

> **Instructions for the AI Assistant / Execution Agent:**
> Act as a Senior Data Engineer and Distributed Systems Architect. Build Project 2 end to end: working code, tests, documentation, a showcase web page, and a pushed GitHub repository. **Obey Section 0 at all times** (memory file, documentation, doc-sync, git, honesty).

### Target Repository Layout
```
.
├── docker-compose.yml
├── Dockerfile.spark              # Spark + Delta + Kafka connector pinned
├── producer/
│   ├── event_producer.py
│   ├── requirements.txt
│   └── Dockerfile
├── spark/
│   ├── config.py                 # paths, topic, watermark, trigger settings
│   ├── stream_ingestion.py       # Bronze + Silver streaming queries
│   └── gold_compaction.py        # batch aggregate + OPTIMIZE/ZORDER
├── tests/
│   ├── test_producer_logic.py    # unit tests: dup/late ratios, schema validity
│   ├── verify_silver_dedup.py    # asserts zero duplicate event_ids
│   ├── verify_bronze_offsets.py  # asserts no offset gaps/repeats per partition
│   └── verify_recovery.sh        # docker stop/start + assertions
├── scripts/
│   └── inspect_delta.py          # row counts, file counts, history, schema
├── docs/
│   ├── ARCHITECTURE.md
│   ├── CONCEPTS_AND_DESIGN_DECISIONS.md
│   ├── RUNBOOK.md
│   ├── index.html                # showcase web page (GitHub Pages)
│   └── assets/                   # diagrams, screenshots (real ones)
├── PROJECT_MEMORY.md
├── CHANGELOG.md
├── README.md
├── LICENSE                       # MIT unless the user says otherwise
└── .gitignore
```

### Deliverable 1: `docker-compose.yml` (+ `Dockerfile.spark`)
* Redpanda single-node, pinned version, dual listeners (internal `9092`, external `19092`), health check.
* Redpanda Console on **8080**, with a one-shot init container (or documented command) that creates topic `order-events` (e.g. 3 partitions, so offset/partition tests are meaningful).
* Spark container with Delta + Kafka connector pinned and compatible with the Spark version; volumes for `./lakehouse` and `./checkpoints`; port 4040 exposed.
* Producer service (optional profile) so the whole stack can start from one command.
* Use `depends_on` with health conditions. No hardcoded secrets.

### Deliverable 2: `producer/event_producer.py`
* Continuously produces realistic order events to `order-events`, key = `order_id`.
* **~10% duplicates** (re-send same `event_id` + `event_time`).
* **~10% late events** (true old `event_time`, emitted 2-15 minutes later; implement with an in-memory delay buffer, not `sleep`, so throughput isn't blocked).
* CLI/env-configurable: rate (default 50 events/s), dup ratio, late ratio, bootstrap servers, seed (for reproducible tests).
* Clean shutdown on `SIGINT`/`SIGTERM`: flush the buffer, `producer.flush()`, then exit with a summary (emitted / duplicated / delayed counts), which also gets logged to `Measured Results`.
* Delivery callbacks with error logging.

### Deliverable 3: `spark/stream_ingestion.py`
1. Connect to `redpanda:9092`.
2. **Bronze query:** write raw value + Kafka metadata to `/lakehouse/bronze/order_events`, append mode, own checkpoint, explicit `trigger(processingTime=...)`, named query.
3. **Silver query:** `readStream` from the Bronze **Delta** table → parse with an explicit `StructType` (no schema inference) → cast `event_time` to timestamp → `withWatermark("event_time", "10 minutes")` → `dropDuplicates(["event_id", "event_time"])` → add `total_amount`, `event_date` → write to `/lakehouse/silver/order_events`, own checkpoint.
4. Quarantine path for unparseable records.
5. Log `lastProgress` periodically (input rows/s, state rows, watermark) for the docs and web page.
6. Graceful stop on SIGTERM.

### Deliverable 4: `spark/gold_compaction.py`
1. Read Silver (batch), filter `event_type = 'order_created'`.
2. Aggregate per hour/seller/payment type: `hourly_order_count`, `total_revenue`, `avg_freight_cost`.
3. Write to `/lakehouse/gold/hourly_sales` (idempotent: overwrite or MERGE, justified).
4. Run `OPTIMIZE ... ZORDER BY (seller_id)` and print file count / size stats before vs. after. Also optionally compact Bronze/Silver, since they are where the small files actually accumulate; explain why.
5. Show `DESCRIBE HISTORY` output as evidence of the optimize operation.

### Deliverable 5: Tests (new in v2)
* Unit tests for producer ratios and schema validity.
* `verify_silver_dedup.py`: `COUNT(*) == COUNT(DISTINCT event_id)` on Silver.
* `verify_bronze_offsets.py`: per partition, offsets are contiguous with no repeats after recovery.
* `verify_recovery.sh`: run → `docker stop` Spark mid-batch → restart → assertions pass.
* Every test result is recorded in `PROJECT_MEMORY.md`.

### Deliverable 6: `README.md` (interview-ready System Design Doc)
* Mermaid + ASCII architecture diagram.
* Table grain & Medallion layer descriptions.
* Event time vs. processing time explanation with the watermark worked example.
* Detailed section **"Why Streaming is a Batch Problem in Disguise" (Trade-offs & Cost Analysis)**.
* Quickstart, how to run, verify streaming, test failure recovery, inspect Delta tables.
* Troubleshooting (built from the Bugs & Fixes log).
* Link to `docs/` and the live web page.

### Deliverable 7: `docs/CONCEPTS_AND_DESIGN_DECISIONS.md` (the "everything explained" doc)
Written **at the end of the build**, from the Decisions Log and real results. Must be detailed, beginner-readable, and interview-ready. Required contents:

**Part 1: Concepts explained (what, why, how it's used here, a small example for each)**
* Event streaming, brokers, topics, partitions, offsets, consumer groups
* Redpanda vs. Kafka internals (Raft, no ZooKeeper/JVM, Kafka API compatibility)
* Structured Streaming model (unbounded table, micro-batch, triggers, output modes)
* Event time vs. processing time; watermarks; state store; how late data is handled
* Stateful deduplication and why `event_time` must be in the key
* Checkpointing, write-ahead log, offset tracking, recovery
* Delivery semantics: at-most-once / at-least-once / exactly-once, and what this pipeline actually guarantees
* Delta Lake: transaction log (`_delta_log`), ACID, schema enforcement, time travel, `VACUUM`
* Medallion architecture and the purpose of each layer; why Bronze is immutable
* Small file problem, `OPTIMIZE`, Z-ordering, file sizing, `VACUUM`
* Data grain and why Gold's grain was chosen

**Part 2: Alternatives and "why not" (a decision record for each)**
For every row: *what we chose, what else was viable, why it wasn't chosen here, and when the alternative would win.*

| Decision | Chosen | Alternatives to analyze |
|---|---|---|
| Broker | Redpanda | Apache Kafka (KRaft), Pulsar, RabbitMQ, cloud (Kinesis, Event Hubs) |
| Stream engine | Spark Structured Streaming | Apache Flink, Kafka Streams, ksqlDB, Materialize, plain consumer + batch |
| Table format | Delta Lake | Apache Iceberg, Apache Hudi, plain Parquet |
| Dedup approach | `dropDuplicates` + watermark | `dropDuplicatesWithinWatermark`, Delta `MERGE` per micro-batch (`foreachBatch`), dedup at Gold only |
| Bronze content | Raw string + metadata | Parsed Bronze, Avro/Protobuf + Schema Registry |
| Layer wiring | Silver reads Bronze Delta stream | Silver reading Kafka directly, single query fan-out |
| Compaction | Scheduled `OPTIMIZE` | Delta auto-compaction / optimized writes, Liquid Clustering, larger trigger intervals |
| Clustering | Z-ORDER | Partitioning only, Liquid Clustering, none |
| Orchestration | Manual/scheduled scripts | Airflow, Dagster, Prefect, cron |
| Gold transformation | PySpark script | dbt, Spark SQL jobs, materialized views |
| Storage | Local disk | MinIO/S3-compatible, cloud object storage |
| Processing mode | Continuous streaming | `trigger(availableNow=True)` scheduled micro-batches |
| Local env | Docker Compose | Kubernetes/Helm, local installs |

**Part 3: What went wrong and what we learned**: pulled from Bugs & Fixes.
**Part 4: Known limitations and what a production version would add**: monitoring (Prometheus/Grafana), DLQ, schema registry, CI, data quality checks (Great Expectations / Deequ), security, multi-broker replication, backfill strategy, cost tracking.
**Part 5: Interview Q&A**: at least 15 likely questions with concise answers grounded in this project's real numbers.

### Deliverable 8: Showcase Web Page `docs/index.html`
A single self-contained static page (inline CSS/JS, no build step, no external network dependencies except optional web fonts with fallbacks) deployed via **GitHub Pages** (source: `main` branch, `/docs` folder). Requirements:
* Sections: hero + one-line pitch, the problem, architecture diagram (inline SVG or Mermaid rendered statically), Medallion layers, key engineering concerns (dedup, late data, checkpointing, compaction), **real measured results** (events produced, duplicates removed, files before/after `OPTIMIZE`, recovery test outcome), tech stack, trade-offs summary, how to run, links to repo/README/concepts doc.
* Every number comes from `Measured Results` in `PROJECT_MEMORY.md`. No invented metrics. If a metric is missing, omit it.
* Real screenshots (Redpanda Console, Spark UI, Delta history) saved under `docs/assets/` if capturable; otherwise use clean diagrams and say so.
* Responsive (mobile-friendly), accessible (semantic HTML, alt text, sufficient contrast), supports light and dark mode, fast.
* Include meta tags / Open Graph for good link previews.
* Must be updated whenever results or architecture change (Doc-Sync Rule).

### Deliverable 9: GitHub Push & Pages
* Final push to `main` of the target repo; enable Pages from `/docs` (via `gh` CLI or give the user the exact settings steps if auth/permissions prevent it).
* Repo polish: description, topics (`data-engineering`, `spark`, `delta-lake`, `redpanda`, `kafka`, `streaming`, `lakehouse`, `medallion-architecture`), README badges, pinned-worthy structure.
* Confirm `git status` is clean, `git log` shows meaningful commit history, and the remote matches the target URL.
* Report the final repo link and live Pages link to the user.

---

## 6. Step-by-Step Roadmap (Weekend Sprint 2)

Each phase ends with: verification run → `PROJECT_MEMORY.md` updated → doc-sync checklist → commit → push.

* **Phase 0: Bootstrap & Memory**
  * Create folder structure, `git init`, add remote, create `PROJECT_MEMORY.md`, `CHANGELOG.md`, `.gitignore`, skeleton `README.md`. Record environment facts (Docker version, free RAM/disk, OS).
  * *Verify:* `git remote -v` correct; memory file exists and is committed.

* **Phase 1: Container Stack**
  * Write `docker-compose.yml`, `Dockerfile.spark`; `docker compose up -d`; create topic.
  * *Verify:* Redpanda healthy; Console at `http://localhost:8080` shows topic `order-events`; a test message round-trips (`rpk topic produce/consume`).

* **Phase 2: Producer**
  * Build `event_producer.py` + unit tests; run for a fixed duration with a fixed seed.
  * *Verify:* shutdown summary shows ~10% duplicates / ~10% delayed; messages visible in Console.

* **Phase 3: Streaming Ingestion**
  * Build and submit `stream_ingestion.py`; let it run with the producer on.
  * *Verify:* micro-batches in Spark UI; Bronze and Silver Delta tables growing; `verify_silver_dedup.py` passes; capture watermark/state metrics.

* **Phase 4: Gold & Compaction**
  * Build and run `gold_compaction.py`.
  * *Verify:* Gold rows match an independent SQL check on Silver; file count/size before vs. after `OPTIMIZE` recorded; `DESCRIBE HISTORY` captured.

* **Phase 5: Fault Tolerance Verification**
  * Run `verify_recovery.sh`: stop Spark mid-stream, restart, run assertions.
  * *Verify:* zero duplicate `event_id` in Silver; no Bronze offset gaps/repeats; outcome logged honestly (including any failure and the fix).

* **Phase 6: Documentation Completion**
  * Finish `README.md`, `ARCHITECTURE.md`, `RUNBOOK.md`, and the full `CONCEPTS_AND_DESIGN_DECISIONS.md`. Run a full doc-sync pass; make sure every command in the docs actually works by executing them fresh from a clean clone.

* **Phase 7: Showcase Web Page**
  * Build `docs/index.html` from real results; test locally in a browser at mobile and desktop widths, in light and dark mode.

* **Phase 8: Publish**
  * Final push, enable GitHub Pages, repo description/topics, verify the live page loads, hand the user the links.

---

## 7. Definition of Done

- [ ] `docker compose up -d` brings up the stack from a clean clone using only README instructions
- [ ] Producer emits ~10% duplicates and ~10% late events (measured)
- [ ] Silver contains zero duplicate `event_id` after normal run **and** after recovery test
- [ ] Gold matches an independent check; `OPTIMIZE` reduces file count (measured, before/after)
- [ ] All docs consistent with the code (doc-sync checklist empty)
- [ ] `PROJECT_MEMORY.md` current and committed
- [ ] `CONCEPTS_AND_DESIGN_DECISIONS.md` complete with alternatives and "why not"
- [ ] Web page live on GitHub Pages with only real numbers
- [ ] Everything pushed to https://github.com/saber2164/Real-Time-Streaming-Medallion-Lakehouse-Pipeline-
- [ ] Final report to the user: what was built, what was measured, known limitations, links

---

## Appendix A: Change Log from v1

| # | v1 issue | v2 fix | Why it matters |
|---|---|---|---|
| 1 | Console on port `8081` | Moved to `8080` | 8081 is Redpanda's Schema Registry default; the stack would conflict or confuse |
| 2 | Single `9092` listener, with Spark reaching `redpanda:9092` | Dual internal/external listeners | Host tools and containers need different advertised addresses |
| 3 | Dedup on `order_id + event_time` | Dedup on `event_id + event_time` | `order_id` collapses legitimately different events; `event_id` is the true identity |
| 4 | `ZORDER BY (event_date, customer_id)` on Gold | `ZORDER BY (seller_id)` | `customer_id` isn't in Gold; Z-order can't use partition columns |
| 5 | Roadmap used `spark_ingestion.py`, deliverable used `stream_ingestion.py` | Standardized on `stream_ingestion.py` | Avoids a broken command |
| 6 | "Revenue per region" with no region field | Added `region` to schema | Business case and data model now agree |
| 7 | Watermark (10 min) vs. delays (2-15 min) unexplained | Explicit late-data analysis requirement | Shows real understanding of watermark limits |
| 8 | "Exactly-once" stated loosely | Precise delivery-semantics wording | Interviewers probe this exactly |
| 9 | No tests | Added unit + verification scripts | Claims become evidence |
| 10 | Compaction only on Gold | Also discuss/measure Bronze and Silver | Small files accumulate where streaming writes, not in batch-written Gold |
| 11 | No memory/state handling | `PROJECT_MEMORY.md` protocol | Build survives context loss and resumes correctly |
| 12 | Docs written once | Doc-Sync Rule + checklist | No stale docs |
| 13 | No deep-dive doc | Concepts & Design Decisions doc | Learning and interview prep |
| 14 | No showcase | GitHub Pages web page | Portfolio visibility |
| 15 | No Git workflow | Commit/push rules, repo target, auth handling | Safe, traceable delivery |
| 16 | Versions unpinned | Pin Redpanda, Spark, Delta, connector versions | Reproducibility |
