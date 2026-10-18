# Runbook

Operational steps to **run**, **verify**, **inspect**, **recover**, and **reset** the
pipeline. Every command below was executed during the build; the raw outputs are in
`docs/assets/`.

Prerequisite: `docker` and standalone `docker-compose` (this host has no `docker compose`
plugin — use `docker-compose`). Run all commands from the repository root.

---

## 1. First run (from a clean clone)

```bash
# Build the Spark image and start the stack (Redpanda, Console, topic-init, Spark)
docker-compose build spark
docker-compose up -d

# Confirm Redpanda is healthy and the topic exists
docker exec p2-redpanda-1 rpk topic list --brokers redpanda:9092
# -> NAME          PARTITIONS  REPLICAS
#    order-events  3           1

# Console UI (should return HTTP 200 and show the topic)
open http://localhost:8080
```

`topic-init` runs once, creates `order-events` (3 partitions, 1 replica), and exits 0.
It is idempotent: re-running `docker-compose up -d` will not fail.

## 2. Produce events

```bash
# 60 s at 50 events/s, reproducible seed
docker-compose run --rm producer python event_producer.py --duration 60 --rate 50 --seed 42
```

Expected shutdown summary (measured run):

```
Producer shutdown summary: emitted=3277 duplicates=259 delayed=276 late_queued=276 delivery_errors=0
```

> `duplicates` = re-sent `event_id`+`event_time`; `late_queued` = events placed in the
> 2-15 min delay buffer; `delayed` = late events actually emitted (also counted on
> shutdown when the buffer drains).

## 3. Stream into Bronze + Silver

```bash
docker-compose run --rm spark spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,io.delta:delta-spark_2.12:3.2.0 \
  /opt/spark/app/stream_ingestion.py
```

Leave it running while you produce events. Stop it with `Ctrl-C` (SIGINT) or
`docker stop <container>` (SIGTERM); both are handled gracefully.
Spark UI: http://localhost:4040 → **Structured Streaming** tab shows micro-batches,
input rows/s, state rows, and the watermark.

## 4. Verify Bronze and Silver

```bash
docker-compose run --rm spark spark-submit --packages io.delta:delta-spark_2.12:3.2.0 \
  /opt/spark/tests/verify_silver_dedup.py

docker-compose run --rm spark spark-submit --packages io.delta:delta-spark_2.12:3.2.0 \
  /opt/spark/tests/verify_bronze_offsets.py
```

Expected (measured):

```
Silver rows 3018 == distinct event_id 3018 == distinct (event_id, time) 3018
partition 0 [0..1055] 1056 | partition 1 [0..1169] 1170 | partition 2 [0..1050] 1051
```

## 5. Gold aggregation + compaction

```bash
docker-compose run --rm spark spark-submit --packages io.delta:delta-spark_2.12:3.2.0 \
  /opt/spark/app/gold_compaction.py

# Independent cross-check that Gold matches Silver
docker-compose run --rm spark spark-submit --packages io.delta:delta-spark_2.12:3.2.0 \
  /opt/spark/tests/verify_gold.py
```

Expected (measured): Gold rows = 60; `verify_gold` PASS
(order_count 1055 = 1055; total_revenue 269805.58 = 269805.58).
Compaction: Bronze active files 3→1, Silver 4→1, Gold 1→1.

## 6. Inspect the lakehouse

```bash
docker-compose run --rm spark spark-submit --packages io.delta:delta-spark_2.12:3.2.0 \
  /opt/spark/scripts/inspect_delta.py
```

Prints, per table: row count, **active files** (from `_delta_log` via `DESCRIBE DETAIL`),
**physical files** on disk, total/avg size, full schema, and recent
`DESCRIBE HISTORY` with operation metrics.

## 7. Fault-tolerance / recovery test

```bash
bash tests/verify_recovery.sh
```

This uses an **isolated** topic `order-events-recovery` and paths
`/lakehouse/{bronze,silver}/recovery`, `/checkpoints/recovery`, so it never touches the
canonical dataset. It starts the producer, starts the stream, `docker kill`s the stream
mid-stream (SIGKILL), restarts it, and asserts:

```
PASS verify_silver_dedup: zero duplicate event_id; total_amount correct
PASS verify_bronze_offsets: contiguous offsets, no gaps or repeats
```

Measured: Silver 4315 rows == distinct event_id; Bronze contiguous
(p0 [0..1681], p1 [0..1808], p2 [0..1423]).

## 8. Reset

```bash
# Stop everything
docker-compose down

# Wipe lakehouse data + checkpoints (they are gitignored, never committed)
rm -rf lakehouse/* checkpoints/*

# Wipe the broker's data too (topics, offsets) if you want a *truly* clean run
docker-compose down -v      # -v removes the named redpanda-data volume
```

> To re-seed from scratch: `docker-compose down -v; rm -rf lakehouse/* checkpoints/*;
> docker-compose up -d`.

## 9. Unit tests (no containers needed)

```bash
python3 tests/test_producer_logic.py     # 5/5 pass
```

---

## Troubleshooting (from the real Bugs & Fixes log)

| Symptom | Cause | Fix |
|---------|-------|-----|
| `docker-compose up` fails to bind 9092 | host port conflict | only `19092` is published; use `redpanda:9092` inside containers, `localhost:19092` from the host |
| Redpanda container shows **unhealthy** | healthcheck command | healthcheck is `rpk topic list --brokers redpanda:9092` |
| Console pod exits: *you must specify at least one broker* | Console `latest` (v3 config schema) rejects `kafka.brokers` | pinned `redpandadata/console:v2.8.4` (see `console.yaml`) |
| Console pod exits: *config filepath is not set* | the image ENTRYPOINT already is the `console` binary; passing the binary again breaks Go flag parsing | `command: ["-config.filepath", "/config/console.yaml"]` only |
| Producer image build fails on `COPY` | build context is `./producer` | Dockerfile copies `requirements.txt` then `.` relative to that context |
| `topic-init` fails on the second `up` | `rpk topic create` errors if the topic exists | `sh -c '… create … || … describe … >/dev/null'` |
| `spark-submit` can't find the Delta/Kafka jars | jars are not baked into the image | pass `--packages …` on every `spark-submit` (first run downloads to `./.ivy2`, which is mounted and cached) |
| Silver has **fewer** rows than Bronze's distinct keys | the watermark is **shorter** than the producer's maximum lateness, so Spark *drops* valid late events (append dedup) | set `WATERMARK` above max lateness (now `20 minutes` > the 15-min delay); diagnose with `scripts/watermark_report.py` |
| Silver has rows that look duplicated after a late duplicate | watermark window vs producer's 2-15 min delay | expected within the window; see the late-data analysis in `CONCEPTS_AND_DESIGN_DECISIONS.md` |
| Streaming job stops before draining the topic | idle supervisor saw a constant per-batch row count (pre-fix) | fixed: `_total_input_rows()` is now monotonic/cumulative |

## Doc-Sync

Whenever code, schema, ports, or measured results change, update in the same change:
`README.md`, `docs/ARCHITECTURE.md`, `docs/CONCEPTS_AND_DESIGN_DECISIONS.md`,
`docs/RUNBOOK.md`, `docs/index.html`, and the source-of-truth `PROJECT_MEMORY.md`
(Measured Results + Doc Sync Checklist).
