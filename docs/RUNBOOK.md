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

## 5.1 Live dashboard (optional)

One command starts the full live demo (Redpanda, stream, Gold refresher, dashboard,
continuous producer):

```bash
scripts/live_dashboard.sh
# open http://localhost:8501
scripts/stop_live_dashboard.sh
```

The dashboard is also a page of the published site: `docs/dashboard.html` →
`https://saber2164.github.io/Real-Time-Streaming-Medallion-Lakehouse-Pipeline-/dashboard.html`.
Same file, two hosts: the published copy shows the committed snapshot with a
**Published snapshot** badge, the container rewrites one deploy marker so the
local copy shows live numbers.

> This appends to the real `./lakehouse` and `./checkpoints` tables. Run the reset
> in §8 first if you want the canonical counts to stay exactly 3277/3018/60.

Manual control:

```bash
# Refresher + dashboard only
docker-compose --profile dashboard up -d --build gold-refresh dashboard

# Follow refresher/dashboard logs
docker-compose --profile dashboard logs -f gold-refresh dashboard

# One snapshot from existing tables (rewrites Gold)
docker-compose run --rm spark spark-submit --packages io.delta:delta-spark_2.12:3.2.0 \
  /opt/spark/app/gold_refresh.py --once

# Read-only snapshot: preserve OPTIMIZE/ZORDER and the Gold version
docker-compose run --rm spark spark-submit --packages io.delta:delta-spark_2.12:3.2.0 \
  /opt/spark/app/gold_refresh.py --once --metrics-only

# Contract + accounting identity (host Python, stdlib only)
python3 tests/verify_dashboard_metrics.py lakehouse/dashboard/metrics.json

# Health + raw API
curl -s http://127.0.0.1:8501/healthz
curl -s http://127.0.0.1:8501/api/metrics | python3 -m json.tool | head -40
```

Measured live isolated run: Bronze 2,616 → Silver 2,415 (201 duplicates,
quarantine 0), Gold 96 rows, 863 orders, revenue 219,392.90; Gold version
`0 → 19` across 20 refreshes; `numRowsDroppedByWatermark = 0`. Evidence in
`docs/assets/dashboard_live_*`. Full contract and limits: `docs/DASHBOARD.md`.

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
| Dashboard says **Waiting for data** | Silver/Gold not created yet, or refresher not started | start `stream_ingestion.py`; then `docker-compose --profile dashboard up -d gold-refresh dashboard`; check `docker logs p2-gold-refresh-1` |
| Dashboard shows **Published snapshot** | you opened the GitHub Pages copy, not the local server | use `http://localhost:8501`; the container rewrites the deploy marker to `live` |
| Dashboard says **Stale snapshot** | refresher stopped or snapshot older than the threshold | restart `gold-refresh`; check its logs; verify `lakehouse/dashboard/metrics.json` mtime |
| Dashboard healthy but KPIs empty | `METRICS_PATH` mismatch between refresher and server | both default to `/lakehouse/dashboard/metrics.json`; don't override one side only |
| Port 8501 already in use | an old dashboard container or host process | `docker ps`; stop it or change the Compose port mapping |
| `--metrics-only` fails with *Gold table not found* | Gold has not been built yet | run `gold_compaction.py` first, or omit `--metrics-only` |
| Dashboard cannot write `metrics.json` | host `./lakehouse` permissions | ensure the Spark container can write the bind mount |

## Doc-Sync

Whenever code, schema, ports, or measured results change, update in the same change:
`README.md`, `docs/ARCHITECTURE.md`, `docs/CONCEPTS_AND_DESIGN_DECISIONS.md`,
`docs/DASHBOARD.md`, `docs/RUNBOOK.md`, `docs/CLOUD_DEMO.md`, `docs/index.html`,
`CHANGELOG.md`, and the source-of-truth `PROJECT_MEMORY.md`
(Measured Results + Doc Sync Checklist).
