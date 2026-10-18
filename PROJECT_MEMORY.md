# Project Memory - Real-Time Streaming & Medallion Lakehouse Pipeline

**Project:** Real-Time E-Commerce Clickstream & Order Events Ingestion Lakehouse
**Doc:** project_2_architecture_design_doc_v2.md (source of truth)
**Target repo:** https://github.com/saber2164/Real-Time-Streaming-Medallion-Lakehouse-Pipeline-

## Current Status

**Phases 0-8 COMPLETE (verified with real measurements).**
The full pipeline has run end to end: producer -> Redpanda -> Bronze Delta ->
Silver Delta (dedup) -> Gold (aggregate + OPTIMIZE/ZORDER), plus an
ungraceful-crash recovery test. All documentation and the showcase page are
written, and the repo is pushed with a live GitHub Pages site.

**Published:**
- Repo: https://github.com/saber2164/Real-Time-Streaming-Medallion-Lakehouse-Pipeline-
- Pages: https://saber2164.github.io/Real-Time-Streaming-Medallion-Lakehouse-Pipeline-/
  (live, HTTP 200, built from `main` `/docs`).
- Cloud demo: `.github/workflows/demo-run.yml` — run the whole pipeline free on
  GitHub Actions from the Actions tab / the showcase page button
  (`docs/CLOUD_DEMO.md`).

**Assessment (2026-10-18, session 2):**

- ✅ Phase 0 bootstrap. ✅ Phase 1 stack (Redpanda healthy, Console :8080 HTTP 200,
  topic `order-events` 3 partitions). ✅ Phase 2 producer (measured summary captured).
- ✅ Phase 3 ingestion: Bronze = 3277 rows, Silver = 3018 rows.
  `verify_silver_dedup` PASS, `verify_bronze_offsets` PASS.
- ✅ Phase 4 Gold: 60 rows; `verify_gold` PASS (matches independent recompute).
  OPTIMIZE reduced active files Bronze 3->1, Silver 4->1, Gold 1->1.
- ✅ Phase 5 fault tolerance: `verify_recovery.sh` PASS after an ungraceful
  (`docker kill`) mid-stream crash on an isolated topic/paths.
- ✅ Bugs fixed this session (see Bugs & Fixes): Console pin, topic-init idempotency,
  producer Dockerfile build, Silver-from-Bronze wiring, `delayed` double count,
  duration enforcement, env-default handling, quarantine path nesting.
- ✅ Phase 6 docs: `README.md`, `docs/ARCHITECTURE.md`,
  `docs/CONCEPTS_AND_DESIGN_DECISIONS.md` (19 Q&A), `docs/RUNBOOK.md`, `LICENSE` (MIT),
  `CHANGELOG.md`.
- ✅ Phase 7 web page: `docs/index.html` (self-contained, light/dark, inline SVG,
  OpenGraph tags) built from measured results.
- ✅ Phase 8 publish: pushed to `main`; GitHub Pages enabled (source `main` /docs);
  repo description + 8 topics set; live page verified HTTP 200.

## Next Actions

Nothing outstanding. Optional future work: add a `VACUUM` step to physically drop
tombstoned files, add a scheduled Gold refresh, or move to `foreachBatch` MERGE if
late-arriving data must update past hours.

## Decisions Log

| Date | Decision | Alternatives Considered | Reason |
|------|----------|------------------------|--------|
| 2026-10-18 | Use `docker-compose` (standalone) instead of `docker compose` plugin | `docker compose` plugin | `docker compose` plugin is not available on this host; standalone `docker-compose` is installed |
| 2026-10-18 | Python pinned at 3.9 (host) / container | Python 3.10+ (doc says 3.10+) | Host has Python 3.9.6; pinning the container to 3.9 keeps the build reproducible here |
| 2026-10-18 | Pin Console to `v2.8.4` | Console `latest` (v3 config schema) | v3 rejects the `kafka.brokers` YAML key the project uses |
| 2026-10-18 | Silver reads the **Bronze Delta** stream | Silver reads Kafka directly | Single Kafka consumer (Bronze only); replayable; avoids double consumption (doc v2 Part 2) |
| 2026-10-18 | Dedup key `["event_id","event_time"]` | `order_id`; `event_id` alone | `event_id` is the true identity; `event_time` bounds dedup state (doc v2 §3.B.2) |
| 2026-10-18 | Quarantine at sibling `/lakehouse/silver/_quarantine` | nested under Silver table | Nested path would be treated as part of the Silver Delta table |
| 2026-10-18 | Gold write mode = **overwrite** | MERGE per batch | Gold is a derived aggregate; overwrite is idempotent and simplest for a scheduled batch |
| 2026-10-18 | Z-order Gold on `seller_id` only | `event_date`+`customer_id` (v1) | `customer_id` absent; Z-order cannot use a partition column (doc v2 §3.C) |
| 2026-10-18 | Recovery test on an isolated topic/paths | reuse `order-events` | Avoids polluting the canonical measured dataset |
| 2026-10-18 | Cloud demo runs on **GitHub Actions** (`workflow_dispatch`), button links to the run page | Static-page one-click via a token/backend | Pages is static and a browser must not hold a secret; Actions is free, and GitHub login is the safety gate. One-click upgrade documented in `docs/CLOUD_DEMO.md` |
| 2026-10-18 | Add `STREAM_MAX_SECONDS` (bounded run) | kill the container in CI | Lets the streaming job drain and exit cleanly so a cloud runner does not hang; default 0 preserves always-on local mode |

## Environment Facts

| Fact | Value |
|------|-------|
| OS | Darwin 27.0.0 (macOS), arm64 |
| Docker | 29.9.1 |
| docker-compose | standalone at /opt/homebrew/bin/docker-compose (v5.6.0) |
| Python (host) | 3.9.6 |
| Spark (host) | not installed (runs in container) |
| Spark image | p2-spark:latest (built) — Python 3.9-slim, JDK 21, pyspark 3.5.3, delta-spark 3.2.0 |
| Redpanda image | redpandadata/redpanda:v24.3.10 |
| Console image | redpandadata/console:v2.8.4 |
| kafka connector | org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 (passed via --packages at runtime) |
| Spark / Scala / Java | Spark 3.5.3 / Scala 2.12 / Java 21.0.12.1 |

## Commands That Worked

- `docker-compose build spark` — builds p2-spark:latest
- `python3 tests/test_producer_logic.py` — 5/5 producer unit tests pass
- `docker-compose run --rm producer python event_producer.py --duration 60 --rate 50 --seed 42` — measured producer run
- `docker-compose run --rm spark spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,io.delta:delta-spark_2.12:3.2.0 /opt/spark/app/stream_ingestion.py`
- `docker-compose run --rm spark spark-submit --packages io.delta:delta-spark_2.12:3.2.0 /opt/spark/app/gold_compaction.py`
- `docker-compose run --rm spark spark-submit --packages io.delta:delta-spark_2.12:3.2.0 /opt/spark/tests/verify_gold.py`
- `docker-compose run --rm spark spark-submit --packages io.delta:delta-spark_2.12:3.2.0 /opt/spark/scripts/inspect_delta.py`
- `bash tests/verify_recovery.sh`
- `docker exec p2-redpanda-1 rpk topic list --brokers redpanda:9092`

## Bugs & Fixes

| Symptom | Root Cause | Fix |
|---------|-----------|-----|
| `docker-compose up` binds `9092:9092` on host | Port 9092 conflict | Removed host mapping; internal port 9092 on the Docker network; only `19092` exposed to host |
| `docker.redpanda.com` registry unreachable | Registry not accessible from this host | Switched to Docker Hub images `redpandadata/redpanda:v24.3.10`, `redpandadata/console` |
| Redpanda healthcheck marked unhealthy | `rpk cluster health --dry-until-ready` unsupported/failing | Healthcheck uses `rpk topic list --brokers redpanda:9092` |
| `openjdk-11/17` not in Debian trixie | Package unavailable | Used `openjdk-21-jre-headless` |
| `delta-storage==3.2.0` not on PyPI | Bundled in delta-spark, not a separate PyPI package | Removed the separate pip install |
| Console `:8080` fatal "you must specify at least one broker" | Console `latest` (v3 schema) rejects `kafka.brokers` | Pinned `redpandadata/console:v2.8.4` |
| Console fatal "config filepath is not set" | Image ENTRYPOINT is `./console`; compose `command` re-invoked the binary, Go flag parser stopped at the first positional arg | `command: ["-config.filepath", "/config/console.yaml"]` only |
| Producer image build fails | Build context is `./producer` but Dockerfile did `COPY producer/ /app/`; `pip install -r requirements.txt` ran before the file was copied | `COPY requirements.txt` first, then pip install, then `COPY . /app/` |
| Topic init fails on re-up | `rpk topic create` errors if the topic exists (no `--if-not-exists` in this version) | `sh -c 'rpk topic create ... || rpk topic describe ... >/dev/null'` |
| Silver read from Kafka, not Bronze | `stream_ingestion.py` opened two Kafka read streams | Silver now `readStream` from the Bronze Delta table (own checkpoint) |
| Producer never stopped on `--duration` | Duration check was not enforced in the loop | Added `(now - start) >= duration` break before generating the next event |
| Producer `delayed` double-counted | Late events counted at queue time and again at emit time | Count `late_queued` at enqueue; `delayed` only when actually emitted |
| Env-var defaults ignored when flag omitted | `default=None` overrode env | Defaults read `os.getenv(...)` so env sets defaults and CLI still wins |
| Quarantine nested inside Silver table path | Default path was `/lakehouse/silver/order_events/_quarantine` | Moved to sibling `/lakehouse/silver/_quarantine` |
| Cloud run: `spark-submit` fails with `/usr/lib/jvm/java-21-openjdk-arm64/bin/java: No such file or directory` | `Dockerfile.spark` hardcoded the arm64 Java path, which does not exist on amd64 CI runners; `ps` also missing | Point `JAVA_HOME` at an arch-neutral symlink `/usr/lib/jvm/java-21`; install `procps` |

## Measured Results

All numbers below are from commands actually run; raw evidence is in `docs/assets/`.

### Phase 2 - Producer (60 s @ 50 ev/s, seed 42)
- `emitted=3277 duplicates=259 delayed=276 late_queued=276 delivery_errors=0`
- Topic high-watermark sum: 1056 + 1170 + 1051 = **3277** (matches emitted).
- Duplicate ratio ≈ 259/3277 = **7.9%** of emitted; late ratio (queued) ≈ 276/3277 = **8.4%**.
  (Producer targets ~10% each, applied to *fresh* events ≈ 2742, giving ~9.4% / ~10.1%.)

### Phase 3 - Bronze & Silver
- Bronze rows = **3277** (one row per produced Kafka record).
- Silver rows = **3018** = 3277 - 259 duplicates.
- `verify_silver_dedup.py` PASS: rows 3018 == distinct event_id 3018 == distinct
  (event_id, event_time) 3018; rows with bad `total_amount` = 0.
- `verify_bronze_offsets.py` PASS (contiguous, no gaps/repeats):
  p0 [0..1055] = 1056, p1 [0..1169] = 1170, p2 [0..1050] = 1051.

### Phase 4 - Gold & Compaction
- Silver rows read = 3018; `order_created` events = **1055**; Gold rows = **60**
  (grain: hour x seller_id x payment_type).
- `verify_gold.py` PASS: independent groups 60 == Gold rows 60; duplicate grain
  keys 0; order_count gold 1055 == independent 1055; total_revenue
  gold 269805.58 == independent 269805.58; mismatched groups 0.
- OPTIMIZE (DESCRIBE HISTORY operation metrics):
  - Bronze: removed 3, added 1 -> active files **3 -> 1** (210,156 bytes).
  - Silver: removed 4, added 1 -> active files **4 -> 1** (128,692 bytes).
  - Gold:   removed 1, added 1 -> active files **1 -> 1** (4,142 bytes).
- Physical parquet files on disk: Bronze 4 -> 5, Silver 6 -> 7 (tombstoned files
  are only removed by VACUUM, default 7-day retention; the active-file count from
  `_delta_log` is what governs read cost).
- Gold file physically 2 (one tombstoned), active 1, 4,142 bytes.

### Phase 5 - Fault Tolerance (ungraceful crash)
- Isolated topic `order-events-recovery` (3 partitions), paths
  `/lakehouse/{bronze,silver}/recovery`, checkpoints `/checkpoints/recovery`.
- Producer: 45 s @ 100 ev/s seed 7. Stream started, ran 12 s, then `docker kill`
  (SIGKILL) mid-stream; restarted; drained.
- `verify_silver_dedup.py` PASS: rows 4315 == distinct event_id 4315 == distinct
  (event_id, event_time) 4315; bad `total_amount` = 0.
- `verify_bronze_offsets.py` PASS (contiguous after crash+restart):
  p0 [0..1681] = 1682, p1 [0..1808] = 1809, p2 [0..1423] = 1424.
- Bronze total 4915 records; Silver 4315 (= 4915 - 600 duplicates removed by dedup).

### Stack / UI
- Redpanda topic `order-events`: 3 partitions, 1 replica.
- Console reachable at http://localhost:8080 (HTTP 200), connected to Kafka.

## Open Questions

- None blocking. GitHub push auth resolved (stored credential); Pages live.
- MIT `LICENSE` used (doc default).

## Doc Sync Checklist

- [x] README.md (full: architecture, grain, measured results, how-to-run, limitations)
- [x] docs/ARCHITECTURE.md
- [x] docs/CONCEPTS_AND_DESIGN_DECISIONS.md
- [x] docs/RUNBOOK.md
- [x] docs/CLOUD_DEMO.md
- [x] docs/index.html (includes "Run it in the cloud" section + button)
- [x] .github/workflows/demo-run.yml
- [x] CHANGELOG.md (Unreleased section current through Phase 7)
- [x] LICENSE (MIT) added
- [x] PROJECT_MEMORY.md (current)

> Doc-sync pass (2026-10-18): every number in README/docs matches
> `Measured Results` above and the raw evidence in `docs/assets/`.
