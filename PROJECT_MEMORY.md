# Project Memory - Real-Time Streaming & Medallion Lakehouse Pipeline

**Project:** Real-Time E-Commerce Clickstream & Order Events Ingestion Lakehouse
**Doc:** project_2_architecture_design_doc_v2.md (source of truth)
**Target repo:** https://github.com/saber2164/Real-Time-Streaming-Medallion-Lakehouse-Pipeline-

## Current Status

**Phases 0-8 COMPLETE (verified with real measurements); Phase 9 (live dashboard) COMPLETE.**
The full pipeline has run end to end: producer -> Redpanda -> Bronze Delta ->
Silver Delta (dedup) -> Gold (aggregate + OPTIMIZE/ZORDER), plus an
ungraceful-crash recovery test and a read-only live dashboard. All documentation
and the showcase page are written, and the repo is pushed with a live GitHub
Pages site.

**Dashboard (2026-10-20):**
- `spark/gold_refresh.py` recomputes Gold every 10 s and writes an atomic
  `metrics.json`; `dashboard/server.py` (stdlib only) serves it on
  `127.0.0.1:8501`; the page polls every 3 s.
- `spark/gold_logic.py` is shared by batch and live Gold, so the numbers cannot
  drift.
- Isolated live run verified: Bronze 2,616 -> Silver 2,415 (201 duplicates,
  quarantine 0), Gold 96, 863 orders, revenue 219,392.90, 20 refreshes
  (Gold version 0 -> 19; committed API snapshot at 11),
  `numRowsDroppedByWatermark = 0`.
- Canonical read-only snapshot preserved Gold version 2: Bronze 3,277 ->
  Silver 3,018, Gold 60, 1,055 orders, revenue 269,805.58.
- Cloud workflow now generates `--once --metrics-only` metrics, starts the
  dashboard, calls `/healthz` + `/api/metrics`, and runs the contract test.

**Site redesign + separate dashboard page (2026-10-21, Phase 10):**
- The dashboard is now its own page: `docs/dashboard.html` -> `/dashboard.html`
  on GitHub Pages. One file, two hosts: the published copy shows the committed
  snapshot with a "Published snapshot" badge; the container rewrites the deploy
  marker (`<meta name="p2-deploy" content="static">` -> `"live"`) so the local
  copy polls `/api/metrics` every 3 s. No URL probing, no console errors.
- The showcase site was redesigned with a shared design system
  (`docs/assets/site.css`): "control room" aesthetic (graphite ground, blueprint
  grid, hairline rules, drafting corner ticks, one signal-amber accent),
  self-hosted Bricolage Grotesque / IBM Plex Sans / IBM Plex Mono (latin subset,
  167 KB, no CDN), inline SVG charts, light/dark themes, `prefers-reduced-motion`
  respected.
- `.dockerignore` added: `lakehouse/`, `checkpoints/` and `.ivy2/` no longer
  enter the Docker build context; the dashboard image now builds from the repo
  root so it ships `docs/dashboard.html` + assets.
- Verified by DOM evaluation (screenshots unavailable - no visible window):
  fonts loaded, no horizontal overflow, 10 sections, zero console messages in
  both live and static mode.

**Published:**
- Repo: https://github.com/saber2164/Real-Time-Streaming-Medallion-Lakehouse-Pipeline-
- Pages: https://saber2164.github.io/Real-Time-Streaming-Medallion-Lakehouse-Pipeline-/
  (live, HTTP 200, built from `main` `/docs`).
- Cloud demo: `.github/workflows/demo-run.yml` — run the whole pipeline free on
  GitHub Actions from the Actions tab / the showcase page button
  (`docs/CLOUD_DEMO.md`).
- Live dashboard: `scripts/live_dashboard.sh` -> http://localhost:8501;
  design/contract/limits in `docs/DASHBOARD.md`.

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
  `docs/CONCEPTS_AND_DESIGN_DECISIONS.md` (25 Q&A), `docs/DASHBOARD.md`,
  `docs/RUNBOOK.md`, `LICENSE` (MIT), `CHANGELOG.md`.
- ✅ Phase 7 web page: `docs/index.html` (self-contained, light/dark, inline SVG,
  OpenGraph tags) built from measured results.
- ✅ Phase 8 publish: pushed to `main`; GitHub Pages enabled (source `main` /docs);
  repo description + 8 topics set; live page verified HTTP 200.
- ✅ Phase 9 live dashboard: read-only stdlib server, shared Gold logic, atomic
  metrics snapshot, 3 s polling, inline SVG charts, Compose `dashboard` profile,
  live isolated run + canonical snapshot + independent verification. Evidence in
  `docs/assets/dashboard_live_*` and `docs/assets/dashboard_canonical_metrics.json`.
- ✅ Phase 10 site redesign + separate dashboard page: `docs/dashboard.html`
  (published snapshot vs. live, one file), shared design system + self-hosted
  fonts, `.dockerignore`, DOM-verified in both modes, zero console errors.

## Next Actions

Nothing outstanding for the project itself. Optional future work: trigger the
updated cloud workflow once to exercise the new dashboard steps end-to-end
(the YAML, Compose config, dashboard server and metrics contract were validated
locally, but no cloud run has been executed since the dashboard was added); add
a `VACUUM` step to physically drop tombstoned files; upgrade the dashboard to
incremental Gold materialization (`MERGE`/`foreachBatch`) and SSE/WebSocket
push; add auth/TLS before exposing it beyond localhost. The live demo
intentionally appends to the canonical tables — reset first if you need the
exact documented counts.

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
| 2026-10-18 | Drain-aware stop via `STREAM_IDLE_TIMEOUT_SECONDS`; `STREAM_MAX_SECONDS` demoted to backstop | a fixed wall-clock cap | Lets a bounded run drain Kafka → Bronze → Silver before exit instead of relying on a wall-clock cap. Does **not** by itself explain the Silver shortfall (that was the watermark — see next row) |
| 2026-10-18 | **Watermark `10 minutes` → `20 minutes`** | shrink the producer's 2-15 min lateness, or accept loss | In `append` dedup Spark **drops** records below the watermark. A live run lost 137 valid unique orders (Bronze 3277/3018 distinct → Silver 2879-2882). Sizing the watermark **above** the max lateness keeps every key (`numRowsDroppedByWatermark=0`) while preserving the realistic late-data stress |
| 2026-10-18 | Make the idle supervisor count input rows **monotonically** | keep summing each query's last micro-batch | `lastProgress["numInputRows"]` is per-batch (constant under steady input), so the supervisor could declare "idle" mid-stream and stop early (observed: Bronze 2574 of 3277). Cumulative counting stops only on a true drain |
| 2026-10-20 | Dashboard reads a **precomputed JSON snapshot**, not Delta | direct Spark/Delta query from the web tier; a BI tool | No Spark/JVM in the serving path, no credentials in the browser, one aggregate per 10 s cycle instead of one scan per tab, and the read-only server cannot mutate the lakehouse. Trade-off: ~10 s staleness |
| 2026-10-20 | Share Gold logic in `spark/gold_logic.py` | duplicate SQL in batch and live jobs | Batch `gold_compaction.py` and live `gold_refresh.py` must produce the same grain/revenue; one function prevents drift |
| 2026-10-20 | Dashboard is an opt-in Compose **profile**, bound to `127.0.0.1:8501` | start it by default; bind `0.0.0.0` | A plain `docker-compose up -d` keeps the original behavior; the dashboard is localhost-only and not exposed on the LAN |
| 2026-10-20 | Poll `/api/metrics` every **3 s**; stdlib server; inline SVG; no CDN | WebSocket/SSE; React + chart library | Simple, robust, offline-capable, tiny supply chain; 3 s is ample for stakeholders. SSE is the upgrade path for sub-second |
| 2026-10-20 | Recompute Gold every **10 s** with `overwrite` | incremental `MERGE`; skip unchanged writes | At this scale a full recompute is simple and idempotent. Known limit: a new Gold version per cycle (live test: 20 cycles -> version 19); at scale skip unchanged Silver versions and use incremental MERGE |
| 2026-10-20 | CI uses `gold_refresh.py --once --metrics-only` **after** compaction | normal `--once`; metrics before compaction | A normal `--once` overwrites Gold and would undo `OPTIMIZE/ZORDER`; `--metrics-only` reads the compacted Gold table and only writes the JSON, preserving the Gold version |
| 2026-10-20 | Live dashboard demo on isolated topic `order-events-dashboard` and `/lakehouse/dashboard-demo/*` | reuse canonical `order-events`/paths | Keeps the canonical 3277/3018/60 dataset untouched while proving continuous refresh; cleaned up afterwards |
| 2026-10-20 | Cloud dashboard assertions are **invariant-based** | hardcoded 3277/3018/60/1055 | Workflow inputs (`duration`, `rate`, `seed`) are configurable; the contract test asserts accounting identities and shapes instead |
| 2026-10-20 | Visual verification via browser DOM + API/logs | screenshot | Browser screenshots were unavailable in the build environment (no visible desktop window); DOM evaluation confirmed Live status, KPIs, chart element counts and zero console errors |
| 2026-10-21 | Dashboard becomes **its own page** (`docs/dashboard.html`), one file for both hosts | keep it inside the showcase page; two separate pages | A static site cannot poll a live API, so the published copy must show a *captured* snapshot. One file + a deploy marker means the live view and the published page cannot drift, and the page always states which mode it is in (no sample data, no "fake live") |
| 2026-10-21 | Deployment detection via `<meta name="p2-deploy">` marker rewritten by the server | probe `/api/metrics` and treat 404 as "static" | Probing produces a console error on every static load and makes the page guess; the marker makes it deterministic and keeps the console clean |
| 2026-10-21 | Site redesign with a shared design system + self-hosted fonts | framework (React/Tailwind), CDN fonts, template look | The project is judged on craft and self-containment. Plain HTML/CSS with Bricolage Grotesque + IBM Plex (latin subset, committed) keeps it offline-capable, framework-free and build-step-free. "Control room" aesthetic suits an instrument panel for a running pipeline |
| 2026-10-21 | `.dockerignore` for `lakehouse/`, `checkpoints/`, `.ivy2/` | leave the context as it was | Those are runtime volumes, not image content; excluding them shrinks and speeds every build and stops local data from entering the build context |

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
| Dashboard image | `p2-dashboard` (python:3.11-slim, stdlib only, no pip packages) |
| Dashboard port | `127.0.0.1:8501` (Compose profile `dashboard`) |

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
- `bash scripts/live_dashboard.sh` — start Redpanda + stream + Gold refresher + dashboard + continuous producer
- `bash scripts/stop_live_dashboard.sh` — stop the live demo
- `docker-compose --profile dashboard up -d --build gold-refresh dashboard` — refresher + dashboard only
- `docker-compose run --rm spark spark-submit --packages io.delta:delta-spark_2.12:3.2.0 /opt/spark/app/gold_refresh.py --once` — one Gold + metrics refresh
- `docker-compose run --rm spark spark-submit --packages io.delta:delta-spark_2.12:3.2.0 /opt/spark/app/gold_refresh.py --once --metrics-only` — metrics only, preserve Gold version/compaction
- `python3 tests/verify_dashboard_metrics.py lakehouse/dashboard/metrics.json` — contract + accounting identity
- `curl -s http://127.0.0.1:8501/healthz` and `curl -s http://127.0.0.1:8501/api/metrics`

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
| **Valid late events silently lost** (Silver 2879-2882, not 3018) | `append` dedup + a 10-min watermark **shorter** than the producer's 15-min max lateness → Spark *drops* sub-watermark records. Masked locally because the producer ran to completion *before* the stream (backlog landed while the watermark was still 0) | Watermark `10 minutes` → `20 minutes` (above max lateness). Verified live: Bronze 3277 / 3018 distinct → Silver **3018**, `numRowsDroppedByWatermark=0`. Added `scripts/watermark_report.py` + a Bronze-completeness invariant in `tests/verify_silver_dedup.py` |
| Idle supervisor stopped the stream **mid-run** (Bronze 2574 of 3277) | `_total_input_rows()` summed each query's *last micro-batch* row count — constant under steady input, so "no change" was mistaken for idle | Accumulate rows monotonically, deduped by `(query, batchId)`; the value only grows while input arrives |
| Dashboard metrics step after compaction would undo `OPTIMIZE`/ZORDER | A normal `gold_refresh.py --once` rewrites Gold (mode overwrite) | Added `--metrics-only`: read the existing Gold table, write only `metrics.json`, preserve Gold version/compaction. CI uses it after `gold_compaction.py` |
| "Newest event" KPI was ambiguous | It used the newest **Kafka ingestion** timestamp, not `event_time` | Relabeled the UI "Newest event / arrival age"; docs define it as wall clock minus newest `kafka_timestamp` |
| Refresh cycles wrote identical Gold files when no new data arrived | Full recompute/overwrite every cycle (design choice at this scale) | Documented as a limitation: skip writes when Silver's version is unchanged or move to incremental MERGE at scale; live test reached Gold version 19 in 20 cycles |

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
  (event_id, event_time) 3018; rows with bad `total_amount` = 0. (Now also asserts
  Silver retains **every distinct Bronze key**.)
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

### Phase 6 - Cloud (GitHub Actions) and the live late-data bug
- Cloud run **#39152253107** SUCCEEDED (all 17 steps) under the old 10-min watermark:
  Bronze 3277, **Silver 2879**, Gold 120 rows (order_count 1004 = 1004, revenue
  255878.9 = 255878.9, mismatched 0); OPTIMIZE Bronze 33→1, Silver 44→1, Gold 1→1.
- Cloud run **#39153174000** SUCCEEDED (idle stop fired after a true drain), also
  10-min watermark: **Silver 2882**, Gold 60 rows (order_count 1007 = 1007, revenue
  256933.1 = 256933.1, mismatched 0); OPTIMIZE Bronze 33→1, Silver 40→1, Gold 1→1.
  - Both cloud runs are **buggy** (rows dropped); they are why the watermark was
    raised. Gold row count differs (120 vs 60) only because the run crossed
    hour buckets (20 sellers × 3 payment types = 60/hour).
- Controlled **live-arrival probe** (stream first, producer second) on isolated
  topic `order-events-live`: at 10 min, Bronze 3277 / 3018 distinct → Silver **2881**,
  **137 late unique events dropped**, `numRowsDroppedByWatermark > 0`.
- After the fix (`WATERMARK=20 minutes`, monotonic supervisor), same live probe:
  Bronze 3277 / 3018 distinct → Silver **3018**, quarantine 0,
  `numRowsDroppedByWatermark=0`, idle stop after a true drain. `scripts/watermark_report.py`
  RESULT: "late unique events are RETAINED (no data loss)".
- Cloud run **#39156492225** SUCCEEDED (all steps) **with the fix**, same inputs
  (60 s @ 50/s seed 42, idle_stop 45 s):
  - Producer: `emitted=3277 duplicates=259 delayed=276 late_queued=276 delivery_errors=0`.
  - Bronze **3277** (p0 1056, p1 1170, p2 1051; contiguous) → Silver **3018**;
    distinct `event_id` 3018; distinct `(event_id, event_time)` 3018;
    **Bronze distinct dedup keys 3018** (completeness holds); bad `total_amount` 0;
    `numRowsDroppedByWatermark=0` on the `dedupe` state operator.
  - Idle stop fired after a true drain ("processing 9831 rows").
  - Gold **60** rows; duplicate grain keys 0; order_count 1055 = 1055;
    total_revenue 269805.58 = 269805.58; mismatched 0.
  - OPTIMIZE: Bronze 33→1 (303,958 → 208,260 bytes), Silver 40→1 (362,778 → 127,234 bytes),
    Gold 1→1 (4,175 bytes).
  - **Cloud == local canonical** (Silver 3018, Gold 60, revenue 269805.58): the fix
    restored correctness and cross-environment reproducibility.

### Phase 9 - Live dashboard (2026-10-20)

Isolated live run (canonical tables untouched):

- Topic `order-events-dashboard`; paths `/lakehouse/dashboard-demo/{bronze,silver,gold}`,
  quarantine `/lakehouse/dashboard-demo/silver/_quarantine`, checkpoints
  `/checkpoints/dashboard-demo`.
- Stream started **before** the producer; producer 60 s @ 40 events/s, ~10%
  duplicates and ~10% late; refresher every 5 s for the test (shipped default 10 s).
- Dashboard API snapshot captured at `2026-10-20T16:07:27Z` (Gold version 11):
  Bronze **2616**, Silver **2415**, duplicates **201** (7.68%), quarantine **0**,
  Gold **96**, orders **863**, revenue **219392.90**, AOV **254.22**, sellers **20**.
- Counts while open: t1 Bronze 926 / Silver 708 / orders 238 / revenue 56345.40;
  t2 1726 / 1438 / 519 / 128841.28; final 2616 / 2415 / 863 / 219392.90.
- Data freshness 40.68 s (wall clock - newest Kafka ingestion timestamp);
  lateness avg 187 s / p95 532 s / max 999 s.
- `numRowsDroppedByWatermark = 0` throughout; final dedupe state `numRowsTotal=2415`.
- Refresher completed **20 cycles**, Gold version `0 -> 19`. The committed API
  snapshot is version 11; counts were stable after version 9 and later cycles
  refreshed the same numbers.
- Independent `verify_silver_dedup.py`: Silver 2415 = distinct `event_id` 2415 =
  distinct `(event_id, event_time)` 2415; Bronze distinct dedup keys 2415; bad
  `total_amount` 0. `verify_gold.py`: Gold 96 rows, duplicate grain keys 0,
  order_count 863 = 863, revenue 219392.9 = 219392.9, mismatched groups 0.
- Browser DOM (live page): status **Live**, KPIs `$219,393 / 863 / $254.22 / 20 /
  201`, 10 seller bars, 2 hourly bars, 4 donut SVG circles, zero console errors.
- Canonical read-only snapshot (`--once --metrics-only`, `2026-10-20T16:12:33Z`):
  Bronze 3277, Silver 3018, duplicates 259, Gold 60, orders 1055, revenue
  269805.58, Gold version **2 preserved** (no rewrite). Contract test PASS.
- Evidence: `docs/assets/dashboard_live_metrics.json`,
  `dashboard_live_refresh.txt`, `dashboard_live_ingest.txt`,
  `dashboard_live_verify.txt`, `dashboard_canonical_metrics.json`.

### Stack / UI
- Redpanda topic `order-events`: 3 partitions, 1 replica.
- Console reachable at http://localhost:8080 (HTTP 200), connected to Kafka.
- Site: `docs/index.html` (showcase) + `docs/dashboard.html` (dashboard page),
  shared design system `docs/assets/site.css` + `docs/assets/dashboard.css`,
  self-hosted fonts `docs/assets/fonts/` (4 woff2 files, 167 KB).
- Published: `/` (showcase) and `/dashboard.html` (dashboard, snapshot mode).

## Open Questions

- None blocking. GitHub push auth resolved (stored credential); Pages live.
- MIT `LICENSE` used (doc default).

## Doc Sync Checklist

- [x] README.md (full: architecture, grain, measured results, how-to-run, limitations)
- [x] docs/ARCHITECTURE.md
- [x] docs/CONCEPTS_AND_DESIGN_DECISIONS.md
- [x] docs/DASHBOARD.md (new: serving layer, JSON contract, live run, limits)
- [x] docs/RUNBOOK.md
- [x] docs/CLOUD_DEMO.md
- [x] docs/index.html (includes "Run it in the cloud" + live dashboard sections)
- [x] docs/dashboard.html (dashboard as its own page: live locally, published snapshot on Pages)
- [x] docs/assets/site.css + dashboard.css + fonts (shared design system, no CDN)
- [x] .github/workflows/demo-run.yml (dashboard metrics + API contract)
- [x] CHANGELOG.md (Unreleased section current through Phase 10)
- [x] LICENSE (MIT) added
- [x] PROJECT_MEMORY.md (current)
- [x] docs/assets/dashboard_live_* + dashboard_canonical_metrics.json

> Doc-sync pass (2026-10-18): every number in README/docs matches
> `Measured Results` above and the raw evidence in `docs/assets/`.
>
> Correction pass (2026-10-18, after the cloud runs): the earlier interim claim that
> the Silver shortfall was a *drain/timing* problem was **wrong**. Root cause was the
> **10-minute watermark dropping valid late events** (proven with a controlled live
> probe: 137 unique events lost). Watermark raised to 20 min and the idle supervisor
> made monotonic; README, ARCHITECTURE, CONCEPTS, RUNBOOK, CLOUD_DEMO, CHANGELOG and
> index.html were all updated to the measured explanation.
>
> Dashboard pass (2026-10-20): added `docs/DASHBOARD.md`, `tests/verify_dashboard_metrics.py`,
> the Compose `dashboard` profile, live-run evidence, and cloud API verification.
> README, ARCHITECTURE, CONCEPTS (25 Q&A), RUNBOOK, CLOUD_DEMO, CHANGELOG,
> docs/index.html and this file were updated from the actual live run and
> `--metrics-only` canonical snapshot. Numbers in this file match
> `docs/assets/dashboard_live_metrics.json` and
> `docs/assets/dashboard_canonical_metrics.json`.
