# Running the pipeline in the cloud (free, on demand)

This document explains how the same pipeline that runs on your laptop can be
executed **on GitHub's cloud runners, triggered from a button on the showcase
page** — with no cloud account, no credit card, and no cost.

---

## What "run in the cloud" means here

The pipeline is a set of Docker containers (Redpanda, a Spark streaming job, a
synthetic producer) plus Delta tables on disk. GitHub Actions gives us a free,
ephemeral Linux VM where we can:

1. check out the repo,
2. build the images,
3. start Redpanda and create the topic,
4. run the streaming job (bounded — it drains the topic and stops itself),
5. produce events,
6. build Gold and compact with `OPTIMIZE`/`Z-ORDER`,
7. generate a read-only dashboard snapshot and verify the dashboard API,
8. run the same verification scripts as locally, and
9. upload the evidence as build artifacts.

The workflow lives at **`.github/workflows/demo-run.yml`**.

---

## How the trigger works (and why it's shaped this way)

```
   [ Button on docs/index.html ]
              │  opens
              ▼
   [ GitHub "Run workflow" page ]  ← GitHub login = authentication + safety gate
              │  dispatches
              ▼
   [ GitHub Actions runner (ubuntu-latest) ]
              │  docker compose up
              ▼
   [ Redpanda → Spark (Bronze→Silver→Gold) → verify → dashboard API ] → run summary + artifacts
```

Two constraints force this design:

1. **GitHub Pages is static.** It serves files only; it cannot execute code or
   start jobs.
2. **A browser must never hold a secret.** Starting a job requires a token or
   cloud key. If that key were embedded in the page, anyone could use it.

So the button deep-links to GitHub's secure *Run workflow* page. Clicking
**Run workflow** there (signed in) is the trigger, and GitHub's own auth is the
safety gate. This is the standard, zero-cost way to do "run it on demand" from a
static site.

---

## How to trigger a run

**From GitHub:** repository → **Actions** → **Cloud demo run** → **Run workflow**.
You can override:

| Input | Meaning | Default |
|-------|---------|---------|
| `duration` | producer duration in seconds | `60` |
| `rate` | events per second | `50` |
| `seed` | RNG seed (reproducible data) | `42` |
| `stream_seconds` | safety backstop before the stream stops itself | `600` |
| `idle_seconds` | stop once no new rows arrive for this long (full drain) | `45` |

**From the showcase page:** click **“Run it in the cloud (free)”**, then
**Run workflow**.

---

## Why the stream stops by itself

A streaming job normally runs forever. On a CI runner that would hang the job.
So `spark/stream_ingestion.py` gained two optional, documented switches:

```bash
-e STREAM_IDLE_TIMEOUT_SECONDS=45   # preferred: stop after the pipeline goes idle
-e STREAM_MAX_SECONDS=600           # safety backstop: hard wall-clock upper bound
```

- **`STREAM_IDLE_TIMEOUT_SECONDS` (preferred).** Once at least one batch has been
  processed and then **no new input rows arrive for this many seconds**, the job
  stops itself, so the whole Kafka → Bronze → Silver cascade **drains fully**
  before exit.
- **`STREAM_MAX_SECONDS`** is only a backstop so a runner can never hang forever.

Both default to `0` (disabled), preserving the original always-on behaviour for
local use.

### Two bugs the cloud runs exposed (both fixed)

Running **live** (stream first, producer second) — which is how the cloud, and
any real deployment, behaves — surfaced two issues that the "producer-first"
local flow had hidden:

1. **A 10-minute watermark silently dropped valid late events.** In an append
   dedup, Spark *drops* any record whose `event_time` is below the watermark.
   The producer delays events by 2-15 minutes, so a live run lost every order
   delayed more than 10 minutes: Bronze had 3277 rows / 3018 distinct keys, but
   Silver had only **2879-2882** rows. The fix is to size the watermark **above**
   the maximum lateness (now **20 minutes**), so nothing valid is dropped.
   Reproduce/diagnose with `scripts/watermark_report.py`.
2. **The idle supervisor could stop mid-stream.** It summed each query's *last
   micro-batch* row count, which is constant under steady input (e.g. ~250 rows
   every 5 s), so it could declare "idle" while data was still arriving. It now
   accumulates rows **monotonically** (deduped by `(query, batchId)`), so it
   stops only when the pipeline genuinely drains.

Both are covered by the verification suite: `tests/verify_silver_dedup.py` now
also asserts that Silver retains **every** distinct Bronze key, which is what
catches silent late-data loss.

---

## Dashboard verification in CI

After Gold compaction, the workflow produces a dashboard snapshot and verifies
the HTTP API, so the serving layer is tested on every cloud run:

```bash
# Read the compacted Gold table; write only metrics.json.
# Do NOT use a normal --once here: it would overwrite Gold and undo OPTIMIZE/ZORDER.
docker compose run --rm -T --no-deps spark spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  /opt/spark/app/gold_refresh.py --once --metrics-only

docker compose --profile dashboard build dashboard
docker compose --profile dashboard up -d dashboard
curl -sf http://127.0.0.1:8501/healthz
curl -sf http://127.0.0.1:8501/api/metrics > dashboard_metrics.json
python3 tests/verify_dashboard_metrics.py dashboard_metrics.json
```

The contract test is **invariant-based**, not tied to canonical counts: it
asserts the accounting identity
(`bronze_rows = silver_rows + duplicates_removed + quarantine_rows`), that the
payment/hour breakdowns sum to the KPIs, that the top-seller list is sorted and
capped at 10, and that lateness is ordered `avg ≤ p95 ≤ max`. That keeps the
workflow meaningful when `duration`, `rate`, or `seed` change.

Artifacts uploaded by the run:

| Artifact | Content |
|----------|---------|
| `dashboard_refresh.txt` | `Mode:`, `REFRESH` and `One-shot` log lines |
| `dashboard_metrics.json` | exact `/api/metrics` payload |
| `dashboard_health.json` | `/healthz` response |
| `verify_dashboard_metrics.txt` | contract + KPI output |

CI deliberately does **not** start the continuous `gold-refresh` service: a
finite workflow should not leave a long-lived process behind. `--once` proves
the same JSON contract. See [`DASHBOARD.md`](DASHBOARD.md) for the full design.

The container serves the **same page** that GitHub Pages publishes
(`docs/dashboard.html`): the workflow rewrites the deploy marker to `live`, so
`/api/metrics` is polled exactly as it is on a laptop.

---

## Reproducibility

Because the producer is seeded (default `42`) and each run starts from an empty
topic and empty Delta paths on a fresh runner, a cloud run reproduces the same
shape as the local run:

- Bronze = one row per produced record,
- Silver = distinct `(event_id, event_time)`,
- Gold = hourly × seller × payment-type aggregates.

Exact row counts can differ slightly if you change `duration`, `rate`, or
`seed`. The verification scripts assert the **invariants** (no duplicate dedup
keys, contiguous Kafka offsets, Gold matches an independent recomputation), so
they are meaningful regardless of the exact count.

---

## Optional upgrade: a true one-click button

If you want a single click on the page to start a run **without** the extra
GitHub screen, add a tiny serverless function that holds a **scoped** token and
calls the GitHub *workflow dispatch* API:

```
[ button ] → POST [ serverless function (holds PAT, actions:write) ]
                              │  POST /actions/workflows/demo-run.yml/dispatches
                              ▼
                     GitHub Actions run
```

Options: Cloudflare Workers, Vercel/Netlify functions, AWS Lambda — all have
free tiers. Rules of thumb:

- Use a **fine-grained** token limited to this one repo with **Actions: write**
  (and nothing else).
- Store it as a secret in the function platform, never in the page.
- Add rate limiting / a shared secret so the public URL can't be abused to burn
  your Actions minutes.
- Show the run status by fetching the badge or the workflow-runs API.

This is deliberately **not** enabled by default: an open, unauthenticated
"start a job" endpoint is an abuse and cost risk. The default (button → GitHub's
authenticated Run-workflow page) is safe and free.

---

## Cost and limits (GitHub Actions free tier)

- Public repositories get **unlimited** standard-runner minutes.
- Jobs are capped (default 6 hours; this workflow sets `timeout-minutes: 45`).
- Runners are **ephemeral**: no persistent lakehouse between runs (that's why the
  workflow builds a fresh dataset each time and uploads evidence as artifacts).
- Docker image build and Spark jar download are the slow parts; a cache for
  `~/.ivy2` is included to speed up repeat runs.

---

## Porting further (beyond GitHub Actions)

The same containers map cleanly onto managed cloud services when you want
persistent, production-grade streaming:

| Concern | Local / CI | Managed option |
|---------|-----------|----------------|
| Broker | Redpanda container | Redpanda Cloud, Confluent, MSK, Event Hubs |
| Spark | `spark-submit` in Docker | Databricks Jobs, EMR Serverless, Dataproc |
| Storage | local `/lakehouse` | S3 / ADLS / GCS (Delta works on object storage) |
| Orchestration | Actions workflow | Airflow, Databricks Workflows, Step Functions |

See `docs/ARCHITECTURE.md` for the pipeline internals and
`docs/CONCEPTS_AND_DESIGN_DECISIONS.md` for why each design choice was made.
