# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Conventional Commits](https://www.conventionalcommits.org/).

## [Unreleased]

### Added
- `.github/workflows/demo-run.yml`: free, on-demand cloud execution of the full
  pipeline on GitHub Actions (`workflow_dispatch`), with evidence artifacts.
- `docs/CLOUD_DEMO.md`: how the cloud trigger works, reproducibility, and the
  optional one-click upgrade path.
- `STREAM_MAX_SECONDS` option in `spark/config.py` + `spark/stream_ingestion.py`:
  bounded run that drains the topic and stops itself (0 = run forever).
- `docs/ARCHITECTURE.md`: layered architecture, data contracts, Bronze/Silver/Gold
  grain, streaming topology, recovery model.
- `docs/CONCEPTS_AND_DESIGN_DECISIONS.md`: plain-language concepts plus 19 design
  Q&A (watermarks, dedup, read-from-Delta, Gold overwrite, Z-order, recovery).
- `docs/RUNBOOK.md`: bring-up, measured run order, verification, teardown, troubleshooting.
- `docs/index.html`: self-contained showcase page (light/dark, inline SVG, OpenGraph).
- `LICENSE` (MIT).
- `spark/stream_ingestion.py`: Bronze (Kafka -> raw Delta) and Silver (Bronze
  Delta stream -> parsed, 10-min watermark, `dropDuplicates(["event_id","event_time"])`)
  streaming queries, each with its own checkpoint; quarantine path for unparseable rows.
- `spark/gold_compaction.py`: hourly/seller/payment-type aggregation + `OPTIMIZE ... ZORDER BY (seller_id)`
  with active-file before/after metrics.
- `spark/config.py`: single source of truth for paths, topic, watermark, dedup keys, trigger.
- `producer/event_producer.py` + `tests/test_producer_logic.py`: synthetic events
  with ~10% duplicates and ~10% delayed events, unit-tested.
- `tests/verify_silver_dedup.py`, `tests/verify_bronze_offsets.py`, `tests/verify_gold.py`,
  `tests/verify_recovery.sh`; `scripts/inspect_delta.py`.
- `docs/assets/` raw evidence (verify outputs, inspect_delta, gold run, recovery run).
- `console.yaml` (Redpanda Console config).
- `docker-compose.yml` + `Dockerfile.spark`: Redpanda, Console, topic-init, Spark, producer (profile).

### Changed
- Silver is derived from the **Bronze Delta stream** instead of a second Kafka consumer.
- Quarantine path moved to a sibling of the Silver table (`/lakehouse/silver/_quarantine`).
- Console pinned to `redpandadata/console:v2.8.4`.
- Producer Dockerfile copies `requirements.txt` before `pip install` (context is `./producer`).

### Fixed
- `Dockerfile.spark`: `JAVA_HOME` now uses an architecture-neutral symlink
  (`/usr/lib/jvm/java-21`) instead of a hardcoded `java-21-openjdk-arm64` path,
  so the image runs on both arm64 (Apple Silicon) and amd64 (CI). Also installs
  `procps` (Spark's launcher calls `ps`). Found by the first cloud run.
- Producer now enforces `--duration`.
- Producer `delayed` count is no longer double-counted.
- Producer CLI env-var defaults honored when a flag is omitted.
- `topic-init` is idempotent (create-or-describe), so re-ups succeed.

### Security
- No secrets are committed; `.gitignore` excludes `lakehouse/`, `checkpoints/`, `*.parquet`, `.env`.

## [1.0.0] - 2026-10-18

### Added
- Architecture design doc v2 (project_2_architecture_design_doc_v2.md)
- Architecture design doc v1 (project_2_architecture_design_doc.md)
- PROJECT_MEMORY.md (persistent agent memory protocol per doc v2 §0.1)
- .gitignore excluding lakehouse/, checkpoints/, __pycache__/, .env, *.parquet
- Folder structure per doc v2 §5 Target Repository Layout
