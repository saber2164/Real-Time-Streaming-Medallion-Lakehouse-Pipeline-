# Project Memory - Real-Time Streaming & Medallion Lakehouse Pipeline

**Project:** Real-Time E-Commerce Clickstream & Order Events Ingestion Lakehouse
**Doc:** project_2_architecture_design_doc_v2.md (source of truth)
**Target repo:** https://github.com/saber2164/Real-Time-Streaming-Medallion-Lakehouse-Pipeline-

## Current Status

Phase 0: Bootstrap & Memory. Git initialized, folder structure created, PROJECT_MEMORY.md written. Nothing built yet.

## Next Actions

1. Write .gitignore, CHANGELOG.md, README.md skeleton
2. Write docker-compose.yml + Dockerfile.spark + producer Dockerfile (Phase 1)
3. Start Redpanda + Spark stack, create topic order-events (3 partitions)
4. Build producer/event_producer.py + unit tests (Phase 2)
5. Build spark/stream_ingestion.py (Bronze + Silver) (Phase 3)
6. Build spark/gold_compaction.py (Phase 4)
7. Fault tolerance verification (Phase 5)
8. Documentation (Phase 6) + web page (Phase 7) + publish (Phase 8)

## Decisions Log

| Date | Decision | Alternatives Considered | Reason |
|------|----------|------------------------|--------|
| 2026-10-18 | Use `docker-compose` (standalone) instead of `docker compose` plugin | `docker compose` plugin | `docker compose` is not available on this host; standalone `docker-compose` is installed |
| 2026-10-18 | Python version pinned at 3.9 (host) / container to match | Python 3.10+ (doc says 3.10+) | Host has Python 3.9.6; will pin container Python to 3.9 to keep build reproducible on this machine |

## Environment Facts

| Fact | Value |
|------|-------|
| OS | Darwin 27.0.0 (macOS), arm64 |
| Docker | 29.9.1 |
| docker-compose | standalone at /opt/homebrew/bin/docker-compose |
| Python (host) | 3.9.6 |
| Spark (host) | not installed (runs in container) |
| Disk free | ~329 GiB |
| Memory | ~3.4G shown (macOS unified; verify via container) |

## Commands That Worked

- (none yet)

## Bugs & Fixes

- (none yet)

## Measured Results

- (none yet - "not yet measured" until commands are run)

## Open Questions

- GitHub auth for pushing to target repo: user must authenticate (SSH key / gh auth login). Do not ask user to paste a token into chat.
- Whether user wants MIT LICENSE (doc says MIT unless user says otherwise) — proceeding with MIT.

## Doc Sync Checklist

- [ ] README.md (skeleton only)
- [ ] docs/ARCHITECTURE.md (not written)
- [ ] docs/CONCEPTS_AND_DESIGN_DECISIONS.md (not written)
- [ ] docs/RUNBOOK.md (not written)
- [ ] docs/index.html (not written)
- [ ] CHANGELOG.md (not written)
- [ ] PROJECT_MEMORY.md (current)
