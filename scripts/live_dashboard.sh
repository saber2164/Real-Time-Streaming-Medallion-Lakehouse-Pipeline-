#!/usr/bin/env bash
#
# scripts/live_dashboard.sh
#
# Start the live stakeholder demo:
#   Redpanda + Console + streaming ingestion + continuous producer
#   + the 10-second Gold refresher + the read-only dashboard.
#
# Open http://localhost:8501 after it starts.
#
# This APPENDS to ./lakehouse and ./checkpoints (the real Medallion tables).
# Use docs/RUNBOOK.md §"Reset" for a clean slate. Stop with
# scripts/stop_live_dashboard.sh.

set -euo pipefail
cd "$(dirname "$0")/.."

if ! docker info >/dev/null 2>&1; then
  echo "ERROR: the Docker daemon is not running. Start Docker Desktop or Colima first." >&2
  exit 1
fi

COMPOSE=${COMPOSE:-docker-compose}

echo "==> Starting Redpanda + Console"
$COMPOSE up -d redpanda console

echo "==> Waiting for Redpanda health"
redpanda_id=$($COMPOSE ps -q redpanda)
for _ in $(seq 1 60); do
  status=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$redpanda_id" 2>/dev/null || echo unknown)
  [ "$status" = "healthy" ] && break
  sleep 2
done
if [ "${status:-unknown}" != "healthy" ]; then
  echo "ERROR: Redpanda did not become healthy. Run: $COMPOSE logs redpanda" >&2
  exit 1
fi

echo "==> Ensuring topic order-events exists"
$COMPOSE run --rm -T --no-deps topic-init

echo "==> Starting streaming ingestion (detached container: p2-ingest)"
docker rm -f p2-ingest >/dev/null 2>&1 || true
$COMPOSE run -d -T --no-deps --name p2-ingest spark \
  spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,io.delta:delta-spark_2.12:3.2.0 \
  /opt/spark/app/stream_ingestion.py >/dev/null

echo "==> Starting Gold refresher + dashboard"
$COMPOSE --profile dashboard up -d --build gold-refresh dashboard

echo "==> Starting continuous producer (50 events/s, 10% dup, 10% late)"
$COMPOSE --profile producer up -d --build producer

cat <<'EOF'

Live demo is starting.

  Dashboard : http://localhost:8501
  Console   : http://localhost:8080
  Health    : http://localhost:8501/healthz
  Metrics   : http://localhost:8501/api/metrics

The dashboard shows a "Waiting for data" state until the first Gold refresh
(about 10-30 s after the first events reach Silver).

Useful commands:
  docker logs -f p2-ingest
  docker-compose --profile dashboard logs -f gold-refresh
  docker-compose --profile dashboard logs -f dashboard

Stop: scripts/stop_live_dashboard.sh
EOF
