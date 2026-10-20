#!/usr/bin/env bash
#
# scripts/stop_live_dashboard.sh
#
# Stop the live demo containers. Lakehouse data and checkpoints are intentionally
# kept; use docs/RUNBOOK.md §"Reset" if you want a clean slate.

set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE=${COMPOSE:-docker-compose}

docker rm -f p2-ingest >/dev/null 2>&1 || true
$COMPOSE --profile producer --profile dashboard stop producer gold-refresh dashboard 2>/dev/null || true
$COMPOSE --profile producer --profile dashboard rm -f producer gold-refresh dashboard 2>/dev/null || true

echo "Stopped live demo containers. ./lakehouse and ./checkpoints were kept."
echo "Redpanda and Console are still running; stop them with: $COMPOSE stop redpanda console"
