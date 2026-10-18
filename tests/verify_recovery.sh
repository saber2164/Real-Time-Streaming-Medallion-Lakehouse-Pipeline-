#!/usr/bin/env bash
#
# tests/verify_recovery.sh
#
# Fault-tolerance verification: prove that the Bronze+Silver streaming job
# resumes correctly after an UNGRACEFUL mid-stream crash -- no duplicate
# event_id in Silver, and contiguous Kafka offsets in Bronze per partition.
#
# Design
# ------
# To avoid polluting the canonical dataset (topic `order-events`, tables under
# lakehouse/bronze/order_events and lakehouse/silver/order_events), this test
# uses an ISOLATED topic and paths:
#     topic        order-events-recovery
#     Bronze       /lakehouse/bronze/recovery
#     Silver       /lakehouse/silver/recovery
#     checkpoints  /checkpoints/recovery
#
# Sequence
# --------
#   1. Reset recovery artifacts; (re)create the recovery topic (3 partitions).
#   2. Start the producer (detached) into the recovery topic.
#   3. Start the streaming ingestion job (detached).
#   4. `docker kill` the streaming job mid-stream (SIGKILL = worst case:
#      a micro-batch may be half-written when the process dies).
#   5. Restart the streaming job; let it drain.
#   6. Assert recovery with verify_silver_dedup.py + verify_bronze_offsets.py.
#
# Exit code 0 = PASS, non-zero = FAIL.
#
# Run from the repository root:
#     bash tests/verify_recovery.sh
#
set -uo pipefail

# --- configuration -----------------------------------------------------------
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

DC="docker-compose"
DOCKER="docker"
RP="$DOCKER exec p2-redpanda-1 rpk"

RECOVERY_TOPIC="order-events-recovery"
RECOVERY_BRONZE="/lakehouse/bronze/recovery"
RECOVERY_SILVER="/lakehouse/silver/recovery"
RECOVERY_CKPTS="/checkpoints/recovery"

STREAM_NAME="p2-recovery-stream"
PRODUCER_NAME="p2-recovery-producer"

PRODUCER_DURATION=45
PRODUCER_RATE=100
PRODUCER_SEED=7

EVIDENCE_DIR="docs/assets"
mkdir -p "$EVIDENCE_DIR"

PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,io.delta:delta-spark_2.12:3.2.0"

log() { echo "[recovery] $*"; }

cleanup_containers() {
  $DOCKER rm -f "$STREAM_NAME" "$PRODUCER_NAME" >/dev/null 2>&1 || true
}

fail() {
  echo
  echo "FAIL verify_recovery: $*"
  exit 1
}

# --- 1. reset state ----------------------------------------------------------
log "Resetting recovery artifacts..."
cleanup_containers
rm -rf lakehouse/bronze/recovery lakehouse/silver/recovery checkpoints/recovery
$RP topic delete "$RECOVERY_TOPIC" --brokers redpanda:9092 >/dev/null 2>&1 || true
$RP topic create "$RECOVERY_TOPIC" --partitions 3 --replicas 1 --brokers redpanda:9092 \
  || fail "could not create topic $RECOVERY_TOPIC"
log "Created topic $RECOVERY_TOPIC (3 partitions)"

# start_stream: launch the ingestion job detached against the recovery paths.
start_stream() {
  $DC run --rm -d --name "$STREAM_NAME" \
    -e "KAFKA_TOPIC=$RECOVERY_TOPIC" \
    -e "BRONZE_PATH=$RECOVERY_BRONZE" \
    -e "SILVER_PATH=$RECOVERY_SILVER" \
    -e "CHECKPOINT_BASE=$RECOVERY_CKPTS" \
    -e "QUARANTINE_PATH=/lakehouse/silver/recovery_quarantine" \
    spark spark-submit --packages "$PACKAGES" /opt/spark/app/stream_ingestion.py >/dev/null 2>&1
  log "Stream container $STREAM_NAME started"
}

# --- 2. start producer (generates data continuously) -------------------------
log "Starting producer: duration=${PRODUCER_DURATION}s rate=${PRODUCER_RATE}/s seed=${PRODUCER_SEED}"
$DC --profile producer run --rm -d --name "$PRODUCER_NAME" \
  -e "TOPIC=$RECOVERY_TOPIC" \
  -e "DURATION=$PRODUCER_DURATION" \
  -e "RATE=$PRODUCER_RATE" \
  -e "SEED=$PRODUCER_SEED" \
  producer >/dev/null 2>&1 || fail "could not start producer"

# --- 3. start stream, let it process a few micro-batches ---------------------
sleep 6           # let producer build a backlog
start_stream
sleep 12          # let the stream process a few micro-batches

# --- 4. kill the stream mid-stream (ungraceful) ------------------------------
log "Killing stream container mid-stream (SIGKILL)..."
PRE_KILL_ROWS=$($DOCKER logs "$STREAM_NAME" 2>&1 | grep -c "PROGRESS" || true)
$DOCKER kill "$STREAM_NAME" >/dev/null 2>&1 || true
$DOCKER rm -f "$STREAM_NAME" >/dev/null 2>&1 || true
log "Stream killed (progress log lines seen before kill: $PRE_KILL_ROWS)"
sleep 3

# --- 5. restart stream and let it drain --------------------------------------
log "Restarting stream (recovery from checkpoint)..."
start_stream
# Wait for the producer to finish and the stream to catch up on the backlog.
sleep $((PRODUCER_DURATION + 5))
log "Stopping stream gracefully..."
$DOCKER stop "$STREAM_NAME" >/dev/null 2>&1 || true
$DOCKER rm -f "$STREAM_NAME" >/dev/null 2>&1 || true
cleanup_containers

# --- 6. assertions -----------------------------------------------------------
log "Running assertions (verify_silver_dedup.py, verify_bronze_offsets.py)..."

echo "========================================================================"
echo "RECOVERY TEST EVIDENCE ($(date -u +%Y-%m-%dT%H:%M:%SZ))"
echo "  topic=$RECOVERY_TOPIC bronze=$RECOVERY_BRONZE silver=$RECOVERY_SILVER"
echo "  producer: duration=${PRODUCER_DURATION}s rate=${PRODUCER_RATE}/s seed=${PRODUCER_SEED}"
echo "  kill point: after 12s of streaming; restart, then drain"
echo "========================================================================"

$DC run --rm \
  -e "SILVER_PATH=$RECOVERY_SILVER" \
  -e "BRONZE_PATH=$RECOVERY_BRONZE" \
  spark spark-submit --packages "io.delta:delta-spark_2.12:3.2.0" \
  /opt/spark/tests/verify_silver_dedup.py 2>/dev/null | tee "$EVIDENCE_DIR/verify_recovery_silver_dedup.txt"
DEDUP_RC=${PIPESTATUS[0]}

$DC run --rm \
  -e "BRONZE_PATH=$RECOVERY_BRONZE" \
  -e "SILVER_PATH=$RECOVERY_SILVER" \
  spark spark-submit --packages "io.delta:delta-spark_2.12:3.2.0" \
  /opt/spark/tests/verify_bronze_offsets.py 2>/dev/null | tee "$EVIDENCE_DIR/verify_recovery_bronze_offsets.txt"
OFFSETS_RC=${PIPESTATUS[0]}

{
  echo "dedup_exit_code=$DEDUP_RC"
  echo "offsets_exit_code=$OFFSETS_RC"
} | tee "$EVIDENCE_DIR/verify_recovery_summary.txt"

echo
if [ "$DEDUP_RC" -eq 0 ] && [ "$OFFSETS_RC" -eq 0 ]; then
  echo "PASS verify_recovery: Silver has zero duplicate event_id and Bronze offsets are contiguous after an ungraceful crash."
  exit 0
fi
fail "recovery assertions failed (dedup_rc=$DEDUP_RC offsets_rc=$OFFSETS_RC)"
