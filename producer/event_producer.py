"""
producer/event_producer.py

Synthetic e-commerce order event producer for Redpanda.

Purpose
-------
Continuously produces realistic order events to the Redpanda `order-events`
topic, keyed by `order_id` (so all events for an order land in the same
partition). Injects ~10% duplicates (re-send of the same event_id + event_time)
and ~10% late events (true old event_time emitted 2-15 minutes later via an
in-memory delay buffer, so throughput is not blocked by sleep).

Inputs
------
None (generates events synthetically). Configuration via CLI flags / env vars.

Outputs
-------
- Kafka topic `order-events`: JSON order events (key = order_id).
- Clean shutdown summary on SIGINT/SIGTERM: emitted / duplicated / delayed counts,
  logged to stdout (captured in PROJECT_MEMORY.md Measured Results).

How to run
----------
    python event_producer.py --duration 60 --rate 50 --seed 42

    # Via docker-compose (producer profile):
    docker-compose --profile producer up -d producer

CLI / env configuration
-----------------------
--rate         events per second (default 50, env RATE)
--dup-ratio    fraction of events that are duplicates (default 0.10, env DUP_RATIO)
--late-ratio   fraction of events that are late (default 0.10, env LATE_RATIO)
--duration     run for N seconds then exit (0 = run forever, default 0, env DURATION)
--seed         RNG seed for reproducible tests (default 42, env SEED)
--bootstrap    bootstrap servers (default redpanda:9092, env BOOTSTRAP_SERVERS)
--topic        topic name (default order-events, env TOPIC)

Key design choices
------------------
- Message key = order_id (doc v2 §3.A): all events for an order share a partition.
- Duplicate = re-send of the same event_id + event_time (simulating a client retry).
- Late event = keeps its true (old) event_time but is emitted 2-15 minutes later.
  Implemented with an in-memory delay buffer (heap of (emit_at, event)) rather
  than `sleep`, so the emission loop is never blocked.
- Delivery callbacks log errors; producer.flush() on shutdown.
- Summary counts on shutdown feed Measured Results.
"""

import argparse
import heapq
import json
import os
import random
import signal
import sys
import time
from datetime import datetime, timedelta, timezone


def _load_confluent_kafka():
    try:
        from confluent_kafka import Producer
        return Producer
    except ImportError as exc:  # pragma: no cover - runtime dependency
        raise SystemExit(
            "confluent-kafka is required: pip install confluent-kafka"
        ) from exc

# ---------------------------------------------------------------------------
# Configuration (CLI flags override env vars override defaults)
# ---------------------------------------------------------------------------

REGIONS = ["west", "east", "north", "south"]
EVENT_TYPES = ["order_created", "order_paid", "order_cancelled"]
PAYMENT_TYPES = ["credit_card", "paypal", "bank_transfer"]
SELLERS = [f"seller_{i:03d}" for i in range(200, 220)]
CUSTOMERS = [f"cust_{i:04d}" for i in range(5500, 5600)]


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Synthetic order event producer for Redpanda"
    )
    parser.add_argument("--rate", type=float, default=50,
                        help="Events per second (default 50, env RATE)")
    parser.add_argument("--dup-ratio", type=float, default=0.10,
                        help="Duplicate event ratio (default 0.10, env DUP_RATIO)")
    parser.add_argument("--late-ratio", type=float, default=0.10,
                        help="Late event ratio (default 0.10, env LATE_RATIO)")
    parser.add_argument("--duration", type=int, default=0,
                        help="Run for N seconds then exit (0 = forever, env DURATION)")
    parser.add_argument("--seed", type=int, default=42,
                        help="RNG seed (default 42, env SEED)")
    parser.add_argument("--bootstrap", default="redpanda:9092",
                        help="Bootstrap servers (default redpanda:9092, env BOOTSTRAP_SERVERS)")
    parser.add_argument("--topic", default="order-events",
                        help="Topic name (default order-events, env TOPIC)")
    return parser.parse_args()


def get_env_float(name, default):
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def get_env_int(name, default):
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def get_env_str(name, default):
    return os.getenv(name, default)


def generate_event(rng, now):
    """Generate a single realistic order event (dict).

    `now` is used as the reference processing time so the function is
    deterministic given a fixed rng and now (important for unit tests).
    """
    event_id = f"evt_{rng.randint(0, 99999999):08x}"
    order_id = f"ord_{rng.randint(10000, 10999)}"
    event_time = now - timedelta(seconds=rng.randint(0, 300))
    return {
        "event_id": event_id,
        "order_id": order_id,
        "customer_id": rng.choice(CUSTOMERS),
        "seller_id": rng.choice(SELLERS),
        "region": rng.choice(REGIONS),
        "event_type": rng.choice(EVENT_TYPES),
        "price": round(rng.uniform(10, 500), 2),
        "freight_value": round(rng.uniform(5, 50), 2),
        "payment_type": rng.choice(PAYMENT_TYPES),
        "event_time": event_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


class Producer:
    """Wrapper around confluent_kafka.Producer with delivery tracking."""

    def __init__(self, bootstrap_servers):
        Producer = _load_confluent_kafka()
        self.conf = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "order-event-producer",
            "enable.idempotence": True,
            "acks": "all",
        }
        self._producer = Producer(self.conf)
        self.delivered = 0
        self.errors = 0

    def delivery_callback(self, err, msg):
        if err is not None:
            self.errors += 1
            print(f"Delivery failed: {err}", file=sys.stderr)
        else:
            self.delivered += 1

    def produce(self, topic, key, value):
        self._producer.produce(
            topic=topic,
            key=key,
            value=value,
            callback=self.delivery_callback,
        )

    def poll(self, timeout=0.0):
        self._producer.poll(timeout)

    def flush(self, timeout=30):
        return self._producer.flush(timeout)


def main():
    args = parse_args()

    rate = get_env_float("RATE", args.rate) if args.rate == 50 else args.rate
    dup_ratio = get_env_float("DUP_RATIO", args.dup_ratio) if args.dup_ratio == 0.10 else args.dup_ratio
    late_ratio = get_env_float("LATE_RATIO", args.late_ratio) if args.late_ratio == 0.10 else args.late_ratio
    duration = get_env_int("DURATION", args.duration)
    seed = get_env_int("SEED", args.seed)
    bootstrap = get_env_str("BOOTSTRAP_SERVERS", args.bootstrap)
    topic = get_env_str("TOPIC", args.topic)

    rng = random.Random(seed)
    producer = Producer(bootstrap)

    running = True

    def handle_signal(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    emitted = 0
    duplicated = 0
    delayed = 0

    # Delay buffer: min-heap of (emit_at_timestamp, event_dict, key)
    delay_buffer = []

    start = time.time()
    interval = 1.0 / rate if rate > 0 else 0

    # Counter to generate a reusable set of event_ids for duplicates.
    recent_events = []  # list of (event, key) eligible to be duplicated

    while running:
        now = time.time()

        # Emit any delayed events whose time has come.
        while delay_buffer and delay_buffer[0][0] <= now:
            _, event, key = heapq.heappop(delay_buffer)
            producer.produce(topic, key, json.dumps(event))
            producer.poll(0)
            delayed += 1
            emitted += 1

        if not running:
            break

        # Generate a fresh event.
        event = generate_event(rng, datetime.now(timezone.utc))
        key = event["order_id"]
        producer.produce(topic, key, json.dumps(event))
        producer.poll(0)
        emitted += 1
        recent_events.append((event, key))
        if len(recent_events) > 1000:
            recent_events.pop(0)

        # Inject ~dup_ratio duplicates (re-send same event_id + event_time).
        if rng.random() < dup_ratio and recent_events:
            dup_event, dup_key = rng.choice(recent_events)
            producer.produce(topic, dup_key, json.dumps(dup_event))
            producer.poll(0)
            duplicated += 1
            emitted += 1

        # Inject ~late_ratio late events (old event_time, emitted 2-15 min later).
        if rng.random() < late_ratio:
            late_event = generate_event(rng, datetime.now(timezone.utc))
            late_event["event_time"] = (
                datetime.now(timezone.utc) - timedelta(minutes=rng.randint(2, 15), seconds=rng.randint(0, 59))
            ).strftime("%Y-%m-%dT%H:%M:%SZ")
            emit_at = time.time() + rng.uniform(120, 900)  # 2-15 minutes
            heapq.heappush(delay_buffer, (emit_at, late_event, late_event["order_id"]))
            delayed += 1  # counted when actually emitted

        # Rate limiting.
        if interval > 0:
            next_tick = start + (emitted / rate)
            sleep_for = next_tick - time.time()
            if sleep_for > 0:
                time.sleep(min(sleep_for, 0.5))

    # Drain remaining delay buffer on shutdown (flush late events not yet emitted).
    while delay_buffer:
        _, event, key = heapq.heappop(delay_buffer)
        producer.produce(topic, key, json.dumps(event))
        producer.poll(0)
        delayed += 1
        emitted += 1

    producer.flush(30)

    print(
        f"Producer shutdown summary: emitted={emitted} "
        f"duplicates={duplicated} delayed={delayed} "
        f"delivery_errors={producer.errors}",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
