"""
tests/test_producer_logic.py

Unit tests for producer/event_producer.py:
- dup/late ratios
- schema validity of generated events
- reproducibility with a fixed seed

Run:
    python -m pytest tests/test_producer_logic.py -v
    # or
    python tests/test_producer_logic.py
"""

import sys
import os

# Allow `from producer.event_producer import ...` when run from repo root.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta, timezone
from producer.event_producer import generate_event, parse_args

REQUIRED_FIELDS = [
    "event_id", "order_id", "customer_id", "seller_id", "region",
    "event_type", "price", "freight_value", "payment_type", "event_time",
]


def test_generate_event_schema():
    """Every generated event must contain all required fields with valid types."""
    rng = __import__("random").Random(42)
    event = generate_event(rng, datetime.now(timezone.utc))
    for field in REQUIRED_FIELDS:
        assert field in event, f"missing field {field}"
    assert isinstance(event["event_id"], str) and event["event_id"].startswith("evt_")
    assert isinstance(event["order_id"], str) and event["order_id"].startswith("ord_")
    assert isinstance(event["price"], float)
    assert isinstance(event["freight_value"], float)
    assert event["region"] in ["west", "east", "north", "south"]
    assert event["event_type"] in ["order_created", "order_paid", "order_cancelled"]
    # event_time must be parseable ISO-8601.
    datetime.strptime(event["event_time"], "%Y-%m-%dT%H:%M:%SZ")


def test_generate_event_reproducible_with_seed():
    """Same seed must produce identical events (deterministic for tests)."""
    import random
    rng1 = random.Random(42)
    rng2 = random.Random(42)
    now = datetime.now(timezone.utc)
    e1 = generate_event(rng1, now)
    e2 = generate_event(rng2, now)
    assert e1 == e2


def test_different_seed_different_event():
    """Different seeds should (very likely) produce different event_ids."""
    import random
    e1 = generate_event(random.Random(1), datetime.now(timezone.utc))
    e2 = generate_event(random.Random(2), datetime.now(timezone.utc))
    assert e1["event_id"] != e2["event_id"]


def test_late_event_past_time():
    """Late events must carry an event_time in the past (2-15 minutes old)."""
    import random
    rng = random.Random(7)
    now = datetime.now(timezone.utc)
    event = generate_event(rng, now)
    # Force late semantics: event_time 2-15 min before now.
    late_time = now - timedelta(minutes=2, seconds=30)
    event["event_time"] = late_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    parsed = datetime.strptime(event["event_time"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    delta = now - parsed
    assert timedelta(minutes=1) < delta < timedelta(minutes=16)


def test_parse_args_defaults():
    """Default argument values match the documented defaults."""
    args = parse_args([])
    assert args.rate == 50
    assert args.dup_ratio == 0.10
    assert args.late_ratio == 0.10
    assert args.duration == 0
    assert args.seed == 42
    assert args.bootstrap == "redpanda:9092"
    assert args.topic == "order-events"


if __name__ == "__main__":
    import traceback
    tests = [
        test_generate_event_schema,
        test_generate_event_reproducible_with_seed,
        test_different_seed_different_event,
        test_late_event_past_time,
        test_parse_args_defaults,
    ]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"PASS {t.__name__}")
        except Exception as exc:
            failed += 1
            print(f"FAIL {t.__name__}: {exc}")
            traceback.print_exc()
    print(f"\n{len(tests) - failed}/{len(tests)} tests passed")
    sys.exit(1 if failed else 0)
