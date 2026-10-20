"""
tests/verify_dashboard_metrics.py

Independent contract check for the dashboard's metrics snapshot.

This does not trust the producer of the JSON. It checks the accounting
identities that must hold for a correct Medallion run:

  Bronze rows = Silver rows + duplicates removed + quarantine rows
  sum(payment_mix.revenue)      == total_revenue
  sum(payment_mix.orders)       == total_orders
  sum(revenue_by_hour.revenue)  == total_revenue
  sum(revenue_by_hour.orders)   == total_orders

and the shapes the UI relies on (sorted series, top-seller limit, lateness
ordering, non-negative values).

Usage
-----
    python3 tests/verify_dashboard_metrics.py docs/assets/dashboard_metrics.json

Exit code 0 = PASS, 1 = FAIL.
"""

import json
import sys

TOL = 0.05  # values are rounded to 2 dp before being written


def fail(message):
    print(f"FAIL: {message}")
    raise SystemExit(1)


def close(a, b, label):
    if abs(float(a) - float(b)) > TOL:
        fail(f"{label}: {a} != {b} (tolerance {TOL})")


def require(condition, message):
    if not condition:
        fail(message)


def main(path):
    with open(path, encoding="utf-8") as handle:
        data = json.load(handle)

    for key in ("schema_version", "status", "generated_at", "kpis", "pipeline", "charts"):
        require(key in data, f"missing top-level key {key!r}")

    require(data["status"] == "live", f"status is {data['status']!r}, expected 'live'")

    k = data["kpis"]
    for key in (
        "total_revenue",
        "total_orders",
        "avg_order_value",
        "sellers",
        "gold_rows",
        "silver_rows",
        "bronze_rows",
        "quarantine_rows",
        "duplicates_removed",
        "dedup_rate_pct",
        "data_freshness_seconds",
        "avg_lateness_seconds",
        "max_lateness_seconds",
        "p95_lateness_seconds",
    ):
        require(key in k, f"kpis missing {key!r}")

    bronze = k["bronze_rows"]
    silver = k["silver_rows"]
    quarantine = k["quarantine_rows"] or 0
    duplicates = k["duplicates_removed"]
    require(bronze is not None and bronze > 0, f"bronze_rows invalid: {bronze}")
    require(silver > 0, f"silver_rows invalid: {silver}")
    require(
        bronze == silver + duplicates + quarantine,
        "row accounting broken: "
        f"bronze {bronze} != silver {silver} + duplicates {duplicates} "
        f"+ quarantine {quarantine}",
    )

    orders = k["total_orders"]
    revenue = k["total_revenue"]
    require(orders > 0, f"total_orders invalid: {orders}")
    require(revenue > 0, f"total_revenue invalid: {revenue}")
    close(k["avg_order_value"], revenue / orders, "avg_order_value")
    close(k["dedup_rate_pct"], 100.0 * duplicates / bronze, "dedup_rate_pct")

    require(k["data_freshness_seconds"] >= 0, "negative data freshness")
    require(
        k["avg_lateness_seconds"] <= k["p95_lateness_seconds"] <= k["max_lateness_seconds"],
        "lateness ordering broken: "
        f"avg={k['avg_lateness_seconds']} p95={k['p95_lateness_seconds']} "
        f"max={k['max_lateness_seconds']}",
    )
    require(k["sellers"] > 0 and k["sellers"] <= bronze, f"sellers invalid: {k['sellers']}")

    charts = data["charts"]
    for key in ("events_by_minute", "revenue_by_hour", "top_sellers", "payment_mix"):
        require(key in charts, f"charts missing {key!r}")

    mix = charts["payment_mix"]
    require(len(mix) > 0, "payment_mix is empty")
    close(sum(row["revenue"] for row in mix), revenue, "payment_mix revenue")
    require(
        sum(row["orders"] for row in mix) == orders,
        "payment_mix orders do not sum to total_orders",
    )

    hourly = charts["revenue_by_hour"]
    require(len(hourly) > 0, "revenue_by_hour is empty")
    close(sum(row["revenue"] for row in hourly), revenue, "revenue_by_hour revenue")
    require(
        sum(row["orders"] for row in hourly) == orders,
        "revenue_by_hour orders do not sum to total_orders",
    )

    minute = charts["events_by_minute"]
    require(len(minute) > 0, "events_by_minute is empty")
    stamps = [row["minute"] for row in minute]
    require(stamps == sorted(stamps), "events_by_minute is not sorted")
    for row in minute:
        require(
            row["events"] >= row["orders"] >= 0,
            f"per-minute events/orders invalid: {row}",
        )

    sellers = charts["top_sellers"]
    require(0 < len(sellers) <= 10, f"top_sellers length invalid: {len(sellers)}")
    revenues = [row["revenue"] for row in sellers]
    require(revenues == sorted(revenues, reverse=True), "top_sellers is not sorted desc")
    require(all(v >= 0 for v in revenues), "negative seller revenue")

    pipeline = data["pipeline"]
    require(pipeline.get("watermark"), "pipeline.watermark missing")
    require(pipeline.get("dedup_keys") == ["event_id", "event_time"], "dedup_keys wrong")
    require(isinstance(pipeline.get("gold_version"), int), "gold_version missing")

    print("PASS: dashboard metrics contract")
    print(f"  Bronze rows        : {bronze:,}")
    print(f"  Silver rows        : {silver:,}")
    print(f"  Duplicates removed : {duplicates:,} ({k['dedup_rate_pct']:.2f}%)")
    print(f"  Gold rows          : {k['gold_rows']:,}")
    print(f"  Orders             : {orders:,}")
    print(f"  Product revenue    : {revenue:,.2f}")
    print(f"  Avg order value    : {k['avg_order_value']:,.2f}")
    print(f"  Sellers            : {k['sellers']:,}")
    print(f"  Newest event age   : {k['data_freshness_seconds']:.1f}s")
    print(
        "  Lateness avg/p95/max: "
        f"{k['avg_lateness_seconds']:.1f}s / {k['p95_lateness_seconds']:.1f}s / "
        f"{k['max_lateness_seconds']:.1f}s"
    )
    print(f"  Gold version       : {pipeline['gold_version']}")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: python3 tests/verify_dashboard_metrics.py <metrics.json>")
        raise SystemExit(2)
    raise SystemExit(main(sys.argv[1]))
