"""Synthetic order generator with deliberately messy data.

Seeded by (date, region), so the same run date always produces the same file.
That keeps the pipeline reproducible and makes re-runs idempotent.

Built-in problems the pipeline has to handle:
  * duplicate rows
  * null order IDs and negative amounts
  * late-arriving orders (placed yesterday, delivered today)
  * schema drift (Mombasa renames `amount` to `total_amount` on some days)
  * an "incident" day where one region sends mostly corrupt data
"""
import json
import random
from datetime import date, datetime, time, timedelta
from pathlib import Path

PRODUCTS = {
    "data bundle 1GB": 250.0,
    "maize flour 2kg": 190.0,
    "solar lamp": 1800.0,
    "phone case": 450.0,
    "earphones": 1200.0,
    "rice 5kg": 850.0,
}
STATUSES = ["paid", "paid", "paid", "shipped", "delivered"]

NORMAL_BAD_RATE = 0.04
INCIDENT_BAD_RATE = 0.35
LATE_RATE = 0.05
DUPLICATE_RATE = 0.03


def is_incident(ds: str, region: str) -> bool:
    """Eldoret sends mostly corrupt data on days of the month divisible by 9."""
    return region == "eldoret" and int(ds[-2:]) % 9 == 0


def has_schema_drift(ds: str, region: str) -> bool:
    """Mombasa renames `amount` to `total_amount` on days divisible by 10."""
    return region == "mombasa" and int(ds[-2:]) % 10 == 0


def generate_orders(region: str, ds: str) -> list[dict]:
    rng = random.Random(f"{ds}|{region}")
    run_day = date.fromisoformat(ds)
    bad_rate = INCIDENT_BAD_RATE if is_incident(ds, region) else NORMAL_BAD_RATE

    rows = []
    for i in range(rng.randint(400, 600)):
        late = rng.random() < LATE_RATE
        day = run_day - timedelta(days=1) if late else run_day
        ts = datetime.combine(day, time()) + timedelta(seconds=rng.randint(0, 86399))
        product = rng.choice(list(PRODUCTS))
        qty = rng.randint(1, 5)
        row = {
            "order_id": f"{region[:3].upper()}-{ds.replace('-', '')}-{i:05d}",
            "customer_id": f"C{rng.randint(1, 3000):05d}",
            "region": region,
            "product": product,
            "quantity": qty,
            "unit_price": PRODUCTS[product],
            "amount": round(qty * PRODUCTS[product], 2),
            "currency": "KES",
            "status": rng.choice(STATUSES),
            "order_ts": ts.isoformat(),
        }
        if rng.random() < bad_rate:
            if rng.choice(["null_id", "negative_amount"]) == "null_id":
                row["order_id"] = None
            else:
                row["amount"] = -abs(row["amount"])
        rows.append(row)

    good = [r for r in rows if r["order_id"] is not None and r["amount"] > 0]
    rows += [dict(r) for r in rng.sample(good, int(len(good) * DUPLICATE_RATE))]
    rng.shuffle(rows)

    if has_schema_drift(ds, region):
        for r in rows:
            r["total_amount"] = r.pop("amount")
    return rows


def write_orders(region: str, ds: str, out: str) -> str:
    """Write one JSON-lines file for (region, ds). Overwrites, so reruns are idempotent."""
    path = Path(out) / f"dt={ds}" / f"{region}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for row in generate_orders(region, ds):
            f.write(json.dumps(row) + "\n")
    return str(path)
