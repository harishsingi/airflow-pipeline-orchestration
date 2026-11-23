"""Generate synthetic order data for local runs.

All values are invented; no real customer, order, or amount data is used.
A few deliberately bad rows (missing customer, duplicate id, out-of-range
amount) are included so the validation step has something to catch.
"""

import csv
import random
from pathlib import Path

OUT = Path(__file__).resolve().parent.parent / "data" / "orders.csv"

random.seed(42)

REGIONS = ["north", "south", "east", "west"]
CHANNELS = ["web", "mobile", "store"]


def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for i in range(1, 119):
        rows.append(
            {
                "order_id": f"ORD-{10000 + i}",
                "customer_id": f"CUST-{random.randint(100, 999)}",
                "amount": round(random.uniform(5, 9500), 2),
                "region": random.choice(REGIONS),
                "channel": random.choice(CHANNELS),
                "order_date": "2026-11-22",
            }
        )
    # Deliberately bad rows, so validation has something to catch.
    rows.append(
        {
            "order_id": "ORD-10119",
            "customer_id": "",
            "amount": 120.5,
            "region": "north",
            "channel": "web",
            "order_date": "2026-11-22",
        }
    )
    rows.append(
        {
            "order_id": "ORD-10001",  # duplicate id
            "customer_id": "CUST-555",
            "amount": 42.0,
            "region": "south",
            "channel": "mobile",
            "order_date": "2026-11-22",
        }
    )
    rows.append(
        {
            "order_id": "ORD-10120",
            "customer_id": "CUST-777",
            "amount": 25000.0,  # out of range
            "region": "east",
            "channel": "store",
            "order_date": "2026-11-22",
        }
    )
    with OUT.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {len(rows)} synthetic rows to {OUT}")


if __name__ == "__main__":
    main()
