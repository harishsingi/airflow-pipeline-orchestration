"""Local runner: execute the daily pipeline steps in order, in plain Python.

Mirrors the daily_orders_pipeline DAG (extract -> validate -> transform ->
load -> notify) without needing Airflow, so the logic can be exercised and
tested locally. Bad rows are quarantined, never loaded.
"""

import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data" / "orders.csv"
QUARANTINE = ROOT / "data" / "quarantine.csv"
WAREHOUSE = ROOT / "data" / "warehouse_orders.csv"


def extract():
    with DATA.open() as f:
        rows = list(csv.DictReader(f))
    print(f"extract:   {len(rows)} raw records staged")
    return rows


def validate(rows):
    failures = []
    bad_idx = set()
    for i, r in enumerate(rows):
        if not r["customer_id"]:
            failures.append((r["order_id"], "not_null:customer_id"))
            bad_idx.add(i)
    seen = {}
    for i, r in enumerate(rows):
        if r["order_id"] in seen:
            failures.append((r["order_id"], "unique:order_id"))
            bad_idx.add(i)  # only the later duplicate is bad, not the original
        else:
            seen[r["order_id"]] = i
    for i, r in enumerate(rows):
        amount = float(r["amount"])
        if not 0 <= amount <= 10000:
            failures.append((r["order_id"], "range:amount[0,10000]"))
            bad_idx.add(i)
    print(f"validate:  {len(failures)} rule failures across {len(bad_idx)} records")
    return failures, bad_idx


def transform(rows, bad_idx):
    clean = [r for i, r in enumerate(rows) if i not in bad_idx]
    for r in clean:
        r["amount_usd"] = f"{float(r['amount']):.2f}"
    print(f"transform: {len(clean)} clean records modeled")
    return clean


def load(clean):
    with WAREHOUSE.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(clean[0].keys()))
        writer.writeheader()
        writer.writerows(clean)
    print(f"load:      {len(clean)} records written to warehouse/orders (partition 2026-11-22)")


def quarantine(rows, bad_idx):
    bad = [r for i, r in enumerate(rows) if i in bad_idx]
    with QUARANTINE.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(bad[0].keys()))
        writer.writeheader()
        writer.writerows(bad)
    print(f"quarantine: {len(bad)} records quarantined (never to warehouse)")


def main():
    if not DATA.exists():
        print("data/orders.csv missing; run: python -m src.generate_data", file=sys.stderr)
        sys.exit(1)
    rows = extract()
    failures, bad_idx = validate(rows)
    clean = transform(rows, bad_idx)
    quarantine(rows, bad_idx)
    load(clean)
    print(f"notify:    daily_orders_pipeline succeeded, {len(clean)} rows")


if __name__ == "__main__":
    main()
