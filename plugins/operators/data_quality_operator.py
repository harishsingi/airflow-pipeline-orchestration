"""Custom operator: run rule checks against a dataset and fail the task
when a rule breaks, so the load never runs on bad data.

Rules are config, not code: not-null columns, unique columns, and numeric
ranges, all declared on the task. The same checks the validation framework
runs offline, here as a step inside the DAG.
"""

from airflow.models import BaseOperator


class DataQualityOperator(BaseOperator):
    """Validate a dataset against not-null, uniqueness, and range rules."""

    def __init__(self, dataset, rules, **kwargs):
        super().__init__(**kwargs)
        self.dataset = dataset
        self.rules = rules or {}

    def execute(self, context):
        # In production this reads the staged dataset; the rule logic is
        # identical to the offline validator, so the checks stay in sync.
        rows = self._read_dataset()
        failures = []
        for column in self.rules.get("not_null", []):
            bad = [r for r in rows if r.get(column) in (None, "")]
            if bad:
                failures.append(f"not_null:{column} ({len(bad)} failed)")
        for column in self.rules.get("unique", []):
            seen = set()
            dupes = 0
            for r in rows:
                value = r.get(column)
                if value in seen:
                    dupes += 1
                seen.add(value)
            if dupes:
                failures.append(f"unique:{column} ({dupes} duplicates)")
        for column, (low, high) in self.rules.get("range", {}).items():
            bad = [r for r in rows if not (low <= r.get(column, low - 1) <= high)]
            if bad:
                failures.append(f"range:{column}[{low},{high}] ({len(bad)} failed)")
        if failures:
            raise ValueError(
                f"data quality failed for {self.dataset}: " + "; ".join(failures)
            )
        self.log.info("data quality PASS for %s (%d rows)", self.dataset, len(rows))
        return {"dataset": self.dataset, "rows": len(rows), "status": "PASS"}

    def _read_dataset(self):
        # Placeholder for the warehouse read; the local runner in src/
        # exercises the same rule logic against synthetic CSV data.
        return []
