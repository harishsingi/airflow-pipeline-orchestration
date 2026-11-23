"""Shared defaults for every DAG in this project.

One place for the retry policy, SLA, and alerting behaviour so a new DAG
picks up the same guarantees instead of re-declaring them.
"""

from datetime import datetime, timedelta

DEFAULT_OWNER = "data-engineering"

DEFAULT_ARGS = {
    "owner": DEFAULT_OWNER,
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    # Transient failures (a flaky source, a brief warehouse hiccup) retry
    # automatically instead of paging someone at 2am.
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    # The whole daily run is expected inside 30 minutes; an SLA miss
    # alerts on-call before users notice a stale dashboard.
    "sla": timedelta(minutes=30),
}

START_DATE = datetime(2026, 11, 1)

TAGS = ["data-engineering", "etl"]
