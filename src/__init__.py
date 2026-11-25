"""Airflow pipeline orchestration package.

Public surface: config loading, the rule engine, transforms, storage,
and the daily pipeline stages shared by the DAGs and the local runner.
"""

from __future__ import annotations

__version__ = "0.2.0"

__all__ = ["__version__"]
