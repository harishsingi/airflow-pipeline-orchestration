"""Custom Airflow operators for this project."""

from __future__ import annotations

try:  # project-root layout (local runs, tests)
    from plugins.operators.data_quality_operator import DataQualityOperator
except ImportError:  # Airflow plugins-folder layout
    from operators.data_quality_operator import DataQualityOperator

__all__ = ["DataQualityOperator"]
