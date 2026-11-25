"""Custom Airflow sensors for this project."""

from __future__ import annotations

try:  # project-root layout (local runs, tests)
    from plugins.sensors.partition_sensor import PartitionSensor
except ImportError:  # Airflow plugins-folder layout
    from sensors.partition_sensor import PartitionSensor

__all__ = ["PartitionSensor"]
