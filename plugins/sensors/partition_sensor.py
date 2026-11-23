"""Sensor: wait until a dated partition file exists before proceeding.

Used when a DAG depends on a file landing (an upstream extract, a vendor
drop) rather than on another DAG's task. Poke mode keeps the check cheap;
reschedule mode frees the worker slot between pokes.
"""

import os

from airflow.sensors.base import BaseSensorOperator


class PartitionSensor(BaseSensorOperator):
    """Succeed when the partition file for the run date exists."""

    def __init__(self, base_path, partition_template="{ds}", **kwargs):
        super().__init__(**kwargs)
        self.base_path = base_path
        self.partition_template = partition_template

    def poke(self, context):
        partition = self.partition_template.format(**context)
        path = os.path.join(self.base_path, partition)
        exists = os.path.exists(path)
        self.log.info("checking partition %s: %s", path, "found" if exists else "waiting")
        return exists
