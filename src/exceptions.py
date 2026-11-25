"""Shared exceptions for the pipeline package.

One exception type per failure mode so callers (and Airflow retries) can
tell a bad file from bad data from a bad config, instead of catching a
bare ``ValueError`` and guessing.
"""

from __future__ import annotations


class PipelineError(Exception):
    """Base class for all pipeline failures."""


class ConfigError(PipelineError):
    """The pipeline config file is missing, malformed, or invalid."""


class SchemaError(PipelineError):
    """Input data does not match the expected schema (columns, types)."""


class ValidationError(PipelineError):
    """A data-quality rule configuration or check failed."""


class DataQualityError(ValidationError):
    """Rows failed one or more data-quality rules.

    Carries the counts so the operator and the local runner can log
    exactly how many rows were rejected and why, instead of only a
    pass/fail flag.
    """

    def __init__(
        self,
        message: str,
        *,
        rows_in: int = 0,
        rows_rejected: int = 0,
        failures_by_rule: dict[str, int] | None = None,
    ) -> None:
        super().__init__(message)
        self.rows_in = rows_in
        self.rows_rejected = rows_rejected
        self.failures_by_rule = dict(failures_by_rule or {})


class TransformError(PipelineError):
    """A clean row could not be transformed into the warehouse model."""


class StorageError(PipelineError):
    """A file could not be read or written (missing, unreadable, unwritable)."""
