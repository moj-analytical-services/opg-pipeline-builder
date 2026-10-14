"""Utility functions for validating logging configuration and S3 prefixes."""

import datetime
import json
from typing import Any
from urllib.parse import quote

from opg_pipeline_builder.logging import log_exceptions as exc

ATHENA_COLUMN_TYPES: dict[str, str] = {
    "pipeline": "string",
    "run_id": "string",
    "data_delivery_period": "string",
    "attempt_no": "int",
    "logger_name": "string",
    "module": "string",
    "function": "string",
    "line_number": "bigint",
    "log_level": "string",
    "log_timestamp": "timestamp",
    "process": "string",
    "table": "string",
    "field": "string",
    "process_stage": "string",
    "message": "string",
    "exception": "string",
}


def not_empty(value: str | None) -> bool:
    """Check that a string is not empty.

    Args:
        value: The string to check.

    Returns:
        bool: True if the string is non-empty and not just whitespace, False otherwise.

    Must not be None, whitespace-only, or an empty string.
    """
    if not isinstance(value, str):
        return False
    return bool(value.strip())


def is_timezone_aware(value: datetime.datetime) -> bool:
    """Check if a datetime object is timezone-aware.

    Args:
        value: The datetime object to check.

    Returns:
        bool: True if the datetime object is timezone-aware, False otherwise.
    """
    return value.tzinfo is not None and value.utcoffset() is not None


def validate_s3_prefix(prefix: str, name: str, bucket: str) -> None:
    """Check that the prefix is a non-empty, bucket-relative S3 key prefix.

    It must not start with '/', include the 's3://' scheme, or start with the bucket name.
    """
    if not not_empty(prefix) or prefix.startswith("/"):
        err = f"Validating StructuredLogRecord: '{name}' must be non-empty and not start with '/'."
        raise exc.InvalidPrefixError(err)
    if "s3://" in prefix.lower() or prefix.split("/", 1)[0] == bucket:
        err = f"Validating StructuredLogRecord: '{name}' must not include 's3://' or the bucket."
        raise exc.InvalidPrefixError(err)


def validate_prefix_layout(log_store_prefix: str, athena_prefix: str) -> None:
    """Check that the raw JSONL files are not stored inside the location of the Athena table."""
    log_root = f"{log_store_prefix.rstrip('/')}/"
    athena_root = f"{athena_prefix.rstrip('/')}/"
    if log_root.startswith(athena_root):
        err = (
            "Validating StructuredLogRecord: 'log_store_prefix' must not be the same as, or nested"
            " inside 'athena_prefix'."
        )
        raise exc.InvalidPrefixError(err)


def encode_partition_value(value: object) -> str:
    """Percent-encode a Hive partition value so it is always a single, unambiguous S3 key segment."""
    return quote(str(value), safe="")


def to_jsonl_line(row: dict[str, Any]) -> str:
    return json.dumps(row, separators=(",", ":"))
