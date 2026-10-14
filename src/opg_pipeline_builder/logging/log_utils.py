"""Utility functions and constants for structured logging."""

import datetime
import json
import logging
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


def not_empty(value: object) -> bool:
    """Return whether a value is a non-empty string after trimming whitespace.

    Args:
        value: Value to check; non-string values are treated as empty.

    Returns:
        True for a non-empty string, otherwise False.
    """
    if not isinstance(value, str):
        return False
    return bool(value.strip())


def is_timezone_aware(value: datetime.datetime) -> bool:
    """Return whether a datetime has a timezone and a defined UTC offset.

    Args:
        value: The datetime object to check.

    Returns:
        True if the datetime is timezone-aware, otherwise False.
    """
    return value.tzinfo is not None and value.utcoffset() is not None


def truncate_text(value: str, max_length: int, *, keep_end: bool = False) -> str:
    """Cap text at max_length characters and report the number of removed characters.

    By default the start is kept. Set keep_end to keep the end instead (e.g. for tracebacks, where the
    raised exception is the last line).

    Args:
        value: Text to preserve in truncated form.
        max_length: Maximum number of original text characters to retain.
        keep_end: Whether to retain the final rather than initial characters.

    Returns:
        The original text when it fits, or retained text with a truncation marker.
    """
    if len(value) <= max_length:
        return value
    marker = f"...[truncated {len(value) - max_length} characters]..."
    if keep_end:
        return f"{marker}{value[-max_length:]}"
    return f"{value[:max_length]}{marker}"


def validate_s3_prefix(prefix: str, name: str, bucket: str) -> None:
    """Require a non-empty, bucket-relative S3 key prefix.

    The prefix cannot start with '/', contain the 's3://' scheme (case-insensitive), or have the
    bucket name as its first path segment.

    Args:
        prefix: Bucket-relative prefix to validate.
        name: Configuration field name used in validation errors.
        bucket: S3 bucket that the prefix must not include.

    Raises:
        InvalidPrefixError: If the prefix is empty, absolute, or includes the bucket or scheme.
    """
    if not not_empty(prefix) or prefix.startswith("/"):
        err = f"Validating StructuredLogRecord: '{name}' must be non-empty and not start with '/'."
        raise exc.InvalidPrefixError(err)
    if "s3://" in prefix.lower() or prefix.split("/", 1)[0] == bucket:
        err = f"Validating StructuredLogRecord: '{name}' must not include 's3://' or the bucket."
        raise exc.InvalidPrefixError(err)


def validate_prefix_layout(log_store_prefix: str, athena_prefix: str) -> None:
    """Require the raw JSONL prefix to be outside the Athena table prefix.

    Equal prefixes and raw prefixes nested beneath the Athena prefix are rejected.

    Args:
        log_store_prefix: Prefix used for raw JSONL log objects.
        athena_prefix: Prefix used for curated Athena data.

    Raises:
        InvalidPrefixError: If the raw prefix is equal to or nested beneath the Athena prefix.
    """
    log_root = f"{log_store_prefix.rstrip('/')}/"
    athena_root = f"{athena_prefix.rstrip('/')}/"
    if log_root.startswith(athena_root):
        err = (
            "Validating StructuredLogRecord: 'log_store_prefix' must not be the same as, or nested"
            " inside 'athena_prefix'."
        )
        raise exc.InvalidPrefixError(err)


def encode_partition_value(value: object) -> str:
    """Percent-encode a Hive partition value as one unambiguous S3 key segment.

    Args:
        value: Partition value converted to text before encoding.

    Returns:
        A percent-encoded string safe to use as one path segment.
    """
    return quote(str(value), safe="")


def to_jsonl_line(row: dict[str, Any]) -> str:
    """Serialize one dictionary as compact JSON suitable for a JSONL line.

    Args:
        row: Structured logging fields to serialize.

    Returns:
        Compact JSON text for one line-delimited record.
    """
    return json.dumps(row, separators=(",", ":"))


def is_handler_attached(handler: str) -> bool:
    """Return whether the root logger has a handler with the given name.

    Args:
        handler: Exact handler name to search for on the root logger.

    Returns:
        True if a root handler has the exact requested name, otherwise False.
    """
    return any(h.get_name() == handler for h in logging.getLogger().handlers)
