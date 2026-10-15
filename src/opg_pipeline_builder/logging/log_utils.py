"""Utility functions and constants for structured logging."""

import json
import logging
from typing import Any
from urllib.parse import quote

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


def validate_s3_prefix(prefix: str, name: str, bucket: str) -> str:
    """Require a non-empty, bucket-relative S3 key prefix.

    The prefix cannot start with '/', contain the 's3://' scheme (case-insensitive), or have the
    bucket name as its first path segment.

    Args:
        prefix: Bucket-relative prefix to validate.
        name: Configuration field name used in validation errors.
        bucket: S3 bucket that the prefix must not include.

    Returns:
        An error message if the prefix is invalid, otherwise an empty string.
    """
    errors = []
    if not not_empty(prefix) or prefix.startswith("/"):
        errors.append(f"'{name}' must be non-empty and not start with '/'.")
    if "s3://" in prefix.lower() or prefix.split("/", 1)[0] == bucket:
        errors.append(f"'{name}' must not include 's3://' or the bucket.")
    return " ".join(errors)


def validate_prefix_layout(log_store_prefix: str, athena_prefix: str) -> str:
    """Require the raw JSONL prefix to be outside the Athena table prefix.

    Equal prefixes and raw prefixes nested beneath the Athena prefix are rejected.

    Args:
        log_store_prefix: Prefix used for raw JSONL log objects.
        athena_prefix: Prefix used for curated Athena data.

    Returns:
        An error message if the raw prefix is equal to or nested beneath the Athena prefix, otherwise an empty string.
    """
    log_root = f"{log_store_prefix.rstrip('/')}/"
    athena_root = f"{athena_prefix.rstrip('/')}/"
    err = ""
    if log_root.startswith(athena_root):
        err = "'log_store_prefix' must not be the same as, or nested inside, the 'athena_prefix'."
    return err


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
