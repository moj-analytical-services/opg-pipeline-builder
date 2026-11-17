# """Unit tests for the logging helper utilities."""
import datetime as dt
import logging
from datetime import UTC

import pytest

from opg_pipeline_builder.logging import log_utils


def test_format_datetime() -> None:
    """Test that format_datetime returns the expected ISO string."""
    test_datetime = dt.datetime(2026, 9, 22, 10, 45, 23, 245000, tzinfo=UTC)
    formatted = log_utils.format_datetime(test_datetime)
    assert formatted == "2026-09-22T10:45:23.245000Z"


@pytest.mark.parametrize(("value"), [("non-empty"), ("   ."), ("a  b")])
def test_not_empty_true(value: str) -> None:
    """Test that not_empty returns True for non-empty strings."""
    assert log_utils.not_empty(value)


@pytest.mark.parametrize(
    ("value"),
    [
        (""),
        (None),
        ("   "),
        (1),
        (True),
        (["A"]),
        ({"A": "B"}),
    ],
)
def test_not_empty_false(value: object) -> None:
    """Test that not_empty returns False for empty or whitespace-only strings."""
    assert not log_utils.not_empty(value)


@pytest.mark.parametrize(
    ("value", "keep_end", "expected"),
    [
        ("", False, ""),
        ("12345", False, "12345"),
        ("1234", True, "1234"),
        ("1234567", False, "12345...[truncated 2 characters]..."),
        ("1234567", True, "...[truncated 2 characters]...34567"),
    ],
)
def test_truncate_text(
    value: str,
    keep_end: bool,
    expected: str,
) -> None:
    """Test that text over the limit is capped from the chosen end, with a marker of what was removed."""
    assert log_utils.truncate_text(value, 5, keep_end=keep_end) == expected


@pytest.mark.parametrize("prefix", ["valid/prefix", "prefix/with/my-bucket/name"])
def test_validate_s3_prefix_valid(prefix: str) -> None:
    """Test that a valid S3 prefix passes validation."""
    log_utils.validate_s3_prefix(prefix=prefix, name="log_store", bucket="my-bucket")


@pytest.mark.parametrize("prefix", ["", "   ", "/prefix/to/log", "/"])
def test_validate_s3_prefix_empty_or_absolute(prefix: str) -> None:
    """Test that empty and leading-slash prefixes raise a InvalidPrefixError."""
    errors = log_utils.validate_s3_prefix(prefix, "log_store", "my-bucket")
    assert errors == "'log_store' must be non-empty and not start with '/'."


@pytest.mark.parametrize(
    ("prefix", "bucket"),
    [
        ("s3://invalid/prefix", "my-bucket"),
        ("s3://my-bucket/invalid/prefix", "my-bucket"),
        ("my-bucket/invalid/prefix", "my-bucket"),
        ("my-bucket", "my-bucket"),
    ],
)
def test_validate_s3_contains_bucket_or_s3(prefix: str, bucket: str) -> None:
    """Test that an invalid S3 prefix raises a InvalidPrefixError."""
    errors = log_utils.validate_s3_prefix(prefix, "log_store", bucket)
    assert errors == "'log_store' must not include 's3://' or the bucket name."


@pytest.mark.parametrize(
    ("log_store_prefix", "athena_prefix", "valid"),
    [
        ("prefix/to/log", "prefix/to/log/curated/", True),
        ("prefix", "prefix/curated/", True),
        ("raw/logs", "curated/logs", True),
        ("logs", "logs-curated", True),
        ("logs", "logs", False),
        ("logs/", "logs", False),
        ("curated/raw", "curated", False),
        ("curated/raw/", "curated/", False),
    ],
)
def test_validate_prefix_layout(
    log_store_prefix: str,
    athena_prefix: str,
    valid: bool,
) -> None:
    """Test that the raw log prefix cannot equal or sit inside the curated Athena prefix."""
    errors = log_utils.validate_prefix_layout(log_store_prefix, athena_prefix)
    assert (
        errors == ""
        if valid
        else "'log_store_prefix' must not be the same as, or nested inside, the 'athena_prefix'."
    )


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (
            "scheduled__2026-09-22T00:00:00+00:00",
            "scheduled__2026-09-22T00%3A00%3A00%2B00%3A00",
        ),
        ("manual/run 1", "manual%2Frun%201"),
        ("a=b", "a%3Db"),
        (3, "3"),
        ("2024-01-02T00:00:00Z", "2024-01-02T00%3A00%3A00Z"),
    ],
)
def test_encode_partition_value(value: object, expected: str) -> None:
    """Test that partition values are percent-encoded into a single safe key segment."""
    assert log_utils.encode_partition_value(value) == expected


def test_to_jsonl_line() -> None:
    """Test that a dictionary is converted to a JSONL line."""
    data = {"key": "value", "number": 42}
    expected = '{"key":"value","number":42}'
    assert log_utils.to_jsonl_line(data) == expected


def test_is_handler_attached() -> None:
    """Return false before a named handler is attached and true after attachment."""
    root_logger = logging.getLogger()
    handler = logging.StreamHandler()
    handler.set_name("is_handler_attached_test")

    assert not log_utils.is_handler_attached("is_handler_attached_test")
    root_logger.addHandler(handler)
    try:
        assert log_utils.is_handler_attached("is_handler_attached_test")
    finally:
        root_logger.removeHandler(handler)
        handler.close()
