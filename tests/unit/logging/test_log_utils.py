"""Unit tests for the logging helper utilities."""

from contextlib import nullcontext as does_not_raise
from typing import Any

import pytest

from opg_pipeline_builder.logging import log_exceptions as exc
from opg_pipeline_builder.logging import log_utils


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
        ("12345", False, "12345"),  # Exactly at the limit
        ("1234", True, "1234"),  # Under the limit
        ("1234567", False, "12345...[truncated 2 characters]..."),
        ("1234567", True, "...[truncated 2 characters]...34567"),
    ],
)
def test_truncate_text(
    value: str,
    keep_end: bool,  # noqa: FBT001
    expected: str,
) -> None:
    """Test that text over the limit is capped from the chosen end, with a marker of what was removed."""
    assert log_utils.truncate_text(value, 5, keep_end=keep_end) == expected


def test_validate_s3_prefix_valid() -> None:
    """Test that a valid S3 prefix passes validation."""
    log_utils.validate_s3_prefix(
        prefix="valid/prefix", name="log_store", bucket="my-bucket"
    )


@pytest.mark.parametrize(
    ("prefix", "bucket"),
    [
        ("s3://invalid/prefix", "my-bucket"),
        ("s3://my-bucket/invalid/prefix", "my-bucket"),
        ("my-bucket/invalid/prefix", "my-bucket"),
        ("my-bucket", "my-bucket"),
    ],
)
def test_validate_s3_prefix_invalid(prefix: str, bucket: str) -> None:
    """Test that an invalid S3 prefix raises a InvalidPrefixError."""
    with pytest.raises(
        exc.InvalidPrefixError,
        match="'log_store' must not include 's3://' or the bucket",
    ):
        log_utils.validate_s3_prefix(prefix, "log_store", bucket)


@pytest.mark.parametrize(
    ("prefix", "bucket"),
    [
        ("pipeline-logs/raw", "logs"),
        ("raw/logs/curated", "logs"),
        ("logs-archive/raw", "logs"),
    ],
)
def test_validate_s3_prefix_allows_bucket_name_outside_first_segment(
    prefix: str, bucket: str
) -> None:
    """Test that the bucket name only invalidates a prefix when it is the first path segment."""
    log_utils.validate_s3_prefix(prefix, "log_store", bucket)


@pytest.mark.parametrize("prefix", ["", "   ", "/prefix/to/log", "/"])
def test_validate_s3_prefix_rejects_empty_or_absolute(prefix: str) -> None:
    """Test that empty and leading-slash prefixes raise a InvalidPrefixError."""
    with pytest.raises(exc.InvalidPrefixError, match="must be non-empty"):
        log_utils.validate_s3_prefix(prefix, "log_store", "my-bucket")


@pytest.mark.parametrize(
    ("log_store_prefix", "athena_prefix", "expectation"),
    [
        ("prefix/to/log", "prefix/to/log/curated/", does_not_raise()),
        ("prefix", "prefix/curated/", does_not_raise()),
        ("raw/logs", "curated/logs", does_not_raise()),
        ("logs", "logs-curated", does_not_raise()),
        ("logs", "logs", pytest.raises(exc.InvalidPrefixError)),
        ("logs/", "logs", pytest.raises(exc.InvalidPrefixError)),
        ("curated/raw", "curated", pytest.raises(exc.InvalidPrefixError)),
        ("curated/raw/", "curated/", pytest.raises(exc.InvalidPrefixError)),
    ],
)
def test_validate_prefix_layout(
    log_store_prefix: str, athena_prefix: str, expectation: Any
) -> None:
    """Test that the raw log prefix cannot equal or sit inside the curated Athena prefix."""
    with expectation:
        log_utils.validate_prefix_layout(log_store_prefix, athena_prefix)


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
