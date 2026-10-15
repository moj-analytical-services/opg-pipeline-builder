import logging
from collections.abc import Generator
from datetime import UTC, datetime

import boto3
import pytest
from _pytest.monkeypatch import MonkeyPatch
from moto import mock_aws

from opg_pipeline_builder.logging.log import configure_logging


@pytest.fixture
def monkeypatch_session() -> Generator[MonkeyPatch]:
    """Create MonkeyPatch for setting environment variables."""
    m = MonkeyPatch()
    yield m
    m.undo()


@pytest.fixture(autouse=True)
def set_env_vars(monkeypatch_session: MonkeyPatch) -> None:
    """Run before all tests to create environment variables."""
    test_env_vars = {
        "AWS_ACCESS_KEY_ID": "test_key_id",
        "AWS_SECRET_ACCESS_KEY": "test_access_key",  # pragma: allowlist secret nosec
        "AWS_SECURITY_TOKEN": "test_security_token",  # nosec
        "AWS_SESSION_TOKEN": "test_session_token",  # nosec
        "AWS_DEFAULT_REGION": "eu-west-1",
        "DEFAULT_BUCKET": "test-bucket",
        "DATABASE": "test-database",
        "ENV": "test",
    }

    for key, value in test_env_vars.items():
        monkeypatch_session.setenv(key, value)


@pytest.fixture(autouse=True, scope="session")
def suppress_aws_logs() -> Generator[None]:
    """Suppress AWS diagnostics during tests and restore the original logger levels."""
    loggers = [logging.getLogger(name) for name in ("botocore", "awswrangler", "boto3")]
    levels = [logger.level for logger in loggers]
    for logger in loggers:
        logger.setLevel(logging.CRITICAL)
    try:
        yield
    finally:
        for logger, level in zip(loggers, levels, strict=True):
            logger.setLevel(level)


@pytest.fixture(name="s3", scope="session")
def mock_s3(suppress_aws_logs: None) -> Generator[boto3.client]:
    """Return a mocked S3 client."""
    with mock_aws():
        yield boto3.client("s3", region_name="eu-west-2")


@pytest.fixture(name="glue", scope="session")
def mock_glue(suppress_aws_logs: None) -> Generator[boto3.client]:
    """Return a mocked Glue client."""
    with mock_aws():
        yield boto3.client("glue", region_name="eu-west-2")


@pytest.fixture(autouse=True, scope="session")
def setup_log_bucket(s3: boto3.client) -> Generator[None]:
    """Create the session log bucket and remove its remaining objects on teardown."""
    s3.create_bucket(
        Bucket="log-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    yield

    objects = s3.list_objects_v2(Bucket="log-bucket").get("Contents", [])
    for obj in objects:
        s3.delete_object(Bucket="log-bucket", Key=obj["Key"])

    s3.delete_bucket(Bucket="log-bucket")


@pytest.fixture(autouse=True, scope="session")
def setup_logging(setup_log_bucket: None) -> Generator[None]:
    """Configure root logging for tests without registering disposable logs on teardown."""
    root_logger = logging.getLogger()
    original_level = root_logger.level
    controller = configure_logging(
        bucket="log-bucket",
        log_store_prefix="prefix",
        athena_prefix="prefix/curated",
        athena_database_name="test_logging",
        athena_table_name="pipeline_logs",
        pipeline="test_pipeline",
        data_delivery_period=datetime(2026, 6, 1, 12, 30, 00, tzinfo=UTC),
        attempt_no=1,
        run_id="test-run-id",
    )

    try:
        yield
    finally:
        for handler in controller._handlers:
            root_logger.removeHandler(handler)
            # Tests delete raw objects between cases; do not upload or register session logs.
            logging.Handler.close(handler)
        root_logger.setLevel(original_level)


@pytest.fixture(autouse=True)
def clear_log_bucket(s3: boto3.client) -> None:
    """Clear the contents of the log bucket."""
    objects = s3.list_objects_v2(Bucket="log-bucket").get("Contents", [])
    for obj in objects:
        s3.delete_object(Bucket="log-bucket", Key=obj["Key"])


@pytest.fixture(autouse=True)
def configure_caplog(
    caplog: pytest.LogCaptureFixture,
) -> pytest.LogCaptureFixture:
    """Expose pytest's root-attached capture handler without changing its ownership."""
    return caplog
