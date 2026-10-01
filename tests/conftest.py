import logging
from collections.abc import Generator
from datetime import UTC, datetime

import boto3
import pytest
from _pytest.monkeypatch import MonkeyPatch
from moto import mock_aws

from opg_pipeline_builder.logging.log import PACKAGE_LOGGER_NAME, configure_logging


@pytest.fixture(scope="function")
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
        "DATABASE": "test_pipeline",
        "ENV": "test",
    }

    for key, value in test_env_vars.items():
        monkeypatch_session.setenv(key, value)


@pytest.fixture(name="_suppress_aws_logs", autouse=True, scope="session")
def suppress_aws_logs() -> Generator[None]:
    """Suppress AWS library logs for the lifetime of the mocked clients."""
    loggers = [logging.getLogger(name) for name in ("botocore", "awswrangler", "boto3")]
    original_levels = [logger.level for logger in loggers]
    for logger in loggers:
        logger.setLevel(logging.CRITICAL)

    yield

    for logger, level in zip(loggers, original_levels, strict=True):
        logger.setLevel(level)


@pytest.fixture(name="s3", scope="session")
def mock_s3(_suppress_aws_logs: None) -> Generator[boto3.client]:
    "Return a mocked S3 client."
    with mock_aws():
        yield boto3.client("s3", region_name="eu-west-2")


@pytest.fixture(name="glue", scope="session")
def mock_glue(_suppress_aws_logs: None) -> Generator[boto3.client]:
    "Return a mocked glue client."
    with mock_aws():
        yield boto3.client("glue", region_name="eu-west-2")


@pytest.fixture(autouse=True, scope="session")
def setup_log_bucket(s3: boto3.client) -> Generator[None]:
    """Create and clean up the log bucket for each test function."""
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
def setup_logging(s3: boto3.client) -> Generator[None]:
    """Configure package logging for the test session."""
    configure_logging(
        bucket="log-bucket",
        prefix="prefix",
        pipeline="pipeline_name",
        data_delivery_period=datetime(2026, 6, 1, 12, 30, 00, tzinfo=UTC),
        attempt_no=1,
        run_id="test-run-id",
    )

    yield

    package_logger = logging.getLogger(PACKAGE_LOGGER_NAME)
    for handler in list(package_logger.handlers):
        handler.close()
        package_logger.removeHandler(handler)
    package_logger.propagate = True
    package_logger.setLevel(logging.NOTSET)


@pytest.fixture(autouse=True, scope="function")
def clear_log_bucket(s3: boto3.client) -> None:
    """Clear the contents of the log bucket."""
    objects = s3.list_objects_v2(Bucket="log-bucket").get("Contents", [])
    for obj in objects:
        s3.delete_object(Bucket="log-bucket", Key=obj["Key"])


@pytest.fixture(autouse=True, scope="function")
def configure_caplog(
    caplog: pytest.LogCaptureFixture,
) -> Generator[pytest.LogCaptureFixture]:
    """Configure the caplog handler for the package logger."""
    package_logger = logging.getLogger(PACKAGE_LOGGER_NAME)
    package_logger.addHandler(caplog.handler)

    yield caplog

    package_logger.removeHandler(caplog.handler)
