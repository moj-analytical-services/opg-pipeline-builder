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
        "DATABASE": "test-database",
    }

    for key, value in test_env_vars.items():
        monkeypatch_session.setenv(key, value)


@pytest.fixture(name="s3", scope="session")
def mock_s3() -> Generator[boto3.client]:
    "Return a mocked S3 client."
    with mock_aws():
        yield boto3.client("s3", region_name="eu-west-2")


@pytest.fixture(name="glue", scope="session")
def mock_glue() -> Generator[boto3.client]:
    "Return a mocked glue client."
    with mock_aws():
        yield boto3.client("glue", region_name="eu-west-2")


@pytest.fixture(autouse=True, scope="session")
def setup_logging(s3: boto3.client) -> Generator[None]:
    """Set logging level to CRITICAL for libraries that spit out a lot of DEBUG logs."""
    logging.getLogger("botocore").setLevel(logging.CRITICAL)
    logging.getLogger("awswrangler").setLevel(logging.CRITICAL)
    logging.getLogger("boto3").setLevel(logging.CRITICAL)

    s3.create_bucket(
        Bucket="log-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    configure_logging(
        bucket="log-bucket",
        prefix="prefix",
        database="database_name",
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

    logging.getLogger("botocore").setLevel(logging.DEBUG)
    logging.getLogger("awswrangler").setLevel(logging.DEBUG)
    logging.getLogger("boto3").setLevel(logging.DEBUG)


@pytest.fixture(autouse=True, scope="function")
def configure_caplog(
    caplog: pytest.LogCaptureFixture,
) -> Generator[pytest.LogCaptureFixture]:
    """Configure the caplog handler for the package logger."""
    package_logger = logging.getLogger(PACKAGE_LOGGER_NAME)
    package_logger.addHandler(caplog.handler)

    yield caplog

    package_logger.removeHandler(caplog.handler)
