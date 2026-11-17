import logging
from collections.abc import Generator

import boto3
import pytest
from _pytest.monkeypatch import MonkeyPatch
from moto import mock_aws


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


@pytest.fixture(autouse=True, scope="session")
def set_log_level() -> Generator[None]:
    """Set logging level to CRITICAL for libraries that spit out a lot of DEBUG logs."""
    logging.getLogger("botocore").setLevel(logging.CRITICAL)
    logging.getLogger("awswrangler").setLevel(logging.CRITICAL)
    logging.getLogger("boto3").setLevel(logging.CRITICAL)
    logging.getLogger().setLevel(logging.CRITICAL)

    yield
    logging.getLogger("botocore").setLevel(logging.DEBUG)
    logging.getLogger("awswrangler").setLevel(logging.DEBUG)
    logging.getLogger("boto3").setLevel(logging.DEBUG)
    logging.getLogger().setLevel(logging.DEBUG)


@pytest.fixture(name="s3", scope="function")
def mock_s3() -> Generator[boto3.client]:
    "Return a mocked S3 client."
    with mock_aws():
        yield boto3.client("s3", region_name="eu-west-2")


@pytest.fixture(name="glue", scope="function")
def mock_glue() -> Generator[boto3.client]:
    "Return a mocked glue client."
    with mock_aws():
        yield boto3.client("glue", region_name="eu-west-2")
