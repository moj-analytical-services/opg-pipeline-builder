import logging
from collections.abc import Generator
from datetime import UTC, datetime

import boto3
import pytest

from opg_pipeline_builder.logging.log import PACKAGE_LOGGER_NAME, configure_logging


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
    """Set logging level to CRITICAL for libraries that spit out a lot of DEBUG logs."""
    logging.getLogger("botocore").setLevel(logging.CRITICAL)
    logging.getLogger("awswrangler").setLevel(logging.CRITICAL)
    logging.getLogger("boto3").setLevel(logging.CRITICAL)

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
def clear_log_bucket(s3: boto3.client) -> None:
    """Clear the contents of the log bucket."""
    objects = s3.list_objects_v2(Bucket="log-bucket").get("Contents", [])
    for obj in objects:
        s3.delete_object(Bucket="log-bucket", Key=obj["Key"])
