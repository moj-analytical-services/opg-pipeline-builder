import logging
import os
import shutil
from pathlib import Path
from typing import Any, Callable
from unittest.mock import MagicMock

import aiobotocore.awsrequest
import aiobotocore.endpoint
import aiohttp
import aiohttp.client_reqrep
import aiohttp.typedefs
import boto3
import botocore
import botocore.awsrequest
import botocore.model
import pytest
from moto import mock_s3

from tests.helpers import copy

logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("moto").setLevel(logging.WARNING)


@pytest.fixture(scope="module", autouse=True)
def tests_env_setup_and_teardown():
    if "TEST_ENV" in os.environ:
        test_env = os.environ["TEST_ENV"]
    else:
        test_env = "local"

    TEMP_ENV_VARS = {
        "DEFAULT_DB_ENV": "test",
        "SOURCE_DB_ENV": "testdb",
        "SOURCE_TBLS_ENV": "table1;table2;table3",
        "ETL_STAGE_ENV": "raw_hist_to_curated",
        "GITHUB_TAG": "testing",
        "TEST_ENV": test_env,
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",  # pragma: allowlist secret
        "AWS_SECURITY_TOKEN": "testing",
        "AWS_SESSION_TOKEN": "testing",
        "AWS_DEFAULT_REGION": "eu-west-1",
        "IAM_ROLE": "test_iam",
        "ATHENA_DB_PREFIX": "testdb",
    }

    # Will be executed before the first test
    old_environ = dict(os.environ)
    os.environ.update(TEMP_ENV_VARS)

    yield
    # Will be executed after the last test
    os.environ.clear()
    os.environ.update(old_environ)

    temp_dbs = {"__temp_derived__", "_testdb_test"}
    for testdb in temp_dbs:
        if testdb in os.listdir():
            os.remove(testdb)


@pytest.fixture(scope="module", autouse=True)
def copy_files():
    test_directories = {"configs", "meta_data/test", "glue_jobs", "pipelines", "sql"}

    for dir in test_directories:
        try:
            _ = Path(dir).mkdir(parents=True)
        except FileExistsError:
            pass

    test_config_files = os.listdir("tests/data/configs")
    for config_file in test_config_files:
        shutil.copyfile(
            src=os.path.join("tests/data/configs", config_file),
            dst=os.path.join("configs", config_file),
        )

    copy("tests/data/sql", "sql")
    copy("tests/data/meta_data", "meta_data/test")
    copy("tests/data/glue_jobs", "glue_jobs")

    test_py_files = os.listdir("tests/data/python_scripts")
    for py_file in test_py_files:
        shutil.copyfile(
            src=os.path.join("tests/data/python_scripts", py_file),
            dst=os.path.join("pipelines", py_file),
        )

    yield

    for config_file in test_config_files:
        os.remove(os.path.join("configs", config_file))

    for py_file in test_py_files:
        os.remove(os.path.join("pipelines", py_file))

    _ = [shutil.rmtree(dir) for dir in test_directories]
    shutil.rmtree("meta_data")


@pytest.fixture(scope="function")
def s3():
    with mock_s3():
        yield boto3.resource("s3", region_name="eu-west-1")


@pytest.fixture(scope="function")
def s3_client():
    with mock_s3():
        yield boto3.client("s3", region_name="eu-west-1")


# Below code suggested by
# https://github.com/aio-libs/aiobotocore/issues/755#issuecomment-1424945194
# for dealing with raw headers error when mocking
# S3 and reading using e.g. pd.read_csv


class MockAWSResponse(aiobotocore.awsrequest.AioAWSResponse):
    """
    Mocked AWS Response.

    https://github.com/aio-libs/aiobotocore/issues/755
    https://gist.github.com/giles-betteromics/12e68b88e261402fbe31c2e918ea4168
    """

    def __init__(self, response: botocore.awsrequest.AWSResponse):
        self._moto_response = response
        self.status_code = response.status_code
        self.raw = MockHttpClientResponse(response)

    # adapt async methods to use moto's response
    async def _content_prop(self) -> bytes:
        return self._moto_response.content

    async def _text_prop(self) -> str:
        return self._moto_response.text


class MockHttpClientResponse(aiohttp.client_reqrep.ClientResponse):
    """
    Mocked HTP Response.

    See <MockAWSResponse> Notes
    """

    def __init__(self, response: botocore.awsrequest.AWSResponse):
        """
        Mocked Response Init.
        """

        async def read(self: MockHttpClientResponse, n: int = -1) -> bytes:
            return response.content

        self.content = MagicMock(aiohttp.StreamReader)
        self.content.read = read
        self.response = response

    @property
    def raw_headers(self) -> Any:
        """
        Return the headers encoded the way that aiobotocore expects them.
        """
        return {
            k.encode("utf-8"): str(v).encode("utf-8")
            for k, v in self.response.headers.items()
        }.items()


@pytest.fixture(scope="session", autouse=True)
def patch_aiobotocore() -> None:
    """
    Pytest Fixture Supporting S3FS Mocks.

    See <MockAWSResponse> Notes
    """

    def factory(original: Callable[[Any, Any], Any]) -> Callable[[Any, Any], Any]:
        """
        Response Conversion Factory.
        """

        def patched_convert_to_response_dict(
            http_response: botocore.awsrequest.AWSResponse,
            operation_model: botocore.model.OperationModel,
        ) -> Any:
            return original(MockAWSResponse(http_response), operation_model)

        return patched_convert_to_response_dict

    aiobotocore.endpoint.convert_to_response_dict = factory(
        aiobotocore.endpoint.convert_to_response_dict
    )
