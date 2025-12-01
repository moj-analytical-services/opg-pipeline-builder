import io
import logging
import os
import pathlib
import shutil
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Callable
from unittest.mock import MagicMock

import aiobotocore.awsrequest
import aiobotocore.endpoint
import aiohttp
import aiohttp.client_reqrep
import aiohttp.typedefs
import awswrangler as wr
import boto3
import botocore
import botocore.awsrequest
import botocore.model
import pandas as pd
import pytest
import yaml
from dataengineeringutils3.s3 import s3_path_to_bucket_key
from moto import mock_aws

from opg_pipeline_builder.database import Database
from opg_pipeline_builder.validator import PipelineConfig

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
        "DATABASE_VERSION": "test",
        "DATABASE": "testdb",
        "SOURCE_TBLS_ENV": "table1;table2;table3",
        "STEP": "raw_hist_to_curated",
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


def pytest_unconfigure(config: pytest.Config) -> None:
    """Run after all tests complete to clear up test data and vars"""
    path = Path("tests/data/meta_data/test_output")
    if path.is_dir():
        shutil.rmtree(path)


@pytest.fixture(name="config")
def create_config() -> PipelineConfig:
    with Path("tests/data/configs/testdb.yml").open(encoding="utf-8") as f:
        config_yml = yaml.safe_load(f)
    return PipelineConfig(**config_yml)


@pytest.fixture(name="database")
def create_database(config: PipelineConfig) -> Database:
    return Database(config)


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
    with mock_aws():
        yield boto3.resource("s3", region_name="eu-west-1")


@pytest.fixture(scope="function")
def s3_client():
    with mock_aws():
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
        self.headers = response.headers
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
        self._loop = None

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


land_bucket = "mojap-land"
raw_hist_bucket = "mojap-raw-hist"
raw_bucket = "mojap-raw"
dummy_bucket = "dummy-bucket"
dep_bucket = "alpha-dep-etl"


def copy(odir, ndir):
    odir_path = pathlib.Path(odir)
    for path in odir_path.rglob("*"):
        if path.is_file():
            test_dir, file = os.path.split(path)
            dest_dir = test_dir.replace(odir, ndir)

            if not os.path.isdir(dest_dir):
                os.makedirs(dest_dir)

            shutil.copyfile(path, os.path.join(dest_dir, file))


class MockS3FilesystemReadInputStream:
    @staticmethod
    @contextmanager
    def open_input_stream(s3_file_path_in: str) -> io.BytesIO:
        s3_resource = boto3.resource("s3")
        bucket, key = s3_path_to_bucket_key(s3_file_path_in)
        obj_bytes = s3_resource.Object(bucket, key).get()["Body"].read()
        obj_io_bytes = io.BytesIO(obj_bytes)
        try:
            yield obj_io_bytes
        finally:
            obj_io_bytes.close()

    @staticmethod
    @contextmanager
    def open_input_file(s3_file_path_in: str):
        s3_client = boto3.client("s3")
        bucket, key = s3_path_to_bucket_key(s3_file_path_in)
        tmp_file = NamedTemporaryFile(suffix=pathlib.Path(key).suffix)
        s3_client.download_file(bucket, key, tmp_file.name)
        yield tmp_file.name


def mock_get_file(*args, **kwargs):
    return MockS3FilesystemReadInputStream()


def mock_aws_delete_job(*args, **kwargs):
    return {"JOB_NAME": "testdb_job"}


def mock_wr_repair_partitions(*args, **kwargs):
    return None


def set_up_s3(mocked_s3):
    """
    Used to setup mocked s3 before a run that expects data in S3
    """
    buckets = [land_bucket, raw_bucket, raw_hist_bucket, dummy_bucket, dep_bucket]

    for b in buckets:
        mocked_s3.meta.client.create_bucket(
            Bucket=b,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
        )


def mock_writer_write(df, output_path, *args, **kwargs):
    ext = pathlib.Path(output_path).suffix
    if ext == ".csv":
        wr.s3.to_csv(df, output_path, index=False)
    elif ext in [".parquet", ".snappy.parquet"]:
        wr.s3.to_parquet(df, output_path, index=False)
    else:
        raise NotImplementedError("Please add new writer")


def mock_reader_read(path, metadata, *args, **kwargs):
    ext = pathlib.Path(path).suffix
    with NamedTemporaryFile(suffix=ext) as tmp:
        wr.s3.download(path, tmp.name)
        if ext == ".csv":
            df = pd.read_csv(tmp.name)
        elif ext in [".parquet", ".snappy.parquet"]:
            df = pd.read_parquet(tmp.name)
        else:
            raise NotImplementedError("Please add new writer")

        for column in metadata.columns:
            if column["type"].startswith("date") or column["type"].startswith("time"):
                df[column["name"]] = df[column["name"]].astype("string")

        data_types = deepcopy(df.dtypes)
        for colname, coltype in data_types.items():
            lower_coltype = str(coltype).lower()
            if lower_coltype == "object":
                df[colname] = df[colname].astype("string")
            else:
                df[colname] = df[colname].astype(lower_coltype)

    return [df] if "chunksize" in kwargs else df


@pytest.fixture(autouse=True, scope="session")
def fix_set_log_level() -> None:
    """Set logging level to CRITICAL for libraries that spit out a lot of DEBUG logs."""
    logging.getLogger("botocore").setLevel(logging.CRITICAL)
    logging.getLogger("awswrangler").setLevel(logging.CRITICAL)
    logging.getLogger("opg_pipeline_builder").setLevel(logging.CRITICAL)
    logging.getLogger("boto3").setLevel(logging.CRITICAL)
    logging.getLogger("s3transfer").setLevel(logging.CRITICAL)
    logging.getLogger().setLevel(logging.CRITICAL)

    yield
    logging.getLogger("botocore").setLevel(logging.DEBUG)
    logging.getLogger("awswrangler").setLevel(logging.DEBUG)
    logging.getLogger("opg_pipeline_builder").setLevel(logging.DEBUG)
    logging.getLogger("boto3").setLevel(logging.DEBUG)
    logging.getLogger().setLevel(logging.DEBUG)
