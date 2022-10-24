import pytest
import os
import boto3
import shutil
import logging
from moto import mock_s3
from tests.helpers import copy
from pathlib import Path


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
