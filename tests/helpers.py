import io
import boto3
from contextlib import contextmanager
from dataengineeringutils3.s3 import s3_path_to_bucket_key
import os
import shutil
import pathlib
from tempfile import NamedTemporaryFile


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


def mock_glue_delete_job(*args, **kwargs):
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
