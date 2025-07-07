import os
import pathlib
import time
from datetime import timedelta
from pathlib import Path

import pytest

from tests.conftest import dummy_bucket, mock_get_file, set_up_s3


@pytest.fixture()
def upload_setup(request):
    test_files, upload_dir = request.param
    test_path = "tests/data/dummy_data/"

    full_paths = [os.path.join(test_path, test_file) for test_file in test_files]

    orig_paths = [os.path.join(upload_dir, test_file) for test_file in test_files]

    iter = zip(full_paths, orig_paths)

    return list(iter)


@pytest.mark.parametrize("upload_setup", [(["dummy_data1.csv"], "orig")], indirect=True)
def test_s3_copy(upload_setup, s3_client):
    from opg_pipeline_builder.utils.utils import s3_copy

    bucket = "somebucket"

    s3_client.create_bucket(
        Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": "eu-west-1"}
    )

    loc_path, orig_path = upload_setup[0]

    s3_client.upload_file(loc_path, bucket, orig_path)

    dest_path = orig_path.replace("orig", "dest")
    copy_arg = (os.path.join(bucket, orig_path), os.path.join(bucket, dest_path))

    s3_copy(copy_arg, client=s3_client)

    orig_obj = s3_client.get_object(Bucket=bucket, Key=orig_path)
    dest_obj = s3_client.get_object(Bucket=bucket, Key=dest_path)

    orig = orig_obj["Body"].read().decode("utf-8")
    dest = dest_obj["Body"].read().decode("utf-8")

    assert orig == dest


@pytest.mark.parametrize(
    "upload_setup",
    [
        (
            [
                "dummy_data1.csv",
                "dummy_data2.csv",
                "dummy_data3.csv",
                "dummy_data4.csv",
            ],
            "orig",
        )
    ],
    indirect=True,
)
def test_s3_bulk_copy(upload_setup, s3_client):
    from opg_pipeline_builder.utils.utils import s3_bulk_copy

    bucket = "somebucket"

    s3_client.create_bucket(
        Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": "eu-west-1"}
    )

    for loc_path, orig_path in upload_setup:
        s3_client.upload_file(loc_path, bucket, orig_path)

    copy_args = [
        (
            os.path.join(bucket, orig_path),
            os.path.join(bucket, orig_path.replace("orig", "dest")),
        )
        for _, orig_path in upload_setup
    ]

    s3_bulk_copy(copy_args)

    orig_objs = [
        s3_client.get_object(Bucket=bucket, Key=orig_path)
        for _, orig_path in upload_setup
    ]
    dest_objs = [
        s3_client.get_object(Bucket=bucket, Key=orig_path.replace("orig", "dest"))
        for _, orig_path in upload_setup
    ]

    origs = [orig_obj["Body"].read().decode("utf-8") for orig_obj in orig_objs]
    dests = [dest_obj["Body"].read().decode("utf-8") for dest_obj in dest_objs]

    assert origs == dests


@pytest.mark.parametrize(
    "test_input, expected",
    [
        (
            "9a4b6363-1fc4-56e5-caf1-95cf1765a1db-c100-2-1648540499.snappy.parquet",
            "9a4b6363-1fc4-56e5-caf1-95cf1765a1db-c100.snappy.parquet",
        ),
        (
            "9a4b6363-1fc4-56e5-caf1-95cf176.snappy.parquet",
            "Filename is not in the expected format.",
        ),
        ("test_csv-2-1648540499.csv", "test_csv.csv"),
        ("test_csv.csv", "Filename is not in the expected format."),
        ("hjfhksdjfh", "Filename is not in the expected format."),
    ],
)
def test_remove_lint_timestamp(test_input, expected):
    from opg_pipeline_builder.utils.utils import remove_lint_filestamp

    file_ext = "".join(pathlib.Path(test_input).suffixes)
    if expected == "Filename is not in the expected format.":
        with pytest.raises(ValueError, match=expected):
            remove_lint_filestamp(test_input, file_ext)

    else:
        assert remove_lint_filestamp(test_input, file_ext) == expected


@pytest.mark.parametrize(
    "test_input, timestamp_partition_name, expected",
    [
        (
            "bucket/mojap_file_land_timestamp=1651237717/file.csv",
            None,
            "mojap_file_land_timestamp=1651237717",
        ),
        (
            "bucket/mojap_file_land_timestamp=1651237717/part=hello/t.snappy.parquet",
            None,
            "mojap_file_land_timestamp=1651237717",
        ),
        (
            "bucket/key.json",
            None,
            "Filepath does not contain the expected MoJ AP Hive partition",
        ),
        (
            "gibb23948984",
            None,
            "Filepath does not contain the expected MoJ AP Hive partition",
        ),
        (
            "bucket/my_funky_partition=1651237709/file.snappy.parquet",
            "my_funky_partition",
            "my_funky_partition=1651237709",
        ),
    ],
)
def test_extract_mojap_partition(test_input, timestamp_partition_name, expected):
    from opg_pipeline_builder.utils.utils import extract_mojap_partition

    if expected == "Filepath does not contain the expected MoJ AP Hive partition":
        with pytest.raises(
            ValueError,
            match="Filepath does not contain the expected MoJ AP Hive partition",
        ):
            extract_mojap_partition(test_input)

    else:
        if timestamp_partition_name is None:
            assert extract_mojap_partition(test_input) == expected
        else:
            assert (
                extract_mojap_partition(
                    test_input, timestamp_partition_name=timestamp_partition_name
                )
                == expected
            )


@pytest.mark.parametrize(
    "partition, timestamp_partition_name, expected",
    [
        ("mojap_file_land_timestamp=", None, None),
        ("mojap_file_land_timestamp=1651237709", None, 1651237709),
        ("gibberish", None, None),
        ("my_funky_partition=1651237707", "my_funky_partition", 1651237707),
    ],
)
def test_extract_mojap_timestamp(partition, timestamp_partition_name, expected):
    from opg_pipeline_builder.utils.utils import extract_mojap_timestamp

    if timestamp_partition_name is None:
        timestamp_partition_name = "mojap_file_land_timestamp"

    if expected is None:
        with pytest.raises(ValueError):
            extract_mojap_timestamp(
                partition, timestamp_partition_name=timestamp_partition_name
            )
    else:
        assert extract_mojap_timestamp(
            partition, timestamp_partition_name=timestamp_partition_name
        )


@pytest.mark.parametrize(
    "upload_setup", [(["dummy_data1.csv", "dummy_data2.csv"], "orig")], indirect=True
)
def test_get_modified_filepaths_from_s3_folder(upload_setup, s3_client):
    from opg_pipeline_builder.utils.utils import \
        get_modified_filepaths_from_s3_folder

    bucket = "mytestbucket"

    s3_client.create_bucket(
        Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": "eu-west-1"}
    )
    loc_path1, orig_path1 = upload_setup[0]
    loc_path2, orig_path2 = upload_setup[1]

    s3_client.upload_file(loc_path1, bucket, orig_path1)

    init_objects = s3_client.list_objects(Bucket=bucket).get("Contents")
    mid_timestamp = init_objects[0].get("LastModified")
    mid_adj_timestamp = mid_timestamp + timedelta(seconds=1)
    prior_timestamp = mid_timestamp - timedelta(seconds=1)
    time.sleep(1)

    s3_client.upload_file(loc_path2, bucket, orig_path2)

    sec_objects = sorted(
        s3_client.list_objects(Bucket=bucket).get("Contents"),
        key=lambda x: x["LastModified"],
    )
    post_timestamp = sec_objects[1].get("LastModified") + timedelta(seconds=1)
    s3_bucket = f"s3://{bucket}"

    all_fps = get_modified_filepaths_from_s3_folder(
        s3_folder_path=s3_bucket,
    )
    expected_paths = [f"s3://{bucket}/{orig}" for orig in [orig_path1, orig_path2]]

    assert all_fps == expected_paths

    first_interval_paths = get_modified_filepaths_from_s3_folder(
        s3_folder_path=s3_bucket,
        modified_after=prior_timestamp,
        modified_before=mid_adj_timestamp,
    )

    assert first_interval_paths == [expected_paths[0]]

    second_interval_paths = get_modified_filepaths_from_s3_folder(
        s3_folder_path=s3_bucket,
        modified_after=mid_timestamp,
        modified_before=post_timestamp,
    )

    assert second_interval_paths == [expected_paths[1]]

    pre_interval_paths = get_modified_filepaths_from_s3_folder(
        s3_folder_path=s3_bucket, modified_before=mid_timestamp
    )

    assert pre_interval_paths == []

    post_interval_paths = get_modified_filepaths_from_s3_folder(
        s3_folder_path=s3_bucket, modified_after=post_timestamp
    )

    assert post_interval_paths == []


def test_list_configs():
    from opg_pipeline_builder.utils.utils import list_configs

    assert list_configs() == [Path(f).stem for f in os.listdir("tests/data/configs")]


@pytest.mark.parametrize(
    "existing_data, new_file, one_a_day, expected",
    [
        (
            ["data_1663846833.csv", "data_1663760500.csv"],
            "data_1663933331.csv",
            True,
            True,
        ),
        (
            ["data_1663846833.csv", "data_1663760500.csv"],
            "data_1663846840.csv",
            True,
            False,
        ),
        (
            ["data_1663846833.csv", "data_1663760500.csv"],
            "data_1663846840.csv",
            False,
            True,
        ),
        (
            ["gibberjabber.csv", "data_1663760500.csv"],
            "data_1663846840.csv",
            False,
            None,
        ),
    ],
)
def test_check_s3_for_existing_timestamp_file(
    existing_data, new_file, one_a_day, expected
):
    from opg_pipeline_builder.utils.utils import \
        check_s3_for_existing_timestamp_file

    if expected is None:
        with pytest.raises(ValueError):
            check_s3_for_existing_timestamp_file(existing_data, new_file, one_a_day)
    else:
        assert (
            check_s3_for_existing_timestamp_file(existing_data, new_file, one_a_day)
            == expected
        )


@pytest.mark.parametrize(
    "json_path", ["tests/data/meta_data/testdb/curated/table1.json"]
)
def test_pa_read_json_from_s3(s3, monkeypatch, json_path):
    import opg_pipeline_builder.utils.utils as pbutils

    monkeypatch.setattr(pbutils, "S3FileSystem", mock_get_file)
    set_up_s3(s3)
    filename = Path(json_path).name
    s3.meta.client.upload_file(json_path, dummy_bucket, filename)
    pa_tbl = pbutils.pa_read_json_from_s3(f"s3://{dummy_bucket}/{filename}")
    pa_loc_tbl = pbutils.read_json(json_path)
    assert pa_tbl == pa_loc_tbl
