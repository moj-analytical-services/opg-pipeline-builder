import json
from copy import deepcopy
from datetime import datetime
from logging import getLogger

import awswrangler as wr
import boto3
import numpy as np
import pandas as pd
import pyarrow.fs as fs
import pytest
from arrow_pd_parser import reader, writer
from mojap_metadata import Metadata
from moto import mock_aws

from opg_pipeline_builder.database import Database
from opg_pipeline_builder.validator import PipelineConfig
from tests.conftest import mock_get_file, mock_reader_read, mock_writer_write

log = getLogger()

DEFAULT_DATA_FILE = "tests/data/dummy_data/dummy_data1.csv"
DEFAULT_METADATA_FILE = "tests/data/meta_data/test/testdb/raw_hist/table1.json"


@pytest.fixture
def pandas_engine_class():
    from opg_pipeline_builder.transform_engines.pandas import PandasTransformEngine

    yield PandasTransformEngine


@pytest.fixture
def pandas_engine(pandas_engine_class, config: PipelineConfig, database: Database):
    yield pandas_engine_class(config=config, db=database)


@pytest.fixture
def default_metadata():
    yield Metadata.from_json(DEFAULT_METADATA_FILE)


@pytest.fixture
def default_df():
    yield pd.read_csv(DEFAULT_DATA_FILE)


def test_remove_columns_not_in_metadata(pandas_engine, default_df, default_metadata):
    df_with_dummy_data = default_df.copy()
    df_with_dummy_data["dummy_col"] = "dummy value"

    df = pandas_engine.transforms.remove_columns_not_in_metadata(
        df_with_dummy_data, default_metadata
    )

    assert default_df.equals(df)


def test_set_column_names_to_lower(pandas_engine, default_df):
    df = pandas_engine.transforms.set_colnames_to_lower(default_df)
    assert all([c.islower() for c in df.columns.to_list()])


def test_add_attributes_from_headers_and_rename(
    pandas_engine_class,
    default_df,
    default_metadata,
    config: PipelineConfig,
    database: Database,
):
    pandas_engine = pandas_engine_class(
        extract_header_values={
            "field_name": "dummy_field",
            "header_regex": "[0-9]{2}",
        },
        config=config,
        db=database,
    )

    input_df = default_df.copy()
    input_df.columns = [f"{c}12" for c in input_df.columns]

    expected_df = default_df.copy()
    expected_df["dummy_field"] = [
        json.dumps([{"value": "12", "fields": expected_df.columns.to_list()}])
    ] * expected_df.shape[0]

    df = pandas_engine.transforms.add_attributes_from_headers_and_rename(
        input_df,
        default_metadata,
    )

    assert expected_df.equals(df)


def test_add_primary_partition_column(
    pandas_engine_class, default_df, config: PipelineConfig, database: Database
):
    ts = int(datetime.utcnow().timestamp())
    partition_value = f"mojap_file_land_timestamp={str(ts)}"

    pandas_engine = pandas_engine_class(
        add_partition_column=True, config=config, db=database
    )

    expected_df = default_df.copy()
    expected_df["mojap_file_land_timestamp"] = ts

    df = pandas_engine.transforms.add_primary_partition_column(
        default_df,
        partition_value,
    )

    assert expected_df.equals(df)


def test_add_etl_column(pandas_engine, default_df):
    expected_df = default_df.copy()
    expected_df["testdb_etl_version"] = "testing"
    df = pandas_engine.transforms.add_etl_column(default_df)
    assert expected_df.equals(df)


@pytest.mark.parametrize(
    [
        "attributes",
        "error",
        "new_column",
    ],
    [
        (
            {"my_attribute": "my_attribute_val"},
            False,
            pd.DataFrame({"my_attribute": ["my_attribute_val"]}),
        ),
        (
            {"my_int": "my_attribute_val"},
            True,
            None,
        ),
    ],
)
def test_add_attributes_from_config(
    pandas_engine_class,
    default_df,
    attributes,
    error,
    new_column,
    config: PipelineConfig,
    database: Database,
):
    pandas_engine = pandas_engine_class(
        attributes=attributes, config=config, db=database
    )

    if error:
        with pytest.raises(ValueError):
            pandas_engine.transforms.add_attributes_from_config(default_df)
    else:
        expected_df = default_df.copy()
        df = pandas_engine.transforms.add_attributes_from_config(default_df)

        single_column = new_column.copy()
        for _ in range(expected_df.shape[0] - 1):
            new_column = pd.concat([new_column, single_column], ignore_index=True)

        new_column = new_column.reset_index()

        expected_df = pd.concat([expected_df, new_column], axis=1)
        expected_df = expected_df.drop(columns=["index"])

        assert expected_df.equals(df)


def test_remove_null_columns(pandas_engine, default_df, default_metadata):
    default_metadata.update_column({"name": "dummy_column", "type": "null"})
    updated_df = default_df.copy()
    updated_df["dummy_column"] = np.NaN
    df = pandas_engine.transforms.remove_null_columns(updated_df, default_metadata)
    assert df.equals(default_df)


def test_create_null_columns_for_columns_in_meta_not_in_data(
    pandas_engine, default_df, default_metadata
):
    default_metadata.update_column({"name": "dummy_column", "type": "string"})
    expected_df = default_df.copy()
    expected_df["dummy_column"] = np.NaN
    df = pandas_engine.transforms.create_null_columns_for_columns_in_meta_not_in_data(
        default_df,
        default_metadata,
    )
    assert expected_df.equals(df)


def test_input_transform_methods(pandas_engine, default_df, default_metadata):
    default_metadata.update_column({"name": "dummy_column", "type": "null"})

    updated_df = default_df.copy()
    updated_df["dummy_column"] = np.NaN
    updated_df.columns = [x.upper() for x in updated_df.columns]

    df = pandas_engine.transforms.input_transform_methods(
        updated_df,
        default_metadata,
    )

    assert default_df.equals(df)


@pytest.mark.parametrize(
    [
        "extract_header_values",
        "add_partition_column",
        "attributes",
        "header_values_suffix",
        "new_columns",
    ],
    [
        (
            {
                "field_name": "header_field",
                "header_regex": "[0-9]{2}",
            },
            True,
            {"my_attribute": "my_attribute_val"},
            "12",
            pd.DataFrame(
                {
                    "my_attribute": ["my_attribute_val"],
                    "header_field": json.dumps(
                        [
                            {
                                "value": "12",
                                "fields": [
                                    "my_int",
                                    "animal",
                                    "my_email",
                                    "my_datetime",
                                    "my_date",
                                ],
                            }
                        ]
                    ),
                }
            ),
        ),
        (
            None,
            False,
            None,
            None,
            None,
        ),
    ],
)
def test_output_transform_methods(
    pandas_engine_class,
    default_df,
    default_metadata,
    extract_header_values,
    add_partition_column,
    attributes,
    header_values_suffix,
    new_columns,
    config: PipelineConfig,
    database: Database,
) -> None:
    pandas_engine = pandas_engine_class(
        attributes=attributes,
        extract_header_values=extract_header_values,
        add_partition_column=add_partition_column,
        config=config,
        db=database,
    )

    default_metadata.update_column({"name": "string_column", "type": "string"})
    default_metadata.update_column({"name": "testdb_etl_version", "type": "string"})

    if attributes is not None:
        default_metadata.update_column(
            {"name": list(attributes.keys())[0], "type": "string"}
        )

    input_df = default_df.copy()

    if extract_header_values is not None:
        input_df.columns = [f"{c}{header_values_suffix}" for c in input_df.columns]
        default_metadata.update_column(
            {"name": extract_header_values.get("field_name"), "type": "string"}
        )

    input_df["madeup_column"] = "madeup_value"

    expected_df = default_df.copy()
    expected_df["string_column"] = np.NaN
    expected_df["testdb_etl_version"] = "testing"

    if add_partition_column:
        ts = int(datetime.utcnow().timestamp())
        partition_value = f"mojap_file_land_timestamp={str(ts)}"
        expected_df["mojap_file_land_timestamp"] = ts
        default_metadata.update_column(
            {"name": "mojap_file_land_timestamp", "type": "int64"}
        )
    else:
        partition_value = None

    if new_columns is not None:
        single_row = new_columns.copy()
        for _ in range(expected_df.shape[0] - 1):
            new_columns = pd.concat([new_columns, single_row], ignore_index=True)

        new_columns = new_columns.reset_index()

        expected_df = pd.concat([expected_df, new_columns], axis=1)
        expected_df = expected_df.drop(columns=["index"])

    df = pandas_engine.transforms.output_transform_methods(
        input_df,
        partition_value,
        default_metadata,
    )

    expected_df = expected_df.reindex(sorted(expected_df.columns), axis=1)
    df = df.reindex(sorted(df.columns), axis=1)

    assert expected_df.equals(df)


def test_remove_columns_in_meta_not_in_data(pandas_engine, default_metadata):
    expected_metadata = deepcopy(default_metadata)

    default_metadata.update_column(
        {
            "name": "non_existent_column",
            "type": "string",
        }
    )

    default_metadata.remove_column("my_email")
    default_metadata.update_column({"name": "MY_EMAIL", "type": "string"})

    metadata = pandas_engine._remove_columns_in_meta_not_in_data(
        DEFAULT_DATA_FILE,
        default_metadata,
    )

    assert set(expected_metadata.to_dict()) == set(metadata.to_dict())


@pytest.mark.xfail
@mock_aws
@pytest.mark.parametrize(
    [
        "chunk_rest_threshold",
    ],
    [
        (0,),
        (500_000_000,),
    ],
)
def test_transform(
    pandas_engine_class,
    chunk_rest_threshold,
    monkeypatch,
    default_metadata,
    default_df,
):
    s3_client = boto3.client("s3")
    _ = s3_client.create_bucket(
        Bucket="my-dummy-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
    )

    monkeypatch.setattr(fs, "S3FileSystem", mock_get_file)
    monkeypatch.setattr(writer, "write", mock_writer_write)
    monkeypatch.setattr(reader, "read", mock_reader_read)

    pandas_engine = pandas_engine_class(chunk_rest_threshold=chunk_rest_threshold)
    output_metadata = deepcopy(default_metadata)
    output_metadata.update_column(
        {
            "name": "testdb_etl_version",
            "type": "string",
        }
    )

    ts = int(datetime.utcnow().timestamp())
    partition = f"mojap_file_land_timestamp={ts}"

    input_partition_path = (
        f"s3://my-dummy-bucket/dev/testdb/raw_hist/table1/{partition}/"
    )
    output_partition_path = (
        f"s3://my-dummy-bucket/dev/testdb/processed/table1/{partition}/"
    )

    wr.s3.upload(DEFAULT_DATA_FILE, f"{input_partition_path}dummy_data.csv")

    _ = pandas_engine._transform(
        table_name="table1",
        transform_type="default",
        partition=partition,
        input_partition_path=input_partition_path,
        output_partition_path=output_partition_path,
        output_format="parquet",
        input_meta=default_metadata,
        output_meta=output_metadata,
    )

    output_filename = (
        "dummy_data-part-0.parquet"
        if chunk_rest_threshold == 0
        else "dummy_data.parquet"
    )
    df = reader.read(
        output_partition_path + output_filename,
        metadata=output_metadata,
    )

    expected_df = default_df.copy()
    expected_df["testdb_etl_version"] = "testing"
    data_types = deepcopy(expected_df.dtypes)
    for colname, coltype in data_types.items():
        lower_coltype = str(coltype).lower()
        if lower_coltype == "object":
            expected_df[colname] = expected_df[colname].astype("string")
        else:
            expected_df[colname] = expected_df[colname].astype(lower_coltype)

    assert expected_df.equals(df)
