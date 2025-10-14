from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

from opg_pipeline_builder.models import metadata_model as m


def create_stage(
    name: str = "raw", data_type: str = "string", pattern: str = "pattern"
) -> m.Stage:
    return m.Stage(name=name, type=data_type, pattern=pattern)


def create_column(
    name: str = "id",
    nullable: bool = True,
    enum: list[str | int] = ["A", "B"],
    stages: list[m.Stage] = [create_stage()],
) -> m.Column:
    return m.Column(name=name, nullable=nullable, enum=enum, stages=stages)


def create_file_format(name: str = "raw", file_format: str = "parquet") -> m.FileFormat:
    return m.FileFormat(name=name, format=file_format)


def create_table_metadata(
    name: str,
    partitions: list[str],
    file_formats: list[m.FileFormat],
    columns: list[m.Column],
) -> m.TableMetaData:
    return m.TableMetaData(
        converted_from="arrow_schema",
        schema_link="https://link-to-schema.com",
        name=name,
        description="description",
        file_formats=file_formats,
        sensitive=False,
        primary_key=[],
        partitions=partitions,
        columns=columns,
    )


@pytest.mark.parametrize(
    ("data_type"),
    [
        ("string"),
        ("list<struct<"),
        ("list<struct<stuff_and_more_stuff"),
        ("list<struct<stuff_and_list<struct<"),
    ],
)
def test_stage_valid(data_type: str) -> None:
    stage = create_stage(data_type=data_type)
    assert stage.name == "raw"
    assert stage.type == data_type
    assert stage.pattern == "pattern"


@pytest.mark.parametrize(
    ("name", "data_type", "exception", "err"),
    [
        (
            "invalid",
            "string",
            m.InvalidStageError,
            "ETL stage 'invalid' is not in the ALLOWED_ETL_STAGES constant",
        ),
        (
            "raw",
            "invalid",
            m.InvalidTypeError,
            "Data type 'invalid' is not in the ALLOWED_DATA_TYPES constant",
        ),
        (
            "raw",
            "strong",
            m.InvalidTypeError,
            "Data type 'strong' is not in the ALLOWED_DATA_TYPES constant",
        ),
        (
            "raw",
            "list<struc",
            m.InvalidTypeError,
            "Data type 'list<struc' is not in the ALLOWED_DATA_TYPES constant",
        ),
        (
            "raw",
            "list<string<",
            m.InvalidTypeError,
            "Data type 'list<string<' is not in the ALLOWED_DATA_TYPES constant",
        ),
    ],
)
def test_stage_invalid(name: str, data_type: str, exception: Any, err: str) -> None:
    with pytest.raises(exception) as e:
        create_stage(name, data_type)

    assert str(e.value) == err


def test_column_valid() -> None:
    column = create_column(
        stages=[
            create_stage(),
            create_stage(name="processed"),
            create_stage(name="curated", data_type="int64"),
        ],
    )
    assert column.name == "id"
    assert column.nullable
    assert column.enum == ["A", "B"]
    assert column.stages[0].name == "raw"
    assert column.stages[0].type == "string"
    assert column.stages[1].name == "processed"
    assert column.stages[2].name == "curated"
    assert column.stages[2].type == "int64"


def test_column_get_stage_for_column_valid() -> None:
    column = create_column(
        stages=[
            create_stage(),
            create_stage(name="processed"),
            create_stage(name="curated", data_type="int64"),
        ],
    )

    stage = column.get_stage_for_column("processed")
    assert stage.name == "processed"
    assert stage.type == "string"


def test_column_get_stage_for_column_invalid() -> None:
    column = create_column(
        stages=[
            create_stage(),
            create_stage(name="processed"),
            create_stage(name="curated", data_type="int64"),
        ],
    )

    with pytest.raises(m.InvalidStageError) as e:
        column.get_stage_for_column("invalid")

    assert (
        str(e.value) == "No metadata is configured for stage 'invalid' for column 'id'"
    )


@pytest.mark.parametrize(
    ("stage", "present"), [("raw", True), ("processed", True), ("invalid", False)]
)
def test_column_has_stage_valid(stage: str, present: bool) -> None:
    column = create_column(
        stages=[create_stage(), create_stage(name="processed")],
    )

    assert column.has_stage(stage) is present


def test_file_format_valid() -> None:
    file_format = create_file_format()
    assert file_format.name == "raw"
    assert file_format.format == "parquet"


@pytest.mark.parametrize(
    ("name", "file_format", "exception", "err"),
    [
        (
            "invalid",
            "parquet",
            m.InvalidStageError,
            "ETL stage 'invalid' is not in the ALLOWED_ETL_STAGES constant",
        ),
        (
            "raw",
            "invalid",
            m.InvalidFormatError,
            "File format 'invalid' is not in the ALLOWED_FILE_FORMATS constant",
        ),
    ],
)
def test_file_format_invalid(
    name: str, file_format: str, exception: Any, err: str
) -> None:
    with pytest.raises(exception) as e:
        create_file_format(name, file_format)

    assert str(e.value) == err


def test_table_metadata_valid() -> None:
    table_metadata = create_table_metadata(
        "test_table", ["id"], [create_file_format()], [create_column()]
    )

    assert table_metadata.name == "test_table"
    assert table_metadata.converted_from == "arrow_schema"
    assert table_metadata.file_formats[0].name == "raw"
    assert table_metadata.partitions == ["id"]
    assert table_metadata.columns[0].name == "id"


@pytest.mark.parametrize(
    ("columns", "partitions", "exception", "err"),
    [
        (
            [
                create_column(name="duplicate"),
                create_column(name="unique"),
                create_column(name="duplicate"),
            ],
            ["duplicate"],
            m.DuplicateColumnsError,
            "One or more columns are defined twice for the same table",
        ),
        (
            [
                create_column(name="duplicate"),
                create_column(name="unique"),
                create_column(name="also_unqique"),
            ],
            ["not_present"],
            m.InvalidColumnError,
            "Partition column 'not_present' is not a defined column in the metadata for 'test_table'.",
        ),
    ],
)
def test_table_metadata_invalid(
    columns: list[m.Column], partitions: list[str], exception: Any, err: str
) -> None:
    with pytest.raises(exception) as e:
        create_table_metadata(
            name="test_table",
            columns=columns,
            partitions=partitions,
            file_formats=[create_file_format()],
        )

    assert str(e.value) == err


def test_table_metadata_get_file_format_for_stage_valid() -> None:
    table_metadata = create_table_metadata(
        "test_table",
        ["id"],
        [
            create_file_format(name="raw", file_format="csv"),
            create_file_format(name="processed"),
        ],
        [create_column()],
    )

    file_format = table_metadata.get_file_format_for_stage("processed")
    assert file_format.format == "parquet"


def test_table_metadata_get_file_format_for_stage_invalid() -> None:
    table_metadata = create_table_metadata(
        "test_table",
        ["id"],
        [
            create_file_format(name="raw", file_format="csv"),
            create_file_format(name="processed"),
        ],
        [create_column()],
    )

    with pytest.raises(m.InvalidStageError) as e:
        table_metadata.get_file_format_for_stage("invalid")

    assert (
        str(e.value)
        == "No file format metadata is configured for stage 'invalid' for table 'test_table'"
    )


def test_table_metadata_get_columns_for_stage_populated() -> None:
    table_metadata = create_table_metadata(
        "test_table",
        ["id"],
        [create_file_format()],
        [
            create_column(
                name="id",
                stages=[create_stage(name="raw"), create_stage(name="processed")],
            ),
            create_column(
                name="type",
                stages=[create_stage(name="processed"), create_stage(name="curated")],
            ),
            create_column(
                name="address",
                stages=[create_stage(name="raw"), create_stage(name="curated")],
            ),
            create_column(
                name="name",
                stages=[create_stage(name="processed"), create_stage(name="curated")],
            ),
        ],
    )

    columns = table_metadata.get_columns_for_stage("processed")
    assert [column.name for column in columns] == ["id", "type", "name"]


def test_table_metadata_get_columns_for_stage_empty() -> None:
    table_metadata = create_table_metadata(
        "test_table",
        ["id"],
        [create_file_format()],
        [
            create_column(
                name="id",
                stages=[create_stage(name="raw"), create_stage(name="curated")],
            ),
            create_column(
                name="type",
                stages=[create_stage(name="raw"), create_stage(name="curated")],
            ),
            create_column(
                name="address",
                stages=[create_stage(name="raw"), create_stage(name="curated")],
            ),
            create_column(
                name="name",
                stages=[create_stage(name="raw_hist"), create_stage(name="curated")],
            ),
        ],
    )

    assert not table_metadata.get_columns_for_stage("processed")


def test_table_metadata_get_column_valid() -> None:
    table_metadata = create_table_metadata(
        "test_table",
        ["id"],
        [create_file_format()],
        [create_column(name="id"), create_column(name="name")],
    )

    assert table_metadata.get_column("name") == create_column(name="name")


def test_table_metadata_get_column_invalid() -> None:
    table_metadata = create_table_metadata(
        "test_table",
        ["id"],
        [create_file_format()],
        [create_column(name="id"), create_column(name="name")],
    )

    with pytest.raises(m.InvalidColumnError) as e:
        table_metadata.get_column("invalid")

    assert (
        str(e.value)
        == "Column 'invalid' was not found in the metadata for table 'test_table'."
    )


def test_create_old_style_metadata() -> None:
    table_metadata = create_table_metadata(
        "test_table",
        ["id"],
        [create_file_format(name="curated", file_format="csv"), create_file_format()],
        [
            create_column(
                name="id",
                stages=[
                    create_stage(data_type="date32"),
                    create_stage(name="curated", data_type="string", pattern="blah"),
                ],
            ),
            create_column(
                name="name",
                stages=[
                    create_stage(data_type="int32"),
                    create_stage(
                        name="curated", data_type="float64", pattern="nrettap"
                    ),
                ],
            ),
        ],
    )

    curated_output = table_metadata.create_old_style_metadata("curated")
    raw_output = table_metadata.create_old_style_metadata("raw")

    assert curated_output == {
        "columns": [
            {"name": "id", "type": "string"},
            {"name": "name", "type": "float64"},
        ],
        "_converted_from": "arrow_schema",
        "$schema": "https://link-to-schema.com",
        "name": "test_table",
        "description": "description",
        "file_format": "csv",
        "sensitive": False,
        "primary_key": [],
        "partitions": ["id"],
    }

    assert raw_output == {
        "columns": [
            {"name": "id", "type": "date32"},
            {"name": "name", "type": "int32"},
        ],
        "_converted_from": "arrow_schema",
        "$schema": "https://link-to-schema.com",
        "name": "test_table",
        "description": "description",
        "file_format": "parquet",
        "sensitive": False,
        "primary_key": [],
        "partitions": [],
    }


def test_metadata_valid() -> None:
    metadata = m.MetaData(
        database="test",
        tables={
            "test_table": create_table_metadata(
                "test_table", ["id"], [create_file_format()], [create_column()]
            ),
            "test_table2": create_table_metadata(
                "test_table2",
                ["name"],
                [create_file_format(name="processed")],
                [create_column(name="name")],
            ),
        },
    )

    assert metadata.database == "test"
    assert metadata.tables["test_table"].file_formats[0].format == "parquet"
    assert metadata.tables["test_table"].partitions == ["id"]
    assert metadata.tables["test_table2"].columns[0].name == "name"
    assert metadata.tables["test_table2"].file_formats[0].name == "processed"


def test_metadata_get_table_metadata_valid() -> None:
    metadata = m.MetaData(
        database="test",
        tables={
            "test_table": create_table_metadata(
                "test_table", ["id"], [create_file_format()], [create_column()]
            ),
            "test_table2": create_table_metadata(
                "test_table2",
                ["name"],
                [create_file_format(name="processed")],
                [create_column(name="name")],
            ),
        },
    )

    table_metadata = metadata.get_table_metadata("test_table2")

    assert table_metadata.partitions == ["name"]
    assert table_metadata.name == "test_table2"
    assert table_metadata.columns[0].name == "name"


def test_metadata_get_table_metadata_invalid() -> None:
    metadata = m.MetaData(
        database="test",
        tables={
            "test_table": create_table_metadata(
                "test_table", ["id"], [create_file_format()], [create_column()]
            ),
            "test_table2": create_table_metadata(
                "test_table2",
                ["name"],
                [create_file_format(name="processed")],
                [create_column(name="name")],
            ),
        },
    )

    with pytest.raises(m.InvalidTableError) as e:
        metadata.get_table_metadata("invalid")

    assert (
        str(e.value) == "Table 'invalid' is not configured in the metadata for 'test'"
    )


def test_output_to_df() -> None:
    metadata = m.MetaData(
        database="test",
        tables={
            "test_table": create_table_metadata(
                "test_table",
                ["id"],
                [create_file_format()],
                [
                    create_column(
                        stages=[create_stage(), create_stage(name="curated")]
                    ),
                    create_column(
                        name="name",
                        stages=[create_stage(name="curated", data_type="int64")],
                    ),
                    create_column(name="address", stages=[create_stage()]),
                ],
            ),
            "test_table2": create_table_metadata(
                "test_table2",
                ["name"],
                [create_file_format(name="processed")],
                [
                    create_column(
                        stages=[create_stage(), create_stage(name="curated")]
                    ),
                    create_column(
                        name="name",
                        stages=[create_stage(name="curated", data_type="float64")],
                    ),
                    create_column(name="address", stages=[create_stage()]),
                ],
            ),
        },
    )

    act_df = metadata.output_to_df()
    act_df = act_df.reset_index(drop=True)

    exp_df = pd.DataFrame(
        data={
            "System": ["test", "test", "test", "test"],
            "Dataset": ["test", "test", "test", "test"],
            "Data Table": ["test_table", "test_table", "test_table2", "test_table2"],
            "Data Field": ["id", "name", "id", "name"],
            "Description": ["", "", "", ""],
            "Data Type": ["string", "int64", "string", "float64"],
            "Nullable": [True, True, True, True],
        }
    )

    pd.testing.assert_frame_equal(act_df, exp_df)


def test_load_metadata() -> None:
    metadata = m.load_metadata(Path("tests/data/meta_data"), "test_database")

    assert list(metadata.tables.keys()) == ["test_table", "test_table2"]
    assert metadata.tables["test_table"].columns[0].name == "id"
    assert metadata.tables["test_table2"].columns[0].name == "ids"


def test_output_metadata_as_csv() -> None:
    m.output_metadata_as_csv(
        Path("tests/data/meta_data/new_metadata"),
        ["test_a", "test_b"],
        Path("tests/data"),
    )

    act_df = pd.read_csv("tests/data/metadata.csv")
    act_df = act_df.sort_values(["System", "Data Table", "Data Field"])
    act_df = act_df.reset_index(drop=True)

    exp_df = pd.DataFrame(
        data={
            "System": ["test_a"] * 6 + ["test_b"] * 3,
            "Dataset": ["test_a"] * 6 + ["test_b"] * 3,
            "Data Table": ["test_a"] * 3 + ["test_a2"] * 3 + ["test_b"] * 3,
            "Data Field": [
                "name",
                "email",
                "phone",
                "id",
                "town",
                "phone",
                "name",
                "email",
                "phone",
            ],
            "Description": [np.NaN] * 9,
            "Data Type": ["string", "string", "date32"] * 3,
            "Nullable": [True] * 9,
        }
    )
    exp_df = exp_df.sort_values(["System", "Data Table", "Data Field"])
    exp_df = exp_df.reset_index(drop=True)

    pd.testing.assert_frame_equal(act_df, exp_df, check_dtype=False)

    Path("tests/data/metadata.csv").unlink()
