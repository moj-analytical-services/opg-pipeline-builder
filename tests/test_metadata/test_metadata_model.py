from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from opg_pipeline.metadata import metadata_model as m

from tests.conftest import TEST_METADATA_PATH


def create_stage(
    name: str = "raw", type_name: str = "string", pattern: str = "pattern"
) -> m.Stage:
    return m.Stage(name=name, type=type_name, pattern=pattern)


def create_column(
    name: str = "id", nullable: bool = True, stages: list[m.Stage] = [create_stage()]
) -> m.Column:
    return m.Column(name=name, nullable=nullable, stages=stages)


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


def test_stage_valid() -> None:
    stage = create_stage()
    assert stage.name == "raw"
    assert stage.type == "string"
    assert stage.pattern == "pattern"


@pytest.mark.parametrize(
    ("name", "type_name", "exception", "err"),
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
    ],
)
def test_stage_invalid(name: str, type_name: str, exception: Any, err: str) -> None:
    with pytest.raises(exception) as e:
        create_stage(name, type_name)

    assert str(e.value) == err


def test_column_valid() -> None:
    column = create_column(
        stages=[
            create_stage(),
            create_stage(name="processed"),
            create_stage(name="curated", type_name="int64"),
        ],
    )
    assert column.name == "id"
    assert column.nullable
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
            create_stage(name="curated", type_name="int64"),
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
            create_stage(name="curated", type_name="int64"),
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


def test_load_metadata() -> None:
    with patch(
        "opg_pipeline.metadata.metadata_model.METADATA_PATH",
        Path(TEST_METADATA_PATH),
    ):
        metadata = m.load_metadata("test_database")

    assert list(metadata.tables.keys()) == ["test_table", "test_table2"]
    assert metadata.tables["test_table"].columns[0].name == "id"
    assert metadata.tables["test_table2"].columns[0].name == "ids"
