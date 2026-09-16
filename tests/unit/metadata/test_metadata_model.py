import re
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from opg_pipeline_builder.models import metadata_model as m
from opg_pipeline_builder.models import modelling_exceptions as exc

####################
### TEST HELPERS ###
####################


def create_column(
    name: str = "id",
    description: str = "description",
    semantic_type: str = "generic_string",
    etl_stages: list[str] | None = None,
    sensitive: bool = False,
    is_composite_key: bool = False,
    is_partition: bool = False,
    input_data_type: Any = "str",
    output_data_type: Any = "str",
    input_value_format: str = "",
    output_value_format: str = "",
    regex_pattern: str = "",
    nullable: bool = False,
    allowed_values: list[str | int] | None = None,
    default_value: str | int | None = "default",
) -> m.Column:
    """Create a Column instance with the given parameters."""

    allowed_values = allowed_values or []
    etl_stages = etl_stages or ["raw", "curated"]

    return m.Column.model_validate(
        {
            "name": name,
            "description": description,
            "semantic_type": semantic_type,
            "etl_stages": etl_stages,
            "sensitive": sensitive,
            "is_composite_key": is_composite_key,
            "is_partition": is_partition,
            "input_data_type": input_data_type,
            "output_data_type": output_data_type,
            "input_value_format": input_value_format,
            "output_value_format": output_value_format,
            "regex_pattern": regex_pattern,
            "nullable": nullable,
            "allowed_values": allowed_values,
            "default_value": default_value,
        },
        context={"table_name": "test_table"},
    )


def create_file_format(
    stage: str = "raw", file_format: str = "parquet"
) -> m.FileFormat:
    return m.FileFormat(stage=stage, format=file_format)


def create_table_metadata(
    name: str,
    file_formats: list[m.FileFormat],
    columns: list[m.Column],
    description: str = "description",
) -> m.TableMetaData:
    return m.TableMetaData(
        name=name,
        description=description,
        file_formats=file_formats,
        columns=columns,
    )


####################
### COLUMN TESTS ###
####################


def test_column_valid() -> None:
    column = create_column(
        name="address",
        description="An address",
        semantic_type="postcode",
        sensitive=True,
        is_composite_key=True,
        is_partition=True,
        input_data_type="int",
        output_data_type="str",
        input_value_format="datetime",
        output_value_format="datetime",
        regex_pattern=".*",
        nullable=False,
        allowed_values=[1, 2],
        default_value=1,
    )
    assert column.name == "address"
    assert column.description == "An address"
    assert column.semantic_type == "postcode"
    assert column.etl_stages == ["raw", "curated"]
    assert column.sensitive is True
    assert column.is_composite_key is True
    assert column.is_partition is True
    assert column.input_data_type == int
    assert column.output_data_type == str
    assert column.input_value_format == "datetime"
    assert column.output_value_format == "datetime"
    assert column.regex_pattern == ".*"
    assert not column.nullable
    assert column.allowed_values == [1, 2]
    assert column.default_value == 1


def test_column_validate_name_valid() -> None:
    """Test that an invalid column name raises InvalidColumnNameError"""
    with patch(
        "opg_pipeline_builder.models.metadata_model.is_valid_identifier"
    ) as mock_valid:
        mock_valid.return_value = ""
        col = create_column()
    assert col.name == "id"


def test_column_validate_name_invalid(caplog: pytest.LogCaptureFixture) -> None:
    """Test that an invalid column name raises InvalidColumnNameError"""
    with patch(
        "opg_pipeline_builder.models.metadata_model.is_valid_identifier"
    ) as mock_valid:
        mock_valid.return_value = "Validation error"
        with pytest.raises(exc.InvalidColumnNameError):
            create_column()

    assert any("Validation error" in record.message for record in caplog.records)


@pytest.mark.parametrize(
    ("semantic_type"),
    [
        ("postcode"),
        ("boolean_flag"),
        ("country"),
    ],
)
def test_column_validate_semantic_type_valid(semantic_type: str) -> None:
    """Test that valid semantic types are accepted."""
    create_column(semantic_type=semantic_type)


@pytest.mark.parametrize(
    ("semantic_type"),
    [
        ("pcode"),
        ("invalid"),
        (""),
    ],
)
def test_column_validate_semantic_type_invalid(
    semantic_type: str, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that invalid semantic types raise InvalidSemanticTypeError."""
    with pytest.raises(
        exc.InvalidSemanticTypeError,
        match=f"Semantic type '{semantic_type}' is not in the ALLOWED_SEMANTIC_TYPES constant",
    ):
        create_column(semantic_type=semantic_type)
    assert any("Semantic type" in record.message for record in caplog.records)


@pytest.mark.parametrize(
    ("etl_stage"),
    [(["raw"]), (["curated"]), (["raw", "curated"]), ([])],
)
def test_column_validate_etl_stage_valid(etl_stage: list[str]) -> None:
    """Test that valid ETL stages are accepted."""
    create_column(etl_stages=etl_stage)


@pytest.mark.parametrize(
    ("etl_stage", "exp_err"),
    [
        (["invalid"], "ETL stage 'invalid' is not in the ALLOWED_ETL_STAGES constant"),
        (
            ["invalid", "raw"],
            "ETL stage 'invalid' is not in the ALLOWED_ETL_STAGES constant",
        ),
        (
            ["invalid", "also_invalid"],
            "ETL stage 'invalid' is not in the ALLOWED_ETL_STAGES constant",
        ),
        (
            ["raw", "invalid"],
            "ETL stage 'invalid' is not in the ALLOWED_ETL_STAGES constant",
        ),
    ],
)
def test_column_validate_etl_stage_invalid(
    etl_stage: list[str], exp_err: str, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that invalid ETL stages raise InvalidStageError."""
    with pytest.raises(exc.InvalidStageError, match=exp_err):
        create_column(etl_stages=etl_stage)
    assert any(exp_err in record.message for record in caplog.records)


def test_column_validate_etl_stage_empty(caplog: pytest.LogCaptureFixture) -> None:
    """Test that an empty ETL stages list raises InvalidStageError."""
    with pytest.raises(exc.InvalidStageError, match="ETL stages list cannot be empty"):
        m.Column(
            name="name",
            semantic_type="postcode",
            etl_stages=[],
            input_data_type="str",
            output_data_type="str",
        )
    assert any(
        "ETL stages list cannot be empty" in record.message for record in caplog.records
    )


@pytest.mark.parametrize(
    ("data_type"),
    [
        "str",
        "list[str]",
        "list[list[str]]",
        "int",
        "list[int]",
        "float",
        "list[float]",
        "bool",
        "list[bool]",
        "date",
        "list[date]",
        "datetime",
        "NoneType",
        "list[dict[str, str]]",
    ],
)
def test_column_validate_data_type_valid(data_type: type) -> None:
    """Test that valid data types are accepted."""
    create_column(
        input_data_type=data_type, output_data_type=data_type, default_value=None
    )


@pytest.mark.parametrize(
    ("input_data_type", "output_data_type", "err_attr"),
    [
        ("list[list[list[str]]]", "str", "input_data_type"),
        ("str", "dict[str, list[str]]", "output_data_type"),
        ("list[datetime]", "list[list[str]]", "input_data_type"),
        (
            "list[dict[str, str]]",
            "list[list[list[str]]]",
            "output_data_type",
        ),
        ("NoneType", "list[list[list[str]]]", "output_data_type"),
    ],
)
def test_column_validate_data_type_invalid(
    input_data_type: str,
    output_data_type: str,
    err_attr: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that invalid data types raise InvalidDataTypeError."""
    err_attr_val = (
        input_data_type if err_attr == "input_data_type" else output_data_type
    )
    with pytest.raises(
        exc.InvalidTypeError,
        match=re.escape(f"Data type '{err_attr_val}' for field '{err_attr}' is not"),
    ):
        create_column(
            input_data_type=input_data_type, output_data_type=output_data_type
        )
    assert any(
        f"Data type '{err_attr_val}' for field '{err_attr}' is not" in record.message
        for record in caplog.records
    )


def test_column_validate_value_format_valid() -> None:
    """Test that a valid value format is accepted."""
    create_column(input_value_format="datetime", output_value_format="datetime")


@pytest.mark.parametrize(
    ("input_value_format", "output_value_format", "err_attr"),
    [
        ("invalid", "datetime", "input_value_format"),
        ("datetime", "invalid", "output_value_format"),
        ("invalid", "invalid", "input_value_format"),
    ],
)
def test_column_validate_value_format_invalid(
    input_value_format: str,
    output_value_format: str,
    err_attr: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that an invalid value format raises InvalidValueFormatError."""
    err_attr_val = (
        input_value_format if err_attr == "input_value_format" else output_value_format
    )
    with pytest.raises(
        exc.InvalidFormatError,
        match=re.escape(f"Value format '{err_attr_val}' for field '{err_attr}' is not"),
    ):
        create_column(
            input_value_format=input_value_format,
            output_value_format=output_value_format,
        )
    assert any(
        f"Value format '{err_attr_val}' for field '{err_attr}' is not" in record.message
        for record in caplog.records
    )


@pytest.mark.parametrize(
    ("key", "partition", "nullable"),
    [
        (True, False, False),
        (False, False, True),
    ],
)
def test_column_partition_and_composite_keys_not_nullable_valid(
    key: bool, partition: bool, nullable: bool
) -> None:
    """Test that partition and composite keys cannot be nullable."""
    create_column(is_composite_key=key, is_partition=partition, nullable=nullable)


@pytest.mark.parametrize(
    ("key", "partition", "nullable"),
    [
        (True, False, True),
        (False, True, True),
        (True, True, True),
    ],
)
def test_column_partition_and_composite_keys_not_nullable_invalid(
    key: bool, partition: bool, nullable: bool, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that partition and composite keys cannot be nullable."""
    with pytest.raises(
        exc.InvalidColumnError,
        match="Column 'id' is part of a composite key or partition and cannot be nullable",
    ):
        create_column(is_composite_key=key, is_partition=partition, nullable=nullable)

    assert any(
        "Column 'id' is part of a composite key or partition and cannot be nullable"
        in record.message
        for record in caplog.records
    )


@pytest.mark.parametrize(
    ("data_type", "allowed_values"),
    [
        ("str", ["a", "bee"]),
        ("int", [1, 2]),
    ],
)
def test_column_allowed_values_match_input_data_type_valid(
    data_type: type, allowed_values: list[Any]
) -> None:
    """Test that allowed values match the input data type."""
    create_column(
        input_data_type=data_type,
        allowed_values=allowed_values,
        default_value=allowed_values[0],
    )


@pytest.mark.parametrize(
    ("data_type", "allowed_values"),
    [
        ("list[str]", [["a"], ["a", "b"]]),
        ("list[list[str]]", [[["a"]], [["a", "b"]]]),
        ("list[int]", [[1], [1, 2, 3]]),
        ("float", [12.3334324, 22.4939332]),
        ("list[float]", [[12.3334324], [12.3334324, 22.4939332]]),
        ("bool", [True, False]),
        ("list[bool]", [[True], [True, False]]),
        ("date", [date(2024, 6, 1), date(2024, 6, 2)]),
        ("list[date]", [[date(2024, 6, 1), date(2024, 6, 1), date(2024, 6, 2)]]),
        (
            "datetime",
            [
                datetime(2024, 6, 1, 12, tzinfo=UTC),
                datetime(2024, 6, 2, 12, tzinfo=UTC),
            ],
        ),
        (
            "datetime",
            [
                datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC),
                datetime(2024, 6, 2, 12, 0, 0, tzinfo=UTC),
            ],
        ),
        ("NoneType", [None]),
    ],
)
def test_column_allowed_values_match_input_data_type_skipped(
    data_type: type, allowed_values: list[Any], caplog: pytest.LogCaptureFixture
) -> None:
    """Test that allowed values check is skipped for unsupported data types."""
    create_column(input_data_type=data_type, allowed_values=allowed_values)
    assert (
        "Only check allowed values for 'str' and 'int' input data types. Skipping check for"
        in caplog.text
    )


@pytest.mark.parametrize(
    ("data_type_str", "data_type", "allowed_values"),
    [
        ("int", int, ["a", "b"]),
        ("str", str, [1, 2]),
    ],
)
def test_column_allowed_values_match_input_data_type_invalid(
    data_type_str: str,
    data_type: type,
    allowed_values: list[Any],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that allowed values check raises an error for mismatched input data types."""
    with pytest.raises(exc.InvalidTypeError):
        create_column(input_data_type=data_type_str, allowed_values=allowed_values)

    assert (
        f"Allowed value '{allowed_values[0]}' with type '{type(allowed_values[0])}' does not match the input data type '{data_type}'"
        in caplog.records[0].message
    )


@pytest.mark.parametrize(
    ("data_type", "default_value"),
    [
        ("str", "a"),
        ("int", 1),
    ],
)
def test_column_default_value_match_input_data_type_valid(
    data_type: type, default_value: str | int
) -> None:
    """Test that default value match the input data type."""
    create_column(input_data_type=data_type, default_value=default_value)


@pytest.mark.parametrize(
    ("data_type", "default_value"),
    [
        ("list[str]", ["a"]),
        ("list[int]", [1]),
        ("float", 22.4939332),
        ("list[float]", [12.3334324]),
        ("bool", True),
        ("list[bool]", True),
        ("date", date(2024, 6, 1)),
        ("list[date]", [date(2024, 6, 1)]),
        ("datetime", datetime(2024, 6, 1, 12, tzinfo=UTC)),
    ],
)
def test_column_default_value_match_input_data_type_skipped(
    data_type: type, default_value: Any, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that default value check is skipped for unsupported data types."""
    create_column(input_data_type=data_type, default_value=default_value)
    assert (
        "Only check default value for 'str' and 'int' input data types. Skipping check for"
        in caplog.text
    )


@pytest.mark.parametrize(
    ("data_type_str", "data_type", "default_value"),
    [
        ("int", int, "a"),
        ("str", str, 1),
    ],
)
def test_column_default_value_match_input_data_type_invalid(
    data_type_str: str,
    data_type: type,
    default_value: Any,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that default value check raises an error for mismatched input data types."""
    with pytest.raises(exc.InvalidTypeError):
        create_column(input_data_type=data_type_str, default_value=default_value)

    assert (
        f"Default value '{default_value}' with type '{type(default_value)}' does not match the input data type '{data_type}'"
        in caplog.records[0].message
    )


@pytest.mark.parametrize(
    ("stages", "check_stage", "expected"),
    [
        (["raw", "curated"], "raw", True),
        (["raw", "curated"], "curated", True),
        (["raw"], "curated", False),
        (["raw", "curated"], "invalid", False),
    ],
)
def test_column_exists_in_stage(
    stages: list[str], check_stage: str, expected: bool
) -> None:
    """Test that exists_in_stage returns the correct boolean value."""
    column = create_column(etl_stages=stages)
    assert column.exists_in_stage(check_stage) is expected


@pytest.mark.parametrize(
    ("allowed_values", "value", "expected", "log"),
    [
        (["a", "b", "c"], "a", True, None),
        (["a", "b", "c"], "d", False, None),
        (
            None,
            "e",
            False,
            "There are no allowed values for column 'id'; skipping allowed values check.",
        ),
    ],
)
def test_column_value_is_allowed(
    allowed_values: list[Any],
    value: Any,
    expected: bool,
    log: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that value_is_allowed returns the correct boolean value."""
    column = create_column(allowed_values=allowed_values)
    assert column.value_is_allowed(value) is expected
    if log:
        assert log in caplog.records[0].message


#########################
### FILE FORMAT TESTS ###
#########################


@pytest.mark.parametrize(("stage"), [("raw"), ("curated")])
def test_file_format_validate_stage_valid(stage: str) -> None:
    """Test that a valid ETL stage is correctly validated."""
    file_format = create_file_format(stage=stage)
    assert file_format.stage == stage


def test_file_format_validate_stage_invalid(caplog: pytest.LogCaptureFixture) -> None:
    """Test that an invalid ETL stage raises an error."""
    with pytest.raises(exc.InvalidStageError):
        create_file_format(stage="invalid")

    assert (
        "ETL stage 'invalid' is not in the ALLOWED_ETL_STAGES constant"
        in caplog.records[0].message
    )


@pytest.mark.parametrize(("file_format"), [("parquet"), ("json"), ("csv"), ("xlsx")])
def test_file_format_validate_file_format_valid(file_format: str) -> None:
    """Test that a valid file format is correctly validated."""
    file_format_cls = create_file_format(file_format=file_format)
    assert file_format_cls.format == file_format


def test_file_format_validate_file_format_invalid(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that an invalid file format raises an error."""
    with pytest.raises(exc.InvalidFormatError):
        create_file_format(file_format="invalid")

    assert (
        "File format 'invalid' is not in the ALLOWED_FILE_FORMATS constant"
        in caplog.records[0].message
    )


############################
### TABLE METADATA TESTS ###
############################


def test_table_metadata_valid() -> None:
    """Test that creating valid table metadata works correctly."""
    table_metadata = create_table_metadata(
        name="test_table",
        file_formats=[
            create_file_format(stage="raw"),
            create_file_format(stage="curated"),
        ],
        columns=[create_column()],
        description="description",
    )

    assert table_metadata.name == "test_table"
    assert table_metadata.file_formats[0].stage == "raw"
    assert table_metadata.columns[0].name == "id"
    assert table_metadata.description == "description"


def test_table_metadata_all_fields_unique_valid() -> None:
    """Test that all fields in the table metadata are unique."""
    create_table_metadata(
        name="test_table",
        file_formats=[
            create_file_format(stage="raw"),
            create_file_format(stage="curated"),
        ],
        columns=[
            create_column(name="id"),
            create_column(name="type"),
            create_column(name="description"),
            create_column(name="created_at"),
        ],
    )
    assert True


@pytest.mark.parametrize(
    ("field_names", "duplicate_fields"),
    [
        (["type", "type"], ["type"]),
        (["id", "id", "id", "id", "id"], ["id"]),
        (["description", "id", "address", "type", "description"], ["description"]),
        (
            ["id", "type", "description", "address", "phone", "type", "id"],
            ["id", "type"],
        ),
    ],
)
def test_table_metadata_all_fields_unique_invalid(
    field_names: list[str],
    duplicate_fields: list[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that having duplicate fields in the table metadata raises an error."""
    with pytest.raises(exc.DuplicateFieldsError):
        create_table_metadata(
            "test_table",
            [create_file_format()],
            [create_column(name=field_name) for field_name in field_names],
        )

    log_messages = [record.message for record in caplog.records]
    for duplicate_field in duplicate_fields:
        assert f"Duplicate field found: '{duplicate_field}'" in log_messages


def test_table_metadata_all_file_format_stages_unique_valid() -> None:
    """Test that all file format stages for a table are unique."""
    table_metadata = create_table_metadata(
        "test_table",
        [
            create_file_format(stage="raw", file_format="csv"),
            create_file_format(stage="curated"),
        ],
        [create_column()],
    )

    stages = [ff.stage for ff in table_metadata.file_formats]
    assert len(stages) == len(set(stages))


@pytest.mark.parametrize(
    ("stages", "duplicate_stage"),
    [
        (["raw", "raw"], "raw"),
        (["curated", "raw", "curated"], "curated"),
    ],
)
def test_table_metadata_all_file_format_stages_unique_invalid(
    stages: list[str], duplicate_stage: str, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that having duplicate file format stages in the table metadata raises an error."""
    with pytest.raises(exc.DuplicateFileFormatStagesError):
        create_table_metadata(
            "test_table",
            [create_file_format(stage=stage) for stage in stages],
            [create_column()],
        )

    assert (
        f"Duplicate file format stage found: {duplicate_stage}"
        in caplog.records[0].message
    )


@pytest.mark.parametrize(
    ("format_stages", "column_stages"),
    [
        (["raw"], [["raw"]]),
        (["raw", "curated"], [["raw", "curated"]]),
        (["raw", "curated"], [["raw"], ["curated"]]),
        (["raw", "curated"], [["raw"], ["raw"], ["curated"]]),
        (["raw", "curated"], [["raw"], ["raw", "curated"]]),
        (["curated"], [["curated"], ["curated"]]),
    ],
)
def test_table_metadata_column_stages_match_file_format_stages_valid(
    format_stages: list[str], column_stages: list[list[str]]
) -> None:
    """Test that column stages match the file format stages for a table."""

    columns = []
    for num, stages in enumerate(column_stages):
        columns.append(create_column(name=f"id_{num}", etl_stages=stages))
    table_metadata = create_table_metadata(
        "test_table",
        [create_file_format(stage=stage) for stage in format_stages],
        columns,
    )

    assert format_stages == table_metadata.etl_stages


@pytest.mark.parametrize(
    ("format_stages", "column_stages", "unreconciled_formats", "unreconciled_columns"),
    [
        (["raw"], [["curated"]], ["raw"], ["curated"]),
        (["curated"], [["raw"]], ["curated"], ["raw"]),
        (["raw"], [["curated"], ["curated"], ["raw"]], [], ["curated"]),
        (["raw"], [["curated"], ["raw"], ["curated"]], [], ["curated"]),
        (["raw", "curated"], [["curated"]], ["raw"], []),
        (["raw", "curated"], [["raw"]], ["curated"], []),
        (["raw", "curated"], [["raw"], ["raw"], ["raw"]], ["curated"], []),
        (["raw", "curated"], [["curated"], ["curated"], ["curated"]], ["raw"], []),
        (
            ["curated"],
            [["curated"], ["curated"], ["curated"], ["curated"], ["raw"]],
            [],
            ["raw"],
        ),
    ],
)
def test_table_metadata_column_stages_match_file_format_stages_invalid(
    format_stages: list[str],
    column_stages: list[list[str]],
    unreconciled_formats: list[str],
    unreconciled_columns: list[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that column stages match the file format stages for a table."""

    columns = []
    for num, stages in enumerate(column_stages):
        columns.append(create_column(name=f"id_{num}", etl_stages=stages))
    with pytest.raises(exc.InvalidStageError) as exc_info:
        create_table_metadata(
            "test_table",
            [create_file_format(stage=stage) for stage in format_stages],
            columns,
        )

    for stage in unreconciled_formats:
        assert (
            f"ETL stage '{stage}' is defined in the file formats, but not for any columns"
            in caplog.text
        )
        assert (
            f"The following ETL stages are defined for file formats, but not for any columns: [{', '.join(unreconciled_formats)}]"
            in str(exc_info.value)
        )
    for stage in unreconciled_columns:
        assert (
            f"ETL stage '{stage}' is defined for columns, but not in the file formats"
            in caplog.text
        )
        assert (
            f"The following ETL stages are defined for columns, but not in the file formats: [{', '.join(unreconciled_columns)}]"
            in str(exc_info.value)
        )


def test_table_metadata_etl_stages() -> None:
    """Test that the etl_stages property returns all stages defined in the file formats."""
    format_stages = ["raw", "curated"]
    table_metadata = create_table_metadata(
        "test_table",
        [create_file_format(stage=stage) for stage in format_stages],
        [create_column()],
    )

    assert table_metadata.etl_stages == format_stages


def test_table_metadata_contains_sensitive_data_exists() -> None:
    """Test that the contains_sensitive_data property correctly identifies sensitive columns."""
    table_metadata = create_table_metadata(
        "test_table",
        [create_file_format(stage="raw"), create_file_format(stage="curated")],
        [
            create_column(name="id", sensitive=False),
            create_column(name="ssn", sensitive=True),
            create_column(name="address", sensitive=True),
            create_column(name="forename", sensitive=True),
        ],
    )

    assert table_metadata.contains_sensitive_data is True


def test_table_metadata_contains_sensitive_data_not_exists() -> None:
    """Test that the contains_sensitive_data property correctly identifies sensitive columns."""
    table_metadata_no_sensitive = create_table_metadata(
        "test_table",
        [create_file_format(stage="raw"), create_file_format(stage="curated")],
        [
            create_column(name="id", sensitive=False),
            create_column(name="name", sensitive=False),
        ],
    )

    assert table_metadata_no_sensitive.contains_sensitive_data is False


def test_table_metadata_composite_key() -> None:
    """Test that the composite_key property returns all columns marked as part of the composite key."""
    table_metadata = create_table_metadata(
        "test_table",
        [create_file_format(stage="raw"), create_file_format(stage="curated")],
        [
            create_column(name="id", is_composite_key=True),
            create_column(name="type", is_composite_key=False),
            create_column(name="address", is_composite_key=True),
        ],
    )

    assert [column.name for column in table_metadata.composite_key] == ["id", "address"]


def test_table_metadata_partition_key() -> None:
    """Test that the partition_key property returns all columns marked as part of the partition key."""
    table_metadata = create_table_metadata(
        "test_table",
        [create_file_format(stage="raw"), create_file_format(stage="curated")],
        [
            create_column(name="id", is_partition=True),
            create_column(name="type", is_partition=False),
            create_column(name="address", is_partition=True),
        ],
    )

    assert [column.name for column in table_metadata.partition_key] == ["id", "address"]


def test_table_metadata_get_file_format_for_stage_valid() -> None:
    """Test that get_file_format_for_stage returns the correct file format for a valid stage."""
    table_metadata = create_table_metadata(
        "test_table",
        [
            create_file_format(stage="raw", file_format="csv"),
            create_file_format(stage="curated", file_format="parquet"),
        ],
        [create_column()],
    )

    file_format = table_metadata.get_file_format_for_stage("curated")
    assert file_format.format == "parquet"


def test_table_metadata_get_file_format_for_stage_invalid(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that get_file_format_for_stage raises an error for an invalid stage."""
    table_metadata = create_table_metadata(
        "test_table",
        [
            create_file_format(stage="raw", file_format="csv"),
            create_file_format(stage="curated", file_format="parquet"),
        ],
        [create_column()],
    )

    with pytest.raises(exc.InvalidStageError):
        table_metadata.get_file_format_for_stage("invalid")

    assert (
        "No file format metadata is configured for stage 'invalid' for table 'test_table'"
        in caplog.text
    )


def test_table_metadata_get_columns_for_stage_populated() -> None:
    """Test that get_columns_for_stage returns the correct columns for a populated stage."""
    table_metadata = create_table_metadata(
        "test_table",
        [create_file_format(stage="raw"), create_file_format(stage="curated")],
        [
            create_column(name="id", etl_stages=["raw", "curated"]),
            create_column(name="type", etl_stages=["curated"]),
            create_column(name="address", etl_stages=["raw"]),
        ],
    )

    columns = table_metadata.get_columns_for_stage("curated")
    assert [column.name for column in columns] == ["id", "type"]


def test_table_metadata_get_columns_for_stage_empty() -> None:
    """Test that get_columns_for_stage returns an empty list for a stage with no columns."""
    table_metadata = create_table_metadata(
        "test_table",
        [create_file_format(stage="raw")],
        [
            create_column(name="id", etl_stages=["raw"]),
            create_column(name="type", etl_stages=["raw"]),
            create_column(name="address", etl_stages=["raw"]),
        ],
    )

    assert not table_metadata.get_columns_for_stage("curated")


def test_table_metadata_get_column_populated() -> None:
    """Test that get_column returns the correct column for a populated table."""
    table_metadata = create_table_metadata(
        "test_table",
        [create_file_format(stage="raw"), create_file_format(stage="curated")],
        [create_column(name="id"), create_column(name="name")],
    )

    column = table_metadata.get_column("name")
    assert column.name == "name"
    assert column.etl_stages == ["raw", "curated"]


def test_table_metadata_get_column_empty(caplog: pytest.LogCaptureFixture) -> None:
    """Test that get_column raises an error for an invalid column."""
    table_metadata = create_table_metadata(
        "test_table",
        [create_file_format(stage="raw"), create_file_format(stage="curated")],
        [create_column(name="id"), create_column(name="name")],
    )

    with pytest.raises(exc.InvalidColumnError):
        table_metadata.get_column("invalid")

    assert (
        "Column 'invalid' was not found in the metadata for table 'test_table'."
        in caplog.text
    )


def test_table_metadata_get_sensitive_columns_populated() -> None:
    """Test that get_sensitive_columns returns the correct sensitive columns for a populated table."""
    table_metadata = create_table_metadata(
        "test_table",
        [create_file_format(stage="raw"), create_file_format(stage="curated")],
        [
            create_column(name="id", sensitive=False),
            create_column(name="ssn", sensitive=True),
            create_column(name="email", sensitive=True),
        ],
    )

    sensitive_columns = table_metadata.get_sensitive_columns()
    assert [column.name for column in sensitive_columns] == ["ssn", "email"]


def test_table_metadata_get_sensitive_columns_empty() -> None:
    """Test that get_sensitive_columns returns an empty list for a table with no sensitive columns."""
    table_metadata = create_table_metadata(
        "test_table",
        [create_file_format(stage="raw"), create_file_format(stage="curated")],
        [
            create_column(name="id", sensitive=False),
            create_column(name="name", sensitive=False),
        ],
    )

    sensitive_columns = table_metadata.get_sensitive_columns()
    assert sensitive_columns == []


def test_metadata_valid() -> None:
    """Test that the MetaData model is correctly instantiated and contains the expected tables and columns."""
    metadata = m.MetaData(
        database="test",
        tables={
            "test_table": create_table_metadata(
                "test_table",
                [create_file_format(stage="raw")],
                [create_column(etl_stages=["raw"])],
            ),
            "test_table2": create_table_metadata(
                "test_table2",
                [create_file_format(stage="curated")],
                [create_column(name="name", etl_stages=["curated"])],
            ),
        },
    )

    assert metadata.database == "test"
    assert metadata.tables["test_table"].file_formats[0].format == "parquet"
    assert metadata.tables["test_table2"].columns[0].name == "name"
    assert metadata.tables["test_table2"].file_formats[0].stage == "curated"


def test_metadata_get_table_metadata_valid() -> None:
    """Test that get_table_metadata returns the correct TableMetaData object for a valid table."""
    metadata = m.MetaData(
        database="test",
        tables={
            "test_table": create_table_metadata(
                "test_table",
                [create_file_format(stage="raw")],
                [create_column(name="name", etl_stages=["raw"])],
            ),
            "test_table2": create_table_metadata(
                "test_table2",
                [create_file_format(stage="curated")],
                [
                    create_column(
                        name="other_name", etl_stages=["curated"], is_partition=True
                    )
                ],
            ),
        },
    )

    table_metadata = metadata.get_table_metadata("test_table2")

    assert [col.name for col in table_metadata.partition_key] == ["other_name"]
    assert table_metadata.name == "test_table2"
    assert table_metadata.columns[0].name == "other_name"
    assert table_metadata.file_formats[0].stage == "curated"


def test_metadata_get_table_metadata_invalid(caplog: pytest.LogCaptureFixture) -> None:
    """Test that get_table_metadata raises an InvalidTableError for an invalid table."""
    metadata = m.MetaData(
        database="test",
        tables={
            "test_table": create_table_metadata(
                "test_table",
                [create_file_format(stage="raw")],
                [create_column(etl_stages=["raw"])],
            ),
            "test_table2": create_table_metadata(
                "test_table2",
                [create_file_format(stage="curated")],
                [
                    create_column(name="name", etl_stages=["curated"]),
                ],
            ),
        },
    )

    with pytest.raises(exc.InvalidTableError):
        metadata.get_table_metadata("invalid")

    assert "Table 'invalid' is not configured in the metadata for 'test'" in caplog.text


def test_output_to_df() -> None:
    """Test the output_to_df method of the MetaData class."""
    metadata = m.MetaData(
        database="test",
        tables={
            "test_table": create_table_metadata(
                "test_table",
                [create_file_format(stage="curated"), create_file_format(stage="raw")],
                [
                    create_column(name="id", etl_stages=["curated"]),
                    create_column(
                        name="name", etl_stages=["curated"], output_data_type="int"
                    ),
                    create_column(
                        name="address", etl_stages=["raw"], output_data_type="str"
                    ),
                ],
            ),
            "test_table2": create_table_metadata(
                "test_table2",
                [create_file_format(stage="curated"), create_file_format(stage="raw")],
                [
                    create_column(etl_stages=["curated"], output_data_type="str"),
                    create_column(
                        name="name", etl_stages=["curated"], output_data_type="float"
                    ),
                    create_column(
                        name="address", etl_stages=["raw"], output_data_type="str"
                    ),
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
            "Data Type": [str, int, str, float],
            "Nullable": [False, False, False, False],
        }
    )

    pd.testing.assert_frame_equal(act_df, exp_df)


def test_load_metadata() -> None:
    """Test that metadata is correctly loaded into the model.

    Also serves to validate a bespoke test metadata file which covers most/all use cases
    for the actual metadata (thus testing the models handles them correctly).
    """
    metadata = m.load_metadata(Path("tests/data/meta_data"), "test_database")

    assert sorted(metadata.tables.keys()) == ["test_table", "test_table_2"]
    assert metadata.tables["test_table"].columns[0].name == "id"
    assert metadata.tables["test_table_2"].columns[0].name == "id2"


def test_output_metadata_as_csv() -> None:
    output_path = Path("tests/data/outputs/metadata/")
    output_path.mkdir(parents=True, exist_ok=True)

    m.output_metadata_as_csv(
        Path("tests/data/meta_data"),
        ["test_database"],
        Path("tests/data/outputs/metadata"),
    )

    act_df = pd.read_csv(Path("tests/data/outputs/metadata/metadata.csv"))
    act_df = act_df.sort_values(["System", "Data Table", "Data Field"])
    act_df = act_df.reset_index(drop=True)

    exp_df = pd.DataFrame(
        data={
            "System": ["test_database"] * 26,
            "Dataset": ["test_database"] * 26,
            "Data Table": ["test_table"] * 13 + ["test_table_2"] * 13,
            "Data Field": [
                "address",
                "case_type",
                "created_date",
                "id",
                "is_open",
                "land_datetime",
                "length_open_for",
                "name",
                "number_of_contacts",
                "processed_datetime",
                "record_created_datetime",
                "record_updated_datetime",
                "triage_level",
                "address2",
                "case_type2",
                "created_date2",
                "id2",
                "is_open2",
                "land_datetime",
                "length_open_for2",
                "name2",
                "number_of_contacts2",
                "processed_datetime",
                "record_created_datetime",
                "record_updated_datetime",
                "triage_level2",
            ],
            "Description": [np.NaN] * 26,
            "Data Type": [
                "<class 'str'>",
                "<class 'str'>",
                "<class 'datetime.date'>",
                "<class 'int'>",
                "<class 'bool'>",
                "<class 'datetime.datetime'>",
                "<class 'int'>",
                "<class 'str'>",
                "<class 'int'>",
                "<class 'datetime.datetime'>",
                "<class 'datetime.datetime'>",
                "<class 'datetime.datetime'>",
                "<class 'int'>",
            ]
            * 2,
            "Nullable": [
                True,
                True,
                True,
                False,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
            ]
            * 2,
        }
    )
    exp_df = exp_df.sort_values(["System", "Data Table", "Data Field"])
    exp_df = exp_df.reset_index(drop=True)

    pd.testing.assert_frame_equal(act_df, exp_df, check_dtype=False)

    (output_path / "metadata.csv").unlink()
    output_path.rmdir()
