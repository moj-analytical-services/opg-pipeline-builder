import re
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Literal
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
from pydantic import ValidationError

from opg_pipeline_builder.logging.log import CustomLogFields
from opg_pipeline_builder.models import metadata_model as m
from opg_pipeline_builder.models import modelling_exceptions as exc


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
    **extra_fields: Any,
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
            **extra_fields,
        },
        context={"table_name": "test_table"},
    )


def create_file_format(
    stage: str = "raw", file_format: str = "parquet"
) -> m.FileFormat:
    return m.FileFormat.model_validate(
        {"stage": stage, "format": file_format},
        context={"table_name": "test_table"},
    )


def create_table_metadata(
    name: str,
    file_formats: list[m.FileFormat],
    columns: list[m.Column],
    description: str = "description",
    **extra_fields: Any,
) -> m.TableMetaData:
    return m.TableMetaData(
        name=name,
        description=description,
        file_formats=file_formats,
        columns=columns,
        **extra_fields,
    )


def assert_log_record(
    caplog: pytest.LogCaptureFixture,
    message: str,
    table: str = "test_table",
    field: str = "N/A",
    stage: Literal["Start", "Processing", "End"] = "Processing",
) -> None:
    """Assert that a log message has the expected structured metadata."""
    record = next(record for record in caplog.records if record.getMessage() == message)
    if not record:
        raise AssertionError(f"Log message '{message}' not found")
    assert record.__dict__["custom_fields"] == CustomLogFields(
        process_stage=stage,
        table=table,
        field=field,
    )


class TestColumn:
    def test_column_valid(self) -> None:
        """Test that a column with valid attributes is created correctly."""
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

    def test_validate_name_valid(self) -> None:
        """Test that a valid column name is accepted."""
        with patch(
            "opg_pipeline_builder.models.metadata_model.is_valid_identifier"
        ) as mock_valid:
            mock_valid.return_value = ""
            col = create_column()
        assert col.name == "id"

    def test_validate_name_invalid(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test that an invalid column name raises InvalidColumnNameError"""
        with patch(
            "opg_pipeline_builder.models.metadata_model.is_valid_identifier"
        ) as mock_valid:
            mock_valid.return_value = "Validation error"
            with pytest.raises(exc.InvalidColumnNameError):
                create_column()

        assert_log_record(caplog, "Validation error", field="name")

    @pytest.mark.parametrize(
        ("semantic_type"),
        [
            ("postcode"),
            ("boolean_flag"),
            ("country"),
        ],
    )
    def test_validate_semantic_type_valid(self, semantic_type: str) -> None:
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
    def test_validate_semantic_type_invalid(
        self, semantic_type: str, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that invalid semantic types raise InvalidSemanticTypeError."""
        with pytest.raises(
            exc.InvalidSemanticTypeError,
            match=f"Semantic type '{semantic_type}' is not in the ALLOWED_SEMANTIC_TYPES constant",
        ):
            create_column(semantic_type=semantic_type)
        assert_log_record(
            caplog,
            f"Semantic type '{semantic_type}' is not in the ALLOWED_SEMANTIC_TYPES constant",
            field="semantic_type",
        )

    @pytest.mark.parametrize(
        ("etl_stage"),
        [(["raw"]), (["curated"]), (["raw", "curated"]), ([])],
    )
    def test_validate_etl_stage_valid(self, etl_stage: list[str]) -> None:
        """Test that valid ETL stages are accepted."""
        create_column(etl_stages=etl_stage)

    @pytest.mark.parametrize(
        ("etl_stage", "exp_err"),
        [
            (
                ["invalid"],
                "ETL stage 'invalid' is not in the ALLOWED_ETL_STAGES constant",
            ),
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
            (
                ["raw", "curated", "invalid"],
                "ETL stage 'invalid' is not in the ALLOWED_ETL_STAGES constant",
            ),
        ],
    )
    def test_validate_etl_stage_invalid(
        self, etl_stage: list[str], exp_err: str, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that invalid ETL stages raise InvalidStageError."""
        with pytest.raises(exc.InvalidStageError, match=exp_err):
            create_column(etl_stages=etl_stage)
        assert_log_record(caplog, exp_err, field="etl_stages")

    def test_validate_etl_stage_empty(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test that an empty ETL stages list raises InvalidStageError."""
        with pytest.raises(
            exc.InvalidStageError, match="ETL stages list cannot be empty"
        ):
            m.Column(
                name="name",
                semantic_type="postcode",
                etl_stages=[],
                input_data_type="str",
                output_data_type="str",
            )
        assert_log_record(caplog, "ETL stages list cannot be empty", "", "etl_stages")

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
    def test_validate_data_type_valid(self, data_type: type) -> None:
        """Test that valid data types are accepted."""
        create_column(
            input_data_type=data_type, output_data_type=data_type, default_value=None
        )

    @pytest.mark.parametrize(
        ("in_data_type", "out_data_type", "err_attr"),
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
    def test_validate_data_type_invalid(
        self,
        in_data_type: str,
        out_data_type: str,
        err_attr: str,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Test that invalid data types raise InvalidDataTypeError."""
        err_attr_val = in_data_type if err_attr == "input_data_type" else out_data_type
        with pytest.raises(
            exc.InvalidTypeError,
            match=re.escape(
                f"Data type '{err_attr_val}' for field '{err_attr}' is not"
            ),
        ):
            create_column(input_data_type=in_data_type, output_data_type=out_data_type)

        assert_log_record(
            caplog,
            f"Data type '{err_attr_val}' for field '{err_attr}' is not in the ALLOWED_DATA_TYPES constant",
            field=err_attr,
        )

    def test_validate_value_format_valid(self) -> None:
        """Test that a valid value format is accepted."""
        create_column(input_value_format="datetime", output_value_format="datetime")

    @pytest.mark.parametrize(
        ("in_value_format", "out_value_format", "err_attr"),
        [
            ("invalid", "datetime", "input_value_format"),
            ("datetime", "invalid", "output_value_format"),
            ("invalid", "invalid", "input_value_format"),
        ],
    )
    def test_validate_value_format_invalid(
        self,
        in_value_format: str,
        out_value_format: str,
        err_attr: str,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Test that an invalid value format raises InvalidValueFormatError."""
        err_attr_val = (
            in_value_format if err_attr == "input_value_format" else out_value_format
        )
        with pytest.raises(
            exc.InvalidFormatError,
            match=re.escape(
                f"Value format '{err_attr_val}' for field '{err_attr}' is not"
            ),
        ):
            create_column(
                input_value_format=in_value_format,
                output_value_format=out_value_format,
            )

        assert_log_record(
            caplog,
            f"Value format '{err_attr_val}' for field '{err_attr}' is not in the ALLOWED_VALUE_FORMATS constant",
            "test_table",
            err_attr,
        )

    @pytest.mark.parametrize(
        ("key", "partition", "nullable"),
        [
            (True, False, False),
            (False, False, True),
        ],
    )
    def test_partition_and_composite_keys_not_nullable_valid(
        self,
        key: bool,
        partition: bool,
        nullable: bool,
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
    def test_partition_and_composite_keys_not_nullable_invalid(
        self,
        key: bool,
        partition: bool,
        nullable: bool,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Test that partition and composite keys cannot be nullable."""
        with pytest.raises(
            exc.InvalidColumnError,
            match="Column 'id' is part of a composite key or partition and cannot be nullable",
        ):
            create_column(
                is_composite_key=key, is_partition=partition, nullable=nullable
            )

        assert_log_record(
            caplog,
            "Column 'id' is part of a composite key or partition and cannot be nullable",
            field="nullable",
        )

    @pytest.mark.parametrize(
        ("data_type", "allowed_values"),
        [
            ("str", ["a", "bee"]),
            ("int", [1, 2]),
        ],
    )
    def test_allowed_values_match_input_data_type_valid(
        self,
        data_type: type,
        allowed_values: list[Any],
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
    def test_allowed_values_match_input_data_type_skipped(
        self,
        data_type: type,
        allowed_values: list[Any],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Test that allowed values check is skipped for unsupported data types."""
        create_column(input_data_type=data_type, allowed_values=allowed_values)
        assert_log_record(
            caplog,
            "Only check allowed values for 'str' and 'int' input data types. Skipping check for 'test_table'",
            field="allowed_values",
        )

    @pytest.mark.parametrize(
        ("data_type_str", "data_type", "allowed_values"),
        [
            ("int", int, ["a", "b"]),
            ("str", str, [1, 2]),
        ],
    )
    def test_allowed_values_match_input_data_type_invalid(
        self,
        data_type_str: str,
        data_type: type,
        allowed_values: list[Any],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Test that allowed values check raises an error for mismatched input data types."""
        with pytest.raises(exc.InvalidTypeError):
            create_column(input_data_type=data_type_str, allowed_values=allowed_values)

        assert_log_record(
            caplog,
            f"Allowed value '{allowed_values[0]}' with type '{type(allowed_values[0])}' does not match the input data type '{data_type}'",
            field="allowed_values",
        )

    @pytest.mark.parametrize(
        ("data_type", "default_value"),
        [
            ("str", "a"),
            ("str", ""),
            ("int", 1),
            ("int", 0),
        ],
    )
    def test_default_value_match_input_data_type_valid(
        self, data_type: type, default_value: str | int
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
    def test_default_value_match_input_data_type_skipped(
        self, data_type: type, default_value: Any, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that default value check is skipped for unsupported data types."""
        create_column(input_data_type=data_type, default_value=default_value)
        assert_log_record(
            caplog,
            "Skipping default values check for 'test_table' as it is not int or str",
            field="default_value",
        )

    @pytest.mark.parametrize(
        ("data_type_str", "data_type", "default_value"),
        [
            ("int", int, "a"),
            ("str", str, 1),
        ],
    )
    def test_default_value_match_input_data_type_invalid(
        self,
        data_type_str: str,
        data_type: type,
        default_value: Any,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Test that default value check raises an error for mismatched input data types."""
        with pytest.raises(exc.InvalidTypeError):
            create_column(input_data_type=data_type_str, default_value=default_value)

        assert_log_record(
            caplog,
            f"Default value '{default_value}' with type '{type(default_value)}' does not match the input data type '{data_type}'",
            "test_table",
            "default_value",
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
    def test_exists_in_stage(
        self, stages: list[str], check_stage: str, expected: bool
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
                "Skipping allowed values check for 'id' as no allowed values are configured.",
            ),
        ],
    )
    def test_value_is_allowed(
        self,
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
            assert_log_record(caplog, log, table="N/A", field="allowed_values")


class TestFileFormat:
    @pytest.mark.parametrize(("stage"), [("raw"), ("curated")])
    def test_validate_stage_valid(self, stage: str) -> None:
        """Test that a valid ETL stage is correctly validated."""
        file_format = create_file_format(stage=stage)
        assert file_format.stage == stage

    def test_validate_stage_invalid(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test that an invalid ETL stage raises an error."""
        with pytest.raises(exc.InvalidStageError):
            create_file_format(stage="invalid")

        assert_log_record(
            caplog,
            "ETL stage 'invalid' is not in the ALLOWED_ETL_STAGES constant",
            field="stage",
        )

    @pytest.mark.parametrize(
        ("file_format"), [("parquet"), ("json"), ("csv"), ("xlsx")]
    )
    def test_validate_file_format_valid(self, file_format: str) -> None:
        """Test that a valid file format is correctly validated."""
        file_format_cls = create_file_format(file_format=file_format)
        assert file_format_cls.format == file_format

    def test_validate_file_format_invalid(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Test that an invalid file format raises an error."""
        with pytest.raises(exc.InvalidFormatError):
            create_file_format(file_format="invalid")

        assert_log_record(
            caplog,
            "File format 'invalid' is not in the ALLOWED_FILE_FORMATS constant",
            field="format",
        )


class TestTableMetaData:
    def test_table_metadata_valid(self) -> None:
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

    def test_table_metadata_all_fields_unique_valid(self) -> None:
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
    def test_all_fields_unique_invalid(
        self,
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

        for duplicate_field in duplicate_fields:
            assert_log_record(
                caplog,
                f"Duplicate field found: '{duplicate_field}'",
                field=duplicate_field,
            )

    def test_all_file_format_stages_unique_valid(self) -> None:
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
    def test_all_file_format_stages_unique_invalid(
        self,
        stages: list[str],
        duplicate_stage: str,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Test that having duplicate file format stages in the table metadata raises an error."""
        with pytest.raises(exc.DuplicateFileFormatStagesError):
            create_table_metadata(
                "test_table",
                [create_file_format(stage=stage) for stage in stages],
                [create_column()],
            )

        assert_log_record(
            caplog, f"Duplicate file format stage found: '{duplicate_stage}'"
        )
        assert_log_record(
            caplog,
            "One or more file format stages are defined twice for the same table",
            field="N/A",
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
    def test_column_stages_match_file_format_stages_valid(
        self,
        format_stages: list[str],
        column_stages: list[list[str]],
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
        (
            "format_stages",
            "column_stages",
            "unreconciled_formats",
            "unreconciled_columns",
        ),
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
    def test_column_stages_match_file_format_stages_invalid(
        self,
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
            assert_log_record(
                caplog,
                f"ETL stage '{stage}' is defined in the file formats, but not for any columns",
            )
            assert (
                f"The following ETL stages are defined for file formats, but not for any columns: [{', '.join(unreconciled_formats)}]"
                in str(exc_info.value)
            )
        for stage in unreconciled_columns:
            assert_log_record(
                caplog,
                f"ETL stage '{stage}' is defined for columns, but not in the file formats",
            )
            assert (
                f"The following ETL stages are defined for columns, but not in the file formats: [{', '.join(unreconciled_columns)}]"
                in str(exc_info.value)
            )

    def test_etl_stages(self) -> None:
        """Test that the etl_stages property returns all stages defined in the file formats."""
        format_stages = ["raw", "curated"]
        table_metadata = create_table_metadata(
            "test_table",
            [create_file_format(stage=stage) for stage in format_stages],
            [create_column()],
        )

        assert table_metadata.etl_stages == format_stages

    def test_contains_sensitive_data_exists(self) -> None:
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

    def test_contains_sensitive_data_not_exists(self) -> None:
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

    def test_composite_key(self) -> None:
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

        assert [column.name for column in table_metadata.composite_key] == [
            "id",
            "address",
        ]

    def test_partition_key(self) -> None:
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

        assert [column.name for column in table_metadata.partition_key] == [
            "id",
            "address",
        ]

    def test_get_file_format_for_stage_valid(self) -> None:
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

    def test_get_file_format_for_stage_invalid(
        self,
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

        assert_log_record(
            caplog,
            "No file format metadata is configured for stage 'invalid' for table 'test_table'",
        )

    def test_get_columns_for_stage_populated(self) -> None:
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

    def test_get_columns_for_stage_empty(self) -> None:
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

    def test_get_column_populated(self) -> None:
        """Test that get_column returns the correct column for a populated table."""
        table_metadata = create_table_metadata(
            "test_table",
            [create_file_format(stage="raw"), create_file_format(stage="curated")],
            [create_column(name="id"), create_column(name="name")],
        )

        column = table_metadata.get_column("name")
        assert column.name == "name"
        assert column.etl_stages == ["raw", "curated"]

    def test_get_column_empty(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test that get_column raises an error for an invalid column."""
        table_metadata = create_table_metadata(
            "test_table",
            [create_file_format(stage="raw"), create_file_format(stage="curated")],
            [create_column(name="id"), create_column(name="name")],
        )

        with pytest.raises(exc.InvalidColumnError):
            table_metadata.get_column("invalid")

        assert_log_record(
            caplog,
            "Column 'invalid' was not found in the metadata for table 'test_table'.",
            field="invalid",
        )

    def test_get_sensitive_columns_populated(self) -> None:
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

    def test_get_sensitive_columns_empty(self) -> None:
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


class TestMetaData:
    def test_metadata_valid(self) -> None:
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

    def test_metadata_get_table_metadata_valid(self) -> None:
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

    def test_metadata_get_table_metadata_invalid(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
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

        assert_log_record(
            caplog,
            "Table 'invalid' is not configured in the metadata for 'test'",
            "invalid",
        )

    def test_output_to_df(self) -> None:
        """Test the output_to_df method of the MetaData class."""
        metadata = m.MetaData(
            database="test",
            tables={
                "test_table": create_table_metadata(
                    "test_table",
                    [
                        create_file_format(stage="curated"),
                        create_file_format(stage="raw"),
                    ],
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
                    [
                        create_file_format(stage="curated"),
                        create_file_format(stage="raw"),
                    ],
                    [
                        create_column(etl_stages=["curated"], output_data_type="str"),
                        create_column(
                            name="name",
                            etl_stages=["curated"],
                            output_data_type="float",
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
                "Data Table": [
                    "test_table",
                    "test_table",
                    "test_table2",
                    "test_table2",
                ],
                "Data Field": ["id", "name", "id", "name"],
                "Description": ["", "", "", ""],
                "Data Type": [str, int, str, float],
                "Nullable": [False, False, False, False],
            }
        )

        pd.testing.assert_frame_equal(act_df, exp_df)


def test_load_metadata_success(caplog: pytest.LogCaptureFixture) -> None:
    """Test that metadata is correctly loaded into the model.

    Also serves to validate a bespoke test metadata file which covers most/all use cases
    for the actual metadata (thus testing the models handles them correctly).
    """
    metadata = m.load_metadata(Path("tests/data/meta_data"), "test_database")

    assert sorted(metadata.tables.keys()) == ["test_table", "test_table_2"]
    assert metadata.tables["test_table"].columns[0].name == "id"
    assert metadata.tables["test_table_2"].columns[0].name == "id2"

    assert_log_record(
        caplog,
        "Loading metadata for database: 'test_database'.",
        table="N/A",
        stage="Start",
    )
    assert_log_record(caplog, "Finished loading metadata.", table="N/A", stage="End")


def test_load_metadata_empty(caplog: pytest.LogCaptureFixture) -> None:
    """Test that metadata is correctly loaded into the model.

    Also serves to validate a bespoke test metadata file which covers most/all use cases
    for the actual metadata (thus testing the models handles them correctly).
    """
    with pytest.raises(FileNotFoundError):
        m.load_metadata(Path("tests/data/meta_data"), "non-existent database")

    assert_log_record(
        caplog,
        "Loading metadata for database: 'non-existent database'.",
        table="N/A",
        stage="Start",
    )
    assert_log_record(
        caplog,
        "No metadata was found for database: 'non-existent database'.",
        table="N/A",
    )


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


def test_metadata_models_forbid_extra_fields() -> None:
    """Test that undeclared metadata attributes are rejected."""
    with pytest.raises(ValidationError):
        create_column(unexpected_attribute="value")

    with pytest.raises(ValidationError):
        file_format_data: dict[str, Any] = {
            "stage": "raw",
            "format": "parquet",
            "unexpected_attribute": "value",
        }
        m.FileFormat.model_validate(file_format_data)

    with pytest.raises(ValidationError):
        create_table_metadata(
            "test_table",
            [create_file_format()],
            [create_column()],
            unexpected_attribute="value",
        )

    with pytest.raises(ValidationError):
        metadata_data: dict[str, Any] = {
            "tables": {},
            "database": "test",
            "unexpected_attribute": "value",
        }
        m.MetaData.model_validate(metadata_data)
