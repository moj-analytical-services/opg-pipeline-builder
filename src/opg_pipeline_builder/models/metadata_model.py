import json
from collections import Counter
from logging import getLogger
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel, ValidationInfo, field_validator, model_validator

from opg_pipeline_builder.constants import (
    ALLOWED_DATA_TYPES,
    ALLOWED_ETL_STAGES,
    ALLOWED_FILE_FORMATS,
    ALLOWED_SEMANTIC_TYPES,
    ALLOWED_STRUCT_DATA_TYPES,
    ALLOWED_VALUE_FORMATS,
)
from opg_pipeline_builder.logging.log import CustomFields
from opg_pipeline_builder.models import modelling_exceptions as exc
from opg_pipeline_builder.validation.validators import is_valid_identifier

logger = getLogger(__name__)
log_fields: CustomFields = CustomFields.set_custom_fields(
    "Validation", "Processing", "None", "None"
)


def _get_table_name(info: ValidationInfo) -> str:
    """Extract the table name from the validation context."""
    table_name: str = (info.context or {}).get("table_name", "")
    return table_name


class Column(BaseModel):
    """Pydantic model representing a column that exists in the metadata for a specific table."""

    name: str
    description: str = ""
    semantic_type: str
    etl_stages: list[str]
    sensitive: bool = True
    is_composite_key: bool = False
    is_partition: bool = False
    input_data_type: Any
    output_data_type: Any
    input_value_format: str = ""
    output_value_format: str = ""
    regex_pattern: str = ""
    nullable: bool = False
    allowed_values: list[Any] | None = None
    default_value: Any | None = None

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str, info: ValidationInfo) -> str:
        """Validate that the column name is a valid SQL/Athena identifier."""
        err = is_valid_identifier(value)
        if err:
            log_fields.update(table=_get_table_name(info), field=info.field_name)
            logger.error(err, extra={"custom_fields": log_fields.model_dump()})
            raise exc.InvalidColumnNameError(err)

        return value

    @field_validator("semantic_type")
    @classmethod
    def _validate_semantic_type(cls, value: str, info: ValidationInfo) -> str:
        """Check the provided semantic type is valid."""
        if value in ALLOWED_SEMANTIC_TYPES:
            return value

        err = f"Semantic type '{value}' is not in the ALLOWED_SEMANTIC_TYPES constant"
        log_fields.update(table=_get_table_name(info), field=info.field_name)
        logger.error(err, extra={"custom_fields": log_fields.model_dump()})
        raise exc.InvalidSemanticTypeError(err)

    @field_validator("etl_stages")
    @classmethod
    def _validate_etl_stage(cls, value: list[str], info: ValidationInfo) -> list[str]:
        """Check the provided ETL stage is valid and not empty."""
        if not value:
            err = "ETL stages list cannot be empty"
            log_fields.update(table=_get_table_name(info), field=info.field_name)
            logger.error(err, extra={"custom_fields": log_fields.model_dump()})
            raise exc.InvalidStageError(err)

        for stage in value:
            if stage not in ALLOWED_ETL_STAGES:
                err = f"ETL stage '{stage}' is not in the ALLOWED_ETL_STAGES constant"
                log_fields.update(table=_get_table_name(info), field=info.field_name)
                logger.error(err, extra={"custom_fields": log_fields.model_dump()})
                raise exc.InvalidStageError(err)
        return value

    @field_validator("input_data_type", "output_data_type")
    @classmethod
    def _validate_data_type(cls, value: type, info: ValidationInfo) -> Any:
        """Check the provided data type is valid."""
        if value in ALLOWED_DATA_TYPES or str(value).startswith(
            ALLOWED_STRUCT_DATA_TYPES
        ):
            return value

        err = f"Data type '{value}' for field '{info.field_name}' is not in the ALLOWED_DATA_TYPES constant"
        log_fields.update(table=_get_table_name(info), field=info.field_name)
        logger.error(err, extra={"custom_fields": log_fields.model_dump()})

        raise exc.InvalidTypeError(err)

    @field_validator("input_value_format", "output_value_format")
    @classmethod
    def _validate_value_format(cls, value: str, info: ValidationInfo) -> str:
        """Check the provided value format is valid."""
        if value == "" or value in ALLOWED_VALUE_FORMATS:
            return value

        err = f"Value format '{value}' for field '{info.field_name}' is not in the ALLOWED_VALUE_FORMATS constant"
        log_fields.update(table=_get_table_name(info), field=info.field_name)
        logger.error(err, extra={"custom_fields": log_fields.model_dump()})
        raise exc.InvalidFormatError(err)

    @model_validator(mode="after")
    def _partition_and_composite_keys_not_nullable(
        self, info: ValidationInfo
    ) -> "Column":
        """Ensure that partition and composite key columns are not nullable."""

        if (self.is_composite_key or self.is_partition) and self.nullable:
            err = f"Column '{self.name}' is part of a composite key or partition and cannot be nullable"
            log_fields.update(table=_get_table_name(info), field="nullable")
            logger.error(err, extra={"custom_fields": log_fields.model_dump()})
            raise exc.InvalidColumnError(err)
        return self

    @model_validator(mode="after")
    def _allowed_values_match_input_data_type(self, info: ValidationInfo) -> "Column":
        """Ensure that allowed values match the input data type."""
        log_fields.update(table=_get_table_name(info), field="allowed_values")

        if self.allowed_values:
            if self.input_data_type in (str, int):
                for value in self.allowed_values:
                    if not isinstance(value, self.input_data_type):
                        err = f"Allowed value '{value}' with type '{type(value)}' does not match the input data type '{self.input_data_type}'"
                        logger.error(
                            err, extra={"custom_fields": log_fields.model_dump()}
                        )
                        raise exc.InvalidTypeError(err)
            else:
                logger.info(
                    "Only check allowed values for 'str' and 'int' input data types. Skipping check for '%s'",
                    _get_table_name(info),
                )
        return self

    @model_validator(mode="after")
    def _default_values_match_input_data_type(self, info: ValidationInfo) -> "Column":
        """Ensure that default values match the input data type."""
        log_fields.update(table=_get_table_name(info), field="default_value")

        if self.default_value:
            if self.input_data_type in (str, int):
                if not isinstance(self.default_value, self.input_data_type):
                    err = f"Default value '{self.default_value}' with type '{type(self.default_value)}' does not match the input data type '{self.input_data_type}'"
                    logger.error(err, extra={"custom_fields": log_fields.model_dump()})
                    raise exc.InvalidTypeError(err)
            else:
                logger.info(
                    "Only check default value for 'str' and 'int' input data types. Skipping check for '%s'",
                    _get_table_name(info),
                )
        return self

    def exists_in_stage(self, stage_name: str) -> bool:
        """Return True if the column is configured for a specified stage.

        Args:
            stage_name (str): Name of stage to check if column is configured for.

        Returns:
            bool: Whether the column is configured for the stage
        """
        return stage_name in self.etl_stages

    def value_is_allowed(self, value: Any) -> bool:
        """Return True if the provided value is allowed for the column.

        Args:
            value (Any): The value to check against the allowed values.

        Returns:
            bool: Whether the value is allowed for the column.
        """
        if self.allowed_values:
            return value in self.allowed_values

        log_fields.update(field="allowed_values")
        logger.info(
            "There are no allowed values for column '%s'; skipping allowed values check.",
            self.name,
            extra={"custom_fields": log_fields.model_dump()},
        )
        return False


class FileFormat(BaseModel):
    """Pydantic model representing the file format data is stored in for each ETL stage."""

    stage: str
    format: str

    @field_validator("stage")
    @classmethod
    def validate_stage(cls, value: str, info: ValidationInfo) -> str:
        """Check the provided ETL stage name is valid."""
        if value in ALLOWED_ETL_STAGES:
            return value

        err = f"ETL stage '{value}' is not in the ALLOWED_ETL_STAGES constant"
        log_fields.update(table=_get_table_name(info), field="stage")
        logger.error(err, extra={"custom_fields": log_fields.model_dump()})
        raise exc.InvalidStageError(err)

    @field_validator("format")
    @classmethod
    def validate_file_format(cls, value: str, info: ValidationInfo) -> str:
        """Check the provided file format is valid."""
        if value in ALLOWED_FILE_FORMATS:
            return value

        err = f"File format '{value}' is not in the ALLOWED_FILE_FORMATS constant"
        log_fields.update(table=_get_table_name(info), field="format")
        logger.error(err, extra={"custom_fields": log_fields.model_dump()})
        raise exc.InvalidFormatError(err)


class TableMetaData(BaseModel):
    """Pydantic model representing a metadata entry for a specific table."""

    name: str
    description: str
    file_formats: list[FileFormat]
    columns: list[Column]

    @model_validator(mode="after")
    def validate_all_fields_unique(self) -> "TableMetaData":
        """Check if any columns have the same name."""
        column_counts = Counter(column.name for column in self.columns)
        duplicate = False

        for column, count in column_counts.items():
            if count > 1:
                duplicate = True
                log_fields.update(table=self.name, field=column)
                logger.error(
                    "Duplicate field found: '%s'",
                    column,
                    extra={"custom_fields": log_fields.model_dump()},
                )
        if duplicate:
            err = "One or more columns are defined twice for the same table"
            raise exc.DuplicateFieldsError(err)
        return self

    @model_validator(mode="after")
    def validate_all_file_format_stages_unique(self) -> "TableMetaData":
        """Check if any file format stages are defined more than once."""
        stage_counts = Counter(format_obj.stage for format_obj in self.file_formats)
        duplicate = False

        for stage, count in stage_counts.items():
            if count > 1:
                duplicate = True
                log_fields.update(table=self.name, field=stage)
                logger.error(
                    "Duplicate file format stage found: %s",
                    stage,
                    extra={"custom_fields": log_fields.model_dump()},
                )
        if duplicate:
            err = "One or more file format stages are defined twice for the same table"
            raise exc.DuplicateFileFormatStagesError(err)
        return self

    @model_validator(mode="after")
    def validate_column_stages_match_file_format_stages(self) -> "TableMetaData":
        """Check that all stages defined for columns have a corresponding file format."""
        unreconciled_stages = [format_obj.stage for format_obj in self.file_formats]
        reconciled_stages = []
        file_format_stages = [format_obj.stage for format_obj in self.file_formats]

        for column in self.columns:
            for stage in column.etl_stages:
                if stage in unreconciled_stages:
                    unreconciled_stages.remove(stage)
                    reconciled_stages.append(stage)
                else:
                    if stage not in reconciled_stages:
                        unreconciled_stages.append(stage)

            if not unreconciled_stages:
                break

        if unreconciled_stages:
            log_fields.update(table=self.name, field="None")
            for stage in unreconciled_stages:
                if stage in file_format_stages:
                    logger.error(
                        "ETL stage '%s' is defined in the file formats but not for any columns",
                        stage,
                        extra={"custom_fields": log_fields.model_dump()},
                    )
                else:
                    logger.error(
                        "ETL stage '%s' is defined for columns but not in the file formats",
                        stage,
                        extra={"custom_fields": log_fields.model_dump()},
                    )
            err = f"ETL stages: '{', '.join(unreconciled_stages)}' are defined for columns, or file formats, but not both."
            raise exc.InvalidStageError(err)

        return self

    @property
    def etl_stages(self) -> list[str]:
        """Return a list of all ETL stages defined for this table."""
        return [format_obj.stage for format_obj in self.file_formats]

    @property
    def contains_sensitive_data(self) -> bool:
        """Return True if any column in this table contains sensitive data."""
        return any(column.sensitive for column in self.columns)

    @property
    def composite_key(self) -> list[Column]:
        """Return a list of columns that make up the composite key for this table."""
        return [column for column in self.columns if column.is_composite_key]

    @property
    def partition_key(self) -> list[Column]:
        """Return a list of columns that make up the partition key for this table."""
        return [column for column in self.columns if column.is_partition]

    def get_file_format_for_stage(self, stage_name: str) -> FileFormat:
        """Return the file format for a specific ETL stage.

        Args:
            stage_name (str): The ETL stage that is running (as defined from airflow)

        Returns:
            FileFormat: The file format of this table's data for this ETL stage

        Raises:
            InvalidStageError: The given stage is not configured for the file format attribute

        """
        for format_obj in self.file_formats:
            if format_obj.stage == stage_name:
                return format_obj

        err = f"No file format metadata is configured for stage '{stage_name}' for table '{self.name}'"
        log_fields.update(table=self.name, field="None")
        logger.error(err, extra={"custom_fields": log_fields.model_dump()})
        raise exc.InvalidStageError(err)

    def get_columns_for_stage(self, stage_name: str) -> list[Column]:
        """Return all of the column definitions for a specific ETL stage.

        Args:
            stage_name (str): The ETL stage that is running (as defined from airflow)

        Returns:
            list[Column]: A list of column objects containing all columns that exist for this ETL stage

        """
        return [column for column in self.columns if column.exists_in_stage(stage_name)]

    def get_column(self, name: str) -> Column:
        """Get the definition of a specific column from the metadata

        Args:
            name (str): The name of the column to be returned

        Returns:
            Column: The column object
        """
        for column in self.columns:
            if column.name == name:
                return column

        log_fields.update(table=self.name, field=name)
        err = f"Column '{name}' was not found in the metadata for table '{self.name}'."
        logger.error(err, extra={"log_fields": log_fields})
        raise exc.InvalidColumnError(err)

    def get_sensitive_columns(self) -> list[Column]:
        """Return a list of columns that are marked as sensitive for this table."""
        return [column for column in self.columns if column.sensitive]


class MetaData(BaseModel):
    database: str
    tables: dict[str, TableMetaData]

    def get_table_metadata(self, table_name: str) -> TableMetaData:
        """Retrieve the TableMetaData object for a specific table.

        Args:
            table_name (str): Table to retrieve

        Returns:
            TableMetaData: The TableMetaData object

        """
        table = self.tables.get(table_name, None)

        if not table:
            err = f"Table '{table_name}' is not configured in the metadata for '{self.database}'"
            raise exc.InvalidTableError(err)

        return table

    def output_to_df(self) -> pd.DataFrame:
        """Combine the metadata for each table into a single dataframe

        Returns:
            pd.DataFrame: Combined metadata as adataframe
        """
        output_dfs: list[pd.DataFrame] = []

        for table in self.tables.values():
            data: list[dict[str, Any]] = []
            for column in table.columns:
                if column.has_stage("curated"):
                    data.append(
                        {
                            "System": self.database,
                            "Dataset": self.database,
                            "Data Table": table.name,
                            "Data Field": column.name,
                            "Description": "",
                            "Data Type": column.get_stage_for_column("curated").type,
                            "Nullable": column.nullable,
                        }
                    )
            output_dfs.append(pd.DataFrame(data=data))

        return pd.concat(output_dfs)


def load_metadata(metadata_path: Path, database_name: str) -> MetaData:
    """Load a metadata file and convert it into the Pydantic model.

    Args:
        database_name (str): The name of the database to load metadata for
        metadata_path (Path): The base path where metadata files are stored

    Returns:
        MetaData: The Metadata object
    """

    db_metadata_files = list((metadata_path / database_name).glob("*.json"))

    database_metadata: dict[str, Any] = {}

    for file in db_metadata_files:
        with (file).open(encoding="utf-8", mode="r") as json_file:
            metadata_file = json.load(json_file)
            database_metadata[file.stem] = TableMetaData(**metadata_file)
    return MetaData(database=database_name, tables=database_metadata)


def output_metadata_as_csv(
    metadata_path: Path, metadata_files: list[str], output_path: Path
) -> None:
    """Output metadata for all pipelines as a single CSV file.

    Args:
        metadata_path (Path): The folder path containing all the database specific metadata folders
        metadata_files (list[str]): Folder names within the metadata path to be output
        output_path (Path): The folder to write them metadata to

    Returns:
        None
    """

    metadata_dfs: list[pd.DataFrame] = []

    for database_name in metadata_files:
        metadata = load_metadata(metadata_path, database_name)
        metadata_dfs.append(metadata.output_to_df())

    metadata_df = pd.concat(metadata_dfs)

    metadata_df.to_csv(output_path / "metadata.csv", index=False)
