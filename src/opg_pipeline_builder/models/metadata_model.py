import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel, field_validator, model_validator

from opg_pipeline_builder.constants import (
    ALLOWED_DATA_TYPES,
    ALLOWED_ETL_STAGES,
    ALLOWED_FILE_FORMATS,
)


class InvalidStageError(Exception):
    def __init__(self, error: str):
        super().__init__(error)
        self.error = error


class InvalidColumnError(Exception):
    def __init__(self, error: str):
        super().__init__(error)
        self.error = error


class InvalidTableError(Exception):
    def __init__(self, error: str):
        super().__init__(error)
        self.error = error


class InvalidTypeError(Exception):
    def __init__(self, error: str):
        super().__init__(error)
        self.error = error


class InvalidFormatError(Exception):
    def __init__(self, error: str):
        super().__init__(error)
        self.error = error


class DuplicateColumnsError(Exception):
    def __init__(self, error: str):
        super().__init__(error)
        self.error = error


class Stage(BaseModel):
    """Pydantic model representing the metadata for a given ETL stage for a specific column."""

    name: str
    type: str
    pattern: str = ""

    @field_validator("name")
    @classmethod
    def validate_stage_name(cls, value: str) -> str:
        """Check the provided ETL stage name is valid."""
        if value in ALLOWED_ETL_STAGES:
            return value

        err = f"ETL stage '{value}' is not in the ALLOWED_ETL_STAGES constant"
        raise InvalidStageError(err)

    @field_validator("type")
    @classmethod
    def validate_data_type(cls, value: str) -> str:
        """Check the provided data type is valid."""
        if value in ALLOWED_DATA_TYPES:
            return value

        err = f"Data type '{value}' is not in the ALLOWED_DATA_TYPES constant"
        raise InvalidTypeError(err)


class Column(BaseModel):
    """Pydantic model representing a column that exists in the metadata for a specific table."""

    name: str
    nullable: bool
    stages: list[Stage]

    def get_stage_for_column(self, stage_name: str) -> Stage:
        """Return the Stage object for a column for a given ETL stage.

        Args:
            stage_name (str): The ETL stage that is running (as defined from airflow)

        Returns:
            Stage: The Stage object for the required ETL stage

        Raises:
            InvalidStageError: The column is not configured for the ETL stage searched for
        """
        for stage in self.stages:
            if stage.name == stage_name:
                return stage

        err = f"No metadata is configured for stage '{stage_name}' for column '{self.name}'"
        raise InvalidStageError(err)

    def has_stage(self, stage_name: str) -> bool:
        """Return True if the column is configured for a specified stage.

        Args:
            stage_name (str): Name of stage to check if column is configured for.

        Returns:
            bool: Whether the column is configured for the stage
        """
        return any(stage.name == stage_name for stage in self.stages)


class FileFormat(BaseModel):
    """Pydantic model representing the file format data is stored in for each ETL stage."""

    name: str
    format: str

    @field_validator("name")
    @classmethod
    def validate_stage_name(cls, value: str) -> str:
        """Check the provided ETL stage name is valid."""
        if value in ALLOWED_ETL_STAGES:
            return value

        err = f"ETL stage '{value}' is not in the ALLOWED_ETL_STAGES constant"
        raise InvalidStageError(err)

    @field_validator("format")
    @classmethod
    def validate_file_format(cls, value: str) -> str:
        """Check the provided file format is valid."""
        if value in ALLOWED_FILE_FORMATS:
            return value

        err = f"File format '{value}' is not in the ALLOWED_FILE_FORMATS constant"
        raise InvalidFormatError(err)


class TableMetaData(BaseModel):
    """Pydantic mdoel representing a metadata entry for a specific table."""

    converted_from: str
    schema_link: str
    name: str
    description: str
    file_formats: list[FileFormat]
    sensitive: bool
    primary_key: list[str] = []
    partitions: list[str]
    columns: list[Column]

    @model_validator(mode="after")
    def validate_all_fields_unique(self) -> "TableMetaData":
        """Check if any columns have the same name."""

        all_columns = [column.name for column in self.columns]
        if len(set(all_columns)) != len(all_columns):
            err = "One or more columns are defined twice for the same table"
            raise DuplicateColumnsError(err)
        return self

    @model_validator(mode="after")
    def validate_partition_columns_defined(self) -> "TableMetaData":
        """Check that the parition columns are defined as columns."""
        all_columns = [column.name for column in self.columns]

        for partition in self.partitions:
            if partition not in all_columns:
                err = f"Partition column '{partition}' is not a defined column in the metadata for '{self.name}'."
                raise InvalidColumnError(err)

        return self

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
            if format_obj.name == stage_name:
                return format_obj

        err = f"No file format metadata is configured for stage '{stage_name}' for table '{self.name}'"
        raise InvalidStageError(err)

    def get_columns_for_stage(self, stage_name: str) -> list[Column]:
        """Return all of the column definitions for a specific ETL stage.

        Args:
            stage (str): The ETL stage that is running (as defined from airflow)

        Returns:
            list[Column]: A list of column objects containing all columns that exist for this ETL stage

        """
        return [column for column in self.columns if column.has_stage(stage_name)]

    def get_column(self, column_name: str) -> Column:
        """Get the definition of a specific column from the metadata

        Args:
            column_name (str): The name of the column to be returned

        Returns:
            Column: The column object
        """
        for column in self.columns:
            if column.name == column_name:
                return column

        err = f"Column '{column_name}' was not found in the metadata for table '{self.name}'."
        raise InvalidColumnError(err)


class MetaData(BaseModel):
    database: str
    tables: dict[str, TableMetaData]

    def get_table_metadata(self, table_name: str) -> TableMetaData:
        table = self.tables.get(table_name, None)

        if not table:
            err = f"Table '{table_name}' is not configured in the metadata for '{self.database}'"
            raise InvalidTableError(err)

        return table


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
