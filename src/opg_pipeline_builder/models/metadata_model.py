import json
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel, field_validator, model_validator

from opg_pipeline_builder.constants import (
    ALLOWED_DATA_TYPES,
    ALLOWED_ETL_STAGES,
    ALLOWED_FILE_FORMATS,
    ALLOWED_STRUCT_DATA_TYPES,
    ETLStage,
)
from opg_pipeline_builder.models import modelling_exceptions as exc


class Column(BaseModel):
    """Pydantic model representing a column that exists in the metadata for a specific table."""

    name: str
    description: str = ""
    semantic_type: str
    etl_stages: list[ETLStage] = list(ALLOWED_ETL_STAGES)
    sensitive: bool = True
    is_composite_key: bool = False
    is_partition: bool = False
    input_data_type: str
    output_data_type: str
    input_value_format: str = ""
    output_value_format: str = ""
    regex_pattern: str = ""
    nullable: bool = False
    allowed_values: list[str | int] = []
    default_value: str | int | bool | None = None

    @field_validator("input_data_type", "output_data_type")
    @classmethod
    def validate_data_type(cls, value: str) -> str:
        """Check the provided data type is valid."""
        if value in ALLOWED_DATA_TYPES or value.startswith(ALLOWED_STRUCT_DATA_TYPES):
            return value

        err = f"Data type '{value}' is not in the ALLOWED_DATA_TYPES constant"
        raise exc.InvalidTypeError(err)

    def exists_for_stage(self, stage_name: str) -> bool:
        """Return True if the column is configured for a specified stage.

        Args:
            stage_name (str): Name of stage to check if column is configured for.

        Returns:
            bool: Whether the column is configured for the stage
        """
        return stage_name in self.etl_stages


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
        raise exc.InvalidStageError(err)

    @field_validator("format")
    @classmethod
    def validate_file_format(cls, value: str) -> str:
        """Check the provided file format is valid."""
        if value in ALLOWED_FILE_FORMATS:
            return value

        err = f"File format '{value}' is not in the ALLOWED_FILE_FORMATS constant"
        raise exc.InvalidFormatError(err)


class TableMetaData(BaseModel):
    """Pydantic model representing a metadata entry for a specific table."""

    name: str
    description: str
    file_formats: list[FileFormat]
    primary_key: list[str] = []
    partitions: list[str]
    columns: list[Column]

    @model_validator(mode="after")
    def validate_all_fields_unique(self) -> "TableMetaData":
        """Check if any columns have the same name."""

        all_columns = [column.name for column in self.columns]
        if len(set(all_columns)) != len(all_columns):
            err = "One or more columns are defined twice for the same table"
            raise exc.DuplicateColumnsError(err)
        return self

    @model_validator(mode="after")
    def validate_partition_columns_defined(self) -> "TableMetaData":
        """Check that the partition columns are defined as columns."""
        all_columns = [column.name for column in self.columns]

        for partition in self.partitions:
            if partition not in all_columns:
                err = f"Partition column '{partition}' is not a defined column in the metadata for '{self.name}'."
                raise exc.InvalidColumnError(err)

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
        raise exc.InvalidStageError(err)

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
        raise exc.InvalidColumnError(err)

    def create_old_style_metadata(self, stage: str) -> dict[Any, Any]:
        output_metadata: dict[Any, Any] = {}

        output_metadata["$schema"] = self.schema_link
        output_metadata["_converted_from"] = self.converted_from

        output_metadata["columns"] = []

        for column in self.get_columns_for_stage(stage):
            column_data: dict[str, str | bool | list[str | int]] = {}
            if column.enum:
                column_data["enum"] = column.enum
            column_data["name"] = column.name
            if column.nullable:
                column_data["nullable"] = column.nullable
            if column.get_stage_for_column(stage).pattern:
                column_data["pattern"] = column.get_stage_for_column(stage).pattern
            column_data["type"] = column.get_stage_for_column(stage).type

            output_metadata["columns"].append(column_data)

        output_metadata["description"] = self.description
        output_metadata["file_format"] = self.get_file_format_for_stage(stage).format
        output_metadata["name"] = self.name
        output_metadata["partitions"] = self.partitions if stage == "curated" else []
        output_metadata["primary_key"] = self.primary_key
        output_metadata["sensitive"] = self.sensitive

        return output_metadata


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
