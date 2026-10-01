from typing import Any

from opg_pipeline_builder.models import metadata_model as m
from opg_pipeline_builder.models.pipeline_config import PipelineConfig


##################################
# CREATE METADATA CONFIG OBJECTS #
##################################
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
    if etl_stages is None:
        etl_stages = ["raw", "curated"]

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


##########################
# CREATE PIPELINE CONFIG #
##########################


def create_pipeline_config(
    name: str = "name",
    description: str = "description",
    land_path: str = "s3://bucket-name/{{ env }}/{{ db }}/land/table-name",
    archive_path: str = "s3://bucket-name/{{ env }}/{{ db }}/archive/table-name",
    curated_path: str = "s3://bucket-name/{{ env }}/{{ db }}/curated/table-name",
    github_repo: str = "https://github.com/user/repo",
    data_cadence: str = "daily",
) -> PipelineConfig:
    """Create a PipelineConfig instance with default or provided values."""
    return PipelineConfig(
        name=name,
        description=description,
        land_path=land_path,
        archive_path=archive_path,
        curated_path=curated_path,
        github_repo=github_repo,
        data_cadence=data_cadence,
    )
