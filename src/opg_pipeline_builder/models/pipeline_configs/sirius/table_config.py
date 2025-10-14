from typing import Any, Literal

from croniter import croniter
from pydantic import BaseModel, field_validator, model_validator

from opg_pipeline_builder.utils.constants import transform_types


class FileFormat(BaseModel):
    file_format: Literal["parquet"]


class ETLStages(BaseModel):
    land: FileFormat
    raw: FileFormat
    curated: FileFormat


class LintOptions(BaseModel):
    required: bool
    expect_header: bool
    headers_ignore_case: bool
    allow_missing_cols: bool
    allow_unexpected_cols: bool
    row_limit: int | None = None
    pandas_kwargs: dict[str, bool] = {}
    columns_to_cast: list[str] = []
    columns_original_dtypes: list[str] = []
    columns_cast_types: list[str] = []


class TableConfig(BaseModel):
    etl_stages: ETLStages
    transform_type: str
    lint_options: LintOptions | None = None
    frequency: str
    optional_arguments: dict[str, Any] = {}
    input_data: dict[str, dict[str, str]] | None = None
    sql: dict[str, list[str] | bool] | None = None

    @model_validator(mode="after")
    def check_transform_type_consistency(self) -> "TableConfig":
        if self.transform_type == "derived":
            if self.lint_options:
                raise ValueError("Derived table should not have self.lint_options")
            if not self.input_data:
                raise ValueError("Derived table should have self.input_data")

        else:
            if not self.lint_options:
                raise ValueError(
                    f"{self.transform_type} table should have lint_options"
                )
            if self.input_data:
                raise ValueError(
                    f"{self.transform_type} table should not have input_data"
                )

            if self.transform_type == "default":
                if self.sql:
                    raise ValueError("default table should not have sql")

        return self

    @field_validator("frequency")
    @classmethod
    def check_valid_cron(cls, v: str) -> str:
        if not croniter.is_valid(v):
            raise ValueError(f"{v} isn't a valid cron expression")
        return v

    @field_validator("transform_type")
    @classmethod
    def check_transform_type(cls, v: str) -> str:
        if v not in transform_types:
            raise ValueError(
                f"Transform type should be one of {', '.join(transform_types)}"
            )
        return v
