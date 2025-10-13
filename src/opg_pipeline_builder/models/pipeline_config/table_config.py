from pydantic import BaseModel

class FileFormats(BaseModel):


class TableConfig(BaseModel):
    etl_stages: dict[str, dict[str, str]]
    transform_type: str
    frequency: str
    sql: dict[str, list[str] | bool] | None = None
    lint_options: dict[str, Any] | None = None
    input_data: dict[str, dict[str, str]] | None = None
    optional_arguments: dict[str, Any] | None = None

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

    @field_validator("etl_stages")
    @classmethod
    def check_valid_etl_stages(
        cls,
        v: dict[str, dict[str, str]],
    ) -> dict[str, dict[str, str]]:
        inv_stages = [stage for stage in v if stage not in etl_stages]
        if inv_stages:
            raise ValueError(f"ETL stages must be one of {', '.join(etl_stages)}")
        return v

    @field_validator("transform_type")
    @classmethod
    def check_transform_type(cls, v: str) -> str:
        if v not in transform_types:
            raise ValueError(
                f"Transform type should be one of {', '.join(transform_types)}"
            )
        return v
