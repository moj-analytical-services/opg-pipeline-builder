from logging import getLogger

from pydantic import (
    BaseModel,
    ConfigDict,
    ValidationInfo,
    field_validator,
    model_validator,
)

from opg_pipeline_builder.logging import ModuleLogger
from opg_pipeline_builder.models import modelling_exceptions as exc
from opg_pipeline_builder.models.utils import field_name, table_name
from opg_pipeline_builder.validation.validators import (
    is_valid_identifier,
    is_valid_s3_path,
)

log = ModuleLogger(logger=getLogger(__name__))


class PipelineConfig(BaseModel):
    """Pydantic model representing the pipeline configuration."""

    model_config = ConfigDict(extra="forbid")

    db_name: str
    description: str = ""
    source_path: str | None = None
    raw_path: str
    curated_path: str

    @field_validator("db_name")
    def validate_db_name(self, value: str, info: ValidationInfo) -> str:
        """Validate the database name is a valid SQL/Athena identifier."""

        err = is_valid_identifier(value)
        if err:
            log.error(err, table=table_name(info), field=field_name(info))
            raise exc.InvalidDatabaseNameError(err)

        return value

    @model_validator(mode="after")
    def validate_and_set_raw_path(self, info: ValidationInfo) -> "PipelineConfig":
        """Validate and set the raw and curated paths."""
        err = is_valid_s3_path(self.raw_path, self.db_name)
        if err:
            log.error(err, table=table_name(info), field=field_name(info))
            raise exc.InvalidPathError(err)

        return self

    @model_validator(mode="after")
    def validate_and_set_curated_path(self, info: ValidationInfo) -> "PipelineConfig":
        """Validate and set the curated path."""
        err = is_valid_s3_path(self.curated_path, self.db_name)
        if err:
            log.error(err, table=table_name(info), field=field_name(info))
            raise exc.InvalidPathError(err)

        return self
