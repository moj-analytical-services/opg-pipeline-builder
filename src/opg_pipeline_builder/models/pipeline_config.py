from logging import getLogger

from pydantic import (
    BaseModel,
    ConfigDict,
    ValidationInfo,
    field_validator,
)

from opg_pipeline_builder.logging import ModuleLogger
from opg_pipeline_builder.models import modelling_exceptions as exc
from opg_pipeline_builder.models.utils import field_name
from opg_pipeline_builder.validation.validators import (
    is_valid_identifier,
    is_valid_s3_path_template,
)

log = ModuleLogger(logger=getLogger(__name__))


class PipelineConfig(BaseModel):
    """Pydantic model representing the pipeline configuration."""

    model_config = ConfigDict(extra="forbid")

    name: str
    description: str = ""
    land_path: str
    archive_path: str
    curated_path: str

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str, info: ValidationInfo) -> str:
        """Validate the pipeline name is a valid SQL/Athena identifier."""
        err = is_valid_identifier(value)
        if err:
            log.error(err, table="pipeline_config", field=field_name(info))
            raise exc.InvalidPipelineNameError(err)

        return value

    @field_validator("land_path", "archive_path", "curated_path")
    @classmethod
    def validate_s3_paths(cls, value: str, info: ValidationInfo) -> str:
        """Validate the land, archive, and curated paths."""
        err = is_valid_s3_path_template(value, field_name(info))
        if err:
            log.error(err, table="pipeline_config", field=field_name(info))
            raise exc.InvalidPathError(err)

        return value
