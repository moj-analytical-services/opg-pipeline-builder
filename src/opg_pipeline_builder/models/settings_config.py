"""Loads environment variables into a pydantic model."""

from logging import getLogger

from pydantic import ValidationInfo, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from opg_pipeline_builder.constants import ALLOWED_ENVS
from opg_pipeline_builder.logging.log import ModuleLogger
from opg_pipeline_builder.models.utils import field_name

log = ModuleLogger(logger=getLogger(__name__))


class SettingsConfig(BaseSettings):
    """Extract setting from the DAG via environment variables."""

    ENV: str

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @field_validator("ENV")
    @classmethod
    def validate_environment(cls, value: str, info: ValidationInfo) -> str:
        """Validate that the environment is one of the allowed environments."""
        if value not in ALLOWED_ENVS:
            err = f"ENV must be one of {', '.join(ALLOWED_ENVS)}"
            log.error(err, table="settings_config", field=field_name(info))
            raise ValueError(err)
        return value
