from logging import getLogger

from pydantic import BaseModel, ConfigDict

from opg_pipeline_builder.logging import ModuleLogger
from opg_pipeline_builder.models.pipeline_config import PipelineConfig
from opg_pipeline_builder.models.settings_config import SettingsConfig
from opg_pipeline_builder.models.utils import render_s3_path

log = ModuleLogger(logger=getLogger(__name__))


class RunConfig(BaseModel):
    env: str
    name: str
    land_path: str
    archive_path: str
    curated_path: str

    model_config = ConfigDict(extra="forbid")


def create_run_config(pipeline: PipelineConfig, settings: SettingsConfig) -> RunConfig:
    """Create a run config from the pipeline and settings config"""
    log.info("Creating run config", table="run_config", stage="Start")

    land_path = render_s3_path(pipeline.land_path, settings.ENV, pipeline.name)
    archive_path = render_s3_path(pipeline.archive_path, settings.ENV, pipeline.name)
    curated_path = render_s3_path(pipeline.curated_path, settings.ENV, pipeline.name)

    config = RunConfig(
        env=settings.ENV,
        name=pipeline.name,
        land_path=land_path,
        archive_path=archive_path,
        curated_path=curated_path,
    )

    log.info("Run config created", table="run_config", stage="End")
    return config
