from pydantic import BaseModel

from opg_pipeline_builder.models.pipeline_configs.core_config.table_config import (
    TableConfig,
)


class ETLStepNotConfiguredError(Exception):
    def __init__(self, err: str):
        super().__init__(err)
        self.err = err


class ETLStepConfig(BaseModel):
    step_name: str
    engine_name: str
    transform_name: str = "run"
    transform_kwargs: dict[str, str] = {}


class PathTemplates(BaseModel):
    land: str
    curated: str
    source: str = ""
    raw: str = ""
    raw_hist: str = ""
    processed: str = ""
    derived: str = ""


class PipelineConfig(BaseModel):
    """Core Pydantic model for a pipeline config.

    Specific databases have bespoke implementations on top of this core model
    """

    db_name: str
    description: str
    etl_step_config: list[ETLStepConfig]
    path_templates: PathTemplates
    tables: dict[str, TableConfig]

    def extract_etl_step_config_for_step(self, etl_step: str) -> ETLStepConfig:
        "Return a single ETLStepConfig for a specified ETL step."
        for step_config in self.etl_step_config:
            if etl_step == step_config.step_name:
                return step_config

        err = f"ETL step '{etl_step} is not configured for database '{self.db_name}'."
        raise ETLStepNotConfiguredError(err)


# def read_pipeline_config(db_name: str) -> PipelineConfig:
#     ""
#     try:
#         with open(os.path.join("configs", f"{db_name}.yml")) as config_file:
#             raw_pipeline_config = yaml.safe_load(config_file)

#     except FileNotFoundError:
#         raise ValueError(f"{db_name} config does not exist.")

#     pipeline_config = PipelineConfig(**raw_pipeline_config)

#     return pipeline_config
