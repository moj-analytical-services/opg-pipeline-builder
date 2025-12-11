import os

from jinja2 import Template
from pydantic import BaseModel

from opg_pipeline_builder.models.pipeline_configs.core_config import (
    pipeline_config as core,
)
from opg_pipeline_builder.models.pipeline_configs.core_config import (
    table_config as table,
)
from opg_pipeline_builder.models.pipeline_configs.sirius import (
    sirius_config as sirius_config,
)


class Paths(BaseModel):
    source: str
    land: str
    raw: str
    curated: str
    derived: str


class SiriusRunConfig(BaseModel):
    db_name: str
    etl_step_config: core.ETLStepConfig
    paths: Paths
    db_lint_options: sirius_config.DBLintOptions
    shared_sql: sirius_config.SharedSQL
    optional_arguments: sirius_config.OptionalArguments
    table: table.TableConfig


def render_path_templates(templates: core.PathTemplates, db: str) -> Paths:
    env = os.environ["DATABASE_VERSION"]
    return Paths(
        source=Template(getattr(templates, "source")).render(env=env, db=db),
        land=Template(getattr(templates, "land")).render(env=env, db=db),
        raw=Template(getattr(templates, "raw")).render(env=env, db=db),
        curated=Template(getattr(templates, "curated")).render(env=env, db=db),
        derived=Template(getattr(templates, "derived")).render(env=env, db=db),
    )


def create_run_config(
    config: sirius_config.SiriusPipelineConfig, etl_step: str, table: str
) -> SiriusRunConfig:
    return SiriusRunConfig(
        db_name=config.db_name,
        etl_step_config=config.extract_etl_step_config_for_step(etl_step),
        paths=render_path_templates(config.path_templates, config.db_name),
        db_lint_options=config.db_lint_options,
        shared_sql=config.shared_sql,
        optional_arguments=config.optional_arguments,
        table=config.tables[table],
    )
