import os
from copy import deepcopy
from itertools import chain
from pathlib import Path

import yaml
from data_linter import validation
from pydantic import BaseModel, ValidationError, field_validator, model_validator

from opg_pipeline_builder.models.pipeline_configs.sirius.table_config import TableConfig
from opg_pipeline_builder.utils.constants import etl_stages, sql_path, transform_types


class MissingSQLERror(Exception):
    def __init__(self, err: str):
        super().__init__(err)
        self.err = err


class ETLStepConfig(BaseModel):
    step: str
    engine_name: str
    transform_name: str = "run"
    transform_kwargs: dict[str, str] = {}


class PathTemplates(BaseModel):
    land: str = ""
    raw: str = ""
    raw_hist: str = ""
    processed: str = ""
    curated: str = ""


class DBLintOptions(BaseModel):
    validator_engine: str
    timestamp_partition_name: str
    compress_data: bool = False
    remove_tables_on_pass: bool = True
    all_must_pass: bool = True


class SharedSQL(BaseModel):
    default: str
    custom: str
    derived: list[str]

    @field_validator("default", "custom", "derived")
    @classmethod
    def check_default_sql_exists(cls, v: str) -> str:
        if not (Path(f"sql/sirius/shared/{v}.sql")).exists():
            err = f"shared SQL file for '{v}' does not exist in sql/shared"
            raise MissingSQLERror(err)

        return v


class OptionalArguments(BaseModel):
    get_sirius_data_path: str
    opg_holidats_path: str


class PipelineConfig(BaseModel):
    db_name: str
    description: str
    etl_step_config: dict[str, ETLStepConfig]
    path_templates: PathTemplates
    db_lint_options: DBLintOptions
    shared_sql: SharedSQL
    optional_arguments: OptionalArguments
    tables: dict[str, TableConfig]

    @field_validator("paths")
    @classmethod
    def check_in_etl_stages(cls, v: dict[str, str]) -> dict[str, str]:
        stage = [k for k in v][0]
        if stage not in etl_stages:
            raise ValueError(f"{stage} is not a valid ETL stage")
        return v

    @field_validator("shared_sql")
    @classmethod
    def check_in_transform_types(cls, v: dict[str, list[str]]) -> dict[str, list[str]]:
        transform_type = [k for k, _ in v.items()][0]
        valid_type = transform_type in transform_types
        error_message = f"{transform_type} is not one of {', '.join(transform_types)}"
        if not valid_type:
            raise ValueError(error_message)
        return v

    @model_validator(mode="after")
    def check_derived_inputs(self) -> "PipelineConfig":
        table_inputs = [
            (name, table.input_data)
            for name, table in self.tables.items()
            if table.input_data is not None
        ]

        table_names = [name for name in self.tables]

        for table_name, table_input in table_inputs:
            for input_db, input_table_dict in table_input.items():
                if input_db == self.db_name:
                    missing_tables = [
                        tbl
                        for tbl, _ in input_table_dict.items()
                        if tbl not in table_names
                    ]

                    missing_tables_msg = (
                        f"{table_name} inputs do not exist in config: "
                        + ", ".join(missing_tables)
                    )
                    if missing_tables:
                        raise ValueError(missing_tables_msg)

                else:
                    input_db_config_paths = [
                        p
                        for p in os.listdir("src/opg_pipeline/configs")
                        if Path(p).stem == input_db
                    ]

                    if len(input_db_config_paths) != 1:
                        raise ValueError(f"Cannot find config for {input_db}")

                    input_db_config_path = os.path.join(
                        "configs", input_db_config_paths[0]
                    )

                    with open(input_db_config_path, "r") as f:
                        input_db_config = yaml.safe_load(f)

                    try:
                        input_db_config = PipelineConfig(**input_db_config).dict()
                    except ValidationError as e:
                        raise ValueError(f"Invalid config for {input_db}: {e}")

                    db_tables = input_db_config.get("tables")
                    missing_tables = [
                        tbl
                        for tbl, _ in input_table_dict.items()
                        if tbl not in db_tables
                    ]
                    if missing_tables:
                        raise ValueError(
                            f"{', '.join(missing_tables)} not listed in {input_db} config"
                        )

        return self

    @model_validator(mode="after")
    def check_derived_sql(self) -> "PipelineConfig":
        table_sql_info = [
            (
                name,
                chain(*[sql for _, sql in table.sql.items() if isinstance(sql, list)]),
            )
            for name, table in self.tables.items()
            if table.sql is not None and table.transform_type == "derived"
        ]

        for table_name, table_sql in table_sql_info:
            table_sql_root_path = os.path.join(sql_path, self.db_name, table_name)
            missing_sql = [
                sql
                for sql in table_sql
                if not os.path.exists(os.path.join(table_sql_root_path, f"{sql}.sql"))
            ]
            missing_sql_message = f"SQL for {table_name} is missing: " + ", ".join(
                missing_sql
            )
            if missing_sql:
                raise ValueError(missing_sql_message)

        return self

    @model_validator(mode="after")
    def check_data_linter_config(self) -> "PipelineConfig":
        full_lint_config = deepcopy(self.db_lint_options)
        etl_engines = [e.engine_name for e in self.etl]
        if full_lint_config is not None:
            full_lint_config["tables"] = {}

            for table_name, table in self.tables.items():
                table_tf = table.transform_type

                if table_tf != "derived":
                    try:
                        lint_opt = table.lint_options
                        full_lint_config["tables"][table_name] = lint_opt

                    except Exception:
                        raise KeyError(f"{table} does not have lint options")

            dummy_s3_paths = {
                "land-base-path": "s3://testing-bucket/land/",
                "fail-base-path": "s3://testing-bucket/fail/",
                "pass-base-path": "s3://testing-bucket/pass/",
                "log-base-path": "s3://testing-bucket/logs/",
            }

            full_lint_config = {**dummy_s3_paths, **full_lint_config}  # type: ignore

            validation.load_and_validate_config(full_lint_config)

        elif "data_linter" in etl_engines:
            raise ValueError(
                "db_lint_options config is required to use data_linter engine"
            )

        return self

    @property
    def etl_steps(self) -> list[str]:
        """Set of all etl steps configured for this pipeline."""
        return [step.step for step in self.etl]


def read_pipeline_config(db_name: str) -> PipelineConfig:
    try:
        with open(os.path.join("configs", f"{db_name}.yml")) as config_file:
            raw_pipeline_config = yaml.safe_load(config_file)

    except FileNotFoundError:
        raise ValueError(f"{db_name} config does not exist.")

    pipeline_config = PipelineConfig(**raw_pipeline_config)

    return pipeline_config
