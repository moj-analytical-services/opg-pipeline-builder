import os
from copy import deepcopy
from itertools import chain
from pathlib import Path

import yaml
from data_linter import validation
from pydantic import BaseModel, ValidationError, field_validator, model_validator

from opg_pipeline_builder.models.pipeline_configs.core_config.pipeline_config import (
    PipelineConfig,
)
from opg_pipeline_builder.utils.constants import sql_path


class MissingSQLError(Exception):
    def __init__(self, err: str):
        super().__init__(err)
        self.err = err


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
            raise MissingSQLError(err)

        return v


class OptionalArguments(BaseModel):
    get_sirius_data_path: str
    opg_holidats_path: str


class SiriusPipelineConfig(PipelineConfig):
    db_lint_options: DBLintOptions
    shared_sql: SharedSQL
    optional_arguments: OptionalArguments

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
        etl_engines = [e.engine_name for e in self.etl_step_config.values()]
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
