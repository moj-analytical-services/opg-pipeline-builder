import importlib
import inspect
import os
from copy import deepcopy
from itertools import chain
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml
from croniter import croniter
from data_linter import validation
from pkg_resources import resource_filename
from pydantic import BaseModel, ValidationError, field_validator, model_validator

from opg_pipeline_builder.utils.constants import (
    etl_stages,
    etl_steps,
    sql_path,
    transform_types,
)


class TableConfig(BaseModel):
    etl_stages: Dict[str, Dict[str, str]]
    transform_type: str
    frequency: str
    sql: Optional[Dict[str, Union[List[str], bool]]] = None
    lint_options: Optional[Dict[str, Any]] = None
    input_data: Optional[Dict[str, Dict[str, str]]] = None
    optional_arguments: Optional[Dict[str, Any]] = None

    @model_validator(mode="after")
    def check_transform_type_consistency(self) -> "TableConfig":
        if self.transform_type == "derived":
            assert (
                self.lint_options is None
            ), "Derived table should not have self.lint_options"
            assert (
                self.input_data is not None
            ), "Derived table should have self.input_data"

        else:
            assert (
                self.lint_options is not None
            ), f"{self.transform_type} table should have lint_options"
            assert (
                self.input_data is None
            ), f"{self.transform_type} table should not have input_data"

            if self.transform_type == "default":
                assert self.sql is None, "default table should not have sql"

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
        inv_stages = [k for k, _ in v.items() if k not in etl_stages]
        assert not inv_stages, f"ETL stages must be one of {', '.join(etl_stages)}"
        return v

    @field_validator("transform_type")
    @classmethod
    def check_transform_type(cls, v: str) -> str:
        assert (
            v in transform_types
        ), f"Transform type should be one of {', '.join(transform_types)}"
        return v


class ETLStepConfig(BaseModel):
    step: str
    engine_name: str
    transform_name: Optional[str] = None
    transform_kwargs: Optional[Dict[str, object]] = None

    @field_validator("step")
    @classmethod
    def check_in_etl_steps(cls, v: str) -> str:
        assert v in etl_steps, f"{v} not one of the following: {', '.join(etl_steps)}"
        return v

    @model_validator(mode="after")
    def check_engine_exists(self) -> "ETLStepConfig":
        engine_spec = os.path.exists(
            resource_filename(
                "opg_pipeline_builder", f"transform_engines/{self.engine_name}.py"
            )
        )

        if not engine_spec:
            module_path = f"engines.{self.engine_name}"
            try:
                engine_module = importlib.import_module(module_path)
            except ModuleNotFoundError:
                raise ModuleNotFoundError(
                    f"Cannot find transform engine {self.engine_name}"
                )
        else:
            engine_module = importlib.import_module(
                f"opg_pipeline_builder.transform_engines.{self.engine_name}"
            )

        class_name = (
            "".join([n[0].upper() + n[1:].lower() for n in self.engine_name.split("_")])
            + "TransformEngine"
        )

        try:
            engine = getattr(engine_module, class_name)
        except AttributeError:
            raise AttributeError(
                f"Engine {self.engine_name} should contain a {class_name} class."
            )

        functions = [
            f
            for f in inspect.getmembers(engine, predicate=inspect.isfunction)
            if f[0] == self.transform_name
        ]

        assert (
            len(functions) == 1
        ), f"{class_name} class is missing {self.transform_name} method"

        return self


class PipelineConfig(BaseModel):
    db_name: str
    etl: List[ETLStepConfig]
    description: str
    paths: Dict[str, str]
    db_lint_options: Dict[str, Any]
    shared_sql: Optional[Dict[str, List[str]]] = None
    optional_arguments: Optional[Dict[str, Any]] = None
    tables: Dict[str, TableConfig]

    @model_validator(mode="after")
    def check_shared_sql_exists(self) -> "PipelineConfig":
        if self.shared_sql is not None:
            shared_sql_names = list(chain(*[v for _, v in self.shared_sql.items()]))
            shared_sql_path = os.path.join(sql_path, self.db_name, "shared")

            assert os.path.exists(
                shared_sql_path
            ), "shared sql directory does not exist"

            shared_sql_filenames = [Path(p).stem for p in os.listdir(shared_sql_path)]

            missing_sql = [f for f in shared_sql_names if f not in shared_sql_filenames]
            missing_sql_substr = ", ".join(missing_sql)
            message = (
                f"{missing_sql_substr} are missing in the database's shared sql folder"
            )

            assert not missing_sql, message

        return self

    @field_validator("paths")
    @classmethod
    def check_in_etl_stages(cls, v: dict[str, str]) -> dict[str, str]:
        stage = [k for k, _ in v.items()][0]
        assert stage in etl_stages, f"{stage} is not a valid ETL stage"
        return v

    @field_validator("shared_sql")
    @classmethod
    def check_in_transform_types(cls, v: dict[str, list[str]]) -> dict[str, list[str]]:
        transform_type = [k for k, _ in v.items()][0]
        valid_type = transform_type in transform_types
        error_message = f'{transform_type} is not one of {", ".join(transform_types)}'
        assert valid_type, error_message
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
                    assert not missing_tables, missing_tables_msg

                else:
                    input_db_config_paths = [
                        p for p in os.listdir("configs") if Path(p).stem == input_db
                    ]

                    assert (
                        len(input_db_config_paths) == 1
                    ), f"Cannot find config for {input_db}"

                    input_db_config_path = os.path.join(
                        "configs", input_db_config_paths[0]
                    )

                    with open(input_db_config_path, "r") as f:
                        input_db_config = yaml.safe_load(f)

                    try:
                        input_db_config = self(**input_db_config).dict()
                    except ValidationError as e:
                        assert False, f"Invalid config for {input_db}: {e}"

                    db_tables = input_db_config.get("tables")
                    missing_tables = [
                        tbl
                        for tbl, _ in input_table_dict.items()
                        if tbl not in db_tables
                    ]
                    assert (
                        not missing_tables
                    ), f"{', '.join(missing_tables)} not listed in {input_db} config"

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
            assert not missing_sql, missing_sql_message

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

            full_lint_config = {**dummy_s3_paths, **full_lint_config}

            validation.load_and_validate_config(full_lint_config)

        elif "data_linter" in etl_engines:
            raise ValueError(
                "db_lint_options config is required to use data_linter engine"
            )

        return self


def read_pipeline_config(db_name: str) -> PipelineConfig:
    try:
        with open(os.path.join("configs", f"{db_name}.yml")) as config_file:
            raw_pipeline_config = yaml.safe_load(config_file)

    except FileNotFoundError:
        raise ValueError(f"{db_name} config does not exist.")

    pipeline_config = PipelineConfig(**raw_pipeline_config)

    return pipeline_config
