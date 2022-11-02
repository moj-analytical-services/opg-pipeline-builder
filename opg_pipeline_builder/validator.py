import os
import yaml
import inspect
import importlib

from pathlib import Path
from copy import deepcopy
from itertools import chain
from croniter import croniter
from data_linter import validation
from pkg_resources import resource_filename
from typing import Any, List, Dict, Optional
from pydantic import BaseModel, MissingError, ValidationError, root_validator, validator
from opg_pipeline_builder.utils.constants import (
    etl_stages,
    transform_types,
    sql_path,
    etl_steps,
)


class TableConfig(BaseModel):
    etl_stages: Dict[str, Dict[str, str]]
    transform_type: str
    frequency: str
    sql: Optional[Dict[str, List[str]]] = None
    lint_options: Optional[Dict[str, Any]] = None
    input_data: Optional[Dict[str, Dict[str, str]]] = None

    @root_validator(pre=True, allow_reuse=True)
    def check_transform_type_consistency(cls, values):
        transform_type = values.get("transform_type")
        input_data = values.get("input_data")
        lint_options = values.get("lint_options")
        sql = values.get("sql")

        if transform_type == "derived":
            assert lint_options is None, "Derived table should not have lint_options"
            assert input_data is not None, "Derived table should have input_data"
        else:
            assert (
                lint_options is not None
            ), f"{transform_type} table should have lint_options"
            assert (
                input_data is None
            ), f"{transform_type} table should not have input_data"
            assert sql is None, f"{transform_type} table should not have sql"

        return values

    @validator("frequency", allow_reuse=True)
    def check_valid_cron(cls, v):
        assert croniter.is_valid(v), f"{v} isn't a valid cron expression"
        return v

    @validator("etl_stages", allow_reuse=True)
    def check_valid_etl_stages(cls, v):
        inv_stages = [k for k, _ in v.items() if k not in etl_stages]
        assert not inv_stages, f"ETL stages must be one of {', '.join(etl_stages)}"
        return v

    @validator("transform_type", allow_reuse=True)
    def check_transform_type(cls, v):
        assert (
            v in transform_types
        ), f"Transform type should be one of {', '.join(transform_types)}"
        return v


class ETLStepConfig(BaseModel):
    step: str
    engine_name: str
    transform_name: Optional[str] = None
    transform_kwargs: Optional[Dict[str, object]] = None

    @validator("step", allow_reuse=True)
    def check_in_etl_steps(cls, v):
        assert v in etl_steps, f"{v} not one of the following: {', '.join(etl_steps)}"
        return v

    @validator("engine_name", allow_reuse=True)
    def check_engine_exists(cls, v, values):
        method = values.get("transform_name", "run")

        engine_spec = os.path.exists(
            resource_filename("opg_pipeline_builder", f"transform_engines/{v}.py")
        )

        if not engine_spec:
            module_path = f"engines.{v}"
            try:
                engine_module = importlib.import_module(module_path)
            except ModuleNotFoundError:
                raise ModuleNotFoundError(f"Cannot find transform engine {v}")
        else:
            engine_module = importlib.import_module(
                f"opg_pipeline_builder.transform_engines.{v}"
            )

        class_name = (
            "".join([n[0].upper() + n[1:].lower() for n in v.split("_")])
            + "TransformEngine"
        )

        try:
            engine = getattr(engine_module, class_name)
        except AttributeError:
            raise AttributeError(f"Engine {v} should contain a {class_name} class.")

        functions = [
            f
            for f in inspect.getmembers(engine, predicate=inspect.isfunction)
            if f[0] == method
        ]

        assert len(functions) == 1, f"{class_name} class is missing {method} method"

        return v


class PipelineConfig(BaseModel):
    db_name: str
    etl: List[ETLStepConfig]
    description: str
    paths: Dict[str, str]
    db_lint_options: Dict[str, Any]
    shared_sql: Optional[Dict[str, List[str]]] = None
    optional_arguments: Optional[Dict[str, Any]] = None
    tables: Dict[str, TableConfig]

    @root_validator(allow_reuse=True)
    def check_shared_sql_exists(cls, values):
        db_name, shared_sql = values.get("db_name"), values.get("shared_sql")
        if shared_sql is not None:
            shared_sql_names = list(chain(*[v for _, v in shared_sql.items()]))
            shared_sql_path = os.path.join(sql_path, db_name, "shared")

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

        return values

    @validator("paths", allow_reuse=True)
    def check_in_etl_stages(cls, v):
        stage = [k for k, _ in v.items()][0]
        assert stage in etl_stages, f"{stage} is not a valid ETL stage"
        return v

    @validator("shared_sql", allow_reuse=True)
    def check_in_transform_types(cls, v):
        transform_type = [k for k, _ in v.items()][0]
        valid_type = transform_type in transform_types
        error_message = f'{transform_type} is not one of {", ".join(transform_types)}'
        assert valid_type, error_message
        return v

    @root_validator(pre=True, allow_reuse=True)
    def check_derived_inputs(cls, values):
        db_name, tables = values.get("db_name"), values.get("tables")

        table_inputs = [
            (name, table.get("input_data"))
            for name, table in tables.items()
            if table.get("input_data") is not None
        ]

        table_names = [
            name for name, table in tables.items() if table.get("input_data") is None
        ]

        for table_name, table_input in table_inputs:
            for input_db, input_table_dict in table_input.items():
                if input_db == db_name:
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
                        input_db_config = cls(**input_db_config).dict()
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

        return values

    @root_validator(pre=True, allow_reuse=True)
    def check_derived_sql(cls, values):
        db_name, tables = values.get("db_name"), values.get("tables")

        table_sql_info = [
            (name, chain(*[sql for _, sql in table.get("sql").items()]))
            for name, table in tables.items()
            if table.get("sql") is not None and table.get("transform_type") == "derived"
        ]

        for table_name, table_sql in table_sql_info:
            table_sql_root_path = os.path.join(sql_path, db_name, table_name)
            missing_sql = [
                sql
                for sql in table_sql
                if not os.path.exists(os.path.join(table_sql_root_path, f"{sql}.sql"))
            ]
            missing_sql_message = f"SQL for {table_name} is missing: " ", ".join(
                missing_sql
            )
            assert not missing_sql, missing_sql_message

        return values

    @root_validator(allow_reuse=True)
    def check_data_linter_config(cls, values):
        full_lint_config = deepcopy(values.get("db_lint_options"))
        etl_engines = [e.engine_name for e in values.get("etl")]
        if full_lint_config is not None:
            full_lint_config["tables"] = {}

            for table_name, table in values.get("tables").items():
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
            raise MissingError(
                "db_lint_options config is required to use data_linter engine"
            )

        return values


def read_pipeline_config(db_name: str) -> dict:
    try:
        with open(os.path.join("configs", f"{db_name}.yml")) as config_file:
            raw_pipeline_config = yaml.safe_load(config_file)

    except FileNotFoundError:
        raise ValueError(f"{db_name} config does not exist.")

    pipeline_config = PipelineConfig(**raw_pipeline_config)

    return pipeline_config
