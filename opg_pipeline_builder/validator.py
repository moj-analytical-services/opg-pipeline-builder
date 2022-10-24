import os
import yaml
from .utils.constants import project_root, meta_data_base_path, get_env, etl_stages
from data_linter import validation
from jsonschema import validate, exceptions
from copy import deepcopy


def _validate_config_keys(config: dict) -> None:
    config_keys = list(config.keys())

    if (
        set(config_keys).intersection(
            ["db_name", "description", "db_lint_options", "paths", "tables"]
        )
        is False
    ):
        raise KeyError("config does not contain required keys")


def _validate_db_config_lint(config: dict):
    full_lint_config = deepcopy(config["db_lint_options"])
    if full_lint_config is not None:
        full_lint_config["tables"] = {}
        for table in config.get("tables").keys():
            table_lint_config = config["tables"][table]
            table_tf = table_lint_config["transform_type"]

            if table_tf != "derived":
                try:
                    lint_opt = table_lint_config["lint_options"]
                    full_lint_config["tables"][table] = lint_opt

                except Exception:
                    raise KeyError(f"{table} does not have lint options")

        dummy_s3_paths = {
            "land-base-path": "s3://testing-bucket/land/",  # Where to get the data from
            "fail-base-path": "s3://testing-bucket/fail/",  # Where to write if failed
            "pass-base-path": "s3://testing-bucket/pass/",  # Where to write if passed
            "log-base-path": "s3://testing-bucket/logs/",
        }

        full_lint_config = {**dummy_s3_paths, **full_lint_config}

        try:
            validation.load_and_validate_config(full_lint_config)

        except Exception:
            raise ValueError("Incorrect data_linter config options")


def _validate_db_config_tables(config: dict) -> None:
    meta_path_prefix = os.path.join(meta_data_base_path, get_env(), config["db_name"])

    cfig_tbls = config["tables"]
    cfig_stgs = set()
    for tbl in cfig_tbls:
        tbl_stgs = set(cfig_tbls[tbl]["etl_stages"].keys())
        cfig_stgs = cfig_stgs.union(tbl_stgs)

    stage_meta_tbls = {}
    for stage in cfig_stgs:
        try:
            stage_meta_files = os.listdir(os.path.join(meta_path_prefix, stage))
        except FileNotFoundError:
            stage_meta_files = []

        stage_meta_tbls[stage] = [
            table.replace(".json", "") for table in stage_meta_files
        ]

    cfig_data = config["tables"]
    cfig_tbls = list(config["tables"].keys())

    defcust_tables = [
        table
        for table in cfig_tbls
        if cfig_data[table]["transform_type"] in ["default", "custom"]
    ]

    derived_tables = [
        table for table in cfig_tbls if cfig_data[table]["transform_type"] == "derived"
    ]

    initial_check = True
    for stage in cfig_stgs.intersection({"raw", "raw-hist", "processed", "curated"}):
        if set(defcust_tables) != set(stage_meta_tbls[stage]):
            initial_check = False

    derived_check = set(derived_tables) == set(stage_meta_tbls.get("derived", []))
    final_check = initial_check and derived_check

    if not final_check:
        raise AttributeError("Config tables do not match metadata tables")


def _validate_db_config_etl(config: dict) -> None:
    full_etl_config = deepcopy(config)
    full_etl_config.pop("db_lint_options")
    tables = full_etl_config["tables"].keys()

    for table in tables:
        try:
            full_etl_config["tables"][table].pop("lint_options")

        except KeyError:
            ...

    etl_pattern = "|".join([f"^{stage}$" for stage in etl_stages])

    etl_anyof = [{"required": [stage]} for stage in etl_stages]

    schema = {
        "type": "object",
        "properties": {
            "db_name": {"type": "string"},
            "description": {"type": "string"},
            "buckets": {
                "type": "object",
                "patternProperties": {etl_pattern: {"type": "string"}},
                "unevaluatedProperties": False,
                "anyOf": etl_anyof,
            },
            "optional_arguments": {"anyOf": [{"type": "null"}, {"type": "object"}]},
            "tables": {
                "type": "object",
                "additionalProperties": {
                    "type": "object",
                    "properties": {
                        "transform_type": {"enum": ["default", "custom", "derived"]},
                        "etl_stages": {
                            "type": "object",
                            "patternProperties": {
                                etl_pattern: {
                                    "type": "object",
                                    "properties": {
                                        "file_format": {
                                            "enum": ["parquet", "json", "csv"]
                                        }
                                    },
                                }
                            },
                            "unevaluatedProperties": False,
                            "anyOf": etl_anyof,
                        },
                    },
                    "if": {"properties": {"transform_type": {"const": "derived"}}},
                    "then": {
                        "properties": {
                            "input_data": {
                                "type": "object",
                                "additionalProperties": {
                                    "type": "object",
                                    "additionalProperties": {"type": "string"},
                                },
                            }
                        },
                        "required": ["input_data"],
                    },
                    "required": ["transform_type", "etl_stages"],
                },
            },
        },
    }

    try:
        validate(instance=full_etl_config, schema=schema)

    except exceptions.ValidationError as e:
        raise ValueError(f"Config etl schema validation error: {e}")


def validate_config(config: dict) -> None:
    try:
        _validate_config_keys(config)
        _validate_db_config_tables(config)
        _validate_db_config_lint(config)
        _validate_db_config_etl(config)
    except Exception as e:
        raise exceptions.ValidationError(f"DB config validation error:\n{e}")


def read_db_config(db_name: str) -> dict:
    try:
        with open(
            os.path.join(project_root, "configs", f"{db_name}.yml")
        ) as config_file:
            db_tables = yaml.safe_load(config_file)

    except FileNotFoundError:
        raise ValueError(f"{db_name} config does not exist.")

    validate_config(db_tables)

    return db_tables
