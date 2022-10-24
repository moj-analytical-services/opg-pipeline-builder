import os
import json
import base64
import re
from pathlib import Path
from typing import List, Union, Optional
from datetime import datetime
from dateutil.tz import tzutc
from ast import literal_eval
from binascii import Error

# Constants for pipelines
aws_region = "eu-west-1"
etl_stages = ["land", "raw", "raw-hist", "processed", "curated", "derived", "export"]
etl_steps = [
    "to_land",
    "land_to_raw_hist",
    "land_to_raw",
    "raw_to_processed",
    "raw_to_curated",
    "raw_hist_to_processed",
    "raw_hist_to_curated",
    "processed_to_curated",
    "create_curated_database",
    "create_derived",
    "export_extracts",
]
aws_loggers = {
    "boto",
    "urllib3",
    "s3transfer",
    "boto3",
    "botocore" "nose",
    "awswrangler",
}
transform_types = ["default", "custom", "derived"]

project_root = Path(__file__).absolute().parents[2]
meta_data_base_path = f"{project_root}/meta_data"
glue_jobs_path = f"{project_root}/glue_jobs"
sql_path = f"{project_root}/sql"
engines_path = f"{project_root}/engines"

# Functions that retrieve environment variables


def get_env() -> str:
    """Retrieves DB environment

    Retrieved database environment (e.g. dev, prod).
    Raises error if not specified.

    Return
    ------
    str
        Database environment (e.g. dev, prod)
    """
    try:
        db_env = os.environ["DEFAULT_DB_ENV"]
    except KeyError:
        raise KeyError("DEFAULT_DB_ENV env_var needs to be set")
    return db_env


def get_source_db() -> str:
    """Retrieves source database

    Retrieved source database
    (e.g. sirius, surveys, ...).
    Raises error if not specified.

    Return
    ------
    str
        Database (e.g. sirius, surveys)
    """
    try:
        source_env = os.environ["SOURCE_DB_ENV"]
    except KeyError:
        raise KeyError("SOURCE_DB_ENV env_var needs to be set")
    return source_env


def get_source_tbls() -> Union[List[str], None]:
    """Retrieves source database

    Retrieved source database tables to use
    (e.g. for sirius, 'addresses', or 'cases;persons').
    Returns None if environment variable has not been set.

    Return
    ------
    Union[List[str], None]
        List of tables
    """
    try:
        source_tables = os.environ["SOURCE_TBLS_ENV"].split(";")
    except KeyError:
        source_tables = None
    return source_tables


def get_etl_stage() -> str:
    """Retrieves database etl stage

    Retrieved database etl stage to use
    (e.g. to_land, land_to_raw).
    Raises error if not specified.

    Return
    ------
    str
        ETL stage
    """
    try:
        etl_stage = os.environ["ETL_STAGE_ENV"]
    except KeyError:
        raise KeyError("ETL_STAGE_ENV env_var needs to be set")
    return etl_stage


def get_multiprocessing_settings() -> Union[dict, None]:
    """Retrieves data_linter multiprocessing settings

    Retrieves base64 encoded dictionary from MULTI_PROC_ENV
    environment variable. Result is decoded and returned as
    a dictionary. If variable is not set, returns None.

    Return
    ------
    Union[dict, None]
        Multiprocessing settings as dictionary. See
        opg_etl.utils.lint_utils for validation of the
        resulting object.
    """
    try:
        mp_settings_str = os.environ["MULTI_PROC_ENV"]
        try:
            mp_decode = base64.b64decode(mp_settings_str.encode("ascii"))
            mp_settings = json.loads(mp_decode.decode("ascii"))

        except (UnicodeDecodeError, Error):
            mp_settings = literal_eval(mp_settings_str)
            if not isinstance(mp_settings, dict):
                raise TypeError(
                    "MULTI_PROC_ENV should be a dictionary stored "
                    "as a string, or as a base64 encoded string"
                )

    except KeyError:
        mp_settings = None

    return mp_settings


def get_start_date() -> Union[datetime, None]:
    """Retrieves start date for pipeline

    Retrieves START_DATE environment variable. Converts
    datetime string to datetime object. Used to make
    sure only files in land after this datetime are
    processed by the pipeline. If variable is not set,
    returns None.

    Return
    ------
    Union[datetime, None]
        Datetime for files to process
    """
    try:
        start_str = os.environ["START_DATE"]
        start_dt = datetime.strptime(start_str, "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=tzutc()
        )
    except KeyError:
        start_dt = None

    return start_dt


def get_end_date() -> Union[datetime, None]:
    """Retrieves end date for pipeline

    Retrieves END_DATE environment variable. Converts
    datetime string to datetime object. Used to make
    sure only files in land before this datetime are
    processed by the pipeline. If variable is not set,
    returns None.

    Return
    ------
    Union[datetime, None]
        Datetime for files to process
    """
    try:
        end_str = os.environ["END_DATE"]
        end_dt = datetime.strptime(end_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=tzutc())
    except KeyError:
        end_dt = None

    return end_dt


def get_use_glue() -> bool:
    """Retrieves env var for whether to use AWS Glue

    Retrieves USE_GLUE environment variable. Converts
    to bool object to see whether pipeline should use
    AWS Glue jobs to process table files or not. If
    not set, returns None.

    Return
    ------
    bool
        Indicator of whether to use AWS Glue jobs or not.
    """
    try:
        glue_enable = literal_eval(os.environ["USE_GLUE"])
        if not isinstance(glue_enable, bool):
            raise ValueError(
                "Expecting True or False for 'USE_GLUE' environment variable"
            )
    except KeyError:
        glue_enable = False

    return glue_enable


def get_no_glue_workers() -> Union[int, None]:
    """Retrieves no. of glue workers to use

    Retrieves NO_GLUE_WORKERS environment variable.
    If USE_GLUE is true, then returns an integer for
    the number of glue workers to use (minimum = 2).
    If USE_GLUE is not set, returns None.

    Return
    ------
    Union[int, None]
        No. of glue job workers to use
    """
    try:
        if get_use_glue():
            glue_workers = max(2, int(os.environ["NO_GLUE_WORKERS"]))
        else:
            glue_workers = None
    except KeyError:
        glue_workers = None

    return glue_workers


def get_full_db_name(
    db_name: Optional[str] = None,
    env: Optional[str] = None,
    prefix: Optional[str] = None,
    derived: Optional[bool] = False,
) -> str:
    """Returns full database name

    Returns the full name of the OPG database
    based on the SOURCE_DB_ENV and DEFAULT_DB_ENV
    env variables.

    Paramseters
    -----------
    db_name: Optional[str]
        Name of the source database. Defaults to
        using the output of get_source_db.
    env: Optional[str]
        Database environment version (e.g. dev, prod).
        Defaults to using output of get_env.

    Return
    ------
    str
        Full database name
    """
    if db_name is None:
        db_name = get_source_db()

    if env is None:
        env = get_env()

    if prefix is None:
        prefix = os.environ.get("ATHENA_DB_PREFIX", "")

    env_suffix = f"derived_{env}" if derived else env

    if db_name == "all":
        full_db_name = "_".join([prefix, env_suffix])
    else:
        full_db_name = "_".join([prefix, db_name, env_suffix])

    return full_db_name


def get_metadata_path(db_name: Optional[str] = None, env: Optional[str] = None) -> str:
    """Returns metadata base path

    Returns the base path for the specified
    database's metadata, for the given production
    environment.

    Paramseters
    -----------
    db_name: Optional[str]
        Name of the source database. Defaults to
        using the output of get_source_db.
    env: Optional[str]
        Database environment version (e.g. dev, prod).
        Defaults to using output of get_env.

    Return
    ------
    str
        Base metadata path for db in specified env
    """
    if db_name is None:
        db_name = get_source_db()

    if env is None:
        env = get_env()

    mp = os.path.join(meta_data_base_path, env, db_name)
    return mp


def get_chunk_size() -> Union[int, bool]:
    """Returns chunk size for high memory tables

    Returns chunk size option environment variable
    to pass to e.g. pydbtools or awswrangler
    for chunking reading of large tables into memory.

    Return
    ------
    Union[int, bool]
        Either integer referring to number of records
        in each chunk, or boolean to use default chunking.
    """
    try:
        chunk_str = os.environ["CHUNK_SIZE"]
        chunk = literal_eval(chunk_str)

        if not isinstance(chunk, bool) and not isinstance(chunk, int):
            raise ValueError(
                "CHUNK_SIZE must be a string corresponding " "to a boolean or integer"
            )

    except KeyError:
        chunk = True

    return chunk


def get_dag_timestamp() -> Union[int, None]:
    """Returns DAG run timestamp

    Returns timestamp for when DAG was executed. This will
    either be from the DAG run id if triggered manually
    or the interval end of the DAG run if triggered on
    a schedule. Only does so if temp_staging key-value
    is included in the decoded multiprocessing settings.

    Return
    ------
    Union[int, None]
        Timestamp int for DAG run or None, if not applicable
    """
    mp_args = get_multiprocessing_settings()

    raise_error = False
    if mp_args is not None:
        tmp_staging = mp_args.get("temp_staging", False)
        if tmp_staging is True:
            raise_error = True

    try:
        dag_run_id = os.environ["DAG_RUN_ID"]
        dag_interval_end = os.environ["DAG_INTERVAL_END"]

        manual_run = re.search("manual__", dag_run_id)

        try:
            if manual_run is not None:
                dag_ts = int(
                    datetime.fromisoformat(
                        dag_run_id.replace("manual__", "")
                    ).timestamp()
                )

            else:
                dag_ts = int(datetime.fromisoformat(dag_interval_end).timestamp())

        except Exception as e:
            raise ValueError(
                f"""
                DAG_RUN_TIMESTAMP must be a string in ISO format.
                Error: {e}
                """
            )

    except KeyError:
        if raise_error:
            raise ValueError(
                "DAG_RUN_ID and DAG_INTERVAL_END must be set when temp staging enabled."
            )
        else:
            dag_ts = None

    return dag_ts
