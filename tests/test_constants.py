import os
from datetime import datetime

import pytest
from dateutil.tz import tzutc


def test_get_env():
    from opg_pipeline_builder.utils.constants import get_env

    assert get_env() == "test"

    orig_env = os.environ["DEFAULT_DB_ENV"]
    del os.environ["DEFAULT_DB_ENV"]
    with pytest.raises(KeyError):
        get_env()

    os.environ["DEFAULT_DB_ENV"] = orig_env


def test_get_source_db():
    from opg_pipeline_builder.utils.constants import get_source_db

    assert get_source_db() == "testdb"

    orig_env = os.environ["SOURCE_DB_ENV"]
    del os.environ["SOURCE_DB_ENV"]
    with pytest.raises(KeyError):
        get_source_db()

    os.environ["SOURCE_DB_ENV"] = orig_env


def test_get_source_tbls():
    from opg_pipeline_builder.utils.constants import get_source_tbls

    assert get_source_tbls() == ["table1", "table2", "table3"]

    orig_env = os.environ["SOURCE_TBLS_ENV"]
    del os.environ["SOURCE_TBLS_ENV"]

    assert get_source_tbls() is None

    os.environ["SOURCE_TBLS_ENV"] = orig_env


def test_get_etl_stage():
    from opg_pipeline_builder.utils.constants import get_etl_stage

    assert get_etl_stage() == "raw_hist_to_curated"

    orig_env = os.environ["ETL_STAGE_ENV"]
    del os.environ["ETL_STAGE_ENV"]

    with pytest.raises(KeyError):
        get_etl_stage()

    os.environ["ETL_STAGE_ENV"] = orig_env


@pytest.mark.parametrize(
    "env_var, expected",
    [
        ("eyJlbmFibGUiOiAibG9jYWwifQ==", {"enable": "local"}),
        (
            (
                '{"enable": "pod", "total_workers": 5, "current_worker": 0,'
                ' "close_status": False, "temp_staging": True}'
            ),
            {
                "enable": "pod",
                "total_workers": 5,
                "current_worker": 0,
                "close_status": False,
                "temp_staging": True,
            },
        ),
    ],
)
def test_get_multiprocessing_settings(env_var, expected):
    from opg_pipeline_builder.utils.constants import get_multiprocessing_settings

    assert get_multiprocessing_settings() is None

    os.environ["MULTI_PROC_ENV"] = env_var

    assert get_multiprocessing_settings() == expected

    del os.environ["MULTI_PROC_ENV"]


def test_get_start_date():
    from opg_pipeline_builder.utils.constants import get_start_date

    assert get_start_date() is None

    os.environ["START_DATE"] = "2022-09-01"

    assert get_start_date() == datetime(2022, 9, 1, 0, 0, 0, tzinfo=tzutc())

    del os.environ["START_DATE"]


def test_get_end_date():
    from opg_pipeline_builder.utils.constants import get_end_date

    assert get_end_date() is None

    os.environ["END_DATE"] = "2022-09-30"

    assert get_end_date() == datetime(2022, 9, 30, 23, 59, 59, tzinfo=tzutc())

    del os.environ["END_DATE"]


@pytest.mark.parametrize(
    "enable_glue, expected, error",
    [
        ("True", True, False),
        ("False", False, False),
        ("1_000", None, True),
        (None, False, False),
    ],
)
def test_get_use_glue(enable_glue, expected, error):
    from opg_pipeline_builder.utils.constants import get_use_glue

    if enable_glue is None:
        ug = get_use_glue()
        assert ug == expected
    else:
        os.environ["USE_GLUE"] = enable_glue
        if error:
            with pytest.raises(ValueError):
                get_use_glue()
        else:
            ug = get_use_glue()
            assert ug == expected
        del os.environ["USE_GLUE"]


@pytest.mark.parametrize(
    "use_glue, no_workers, expected",
    [
        ("True", "2", 2),
        ("False", "3", None),
        ("1_000", "4", None),
        (None, "5", None),
        ("True", "1", 2),
    ],
)
def test_get_no_glue_workers(use_glue, no_workers, expected):
    from opg_pipeline_builder.utils.constants import get_no_glue_workers, get_use_glue

    if use_glue is None:
        if "USE_GLUE" in os.environ.keys():
            del os.environ["USE_GLUE"]
        n_wrks = get_no_glue_workers()
        assert n_wrks == expected
    else:
        os.environ["USE_GLUE"] = use_glue
        os.environ["NO_GLUE_WORKERS"] = no_workers

        try:
            _ = get_use_glue()
            error = False
        except ValueError:
            error = True

        if error:
            with pytest.raises(ValueError):
                get_no_glue_workers()
        else:
            assert get_no_glue_workers() == expected

        del os.environ["USE_GLUE"]
        del os.environ["NO_GLUE_WORKERS"]


@pytest.mark.parametrize(
    "db_name, prefix, expected",
    [("all", "dep", "dep_test"), ("testdb", "dep", "dep_testdb_test")],
)
def test_get_opg_db_name(db_name, prefix, expected):
    from opg_pipeline_builder.utils.constants import get_full_db_name

    os.environ["ATHENA_DB_PREFIX"] = prefix
    assert get_full_db_name(db_name) == expected
    del os.environ["ATHENA_DB_PREFIX"]


def test_get_metadata_path():
    from opg_pipeline_builder.utils.constants import get_metadata_path

    assert get_metadata_path() == os.path.join(
        "meta_data",
        os.environ["DEFAULT_DB_ENV"],
        os.environ["SOURCE_DB_ENV"],
    )


@pytest.mark.parametrize(
    "chunk_size, expected, error",
    [
        ("False", False, False),
        ("1_000_000_000", 1_000_000_000, False),
        ("True", True, False),
        ("hello", None, True),
    ],
)
def test_get_chunk_size(chunk_size, expected, error):
    from opg_pipeline_builder.utils.constants import get_chunk_size

    os.environ["CHUNK_SIZE"] = chunk_size
    if error:
        with pytest.raises(ValueError):
            get_chunk_size()
    else:
        ch_rtn = get_chunk_size()
        assert ch_rtn == expected
    del os.environ["CHUNK_SIZE"]


@pytest.mark.parametrize(
    "mp_env_var, dag_run_id, dag_interval_run, expected",
    [
        (
            "eyJlbmFibGUiOiAibG9jYWwifQ==",
            None,
            None,
            None,
        ),
        (
            (
                '{"enable": "pod", "total_workers": 5, "current_worker": 0,'
                '"close_status": False, "temp_staging": True}'
            ),
            None,
            None,
            None,
        ),
        (
            (
                '{"enable": "pod", "total_workers": 5, "current_worker": 0,'
                '"close_status": False, "temp_staging": True}'
            ),
            "scheduled__2022-09-12T05:00:00+00:00",
            "2022-09-13, 05:00:00 UTC",
            1663041600,
        ),
        (
            (
                '{"enable": "pod", "total_workers": 5, "current_worker": 0,'
                '"close_status": False, "temp_staging": True}'
            ),
            "manual__2022-09-12T05:00:00+00:00",
            None,
            1662955200,
        ),
    ],
)
def test_get_dag_timestamp(mp_env_var, dag_run_id, dag_interval_run, expected):
    from opg_pipeline_builder.utils.constants import (
        get_dag_timestamp,
        get_multiprocessing_settings,
    )

    if mp_env_var is not None:
        os.environ["MULTI_PROC_ENV"] = mp_env_var

    mp_args = get_multiprocessing_settings()
    tmp_staging_enabled = mp_args.get("temp_staging", False)

    if dag_run_id is None and dag_interval_run is None and tmp_staging_enabled:
        with pytest.raises(ValueError):
            get_dag_timestamp()
    elif dag_run_id is None and dag_interval_run is None:
        assert get_dag_timestamp() == expected
