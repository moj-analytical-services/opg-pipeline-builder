import os
from copy import deepcopy

import pytest
from mojap_metadata import Metadata


@pytest.mark.parametrize("db_name, expected", [("testdb", True)])
def test_database_init(db_name, expected):
    from opg_pipeline_builder.database import Database

    if expected:
        Database(db_name=db_name)
        pass

    else:
        with pytest.raises(Exception):
            Database(db_name=db_name)


@pytest.fixture(
    params=[
        (
            "testdb",
            {
                "name": "testdb",
                "env": "test",
                "tables": ["table1", "table2", "table3"],
                "metadata_path": "meta_data/test/testdb",
                "lint_config": {
                    "land-base-path": "s3://mojap-land/dep/test/testdb/",
                    "fail-base-path": "s3://mojap-raw-hist/dep/test/testdb/fail/",
                    "pass-base-path": "s3://mojap-raw-hist/dep/test/testdb/pass/",
                    "log-base-path": "s3://mojap-raw-hist/dep/test/testdb/log/",
                    "compress-data": False,
                    "remove-tables-on-pass": True,
                    "all-must-pass": True,
                    "validator-engine": "pandas",
                    "timestamp-partition-name": "mojap_file_land_timestamp",
                    "tables": {
                        "table1": {
                            "required": False,
                            "expect-header": True,
                            "headers-ignore-case": False,
                            "allow-missing-cols": True,
                            "metadata": "meta_data/test/testdb/raw-hist/table1.json",
                        },
                        "table2": {
                            "required": False,
                            "expect-header": True,
                            "headers-ignore-case": False,
                            "allow-missing-cols": True,
                            "metadata": "meta_data/test/testdb/raw-hist/table2.json",
                        },
                    },
                },
                "transform_args": {
                    "transforms": {
                        "table1": {
                            "transform_type": "default",
                            "input": {
                                "path": (
                                    "s3://mojap-raw-hist/dep/test/testdb/pass/table1"
                                ),
                                "file_format": "csv",
                            },
                            "output": {
                                "path": (
                                    "s3://alpha-dep-etl/test/testdb/curated/table1"
                                ),
                                "file_format": "parquet",
                            },
                        },
                        "table2": {
                            "transform_type": "custom",
                            "input": {
                                "path": (
                                    "s3://mojap-raw-hist/dep/test/testdb/pass/table2"
                                ),
                                "file_format": "csv",
                            },
                            "output": {
                                "path": (
                                    "s3://alpha-dep-etl/test/testdb/curated/table2"
                                ),
                                "file_format": "parquet",
                            },
                        },
                        "table3": {
                            "transform_type": "derived",
                            "input": {
                                "testdb": {
                                    "table1": {
                                        "path": (
                                            "s3://mojap-raw-hist/dep/test"
                                            "/testdb/pass/table1"
                                        ),
                                        "frequency": "@daily",
                                        "file_format": "csv",
                                    },
                                    "table2": {
                                        "path": (
                                            "s3://mojap-raw-hist/dep/test/"
                                            "testdb/pass/table2"
                                        ),
                                        "frequency": "@daily",
                                        "file_format": "csv",
                                    },
                                }
                            },
                            "output": {
                                "path": "s3://alpha-dep-etl/test/testdb/derived/table3",
                                "file_format": "parquet",
                            },
                        },
                    }
                },
            },
        )
    ]
)
def db_fixt(request):
    db_name, expected = request.param
    from opg_pipeline_builder.database import Database

    db = Database(db_name=db_name)

    return db, expected


def test_db_name(db_fixt):
    db, expected = db_fixt
    assert db.name == expected["name"]


def test_db_env(db_fixt):
    db, expected = db_fixt
    assert db.env == expected["env"]


def test_db_tables(db_fixt):
    db, expected = db_fixt
    assert db.tables == expected["tables"]


def test_db_config(db_fixt):
    from opg_pipeline_builder.validator import read_pipeline_config

    db, _ = db_fixt
    pipeline_config = read_pipeline_config(db.name)

    assert pipeline_config.model_dump() == db.config


def test_db_metadata_path(db_fixt):
    db, expected = db_fixt
    full_expected_path = expected["metadata_path"]
    assert db.metadata_path == full_expected_path


def test_db_land_path(db_fixt):
    from tests.conftest import land_bucket

    db, _ = db_fixt
    exp_lp = os.path.join("s3://", land_bucket, "dep", db.env, db.name)
    assert db.land_path == exp_lp


def test_db_raw_hist_path(db_fixt):
    from tests.conftest import raw_hist_bucket

    db, _ = db_fixt
    exp_rhp = os.path.join("s3://", raw_hist_bucket, "dep", db.env, db.name)
    assert db.raw_hist_path == exp_rhp


def test_db_processed_path(db_fixt):
    db, _ = db_fixt
    assert db.processed_path == ""


def test_db_curated_path(db_fixt):
    from tests.conftest import dep_bucket

    db, _ = db_fixt
    exp_cp = os.path.join("s3://", dep_bucket, db.env, db.name, "curated")
    assert db.curated_path == exp_cp


@pytest.mark.parametrize(
    "db_table, expected",
    [("testdb.table1", True), ("testdb.table2", True), ("testdb.table3", True)],
)
def test_db_tables_init(db_table, expected):
    from opg_pipeline_builder.database import Database, DatabaseTable

    db_name, tbl_name = db_table.split(".")
    db = Database(db_name)

    if expected:
        DatabaseTable(table_name=tbl_name, db=db)

    else:
        with pytest.raises(Exception):
            DatabaseTable(table_name=tbl_name, db=db)


def test_db_primary_partition(db_fixt):
    db, expected = db_fixt
    assert db.primary_partition_name() == expected.get("lint_config").get(
        "timestamp-partition-name"
    )


@pytest.fixture(
    params=[
        (
            "testdb.table1",
            {
                "name": "table1",
                "db_name": "testdb",
                "transform_type": "default",
                "etl_stages": ["land", "raw_hist", "curated"],
                "file_formats": {
                    "land": {"file_format": "csv"},
                    "raw_hist": {"file_format": "csv"},
                    "curated": {"file_format": "parquet"},
                },
                "table_paths": {
                    "land": "s3://mojap-land/dep/test/testdb/table1",
                    "raw_hist": "s3://mojap-raw-hist/dep/test/testdb/pass/table1",
                    "curated": "s3://alpha-dep-etl/test/testdb/curated/table1",
                },
                "table_meta_paths": {
                    "raw_hist": "meta_data/test/testdb/raw-hist/table1.json",
                    "curated": "meta_data/test/testdb/curated/table1.json",
                },
                "input_data": None,
                "lint_config": {
                    "required": False,
                    "expect-header": True,
                    "headers-ignore-case": False,
                    "allow-missing-cols": True,
                    "metadata": "meta_data/test/testdb/raw-hist/table1.json",
                },
                "transform_args": {
                    "transform_type": "default",
                    "input": {
                        "path": "s3://mojap-raw-hist/dep/test/testdb/pass/table1",
                        "file_format": "csv",
                    },
                    "output": {
                        "path": "s3://alpha-dep-etl/test/testdb/curated/table1",
                        "file_format": "parquet",
                    },
                },
            },
        ),
        (
            "testdb.table2",
            {
                "name": "table2",
                "db_name": "testdb",
                "transform_type": "custom",
                "etl_stages": ["land", "raw-hist", "curated"],
                "file_formats": {
                    "land": {"file_format": "csv"},
                    "raw_hist": {"file_format": "csv"},
                    "curated": {"file_format": "parquet"},
                },
                "table_paths": {
                    "land": "s3://mojap-land/dep/test/testdb/table2",
                    "raw_hist": "s3://mojap-raw-hist/dep/test/testdb/pass/table2",
                    "curated": "s3://alpha-dep-etl/test/testdb/curated/table2",
                },
                "table_meta_paths": {
                    "raw_hist": "meta_data/test/testdb/raw-hist/table2.json",
                    "curated": "meta_data/test/testdb/curated/table2.json",
                },
                "input_data": None,
                "lint_config": {
                    "required": False,
                    "expect-header": True,
                    "headers-ignore-case": False,
                    "allow-missing-cols": True,
                    "metadata": "meta_data/test/testdb/raw-hist/table2.json",
                },
                "transform_args": {
                    "transform_type": "custom",
                    "input": {
                        "path": "s3://mojap-raw-hist/dep/test/testdb/pass/table2",
                        "file_format": "csv",
                    },
                    "output": {
                        "path": "s3://alpha-dep-etl/test/testdb/curated/table2",
                        "file_format": "parquet",
                    },
                },
            },
        ),
        (
            "testdb.table3",
            {
                "name": "table3",
                "db_name": "testdb",
                "transform_type": "derived",
                "etl_stages": ["derived"],
                "file_formats": {"derived": {"file_format": "parquet"}},
                "table_paths": {
                    "derived": "s3://alpha-dep-etl/test/testdb/derived/table3"
                },
                "table_meta_paths": {
                    "derived": "meta_data/test/testdb/derived/table3.json"
                },
                "input_data": {
                    "testdb": {
                        "table1": {
                            "path": "s3://mojap-raw-hist/dep/test/testdb/pass/table1",
                            "frequency": "@daily",
                            "file_format": "csv",
                        },
                        "table2": {
                            "path": "s3://mojap-raw-hist/dep/test/testdb/pass/table2",
                            "frequency": "@daily",
                            "file_format": "csv",
                        },
                    }
                },
                "lint_config": {},
                "transform_args": {
                    "transform_type": "derived",
                    "input": {
                        "testdb": {
                            "table1": {
                                "path": (
                                    "s3://mojap-raw-hist/dep/test/testdb/pass/table1"
                                ),
                                "frequency": "@daily",
                                "file_format": "csv",
                            },
                            "table2": {
                                "path": (
                                    "s3://mojap-raw-hist/dep/test/testdb/pass/table2"
                                ),
                                "frequency": "@daily",
                                "file_format": "csv",
                            },
                        }
                    },
                    "output": {
                        "path": "s3://alpha-dep-etl/test/testdb/derived/table3",
                        "file_format": "parquet",
                    },
                },
            },
        ),
    ]
)
def tbl_fixt(request):
    from opg_pipeline_builder.database import Database, DatabaseTable

    db_table, expected = request.param
    db_name, tbl_name = db_table.split(".")
    db = Database(db_name)

    return DatabaseTable(table_name=tbl_name, db=db), expected


@pytest.fixture
def db_config(tbl_fixt):
    from opg_pipeline_builder.validator import read_pipeline_config

    _, expected = tbl_fixt
    db_config = read_pipeline_config(expected["db_name"])
    return db_config.dict()


def test_db_tbl_name(tbl_fixt):
    tbl, expected = tbl_fixt
    assert expected["name"] == tbl.name


def test_db_tbl_db_name(tbl_fixt):
    tbl, expected = tbl_fixt
    assert expected["db_name"] == tbl.db_name


def test_db_tbl_config(tbl_fixt, db_config):
    tbl, expected = tbl_fixt
    tbl_config = db_config["tables"][expected["name"]]
    assert tbl.config == tbl_config


def test_db_tbl_transform(tbl_fixt, db_config):
    tbl, expected = tbl_fixt
    tbl_config = db_config["tables"][expected["name"]]
    config_transform = tbl_config["transform_type"]
    expected_transform = expected["transform_type"]
    if expected_transform not in ["default", "custom", "derived"]:
        with pytest.raises(
            ValueError,
            match=(
                "Transform type in config should be one of default, custom or derived"
            ),
        ):
            tbl.transform_type()

    else:
        transform_check1 = tbl.transform_type() == config_transform
        transform_check2 = tbl.transform_type() == expected_transform
        assert transform_check1 and transform_check2


def test_db_tbl_etl_stages(tbl_fixt, db_config):
    tbl, expected = tbl_fixt
    tbl_config = db_config["tables"][expected["name"]]
    tbl_etl_stages = list(tbl_config["etl_stages"].keys())
    expected_etl_stages = expected["etl_stages"]
    etl_check1 = tbl.etl_stages() == tbl_etl_stages
    etl_check2 = tbl.etl_stages() == expected_etl_stages
    assert etl_check1 and etl_check2


def test_db_tbl_file_formats(tbl_fixt, db_config):
    tbl, expected = tbl_fixt
    tbl_config = db_config["tables"][expected["name"]]
    tbl_formats = tbl_config["etl_stages"]
    expected_formats = expected["file_formats"]
    formats_check1 = tbl.table_file_formats() == tbl_formats
    formats_check2 = tbl.table_file_formats() == expected_formats
    assert formats_check1 and formats_check2


def test_db_tbl_paths(tbl_fixt):
    tbl, expected = tbl_fixt
    expected_paths = expected["table_paths"]
    assert tbl.table_data_paths() == expected_paths


def test_db_tbl_meta_paths(tbl_fixt):
    tbl, expected = tbl_fixt
    expected_meta_paths = expected["table_meta_paths"]
    full_meta_paths = {
        stage: meta_path for stage, meta_path in expected_meta_paths.items()
    }
    assert tbl.table_meta_paths() == full_meta_paths


def test_db_tbl_input_data(tbl_fixt):
    tbl, expected = tbl_fixt
    expected_meta_paths = expected["input_data"]
    assert tbl.input_data() == expected_meta_paths


def test_db_tbl_lint_config(tbl_fixt):
    tbl, expected = tbl_fixt
    expected_lint_config = expected["lint_config"]
    assert tbl.lint_config() == expected_lint_config


def test_db_tbl_transform_args(tbl_fixt):
    tbl, expected = tbl_fixt
    transform_type = expected["transform_type"]
    input = None if transform_type == "derived" else "raw_hist"
    output = "derived" if transform_type == "derived" else "curated"
    expected_tf_args = expected["transform_args"]
    assert (
        tbl.transform_args(input_stage=input, output_stage=output) == expected_tf_args
    )


def test_db_tbl_get_tbl_meta(tbl_fixt):
    tbl, expected = tbl_fixt
    meta_fps = expected["table_meta_paths"]
    for stage, fp in meta_fps.items():
        moj_meta = Metadata.from_json(fp)
        moj_meta.set_col_type_category_from_types()
        moj_meta_dict = moj_meta.to_dict()
        assert moj_meta_dict == tbl.get_table_metadata(stage=stage).to_dict()


def test_db_tbl_get_table_path(tbl_fixt):
    tbl, expected = tbl_fixt
    tbl_fps = expected["table_paths"]
    for stage, tbl_fp in tbl_fps.items():
        assert tbl_fp == tbl.get_table_path(stage=stage)


def test_db_table(db_fixt):
    from opg_pipeline_builder.database import DatabaseTable

    db, expected = db_fixt
    tables = expected["tables"]
    for table_name in tables:
        assert db.table(table_name) == DatabaseTable(table_name, db)


@pytest.mark.parametrize("specify_tables, spec", [(False, None), (True, 1), (True, -1)])
def test_db_lint_config(db_fixt, specify_tables, spec):
    db, expected = db_fixt
    exp_lint_config = deepcopy(expected["lint_config"])
    tbls = expected["tables"]
    test_tables = tbls[0:spec] if specify_tables else tbls
    tbl_arg = test_tables if specify_tables else None

    spec_tables_config = {}
    for table in test_tables:
        tf_args_tfs = expected["transform_args"]["transforms"]
        tbl_tf = tf_args_tfs[table]["transform_type"]
        if table in exp_lint_config["tables"].keys() and tbl_tf in [
            "default",
            "custom",
        ]:
            tbl_lint = exp_lint_config["tables"][table]
            tbl_meta_pth = tbl_lint["metadata"]
            tbl_lint["metadata"] = tbl_meta_pth
            spec_tables_config[table] = tbl_lint

    exp_lint_config["tables"] = spec_tables_config

    if len(test_tables) == 1:
        orig_log_bp = exp_lint_config["log-base-path"]
        new_log_bp = f"{orig_log_bp}{test_tables[0]}/"
        exp_lint_config["log-base-path"] = new_log_bp

    assert db.lint_config(tables=tbl_arg) == exp_lint_config


@pytest.mark.parametrize("specify_tables, spec", [(False, None), (True, 1), (True, -1)])
def test_db_tf_args(db_fixt, specify_tables, spec):
    db, expected = db_fixt
    exp_tf_args = deepcopy(expected["transform_args"])
    exp_tf_args["db"] = db.name
    tbls = expected["tables"]
    tbls_to_use = tbls[0:spec] if specify_tables else tbls
    tbl_arg = tbls_to_use if specify_tables else None
    exp_tf_args["tables"] = [tbl for tbl in tbls_to_use if tbl != "table3"]
    tfms = exp_tf_args["transforms"]

    spec_tbls_glue = {tbl: tfms[tbl] for tbl in tbls_to_use if tbl != "table3"}

    glue_inputs = {
        tbl: {"input": "raw_hist", "output": "curated"} for tbl in tbls_to_use
    }

    exp_tf_args["transforms"] = spec_tbls_glue

    assert db.transform_args(tbl_arg, **glue_inputs) == exp_tf_args
