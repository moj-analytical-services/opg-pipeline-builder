import base64
import json
import os
from pathlib import Path

import awswrangler as wr
import pytest
from dataengineeringutils3.s3 import s3_path_to_bucket_key

from tests.conftest import mock_get_file


class TestDataLinterEngine:
    db_name = "testdb"
    table_name = "table1"
    input_stage = "land"
    output_stage = "raw-hist"
    land_data_path = "tests/data/dummy_data"
    data_ext = ".csv"

    def get_linter(self):
        import src.transform_engines.data_linter as linter

        return linter

    @pytest.mark.parametrize(
        "mp_args, expected",
        [
            (None, True),
            ({"enable": "local"}, True),
            ({"enable": "pod"}, False),
            ({"enable": "gibberish"}, False),
            ({"total_workers": 10, "current_worker": 1, "close_status": False}, False),
            ({"enable": "pod", "total_workers": 15}, True),
            (
                {
                    "enable": "pod",
                    "total_workers": 5,
                    "current_worker": 3,
                    "temp_staging": True,
                },
                True,
            ),
            ({"enable": "pod", "total_workers": 28, "close_status": True}, True),
            ({"enable": "pod", "total_workers": 7, "current_worker": 8}, False),
        ],
    )
    def test_validate_mp_args(self, mp_args, expected):
        linter = self.get_linter()
        linter_engine = linter.DataLinterTransformEngine
        if expected:
            assert linter_engine._validate_mp_args(mp_args) is None
        else:
            error = ValueError if "enable" in mp_args else KeyError
            with pytest.raises(error):
                linter_engine._validate_mp_args(mp_args)

    @pytest.mark.parametrize(
        "mps_list, dag_run_id, dag_interval_end",
        [
            ([], None, None),
            (
                [
                    {
                        "enable": "pod",
                        "total_workers": 2,
                        "close_status": False,
                        "temp_staging": True,
                    },
                    {
                        "enable": "pod",
                        "total_workers": 2,
                        "current_worker": 0,
                        "close_status": False,
                        "temp_staging": True,
                    },
                    {
                        "enable": "pod",
                        "total_workers": 2,
                        "current_worker": 1,
                        "close_status": False,
                        "temp_staging": True,
                    },
                    {
                        "enable": "pod",
                        "total_workers": 2,
                        "close_status": True,
                        "temp_staging": True,
                    },
                ],
                "manual__2022-09-12T05:00:00+00:00",
                "",
            ),
        ],
    )
    def test_run(self, mps_list, dag_run_id, dag_interval_end, monkeypatch, s3):
        import pyarrow.fs as fs

        monkeypatch.setattr(fs, "S3FileSystem", mock_get_file)

        from opg_pipeline_builder.utils.constants import project_root
        from opg_pipeline_builder.utils.utils import remove_lint_filestamp

        linter = self.get_linter()
        transform = linter.DataLinterTransformEngine(self.db_name)
        db = transform.db
        table = db.table(self.table_name)
        input_path = table.table_data_paths()[self.input_stage]
        output_path = table.table_data_paths()[self.output_stage]

        lb, lk = s3_path_to_bucket_key(input_path)
        rhb, _ = s3_path_to_bucket_key(output_path)

        buckets = [lb, rhb]

        for bucket in buckets:
            s3.meta.client.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
            )

        dummy_data_paths = os.listdir(os.path.join(project_root, self.land_data_path))

        for i, path in enumerate(dummy_data_paths):
            s3.meta.client.upload_file(
                os.path.join(project_root, self.land_data_path, path),
                lb,
                os.path.join(lk, table.name, dummy_data_paths[i]),
            )

        if mps_list:
            for mps in mps_list:
                mps_bts = json.dumps(mps).encode("ascii")
                mps_ec = base64.b64encode(mps_bts).decode("ascii")

                os.environ["MULTI_PROC_ENV"] = mps_ec
                if dag_run_id is not None:
                    os.environ["DAG_RUN_ID"] = dag_run_id
                if dag_interval_end is not None:
                    os.environ["DAG_INTERVAL_END"] = dag_interval_end

                from opg_pipeline_builder.utils.constants import get_dag_timestamp

                transform.run(tables=[table.name], stage=self.output_stage)

                del os.environ["MULTI_PROC_ENV"]
                if "DAG_RUN_ID" in os.environ or "DAG_INTERVAL_END" in os.environ:
                    if mps.get("close_status", False) and mps.get(
                        "temp_staging", False
                    ):
                        prts = set(
                            transform.utils.list_partitions(
                                table.name,
                                stage=self.output_stage,
                                extract_timestamp=True,
                            )
                        )
                        assert prts == {get_dag_timestamp()}
                    os.environ.pop("DAG_RUN_ID", None)
                    os.environ.pop("DAG_INTERVAL_END", None)
        else:
            transform.run(tables=[table.name], stage=self.output_stage)

        processed_files = wr.s3.list_objects(output_path)
        filenames = [
            Path(remove_lint_filestamp(f, file_ext=self.data_ext)).stem
            for f in processed_files
        ]
        original_filenames = [Path(f).stem for f in os.listdir(self.land_data_path)]

        assert sorted(filenames) == sorted(original_filenames)
