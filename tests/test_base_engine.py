import os
import time
from datetime import timedelta
from itertools import chain

import pytest

from tests.conftest import dep_bucket, land_bucket, raw_hist_bucket, set_up_s3


class TestBaseEngineTransform:
    land_bucket = land_bucket
    raw_hist_bucket = raw_hist_bucket
    curated_bucket = dep_bucket

    def base_setup(self, test_files, upload_dir, table_name, partition=""):
        test_path = "tests/data/dummy_data/"
        table_partition = f"{table_name}/{partition}" if partition != "" else table_name

        full_paths = [os.path.join(test_path, test_file) for test_file in test_files]

        orig_paths = [
            os.path.join(upload_dir, table_partition, test_file)
            for test_file in test_files
        ]

        orig_pass_paths = [
            os.path.join(upload_dir, "pass", table_partition, test_file)
            for test_file in test_files
        ]

        iter = zip(full_paths, orig_paths, orig_pass_paths)

        return list(iter)

    def get_transform(self):
        from opg_pipeline_builder.transform_engines.base import BaseTransformEngine

        transform = BaseTransformEngine("testdb")
        return transform

    def test_base_db(self):
        from opg_pipeline_builder.database import Database

        base_tf = self.get_transform()
        assert base_tf.db == Database("testdb")

    def test_list_table_files(self, s3):
        set_up_s3(s3)
        s3_client = s3.meta.client
        transform = self.get_transform()
        env = transform.db.env
        db_name = transform.db.name
        stage_buckets = {"land_bucket", "raw_hist_bucket"}

        setup_table1 = self.base_setup(
            test_files=["dummy_data1.csv", "dummy_data2.csv"],
            upload_dir=f"dep/{env}/{db_name}",
            table_name="table1",
        )

        setup_table2 = self.base_setup(
            test_files=["dummy_data3.csv", "dummy_data4.csv"],
            upload_dir=f"dep/{env}/{db_name}",
            table_name="table2",
        )

        setup = [setup_table1, setup_table2]

        for setup_table in setup:
            loc_path, p1, p2 = setup_table[0]
            for stage in stage_buckets:
                p = p1 if "land" in stage else p2
                s3_client.upload_file(loc_path, getattr(self, stage), p)

        init_objects = chain.from_iterable(
            [
                s3_client.list_objects(Bucket=getattr(self, stage)).get("Contents")
                for stage in stage_buckets
            ]
        )

        mid_timestamps = [o["LastModified"] for o in list(init_objects)]

        mid_timestamp_min = min(mid_timestamps)
        mid_timestamp_max = max(mid_timestamps)
        mid_adj_timestamp = mid_timestamp_max + timedelta(seconds=1)
        prior_timestamp = mid_timestamp_min - timedelta(seconds=1)
        time.sleep((mid_adj_timestamp - prior_timestamp).total_seconds())

        for setup_table in setup:
            loc_path, p1, p2 = setup_table[1]
            for stage in stage_buckets:
                p = p1 if "land" in stage else p2
                s3_client.upload_file(loc_path, getattr(self, stage), p)

        sec_objects = chain.from_iterable(
            [
                s3_client.list_objects(Bucket=getattr(self, stage)).get("Contents")
                for stage in stage_buckets
            ]
        )

        post_timestamp = max(
            [o["LastModified"] for o in list(sec_objects)]
        ) + timedelta(seconds=1)

        for stage_bucket in stage_buckets:
            stage = "land" if "land" in stage_bucket else "raw_hist"
            path_index = 1 if stage == "land" else 2
            for i, table in enumerate(["table1", "table2"]):
                assert transform.utils.list_table_files(stage, table) == [
                    f"s3://{getattr(self, stage_bucket)}/{p[path_index]}"
                    for p in setup[i]
                ]

                assert transform.utils.list_table_files(
                    stage, table, modified_before=mid_adj_timestamp
                ) == [
                    [
                        f"s3://{getattr(self, stage_bucket)}/{p[path_index]}"
                        for p in setup[i]
                    ][0]
                ]

                assert transform.utils.list_table_files(
                    stage, table, modified_after=mid_adj_timestamp
                ) == [
                    [
                        f"s3://{getattr(self, stage_bucket)}/{p[path_index]}"
                        for p in setup[i]
                    ][1]
                ]

                assert (
                    transform.utils.list_table_files(
                        stage, table, modified_after=post_timestamp
                    )
                    == []
                )

    @pytest.mark.parametrize(
        "partition_map, expected",
        [
            (
                {
                    "mojap_file_land_timestamp=1664198347": ["dummy_data1.csv"],
                    "mojap_file_land_timestamp=1674198347": [
                        "dummy_data2.csv",
                        "dummy_data3.csv",
                    ],
                    "mojap_file_land_timestamp=1674198400": ["dummy_data4.csv"],
                },
                True,
            ),
            (
                {
                    "mojap_file_land_timestamp=1664198347": [
                        "dummy_data1.csv",
                        "dummy_data2.csv",
                        "dummy_data3.csv",
                        "dummy_data4.csv",
                    ]
                },
                True,
            ),
            (
                {
                    "gibberish=1664198347": [
                        "dummy_data1.csv",
                        "dummy_data2.csv",
                        "dummy_data3.csv",
                        "dummy_data4.csv",
                    ]
                },
                False,
            ),
            ({"mojap_file_land_timestamp=1664198347": []}, True),
        ],
    )
    def test_list_partitions_and_cleanup(self, s3, partition_map, expected):
        set_up_s3(s3)
        s3_client = s3.meta.client
        transform = self.get_transform()
        env = transform.db.env
        db_name = transform.db.name
        stage_buckets = {"land_bucket", "raw_hist_bucket"}
        prts = [prt for prt, files in partition_map.items() if files]

        i = 0
        setup_table1 = []
        for prt, prt_files in partition_map.items():
            if i == 0:
                prt0 = prt

            if prt_files:
                setup_table1 = [
                    *setup_table1,
                    *self.base_setup(
                        test_files=prt_files,
                        upload_dir=f"dep/{env}/{db_name}",
                        table_name="table1",
                        partition=prt,
                    ),
                ]

        setup_table2 = self.base_setup(
            test_files=["dummy_data1.csv", "dummy_data2.csv"],
            upload_dir=f"dep/{env}/{db_name}",
            table_name="table2",
            partition=prt0,
        )

        setup = [setup_table1, setup_table2]

        for setup_table in setup:
            for loc_path, p1, p2 in setup_table:
                for stage in stage_buckets:
                    p = p1 if "land" in stage else p2
                    s3_client.upload_file(loc_path, getattr(self, stage), p)

        if expected:
            assert set(transform.utils.list_partitions("table1", "land")) == set(prts)
        else:
            with pytest.raises(ValueError):
                transform.utils.list_partitions("table1", "land")

        transform.utils.cleanup_partitions(
            f"s3://{self.land_bucket}/dep/{env}/{db_name}/table1", prts
        )
        assert (
            s3_client.list_objects(
                Bucket=self.land_bucket, Prefix=f"dep/{env}/{db_name}/table1"
            ).get("Contents")
            is None
        )

    @pytest.mark.parametrize(
        "stages_map, expected",
        [
            (
                {
                    "input": {
                        "name": "land",
                        "bucket_name": "land_bucket",
                        "partitions": {
                            "mojap_file_land_timestamp=1674198347": [
                                "dummy_data2.csv",
                                "dummy_data3.csv",
                            ],
                            "mojap_file_land_timestamp=1674198400": ["dummy_data4.csv"],
                        },
                    },
                    "output": {
                        "name": "raw_hist",
                        "bucket_name": "raw_hist_bucket",
                        "partitions": {
                            "mojap_file_land_timestamp=1664198347": ["dummy_data1.csv"],
                            "mojap_file_land_timestamp=1674198347": [
                                "dummy_data2.csv",
                                "dummy_data3.csv",
                            ],
                        },
                    },
                },
                ["mojap_file_land_timestamp=1674198400"],
            ),
            (
                {
                    "input": {
                        "name": "raw_hist",
                        "bucket_name": "raw_hist_bucket",
                        "partitions": {
                            "mojap_file_land_timestamp=1664198347": ["dummy_data1.csv"],
                            "mojap_file_land_timestamp=1674198348": [
                                "dummy_data2.csv",
                                "dummy_data3.csv",
                            ],
                            "mojap_file_land_timestamp=1674198400": ["dummy_data4.csv"],
                        },
                    },
                    "output": {
                        "name": "curated",
                        "bucket_name": "curated_bucket",
                        "partitions": {
                            "mojap_file_land_timestamp=1664198347": ["dummy_data1.csv"],
                        },
                    },
                },
                [
                    "mojap_file_land_timestamp=1674198400",
                    "mojap_file_land_timestamp=1674198348",
                ],
            ),
        ],
    )
    def test_list_unprocessed_partitions(self, s3, stages_map, expected):
        set_up_s3(s3)
        s3_client = s3.meta.client
        transform = self.get_transform()
        env = transform.db.env
        db_name = transform.db.name

        for _, map in stages_map.items():
            stage = map["name"]
            bucket = map["bucket_name"]
            setup = []
            upload_dir = (
                f"dep/{env}/{db_name}"
                if stage != "curated"
                else f"{env}/{db_name}/curated"
            )
            for prt, files in map["partitions"].items():
                setup = [
                    *setup,
                    *self.base_setup(
                        test_files=files,
                        upload_dir=upload_dir,
                        table_name="table1",
                        partition=prt,
                    ),
                ]

            for lp, p1, p2 in setup:
                p = p2 if stage == "raw_hist" else p1
                s3_client.upload_file(lp, getattr(self, bucket), p)

        input_stage = stages_map["input"]["name"]
        output_stage = stages_map["output"]["name"]
        assert (
            transform.utils.list_unprocessed_partitions(
                "table1", input_stage=input_stage, output_stage=output_stage
            )
            == expected
        )

    @pytest.mark.parametrize(
        "stages_map, tables, expected",
        [
            (
                {
                    "input": {
                        "name": "land",
                        "bucket_name": "land_bucket",
                        "partitions": {
                            "mojap_file_land_timestamp=1674198347": [
                                "dummy_data2.csv",
                                "dummy_data3.csv",
                            ],
                            "mojap_file_land_timestamp=1674198400": ["dummy_data4.csv"],
                        },
                    },
                    "output": {
                        "name": "raw_hist",
                        "bucket_name": "raw_hist_bucket",
                        "partitions": {
                            "mojap_file_land_timestamp=1664198347": ["dummy_data1.csv"],
                            "mojap_file_land_timestamp=1674198347": [
                                "dummy_data2.csv",
                                "dummy_data3.csv",
                            ],
                        },
                    },
                },
                ["table1", "table2"],
                {"table1": ["mojap_file_land_timestamp=1674198400"], "table2": []},
            ),
            (
                {
                    "input": {
                        "name": "raw_hist",
                        "bucket_name": "raw_hist_bucket",
                        "partitions": {
                            "mojap_file_land_timestamp=1664198347": ["dummy_data1.csv"],
                            "mojap_file_land_timestamp=1674198348": [
                                "dummy_data2.csv",
                                "dummy_data3.csv",
                            ],
                            "mojap_file_land_timestamp=1674198400": ["dummy_data4.csv"],
                        },
                    },
                    "output": {
                        "name": "curated",
                        "bucket_name": "curated_bucket",
                        "partitions": {
                            "mojap_file_land_timestamp=1664198347": ["dummy_data1.csv"],
                        },
                    },
                },
                ["table1"],
                {
                    "table1": [
                        "mojap_file_land_timestamp=1674198400",
                        "mojap_file_land_timestamp=1674198348",
                    ]
                },
            ),
        ],
    )
    def test_transform_partitions(self, s3, stages_map, tables, expected):
        set_up_s3(s3)
        s3_client = s3.meta.client
        transform = self.get_transform()
        env = transform.db.env
        db_name = transform.db.name

        for _, map in stages_map.items():
            stage = map["name"]
            bucket = map["bucket_name"]
            setup = []
            upload_dir = (
                f"dep/{env}/{db_name}"
                if stage != "curated"
                else f"{env}/{db_name}/curated"
            )
            for prt, files in map["partitions"].items():
                setup = [
                    *setup,
                    *self.base_setup(
                        test_files=files,
                        upload_dir=upload_dir,
                        table_name="table1",
                        partition=prt,
                    ),
                ]

            for lp, p1, p2 in setup:
                p = p2 if stage == "raw_hist" else p1
                s3_client.upload_file(lp, getattr(self, bucket), p)

        if "table2" in tables:
            stage_details = [
                (s["name"], s["bucket_name"]) for _, s in stages_map.items()
            ]
            prts_to_upload = stages_map["input"]["partitions"]
            for stage, bucket in stage_details:
                upload_dir = (
                    f"dep/{env}/{db_name}"
                    if stage != "curated"
                    else f"{env}/{db_name}/curated"
                )
                setup = []
                for prt, files in prts_to_upload.items():
                    setup = [
                        *setup,
                        *self.base_setup(
                            test_files=files,
                            upload_dir=upload_dir,
                            table_name="table2",
                            partition=prt,
                        ),
                    ]

                for lp, p1, p2 in setup:
                    p = p2 if stage == "raw_hist" else p1
                    s3_client.upload_file(lp, getattr(self, bucket), p)

        stages_arg = {k: v["name"] for k, v in stages_map.items()}

        assert transform.utils.transform_partitions(tables, stages_arg) == expected

    @pytest.mark.parametrize(
        "table_name, stages, expected",
        [
            (
                "table1",
                {"input": "land", "output": "curated"},
                (
                    "s3://mojap-land/dep/test/testdb/table1",
                    "s3://alpha-dep-etl/test/testdb/curated/table1",
                    "default",
                ),
            ),
            (
                "table2",
                {"input": "raw_hist", "output": "curated"},
                (
                    "s3://mojap-raw-hist/dep/test/testdb/pass/table2",
                    "s3://alpha-dep-etl/test/testdb/curated/table2",
                    "custom",
                ),
            ),
            (
                "table3",
                {"input": None, "output": "derived"},
                (
                    [
                        (
                            "testdb",
                            "table1",
                            "s3://mojap-raw-hist/dep/test/testdb/pass/table1",
                        ),
                        (
                            "testdb",
                            "table2",
                            "s3://mojap-raw-hist/dep/test/testdb/pass/table2",
                        ),
                    ],
                    "s3://alpha-dep-etl/test/testdb/derived/table3",
                    "derived",
                ),
            ),
        ],
    )
    def test_tf_args(self, table_name, stages, expected):
        transform = self.get_transform()
        assert transform.utils.tf_args(table_name=table_name, stages=stages) == expected

    def test_get_secrets_valid(self) -> None:
        os.environ["dummy"] = "fjg95ihi94wg"
        transform = self.get_transform()
        assert transform.utils.get_secrets("dummy") == "fjg95ihi94wg"

    def test_get_secrets_invalid(self) -> None:
        os.environ["valid"] = "fjg95ihi94wg"
        transform = self.get_transform()
        with pytest.raises(ValueError) as error:
            transform.utils.get_secrets("invalid")
        assert (
            str(error.value)
            == "No value found for secret: 'invalid'. The secret needs to be added to the DAG manifest file and AWS secret manager."  # pragma: allowlist secret
        )
