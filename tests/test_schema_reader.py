from tempfile import NamedTemporaryFile
from typing import Any

import pandas as pd
import pytest
from arrow_pd_parser import writer
from mojap_metadata import Metadata

from tests.conftest import dummy_bucket, mock_get_file, set_up_s3


class TestSchemaReader:
    from opg_pipeline_builder.utils import schema_reader as srutils
    from opg_pipeline_builder.utils.constants import aws_region

    sr_utils = srutils
    aws_region = aws_region
    meta = Metadata.from_json("tests/data/meta_data/test/testdb/raw_hist/table2.json")
    data = pd.DataFrame.from_dict(
        {"my_int": [0, 1, 2, 3, 4], "hobby": ["jump", "hop", "skip", "walk", "run"]}
    )

    def helper(self, s3: Any, monkeypatch: pytest.MonkeyPatch, file_ext: str) -> str:
        monkeypatch.setattr(self.sr_utils, "S3FileSystem", mock_get_file)

        set_up_s3(s3)

        tmp_file = NamedTemporaryFile(suffix=file_ext)
        df = self.data
        meta = self.meta

        if file_ext == "snappy.parquet":
            writer.parquet.write(df, output_path=tmp_file.name, metadata=meta)
        else:
            writer.json.write(df, output_path=tmp_file.name, metadata=meta)

        s3_key = f"dummy_data.{file_ext}"
        s3.meta.client.upload_file(tmp_file.name, dummy_bucket, s3_key)

        return s3_key

    def test_repr(self) -> None:
        assert (
            str(self.sr_utils.SchemaReader())
            == f"""
        SchemaReader object:
            s3: pyarrow._s3fs.S3FileSystem, aws_region={self.aws_region}
        """
        )

    @pytest.mark.parametrize(
        "type, contains_field, expected",
        [
            (
                (
                    "list<struct<id:int64, description:string, "
                    "hobbies:list<struct<id:int64, title:string>>, other:string>>"
                ),
                False,
                [
                    {"id": "int64"},
                    {"description": "string"},
                    {"hobbies": [{"id": "int64"}, {"title": "string"}]},
                    {"other": "string"},
                ],
            ),
            (
                (
                    "field:list<struct<id:int64, description:string, "
                    "hobbies:list<struct<id:int64, title:string>>, other:string>>"
                ),
                True,
                {
                    "field": [
                        {"id": "int64"},
                        {"description": "string"},
                        {"hobbies": [{"id": "int64"}, {"title": "string"}]},
                        {"other": "string"},
                    ]
                },
            ),
        ],
    )
    def test_unpack_type(
        self, type: str, contains_field: bool, expected: list[dict[str, str | Any]]
    ) -> None:
        sr = self.sr_utils.SchemaReader
        assert sr._unpack_type(type, contains_field=contains_field) == expected

    @pytest.mark.parametrize(
        "type, expected",
        [
            (
                (
                    "id:int64, description:string, "
                    "hobbies:list<struct<id:int64, title:string>>, other:string"
                ),
                [
                    "id:int64",
                    "description:string",
                    "hobbies:list<struct<id:int64, title:string>>",
                    "other:string",
                ],
            )
        ],
    )
    def test_split_type(self, type: str, expected: list[str]) -> None:
        sr = self.sr_utils.SchemaReader
        assert sr._split_type(type) == expected

    @pytest.mark.parametrize(
        "col_type, meta_col_type, expected",
        [
            (
                (
                    "list<struct<id:int64, description:string, type:string, "
                    "hobbies:list<struct<id:int64, title:string>>, other:string>>"
                ),
                (
                    "list<struct<id:int64, description:string, "
                    "hobbies:list<struct<id:int64, type:string, title:string>>, "
                    "other:string>>"
                ),
                False,
            ),
            (
                (
                    "list<struct<id:int64, description:string, "
                    "hobbies:list<struct<id:int64, title:string>>, other:string>>"
                ),
                (
                    "list<struct<id:int64, description:string, "
                    "hobbies:list<struct<id:int64, type:string, title:string>>, "
                    "other:string>>"
                ),
                False,
            ),
            (
                (
                    "list<struct<id:int64, description:string, "
                    "hobbies:list<struct<id:int64, type:string, title:string>>, "
                    "other:string>>"
                ),
                (
                    "list<struct<id:int64, description:string, "
                    "hobbies:list<struct<id:int64, title:string>>, "
                    "other:string>>"
                ),
                True,
            ),
            (
                (
                    "list<struct<id:int64, description:string, "
                    "hobbies:list<struct<id:int64, type:string, title:string>>, "
                    "other:string>>"
                ),
                (
                    "list<struct<id:int64, description:string, "
                    "hobbies:list<struct<id:int64, type:string, title:string>>, "
                    "other:string>>"
                ),
                True,
            ),
        ],
    )
    def test_struct_validator(
        self, col_type: str, meta_col_type: str, expected: bool
    ) -> None:
        sr = self.sr_utils.SchemaReader
        assert sr._struct_validator(col_type, meta_col_type) == expected

    @pytest.mark.parametrize(
        "data_columns, meta_columns, expected",
        [
            (
                [
                    {
                        "name": "mystruct",
                        "type": (
                            "list<struct<id:int64, description:string, type:string, "
                            "hobbies:list<struct<id:int64, title:string>>, "
                            "other:string>>"
                        ),
                    },
                    {"name": "myint", "type": "int32"},
                ],
                [
                    {
                        "name": "mystruct",
                        "type": (
                            "list<struct<id:int64, description:string, "
                            "hobbies:list<struct<id:int64, type:string, title:string>>,"
                            " other:string>>"
                        ),
                    },
                    {"name": "myint", "type": "int32"},
                ],
                False,
            ),
            (
                [
                    {
                        "name": "mystruct",
                        "type": (
                            "list<struct<id:int64, description:string, "
                            "hobbies:list<struct<id:int64, title:string>>, "
                            "other:string>>"
                        ),
                    },
                    {
                        "name": "myint",
                        "type": "int32",
                    },
                ],
                [
                    {
                        "name": "mystruct",
                        "type": (
                            "list<struct<id:int64, description:string, "
                            "hobbies:list<struct<id:int64, type:string, title:string>>,"
                            " other:string>>"
                        ),
                    },
                    {
                        "name": "myint",
                        "type": "int32",
                    },
                ],
                False,
            ),
            (
                [
                    {
                        "name": "mystruct",
                        "type": (
                            "list<struct<id:int64, description:string, "
                            "hobbies:list<struct<id:int64, type:string, title:string>>,"
                            " other:string>>"
                        ),
                    },
                    {
                        "name": "myint",
                        "type": "int32",
                    },
                ],
                [
                    {
                        "name": "mystruct",
                        "type": (
                            "list<struct<id:int64, description:string, "
                            "hobbies:list<struct<id:int64, title:string>>, "
                            "other:string>>"
                        ),
                    },
                    {
                        "name": "myint",
                        "type": "int32",
                    },
                ],
                True,
            ),
            (
                [
                    {
                        "name": "mystruct",
                        "type": (
                            "list<struct<id:int64, description:string, "
                            "hobbies:list<struct<id:int64, type:string, title:string>>,"
                            " other:string>>"
                        ),
                    },
                ],
                [
                    {
                        "name": "mystruct",
                        "type": (
                            "list<struct<id:int64, description:string, "
                            "hobbies:list<struct<id:int64, title:string>>, "
                            "other:string>>"
                        ),
                    },
                    {
                        "name": "myint",
                        "type": "int32",
                    },
                ],
                False,
            ),
            (
                [
                    {
                        "name": "mystruct",
                        "type": (
                            "list<struct<id:int64, description:string, "
                            "hobbies:list<struct<id:int64, type:string, title:string>>,"
                            " other:string>>"
                        ),
                    },
                    {
                        "name": "myint",
                        "type": "int32",
                    },
                ],
                [
                    {
                        "name": "mystruct",
                        "type": (
                            "list<struct<id:int64, description:string, "
                            "hobbies:list<struct<id:int64, title:string>>, "
                            "other:string>>"
                        ),
                    },
                ],
                True,
            ),
            (
                [
                    {
                        "name": "mystruct",
                        "type": (
                            "list<struct<id:int64, description:string, "
                            "hobbies:list<struct<id:int64, type:string, title:string>>,"
                            " other:string>>"
                        ),
                    },
                    {
                        "name": "myint",
                        "type": "int32",
                    },
                ],
                [
                    {
                        "name": "mystruct",
                        "type": (
                            "list<struct<id:int64, description:string, "
                            "hobbies:list<struct<id:int64, type:string, title:string>>,"
                            " other:string>>"
                        ),
                    },
                    {
                        "name": "myint",
                        "type": "int32",
                    },
                ],
                True,
            ),
        ],
    )
    def test_validator(
        self,
        data_columns: list[dict[str, str | Any]],
        meta_columns: list[dict[str, str | Any]],
        expected: bool,
    ) -> None:
        sr = self.sr_utils.SchemaReader
        assert sr._validate(data_columns, meta_columns) == expected

    @pytest.mark.parametrize("file_ext", ["snappy.parquet", "json"])
    def test_read_schema(
        self, s3: Any, monkeypatch: pytest.MonkeyPatch, file_ext: str
    ) -> None:
        s3_key = self.helper(s3, monkeypatch, file_ext)
        sr = self.sr_utils.SchemaReader()
        meta = self.meta

        schema = sr.read_schema(f"s3://{dummy_bucket}/{s3_key}", moj_meta=True)

        meta_cols = [{"name": c["name"], "type": c["type"]} for c in meta.columns]
        schema_cols = [{"name": c["name"], "type": c["type"]} for c in schema.columns]

        assert meta_cols == schema_cols

    @pytest.mark.parametrize("file_ext", ["snappy.parquet", "json"])
    def test_check_schemas_match(
        self, s3: Any, monkeypatch: pytest.MonkeyPatch, file_ext: str
    ) -> None:
        s3_key = self.helper(s3, monkeypatch, file_ext)
        sr = self.sr_utils.SchemaReader()
        meta = self.meta
        s3_paths = [f"s3://{dummy_bucket}/{s3_key}"]
        assert sr.check_schemas_match(s3_paths, expected_meta=meta)
