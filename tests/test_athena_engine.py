import os
import re
from copy import deepcopy
from datetime import datetime
from functools import partial
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory

import awswrangler as wr
import boto3
import duckdb
import pandas as pd
import pyarrow.parquet as pq
import pytest
import sqlglot
from arrow_pd_parser import reader, writer
from mojap_metadata.converters.glue_converter import GlueConverter, GlueTable
from moto import mock_aws

from opg_pipeline_builder.utils.constants import get_full_db_name
from tests.conftest import mock_get_file, set_up_s3


class DummyAthenaResponse:
    def __init__(self, status, output_location):
        payload_status = "SUCCEEDED" if status else "FAILED"
        self.raw_payload = {"Status": {"State": payload_status}}
        self.output_location = output_location
        self.manifest_location = ""


class TestAthenaTransformEngine:
    db_name = "athenadb"
    table_name = "table1"
    raw_data_file = "tests/data/dummy_data/dummy_data1.csv"
    output_stage = "curated"
    input_stage = "raw"
    derived_table_name = "table2"
    temp_table_name = "table2_copy"
    temp_table_animal = "chicken"

    def import_athena(self):
        import opg_pipeline_builder.transform_engines.athena as athena

        return athena

    def do_nothing(*args, **kwargs): ...

    def mock_unload(
        self,
        sql,
        path,
        database,
        partitioned_by,
        status,
        output_meta,
        use_glue_meta=True,
        alt_database=None,
        **kwargs,
    ):
        from opg_pipeline_builder.utils.utils import (extract_mojap_partition,
                                                      extract_mojap_timestamp)

        athena = self.import_athena()
        transform = athena.AthenaTransformEngine(self.db_name)
        db = transform.db
        primary_partition = db.primary_partition_name()
        _ = kwargs

        duckdb_sql = sqlglot.transpile(sql, read="presto", write="duckdb")

        tables = [table for table in sqlglot.parse_one(sql).find_all(sqlglot.exp.Table)]

        table = [
            id.name
            for id in tables[0].find_all(sqlglot.exp.Identifier)
            if id.name != database
        ][0]

        glue_client = boto3.client("glue")

        database_to_use = database if alt_database is None else alt_database

        resp = glue_client.get_table(DatabaseName=database_to_use, Name=table)

        glue_s3_location = resp.get("Table").get("StorageDescriptor").get("Location")

        glue_table = GlueTable()
        glue_meta = glue_table.generate_to_meta(database=database_to_use, table=table)
        if primary_partition in glue_meta.partitions:
            glue_meta.remove_column(primary_partition)

        data_paths = wr.s3.list_objects(glue_s3_location)

        for p in data_paths:
            if database_to_use in os.listdir():
                os.remove(database_to_use)

            con = duckdb.connect(database=database_to_use)
            tmp_file = NamedTemporaryFile(suffix=".snappy.parquet")
            _ = wr.s3.download(path=p, local_file=tmp_file.name)

            df = reader.read(  # noqa: F841
                tmp_file.name, metadata=glue_meta if use_glue_meta else None
            )
            prt = extract_mojap_timestamp(
                extract_mojap_partition(p, timestamp_partition_name=primary_partition)
            )

            con.execute(
                f"""
                CREATE TABLE {table} AS
                SELECT
                    *,
                    CAST(
                        {prt} AS INT
                    ) AS {primary_partition}
                FROM df
                """
            )

            con.execute(re.sub(f"{database_to_use}.{table}", table, duckdb_sql[0]))
            final_table = con.arrow()

            tmp = TemporaryDirectory()
            pq.write_to_dataset(
                final_table,
                root_path=tmp.name,
                partition_cols=partitioned_by,
                compression="SNAPPY",
            )

            files = [f.as_posix() for f in Path(tmp.name).rglob("*") if f.is_file()]
            for fp in files:
                tmp_df = reader.read(fp, metadata=output_meta)
                snappy_fp = fp.replace(".parquet", ".snappy.parquet")
                writer.write(tmp_df, output_path=snappy_fp, metadata=output_meta)
                key = snappy_fp.replace(tmp.name, "")
                s3_path = f"{path}{key}"
                wr.s3.upload(local_file=snappy_fp, path=s3_path)

        if database in os.listdir():
            os.remove(database)

        return DummyAthenaResponse(status, path)

    def mock_pydb_read_sql_queries(self, sql, prt):
        return {"unique_prts": [prt]}

    def mock_pydb_create_temp_table(
        self, sql, table_name, con, prt, primary_partition, s3_path
    ):
        tmp_file = NamedTemporaryFile(suffix=".snappy.parquet")
        _ = wr.s3.download(path=s3_path, local_file=tmp_file.name)
        mock_df = reader.read(tmp_file.name)  # noqa: F841

        duckdb_sql = sqlglot.transpile(sql, read="presto", write="duckdb")

        augmented_sql = f"CREATE TABLE {table_name} AS " + re.sub(
            "FROM [a-zA-Z0-9_]+\\.[a-zA-Z0-9]+", "FROM mocked_table", duckdb_sql[0]
        )

        con.execute(
            f"""
            CREATE TABLE mocked_table AS
            SELECT
                *,
                CAST(
                    {prt} AS INT
                ) AS {primary_partition}
            FROM mock_df
            """
        )

        con.execute(augmented_sql)

    def setup_data(self, s3, stage="input"):
        athena = self.import_athena()
        transform = athena.AthenaTransformEngine(self.db_name)
        db = transform.db
        partition_name = db.primary_partition_name()
        table_name = self.table_name
        stage = self.input_stage if stage == "input" else self.output_stage
        set_up_s3(s3)

        table = db.table(table_name)
        paths = table.table_data_paths()
        table_meta = table.get_table_metadata(stage)
        if partition_name in table_meta.column_names:
            table_meta.remove_column(partition_name)

        table_meta.partitions = []

        df = reader.read(self.raw_data_file, metadata=table_meta)
        tmp = NamedTemporaryFile(suffix=".snappy.parquet")
        writer.write(df, tmp.name, metadata=table_meta)
        timestamp = int(datetime.now().timestamp())

        wr.s3.upload(
            tmp.name,
            os.path.join(
                paths[stage],
                f"{partition_name}={timestamp}",
                f"{Path(self.raw_data_file).stem}.snappy.parquet",
            ),
        )

        return athena, timestamp

    @pytest.mark.xfail
    @mock_aws
    @pytest.mark.parametrize("status", [True, False])
    def test_run(self, s3, monkeypatch, status):
        import opg_pipeline_builder.utils.schema_reader as sr
        from opg_pipeline_builder.utils.constants import etl_stages

        monkeypatch.setattr(sr, "S3FileSystem", mock_get_file)

        athena, _ = self.setup_data(s3)
        transform = athena.AthenaTransformEngine("athenadb")
        db = transform.db
        table = db.table(self.table_name)

        output_meta = table.get_table_metadata(self.output_stage)
        copied_meta = deepcopy(output_meta)
        output_partitions = output_meta.partitions
        for prt in output_partitions:
            output_meta.remove_column(prt)

        # monkeypatch.setattr(athena, "SchemaReader", sr.SchemaReader)
        mock_unload_partial = partial(
            self.mock_unload, status=status, output_meta=output_meta
        )

        monkeypatch.setattr(athena.wr.athena, "unload", mock_unload_partial)
        monkeypatch.setattr(athena.wr.athena, "repair_table", self.do_nothing)

        cp = transform.db.table(table.name).table_data_paths()[self.output_stage]
        temp_path = cp
        for stage in etl_stages:
            temp_path = temp_path.replace(stage, "temp")

        if status:
            transform.run(
                stages={"input": self.input_stage, "output": self.output_stage},
                tables=[table.name],
            )

            primary_partition = db.primary_partition_name()
            copied_meta.remove_column(primary_partition)

            files = wr.s3.list_objects(path=cp)

            for i, file in enumerate(files):
                tmp = NamedTemporaryFile(suffix=".snappy.parquet")
                wr.s3.download(file, tmp.name)
                file_df = reader.read(tmp.name, metadata=output_meta)
                original_prts = [
                    prt for prt in copied_meta.partitions if prt != primary_partition
                ]
                for prt in original_prts:
                    prt_type = copied_meta.get_column(prt).get("type")
                    prt_reg = "[0-9]{10}" if "int" in prt_type else "[a-zA-Z]*"
                    prt_substr = re.search(f"{prt}={prt_reg}/", file)[0]
                    prt_val_pre = re.sub(f"{prt}=|/", "", prt_substr)
                    prt_val = int(prt_val_pre) if "int" in prt_type else prt_val_pre
                    file_df[prt] = prt_val
                if i == 0:
                    df = file_df.copy()
                else:
                    df = pd.concat([df, file_df], ignore_index=True, sort=False)

            orig_df = reader.read(self.raw_data_file, metadata=copied_meta)

            df_dict = (
                df.sort_values(by=copied_meta.column_names, ascending=True)
                .sort_index(axis=1)
                .reset_index(drop=True)
                .to_dict()
            )

            orig_df_dict = (
                orig_df.sort_values(by=copied_meta.column_names, ascending=True)
                .sort_index(axis=1)
                .reset_index(drop=True)
                .to_dict()
            )

            assert df_dict == orig_df_dict
            assert (
                len(
                    set(
                        transform.utils.list_partitions(
                            table.name, stage=self.output_stage
                        )
                    )
                )
                == 1
            )

        else:
            with pytest.raises(Exception):
                transform.run(
                    stages={"input": self.input_stage, "output": self.output_stage},
                    tables=[table.name],
                )
            assert len(wr.s3.list_objects(cp)) == 0
            assert len(wr.s3.list_objects(temp_path)) == 0

    def test_create_tmp_table(self, s3, monkeypatch):
        athena, prt = self.setup_data(s3, stage="output")
        con = duckdb.connect(database="__temp__")

        transform = athena.AthenaTransformEngine(self.db_name)
        db = transform.db
        partition_name = db.primary_partition_name()
        table_name = self.derived_table_name

        input_table = db.table(self.table_name)

        table_paths = input_table.table_data_paths()
        data_path = os.path.join(
            table_paths[self.output_stage],
            f"{partition_name}={prt}",
            f"{Path(self.raw_data_file).stem}.snappy.parquet",
        )

        mock_create_temp_table = partial(
            self.mock_pydb_create_temp_table,
            prt=prt,
            primary_partition=partition_name,
            s3_path=data_path,
            con=con,
        )
        monkeypatch.setattr(athena.pydb, "create_temp_table", mock_create_temp_table)

        transform._create_temporary_tables(table_name, snapshot_timestamps=str(prt))

        con.execute(f"SELECT * FROM {self.temp_table_name}")

        tmp_df = con.df()
        os.remove("__temp__")

        assert tmp_df.animal.unique() == self.temp_table_animal

    @pytest.mark.xfail
    @mock_aws
    @pytest.mark.parametrize("status", [True, False])
    def test_run_derived(self, s3, monkeypatch, status):
        import pydbtools.utils as pydb_utils

        import opg_pipeline_builder.utils.schema_reader as sr
        from opg_pipeline_builder.utils.constants import etl_stages

        monkeypatch.setattr(sr, "S3FileSystem", mock_get_file)
        monkeypatch.setattr(
            pydb_utils, "get_database_name_from_userid", lambda _: "__temp__"
        )
        con = duckdb.connect(database="__temp_derived__")  # noqa: F841

        athena, timestamp = self.setup_data(s3, stage="output")
        transform = athena.AthenaTransformEngine("athenadb")
        db = transform.db
        primary_partition = db.primary_partition_name()
        table = db.table(self.derived_table_name)

        output_meta = table.get_table_metadata("derived")
        copied_meta = deepcopy(output_meta)
        output_partitions = output_meta.partitions
        for prt in output_partitions:
            output_meta.remove_column(prt)

        # monkeypatch.setattr(athena, "SchemaReader", sr.SchemaReader)
        mock_unload_partial = partial(
            self.mock_unload,
            status=status,
            output_meta=output_meta,
            use_glue_meta=False,
            alt_database=get_full_db_name(),
        )

        mock_read_sql_queries = partial(self.mock_pydb_read_sql_queries, prt=timestamp)

        monkeypatch.setattr(athena.pydb, "create_temp_table", self.do_nothing)
        monkeypatch.setattr(athena.wr.athena, "unload", mock_unload_partial)
        monkeypatch.setattr(athena.wr.athena, "repair_table", self.do_nothing)
        monkeypatch.setattr(athena.pydb, "read_sql_queries", mock_read_sql_queries)

        dp = transform.db.table(table.name).table_data_paths()["derived"]
        temp_path = dp
        for stage in etl_stages:
            temp_path = temp_path.replace(stage, "temp")

        glue_client = boto3.client("glue")
        output_table = db.table(self.table_name)
        output_meta = output_table.get_table_metadata(self.output_stage)
        output_path = output_table.table_data_paths()[self.output_stage]
        wr.catalog.create_database(get_full_db_name())
        gc = GlueConverter()

        spec = gc.generate_from_meta(
            output_meta, database_name=get_full_db_name(), table_location=output_path
        )

        glue_client.create_table(**spec)

        if status:
            transform.run_derived(
                tables=[table.name], jinja_args={"database_name": get_full_db_name()}
            )

            copied_meta.remove_column(primary_partition)

            files = wr.s3.list_objects(path=dp)

            for i, file in enumerate(files):
                tmp = NamedTemporaryFile(suffix=".snappy.parquet")
                wr.s3.download(file, tmp.name)
                file_df = reader.read(tmp.name)
                original_prts = [
                    prt for prt in copied_meta.partitions if prt != primary_partition
                ]
                for prt in original_prts:
                    prt_type = copied_meta.get_column(prt).get("type")
                    prt_reg = "[0-9]{10}" if "int" in prt_type else "[a-zA-Z]*"
                    prt_substr = re.search(f"{prt}={prt_reg}/", file)[0]
                    prt_val_pre = re.sub(f"{prt}=|/", "", prt_substr)
                    prt_val = int(prt_val_pre) if "int" in prt_type else prt_val_pre
                    file_df[prt] = prt_val
                if i == 0:
                    df = file_df.copy()
                else:
                    df = pd.concat([df, file_df], ignore_index=True, sort=False)

            assert (
                len(set(transform.utils.list_partitions(table.name, stage="derived")))
                == 1
            )
            assert set(df.animal) == {"chicken"}

        else:
            with pytest.raises(Exception):
                transform.run_derived(tables=[table.name])
            assert len(wr.s3.list_objects(dp)) == 0
            assert len(wr.s3.list_objects(temp_path)) == 0
