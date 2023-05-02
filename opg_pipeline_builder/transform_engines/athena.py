import logging
import os
from copy import deepcopy
from typing import Dict, List, Optional, Union

import awswrangler as wr
import boto3
import pydbtools as pydb
from mojap_metadata import Metadata
from mojap_metadata.converters.glue_converter import GlueConverter

from ..database import Database
from ..utils.constants import (
    etl_stages,
    get_end_date,
    get_full_db_name,
    get_source_tbls,
    get_start_date,
)
from ..utils.schema_reader import SchemaReader
from ..utils.utils import extract_mojap_timestamp, s3_bulk_copy
from .base import BaseTransformEngine

_logger: logging.Logger = logging.getLogger(__name__)


class AthenaTransformEngine(BaseTransformEngine):
    """
    sql_table_filter: Optional[bool] = False
        Optional arg whether to use the given table's name to filter
        shared sql tables rather than using the first result.

    **jinja_args
        Optional jinja args to pass onto the pydbtools SQL call.
    """

    sql_table_filter: Optional[bool] = False
    db_search_limit: Optional[Union[int, None]] = None
    jinja_args: Optional[Union[dict, None]] = None

    def _create_shared_tmp_tables(
        self, tf_types: List[str], snapshot_timestamps: str, **jinja_args
    ) -> None:
        """Create db shared temp tables

        Creates shared temporary tables for the database, as
        specified in the database config, for the snapshot
        timestamps given. Timestamps should be a string of the
        form "timestamp1,timestamp2,timestamp3,....".

        Params
        ------
        tf_types: List[str]
            List of table transform types (e.g. default, custom)

        snapshot_timestamps: str
            Timestamps should be a string of the
            form "timestamp1,timestamp2,timestamp3,....".

        **jinja_args
            Additional jinja args to pass onto pydbtools.

        Return
        ------
        None
        """
        db = self.db
        primary_partition = db.primary_partition_name()
        db_name = get_full_db_name(db_name=db.name, env=db.env)

        shared_sql = db.shared_sql_paths(tf_types)

        for sql_tbl, sql_path in shared_sql:
            try:
                check_prts_sql = pydb.render_sql_template(
                    """
                    SELECT DISTINCT({{ primary_partition }}) as distinct_prts
                    FROM __temp__.{{ sql_tbl }}
                    ORDER BY {{ primary_partition }} DESC
                    """,
                    {"sql_tbl": sql_tbl, "primary_partition": primary_partition},
                )
                check_prts = pydb.read_sql_queries(check_prts_sql)
                check_prts_str = [str(prt) for prt in check_prts["distinct_prts"]]
                check_prts_str.sort(reverse=True)
                check_prts_arg = ",".join(check_prts_str)
                delete_table_check = check_prts_arg != snapshot_timestamps
                tmp_tbl_exists = True

            except wr.exceptions.QueryFailed:
                delete_table_check = False
                tmp_tbl_exists = False

            if delete_table_check:
                pydb.delete_table_and_data(database="__temp__", table=sql_tbl)

            create_tmp_tbl_check = delete_table_check or tmp_tbl_exists is False
            if create_tmp_tbl_check:
                sql = pydb.get_sql_from_file(
                    sql_path,
                    jinja_args={
                        "database_name": db_name,
                        "environment": db.env,
                        "snapshot_timestamps": snapshot_timestamps,
                        "github_tag": os.environ["GITHUB_TAG"],
                        "primary_partition": primary_partition,
                        **jinja_args,
                    },
                )

                pydb.create_temp_table(sql, table_name=sql_tbl)

                count_template = pydb.render_sql_template(
                    "SELECT COUNT(*) as count FROM __temp__.{{ sql_tbl }}",
                    jinja_args={"sql_tbl": sql_tbl},
                )

                tbl_check = pydb.read_sql_query(count_template)

                if tbl_check["count"][0] == 0:
                    raise ValueError(
                        f"""
                        No data exists in {sql_tbl} for
                        snapshot {snapshot_timestamps}
                        """
                    )

    def _create_temporary_tables(self, table_name: str, **jinja_args):
        """Create temp tables for the given db table

        Creates temporary tables for the given table as specified
        in the db config. jinja_args are passed onto pydbtools.

        Params
        ------
        table_name: str
            Name of the table to create temp tables for.

        jinja_args:
            jinja args to pass to the underlying SQL
            template.

        Return
        ------
        None
        """
        db = self.db
        db_name = db.name
        table = db.table(table_name)
        primary_partition = db.primary_partition_name()
        tbl_tmp_sql = table.table_sql_paths(type="temp")

        for sql_tmp_tbl, sql_tpt in tbl_tmp_sql:
            tbl_sql = pydb.get_sql_from_file(
                sql_tpt,
                jinja_args={
                    "database_name": db_name,
                    "environment": db.env,
                    "github_tag": os.environ["GITHUB_TAG"],
                    "primary_partition": primary_partition,
                    **jinja_args,
                },
            )

            pydb.create_temp_table(tbl_sql, table_name=sql_tmp_tbl)

    def _default_transform(
        self, sql: str, output_meta: Metadata, output_path: str, tmp_db: str
    ):
        """Default table transformation

        Takes an SQL query for a temporary table in Athena and unloads
        the output to a temporary staging area in S3. The output is then
        checked against the provided metadata and then copied to the output
        path if the check passes. Otherwise, the data is deleted and an error
        is raised.

        Params
        ------
        sql: str
            SQL query to run against the temporary Athena database

        output_meta: Metadata
            Metadata for the data created from the SQL query

        output_path: str
            Location to place final results in S3

        tmp_db: str
            Name of the temporary database

        Return
        ------
        None
        """
        try:
            temp_path = output_path
            for stage in etl_stages:
                temp_path = temp_path.replace(stage, "temp")

            response = wr.athena.unload(
                sql=sql,
                path=temp_path,
                database=tmp_db,
                file_format="parquet",
                compression="snappy",
                partitioned_by=output_meta.partitions,
            )

            if response.raw_payload["Status"]["State"] == "SUCCEEDED":
                response_meta = [
                    f"{response.output_location}.metadata",
                    response.manifest_location,
                ]
                all_tmp_files = wr.s3.list_objects(temp_path)
                tmp_files = [fp for fp in all_tmp_files if fp not in response_meta]

                output_meta_dc = deepcopy(output_meta)
                sr = SchemaReader()
                schema_check = sr.check_schemas_match(
                    tmp_files, output_meta_dc, ext="parquet"
                )

                if schema_check:
                    perm_files = [
                        f"{fo.replace(temp_path, output_path)}" + ".snappy.parquet"
                        if fo.endswith(".snappy.parquet") is False
                        else f"{fo.replace(temp_path, output_path)}"
                        for fo in tmp_files
                    ]

                    copy_args = list(zip(tmp_files, perm_files))
                    _ = s3_bulk_copy(copy_args)
                    wr.s3.delete_objects(temp_path)

                else:
                    _logger.info("Schema validation failed. Deleting temp files")
                    wr.s3.delete_objects(temp_path)
                    raise ValueError("Files do not match expected schema")

            else:
                _logger.info("Failed to execute sql. Deleting temp folder")
                wr.s3.delete_objects(temp_path)
                raise ValueError("Failed to execute unload")

        except Exception as e:
            _logger.info(f"Failed to execute sql because of {e}. Deleting temp folder")
            wr.s3.delete_objects(temp_path)
            raise

    def _custom_transform(
        self, sql: str, output_meta: Metadata, output_path: str, tmp_db: str
    ):
        """Custom table transformation

        Passes arguments to the table specific method (this is the name
        of the table as specified in output meta) e.g. _tablename_transform

        Params
        ------
        sql: str
            SQL query to run against the temporary Athena database

        output_meta: Metadata
            Metadata for the data created from the SQL query

        output_path: str
            Location to place final results in S3

        tmp_db: str
            Name of the temporary database

        Return
        ------
        None
        """
        table_name = output_meta.name
        transform = getattr(self, f"_{table_name}_transform")
        transform(sql, output_meta, output_path, tmp_db)

    def _derived_transform(
        self, sql: str, output_meta: Metadata, output_path: str, tmp_db: str
    ):
        """Derived table transformation

        Transformation to apply to tables marked with 'derived' transformation
        in database config. Will use a private method '_tablename_transform'
        if it is available. This method will only run the SQL marked as
        final in the db config.

        Params
        ------
        sql: str
            SQL query to run against the temporary Athena database

        output_meta: Metadata
            Metadata for the data created from the SQL query

        output_path: str
            Location to place final results in S3

        tmp_db: str
            Name of the temporary database

        Return
        ------
        None
        """
        table_name = output_meta.name
        try:
            transform = getattr(self, f"_{table_name}_transform")
            transform(sql, output_meta, output_path, tmp_db)
        except AttributeError:
            sql_info = self.db.table(table_name).table_sql_paths("final")
            sql_tbl_name, _ = sql_info[0]
            if len(sql_info) == 1 and sql_tbl_name == table_name:
                self._default_transform(
                    sql=sql,
                    output_meta=output_meta,
                    output_path=output_path,
                    tmp_db=tmp_db,
                )

    def run(self, stages: Dict[str, str], tables: Union[List[str], None] = None):
        """Use AWS Athena to perform data transformation

        Implements SQL via AWS Athena to perform transformation.

        Params
        ------
        stages: Dict[str, str]
            Dictionary of the format
            {"input": "input_etl_stage", "output": "output_etl_stage"}

        tables: Union[List[str], None]
            List of tables to apply ETL processing step to. If None, then will
            pass all db tables for processing. Note, tables not applicable to this
            step will be filtered out prior to transformation.

        Return
        ------
        None
        """
        sql_table_filter = self.sql_table_filter
        db_search_limit = self.db_search_limit
        jinja_args = {} if self.jinja_args is None else self.jinja_args
        tables = get_source_tbls() if tables is None else tables

        db = self.db
        db_name = db.name
        primary_partition = db.primary_partition_name()
        env = db.env
        databases = wr.catalog.databases(limit=db_search_limit)

        tbl_prts = self.utils.transform_partitions(tables, stages=stages)

        ipt_stage = stages["input"]
        out_stage = stages["output"]
        inp_stage_lc = ipt_stage.replace("-", "_")

        for table_name, prts in tbl_prts.items():
            _logger.info(f"Starting job for {table_name}")
            if prts:
                tbl = db.table(table_name)
                temp_input_db_name = "_".join(
                    [
                        "_temp",
                        db_name,
                        env,
                        table_name,
                        inp_stage_lc,
                    ]
                )

                input_path, output_path, tf_type = self.utils.tf_args(
                    table_name, stages=stages
                )

                if ipt_stage in ["raw", "raw-hist"]:
                    tbl_cast_args = tbl.get_cast_cols()

                    meta_cast_args = [
                        {"name": col, "type": dtype, "nullable": True}
                        for col, dtype, _ in tbl_cast_args
                    ]

                else:
                    meta_cast_args = []

                input_meta = tbl.get_table_metadata(
                    ipt_stage,
                    [
                        {
                            "name": primary_partition,
                            "type": "int64",
                            "nullable": False,
                            "type_category": "integer",
                        },
                        *meta_cast_args,
                    ],
                )

                input_meta.partitions = [primary_partition]

                if temp_input_db_name in databases.Database.to_list():
                    wr.catalog.delete_database(temp_input_db_name)

                wr.catalog.create_database(temp_input_db_name)

                gc = GlueConverter()
                spec = gc.generate_from_meta(
                    input_meta,
                    database_name=temp_input_db_name,
                    table_location=input_path,
                )

                glue_client = boto3.client("glue")
                glue_client.create_table(**spec)
                wr.athena.repair_table(table=table_name, database=temp_input_db_name)

                output_meta = tbl.get_table_metadata(out_stage)
                output_meta.force_partition_order = "start"

                output_cols = [c["name"].lower() for c in output_meta.columns]
                input_cols = [c["name"].lower() for c in input_meta.columns]
                inandout_cols = [
                    c
                    for c in output_cols
                    if (c in input_cols and c not in output_meta.partitions)
                ]

                sql_columns = ", ".join(inandout_cols)

                etl_version = os.environ["GITHUB_TAG"]

                prts_timestamps = [
                    str(
                        extract_mojap_timestamp(
                            prt, timestamp_partition_name=primary_partition
                        )
                    )
                    for prt in prts
                ]
                prts_sql_clause = ",".join(prts_timestamps)

                sql_paths = db.shared_sql_paths(tf_types=[tf_type])
                if sql_table_filter:
                    sql_table_args = [
                        path for tbl, path in sql_paths if tbl in table_name
                    ]
                    sql_path = sql_table_args[0]
                else:
                    _, sql_path = sql_paths[0]

                sql = pydb.get_sql_from_file(
                    sql_path,
                    {
                        "database_name": temp_input_db_name,
                        "environment": db.env,
                        "table_name": table_name,
                        "github_tag": etl_version,
                        "sql_columns": sql_columns,
                        "snapshot_timestamps": prts_sql_clause,
                        "primary_partition": primary_partition,
                        **jinja_args,
                    },
                )

                transform = getattr(self, f"_{tf_type}_transform")

                try:
                    transform(sql, output_meta, output_path, temp_input_db_name)

                    wr.catalog.delete_database(temp_input_db_name)
                    if ipt_stage != "raw-hist":
                        self.utils.cleanup_partitions(
                            base_data_path=input_path, partitions=prts
                        )

                except Exception as e:
                    _logger.info(
                        (
                            "Failed to write data to curated.\n"
                            f"Error: {e}\n"
                            "Deleting any half-written files.\n"
                        )
                    )

                    self.utils.cleanup_partitions(
                        base_data_path=output_path, partitions=prts
                    )

                    wr.catalog.delete_database(temp_input_db_name)

                    raise

            else:
                _logger.info(f"No partitions to process for {table_name}")

    def _new_derived_table_partitions(
        self,
        table_name: str,
        transform_args: Dict,
        cutoff_sql_clause: str,
        primary_partition: str,
    ):
        ipt_args = transform_args["transforms"][table_name]["input"]
        db_ipts = list(ipt_args.keys())
        tbl_ipts = [list(ipt_args[ipt_db].keys()) for ipt_db in ipt_args]

        db_tbls = sum(
            [
                list(zip([db_ipts[i]] * len(tbl_ipts[i]), tbl_ipts[i]))
                for i in range(len(db_ipts))
            ],
            [],
        )

        db_tbl_tf_ipts = [
            (db, tbl, Database(db).table(tbl).transform_type()) for db, tbl in db_tbls
        ]

        db_tbl_ipts = [
            (db if tf != "derived" else f"{db}_derived", tbl)
            for db, tbl, tf in db_tbl_tf_ipts
        ]

        existing_prts = set(
            self.utils.list_partitions(
                table_name=table_name,
                stage="derived",
                extract_timestamp=True,
                disable_environment=True,
            )
        )

        input_prts_sql = [
            pydb.render_sql_template(
                """
                SELECT DISTINCT({{ primary_partition }}) as unique_prts
                FROM {{ full_db_name }}}}.{{ table_name }}
                WHERE {{ cutoff_clause }}
                ORDER BY {{ primary_partition }} DESC
                """,
                {
                    "full_db_name": get_full_db_name(db_name=ipt_db, env=self.db.env),
                    "table_name": ipt_tbl,
                    "cutoff_clause": cutoff_sql_clause,
                    "primary_partition": primary_partition,
                },
            )
            for ipt_db, ipt_tbl in db_tbl_ipts
        ]

        input_prts = [
            pydb.read_sql_queries(ipt_sql)["unique_prts"] for ipt_sql in input_prts_sql
        ]

        common_prts = set(input_prts[0])
        for prt_list in input_prts[1:]:
            common_prts = common_prts.intersection(set(prt_list))

        new_prts = [str(prt) for prt in common_prts if prt not in existing_prts]
        new_prts.sort(reverse=True)

        return new_prts

    def _create_intermediate_tables_for_derived_table(
        self,
        table_name: str,
        partitions: List[str],
        jinja_args: Dict,
    ):
        prts_arg = ",".join(partitions)

        table = self.db.table(table_name)
        uses_shared_sql = table.table_uses_shared_sql()

        if uses_shared_sql:
            self._create_shared_tmp_tables(
                tf_types=["derived"], snapshot_timestamps=prts_arg, **jinja_args
            )

        self._create_temporary_tables(
            table_name=table_name, **{"snapshot_timestamps": prts_arg, **jinja_args}
        )

    def _create_final_derived_table(
        self,
        table_name: str,
        primary_partition: str,
        partitions: List[str],
        transform_args: Dict,
        jinja_args: Dict,
    ) -> Metadata:
        table = self.db.table(table_name)
        prts_arg = ",".join(partitions)
        _, sql_path = table.table_sql_paths("final")[0]

        output_meta = table.get_table_metadata("derived")
        output_meta.force_partition_order = "start"
        output_path = transform_args["transforms"][table_name]["output"]["path"]

        user_id, _ = pydb.utils.get_user_id_and_table_dir()
        tmp_db = pydb.utils.get_database_name_from_userid(user_id)

        new_jinja_args = {
            "temporary_database": tmp_db,
            "snapshot_timestamps": prts_arg,
            "primary_partition": primary_partition,
            **jinja_args,
        }

        sql = pydb.get_sql_from_file(sql_path, jinja_args=new_jinja_args)

        try:
            self._derived_transform(
                sql,
                output_path=output_path,
                output_meta=output_meta,
                tmp_db=tmp_db,
            )

            return output_meta, output_path

        except Exception as e:
            _logger.info(
                (
                    "Failed to write data to derived.\n"
                    f"Error: {e}\n"
                    "Deleting any half-written files.\n"
                )
            )

            for prt in partitions:
                prt_path = os.path.join(output_path, prt)
                wr.s3.delete_objects(prt_path)

            raise

    def _refresh_and_repair_table(
        self,
        table_name: str,
        database_name: str,
        table_metadata: Metadata,
        table_data_path: str,
    ):
        wr.catalog.delete_table_if_exists(database=database_name, table=table_name)

        gc = GlueConverter()

        spec = gc.generate_from_meta(
            table_metadata,
            database_name=database_name,
            table_location=table_data_path,
        )

        glue_client = boto3.client("glue")
        glue_client.create_table(**spec)
        wr.athena.repair_table(table=table_name, database=database_name)

    def run_derived(
        self,
        tables: List[str],
        stage: Optional[str] = "derived",
        jinja_args: Optional[dict] = None,
    ):
        """Creates derived tables for db using Athena

        Runs the following steps:
        1. Creates share database temporary tables if not created
        2. Creates table specific temporary tables
        3. Runs final SQL and unloads results to derived path

        Will also implement custom derived transformation if a
        _tablename_transform exists.

        Params
        ------
        tables: List[str]
            List of table names.

        **jinja_args:
            Jinja args to pass to pydbtools calls.
        """
        if stage != "derived":
            raise ValueError("Expecting derived ETL step for this transform")

        if jinja_args is None:
            jinja_args = {}

        db = self.db
        primary_partition = db.primary_partition_name()
        db_derived_name = get_full_db_name(db_name=db.name, env=db.env, derived=True)
        tables_to_use = db.tables_to_use(
            tables, stages=["derived"], tf_types=["derived"]
        )

        cutoff_min = (
            int(get_start_date().timestamp()) if get_start_date() is not None else 0
        )
        cutoff_max = (
            int(get_end_date().timestamp()) if get_end_date() is not None else None
        )

        max_cutoff_clause = f"{primary_partition} <= {cutoff_max}"
        min_cutoff_clause = f"{primary_partition} >= {cutoff_min}"
        cutoff_clause = (
            min_cutoff_clause
            if cutoff_max is None
            else (min_cutoff_clause + " AND " + max_cutoff_clause)
        )

        databases = wr.catalog.databases(limit=10_000)
        if db_derived_name not in databases.Database.to_list():
            wr.catalog.create_database(
                db_derived_name, description=f"Derived {db.name} tables"
            )

        tf_args = db.transform_args(
            tables_to_use,
            tf_types=["derived"],
            **{tbl: {"input": "curated", "output": "derived"} for tbl in tables_to_use},
        )

        for tbl in tables_to_use:
            new_prts = self._new_derived_table_partitions(
                table_name=tbl,
                transform_args=tf_args,
                cutoff_sql_clause=cutoff_clause,
                primary_partition=primary_partition,
            )

            if not new_prts:
                continue

            self._create_intermediate_tables_for_derived_table(
                table_name=tbl,
                partitions=new_prts,
                jinja_args=jinja_args,
            )

            output_meta, output_path = self._create_final_derived_table(
                table_name=tbl,
                primary_partition=primary_partition,
                partitions=new_prts,
                transform_args=tf_args,
                jinja_args=jinja_args,
            )

            self._refresh_and_repair_table(
                table_name=output_meta.name,
                database_name=db_derived_name,
                table_metadata=output_meta,
                table_data_path=output_path,
            )
