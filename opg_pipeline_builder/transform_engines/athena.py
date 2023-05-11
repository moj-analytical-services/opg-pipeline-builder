import logging
import os
from typing import Dict, List, Optional, Tuple, Union

import awswrangler as wr
import pydbtools as pydb
from mojap_metadata import Metadata

from ..database import Database
from ..utils.constants import (
    get_end_date,
    get_full_db_name,
    get_source_db,
    get_source_tbls,
    get_start_date,
)
from ..utils.utils import extract_mojap_timestamp
from .base import BaseTransformEngine
from .transforms import athena as athena_transforms
from .utils.athena import AthenaTransformEngineUtils

_logger: logging.Logger = logging.getLogger(__name__)

DEFAULT_ATHENA_TRANSFORM = "parquet"


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
    transforms: Optional[Union[athena_transforms.AthenaTransformations, None]] = None

    def __init__(
        self,
        db_name: Optional[str] = None,
        utils: Optional[AthenaTransformEngineUtils] = None,
        transforms: Optional[athena_transforms.AthenaTransformations] = None,
        transforms_type: Optional[Union[str, None]] = None,
        **kwargs,
    ):
        if db_name is None:
            _logger.debug("Setting database for engine from environment")
            db_name = get_source_db()
            _logger.debug(f"Engine database environment variable set to {db_name}")

        db = Database(db_name=db_name)
        utils = AthenaTransformEngineUtils(db=db) if utils is None else utils

        super().__init__(db_name=db_name, utils=utils, **kwargs)

        if transforms is None:
            transforms_type = (
                DEFAULT_ATHENA_TRANSFORM if transforms_type is None else transforms_type
            )
            formatted_transform_type = transforms_type[0].upper() + transforms_type[1:]
            transforms_class_name = f"Athena{formatted_transform_type}Transformations"
            transforms_class = getattr(athena_transforms, transforms_class_name)
            transforms = transforms_class(utils=self.utils, db=self.db)

        self.transforms = transforms

    def _default_jinja_args(
        self,
        snapshot_timestamps: Optional[Union[str, None]] = None,
        database_name: Optional[Union[str, None]] = None,
        environment: Optional[Union[str, None]] = None,
    ) -> Dict[str, str]:
        environment = self.db.env if environment is None else environment

        database_name = (
            get_full_db_name(db_name=self.db.name, env=environment)
            if database_name is None
            else database_name
        )

        additional_jinja_args = {} if self.jinja_args is None else self.jinja_args

        return {
            "database_name": database_name,
            "environment": environment,
            "snapshot_timestamps": snapshot_timestamps
            if snapshot_timestamps is not None
            else "",
            "github_tag": os.environ["GITHUB_TAG"],
            "primary_partition": self.db.primary_partition_name(),
            **additional_jinja_args,
        }

    def _check_shared_temporary_table_snapshots(
        self,
        temporary_table_name: str,
        expected_snapshots: List[str],
    ) -> Tuple[bool, bool]:
        primary_partition = self.db.primary_partition_name()
        try:
            check_prts_sql = pydb.render_sql_template(
                """
                SELECT DISTINCT({{ primary_partition }}) as distinct_prts
                FROM __temp__.{{ temporary_table_name }}
                ORDER BY {{ primary_partition }} DESC
                """,
                {
                    "temporary_table_name": temporary_table_name,
                    "primary_partition": primary_partition,
                },
            )

            check_prts = pydb.read_sql_queries(check_prts_sql)
            check_prts_str = [str(prt) for prt in check_prts["distinct_prts"]]

            delete_existing_temporary_table = sorted(check_prts_str) != sorted(
                expected_snapshots
            )

            temporary_table_exists = True

        except wr.exceptions.QueryFailed:
            delete_existing_temporary_table = False
            temporary_table_exists = False

        return temporary_table_exists, delete_existing_temporary_table

    def _create_temp_table(
        self,
        temp_table_name: str,
        table_sql_filepath: str,
        base_database_name: Optional[Union[str, None]] = None,
        environment: Optional[Union[str, None]] = None,
        **additional_jinja_args,
    ):
        sql = pydb.get_sql_from_file(
            table_sql_filepath,
            jinja_args={
                **self._default_jinja_args(
                    database_name=base_database_name,
                    environment=environment,
                ),
                **additional_jinja_args,
            },
        )

        pydb.create_temp_table(sql, table_name=temp_table_name)

    def _create_shared_temporary_tables(
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

        shared_sql = db.shared_sql_paths(tf_types)

        for sql_tbl, sql_path in shared_sql:
            (
                temporary_table_exists,
                delete_temporary_table,
            ) = self._check_shared_temporary_table_snapshots(
                temporary_table_name=sql_tbl,
                expected_snapshots=snapshot_timestamps,
            )

            if delete_temporary_table:
                _logger.info(
                    f"Deleting existing shared intermediate temp table {sql_tbl}"
                    + " and its data"
                )
                pydb.delete_table_and_data(database="__temp__", table=sql_tbl)

            create_temporary_table = (
                delete_temporary_table or temporary_table_exists is False
            )

            if create_temporary_table:
                _logger.info(f"Creating shared intermediate temp table {sql_tbl}")
                self._create_temp_table(
                    temp_table_name=sql_tbl,
                    table_sql_filepath=sql_path,
                    snapshot_timestamps=", ".join(snapshot_timestamps),
                    **jinja_args,
                )

                _logger.info(f"Validating shared intermediate temp table {sql_tbl}")
                self.utils.check_table_is_not_empty(table_name=sql_tbl)

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
        tbl_tmp_sql = table.table_sql_paths(type="temp")

        for sql_tmp_tbl, sql_tpt in tbl_tmp_sql:
            _logger.info(f"Creating {sql_tmp_tbl} intermediate table for {table_name}")
            self._create_temp_table(
                temp_table_name=sql_tmp_tbl,
                table_sql_filepath=sql_tpt,
                base_database_name=db_name,
                **jinja_args,
            )

    def _temporary_database_name_for_load(
        self, table_name: str, input_stage: str
    ) -> str:
        input_stage_under = input_stage.replace("-", "_")
        return "_".join(
            [
                "_temp",
                self.db.name,
                self.db.env,
                table_name,
                input_stage_under,
            ]
        )

    def _prepare_input_metadata_for_load(
        self, table_name: str, input_stage: str
    ) -> Metadata:
        tbl = self.db.table(table_name)

        primary_partition = self.db.primary_partition_name()

        if input_stage in ["raw", "raw-hist"]:
            tbl_cast_args = tbl.get_cast_cols()

            meta_cast_args = [
                {"name": col, "type": dtype, "nullable": True}
                for col, dtype, _ in tbl_cast_args
            ]

        else:
            meta_cast_args = []

        input_meta = tbl.get_table_metadata(
            input_stage,
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

        return input_meta

    def _get_sql_partitions(self, partitions: List[str]) -> str:
        prts_timestamps = [
            str(
                extract_mojap_timestamp(
                    prt, timestamp_partition_name=self.db.primary_partition_name()
                )
            )
            for prt in partitions
        ]
        prts_sql_clause = ",".join(prts_timestamps)
        return prts_sql_clause

    def _get_sql_path(self, table_name: str, transformation_type: str) -> str:
        sql_paths = self.db.shared_sql_paths(tf_types=[transformation_type])

        if self.sql_table_filter:
            sql_table_args = [path for tbl, path in sql_paths if tbl in table_name]
            sql_path = sql_table_args[0]

        else:
            _, sql_path = sql_paths[0]

        return sql_path

    def _get_sql(
        self,
        table_name: str,
        partitions: List[str],
        transformation_type: str,
        input_metadata: Metadata,
        output_metadata: Metadata,
        temporary_database_name: str,
    ) -> str:
        common_columns_with_same_types = self.utils.get_common_columns(
            input_metadata=input_metadata,
            output_metadata=output_metadata,
        )

        sql_columns = ",\n".join(common_columns_with_same_types)

        sql_partitions = self._get_sql_partitions(partitions=partitions)

        sql_path = self._get_sql_path(table_name, transformation_type)

        sql = pydb.get_sql_from_file(
            sql_path,
            {
                **self._default_jinja_args(
                    snapshot_timestamps=sql_partitions,
                    database_name=temporary_database_name,
                ),
                "table_name": table_name,
                "sql_columns": sql_columns,
            },
        )

        return sql

    def _execute_load(
        self,
        sql: str,
        transformation_type: str,
        input_stage: str,
        input_path: str,
        output_metadata: Metadata,
        output_path: str,
        temporary_load_database_name: str,
        partitions: List[str],
    ):
        transform = getattr(self.transforms, f"{transformation_type}_transform")

        try:
            transform(sql, output_metadata, output_path, temporary_load_database_name)

            wr.catalog.delete_database(temporary_load_database_name)
            if input_stage != "raw-hist":
                self.utils.cleanup_partitions(
                    base_data_path=input_path, partitions=partitions
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
                base_data_path=output_path, partitions=partitions
            )

            wr.catalog.delete_database(temporary_load_database_name)

            raise

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
        tables = get_source_tbls() if tables is None else tables

        db = self.db
        databases = wr.catalog.databases(limit=self.db_search_limit)
        existing_databases = databases.Database.to_list()

        tbl_prts = self.utils.transform_partitions(tables, stages=stages)

        ipt_stage = stages["input"]
        out_stage = stages["output"]

        for table_name, prts in tbl_prts.items():
            _logger.info(f"Starting job for {table_name}")
            if prts:
                temp_input_db_name = self._temporary_database_name_for_load(
                    table_name, ipt_stage
                )

                _logger.info(f"Preparing input metadata for {table_name} load")
                input_meta = self._prepare_input_metadata_for_load(
                    table_name, ipt_stage
                )

                input_path, output_path, tf_type = self.utils.tf_args(
                    table_name, stages=stages
                )

                _logger.info(
                    f"Creating / recreating temporary database for {table_name} load"
                )
                self.utils.recreate_database(
                    database_name=temp_input_db_name,
                    existing_databases=existing_databases,
                )

                _logger.info(f"Re-freshing and repairing temporary {table_name} table")
                self.utils.refresh_and_repair_table(
                    table_name=table_name,
                    database_name=temp_input_db_name,
                    table_metadata=input_meta,
                    table_data_path=input_path,
                )

                output_meta = db.table(table_name).get_table_metadata(out_stage)
                output_meta.force_partition_order = "start"

                _logger.info(f"Generating SQL for {table_name} load")
                sql = self._get_sql(
                    table_name=table_name,
                    partitions=prts,
                    transformation_type=tf_type,
                    input_metadata=input_meta,
                    output_metadata=output_meta,
                    temporary_database_name=temp_input_db_name,
                )

                _logger.info(f"Executing SQL load for {table_name}")
                self._execute_load(
                    sql=sql,
                    transformation_type=tf_type,
                    input_stage=ipt_stage,
                    input_path=input_path,
                    output_metadata=output_meta,
                    output_path=output_path,
                    temporary_load_database_name=temp_input_db_name,
                    partitions=prts,
                )

                _logger.info(f"Load complete for {table_name}")

            else:
                _logger.info(f"No partitions to process for {table_name}")

    def _new_derived_table_partitions(
        self,
        table_name: str,
        transform_args: Dict,
        primary_partition: str,
    ) -> List[str]:
        cutoff_sql_clause = self._create_derived_cutoff_sql_clause()
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
                FROM {{ full_db_name }}.{{ table_name }}
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
            self._create_shared_temporary_tables(
                tf_types=["derived"], snapshot_timestamps=partitions, **jinja_args
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
            **self._default_jinja_args(
                snapshot_timestamps=prts_arg,
            ),
            "temporary_database": tmp_db,
            **jinja_args,
        }

        sql = pydb.get_sql_from_file(sql_path, jinja_args=new_jinja_args)

        try:
            self.transforms.derived_transform(
                sql,
                output_path=output_path,
                output_meta=output_meta,
                database_name=tmp_db,
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

    def _create_derived_cutoff_sql_clause(self) -> str:
        primary_partition = self.db.primary_partition_name()

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

        return cutoff_clause

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

        databases = wr.catalog.databases(self.db_search_limit)
        if db_derived_name not in databases.Database.to_list():
            _logger.info(
                f"Derived database {db_derived_name} doesn't exist. "
                + "Creating database..."
            )
            wr.catalog.create_database(
                db_derived_name, description=f"Derived {db.name} tables"
            )
            _logger.info(f"Derived database {db_derived_name} created")

        tf_args = db.transform_args(
            tables_to_use,
            tf_types=["derived"],
            **{tbl: {"input": "curated", "output": "derived"} for tbl in tables_to_use},
        )

        for tbl in tables_to_use:
            _logger.info(f"Starting derived table job for {tbl}")

            _logger.info(f"Fetching new input data partitions for {tbl}")
            new_prts = self._new_derived_table_partitions(
                table_name=tbl,
                transform_args=tf_args,
                primary_partition=primary_partition,
            )

            if not new_prts:
                _logger.info(f"No new input partitions for {tbl}")
                continue

            _logger.info(f"Creating intermediate temporary tables for {tbl}")
            self._create_intermediate_tables_for_derived_table(
                table_name=tbl,
                partitions=new_prts,
                jinja_args=jinja_args,
            )

            _logger.info(f"Creating final derived table {tbl}")
            output_meta, output_path = self._create_final_derived_table(
                table_name=tbl,
                primary_partition=primary_partition,
                partitions=new_prts,
                transform_args=tf_args,
                jinja_args=jinja_args,
            )

            _logger.info(f"Refreshing and repairing {tbl}")
            self.utils.refresh_and_repair_table(
                table_name=output_meta.name,
                database_name=db_derived_name,
                table_metadata=output_meta,
                table_data_path=output_path,
            )

            _logger.info(f"Finished derived table job for {tbl}")
