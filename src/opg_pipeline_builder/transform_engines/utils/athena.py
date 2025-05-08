from typing import List, Optional, Union

import awswrangler as wr
import boto3
import pydbtools as pydb
from mojap_metadata import Metadata
from mojap_metadata.converters.glue_converter import GlueConverter

from .utils import TransformEngineUtils


class AthenaTransformEngineUtils(TransformEngineUtils):
    @staticmethod
    def check_table_is_not_empty(table_name: str):
        count_template = pydb.render_sql_template(
            "SELECT COUNT(*) as count FROM __temp__.{{ sql_tbl }}",
            jinja_args={"sql_tbl": table_name},
        )

        tbl_check = pydb.read_sql_query(count_template)

        if tbl_check["count"][0] == 0:
            raise ValueError(f"No data exists in {table_name}")

    def recreate_database(
        self,
        database_name: str,
        existing_databases: Optional[Union[List[str], None]] = None,
    ):
        if existing_databases is None:
            databases_df = wr.catalog.databases(limit=self.db_search_limit)
            existing_databases = databases_df.Database.to_list()

        if database_name in existing_databases:
            wr.catalog.delete_database(database_name)

        wr.catalog.create_database(database_name)

    @staticmethod
    def refresh_and_repair_table(
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
