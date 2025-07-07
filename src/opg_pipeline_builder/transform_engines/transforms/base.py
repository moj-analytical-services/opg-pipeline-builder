from abc import ABC, abstractmethod

from mojap_metadata import Metadata
from pydantic import BaseModel

from ...database import Database
from ..utils.utils import TransformEngineUtils


class BaseTransformations(BaseModel, ABC):
    utils: TransformEngineUtils
    db: Database

    class Config:
        arbitrary_types_allowed = True

    @abstractmethod
    def default_transform(
        self, sql: str, output_meta: Metadata, output_path: str, database_name: str
    ): ...

    def custom_transform(
        self, sql: str, output_meta: Metadata, output_path: str, database_name: str
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
        transform = getattr(self, f"{table_name}_transform")
        transform(sql, output_meta, output_path, database_name)

    def derived_transform(
        self, sql: str, output_meta: Metadata, output_path: str, database_name: str
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
            transform = getattr(self, f"{table_name}_transform")
            transform(sql, output_meta, output_path, database_name)

        except AttributeError:
            sql_info = self.db.table(table_name).table_sql_paths("final")
            sql_tbl_name, _ = sql_info[0]

            if len(sql_info) == 1 and sql_tbl_name == table_name:
                self.default_transform(
                    sql=sql,
                    output_meta=output_meta,
                    output_path=output_path,
                    database_name=database_name,
                )
