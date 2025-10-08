import logging

import awswrangler as wr
import boto3
from botocore.exceptions import ClientError
from mojap_metadata.converters.glue_converter import GlueConverter

from ..models.metadata_model import MetaData
from ..utils.constants import get_full_db_name
from .base import BaseTransformEngine

_logger: logging.Logger = logging.getLogger(__name__)


class CatalogTransformEngine(BaseTransformEngine):
    def run(self, table: str, _: MetaData, stage: str = "curated") -> None:
        """Overlays database over the given tables

        Overlays athena database over the given tables for the
        ETL stage specified using the metadata for that stage.

        Params
        ------
        tables: List[str]
            List of table names.

        stage:
            ETL stage for database to be created.

        Return
        ------
        None
        """
        glue_client = boto3.client("glue")
        db = self.db
        db_name = get_full_db_name(db_name=db.name, env=db.env)
        tables = [table]
        stage = "curated"

        try:
            glue_client.get_database(Name=db_name)

        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityNotFoundException":
                db_meta = {
                    "DatabaseInput": {
                        "Description": db.config["description"],
                        "Name": db_name,
                    }
                }
                _logger.info(f"Creating initial {db_name} db")
                glue_client.create_database(**db_meta)
            else:
                _logger.info("Unexpected error: %s" % e)

        for table in tables:
            _logger.info(f"Updating {db_name}.{table}")
            db_table = db.table(table)

            stage_meta = db_table.get_table_metadata(stage)
            stage_meta.force_partition_order = "start"
            stage_s3_path = db_table.get_table_path(stage)

            if table != stage_meta.name:
                raise ValueError(
                    (
                        "Table name in metadata file is inconsistent:\n"
                        f"{stage_meta.name} (meta)\n"
                        f"{table} (config)"
                    )
                )

            wr.catalog.delete_table_if_exists(database=db_name, table=stage_meta.name)

            gc = GlueConverter()

            spec = gc.generate_from_meta(
                stage_meta, database_name=db_name, table_location=stage_s3_path
            )

            glue_client.create_table(**spec)
            wr.athena.repair_table(table=table, database=db_name)

            _logger.info(f"{db_name}.{table} updated")

        _logger.info(f"Finished updating {db_name}")
