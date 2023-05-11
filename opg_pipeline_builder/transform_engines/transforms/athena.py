import logging
from abc import ABC, abstractmethod
from copy import deepcopy

import awswrangler as wr
from mojap_metadata import Metadata

from ...utils.constants import etl_stages
from ...utils.schema_reader import SchemaReader
from ...utils.utils import s3_bulk_copy
from .base import BaseTransformations

_logger: logging.Logger = logging.getLogger(__name__)


class AthenaTransformations(BaseTransformations, ABC):
    @abstractmethod
    def default_transform(
        self, sql: str, output_meta: Metadata, output_path: str, database_name: str
    ):
        ...


class AthenaParquetTransformations(AthenaTransformations):
    def default_transform(
        self, sql: str, output_meta: Metadata, output_path: str, database_name: str
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

            _logger.info("Performing unload")
            response = wr.athena.unload(
                sql=sql,
                path=temp_path,
                database=database_name,
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
                _logger.info("Validating schema of output")
                schema_check = sr.check_schemas_match(
                    tmp_files, output_meta_dc, ext="parquet"
                )

                if schema_check:
                    _logger.info("Moving output expected location")
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
