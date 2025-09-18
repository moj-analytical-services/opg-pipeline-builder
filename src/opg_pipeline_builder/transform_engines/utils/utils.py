import logging
import os
from copy import deepcopy
from datetime import datetime
from typing import Any

import awswrangler as wr
from dataengineeringutils3.s3 import _add_slash
from mojap_metadata import Metadata
from pydantic import BaseModel

from opg_pipeline_builder.database import Database
from opg_pipeline_builder.utils.constants import (
    get_end_date,
    get_source_tbls,
    get_start_date,
)
from opg_pipeline_builder.utils.utils import (
    extract_mojap_partition,
    extract_mojap_timestamp,
    get_modified_filepaths_from_s3_folder,
)

logger = logging.getLogger(__name__)


class TransformEngineUtils(BaseModel):
    db: Database

    class Config:
        underscore_attrs_are_private = True
        arbitrary_types_allowed = True

    def list_table_files(
        self,
        stage: str,
        table_name: str,
        modified_after: datetime | None = None,
        modified_before: datetime | None = None,
        additional_stages: dict[str, str] | None = None,
        disable_environment: bool = False,
    ) -> list[str]:
        """Lists files in S3 for a given table

        Lists all files in S3 for a given table, at the given ETL stage.
        If modified_after or modified_before are set, then only
        return files modified within that period. Additional stages
        can be set if source data is located in S3.

        Params
        ------
        stage: str
            ETL stage for fetching files.

        table_name: str
            Name of the table to fetch data for.

        modified_after: Optional[datetime, None]
            Specify a datetime for ensuring returned files
            have been modified after that point.

        modified_before: Optional[datetime, None]
            Specify a datetime for ensuring returned files
            have been modified before that point.

        additional_stages: Optional[dict[str, str], None]
            Additional valid stages to pass to function e.g. source
            S3 location of files. Should be a dictionary of the form
            {stage_name: stage_s3_data_path}.

        Return
        ------
        list[str]
            List of S3 paths for the table's data
        """
        if modified_after is None:
            modified_after = get_start_date()

        if modified_before is None and disable_environment is False:
            modified_before = get_end_date()

        table = self.db.table(table_name)
        etl_stages = deepcopy(table.etl_stages())
        table_paths = table.table_data_paths()

        if additional_stages is not None:
            etl_stages += list(additional_stages.keys())

        if stage not in etl_stages:
            stages = ", ".join(etl_stages)
            raise ValueError(f"Stage must be one of {stages}")

        if additional_stages is not None:
            if stage in additional_stages:
                table_stage_path = os.path.join(additional_stages[stage], table_name)
            else:
                table_stage_path = table_paths[stage]
        else:
            table_stage_path = table_paths[stage]

        files: list[str] = get_modified_filepaths_from_s3_folder(
            table_stage_path,
            modified_after=modified_after,
            modified_before=modified_before,
        )

        return files

    def list_partitions(
        self,
        table_name: str,
        stage: str,
        extract_timestamp: bool = False,
        **kwargs: dict[Any, Any],
    ) -> list[str] | list[int]:
        """Lists partitions for table in S3 at ETL stage

        Lists default MoJ partitions located for table at the given
        ETL stage. If extract_timestamp is set to True, then only the
        timetamp will be returned. kwargs are passed onto
        _list_table_files.

        Params
        ------
        table_name: str
            Name of the table to retrieve partitions for.

        stage: str
            ETL stage for the table.

        extract_timestamp: Optional[bool]
            True to retrieve the timestamp only. Defaults to False.

        kwargs: dict[Any, Any]
            Passed onto _list_table_files

        Return
        ------
        list[str] | list[int]
            List of partitions for the table at the given ETL stage.
        """
        files = self.list_table_files(stage=stage, table_name=table_name, **kwargs)
        primary_partition_name = self.db.primary_partition_name()

        partitions = [
            extract_mojap_partition(f, timestamp_partition_name=primary_partition_name)
            for f in files
        ]

        if extract_timestamp:
            partitions = [
                int(
                    extract_mojap_timestamp(
                        p, timestamp_partition_name=primary_partition_name
                    )
                )
                for p in partitions
            ]

        return partitions

    def list_unprocessed_partitions(
        self,
        table_name: str,
        input_stage: str = "raw_hist",
        output_stage: str = "curated",
        **kwargs: dict[Any, Any],
    ) -> list[str] | list[int]:
        """Lists unprocessed partitions for table in S3 at ETL stage

        Returns table's partitions in the input ETL stage set
        that do not exist in the output ETL stage set. kwargs
        are passed onto _list_partitions.

        Params
        ------
        table_name: str
            Name of the table to retrieve partitions for.

        input_stage: str
            Input ETL stage for the table.

        output_stage: str
            Output ETL stage for the table.

        kwargs: dict[Any, Any]
            Passed onto _list_unprocessed_partitions.

        Return
        ------
        list[str] | list[int]]
            List of unprocessed partitions for the table at the
            given ETL stage.
        """
        input_partitions = self.list_partitions(
            stage=input_stage, table_name=table_name, **kwargs
        )

        output_partitions = self.list_partitions(
            stage=output_stage, table_name=table_name, **kwargs
        )

        new_partitions = [f for f in input_partitions if f not in output_partitions]

        new_partitions_list: list[str] = sorted(list(set(new_partitions)), reverse=True)

        return new_partitions_list

    def transform_partitions(
        self,
        tables: list[str] | None = get_source_tbls(),
        stages: dict[str, str] = {"input": "raw_hist", "output": "curated"},
        tf_types: list[str] = ["default", "custom"],
    ) -> dict[str, list[str] | list[int]]:
        """Lists unprocessed partitions for a set of tables

        Returns a dictionary of unprocessed partitions for the
        tables, stages and table transform types specified. Dict
        will be of the form:
        {table_name: [unprocessed_partitions], ...}

        Params
        ------
        tables: list[str] | None
            List of table names. None to pass all db tables.

        stages: dict[str, str]
            Dictionary of the form
            {"input": "input_stage_name", "output": "output_stage_name"}

        tf_types: list[str]
            List of table transform types to filter given table names on.

        Return
        ------
        dict[str, list[str] | list[int]]
            Dictionary conatining list of unprocessed partitions for
            the tables passed, given the ETL stages specified.
        """
        stages_list = [v for _, v in stages.items()]
        tables_to_use = self.db.tables_to_use(
            tables, stages=stages_list, tf_types=tf_types
        )
        transform_partitions_dict = {}
        for table in tables_to_use:
            transform_partitions_dict[table] = self.list_unprocessed_partitions(
                table, input_stage=stages["input"], output_stage=stages["output"]
            )

        return transform_partitions_dict

    def tf_args(
        self,
        table_name: str,
        stages: dict[str, str] = {"input": "raw_hist", "output": "curated"},
    ) -> tuple[str, str, str]:
        """Transformation arguments for specified table

        Returns a tuple consisting of the
        transform type, input S3 path and output S3 path
        for the given table and stages.

        Params
        ------
        table_name: str
            Name of the table to create temp tables for.

        stages: dict[str, str]
            Dictionary of the form
            {"input": "input_stage_name", "output": "output_stage_name"}

        Return
        ------
        tuple[str, str, str]
            transform type, input S3 path and output S3 path
            for the given table and stages.
        """
        transform_stage_args = {table_name: stages}

        tf_args = self.db.transform_args([table_name], **transform_stage_args)

        tf_type = tf_args["transforms"][table_name]["transform_type"]
        output_path = tf_args["transforms"][table_name]["output"]["path"]

        if tf_type != "derived":
            input_path = tf_args["transforms"][table_name]["input"]["path"]
        else:
            inputs = tf_args["transforms"][table_name]["input"]
            input_path = []
            for k, v in inputs.items():
                for tbl, info in v.items():
                    input_path.append((k, tbl, info["path"]))

        return (input_path, output_path, tf_type)

    def get_secrets(self, secret_key: str) -> str:
        """Return secret from environment variable.

        Looks up the value from the environment variables for the
        given secret key.

        Params
        ------
        secret_key: str
            Name of the secret to retrieve.

        Return
        ------
        str
            Secret value from environment variable containing the secret.
        """
        secret = os.getenv(secret_key, None)

        if not secret:
            err = f"No value found for secret: '{secret_key}'. The secret needs to be added to the DAG manifest file and AWS secret manager."
            raise ValueError(err)

        return secret

    @staticmethod
    def cleanup_partitions(base_data_path: str, partitions: list[str]) -> None:
        """Deletes partitions data

        Deletes data against partitions specified

        Params
        ------
        base_data_path: str
            Base directory for partitions in S3

        partitions: list[str]
            List of s3 partitions to delete
        """
        for prt in partitions:
            prt_path = os.path.join(base_data_path, prt)
            wr.s3.delete_objects(_add_slash(prt_path))

    @staticmethod
    def get_common_columns(
        input_metadata: Metadata,
        output_metadata: Metadata,
    ) -> list[str]:
        input_columns = input_metadata.columns
        output_columns = output_metadata.columns

        input_column_names = [c["name"].lower() for c in input_columns]
        output_column_names = [c["name"].lower() for c in output_columns]

        input_column_types = {c["name"].lower(): c["type"] for c in input_columns}
        output_column_types = {c["name"].lower(): c["type"] for c in output_columns}

        common_columns = [
            c
            for c in output_column_names
            if (c in input_column_names and c not in output_metadata.partitions)
        ]

        common_columns_with_same_types = [
            c for c in common_columns if input_column_types[c] == output_column_types[c]
        ]

        return common_columns_with_same_types
