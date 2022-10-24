import os
import logging
import awswrangler as wr

from copy import deepcopy
from inspect import signature
from datetime import datetime
from dataengineeringutils3.s3 import _add_slash
from ssm_parameter_store import EC2ParameterStore
from typing import Union, List, Dict, Optional, Tuple

from ..database import Database
from ..utils.constants import (
    aws_region,
    aws_loggers,
    get_source_db,
    get_source_tbls,
    get_start_date,
    get_end_date,
)
from ..utils.utils import (
    extract_mojap_timestamp,
    get_modified_filepaths_from_s3_folder,
    extract_mojap_partition,
)


class BaseTransformEngine:
    def __init__(self, db_name: Optional[str] = None, debug: Optional[bool] = False):
        if db_name is None:
            db_name = get_source_db()
        self._db = Database(db_name=db_name)

        if debug:
            for name in logging.Logger.manager.loggerDict.keys():
                if any([logger in name for logger in aws_loggers]):
                    logging.getLogger(name).setLevel(logging.WARNING)

        self._validate()

    @staticmethod
    def _check_public_method_args(parameters: object) -> bool:
        if "tables" not in parameters:
            return False

        if "stages" or "stage" in parameters:
            return True

    def _validate(self):
        methods = [
            signature(getattr(self, method_name)).parameters
            for method_name in dir(self)
            if callable(getattr(self, method_name)) and not method_name.startswith("_")
        ]

        validation = all(
            [
                BaseTransformEngine._check_public_method_args(parameters)
                for parameters in methods
            ]
        )

        if not validation:
            raise AssertionError(
                "Transform engine public methods have invalid" " arguments."
            )

    @property
    def db(self) -> str:
        return self._db

    # Helper methods - all private
    def _list_table_files(
        self,
        stage: str,
        table_name: str,
        modified_after: Optional[Union[datetime, None]] = None,
        modified_before: Optional[Union[datetime, None]] = None,
        additional_stages: Optional[Union[Dict[str, str], None]] = None,
    ) -> List[str]:
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

        modified_after: Optional[Union[datetime, None]]
            Specify a datetime for ensuring returned files
            have been modified after that point.

        modified_before: Optional[Union[datetime, None]]
            Specify a datetime for ensuring returned files
            have been modified before that point.

        additional_stages: Optional[Union[Dict[str, str], None]]
            Additional valid stages to pass to function e.g. source
            S3 location of files. Should be a dictionary of the form
            {stage_name: stage_s3_data_path}.

        Return
        ------
        List[str]
            List of S3 paths for the table's data
        """
        if modified_after is None:
            modified_after = get_start_date()

        if modified_before is None:
            modified_before = get_end_date()

        table = self._db.table(table_name)
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

        files = get_modified_filepaths_from_s3_folder(
            table_stage_path,
            modified_after=modified_after,
            modified_before=modified_before,
        )

        return files

    def _list_partitions(
        self,
        table_name: str,
        stage: str,
        extract_timestamp: Optional[bool] = False,
        **kwargs,
    ) -> Union[List[str], List[int]]:
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

        kwargs:
            Passed onto _list_table_files

        Return
        ------
        Union[List[str], List[int]]
            List of partitions for the table at the given ETL stage.
        """
        files = self._list_table_files(stage=stage, table_name=table_name, **kwargs)
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

    def _list_unprocessed_partitions(
        self,
        table_name: str,
        input_stage: str = "raw-hist",
        output_stage: str = "curated",
        **kwargs,
    ) -> Union[List[str], List[int]]:
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

        kwargs:
            Passed onto _list_unprocessed_partitions.

        Return
        ------
        Union[List[str], List[int]]
            List of unprocessed partitions for the table at the
            given ETL stage.
        """
        input_partitions = self._list_partitions(
            stage=input_stage, table_name=table_name, **kwargs
        )

        output_partitions = self._list_partitions(
            stage=output_stage, table_name=table_name, **kwargs
        )

        new_partitions = [f for f in input_partitions if f not in output_partitions]

        new_partitions_list = sorted(list(set(new_partitions)), reverse=True)

        return new_partitions_list

    def _transform_partitions(
        self,
        tables: Union[List[str], None] = get_source_tbls(),
        stages: Dict[str, str] = {"input": "raw-hist", "output": "curated"},
        tf_types: List[str] = ["default", "custom"],
    ) -> Dict[str, Union[List[str], List[int]]]:
        """Lists unprocessed partitions for a set of tables

        Returns a dictionary of unprocessed partitions for the
        tables, stages and table transform types specified. Dict
        will be of the form:
        {table_name: [unprocessed_partitions], ...}

        Params
        ------
        tables: Union[List[str], None]
            List of table names. None to pass all db tables.

        stages: Dict[str, str]
            Dictionary of the form
            {"input": "input_stage_name", "output": "output_stage_name"}

        tf_types: List[str]
            List of table transform types to filter given table names on.

        Return
        ------
        Dict[str, Union[List[str], List[int]]]
            Dictionary conatining list of unprocessed partitions for
            the tables passed, given the ETL stages specified.
        """
        stages_list = [v for _, v in stages.items()]
        tables_to_use = self.db.tables_to_use(
            tables, stages=stages_list, tf_types=tf_types
        )
        transform_partitions_dict = {}
        for table in tables_to_use:
            transform_partitions_dict[table] = self._list_unprocessed_partitions(
                table, input_stage=stages["input"], output_stage=stages["output"]
            )

        return transform_partitions_dict

    def _tf_args(
        self,
        table_name: str,
        stages: Dict[str, str] = {"input": "raw-hist", "output": "curated"},
    ) -> Tuple[str, str, str]:
        """Transformation arguments for specified table

        Returns a tuple consisting of the
        transform type, input S3 path and output S3 path
        for the given table and stages.

        Params
        ------
        table_name: str
            Name of the table to create temp tables for.

        stages: Dict[str, str]
            Dictionary of the form
            {"input": "input_stage_name", "output": "output_stage_name"}

        Return
        ------
        Tuple[str, str, str]
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

    def _get_secrets(self, secret_key: str) -> str:
        """Return secret from parameter store

        Returns the given secret from the EC2 parameter store
        for the pipeline. Note, IAM_ROLE env variable needs to
        be set for this method.

        Params
        ------
        secret_key: str
            Name of the secret to retrieve.

        Return
        ------
        str
            Secret value from parameter store.
        """
        iam_role = os.environ["IAM_ROLE"]
        store = EC2ParameterStore(region_name=aws_region)

        param_path = os.path.join("/alpha/airflow/", iam_role, "secrets", self.db.name)

        secrets = store.get_parameters_by_path(param_path)

        return secrets[secret_key]

    @staticmethod
    def _cleanup_partitions(base_data_path: str, partitions: List[str]):
        """Deletes partitions data

        Deletes data against partitions specified

        Params
        ------
        base_data_path: str
            Base directory for partitions in S3

        partitions: List[str]
            List of s3 partitions to delete
        """
        for prt in partitions:
            prt_path = os.path.join(base_data_path, prt)
            wr.s3.delete_objects(_add_slash(prt_path))
