import logging
import os
from concurrent.futures import Future, ProcessPoolExecutor
from copy import deepcopy
from functools import partial
from typing import List, Optional, Union

import awswrangler as wr
from data_linter import validation
from dataengineeringutils3.s3 import get_filepaths_from_s3_folder
from jsonschema import exceptions, validate

from ..utils.constants import get_dag_timestamp, get_multiprocessing_settings
from ..utils.utils import extract_mojap_partition, get_modified_filepaths_from_s3_folder
from .base import BaseTransformEngine

_logger: logging.Logger = logging.getLogger(__name__)


class DataLinterTransformEngine(BaseTransformEngine):
    """
    mp_args: Optional[Union[dict, None]]
        Multiprocessing arguments for data_linter. See validator
        in opg_etl.utils.linter_utils for structure of the dictionary
        to pass.

    dag_timestamp: Optional[Union[int, None]]
        Integer timestamp for the pipeline run. Only needs to be specified
        if temp_staging is set to True in mp_args.
    """

    mp_args: Optional[Union[dict, None]] = None
    dag_timestamp: Optional[Union[int, None]] = get_dag_timestamp()

    @staticmethod
    def _callback(future: Future, worker: int):
        _logger.info(f"Worker {worker} complete")

    @staticmethod
    def _validate_mp_args(mp_args: dict) -> None:
        schema = {
            "type": "object",
            "properties": {
                "enable": {"type": "string", "enum": ["local", "pod"]},
                "total_workers": {"type": "integer"},
                "current_worker": {"anyOf": [{"type": "integer"}, {"type": "null"}]},
                "temp_staging": {"anyOf": [{"type": "boolean"}, {"type": "null"}]},
                "close_status": {"type": "boolean"},
            },
        }

        if mp_args is not None:
            if "enable" not in mp_args.keys():
                raise KeyError('mp_args must contain an "enable" key-value pair')

            mp_enable = mp_args["enable"]

            if mp_enable == "local":
                schema = {
                    "type": "object",
                    "properties": {
                        k: v
                        for k, v in schema["properties"].items()
                        if k in ["enable", "temp_staging"]
                    },
                }
            if mp_enable == "pod":
                schema = {**schema, "required": ["enable", "total_workers"]}

            try:
                validate(instance=mp_args, schema=schema)

            except exceptions.ValidationError as e:
                raise ValueError(f"Prod schema validation error: {e}")

            if "total_workers" in mp_args and "current_worker" in mp_args:
                current_worker = mp_args["current_worker"]
                total_workers = mp_args["total_workers"]
                if current_worker > total_workers - 1:
                    raise ValueError("Current worker is out of range.")

    @staticmethod
    def _start_linter(config, mp_args: dict) -> None:
        if mp_args is not None:
            mp_enable = mp_args["enable"]

            if mp_enable == "local":
                max_workers = os.cpu_count()
                config_dc = deepcopy(config)

                _logger.info(
                    (
                        f"Creating parallel run config files for {max_workers}"
                        " (CPU count) workers"
                    )
                )
                validation.para_run_init(max_workers, config_dc)

            elif mp_enable == "pod":
                current_worker = mp_args.get("current_worker", None)
                close_status = mp_args.get("close_status", False)

                if current_worker is None and close_status is False:
                    max_workers = mp_args["total_workers"]
                    _logger.info(
                        (
                            f"Creating parallel run config files for {max_workers}"
                            " (environment) workers"
                        )
                    )
                    validation.para_run_init(max_workers, config)

            else:
                raise ValueError("Unknown mp_args error")

    @classmethod
    def _run_linter(cls, config, mp_args: dict) -> None:
        if mp_args is None:
            _logger.info("Running validation with no multiprocessing")
            validation.run_validation(config)

        elif mp_args["enable"] == "local":
            max_workers = os.cpu_count()
            workers = range(0, max_workers)
            config_dc = [deepcopy(config) for _ in workers]
            _logger.info(f"Running validation with {max_workers} workers")

            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                val_futures = [
                    executor.submit(
                        validation.para_run_validation,
                        config_num=i,
                        config=config_dc[i],
                    )
                    for i in workers
                ]

                for j, val_future in enumerate(val_futures):
                    callback_j = partial(cls._callback, worker=j)
                    val_future.add_done_callback(callback_j)

        elif mp_args["enable"] == "pod":
            worker = mp_args.get("current_worker", None)
            _logger.info(f"Running validation for worker {worker}")
            close_status = mp_args.get("close_status", False)
            if worker is not None and not close_status:
                validation.para_run_validation(worker, config)

        else:
            raise ValueError("Invalid multiprocessing enable argument")

    @staticmethod
    def _close_linter(config, mp_args: dict) -> None:
        if mp_args is not None:
            mp_enable = mp_args["enable"]
            if mp_enable == "local":
                config_dc = deepcopy(config)
                _logger.info("Collecting statuses and moving files")
                validation.para_collect_all_status(config_dc)
                validation.para_collect_all_logs(config_dc)

            elif mp_enable == "pod":
                close_status = mp_args.get("close_status")
                if close_status:
                    _logger.info("Collecting statuses and moving files")
                    validation.para_collect_all_status(config)
                    validation.para_collect_all_logs(config)

            else:
                raise ValueError("Unknown mp_args error")

    @staticmethod
    def _move_from_tmp_to_pass(
        config,
        mp_args,
        dag_timestamp: int,
        timestamp_partition_name: Optional[str] = "mojap_file_land_timestamp",
    ) -> None:
        proceed = False
        if mp_args is not None:
            mp_enable = mp_args["enable"]
            tmp_staging = mp_args.get("temp_staging", False)
            tmp_staging = False if tmp_staging is None else tmp_staging

            if mp_enable == "local" and tmp_staging:
                proceed = True

            elif mp_enable == "pod":
                if mp_args["close_status"] and tmp_staging:
                    proceed = True

        if proceed:
            pass_tmp_path = config["pass-base-path"]
            if "temp" not in pass_tmp_path:
                raise ValueError("Expecting temp in pass base path")

            for tbl_name in config["tables"]:
                tbl_tmp_path = os.path.join(pass_tmp_path, tbl_name)
                tbl_tmp_files = get_modified_filepaths_from_s3_folder(tbl_tmp_path)

                old_prts = [
                    extract_mojap_partition(
                        f, timestamp_partition_name=timestamp_partition_name
                    )
                    for f in tbl_tmp_files
                ]

                if old_prts:
                    old_prt = old_prts[0]

                    if len(set(old_prts)) > 1:
                        msg = "Process is designed to only run on a single partition of"
                        "raw data. More than one partition was found. Partitions:"
                        f"{','.join(old_prts)}"
                        raise ValueError(msg)

                    new_prt = f"{timestamp_partition_name}={dag_timestamp}"

                    tbl_perm_files = tbl_tmp_files[0].replace("temp/", "")
                    tbl_perm_dag_files = tbl_perm_files.replace(old_prt, new_prt)
                    target_path = tbl_perm_dag_files.rsplit("/", 1)[0]

                    _logger.info(
                        f"Copying files for table {tbl_name} from temporary directory"
                    )
                    wr.s3.copy_objects(tbl_tmp_files, tbl_tmp_path, str(target_path))

                _logger.info(
                    f"Deleting files in temporary directory for table {tbl_name}"
                )
                wr.s3.delete_objects(tbl_tmp_files)

    def _get_database_linter_config(
        self, tables: List[str], stage: str, temporary_staging: bool
    ) -> dict:
        return self.db.lint_config(
            tables, meta_stage=stage, tmp_staging=temporary_staging
        )

    def run(self, tables: List[str], stage: str = "raw_hist") -> None:
        """Runs data_linter based on db config over the given tables

        Runs data_linter over data in land and moves it to
        the stage specified. This method also splits the
        linting process up depending on the multiprocessing
        arguments set. If temp staging is set in mp_args, then
        data will be transferred to a temporary staging area
        and then moved to a partition with the value of
        dag_timestamp.

        Params
        ------
        tables: List[str]
            List of table names.

        stage: str
            ETL stage for linter metadata

        Return
        ------
        None
        """
        mp_args = self.mp_args
        dag_timestamp = self.dag_timestamp

        if mp_args is None:
            _logger.info("Setting multiprocessing arguments from environment")
            mp_args = get_multiprocessing_settings()

        if dag_timestamp is None:
            _logger.info("Setting run timestamp from environment")
            dag_timestamp = get_dag_timestamp()

        _logger.info("Validating multiprocessing arguments")
        self._validate_mp_args(mp_args)

        tmp_staging = False
        if mp_args is not None:
            tmp_staging = mp_args.get("temp_staging", False)
            tmp_staging = False if tmp_staging is None else tmp_staging
            _logger.info(f"Using temporary staging directory: {tmp_staging}")

        db = self.db
        primary_partition = db.primary_partition_name()
        db_config = self._get_database_linter_config(tables, stage, tmp_staging)

        if db_config:
            if get_filepaths_from_s3_folder(db._land_path + "/"):
                _logger.info("Running data linter")
                self._start_linter(db_config, mp_args)
                self._run_linter(db_config, mp_args)
                self._close_linter(db_config, mp_args)
                self._move_from_tmp_to_pass(
                    db_config,
                    mp_args=mp_args,
                    dag_timestamp=dag_timestamp,
                    timestamp_partition_name=primary_partition,
                )

            else:
                _logger.info(f"No files for {db.name} in land to validate")

        else:
            _logger.info(f"{db.name} hasn't got linting options")
