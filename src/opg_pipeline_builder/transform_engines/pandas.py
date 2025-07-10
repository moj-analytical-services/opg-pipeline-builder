import logging
import os
from pathlib import Path
from typing import Any

import awswrangler as wr
from arrow_pd_parser import reader, writer
from mojap_metadata import Metadata

from ..database import Database
from ..utils.constants import get_source_db
from ..validator import PipelineConfig
from .enrich_meta import EnrichMetaTransformEngine
from .transforms.panda import PandasTransformations
from .utils.utils import TransformEngineUtils

_logger: logging.Logger = logging.getLogger(__name__)


class PandasTransformEngine(EnrichMetaTransformEngine):
    chunk_memory_size: int | str = 100_000
    chunk_rest_threshold: int = 500_000_000
    add_partition_column: bool = False
    attributes_file: str | None = None
    attributes: dict | None = None
    extract_header_values: dict[str, str] | None = None
    enrich_meta: bool = True
    final_partition_stage: str = "curated"
    transforms: PandasTransformations | None = None

    def __init__(
        self,
        config: PipelineConfig,
        db_name: str | None = None,
        utils: TransformEngineUtils | None = None,
        transforms: PandasTransformations | None = None,
        **kwargs: dict[Any, Any],
    ) -> None:
        if db_name is None:
            _logger.debug("Setting database for engine from environment")
            db_name = get_source_db()
            _logger.debug(f"Engine database environment variable set to {db_name}")

        db = Database(config=config)
        utils = TransformEngineUtils(db=db) if utils is None else utils

        super().__init__(db_name=db_name, utils=utils, **kwargs)

        if transforms is None:
            transforms = PandasTransformations(**self.dict())

        self.transforms = transforms

    @classmethod
    def _remove_columns_in_meta_not_in_data(
        cls,
        filepath: str,
        metadata: Metadata,
    ) -> Metadata:
        data_column_names = cls._get_column_names(filepath)

        for meta_column_name in metadata.column_names:
            if meta_column_name not in data_column_names:
                if meta_column_name.lower() in data_column_names:
                    column = metadata.get_column(meta_column_name)
                    column["name"] = meta_column_name.lower()
                    metadata.update_column(column)

                elif meta_column_name.upper() in data_column_names:
                    column = metadata.get_column(meta_column_name)
                    column["name"] = meta_column_name.upper()
                    metadata.update_column(column)

                else:
                    metadata.remove_column(meta_column_name)

        return metadata

    def _apply_iterable(
        self,
        filepath: str,
        table_name: str,
        transform_type: str,
        partition: str,
        output_partition_path: str,
        output_partition_format: str,
        input_meta: Metadata,
        output_meta: Metadata,
    ) -> None:
        _logger.info(f"Updating metadata for {table_name} from data")
        updated_input_meta = self._remove_columns_in_meta_not_in_data(
            filepath, input_meta
        )

        _logger.info("Reading data in chunks")
        dfs = reader.read(
            filepath, metadata=updated_input_meta, chunksize=self.chunk_memory_size
        )

        _logger.info("Looping through chunks")
        i = 0
        for df in dfs:
            _logger.info(f"Transforming chunk {i}")
            if transform_type == "custom":
                tf_df = self.transforms.custom_transform(
                    df, table_name, partition, updated_input_meta, output_meta
                )
            else:
                _logger.info(df)
                tf_df = self.transforms.default_transform(
                    df, partition, updated_input_meta, output_meta
                )

            _logger.info(f"Getting output filepath for chunk {i}")
            output_filepath = os.path.join(
                output_partition_path,
                Path(filepath).stem
                + f"-part-{i}"
                + (
                    output_partition_format
                    if output_partition_format.startswith(".")
                    else "." + output_partition_format
                ),
            )
            _logger.info(f"Output filepath: {output_filepath}")

            _logger.info(f"Writing chunk {i}")
            writer.write(
                tf_df,
                output_path=output_filepath,
                metadata=output_meta,
            )
            i += 1

    def _apply(
        self,
        filepath: str,
        table_name: str,
        transform_type: str,
        partition: str,
        output_partition_path: str,
        output_partition_format: str,
        input_meta: Metadata,
        output_meta: Metadata,
    ) -> None:
        _logger.info(f"Updating metadata for {table_name} from data")
        updated_input_meta = self._remove_columns_in_meta_not_in_data(
            filepath, input_meta
        )

        _logger.info(f"Reading filepath {filepath} for {table_name}")
        df = reader.read(filepath, metadata=updated_input_meta)

        if transform_type == "custom":
            _logger.info(
                f"Applying custom transformation to {filepath} for {table_name}"
            )
            df = self.transforms.custom_transform(
                df, table_name, partition, updated_input_meta, output_meta
            )
        else:
            _logger.info(
                f"Applying default transformation to {filepath} for {table_name}"
            )
            df = self.transforms.default_transform(
                df, partition, updated_input_meta, output_meta
            )

        output_filepath = os.path.join(
            output_partition_path,
            Path(filepath).stem
            + (
                output_partition_format
                if output_partition_format.startswith(".")
                else "." + output_partition_format
            ),
        )

        _logger.info(f"Writing transformed file for {table_name} to {output_filepath}")
        writer.write(
            df,
            output_path=output_filepath,
            metadata=output_meta,
        )

    def _transform(
        self,
        table_name: str,
        transform_type: str,
        partition: str,
        input_partition_path: str,
        output_partition_path: str,
        output_format: str,
        input_meta: Metadata,
        output_meta: Metadata,
    ) -> None:
        files_to_process = wr.s3.list_objects(input_partition_path)
        for file in files_to_process:
            partition_input_info = wr.s3.describe_objects(file)
            max_length = max(
                [v.get("ContentLength") for _, v in partition_input_info.items()]
            )

            if max_length > self.chunk_rest_threshold:
                _logger.info(f"Processing file {file} for {table_name} in chunks")
                self._apply_iterable(
                    file,
                    table_name,
                    transform_type,
                    partition,
                    output_partition_path,
                    output_format,
                    input_meta,
                    output_meta,
                )
            else:
                _logger.info(f"Processing file {file} for {table_name}")
                self._apply(
                    file,
                    table_name,
                    transform_type,
                    partition,
                    output_partition_path,
                    output_format,
                    input_meta,
                    output_meta,
                )

    def run(self, tables: list[str], stages: dict[str, str]) -> None:
        input_stage = stages.get("input")
        output_stage = stages.get("output")

        if self.enrich_meta:
            _logger.info("Enriching metadata based on data")
            self._enrich_metadata(
                tables, meta_stage=input_stage, raw_data_stage=input_stage
            )

        _logger.info("Checking for unprocessed partitions")
        unprocessed_partitions = [
            (
                table_name,
                self.utils.list_unprocessed_partitions(
                    table_name,
                    input_stage=input_stage,
                    output_stage=self.final_partition_stage,
                ),
            )
            for table_name in tables
        ]
        unprocessed_partitions = [tp for tp in unprocessed_partitions if tp[1]]

        for table_name, partitions in unprocessed_partitions:
            _logger.info(f"Starting job for {table_name}")
            table = self.db.table(table_name)
            transform_type = table.transform_type()

            input_path = table.get_table_path(input_stage)
            output_path = table.get_table_path(output_stage)

            input_meta = table.get_table_metadata(input_stage)
            output_meta = table.get_table_metadata(output_stage)
            output_format = (
                table.table_file_formats().get(output_stage).get("file_format")
            )

            for partition in partitions:
                partition_input_path = os.path.join(input_path, partition)
                partition_output_path = os.path.join(output_path, partition)

                _logger.info(f"Processing {partition} for {table_name}")
                self._transform(
                    table_name,
                    transform_type,
                    partition,
                    partition_input_path,
                    partition_output_path,
                    output_format,
                    input_meta,
                    output_meta,
                )
