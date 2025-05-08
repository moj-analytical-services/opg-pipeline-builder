import json
import logging
import os
import re
from typing import Dict, Optional, Union

import numpy as np
from dataengineeringutils3.s3 import read_json_from_s3
from jinja2 import Template

from .base import BaseTransformations

_logger: logging.Logger = logging.getLogger(__name__)


class PandasTransformations(BaseTransformations):
    add_partition_column: Optional[bool] = False
    attributes_file: Optional[Union[str, None]] = None
    attributes: Optional[Union[dict, None]] = None
    extract_header_values: Optional[Union[Dict[str, str], None]] = None

    @staticmethod
    def remove_columns_not_in_metadata(df, meta):
        _logger.info("Removing columns in data not in metadata")
        df_columns = df.columns.to_list()
        meta_columns = meta.column_names
        columns_to_drop = [c for c in df_columns if c not in meta_columns]
        return df.drop(columns=columns_to_drop, inplace=False)

    def set_colnames_to_lower(self, df):
        _logger.info("Setting data column names to lower case")
        df.columns = [x.lower() for x in df.columns]
        return df

    def add_attributes_from_headers_and_rename(self, df, output_meta):
        extract_header_values = self.extract_header_values
        if extract_header_values is not None:
            _logger.info("Adding attributes to data from column headers")
            header_field_name = extract_header_values.get("field_name")
            header_regex = extract_header_values.get("header_regex")
            headers = df.columns.to_list()

            header_values = [(c, re.search(header_regex, c)) for c in headers]

            header_values = [(c, v[0]) for c, v in header_values if v is not None]
            headers_in_scope = [c for c, _ in header_values]

            unique_values = set([v for _, v in header_values])

            mapping = [
                {
                    "value": v,
                    "fields": [
                        re.sub(header_regex, "", c)
                        for c, v0 in header_values
                        if v0 == v
                        and re.sub(
                            header_regex,
                            "",
                            c,
                        )
                        in output_meta.column_names
                    ],
                }
                for v in unique_values
            ]

            header_field_value = (
                [json.dumps(mapping)] if len(unique_values) > 0 else [None]
            )
            df[header_field_name] = header_field_value * df.shape[0]
            df.columns = [
                re.sub(header_regex, "", c) if c in headers_in_scope else c
                for c in df.columns
            ]

        return df

    def add_primary_partition_column(self, df, partition_value):
        if self.add_partition_column:
            _logger.info("Adding primary partition column")
            primary_partition = self.db.primary_partition_name()
            df[primary_partition] = int(
                re.sub(f"{primary_partition}=", "", partition_value)
            )
        return df

    def add_etl_column(self, df):
        _logger.info("Adding etl version column")
        tag = os.environ["GITHUB_TAG"]
        db_prefix = os.environ.get("ATHENA_DB_PREFIX", "")
        col_name = "etl_version" if db_prefix == "" else f"{db_prefix}_etl_version"
        df[col_name] = tag
        return df

    def add_attributes_from_json_file(self, df):
        if self.attributes_file is not None:
            _logger.info("Adding attributes to data from file")
            attributes_fp = (
                Template(self.attributes_file).render(db=self.db.name, env=self.db.env)
                if self.attributes_file is not None
                else None
            )

            attr_dict = (
                read_json_from_s3(attributes_fp) if attributes_fp is not None else {}
            )

            for k, v in attr_dict.items():
                if k in df.columns.to_list():
                    raise ValueError(f"{k} already set")
                df[k] = v

        return df

    def add_attributes_from_config(self, df):
        _logger.info("Adding attributes to data from config")
        attr_dict = self.attributes if self.attributes is not None else {}
        for k, v in attr_dict.items():
            if k in df.columns.to_list():
                raise ValueError(f"{k} already set")
            df[k] = v
        return df

    @staticmethod
    def remove_null_columns(df, input_meta):
        _logger.info("Removing null type columns from data")
        null_columns = [
            c["name"].lower() for c in input_meta.columns if c["type"] == "null"
        ]
        if null_columns:
            df = df.drop(columns=null_columns, inplace=False)
        return df

    @staticmethod
    def create_null_columns_for_columns_in_meta_not_in_data(df, output_meta):
        _logger.info("Creating null columns for columns in metadata not in data")
        column_names = output_meta.column_names
        partitions = output_meta.partitions
        df_columns = df.columns.tolist()
        for column_name in column_names:
            if column_name not in partitions and column_name not in df_columns:
                df[column_name] = np.NaN
        return df

    def input_transform_methods(self, df, input_meta):
        _logger.info("Running input transformations on data")
        df = self.set_colnames_to_lower(df)
        df = self.remove_null_columns(df, input_meta)
        return df

    def output_transform_methods(self, df, partition, output_meta):
        _logger.info("Running output transformations on data")
        df = self.add_attributes_from_headers_and_rename(df, output_meta)
        df = self.add_primary_partition_column(df, partition)
        df = self.add_etl_column(df)
        df = self.add_attributes_from_json_file(df)
        df = self.add_attributes_from_config(df)
        df = self.remove_columns_not_in_metadata(df, output_meta)
        df = self.create_null_columns_for_columns_in_meta_not_in_data(df, output_meta)
        return df

    def default_transform(self, df, partition, input_meta, output_meta):
        df = self.input_transform_methods(df, input_meta)
        df = self.output_transform_methods(df, partition, output_meta)
        return df

    def custom_transform(self, df, table_name, partition, input_meta, output_meta):
        try:
            transform = getattr(self, f"{table_name}_transform")
            df = transform(df, partition, input_meta, output_meta)
        except AttributeError:
            df = self.default_transform(df, partition, input_meta, output_meta)
        return df

    def derived_transform(self, df, table_name, partition, input_meta, output_meta):
        return self.custom_transform(
            df,
            table_name,
            partition,
            input_meta,
            output_meta,
        )
