import json
import os
import re
from pathlib import Path
from typing import List, Optional

import pandas as pd
from jinja2 import Template
from mojap_metadata import Metadata

from ..utils.exceptions import NoFilesForTable
from ..utils.schema_reader import SchemaReader
from .base import BaseTransformEngine


class EnrichMetaTransformEngine(BaseTransformEngine):
    raw_data_stage: Optional[str] = "land"

    @staticmethod
    def _get_meta_template_path(table_meta_path: str):
        table_meta_pathlib = Path(table_meta_path)
        table_meta_template_path = os.path.join(
            table_meta_pathlib.parent.as_posix(), "templates", table_meta_pathlib.name
        )
        return table_meta_template_path

    @staticmethod
    def _get_column_names_from_csv(filepath: str) -> List[str]:
        table_head_df = pd.read_csv(filepath, nrows=0)
        table_columns = table_head_df.columns.to_list()
        return table_columns

    @staticmethod
    def _get_column_names_from_parquet(filepath: str) -> List[str]:
        sr = SchemaReader()
        meta = sr.read_schema(s3_path=filepath, moj_meta=True)
        table_columns = meta.column_names
        return table_columns

    @classmethod
    def _get_column_names(cls, filepath: str) -> List[str]:
        ext = Path(filepath).suffix.replace(".", "")
        method_name = f"_get_column_names_from_{ext}"
        method = getattr(cls, method_name)
        return method(filepath)

    def _extract_jinja_values_from_column_names(
        self, table_name: str, raw_data_stage: Optional[str] = "land"
    ):
        table_files = sorted(
            self.utils.list_table_files(
                stage=raw_data_stage,
                table_name=table_name,
            ),
            reverse=True,
        )

        if len(table_files) == 0:
            NoFilesForTable(f"No files present for {table_name} in {raw_data_stage}")

        table_columns = self._get_column_names(table_files[0])

        regex_dict = {
            col: re.search("([0-9]{2}(?=[a-zA-Z]+))|([0-9]{2}$)", col)
            for col in table_columns
        }

        jinja_upper_dict = {
            re.sub("([0-9]{2}(?=[a-zA-Z]+))|([0-9]{2}$)", "", col).upper()
            + "_year": val[0]
            for col, val in regex_dict.items()
            if val is not None
        }

        jinja_lower_dict = {
            re.sub("([0-9]{2}(?=[a-zA-Z]+))|([0-9]{2}$)", "", col).lower()
            + "_year": val[0]
            for col, val in regex_dict.items()
            if val is not None
        }

        jinja_dict = {**jinja_lower_dict, **jinja_upper_dict}

        return jinja_dict

    def _enrich_metadata(
        self,
        tables: List[str],
        meta_stage: Optional[str] = "raw-hist",
        raw_data_stage: Optional[str] = "land",
    ):
        for table_name in tables:
            table = self.db.table(table_name)

            table_meta_path = table.table_meta_paths().get(meta_stage)
            table_meta_template_path = self._get_meta_template_path(table_meta_path)

            jinja_kwargs = self._extract_jinja_values_from_column_names(
                table_name=table_name, raw_data_stage=raw_data_stage
            )

            with open(table_meta_template_path, "r") as f:
                raw_table_meta = f.read()

            table_meta_template = Template(raw_table_meta)
            rendered_meta_str = table_meta_template.render(**jinja_kwargs)

            rendered_meta_dict = json.loads(rendered_meta_str)
            rendered_meta = Metadata.from_dict(rendered_meta_dict)
            rendered_meta.to_json(table_meta_path, indent=4)
