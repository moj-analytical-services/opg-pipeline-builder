import os
import re
from copy import deepcopy
from pathlib import Path
from typing import Any

from dataengineeringutils3.s3 import s3_path_to_bucket_key
from mojap_metadata import Metadata
from mojap_metadata.converters.arrow_converter import ArrowConverter
from pyarrow import Schema
from pyarrow.fs import S3FileSystem
from pyarrow.json import read_json
from pyarrow.parquet import read_schema

from .constants import aws_region


class SchemaReader:
    """General class for reading file schemas

    Class for reading schemas for parquet and json files in S3.
    Optimised to read parquet schemas without reading the whole
    object into memory. Uses pyarrow under the hood. Has options
    to convert to mojap meta standard.

    Methods
    -------
    read_schema(
        s3_path,
        moj_meta=False,
        ext=None
    )
        Reads a schema from an object in S3 and optionally converts
        it to a mojap meta object. Infers file format from extension,
        but if extension not provided, you can use ext argument to
        specify the schema reader to use.

    check_schemas_match(
        s3_paths,
        expected_meta,
        keep_partitions=False,
        mojap_base=True,
        ext=None
    )
        Checks whether a list of objects match the expected moj meta
        provided. This can be either the local filepath to the expected moj meta
        or a moj meta object. mojap_base can be used to convert meta to pyarrow
        schemas for comparison.
    """

    def __init__(self) -> None:
        """
        Parameters
        ----------
        s3 : S3FileSystem
            A S3FileSystem object from pyarrow
        """
        self.s3 = S3FileSystem(region=aws_region)

    def __repr__(self) -> str:
        return f"""
        SchemaReader object:
            s3: pyarrow._s3fs.S3FileSystem, aws_region={aws_region}
        """

    def _read_parquet_schema(self, s3_path: str) -> Schema:
        """Private method for reading parquet schemas

        Private method for reading a schema for a parquet file in S3.
        This method shouldn't be called directly. Use read_schema
        instead.

        Parameters
        ----------
        s3_path : str
            S3 path for the parquet file

        Return
        ------
        Schema
            PyArrow schema for the parquet file
        """
        b, k = s3_path_to_bucket_key(s3_path)
        pa_pth = os.path.join(b, k)
        with self.s3.open_input_file(pa_pth) as file:
            schema = read_schema(file)
        return schema

    def _read_json_schema(self, s3_path: str) -> Schema:
        """Private method for reading json schemas

        Private method for reading a schema for a json file in S3.
        This method shouldn't be called directly. Use read_schema
        instead.

        Parameters
        ----------
        s3_path : str
            S3 path for the json file

        Return
        ------
        Schema
            PyArrow schema for the json file
        """
        b, k = s3_path_to_bucket_key(s3_path)
        pa_pth = os.path.join(b, k)
        with self.s3.open_input_file(pa_pth) as file:
            json_data = read_json(file)
            schema = json_data.schema
        return schema

    @staticmethod
    def _split_type(x: str) -> list[str]:
        no_commas = x.count(", ")
        split_x = x.split(", ")
        i = 0
        indicies: list[int] = []
        split = []

        while i <= no_commas:
            max_index = max([*indicies, 0])
            i += 1

            val_i = ", ".join(split_x[max_index:i])
            count_open_brackets = val_i.count("<")
            count_close_bracket = val_i.count(">")

            if count_open_brackets == count_close_bracket:
                indicies.append(i)
                split.append(val_i)

        return split

    @classmethod
    def _unpack_type(cls, value: str, contains_field: bool = True) -> dict[Any, Any]:
        if contains_field:
            split_type = value.split(":")
            field = split_type[0]
            ftype = re.sub(f"^{field}:", "", value)
        else:
            ftype = value

        if ftype.startswith("struct<") and ftype.endswith(">"):
            new_type = re.sub("^struct<|>$", "", ftype)
            split_new_type = cls._split_type(new_type)
            final_vals = [cls._unpack_type(t) for t in split_new_type]
        elif ftype.startswith("list<") and ftype.endswith(">"):
            new_type = re.sub("^list<|>$", "", ftype)
            if new_type.startswith("struct"):
                final_vals = cls._unpack_type(new_type, contains_field=False)
            else:
                final_vals = ftype
        else:
            final_vals = ftype

        full_final_val = {field: final_vals} if contains_field else final_vals

        return full_final_val

    @classmethod
    def _struct_validator(
        cls, col_type: Any, meta_col_type: str, unpack: bool = True
    ) -> bool:
        valid = True

        if unpack:
            meta_col_type = cls._unpack_type(meta_col_type, contains_field=False)
            col_type = cls._unpack_type(col_type, contains_field=False)

        ds_map = {}
        for ds_col in col_type:
            ds_name, ds_type = list(ds_col.items())[0]
            ds_map[ds_name] = ds_type

        for ms_col in meta_col_type:
            ms_name, ms_type = list(ms_col.items())[0]
            if ms_name in ds_map:
                cds_type = ds_map[ms_name]
                if isinstance(ms_type, str):
                    if ms_type != cds_type:
                        valid = False
                        break
                else:
                    if not cls._struct_validator(cds_type, ms_type, unpack=False):
                        valid = False
                        break
            else:
                valid = False
                break

        return valid

    @classmethod
    def _validate(cls, data_columns, meta_columns) -> bool:
        valid = True
        data_column_lookup = {c["name"]: c["type"] for c in data_columns}
        for col in meta_columns:
            col_name = col["name"]
            col_type = col["type"]
            if col_name in data_column_lookup:
                data_col_type = data_column_lookup[col_name]
                if "struct<" in col_type:
                    if not cls._struct_validator(data_col_type, col_type):
                        valid = False
                        break
                else:
                    if data_column_lookup[col_name] != col_type:
                        valid = False
                        break
            else:
                valid = False
                break

        return valid

    def read_schema(
        self, s3_path: str, moj_meta: bool = False, ext: str | None = None
    ) -> Metadata | Schema:
        """Method for reading schemas

        Reads a schema from an object in S3 and optionally converts
        it to a mojap meta object. Infers file format from extension,
        but if extension not provided, you can use ext argument to
        specify the schema reader to use.

        Parameters
        ----------
        s3_path : str
            S3 path for the file
        moj_meta: bool
            Whether to return the schema as a MoJ
            metadata object
        ext: str
            File extension of the file, if not
            specified in the s3_path

        Return
        ------
        Schema
            PyArrow schema or MoJ Metadata object
            for the file in question
        """
        if ext is None:
            ext = Path(s3_path).suffix.replace(".", "")

        reader = getattr(self, f"_read_{ext}_schema")
        schema = reader(s3_path)

        if moj_meta:
            ac = ArrowConverter()
            schema = ac.generate_to_meta(schema)

        return schema

    def check_schemas_match(
        self,
        s3_paths: list[str],
        expected_meta: Metadata | str,
        keep_partitions: bool = False,
        mojap_base: bool = True,
        ext: str | None = None,
    ) -> bool:
        """Method for checking files match a schema

        Checks whether a list of objects match the expected moj meta
        provided. This can be either the local filepath to the expected moj meta
        or a moj meta object. mojap_base can be used to convert meta to pyarrow
        schemas for comparison.

        Parameters
        ----------
        s3_paths : str
            List of S3 paths for the files to check
        expected_meta: Union[Metadata, str]
            MoJ metadata or local path to meta json
        keep_partitions: bool
            Include partition columns in comparison
        moj_base: bool
            Whether to compare using the MoJ Meta
            or PyArrow schema
        ext: str
            File extension of the files, if not
            specified in the s3_paths

        Return
        ------
        bool
            True if files match the given metadata
        """
        if isinstance(expected_meta, str):
            expected_meta = Metadata.from_json(expected_meta)

        if keep_partitions is False:
            prts = deepcopy(expected_meta.partitions)
            for prt in prts:
                expected_meta.remove_column(prt)

        ac = ArrowConverter()
        if mojap_base is False:
            expected_schema = ac.generate_from_meta(expected_meta)
        else:
            expected_schema = [
                {"name": c["name"], "type": c["type"]} for c in expected_meta.columns
            ]
            expected_schema = sorted(expected_schema, key=lambda x: x["name"])

        print(expected_schema)

        return_val = True
        for pth in s3_paths:
            schema = self.read_schema(pth, moj_meta=mojap_base, ext=ext)
            if mojap_base:
                schema = [
                    {"name": c["name"], "type": c["type"]} for c in schema.columns
                ]
                schema = sorted(schema, key=lambda x: x["name"])
                print(schema)
                if not self._validate(schema, expected_schema):
                    return_val = False

            else:
                if schema != expected_schema:
                    return_val = False

        return return_val
