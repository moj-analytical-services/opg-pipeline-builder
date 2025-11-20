from __future__ import annotations

import os
from copy import deepcopy
from typing import Any

from croniter import croniter
from jinja2 import Template
from mojap_metadata import Metadata

from .utils.constants import get_env, get_metadata_path, get_source_tbls
from .validator import PipelineConfig


class Database:
    """Database class for processing settings from a given config.

    Class that represents and manipulates database config settings,
    including metadata, linting configs, transformation arguments
    and storage locations in S3.

    Methods:
        **table**:
            Returns a DatabaseTable object for the given table name.

        **tables_to_use**:
            Returns a subset of tables as a list based on stages
            specified (e.g. land, curated) and table transform types to
            include (e.g. default, custom, derived).

        **lint_config**:
            Returns a data_linter config for the tables and stage
            specified. If tmp_staging is set to True, passing files
            will be placed in a temporary directory in S3.

        **transform_args**:
            Returns a dictionary containing transformation parameters
            for the tables supplied. This will include input and output
            data locations, transformation types (e.g. default, custom,
            derived) etc.

        **shared_sql_paths**:
            Returns a list of tuples consisting of the temp table
            name and temp table sql path for sql shared accross the
            given transform types.
    """

    def __init__(self, config: PipelineConfig) -> None:
        self._name = config.db_name
        self._config = config.model_dump()

        tables = list(self._config["tables"].keys())
        paths: dict[str, str] = self._config["paths"]

        lpt: Template = Template(paths.get("land", ""))
        rhpt: Template = Template(paths.get("raw_hist", ""))
        rpt: Template = Template(paths.get("raw", ""))
        ppt: Template = Template(paths.get("processed", ""))
        cpt: Template = Template(paths.get("curated", ""))
        dpt: Template = Template(paths.get("derived", ""))

        lp = lpt.render(env=get_env(), db=self._name)
        rp = rpt.render(env=get_env(), db=self._name)
        rhp = rhpt.render(env=get_env(), db=self._name)
        pp = ppt.render(env=get_env(), db=self._name)
        cp = cpt.render(env=get_env(), db=self._name)
        dp = dpt.render(env=get_env(), db=self._name)

        self._env = get_env()
        self._tables = tables
        self._metadata_path = get_metadata_path(self._name)
        self._land_path = lp
        self._raw_path = rp
        self._raw_hist_path = rhp
        self._processed_path = pp
        self._curated_path = cp
        self._derived_path = dp

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Database):
            return NotImplemented

        else:
            return other.__dict__ == self.__dict__

    @property
    def name(self) -> str:
        return self._name

    @property
    def env(self) -> str:
        return self._env

    @property
    def tables(self) -> list[str]:
        return self._tables

    @property
    def config(self) -> dict[str, str]:
        return self._config

    @property
    def metadata_path(self) -> str:
        return self._metadata_path

    @property
    def land_path(self) -> str:
        return self._land_path

    @property
    def raw_path(self) -> str:
        return self._raw_path

    @property
    def raw_hist_path(self) -> str:
        return self._raw_hist_path

    @property
    def processed_path(self) -> str:
        return self._processed_path

    @property
    def curated_path(self) -> str:
        return self._curated_path

    @property
    def derived_path(self) -> str:
        return self._derived_path

    def table(self, table_name: str) -> DatabaseTable:
        """Returns an DatabaseTable object for the given table name.

        Parameters:
            table_name (str):
                Name of the table to return as an DatabaseTable. This must
                be a table specified in the database's config.

        Returns:
            DatabaseTable: The DatabaseTable object for the given table
        """
        return DatabaseTable(table_name=table_name, db=self)

    def _validate_tables(
        self,
        table_list: list[str],
        stages: list[str] | None = None,
        tf_types: list[str] | None = None,
    ) -> list[str]:
        """Private method for filtering tables given
        ETL stages used, and table transform types. Use
        `tables_to_use` instead.

        Parameters:
            table_list (list[str]):
                List of table names to validate/filter.

            stages (list[str] | None]):
                List of ETL stages to filter tables on.

            tf_types (list[str] | None):
                List of table transform types to filter
                tables on.

        Returns:
            (list[str]): Filtered list of table names.
        """
        cp_table_list = deepcopy(table_list)
        valid_tables = []
        for table in cp_table_list:
            tbl = self.table(table)

            tbl_stages = set(tbl.etl_stages())
            tbl_tf_type = tbl.transform_type()

            test_stages = set(tbl_stages) if stages is None else set(stages)
            test_tf_types = [tbl_tf_type] if tf_types is None else tf_types

            if test_stages.intersection(tbl_stages) and tbl_tf_type in test_tf_types:
                valid_tables.append(table)

        return valid_tables

    def tables_to_use(
        self,
        table_list: list[str] | None = None,
        stages: list[str] | None = None,
        tf_types: list[str] | None = None,
    ) -> list[str]:
        """Method for filtering tables given
        ETL stages used, and table transform types.

        Returns a list of filtered tables. If `table_list`
        is set to `None`, the method will filter all tables

        Parameters:
            table_list (list[str]):
                List of table names to validate/filter.

            stages (list[str] | None):
                List of ETL stages to filter tables on.

            tf_types (list[str] | None):
                List of table transform types to filter
                tables on.

        Returns:
            list[str]): Filtered list of table names.
        """
        if table_list is None:
            table_list = self.tables if get_source_tbls() is None else get_source_tbls()

        up_tbl_list = self._validate_tables(table_list, stages, tf_types)

        tables = up_tbl_list

        return tables

    def lint_config(
        self,
        tables: list[str] | None = None,
        meta_stage: str = "raw_hist",
        tmp_staging: bool = False,
    ) -> dict[str, str | bool] | dict[None, None]:
        """Returns data linter config for the db

        Returns a data_linter config for the tables and stage
        specified. If `tmp_staging` is set to `True`, passing files
        will be placed in a temporary directory in S3.

        Parameters:
            tables (list[str] | None]):
                List of tables to include in linter config.

            meta_stage (str):
                ETL stage for meta to check the data against.

            tmp_staging (bool):
                True or False for whether to write passing data
                to a temporary directory in S3.

        Returns:
            dict[str, str | bool] | dict[None, None]: data_linter config dictionary
        """

        if tables is None:
            tables = self.tables if get_source_tbls() is None else get_source_tbls()

        log_suffix = tables[0] + "/" if len(tables) == 1 else ""

        base_path = self.raw_hist_path if meta_stage == "raw_hist" else self.raw_path

        db_config = self._config
        if db_config["db_lint_options"] is not None:
            pass_suffix = "temp/pass/" if tmp_staging else "pass/"
            config_paths = {
                "land-base-path": self.land_path + "/",
                "fail-base-path": os.path.join(base_path, "fail/"),
                "pass-base-path": os.path.join(base_path, pass_suffix),
                "log-base-path": os.path.join(base_path, f"log/{log_suffix}"),
            }

            db_lint_config = db_config["db_lint_options"]

            lint_config = {**config_paths, **db_lint_config, "tables": {}}

            for table_name in tables:
                table_lint_config = deepcopy(
                    self.table(table_name).lint_config(meta_stage=meta_stage)
                )
                if table_lint_config:
                    lint_config["tables"][table_name] = table_lint_config

        else:
            lint_config = {}

        return lint_config

    def primary_partition_name(self) -> str:
        """Returns the primary partition for the database

        The primary partition name will be returned as specified
        in `db_lint_options` in the pipeline config.

        Returns:
            (str): Primary partition name
        """
        config = self._config
        db_lint_config = config["db_lint_options"]
        partition_name: str = db_lint_config["timestamp-partition-name"]
        return partition_name

    def transform_args(
        self,
        tables: list[str] | None = None,
        tf_types: list[str] | None = None,
        **stages: dict[Any, Any],
    ) -> dict[str, Any]:
        """Transformation arguments for database tables

        Returns a dictionary containing transformation parameters
        for the tables supplied. This will include input and output
        data locations, transformation types (e.g. `default`, `custom`,
        `derived`) etc.

        Parameters:
            tables (list[str] | None):
                Tables in database to return transform arguments for

            tf_types (List[str] | None):
                List of transformation types to filter tables on

            **stages (dict[Any, Any]):
                Dictionary of the form
                `{"table_name: {"input": .., "output": ..}, ...}`
                where input and output values should correspond to
                an ETL stage (e.g. curated)

        Returns:
            dict[str, Any]: Summary dictionary of all the transfomrations
                   to apply to the tables.
        """
        inpt = [stages[k]["input"] for k in stages]
        outpt = [stages[k]["output"] for k in stages]
        db_stages = list(set(inpt + outpt))

        tables = self.tables_to_use(
            table_list=tables, stages=db_stages, tf_types=tf_types
        )

        transform_args = {"db": self._name, "tables": tables}

        transform_args["transforms"] = {}
        for table_name in tables:
            table = self.table(table_name)
            transform_args["transforms"][table_name] = table.transform_args(
                stages[table_name]["input"], stages[table_name]["output"]
            )

        return transform_args

    def shared_sql_paths(self, tf_types: list[str]) -> list[tuple[str, str]]:
        """SQL files used to create shared temporary tables

        Returns a list of tuples consisting of the temp table
        name and temp table sql path for sql shared accross the
        given transform types.

        Parameters:
            tf_types (list[str]):
                List of transformation types to filter sql files on.

        Returns:
            (list[tuple[str, str]]): List of tuples consisting of the
                                     shared sql table name and path.
        """
        db_config = self.config
        shsql_config = db_config.get("shared_sql", {})

        sql_base_path = os.path.join("sql", self.name, "shared")

        init_tf_type = tf_types[0]
        shsql_intersect = shsql_config.get(init_tf_type, [])
        for tf_type in tf_types[1:]:
            shsql_type = shsql_config.get(tf_type, [])
            shsql_intersect = [tbl for tbl in shsql_type if tbl in shsql_intersect]

        sql_tbl_rn = [
            (tbl, os.path.join(sql_base_path, f"{tbl}.sql")) for tbl in shsql_intersect
        ]

        return sql_tbl_rn


class DatabaseTable:
    """Database Table class

    Class that represents and manipulates table config settings,
    including metadata, linting configs, transformation arguments
    and storage locations in S3.

    Methods
    -------
    transform_type(
        self
    )
        Returns the transform type of the given table, as long
        as it is one of default, custom or derived.

    frequency(
        self
    )
        Returns the cron schedule for the table, as long as
        the cron in the config is valid.

    etl_stages(
        self
    )
        Returns a list of the ETL stages specified for the table.

    table_file_formats(
        self
    )
        Returns the file formats of the table for each ETL stage.

    table_data_paths(
        self
    )
        Returns the S3 file paths for the table's data for each
        ETL stage.

    table_meta_paths(
        self
    )
        Returns the local paths for the table's data for each
        ETL stage.

    table_sql_paths(
        self,
        type: str
    )
        Returns a list of tuples consisting of the sql table name
        and sql path for the given type (temp or final).

    input_data(
        self
    )
        Returns a dictionary specifying source database tables
        and their associated data paths and file formats, if the
        table is a derived table. Otherwise returns None.

    lint_config(
        self,
        meta_stage: str = 'raw_hist'
    )
        Returns a config for the table for the stage
        specified.

    transform_args(
        self,
        input_stage: Union[str, None],
        output_stage: str = "curated"
    )
        Returns a dictionary with input and output
        paths and formats for the transformation to be
        applied to the table's data.

    get_table_metadata(
        self,
        stage: str,
        updates: Union[List[Dict[str, str]], None] = None
    )
        Fetches metadata for the table at the specified ETL
        stage. Also updates the metadata columns if provided.

    get_table_path(
        self,
        stage: str
    )
        Returns the path to the data for the table in S3 for
        the ETL stage specified.

    get_cast_cols(
        self
    )
        Returns columns to cast during linting as a list of
        three element tuples consisting of the column name,
        original data type, and casted data type.

    """

    def __init__(self, table_name: str, db: Database) -> None:
        if table_name in db._tables:
            self._db_name = db.name
            self._name = table_name
            self._db = db
            db_config = db._config
            self._config: dict[str, Any] = db_config["tables"][table_name]

        else:
            err = (
                f"Table: '{table_name}' is not present in the config for this database"
            )
            raise KeyError(err)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DatabaseTable):
            return NotImplemented

        else:
            db_name_check = other.db_name == self.db_name
            name_check = other.name == self.name
            config_check = other.config == self.config

            return db_name_check and name_check and config_check

    @property
    def name(self) -> str:
        return self._name

    @property
    def db_name(self) -> str:
        return self._db_name

    @property
    def config(self) -> dict[str, str]:
        return self._config

    @property
    def db(self) -> Database:
        return self._db

    @property
    def optional_arguments(self) -> dict[str, Any] | None:
        return self._config.get("optional_arguments")

    def transform_type(self) -> str:
        """Returns table transform type

        Returns the transform type of the given table, as long
        as it is one of default, custom or derived. This method
        will raise an error otherwise.

        Return
        ------
        str
            One of default, custom or derived
        """
        transform_type: str = self._config["transform_type"]

        if transform_type not in ["default", "custom", "derived"]:
            raise ValueError(
                "Transform type in config should be"
                + " one of default, custom or derived"
            )

        return transform_type

    def frequency(self) -> str:
        """Returns table update frequency

        Returns the table's update frequency if it is
        a valid CRON expression.

        Return
        ------
        str
            Table's update frequency
        """
        frequency: str = self._config["frequency"]

        if croniter.is_valid(frequency) is False:
            raise ValueError("Frequency should be a valid cron expression")

        return frequency

    def etl_stages(self) -> list[str]:
        """Returns table ETL stages

        Returns the a list of ETL stages applied
        to the table.

        Return
        ------
        list[str]
            Table's ETL stages
        """
        etl_stages = list(self._config["etl_stages"].keys())
        return etl_stages

    def table_file_formats(self) -> dict[str, str]:
        """Returns the table's file formats

        Returns the table's file formats at
        each ETL stage.

        Return
        ------
        dict[str, str]
            Key: ETL stage, Value: File format
        """
        table_config = self._config
        input_file_format: dict[str, str] = table_config["etl_stages"]

        return input_file_format

    def table_data_paths(self) -> dict[str, str]:
        """Returns the table's data paths

        Returns the table's data paths at
        each ETL stage.

        Return
        ------
        dict[str, str]
            Key: ETL stage, Value: Data path in S3
        """
        table_name = self._name
        db = self._db

        etl_lt = self.etl_stages()

        etl_path_fun_names = [
            f"""{fun_name.replace("-", "_")}_path""" for fun_name in etl_lt
        ]

        etl_functions = [getattr(db, fun_name) for fun_name in etl_path_fun_names]

        stage_paths = zip(etl_lt, etl_functions)

        table_paths = {
            stage: os.path.join(
                path,
                (
                    table_name
                    if stage not in ["raw", "raw_hist"]
                    else f"pass/{table_name}"
                ),
            )
            for stage, path in stage_paths
        }

        return table_paths

    def table_meta_paths(self) -> dict[str, str]:
        """Returns the table's metadata paths

        Returns the table's metadata paths at
        each ETL stage.

        Return
        ------
        dict[str, str]
            Key: ETL stage, Value: Local metadata path
        """
        transform_type = self.transform_type()
        stages = deepcopy(self.etl_stages())
        if "land" in stages:
            stages.remove("land")

        used_stages = stages if transform_type in ["default", "custom"] else ["derived"]

        table_meta_paths = {
            type: os.path.join(
                f"src/opg_pipeline/old_metadata/{type}", f"{self._name}.json"
            )
            for type in used_stages
        }

        return table_meta_paths

    def table_sql_paths(self, type: str) -> list[tuple[str, str]]:
        """Returns the table's specific SQL tables and paths

        Returns the table's specific SQL tables and paths
        as a list of tuples, for the given SQL query type
        (one of temp or final).

        Return
        ------
        list[tuple[str, str]]
            [(sql_table_name, sql_table_path)]
        """
        tbl_config = self.config
        tbl_sql = tbl_config.get("sql", {})
        tbl_sql_type = tbl_sql.get(type, [])

        sql_dir = os.path.join("sql", self.db_name, self.name)

        sql_paths = [(tbl, os.path.join(sql_dir, f"{tbl}.sql")) for tbl in tbl_sql_type]

        return sql_paths

    def table_uses_shared_sql(self) -> bool:
        tbl_config = self.config
        tbl_sql = tbl_config.get("sql", {})
        return tbl_sql.get("shared", False)

    def input_data(self) -> dict[str, dict[str, str]] | None:
        """Returns the table's input dataset paths

        Returns the table's data inputs, including from other
        pipelines. Will return None if the table either
        uses a default or custom transformation.

        Return
        ------
        dict[str, dict[str, str]] | None]
            {
                input_db_name: {
                    input_table_name: {
                        path: input_data_path,
                        frequency: input_data_frequency,
                        file_format: input_data_file_format
                    }, ...
                }, ...
            }
        """
        transform_type = self.transform_type()
        if transform_type == "derived":
            config = self._config
            try:
                input_data = config["input_data"]
            except KeyError:
                raise KeyError("Derived table should have inputs listed in config.")

            all_data_paths: dict[str, dict[str, str]] = {}
            for db_name in input_data:
                db = Database(self.config)
                tables = input_data[db_name]
                table_names = list(tables.keys())
                all_data_paths[db_name]: dict[str, str] = {}

                for table_name in table_names:
                    if table_name == self._name:
                        raise ValueError(
                            "Derived table cannot have itself "
                            + "as an input data path in config."
                        )

                    table_stage = tables[table_name]
                    table = DatabaseTable(table_name, db)
                    table_formats = table.table_file_formats()
                    table_paths = table.table_data_paths()
                    data_paths = table_paths[table_stage]
                    data_formats = table_formats[table_stage]
                    table_freq = table.frequency()

                    all_data_paths[db_name][table_name] = {
                        **{"path": data_paths, "frequency": table_freq},
                        **data_formats,
                    }

        else:
            all_data_paths = None

        return all_data_paths

    @staticmethod
    def _convert_pandas_kwargs_in_config(
        config: dict[str, Any],
        metadata_path: str = "",
    ) -> dict[str, Any]:
        pandas_kwargs = config.get("pandas-kwargs", None)

        converted_kwargs = {}
        if pandas_kwargs is not None:
            if "parquet_expect_full_schema" in pandas_kwargs:
                _ = pandas_kwargs.pop("parquet_expect_full_schema")
                cols_to_cast = config["columns_to_cast"]
                cols_cast_types = config["columns_cast_types"]
                cols_map = dict(zip(cols_to_cast, cols_cast_types))
                tbl_meta = Metadata.from_json(metadata_path)

                for c in tbl_meta.column_names:
                    if c not in cols_to_cast:
                        tbl_meta.remove_column(c)
                    else:
                        tbl_meta.update_column({"name": c, "type": cols_map[c]})

                converted_kwargs["metadata"] = tbl_meta.to_dict()
                converted_kwargs["parquet_expect_full_schema"] = False

            config["pandas-kwargs"] = {**pandas_kwargs, **converted_kwargs}

        return config

    def lint_config(
        self, meta_stage: str = "raw_hist"
    ) -> dict[str, str | bool] | dict[None, None]:
        """Returns data linter config for the table

        Returns a data_linter table config for the stage
        specified.

        Parameters
        ----------
        meta_stage: str
            ETL stage for meta to check the data against.

        Return
        ------
        dict[str, str | bool] | dict[None, None]:
            data_linter config dictionary for table
        """
        transform_type = self.transform_type()
        config = {}
        if transform_type in ["default", "custom"]:
            table_lint_config = self._config
            if "lint_options" not in table_lint_config.keys():
                raise KeyError(f"Lint options have not been specified for {self._name}")

            table_meta = self.table_meta_paths()
            table_meta_stage = table_meta[meta_stage]

            original_config = deepcopy(table_lint_config["lint_options"])

            updated_config = self._convert_pandas_kwargs_in_config(
                config=original_config,
                metadata_path=table_meta_stage,
            )

            config = {
                k: v
                for k, v in updated_config.items()
                if k
                not in [
                    "columns_to_cast",
                    "columns_original_dtypes",
                    "columns_cast_types",
                ]
            }

            config["metadata"] = table_meta_stage

        return config

    def transform_args(
        self, input_stage: str | None, output_stage: str = "curated"
    ) -> dict:
        """Transformation arguments for the database table

        Returns a dictionary containing transformation parameters
        for the table. This will include input and output
        data locations and transformation types (e.g. default, custom,
        derived) etc.

        Parameters
        ----------
        input_stage: str | None
            Input ETL stage for table transform. Can only be
            None if the table has a 'derived' table transform.

        output_stage: str
            Output ETL stage for table transform.

        Return
        ------
        dict
            {
                "transform_type": "table transform type"
                "input": {
                    "path": "input data path",
                    "file_format": "input data file format"
                },
                "output": {
                    "path": "output data path",
                    "file_format": "output data file format"
                }
            }
        """
        transform_type = self.transform_type()
        transform_args = {}
        transform_args["transform_type"] = transform_type

        if transform_type == "derived":
            transform_args["input"] = self.input_data()
            transform_args["output"] = {
                "path": self.table_data_paths()[output_stage],
                **self.table_file_formats()[output_stage],
            }

        else:
            etl = self.etl_stages()
            stage_list = [input_stage, output_stage]
            if set(stage_list).issubset(etl) is False:
                raise KeyError(
                    f"Input and output stages aren't listed in {self.name} config"
                )

            tdps = self.table_data_paths()
            tdff = self.table_file_formats()

            transform_args["input"] = {"path": tdps[input_stage], **tdff[input_stage]}

            transform_args["output"] = {
                "path": tdps[output_stage],
                **tdff[output_stage],
            }

        return transform_args

    def get_table_metadata(
        self, stage: str, updates: list[dict[str, str] | None] = None
    ) -> Metadata:
        """Fetches MoJ Metadata for the table

        Returns MoJ Metadata for the table at the ETL stage
        supplied. This method will also update the metadata
        if updates are provided (these are forwarded to the
        MoJ Metadata update_column method).

        Parameters
        ----------
        stage: str
            ETL stage for meta to retrieve.

        updates: list[dict[str, str] | None]
            Only required for updating meta. Expects a list
            of dictionary objects to pass to MoJ Metadata's
            update_column method.

        Return
        ------
        Metadata
            MoJ Metadata for table at given ETL stage
        """
        table_meta_paths = self.table_meta_paths()
        stage_meta_path = table_meta_paths[stage]

        meta = Metadata.from_json(stage_meta_path)
        meta.set_col_type_category_from_types()

        if updates is not None:
            for col_dict in updates:
                meta.update_column(col_dict)

        return meta

    def get_table_path(self, stage: str) -> str:
        """Fetches table's data S3 path for given ETL stage

        Returns the S3 data path for the table at the
        ETL stage supplied.

        Parameters
        ----------
        stage: str
            ETL stage for data path to retrieve.

        Return
        ------
        str
            S3 path to data for given ETL stage.
        """
        table_s3_paths = self.table_data_paths()
        stage_s3_path = table_s3_paths[stage]

        return stage_s3_path

    def get_cast_cols(self) -> list[tuple[str, str, str]]:
        """Fetches table columns that require casting

        Returns a list of columns that require casting,
        primarily when linting. Original data types and
        casted data types will also be returned.

        Return
        ------
        list[tuple[str, str, str]]
            [
                (column_name, original_dtype, casted_dtype),
                ...
            ]
        """
        tbl_lint = self.config["lint_options"]
        tbl_cast_cols = tbl_lint.get("columns_to_cast", [])
        tbl_cast_dtype = tbl_lint.get("columns_original_dtypes", [])
        tbl_cast_vals = tbl_lint.get("columns_cast_types", [])
        tbl_cast_args = list(zip(tbl_cast_cols, tbl_cast_dtype, tbl_cast_vals))
        return tbl_cast_args
