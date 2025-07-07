from typing import List, Union

from opg_etl.base_classes.opg_pipeline import OPGPipeline
from opg_etl.utils.constants import (get_multiprocessing_settings,
                                     get_source_tbls)


class testdbPipeline(OPGPipeline):
    def __init__(self):
        super().__init__(db_name="testdb")

    def to_land(self, tables: Union[List[str], None] = get_source_tbls()):
        super().to_land(tables)

    def land_to_raw(
        self, tables, mp_args: Union[str, int, None] = get_multiprocessing_settings()
    ):
        super().land_to_raw(tables)

    def land_to_raw_hist(
        self,
        tables: Union[List[str], None] = get_source_tbls(),
        mp_args: Union[str, int, None] = get_multiprocessing_settings(),
    ):
        tables_to_use = self.db.tables_to_use(
            tables, stages=["raw-hist"], tf_types=["default", "custom"]
        )

        if tables_to_use:
            self._run_data_linter(
                tables=tables_to_use, stage="raw-hist", mp_args=mp_args
            )

    def raw_to_processed(self, tables: Union[List[str], None] = get_source_tbls()):
        super().raw_to_processed(tables)

    def raw_hist_to_processed(self, tables: Union[List[str], None] = get_source_tbls()):
        super().raw_hist_to_processed(tables)

    def processed_to_curated(self, tables: Union[List[str], None] = get_source_tbls()):
        super().processed_to_curated(tables)

    def raw_to_curated(
        self, tables: Union[List[str], None] = get_source_tbls(), **kwargs
    ):
        super().raw_to_curated(tables, **kwargs)

    def raw_hist_to_curated(
        self,
        tables: Union[List[str], None] = get_source_tbls(),
    ):
        tables_to_use = self.db.tables_to_use(
            tables, stages=["raw-hist", "curated"], tf_types=["default", "custom"]
        )

        self._glue_job_transform(tables=tables_to_use)

    def create_db(self, tables: Union[List[str], None] = get_source_tbls()):
        tables_to_use = self.db.tables_to_use(
            tables, stages=["curated"], tf_types=["default", "custom"]
        )

        self._overlay_athena_db(tables=tables_to_use, stage="curated")

    def create_derived(self, tables: Union[List[str], None] = get_source_tbls()):
        super().create_derived(tables)

    def export_extracts(self, tables: Union[List[str], None] = get_source_tbls()):
        super().export_extracts(tables)
