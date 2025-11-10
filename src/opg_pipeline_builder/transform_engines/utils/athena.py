from typing import Optional

import awswrangler as wr
import boto3
import pydbtools as pydb
from mojap_metadata import Metadata
from mojap_metadata.converters.glue_converter import GlueConverter

from opg_pipeline_builder.transform_engines.utils.utils import TransformEngineUtils


class AthenaTransformEngineUtils(TransformEngineUtils):
    @staticmethod
    def check_table_is_not_empty(table_name: str) -> None:
        count_template = pydb.render_sql_template(
            "SELECT COUNT(*) as count FROM __temp__.{{ sql_tbl }}",
            jinja_args={"sql_tbl": table_name},
        )

        tbl_check = pydb.read_sql_query(count_template)

        if tbl_check["count"][0] == 0:
            raise ValueError(f"No data exists in {table_name}")

    def recreate_database(
        self,
        database_name: str,
        existing_databases: Optional[list[str] | None] = None,
    ) -> None:
        if existing_databases is None:
            databases_df = wr.catalog.databases(limit=self.db_search_limit)
            existing_databases = databases_df.Database.to_list()

        if database_name in existing_databases:
            wr.catalog.delete_database(database_name)

        wr.catalog.create_database(database_name)

        # === DIAGNOSTICS ===
        try:
            print(f"[DIAG] Glue DB ensured: {database_name}")
            dbs = wr.catalog.databases(limit=500).Database.to_list()
            print(f"[DIAG] DB exists now? {database_name in dbs}")
        except Exception as e:
            print(f"[DIAG] Failed to confirm database existence: {e}")

    @staticmethod
    def refresh_and_repair_table(
        table_name: str,
        database_name: str,
        table_metadata: Metadata,
        table_data_path: str,
    ) -> None:
        wr.catalog.delete_table_if_exists(database=database_name, table=table_name)

        gc = GlueConverter()

        spec = gc.generate_from_meta(
            table_metadata,
            database_name=database_name,
            table_location=table_data_path,
        )

        glue_client = boto3.client("glue")
        glue_client.create_table(**spec)
        wr.athena.repair_table(table=table_name, database=database_name)

        # === DIAGNOSTICS ===
        import json

        import pyarrow.fs as pafs
        import pyarrow.parquet as pq

        def _dump_parquet_prefix(
            uri: str,
            focus_col: str = "address_lines",
            max_files: int = 5,
            sample_rows: int = 5,
        ):
            try:
                fs, path = pafs.FileSystem.from_uri(uri)
                info = fs.get_file_info([path])[0]
                files = []
                if info.is_file:
                    files = [path]
                else:
                    sel = pafs.FileSelector(path, recursive=True)
                    files = [
                        fi.path
                        for fi in fs.get_file_info(sel)
                        if fi.is_file and fi.path.lower().endswith(".parquet")
                    ]
                files = files[:max_files]
                print(
                    f"[DIAG] Inspecting up to {len(files)} Parquet file(s) under {uri}"
                )
                for p in files:
                    with fs.open_input_file(p) as f:
                        pf = pq.ParquetFile(f)
                        sch = pf.schema_arrow
                        flds = {fld.name: str(fld.type) for fld in sch}
                        print(f"[DIAG] File: {p}")
                        print(f"[DIAG]   Schema: {flds}")
                        if focus_col in flds:
                            arr = pf.read(columns=[focus_col]).column(0).to_pylist()
                            shown = 0
                            for v in arr:
                                if v is not None:
                                    val = (
                                        json.dumps(v, ensure_ascii=False)
                                        if isinstance(v, (list, dict))
                                        else str(v)
                                    )
                                    print(
                                        f"[DIAG]   Sample {focus_col}: {val} :: {type(v).__name__}"
                                    )
                                    shown += 1
                                    if shown >= sample_rows:
                                        break
                            if shown == 0:
                                print(f"[DIAG]   No non-null samples for {focus_col}")
                        else:
                            print(f"[DIAG]   Column {focus_col} not present")
            except Exception as e:
                print(f"[DIAG] Failed to inspect Parquet at {uri}: {e}")

        def _dump_glue_table(db: str, tbl: str):
            try:
                t = glue_client.get_table(DatabaseName=db, Name=tbl)["Table"]
                sd = t["StorageDescriptor"]
                cols = [(c["Name"], c["Type"]) for c in sd["Columns"]]
                params = t.get("Parameters", {}) or {}
                print(f"[DIAG] Glue table: {db}.{tbl}")
                print(
                    f"[DIAG]   TableType={t.get('TableType')}  Location={sd.get('Location')}"
                )
                print(
                    f"[DIAG]   Serde={(sd.get('SerdeInfo') or {}).get('SerializationLibrary')}  InputFormat={sd.get('InputFormat')}"
                )
                print(f"[DIAG]   Columns={cols}")
                if t.get("PartitionKeys"):
                    print(
                        f"[DIAG]   PartitionKeys={[(p['Name'], p['Type']) for p in t['PartitionKeys']]}"
                    )
                # Hint if it's a VIEW
                if (
                    "viewOriginalText" in params
                    or "presto_view_original_text" in params
                ):
                    print("[DIAG]   This is a VIRTUAL_VIEW (stored SQL present).")
            except Exception as e:
                print(f"[DIAG] Failed to read Glue table {db}.{tbl}: {e}")

        print(
            f"[DIAG] After create+repair, dumping Glue schema and sample files for {database_name}.{table_name}"
        )
        _dump_glue_table(database_name, table_name)
        _dump_parquet_prefix(table_data_path)
