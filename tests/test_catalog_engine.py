from pathlib import Path

import boto3
import pytest
from mojap_metadata.converters.glue_converter import GlueConverter, GlueTable
from moto import mock_aws

from opg_pipeline_builder.database import Database
from opg_pipeline_builder.models.metadata_model import load_metadata
from opg_pipeline_builder.validator import PipelineConfig


class TestCatalogTransformEngine:
    @staticmethod
    def do_nothing(*args, **kwargs): ...

    @mock_aws
    @pytest.mark.xfail
    def test_run(self, monkeypatch, config: PipelineConfig, database: Database):
        import opg_pipeline_builder.transform_engines.catalog as catalog
        from opg_pipeline_builder.utils.constants import get_full_db_name

        db_name = "testdb"
        table_names = ["table1", "table2"]
        stage = "curated"

        monkeypatch.setattr(catalog.wr.athena, "repair_table", self.do_nothing)
        transform = catalog.CatalogTransformEngine(config=config, db=database)
        for table_name in table_names:
            metadata = load_metadata(
                Path("tests/data/meta_data/new_metadata"),
                "test",
            )
            transform.run(table=table_name, _=metadata, stage=stage)

            glue_table = GlueTable()
            gc = GlueConverter()
            full_db_name = get_full_db_name(db_name, transform.db.env)
            glue_client = boto3.client("glue")

            table = transform.db.table(table_name)
            table_glue_meta = glue_table.generate_to_meta(full_db_name, table_name)
            table_meta = table.get_table_metadata(stage)

            resp = glue_client.get_table(DatabaseName=full_db_name, Name=table_name)

            glue_s3_location = (
                resp.get("Table").get("StorageDescriptor").get("Location")
            )

            meta_s3_location = table.table_data_paths().get(stage)

            assert glue_s3_location == meta_s3_location

            spec_glue = gc.generate_from_meta(
                table_glue_meta, database_name=db_name, table_location=meta_s3_location
            )

            spec_meta = gc.generate_from_meta(
                table_meta, database_name=db_name, table_location=meta_s3_location
            )

            assert spec_glue == spec_meta
