import boto3
from mojap_metadata.converters.glue_converter import GlueConverter, GlueTable
from moto import mock_athena, mock_glue


class TestCatalogTransformEngine:
    @staticmethod
    def do_nothing(*args, **kwargs): ...

    @mock_glue
    @mock_athena
    def test_run(self, monkeypatch):
        import src.transform_engines.catalog as catalog

        from opg_pipeline_builder.utils.constants import get_full_db_name

        db_name = "testdb"
        table_names = ["table1", "table2"]
        stage = "curated"

        monkeypatch.setattr(catalog.wr.athena, "repair_table", self.do_nothing)
        transform = catalog.CatalogTransformEngine(db_name)
        transform.run(tables=table_names, stage=stage)

        glue_table = GlueTable()
        gc = GlueConverter()
        full_db_name = get_full_db_name(db_name, transform.db.env)
        glue_client = boto3.client("glue")

        for table_name in table_names:
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
