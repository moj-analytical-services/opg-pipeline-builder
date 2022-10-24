from functools import partial


class TestPipelineBuilder:
    database_name = "testdb"

    def test_pipeline_builder(self):
        from opg_pipeline_builder.pipeline_builder import PipelineBuilder
        from opg_pipeline_builder.transform_engines.athena import AthenaTransformEngine

        athena_engine = AthenaTransformEngine(db_name=self.database_name)

        pipeline_builder = PipelineBuilder(db_name=self.database_name)
        pipeline = (
            pipeline_builder.register_transform_engine(
                engine_name="athena", transform_engine=athena_engine
            )
            .add_etl_step("land_to_raw_hist")
            .with_transform("data_linter")
            .add_etl_step("raw_hist_to_curated")
            .with_transform("athena")
            .add_etl_step("create_curated_database")
            .with_transform("catalog")
            .build_pipeline()
        )

        transforms = pipeline_builder._db_transforms
        db = pipeline_builder._db

        lrh = partial(
            transforms.data_linter.run,
            tables=db.tables_to_use(stages=["land", "raw-hist"]),
            stage="raw-hist",
        )

        rhc = partial(
            transforms.athena.run,
            tables=db.tables_to_use(stages=["raw-hist", "curated"]),
            stages={"input": "raw-hist", "output": "curated"},
        )

        ccdb = partial(
            transforms.catalog.run,
            tables=db.tables_to_use(stages=["curated"]),
            stage="curated",
        )

        assert str(pipeline.land_to_raw_hist) == str(lrh)
        assert str(pipeline.raw_hist_to_curated) == str(rhc)
        assert str(pipeline.create_curated_database) == str(ccdb)
