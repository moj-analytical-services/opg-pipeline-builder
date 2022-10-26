from functools import partial
import re
from datetime import datetime


class TestPipelineBuilder:
    database_name = "testdb"

    @classmethod
    def check_methods_match(cls, *methods):
        method_strs = [str(m) for m in methods]
        cleaned_strs = [re.sub(" object at [a-z0-9]{14}", "", m) for m in method_strs]
        return len(set(cleaned_strs)) == 1

    def test_pipeline_builder(self):
        from opg_pipeline_builder.pipeline_builder import PipelineBuilder
        from opg_pipeline_builder.transform_engines.athena import AthenaTransformEngine

        dag_timestamp = int(datetime.utcnow().timestamp())
        pipeline_builder = PipelineBuilder(db_name=self.database_name)
        pipeline = (
            pipeline_builder.register_transform_engine(
                engine_name="athena", transform_engine=AthenaTransformEngine
            )
            .add_etl_step("land_to_raw_hist")
            .with_transform("data_linter", dag_timestamp=dag_timestamp)
            .add_etl_step("raw_hist_to_curated")
            .with_transform("athena")
            .add_etl_step("create_curated_database")
            .with_transform("catalog")
            .build_pipeline()
        )

        transforms = pipeline_builder._db_transforms
        db = pipeline_builder._db

        lrh = partial(
            transforms.data_linter(dag_timestamp=dag_timestamp).run,
            tables=db.tables_to_use(stages=["land", "raw-hist"]),
            stage="raw-hist",
        )

        rhc = partial(
            transforms.athena().run,
            tables=db.tables_to_use(stages=["raw-hist", "curated"]),
            stages={"input": "raw-hist", "output": "curated"},
        )

        ccdb = partial(
            transforms.catalog().run,
            tables=db.tables_to_use(stages=["curated"]),
            stage="curated",
        )

        assert self.check_methods_match(pipeline.land_to_raw_hist, lrh)
        assert self.check_methods_match(pipeline.raw_hist_to_curated, rhc)
        assert self.check_methods_match(pipeline.create_curated_database, ccdb)
