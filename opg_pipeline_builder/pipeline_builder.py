from __future__ import annotations

import importlib
from functools import partial
from inspect import signature
from pathlib import Path
from typing import Callable, List, Optional

from .database import Database
from .pipeline import Pipeline
from .transform_engines.base import BaseTransformEngine
from .transforms import Transforms
from .utils.constants import etl_stages, etl_steps, get_source_db, get_source_tbls
from .utils.utils import do_nothing
from .validator import read_pipeline_config


class PipelineBuilder:
    """**Pipeline builder class**

    The `PipelineBuilder` class is used to construct a
    `Pipeline` object.

    Properties: ToDo
    """

    def __init__(
        self,
        db_name: Optional[str] = None,
        db_transforms: Optional[Transforms] = None,
        debug: bool = False,
    ) -> None:
        if db_name is None:
            db_name = get_source_db()

        self._db_name = db_name
        self._debug = debug
        self._builder_specs = {}

        if db_transforms is not None:
            if isinstance(db_transforms, Transforms):
                self._db_transforms = db_transforms
            else:
                raise TypeError("db_transforms must be a Transforms object")
        else:
            self._db_transforms = Transforms()
            self._db = Database(db_name)

    def _add_specs(self, etl_step, transform) -> PipelineBuilder:
        current_specs = self._builder_specs
        new_specs = {etl_step: transform, **current_specs}
        self._builder_specs = new_specs
        return self

    def register_transform_engine(
        self, engine_name: str, transform_engine: BaseTransformEngine
    ):
        transforms = self._db_transforms
        setattr(transforms, engine_name, transform_engine)
        self._db_transforms = transforms
        return self

    def add_etl_step(self, etl_step_name: str) -> ETLStepBuilder:
        if etl_step_name not in etl_steps:
            raise ValueError(f"{etl_step_name} isn't a valid ETL step.")
        etl_builder = ETLStepBuilder(
            etl_step=etl_step_name, pipeline_builder=self, debug=self._debug
        )
        return etl_builder

    def build_pipeline(self):
        self._validate()
        return Pipeline(db_name=self._db_name, **self._builder_specs)

    def _validate(self):
        # TODO
        ...

    @classmethod
    def build_pipeline_from_config(cls, db_name: str) -> Pipeline:
        pipeline_builder = cls(db_name=db_name)
        pipeline_config = read_pipeline_config(db_name)
        custom_engines_path = Path("engines")
        custom_engines = [x.stem for x in custom_engines_path.glob("*.py")]

        pipeline_etl = pipeline_config.etl
        for step in pipeline_etl:
            step_name = step.step
            engine_name = step.engine_name
            transform_name = step.transform_name
            transform_kwargs = (
                step.transform_kwargs if step.transform_kwargs is not None else {}
            )

            if engine_name in custom_engines:
                engine = importlib.import_module(f"engines.{engine_name}")
                engine_class_name = (
                    "".join(
                        [n[0].upper() + n[1:].lower() for n in engine_name.split("_")]
                    )
                    + "TransformEngine"
                )
                engine_class = getattr(engine, engine_class_name)
                pipeline_builder = pipeline_builder.register_transform_engine(
                    engine_name=engine_name, transform_engine=engine_class
                )

            pipeline_builder = pipeline_builder.add_etl_step(
                etl_step_name=step_name
            ).with_transform(
                engine_name=engine_name,
                transform_name=transform_name,
                **transform_kwargs,
            )

        return pipeline_builder.build_pipeline()


class ETLStepBuilder:
    def __init__(
        self,
        etl_step: str,
        pipeline_builder: PipelineBuilder,
        debug: Optional[bool] = False,
    ) -> None:
        self._pipeline_builder = pipeline_builder
        self._etl_step = etl_step
        self._debug = debug

    def with_transform(
        self, engine_name: str, transform_name: Optional[str] = None, **transform_kwargs
    ):
        if transform_name is None:
            transform_name = "run"

        db_name = self._pipeline_builder._db_name

        transforms = self._pipeline_builder._db_transforms
        engine = getattr(transforms, engine_name)
        initialised_engine = engine(db_name=db_name, debug=self._debug)
        parsed_engine = initialised_engine.parse_obj(transform_kwargs)
        specified_transform = getattr(parsed_engine, transform_name)
        augmented_transform = self._augment_transform(specified_transform)

        return self._pipeline_builder._add_specs(self._etl_step, augmented_transform)

    def _augment_transform(
        self,
        transform: Callable,
        transform_types: Optional[List[str]] = None,
    ):
        pipeline_builder = self._pipeline_builder
        db = pipeline_builder._db
        tables = db.tables if get_source_tbls() is None else get_source_tbls()

        etl_step = self._etl_step
        raw_hist_flag = "raw_hist" in etl_step

        etl_stages_mod = (
            [stage.replace("-", "_") for stage in etl_stages if stage != "raw"]
            if raw_hist_flag
            else [
                stage.replace("-", "_") for stage in etl_stages if stage != "raw-hist"
            ]
        )

        tf_stages = [
            (etl_step.index(stage), stage.replace("_", "-"))
            for stage in etl_stages_mod
            if stage in etl_step
        ]
        tf_stages.sort(key=lambda x: x[0])
        tf_stages_set = set([stage for _, stage in tf_stages])
        tf_min_stage = tf_stages[0][1]
        tf_max_stage = tf_stages[-1][1]

        if transform_types is None:
            tf_types_set = set(
                [
                    db.table(tbl).transform_type()
                    for tbl in tables
                    if tf_stages_set.intersection(db.table(tbl).etl_stages())
                ]
            )
            transform_types = list(tf_types_set)

        transform_parameters = signature(transform).parameters

        if "stage" in transform_parameters:
            step_kwargs: dict[str, str | dict[str, str]] = {"stage": tf_max_stage}
        elif "stages" in transform_parameters:
            step_kwargs = {"stages": {"input": tf_min_stage, "output": tf_max_stage}}

        tables_to_use = db.tables_to_use(
            tables, stages=list(tf_stages_set), tf_types=transform_types
        )

        if tables_to_use:
            partial_transform = partial(transform, tables=tables_to_use, **step_kwargs)
        else:
            partial_transform = do_nothing

        return partial_transform
