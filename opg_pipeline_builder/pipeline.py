from .transforms import Transforms
from .database import Database
from .utils.constants import (
    etl_steps,
    get_source_db,
)
from .utils.utils import do_nothing
from typing import Callable, Optional


class Pipeline:
    """**Pipeline class**

    Pipeline object for running an ETL job. An instantiated object
    will have a property for each pre-defined ETL step which will
    be a function. For a given pipeline `pipeline`, to run e.g.
    the ETL step from land to raw you would call
    `pipeline.land_to_raw()`. See Properties below for valid ETL
    steps.

    This class isn't expected to be
    instantiated directly. Instead you should use the `PipelineBuilder`
    class.

    Properties:
        **to_land:**
            Callable for processing data from source to land location.

        **land_to_raw:**
            Callable for processing data from land to raw location.

        **land_to_raw_hist:**
            Callable for processing data from land to raw-hist location.

        **raw_to_processed:**
            Callable for processing data from raw to processed location.

        **raw_to_curated:**
            Callable for processing data from raw to curated location.

        **raw_hist_to_processed:**
            Callable for processing data from raw-hist to processed location.

        **raw_hist_to_curated:**
            Callable for processing data from raw-hist to curated location.

        **processed_to_curated:**
            Callable for processing data from processed to curated location.

        **create_curated_database:**
            Callable for overlaying database schema over curated data.

        **create_derived:**
            Callable for creating derived tables from curated data.

        **export_extracts:**
            Callable for exporting extracts of data to other S3 locations.
    """

    def __init__(
        self, db_name: Optional[str] = None, debug: bool = False, **pipeline_etl_steps
    ) -> None:
        if db_name is None:
            db_name = get_source_db()

        self._db = Database(db_name)
        self._tf = Transforms(db_name=db_name, debug=debug)

        missing_etl_steps = {
            step: None for step in etl_steps if step not in pipeline_etl_steps
        }

        all_etl_steps = {**pipeline_etl_steps, **missing_etl_steps}

        for etl_step, etl_step_func in all_etl_steps.items():
            if etl_step not in etl_steps:
                raise KeyError(f"{etl_step} is not a valid ETL step")

            etl_attr_val = do_nothing if etl_step_func is None else etl_step_func

            _ = setattr(self, f"_{etl_step}", etl_attr_val)

    def __setattr__(self, key: str, value: Callable):
        if key.startswith("_") and (key[1:] in etl_steps or key in {"_db", "_tf"}):
            self.__dict__[key] = value
        else:
            raise NotImplementedError("Setter not implemented for this attribute")

    def __getattr__(self, key: str):
        if not key.startswith("_") and key not in {"db", "tf"}:
            return self.__dict__[f"_{key}"]
        else:
            return self.__dict__[key]
