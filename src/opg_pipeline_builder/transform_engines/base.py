import logging
from inspect import getmembers, isfunction, signature

from pydantic import BaseModel

from ..database import Database
from ..validator import PipelineConfig
from .utils.utils import TransformEngineUtils

_logger: logging.Logger = logging.getLogger(__name__)


class BaseTransformEngine(BaseModel):
    config: PipelineConfig
    db: Database
    utils: TransformEngineUtils

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, config: PipelineConfig, db: Database) -> None:
        utils = TransformEngineUtils(db=db)
        super().__init__(config=config, db=db, utils=utils)
        self._validate_method_kwargs()

    @staticmethod
    def _check_public_method_args(parameters: object) -> bool:
        if "tables" not in parameters:
            return False

        if "stages" or "stage" in parameters:
            return True

    def _validate_method_kwargs(self):
        methods = [
            signature(getattr(self, method_name)).parameters
            for method_name, _ in getmembers(self, predicate=isfunction)
            if not method_name.startswith("_")
        ]

        validation = all(
            [
                BaseTransformEngine._check_public_method_args(parameters)
                for parameters in methods
            ]
        )

        if not validation:
            raise AssertionError(
                "Transform engine public methods have invalid" " arguments."
            )
