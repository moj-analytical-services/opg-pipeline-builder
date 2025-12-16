import logging
from inspect import getmembers, isfunction, signature
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from ..database import Database
from ..validator import PipelineConfig
from .utils.utils import TransformEngineUtils

_logger: logging.Logger = logging.getLogger(__name__)


class BaseTransformEngine(BaseModel):
    config: PipelineConfig
    db: Database
    utils: TransformEngineUtils = Field(init=False)

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def model_post_init(self, _: Any) -> None:
        self.utils = TransformEngineUtils(db=self.db)
        self._validate_method_kwargs()

    @staticmethod
    def _check_public_method_args(parameters: list[str]) -> bool:
        if "tables" not in parameters:
            return False

        if "stages" or "stage" in parameters:
            return True

        else:
            return False

    def _validate_method_kwargs(self) -> None:
        methods = [
            signature(getattr(self, method_name)).parameters
            for method_name, _ in getmembers(self, predicate=isfunction)
            if not method_name.startswith("_")
        ]

        validation = all(
            [
                BaseTransformEngine._check_public_method_args(parameters)  # type: ignore
                for parameters in methods
            ]
        )

        if not validation:
            raise AssertionError(
                "Transform engine public methods have invalid arguments."
            )
