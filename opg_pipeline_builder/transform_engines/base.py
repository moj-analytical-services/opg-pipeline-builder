import logging
from inspect import getmembers, isfunction, signature
from typing import Optional

from pydantic import BaseModel

from ..database import Database
from ..utils.constants import get_source_db
from .utils.utils import TransformEngineUtils

_logger: logging.Logger = logging.getLogger(__name__)


class BaseTransformEngine(BaseModel):
    db: Database
    utils: TransformEngineUtils

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, db_name: Optional[str] = None, **kwargs):
        if db_name is None:
            _logger.debug("Setting database for engine from environment")
            db_name = get_source_db()
            _logger.debug(f"Engine database environment variable set to {db_name}")

        _logger.debug("Creating database object for engine")
        db = Database(db_name=db_name)

        _logger.debug("Creating utils object for engine")
        utils = TransformEngineUtils(db=db)

        _logger.debug(f"Creating engine with database {db.name}")
        super().__init__(db=db, utils=utils, **kwargs)

        _logger.debug("Validating engine public method arguments")
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
