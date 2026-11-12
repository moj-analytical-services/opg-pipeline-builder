from datetime import datetime
from logging import getLogger

from pydantic import BaseModel

logger = getLogger(__name__)


class ParentValidationErrors(BaseModel):
    """Model representing parent validation errors."""

    parent_error_code: str
    short_desc: str
    long_desc: str


class DatabaseValidationErrors(BaseModel):
    """Model representing database validation errors."""

    pipeline_name: str
    pipeline_error_code: str
    parent_error_code: str
    context: str


class ValidationIssue(BaseModel):
    """Model representing a validation issue."""

    pipeline_name: str
    data_delivery_period: datetime
    attempt_no: int
    table_name: str
    identifier_column: str
    identifier_value: str
    invalid_column: str
    pipeline_error_code: str
    invalid_value: str
