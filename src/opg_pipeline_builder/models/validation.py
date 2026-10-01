from datetime import datetime
from logging import getLogger

from pydantic import BaseModel

logger = getLogger(__name__)


class ParentValidationErrors(BaseModel):
    parent_error_code: str
    short_desc: str
    long_desc: str


class DatabaseValidationErrors(BaseModel):
    database_name: str
    database_error_code: str
    parent_error_code: str
    context: str


class ValidationIssue(BaseModel):
    database_name: str
    data_delivery_period: datetime
    attempt_no: int
    table_name: str
    identifier_column: str
    identifier_value: str
    invalid_column: str
    database_error_code: str
    invalid_value: str
