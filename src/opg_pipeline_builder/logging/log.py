import json
import logging
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, ValidationError, field_validator

PACKAGE_LOGGER_NAME = "opg_pipeline_builder"
_CONSOLE_HANDLER_NAME = "opg_pipeline_builder_console"
_JSONL_HANDLER_NAME = "opg_pipeline_builder_jsonl"


def _json_default(value: object) -> str:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


class StructuredLogRecord(BaseModel):
    """Pydantic model representing a structured log record written to JSONL."""

    database: str
    data_delivery_period: datetime
    attempt_no: int
    logger_name: str
    module: str
    function: str
    line_number: int
    log_level: str
    log_timestamp: datetime
    process_stage: Literal["Start", "Processing", "End"]
    table: str
    field: str
    message: str

    model_config = ConfigDict(extra="forbid")

    @field_validator("data_delivery_period", "log_timestamp")
    @classmethod
    def ensure_utc_aware(cls, value: datetime) -> datetime:
        """Require timezone-aware datetimes and normalize them to UTC."""
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Datetime fields must be timezone-aware.")
        return value.astimezone(UTC)


def _validate_logger_inputs(
    *,
    bucket: str,
    prefix: str,
    database: str,
    data_delivery_period: datetime,
    attempt_no: int,
    batch_size: int,
) -> None:
    """Check that logger configuration inputs are valid."""
    err = ""
    if not bucket.strip():
        err += "Bucket name must be non-empty. "
    if not prefix.strip():
        err += "Prefix name must be non-empty. "
    if not database.strip():
        err += "Database must be non-empty. "
    if data_delivery_period.tzinfo is None or data_delivery_period.utcoffset() is None:
        err += "Data delivery period must be timezone-aware. "
    if attempt_no < 1:
        err += "Attempt number must be >= 1. "
    if batch_size < 1:
        err += "Batch size must be >= 1. "
    if err:
        raise ValueError(err)


class JsonlLogHandler(logging.Handler):
    """Append each validated log record as one JSON line."""

    def __init__(
        self,
        *,
        bucket: str,
        prefix: str,
        database: str,
        data_delivery_period: datetime,
        attempt_no: int,
        log_path: str | Path,
        batch_size: int = 500,
    ) -> None:
        super().__init__(level=logging.INFO)
        del bucket
        self._prefix = prefix
        self._database = database
        self._data_delivery_period = data_delivery_period
        self._attempt_no = attempt_no
        del batch_size
        self._log_path = Path(log_path)

    def emit(self, record: logging.LogRecord) -> None:
        """Append one record immediately to the JSONL file."""
        row = self._record_to_row(record)
        self.acquire()
        try:
            with self._log_path.open("a", encoding="utf-8") as log_file:
                json.dump(row, log_file, default=_json_default, separators=(",", ":"))
                log_file.write("\n")
                log_file.flush()
        except (OSError, TypeError, ValueError) as error:
            print(
                f"Failed to append log record to {self._log_path}: {type(error).__name__}: {error}",
                file=sys.stderr,
                flush=True,
            )
        finally:
            self.release()

    def create_error_log_record(
        self,
        record: logging.LogRecord,
        table: str,
        field: str,
        message: str,
    ) -> StructuredLogRecord:
        """Create a structured log record for error logging."""
        return StructuredLogRecord(
            database=(self._database if isinstance(self._database, str) else "Unknown"),
            data_delivery_period=(
                self._data_delivery_period
                if isinstance(self._data_delivery_period, datetime)
                else datetime(1970, 1, 1, tzinfo=UTC)
            ),
            attempt_no=self._attempt_no if isinstance(self._attempt_no, int) else 0,
            logger_name=record.name if isinstance(record.name, str) else "Unknown",
            module=record.module if isinstance(record.module, str) else "Unknown",
            function=record.funcName if isinstance(record.funcName, str) else "Unknown",
            line_number=record.lineno if isinstance(record.lineno, int) else 0,
            log_level="ERROR",
            log_timestamp=datetime.now(tz=UTC),
            process_stage="Processing",
            table=table if isinstance(table, str) else "Unknown",
            field=field if isinstance(field, str) else "Unknown",
            message=message,
        )

    def _record_to_row(
        self, record: logging.LogRecord
    ) -> dict[str, str | int | datetime]:
        """Convert a logging.LogRecord to a dictionary suitable for JSONL."""
        custom_fields_dict = getattr(record, "custom_fields", {})
        table = "Unknown"
        field = "Unknown"
        if isinstance(custom_fields_dict, dict):
            table = custom_fields_dict.get("table", "Unknown")
            field = custom_fields_dict.get("field", "Unknown")

        try:
            log_record = StructuredLogRecord(
                database=self._database,
                data_delivery_period=self._data_delivery_period,
                attempt_no=self._attempt_no,
                logger_name=record.name,
                module=record.module,
                function=record.funcName,
                line_number=record.lineno,
                log_level=record.levelname,
                log_timestamp=datetime.fromtimestamp(record.created, tz=UTC),
                **custom_fields_dict,
                message=record.getMessage(),
            )
        except ValidationError:
            log_record = self.create_error_log_record(
                record=record,
                table=table,
                field=field,
                message=f"Failed to parse custom log fields: {custom_fields_dict}",
            )

        return log_record.model_dump(mode="json")


def configure_logging(
    bucket: str,
    prefix: str,
    database: str,
    data_delivery_period: datetime,
    attempt_no: int,
    batch_size: int = 500,
    *,
    log_path: str | Path | None = None,
) -> logging.Logger:
    """Configure package logging once with a shared console and JSONL handler.

    Args:
        bucket: str
            The S3 bucket where logs will be stored.
        prefix: str
            The S3 prefix (folder path) under which logs will be stored.
        database: str
            The name of the database associated with the logs.
        data_delivery_period: datetime
            The data delivery period for the logs.
        attempt_no: int
            The attempt number for this data delivery period.
        batch_size: int, optional
            Retained for call compatibility; JSONL records are not batched.
        log_path: str or pathlib.Path, optional
            Local JSONL path. If omitted, a path is derived from ``prefix``.
    Returns:
        logging.Logger: The configured package logger.
    """
    package_logger = logging.getLogger(PACKAGE_LOGGER_NAME)
    package_logger.setLevel(logging.INFO)
    package_logger.propagate = False

    if package_logger.handlers:
        raise RuntimeError(
            "Logger has already been configured. This should only be done once per process."
        )

    _validate_logger_inputs(
        bucket=bucket,
        prefix=prefix,
        database=database,
        data_delivery_period=data_delivery_period,
        attempt_no=attempt_no,
        batch_size=batch_size,
    )

    output_path = Path(
        log_path
        or Path(prefix)
        / database
        / f"data_delivery_period={data_delivery_period.strftime('%Y%m%d')}"
        / f"attempt_no={attempt_no}_{os.getpid()}.jsonl"
    )
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.touch(exist_ok=True)
    except OSError as e:
        raise RuntimeError(f"Failed to prepare JSONL log file {output_path}") from e

    stream_handler = logging.StreamHandler()
    stream_handler.set_name(_CONSOLE_HANDLER_NAME)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s | %(name)s | %(funcName)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    package_logger.addHandler(stream_handler)

    jsonl_handler = JsonlLogHandler(
        bucket=bucket,
        prefix=prefix,
        database=database,
        data_delivery_period=data_delivery_period,
        attempt_no=attempt_no,
        log_path=output_path,
        batch_size=batch_size,
    )

    jsonl_handler.set_name(_JSONL_HANDLER_NAME)
    package_logger.addHandler(jsonl_handler)

    return package_logger


class ModuleLogger(BaseModel):
    """Model to validate that all required custom fields have been provided."""

    logger: logging.Logger

    model_config = ConfigDict(extra="forbid", frozen=True, arbitrary_types_allowed=True)

    def error(
        self,
        message: str,
        *args: object,
        table: str,
        field: str,
        stage: Literal["Start", "Processing", "End"] = "Processing",
    ) -> None:
        """Log a metadata validation error with structured context."""
        custom_fields = {
            "process_stage": stage,
            "table": table,
            "field": field,
        }

        self.logger.error(
            message,
            *args,
            stacklevel=2,
            extra={"custom_fields": custom_fields},
        )

    def info(
        self,
        message: str,
        *args: object,
        table: str,
        field: str,
        stage: Literal["Start", "Processing", "End"] = "Processing",
    ) -> None:
        """Log a metadata validation event with structured context."""
        custom_fields = {
            "process_stage": stage,
            "table": table,
            "field": field,
        }
        self.logger.info(
            message,
            *args,
            stacklevel=2,
            extra={"custom_fields": custom_fields},
        )
