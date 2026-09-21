import json
import logging
import os
import sys
from datetime import UTC, datetime
from typing import Any, Literal
from urllib.parse import quote

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from pydantic import BaseModel, ConfigDict, field_validator

PACKAGE_LOGGER_NAME = "opg_pipeline_builder"
_CONSOLE_HANDLER_NAME = "opg_pipeline_builder_console"
_JSONL_HANDLER_NAME = "opg_pipeline_builder_jsonl"


def _json_default(value: object) -> str:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


class CustomLogFields(BaseModel):
    """Pydantic model representing custom log fields."""

    table: str
    field: str
    process_stage: Literal["Start", "Processing", "End"]

    model_config = ConfigDict(extra="forbid")


class StructuredLogRecord(BaseModel):
    """Pydantic model representing a structured log record written to JSONL."""

    database: str
    run_id: str
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


class JsonlLogHandler(logging.Handler):
    """Buffer records and upload JSONL objects to S3 in batches."""

    def __init__(
        self,
        *,
        bucket: str,
        prefix: str,
        database: str,
        data_delivery_period: datetime,
        attempt_no: int,
        run_id: str,
        batch_size: int = 500,
    ) -> None:
        super().__init__(level=logging.INFO)
        self._bucket = bucket
        self._prefix = prefix
        self._database = database
        self._run_id = run_id
        self._data_delivery_period = data_delivery_period
        self._attempt_no = attempt_no
        self._base_batch_size = batch_size
        self._batch_size = batch_size
        self._part_number = 0
        self._buffer: list[dict[str, Any]] = []
        self._write_failures = 0
        self._s3 = boto3.client("s3")

        self._validate_logger_inputs()
        self._validate_log_location()

    def _validate_logger_inputs(self) -> None:
        """Check that logger configuration inputs are valid."""
        err = ""
        if not self._bucket.strip():
            err += "Bucket name must be non-empty. "
        if not self._prefix.strip():
            err += "Prefix name must be non-empty. "
        if not self._database.strip():
            err += "Database must be non-empty. "
        if not self._run_id.strip():
            err += "Run ID must be non-empty. "
        if (
            self._data_delivery_period.tzinfo is None
            or self._data_delivery_period.utcoffset() is None
        ):
            err += "Data delivery period must be timezone-aware. "
        if self._attempt_no < 1:
            err += "Attempt number must be >= 1. "
        if self._base_batch_size < 1:
            err += "Batch size must be >= 1. "
        if err:
            raise ValueError(err)

    def _validate_log_location(self) -> None:
        """Validate S3 write access with a unique temporary object."""
        key = self._object_key(f"preflight-{os.getpid()}", 0)
        try:
            self._s3.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=b"",
                ContentType="application/x-ndjson",
            )
            self._s3.delete_object(Bucket=self._bucket, Key=key)
        except (BotoCoreError, ClientError, OSError) as error:
            raise RuntimeError(
                f"Failed to prepare JSONL log location s3://{self._bucket}/{self._prefix}"
            ) from error

    def emit(self, record: logging.LogRecord) -> None:
        """Append one record immediately to the JSONL file."""
        row = self._record_to_row(record)
        self.acquire()
        try:
            self._buffer.append(row)
            if len(self._buffer) >= self._batch_size:
                self._flush_locked()
        finally:
            self.release()

    def close(self) -> None:
        """Upload remaining records and fail if shutdown cannot persist them."""
        try:
            self.acquire()
            try:
                self._flush_locked()
                if self._buffer:
                    raise RuntimeError(
                        f"Failed to write {len(self._buffer)} log records to S3."
                    )
            finally:
                self.release()
        finally:
            super().close()

    def _flush_locked(self) -> None:
        """Upload the current buffer as one JSONL object; caller holds the lock."""
        if not self._buffer:
            return

        body = "".join(
            json.dumps(row, default=_json_default, separators=(",", ":")) + "\n"
            for row in self._buffer
        ).encode("utf-8")
        key = self._object_key(str(os.getpid()), self._part_number)

        try:
            self._s3.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=body,
                ContentType="application/x-ndjson",
            )
        except (BotoCoreError, ClientError, OSError, RuntimeError, ValueError) as error:
            self._write_failures += 1
            print(
                f"Failed to write a batch of logs to s3://{self._bucket}/{key} "
                f"(failure_count={self._write_failures}). Error: {error}",
                file=sys.stderr,
                flush=True,
            )
            self._batch_size += self._base_batch_size
            if self._batch_size > self._base_batch_size * 4:
                raise RuntimeError(
                    "JSONL log sink failure threshold exceeded."
                ) from error

            self._buffer.append(
                StructuredLogRecord(
                    database=self._database,
                    run_id=self._run_id,
                    data_delivery_period=self._data_delivery_period,
                    attempt_no=self._attempt_no,
                    logger_name=PACKAGE_LOGGER_NAME,
                    module="opg_pipeline_builder.logging.log",
                    function="_flush_locked",
                    line_number=sys._getframe().f_lineno,
                    log_level="ERROR",
                    log_timestamp=datetime.now(tz=UTC),
                    process_stage="Processing",
                    table="Unknown",
                    field="Unknown",
                    message=(
                        f"Failed to write JSONL log batch "
                        f"(failure_count={self._write_failures})."
                    ),
                ).model_dump()
            )
            return

        self._part_number += 1
        self._buffer.clear()
        self._write_failures = 0
        self._batch_size = self._base_batch_size

    def _object_key(self, process_id: str, part_number: int) -> str:
        return (
            f"{self._prefix}/database={quote(self._database, safe='')}/"
            f"data_delivery_period={self._data_delivery_period.strftime('%Y%m%d')}/"
            f"attempt_no={self._attempt_no}/"
            f"run_id={self._run_id}/"
            f"{process_id}_{part_number}.jsonl"
        )

    def _record_to_row(
        self, record: logging.LogRecord
    ) -> dict[str, str | int | datetime]:
        """Convert a logging.LogRecord to a dictionary suitable for JSONL."""
        custom_fields: CustomLogFields | None = getattr(record, "custom_fields", None)

        if not custom_fields or not isinstance(custom_fields, CustomLogFields):
            err = (
                "Custom log fields is missing or is not an instance of CustomLogFields."
            )
            raise ValueError(err)

        log_record = StructuredLogRecord(
            database=self._database,
            run_id=self._run_id,
            data_delivery_period=self._data_delivery_period,
            attempt_no=self._attempt_no,
            logger_name=record.name,
            module=record.module,
            function=record.funcName,
            line_number=record.lineno,
            log_level=record.levelname,
            log_timestamp=datetime.fromtimestamp(record.created, tz=UTC),
            **custom_fields.model_dump(),
            message=record.getMessage(),
        )

        return log_record.model_dump(mode="json")


def configure_logging(
    bucket: str,
    prefix: str,
    database: str,
    data_delivery_period: datetime,
    attempt_no: int,
    run_id: str,
    batch_size: int = 500,
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
            Number of records written in each JSONL S3 object.
        run_id: str
            Airflow run identifier used to isolate records and objects.

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

    jsonl_handler = JsonlLogHandler(
        bucket=bucket,
        prefix=prefix,
        database=database,
        run_id=run_id,
        data_delivery_period=data_delivery_period,
        attempt_no=attempt_no,
        batch_size=batch_size,
    )

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
        table: str = "N/A",
        field: str = "N/A",
        stage: Literal["Start", "Processing", "End"] = "Processing",
    ) -> None:
        """Log a metadata validation error with structured context."""
        custom_fields = CustomLogFields(table=table, field=field, process_stage=stage)

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
        table: str = "N/A",
        field: str = "N/A",
        stage: Literal["Start", "Processing", "End"] = "Processing",
    ) -> None:
        """Log a metadata validation event with structured context."""
        custom_fields = CustomLogFields(table=table, field=field, process_stage=stage)
        self.logger.info(
            message,
            *args,
            stacklevel=2,
            extra={"custom_fields": custom_fields},
        )
