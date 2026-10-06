import json
import logging
import os
import sys
import uuid
from datetime import UTC, datetime
from typing import Any, Literal, Self

import awswrangler as wr
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from pydantic import BaseModel, ConfigDict, ValidationInfo, field_validator

PACKAGE_LOGGER_NAME = "opg_pipeline_builder"
_JSONL_HANDLER_NAME = "opg_pipeline_builder_jsonl"
_CURATED_LOG_PATH = os.environ.get("CURATED_LOG_PATH")
_CURATED_LOG_DATABASE = "opg_test_logging"
_CURATED_LOG_TABLE = "test_logging_curated"


class CustomLogFields(BaseModel):
    """Pydantic model representing custom log fields.

    These fields provide context for individual log entries. The model requires
    all three fields and rejects additional values.
    """

    process: str
    table: str
    field: str
    process_stage: Literal["Start", "Processing", "End"]

    model_config = ConfigDict(extra="forbid")


class StructuredLogRecord(BaseModel):
    """Pydantic model representing a structured log record written to JSONL.

    This is the definitive definition of a valid log record. It receives data from the logger.record object,
    the CustomLogFields values and pipeline/run values stored in the JSONL handler via the configuration.
    """

    pipeline: str
    run_id: str
    data_delivery_period: datetime
    attempt_no: int
    logger_name: str
    module: str
    function: str
    line_number: int
    log_level: str
    log_timestamp: datetime
    process: str
    table: str
    field: str
    process_stage: Literal["Start", "Processing", "End"]
    message: str

    model_config = ConfigDict(extra="forbid")

    @field_validator("pipeline", "run_id")
    @classmethod
    def validate_pipeline_and_run_id(cls, value: str, info: ValidationInfo) -> str:
        """Require non-empty run context identifiers."""
        if not value.strip():
            err = f"Validation of StructuredLogRecord failed: field '{info.field_name}' must be non-empty."
            raise ValueError(err)
        return value

    @field_validator("attempt_no")
    @classmethod
    def validate_attempt_no(cls, value: int) -> int:
        """Require a positive run attempt number."""
        if value < 1:
            raise ValueError("Attempt number must be an integer >= 1.")
        return value

    @field_validator("data_delivery_period", "log_timestamp")
    @classmethod
    def validate_datetimes_are_timezone_aware(cls, value: datetime) -> datetime:
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
        pipeline: str,
        data_delivery_period: datetime,
        attempt_no: int,
        run_id: str,
        batch_size: int = 500,
    ) -> None:
        super().__init__(level=logging.INFO)
        self._bucket = bucket
        self._prefix = prefix
        self._pipeline = pipeline
        self._run_id = run_id
        self._data_delivery_period = data_delivery_period
        self._attempt_no = attempt_no
        self._base_batch_size = batch_size
        self._batch_size = batch_size
        self._part_number = 0
        self._writer_id = uuid.uuid4().hex
        self._buffer: list[dict[str, Any]] = []
        self._write_failures = 0
        self._s3 = boto3.client("s3")

        self._validate_log_location()
        self._write_configuration_log()

    def _validate_log_location(self) -> None:
        """Validate S3 write access with one permanent marker object.

        This always writes to the same location and is just a marker file to confirm that
        the s3 location is reachable and the pipeline has permission to write to it. Otherwise
        the retry logic could see the pipeline progress before ultimately failing when trying
        to write the log the final time. Instead, fail fast here.
        """
        key = f"{self._prefix.rstrip('/')}/logging-marker/marker.txt"
        try:
            self._s3.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=b"",
            )
        except (BotoCoreError, ClientError, OSError) as error:
            raise RuntimeError(
                f"Failed to prepare JSONL log location s3://{self._bucket}/{key}"
            ) from error

    def _write_configuration_log(self) -> None:
        """Write a log record indicating that logging has been configured successfully.

        By successfully creating a StructuredLogRecord, it confirms that the configuration values
        are valid for the model. This is part of the fail fast strategy to prevent the pipelines
        processing before failing when it attempts to create logs.
        """
        self._buffer.append(
            StructuredLogRecord(
                pipeline=self._pipeline,
                run_id=self._run_id,
                data_delivery_period=self._data_delivery_period,
                attempt_no=self._attempt_no,
                logger_name=PACKAGE_LOGGER_NAME,
                module="opg_pipeline_builder.logging.log",
                function="configure_logging",
                line_number=sys._getframe().f_lineno,
                log_level="INFO",
                log_timestamp=datetime.now(tz=UTC),
                process="Configuration",
                table="N/A",
                field="N/A",
                process_stage="End",
                message="Logging configured successfully.",
            ).model_dump(mode="json")
        )

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
        """Upload all remaining log records and close the handlers.

        Fails the pipeline if it was unable to upload or register all run logs.
        """
        if self._closed:
            return

        try:
            self.acquire()
            try:
                self._flush_locked()
                if self._buffer:
                    raise RuntimeError(
                        f"Failed to write {len(self._buffer)} log records to S3."
                    )
                self._register_logs_in_athena()
            finally:
                self.release()
        finally:
            super().close()

    def _create_key_directory(self) -> str:
        """Generate the S3 key directory, excluding the filename."""
        return (
            f"{self._prefix.rstrip('/')}/"
            f"data_delivery_period={self._data_delivery_period.strftime('%Y%m%d')}/"
            f"attempt_no={self._attempt_no}/"
            f"run_id={self._run_id}/"
        )

    def _register_logs_in_athena(self) -> None:
        """Append this run's JSONL records to the curated Athena dataset."""
        source_path = f"s3://{self._bucket}/{self._create_key_directory()}"
        logs = wr.s3.read_json(path=source_path, lines=True)
        databases = wr.catalog.databases().Database.to_list()

        if _CURATED_LOG_DATABASE not in databases:
            wr.catalog.create_database(_CURATED_LOG_DATABASE)

        wr.s3.to_parquet(
            df=logs,
            path=_CURATED_LOG_PATH,
            dataset=True,
            mode="append",
            database=_CURATED_LOG_DATABASE,
            table=_CURATED_LOG_TABLE,
            partition_cols=["data_delivery_period", "attempt_no", "run_id"],
        )

    def _put_object(self, key: str, body: bytes) -> None:
        """Upload one JSONL batch to S3."""
        self._s3.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=body,
            ContentType="application/x-ndjson",
        )

    def _flush_locked(self) -> None:
        """Upload the current buffer as one JSONL object.

        Contains retry logic to extend the batch size upon failure, so that the pipeline can continue, generating
        more logs and then attempting to flush again later. Has an upper limit on retry attempts. Generates an error
        log ever time it fails to upload.
        """
        if not self._buffer:
            return

        body = "".join(
            json.dumps(row, separators=(",", ":")) + "\n" for row in self._buffer
        ).encode("utf-8")
        key = f"{self._create_key_directory()}{os.getpid()!s}_{self._writer_id}_{self._part_number}.jsonl"

        try:
            self._put_object(key, body)
        except (BotoCoreError, ClientError, OSError, RuntimeError, ValueError) as error:
            self._write_failures += 1
            print(
                f"Failed to write a batch of logs to s3://{self._bucket}/{key} "
                f"(failure_count={self._write_failures}). Error: {error}",
                flush=True,
            )
            self._batch_size += self._base_batch_size
            if self._batch_size > self._base_batch_size * 4:
                err = f"Have attempted to write JSONL logs {self._write_failures} times. Retry threshold exceeded"
                raise RuntimeError(err) from error

            self._buffer.append(
                StructuredLogRecord(
                    pipeline=self._pipeline,
                    run_id=self._run_id,
                    data_delivery_period=self._data_delivery_period,
                    attempt_no=self._attempt_no,
                    logger_name=PACKAGE_LOGGER_NAME,
                    module="opg_pipeline_builder.logging.log",
                    function="_flush_locked",
                    line_number=sys._getframe().f_lineno,
                    log_level="ERROR",
                    log_timestamp=datetime.now(tz=UTC),
                    process="Logging",
                    table="Unknown",
                    field="Unknown",
                    process_stage="Processing",
                    message=(
                        f"Failed to write JSONL log batch "
                        f"(failure_count={self._write_failures})."
                    ),
                ).model_dump(mode="json")
            )
            return

        self._part_number += 1
        self._buffer.clear()
        self._write_failures = 0
        self._batch_size = self._base_batch_size

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
            pipeline=self._pipeline,
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


class LoggingController:
    """Provides package level logging controls for manually flushing and shutting down the logging system.

    Prefer using this as a context manager (`with configure_logging(...) as controller:`) around the
    pipeline body. That way shutdown() runs deterministically inside the task itself and a failed final
    flush raises normally, failing the task. Relying solely on the interpreter's own atexit-triggered
    logging.shutdown() is not sufficient: it only swallows OSError/ValueError raised by close(), and even
    when other errors do propagate out of it, Python reports them as "Exception ignored in atexit
    callback" without failing the process - so a pipeline can appear to succeed while silently losing its
    trailing logs.
    """

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger
        self._is_shutdown = False

    def __enter__(self) -> Self:
        """Support use as a context manager."""
        return self

    def __exit__(self, *_exc_info: object) -> None:
        """Shut down deterministically so a failed final flush raises inside the task."""
        self.shutdown()

    def flush(self) -> None:
        """Upload buffered records without closing package logging."""
        if self._is_shutdown:
            raise RuntimeError("Logging has already been shut down.")
        for handler in self._logger.handlers:
            if isinstance(handler, JsonlLogHandler):
                handler.acquire()
                try:
                    handler._flush_locked()
                finally:
                    handler.release()

    def shutdown(self) -> None:
        """Flush, close, and remove package logging handlers."""
        if self._is_shutdown:
            return

        close_error: Exception | None = None

        for handler in self._logger.handlers:
            if isinstance(handler, JsonlLogHandler):
                try:
                    handler.close()
                except (RuntimeError, TypeError, ValueError) as error:
                    close_error = close_error or error
                finally:
                    self._logger.removeHandler(handler)

        if close_error:
            raise close_error

        self._is_shutdown = True


def configure_logging(
    bucket: str,
    prefix: str,
    pipeline: str,
    data_delivery_period: datetime,
    attempt_no: int,
    run_id: str,
    batch_size: int = 500,
) -> LoggingController:
    """Configure package logging once with a shared console and JSONL handler.

    Ensures configuration can only be run once and generates a package-level logger and
    controller configured for the specific pipeline run.

    Args:
        bucket: str
            The S3 bucket where logs will be stored.
        prefix: str
            The S3 prefix (folder path) under which logs will be stored.
        pipeline: str
            The name of the pipeline associated with the logs.
        data_delivery_period: datetime
            The data delivery period for the logs.
        attempt_no: int
            The attempt number for this data delivery period.
        run_id: str
            Airflow run identifier used to isolate records and objects.
        batch_size: int, optional
            Number of records written in each JSONL S3 object.

    Returns:
        LoggingController: Controller for flushing and shutting down logging.

    Raises:
        ValueError: If the batch size is less than 1.
        RuntimeError: If the logger has already been configured.
    """
    if batch_size < 1 or isinstance(batch_size, bool):
        raise ValueError("Batch size must be an integer >= 1.")

    package_logger = logging.getLogger()
    package_logger.setLevel(logging.INFO)

    if any(
        handler.get_name() == _JSONL_HANDLER_NAME for handler in package_logger.handlers
    ):
        raise RuntimeError(
            "Logger has already been configured. This should only be done once per process."
        )

    jsonl_handler = JsonlLogHandler(
        bucket=bucket,
        prefix=prefix,
        pipeline=pipeline,
        run_id=run_id,
        data_delivery_period=data_delivery_period,
        attempt_no=attempt_no,
        batch_size=batch_size,
    )

    jsonl_handler.set_name(_JSONL_HANDLER_NAME)
    package_logger.addHandler(jsonl_handler)

    return LoggingController(package_logger)


class ModuleLogger(BaseModel):
    """Model to validate that all required custom fields have been provided."""

    logger: logging.Logger

    model_config = ConfigDict(extra="forbid", frozen=True, arbitrary_types_allowed=True)

    def error(
        self,
        message: str,
        *args: object,
        process: str = "N/A",
        table: str = "N/A",
        field: str = "N/A",
        stage: Literal["Start", "Processing", "End"] = "Processing",
    ) -> None:
        """Log a metadata validation error with structured context."""
        custom_fields = CustomLogFields(
            process=process, table=table, field=field, process_stage=stage
        )

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
        process: str = "N/A",
        table: str = "N/A",
        field: str = "N/A",
        stage: Literal["Start", "Processing", "End"] = "Processing",
    ) -> None:
        """Log a metadata validation event with structured context."""
        custom_fields = CustomLogFields(
            process=process, table=table, field=field, process_stage=stage
        )
        self.logger.info(
            message,
            *args,
            stacklevel=2,
            extra={"custom_fields": custom_fields},
        )
