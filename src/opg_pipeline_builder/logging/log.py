"""Structured logging utilities for the OPG pipeline builder."""

import logging
import os
import sys
import threading
import traceback
import uuid
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from types import TracebackType
from typing import Any, Literal, Self

import awswrangler as wr
import boto3
import pandas as pd
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    TypeAdapter,
    ValidationInfo,
    field_validator,
    model_validator,
)

from opg_pipeline_builder.logging import log_exceptions as exc
from opg_pipeline_builder.logging import log_utils as u
from opg_pipeline_builder.validation.validators import is_valid_identifier

_JSONL_HANDLER_NAME = "json_handler"
_CONSOLE_HANDLER_NAME = "console_handler"
_S3_RETRY_CONFIG = Config(retries={"mode": "standard", "max_attempts": 5})
_CONFIGURE_LOCK = threading.Lock()  # Ensures thread-safe configuration
_MAX_TEXT_LENGTH = 32_768  # Caps the message and exception text written to JSONL
_DATETIME_JSON = TypeAdapter(datetime)
_UNMANAGED_FLUSH_WARNING = (
    "JsonlLogHandler is being flushed outside of LoggingController. Use "
    "'with configure_logging(...) as controller:' for proper management of the JSONL handler."
)

LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
ProcessStage = Literal["Start", "Processing", "End"]


class StructuredLogRecord(BaseModel):
    """Pydantic model representing a structured log record written to JSONL."""

    pipeline: str
    run_id: str
    data_delivery_period: datetime
    attempt_no: int
    logger_name: str
    module: str
    function: str
    line_number: int
    log_level: LogLevel
    log_timestamp: datetime
    process: str
    table: str
    field: str
    process_stage: ProcessStage
    message: str
    exception: str = ""

    model_config = ConfigDict(extra="forbid", frozen=True)


class CustomLogFields(BaseModel):
    """Pydantic model representing custom log fields.

    These fields provide context for individual log entries. All four fields are required,
    'process' must be non-empty (surrounding whitespace is stripped), and additional fields
    are rejected.
    """

    process: str
    table: str
    field: str
    process_stage: ProcessStage

    model_config = ConfigDict(extra="forbid", frozen=True)

    @field_validator("process", mode="before")
    @classmethod
    def validate_process(cls, value: str) -> str:
        """Validate that the 'process' field is non-empty and strip surrounding whitespace."""
        if not u.not_empty(value):
            err = "Validation of CustomLogFields failed: field 'process' must be non-empty."
            raise exc.InvalidProcessError(err)
        return value.strip()


class LoggingConfig(BaseModel):
    """Pydantic model for validating user supplied configuration."""

    bucket: str
    log_store_prefix: str
    athena_prefix: str
    athena_database_name: str
    athena_table_name: str
    pipeline: str
    data_delivery_period: datetime
    attempt_no: int
    run_id: str
    batch_size: int = Field(strict=True)

    model_config = ConfigDict(extra="forbid", frozen=True)

    @model_validator(mode="after")
    def validate_s3_prefixes(self) -> "LoggingConfig":
        """Validate that S3 prefixes are correctly formatted and don't clash.

        The eventual location of the parquet files (Athena table) may reside inside
        the location of the log store prefix. But the log store prefix must not be
        the same as, or nested inside, the Athena prefix.
        """
        u.validate_s3_prefix(self.log_store_prefix, "log_store_prefix", self.bucket)
        u.validate_s3_prefix(self.athena_prefix, "athena_prefix", self.bucket)
        u.validate_prefix_layout(self.log_store_prefix, self.athena_prefix)
        return self

    @field_validator("athena_database_name", "athena_table_name")
    @classmethod
    def validate_athena_db_and_table(cls, value: str, info: ValidationInfo) -> str:
        """Require Athena database and table names to be valid identifiers."""
        identifier_error = is_valid_identifier(value)
        if identifier_error:
            err = f"Validating StructuredLogRecord: {identifier_error}: '{info.field_name}.'"
            raise ValueError(err)
        return value

    @field_validator("pipeline", "run_id")
    @classmethod
    def validate_pipeline_and_run_id(cls, value: str, info: ValidationInfo) -> str:
        """Require non-empty run context identifiers."""
        if not u.not_empty(value):
            err = f"Validating StructuredLogRecord: '{info.field_name}' must be non-empty."
            raise ValueError(err)
        return value

    @field_validator("data_delivery_period")
    @classmethod
    def validate_datetimes_are_timezone_aware(
        cls, value: datetime, info: ValidationInfo
    ) -> datetime:
        """Require timezone-aware datetimes and normalize them to UTC."""
        if not u.is_timezone_aware(value):
            err = f"Validating StructuredLogRecord: '{info.field_name}' must be timezone-aware."
            raise ValueError(err)
        return value.astimezone(UTC)

    @field_validator("attempt_no", "batch_size")
    @classmethod
    def validate_attempt_no(cls, value: int, info: ValidationInfo) -> int:
        """Require positive run attempt numbers and batch sizes."""
        if value < 1:
            err = f"Validating StructuredLogRecord: '{info.field_name}' must be >=1."
            raise ValueError(err)
        return value


class JsonlLogHandler(logging.Handler):
    """Buffer records and upload JSONL objects to S3 in batches.

    The handler is attached to the root logger so that records from consuming repositories are
    captured. Only records carrying 'custom_fields' (i.e. emitted via ModuleLogger) are written.
    Records without them (e.g. from third-party libraries, or code that bypasses ModuleLogger)
    are skipped here but still reach any other handler, such as the console handler.
    """

    _closed: bool

    def __init__(
        self,
        *,
        logging_config: LoggingConfig,
    ) -> None:
        """Initialize the JSONL log handler with the given configuration."""
        super().__init__(level=logging.INFO)
        self._config = logging_config
        self._batch_size = self._config.batch_size
        self._base_batch_size = self._config.batch_size
        self._part_number = 0
        self._writer_id = uuid.uuid4().hex
        self._buffer: list[dict[str, Any]] = []
        self._write_failures = 0
        self._unmanaged_flush_warned = False
        self._s3 = boto3.client("s3", config=_S3_RETRY_CONFIG)

        self._validate_log_location()
        self._write_configuration_log()

    def _validate_log_location(self) -> None:
        """Check the S3 log location is reachable by writing one permanent marker object.

        This always writes to the same location and is just a marker file to confirm that
        the s3 location is reachable. It does not prove every permission the run needs (e.g.
        reading the raw logs back, Glue or Athena access), but it does fail fast before the
        pipeline starts processing if the bucket or prefix cannot be written to at all.
        """
        key = f"{self._config.log_store_prefix.rstrip('/')}/logging-marker/marker.txt"
        try:
            self._s3.put_object(
                Bucket=self._config.bucket,
                Key=key,
                Body=b"",
            )
        except (BotoCoreError, ClientError, OSError) as error:
            err = f"Failed to write marker object to s3://{self._config.bucket}/{key}: {error}"
            raise RuntimeError(err) from error

    def _build_row(
        self, *, message: str, exception: str = "", **fields: Any
    ) -> dict[str, Any]:
        """Generate a structured log record and return it as a JSON-serializable dictionary."""
        return StructuredLogRecord(
            pipeline=self._config.pipeline,
            run_id=self._config.run_id,
            data_delivery_period=self._config.data_delivery_period,
            attempt_no=self._config.attempt_no,
            message=u.truncate_text(message, _MAX_TEXT_LENGTH),
            exception=u.truncate_text(exception, _MAX_TEXT_LENGTH, keep_end=True),
            **fields,
        ).model_dump(mode="json")

    def _internal_record(
        self,
        *,
        log_level: LogLevel,
        process: str,
        process_stage: ProcessStage,
        message: str,
        exception: str = "",
    ) -> dict[str, Any]:
        """Build a validated row for a record generated by the logging module itself.

        The function name and line number are taken from the calling frame so they point at the
        code that generated the record.
        """
        caller = sys._getframe(1)  # noqa: SLF001  # pylint: disable=W0212
        return self._build_row(
            logger_name=logging.getLogger().name,
            module="logging.log",
            function=caller.f_code.co_name,
            line_number=caller.f_lineno,
            log_level=log_level,
            log_timestamp=datetime.now(tz=UTC),
            process=process,
            table="",
            field="",
            process_stage=process_stage,
            message=message,
            exception=exception,
        )

    def _write_configuration_log(self) -> None:
        """Write a log record indicating that logging has been configured successfully."""
        self._buffer.append(
            self._internal_record(
                log_level="INFO",
                process="Configuration",
                process_stage="End",
                message="Logging configured successfully.",
            )
        )

    @contextmanager
    def _locked(self) -> Generator[None]:
        """Hold the handler lock for the duration of the block."""
        self.acquire()
        try:
            yield
        finally:
            self.release()

    def emit(self, record: logging.LogRecord) -> None:
        """Buffer one record, uploading a batch to S3 when the buffer is full.

        Raises:
            TypeError: If 'custom_fields' is truthy but is not a CustomLogFields instance.
            RuntimeError: If handler has already been closed, so records are never silently lost.
        """
        # Skips logging calls without custom_fields which are required for the JSONL log format.
        custom_fields = getattr(record, "custom_fields", None)
        if not custom_fields:
            return
        if not isinstance(custom_fields, CustomLogFields):
            err = "Custom log fields is not an instance of CustomLogFields."
            raise TypeError(err)

        row = self._record_to_row(record, custom_fields)

        with self._locked():
            if self._closed:
                err = "Cannot write log record: the JSONL log handler has already been closed."
                raise RuntimeError(err)
            self._buffer.append(row)
            if len(self._buffer) >= self._batch_size:
                self._flush_locked()

    def close(self) -> None:
        """Upload all remaining log records and close the handlers.

        Fails the pipeline if it was unable to upload or register all run logs. The closed
        check and the closed flag are both handled under the handler lock, so concurrent
        close() calls (e.g. two threads shutting down together) upload and register the run's
        logs exactly once.
        """
        with self._locked():
            if self._closed:
                return

            try:
                self._flush_locked()
                if self._buffer:
                    err = f"Failed to write {len(self._buffer)} log records to S3."
                    raise RuntimeError(err)  # noqa: TRY301
            except Exception:
                self._dump_unwritten_records()
                raise
            else:
                self._register_logs_in_athena()
            finally:
                super().close()

    def _dump_unwritten_records(self) -> None:
        """Print records that could not be uploaded to stderr, so the task log still holds them."""
        print(  # noqa: T201
            f"ERROR: {len(self._buffer)} log records could not be written to "
            f"s3://{self._config.bucket}/{self._config.log_store_prefix}. Dumping records:",
            file=sys.stderr,
        )
        for row in self._buffer:
            print(u.to_jsonl_line(row), file=sys.stderr)  # noqa: T201
        sys.stderr.flush()

    def _create_key_directory(self, prefix: str) -> str:
        """Generate the S3 key directory, excluding the filename.

        Partition values are percent-encoded so characters such as ':', '+' and '/' cannot create
        extra path segments or ambiguous keys.
        """
        partitions = {
            "data_delivery_period": _DATETIME_JSON.dump_python(
                self._config.data_delivery_period, mode="json"
            ),
            "attempt_no": self._config.attempt_no,
            "run_id": self._config.run_id,
        }
        return f"{prefix.rstrip('/')}/" + "".join(
            f"{name}={u.encode_partition_value(value)}/"
            for name, value in partitions.items()
        )

    def _object_key(self, part_number: int) -> str:
        """Return the S3 key of one JSONL object written by this handler.

        Threads in a process share this handler, and every upload happens under its lock, so each
        successful upload gets the next part number. Separate processes have their own handler, pid
        and writer_id. A key is therefore unique to one upload.
        """
        return (
            f"{self._create_key_directory(self._config.log_store_prefix)}"
            f"{os.getpid()}_{self._writer_id}_{part_number}.jsonl"
        )

    def _register_logs_in_athena(self) -> None:
        """Append the JSONL objects written by this handler to the curated Athena dataset.

        Only the exact objects written by this handler are read, so concurrent writers in the
        same run (e.g. other processes) never register each other's records. The Glue database
        is created here if it does not exist.

        Keys are rebuilt rather than tracked: _part_number only increments after a successful
        upload (a failed upload retries under the same part number), so parts 0..n-1 are exactly
        the objects written.
        """
        if self._part_number == 0:
            return

        logs = wr.s3.read_json(
            path=[
                f"s3://{self._config.bucket}/{self._object_key(part)}"
                for part in range(self._part_number)
            ],
            lines=True,
            dtype=False,
            convert_dates=False,
        )
        # Pydantic omits fractional seconds when they are zero, so the format varies between rows.
        logs["log_timestamp"] = pd.to_datetime(
            logs["log_timestamp"], utc=True, format="ISO8601"
        )
        wr.catalog.create_database(self._config.athena_database_name, exist_ok=True)

        wr.s3.to_parquet(
            df=logs,
            path=f"s3://{self._config.bucket}/{self._config.athena_prefix}",
            dataset=True,
            mode="append",
            database=self._config.athena_database_name,
            table=self._config.athena_table_name,
            partition_cols=["data_delivery_period", "attempt_no", "run_id"],
            dtype=u.ATHENA_COLUMN_TYPES,
        )

    def _put_object(self, key: str, body: bytes) -> None:
        """Upload one JSONL batch to S3."""
        self._s3.put_object(
            Bucket=self._config.bucket,
            Key=key,
            Body=body,
            ContentType="application/x-ndjson",
        )

    def _flush_locked(self) -> None:
        """Upload the current buffer as one JSONL object.

        Contains retry logic to extend the batch size upon failure, so that the pipeline can
        continue, generating more logs and then attempting to flush again later. Has an upper limit
        on retry attempts. Generates an error log ever time it fails to upload. Only S3/network
        errors count as sink failures; any other exception is a bug and propagates immediately.
        """
        if not self._buffer:
            return

        body = "".join(u.to_jsonl_line(row) + "\n" for row in self._buffer).encode(
            "utf-8"
        )
        key = self._object_key(self._part_number)

        try:
            self._put_object(key, body)
        except (BotoCoreError, ClientError, OSError) as error:
            self._write_failures += 1
            print(  # noqa: T201
                f"Failed to write a batch of logs to s3://{self._config.bucket}/{key} "
                f"(failure_count={self._write_failures}). Error: {error}",
                file=sys.stderr,
                flush=True,
            )
            self._batch_size += self._base_batch_size
            if self._batch_size > self._base_batch_size * 4:
                err = f"Attempted to write logs {self._write_failures} times. Retries exceeded"
                raise RuntimeError(err) from error

            self._buffer.append(
                self._internal_record(
                    log_level="ERROR",
                    process="Logging",
                    process_stage="Processing",
                    message=(
                        f"Failed to write JSONL log batch "
                        f"(failure_count={self._write_failures})."
                    ),
                    exception="".join(traceback.format_exception(error)),
                )
            )
            return

        self._part_number += 1
        self._buffer.clear()
        self._write_failures = 0
        self._batch_size = self._base_batch_size

    def _record_to_row(
        self, record: logging.LogRecord, custom_fields: CustomLogFields
    ) -> dict[str, Any]:
        """Convert a logging.LogRecord to a dictionary suitable for JSONL.

        If the record carries exception info, the formatted traceback (including chained causes,
        notes and exception group members) is written to the 'exception' column. It is cached on
        record.exc_text, sothe console handler prints the same text without formatting it again.
        """
        exception = ""
        if record.exc_info and record.exc_info[0] is not None:
            if not record.exc_text:
                record.exc_text = logging.Formatter().formatException(record.exc_info)
            exception = record.exc_text

        return self._build_row(
            logger_name=record.name,
            module=record.module,
            function=record.funcName,
            line_number=record.lineno,
            log_level=record.levelname,
            log_timestamp=datetime.fromtimestamp(record.created, tz=UTC),
            **custom_fields.model_dump(),
            message=record.getMessage(),
            exception=exception,
        )

    def flush(self) -> None:
        """Warn once about unmanaged flushing without uploading any records.

        Python's logging shutdown calls this before close(). For an open handler, the warning
        is printed to stderr and buffered for the subsequent upload. Closed handlers and
        repeated calls do nothing. Use LoggingController.flush() for an actual upload.
        """
        with self._locked():
            if self._closed or self._unmanaged_flush_warned:
                return
            self._unmanaged_flush_warned = True
            print(_UNMANAGED_FLUSH_WARNING, file=sys.stderr, flush=True)  # noqa: T201
            self._buffer.append(
                self._internal_record(
                    log_level="WARNING",
                    process="Logging",
                    process_stage="End",
                    message=_UNMANAGED_FLUSH_WARNING,
                )
            )


class LoggingController:
    """Provides package level logging controls for manually flushing and shutting down logging.

    Prefer using this as a context manager (`with configure_logging(...) as controller:`)
    around the pipeline body. That way shutdown() runs deterministically inside the task
    itself and a failed final flush raises normally, failing the task. Relying solely on the
    interpreter's own atexit-triggered logging.shutdown() is not sufficient: it only swallows
    OSError/ValueError raised by close(), and even when other errors do propagate out of it,
    Python reports them as "Exception ignored in atexit callback" without failing the process
    - so a pipeline can appear to succeed while silently losing its trailing logs.

    The controller only flushes, closes and removes the handlers it was given (those created by
    configure_logging). Any other handlers on the logger are left untouched.
    """

    def __init__(
        self, logger: logging.Logger, handlers: Sequence[logging.Handler]
    ) -> None:
        """Initialize the LoggingController with the given logger and handlers."""
        self._logger = logger
        self._handlers = list(handlers)
        self._is_shutdown = False

    def __enter__(self) -> Self:
        """Return this controller when entering its context manager."""
        return self

    def __exit__(
        self,
        _: type[BaseException] | None,
        exc_value: BaseException | None,
        __: TracebackType | None,
    ) -> None:
        """Shut down deterministically so a failed final flush raises inside the task.

        If the body already raised, that exception stays the primary error: a shutdown failure is
        printed to stderr and attached to it as a note rather than replacing it.
        """
        try:
            self.shutdown()
        except Exception as shutdown_error:  # pylint: disable=W0718
            if exc_value is None:
                raise
            traceback.print_exception(shutdown_error, file=sys.stderr)
            exc_value.add_note(
                f"Logging shutdown also failed: "
                f"{type(shutdown_error).__name__}: {shutdown_error}"
            )

    def flush(self) -> None:
        """Attempt to upload buffered records without closing the owned JSONL handlers.

        Recoverable sink failures retain the buffer for a later attempt.

        Raises:
            RuntimeError: If the controller is shut down or a handler exceeds its retry cap.
            Exception: If a handler encounters an unexpected error during serialization or upload.
        """
        if self._is_shutdown:
            err = "Logging has already been shut down."
            raise RuntimeError(err)
        for handler in self._handlers:
            if isinstance(handler, JsonlLogHandler):
                with handler._locked():  # noqa: SLF001  # pylint: disable=W0212
                    handler._flush_locked()  # noqa: SLF001  # pylint: disable=W0212

    def shutdown(self) -> None:
        """Flush, close, and remove the handlers created by configure_logging.

        Every handler is closed and removed even if an earlier one fails. The first error is
        re-raised with notes describing any later errors. Logging is marked as shut down
        regardless, because closed handlers cannot be reopened.
        """
        if self._is_shutdown:
            return

        self._is_shutdown = True
        errors: list[Exception] = []
        for handler in self._handlers:
            try:
                handler.close()
            except Exception as error:  # noqa: BLE001  # pylint: disable=W0718
                errors.append(error)
            finally:
                self._logger.removeHandler(handler)

        if errors:
            first_error = errors[0]
            for other_error in errors[1:]:
                first_error.add_note(
                    f"Another logging handler also failed to close: "
                    f"{type(other_error).__name__}: {other_error}"
                )
            raise first_error


def configure_logging(  # noqa: PLR0917 PLR0913
    bucket: str,
    log_store_prefix: str,
    athena_prefix: str,
    athena_database_name: str,
    athena_table_name: str,
    pipeline: str,
    data_delivery_period: datetime,
    attempt_no: int,
    run_id: str,
    batch_size: int = 500,
) -> LoggingController:
    """Configure root logging once with a shared console and JSONL handler.

    Handlers are attached to the root logger so that logs from consuming repositories (which have
    their own module namespaces) are captured. Ensures configuration can only be run once and
    returns a controller configured for the specific pipeline run.

    Args:
        bucket: S3 bucket where raw and curated logs will be stored.
        log_store_prefix: Bucket-relative raw JSONL prefix, without the bucket or 's3://'.
            Must not equal or sit inside athena_prefix.
        athena_prefix: Bucket-relative curated Parquet prefix, without the bucket or 's3://'.
        athena_database_name: Glue database for curated logs; created on registration if absent.
        athena_table_name: Athena table for the curated Parquet dataset.
        pipeline: Non-empty pipeline identifier associated with the logs.
        data_delivery_period: Timezone-aware delivery period, normalised to UTC.
        attempt_no: Positive attempt number for this data delivery period.
        run_id: Non-empty run identifier used to isolate records and objects.
        batch_size: Positive integer buffer threshold; defaults to 500. Booleans, strings
            and floats are rejected rather than coerced.

    Returns:
        Controller owning the newly attached JSONL and console handlers.

    Raises:
        ValidationError: If configuration fields fail type, identifier, range or timezone validation.
        InvalidPrefixError: If either prefix is invalid or the prefixes overlap.
        RuntimeError: If the logger has already been configured or the log location is unreachable.
    """
    root_logger = logging.getLogger()

    with _CONFIGURE_LOCK:
        if u.is_handler_attached(_JSONL_HANDLER_NAME):
            err = "Logger has already been configured."
            raise RuntimeError(err)

        logging_config = LoggingConfig(
            pipeline=pipeline,
            data_delivery_period=data_delivery_period,
            attempt_no=attempt_no,
            run_id=run_id,
            bucket=bucket,
            log_store_prefix=log_store_prefix,
            athena_prefix=athena_prefix,
            athena_database_name=athena_database_name,
            athena_table_name=athena_table_name,
            batch_size=batch_size,
        )

        jsonl_handler = JsonlLogHandler(logging_config=logging_config)
        jsonl_handler.set_name(_JSONL_HANDLER_NAME)

        stream_handler = logging.StreamHandler()
        stream_handler.set_name(_CONSOLE_HANDLER_NAME)
        stream_handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s | %(name)s | %(funcName)s | %(levelname)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )

        root_logger.setLevel(logging.INFO)
        root_logger.addHandler(jsonl_handler)
        root_logger.addHandler(stream_handler)

    return LoggingController(root_logger, [jsonl_handler, stream_handler])


class ModuleLogger(BaseModel):
    """Wrap a logger so every record carries validated custom fields.

    Table precedence: the table passed to info()/error(), then the innermost
    active with_table() block in the current thread/task, then the table set
    on the ModuleLogger.
    """

    logger: logging.Logger
    process: str | None = None
    table: str = ""

    # Per-instance, per-thread/task table set by with_table(); None means no block is active.
    _table_override: ContextVar[str | None] = PrivateAttr(
        default_factory=lambda: ContextVar[str | None](
            "module_logger_table", default=None
        )
    )

    model_config = ConfigDict(
        extra="forbid", arbitrary_types_allowed=True, validate_assignment=True
    )

    @contextmanager
    def with_table(self, table: str) -> Generator[None]:
        """Set the table for log calls made in this thread/task within this block.

        The value is held in a ContextVar, so concurrent threads/tasks sharing the
        ModuleLogger never see each other's table, and nested blocks restore the
        previous value on exit. New threads (including ThreadPoolExecutor workers)
        start with no active block, so they fall back to the logger's table.
        """
        token = self._table_override.set(table)
        try:
            yield
        finally:
            self._table_override.reset(token)

    def _require_logging_configured(self) -> None:
        """Raise if the JSONL handler is not attached to the root logger."""
        if not u.is_handler_attached(_JSONL_HANDLER_NAME):
            err = "Logging is not configured. Call configure_logging() before logging"
            raise exc.LoggingNotConfiguredError(err)

    def get_table_value(self, log_call_table: str | None = None) -> str:
        """Get the effective table value for this logger."""
        if log_call_table is not None:
            return log_call_table
        table_override = self._table_override.get()
        return self.table if table_override is None else table_override

    def error(  # noqa: PLR0913
        self,
        message: str,
        *args: object,
        process: str | None = None,
        table: str | None = None,
        field: str = "",
        stage: ProcessStage = "Processing",
        exc_info: bool | BaseException = False,
    ) -> None:
        """Log an error with structured context.

        Pass exc_info=True inside an 'except' block (or pass the exception itself) to
        record the full traceback, including chained causes, notes and exception group members,
        in the 'exception' column and on the console.
        """
        self._require_logging_configured()
        custom_fields = CustomLogFields(
            process=process if process is not None else self.process,
            table=self.get_table_value(table),
            field=field,
            process_stage=stage,
        )
        self.logger.error(
            message,
            *args,
            exc_info=exc_info,
            stacklevel=2,
            extra={"custom_fields": custom_fields},
        )

    def info(
        self,
        message: str,
        *args: object,
        process: str | None = None,
        table: str | None = None,
        field: str = "",
        stage: ProcessStage = "Processing",
    ) -> None:
        """Log an informational event with structured context."""
        self._require_logging_configured()

        custom_fields = CustomLogFields(
            process=process if process is not None else self.process,
            table=self.get_table_value(table),
            field=field,
            process_stage=stage,
        )
        self.logger.info(
            message,
            *args,
            stacklevel=2,
            extra={"custom_fields": custom_fields},
        )
