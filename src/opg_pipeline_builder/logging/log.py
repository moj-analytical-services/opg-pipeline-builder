import logging
import os
from datetime import UTC, datetime
from typing import Any, Literal

import awswrangler as wr
import pandas as pd
from botocore.exceptions import BotoCoreError, ClientError
from pydantic import BaseModel, ConfigDict, ValidationError, field_validator

PACKAGE_LOGGER_NAME = "opg_pipeline_builder"
_CONSOLE_HANDLER_NAME = "opg_pipeline_builder_console"
_PARQUET_HANDLER_NAME = "opg_pipeline_builder_parquet"


class StructuredLogRecord(BaseModel):
    """Pydantic model representing a structured log record that is written to parquet."""

    database_name: str
    data_delivery_period: datetime
    attempt_no: int
    logger_name: str
    module: str
    function: str
    line_number: int
    log_level: str
    log_timestamp: datetime
    pipeline_activity: Literal["BAU", "Deletion", "Logging", "Validation"]
    process_stage: Literal["Start", "Processing", "End"]
    table_name: str
    field_name: str
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
    database_name: str,
    data_delivery_period: datetime,
    attempt_no: int,
    batch_size: int,
) -> None:
    """Check that logger configuration inputs are valid."""
    if not bucket.strip():
        raise ValueError("bucket name must be non-empty")
    if not prefix.strip():
        raise ValueError("prefix name must be non-empty")
    if not database_name.strip():
        raise ValueError("database_name must be non-empty")
    if data_delivery_period.tzinfo is None or data_delivery_period.utcoffset() is None:
        raise ValueError("data_delivery_period must be timezone-aware")
    if attempt_no < 1:
        raise ValueError("attempt_no must be >= 1")
    if batch_size < 1:
        raise ValueError("batch_size must be >= 1")


def _validate_log_location(
    bucket: str,
    prefix: str,
    database_name: str,
    data_delivery_period: datetime,
    attempt_no: int,
) -> None:
    """Validate that the s3 bucket exists and the prefix is writable."""
    test_log_path = f"s3://{bucket}/{prefix}/test_{database_name}_{data_delivery_period.strftime('%Y%m%dT%H%M%S')}_{attempt_no}.snappy.parquet"
    test_log = pd.DataFrame({"test": ["test"]})
    try:
        wr.s3.to_parquet(
            test_log,
            path=test_log_path,
            index=False,
            compression="snappy",
        )
    except Exception as e:
        raise RuntimeError(
            f"Failed to write test log to s3://{bucket}/{prefix}/ for {database_name}: {data_delivery_period.strftime('%Y%m%dT%H%M%S')} (attempt no: {attempt_no}). Please check the bucket and prefix are correct."
        ) from e
    wr.s3.delete_objects(test_log_path)


class ParquetLogHandler(logging.Handler):
    """Buffered parquet log writer that emits chunked files."""

    def __init__(
        self,
        *,
        bucket: str,
        prefix: str,
        database_name: str,
        data_delivery_period: datetime,
        attempt_no: int,
        batch_size: int = 500,
    ) -> None:
        super().__init__(level=logging.INFO)
        self._bucket = bucket
        self._prefix = prefix
        self._database_name = database_name
        self._data_delivery_period = data_delivery_period
        self._attempt_no = attempt_no
        self._base_batch_size = batch_size
        self._batch_size = batch_size
        self._part_number = 0
        self._buffer: list[dict[str, Any]] = []
        self._write_failures = 0

    def emit(self, record: logging.LogRecord) -> None:
        """Append records to buffer and flush to parquet in batches."""
        row = self._record_to_row(record)
        self.acquire()
        try:
            self._buffer.append(row)
            if len(self._buffer) >= self._batch_size:
                self._flush_locked()
        finally:
            self.release()

    def close(self) -> None:
        """Flush any remaining logs to S3 and close the handler."""
        try:
            self.acquire()
            try:
                self._flush_locked()
                if self._buffer:
                    print(f"Failed to write {len(self._buffer)} log records to S3.")
                    self._buffer.clear()
                    raise RuntimeError("Failed to write all log records to S3.")
            finally:
                self.release()
        finally:
            super().close()

    def _flush_locked(self) -> None:
        """Flush the buffer to S3 as a parquet file. Assumes the lock is already acquired."""
        if not self._buffer:
            return

        df = pd.DataFrame(self._buffer)
        pid = os.getpid()  # Differentiate between parallel processes in the same run

        output_path = (
            f"s3://{self._bucket}/{self._prefix}/{self._database_name}/data_delivery_period={self._data_delivery_period.strftime('%Y%m%d')}/"
            f"attempt_no={self._attempt_no}/{pid}_{self._part_number}.snappy.parquet"
        )

        try:
            wr.s3.to_parquet(df, path=output_path, index=False, compression="snappy")

        except (BotoCoreError, ClientError, OSError, RuntimeError, ValueError) as e:
            self._write_failures += 1
            print(
                f"Failed to write a batch of logs to S3 (failure_count={self._write_failures}). Error: {e}"
            )

            self._batch_size += self._base_batch_size

            if self._batch_size > self._base_batch_size * 4:
                print(
                    f"Failed to write {len(self._buffer)} logs to {output_path}: {e}."
                )
                raise RuntimeError(
                    "Parquet log sink failure threshold exceeded."
                ) from e

            log_record = StructuredLogRecord(
                database_name=self._database_name,
                data_delivery_period=self._data_delivery_period,
                attempt_no=self._attempt_no,
                logger_name=PACKAGE_LOGGER_NAME,
                module="opg_pipeline_builder.logging.log",
                function="_flush_locked",
                line_number=0,
                log_level="ERROR",
                log_timestamp=datetime.now(tz=UTC),
                pipeline_activity="Logging",
                process_stage="Processing",
                table_name="Unknown",
                field_name="Unknown",
                message=(
                    f"Failed to write {len(self._buffer)} logs to {output_path}: {e} "
                    f"(failure_count={self._write_failures})."
                ),
            )
            self._buffer.append(log_record.model_dump())

            return

        self._part_number += 1
        self._buffer.clear()
        self._write_failures = 0

        if self._batch_size != self._base_batch_size:
            self._batch_size = self._base_batch_size

    def create_error_log_record(
        self,
        record: logging.LogRecord,
        table_name: str,
        field_name: str,
        message: str,
    ) -> StructuredLogRecord:
        """Create a structured log record for error logging."""
        return StructuredLogRecord(
            database_name=(
                self._database_name
                if isinstance(self._database_name, str)
                else "Unknown"
            ),
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
            pipeline_activity="Logging",
            process_stage="Processing",
            table_name=table_name if isinstance(table_name, str) else "Unknown",
            field_name=field_name if isinstance(field_name, str) else "Unknown",
            message=message,
        )

    def _record_to_row(
        self, record: logging.LogRecord
    ) -> dict[str, str | int | datetime]:
        """Convert a logging.LogRecord to a dictionary suitable for writing to parquet."""
        custom_fields_dict = getattr(record, "custom_fields", {})
        table_name = "Unknown"
        field_name = "Unknown"
        if isinstance(custom_fields_dict, dict):
            table_name = custom_fields_dict.get("table_name", "Unknown")
            field_name = custom_fields_dict.get("field_name", "Unknown")

        try:
            log_record = StructuredLogRecord(
                database_name=self._database_name,
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
                table_name=table_name,
                field_name=field_name,
                message=f"Failed to parse custom log fields: {custom_fields_dict}",
            )

        return log_record.model_dump()


def configure_logging(
    bucket: str,
    prefix: str,
    database_name: str,
    data_delivery_period: datetime,
    attempt_no: int,
    batch_size: int = 500,
) -> logging.Logger:
    """Configure package logging once with a shared console handler.

    Args:
        bucket: str
            The S3 bucket where logs will be stored.
        prefix: str
            The S3 prefix (folder path) under which logs will be stored.
        database_name: str
            The name of the database associated with the logs.
        data_delivery_period: datetime
            The data delivery period for the logs.
        attempt_no: int
            The attempt number for this data delivery period.
        batch_size: int, optional
            The number of log records to batch together before writing to S3, by default 500.
    Returns:
        logging.Logger: The configured package logger.
    """
    package_logger = logging.getLogger(PACKAGE_LOGGER_NAME)
    package_logger.setLevel(logging.INFO)
    package_logger.propagate = False

    if any(
        handler.get_name() in {_CONSOLE_HANDLER_NAME, _PARQUET_HANDLER_NAME}
        for handler in package_logger.handlers
    ):
        raise RuntimeError(
            "Logger has already been configured. This should only be done once per process."
        )

    _validate_logger_inputs(
        bucket=bucket,
        prefix=prefix,
        database_name=database_name,
        data_delivery_period=data_delivery_period,
        attempt_no=attempt_no,
        batch_size=batch_size,
    )

    _validate_log_location(
        bucket, prefix, database_name, data_delivery_period, attempt_no
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

    parquet_handler = ParquetLogHandler(
        bucket=bucket,
        prefix=prefix,
        database_name=database_name,
        data_delivery_period=data_delivery_period,
        attempt_no=attempt_no,
        batch_size=batch_size,
    )

    parquet_handler.set_name(_PARQUET_HANDLER_NAME)
    package_logger.addHandler(parquet_handler)

    return package_logger


class CustomFields(BaseModel):
    """Model to validate that all required custom fields have been provided."""

    pipeline_activity: Literal["BAU", "Deletion", "Logging", "Validation"]
    process_stage: Literal["Start", "Processing", "End"]
    table_name: str
    field_name: str

    model_config = ConfigDict(extra="forbid")

    @classmethod
    def set_custom_fields(
        self,
        pipeline_activity: Literal["BAU", "Deletion", "Logging", "Validation"],
        process_stage: Literal["Start", "Processing", "End"],
        table_name: str,
        field_name: str,
    ) -> "CustomFields":
        """Create a new instance of CustomFields with the provided values.

        Args:
            pipeline_activity (str): The pipeline activity.
            process_stage (str): The process stage.
            table_name (str): The name of the table.
            field_name (str): The name of the field.

        Returns:
            CustomFields: A new instance of CustomFields with the provided values.
        """
        return CustomFields(
            pipeline_activity=pipeline_activity,
            process_stage=process_stage,
            table_name=table_name,
            field_name=field_name,
        )

    def update(
        self,
        pipeline_activity: (
            Literal["BAU", "Deletion", "Logging", "Validation"] | None
        ) = None,
        process_stage: Literal["Start", "Processing", "End"] | None = None,
        table_name: str | None = None,
        field_name: str | None = None,
    ) -> None:
        """Updates the custom fields of the current instance.

        Args:
            pipeline_activity (str, optional): The new pipeline activity. Defaults to None.
            process_stage (str, optional): The new process stage. Defaults to None.
            table_name (str, optional): The new table name. Defaults to None.
            field_name (str, optional): The new field name. Defaults to None.
        """
        self.pipeline_activity = (
            pipeline_activity if pipeline_activity else self.pipeline_activity
        )
        self.process_stage = process_stage if process_stage else self.process_stage
        self.table_name = table_name if table_name else self.table_name
        self.field_name = field_name if field_name else self.field_name
