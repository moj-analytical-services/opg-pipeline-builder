import logging
import os
from datetime import UTC, datetime
from typing import Any, Literal

import awswrangler as wr
import pandas as pd
from pydantic import BaseModel, ConfigDict, ValidationError, field_validator

PACKAGE_LOGGER_NAME = "opg_pipeline_builder"
_CONSOLE_HANDLER_NAME = "opg_pipeline_builder_console"
_PARQUET_HANDLER_NAME = "opg_pipeline_builder_parquet"


class StructuredLogRecord(BaseModel):
    logger_name: str
    module: str
    function: str
    line_number: int
    database_name: str
    data_delivery_period: datetime
    attempt_no: int
    pipeline_activity: Literal["BAU", "Deletion", "Logging Error", "Validation"]
    table_name: str
    field_name: str
    log_level: str
    log_timestamp: datetime
    message: str

    model_config = ConfigDict(extra="forbid")

    @field_validator("data_delivery_period", "log_timestamp")
    @classmethod
    def ensure_utc_aware(cls, value: datetime) -> datetime:
        """Require timezone-aware datetimes and normalize them to UTC."""
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Datetime fields must be timezone-aware.")
        return value.astimezone(UTC)


class ParquetLogHandler(logging.Handler):
    """Buffered parquet log writer that emits chunked files."""

    def __init__(
        self,
        *,
        bucket: str,
        prefix: str,
        session_datetime: datetime,
        batch_size: int = 500,
    ) -> None:
        super().__init__(level=logging.INFO)
        self._bucket = bucket
        self._prefix = prefix
        self._session_datetime = session_datetime
        self._batch_size = batch_size
        self._part_number = 0
        self._buffer: list[dict[str, Any]] = []

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
        try:
            self.acquire()
            try:
                self._flush_locked()
            finally:
                self.release()
        finally:
            super().close()

    def _flush_locked(self) -> None:
        if not self._buffer:
            return

        df = pd.DataFrame(self._buffer)

        pid = os.getpid()  # Differentiate logs from parallel processes in the same run

        # Differentiate logs between airflow DAGs
        database = os.environ.get("DATABASE", "Unknown")

        output_path = (
            f"s3://{self._bucket}/{self._prefix}/run_date={self._session_datetime.strftime('%Y%m%d')}/"
            f"run_datetime={self._session_datetime.strftime('%Y%m%dT%H%M%SZ')}/"
            f"{database}_{pid}_{self._part_number}.snappy.parquet"
        )
        wr.s3.to_parquet(df, path=output_path, index=False, compression="snappy")

        self._part_number += 1
        self._buffer.clear()

    def _record_to_row(
        self, record: logging.LogRecord
    ) -> dict[str, str | int | datetime]:

        custom_fields_dict = getattr(record, "custom_fields", {})
        try:
            log_record = StructuredLogRecord(
                logger_name=record.name,
                module=record.module,
                function=record.funcName,
                line_number=record.lineno,
                log_level=record.levelname,
                log_timestamp=datetime.fromtimestamp(record.created, tz=UTC),
                message=record.getMessage(),
                **custom_fields_dict,
            )
        except ValidationError:
            log_record = StructuredLogRecord(
                logger_name=record.name if isinstance(record.name, str) else "Unknown",
                module=record.module if isinstance(record.module, str) else "Unknown",
                function=(
                    record.funcName if isinstance(record.funcName, str) else "Unknown"
                ),
                line_number=record.lineno if isinstance(record.lineno, int) else 0,
                database_name="Unknown",
                data_delivery_period=datetime(1970, 1, 1, tzinfo=UTC),
                attempt_no=0,
                pipeline_activity="Logging Error",
                table_name="Unknown",
                field_name="Unknown",
                log_level=(
                    record.levelname if isinstance(record.levelname, str) else "Unknown"
                ),
                log_timestamp=datetime.fromtimestamp(record.created, tz=UTC),
                message="Failed to parse custom log fields: " + str(custom_fields_dict),
            )

        return log_record.model_dump()


def configure_logging(
    bucket: str, prefix: str, session_datetime: datetime, batch_size: int = 500
) -> logging.Logger:
    """Configure package logging once with a shared console handler."""
    package_logger = logging.getLogger(PACKAGE_LOGGER_NAME)
    package_logger.setLevel(logging.INFO)
    package_logger.propagate = False

    if not any(
        _CONSOLE_HANDLER_NAME == handler.get_name()
        for handler in package_logger.handlers
    ):
        stream_handler = logging.StreamHandler()
        stream_handler.set_name(_CONSOLE_HANDLER_NAME)
        package_logger.addHandler(stream_handler)

        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s | %(name)s | %(funcName)s | %(levelname)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
    else:
        err = (
            "Logger has already been configured with a console handler."
            "This should only be done once per process."
        )
        raise RuntimeError(err)

    if not any(
        _PARQUET_HANDLER_NAME == handler.get_name()
        for handler in package_logger.handlers
    ):
        parquet_handler = ParquetLogHandler(
            bucket=bucket,
            prefix=prefix,
            session_datetime=session_datetime,
            batch_size=batch_size,
        )

        parquet_handler.set_name(_PARQUET_HANDLER_NAME)
        package_logger.addHandler(parquet_handler)

    return package_logger
