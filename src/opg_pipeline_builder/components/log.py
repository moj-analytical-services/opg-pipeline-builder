import logging
from datetime import UTC, datetime
from pydantic import BaseModel, ConfigDict, ValidationError
from typing import Any, Literal

import awswrangler as wr
import pyarrow as pa

PACKAGE_LOGGER_NAME = "opg_pipeline_builder"
_CONSOLE_HANDLER_NAME = "opg_pipeline_builder_console"
_PARQUET_HANDLER_NAME = "opg_pipeline_builder_parquet"


# Pydantic model for the full log - enforce types and output to DF / pyarrow (use an explicit schema)
# Think about parallel processing risks


class CustomLogFields(BaseModel):
    database_name: str
    data_delivery_period: datetime
    attempt_no: int
    pipeline_activity: Literal["BAU", "Deletion", "Validation"]
    table_name: str
    field_name: str

    model_config = ConfigDict(extra="forbid")


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

    def flush(self) -> None:
        self.acquire()
        try:
            self._flush_locked()
        finally:
            self.release()

    def close(self) -> None:
        try:
            self.flush()
        finally:
            super().close()

    def _flush_locked(self) -> None:
        if not self._buffer:
            return

        table = pa.Table.from_pylist(self._buffer).to_pandas()
        output_path = (
            f"s3://{self._bucket}/{self._prefix}/run_date={self._session_datetime.strftime('%Y%m%d')}/"
            f"run_datetime={self._session_datetime.strftime('%Y%m%dT%H%M%SZ')}/log_{self._part_number:05d}.parquet"
        )

        wr.s3.to_parquet(table, path=str(output_path), index=False)
        self._part_number += 1
        self._buffer.clear()

    def _record_to_row(
        self, record: logging.LogRecord
    ) -> dict[str, str | int | datetime]:

        custom_fields_dict = getattr(record, "custom_fields", {})
        try:
            custom_fields = CustomLogFields(**custom_fields_dict)
        except ValidationError:
            return {
                "logger_name": record.name,
                "module": record.module,
                "function": record.funcName,
                "line_number": record.lineno,
                "database_name": "Unknown",
                "data_delivery_period": datetime(1970, 1, 1, tzinfo=UTC),
                "attempt_no": 0,
                "pipeline_activity": "Parsing custom log fields",
                "table_name": "Unknown",
                "field_name": "Unknown",
                "log_level": record.levelname,
                "log_timestamp": datetime.fromtimestamp(
                    record.created, tz=UTC
                ).isoformat(),
                "message": "Failed to parse custom log fields: "
                + str(custom_fields_dict),
            }

        return {
            "logger_name": record.name,
            "module": record.module,
            "function": record.funcName,
            "line_number": record.lineno,
            "database_name": custom_fields.database_name,
            "data_delivery_period": custom_fields.data_delivery_period,
            "attempt_no": custom_fields.attempt_no,
            "pipeline_activity": custom_fields.pipeline_activity,
            "table_name": custom_fields.table_name,
            "field_name": custom_fields.field_name,
            "log_level": record.levelname,
            "log_timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "message": record.getMessage(),
        }


def configure_logging(
    bucket: str, prefix: str, session_datetime: datetime
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

    if not any(
        _PARQUET_HANDLER_NAME == handler.get_name()
        for handler in package_logger.handlers
    ):
        parquet_handler = ParquetLogHandler(
            bucket=bucket,
            prefix=prefix,
            session_datetime=session_datetime,
            batch_size=500,
        )

        parquet_handler.set_name(_PARQUET_HANDLER_NAME)
        package_logger.addHandler(parquet_handler)
    return package_logger
