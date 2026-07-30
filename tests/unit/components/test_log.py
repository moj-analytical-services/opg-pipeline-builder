import logging
import os
from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from threading import Thread
from unittest.mock import patch

import awswrangler as wr
import boto3
import pandas as pd
import pytest
from freezegun import freeze_time

from opg_pipeline_builder.components.log import (
    _CONSOLE_HANDLER_NAME,
    _PARQUET_HANDLER_NAME,
    PACKAGE_LOGGER_NAME,
    ParquetLogHandler,
    StructuredLogRecord,
    configure_logging,
)


def create_parquet_handler(batch_size: int = 2) -> ParquetLogHandler:
    """Fixture to provide a ParquetLogHandler instance for tests."""
    return ParquetLogHandler(
        bucket=os.environ.get("DEFAULT_BUCKET", ""),
        prefix="prefix/to/log",
        session_datetime=datetime(2024, 1, 4, 10, 15, 5, tzinfo=UTC),
        batch_size=batch_size,
    )


def create_log_record(
    line_number: str | int = 42,
    msg: str = "test log",
) -> tuple[logging.LogRecord, dict[str, str | int | datetime]]:
    """Create a sample log record for testing."""
    log_record = {
        "name": "opg_pipeline_builder.test_module",
        "module": "test_module",
        "funcName": "test_func",
        "lineno": line_number,
        "levelname": "INFO",
        "msg": msg,
        "args": (),
        "created": datetime(2024, 1, 4, 10, 16, 6, tzinfo=UTC).timestamp(),
        "custom_fields": {
            "database_name": "test_db",
            "data_delivery_period": datetime(2024, 1, 2, tzinfo=UTC),
            "attempt_no": 1,
            "pipeline_activity": "Validation",
            "table_name": "table_a",
            "field_name": "field_a",
        },
    }

    return logging.makeLogRecord(log_record), {
        "logger_name": "opg_pipeline_builder.test_module",
        "module": "test_module",
        "function": "test_func",
        "line_number": line_number,
        "database_name": "test_db",
        "data_delivery_period": datetime(2024, 1, 2, tzinfo=UTC),
        "attempt_no": 1,
        "pipeline_activity": "Validation",
        "table_name": "table_a",
        "field_name": "field_a",
        "log_level": "INFO",
        "log_timestamp": datetime(2024, 1, 4, 10, 16, 6, tzinfo=UTC),
        "message": msg,
    }


def mock_flush_locked(self: ParquetLogHandler) -> None:
    self._buffer.clear()


@pytest.fixture(autouse=True)
def _reset_package_logger() -> Generator[None]:
    """Reset the package logger before and after each test.

    Explicitly closes handlers after test to flush pending writes to S3 BEFORE
    the moto mock_aws() context exits, otherwise credentials will fail.
    """
    package_logger = logging.getLogger(PACKAGE_LOGGER_NAME)

    package_logger.handlers.clear()
    package_logger.propagate = True
    package_logger.setLevel(logging.NOTSET)

    yield

    for handler in list(package_logger.handlers):
        handler.close()
    package_logger.handlers.clear()


@pytest.mark.parametrize(
    ("data_delivery_period", "log_timestamp"),
    [
        (
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2024, 1, 1),  # noqa: DTZ001
        ),
        (
            datetime(2024, 1, 1),  # noqa: DTZ001
            datetime(2024, 1, 1, 12, 0, tzinfo=UTC),
        ),
    ],
)
def test_structured_log_record_invalid_datetime(
    data_delivery_period: datetime,
    log_timestamp: datetime,
) -> None:
    with pytest.raises(ValueError, match="Datetime fields must be timezone-aware."):
        StructuredLogRecord(
            logger_name="test_logger",
            module="test_module",
            function="test_function",
            line_number=1,
            database_name="db",
            data_delivery_period=data_delivery_period,
            attempt_no=1,
            pipeline_activity="BAU",
            table_name="table",
            field_name="field",
            log_level="INFO",
            log_timestamp=log_timestamp,
            message="msg",
        )


def test_emit_with_flush() -> None:
    """Test that emit method correctly acquires and releases and calls flush_locked when buffer reaches batch size."""
    handler = create_parquet_handler()

    record_1, _ = create_log_record(line_number=42)
    record_2, _ = create_log_record(line_number=43)

    with patch(
        "opg_pipeline_builder.components.log.ParquetLogHandler._flush_locked",
        side_effect=mock_flush_locked,
        autospec=True,
    ) as mock_flush:
        for record in [record_1, record_2]:
            handler.emit(record)

    assert mock_flush.call_count == 1
    assert len(handler._buffer) == 0


def test_emit_without_flush() -> None:
    """Test that emit method does not call flush_locked when buffer has not reached batch size."""
    handler = create_parquet_handler(batch_size=3)
    record_1, record_1_dict = create_log_record(line_number=42)
    record_2, record_2_dict = create_log_record(line_number=43)

    with patch(
        "opg_pipeline_builder.components.log.ParquetLogHandler._flush_locked",
        side_effect=mock_flush_locked,
        autospec=True,
    ) as mock_flush:
        for record in [record_1, record_2]:
            handler.emit(record)

    assert mock_flush.call_count == 0
    assert handler._buffer == [
        record_1_dict,
        record_2_dict,
    ]


def test_close() -> None:
    """Test the close method calls flush and releases the lock."""
    handler = create_parquet_handler()

    handler._buffer.append({"mock": "record"})

    with patch(
        "opg_pipeline_builder.components.log.ParquetLogHandler._flush_locked",
        side_effect=mock_flush_locked,
        autospec=True,
    ) as mock_flush:
        handler.close()

    assert mock_flush.call_count == 1
    assert len(handler._buffer) == 0


def test_flush_locked_success() -> None:
    handler = create_parquet_handler()
    record, _ = create_log_record()
    handler._buffer.append(handler._record_to_row(record))

    with patch(
        "opg_pipeline_builder.components.log.wr.s3.to_parquet",
        autospec=True,
    ) as mock_to_parquet:
        handler._flush_locked()

    assert mock_to_parquet.call_count == 1
    assert len(handler._buffer) == 0


def test_flush_locked_single_part(s3: boto3.client) -> None:
    """Test that _flush_locked writes to mocked S3."""
    s3.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    pid = os.getpid()

    handler = create_parquet_handler()
    _, record_1 = create_log_record(line_number=42)
    _, record_2 = create_log_record(line_number=43)
    _, record_3 = create_log_record(line_number=44)

    handler._buffer = [record_1, record_2, record_3]

    handler._flush_locked()

    written_log = wr.s3.read_parquet(
        path=f"s3://test-bucket/prefix/to/log/test-database/run_date=20240104/run_datetime=20240104T101505Z/{pid}_0.snappy.parquet",
    )

    pd.testing.assert_frame_equal(
        written_log, pd.DataFrame([record_1, record_2, record_3]), check_dtype=False
    )
    assert len(handler._buffer) == 0


def test_flush_locked_multiple_parts(s3: boto3.client) -> None:
    """Test that _flush_locked writes multiple parts to mocked S3 when buffer exceeds batch size."""
    s3.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    pid = os.getpid()

    handler = create_parquet_handler(batch_size=2)
    _, record_1 = create_log_record(line_number=42)
    _, record_2 = create_log_record(line_number=43)
    _, record_3 = create_log_record(line_number=44)

    handler._buffer = [record_1, record_2]
    handler._flush_locked()
    handler._buffer = [record_3]
    handler._flush_locked()

    written_log_part_0 = wr.s3.read_parquet(
        path=f"s3://test-bucket/prefix/to/log/test-database/run_date=20240104/run_datetime=20240104T101505Z/{pid}_0.snappy.parquet",
    )
    written_log_part_1 = wr.s3.read_parquet(
        path=f"s3://test-bucket/prefix/to/log/test-database/run_date=20240104/run_datetime=20240104T101505Z/{pid}_1.snappy.parquet",
    )

    pd.testing.assert_frame_equal(
        written_log_part_0, pd.DataFrame([record_1, record_2]), check_dtype=False
    )
    pd.testing.assert_frame_equal(
        written_log_part_1, pd.DataFrame([record_3]), check_dtype=False
    )


def test_flush_locked_empty_buffer() -> None:
    """Test that _flush_locked does nothing when buffer is empty."""
    handler = create_parquet_handler()

    with patch(
        "opg_pipeline_builder.components.log.wr.s3.to_parquet",
        autospec=True,
    ) as mock_to_parquet:
        handler._flush_locked()

    assert mock_to_parquet.call_count == 0
    assert len(handler._buffer) == 0


def test_record_to_row_success() -> None:
    handler = create_parquet_handler()

    log_record, _ = create_log_record()
    out_dict = handler._record_to_row(log_record)

    assert out_dict == {
        "logger_name": "opg_pipeline_builder.test_module",
        "module": "test_module",
        "function": "test_func",
        "line_number": 42,
        "database_name": "test_db",
        "data_delivery_period": datetime(2024, 1, 2, tzinfo=UTC),
        "attempt_no": 1,
        "pipeline_activity": "Validation",
        "table_name": "table_a",
        "field_name": "field_a",
        "log_level": "INFO",
        "log_timestamp": datetime(2024, 1, 4, 10, 16, 6, tzinfo=UTC),
        "message": "test log",
    }


def test_record_to_row_validation_error() -> None:
    handler = create_parquet_handler()
    record, _ = create_log_record(line_number="not_an_int")

    out_dict = handler._record_to_row(record)

    assert out_dict == {
        "logger_name": "opg_pipeline_builder.test_module",
        "module": "test_module",
        "function": "test_func",
        "line_number": 0,
        "database_name": "Unknown",
        "data_delivery_period": datetime(1970, 1, 1, tzinfo=UTC),
        "attempt_no": 0,
        "pipeline_activity": "Logging Error",
        "table_name": "Unknown",
        "field_name": "Unknown",
        "log_level": "INFO",
        "log_timestamp": datetime(2024, 1, 4, 10, 16, 6, tzinfo=UTC),
        "message": (
            "Failed to parse custom log fields: {'database_name': 'test_db', 'data_delivery_period': datetime.datetime(2024, 1, 2, 0, 0, tzinfo=datetime.timezone.utc), 'attempt_no': 1, 'pipeline_activity': 'Validation', 'table_name': 'table_a', 'field_name': 'field_a'}"
        ),
    }


def test_configure_logging_create_correct_handlers() -> None:
    """Test that configure_logging creates both console and parquet handlers."""
    package_logger = configure_logging(
        bucket="test-bucket",
        prefix="pipeline-logs",
        session_datetime=datetime(2024, 1, 6, 10, 30, tzinfo=UTC),
        batch_size=100,
    )

    assert any(
        _CONSOLE_HANDLER_NAME == handler.get_name()
        for handler in package_logger.handlers
    )
    assert any(
        _PARQUET_HANDLER_NAME == handler.get_name()
        for handler in package_logger.handlers
    )

    assert package_logger.level == logging.INFO
    assert package_logger.propagate is False
    parquet_handler: ParquetLogHandler = package_logger.handlers[1]  # type: ignore[assignment]
    assert parquet_handler._bucket == "test-bucket"
    assert parquet_handler._prefix == "pipeline-logs"
    assert parquet_handler._session_datetime == datetime(2024, 1, 6, 10, 30, tzinfo=UTC)
    assert parquet_handler._batch_size == 100


def test_configure_logging_no_second_configure() -> None:
    """Test that configure_logging raises RuntimeError if called a second time."""
    _ = configure_logging(
        bucket="test-bucket",
        prefix="pipeline-logs",
        session_datetime=datetime(2024, 1, 6, 10, 30, tzinfo=UTC),
    )

    with pytest.raises(RuntimeError, match="Logger has already been configured"):
        configure_logging(
            bucket="test-bucket",
            prefix="pipeline-logs",
            session_datetime=datetime(2024, 1, 6, 10, 30, tzinfo=UTC),
        )


def test_multiple_module_loggers_write_to_mocked_s3(
    s3: boto3.client,
) -> None:
    """Test that multiple loggers from different modules write to the same ParquetLogHandler and mocked S3."""
    s3.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    package_logger = configure_logging(
        bucket="test-bucket",
        prefix="pipeline-logs",
        session_datetime=datetime(2024, 1, 6, 10, 30, tzinfo=UTC),
    )

    logger_1 = logging.getLogger("opg_pipeline_builder.module_1")
    logger_2 = logging.getLogger("opg_pipeline_builder.module_2")

    custom_fields = {
        "database_name": "test_db",
        "data_delivery_period": datetime(2024, 1, 2, tzinfo=UTC),
        "attempt_no": 1,
        "pipeline_activity": "Validation",
        "table_name": "table_a",
        "field_name": "field_a",
    }

    logger_1.info("Log 1 from module 1", extra={"custom_fields": custom_fields})
    logger_1.info("Log 2 from module 1", extra={"custom_fields": custom_fields})
    logger_2.info("Log 1 from module 2", extra={"custom_fields": custom_fields})
    logger_2.info("Log 2 from module 2", extra={"custom_fields": custom_fields})
    logger_1.info("Log 3 from module 1", extra={"custom_fields": custom_fields})

    package_logger.handlers[1].close()

    pid = os.getpid()

    written_log = wr.s3.read_parquet(
        path=f"s3://test-bucket/pipeline-logs/test-database/run_date=20240106/run_datetime=20240106T103000Z/{pid}_0.snappy.parquet",
    )

    assert list(written_log["logger_name"]) == [
        "opg_pipeline_builder.module_1",
        "opg_pipeline_builder.module_1",
        "opg_pipeline_builder.module_2",
        "opg_pipeline_builder.module_2",
        "opg_pipeline_builder.module_1",
    ]
    assert list(written_log["message"]) == [
        "Log 1 from module 1",
        "Log 2 from module 1",
        "Log 1 from module 2",
        "Log 2 from module 2",
        "Log 3 from module 1",
    ]
    assert list(written_log["database_name"]) == ["test_db"] * 5
    assert list(written_log["pipeline_activity"]) == ["Validation"] * 5
    assert len(written_log) == 5


def _mp_worker(process_num: int) -> None:
    """Emit logs in a thread using parent's logger."""
    logger = logging.getLogger(PACKAGE_LOGGER_NAME)

    custom_fields = {
        "database_name": "test-database",
        "data_delivery_period": datetime(2024, 1, 2, tzinfo=UTC),
        "attempt_no": 1,
        "pipeline_activity": "Validation",
        "table_name": "table_a",
        "field_name": "field_a",
    }

    with freeze_time("2024-01-06 10:30:00") as frozen_time:
        for i in range(6):
            logger.info(
                f"Log {i + 1} from process {process_num}",
                extra={"custom_fields": custom_fields},
            )
            frozen_time.tick(delta=timedelta(seconds=1))


def test_multiprocessing_generates_pid_isolated_output_paths(s3: boto3.client) -> None:
    """Test that two threads write logs with PID-isolated output paths and parts logic works."""
    s3.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    logger = configure_logging(
        bucket="test-bucket",
        prefix="pipeline-logs",
        session_datetime=datetime(2024, 1, 6, 10, 30, tzinfo=UTC),
        batch_size=3,
    )

    # Use threads to share S3 mock context
    thread_1 = Thread(target=_mp_worker, args=(1,))
    thread_2 = Thread(target=_mp_worker, args=(2,))

    thread_1.start()
    thread_2.start()
    thread_1.join(timeout=30)
    thread_2.join(timeout=30)

    for handler in logger.handlers:
        handler.close()

    expected_num_of_log_files = 4

    all_logs = wr.s3.list_objects(
        path="s3://test-bucket/pipeline-logs/test-database/run_date=20240106/run_datetime=20240106T103000Z/"
    )
    assert len(all_logs) == expected_num_of_log_files

    loaded_files = []
    for i in range(expected_num_of_log_files):
        log_file = wr.s3.read_parquet(
            path=f"s3://test-bucket/pipeline-logs/test-database/run_date=20240106/run_datetime=20240106T103000Z/{os.getpid()}_{i}.snappy.parquet",
        )
        loaded_files.append(log_file)

    combined_logs = pd.concat(loaded_files, ignore_index=True)

    # Build expected logs with predictable timestamps from freezegun
    expected_logs = []
    for process_num in [1, 2]:
        for log_num in range(1, 7):
            expected_logs.append(
                {
                    "logger_name": PACKAGE_LOGGER_NAME,
                    "module": "test_log",
                    "function": "_mp_worker",
                    "line_number": 449,
                    "database_name": "test-database",
                    "data_delivery_period": datetime(2024, 1, 2, tzinfo=UTC),
                    "attempt_no": 1,
                    "pipeline_activity": "Validation",
                    "table_name": "table_a",
                    "field_name": "field_a",
                    "log_level": "INFO",
                    "log_timestamp": datetime(
                        2024, 1, 6, 10, 30, log_num - 1, tzinfo=UTC
                    ),
                    "message": f"Log {log_num} from process {process_num}",
                }
            )

    expected_df = pd.DataFrame(expected_logs)

    # Sort both by message for deterministic comparison
    combined_logs_sorted = combined_logs.sort_values("message").reset_index(drop=True)
    expected_df_sorted = expected_df.sort_values("message").reset_index(drop=True)

    # Compare exactly
    pd.testing.assert_frame_equal(
        combined_logs_sorted,
        expected_df_sorted,
        check_dtype=False,
    )
