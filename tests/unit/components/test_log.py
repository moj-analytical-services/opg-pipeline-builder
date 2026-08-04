import logging
import os
from collections.abc import Generator
from datetime import UTC, datetime
from threading import Thread
from typing import Literal, TypedDict
from unittest.mock import patch

import awswrangler as wr
import boto3
import pandas as pd
import pytest
from freezegun import freeze_time
from pydantic import ValidationError

from opg_pipeline_builder.components.log import (
    _CONSOLE_HANDLER_NAME,
    _PARQUET_HANDLER_NAME,
    PACKAGE_LOGGER_NAME,
    CustomFields,
    ParquetLogHandler,
    StructuredLogRecord,
    _validate_log_location,
    _validate_logger_inputs,
    configure_logging,
)


def create_parquet_handler(
    data_delivery_period: datetime = datetime(2024, 1, 2, tzinfo=UTC),
    batch_size: int = 2,
) -> ParquetLogHandler:
    """Fixture to provide a ParquetLogHandler instance for tests."""
    return ParquetLogHandler(
        bucket=os.environ.get("DEFAULT_BUCKET", ""),
        prefix="prefix/to/log",
        database_name="test-database",
        data_delivery_period=data_delivery_period,
        attempt_no=1,
        batch_size=batch_size,
    )


class LogRecordTypedDict(TypedDict):
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


class StructuredLogRecordTypedDict(LogRecordTypedDict):
    database_name: str
    data_delivery_period: datetime
    attempt_no: int


def create_log_record(
    log_timestamp: datetime = datetime(2024, 1, 4, 10, 16, 6, tzinfo=UTC),
    line_number: int = 42,
    msg: str = "test log",
    data_delivery_period: datetime = datetime(2024, 1, 2, tzinfo=UTC),
) -> tuple[logging.LogRecord, LogRecordTypedDict, StructuredLogRecordTypedDict]:
    """Create a sample log record for testing."""
    log_record = {
        "name": "opg_pipeline_builder.test_module",
        "module": "test_module",
        "funcName": "test_func",
        "lineno": line_number,
        "levelname": "INFO",
        "msg": msg,
        "args": (),
        "created": log_timestamp.timestamp(),
        "custom_fields": {
            "pipeline_activity": "Validation",
            "process_stage": "Processing",
            "table_name": "table_a",
            "field_name": "field_a",
        },
    }

    log_record_obj = logging.makeLogRecord(log_record)
    log_record_dict: LogRecordTypedDict = {
        "logger_name": "opg_pipeline_builder.test_module",
        "module": "test_module",
        "function": "test_func",
        "line_number": line_number,
        "log_level": "INFO",
        "log_timestamp": log_timestamp,
        "pipeline_activity": "Validation",
        "process_stage": "Processing",
        "table_name": "table_a",
        "field_name": "field_a",
        "message": msg,
    }
    structured_log_record_dict: StructuredLogRecordTypedDict = {
        "database_name": "test-database",
        "data_delivery_period": data_delivery_period,
        "attempt_no": 1,
        **log_record_dict,
    }

    return log_record_obj, log_record_dict, structured_log_record_dict


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


def test_custom_fields_reject_extra() -> None:
    """Test that StructuredLogRecord rejects extra fields."""
    with pytest.raises(ValidationError) as exc_info:
        CustomFields(
            pipeline_activity="Validation",
            process_stage="Processing",
            table_name="table_a",
            field_name="field_a",
            extra_field="not_allowed",  # type: ignore
        )

    assert "Extra inputs are not permitted" in str(exc_info.value)


def test_structured_log_record_reject_extra() -> None:
    """Test that StructuredLogRecord rejects extra fields."""
    with pytest.raises(ValidationError) as exc_info:
        _, __, structured_log_record_dict = create_log_record()
        StructuredLogRecord(
            **structured_log_record_dict,
            extra_field="not_allowed",  # type: ignore
        )
    assert "Extra inputs are not permitted" in str(exc_info.value)


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
    """Test that StructuredLogRecord raises ValueError for non-timezone-aware datetime fields."""
    with pytest.raises(ValueError, match="Datetime fields must be timezone-aware."):
        _, __, structured_log_record_dict = create_log_record(
            log_timestamp, data_delivery_period=data_delivery_period
        )
        StructuredLogRecord(
            **structured_log_record_dict,
        )


@pytest.mark.parametrize(
    (
        "bucket",
        "prefix",
        "database_name",
        "data_delivery_period",
        "attempt_no",
        "batch_size",
        "err",
    ),
    [
        (
            "",
            "prefix",
            "db",
            datetime(2024, 1, 1, tzinfo=UTC),
            1,
            2,
            "bucket name must be non-empty",
        ),
        (
            "bucket",
            "",
            "db",
            datetime(2024, 1, 1, tzinfo=UTC),
            1,
            2,
            "prefix name must be non-empty",
        ),
        (
            "bucket",
            "prefix",
            "",
            datetime(2024, 1, 1, tzinfo=UTC),
            1,
            2,
            "database_name must be non-empty",
        ),
        (
            "bucket",
            "prefix",
            "db",
            datetime(2024, 1, 1, tzinfo=UTC),
            1,
            2,
            "data_delivery_period must be timezone-aware",
        ),
        (
            "bucket",
            "prefix",
            "db",
            datetime(2024, 1, 1, tzinfo=UTC),
            -1,
            2,
            "attempt_no must be >= 1",
        ),
        (
            "bucket",
            "prefix",
            "db",
            datetime(2024, 1, 1, tzinfo=UTC),
            1,
            0,
            "batch_size must be >= 1",
        ),
    ],
)
def test_validate_logger_inputs(
    bucket: str,
    prefix: str,
    database_name: str,
    data_delivery_period: datetime,
    attempt_no: int,
    batch_size: int,
    err: str,
) -> None:
    """Test that _validate_logger_inputs raises ValueError for invalid inputs."""
    with pytest.raises(ValueError, match=err):
        _validate_logger_inputs(
            bucket=bucket,
            prefix=prefix,
            database_name=database_name,
            data_delivery_period=data_delivery_period,
            attempt_no=attempt_no,
            batch_size=batch_size,
        )


def test_validate_log_location_success(s3: boto3.client) -> None:
    """Test that validate log location correctly creates and deletes test file in S3."""
    s3.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    _validate_log_location(
        bucket="test-bucket",
        prefix="test/log",
        database_name="test-database",
        data_delivery_period=datetime(2024, 1, 2, tzinfo=UTC),
        attempt_no=1,
    )

    assert not wr.s3.does_object_exist(
        path="s3://test-bucket/test/log/test_test-database_20240102T000000_1.snappy.parquet"
    )


def test_validate_log_location_fail(s3: boto3.client) -> None:
    """Test that validate log location correctly raises a RuntimeError."""
    s3.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    with (
        patch(
            "opg_pipeline_builder.components.log.wr.s3.to_parquet",
            side_effect=ValueError("S3 write failed"),
        ),
        pytest.raises(RuntimeError) as exc_info,
    ):
        _validate_log_location(
            bucket="test-bucket",
            prefix="test/log",
            database_name="test-database",
            data_delivery_period=datetime(2024, 1, 2, tzinfo=UTC),
            attempt_no=1,
        )

    assert "Failed to write test log to s3" in str(exc_info.value)


def test_emit_with_flush() -> None:
    """Test that emit method correctly acquires and releases and calls flush_locked when buffer reaches batch size."""
    handler = create_parquet_handler()

    record_1, _, __ = create_log_record(line_number=42)
    record_2, _, __ = create_log_record(line_number=43)

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
    record_1, __, structured_log_record_dict_1 = create_log_record(line_number=42)
    record_2, __, structured_log_record_dict_2 = create_log_record(line_number=43)

    with patch(
        "opg_pipeline_builder.components.log.ParquetLogHandler._flush_locked",
        side_effect=mock_flush_locked,
        autospec=True,
    ) as mock_flush:
        for record in [record_1, record_2]:
            handler.emit(record)

    assert mock_flush.call_count == 0
    assert handler._buffer == [
        structured_log_record_dict_1,
        structured_log_record_dict_2,
    ]


def test_close_success() -> None:
    """Test the close method calls flush and empties the buffer."""
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


def test_close_fail() -> None:
    """Test the close method calls flush and raises RuntimeError when flush fails."""
    handler = create_parquet_handler()

    handler._buffer.append({"mock": "record"})

    with (
        patch(
            "opg_pipeline_builder.components.log.ParquetLogHandler._flush_locked",
            side_effect=None,
        ) as mock_flush,
        pytest.raises(RuntimeError, match="Failed to write all log records to S3"),
    ):
        handler.close()

    assert mock_flush.call_count == 1
    assert len(handler._buffer) == 0


def test_flush_locked_single_part(s3: boto3.client) -> None:
    """Test that _flush_locked writes to mocked S3."""
    s3.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    pid = os.getpid()

    handler = create_parquet_handler()
    _, _, record_1 = create_log_record(line_number=42)
    _, _, record_2 = create_log_record(line_number=43)
    _, _, record_3 = create_log_record(line_number=44)

    handler._buffer = [record_1, record_2, record_3]  # type: ignore[list-item]

    handler._flush_locked()

    written_log = wr.s3.read_parquet(
        path=f"s3://test-bucket/prefix/to/log/test-database/data_delivery_period=20240102/attempt_no=1/{pid}_0.snappy.parquet",
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
    _, _, record_1 = create_log_record(line_number=42)
    _, _, record_2 = create_log_record(line_number=43)
    _, _, record_3 = create_log_record(line_number=44)

    handler._buffer = [record_1, record_2]  # type: ignore[list-item]
    handler._flush_locked()
    handler._buffer = [record_3]  # type: ignore[list-item]
    handler._flush_locked()

    written_log_part_0 = wr.s3.read_parquet(
        path=f"s3://test-bucket/prefix/to/log/test-database/data_delivery_period=20240102/attempt_no=1/{pid}_0.snappy.parquet",
    )
    written_log_part_1 = wr.s3.read_parquet(
        path=f"s3://test-bucket/prefix/to/log/test-database/data_delivery_period=20240102/attempt_no=1/{pid}_1.snappy.parquet",
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


def test_flush_locked_fail(
    s3: boto3.client, capsys: pytest.CaptureFixture[str]
) -> None:
    """Test that _flush_locked increases batch size to hard limit and fails, if repeated writes fail."""
    s3.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    handler = create_parquet_handler(batch_size=1)
    _, _, record_1 = create_log_record(line_number=42)
    handler._buffer = [record_1]  # type: ignore[list-item]

    with (
        patch(
            "opg_pipeline_builder.components.log.wr.s3.to_parquet",
            side_effect=ValueError("S3 write failed"),
            autospec=True,
        ) as mock_write,
        pytest.raises(
            RuntimeError, match="Parquet log sink failure threshold exceeded."
        ),
    ):
        # Simulate flush locked failing and then being called later in the pipeline again, until it raises an exception
        handler._flush_locked()
        handler._flush_locked()
        handler._flush_locked()
        handler._flush_locked()
        handler._flush_locked()  # This one should not be reached

    output = capsys.readouterr().out

    assert "Failed to write a batch of logs to S3 (failure_count=1)" in output
    assert "Failed to write a batch of logs to S3 (failure_count=2)" in output
    assert "Failed to write a batch of logs to S3 (failure_count=3)" in output
    assert "Failed to write a batch of logs to S3 (failure_count=4)" in output
    assert "Failed to write a batch of logs to S3 (failure_count=5)" not in output
    assert "Failed to write 4 logs to" in output
    assert mock_write.call_count == 4
    assert handler._batch_size == 5
    assert len(handler._buffer) == 4


def test_flush_locked_fail_then_success(
    s3: boto3.client, capsys: pytest.CaptureFixture[str]
) -> None:
    """Test that _flush_locked increases batch size to hard limit and fails, if repeated writes fail."""
    s3.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    handler = create_parquet_handler(batch_size=1)
    _, _, record_1 = create_log_record(line_number=42)
    handler._buffer = [record_1]  # type: ignore[list-item]

    with (
        patch(
            "opg_pipeline_builder.components.log.wr.s3.to_parquet",
            side_effect=ValueError("S3 write failed"),
            autospec=True,
        ) as mock_write,
        freeze_time("2024-01-02 12:00:00"),
    ):
        # Simulate flush locked failing and then being called later in the pipeline again, until it raises an exception
        handler._flush_locked()

    output = capsys.readouterr().out
    assert "Failed to write a batch of logs to S3 (failure_count=1)" in output
    assert "Failed to write a batch of logs to S3 (failure_count=2)" not in output
    assert mock_write.call_count == 1
    assert handler._batch_size == 2
    assert len(handler._buffer) == 2

    handler._flush_locked()

    out_file = wr.s3.read_parquet(
        path=f"s3://test-bucket/prefix/to/log/test-database/data_delivery_period=20240102/attempt_no=1/{os.getpid()}_0.snappy.parquet",
    )

    assert handler._batch_size == 1
    assert len(handler._buffer) == 0
    pd.testing.assert_frame_equal(
        out_file,
        pd.DataFrame(
            [
                record_1,
                {
                    "database_name": "test-database",
                    "data_delivery_period": datetime(2024, 1, 2, tzinfo=UTC),
                    "attempt_no": 1,
                    "logger_name": PACKAGE_LOGGER_NAME,
                    "module": "opg_pipeline_builder.components.log",
                    "function": "_flush_locked",
                    "line_number": 0,
                    "log_level": "ERROR",
                    "log_timestamp": datetime(2024, 1, 2, 12, 0, tzinfo=UTC),
                    "pipeline_activity": "Logging",
                    "process_stage": "Processing",
                    "table_name": "Unknown",
                    "field_name": "Unknown",
                    "message": (
                        f"Failed to write 1 logs to s3://test-bucket/prefix/to/log/test-database/data_delivery_period=20240102/attempt_no=1/{os.getpid()}_0.snappy.parquet: S3 write failed (failure_count=1)."
                    ),
                },
            ]
        ),
        check_dtype=False,
    )


def test_record_to_row_success() -> None:
    """Test that _record_to_row correctly converts a log record to a dictionary."""
    handler = create_parquet_handler()

    log_record, _, __ = create_log_record()
    out_dict = handler._record_to_row(log_record)

    assert out_dict == {
        "database_name": "test-database",
        "data_delivery_period": datetime(2024, 1, 2, tzinfo=UTC),
        "attempt_no": 1,
        "logger_name": "opg_pipeline_builder.test_module",
        "module": "test_module",
        "function": "test_func",
        "line_number": 42,
        "log_level": "INFO",
        "log_timestamp": datetime(2024, 1, 4, 10, 16, 6, tzinfo=UTC),
        "pipeline_activity": "Validation",
        "process_stage": "Processing",
        "table_name": "table_a",
        "field_name": "field_a",
        "message": "test log",
    }


def test_record_to_row_validation_error() -> None:
    """Test that _record_to_row handles ValidationError and returns a log record with error details."""
    handler = create_parquet_handler()
    record, _, __ = create_log_record(line_number="not_an_int")  # type: ignore[arg-type]

    with freeze_time("2024-01-02 00:00:00"):
        out_dict = handler._record_to_row(record)

    assert out_dict == {
        "database_name": "test-database",
        "data_delivery_period": datetime(2024, 1, 2, tzinfo=UTC),
        "attempt_no": 1,
        "logger_name": "opg_pipeline_builder.test_module",
        "module": "test_module",
        "function": "test_func",
        "line_number": 0,
        "log_level": "ERROR",
        "log_timestamp": datetime(2024, 1, 2, 0, 0, tzinfo=UTC),
        "pipeline_activity": "Logging",
        "process_stage": "Processing",
        "table_name": "table_a",
        "field_name": "field_a",
        "message": (
            "Failed to parse custom log fields: {'pipeline_activity': 'Validation', 'process_stage': 'Processing', 'table_name': 'table_a', 'field_name': 'field_a'}"
        ),
    }


def test_configure_logging_create_correct_handlers() -> None:
    """Test that configure_logging creates both console and parquet handlers."""
    with (
        patch(
            "opg_pipeline_builder.components.log._validate_logger_inputs",
            autospec=True,
        ) as mock_validate_inputs,
        patch(
            "opg_pipeline_builder.components.log._validate_log_location",
            autospec=True,
        ) as mock_validate_location,
    ):
        package_logger = configure_logging(
            bucket="test-bucket",
            prefix="pipeline-logs",
            database_name="test-database",
            data_delivery_period=datetime(2024, 1, 2, tzinfo=UTC),
            attempt_no=1,
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
    assert parquet_handler._database_name == "test-database"
    assert parquet_handler._data_delivery_period == datetime(2024, 1, 2, tzinfo=UTC)
    assert parquet_handler._attempt_no == 1
    assert parquet_handler._batch_size == 100
    assert mock_validate_inputs.call_count == 1
    assert mock_validate_location.call_count == 1


def test_configure_logging_no_second_configure() -> None:
    """Test that configure_logging raises RuntimeError if called a second time."""
    with patch(
        "opg_pipeline_builder.components.log._validate_log_location", autospec=True
    ):
        _ = configure_logging(
            bucket="test-bucket",
            prefix="pipeline-logs",
            database_name="test-database",
            data_delivery_period=datetime(2024, 1, 2, tzinfo=UTC),
            attempt_no=1,
            batch_size=100,
        )

    with pytest.raises(RuntimeError, match="Logger has already been configured"):
        configure_logging(
            bucket="test-bucket",
            prefix="pipeline-logs",
            database_name="test-database",
            data_delivery_period=datetime(2024, 1, 3, tzinfo=UTC),
            attempt_no=1,
            batch_size=100,
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
        database_name="test-database",
        attempt_no=1,
        data_delivery_period=datetime(2024, 1, 6, tzinfo=UTC),
        batch_size=5,
    )

    logger_1 = logging.getLogger("opg_pipeline_builder.module_1")
    logger_2 = logging.getLogger("opg_pipeline_builder.module_2")

    custom_fields = {
        "pipeline_activity": "Validation",
        "process_stage": "Processing",
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
        path=f"s3://test-bucket/pipeline-logs/test-database/data_delivery_period=20240106/attempt_no=1/{pid}_0.snappy.parquet",
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
    assert list(written_log["database_name"]) == ["test-database"] * 5
    assert list(written_log["pipeline_activity"]) == ["Validation"] * 5
    assert len(written_log) == 5


def _mp_worker(process_num: int) -> None:
    """Emit logs in a thread using parent's logger."""
    logger = logging.getLogger(PACKAGE_LOGGER_NAME)

    custom_fields = {
        "pipeline_activity": "Validation",
        "process_stage": "Processing",
        "table_name": "table_a",
        "field_name": "field_a",
    }

    for i in range(6):
        logger.info(
            f"Log {i + 1} from process {process_num}",
            extra={"custom_fields": custom_fields},
        )


def test_multiprocessing_generates_pid_isolated_output_paths(s3: boto3.client) -> None:
    """Test that two threads write logs with PID-isolated output paths and parts logic works."""
    s3.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    logger = configure_logging(
        bucket="test-bucket",
        prefix="pipeline-logs",
        database_name="test-database",
        data_delivery_period=datetime(2024, 1, 6, tzinfo=UTC),
        attempt_no=1,
        batch_size=3,
    )

    # Use threads to share S3 mock context
    test_window_start = datetime.now(tz=UTC)
    thread_1 = Thread(target=_mp_worker, args=(1,))
    thread_2 = Thread(target=_mp_worker, args=(2,))

    thread_1.start()
    thread_2.start()
    thread_1.join(timeout=30)
    thread_2.join(timeout=30)
    test_window_end = datetime.now(tz=UTC)

    for handler in logger.handlers:
        handler.close()

    expected_num_of_log_files = 4

    all_logs = wr.s3.list_objects(
        path="s3://test-bucket/pipeline-logs/test-database/data_delivery_period=20240106/"
    )
    assert len(all_logs) == expected_num_of_log_files

    loaded_files = []
    for i in range(expected_num_of_log_files):
        log_file = wr.s3.read_parquet(
            path=f"s3://test-bucket/pipeline-logs/test-database/data_delivery_period=20240106/attempt_no=1/{os.getpid()}_{i}.snappy.parquet",
        )
        loaded_files.append(log_file)

    combined_logs = pd.concat(loaded_files, ignore_index=True)

    # Build expected logs for deterministic fields.
    expected_logs = []
    for process_num in [1, 2]:
        for log_num in range(1, 7):
            expected_logs.append(
                {
                    "database_name": "test-database",
                    "data_delivery_period": datetime(2024, 1, 6, tzinfo=UTC),
                    "attempt_no": 1,
                    "logger_name": PACKAGE_LOGGER_NAME,
                    "module": "test_log",
                    "function": "_mp_worker",
                    "line_number": 778,
                    "log_level": "INFO",
                    "pipeline_activity": "Validation",
                    "process_stage": "Processing",
                    "table_name": "table_a",
                    "field_name": "field_a",
                    "message": f"Log {log_num} from process {process_num}",
                }
            )

    expected_df = pd.DataFrame(expected_logs)

    # Sort both by message for deterministic comparison
    combined_logs_sorted = combined_logs.sort_values("message").reset_index(drop=True)
    expected_df_sorted = expected_df.sort_values("message").reset_index(drop=True)

    combined_logs_sorted["log_timestamp"] = pd.to_datetime(
        combined_logs_sorted["log_timestamp"], utc=True
    )

    timestamp_series = combined_logs_sorted["log_timestamp"]
    allowed_range_start = test_window_start - pd.Timedelta(seconds=1)
    allowed_range_end = test_window_end + pd.Timedelta(seconds=1)
    timestamps_in_range = timestamp_series.between(
        allowed_range_start, allowed_range_end, inclusive="both"
    )

    # Time checks are tolerant to scheduler jitter but still validate correctness.
    assert timestamp_series.notna().all()
    assert timestamp_series.dt.tz is not None
    assert timestamps_in_range.sum() >= 10

    # Compare deterministic fields exactly.
    pd.testing.assert_frame_equal(
        combined_logs_sorted.drop(columns=["log_timestamp"]),
        expected_df_sorted,
        check_dtype=False,
    )
