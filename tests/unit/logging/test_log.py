# pyright: reportPrivateUsage=false

import inspect
import json
import logging
import os
from collections.abc import Generator
from contextlib import nullcontext as does_not_raise
from datetime import UTC, datetime, timedelta, timezone
from threading import Thread
from typing import Any
from unittest.mock import patch

import boto3
import pytest
from botocore.exceptions import BotoCoreError, ClientError
from freezegun import freeze_time
from pydantic import ValidationError

from opg_pipeline_builder.logging.log import (
    _CONSOLE_HANDLER_NAME,
    _JSONL_HANDLER_NAME,
    PACKAGE_LOGGER_NAME,
    CustomLogFields,
    JsonlLogHandler,
    LoggingController,
    ModuleLogger,
    StructuredLogRecord,
    configure_logging,
)

RUN_ID = "scheduled__2026-09-22T00:00:00+00:00"
DELIVERY_PERIOD = datetime(2024, 1, 2, tzinfo=UTC)
DEFAULT_BUCKET = "log-bucket"
DEFAULT_PREFIX = "prefix/to/log"
DEFAULT_DATABASE = "test-database"


def create_bucket(s3: boto3.client, bucket: str = "log-bucket") -> None:
    """Create an S3 bucket for a test."""
    s3.create_bucket(
        Bucket=bucket,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )


def create_handler(
    apply_patches: bool = True,
    bucket: str = DEFAULT_BUCKET,
    prefix: str = DEFAULT_PREFIX,
    database: str = DEFAULT_DATABASE,
    data_delivery_period: datetime = DELIVERY_PERIOD,
    attempt_no: int = 1,
    run_id: str = RUN_ID,
    batch_size: int = 2,
) -> JsonlLogHandler:
    """Create a JSONL handler backed by mocked S3."""
    if apply_patches:
        with (
            patch.object(JsonlLogHandler, "_validate_log_location"),
            patch.object(JsonlLogHandler, "_write_configuration_log"),
        ):
            return JsonlLogHandler(
                bucket=bucket,
                prefix=prefix,
                database=database,
                data_delivery_period=data_delivery_period,
                attempt_no=attempt_no,
                run_id=run_id,
                batch_size=batch_size,
            )
    else:
        return JsonlLogHandler(
            bucket=bucket,
            prefix=prefix,
            database=database,
            data_delivery_period=data_delivery_period,
            attempt_no=attempt_no,
            run_id=run_id,
            batch_size=batch_size,
        )


def create_log_record(
    *,
    timestamp: datetime = datetime(2024, 1, 4, 10, 16, 6, tzinfo=UTC),
    line_number: int = 42,
    message: str = "test log",
    args: tuple[object, ...] = (),
    custom_fields: CustomLogFields | None = None,
) -> logging.LogRecord:
    """Create a representative logging record."""
    custom_fields = custom_fields or CustomLogFields(
        process_stage="Processing",
        table="table_a",
        field="field_a",
    )
    return logging.makeLogRecord(
        {
            "name": "opg_pipeline_builder.test_module",
            "module": "test_module",
            "funcName": "test_func",
            "lineno": line_number,
            "levelname": "INFO",
            "levelno": logging.INFO,
            "msg": message,
            "args": args,
            "created": timestamp.timestamp(),
            "custom_fields": custom_fields,
        }
    )


def mock_flush_locked(handler: JsonlLogHandler) -> None:
    """Mock the _flush_locked method of a JsonlLogHandler."""
    handler._buffer = []


def read_jsonl_logs(
    s3: boto3.client,
    *,
    bucket: str = DEFAULT_BUCKET,
    prefix: str = f"{DEFAULT_PREFIX}",
) -> dict[str, list[dict[str, Any]]]:
    """Read all non-marker JSONL logs under a prefix."""
    logs = s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
    output: dict[str, list[dict[str, Any]]] = {}
    for item in logs:
        key = item["Key"]
        if key.endswith("logging-marker/marker.txt"):
            continue
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode()
        output[key] = [json.loads(line) for line in body.splitlines()]
    return output


@pytest.fixture(autouse=True)
def reset_package_logger() -> Generator[None]:
    """Restore the shared package logger without flushing stale handlers."""
    package_logger = logging.getLogger(PACKAGE_LOGGER_NAME)
    original_handlers = list(package_logger.handlers)
    original_level = package_logger.level
    original_propagate = package_logger.propagate
    package_logger.handlers.clear()

    yield

    for handler in list(package_logger.handlers):
        package_logger.removeHandler(handler)
        logging.Handler.close(handler)
    package_logger.handlers.extend(original_handlers)
    package_logger.setLevel(original_level)
    package_logger.propagate = original_propagate


class TestCustomLogFields:
    """Tests for the CustomLogFields model."""

    def test_accepts_valid_fields(self) -> None:
        """Test that valid custom log fields are accepted."""
        fields = CustomLogFields(
            table="table_a", field="field_a", process_stage="Start"
        )
        assert fields.model_dump() == {
            "table": "table_a",
            "field": "field_a",
            "process_stage": "Start",
        }

    @pytest.mark.parametrize("missing", [("table"), ("field"), ("process_stage")])
    def test_rejects_missing_fields(self, missing: str) -> None:
        """Test that missing required custom log fields are rejected."""
        values = {
            "table": "table_a",
            "field": "field_a",
            "process_stage": "Processing",
        }
        del values[missing]
        with pytest.raises(ValidationError):
            CustomLogFields.model_validate(values)

    def test_rejects_extra_fields(self) -> None:
        """Test that extra custom log fields are rejected."""
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            CustomLogFields.model_validate(
                {
                    "table": "table_a",
                    "field": "field_a",
                    "process_stage": "Processing",
                    "message": "reserved",
                }
            )

    def test_rejects_invalid_process_stage(self) -> None:
        """Test that an invalid process stage is rejected."""
        with pytest.raises(ValidationError):
            CustomLogFields(
                table="table_a",
                field="field_a",
                process_stage="Invalid",  # type: ignore[arg-type]
            )


class TestStructuredLogRecord:
    """Tests for the StructuredLogRecord model."""

    @property
    def valid_values(self) -> dict[str, Any]:
        """Create a dictionary of valid structured log record values."""
        return {
            "database": DEFAULT_DATABASE,
            "run_id": RUN_ID,
            "data_delivery_period": DELIVERY_PERIOD,
            "attempt_no": 1,
            "logger_name": "opg_pipeline_builder.module",
            "module": "module",
            "function": "function",
            "line_number": 42,
            "log_level": "INFO",
            "log_timestamp": datetime(2024, 1, 2, 12, tzinfo=UTC),
            "process_stage": "Processing",
            "table": "table_a",
            "field": "field_a",
            "message": "event",
        }

    def test_accepts_complete_record_and_json_dump(self) -> None:
        """Test that a complete structured log record is accepted and can be dumped to JSON."""
        record = StructuredLogRecord.model_validate(self.valid_values)
        output = record.model_dump()
        assert output == self.valid_values

    def test_rejects_extra_field(self) -> None:
        """Test that extra fields in a structured log record are rejected."""
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            StructuredLogRecord.model_validate(
                {**self.valid_values, "extra_field": "not allowed"}
            )

    @pytest.mark.parametrize("field", [("database"), ("run_id")])
    def test_validate_database_and_run_id_invalid(self, field: str) -> None:
        """Test that empty run context fields are rejected."""
        with pytest.raises(
            ValidationError,
            match=f"Validation of StructuredLogRecord failed: field '{field}' must be non-empty",
        ):
            StructuredLogRecord.model_validate({**self.valid_values, field: " "})

    @pytest.mark.parametrize(
        ("attempt_no", "exception"),
        [
            (0, pytest.raises(ValidationError)),
            (-1, pytest.raises(ValidationError)),
            (1, does_not_raise()),
            (99, does_not_raise()),
        ],
    )
    def test_validate_attempt_no_invalid(self, attempt_no: int, exception: Any) -> None:
        """Test that non-positive attempt numbers are rejected."""
        with exception:
            StructuredLogRecord.model_validate(
                {**self.valid_values, "attempt_no": attempt_no}
            )

    @pytest.mark.parametrize(
        ("field", "datetime_value"),
        [
            (
                "data_delivery_period",
                datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone(timedelta(hours=-5))),
            ),
            (
                "log_timestamp",
                datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone(timedelta(hours=-5))),
            ),
        ],
    )
    def test_validate_datetimes_are_timezone_aware_set_utc(
        self, field: str, datetime_value: datetime
    ) -> None:
        """Test that naive datetime fields are rejected."""
        log_record = StructuredLogRecord.model_validate(
            {**self.valid_values, field: datetime_value}
        )
        assert getattr(log_record, field) == datetime(2024, 1, 2, 17, 0, 0, tzinfo=UTC)

    @pytest.mark.parametrize(
        ("field"),
        [("data_delivery_period"), ("log_timestamp")],
    )
    def test_validate_datetimes_are_timezone_aware_invalid(self, field: str) -> None:
        """Test that naive datetime fields are rejected."""
        with pytest.raises(ValidationError):
            StructuredLogRecord.model_validate(
                {**self.valid_values, field: datetime(2024, 1, 2)}  # noqa: DTZ001
            )


class TestJsonlLogHandler:
    def test_init(self, s3: boto3.client) -> None:
        """Test that the handler initializes correctly with default values."""
        create_bucket(s3)
        with (
            patch.object(JsonlLogHandler, "_validate_log_location") as mock_location,
            patch.object(JsonlLogHandler, "_write_configuration_log") as mock_write,
        ):
            handler = create_handler(apply_patches=False)
            assert handler._bucket == DEFAULT_BUCKET
            assert handler._prefix == DEFAULT_PREFIX
            assert handler._database == DEFAULT_DATABASE
            assert handler._run_id == RUN_ID
            assert handler._data_delivery_period == DELIVERY_PERIOD
            assert handler._attempt_no == 1
            assert handler._base_batch_size == 2
            assert handler._batch_size == 2
            assert handler._part_number == 0
            assert handler._writer_id
            assert handler._buffer == []
            assert handler._write_failures == 0
            assert mock_location.call_count == 1
            assert mock_write.call_count == 1
            logging.Handler.close(handler)

    def test_validate_log_location_valid(self, s3: boto3.client) -> None:
        """Test that the marker file is generated, and only one ever exists."""
        create_bucket(s3)
        create_handler(apply_patches=False)
        create_handler(
            database="other-database",
            attempt_no=2,
            run_id="other-run",
            apply_patches=False,
        )
        create_handler(
            database="another-database",
            attempt_no=3,
            run_id="another-run",
            apply_patches=False,
        )

        keys = [
            item["Key"]
            for item in s3.list_objects_v2(
                Bucket=DEFAULT_BUCKET, Prefix=DEFAULT_PREFIX
            ).get("Contents", [])
        ]
        assert keys == [f"{DEFAULT_PREFIX}/logging-marker/marker.txt"]

    @pytest.mark.parametrize(
        ("exception"),
        [
            (BotoCoreError),
            (
                ClientError(
                    {"Error": {"Code": "AccessDenied", "Message": "denied"}},
                    "PutObject",
                )
            ),
            (OSError),
        ],
    )
    def test_validate_log_location_invalid(
        self, exception: Any, s3: boto3.client
    ) -> None:
        """Test that an exception is raised when the log location is invalid or inaccessible."""
        create_bucket(s3)
        with (
            patch.object(s3, "put_object", side_effect=exception),
            patch(
                "opg_pipeline_builder.logging.log.boto3.client",
                return_value=s3,
            ),
            pytest.raises(RuntimeError, match="Failed to prepare JSONL log location"),
        ):
            create_handler(apply_patches=False)

    def test_write_configuration_log(self, s3: boto3.client) -> None:
        """Test that the configuration log is written correctly."""
        create_bucket(s3)
        with freeze_time("2024-01-02T00:00:00Z"):
            handler = create_handler()
            handler._write_configuration_log()

        assert handler._buffer == [
            {
                "database": DEFAULT_DATABASE,
                "run_id": RUN_ID,
                "data_delivery_period": DELIVERY_PERIOD.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "attempt_no": 1,
                "logger_name": PACKAGE_LOGGER_NAME,
                "module": "opg_pipeline_builder.logging.log",
                "function": "configure_logging",
                "line_number": 150,
                "log_level": "INFO",
                "log_timestamp": "2024-01-02T00:00:00Z",
                "process_stage": "Start",
                "table": "N/A",
                "field": "N/A",
                "message": "Logging configured successfully.",
            }
        ]
        logging.Handler.close(handler)

    def test_emit_below_threshold(self) -> None:
        """Test that emit adds a record to the buffer but does not flush below the threshold."""
        handler = create_handler(batch_size=5)

        with (
            patch.object(JsonlLogHandler, "_record_to_row") as mock_record,
            patch.object(JsonlLogHandler, "_flush_locked") as mock_flush,
        ):
            mock_record.side_effect = (
                {"message": "First application event"},
                {"message": "Second application event"},
            )
            handler.emit(create_log_record(message="First application event"))
            handler.emit(create_log_record(message="Second application event"))

        assert mock_record.call_count == 2
        assert not mock_flush.called
        assert mock_record.call_args_list[0].args[0].msg == "First application event"
        assert mock_record.call_args_list[1].args[0].msg == "Second application event"
        assert handler._buffer == [
            {"message": "First application event"},
            {"message": "Second application event"},
        ]

    def test_emit_flushes_at_threshold(self) -> None:
        """Test that emit flushes the buffer when the threshold is reached."""
        handler = create_handler(batch_size=3)

        with (
            patch.object(JsonlLogHandler, "_record_to_row") as mock_record,
            patch.object(
                JsonlLogHandler,
                "_flush_locked",
                side_effect=mock_flush_locked,
                autospec=True,
            ) as mock_flush,
        ):
            mock_record.side_effect = (
                {"message": "First application event"},
                {"message": "Second application event"},
                {"message": "Third application event"},
            )
            handler.emit(create_log_record(message="First application event"))
            handler.emit(create_log_record(message="Second application event"))
            handler.emit(create_log_record(message="Third application event"))

        assert mock_record.call_count == 3
        assert mock_flush.called
        assert mock_record.call_args_list[0].args[0].msg == "First application event"
        assert mock_record.call_args_list[1].args[0].msg == "Second application event"
        assert mock_record.call_args_list[2].args[0].msg == "Third application event"
        assert handler._buffer == []

    def test_close_success(self) -> None:
        """Test that the handler closes successfully and flushes the buffer."""
        handler = create_handler(batch_size=3)
        handler._buffer = [
            {"message": "First application event"},
            {"message": "Second application event"},
        ]

        with patch.object(
            JsonlLogHandler,
            "_flush_locked",
            side_effect=mock_flush_locked,
            autospec=True,
        ) as mock_flush:
            handler.close()

        assert mock_flush.called
        assert handler._buffer == []

    def test_close_fail(self) -> None:
        """Test that the handler raises an exception if it failed to flush the buffer."""
        handler = create_handler()
        handler._buffer = [
            {"message": "First application event"},
            {"message": "Second application event"},
        ]

        with (
            patch.object(JsonlLogHandler, "_flush_locked", autospec=True) as mock_flush,
            pytest.raises(RuntimeError),
        ):
            handler.close()

        assert mock_flush.called
        assert handler._buffer == [
            {"message": "First application event"},
            {"message": "Second application event"},
        ]

    def test_object_key(self) -> None:
        """Test that the object key is generated correctly."""
        handler = create_handler()
        key = handler._object_key(str(os.getpid()), handler._part_number)
        assert (
            key
            == f"prefix/to/log/data_delivery_period=20240102/attempt_no=1/run_id={RUN_ID}/{os.getpid()}_{handler._writer_id}_0.jsonl"
        )

    def test_put_object_success(self, s3: boto3.client) -> None:
        """Test that the handler successfully puts an object to S3."""
        create_bucket(s3, "log-bucket")
        handler = create_handler()

        key = handler._object_key(str(os.getpid()), handler._part_number)
        bytes_data = b"some random data"

        handler._put_object(key, bytes_data)

        data = s3.get_object(Bucket="log-bucket", Key=key)
        assert data["Body"].read() == bytes_data

    def test_flush_locked_success(self, s3: boto3.client) -> None:
        """Test that the handler successfully flushes the buffer to S3."""
        create_bucket(s3, "log-bucket")
        handler = create_handler(apply_patches=False, batch_size=5)

        handler.emit(create_log_record(message="First application event"))
        handler.emit(create_log_record(message="Second application event"))
        handler.emit(create_log_record(message="Third application event"))

        handler._flush_locked()

        assert handler._buffer == []
        assert handler._part_number == 1
        assert handler._batch_size == 5

        handler.emit(create_log_record(message="Fourth application event"))

        handler._flush_locked()

        assert handler._buffer == []
        assert handler._part_number == 2
        assert handler._batch_size == 5

        logs = read_jsonl_logs(s3)

        log_1_key = f"prefix/to/log/data_delivery_period=20240102/attempt_no=1/run_id={RUN_ID}/{os.getpid()}_{handler._writer_id}_0.jsonl"

        assert log_1_key in logs
        log_1_records = logs[log_1_key]

        assert len(log_1_records) == 4
        assert log_1_records[0]["message"] == "Logging configured successfully."
        assert log_1_records[1]["message"] == "First application event"
        assert log_1_records[2]["message"] == "Second application event"
        assert log_1_records[3]["message"] == "Third application event"

        log_2_key = f"prefix/to/log/data_delivery_period=20240102/attempt_no=1/run_id={RUN_ID}/{os.getpid()}_{handler._writer_id}_1.jsonl"
        assert log_2_key in logs
        log_2_records = logs[log_2_key]

        assert len(log_2_records) == 1
        assert log_2_records[0]["message"] == "Fourth application event"

    def test_flush_locked_empty(self) -> None:
        """Test that flushing an empty buffer does not raise an error."""
        handler = create_handler(batch_size=5)
        with patch.object(JsonlLogHandler, "_put_object") as mock_put:
            handler._flush_locked()

        mock_put.assert_not_called()
        assert handler._buffer == []
        assert handler._part_number == 0
        assert handler._batch_size == 5

    @pytest.mark.parametrize(
        ("exception"),
        [
            (BotoCoreError()),
            (ClientError({}, "operation")),
            (OSError()),
            (RuntimeError()),
            (ValueError()),
        ],
    )
    def test_flush_locked_fail_then_succeed(
        self, exception: Any, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Test that the full buffer is correctly flushed after two failed attempts."""
        handler = create_handler(batch_size=5)

        for iteration in range(1, 3):
            handler.emit(create_log_record(message=f"application event: {iteration}"))
            with patch.object(
                JsonlLogHandler, "_put_object", side_effect=exception
            ) as mock_put:
                handler._flush_locked()
            assert handler._write_failures == iteration
            assert f"failure_count={iteration}" in capsys.readouterr().out
            assert handler._batch_size == 5 * (iteration + 1)
            assert any(
                "Failed to write JSONL" in log["message"] for log in handler._buffer
            )
            assert len(handler._buffer) == iteration * 2

        with patch.object(JsonlLogHandler, "_put_object") as mock_put:
            handler._flush_locked()

        mock_put.assert_called_once()
        assert handler._buffer == []
        assert handler._part_number == 1
        assert handler._batch_size == 5
        assert handler._write_failures == 0

    def test_flush_locked_fail_then_hit_threshold(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Test that the full buffer is correctly flushed after two failed attempts."""
        handler = create_handler(batch_size=5)

        for iteration in range(1, 4):
            handler.emit(create_log_record(message=f"application event: {iteration}"))
            with patch.object(JsonlLogHandler, "_put_object", side_effect=OSError):
                handler._flush_locked()
            assert handler._write_failures == iteration
            assert f"failure_count={iteration}" in capsys.readouterr().out
            assert handler._batch_size == 5 * (iteration + 1)
            assert len(handler._buffer) == iteration * 2

        handler.emit(create_log_record(message=f"application event: {iteration}"))
        with (
            patch.object(JsonlLogHandler, "_put_object", side_effect=OSError),
            pytest.raises(
                RuntimeError, match="Have attempted to write JSONL logs 4 times"
            ),
        ):
            handler._flush_locked()

    def test_record_to_row_success(self) -> None:
        """Test that a log record is correctly converted to a row."""
        handler = create_handler(batch_size=5)

        with freeze_time("2024-06-01 12:00:00"):
            record = create_log_record(
                timestamp=datetime.now(tz=UTC), message="application event: 1"
            )
            row = handler._record_to_row(record)
            assert row == {
                "database": DEFAULT_DATABASE,
                "run_id": RUN_ID,
                "data_delivery_period": DELIVERY_PERIOD.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "attempt_no": 1,
                "logger_name": "opg_pipeline_builder.test_module",
                "module": "test_module",
                "function": "test_func",
                "line_number": 42,
                "log_level": "INFO",
                "log_timestamp": "2024-06-01T12:00:00Z",
                "process_stage": "Processing",
                "table": "table_a",
                "field": "field_a",
                "message": "application event: 1",
            }

    @pytest.mark.parametrize(
        ("custom_fields"),
        [
            ({}),
            (1),
            (None),
        ],
    )
    def test_record_to_row_invalid(self, custom_fields: Any) -> None:
        """Test that an invalid log record raises an error."""
        handler = create_handler(batch_size=5)

        record = create_log_record(timestamp=datetime.now(tz=UTC))
        record.custom_fields = custom_fields
        with pytest.raises(ValueError, match="Custom log fields is missing"):
            handler._record_to_row(record)


class TestLoggingController:
    def create_controller(self) -> LoggingController:
        logger = logging.getLogger(PACKAGE_LOGGER_NAME)
        handler = create_handler(batch_size=5)
        logger.addHandler(handler)
        logger.addHandler(logging.StreamHandler())
        return LoggingController(logger)

    def test_flush_success(self) -> None:
        """Test that the flush method calls the handler's _flush_locked method."""
        controller = self.create_controller()
        with patch.object(
            controller._logger.handlers[0], "_flush_locked"
        ) as mock_flush_locked:
            controller.flush()

        mock_flush_locked.assert_called_once()

    def test_flush_fails_when_shutdown(self) -> None:
        """Test that the flush method raises an error if logging has been shut down."""
        controller = self.create_controller()
        controller._is_shutdown = True
        with pytest.raises(RuntimeError, match="Logging has already been shut down."):
            controller.flush()

    def test_shutdown_success(self) -> None:
        """Test that the shutdown method sets the _is_shutdown flag to True."""
        controller = self.create_controller()
        with patch.object(JsonlLogHandler, "close") as mock_close:
            controller.shutdown()

        assert mock_close.call_count == 1  # Only checking the json handler
        assert controller._is_shutdown
        assert not controller._logger.handlers

    def test_shutdown_fails_when_already_shutdown(self) -> None:
        """Test that the shutdown method raises an error if logging has already been shut down."""
        controller = self.create_controller()
        controller._is_shutdown = True
        with patch.object(JsonlLogHandler, "close") as mock_close:
            controller.shutdown()

        assert mock_close.call_count == 0

    def test_shutdown_error(self) -> None:
        """Test that the shutdown method handles errors during handler closure."""
        controller = self.create_controller()
        with (
            patch.object(
                JsonlLogHandler, "close", side_effect=RuntimeError
            ) as mock_close,
            pytest.raises(RuntimeError),
        ):
            controller.shutdown()

        assert mock_close.call_count == 1
        assert not controller._is_shutdown
        assert len(controller._logger.handlers) == 0

    def test_context_manager_calls_shutdown_on_normal_exit(self) -> None:
        """Test that exiting a `with` block normally shuts down logging."""
        controller = self.create_controller()
        with (
            patch.object(JsonlLogHandler, "close") as mock_close,
            controller,
        ):
            assert True

        assert mock_close.call_count == 1
        assert controller._is_shutdown

    def test_context_manager_calls_shutdown_and_propagates_body_error(self) -> None:
        """Test that a body exception still triggers shutdown and is not swallowed."""
        controller = self.create_controller()
        with (
            patch.object(JsonlLogHandler, "close") as mock_close,
            pytest.raises(ValueError, match="pipeline failed"),
            controller,
        ):
            raise ValueError("pipeline failed")

        assert mock_close.call_count == 1
        assert controller._is_shutdown

    def test_context_manager_raises_on_failed_final_flush(self) -> None:
        """Test that a failed final flush raises inside the `with` block, not via atexit."""
        controller = self.create_controller()
        with (
            patch.object(
                JsonlLogHandler, "close", side_effect=RuntimeError("flush failed")
            ),
            pytest.raises(RuntimeError, match="flush failed"),
            controller,
        ):
            pass


class TestConfigureLogging:
    """Tests for the configure_logging function."""

    def configure_logging_wrapper(
        self,
        bucket: str = DEFAULT_BUCKET,
        prefix: str = DEFAULT_PREFIX,
        database: str = DEFAULT_DATABASE,
        data_delivery_period: datetime = DELIVERY_PERIOD,
        attempt_no: int = 1,
        run_id: str = RUN_ID,
        batch_size: int = 500,
    ) -> LoggingController:
        """Wrapper for the configure_logging function to simplify test calls."""
        return configure_logging(
            bucket=bucket,
            prefix=prefix,
            database=database,
            data_delivery_period=data_delivery_period,
            attempt_no=attempt_no,
            run_id=run_id,
            batch_size=batch_size,
        )

    def test_configure_returns_success(self, s3: boto3.client) -> None:
        """Test that configure_logging returns a LoggingController and sets up handlers correctly."""
        create_bucket(s3)
        controller = self.configure_logging_wrapper()
        package_logger = logging.getLogger(PACKAGE_LOGGER_NAME)
        assert package_logger.level == logging.INFO
        assert package_logger.propagate is False
        assert [handler.get_name() for handler in package_logger.handlers] == [
            _CONSOLE_HANDLER_NAME,
            _JSONL_HANDLER_NAME,
        ]
        assert isinstance(controller, LoggingController)

    @pytest.mark.parametrize("batch_size", [0, -1, True])
    def test_configure_invalid_batch_size(self, batch_size: int) -> None:
        """Test that configure_logging raises a ValueError for invalid batch sizes."""
        with pytest.raises(ValueError, match="Batch size must be an integer >= 1"):
            self.configure_logging_wrapper(batch_size=batch_size)

    def test_duplicate_configuration_raises(self, s3: boto3.client) -> None:
        """Test that configuring logging twice raises a RuntimeError."""
        create_bucket(s3)
        self.configure_logging_wrapper()
        with pytest.raises(RuntimeError, match="Logger has already been configured"):
            self.configure_logging_wrapper()

    def test_fails_when_bucket_missing(self, s3: boto3.client) -> None:
        """Test that configure_logging raises immediately if S3 is unreachable, attaching no handlers."""
        with pytest.raises(RuntimeError, match="Failed to prepare JSONL log location"):
            self.configure_logging_wrapper(bucket="does-not-exist")

        assert logging.getLogger(PACKAGE_LOGGER_NAME).handlers == []


class TestModuleLogger:
    def test_rejects_extra_model_fields(self) -> None:
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            ModuleLogger.model_validate(
                {
                    "logger": logging.getLogger("opg_pipeline_builder.test"),
                    "extra_field": "not allowed",
                }
            )

    def test_error(self) -> None:
        """Test that the error method logs an error message correctly."""
        module_logger = ModuleLogger(
            logger=logging.getLogger("opg_pipeline_builder.test")
        )

        with patch.object(logging.Logger, "error") as mock_error:
            module_logger.error("Test error message")
            module_logger.error(
                "Another test error message",
                table="table",
                field="field",
                stage="Start",
            )
        assert mock_error.call_count == 2
        assert mock_error.call_args_list[0][0] == ("Test error message",)
        assert mock_error.call_args_list[0][1]["extra"] == {
            "custom_fields": CustomLogFields(
                table="N/A",
                field="N/A",
                process_stage="Processing",
            )
        }
        assert mock_error.call_args_list[1][0] == ("Another test error message",)
        assert mock_error.call_args_list[1][1]["extra"] == {
            "custom_fields": CustomLogFields(
                table="table",
                field="field",
                process_stage="Start",
            )
        }

    def test_info(self) -> None:
        """Test that the info method logs an info message correctly."""
        module_logger = ModuleLogger(
            logger=logging.getLogger("opg_pipeline_builder.test")
        )

        with patch.object(logging.Logger, "info") as mock_info:
            module_logger.info("Test info message")
            module_logger.info(
                "Another test info message",
                table="table",
                field="field",
                stage="Start",
            )
        assert mock_info.call_count == 2
        assert mock_info.call_args_list[0][0] == ("Test info message",)
        assert mock_info.call_args_list[0][1]["extra"] == {
            "custom_fields": CustomLogFields(
                table="N/A",
                field="N/A",
                process_stage="Processing",
            )
        }
        assert mock_info.call_args_list[1][0] == ("Another test info message",)
        assert mock_info.call_args_list[1][1]["extra"] == {
            "custom_fields": CustomLogFields(
                table="table",
                field="field",
                process_stage="Start",
            )
        }

    def test_lazy_percent_formatting_is_applied(self, s3: boto3.client) -> None:
        """Test that %s-style args passed through are lazily interpolated into the final message."""
        create_bucket(s3)
        _ = configure_logging(
            bucket=DEFAULT_BUCKET,
            prefix=DEFAULT_PREFIX,
            database=DEFAULT_DATABASE,
            data_delivery_period=DELIVERY_PERIOD,
            attempt_no=1,
            run_id=RUN_ID,
            batch_size=3,
        )

        module_logger = ModuleLogger(
            logger=logging.getLogger(f"{PACKAGE_LOGGER_NAME}.test")
        )
        module_logger.info("Processed %s rows in %s seconds", 42, 1.5, table="table_a")
        module_logger.info("Processed %s rows in %s seconds", 42, 2.5, table="table_b")

        jsonl_handler = next(
            handler
            for handler in logging.getLogger(PACKAGE_LOGGER_NAME).handlers
            if isinstance(handler, JsonlLogHandler)
        )

        key = jsonl_handler._object_key(str(os.getpid()), 0)

        logs = read_jsonl_logs(s3, bucket=DEFAULT_BUCKET, prefix=DEFAULT_PREFIX)
        log = logs[key]

        assert "Processed 42 rows in 1.5 seconds" in log[1]["message"]
        assert "Processed 42 rows in 2.5 seconds" in log[2]["message"]


class TestLoggingEndToEnd:
    """End-to-end tests exercising configure_logging, ModuleLogger and LoggingController together.

    Only the S3 backend is mocked (via the `s3` fixture); everything else runs exactly as it
    would in production, and assertions are made against the actual JSONL objects written to S3.

    Scenarios covered:
    - Simulate full pipeline runs with:
        - Logging across multiple module loggers
            - Including modules logging in a mixed order
        - Logging across different log levels
        - Multiple flushes:
            - Triggered by batch size
            - Triggered by manual flush
        - Shutdowns triggered:
            - Manually
            - Via context manager
            - Natural end of process (the atexit-triggered fallback).
        - Parallel processing
        - Flush failures then success
        - Flush failures then termination
        - Writing multiple parts to S3
    - All unique error scenarios included
    """

    def configure(
        self,
        *,
        batch_size: int = 3,
        database: str = DEFAULT_DATABASE,
        attempt_no: int = 1,
        run_id: str = RUN_ID,
    ) -> tuple[LoggingController, JsonlLogHandler]:
        """Configure logging and return the controller plus the underlying JSONL handler."""
        controller = configure_logging(
            bucket=DEFAULT_BUCKET,
            prefix=DEFAULT_PREFIX,
            database=database,
            data_delivery_period=DELIVERY_PERIOD,
            attempt_no=attempt_no,
            run_id=run_id,
            batch_size=batch_size,
        )
        jsonl_handler = next(
            handler
            for handler in controller._logger.handlers
            if isinstance(handler, JsonlLogHandler)
        )
        return controller, jsonl_handler

    def expected_object_key(
        self,
        jsonl_handler: JsonlLogHandler,
        part_number: int,
        *,
        attempt_no: int = 1,
        run_id: str = RUN_ID,
    ) -> str:
        """Build the expected S3 key for a given part number."""
        return (
            f"{DEFAULT_PREFIX}/data_delivery_period=20240102/attempt_no={attempt_no}/"
            f"run_id={run_id}/{os.getpid()}_{jsonl_handler._writer_id}_{part_number}.jsonl"
        )

    def base_row(
        self,
        *,
        logger_name: str = PACKAGE_LOGGER_NAME,
        module: str = "opg_pipeline_builder.logging.log",
        function: str,
        line_number: int,
        log_level: str,
        process_stage: str = "Processing",
        table: str = "N/A",
        field: str = "N/A",
        message: str,
        database: str = DEFAULT_DATABASE,
        attempt_no: int = 1,
        run_id: str = RUN_ID,
        log_timestamp: str = "2024-01-02T00:00:00Z",
    ) -> dict[str, Any]:
        """Build the expected structured row for a real logged record."""
        return {
            "database": database,
            "run_id": run_id,
            "data_delivery_period": "2024-01-02T00:00:00Z",
            "attempt_no": attempt_no,
            "logger_name": logger_name,
            "module": module,
            "function": function,
            "line_number": line_number,
            "log_level": log_level,
            "log_timestamp": log_timestamp,
            "process_stage": process_stage,
            "table": table,
            "field": field,
            "message": message,
        }

    def test_pipeline_multiple_flushes_and_deliberate_shutdown(
        self, s3: boto3.client
    ) -> None:
        """Test a full pipeline run: an automatic batch flush, a manual flush, then a deliberate shutdown."""
        create_bucket(s3, DEFAULT_BUCKET)
        function_name = inspect.currentframe().f_code.co_name  # type: ignore[union-attr]

        with freeze_time("2024-01-02T00:00:00Z"):
            controller, jsonl_handler = self.configure(batch_size=4)
            extract_logger = ModuleLogger(
                logger=logging.getLogger(f"{PACKAGE_LOGGER_NAME}.extract")
            )
            load_logger = ModuleLogger(
                logger=logging.getLogger(f"{PACKAGE_LOGGER_NAME}.load")
            )

            # These three records bring the buffer (which already holds the config log) to the
            # batch_size of 3, triggering an automatic flush.
            line_1 = inspect.currentframe().f_lineno + 1  # type: ignore[union-attr]
            extract_logger.info("Extract started", table="table_a", stage="Start")
            line_2 = inspect.currentframe().f_lineno + 1  # type: ignore[union-attr]
            extract_logger.info("Extract row", table="table_a", field="col_a")
            line_3 = inspect.currentframe().f_lineno + 1  # type: ignore[union-attr]
            load_logger.info("Load started", table="table_a", stage="Start")

            # These two records stay below the threshold, so nothing is flushed automatically.
            line_4 = inspect.currentframe().f_lineno + 1  # type: ignore[union-attr]
            load_logger.info("Loading field", table="table_a", field="col_a")
            line_5 = inspect.currentframe().f_lineno + 1  # type: ignore[union-attr]
            load_logger.error("Load validation failed", table="table_a", field="col_a")

            # A deliberate manual flush uploads the buffered records without closing anything.
            controller.flush()

            line_6 = inspect.currentframe().f_lineno + 1  # type: ignore[union-attr]
            load_logger.info("Load finished", table="table_a", field="N/A", stage="End")

            controller.shutdown()

        package_logger = logging.getLogger(PACKAGE_LOGGER_NAME)
        assert package_logger.handlers == []
        assert controller._is_shutdown is True
        assert jsonl_handler._buffer == []

        logs = read_jsonl_logs(s3, bucket=DEFAULT_BUCKET, prefix=DEFAULT_PREFIX)
        part_0_key = self.expected_object_key(jsonl_handler, 0)
        part_1_key = self.expected_object_key(jsonl_handler, 1)
        part_2_key = self.expected_object_key(jsonl_handler, 2)
        assert set(logs) == {part_0_key, part_1_key, part_2_key}

        assert logs[part_0_key] == [
            self.base_row(
                function="configure_logging",
                line_number=150,
                log_level="INFO",
                process_stage="Start",
                message="Logging configured successfully.",
                attempt_no=1,
            ),
            self.base_row(
                logger_name=f"{PACKAGE_LOGGER_NAME}.extract",
                module="test_log",
                function=function_name,
                line_number=line_1,
                log_level="INFO",
                process_stage="Start",
                table="table_a",
                field="N/A",
                message="Extract started",
            ),
            self.base_row(
                logger_name=f"{PACKAGE_LOGGER_NAME}.extract",
                module="test_log",
                function=function_name,
                line_number=line_2,
                log_level="INFO",
                process_stage="Processing",
                table="table_a",
                field="col_a",
                message="Extract row",
            ),
            self.base_row(
                logger_name=f"{PACKAGE_LOGGER_NAME}.load",
                module="test_log",
                function=function_name,
                line_number=line_3,
                log_level="INFO",
                process_stage="Start",
                table="table_a",
                field="N/A",
                message="Load started",
            ),
        ]
        assert logs[part_1_key] == [
            self.base_row(
                logger_name=f"{PACKAGE_LOGGER_NAME}.load",
                module="test_log",
                function=function_name,
                line_number=line_4,
                log_level="INFO",
                process_stage="Processing",
                table="table_a",
                field="col_a",
                message="Loading field",
            ),
            self.base_row(
                logger_name=f"{PACKAGE_LOGGER_NAME}.load",
                module="test_log",
                function=function_name,
                line_number=line_5,
                log_level="ERROR",
                process_stage="Processing",
                table="table_a",
                field="col_a",
                message="Load validation failed",
            ),
        ]
        assert logs[part_2_key] == [
            self.base_row(
                logger_name=f"{PACKAGE_LOGGER_NAME}.load",
                module="test_log",
                function=function_name,
                line_number=line_6,
                log_level="INFO",
                process_stage="End",
                table="table_a",
                field="N/A",
                message="Load finished",
            ),
        ]

    @pytest.mark.parametrize(
        ("trigger", "expect_shutdown_recorded", "expect_handler_removed"),
        [
            ("context_manager", True, True),
            ("logging_shutdown", False, False),
        ],
    )
    def test_shutdown_trigger_flushes_pending_logs(
        self,
        s3: boto3.client,
        trigger: str,
        expect_shutdown_recorded: bool,
        expect_handler_removed: bool,
    ) -> None:
        """Test that both ways of triggering shutdown flush pending logs identically, but leave
        the controller/handler in different states afterward.

        `with controller:` (the recommended usage) calls LoggingController.shutdown() on exit,
        which removes the handler and records `_is_shutdown`. Falling back to the interpreter's
        own atexit-registered `logging.shutdown()` (what runs if a task forgets to use the
        context manager) flushes and closes the handler too, but never calls `removeHandler()`
        and never touches the controller, so both stay unaffected.
        """
        create_bucket(s3, DEFAULT_BUCKET)
        function_name = inspect.currentframe().f_code.co_name  # type: ignore[union-attr]

        with freeze_time("2024-01-02T00:00:00Z"):
            controller, jsonl_handler = self.configure(batch_size=10)
            pipeline_logger = ModuleLogger(
                logger=logging.getLogger(f"{PACKAGE_LOGGER_NAME}.pipeline")
            )

            if trigger == "context_manager":
                with controller as entered:
                    assert entered is controller
                    line_1 = inspect.currentframe().f_lineno + 1  # type: ignore[union-attr]
                    pipeline_logger.info(
                        "Pipeline started", table="table_a", stage="Start"
                    )
            else:
                line_1 = inspect.currentframe().f_lineno + 1  # type: ignore[union-attr]
                pipeline_logger.info("Pipeline started", table="table_a", stage="Start")
                logging.shutdown()

        package_logger = logging.getLogger(PACKAGE_LOGGER_NAME)
        assert jsonl_handler._buffer == []
        assert controller._is_shutdown is expect_shutdown_recorded
        assert (jsonl_handler not in package_logger.handlers) is expect_handler_removed

        logs = read_jsonl_logs(s3, bucket=DEFAULT_BUCKET, prefix=DEFAULT_PREFIX)
        key = self.expected_object_key(jsonl_handler, 0)
        assert list(logs) == [key]
        assert logs[key] == [
            self.base_row(
                function="configure_logging",
                line_number=150,
                log_level="INFO",
                process_stage="Start",
                message="Logging configured successfully.",
                attempt_no=1,
            ),
            self.base_row(
                logger_name=f"{PACKAGE_LOGGER_NAME}.pipeline",
                module="test_log",
                function=function_name,
                line_number=line_1,
                log_level="INFO",
                process_stage="Start",
                table="table_a",
                field="N/A",
                message="Pipeline started",
            ),
        ]

    @pytest.mark.parametrize("trigger", ["context_manager", "logging_shutdown"])
    def test_shutdown_trigger_raises_on_failed_final_flush(
        self, s3: boto3.client, trigger: str
    ) -> None:
        """Test that a failed final flush is never silently discarded, however shutdown is
        triggered.

        Via the context manager this fails the task itself (the recommended, fatal path); via
        the atexit-triggered logging.shutdown() fallback it's only reported to stderr, not fatal
        to the process, since shutdown() runs from atexit and Python only prints "Exception
        ignored in atexit callback" for errors it doesn't recognise. Either way the RuntimeError
        propagates out of the trigger itself, and nothing partial reaches S3.
        """
        create_bucket(s3, DEFAULT_BUCKET)

        with freeze_time("2024-01-02T00:00:00Z"):
            controller, jsonl_handler = self.configure(batch_size=10)
            pipeline_logger = ModuleLogger(
                logger=logging.getLogger(f"{PACKAGE_LOGGER_NAME}.pipeline")
            )
            pipeline_logger.info("Pipeline started", table="table_a", stage="Start")

            with (
                patch.object(JsonlLogHandler, "_put_object", side_effect=OSError()),
                pytest.raises(RuntimeError, match="Failed to write"),
            ):
                if trigger == "context_manager":
                    with controller:
                        pass
                else:
                    logging.shutdown()

        assert len(jsonl_handler._buffer) > 0
        logs = read_jsonl_logs(s3, bucket=DEFAULT_BUCKET, prefix=DEFAULT_PREFIX)
        assert logs == {}

    def test_flush_delegates_retry_and_threshold_behaviour_to_handler(
        self, s3: boto3.client
    ) -> None:
        """Test that controller.flush() correctly delegates to the handler's retry/threshold logic.

        The retry and threshold mechanics themselves are already exhaustively unit tested via
        JsonlLogHandler._flush_locked directly (test_flush_locked_fail_then_succeed and
        test_flush_locked_fail_then_hit_threshold); this just proves the E2E process wired
        through controller.flush() handles the same failures and recovery correctly, without
        re-asserting every detail already covered there.
        """
        create_bucket(s3, DEFAULT_BUCKET)
        controller, jsonl_handler = self.configure(batch_size=100)
        pipeline_logger = ModuleLogger(
            logger=logging.getLogger(f"{PACKAGE_LOGGER_NAME}.pipeline")
        )
        pipeline_logger.info("Started processing", table="table_a", stage="Start")

        with patch.object(JsonlLogHandler, "_put_object", side_effect=OSError()):
            # A transient failure is recoverable - controller.flush() just records it.
            controller.flush()
            assert jsonl_handler._write_failures == 1

            # Repeated failures eventually exceed the retry threshold and raise through flush().
            with pytest.raises(
                RuntimeError, match="Have attempted to write JSONL logs 4 times"
            ):
                for _ in range(3):
                    controller.flush()

        # Recovery: once S3 is reachable again, the next flush succeeds and writes everything
        # buffered, including the synthetic failure notices recorded along the way.
        controller.flush()

        logs = read_jsonl_logs(s3, bucket=DEFAULT_BUCKET, prefix=DEFAULT_PREFIX)
        key = self.expected_object_key(jsonl_handler, 0)
        assert list(logs) == [key]
        assert [record["message"] for record in logs[key]] == [
            "Logging configured successfully.",
            "Started processing",
            "Failed to write JSONL log batch (failure_count=1).",
            "Failed to write JSONL log batch (failure_count=2).",
            "Failed to write JSONL log batch (failure_count=3).",
        ]
        assert jsonl_handler._write_failures == 0
        assert jsonl_handler._buffer == []

        controller.shutdown()

    def test_multithreaded_logging(self, s3: boto3.client) -> None:
        """Test that multiple threads writing through the same handler produce exactly the
        expected records, with none lost or duplicated, and sequential, gap-free part numbers.
        """
        create_bucket(s3, DEFAULT_BUCKET)
        worker_count = 3
        messages_per_worker = 5

        with freeze_time("2024-01-02T00:00:00Z"):
            controller, jsonl_handler = self.configure(batch_size=4)

            def worker(worker_number: int) -> None:
                worker_logger = ModuleLogger(
                    logger=logging.getLogger(
                        f"{PACKAGE_LOGGER_NAME}.worker_{worker_number}"
                    )
                )
                for message_number in range(messages_per_worker):
                    worker_logger.info(
                        f"worker-{worker_number}-{message_number}",
                        table="table_a",
                        stage="Processing",
                    )

            threads = [
                Thread(target=worker, args=(number,)) for number in range(worker_count)
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=30)

            controller.shutdown()

        assert all(not thread.is_alive() for thread in threads)

        logs = read_jsonl_logs(s3, bucket=DEFAULT_BUCKET, prefix=DEFAULT_PREFIX)
        all_records = [record for records in logs.values() for record in records]
        worker_records = [
            record
            for record in all_records
            if record["message"] != "Logging configured successfully."
        ]

        # Exactly one config record, and every worker message present exactly once - proves the
        # shared lock in emit()/_flush_locked() prevents both lost and duplicated records.
        assert len(all_records) - len(worker_records) == 1
        expected_pairs = {
            (
                f"opg_pipeline_builder.worker_{worker_number}",
                f"worker-{worker_number}-{message_number}",
            )
            for worker_number in range(worker_count)
            for message_number in range(messages_per_worker)
        }
        actual_pairs = {
            (record["logger_name"], record["message"]) for record in worker_records
        }
        assert actual_pairs == expected_pairs

        # Spot-check one record's full structure; per-field conversion correctness is already
        # covered by test_record_to_row_success, so this just confirms it holds under threads too.
        assert (
            self.base_row(
                logger_name="opg_pipeline_builder.worker_0",
                module="test_log",
                function="worker",
                line_number=next(
                    record["line_number"]
                    for record in worker_records
                    if record["message"] == "worker-0-0"
                ),
                log_level="INFO",
                process_stage="Processing",
                table="table_a",
                message="worker-0-0",
            )
            in worker_records
        )

        # Concurrent flushes still assign sequential, gap-free part numbers under the lock.
        expected_prefix = self.expected_object_key(jsonl_handler, 0).rsplit("_", 1)[0]
        assert all(key.startswith(expected_prefix) for key in logs)
        part_numbers = sorted(
            int(key.rsplit("_", 1)[1].removesuffix(".jsonl")) for key in logs
        )
        assert part_numbers == list(range(len(part_numbers)))

    def test_pipeline_with_no_application_logs_still_flushes_config_record(
        self, s3: boto3.client
    ) -> None:
        """Test that shutting down immediately after configure_logging still flushes the config record."""
        create_bucket(s3, DEFAULT_BUCKET)

        with freeze_time("2024-01-02T00:00:00Z"):
            controller, jsonl_handler = self.configure(batch_size=10)
            with controller:
                pass

        logs = read_jsonl_logs(s3, bucket=DEFAULT_BUCKET, prefix=DEFAULT_PREFIX)
        key = self.expected_object_key(jsonl_handler, 0)
        assert list(logs) == [key]
        assert logs[key] == [
            self.base_row(
                function="configure_logging",
                line_number=150,
                log_level="INFO",
                process_stage="Start",
                message="Logging configured successfully.",
                attempt_no=1,
            )
        ]

    def test_two_pipeline_runs_with_different_attempt_numbers_do_not_collide(
        self, s3: boto3.client
    ) -> None:
        """Test that two runs for the same database/data_delivery_period but different attempt_no
        write to distinct S3 partitions without colliding or mixing records.
        """
        create_bucket(s3, DEFAULT_BUCKET)

        with freeze_time("2024-01-02T00:00:00Z"):
            first_controller, first_handler = self.configure(
                batch_size=10, attempt_no=1
            )
            with first_controller:
                ModuleLogger(
                    logger=logging.getLogger(f"{PACKAGE_LOGGER_NAME}.attempt")
                ).info("First attempt processing", table="table_a", stage="Start")

            second_controller, second_handler = self.configure(
                batch_size=10, attempt_no=2
            )
            with second_controller:
                ModuleLogger(
                    logger=logging.getLogger(f"{PACKAGE_LOGGER_NAME}.attempt")
                ).info("Second attempt processing", table="table_a", stage="Start")

        logs = read_jsonl_logs(s3, bucket=DEFAULT_BUCKET, prefix=DEFAULT_PREFIX)
        first_key = self.expected_object_key(first_handler, 0, attempt_no=1)
        second_key = self.expected_object_key(second_handler, 0, attempt_no=2)
        assert set(logs) == {first_key, second_key}

        assert [record["message"] for record in logs[first_key]] == [
            "Logging configured successfully.",
            "First attempt processing",
        ]
        assert all(record["attempt_no"] == 1 for record in logs[first_key])

        assert [record["message"] for record in logs[second_key]] == [
            "Logging configured successfully.",
            "Second attempt processing",
        ]
        assert all(record["attempt_no"] == 2 for record in logs[second_key])
