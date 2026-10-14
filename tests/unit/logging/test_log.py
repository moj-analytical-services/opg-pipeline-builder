# pyright: reportPrivateUsage=false

import contextvars
import inspect
import io
import json
import logging
import os
import sys
import time
import weakref
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from contextlib import nullcontext as does_not_raise
from datetime import UTC, datetime, timedelta, timezone
from threading import Barrier, Event, Thread, current_thread
from typing import Any
from unittest.mock import patch
from zoneinfo import ZoneInfo

import boto3
import pandas as pd
import pytest
from botocore.exceptions import BotoCoreError, ClientError
from freezegun import freeze_time
from pydantic import ValidationError

from opg_pipeline_builder.logging import log, logging_utils
from opg_pipeline_builder.logging import log_exceptions as exc

RUN_ID = "scheduled__2026-09-22T00:00:00+00:00"
ENCODED_RUN_ID = "scheduled__2026-09-22T00%3A00%3A00%2B00%3A00"
ENCODED_DELIVERY_PERIOD = "2024-01-02T00%3A00%3A00Z"
DELIVERY_PERIOD = datetime(2024, 1, 2, tzinfo=UTC)
DEFAULT_BUCKET = "log-bucket"
DEFAULT_LOG_STORE_PREFIX = "prefix/to/log"
DEFAULT_ATHENA_PREFIX = "prefix/to/log/curated/"
DEFAULT_ATHENA_DATABASE = "opg_test_logging"
DEFAULT_ATHENA_TABLE = "test_logging_curated"
DEFAULT_PIPELINE = "test_pipeline"


##################
# LOG TEST UTILS #
##################


def configuration_log_line_number(
    function: Any = log.JsonlLogHandler._write_configuration_log,
) -> int:
    """Return the source line of the internal record call inside the given handler method."""
    source, first_line = inspect.getsourcelines(function)
    return first_line + next(
        index for index, line in enumerate(source) if "self._internal_record(" in line
    )


def config_values(**overrides: Any) -> dict[str, Any]:
    """Return fresh configuration inputs shared by model, handler and integration tests."""
    return {
        "pipeline": DEFAULT_PIPELINE,
        "data_delivery_period": DELIVERY_PERIOD,
        "attempt_no": 1,
        "run_id": RUN_ID,
        "bucket": DEFAULT_BUCKET,
        "log_store_prefix": DEFAULT_LOG_STORE_PREFIX,
        "athena_prefix": DEFAULT_ATHENA_PREFIX,
        "athena_database_name": DEFAULT_ATHENA_DATABASE,
        "athena_table_name": DEFAULT_ATHENA_TABLE,
        "batch_size": 500,
        **overrides,
    }


def create_handler(apply_patches: bool = True, **overrides: Any) -> log.JsonlLogHandler:
    """Create a handler, optionally suppressing preflight and the configuration row."""
    config = log.LoggingConfig.model_validate(
        config_values(**{"batch_size": 2, **overrides})
    )
    with (
        patch.object(log.JsonlLogHandler, "_validate_log_location")
        if apply_patches
        else nullcontext(),
        patch.object(log.JsonlLogHandler, "_write_configuration_log")
        if apply_patches
        else nullcontext(),
    ):
        return log.JsonlLogHandler(logging_config=config)


def create_log_record(
    *,
    timestamp: datetime = datetime(2024, 1, 4, 10, 16, 6, tzinfo=UTC),
    line_number: int = 42,
    message: str = "test log",
    args: tuple[object, ...] = (),
    custom_fields: log.CustomLogFields | None = None,
) -> logging.LogRecord:
    """Create a representative logging record."""
    custom_fields = custom_fields or log.CustomLogFields(
        process="Logging",
        table="table_a",
        field="field_a",
        process_stage="Processing",
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


def mock_flush_locked(handler: log.JsonlLogHandler) -> None:
    """Mock the _flush_locked method of a JsonlLogHandler."""
    handler._buffer = []


def read_jsonl_logs(
    s3: boto3.client,
    *,
    bucket: str = DEFAULT_BUCKET,
    prefix: str = DEFAULT_LOG_STORE_PREFIX,
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


def simulate_atexit_shutdown(controller: log.LoggingController) -> None:
    """Run the interpreter's atexit `logging.shutdown()` against this controller's handlers only.

    A bare `logging.shutdown()` closes every live handler in the process, including the
    session-scoped handler from conftest, which would then reject later tests' records.
    """
    logging.shutdown(
        handlerList=[weakref.ref(handler) for handler in controller._handlers]
    )


@pytest.fixture(autouse=True, scope="function")
def reset_root_logger() -> Generator[None]:
    """Isolate test JSONL handlers while preserving host-owned root handlers."""
    root_logger = logging.getLogger()
    original_handlers = list(root_logger.handlers)
    original_level = root_logger.level
    session_handlers = [
        handler
        for handler in original_handlers
        if handler.get_name() in {log._CONSOLE_HANDLER_NAME, log._JSONL_HANDLER_NAME}
    ]
    for handler in session_handlers:
        root_logger.removeHandler(handler)

    yield

    for handler in list(root_logger.handlers):
        if handler not in original_handlers:
            root_logger.removeHandler(handler)
            logging.Handler.close(handler)
    for handler in session_handlers:
        if handler not in root_logger.handlers:
            root_logger.addHandler(handler)
    root_logger.setLevel(original_level)


@pytest.fixture(autouse=True, scope="function")
def retire_test_handlers() -> Generator[None]:
    """Mark JSONL handlers created by a test as closed so the real atexit shutdown skips them.

    Without this, handlers a test built but never closed (or whose close was patched) would be
    flushed and uploaded by `logging.shutdown()` at interpreter exit.
    """
    existing = {id(ref) for ref in logging._handlerList}  # type: ignore[attr-defined]

    yield

    for ref in list(logging._handlerList):  # type: ignore[attr-defined]
        handler = ref()
        if (
            id(ref) not in existing
            and isinstance(handler, log.JsonlLogHandler)
            and not handler._closed
        ):
            logging.Handler.close(handler)


@pytest.fixture
def logging_configured() -> Generator[None]:
    """Satisfy ModuleLogger's configured check without building a real JSONL handler."""
    stand_in = logging.NullHandler()
    stand_in.set_name(log._JSONL_HANDLER_NAME)
    root_logger = logging.getLogger()
    root_logger.addHandler(stand_in)
    try:
        yield
    finally:
        root_logger.removeHandler(stand_in)


@pytest.fixture(autouse=True, scope="function")
def mock_athena_clients() -> Generator[None]:
    """Keep handler lifecycle tests independent of external Athena and Glue calls."""
    with (
        patch("opg_pipeline_builder.logging.log.wr.catalog.create_database"),
        patch(
            "opg_pipeline_builder.logging.log.wr.s3.read_json",
            side_effect=lambda **_: pd.DataFrame({"log_timestamp": []}),
        ),
        patch("opg_pipeline_builder.logging.log.wr.s3.to_parquet"),
    ):
        yield


##############
# UNIT TESTS #
##############


class TestCustomLogFields:
    """Tests for the CustomLogFields model."""

    def test_accepts_valid_fields(self) -> None:
        """Test that valid custom log fields are accepted."""
        fields = log.CustomLogFields(
            process="Logging", table="table_a", field="field_a", process_stage="Start"
        )
        assert fields.model_dump() == {
            "process": "Logging",
            "table": "table_a",
            "field": "field_a",
            "process_stage": "Start",
        }

    @pytest.mark.parametrize(
        "missing", [("process"), ("table"), ("field"), ("process_stage")]
    )
    def test_rejects_missing_fields(self, missing: str) -> None:
        """Test that missing required custom log fields are rejected."""
        values = {
            "process": "Logging",
            "table": "table_a",
            "field": "field_a",
            "process_stage": "Processing",
        }
        del values[missing]
        with pytest.raises(ValidationError):
            log.CustomLogFields.model_validate(values)

    def test_rejects_extra_fields(self) -> None:
        """Test that extra custom log fields are rejected."""
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            log.CustomLogFields.model_validate(
                {
                    "process": "Logging",
                    "table": "table_a",
                    "field": "field_a",
                    "process_stage": "Processing",
                    "message": "reserved",
                }
            )

    def test_rejects_invalid_process_stage(self) -> None:
        """Test that an invalid process stage is rejected."""
        with pytest.raises(ValidationError):
            log.CustomLogFields(
                process="Logging",
                table="table_a",
                field="field_a",
                process_stage="Invalid",
            )

    @pytest.mark.parametrize("invalid_process", ["", "   ", None])
    def test_rejects_empty_process(self, invalid_process: Any) -> None:
        """Test that empty, whitespace-only, and None 'process' values are rejected.

        InvalidProcessError is not a ValueError, so Pydantic propagates it unwrapped.
        """
        with pytest.raises(
            exc.InvalidProcessError, match="field 'process' must be non-empty"
        ):
            log.CustomLogFields(
                process=invalid_process,
                table="table_a",
                field="field_a",
                process_stage="Start",
            )

    @pytest.mark.parametrize("padded_process", [" Load", "Load ", "  Load\t"])
    def test_strips_process_whitespace(self, padded_process: str) -> None:
        """Test that surrounding whitespace is stripped from 'process', so Athena group-bys are not split."""
        fields = log.CustomLogFields(
            process=padded_process,
            table="table_a",
            field="field_a",
            process_stage="Start",
        )
        assert fields.process == "Load"


class TestStructuredLogRecord:
    """Tests for the StructuredLogRecord model."""

    @property
    def valid_values(self) -> dict[str, Any]:
        """Create a dictionary of valid structured log record values."""
        return {
            "pipeline": DEFAULT_PIPELINE,
            "run_id": RUN_ID,
            "data_delivery_period": DELIVERY_PERIOD,
            "attempt_no": 1,
            "logger_name": "opg_pipeline_builder.module",
            "module": "module",
            "function": "function",
            "line_number": 42,
            "log_level": "INFO",
            "log_timestamp": datetime(2024, 1, 2, 12, tzinfo=UTC),
            "process": "Logging",
            "table": "table_a",
            "field": "field_a",
            "process_stage": "Processing",
            "message": "event",
            "exception": "",
        }

    def test_accepts_complete_record_and_json_dump(self) -> None:
        """Retain the complete schema and JSON serialization contract."""
        record = log.StructuredLogRecord.model_validate(self.valid_values)
        output = record.model_dump()
        assert output == self.valid_values
        assert record.model_dump(mode="json") == {
            **self.valid_values,
            "data_delivery_period": "2024-01-02T00:00:00Z",
            "log_timestamp": "2024-01-02T12:00:00Z",
        }
        with pytest.raises(ValidationError, match="frozen"):
            record.message = "changed"  # type: ignore[misc]

    def test_rejects_extra_field(self) -> None:
        """Test that extra fields in a structured log record are rejected."""
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            log.StructuredLogRecord.model_validate(
                {**self.valid_values, "extra_field": "not allowed"}
            )

    @pytest.mark.parametrize(
        ("log_level", "expectation"),
        [
            ("DEBUG", does_not_raise()),
            ("INFO", does_not_raise()),
            ("WARNING", does_not_raise()),
            ("ERROR", does_not_raise()),
            ("CRITICAL", does_not_raise()),
            ("TRACE", pytest.raises(ValidationError)),
            ("info", pytest.raises(ValidationError)),
            ("", pytest.raises(ValidationError)),
        ],
    )
    def test_validate_log_level(self, log_level: str, expectation: Any) -> None:
        """Test that only standard upper-case logging level names are accepted."""
        with expectation:
            log.StructuredLogRecord.model_validate(
                {**self.valid_values, "log_level": log_level}
            )

    @pytest.mark.parametrize("field", ["data_delivery_period", "log_timestamp"])
    def test_generated_timestamps_serialize_as_utc(self, field: str) -> None:
        """The row schema serializes the UTC timestamps supplied by its producers."""
        record = log.StructuredLogRecord.model_validate(self.valid_values)
        assert record.model_dump(mode="json")[field].endswith("Z")


class TestLoggingConfig:
    """Validate run-level inputs once, separately from individual log rows."""

    @pytest.mark.parametrize("field", ["pipeline", "run_id"])
    @pytest.mark.parametrize("value", ["", " ", "\t", None])
    def test_rejects_empty_run_context(self, field: str, value: Any) -> None:
        """Reject empty run identifiers and report the offending field."""
        with pytest.raises(ValidationError) as raised:
            log.LoggingConfig.model_validate(config_values(**{field: value}))
        assert raised.value.errors()[0]["loc"] == (field,)

    @pytest.mark.parametrize("field", ["attempt_no", "batch_size"])
    @pytest.mark.parametrize("value", [0, -1, 1, 99])
    def test_positive_counts(self, field: str, value: int) -> None:
        """Both run attempts and batch sizes must be positive."""
        expectation = (
            pytest.raises(ValidationError, match="must be >=1")
            if value < 1
            else nullcontext()
        )
        with expectation:
            config = log.LoggingConfig.model_validate(config_values(**{field: value}))
            assert getattr(config, field) == value

    @pytest.mark.parametrize("value", [True, False, 2.5, 10.0, "10", None])
    def test_batch_size_is_strict(self, value: Any) -> None:
        """Do not coerce booleans, floats or strings into batch sizes."""
        with pytest.raises(ValidationError) as raised:
            log.LoggingConfig.model_validate(config_values(batch_size=value))
        assert raised.value.errors()[0]["loc"] == ("batch_size",)

    @pytest.mark.parametrize("field", ["athena_database_name", "athena_table_name"])
    @pytest.mark.parametrize(
        "value", ["", "has space", "Upper", "1name", "_name", "a-b", "é", "a" * 64]
    )
    def test_rejects_invalid_athena_identifiers(self, field: str, value: str) -> None:
        """Database and table names share the project's identifier constraints."""
        with pytest.raises(ValidationError) as raised:
            log.LoggingConfig.model_validate(config_values(**{field: value}))
        assert raised.value.errors()[0]["loc"] == (field,)

    def test_complete_configuration_is_frozen_and_forbids_extra_fields(self) -> None:
        """Validated inputs cannot be mutated and unexpected keys are rejected."""
        values = config_values()
        config = log.LoggingConfig.model_validate(values)
        assert config.model_dump() == values
        with pytest.raises(ValidationError, match="frozen"):
            config.batch_size = 1  # type: ignore[misc]
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            log.LoggingConfig.model_validate({**values, "extra_field": "unexpected"})


class TestJsonlLogHandler:
    def test_init(self, s3: boto3.client) -> None:
        """Test that the handler initializes correctly with default values."""
        with (
            patch.object(
                log.JsonlLogHandler, "_validate_log_location"
            ) as mock_location,
            patch.object(log.JsonlLogHandler, "_write_configuration_log") as mock_write,
        ):
            handler = create_handler(apply_patches=False)
            assert handler._config.model_dump() == config_values(batch_size=2)
            assert handler._batch_size == 2
            assert handler._base_batch_size == 2
            assert handler._part_number == 0
            assert handler._writer_id
            assert handler._buffer == []
            assert handler._write_failures == 0
            assert mock_location.call_count == 1
            assert mock_write.call_count == 1
            logging.Handler.close(handler)

    def test_validate_log_location_valid(self, s3: boto3.client) -> None:
        """Test that the marker file is generated, and only one ever exists."""

        create_handler(apply_patches=False)
        create_handler(
            pipeline="other-pipeline",
            attempt_no=2,
            run_id="other-run",
            apply_patches=False,
        )
        create_handler(
            pipeline="another-pipeline",
            attempt_no=3,
            run_id="another-run",
            apply_patches=False,
        )

        keys = [
            item["Key"]
            for item in s3.list_objects_v2(
                Bucket=DEFAULT_BUCKET, Prefix=DEFAULT_LOG_STORE_PREFIX
            ).get("Contents", [])
        ]
        assert keys == [f"{DEFAULT_LOG_STORE_PREFIX}/logging-marker/marker.txt"]

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
        with (
            patch.object(s3, "put_object", side_effect=exception),
            patch(
                "opg_pipeline_builder.logging.log.boto3.client",
                return_value=s3,
            ),
            pytest.raises(RuntimeError, match="Failed to write marker object"),
        ):
            create_handler(apply_patches=False)

    def test_init_configures_s3_retries(self) -> None:
        """Test that the S3 client is created with explicit standard-mode retries."""
        with patch("opg_pipeline_builder.logging.log.boto3.client") as mock_client:
            handler = create_handler()

        mock_client.assert_called_once_with("s3", config=log._S3_RETRY_CONFIG)
        # botocore normalises max_attempts=5 retries to 6 total attempts (initial call + 5 retries).
        assert log._S3_RETRY_CONFIG.retries == {
            "mode": "standard",
            "total_max_attempts": 6,
        }
        logging.Handler.close(handler)

    @pytest.mark.parametrize(
        ("period", "expected_period", "expected_partition"),
        [
            (DELIVERY_PERIOD, DELIVERY_PERIOD, ENCODED_DELIVERY_PERIOD),
            (
                datetime(2024, 6, 1, tzinfo=ZoneInfo("Europe/London")),
                datetime(2024, 5, 31, 23, tzinfo=UTC),
                "2024-05-31T23%3A00%3A00Z",
            ),
            (
                datetime(2024, 1, 1, 20, tzinfo=timezone(timedelta(hours=-5))),
                datetime(2024, 1, 2, 1, tzinfo=UTC),
                "2024-01-02T01%3A00%3A00Z",
            ),
        ],
    )
    def test_init_normalises_data_delivery_period_to_utc(
        self, period: datetime, expected_period: datetime, expected_partition: str
    ) -> None:
        """Test that the path partition and the record value are the same ISO UTC timestamp."""
        handler = create_handler(data_delivery_period=period)

        assert handler._config.data_delivery_period == expected_period
        assert handler._config.data_delivery_period.tzinfo is UTC
        assert handler._create_key_directory("p").startswith(
            f"p/data_delivery_period={expected_partition}/"
        )
        record = create_log_record()
        row = handler._record_to_row(record, vars(record)["custom_fields"])
        assert row["data_delivery_period"] == expected_period.strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        logging.Handler.close(handler)

    def test_init_rejects_naive_data_delivery_period(self) -> None:
        """Test that a naive data delivery period is rejected before any S3 call."""
        with (
            patch("opg_pipeline_builder.logging.log.boto3.client") as mock_client,
            pytest.raises(
                ValidationError, match="'data_delivery_period' must be timezone-aware"
            ),
        ):
            create_handler(
                apply_patches=False,
                data_delivery_period=datetime(2024, 1, 2),  # noqa: DTZ001
            )
        mock_client.assert_not_called()

    @pytest.mark.parametrize(
        ("logger_name", "custom_fields", "expectation", "written"),
        [
            # No custom fields: skipped (console only), whatever the namespace.
            ("opg_pipeline_builder.models.x", "missing", does_not_raise(), False),
            ("consumer_repo.etl", "missing", does_not_raise(), False),
            ("urllib3.connectionpool", "missing", does_not_raise(), False),
            ("root", "missing", does_not_raise(), False),
            ("consumer_repo.etl", None, does_not_raise(), False),
            # Valid custom fields: written, whatever the namespace.
            ("opg_pipeline_builder.models.x", "valid", does_not_raise(), True),
            ("third_party.module", "valid", does_not_raise(), True),
            # Present but not a CustomLogFields instance: rejected.
            ("consumer_repo.etl", {"process": "x"}, pytest.raises(TypeError), False),
            ("consumer_repo.etl", 1, pytest.raises(TypeError), False),
            ("consumer_repo.etl", {}, does_not_raise(), False),
            ("consumer_repo.etl", False, does_not_raise(), False),
        ],
    )
    def test_emit_requires_custom_fields(
        self,
        logger_name: str,
        custom_fields: Any,
        expectation: Any,
        written: bool,
    ) -> None:
        """Test that only records with valid custom fields are written; records without them are skipped."""
        handler = create_handler(batch_size=10)
        record_values: dict[str, Any] = {
            "name": logger_name,
            "module": "module",
            "funcName": "func",
            "lineno": 1,
            "levelname": "WARNING",
            "levelno": logging.WARNING,
            "msg": "event",
        }
        if custom_fields == "valid":
            record_values["custom_fields"] = log.CustomLogFields(
                process="Logging", table="", field="", process_stage="Processing"
            )
        elif custom_fields != "missing":
            record_values["custom_fields"] = custom_fields

        with (
            patch.object(
                handler, "_record_to_row", wraps=handler._record_to_row
            ) as convert,
            expectation,
        ):
            handler.emit(logging.makeLogRecord(record_values))

        assert convert.call_count == int(written)
        assert len(handler._buffer) == int(written)
        logging.Handler.close(handler)

    def test_emit_after_close_raises(self) -> None:
        """Test that records emitted after close raise rather than being silently lost."""
        handler = create_handler(batch_size=5)
        handler.close()

        with pytest.raises(RuntimeError, match="already been closed"):
            handler.emit(create_log_record())
        assert handler._buffer == []

    def test_flush_warns_when_open_and_is_silent_when_closed(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Test that flush() never uploads or raises, and warns (stderr + one buffered record) only once."""
        handler = create_handler(batch_size=5)
        handler._buffer = [{"message": "buffered"}]

        with patch.object(log.JsonlLogHandler, "_put_object") as mock_put:
            handler.flush()
            handler.flush()
        mock_put.assert_not_called()
        assert (
            capsys.readouterr().err.count("flushed outside of LoggingController") == 1
        )
        assert len(handler._buffer) == 2
        warning = handler._buffer[-1]
        assert warning["log_level"] == "WARNING"
        assert warning["function"] == "flush"
        assert warning["process"] == "Logging"
        assert warning["process_stage"] == "End"
        assert warning["line_number"] == configuration_log_line_number(
            log.JsonlLogHandler.flush
        )
        assert "flushed outside of LoggingController" in warning["message"]

        handler._buffer = []
        handler.close()
        handler.flush()
        assert capsys.readouterr().err == ""

    def test_logging_shutdown_still_closes_and_uploads_after_flush_warning(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Test that the stdlib atexit path warns via flush() but still uploads via close()."""
        handler = create_handler(batch_size=5)
        handler.emit(create_log_record(message="trailing log"))

        with patch.object(log.JsonlLogHandler, "_put_object") as mock_put:
            logging.shutdown(handlerList=[weakref.ref(handler)])

        mock_put.assert_called_once()
        body = mock_put.call_args.args[1]
        assert b"trailing log" in body
        assert b"flushed outside of LoggingController" in body
        assert handler._closed
        assert "flushed outside of LoggingController" in capsys.readouterr().err

    def test_write_configuration_log(self, s3: boto3.client) -> None:
        """Test that the configuration log is written correctly."""

        with freeze_time("2024-01-02T00:00:00Z"):
            handler = create_handler()
            handler._write_configuration_log()

        assert handler._buffer == [
            {
                "pipeline": DEFAULT_PIPELINE,
                "run_id": RUN_ID,
                "data_delivery_period": DELIVERY_PERIOD.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "attempt_no": 1,
                "logger_name": logging.getLogger().name,
                "module": "logging.log",
                "function": "_write_configuration_log",
                "line_number": configuration_log_line_number(),
                "log_level": "INFO",
                "log_timestamp": "2024-01-02T00:00:00Z",
                "process": "Configuration",
                "table": "",
                "field": "",
                "process_stage": "End",
                "message": "Logging configured successfully.",
                "exception": "",
            }
        ]
        logging.Handler.close(handler)

    @pytest.mark.parametrize("count", [2, 3])
    def test_emit_buffers_and_flushes_at_threshold(self, count: int) -> None:
        """Below the threshold rows remain in order; at the threshold they are flushed."""
        handler = create_handler(batch_size=3)
        records = [
            create_log_record(message=f"event {number}") for number in range(count)
        ]
        rows = [{"message": record.msg} for record in records]

        with (
            patch.object(
                log.JsonlLogHandler, "_record_to_row", side_effect=rows
            ) as mock_record,
            patch.object(
                log.JsonlLogHandler,
                "_flush_locked",
                side_effect=mock_flush_locked,
                autospec=True,
            ) as mock_flush,
        ):
            for record in records:
                handler.emit(record)

        assert [call.args for call in mock_record.call_args_list] == [
            (record, vars(record)["custom_fields"]) for record in records
        ]
        assert mock_flush.call_count == int(count == 3)
        assert handler._buffer == ([] if count == 3 else rows)

    @pytest.mark.parametrize("registration_fails", [False, True])
    def test_close_after_successful_flush(
        self, registration_fails: bool, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Close once and propagate registration failures without dumping uploaded records."""
        handler = create_handler(batch_size=3)
        handler._buffer = [
            {"message": "First application event"},
            {"message": "Second application event"},
        ]

        with (
            patch.object(
                log.JsonlLogHandler,
                "_flush_locked",
                side_effect=mock_flush_locked,
                autospec=True,
            ) as mock_flush,
            patch.object(
                log.JsonlLogHandler,
                "_register_logs_in_athena",
                side_effect=RuntimeError("Athena unavailable")
                if registration_fails
                else None,
            ) as mock_register,
        ):
            with (
                pytest.raises(RuntimeError, match="Athena unavailable")
                if registration_fails
                else nullcontext()
            ):
                handler.close()
            handler.close()

        mock_flush.assert_called_once()
        mock_register.assert_called_once_with()
        assert handler._buffer == []
        assert handler._closed
        assert capsys.readouterr().err == ""

    def test_concurrent_close_registers_once(self) -> None:
        """Test that a close() which starts while another close() holds the lock does not register again.

        The second thread enters close() before the handler is closed, then waits for the lock. Once the
        first close() finishes, the second must see the handler is closed and return without flushing or
        registering the run's logs a second time.
        """
        handler = create_handler()
        handler._buffer = [{"message": "event"}]
        main_thread = current_thread()
        second_waiting = Event()
        original_acquire = handler.acquire

        def acquire() -> None:
            if current_thread() is not main_thread:
                second_waiting.set()
            original_acquire()

        handler.acquire = acquire  # type: ignore[method-assign]

        with (
            patch.object(
                log.JsonlLogHandler,
                "_flush_locked",
                side_effect=mock_flush_locked,
                autospec=True,
            ) as mock_flush,
            patch.object(
                log.JsonlLogHandler, "_register_logs_in_athena"
            ) as mock_register,
        ):
            original_acquire()
            try:
                second = Thread(target=handler.close)
                second.start()
                assert second_waiting.wait(timeout=5)
                handler.close()
            finally:
                handler.release()
            second.join(timeout=5)

        assert not second.is_alive()
        mock_flush.assert_called_once()
        mock_register.assert_called_once_with()
        assert handler._closed

    @pytest.mark.parametrize(
        ("flush_side_effect", "match"),
        [
            # Retry cap reached inside _flush_locked.
            (RuntimeError("Failed to write logs after 3 attempts"), "after 3 attempts"),
            # _flush_locked returns but leaves records behind (e.g. a failed write below the cap).
            (None, "Failed to write 2 log records to S3"),
        ],
    )
    def test_close_failure_dumps_unwritten_records_to_stderr(
        self,
        flush_side_effect: Exception | None,
        match: str,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test that a terminal close failure prints every unwritten record to stderr before raising."""
        handler = create_handler()
        rows = [{"message": "First application event"}, {"message": "Second, ü"}]
        handler._buffer = list(rows)

        with (
            patch.object(
                log.JsonlLogHandler, "_flush_locked", side_effect=flush_side_effect
            ) as mock_flush,
            patch.object(
                log.JsonlLogHandler, "_register_logs_in_athena"
            ) as mock_register,
            pytest.raises(RuntimeError, match=match),
        ):
            handler.close()

        mock_register.assert_not_called()
        mock_flush.assert_called_once_with()
        assert handler._buffer == rows
        assert handler._closed
        header, *dumped = capsys.readouterr().err.splitlines()
        assert header == (
            f"ERROR: 2 log records could not be written to s3://{DEFAULT_BUCKET}/"
            f"{DEFAULT_LOG_STORE_PREFIX}. Dumping records:"
        )
        assert [json.loads(line) for line in dumped] == rows

    def test_create_key_directory(self) -> None:
        """Test that the object key is generated correctly."""
        handler = create_handler()
        key_directory = handler._create_key_directory(DEFAULT_LOG_STORE_PREFIX)
        assert (
            key_directory
            == f"prefix/to/log/data_delivery_period={ENCODED_DELIVERY_PERIOD}/attempt_no=1/run_id={ENCODED_RUN_ID}/"
        )

    def test_registers_scoped_run_as_partitioned_dataset(self) -> None:
        """Test that only this handler's written keys are registered, with explicit Athena types."""
        log_store_prefix = "custom/log-store"
        athena_prefix = "custom/logs/"
        database_name = "custom_log_database"
        table_name = "pipeline_events"

        handler = create_handler(
            log_store_prefix=log_store_prefix,
            athena_prefix=athena_prefix,
            athena_database_name=database_name,
            athena_table_name=table_name,
        )
        handler._part_number = 2
        raw_logs = pd.DataFrame(
            {
                "log_timestamp": [
                    "2024-01-02T10:00:00.123456Z",
                    "2024-01-02T11:00:00Z",
                ],
                "message": ["first", "second"],
            }
        )

        with (
            patch(
                "opg_pipeline_builder.logging.log.wr.s3.read_json",
                return_value=raw_logs,
            ) as mock_read_json,
            patch(
                "opg_pipeline_builder.logging.log.wr.s3.to_parquet"
            ) as mock_to_parquet,
            patch(
                "opg_pipeline_builder.logging.log.wr.catalog.create_database"
            ) as mock_create_database,
        ):
            handler._register_logs_in_athena()

        mock_read_json.assert_called_once_with(
            path=[
                f"s3://{DEFAULT_BUCKET}/custom/log-store/data_delivery_period={ENCODED_DELIVERY_PERIOD}"
                f"/attempt_no=1/run_id={ENCODED_RUN_ID}/{os.getpid()}_{handler._writer_id}_{part}.jsonl"
                for part in range(2)
            ],
            lines=True,
            dtype=False,
            convert_dates=False,
        )
        mock_create_database.assert_called_once_with(database_name, exist_ok=True)
        kwargs = dict(mock_to_parquet.call_args.kwargs)
        registered = kwargs.pop("df")
        assert kwargs == {
            "path": f"s3://{DEFAULT_BUCKET}/{athena_prefix}",
            "dataset": True,
            "mode": "append",
            "database": database_name,
            "table": table_name,
            "partition_cols": ["data_delivery_period", "attempt_no", "run_id"],
            "dtype": logging_utils.ATHENA_COLUMN_TYPES,
        }
        assert isinstance(registered["log_timestamp"].dtype, pd.DatetimeTZDtype)
        assert str(registered["log_timestamp"].dt.tz) == "UTC"
        assert registered["log_timestamp"].tolist() == [
            pd.Timestamp("2024-01-02T10:00:00.123456Z"),
            pd.Timestamp("2024-01-02T11:00:00Z"),
        ]

    def test_athena_column_types_match_structured_log_record(self) -> None:
        """Test that every StructuredLogRecord field has an explicit Athena type."""
        assert list(logging_utils.ATHENA_COLUMN_TYPES) == list(
            log.StructuredLogRecord.model_fields
        )

    def test_put_object_success(self, s3: boto3.client) -> None:
        """Test that the handler successfully puts an object to S3."""

        handler = create_handler()

        key = handler._object_key(handler._part_number)
        bytes_data = b"some random data"

        handler._put_object(key, bytes_data)

        data = s3.get_object(Bucket="log-bucket", Key=key)
        assert data["Body"].read() == bytes_data

    def test_flush_locked_success(self, s3: boto3.client) -> None:
        """Test that the handler successfully flushes the buffer to S3."""

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

        log_1_key = f"prefix/to/log/data_delivery_period={ENCODED_DELIVERY_PERIOD}/attempt_no=1/run_id={ENCODED_RUN_ID}/{os.getpid()}_{handler._writer_id}_0.jsonl"

        assert log_1_key in logs
        log_1_records = logs[log_1_key]

        assert len(log_1_records) == 4
        assert log_1_records[0]["message"] == "Logging configured successfully."
        assert log_1_records[1]["message"] == "First application event"
        assert log_1_records[2]["message"] == "Second application event"
        assert log_1_records[3]["message"] == "Third application event"

        log_2_key = f"prefix/to/log/data_delivery_period={ENCODED_DELIVERY_PERIOD}/attempt_no=1/run_id={ENCODED_RUN_ID}/{os.getpid()}_{handler._writer_id}_1.jsonl"
        assert log_2_key in logs
        log_2_records = logs[log_2_key]

        assert len(log_2_records) == 1
        assert log_2_records[0]["message"] == "Fourth application event"

    def test_flush_locked_empty(self) -> None:
        """Test that flushing an empty buffer does not raise an error."""
        handler = create_handler(batch_size=5)
        with patch.object(log.JsonlLogHandler, "_put_object") as mock_put:
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
                log.JsonlLogHandler, "_put_object", side_effect=exception
            ) as mock_put:
                handler._flush_locked()
            assert handler._write_failures == iteration
            assert f"failure_count={iteration}" in capsys.readouterr().err
            assert handler._batch_size == 5 * (iteration + 1)
            assert any(
                "Failed to write JSONL" in log["message"] for log in handler._buffer
            )
            assert len(handler._buffer) == iteration * 2

        with patch.object(log.JsonlLogHandler, "_put_object") as mock_put:
            handler._flush_locked()

        mock_put.assert_called_once()
        assert handler._buffer == []
        assert handler._part_number == 1
        assert handler._batch_size == 5
        assert handler._write_failures == 0

    @pytest.mark.parametrize(
        "exception", [RuntimeError("bug"), ValueError("bug"), TypeError("bug")]
    )
    def test_flush_locked_non_sink_error_propagates(self, exception: Exception) -> None:
        """Test that errors other than S3/network failures are treated as bugs, not sink failures.

        They propagate immediately and leave the retry state untouched, so they are not reported as a
        failed write and retried.
        """
        handler = create_handler(batch_size=5)
        handler.emit(create_log_record())
        buffered = list(handler._buffer)

        with (
            patch.object(log.JsonlLogHandler, "_put_object", side_effect=exception),
            pytest.raises(type(exception), match="bug"),
        ):
            handler._flush_locked()

        assert handler._write_failures == 0
        assert handler._batch_size == 5
        assert handler._buffer == buffered
        assert handler._part_number == 0

    def test_flush_locked_failure_record(self) -> None:
        """Test the internal failure record's attribution and empty table/field values."""
        handler = create_handler(batch_size=5)
        handler.emit(create_log_record())

        with patch.object(
            log.JsonlLogHandler, "_put_object", side_effect=OSError("disk gone")
        ):
            handler._flush_locked()

        failure = handler._buffer[-1]
        assert failure["exception"].startswith("Traceback (most recent call last):")
        assert failure["exception"].rstrip().endswith("OSError: disk gone")
        assert (
            failure["message"] == "Failed to write JSONL log batch (failure_count=1)."
        )
        assert failure["function"] == "_flush_locked"
        assert failure["line_number"] == configuration_log_line_number(
            log.JsonlLogHandler._flush_locked
        )
        assert failure["module"] == "logging.log"
        assert failure["log_level"] == "ERROR"
        assert failure["table"] == ""
        assert failure["field"] == ""
        assert handler._part_number == 0

    def test_registration_skipped_when_nothing_uploaded(self) -> None:
        """Test that no Glue/S3 calls are made when this handler never uploaded an object."""
        handler = create_handler()
        with (
            patch("opg_pipeline_builder.logging.log.wr.s3.read_json") as mock_read_json,
            patch(
                "opg_pipeline_builder.logging.log.wr.s3.to_parquet"
            ) as mock_to_parquet,
            patch(
                "opg_pipeline_builder.logging.log.wr.catalog.create_database"
            ) as mock_create_database,
        ):
            handler._register_logs_in_athena()

        mock_read_json.assert_not_called()
        mock_to_parquet.assert_not_called()
        mock_create_database.assert_not_called()

    def test_registration_reads_exactly_the_uploaded_keys(self) -> None:
        """Test that the keys rebuilt at registration match the keys uploaded, and a failed upload does
        not consume a part number."""
        handler = create_handler(batch_size=5)
        uploaded: list[str] = []

        handler.emit(create_log_record())
        with patch.object(log.JsonlLogHandler, "_put_object") as mock_put:
            handler._flush_locked()
        uploaded.append(mock_put.call_args.args[0])
        handler.emit(create_log_record())
        with patch.object(log.JsonlLogHandler, "_put_object", side_effect=OSError):
            handler._flush_locked()
        with patch.object(log.JsonlLogHandler, "_put_object") as mock_put:
            handler._flush_locked()
        uploaded.append(mock_put.call_args.args[0])

        with (
            patch(
                "opg_pipeline_builder.logging.log.wr.s3.read_json",
                return_value=pd.DataFrame({"log_timestamp": ["2024-01-02T00:00:00Z"]}),
            ) as mock_read_json,
            patch("opg_pipeline_builder.logging.log.wr.s3.to_parquet"),
            patch("opg_pipeline_builder.logging.log.wr.catalog.create_database"),
        ):
            handler._register_logs_in_athena()

        prefix = f"prefix/to/log/data_delivery_period={ENCODED_DELIVERY_PERIOD}/attempt_no=1/run_id={ENCODED_RUN_ID}/"
        assert uploaded == [
            f"{prefix}{os.getpid()}_{handler._writer_id}_{part}.jsonl"
            for part in range(2)
        ]
        assert mock_read_json.call_args.kwargs["path"] == [
            f"s3://{DEFAULT_BUCKET}/{key}" for key in uploaded
        ]

    def test_flush_locked_fail_then_hit_threshold(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Test that the full buffer is correctly flushed after two failed attempts."""
        handler = create_handler(batch_size=5)

        for iteration in range(1, 4):
            handler.emit(create_log_record(message=f"application event: {iteration}"))
            with patch.object(log.JsonlLogHandler, "_put_object", side_effect=OSError):
                handler._flush_locked()
            assert handler._write_failures == iteration
            assert f"failure_count={iteration}" in capsys.readouterr().err
            assert handler._batch_size == 5 * (iteration + 1)
            assert len(handler._buffer) == iteration * 2

        handler.emit(create_log_record(message=f"application event: {iteration}"))
        with (
            patch.object(log.JsonlLogHandler, "_put_object", side_effect=OSError),
            pytest.raises(
                RuntimeError, match="Attempted to write logs 4 times. Retries exceeded"
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
            row = handler._record_to_row(record, vars(record)["custom_fields"])
            assert row == {
                "pipeline": DEFAULT_PIPELINE,
                "run_id": RUN_ID,
                "data_delivery_period": DELIVERY_PERIOD.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "attempt_no": 1,
                "logger_name": "opg_pipeline_builder.test_module",
                "module": "test_module",
                "function": "test_func",
                "line_number": 42,
                "log_level": "INFO",
                "log_timestamp": "2024-06-01T12:00:00Z",
                "process": "Logging",
                "table": "table_a",
                "field": "field_a",
                "process_stage": "Processing",
                "message": "application event: 1",
                "exception": "",
            }

    def test_record_to_row_captures_exception(self) -> None:
        """Test that the full traceback (cause, context, notes) is written to the exception column."""
        handler = create_handler(batch_size=5)
        record = create_log_record(message="Load failed")

        try:
            try:
                raise KeyError("missing_column")
            except KeyError as cause:
                error = ValueError("bad row")
                error.add_note("while loading table_a")
                raise error from cause
        except ValueError:
            record.exc_info = sys.exc_info()

        row = handler._record_to_row(record, vars(record)["custom_fields"])

        exception = row["exception"]
        assert isinstance(exception, str)
        assert row["message"] == "Load failed"
        assert exception.startswith("Traceback (most recent call last):")
        assert "KeyError: 'missing_column'" in exception
        assert "The above exception was the direct cause" in exception
        assert "ValueError: bad row" in exception
        assert exception.rstrip().endswith("while loading table_a")
        # Cached so the console handler prints the same text without formatting it again.
        assert record.exc_text == exception

    @pytest.mark.parametrize("exc_info", [None, (None, None, None)])
    def test_record_to_row_without_exception(self, exc_info: Any) -> None:
        """Test that the exception column is empty when there is no active exception.

        exc_info=True outside an 'except' block produces (None, None, None).
        """
        handler = create_handler(batch_size=5)
        record = create_log_record()
        record.exc_info = exc_info

        assert (
            handler._record_to_row(record, vars(record)["custom_fields"])["exception"]
            == ""
        )

    def test_record_to_row_truncates_message_and_exception(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that long text is capped: the message keeps its start, the traceback keeps its end."""
        monkeypatch.setattr(log, "_MAX_TEXT_LENGTH", 10)
        handler = create_handler(batch_size=5)
        record = create_log_record(message="0123456789ABCDEF")
        record.exc_info = (ValueError, ValueError("x"), None)
        record.exc_text = "Traceback line\nValueError: x"

        row = handler._record_to_row(record, vars(record)["custom_fields"])

        assert row["message"] == "0123456789...[truncated 6 characters]..."
        assert row["exception"] == "...[truncated 18 characters]...ueError: x"


class TestLoggingController:
    @pytest.fixture
    def third_party_handler(self) -> Generator[logging.Handler]:
        """A handler owned by someone else (e.g. Airflow or pytest) attached to the root logger."""
        handler = logging.NullHandler()
        logging.getLogger().addHandler(handler)
        yield handler
        logging.getLogger().removeHandler(handler)

    def create_controller(self) -> log.LoggingController:
        logger = logging.getLogger()
        handler = create_handler(batch_size=5)
        handler.set_name(log._JSONL_HANDLER_NAME)
        console_handler = logging.StreamHandler()
        console_handler.set_name(log._CONSOLE_HANDLER_NAME)
        logger.addHandler(handler)
        logger.addHandler(console_handler)
        return log.LoggingController(logger, [handler, console_handler])

    @staticmethod
    def owned_handlers_removed(controller: log.LoggingController) -> bool:
        return all(
            handler not in controller._logger.handlers
            for handler in controller._handlers
        )

    def test_flush_success(self) -> None:
        """Test that the flush method calls the handler's _flush_locked method."""
        controller = self.create_controller()
        with patch.object(log.JsonlLogHandler, "_flush_locked") as mock_flush_locked:
            controller.flush()

        mock_flush_locked.assert_called_once()

    def test_flush_fails_when_shutdown(self) -> None:
        """Test that the flush method raises an error if logging has been shut down."""
        controller = self.create_controller()
        controller._is_shutdown = True
        with pytest.raises(RuntimeError, match="Logging has already been shut down."):
            controller.flush()

    def test_shutdown_only_closes_owned_handlers(
        self, third_party_handler: logging.Handler
    ) -> None:
        """Test that shutdown closes and removes its own handlers and leaves others untouched."""
        controller = self.create_controller()
        with (
            patch.object(log.JsonlLogHandler, "close") as mock_jsonl_close,
            patch.object(logging.StreamHandler, "close") as mock_console_close,
            patch.object(third_party_handler, "close") as mock_third_party_close,
        ):
            controller.shutdown()
            controller.shutdown()

        mock_jsonl_close.assert_called_once()
        mock_console_close.assert_called_once()
        mock_third_party_close.assert_not_called()
        assert third_party_handler in logging.getLogger().handlers
        assert controller._is_shutdown
        assert self.owned_handlers_removed(controller)

    def test_shutdown_fails_when_already_shutdown(self) -> None:
        """Test that the shutdown method does nothing if logging has already been shut down."""
        controller = self.create_controller()
        controller._is_shutdown = True
        with patch.object(log.JsonlLogHandler, "close") as mock_close:
            controller.shutdown()

        assert mock_close.call_count == 0

    @pytest.mark.parametrize(
        "error",
        [
            RuntimeError("flush failed"),
            TypeError("bad type"),
            ValueError("bad value"),
            OSError("disk"),
            KeyError("missing"),
            ClientError({"Error": {"Code": "AccessDenied"}}, "GetObject"),
        ],
    )
    def test_shutdown_error_still_closes_remaining_handlers(
        self, error: Exception
    ) -> None:
        """Test that any close error is re-raised after every handler is closed and removed."""
        controller = self.create_controller()
        with (
            patch.object(log.JsonlLogHandler, "close", side_effect=error),
            patch.object(logging.StreamHandler, "close") as mock_console_close,
            pytest.raises(type(error)) as raised,
        ):
            controller.shutdown()

        assert raised.value is error
        mock_console_close.assert_called_once()
        assert controller._is_shutdown
        assert self.owned_handlers_removed(controller)

    def test_shutdown_multiple_errors_raises_first_with_notes(self) -> None:
        """Test that later close errors are attached as notes to the first one."""
        controller = self.create_controller()
        with (
            patch.object(
                log.JsonlLogHandler, "close", side_effect=RuntimeError("jsonl failed")
            ),
            patch.object(
                logging.StreamHandler, "close", side_effect=ValueError("console failed")
            ),
            pytest.raises(RuntimeError, match="jsonl failed") as raised,
        ):
            controller.shutdown()

        assert raised.value.__notes__ == [
            "Another logging handler also failed to close: ValueError: console failed"
        ]
        assert controller._is_shutdown
        assert self.owned_handlers_removed(controller)

    @pytest.mark.parametrize("body_fails", [False, True])
    @pytest.mark.parametrize("shutdown_fails", [False, True])
    def test_context_manager_preserves_primary_error(
        self, body_fails: bool, shutdown_fails: bool, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Exercise all four outcomes, preserving error identity, notes, diagnostics and cleanup."""
        controller = self.create_controller()
        body_error = ValueError("pipeline failed") if body_fails else None
        shutdown_error = RuntimeError("flush failed") if shutdown_fails else None
        primary_error = body_error if body_fails else shutdown_error
        expectation = (
            pytest.raises(type(primary_error))
            if primary_error is not None
            else nullcontext()
        )
        with (
            patch.object(
                log.JsonlLogHandler, "close", side_effect=shutdown_error
            ) as mock_close,
            expectation as raised,
        ):
            with controller as entered:
                assert entered is controller
                if body_error is not None:
                    raise body_error

        mock_close.assert_called_once_with()
        if primary_error is not None:
            assert raised is not None
            assert raised.value is primary_error
            assert getattr(raised.value, "__notes__", []) == (
                ["Logging shutdown also failed: RuntimeError: flush failed"]
                if body_fails and shutdown_fails
                else []
            )
        stderr = capsys.readouterr().err
        if body_fails and shutdown_fails:
            assert "Traceback (most recent call last):" in stderr
            assert "RuntimeError: flush failed" in stderr
        else:
            assert stderr == ""
        assert controller._is_shutdown
        assert self.owned_handlers_removed(controller)


class TestConfigureLogging:
    """Tests for the configure_logging function."""

    def configure_logging_wrapper(self, **overrides: Any) -> log.LoggingController:
        """Wrapper for the configure_logging function to simplify test calls."""
        return log.configure_logging(**config_values(**overrides))

    def test_configure_returns_success(self, s3: boto3.client) -> None:
        """Test that configure_logging returns a LoggingController and sets up handlers correctly."""

        controller = self.configure_logging_wrapper(
            log_store_prefix="custom/log-store",
            athena_prefix="custom/logs/",
            athena_database_name="custom_log_database",
            athena_table_name="pipeline_events",
        )
        root_logger = logging.getLogger()
        assert controller._logger is root_logger
        assert root_logger.level == logging.INFO
        handlers_by_name = {
            handler.get_name(): handler for handler in root_logger.handlers
        }
        assert log._JSONL_HANDLER_NAME in handlers_by_name
        assert log._CONSOLE_HANDLER_NAME in handlers_by_name
        assert handlers_by_name[log._CONSOLE_HANDLER_NAME].formatter._fmt == (  # type: ignore[union-attr]
            "%(asctime)s | %(name)s | %(funcName)s | %(levelname)s | %(message)s"
        )
        jsonl_handler = handlers_by_name[log._JSONL_HANDLER_NAME]
        assert isinstance(jsonl_handler, log.JsonlLogHandler)
        assert jsonl_handler._config.log_store_prefix == "custom/log-store"
        assert jsonl_handler._config.athena_prefix == "custom/logs/"
        assert jsonl_handler._config.athena_database_name == "custom_log_database"
        assert jsonl_handler._config.athena_table_name == "pipeline_events"
        assert isinstance(controller, log.LoggingController)
        console_handler = handlers_by_name[log._CONSOLE_HANDLER_NAME]
        assert controller._handlers == [jsonl_handler, console_handler]
        assert isinstance(console_handler, logging.StreamHandler)
        assert root_logger.handlers.index(jsonl_handler) < root_logger.handlers.index(
            console_handler
        )
        assert jsonl_handler._config.batch_size == 500

    @pytest.mark.parametrize(
        ("prefix_name", "invalid_prefix"),
        [
            ("log_store_prefix", "s3://log-bucket/custom/logs/"),
            ("log_store_prefix", "log-bucket/custom/logs/"),
            ("athena_prefix", "s3://log-bucket/custom/logs/"),
            ("athena_prefix", "log-bucket/custom/logs/"),
        ],
    )
    def test_configure_rejects_bucket_or_scheme_in_prefix(
        self, prefix_name: str, invalid_prefix: str
    ) -> None:
        """Reject full S3 URIs where bucket-relative prefixes are required."""
        with (
            patch("opg_pipeline_builder.logging.log.boto3.client") as mock_client,
            pytest.raises(
                exc.InvalidPrefixError,
                match=f"'{prefix_name}' must not include 's3://' or the bucket",
            ),
        ):
            self.configure_logging_wrapper(**{prefix_name: invalid_prefix})

        mock_client.assert_not_called()

    @pytest.mark.parametrize("batch_size", [0, -1, True, False, 2.5, "10", None])
    def test_configure_invalid_batch_size(self, batch_size: int) -> None:
        """Test that configure_logging raises a ValueError for invalid batch sizes."""
        with pytest.raises(ValidationError):
            self.configure_logging_wrapper(batch_size=batch_size)

    def test_duplicate_configuration_raises(self, s3: boto3.client) -> None:
        """Test that configuring logging twice raises a RuntimeError."""

        self.configure_logging_wrapper()
        with pytest.raises(RuntimeError, match="Logger has already been configured"):
            self.configure_logging_wrapper()

    def test_concurrent_configuration_attaches_one_handler(
        self, s3: boto3.client
    ) -> None:
        """Test that concurrent configure_logging calls cannot both pass the 'already configured' check.

        The slow preflight widens the window between the check and addHandler; without the module lock
        both threads would configure and two JSONL handlers would be attached to root.
        """
        barrier = Barrier(2)
        outcomes: list[str] = []

        def worker() -> None:
            barrier.wait(timeout=5)
            try:
                self.configure_logging_wrapper()
                outcomes.append("configured")
            except RuntimeError as error:
                outcomes.append(str(error))

        with patch.object(
            log.JsonlLogHandler,
            "_validate_log_location",
            side_effect=lambda: time.sleep(0.2),
        ):
            threads = [Thread(target=worker) for _ in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=10)

        assert sorted(outcomes) == [
            "Logger has already been configured.",
            "configured",
        ]
        assert [handler.get_name() for handler in logging.getLogger().handlers].count(
            log._JSONL_HANDLER_NAME
        ) == 1

    def test_duplicate_configuration_ignores_third_party_handlers(
        self, s3: boto3.client
    ) -> None:
        """Test that configuring logging ignores third-party handlers."""
        root_logger = logging.getLogger()
        third_party_handler = logging.StreamHandler()
        root_logger.addHandler(third_party_handler)

        self.configure_logging_wrapper()

        assert third_party_handler in root_logger.handlers

    def test_fails_when_bucket_missing(self, s3: boto3.client) -> None:
        """Test that configure_logging raises immediately if S3 is unreachable, attaching no handlers."""
        with pytest.raises(RuntimeError, match="Failed to write marker object"):
            self.configure_logging_wrapper(bucket="does-not-exist")

        handler_names = [handler.get_name() for handler in logging.getLogger().handlers]
        assert log._JSONL_HANDLER_NAME not in handler_names
        assert log._CONSOLE_HANDLER_NAME not in handler_names

    @pytest.mark.parametrize(
        ("log_store_prefix", "athena_prefix"),
        [("logs", "logs"), ("curated/raw", "curated/")],
    )
    def test_configure_rejects_overlapping_prefixes(
        self, log_store_prefix: str, athena_prefix: str
    ) -> None:
        """Test that raw logs cannot be written inside the curated Athena location."""
        with (
            patch("opg_pipeline_builder.logging.log.boto3.client") as mock_client,
            pytest.raises(
                exc.InvalidPrefixError,
                match="must not be the same as, or nested inside",
            ),
        ):
            self.configure_logging_wrapper(
                log_store_prefix=log_store_prefix, athena_prefix=athena_prefix
            )
        mock_client.assert_not_called()

    def test_configure_rejects_naive_data_delivery_period(self) -> None:
        """Test that a naive delivery period fails configuration and attaches no handlers."""
        with pytest.raises(ValueError, match="must be timezone-aware"):
            self.configure_logging_wrapper(
                data_delivery_period=datetime(2024, 1, 2)  # noqa: DTZ001
            )

        handler_names = [handler.get_name() for handler in logging.getLogger().handlers]
        assert log._JSONL_HANDLER_NAME not in handler_names
        assert log._CONSOLE_HANDLER_NAME not in handler_names


class TestModuleLogger:
    def test_rejects_extra_model_fields(self) -> None:
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            log.ModuleLogger.model_validate(
                {
                    "logger": logging.getLogger("opg_pipeline_builder.test"),
                    "process": "Logging",
                    "table": "Logger",
                    "extra_field": "not allowed",
                }
            )

    @pytest.mark.usefixtures("logging_configured")
    @pytest.mark.parametrize("method", ["error", "info"])
    @pytest.mark.parametrize(
        ("init", "ctx", "call", "expected"),
        [
            ("", None, None, ""),  # Not set - use default
            ("Log", None, None, "Log"),  # Set in logger
            ("", None, "Log_call", "Log_call"),  # Set in func
            ("Log", None, "Log_call", "Log_call"),  # Set in logger, override in func
            ("Log", None, "", ""),  # Set in logger, explicitly cleared in func
            ("Log", "Log_ctx", "  ", "  "),  # An explicit value is not normalised
            ("", "Log_ctx", None, "Log_ctx"),  # Set in ctx mgr
            ("", "Log_ctx", "Log_call", "Log_call"),  # Set in ctx mgr, override in func
            ("Log", "Log_ctx", None, "Log_ctx"),  # Set in logger, override in ctx mgr
            ("Log", "", None, ""),  # Set in logger, cleared by ctx mgr
            ("Log", "  ", None, "  "),  # Preserve explicit whitespace in a context
            ("Log", "Log_ctx", "", ""),  # Set in logger & ctx mgr, cleared in func
            (
                "Log",
                "Log_ctx",
                "Log_call",
                "Log_call",
            ),  # Set in logger, override by ctx mgr & func
        ],
    )
    def test_module_logger_table_precedence(
        self,
        method: str,
        init: str,
        ctx: str | None,
        call: str | None,
        expected: str,
    ) -> None:
        """Test table precedence across ModuleLogger init, with_table context manager, and log call."""
        module_logger = log.ModuleLogger(
            logger=logging.getLogger("opg_pipeline_builder.test"),
            process="Logging",
            table=init,
        )

        cm = module_logger.with_table(ctx) if ctx is not None else nullcontext()
        with patch.object(logging.Logger, method) as mock_log, cm:
            log_fn = getattr(module_logger, method)
            log_fn("Test message 1", table=call)
            log_fn("Test message 2", table=call, field="field", stage="Start")

        assert [c.args for c in mock_log.call_args_list] == [
            ("Test message 1",),
            ("Test message 2",),
        ]
        assert [
            c.kwargs["extra"]["custom_fields"] for c in mock_log.call_args_list
        ] == [
            log.CustomLogFields(
                process="Logging", table=expected, field="", process_stage="Processing"
            ),
            log.CustomLogFields(
                process="Logging", table=expected, field="field", process_stage="Start"
            ),
        ]

    @pytest.mark.usefixtures("logging_configured")
    @pytest.mark.parametrize("method", ["error", "info"])
    @pytest.mark.parametrize(
        ("init", "call", "expected"),
        [
            ("Logging", None, "Logging"),  # Set on logger
            (None, "Logging_call", "Logging_call"),  # Set in func
            ("Logging", "Changed", "Changed"),  # Set on logger, override in func
        ],
    )
    def test_module_logger_process_override(
        self, method: str, init: str | None, call: str | None, expected: str
    ) -> None:
        """Test process precedence across ModuleLogger init and log call."""
        module_logger = log.ModuleLogger(
            logger=logging.getLogger("opg_pipeline_builder.test"),
            process=init,
        )
        with patch.object(logging.Logger, method) as mock_log:
            log_fn = getattr(module_logger, method)
            log_fn("Msg 1", process=call)
            log_fn("Msg 2", process=call, field="field", stage="Start")

        assert [c.args for c in mock_log.call_args_list] == [("Msg 1",), ("Msg 2",)]
        assert [
            c.kwargs["extra"]["custom_fields"].process for c in mock_log.call_args_list
        ] == [expected, expected]

    @pytest.mark.usefixtures("logging_configured")
    @pytest.mark.parametrize(
        ("kwargs", "expected"),
        [
            ({}, False),  # Default: no traceback
            ({"exc_info": True}, True),
            ({"exc_info": ValueError("given")}, "exception instance"),
        ],
    )
    def test_error_passes_exc_info(self, kwargs: dict[str, Any], expected: Any) -> None:
        """Test that error() forwards exc_info to the logger, so the traceback reaches the handlers."""
        module_logger = log.ModuleLogger(
            logger=logging.getLogger("opg_pipeline_builder.test"), process="Logging"
        )
        with patch.object(logging.Logger, "error") as mock_log:
            module_logger.error("Failed", **kwargs)

        exc_info = mock_log.call_args.kwargs["exc_info"]
        if expected == "exception instance":
            assert exc_info is kwargs["exc_info"]
        else:
            assert exc_info is expected

    @pytest.mark.usefixtures("logging_configured")
    @pytest.mark.parametrize("method", ["error", "info"])
    @pytest.mark.parametrize("invalid_process", ["", "   ", None])
    def test_module_logger_rejects_empty_process(
        self, method: str, invalid_process: Any
    ) -> None:
        """Test that ModuleLogger rejects logging when process is empty, whitespace, or None without fallback."""
        module_logger = log.ModuleLogger(
            logger=logging.getLogger("opg_pipeline_builder.test"),
            process=invalid_process,
        )
        log_fn = getattr(module_logger, method)
        with pytest.raises(exc.InvalidProcessError):
            log_fn("Test message", process=invalid_process)

    @pytest.mark.usefixtures("logging_configured")
    @pytest.mark.parametrize(
        ("method", "level"), [("error", logging.ERROR), ("info", logging.INFO)]
    )
    def test_log_attributes_record_to_caller(self, method: str, level: int) -> None:
        """Test that records are attributed to the code calling info()/error(), not to ModuleLogger."""
        records: list[logging.LogRecord] = []
        logger = logging.getLogger("opg_pipeline_builder.test.attribution")

        class Capture(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                records.append(record)

        capture = Capture()
        logger.addHandler(capture)
        logger.propagate = False
        module_logger = log.ModuleLogger(logger=logger, process="Logging")

        try:
            expected_line = inspect.currentframe().f_lineno + 1  # type: ignore[union-attr]
            getattr(module_logger, method)("value %s", "x", table="t", field="f")
        finally:
            logger.removeHandler(capture)
            logger.propagate = True

        [record] = records
        assert record.levelno == level
        assert record.getMessage() == "value x"
        assert record.funcName == "test_log_attributes_record_to_caller"
        assert record.lineno == expected_line
        assert record.pathname == __file__
        assert record.custom_fields == log.CustomLogFields(  # type: ignore[attr-defined]
            process="Logging", table="t", field="f", process_stage="Processing"
        )

    @pytest.mark.usefixtures("logging_configured")
    def test_with_table_is_scoped_and_nests(self) -> None:
        """Test with_table never mutates self.table, nests, and restores the previous override on exit."""
        module_logger = log.ModuleLogger(
            logger=logging.getLogger("opg_pipeline_builder.test"),
            process="Logging",
            table="Original",
        )
        with patch.object(logging.Logger, "info") as mock_info:
            with module_logger.with_table("Outer"):
                assert module_logger.table == "Original"
                module_logger.info("outer 1")
                with module_logger.with_table("Inner"):
                    module_logger.info("inner")
                    with module_logger.with_table(""):
                        module_logger.info("explicit empty")
                module_logger.info("outer 2")
            module_logger.info("after")

        assert [
            c.kwargs["extra"]["custom_fields"].table for c in mock_info.call_args_list
        ] == ["Outer", "Inner", "", "Outer", "Original"]
        assert module_logger.table == "Original"

    @pytest.mark.usefixtures("logging_configured")
    def test_with_table_restores_after_exception(self) -> None:
        """Test the override is reset even when the block raises."""
        module_logger = log.ModuleLogger(
            logger=logging.getLogger("opg_pipeline_builder.test"),
            process="Logging",
            table="Original",
        )
        with pytest.raises(ValueError, match="boom"), module_logger.with_table("Temp"):
            raise ValueError("boom")

        with patch.object(logging.Logger, "info") as mock_info:
            module_logger.info("after")
        assert mock_info.call_args.kwargs["extra"]["custom_fields"].table == "Original"

    @pytest.mark.usefixtures("logging_configured")
    @pytest.mark.parametrize("method", ["info", "error"])
    @pytest.mark.parametrize("table", [1, ["t"]])
    def test_with_table_rejects_non_str_at_log_call(
        self, method: str, table: Any
    ) -> None:
        """Table values are validated by CustomLogFields when a log is emitted."""
        module_logger = log.ModuleLogger(
            logger=logging.getLogger("opg_pipeline_builder.test"), process="Logging"
        )
        with (
            pytest.raises(ValidationError),
            patch.object(logging.Logger, method) as mock_log,
            module_logger.with_table(table),
        ):
            getattr(module_logger, method)("invalid table")
        mock_log.assert_not_called()

    @staticmethod
    def capture_tables(module_logger: log.ModuleLogger) -> list[tuple[str, str]]:
        """Attach a handler to the ModuleLogger's logger recording (thread name, table) per record."""
        captured: list[tuple[str, str]] = []

        class _Capture(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                fields = vars(record)["custom_fields"]
                assert isinstance(fields, log.CustomLogFields)
                captured.append((record.threadName or "", fields.table))

        module_logger.logger.addHandler(_Capture())
        return captured

    @pytest.fixture
    def threaded_logger(self) -> Generator[log.ModuleLogger]:
        """ModuleLogger on a dedicated logger whose handlers are removed after the test."""
        logger = logging.getLogger("opg_pipeline_builder.test.threads")
        yield log.ModuleLogger(logger=logger, process="Logging", table="Default")
        logger.handlers.clear()

    @pytest.mark.usefixtures("logging_configured")
    def test_with_table_is_isolated_between_threads(
        self, threaded_logger: log.ModuleLogger
    ) -> None:
        """Test concurrent with_table blocks on a shared ModuleLogger do not leak into each other.

        Each thread enters its block, waits until every thread is inside, then logs. With a shared
        mutable self.table, all threads would see the last value written.
        """
        captured = self.capture_tables(threaded_logger)
        tables = ["A", "B", "C", "D", None]
        barrier = Barrier(len(tables))

        def worker(table: str | None) -> None:
            cm = threaded_logger.with_table(table) if table else nullcontext()
            with cm:
                barrier.wait(timeout=5)
                threaded_logger.info("m")

        threads = [Thread(target=worker, args=(t,), name=f"t-{t}") for t in tables]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

        assert sorted(captured) == [
            ("t-A", "A"),
            ("t-B", "B"),
            ("t-C", "C"),
            ("t-D", "D"),
            ("t-None", "Default"),
        ]

    @pytest.mark.usefixtures("logging_configured")
    def test_with_table_thread_propagation(
        self, threaded_logger: log.ModuleLogger
    ) -> None:
        """Test new threads/executor workers do not inherit with_table, unless run in a copied context."""
        captured = self.capture_tables(threaded_logger)

        with threaded_logger.with_table("Caller"):
            thread = Thread(target=threaded_logger.info, args=("m",), name="plain")
            thread.start()
            thread.join(timeout=5)
            with ThreadPoolExecutor(max_workers=1, thread_name_prefix="pool") as pool:
                pool.submit(threaded_logger.info, "m").result(timeout=5)
                ctx = contextvars.copy_context()
                pool.submit(ctx.run, threaded_logger.info, "m").result(timeout=5)

        assert [table for _, table in captured] == ["Default", "Default", "Caller"]

    @pytest.mark.parametrize("method", ["error", "info"])
    def test_raises_when_logging_not_configured(self, method: str) -> None:
        """Test logging through ModuleLogger before configure_logging() raises instead of being dropped."""
        module_logger = log.ModuleLogger(
            logger=logging.getLogger("opg_pipeline_builder.test"), process="Logging"
        )
        with (
            patch.object(logging.Logger, method) as mock_log,
            pytest.raises(exc.LoggingNotConfiguredError, match="not configured"),
        ):
            getattr(module_logger, method)("message")
        mock_log.assert_not_called()

    @pytest.mark.parametrize("method", ["error", "info"])
    def test_raises_after_logging_shut_down(
        self, s3: boto3.client, method: str
    ) -> None:
        """Test logging through ModuleLogger after shutdown raises instead of being silently lost."""
        controller = log.configure_logging(**config_values())
        module_logger = log.ModuleLogger(
            logger=logging.getLogger("opg_pipeline_builder.test"), process="Logging"
        )
        getattr(module_logger, method)("before shutdown")
        controller.shutdown()

        with pytest.raises(exc.LoggingNotConfiguredError):
            getattr(module_logger, method)("after shutdown")

    def test_lazy_formatting_is_applied(self, s3: boto3.client) -> None:
        """Test that %s-style args passed through are lazily interpolated into the final message."""

        log.configure_logging(**config_values(batch_size=3))

        module_logger = log.ModuleLogger(
            logger=logging.getLogger("third_party.test"), process="Lazy formatting"
        )
        module_logger.info("Processed %s rows in %s seconds", 42, 1.5, table="table_a")
        module_logger.info("Processed %s rows in %s seconds", 42, 2.5, table="table_b")

        jsonl_handler = next(
            handler
            for handler in logging.getLogger().handlers
            if isinstance(handler, log.JsonlLogHandler)
        )

        key = jsonl_handler._object_key(0)

        all_logs = read_jsonl_logs(
            s3, bucket=DEFAULT_BUCKET, prefix=DEFAULT_LOG_STORE_PREFIX
        )
        test_log = all_logs[key]

        assert test_log[1]["logger_name"] == "third_party.test"
        assert test_log[2]["logger_name"] == "third_party.test"
        assert "Processed 42 rows in 1.5 seconds" in test_log[1]["message"]
        assert "Processed 42 rows in 2.5 seconds" in test_log[2]["message"]


class TestLoggingEndToEnd:
    """Exercise real logging and JSONL uploads against moto S3.

    Glue and Parquet writes are mocked; registration tests read the real uploaded objects.
    """

    def configure(
        self, **overrides: Any
    ) -> tuple[log.LoggingController, log.JsonlLogHandler]:
        """Configure logging and return the controller plus the underlying JSONL handler."""
        controller = log.configure_logging(
            **config_values(**{"batch_size": 3, **overrides})
        )
        jsonl_handler = next(
            handler
            for handler in controller._logger.handlers
            if isinstance(handler, log.JsonlLogHandler)
        )
        return controller, jsonl_handler

    def expected_object_key(
        self, jsonl_handler: log.JsonlLogHandler, part_number: int
    ) -> str:
        """Build the expected S3 key for a given part number."""
        return (
            f"{jsonl_handler._create_key_directory(jsonl_handler._config.log_store_prefix)}"
            f"{os.getpid()}_{jsonl_handler._writer_id}_{part_number}.jsonl"
        )

    def base_row(
        self,
        *,
        logger_name: str = logging.getLogger().name,
        module: str = "logging.log",
        function: str,
        line_number: int,
        log_level: str,
        process: str = "",
        table: str = "",
        field: str = "",
        process_stage: str = "Processing",
        message: str,
        exception: str = "",
        pipeline: str = DEFAULT_PIPELINE,
        attempt_no: int = 1,
        run_id: str = RUN_ID,
        log_timestamp: str = "2024-01-02T00:00:00Z",
    ) -> dict[str, Any]:
        """Build the expected structured row for a real logged record."""
        return {
            "pipeline": pipeline,
            "run_id": run_id,
            "data_delivery_period": "2024-01-02T00:00:00Z",
            "attempt_no": attempt_no,
            "logger_name": logger_name,
            "module": module,
            "function": function,
            "line_number": line_number,
            "log_level": log_level,
            "log_timestamp": log_timestamp,
            "process": process,
            "table": table,
            "field": field,
            "process_stage": process_stage,
            "message": message,
            "exception": exception,
        }

    def configuration_row(self, **overrides: Any) -> dict[str, Any]:
        """Expected startup record, independent of the production row builder."""
        return self.base_row(
            function="_write_configuration_log",
            line_number=configuration_log_line_number(),
            log_level="INFO",
            process="Configuration",
            process_stage="End",
            message="Logging configured successfully.",
            **overrides,
        )

    def test_pipeline_multiple_flushes_and_deliberate_shutdown(
        self, s3: boto3.client
    ) -> None:
        """Test a full pipeline run: an automatic batch flush, a manual flush, then a deliberate shutdown."""
        expected_rows = []
        with freeze_time("2024-01-02T00:00:00Z"):
            controller, jsonl_handler = self.configure(batch_size=4)
            loggers = {
                name: log.ModuleLogger(
                    logger=logging.getLogger(f"root.{name}"),
                    process="E2E test",
                    table="E2E table",
                )
                for name in ("extract", "load")
            }
            events = [
                ("extract", "info", "Extract started", "Start", ""),
                ("extract", "info", "Extract row", "Processing", "col_a"),
                ("load", "info", "Load started", "Start", ""),
                ("load", "info", "Loading field", "Processing", "col_a"),
                ("load", "error", "Load validation failed", "Processing", "col_a"),
                ("load", "info", "Load finished", "End", ""),
            ]
            for index, (name, method, message, stage, field) in enumerate(events):
                frame = inspect.currentframe()
                assert frame is not None
                line_number = frame.f_lineno + 1
                getattr(loggers[name], method)(message, stage=stage, field=field)
                expected_rows.append(
                    self.base_row(
                        logger_name=f"root.{name}",
                        module="test_log",
                        function=frame.f_code.co_name,
                        line_number=line_number,
                        log_level=method.upper(),
                        process="E2E test",
                        table="E2E table",
                        process_stage=stage,
                        field=field,
                        message=message,
                    )
                )
                if index == 4:
                    controller.flush()
            controller.shutdown()

        root_logger = logging.getLogger()
        assert all(
            handler.get_name()
            not in (log._CONSOLE_HANDLER_NAME, log._JSONL_HANDLER_NAME)
            for handler in root_logger.handlers
        )
        assert controller._is_shutdown is True
        assert jsonl_handler._buffer == []

        assert read_jsonl_logs(s3) == {
            self.expected_object_key(jsonl_handler, 0): [
                self.configuration_row(),
                *expected_rows[:3],
            ],
            self.expected_object_key(jsonl_handler, 1): expected_rows[3:5],
            self.expected_object_key(jsonl_handler, 2): expected_rows[5:],
        }

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

        function_name = inspect.currentframe().f_code.co_name  # type: ignore[union-attr]

        with freeze_time("2024-01-02T00:00:00Z"):
            controller, jsonl_handler = self.configure(batch_size=10)
            console_handler = next(
                handler
                for handler in controller._logger.handlers
                if handler.get_name() == log._CONSOLE_HANDLER_NAME
            )
            pipeline_logger = log.ModuleLogger(
                logger=logging.getLogger(f"{logging.getLogger().name}.pipeline")
            )

            if trigger == "context_manager":
                with controller as entered:
                    assert entered is controller
                    line_1 = inspect.currentframe().f_lineno + 1  # type: ignore[union-attr]
                    pipeline_logger.info(
                        "Pipeline started",
                        process="E2E test",
                        table="table_a",
                        stage="Start",
                    )
            else:
                line_1 = inspect.currentframe().f_lineno + 1  # type: ignore[union-attr]
                pipeline_logger.info(
                    "Pipeline started",
                    process="E2E test",
                    table="table_a",
                    stage="Start",
                )
                simulate_atexit_shutdown(controller)

        root_logger = logging.getLogger()
        assert jsonl_handler._buffer == []
        assert controller._is_shutdown is expect_shutdown_recorded
        assert (jsonl_handler not in root_logger.handlers) is expect_handler_removed
        assert (console_handler not in root_logger.handlers) is expect_handler_removed

        logs = read_jsonl_logs(
            s3, bucket=DEFAULT_BUCKET, prefix=DEFAULT_LOG_STORE_PREFIX
        )
        key = self.expected_object_key(jsonl_handler, 0)
        assert list(logs) == [key]
        assert logs[key] == [
            self.configuration_row(),
            self.base_row(
                logger_name=f"{logging.getLogger().name}.pipeline",
                module="test_log",
                function=function_name,
                line_number=line_1,
                log_level="INFO",
                process="E2E test",
                process_stage="Start",
                table="table_a",
                field="",
                message="Pipeline started",
            ),
            *(
                []
                if expect_shutdown_recorded
                else [
                    self.base_row(
                        function="flush",
                        line_number=configuration_log_line_number(
                            log.JsonlLogHandler.flush
                        ),
                        log_level="WARNING",
                        process="Logging",
                        process_stage="End",
                        message=log._UNMANAGED_FLUSH_WARNING,
                    )
                ]
            ),
        ]

    @pytest.mark.parametrize("trigger", ["context_manager", "logging_shutdown"])
    def test_shutdown_trigger_raises_on_failed_final_flush(
        self, s3: boto3.client, trigger: str, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Test that a failed final flush is never silently discarded, however shutdown is
        triggered.

        Via the context manager this fails the task itself (the recommended, fatal path); via
        the atexit-triggered logging.shutdown() fallback it's only reported to stderr, not fatal
        to the process, since shutdown() runs from atexit and Python only prints "Exception
        ignored in atexit callback" for errors it doesn't recognise. Either way the RuntimeError
        propagates out of the trigger itself, and nothing partial reaches S3.
        """

        with freeze_time("2024-01-02T00:00:00Z"):
            controller, jsonl_handler = self.configure(batch_size=10)
            pipeline_logger = log.ModuleLogger(
                logger=logging.getLogger(f"{logging.getLogger().name}.pipeline")
            )
            pipeline_logger.info(
                "Pipeline started", process="E2E test", table="table_a", stage="Start"
            )

            with (
                patch.object(log.JsonlLogHandler, "_put_object", side_effect=OSError()),
                pytest.raises(RuntimeError, match="Failed to write"),
            ):
                if trigger == "context_manager":
                    with controller:
                        pass
                else:
                    simulate_atexit_shutdown(controller)

        assert len(jsonl_handler._buffer) > 0
        logs = read_jsonl_logs(
            s3, bucket=DEFAULT_BUCKET, prefix=DEFAULT_LOG_STORE_PREFIX
        )
        assert logs == {}
        stderr = capsys.readouterr().err
        assert "could not be written" in stderr
        dumped = [
            json.loads(line) for line in stderr.splitlines() if line.startswith("{")
        ]
        assert [row["message"] for row in dumped] == [
            row["message"] for row in jsonl_handler._buffer
        ]
        assert "Pipeline started" in [row["message"] for row in dumped]

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

        controller, jsonl_handler = self.configure(batch_size=100)
        pipeline_logger = log.ModuleLogger(
            logger=logging.getLogger(f"{logging.getLogger().name}.pipeline")
        )
        pipeline_logger.info(
            "Started processing", process="E2E test", table="table_a", stage="Start"
        )

        with patch.object(log.JsonlLogHandler, "_put_object", side_effect=OSError()):
            # A transient failure is recoverable - controller.flush() just records it.
            controller.flush()
            assert jsonl_handler._write_failures == 1

            # Repeated failures eventually exceed the retry threshold and raise through flush().
            with pytest.raises(
                RuntimeError, match="Attempted to write logs 4 times. Retries exceeded"
            ):
                for _ in range(3):
                    controller.flush()

        # Recovery: once S3 is reachable again, the next flush succeeds and writes everything
        # buffered, including the synthetic failure notices recorded along the way.
        controller.flush()

        logs = read_jsonl_logs(
            s3, bucket=DEFAULT_BUCKET, prefix=DEFAULT_LOG_STORE_PREFIX
        )
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

        worker_count = 3
        messages_per_worker = 5

        with freeze_time("2024-01-02T00:00:00Z"):
            controller, jsonl_handler = self.configure(batch_size=4)

            def worker(worker_number: int) -> None:
                worker_logger = log.ModuleLogger(
                    logger=logging.getLogger(
                        f"{logging.getLogger().name}.worker_{worker_number}"
                    ),
                    process="E2E test",
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

        logs = read_jsonl_logs(
            s3, bucket=DEFAULT_BUCKET, prefix=DEFAULT_LOG_STORE_PREFIX
        )
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
                f"root.worker_{worker_number}",
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
                logger_name="root.worker_0",
                module="test_log",
                function="worker",
                line_number=next(
                    record["line_number"]
                    for record in worker_records
                    if record["message"] == "worker-0-0"
                ),
                log_level="INFO",
                process="E2E test",
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

    @staticmethod
    def read_json_via_moto(s3: boto3.client) -> Any:
        """Build a wr.s3.read_json replacement that reads the exact requested objects from moto."""

        def read_json(path: list[str], **kwargs: Any) -> pd.DataFrame:
            assert isinstance(path, list), (
                "Registration must read an explicit list of keys"
            )
            frames = []
            for uri in path:
                bucket, key = uri.removeprefix("s3://").split("/", 1)
                body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode()
                frames.append(pd.read_json(io.StringIO(body), **kwargs))
            return pd.concat(frames, ignore_index=True)

        return read_json

    @pytest.mark.usefixtures("logging_configured")
    def test_concurrent_writers_in_same_run_register_only_their_own_records(
        self, s3: boto3.client
    ) -> None:
        """Test that two writers (e.g. two processes) in the same run each register only their own
        objects, so every record lands in Athena exactly once even though they share a run prefix.

        Writers are attached to non-propagating named loggers to simulate separate processes, so the
        root-level configured check is satisfied with a stand-in.
        """
        threads_per_writer = 3
        messages_per_thread = 5
        writers = [create_handler(apply_patches=False, batch_size=4) for _ in range(2)]
        loggers = []
        for number, writer in enumerate(writers):
            logger = logging.getLogger(f"opg_pipeline_builder.e2e.writer_{number}")
            logger.propagate = False
            logger.setLevel(logging.INFO)
            logger.addHandler(writer)
            loggers.append(log.ModuleLogger(logger=logger, process="E2E test"))

        def worker(writer_number: int, thread_number: int) -> None:
            for message_number in range(messages_per_thread):
                loggers[writer_number].info(
                    "writer-%s-thread-%s-%s",
                    writer_number,
                    thread_number,
                    message_number,
                )

        threads = [
            Thread(target=worker, args=(writer_number, thread_number))
            for writer_number in range(len(writers))
            for thread_number in range(threads_per_writer)
        ]
        registered: list[pd.DataFrame] = []
        try:
            with (
                patch(
                    "opg_pipeline_builder.logging.log.wr.s3.read_json",
                    side_effect=self.read_json_via_moto(s3),
                ),
                patch(
                    "opg_pipeline_builder.logging.log.wr.s3.to_parquet",
                    side_effect=lambda df, **_: registered.append(df),
                ),
            ):
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join(timeout=30)
                # Writer 0 closes while writer 1's objects already exist under the same run prefix.
                writers[0].close()
                writers[1].close()
        finally:
            for module_logger, writer in zip(loggers, writers, strict=True):
                module_logger.logger.removeHandler(writer)
                module_logger.logger.propagate = True

        assert all(not thread.is_alive() for thread in threads)
        all_s3_messages = sorted(
            record["message"]
            for records in read_jsonl_logs(s3).values()
            for record in records
        )
        assert len(registered) == len(writers)
        for writer_number, frame in enumerate(registered):
            application_messages = {
                message
                for message in frame["message"]
                if message != "Logging configured successfully."
            }
            assert application_messages == {
                f"writer-{writer_number}-thread-{thread_number}-{message_number}"
                for thread_number in range(threads_per_writer)
                for message_number in range(messages_per_thread)
            }
            assert set(frame["logger_name"]) == {
                f"opg_pipeline_builder.e2e.writer_{writer_number}",
                logging.getLogger().name,
            }
        registered_messages = sorted(
            message for frame in registered for message in frame["message"]
        )
        assert registered_messages == all_s3_messages
        assert len(registered_messages) == len(set(registered_messages)) + 1

    def test_shutdown_during_threaded_logging(self, s3: boto3.client) -> None:
        """Test shutting down while worker threads are still logging.

        Records logged before shutdown are all uploaded and registered exactly once. Records logged
        after shutdown are not written: once the handlers are detached ModuleLogger raises
        LoggingNotConfiguredError, a record racing the close raises RuntimeError rather than being
        silently buffered, and nothing is registered twice.
        """
        worker_count = 4
        pre_shutdown_messages = 6
        ready = [Event() for _ in range(worker_count)]
        stop = Event()
        unexpected_errors: list[BaseException] = []
        registered: list[pd.DataFrame] = []

        controller, jsonl_handler = self.configure(batch_size=5)

        def worker(worker_number: int) -> None:
            worker_logger = log.ModuleLogger(
                logger=logging.getLogger(
                    f"opg_pipeline_builder.e2e.worker_{worker_number}"
                ),
                process="E2E test",
            )
            try:
                for message_number in range(pre_shutdown_messages):
                    worker_logger.info("pre-%s-%s", worker_number, message_number)
                ready[worker_number].set()
                message_number = 0
                while not stop.is_set():
                    try:
                        worker_logger.info("post-%s-%s", worker_number, message_number)
                    except exc.LoggingNotConfiguredError:
                        pass  # Handlers already detached from root.
                    except RuntimeError as error:
                        assert "already been closed" in str(error)  # Raced the close.
                    message_number += 1
            except BaseException as error:  # noqa: BLE001
                unexpected_errors.append(error)
                ready[worker_number].set()

        threads = [
            Thread(target=worker, args=(number,)) for number in range(worker_count)
        ]
        with (
            patch(
                "opg_pipeline_builder.logging.log.wr.s3.read_json",
                side_effect=self.read_json_via_moto(s3),
            ),
            patch(
                "opg_pipeline_builder.logging.log.wr.s3.to_parquet",
                side_effect=lambda df, **_: registered.append(df),
            ),
        ):
            for thread in threads:
                thread.start()
            for event in ready:
                assert event.wait(timeout=30)
            controller.shutdown()
            stop.set()
            for thread in threads:
                thread.join(timeout=30)

        assert unexpected_errors == []
        assert all(not thread.is_alive() for thread in threads)
        assert controller._is_shutdown
        assert jsonl_handler._closed
        assert jsonl_handler not in logging.getLogger().handlers

        s3_messages = sorted(
            record["message"]
            for records in read_jsonl_logs(s3).values()
            for record in records
        )
        [frame] = registered
        assert sorted(frame["message"]) == s3_messages
        assert len(s3_messages) == len(set(s3_messages))
        assert {
            f"pre-{worker_number}-{message_number}"
            for worker_number in range(worker_count)
            for message_number in range(pre_shutdown_messages)
        } <= set(s3_messages)

    def test_pipeline_with_no_application_logs_still_flushes_config_record(
        self, s3: boto3.client
    ) -> None:
        """Test that shutting down immediately after configure_logging still flushes the config record."""

        with freeze_time("2024-01-02T00:00:00Z"):
            controller, jsonl_handler = self.configure(batch_size=10)
            with controller:
                pass

        logs = read_jsonl_logs(
            s3, bucket=DEFAULT_BUCKET, prefix=DEFAULT_LOG_STORE_PREFIX
        )
        key = self.expected_object_key(jsonl_handler, 0)
        assert list(logs) == [key]
        assert logs[key] == [self.configuration_row()]

    def test_error_with_exception_written_to_jsonl_and_console(
        self, s3: boto3.client, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Test that error(exc_info=True) writes the full traceback to JSONL and prints it to the console."""
        with freeze_time("2024-01-02T00:00:00Z"):
            controller, jsonl_handler = self.configure(batch_size=10)
            module_logger = log.ModuleLogger(
                logger=logging.getLogger(f"{logging.getLogger().name}.load"),
                process="E2E test",
                table="E2E table",
            )
            with controller:
                try:
                    try:
                        raise KeyError("missing_column")
                    except KeyError as cause:
                        raise ValueError("bad row") from cause
                except ValueError:
                    module_logger.error("Load failed", exc_info=True)
                module_logger.info("Load finished", stage="End")

        logs = read_jsonl_logs(
            s3, bucket=DEFAULT_BUCKET, prefix=DEFAULT_LOG_STORE_PREFIX
        )
        _, error_row, info_row = logs[self.expected_object_key(jsonl_handler, 0)]

        exception = error_row["exception"]
        assert error_row["message"] == "Load failed"
        assert exception.startswith("Traceback (most recent call last):")
        assert "KeyError: 'missing_column'" in exception
        assert "The above exception was the direct cause" in exception
        assert exception.rstrip().endswith("ValueError: bad row")
        assert info_row["exception"] == ""
        assert exception in capsys.readouterr().err

    def test_two_pipeline_runs_with_different_attempt_numbers_do_not_collide(
        self, s3: boto3.client
    ) -> None:
        """Test that two runs for the same pipeline/data_delivery_period but different attempt_no
        write to distinct S3 partitions without colliding or mixing records.
        """

        expected = {}
        with freeze_time("2024-01-02T00:00:00Z"):
            for attempt, message in [
                (1, "First attempt processing"),
                (2, "Second attempt processing"),
            ]:
                controller, handler = self.configure(batch_size=10, attempt_no=attempt)
                with controller:
                    log.ModuleLogger(logger=logging.getLogger("root.attempt")).info(
                        message,
                        process="E2E test",
                        table="table_a",
                        stage="Start",
                    )
                expected[self.expected_object_key(handler, 0)] = (attempt, message)

        logs = read_jsonl_logs(s3)
        assert set(logs) == set(expected)
        for key, (attempt, message) in expected.items():
            assert [record["message"] for record in logs[key]] == [
                "Logging configured successfully.",
                message,
            ]
            assert all(record["attempt_no"] == attempt for record in logs[key])
