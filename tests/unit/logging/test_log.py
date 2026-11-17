"""Tests for structured logging configuration, handlers, controllers and integration behavior."""

# pyright: reportPrivateUsage=false
# pylint: disable=protected-access

import inspect
import json
import logging
import os
import re
import sys
import time
import traceback
from collections.abc import Callable, Generator
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta, timezone
from threading import Barrier, Event, current_thread
from typing import Any, Literal, cast
from unittest.mock import call, patch

import awswrangler as wr
import boto3
import pandas as pd
import pytest
from botocore.exceptions import BotoCoreError, ClientError
from pydantic import ValidationError

from opg_pipeline_builder.logging import log
from opg_pipeline_builder.logging import log_exceptions as exc
from opg_pipeline_builder.logging import log_utils as u
from opg_pipeline_builder.validation import validation_exceptions as vexc

DEFAULT_PIPELINE = "test_pipeline"
DEFAULT_RUN_ID = "scheduled__2026-09-22T00:00:00+00:00"
ENCODED_RUN_ID = "scheduled__2026-09-22T00%3A00%3A00%2B00%3A00"
DEFAULT_BUCKET = "log-bucket"
DEFAULT_LOG_STORE_PREFIX = "prefix/to/log"
DEFAULT_ATHENA_PREFIX = "prefix/to/log/curated/"
DEFAULT_ATHENA_DATABASE = "opg_test_logging"
DEFAULT_ATHENA_TABLE = "test_logging_curated"
DEFAULT_BATCH_SIZE = 5
DEFAULT_PROCESS = "Logging"
DEFAULT_TABLE = "table_a"
DEFAULT_FIELD = "field_a"
DEFAULT_PROCESS_STAGE: Literal["Processing"] = "Processing"
DEFAULT_LEVEL: Literal["INFO"] = "INFO"
DEFAULT_LINE_NUMBER = 42
DEFAULT_MESSAGE = "test log"

##################
# LOG TEST UTILS #
##################


class DeliveryPeriod:
    """Wrapper for the default delivery period to be used in tests."""

    _DATETIME = datetime(2024, 1, 2, tzinfo=UTC)
    _ENCODED = "2024-01-02T00%3A00%3A00.000000Z"
    ISO = "2024-01-02T00:00:00.000000Z"


class LogTimestamp:
    """Wrapper for the default log timestamp to be used in tests."""

    _TIMESTAMP = 1704363366.0
    ISO = "2024-01-04T10:16:06.000000Z"


def get_log_line_number(
    function: Any = log.JsonlLogHandler._write_configuration_log,
) -> int:
    """Return the source line of the internal record call inside the given handler method."""
    source, first_line = inspect.getsourcelines(function)
    return first_line + next(
        index for index, line in enumerate(source) if "self._internal_record(" in line
    )


def create_config(**overrides: Any) -> dict[str, Any]:
    """Return fresh configuration inputs shared by model, handler and integration tests."""
    return {
        "pipeline": DEFAULT_PIPELINE,
        "data_delivery_period": DeliveryPeriod._DATETIME,
        "attempt_no": 1,
        "run_id": DEFAULT_RUN_ID,
        "bucket": DEFAULT_BUCKET,
        "log_store_prefix": DEFAULT_LOG_STORE_PREFIX,
        "athena_prefix": DEFAULT_ATHENA_PREFIX,
        "athena_database_name": DEFAULT_ATHENA_DATABASE,
        "athena_table_name": DEFAULT_ATHENA_TABLE,
        "batch_size": DEFAULT_BATCH_SIZE,
        **overrides,
    }


def create_custom_fields(**overrides: Any) -> dict[str, Any]:
    """Return custom-field inputs with only scenario-specific values overridden."""
    return {
        "process": DEFAULT_PROCESS,
        "table": DEFAULT_TABLE,
        "field": DEFAULT_FIELD,
        "process_stage": DEFAULT_PROCESS_STAGE,
        **overrides,
    }


def create_log_record(
    **overrides: Any,
) -> logging.LogRecord:
    """Create a representative logging record."""
    return logging.makeLogRecord(
        {
            "name": "opg_pipeline_builder.test_module",
            "module": "test_module",
            "funcName": "test_func",
            "lineno": DEFAULT_LINE_NUMBER,
            "levelname": DEFAULT_LEVEL,
            "levelno": logging.getLevelNamesMapping()[DEFAULT_LEVEL],
            "msg": DEFAULT_MESSAGE,
            "args": (),
            "created": LogTimestamp._TIMESTAMP,
            "custom_fields": create_custom_fields(),
            **overrides,
        }
    )


def create_structured_log(
    config: dict[str, Any] | None = None,
    custom_fields: dict[str, Any] | None = None,
    log_record: logging.LogRecord | None = None,
    **overrides: Any,
) -> dict[str, Any]:
    """Create a representative structured log record as a dictionary."""
    config = create_config() if config is None else config
    custom_fields = create_custom_fields() if custom_fields is None else custom_fields
    log_record = log_record or create_log_record(custom_fields=custom_fields)

    return {
        "pipeline": config.get("pipeline"),
        "run_id": config.get("run_id"),
        "data_delivery_period": config.get("data_delivery_period"),
        "attempt_no": config.get("attempt_no"),
        "logger_name": log_record.name,
        "module": log_record.module,
        "function": log_record.funcName,
        "line_number": log_record.lineno,
        "log_level": log_record.levelname,
        "log_timestamp": log_record.created,
        "process": custom_fields.get("process"),
        "table": custom_fields.get("table"),
        "field": custom_fields.get("field"),
        "process_stage": custom_fields.get("process_stage"),
        "message": log_record.getMessage(),
        "exception": log_record.exc_text or "",
        **overrides,
    }


def create_handler(
    *, apply_patches: bool = True, **overrides: Any
) -> log.JsonlLogHandler:
    """Create a handler with a small test batch, optionally suppressing startup work."""
    config = log.LoggingConfig.model_validate(create_config(**overrides))
    with (
        (
            patch.object(log.JsonlLogHandler, "_validate_log_location")
            if apply_patches
            else nullcontext()
        ),
        (
            patch.object(log.JsonlLogHandler, "_write_configuration_log")
            if apply_patches
            else nullcontext()
        ),
    ):
        return log.JsonlLogHandler(logging_config=config)


def create_module_logger(**overrides: Any) -> log.ModuleLogger:
    """Create a module logger with only scenario-specific context overridden."""
    return log.ModuleLogger(
        **{
            "logger": logging.getLogger("opg_pipeline_builder.test"),
            "process": DEFAULT_PROCESS,
            **overrides,
        }
    )


@pytest.fixture(autouse=True)
def reset_root() -> Generator[None]:
    """Isolate test JSONL handlers while preserving host-owned root handlers."""
    root = logging.getLogger()
    original_handlers = list(root.handlers)
    original_level = root.level
    session_handlers = [
        handler
        for handler in original_handlers
        if handler.get_name() in {log._CONSOLE_HANDLER_NAME, log._JSONL_HANDLER_NAME}
    ]
    for handler in session_handlers:
        root.removeHandler(handler)

    yield

    for handler in list(root.handlers):
        if handler not in original_handlers:
            root.removeHandler(handler)
            logging.Handler.close(handler)
    for handler in session_handlers:
        if handler not in root.handlers:
            root.addHandler(handler)
    root.setLevel(original_level)


@pytest.fixture(autouse=True)
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
    root = logging.getLogger()
    root.addHandler(stand_in)
    try:
        yield
    finally:
        root.removeHandler(stand_in)


##############
# UNIT TESTS #
##############


class TestStructuredLogRecord:
    """Tests for the StructuredLogRecord model."""

    @pytest.mark.parametrize(
        ("period", "expected"),
        [
            (DeliveryPeriod._DATETIME, DeliveryPeriod.ISO),
            (
                datetime(2024, 1, 2, microsecond=999999, tzinfo=UTC),
                "2024-01-02T00:00:00.999999Z",
            ),
        ],
    )
    def test_data_delivery_period(
        self, period: datetime | str | None, expected: str | None
    ) -> None:
        """Preserve each aware delivery period and emit its expected ISO representation."""
        data = create_structured_log(data_delivery_period=period)
        record = log.StructuredLogRecord.model_validate(data)

        assert record.data_delivery_period == period
        assert isinstance(record.data_delivery_period, datetime)
        assert record.model_dump()["data_delivery_period"] == period
        assert record.model_dump(mode="json")["data_delivery_period"] == expected

    @pytest.mark.parametrize(
        ("timestamp", "expected"),
        [
            (LogTimestamp._TIMESTAMP, LogTimestamp.ISO),
            (1704363366.000001, "2024-01-04T10:16:06.000001Z"),
            (float("nan"), None),
            (float("inf"), None),
            (-float("inf"), None),
        ],
    )
    def test_log_timestamp(self, timestamp: float, expected: str | None) -> None:
        """Reject non-finite timestamps and serialize finite values as UTC strings."""
        data = create_structured_log(log_timestamp=timestamp)
        if expected is None:
            with pytest.raises(ValidationError, match="finite"):
                log.StructuredLogRecord.model_validate(data)
            return

        record = log.StructuredLogRecord.model_validate(data)

        assert record.log_timestamp == timestamp
        assert isinstance(record.log_timestamp, float)
        assert record.model_dump()["log_timestamp"] == timestamp
        assert record.model_dump(mode="json")["log_timestamp"] == expected

    def test_valid_record(self) -> None:
        """Accept a valid record and preserve its fields in Python and JSON output."""
        data = create_structured_log()
        record = log.StructuredLogRecord.model_validate(data)
        output = record.model_dump()
        assert output == data
        assert record.model_dump(mode="json") == {
            **data,
            "data_delivery_period": "2024-01-02T00:00:00.000000Z",
            "log_timestamp": LogTimestamp.ISO,
        }

    def test_is_frozen_and_forbids_extra(self) -> None:
        """Reject field assignment and unknown fields on structured records."""
        assert log.StructuredLogRecord.model_config["frozen"] is True
        assert log.StructuredLogRecord.model_config["extra"] == "forbid"


class TestCustomLogFields:
    """Tests for the CustomLogFields model."""

    def test_valid_fields(self) -> None:
        """Accept valid custom fields and preserve their supplied values."""
        values = create_custom_fields()
        fields = log.CustomLogFields.model_validate(values)
        assert fields.model_dump() == values

    def test_is_frozen_and_forbids_extra(self) -> None:
        """Reject field assignment and unknown fields on custom log fields."""
        assert log.CustomLogFields.model_config["frozen"] is True
        assert log.CustomLogFields.model_config["extra"] == "forbid"

    def test_validate_process_valid(self) -> None:
        """Accept a non-empty process name and preserve its trimmed value."""
        fields = log.CustomLogFields.model_validate(create_custom_fields())
        assert fields.process == "Logging"

    @pytest.mark.parametrize("invalid_process", ["", "   ", None])
    def test_validate_process_empty(self, invalid_process: Any) -> None:
        """Raise InvalidProcessError for empty, whitespace-only, or None process values."""
        with pytest.raises(
            exc.InvalidProcessError, match="field 'process' must be non-empty"
        ):
            log.CustomLogFields.model_validate(
                create_custom_fields(process=invalid_process)
            )

    @pytest.mark.parametrize("padded_process", [" Load", "Load ", "  Load\t"])
    def test_strips_process_whitespace(self, padded_process: str) -> None:
        """Strip surrounding whitespace before storing the process name."""
        fields = log.CustomLogFields.model_validate(
            create_custom_fields(process=padded_process)
        )
        assert fields.process == "Load"


class TestLoggingConfig:
    """Validate run-level inputs once, separately from individual log rows."""

    @pytest.mark.parametrize(
        ("period", "expected_period", "attempt_no", "run_id"),
        [
            (DeliveryPeriod._DATETIME, DeliveryPeriod.ISO, 1, DEFAULT_RUN_ID),
            (
                datetime(2024, 1, 2, microsecond=123456, tzinfo=UTC),
                "2024-01-02T00:00:00.123456Z",
                2,
                "manual/run 1",
            ),
            (
                datetime(2024, 1, 2, 5, tzinfo=timezone(timedelta(hours=5))),
                DeliveryPeriod.ISO,
                3,
                "manual=run+1",
            ),
        ],
    )
    def test_partitions(
        self, period: datetime, expected_period: str, attempt_no: int, run_id: str
    ) -> None:
        """Build ordered partitions with the UTC period and supplied run identifiers."""
        config = log.LoggingConfig.model_validate(
            create_config(
                data_delivery_period=period, attempt_no=attempt_no, run_id=run_id
            )
        )

        assert list(config.partitions.items()) == [
            ("data_delivery_period", expected_period),
            ("attempt_no", attempt_no),
            ("run_id", run_id),
        ]

    @pytest.mark.parametrize(
        ("period", "expected"),
        [
            (DeliveryPeriod._DATETIME, DeliveryPeriod.ISO),
            (
                datetime(2024, 1, 2, microsecond=999999, tzinfo=UTC),
                "2024-01-02T00:00:00.999999Z",
            ),
        ],
    )
    def test_data_delivery_period(
        self, period: datetime | str | None, expected: str | None
    ) -> None:
        """Preserve the aware delivery datetime and emit its expected ISO representation."""
        data = create_config(data_delivery_period=period)
        record = log.LoggingConfig.model_validate(data)

        assert record.data_delivery_period == period
        assert isinstance(record.data_delivery_period, datetime)
        assert record.model_dump()["data_delivery_period"] == period
        assert record.model_dump(mode="json")["data_delivery_period"] == expected

    def test_valid_config(self) -> None:
        """Accept valid logging configuration and preserve all supplied fields."""
        valid_config = create_config()
        config = log.LoggingConfig.model_validate(valid_config)
        assert config.model_dump() == valid_config

    def test_is_frozen_and_forbids_extra(self) -> None:
        """Reject field assignment and unknown fields on logging configuration."""
        assert log.LoggingConfig.model_config["frozen"] is True
        assert log.LoggingConfig.model_config["extra"] == "forbid"

    @pytest.mark.parametrize(
        ("log_store_prefix", "athena_prefix"),
        [
            ("prefix/to/logs", "prefix/to/logs/athena"),
            ("prefix/to/logs", "prefix/to/athena"),
        ],
    )
    def test_validate_s3_prefixes_valid(
        self, log_store_prefix: str, athena_prefix: str
    ) -> None:
        """Accept bucket-relative raw and curated prefixes that do not overlap."""
        valid_config = create_config(
            log_store_prefix=log_store_prefix, athena_prefix=athena_prefix
        )
        config = log.LoggingConfig.model_validate(valid_config)
        assert config.log_store_prefix == log_store_prefix
        assert config.athena_prefix == athena_prefix

    @pytest.mark.parametrize(
        ("log_store_prefix", "athena_prefix", "exp_error"),
        [
            (
                "",
                "name",
                "'log_store_prefix' must be non-empty and not start with '/'.",
            ),
            ("name", " ", "'athena_prefix' must be non-empty and not start with '/'."),
            (
                "/prefix/",
                "name",
                "'log_store_prefix' must be non-empty and not start with '/'.",
            ),
            (
                "s3://prefix/",
                "name",
                "'log_store_prefix' must not include 's3://' or the bucket.",
            ),
            (
                "name",
                "log-bucket/prefix",
                "'athena_prefix' must not include 's3://' or the bucket.",
            ),
            (
                "prefix/to/logs",
                "prefix/to/logs/",
                "'log_store_prefix' must not be the same as, or nested inside, the 'athena_prefix'.",
            ),
            (
                "prefix/to/athena/logs",
                "prefix/to/athena",
                "'log_store_prefix' must not be the same as, or nested inside, the 'athena_prefix'.",
            ),
            (
                "/invalid/prefix/to/logs",
                "s3://invalid/prefix/to/athena",
                "'log_store_prefix' must be non-empty and not start with '/'. 'athena_prefix' must not include 's3://' or the bucket.",
            ),
            (
                "/invalid/prefix/to/logs",
                "/invalid/prefix/to/logs",
                "'log_store_prefix' must be non-empty and not start with '/'. 'athena_prefix' must be non-empty and not start with '/'. 'log_store_prefix' must not be the same as, or nested inside, the 'athena_prefix'.",
            ),
        ],
    )
    def test_validate_s3_prefixes_invalid(
        self, log_store_prefix: str, athena_prefix: str, exp_error: str
    ) -> None:
        """Raise InvalidPrefixError for malformed or overlapping S3 prefixes."""
        with pytest.raises(exc.InvalidPrefixError, match=exp_error):
            log.LoggingConfig.model_validate(
                create_config(
                    log_store_prefix=log_store_prefix, athena_prefix=athena_prefix
                )
            )

    @pytest.mark.parametrize("field", ["athena_database_name", "athena_table_name"])
    @pytest.mark.parametrize(
        "value", ["name_of_object", "name", "has_1_number", "a" * 63]
    )
    def test_validate_athena_db_and_table_valid(self, field: str, value: str) -> None:
        """Accept valid lowercase database and table identifiers."""
        config = log.LoggingConfig.model_validate(create_config(**{field: value}))
        assert getattr(config, field) == value

    @pytest.mark.parametrize("field", ["athena_database_name", "athena_table_name"])
    @pytest.mark.parametrize(
        ("value", "exp_error"),
        [
            ("", "Identifier cannot be empty."),
            ("has space", "Identifier cannot contain whitespace characters."),
            ("Upper", "Identifier must be lowercase."),
            ("UPPER", "Identifier must be lowercase."),
            ("1number", "Identifier cannot start with a number or underscore."),
            ("_underscore", "Identifier cannot start with a number or underscore."),
            ("a-b", "Identifier contains invalid characters."),
            ("é", "Identifier contains invalid characters."),
            ("a" * 64, "Identifier must not exceed 63 characters."),
            (
                "_HAS SPACES-with-this-incorrect-character-é-and is way toooo long",
                "Identifier cannot contain whitespace characters. Identifier must be lowercase. Identifier cannot start with a number or underscore. Identifier contains invalid characters. Identifier must not exceed 63 characters.",
            ),
        ],
    )
    def test_validate_athena_db_and_table_invalid(
        self, field: str, value: str, exp_error: str
    ) -> None:
        """Raise InvalidIdentifierError for invalid database and table identifiers."""
        with pytest.raises(
            vexc.InvalidIdentifierError, match=f"'{field}': {exp_error}"
        ):
            log.LoggingConfig.model_validate(create_config(**{field: value}))

    @pytest.mark.parametrize("field", ["pipeline", "run_id"])
    def test_validate_pipeline_and_run_id_valid(self, field: str) -> None:
        """Accept a non-empty pipeline or run identifier and preserve its value."""
        test_log = log.LoggingConfig.model_validate(
            create_config(**{field: "valid_value"})
        )
        assert getattr(test_log, field) == "valid_value"

    @pytest.mark.parametrize("field", ["pipeline", "run_id"])
    @pytest.mark.parametrize("value", ["", " ", "\t"])
    def test_validate_pipeline_and_run_id_invalid(self, field: str, value: Any) -> None:
        """Raise InvalidRunIdentifierError for empty or whitespace-only identifiers."""
        with pytest.raises(
            exc.InvalidRunIdentifierError, match=f"'{field}' must be non-empty"
        ):
            log.LoggingConfig.model_validate(create_config(**{field: value}))

    @pytest.mark.parametrize(
        ("data_delivery_period", "expected"),
        [
            (
                datetime(2024, 1, 1, 12, 0, tzinfo=UTC),
                datetime(2024, 1, 1, 12, 0, tzinfo=UTC),
            ),
            (
                datetime(2024, 1, 1, 12, 0, tzinfo=timezone(timedelta(hours=5))),
                datetime(2024, 1, 1, 7, 0, tzinfo=UTC),
            ),
        ],
    )
    def test_validate_datetimes_valid(
        self, data_delivery_period: datetime, expected: datetime
    ) -> None:
        """Accept aware delivery periods and normalize each one to its expected UTC value."""
        test_log = log.LoggingConfig.model_validate(
            create_config(data_delivery_period=data_delivery_period)
        )
        assert test_log.data_delivery_period == expected
        assert test_log.data_delivery_period.tzinfo is UTC

    def test_validate_datetimes_invalid(self) -> None:
        """Raise InvalidDatetimeError when the delivery period is timezone-naive."""
        with pytest.raises(
            vexc.InvalidDatetimeError,
            match="'data_delivery_period' must be timezone-aware",
        ):
            log.LoggingConfig.model_validate(
                create_config(
                    data_delivery_period=datetime(2024, 1, 1, 12, 0),
                )
            )

    @pytest.mark.parametrize("field", ["attempt_no", "batch_size"])
    @pytest.mark.parametrize("value", [0, -1, 1, 99])
    def test_validate_attempt_and_batch(self, field: str, value: int) -> None:
        """Accept positive attempt and batch values and reject values below one."""
        expectation = (
            pytest.raises(ValueError, match=f"'{field}' must be >=1")
            if value < 1
            else nullcontext()
        )
        with expectation:
            config = log.LoggingConfig.model_validate(create_config(**{field: value}))
            assert getattr(config, field) == value

    @pytest.mark.parametrize("field", ["attempt_no", "batch_size"])
    @pytest.mark.parametrize("value", [True, False, 2.5, 10.0, "10", None])
    def test_attempt_and_batch_is_strict(self, field: str, value: Any) -> None:
        """Reject booleans, floats, strings, and None instead of coercing integers."""
        with pytest.raises(ValidationError) as error:
            log.LoggingConfig.model_validate(create_config(**{field: value}))

        assert "Input should be a valid integer" in str(error.value)
        assert field in str(error.value)


class TestJsonlLogHandler:
    """Test JSONL buffering, S3 uploads, registration and handler lifecycle."""

    def test_init(self, s3: boto3.client) -> None:  # pylint: disable=unused-argument
        """Initialize validated defaults, an empty buffer, and startup records."""
        with (
            patch.object(
                log.JsonlLogHandler, "_validate_log_location"
            ) as mock_location,
            patch.object(log.JsonlLogHandler, "_write_configuration_log") as mock_write,
        ):
            handler = create_handler(apply_patches=False)
            assert handler._config.model_dump() == create_config()
            assert handler._batch_size == DEFAULT_BATCH_SIZE
            assert handler._base_batch_size == DEFAULT_BATCH_SIZE
            assert handler._part_number == 0
            assert handler._writer_id
            assert handler._buffer == []
            assert handler._write_failures == 0
            assert not handler._unmanaged_flush_warned
            mock_location.assert_called_once()
            mock_write.assert_called_once()
            logging.Handler.close(handler)

    def test_init_configures_s3_retries(self) -> None:
        """Create the S3 client with standard retry mode and five attempts."""
        with patch("opg_pipeline_builder.logging.log.boto3.client") as mock_client:
            handler = create_handler()

        mock_client.assert_called_once_with("s3", config=log._S3_RETRY_CONFIG)
        logging.Handler.close(handler)

    def test_validate_log_location_valid(
        self,
        s3: boto3.client,  # pylint: disable=unused-argument
    ) -> None:
        """Write one marker object and reuse its key across handler initializations."""
        path = f"s3://{DEFAULT_BUCKET}/{DEFAULT_LOG_STORE_PREFIX}/"
        create_handler(apply_patches=False)

        assert wr.s3.list_objects(path) == [f"{path}logging-marker/marker.txt"]

        create_handler(
            pipeline="other-pipeline",
            attempt_no=2,
            run_id="other-run",
            apply_patches=False,
        )

        assert wr.s3.list_objects(path) == [f"{path}logging-marker/marker.txt"]

    @pytest.mark.parametrize(
        ("exception", "exp_exc_msg"),
        [
            (BotoCoreError, "An unspecified error occurred"),
            (
                ClientError(
                    {"Error": {"Code": "AccessDenied", "Message": "denied"}},
                    "PutObject",
                ),
                "An error occurred (AccessDenied) when calling the PutObject operation: denied",
            ),
            (OSError, ""),
        ],
    )
    def test_validate_log_location_invalid(
        self, exception: Any, exp_exc_msg: str, s3: boto3.client
    ) -> None:
        """Wrap marker-write failures in RuntimeError with the failing S3 location."""
        with (
            patch.object(s3, "put_object", side_effect=exception),
            patch(
                "opg_pipeline_builder.logging.log.boto3.client",
                return_value=s3,
            ),
            pytest.raises(
                RuntimeError,
                match=re.escape(
                    f"Failed to write marker object to s3://{DEFAULT_BUCKET}/{DEFAULT_LOG_STORE_PREFIX}/logging-marker/marker.txt: {exp_exc_msg}"
                ),
            ),
        ):
            create_handler(apply_patches=False)

    def test_write_configuration_log(self) -> None:
        """Append one configuration-success record to the handler buffer."""
        handler = create_handler()
        record = log.StructuredLogRecord(**create_structured_log())

        with patch.object(
            handler, "_internal_record", return_value=record
        ) as mock_record:
            handler._write_configuration_log()

        mock_record.assert_called_once_with(
            log_level="INFO",
            process="Configuration",
            process_stage="End",
            message="Logging configured successfully.",
        )
        assert handler._buffer == [record.model_dump(mode="json")]

    def test_locked(self) -> None:
        """Hold the handler lock inside the context and release it on exit."""
        handler = create_handler()

        def can_acquire_lock() -> bool:
            """Return whether another thread can acquire the lock immediately."""
            assert handler.lock is not None
            acquired = handler.lock.acquire(blocking=False)
            if acquired:
                handler.lock.release()
            return acquired

        with (
            ThreadPoolExecutor(max_workers=1) as executor,
            patch.object(handler, "acquire", wraps=handler.acquire) as mock_acquire,
            patch.object(handler, "release", wraps=handler.release) as mock_release,
        ):
            with handler._locked():
                mock_acquire.assert_called_once_with()
                mock_release.assert_not_called()
                assert not executor.submit(can_acquire_lock).result()

            mock_release.assert_called_once_with()
            assert executor.submit(can_acquire_lock).result()

    def test_build_row(self) -> None:
        """Build a structured row and truncate its message and exception fields."""
        handler = create_handler()
        structured_log = create_structured_log()
        fields = {
            name: value
            for name, value in structured_log.items()
            if name not in {"pipeline", "run_id", "data_delivery_period", "attempt_no"}
        }

        with patch(
            "opg_pipeline_builder.logging.log.u.truncate_text",
            wraps=u.truncate_text,
        ) as mock_truncate:
            row = handler._build_row(**fields)

        assert row == log.StructuredLogRecord(**structured_log)
        assert mock_truncate.call_count == 2
        assert mock_truncate.call_args_list[0][0][0] == fields["message"]
        assert mock_truncate.call_args_list[1][0][0] == fields["exception"]

    @pytest.mark.parametrize("exception", ["", "internal exception"])
    def test_internal_record(self, exception: str) -> None:
        """Build an internal record with caller metadata and supplied exception text."""
        handler = create_handler()

        with patch.object(time, "time", return_value=LogTimestamp._TIMESTAMP):
            line_number = sys._getframe().f_lineno + 1
            row = handler._internal_record(
                log_level=DEFAULT_LEVEL,
                process=DEFAULT_PROCESS,
                process_stage=DEFAULT_PROCESS_STAGE,
                message=DEFAULT_MESSAGE,
                **({"exception": exception} if exception else {}),
            )

        expected = create_structured_log(
            custom_fields=create_custom_fields(table="", field=""),
            logger_name=logging.getLogger().name,
            module="logging.log",
            function="test_internal_record",
            line_number=line_number,
            exception=exception,
        )

        assert isinstance(row, log.StructuredLogRecord)
        assert row.model_dump() == expected

    @pytest.mark.parametrize(
        "exception_case",
        [
            "no_exception",
            "empty_exc_info",
            "stale_text_without_exc_info",
            "preformatted_exception",
            "unformatted_exception",
        ],
    )
    def test_record_to_row_passes_expected_fields(self, exception_case: str) -> None:
        """Pass formatted messages and traceback text into the validated structured row."""
        handler = create_handler()
        record = create_log_record(msg="application event: %s", args=(1,))
        custom_fields = log.CustomLogFields.model_validate(
            vars(record)["custom_fields"]
        )
        expected_exception = ""

        if exception_case in {"preformatted_exception", "unformatted_exception"}:
            try:
                try:
                    err = "missing_column"
                    raise KeyError(err)
                except KeyError as cause:
                    error = ValueError("bad row")
                    error.add_note("while loading table_a")
                    raise error from cause
            except ValueError:
                record.exc_info = sys.exc_info()

            expected_exception = logging.Formatter().formatException(record.exc_info)
            if exception_case == "preformatted_exception":
                record.exc_text = expected_exception
            else:
                assert "KeyError: 'missing_column'" in expected_exception
                assert "The above exception was the direct cause" in expected_exception
                assert "ValueError: bad row" in expected_exception
                assert expected_exception.rstrip().endswith("while loading table_a")
        elif exception_case == "empty_exc_info":
            record.exc_info = (None, None, None)
        elif exception_case == "stale_text_without_exc_info":
            record.exc_text = "stale traceback text"

        with patch.object(handler, "_build_row") as mock_build:
            handler._record_to_row(record, custom_fields)

        mock_build.assert_called_once_with(
            logger_name=record.name,
            module=record.module,
            function=record.funcName,
            line_number=record.lineno,
            log_level=record.levelname,
            log_timestamp=record.created,
            **custom_fields.model_dump(),
            message="application event: 1",
            exception=expected_exception,
        )
        if exception_case in {"preformatted_exception", "unformatted_exception"}:
            assert record.exc_text == expected_exception

    @pytest.mark.parametrize("prefix", ["raw/logs", "raw/logs/"])
    def test_create_key_directory(self, prefix: str) -> None:
        """Trim the prefix and percent-encode each partition as one path segment."""
        handler = create_handler()
        directory = handler._create_key_directory(prefix)
        assert directory == (
            f"raw/logs/data_delivery_period={DeliveryPeriod._ENCODED}/"
            f"attempt_no=1/run_id={ENCODED_RUN_ID}/"
        )

    def test_object_key(self) -> None:
        """Include process, writer, and part identifiers in each JSONL object key."""
        handler = create_handler()
        part_number = 1
        with patch.object(
            log.JsonlLogHandler,
            "_create_key_directory",
            return_value="mocked/directory/",
        ):
            key = handler._object_key(part_number)
        assert (
            key == "mocked/directory/"
            f"{os.getpid()}_{handler._writer_id}_{part_number}.jsonl"
        )
        assert key.endswith(f"{os.getpid()}_{handler._writer_id}_{part_number}.jsonl")

    @pytest.mark.parametrize("error", [None, OSError("upload failed")])
    def test_put_object(self, error: Exception | None) -> None:
        """Forward exact S3 upload arguments and propagate a supplied upload failure."""
        handler = create_handler()
        with (
            pytest.raises(type(error)) if error else nullcontext(),
            patch.object(handler._s3, "put_object", side_effect=error) as mock_put,
        ):
            handler._put_object("logs/part.jsonl", b'{"message":"event"}\n')

        mock_put.assert_called_once_with(
            Bucket=DEFAULT_BUCKET,
            Key="logs/part.jsonl",
            Body=b'{"message":"event"}\n',
            ContentType="application/x-ndjson",
        )

    @pytest.mark.parametrize("row_count", [1, 3])
    def test_flush_locked_success(
        self,
        row_count: int,
        capsys: pytest.CaptureFixture[str],  # pylint: disable=unused-argument
    ) -> None:
        """Serialize rows in order and reset retry state after successful upload."""
        handler = create_handler()
        rows = [
            create_structured_log(
                message=f"event {index}",
                log_timestamp=f"original event time {index}",
                data_delivery_period=DeliveryPeriod.ISO,
            )
            for index in range(row_count)
        ]
        handler._buffer = list(rows)
        handler._write_failures = 1
        handler._batch_size = DEFAULT_BATCH_SIZE * 2

        with (
            patch.object(handler, "_object_key", return_value="part.jsonl") as mock_key,
            patch.object(handler, "_put_object") as mock_put,
            patch.object(handler, "_internal_record") as mock_internal,
            patch.object(u, "to_jsonl_line", wraps=u.to_jsonl_line) as serialize,
        ):
            handler._flush_locked()

        assert serialize.call_args_list == [call(row) for row in rows]
        mock_key.assert_called_once_with(0)

        uploaded_key, body = mock_put.call_args.args
        assert uploaded_key == "part.jsonl"
        assert isinstance(body, bytes)
        for index in range(row_count):
            assert f"event {index}".encode() in body
        assert body.endswith(b"\n")

        mock_internal.assert_not_called()
        assert handler._part_number == 1
        assert handler._buffer == []
        assert handler._write_failures == 0
        assert handler._batch_size == DEFAULT_BATCH_SIZE

    def test_flush_locked_empty_buffer(self) -> None:
        """Skip serialization, key creation, and upload when the buffer is empty."""
        handler = create_handler()
        with (
            patch.object(handler, "_object_key", return_value="part.jsonl") as mock_key,
            patch.object(handler, "_put_object") as mock_put,
            patch.object(u, "to_jsonl_line", wraps=u.to_jsonl_line) as mock_serialize,
        ):
            handler._flush_locked()

        assert handler._buffer == []
        assert handler._part_number == 0
        mock_key.assert_not_called()
        mock_put.assert_not_called()
        mock_serialize.assert_not_called()

    @pytest.mark.parametrize(
        "error",
        [
            BotoCoreError(),
            ClientError(
                {"Error": {"Code": "AccessDenied", "Message": "denied"}}, "PutObject"
            ),
            OSError(),
        ],
    )
    def test_flush_locked_sink_failure(
        self,
        error: Exception,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Retain rows, grow the threshold, and buffer a record after retryable sink failure."""
        handler = create_handler()

        initial_log = {"message": "original", "log_timestamp": LogTimestamp.ISO}
        handler._buffer = [initial_log]
        handler._part_number = 2
        handler._write_failures = 1
        handler._batch_size = DEFAULT_BATCH_SIZE * 2
        failure = {"message": "failure record"}

        with (
            patch.object(u, "to_jsonl_line", wraps=u.to_jsonl_line) as mock_serialize,
            patch.object(handler, "_object_key", return_value="part.jsonl") as key,
            patch.object(handler, "_put_object", side_effect=error) as put_object,
            patch.object(handler, "_internal_record") as internal_record,
        ):
            internal_record.return_value.model_dump.return_value = failure
            handler._flush_locked()

        mock_serialize.assert_called_once_with(initial_log)
        key.assert_called_once_with(2)
        put_object.assert_called_once_with(
            "part.jsonl", (u.to_jsonl_line(initial_log) + "\n").encode()
        )
        assert handler._write_failures == 2
        assert handler._batch_size == DEFAULT_BATCH_SIZE * 3
        assert handler._part_number == 2
        assert handler._buffer[0] is initial_log
        assert capsys.readouterr().err == (
            f"Failed to write a batch of logs to s3://{DEFAULT_BUCKET}/part.jsonl "
            f"(failure_count=2). Error: {error}\n"
        )
        internal_record.assert_called_once_with(
            log_level="ERROR",
            process="Logging",
            process_stage="Processing",
            message="Failed to write JSONL log batch (failure_count=2).",
            exception="".join(traceback.format_exception(error)),
        )
        internal_record.return_value.model_dump.assert_called_once_with(mode="json")
        assert handler._buffer == [initial_log, failure]

    def test_flush_locked_final_retry(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Raise after the fourth sink failure and retain rows without a retry record."""
        handler = create_handler()
        initial_log = {"message": "original"}
        handler._buffer = [initial_log]
        handler._part_number = 2
        handler._write_failures = 3
        handler._batch_size = DEFAULT_BATCH_SIZE * 4
        error = OSError("upload failed")

        with (
            patch.object(handler, "_object_key", return_value="part.jsonl") as key,
            patch.object(handler, "_put_object", side_effect=error) as put_object,
            patch.object(handler, "_internal_record") as internal_record,
            pytest.raises(RuntimeError, match="Retries exceeded") as raised,
        ):
            handler._flush_locked()

        key.assert_called_once_with(2)
        put_object.assert_called_once()
        assert raised.value.__cause__ is error
        assert str(raised.value) == "Attempted to write logs 4 times. Retries exceeded"
        internal_record.assert_not_called()
        assert handler._buffer == [initial_log]
        assert handler._buffer[0] is initial_log
        assert handler._part_number == 2
        assert handler._write_failures == 4
        assert handler._batch_size == DEFAULT_BATCH_SIZE * 5
        assert capsys.readouterr().err == (
            f"Failed to write a batch of logs to s3://{DEFAULT_BUCKET}/part.jsonl "
            f"(failure_count=4). Error: {error}\n"
        )

    @pytest.mark.parametrize(
        "dependency", ["to_jsonl_line", "_object_key", "_put_object"]
    )
    @pytest.mark.parametrize("error_type", [ValueError, TypeError, RuntimeError])
    def test_flush_locked_unexpected_failure(
        self,
        dependency: str,
        error_type: type[Exception],
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Propagate unexpected serialization or upload errors without changing buffer state."""
        handler = create_handler()
        initial_log = {"message": "original"}
        handler._buffer = [initial_log]
        error = error_type("bug")
        target = u if dependency == "to_jsonl_line" else handler
        with (
            patch.object(handler, "_put_object") as put_object,
            patch.object(handler, "_internal_record") as internal_record,
            patch.object(target, dependency, side_effect=error),
            pytest.raises(error_type) as raised,
        ):
            handler._flush_locked()

        assert raised.value is error
        internal_record.assert_not_called()
        if dependency != "_put_object":
            put_object.assert_not_called()
        assert handler._buffer == [initial_log]
        assert handler._part_number == 0
        assert handler._batch_size == DEFAULT_BATCH_SIZE
        assert handler._write_failures == 0
        assert capsys.readouterr().err == ""

    def test_register_logs_in_athena(self) -> None:
        """Read owned JSONL objects and append typed rows to the partitioned dataset."""
        handler = create_handler()
        handler._part_number = 1

        timestamps = [
            "2024-01-04T10:16:06Z",
            "2024-01-04T10:16:06.123456Z",
            "2024-01-04T12:16:06.000001+02:00",
        ]

        rows = [
            create_structured_log(
                message=f"event {index}",
                log_timestamp=timestamp,
                data_delivery_period=DeliveryPeriod.ISO,
            )
            for index, timestamp in enumerate(timestamps)
        ]

        raw_logs = pd.DataFrame(rows)

        expected = raw_logs.copy(deep=True)
        expected["log_timestamp"] = pd.to_datetime(
            timestamps, utc=True, format="ISO8601"
        )

        with (
            patch.object(
                handler, "_object_key", side_effect=lambda part: f"part-{part}.jsonl"
            ) as object_key,
            patch.object(wr.s3, "read_json", return_value=raw_logs) as read_json,
            patch.object(wr.catalog, "create_database") as create_database,
            patch.object(wr.s3, "to_parquet") as to_parquet,
        ):
            handler._register_logs_in_athena()

        assert handler._part_number == 1
        assert object_key.call_args_list == [call(part) for part in range(1)]
        read_json.assert_called_once_with(
            path=[f"s3://{DEFAULT_BUCKET}/part-{part}.jsonl" for part in range(1)],
            lines=True,
            dtype=False,
            convert_dates=False,
        )
        create_database.assert_called_once_with(DEFAULT_ATHENA_DATABASE, exist_ok=True)

        actual_logs = to_parquet.call_args.kwargs["df"]
        pd.testing.assert_frame_equal(actual_logs, expected)

        remaining_kwargs = to_parquet.call_args.kwargs
        remaining_kwargs = {
            key: value for key, value in remaining_kwargs.items() if key != "df"
        }
        assert remaining_kwargs == {
            "path": f"s3://{DEFAULT_BUCKET}/{DEFAULT_ATHENA_PREFIX}",
            "dataset": True,
            "mode": "append",
            "database": DEFAULT_ATHENA_DATABASE,
            "table": DEFAULT_ATHENA_TABLE,
            "partition_cols": ["data_delivery_period", "attempt_no", "run_id"],
            "dtype": u.ATHENA_COLUMN_TYPES,
        }

    @pytest.mark.parametrize(("part_no", "read_count"), [(0, 0), (1, 1)])
    def test_register_logs_in_athena_empty(self, part_no: int, read_count: int) -> None:
        """Skip Glue and Parquet writes when there are no uploaded log rows."""
        handler = create_handler()
        handler._part_number = part_no

        with (
            patch.object(
                handler, "_object_key", side_effect=lambda part: f"part-{part}.jsonl"
            ) as object_key,
            patch.object(wr.s3, "read_json", return_value=pd.DataFrame()) as read_json,
            patch.object(wr.catalog, "create_database") as create_database,
            patch.object(wr.s3, "to_parquet") as to_parquet,
        ):
            handler._register_logs_in_athena()

        assert object_key.call_args_list == [call(part) for part in range(part_no)]
        assert read_json.call_count == read_count
        create_database.assert_not_called()
        to_parquet.assert_not_called()

    @pytest.mark.parametrize(
        "failure_stage", ["read_json", "create_database", "to_parquet"]
    )
    def test_register_logs_in_athena_failure(self, failure_stage: str) -> None:
        """Wrap read, database-create, and Parquet-write failures in RuntimeError."""
        handler = create_handler()
        handler._part_number = 1
        error = OSError("registration failed")
        with (
            patch.object(handler, "_object_key", return_value="part.jsonl"),
            patch.object(
                wr.s3,
                "read_json",
                return_value=pd.DataFrame({"log_timestamp": [LogTimestamp.ISO]}),
            ) as read_json,
            patch.object(wr.catalog, "create_database") as create_database,
            patch.object(wr.s3, "to_parquet") as to_parquet,
        ):
            dependencies = {
                "read_json": read_json,
                "create_database": create_database,
                "to_parquet": to_parquet,
            }
            dependencies[failure_stage].side_effect = error
            with pytest.raises(
                RuntimeError, match="Failed to register logs in Athena"
            ) as raised:
                handler._register_logs_in_athena()
        assert raised.value.__cause__ is error
        read_json.assert_called_once()
        assert create_database.call_count == int(failure_stage != "read_json")
        assert to_parquet.call_count == int(failure_stage == "to_parquet")
        assert handler._part_number == 1

    def test_dump_unwritten_records(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Write unwritten rows to stderr and leave the handler buffer unchanged."""
        handler = create_handler()
        rows = [
            {"message": f"event {index}", "log_timestamp": LogTimestamp.ISO}
            for index in range(3)
        ]
        handler._buffer = rows
        with (
            patch.object(u, "to_jsonl_line", wraps=u.to_jsonl_line) as jsonl,
            patch.object(sys.stderr, "flush", wraps=sys.stderr.flush) as flush,
        ):
            handler._dump_unwritten_records()
        assert capsys.readouterr().err == (
            f"ERROR: 3 log records could not be written to "
            f"s3://{DEFAULT_BUCKET}/{DEFAULT_LOG_STORE_PREFIX}. Dumping records:\n"
            + "".join(
                f'{{"message":"event {index}","log_timestamp":"{LogTimestamp.ISO}"}}\n'
                for index in range(3)
            )
        )
        assert jsonl.call_args_list == [call(row) for row in rows]
        flush.assert_called_once_with()
        assert handler._buffer == rows

    def test_emit_buffers_and_flushes(self) -> None:
        """Buffer rows in order and trigger one flush when the threshold is reached."""
        handler = create_handler(batch_size=3)

        fields = log.CustomLogFields.model_validate(create_custom_fields())
        records = [
            create_log_record(msg=f"event {index}", custom_fields=fields)
            for index in range(3)
        ]

        with (
            patch.object(
                handler,
                "_record_to_row",
                side_effect=[{"A": "0"}, {"B": "1"}, {"C": "2"}],
            ) as row,
            patch.object(handler, "_locked", wraps=handler._locked) as locked,
            patch.object(handler, "_flush_locked") as flush,
        ):
            for record in records:
                handler.emit(record)

        assert row.call_count == 3
        assert row.call_args_list == [call(records[i], fields) for i in range(3)]
        assert locked.call_count == 3
        flush.assert_called_once()
        assert handler._buffer == [{"A": "0"}, {"B": "1"}, {"C": "2"}]

    @pytest.mark.parametrize(
        ("custom_fields", "exception"),
        [
            ("missing", nullcontext()),
            (None, nullcontext()),
            ({}, nullcontext()),
            (False, nullcontext()),
            ({"A": "B"}, pytest.raises(TypeError, match="instance of CustomLogFields")),
            (1, pytest.raises(TypeError, match="instance of CustomLogFields")),
        ],
    )
    def test_emit_invalid_custom_fields(
        self,
        custom_fields: Any,
        exception: Any,
    ) -> None:
        """Skip missing context and reject invalid supplied values before buffering."""
        handler = create_handler()
        record = create_log_record()

        if custom_fields == "missing":
            del vars(record)["custom_fields"]
        else:
            vars(record)["custom_fields"] = custom_fields

        with (
            exception,
            patch.object(handler, "_record_to_row", return_value={}) as row,
            patch.object(handler, "_locked", wraps=handler._locked) as locked,
            patch.object(handler, "_flush_locked") as flush,
        ):
            handler.emit(record)

        row.assert_not_called()
        locked.assert_not_called()
        flush.assert_not_called()

    def test_emit_invalid_closed(
        self,
    ) -> None:
        """Raise RuntimeError without writing when the handler is already closed."""
        handler = create_handler()
        handler._closed = True
        record = create_log_record()

        with pytest.raises(RuntimeError, match="already been closed"):
            handler.emit(record)

    @pytest.mark.parametrize("closed", [False, True])
    def test_close(self, closed: bool) -> None:
        """Close open handlers once and leave already closed handlers untouched."""
        handler = create_handler()
        handler._closed = closed

        with (
            patch.object(handler, "_locked") as locked,
            patch.object(handler, "_flush_locked") as flush,
            patch.object(handler, "_register_logs_in_athena") as register,
            patch.object(logging.Handler, "close", autospec=True) as base_close,
        ):
            handler.close()

        locked.assert_called_once()
        if closed:
            flush.assert_not_called()
            register.assert_not_called()
            base_close.assert_not_called()
        else:
            flush.assert_called_once()
            register.assert_called_once()
            base_close.assert_called_once_with(handler)

    def test_close_buffer_remains(self) -> None:
        """Dump remaining rows and raise without registration when the final flush fails."""
        handler = create_handler()
        handler._buffer = [{"A": "B"}]
        assert handler._buffer == [{"A": "B"}]

        with (
            patch.object(handler, "_locked") as locked,
            patch.object(handler, "_flush_locked") as flush,
            patch.object(handler, "_dump_unwritten_records") as dump,
            patch.object(handler, "_register_logs_in_athena") as register,
            patch.object(logging.Handler, "close", autospec=True) as base_close,
            pytest.raises(
                RuntimeError, match=re.escape("Failed to write 1 log records to S3.")
            ),
        ):
            handler.close()

        locked.assert_called_once()
        flush.assert_called_once()
        dump.assert_called_once()
        register.assert_not_called()
        base_close.assert_called_once_with(handler)

    def test_concurrent_close(self) -> None:
        """Serialize concurrent closes so upload and registration each happen once."""
        handler = create_handler()
        waiting = Event()
        acquire_lock = handler.acquire
        main_thread = current_thread()

        def acquire() -> None:
            """Signal the worker and acquire the handler lock."""
            if current_thread() is not main_thread:
                waiting.set()
            acquire_lock()

        with (
            patch.object(handler, "acquire", side_effect=acquire),
            patch.object(handler, "_flush_locked") as flush,
            patch.object(handler, "_register_logs_in_athena") as register,
            ThreadPoolExecutor(max_workers=1) as executor,
        ):
            acquire_lock()
            try:
                pending = executor.submit(handler.close)
                assert waiting.wait(timeout=5)
                handler.close()
            finally:
                handler.release()
            pending.result(timeout=5)
        flush.assert_called_once_with()
        register.assert_called_once_with()
        assert handler._closed

    def test_flush_runs(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Warn and buffer once on unmanaged flush without uploading records."""
        handler = create_handler()

        warning = {"message": log._UNMANAGED_FLUSH_WARNING}

        with (
            patch.object(handler, "_locked", wraps=handler._locked) as locked,
            patch.object(handler, "_internal_record") as internal_record,
            patch.object(handler, "_flush_locked") as upload,
        ):
            internal_record.return_value.model_dump.return_value = warning
            handler.flush()

        locked.assert_called_once()
        assert handler._unmanaged_flush_warned is True
        assert capsys.readouterr().err == log._UNMANAGED_FLUSH_WARNING + "\n"
        assert handler._buffer == [warning]
        upload.assert_not_called()
        internal_record.assert_called_once_with(
            log_level="WARNING",
            process="Logging",
            process_stage="End",
            message=log._UNMANAGED_FLUSH_WARNING,
        )

    @pytest.mark.parametrize(
        ("closed", "warned"),
        [(True, True), (True, False), (False, True)],
    )
    def test_flush_closed_repeated(
        self,
        closed: bool,
        warned: bool,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Skip warning and buffering for a closed or previously warned handler."""
        handler = create_handler()
        handler._closed = closed
        handler._unmanaged_flush_warned = warned

        with (
            patch.object(handler, "_locked", wraps=handler._locked) as locked,
            patch.object(handler, "_internal_record") as internal_record,
            patch.object(handler, "_flush_locked") as upload,
        ):
            handler.flush()

        locked.assert_called_once()
        assert not capsys.readouterr().err
        assert handler._buffer == []
        upload.assert_not_called()
        internal_record.assert_not_called()


class TestLoggingController:
    """Test controller delegation, ownership and deterministic shutdown."""

    @pytest.fixture
    def controller(self) -> log.LoggingController:
        """Attach owned handlers and leave host handlers outside the controller."""
        root = logging.getLogger()
        handlers = [create_handler(), logging.StreamHandler()]
        for handler in handlers:
            root.addHandler(handler)
        return log.LoggingController(root, handlers)

    def test_init(self, controller: log.LoggingController) -> None:
        """Retain the logger and copy the owned handler sequence with an open initial state."""
        assert controller._logger is logging.getLogger()
        assert [h.__class__ for h in controller._handlers] == [
            log.JsonlLogHandler,
            logging.StreamHandler,
        ]
        assert not controller._is_shutdown

    def test_enter(self, controller: log.LoggingController) -> None:
        """Entering returns the same controller without flushing or shutting it down."""
        with controller:
            assert not controller._is_shutdown

    def test_exit_success(self, controller: log.LoggingController) -> None:
        """Exiting without errors calls shutdown and does not raise."""
        with patch.object(controller, "shutdown") as shutdown:
            controller.__exit__(None, None, None)
        shutdown.assert_called_once_with()

    @pytest.mark.parametrize("body_fails", [False, True])
    @pytest.mark.parametrize("shutdown_fails", [False, True])
    def test_exit(
        self,
        controller: log.LoggingController,
        body_fails: bool,
        shutdown_fails: bool,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Preserve a body exception and annotate it if shutdown also fails."""
        body_error = ValueError("body failed") if body_fails else None
        shutdown_err = RuntimeError("shutdown failed") if shutdown_fails else None
        primary_err = body_error or shutdown_err
        expectation = pytest.raises(type(primary_err)) if primary_err else nullcontext()

        with (
            expectation as raised,
            patch.object(controller, "shutdown", side_effect=shutdown_err) as shutdown,
            controller as _,
        ):
            if body_error:
                raise body_error

        shutdown.assert_called_once_with()
        if primary_err and raised:
            assert raised.value is primary_err
            assert getattr(primary_err, "__notes__", []) == (
                ["Logging shutdown also failed: RuntimeError: shutdown failed"]
                if body_fails and shutdown_fails
                else []
            )
        stderr = capsys.readouterr().err
        if body_fails and shutdown_fails:
            assert stderr.startswith("Traceback (most recent call last):")
            assert stderr.rstrip().endswith("RuntimeError: shutdown failed")
        else:
            assert stderr == ""

    def test_flush_success(
        self,
        controller: log.LoggingController,
    ) -> None:
        """Flush each owned JSONL handler once and leave console handlers untouched."""
        jsonl = controller._handlers[0]
        console = controller._handlers[1]
        assert isinstance(jsonl, log.JsonlLogHandler)

        with (
            patch.object(jsonl, "_locked", wraps=jsonl._locked) as locked,
            patch.object(jsonl, "_flush_locked") as flush,
            patch.object(console, "flush") as console_flush,
            patch.object(console, "close") as console_close,
        ):
            controller.flush()

        assert locked.call_args_list == [call()]
        assert flush.call_args_list == [call()]
        console_flush.assert_not_called()
        console_close.assert_not_called()

    def test_flush_failure(
        self,
        controller: log.LoggingController,
    ) -> None:
        """Wrap the first JSONL flush failure and skip remaining and console handlers."""
        jsonl = controller._handlers[0]
        console = controller._handlers[1]
        assert isinstance(jsonl, log.JsonlLogHandler)
        error = RuntimeError("flush failed")

        with (
            patch.object(jsonl, "_locked", wraps=jsonl._locked) as locked,
            patch.object(jsonl, "_flush_locked", side_effect=error) as flush,
            patch.object(console, "flush") as console_flush,
            patch.object(console, "close") as console_close,
            pytest.raises(RuntimeError, match="Failed to flush handler") as raised,
        ):
            controller.flush()

        assert locked.call_args_list == [call()]
        assert flush.call_args_list == [call()]
        console_flush.assert_not_called()
        console_close.assert_not_called()
        assert "Failed to flush handler" in str(raised.value)
        assert raised.value.__cause__ is error

    def test_flush_is_shutdown(self, controller: log.LoggingController) -> None:
        """Raise RuntimeError if flush is called after shutdown."""
        controller._is_shutdown = True
        with (
            pytest.raises(
                RuntimeError, match=re.escape("Logging has already been shut down.")
            ),
            patch.object(controller._handlers[0], "_locked") as locked,
        ):
            controller.flush()

        locked.assert_not_called()

    def test_shutdown_success(
        self,
        controller: log.LoggingController,
    ) -> None:
        """Close and detach each owned handler once while preserving host handlers."""
        jsonl, console = controller._handlers

        # Add a host handler to the root logger to test that it is preserved during shutdown.
        root = controller._logger
        host = logging.NullHandler()
        root.addHandler(host)
        host_handlers = [
            handler for handler in root.handlers if handler not in controller._handlers
        ]

        with (
            patch.object(host, "close") as host_close,
            patch.object(jsonl, "close") as jsonl_close,
            patch.object(console, "close") as console_close,
            patch.object(root, "removeHandler", wraps=root.removeHandler) as remove,
        ):
            controller.shutdown()

        jsonl_close.assert_called_once()
        console_close.assert_called_once()
        host_close.assert_not_called()
        remove.assert_has_calls([call(jsonl), call(console)])
        assert controller._is_shutdown
        assert root.handlers == host_handlers

    def test_shutdown_failure(
        self,
        controller: log.LoggingController,
    ) -> None:
        """Close and detach all owned handlers, then raise the first error with later failures noted."""
        jsonl, console = controller._handlers

        # Add a host handler to the root logger to test that it is preserved during shutdown.
        root = controller._logger
        host = logging.NullHandler()
        root.addHandler(host)
        hosts = [
            handler for handler in root.handlers if handler not in controller._handlers
        ]

        errors = [RuntimeError("JSONL failed"), ValueError("console failed")]

        with (
            patch.object(host, "close") as host_close,
            patch.object(jsonl, "close", side_effect=errors[0]) as jsonl_close,
            patch.object(console, "close", side_effect=errors[1]) as console_close,
            patch.object(root, "removeHandler", wraps=root.removeHandler) as remove,
            pytest.raises(type(errors[0])) as raised,
        ):
            controller.shutdown()

        jsonl_close.assert_called_once()
        console_close.assert_called_once()
        host_close.assert_not_called()
        remove.assert_has_calls([call(jsonl), call(console)])
        assert controller._is_shutdown
        assert root.handlers == (hosts)
        assert raised.value is errors[0]
        assert getattr(raised.value, "__notes__", []) == [
            "Another logging handler also failed to close: ValueError: console failed"
        ]

    def test_shutdown_is_shutdown(self, controller: log.LoggingController) -> None:
        """Skip handler closes when the controller has already been shut down."""
        controller._is_shutdown = True
        with patch.object(controller._handlers[0], "close") as close:
            controller.shutdown()
        assert controller._is_shutdown
        close.assert_not_called()


class TestConfigureLogging:
    """Test configuration wiring and fail-fast behavior independently of handler methods."""

    def test_configure_logging_success(self) -> None:
        """Validate config once, configure both handlers, and return their owning controller."""
        arguments = create_config()
        controller = log.configure_logging(**arguments)

        jsonl_handler = controller._handlers[0]
        assert isinstance(jsonl_handler, log.JsonlLogHandler)
        assert jsonl_handler.name == log._JSONL_HANDLER_NAME

        stream_handler = controller._handlers[1]
        assert isinstance(stream_handler, logging.StreamHandler)
        assert stream_handler.name == log._CONSOLE_HANDLER_NAME

        assert jsonl_handler._config.model_dump() == arguments
        assert controller._logger.level == logging.INFO

    def test_configure_logging_already_attached(self) -> None:
        """Reject duplicate configuration without creating or attaching new handlers."""
        with (
            patch.object(u, "is_handler_attached", return_value=True) as attached,
            pytest.raises(RuntimeError, match="Logger has already been configured"),
        ):
            log.configure_logging(**create_config())

        attached.assert_called_once_with(log._JSONL_HANDLER_NAME)

    def test_concurrent_configuration(self) -> None:
        """Allow one concurrent configuration and reject the other without duplicate attachment."""
        handler = create_handler()
        barrier = Barrier(2)

        def configure() -> log.LoggingController | RuntimeError:
            """Synchronize a configure attempt and return its controller or duplicate error."""
            barrier.wait(timeout=5)
            try:
                return log.configure_logging(**create_config())
            except RuntimeError as error:
                return error

        with (
            patch.object(log, "LoggingConfig", wraps=log.LoggingConfig) as config,
            patch.object(log, "JsonlLogHandler", return_value=handler) as handler_class,
            patch.object(log.LoggingController, "shutdown") as shutdown,
            ThreadPoolExecutor(max_workers=2) as executor,
        ):
            futures = [executor.submit(configure) for _ in range(2)]
            outcomes = [future.result(timeout=5) for future in futures]
        controllers = [
            result for result in outcomes if isinstance(result, log.LoggingController)
        ]
        errors = [result for result in outcomes if isinstance(result, RuntimeError)]
        assert len(controllers) == 1
        assert len(errors) == 1
        assert str(errors[0]) == "Logger has already been configured."
        config.assert_called_once_with(**create_config())
        handler_class.assert_called_once_with(logging_config=handler._config)
        shutdown.assert_not_called()
        assert logging.getLogger().handlers.count(handler) == 1


class TestModuleLogger:
    """Test structured call arguments and scoped table context without emitting real records."""

    def test_model_config(self) -> None:
        """Reject extra fields and validate attribute assignments while allowing Logger instances."""
        assert log.ModuleLogger.model_config == {
            "extra": "forbid",
            "arbitrary_types_allowed": True,
            "validate_assignment": True,
        }
        logger = logging.getLogger("opg_pipeline_builder.test")
        module_logger = log.ModuleLogger(logger=logger)
        assert module_logger.logger is logger
        assert module_logger.process is None
        assert module_logger.table == ""
        assert module_logger._table_override.get() is None
        assert (
            create_module_logger()._table_override is not module_logger._table_override
        )

    @pytest.mark.parametrize(
        "fields",
        [
            {"unexpected": "value"},
            {"logger": "not a logger"},
            {"process": 1},
            {"table": None},
        ],
    )
    def test_invalid_fields(self, fields: dict[str, Any]) -> None:
        """Raise validation errors for invalid model construction and field assignment."""
        with pytest.raises(ValidationError):
            create_module_logger(**fields)
        module_logger = create_module_logger()
        name, value = next(iter(fields.items()))
        with pytest.raises((ValidationError, ValueError)):
            setattr(module_logger, name, value)

    def test_with_table_success(self) -> None:
        """Restore nested table overrides on normal exit without changing the logger default."""
        module_logger = create_module_logger(table="default")

        with module_logger.with_table("outer"):
            assert module_logger._table_override.get() == "outer"
            with module_logger.with_table(""):
                assert module_logger._table_override.get() == ""
                assert module_logger.table == "default"
            assert module_logger._table_override.get() == "outer"
            assert module_logger.table == "default"
        assert module_logger._table_override.get() is None
        assert module_logger.table == "default"

    def test_with_table_failure(self) -> None:
        """Restore the parent table after an exception without changing the default."""
        module_logger = create_module_logger(table="default")
        error = ValueError("body failed")

        def raise_inside_table() -> None:
            """Raise the expected body error while the inner table override is active."""
            with module_logger.with_table(""):
                assert module_logger._table_override.get() == ""
                assert module_logger.table == "default"
                raise error

        with module_logger.with_table("outer"):
            assert module_logger._table_override.get() == "outer"
            with pytest.raises(ValueError, match="body failed") as raised:
                raise_inside_table()
            assert raised.value is error
            assert module_logger._table_override.get() == "outer"
            assert module_logger.table == "default"
        assert module_logger._table_override.get() is None
        assert module_logger.table == "default"

    @pytest.mark.parametrize(
        ("configured", "exception"),
        [
            (False, pytest.raises(exc.LoggingNotConfiguredError)),
            (True, nullcontext()),
        ],
    )
    def test_require_logging_configured(
        self,
        configured: bool,
        exception: Any,
    ) -> None:
        """Only the package JSONL handler satisfies the configured guard."""
        module_logger = create_module_logger()
        with (
            exception as raised,
            patch.object(u, "is_handler_attached", return_value=configured) as attached,
        ):
            module_logger._require_logging_configured()
        attached.assert_called_once_with(log._JSONL_HANDLER_NAME)
        if not configured:
            assert str(raised.value) == (
                "Logging is not configured. Call configure_logging() before logging"
            )

    @pytest.mark.parametrize(
        ("default", "override", "explicit", "expected"),
        [
            ("", None, None, ""),
            ("default", None, None, "default"),
            ("default", "context", None, "context"),
            ("default", "", None, ""),
            ("default", "  ", None, "  "),
            ("default", None, "call", "call"),
            ("default", "context", "call", "call"),
            ("default", "context", "", ""),
            ("default", "context", "  ", "  "),
        ],
    )
    def test_get_table_value(
        self, default: str, override: str | None, explicit: str | None, expected: str
    ) -> None:
        """Resolve explicit, contextual, and default table values in precedence order."""
        module_logger = create_module_logger(table=default)
        module_logger._table_override.set(override)
        assert module_logger.get_table_value(explicit) == expected
        assert module_logger.table == default
        assert module_logger._table_override.get() == override

    def test_error(self) -> None:
        """Forward three error calls with default, explicit, and contextual fields."""
        module_logger = create_module_logger(process="Transform", table="default")
        error = ValueError("bad record")

        with (
            patch.object(module_logger.logger, "error") as emit,
            patch.object(module_logger, "_require_logging_configured") as configured,
        ):
            module_logger.error("failed to process %s", "table_a", exc_info=error)
            module_logger.error(
                "Also failed to process %s",
                "table_b",
                process="Second transform",
                table="table_b",
                field="identifier",
                stage="End",
                exc_info=error,
            )
            with module_logger.with_table("table_c"):
                module_logger.error("Failed to process %s", "table_c", exc_info=error)

        emit.assert_has_calls(
            [
                call(
                    "failed to process %s",
                    "table_a",
                    exc_info=error,
                    stacklevel=2,
                    extra={
                        "custom_fields": log.CustomLogFields(
                            process="Transform",
                            table="default",
                            field="",
                            process_stage="Processing",
                        )
                    },
                ),
                call(
                    "Also failed to process %s",
                    "table_b",
                    exc_info=error,
                    stacklevel=2,
                    extra={
                        "custom_fields": log.CustomLogFields(
                            process="Second transform",
                            table="table_b",
                            field="identifier",
                            process_stage="End",
                        )
                    },
                ),
                call(
                    "Failed to process %s",
                    "table_c",
                    exc_info=error,
                    stacklevel=2,
                    extra={
                        "custom_fields": log.CustomLogFields(
                            process="Transform",
                            table="table_c",
                            field="",
                            process_stage="Processing",
                        )
                    },
                ),
            ]
        )
        assert configured.call_count == 3

    def test_info(self) -> None:
        """Forward three info calls with default, explicit, and contextual table values."""
        module_logger = create_module_logger(process="Extract", table="default")

        with (
            patch.object(module_logger.logger, "info") as emit,
            patch.object(module_logger, "_require_logging_configured") as configured,
        ):
            module_logger.info("loaded %s rows", 3)
            module_logger.info(
                "Also loaded %s rows",
                4,
                process="Second extract",
                table="table_b",
                field="row_count",
                stage="End",
            )
            with module_logger.with_table("table_c"):
                module_logger.info("Loaded %s rows", 5, field="row_count")

        emit.assert_has_calls(
            [
                call(
                    "loaded %s rows",
                    3,
                    stacklevel=2,
                    extra={
                        "custom_fields": log.CustomLogFields(
                            process="Extract",
                            table="default",
                            field="",
                            process_stage="Processing",
                        )
                    },
                ),
                call(
                    "Also loaded %s rows",
                    4,
                    stacklevel=2,
                    extra={
                        "custom_fields": log.CustomLogFields(
                            process="Second extract",
                            table="table_b",
                            field="row_count",
                            process_stage="End",
                        )
                    },
                ),
                call(
                    "Loaded %s rows",
                    5,
                    stacklevel=2,
                    extra={
                        "custom_fields": log.CustomLogFields(
                            process="Extract",
                            table="table_c",
                            field="row_count",
                            process_stage="Processing",
                        )
                    },
                ),
            ]
        )
        assert configured.call_count == 3


class TestLoggingEndToEnd:
    """Exercise configured logging from application calls through S3 and Parquet."""

    def configure_logging(
        self, scenario: str, *, batch_size: int
    ) -> log.LoggingController:
        """Configure real S3-backed logging with isolated raw and curated prefixes."""
        return log.configure_logging(
            bucket=DEFAULT_BUCKET,
            log_store_prefix=f"e2e/{scenario}/raw",
            athena_prefix=f"e2e/{scenario}/curated",
            athena_database_name=DEFAULT_ATHENA_DATABASE,
            athena_table_name=f"e2e_{scenario}",
            pipeline=DEFAULT_PIPELINE,
            data_delivery_period=DeliveryPeriod._DATETIME,
            attempt_no=1,
            run_id=f"e2e-{scenario}",
            batch_size=batch_size,
        )

    def run_lifecycle(
        self,
        mode: str,
        controller: log.LoggingController,
        handler: log.JsonlLogHandler,
        write_logs: Callable[[], None],
    ) -> None:
        """Run the shared events through the selected flush and shutdown mode."""
        if mode in {"context_manager", "manual_flush"}:
            with controller:
                write_logs()
                if mode == "manual_flush":
                    controller.flush()
                    assert handler._part_number == 1
                    assert not handler._closed
            return

        write_logs()
        if mode == "automatic_flush":
            assert handler._part_number == 1
        controller.shutdown()

    def expected_e2e_event(
        self,
        error_type: str,
        process: str,
        exception: str = "",
        process_stage: log.ProcessStage = "Processing",
        *,
        message: str | None = None,
        table: str = "table",
        field: str | None = None,
    ) -> dict[str, Any]:
        """Build one expected structured event for an isolated E2E logging run."""
        return {
            "log_timestamp": LogTimestamp.ISO,
            "log_level": error_type.upper(),
            "process": process,
            "table": table,
            "field": error_type if field is None else field,
            "process_stage": process_stage,
            "message": f"This is {error_type}" if message is None else message,
            "exception": exception,
        }

    def read_jsonl_rows(self, s3: boto3.client, prefix: str) -> list[dict[str, Any]]:
        """Read JSONL objects written under a raw-log prefix from the mocked S3 bucket."""
        objects = s3.list_objects_v2(Bucket=DEFAULT_BUCKET, Prefix=prefix).get(
            "Contents", []
        )
        rows: list[dict[str, Any]] = []
        for obj in objects:
            if not obj["Key"].endswith(".jsonl"):
                continue
            body = s3.get_object(Bucket=DEFAULT_BUCKET, Key=obj["Key"])["Body"].read()
            rows.extend(json.loads(line) for line in body.decode("utf-8").splitlines())
        return rows

    def read_curated_rows(self, prefix: str) -> list[dict[str, Any]]:
        """Read curated parquet rows written under an E2E test prefix."""
        frame = wr.s3.read_parquet(path=f"s3://{DEFAULT_BUCKET}/{prefix}", dataset=True)
        return cast("list[dict[str, Any]]", frame.to_dict(orient="records"))

    def assert_e2e_events_match(
        self,
        actual_events: list[dict[str, Any]],
        expected_events: list[dict[str, Any]],
    ) -> None:
        """Match each expected event to one actual row and check all expected fields."""
        expected_processes = {event["process"] for event in expected_events}
        actual_events = [
            row for row in actual_events if row["process"] in expected_processes
        ]
        for row in actual_events:
            row["log_timestamp"] = u.format_datetime(
                pd.to_datetime(row["log_timestamp"], utc=True).to_pydatetime()
            )
            row["exception"] = row["exception"].removesuffix("\n")

        actual_by_message = {row["message"]: row for row in actual_events}
        expected_by_message = {row["message"]: row for row in expected_events}

        assert len(actual_by_message) == len(actual_events)
        assert set(actual_by_message) == set(expected_by_message)
        for message, expected in expected_by_message.items():
            actual = actual_by_message[message]
            assert {field: actual[field] for field in expected} == expected

    @pytest.mark.parametrize(
        ("scenario", "batch_size"),
        [
            ("context_manager", 10),
            ("manual_flush", 10),
            ("automatic_flush", 3),
            ("explicit_shutdown", 10),
        ],
    )
    def test_logging_e2e_context_manager(
        self,
        scenario: str,
        batch_size: int,
        s3: boto3.client,
    ) -> None:
        """Persist expected INFO and ERROR rows through each lifecycle mode to raw and curated stores."""
        controller = self.configure_logging(scenario, batch_size=batch_size)

        module_logger = log.ModuleLogger(
            logger=logging.getLogger(f"opg_pipeline_builder.e2e.{scenario}"),
            process="CtxMgr",
            table="table",
        )

        jsonl_handler = controller._handlers[0]
        assert isinstance(jsonl_handler, log.JsonlLogHandler)

        def write_logs() -> None:
            """Emit the shared INFO and ERROR events for the selected lifecycle mode."""
            module_logger.info("This is info", field="info")
            module_logger.error(
                "This is error", field="error", exc_info=ValueError("Error")
            )

        try:
            with (
                patch.object(time, "time", return_value=LogTimestamp._TIMESTAMP),
                patch.object(
                    time,
                    "time_ns",
                    return_value=int(LogTimestamp._TIMESTAMP) * 1_000_000_000,
                ),
            ):
                self.run_lifecycle(scenario, controller, jsonl_handler, write_logs)
        finally:
            if not controller._is_shutdown:
                controller.shutdown()

        assert controller._is_shutdown
        assert jsonl_handler._closed

        expected_events = [
            self.expected_e2e_event(error_type="info", process="CtxMgr"),
            self.expected_e2e_event(
                error_type="error", process="CtxMgr", exception="ValueError: Error"
            ),
        ]

        raw_events = self.read_jsonl_rows(s3, f"e2e/{scenario}/raw/")
        self.assert_e2e_events_match(raw_events, expected_events)

        curated_events = self.read_curated_rows(f"e2e/{scenario}/curated")
        self.assert_e2e_events_match(curated_events, expected_events)

    def test_threaded_logging(self, s3: boto3.client) -> None:
        """Persist concurrent default, scoped, and explicit-table events in raw and curated stores."""
        worker_count = 4

        controller = self.configure_logging("threaded", batch_size=4)
        handler = controller._handlers[0]
        assert isinstance(handler, log.JsonlLogHandler)

        module_logger = log.ModuleLogger(
            logger=logging.getLogger("opg_pipeline_builder.e2e.threaded"),
            process="Threaded",
            table="fallback_table",
        )
        barrier = Barrier(worker_count)

        def log_from_worker(worker: int) -> None:
            """Emit fallback, scoped, and explicit-table events for one worker."""
            barrier.wait(timeout=5)
            module_logger.info("default-%s", worker, field="default_field")
            with module_logger.with_table(f"scoped_{worker}"):
                module_logger.info("scoped-%s", worker, field="scoped_field")
                module_logger.error(
                    "explicit-%s",
                    worker,
                    table=f"explicit_{worker}",
                    field="explicit_field",
                    exc_info=ValueError(f"thread failure {worker}"),
                )

        def write_logs() -> None:
            """Run all workers concurrently and propagate any worker exception."""
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                futures = [
                    executor.submit(log_from_worker, worker)
                    for worker in range(worker_count)
                ]
                for future in futures:
                    future.result(timeout=10)

        try:
            with (
                patch.object(time, "time", return_value=LogTimestamp._TIMESTAMP),
                patch.object(
                    time,
                    "time_ns",
                    return_value=int(LogTimestamp._TIMESTAMP) * 1_000_000_000,
                ),
            ):
                self.run_lifecycle("explicit_shutdown", controller, handler, write_logs)
        finally:
            if not controller._is_shutdown:
                controller.shutdown()

        expected_events = []
        for worker in range(worker_count):
            expected_events.extend(
                [
                    self.expected_e2e_event(
                        "info",
                        "Threaded",
                        message=f"default-{worker}",
                        table="fallback_table",
                        field="default_field",
                    ),
                    self.expected_e2e_event(
                        "info",
                        "Threaded",
                        message=f"scoped-{worker}",
                        table=f"scoped_{worker}",
                        field="scoped_field",
                    ),
                    self.expected_e2e_event(
                        "error",
                        "Threaded",
                        exception=f"ValueError: thread failure {worker}",
                        message=f"explicit-{worker}",
                        table=f"explicit_{worker}",
                        field="explicit_field",
                    ),
                ]
            )

        raw_events = self.read_jsonl_rows(s3, "e2e/threaded/raw/")
        self.assert_e2e_events_match(raw_events, expected_events)

        curated_events = self.read_curated_rows("e2e/threaded/curated")
        self.assert_e2e_events_match(curated_events, expected_events)
