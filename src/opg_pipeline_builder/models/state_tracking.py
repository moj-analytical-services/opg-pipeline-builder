from datetime import datetime
from logging import getLogger
from typing import Literal

from pydantic import BaseModel, ValidationInfo, field_validator

from opg_pipeline_builder.constants import ALLOWED_STATUSES
from opg_pipeline_builder.models.utils import field_name, table_name

logger = getLogger(__name__)


class DataDeliveryTracker(BaseModel):
    data_delivery_period: datetime
    pipeline_name: str
    received_datetime: datetime
    pipeline_params: str
    current_pipeline_stage: str
    valid: bool
    status: str

    @field_validator("status")
    @classmethod
    def validate_status(cls, value: str, info: ValidationInfo) -> str:
        """Validate that the status is one of the allowed statuses."""
        if value not in ALLOWED_STATUSES:
            err = f"Status '{value}' is not allowed. Must be one of {ALLOWED_STATUSES}."
            logger.error(err, table=table_name(info), field=field_name(info))
            raise ValueError(err)
        return value


class TableTracker(BaseModel):
    pipeline_name: str
    table_name: str
    current_pipeline_stage: str
    status: str
    last_data_delivery_period: datetime
    last_attempt_no: int
    last_run_id: str
    last_committed_snapshot_id: str

    @field_validator("status")
    @classmethod
    def validate_status(cls, value: str, info: ValidationInfo) -> str:
        """Validate that the status is one of the allowed statuses."""
        if value not in ALLOWED_STATUSES:
            err = f"Status '{value}' is not allowed. Must be one of {ALLOWED_STATUSES}."
            logger.error(err, table=table_name(info), field=field_name(info))
            raise ValueError(err)
        return value


class FileTracker(BaseModel):
    pipeline_name: str
    data_delivery_period: datetime
    filename: str
    file_received_date: datetime
    is_current: bool
    landing_filepath: str
    archive_filepath: str
    landing_checksum_sha256: str
    archive_checksum_sha256: str
    status: str
    number_of_records: int
    file_size_bytes: int
    storage_tier: str
    tags: list[str]

    @field_validator("status")
    @classmethod
    def validate_status(cls, value: str, info: ValidationInfo) -> str:
        """Validate that the status is one of the allowed statuses."""
        if value not in ALLOWED_STATUSES:
            err = f"Status '{value}' is not allowed. Must be one of {ALLOWED_STATUSES}."
            logger.error(err, table=table_name(info), field=field_name(info))
            raise ValueError(err)
        return value


class RunSourceFileTracker(BaseModel):
    pipeline_name: str
    data_delivery_period: datetime
    attempt_no: int
    filename: str
    file_received_date: datetime
    source_type: Literal["Landing", "Archive"]
    source_role: Literal["new", "corrected", "unchanged", "reconstituted"]
    target_tables: list[str]
    staging_filepath: str
    curated_filepath: str
    curated_checksum_sha256: str
    status: str
    records_selected: int
    records_written: int

    @field_validator("status")
    @classmethod
    def validate_status(cls, value: str, info: ValidationInfo) -> str:
        """Validate that the status is one of the allowed statuses."""
        if value not in ALLOWED_STATUSES:
            err = f"Status '{value}' is not allowed. Must be one of {ALLOWED_STATUSES}."
            logger.error(err, table=table_name(info), field=field_name(info))
            raise ValueError(err)
        return value


class RunTracker(BaseModel):
    pipeline_name: str
    data_delivery_period: datetime
    attempt_no: int
    run_start: datetime
    run_end: datetime
    run_id: str
    purpose: Literal[
        "BAU",
        "Deletion",
        "Remediation",
        "Maintenance",
    ]
    status: str
    lease_expires_at: datetime
    log_location: str

    @field_validator("status")
    @classmethod
    def validate_status(cls, value: str, info: ValidationInfo) -> str:
        """Validate that the status is one of the allowed statuses."""
        if value not in ALLOWED_STATUSES:
            err = f"Status '{value}' is not allowed. Must be one of {ALLOWED_STATUSES}."
            logger.error(err, table=table_name(info), field=field_name(info))
            raise ValueError(err)
        return value


def create_run_tracker(
    pipeline_name: str,
    data_delivery_period: datetime,
    attempt_no: int,
    run_start: datetime,
    run_end: datetime,
    run_id: str,
    purpose: Literal[
        "BAU",
        "Deletion",
        "Remediation",
        "Maintenance",
    ],
    status: str,
    lease_expires_at: datetime,
    log_location: str,
) -> RunTracker:
    return RunTracker(
        pipeline_name=pipeline_name,
        data_delivery_period=data_delivery_period,
        attempt_no=attempt_no,
        run_start=run_start,
        run_end=run_end,
        run_id=run_id,
        purpose=purpose,
        status=status,
        lease_expires_at=lease_expires_at,
        log_location=log_location,
    )


class PipelineRunTracker(BaseModel):
    pipeline_name: str
    data_delivery_period: datetime
    attempt_no: int
    reference_snapshot_ids: list[str]
    mutation_set_id: str
    mutation_contract_location: str
    records_processed: int
    records_inserted: int
    records_change: int
    records_deleted: int


class MaintenanceRunTracker(BaseModel):
    pipeline_name: str
    data_delivery_period: datetime
    attempt_no: int
    maintenance_target_tavles: list[str]
    maintenance_reason: str
    pre_snapshot_id: str
    post_snapshot_id: str
    maintenance_operation_performance: list[str]
    maintenance_deferred_operations: list[str]
    metrics_location: str

    @field_validator("status")
    @classmethod
    def validate_status(cls, value: str, info: ValidationInfo) -> str:
        """Validate that the status is one of the allowed statuses."""
        if value not in ALLOWED_STATUSES:
            err = f"Status '{value}' is not allowed. Must be one of {ALLOWED_STATUSES}."
            logger.error(err, table=table_name(info), field=field_name(info))
            raise ValueError(err)
        return value
