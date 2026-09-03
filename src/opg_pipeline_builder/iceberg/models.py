"""Record models for Iceberg table interactions.

A business model inherits `Record`, declares its own fields, and carries
its SCD2 attributes as a nested `Scd2` block rather than inheriting them. The
model therefore mirrors the source record, and the validity attributes are
attached by policy-aware code as a separate, separately validated object.

Both types are mutable (`validate_assignment=True`, so every mutation is still
validated). Closing, retiring or deleting a row only ever changes `valid_to`,
`is_current` and `deleted` - never `valid_from`, the field a merge uses as its
match key - so mutating those three in place is safe: the object still matches
the row it came from. The one case that does change `valid_from` - reactivating
a closed version, or shifting a boundary to admit a late-arriving fact - must
mutate a `Record.duplicate()` instead, so the original row is still
available, unmodified, as the match key for the row it supersedes.
"""

from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, model_validator

from opg_pipeline_builder.iceberg import exceptions as exc
from opg_pipeline_builder.temp_objects.config_models import (
    TableConfig,
)

SENTINEL_VALID_TO = datetime(9999, 12, 31, tzinfo=UTC)
INTERVAL_GRANULARITY = timedelta(seconds=1)


class Scd2(BaseModel):
    """The SCD2 validity attributes of one row.

    Contains the logic to alter SCD2 attributes according to business rules.

    Methods:
    - close: Close the record before the successor's valid_from.
    - delete: Logically delete the record from the given moment.
    - deprecate: Deprecate the record from the given moment.
    - reopen: Reopen the record to its default state.
    """

    model_config = ConfigDict(
        extra="forbid", validate_assignment=True, populate_by_name=True
    )

    valid_from: datetime
    valid_to: datetime = SENTINEL_VALID_TO
    is_current: bool = True
    deleted: bool = False

    @model_validator(mode="after")
    def val_valid_to_after_valid_from(self) -> Self:
        if self.valid_to < self.valid_from:
            raise exc.ValidToBeforeValidFromError("valid_to must be after valid_from")
        return self

    @model_validator(mode="after")
    def val_current_and_deleted(self) -> Self:
        if self.is_current and self.deleted:
            raise exc.DeletedRecordIsCurrentError(
                "A record cannot be both current and deleted"
            )
        return self

    def close(self, closed_at: datetime) -> None:
        """Close a record before the successor's valid_from.

        Uses configured INTERVAL_GRANULARITY to close off a record by a fixed
        interval before the successor's valid_from.
        """
        self.valid_to = closed_at
        self.is_current = False

    def delete(self, deleted_at: datetime) -> None:
        """Logically delete record from the given moment.

        Logically deleted rows have no successor and remain valid for the period between
        its valid_from and valid_to. The key is left with no current record.
        """
        self.valid_to = deleted_at
        self.is_current = False
        self.deleted = True

    def reopen(self) -> None:
        """Reopen a record.

        Reopens a record by setting it back to a default state, including undoing a
        delete marker, if one was placed
        """
        self.valid_to = SENTINEL_VALID_TO
        self.deleted = False
        self.is_current = True

    def shift_validity(
        self, valid_from: datetime | None, valid_to: datetime | None
    ) -> None:
        """Shifts the validity period of a record.

        Shifts the validity period of a record by updating its `valid_from` and `valid_to` attributes.
        """
        if valid_from:
            self.valid_from = valid_from
        if valid_to:
            self.valid_to = valid_to


class Record(Scd2):
    """Base class for business row models.

    Business keys, values and validation are defined in individual subclasses.
    They also define how the the SCD2 attributes are managed for that
    particular business row.
    """

    def duplicate_record(self) -> Self:
        """Return an independent copy, safe to mutate without affecting this row."""
        return self.model_copy(deep=True)

    def output_data(self) -> dict[str, Any]:
        """Return a dictionary of this record's data, including SCD2 attributes."""
        return self.model_dump()

    def business_key(self, table: TableConfig) -> tuple[Any, ...]:
        """Return this record's business key, in configured column order."""
        return tuple(getattr(self, column) for column in table.business_key_columns)

    def content_hash(self, table: TableConfig) -> str:
        """Hash of the value columns, used to detect changed definitions."""
        payload = "|".join(
            repr(getattr(self, column)) for column in table.value_columns
        )
        return sha256(payload.encode()).hexdigest()
