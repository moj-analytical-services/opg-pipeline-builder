from datetime import UTC, datetime

import pytest

from opg_pipeline_builder.iceberg import exceptions as exc
from opg_pipeline_builder.iceberg.models import SENTINEL_VALID_TO, Scd2


def test_scd2_invalid_valid_to() -> None:
    """Test that ValidToBeforeValidFromError is raised when valid_to is before valid_from."""
    with pytest.raises(exc.ValidToBeforeValidFromError):
        Scd2(
            valid_from=datetime(2024, 1, 1, tzinfo=UTC),
            valid_to=datetime(2023, 12, 31, tzinfo=UTC),
        )


def test_scd2_invalid_current_and_deleted() -> None:
    """Test that DeletedRecordIsCurrentError is raised when a record is marked as deleted but also current."""
    with pytest.raises(exc.DeletedRecordIsCurrentError):
        Scd2(valid_from=datetime(2024, 1, 1, tzinfo=UTC), deleted=True)


def test_scd2_close() -> None:
    """Test that SCD2 close method alters attributes correctly."""
    scd2 = Scd2(valid_from=datetime(2024, 1, 1, tzinfo=UTC))
    scd2.close(closed_at=datetime(2024, 12, 31, tzinfo=UTC))

    assert scd2.valid_from == datetime(2024, 1, 1, tzinfo=UTC)
    assert scd2.valid_to == datetime(2024, 12, 31, tzinfo=UTC)
    assert not scd2.is_current
    assert not scd2.deleted


def test_scd2_delete() -> None:
    """Test that SCD2 delete method alters attributes correctly."""
    scd2 = Scd2(valid_from=datetime(2024, 1, 1, tzinfo=UTC))
    scd2.delete(deleted_at=datetime(2024, 12, 31, tzinfo=UTC))

    assert scd2.valid_from == datetime(2024, 1, 1, tzinfo=UTC)
    assert scd2.valid_to == datetime(2024, 12, 31, tzinfo=UTC)
    assert scd2.deleted
    assert not scd2.is_current


def test_scd2_reopen_from_close() -> None:
    """Test that SCD2 reopen method alters attributes correctly after being closed."""
    scd2 = Scd2(valid_from=datetime(2024, 1, 1, tzinfo=UTC))
    scd2.close(closed_at=datetime(2024, 12, 31, tzinfo=UTC))
    scd2.reopen()

    assert scd2.valid_from == datetime(2024, 1, 1, tzinfo=UTC)
    assert scd2.valid_to == SENTINEL_VALID_TO
    assert scd2.is_current
    assert not scd2.deleted


def test_scd2_reopen_from_delete() -> None:
    """Test that SCD2 reopen method alters attributes correctly after being deleted."""
    scd2 = Scd2(valid_from=datetime(2024, 1, 1, tzinfo=UTC))
    scd2.delete(deleted_at=datetime(2024, 12, 31, tzinfo=UTC))
    scd2.reopen()

    assert scd2.valid_from == datetime(2024, 1, 1, tzinfo=UTC)
    assert scd2.valid_to == SENTINEL_VALID_TO
    assert scd2.is_current
    assert not scd2.deleted


def test_scd2_shift_validity() -> None:
    """Test that SCD2 shift_validity method alters attributes correctly."""
    scd2 = Scd2(valid_from=datetime(2024, 1, 1, tzinfo=UTC))
    scd2.shift_validity(
        valid_from=datetime(2024, 2, 1, tzinfo=UTC),
        valid_to=datetime(2024, 12, 31, tzinfo=UTC),
    )

    assert scd2.valid_from == datetime(2024, 2, 1, tzinfo=UTC)
    assert scd2.valid_to == datetime(2024, 12, 31, tzinfo=UTC)
    assert scd2.is_current
    assert not scd2.deleted
