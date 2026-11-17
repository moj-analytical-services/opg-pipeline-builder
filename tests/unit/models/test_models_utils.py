import re
from unittest.mock import Mock, patch

import pytest
from pydantic import ValidationInfo

from opg_pipeline_builder.models import utils as u


def test_table_name_exists() -> None:
    """Test that the table name is correctly extracted from the validation context."""
    info = Mock(spec=ValidationInfo, context={"table_name": "test_table"})
    assert u.table_name(info) == "test_table"


def test_table_name_not_exists() -> None:
    """Test that an empty string is returned when the table name is not in the validation context."""
    info = Mock(spec=ValidationInfo, context={})
    assert u.table_name(info) == ""


def test_field_name_exists() -> None:
    """Test that the field name is correctly extracted from the validation context."""
    info = Mock(spec=ValidationInfo, context={}, field_name="test_field")
    assert u.field_name(info) == "test_field"


def test_field_name_not_exists() -> None:
    """Test that an empty string is returned when the field name is not in the validation context."""
    info = Mock(spec=ValidationInfo, context={}, field_name=None)
    assert u.field_name(info) == ""


def test_render_s3_path_valid() -> None:
    """Test that the S3 path is correctly rendered."""
    with patch(
        "opg_pipeline_builder.models.utils.is_valid_s3_path", return_value=None
    ) as mock_valid:
        rendered_path = u.render_s3_path(
            "s3://bucket-name/{{ env }}/{{ db }}/land",
            env="dev",
            db="test_db",
        )
        assert rendered_path == "s3://bucket-name/dev/test_db/land"
        assert mock_valid.call_count == 1


def test_render_s3_path_missing_args() -> None:
    """Test that the S3 path is correctly rendered."""
    with (
        patch("opg_pipeline_builder.models.utils.is_valid_s3_path") as mock_valid,
        pytest.raises(
            TypeError,
            match=re.escape(
                "render_s3_path() missing 1 required positional argument: 'db'"
            ),
        ),
    ):
        u.render_s3_path("s3://bucket-name/{{ env }}/{{ db }}/land", env="dev")  # type: ignore[call-arg]
    assert mock_valid.call_count == 0


def test_render_s3_path_invalid() -> None:
    """Test that an invalid S3 path raises a ValueError."""
    with (
        patch(
            "opg_pipeline_builder.models.utils.is_valid_s3_path",
            return_value="Error Found",
        ) as mock_render,
        pytest.raises(ValueError, match="Invalid S3 path: Error Found"),
    ):
        u.render_s3_path(
            "s3://bucket-name/{{ env }}/{{ db }}/land", env="dev", db="test_db"
        )
    assert mock_render.call_count == 1
