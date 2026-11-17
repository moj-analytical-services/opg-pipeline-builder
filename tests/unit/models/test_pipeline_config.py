from unittest.mock import patch

import pytest

from opg_pipeline_builder.models import modelling_exceptions as exc
from opg_pipeline_builder.models.pipeline_config import PipelineConfig


def create_pipeline_config(
    name: str = "name",
    description: str = "description",
    land_path: str = "s3://bucket-name/{{ env }}/{{ db }}/land/table-name",
    archive_path: str = "s3://bucket-name/{{ env }}/{{ db }}/archive/table-name",
    curated_path: str = "s3://bucket-name/{{ env }}/{{ db }}/curated/table-name",
    github_repo: str = "https://github.com/user/repo",
    data_cadence: str = "daily",
) -> PipelineConfig:
    """Create a PipelineConfig instance with default or provided values."""
    return PipelineConfig(
        name=name,
        description=description,
        land_path=land_path,
        archive_path=archive_path,
        curated_path=curated_path,
        github_repo=github_repo,
        data_cadence=data_cadence,
    )


class TestPipelineConfig:
    def test_validate_name_valid(self) -> None:
        with patch(
            "opg_pipeline_builder.models.pipeline_config.is_valid_identifier",
            return_value="",
        ) as mock_valid:
            config = create_pipeline_config()

        assert config.name == "name"
        assert mock_valid.call_count == 1

    def test_validate_name_invalid(self) -> None:
        with (
            patch(
                "opg_pipeline_builder.models.pipeline_config.is_valid_identifier",
                return_value="Error found",
            ) as mock_valid,
            pytest.raises(exc.InvalidPipelineNameError, match="Error found"),
        ):
            create_pipeline_config(name="invalid_name")

        assert mock_valid.call_count == 1

    def test_validate_s3_paths_valid(self) -> None:
        with patch(
            "opg_pipeline_builder.models.pipeline_config.is_valid_s3_path_template",
            return_value="",
        ) as mock_valid:
            config = create_pipeline_config()
            assert (
                config.land_path
                == "s3://bucket-name/{{ env }}/{{ db }}/land/table-name"
            )
            assert (
                config.archive_path
                == "s3://bucket-name/{{ env }}/{{ db }}/archive/table-name"
            )
            assert (
                config.curated_path
                == "s3://bucket-name/{{ env }}/{{ db }}/curated/table-name"
            )
        assert mock_valid.call_count == 3

    def test_validate_s3_paths_invalid(self) -> None:
        with (
            patch(
                "opg_pipeline_builder.models.pipeline_config.is_valid_s3_path_template",
                return_value="Error found",
            ) as mock_valid,
            pytest.raises(exc.InvalidPathError, match="Error found"),
        ):
            create_pipeline_config(land_path="invalid_land_path")
        assert mock_valid.call_count == 1
