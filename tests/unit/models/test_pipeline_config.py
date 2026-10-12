from unittest.mock import patch

import pytest

from opg_pipeline_builder.models import modelling_exceptions as exc
from tests.test_utils import assert_log_record, create_pipeline_config


class TestPipelineConfig:
    def test_validate_name_valid(self) -> None:
        """Test the validation of a valid pipeline name."""
        with patch(
            "opg_pipeline_builder.models.pipeline_config.is_valid_identifier",
            return_value="",
        ) as mock_valid:
            config = create_pipeline_config()

        assert config.name == "test_pipeline"
        assert mock_valid.call_count == 1

    def test_validate_name_invalid(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test the validation of an invalid pipeline name."""
        with (
            patch(
                "opg_pipeline_builder.models.pipeline_config.is_valid_identifier",
                return_value="Invalid identifier",
            ) as mock_valid,
            pytest.raises(
                exc.InvalidPipelineNameError,
                match="Pipeline name '1_invalid_name' is invalid: Invalid identifier.",
            ),
        ):
            create_pipeline_config(name="1_invalid_name")

        assert mock_valid.call_count == 1
        assert_log_record(
            caplog,
            message="Pipeline name '1_invalid_name' is invalid: Invalid identifier.",
            process="Pipeline validation",
            table="pipeline",
            field="name",
            stage="Processing",
        )

    def test_validate_s3_paths_valid(self) -> None:
        """Test the validation of valid S3 paths."""
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

    def test_validate_s3_paths_invalid(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test the validation of invalid S3 paths."""
        with (
            patch(
                "opg_pipeline_builder.models.pipeline_config.is_valid_s3_path_template",
                return_value="Error found",
            ) as mock_valid,
            pytest.raises(
                exc.InvalidPathError,
                match="S3 path for field 'land_path' is invalid: Error found.",
            ),
        ):
            create_pipeline_config(land_path="invalid_land_path")
        assert mock_valid.call_count == 1
        assert_log_record(
            caplog,
            message="S3 path for field 'land_path' is invalid: Error found.",
            process="Pipeline validation",
            table="pipeline",
            field="land_path",
            stage="Processing",
        )

    def test_create_reference_table(self) -> None:
        """Test the creation of the reference table for the pipeline configuration."""
        config = create_pipeline_config()
        reference_table = config.create_reference_table()
        assert reference_table.shape[0] == 1
        assert reference_table.shape[1] == 4
        assert reference_table["pipeline_name"][0] == config.name
        assert reference_table["description"][0] == config.description
        assert reference_table["github_repo"][0] == config.github_repo
        assert reference_table["data_cadence"][0] == config.data_cadence
