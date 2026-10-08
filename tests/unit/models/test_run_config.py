import pytest

from opg_pipeline_builder.models.pipeline_config import PipelineConfig
from opg_pipeline_builder.models.run_config import create_run_config
from opg_pipeline_builder.models.settings_config import SettingsConfig
from tests.test_utils import assert_log_record


def test_create_run_config(caplog: pytest.LogCaptureFixture) -> None:
    """Test the creation of a RunConfig instance."""

    pipeline = PipelineConfig(
        name="test_pipeline",
        description="description",
        land_path="s3://bucket-name/{{ env }}/{{ db }}/land/table-name",
        archive_path="s3://bucket-name/{{ env }}/{{ db }}/archive/table-name",
        curated_path="s3://bucket-name/{{ env }}/{{ db }}/curated/table-name",
        github_repo="https://github.com/user/repo",
        data_cadence="daily",
    )
    settings = SettingsConfig(ENV="test", PIPELINE_NAME="test_pipeline")
    run_config = create_run_config(pipeline=pipeline, settings=settings)

    assert run_config.env == "test"
    assert run_config.name == "test_pipeline"
    assert run_config.land_path == "s3://bucket-name/test/test_pipeline/land/table-name"
    assert (
        run_config.archive_path
        == "s3://bucket-name/test/test_pipeline/archive/table-name"
    )
    assert (
        run_config.curated_path
        == "s3://bucket-name/test/test_pipeline/curated/table-name"
    )
    assert_log_record(caplog, "Creating run config", table="run_config", stage="Start")
    assert_log_record(caplog, "Run config created", table="run_config", stage="End")


def test_create_run_config_mismatched_pipeline_name(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that creating a RunConfig with mismatched pipeline name raises an error."""
    pipeline = PipelineConfig(
        name="wrong_pipeline",
        description="description",
        land_path="s3://bucket-name/{{ env }}/{{ db }}/land/table-name",
        archive_path="s3://bucket-name/{{ env }}/{{ db }}/archive/table-name",
        curated_path="s3://bucket-name/{{ env }}/{{ db }}/curated/table-name",
        github_repo="https://github.com/user/repo",
        data_cadence="daily",
    )
    settings = SettingsConfig(ENV="test", PIPELINE_NAME="test_pipeline")
    with pytest.raises(
        ValueError,
        match="Pipeline name 'wrong_pipeline' does not match settings PIPELINE_NAME 'test_pipeline'",
    ):
        create_run_config(pipeline=pipeline, settings=settings)

    assert_log_record(caplog, "Creating run config", table="run_config", stage="Start")
    assert_log_record(
        caplog,
        "Pipeline name 'wrong_pipeline' does not match settings PIPELINE_NAME 'test_pipeline'",
        table="run_config",
    )
