import pytest

from opg_pipeline_builder.models.pipeline_config import PipelineConfig
from opg_pipeline_builder.models.run_config import create_run_config
from opg_pipeline_builder.models.settings_config import SettingsConfig


def test_create_run_config(caplog: pytest.LogCaptureFixture) -> None:
    """Test the creation of a RunConfig instance."""

    pipeline = PipelineConfig(
        name="db_name",
        description="description",
        land_path="s3://bucket-name/{{ env }}/{{ db }}/land/table-name",
        archive_path="s3://bucket-name/{{ env }}/{{ db }}/archive/table-name",
        curated_path="s3://bucket-name/{{ env }}/{{ db }}/curated/table-name",
    )
    settings = SettingsConfig(ENV="test")
    run_config = create_run_config(pipeline=pipeline, settings=settings)

    assert run_config.env == "test"
    assert run_config.name == "db_name"
    assert run_config.land_path == "s3://bucket-name/test/db_name/land/table-name"
    assert run_config.archive_path == "s3://bucket-name/test/db_name/archive/table-name"
    assert run_config.curated_path == "s3://bucket-name/test/db_name/curated/table-name"
    assert all(
        exp_msg in caplog.text
        for exp_msg in ["Creating run config", "Run config created"]
    )
