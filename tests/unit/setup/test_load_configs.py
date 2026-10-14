from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from opg_pipeline_builder.setup.load_configs import (
    load_pipeline_config,
    setup_run_config,
)
from tests.test_utils import assert_log_record, create_pipeline_config, output_yaml_data


def test_load_pipeline_config_success(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    pipeline_config = create_pipeline_config()
    config_path = tmp_path / f"{pipeline_config.name}.yml"
    output_yaml_data(config_path, pipeline_config.model_dump(mode="json"))

    assert load_pipeline_config(tmp_path, pipeline_config.name) == pipeline_config
    assert_log_record(
        caplog,
        f"Loading configuration for pipeline '{pipeline_config.name}'",
        stage="Start",
    )
    assert_log_record(
        caplog,
        f"Configuration for pipeline '{pipeline_config.name}' loaded successfully",
        stage="End",
    )


def test_load_pipeline_config_file_not_found(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    pipeline_name = "non_existent_pipeline"

    with pytest.raises(FileNotFoundError):
        load_pipeline_config(tmp_path, pipeline_name)

    assert_log_record(
        caplog, f"Loading configuration for pipeline '{pipeline_name}'", stage="Start"
    )
    assert_log_record(
        caplog,
        f"Configuration for pipeline '{pipeline_name}' not found in {tmp_path}",
        stage="End",
    )


def test_load_pipeline_config_yaml_error(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    pipeline_config = create_pipeline_config()
    config_path = tmp_path / f"{pipeline_config.name}.yml"
    output_yaml_data(config_path, pipeline_config.model_dump(mode="json"))

    with (
        patch("yaml.safe_load", side_effect=yaml.YAMLError("mocked error")),
        pytest.raises(yaml.YAMLError, match="mocked error"),
    ):
        load_pipeline_config(tmp_path, pipeline_config.name)

    assert_log_record(
        caplog,
        f"Loading configuration for pipeline '{pipeline_config.name}'",
        stage="Start",
    )
    assert_log_record(
        caplog,
        f"Failed to parse configuration for pipeline '{pipeline_config.name}'",
        stage="End",
    )


def test_setup_run_config(caplog: pytest.LogCaptureFixture) -> None:
    """Test that a RunConfig object is set up correctly and returned."""
    with patch(
        "opg_pipeline_builder.setup.load_configs.load_pipeline_config",
        return_value=create_pipeline_config(),
    ):
        run_config = setup_run_config(configs_folder=Path("."))

    assert run_config.env == "test"
    assert run_config.name == "test_pipeline"
    assert not "{{ db }}" in run_config.land_path
    assert not "{{ env }}" in run_config.archive_path
    assert "test" in run_config.curated_path
    assert_log_record(caplog, "Setting up run configuration", stage="Start")
    assert_log_record(
        caplog,
        "Loaded DAG settings: Running 'test_pipeline' pipeline in test",
        stage="Processing",
    )
    assert_log_record(
        caplog, "Loaded pipeline configuration for 'test_pipeline'", stage="Processing"
    )
    assert_log_record(caplog, "Run configuration set up successfully", stage="End")
