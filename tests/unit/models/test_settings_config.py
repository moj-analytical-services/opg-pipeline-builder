import os

import pytest

from opg_pipeline_builder.constants import ALLOWED_ENVS
from opg_pipeline_builder.models.settings_config import SettingsConfig


@pytest.mark.parametrize("env", ["test", "preprod", "prod"])
def test_settings_config_env_valid(env: str) -> None:
    """Test that the environment setting in the settings config is valid."""
    os.environ["ENV"] = env
    config = SettingsConfig()
    assert config.ENV == env


@pytest.mark.parametrize(("env"), [("invalid_env"), ("staging")])
def test_settings_config_env_invalid(env: str) -> None:
    """Test that an invalid environment setting raises an error."""
    os.environ["ENV"] = env
    with pytest.raises(
        ValueError, match=f"ENV must be one of {', '.join(ALLOWED_ENVS)}"
    ):
        SettingsConfig()
