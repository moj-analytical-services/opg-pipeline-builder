from pathlib import Path
import yaml
from logging import getLogger

from opg_pipeline_builder.logging import ModuleLogger
from opg_pipeline_builder.models.run_config import create_run_config, RunConfig
from opg_pipeline_builder.models.settings_config import SettingsConfig
from opg_pipeline_builder.models.pipeline_config import PipelineConfig

log = ModuleLogger(logger=getLogger(__name__))


def load_pipeline_config(configs_folder: Path, name: str) -> PipelineConfig:
    """Load the configuration for the specified pipeline.

    Args:
        configs_folder (Path): The path to the folder containing the pipeline configuration files.
        name (str): The name of the pipeline being run

    Returns:
        PipelineConfig: The loaded configuration for the pipeline.

    Raises:
        FileNotFoundError: If the configuration file for the specified pipeline is not found.
        yaml.YAMLError: If there is an error parsing the YAML configuration file.
    """
    log.info("Loading configuration for pipeline '%s'", name, stage="Start")

    if not (configs_folder / f"{name}.yml").exists():
        err = f"Configuration for pipeline '{name}' not found in {configs_folder}"
        log.error(err)
        raise FileNotFoundError(err)

    try:
        with (configs_folder / f"{name}.yml").open(encoding="utf-8") as config_file:
            config = yaml.safe_load(config_file)
    except yaml.YAMLError as exc:
        log.error("Failed to parse configuration for pipeline '%s': %s", name, exc)
        raise

    pipeline_config = PipelineConfig(**config)

    log.info("Configuration for pipeline '%s' loaded successfully", name, stage="End")
    return pipeline_config


def setup_run_config(configs_folder: Path) -> RunConfig:
    log.info("Setting up run configuration", stage="Start")

    settings = SettingsConfig()
    log.info(
        "Loaded DAG settings: Running '%s' pipeline in %s",
        settings.PIPELINE_NAME,
        settings.ENV,
    )

    pipeline = load_pipeline_config(
        configs_folder=configs_folder, name=settings.PIPELINE_NAME
    )
    log.info("Loaded pipeline configuration for '%s'", pipeline.name)

    run_config = create_run_config(
        pipeline=pipeline,
        settings=settings,
    )

    log.info("Run configuration set up successfully", stage="End")
    return run_config
