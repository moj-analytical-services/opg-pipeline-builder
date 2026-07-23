import logging
from typing import Optional

PACKAGE_LOGGER_NAME = "opg_pipeline_builder"
_CONSOLE_HANDLER_NAME = "opg_pipeline_builder_console"


def configure_logging() -> logging.Logger:
    """Configure package logging once with a shared console handler."""
    package_logger = logging.getLogger(PACKAGE_LOGGER_NAME)
    package_logger.setLevel(logging.INFO)
    package_logger.propagate = False

    if not any(
        _CONSOLE_HANDLER_NAME == handler.get_name()
        for handler in package_logger.handlers
    ):
        stream_handler = logging.StreamHandler()
        stream_handler.set_name(_CONSOLE_HANDLER_NAME)
        package_logger.addHandler(stream_handler)

        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s | %(name)s | %(funcName)s | %(levelname)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
    return package_logger


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Return a named logger; package logs share one configured console handler."""
    configure_logging()
    return logging.getLogger(name or PACKAGE_LOGGER_NAME)
