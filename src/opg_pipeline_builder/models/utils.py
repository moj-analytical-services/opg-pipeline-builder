from logging import getLogger

from jinja2 import StrictUndefined, Template
from pydantic import ValidationInfo

from opg_pipeline_builder.logging.log import ModuleLogger
from opg_pipeline_builder.validation.validators import is_valid_s3_path

log = ModuleLogger(logger=getLogger(__name__))


def table_name(info: ValidationInfo) -> str:
    """Extract the table name from the validation context."""
    table: str = (info.context or {}).get("table_name", "")
    return table


def field_name(info: ValidationInfo) -> str:
    """Extract the field name from Pydantic validation information."""
    return info.field_name or ""


def render_s3_path(s3_path: str, env: str, db: str) -> str:
    """Render an S3 path using Jinja2 templates and the provided context."""
    template_path: Template = Template(s3_path, undefined=StrictUndefined)
    rendered_path = template_path.render(env=env, db=db)

    err = is_valid_s3_path(rendered_path, db)
    if err:
        log.error(f"Invalid S3 path: {err}")
        raise ValueError(f"Invalid S3 path: {err}")

    return rendered_path
