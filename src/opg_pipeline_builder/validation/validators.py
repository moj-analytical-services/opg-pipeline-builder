from datetime import datetime

from opg_pipeline_builder.constants import ALLOWED_ENVS, MAX_IDENTIFIER_LENGTH


def is_valid_identifier(identifier: str) -> str:
    """Validate that the provided identifier is a valid SQL/Athena identifier.

    Checks:
    - Must not be empty
    - Must not contain any whitespace characters
    - Must contain only lowercase ASCII letters, digits, and underscores
    - Must not start with a number of underscore
    - Must be lowercase
    - Must not exceed 63 characters

    Returns an error message if invalid, or an empty string if valid.
    """
    errors = []
    if not identifier:
        return "Identifier cannot be empty."
    if any(char.isspace() for char in identifier):
        errors.append("Identifier cannot contain whitespace characters.")
    if not identifier.islower():
        errors.append("Identifier must be lowercase.")
    if identifier[0].isdigit() or identifier[0] == "_":
        errors.append("Identifier cannot start with a number or underscore.")
    if not all(
        character.isascii() and (character.isalnum() or character == "_")
        for character in identifier
    ):
        errors.append("Identifier contains invalid characters.")
    if len(identifier) > MAX_IDENTIFIER_LENGTH:
        errors.append(f"Identifier must not exceed {MAX_IDENTIFIER_LENGTH} characters.")
    return " ".join(errors)


def is_valid_s3_path_template(filepath: str, field: str = "") -> str:
    """Validate that the provided filepath template is valid.

    Checks:
    - Must not be empty
    - Must start with 's3://'
    - Must contain an environment variable placeholder
    - Must contain a database name variable placeholder
    - Must contain the etl stage as a subdirectory

    Returns an error message if invalid, or an empty string if valid.
    """
    if not filepath:
        return "S3 path cannot be empty"
    if not filepath.startswith("s3://"):
        return "S3 path must start with 's3://'"
    if "/{{ env }}/" not in filepath:
        return "S3 path must contain an environment variable placeholder '{{ env }}' as a subdirectory"
    if "/{{ db }}/" not in filepath:
        return "S3 path must contain a database name variable placeholder '{{ db }}' as a subdirectory"
    if f"/{field.split('_', maxsplit=1)[0]}/" not in filepath:
        return f"S3 path must contain the corresponding etl stage '{field.split('_', maxsplit=1)[0]}' as a subdirectory"
    return ""


def is_valid_s3_path(filepath: str, db_name: str) -> str:
    """Validate that the provided filepath is a valid S3 path.

    Checks:
    - Must contain one of the allowed environments as a subdirectory
    - Must contain the database name as a subdirectory

    Returns an error message if invalid, or an empty string if valid.
    """
    if not any(f"/{env}/" in filepath for env in ALLOWED_ENVS):
        return f"S3 path must contain one of the allowed environments: {', '.join(ALLOWED_ENVS)} as a subdirectory"
    if f"/{db_name}/" not in filepath:
        return f"S3 path must contain the database name '{db_name}' as a subdirectory"
    return ""


def is_timezone_aware(value: datetime) -> bool:
    """Return whether a datetime has a timezone and a defined UTC offset.

    Args:
        value: The datetime object to check.

    Returns:
        True if the datetime is timezone-aware, otherwise False.
    """
    return value.tzinfo is not None and value.utcoffset() is not None
