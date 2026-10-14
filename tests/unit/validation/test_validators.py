import pytest

from opg_pipeline_builder.constants import ALLOWED_ENVS
from opg_pipeline_builder.validation import validators as v


@pytest.mark.parametrize(
    ("column_name"),
    [
        ("address"),
        ("full_address"),
        ("address_1"),
        ("t"),
        ("a_very_long_table_name_that_is_under_sixty_three_characters"),
    ],
)
def test_valid_identifier_valid(column_name: str) -> None:
    """Test that a valid identifier returns an empty string"""
    assert v.is_valid_identifier(column_name) == ""


@pytest.mark.parametrize(
    ("column_name", "exc_msg"),
    [
        ("", "Identifier cannot be empty"),
        ("with internal spaces", "Identifier cannot contain whitespace characters"),
        (" leading_space", "Identifier cannot contain whitespace characters"),
        ("trailing_space ", "Identifier cannot contain whitespace characters"),
        ("tab\tcharacter", "Identifier cannot contain whitespace characters"),
        ("newline\ncharacter", "Identifier cannot contain whitespace characters"),
        ("special!@#characters", "Identifier contains invalid characters"),
        ("unicode\u2603character", "Identifier contains invalid characters"),
        ("emoji😀character", "Identifier contains invalid characters"),
        ("calf\u00e9", "Identifier contains invalid characters"),
        ("Upper_Case", "Identifier must be lowercase"),
        ("1starts_with_number", "Identifier cannot start with a number or underscore"),
        (
            "_starts_with_underscore",
            "Identifier cannot start with a number or underscore",
        ),
        ("a" * 64, "Identifier must not exceed 63 characters"),
    ],
)
def test_valid_identifier_invalid(column_name: str, exc_msg: str) -> None:
    """Test that an invalid identifier returns the correct error message"""
    assert v.is_valid_identifier(column_name) == exc_msg


def test_s3_path_template_valid() -> None:
    """Test that a valid S3 path template returns an empty string"""
    valid_template = "s3://bucket/{{ env }}/{{ db }}/land/"
    assert v.is_valid_s3_path_template(valid_template, "land_path") == ""


@pytest.mark.parametrize(
    ("filepath", "path_field", "exp_error"),
    [
        ("", "land_path", "S3 path cannot be empty"),
        (
            "bucket-name/{{ env }}/{{ db }}/archive/",
            "archive_path",
            "S3 path must start with 's3://'",
        ),
        (
            "s3:/bucket-name/{{ env }}/{{ db }}/curated/",
            "curated_path",
            "S3 path must start with 's3://'",
        ),
        (
            "s3://bucket-name/{{ db }}/land/",
            "land_path",
            "S3 path must contain an environment variable placeholder '{{ env }}' as a subdirectory",
        ),
        (
            "s3://bucket-name/{{ env }}/archive/",
            "archive_path",
            "S3 path must contain a pipeline name variable placeholder '{{ db }}' as a subdirectory",
        ),
        (
            "s3://bucket-name/{{ env }}/{{ db }}/",
            "land_path",
            "S3 path must contain the corresponding etl stage 'land' as a subdirectory",
        ),
        (
            "s3://bucket-name/{{ env }}/{{ db }}/invalid/",
            "archive_path",
            "S3 path must contain the corresponding etl stage 'archive' as a subdirectory",
        ),
        (
            "s3://bucket-name/{{ env }}/{{ db }}/land/",
            "curated_path",
            "S3 path must contain the corresponding etl stage 'curated' as a subdirectory",
        ),
    ],
)
def test_s3_path_template_invalid(
    filepath: str, path_field: str, exp_error: str
) -> None:
    """Test that an invalid S3 path template returns the correct error message"""
    err = v.is_valid_s3_path_template(filepath, path_field)
    assert err == exp_error


def test_s3_path_valid() -> None:
    """Test that a valid S3 path returns an empty string"""
    valid_path = "s3://bucket-name/test/my_pipeline/etl_stage/"
    assert v.is_valid_s3_path(valid_path, "my_pipeline") == ""


@pytest.mark.parametrize(
    ("filepath", "exp_error"),
    [
        (
            "s3://bucket-name/my_pipeline/etl_stage/",
            f"S3 path must contain one of the allowed environments: {', '.join(ALLOWED_ENVS)} as a subdirectory",
        ),
        (
            "s3://bucket-name/dev/my_pipeline/etl_stage/",
            f"S3 path must contain one of the allowed environments: {', '.join(ALLOWED_ENVS)} as a subdirectory",
        ),
        (
            "s3://bucket-name/test/invalid_pipeline/etl_stage/",
            "S3 path must contain the pipeline name 'my_pipeline' as a subdirectory",
        ),
    ],
)
def test_s3_path_invalid(filepath: str, exp_error: str) -> None:
    """Test that an invalid S3 path returns the correct error message"""
    err = v.is_valid_s3_path(filepath, "my_pipeline")
    assert err == exp_error
