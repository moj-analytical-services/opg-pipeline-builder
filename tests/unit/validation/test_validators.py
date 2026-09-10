import pytest

from opg_pipeline_builder.validation import validators as v


@pytest.mark.parametrize(
    ("column_name"),
    [
        ("address"),
        ("full_address"),
        ("address_1"),
        ("_address"),
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
        ("UPPER_CASE", "Identifier must be lowercase"),
        ("1starts_with_number", "Identifier cannot start with a number"),
    ],
)
def test_valid_identifier_invalid(column_name: str, exc_msg: str) -> None:
    """Test that an invalid identifier returns the correct error message"""
    assert v.is_valid_identifier(column_name) == exc_msg
