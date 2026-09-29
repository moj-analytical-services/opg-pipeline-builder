from pydantic import ValidationInfo


def table_name(info: ValidationInfo) -> str:
    """Extract the table name from the validation context."""
    table: str = (info.context or {}).get("table_name", "")
    return table


def field_name(info: ValidationInfo) -> str:
    """Extract the field name from Pydantic validation information."""
    return info.field_name or ""
