from datetime import date, datetime

ALLOWED_SEMANTIC_TYPES: tuple[str, ...] = (
    "date",
    "datetime",
    "timestamp",
    "period_of_time",
    "quantity",
    "full_address",
    "town",
    "county",
    "country",
    "postcode",
    "string_classification",
    "integer_classification",
    "generic_string",
    "boolean_flag",
    "numeric_identifier",
    "string_identifier",
)

ALLOWED_ETL_STAGES: tuple[str, ...] = ("raw", "curated")

ALLOWED_DATA_TYPES: dict[str, type] = {
    "str": str,
    "list[str]": list[str],
    "list[list[str]]": list[list[str]],
    "int": int,
    "list[int]": list[int],
    "float": float,
    "list[float]": list[float],
    "bool": bool,
    "list[bool]": list[bool],
    "date": date,
    "list[date]": list[date],
    "datetime": datetime,
    "NoneType": type(None),
    "list[dict[str, str]]": list[dict[str, str]],
}

ALLOWED_VALUE_FORMATS: tuple[str, ...] = ("datetime", "date")

ALLOWED_FILE_FORMATS: tuple[str, ...] = ("parquet", "csv", "json", "xlsx")
