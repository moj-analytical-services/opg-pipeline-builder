from datetime import date, datetime

ALLOWED_SEMANTIC_TYPES: tuple[str, ...] = (
    "date",
    "time",
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

ALLOWED_DATA_TYPES: tuple[type, ...] = (
    str,
    list[str],
    list[list[str]],
    int,
    list[int],
    float,
    list[float],
    bool,
    list[bool],
    date,
    list[date],
    datetime,
    type(None),
)

ALLOWED_STRUCT_DATA_TYPES: tuple[str, ...] = ("list[dict[",)

ALLOWED_VALUE_FORMATS: tuple[str, ...] = ("date-time",)

ALLOWED_FILE_FORMATS: tuple[str, ...] = ("parquet", "csv", "json", "xlsx")
