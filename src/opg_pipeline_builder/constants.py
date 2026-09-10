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

ALLOWED_DATA_TYPES: tuple[str, ...] = (
    "string",
    "list<string>",
    "list<list<string>>",
    "int32",
    "list<int32>",
    "int64",
    "float64",
    "list<float64>",
    "decimal128(12,2)",
    "bool",
    "list<bool>",
    "date32",
    "list<date32>",
    "timestamp(ns)",
    "timestamp(ms)",
    "timestamp(s)",
    "datetime",
    "null",
)

ALLOWED_STRUCT_DATA_TYPES: tuple[str, ...] = ("list<struct<",)

ALLOWED_VALUE_FORMATS: tuple[str, ...] = ("date-time",)

ALLOWED_FILE_FORMATS: tuple[str, ...] = ("parquet", "csv", "json", "xlsx")
