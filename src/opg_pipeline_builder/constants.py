ALLOWED_DATA_TYPES: tuple[str, ...] = (
    "string",
    "list<string>",
    "int32",
    "list<int32>",
    "int64",
    "float64",
    "list<float64>",
    "decimal128(12,2)",
    "bool",
    "date32",
    "timestamp(ns)",
    "timestamp(ms)",
    "null",
)

ALLOWED_STRUCT_DATA_TYPES: tuple[str, ...] = ("list<struct<",)

ALLOWED_ETL_STAGES: tuple[str, ...] = (
    "land",
    "raw",
    "raw_hist",
    "processed",
    "curated",
    "derived",
)


ALLOWED_FILE_FORMATS: tuple[str, ...] = ("parquet", "csv", "json", "xlsx")
