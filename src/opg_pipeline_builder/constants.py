ALLOWED_DATA_TYPES: tuple[str, ...] = (
    "string",
    "list<string>",
    "int32",
    "int64",
    "float64",
    "bool",
    "date32",
    "timestamp(ns)",
    "timestamp(ms)",
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
