from typing import Literal

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


ALLOWED_FILE_FORMATS: tuple[str, ...] = ("parquet", "csv", "json", "xlsx")

type ETLStage = Literal["raw", "curated"]
ALLOWED_ETL_STAGES: tuple[ETLStage, ...] = ("raw", "curated")
