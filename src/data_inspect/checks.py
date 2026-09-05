from __future__ import annotations
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, NumericType, StringType, TimestampType
from data_inspect.config import Settings
from data_inspect.models import ColumnProfile, NumericStats, StringStats



TRUE_TOKEN_TYPES: ("true", "1", "yes")
FALSE_TOKEN_TYPES : ("false", "0", "no")
BOOL_TOKENS = set(TRUE_TOKEN_TYPES + FALSE_TOKEN_TYPES)
DATE_HINTS = ("date", "_dt", "dt_", "time", "ts", "datetime")

def completeness(df: DataFrame) -> dict[str, tuple[int, int]]:
    if not df.columns:
        return {}

    exprs = []
    for col in df.columns:
        exprs.append(F.sum(F.col(col).isNull().cast("long")).alias(f"{col}__nulls"))
        exprs.append(F.countDistinct(col).alias(f"{col}__distinct"))
    row = df.agg(*exprs).collect()[0]

    return {
        col: (int(row[f'{col}__nulls'] or 0), int(row[f"{col}__distinct"] or 0))
        for col in df.columns
    }