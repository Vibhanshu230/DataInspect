from __future__ import annotations
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, NumericType, StringType, TimestampType
from data_inspect.config import Settings
from data_inspect.models import ColumnProfile, NumericStats, StringStats



TRUE_TOKEN_TYPES = ("true", "1", "yes", "t", "y")
FALSE_TOKEN_TYPES = ("false", "0", "no", "n", "f")
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

def duplicate_rows(df: DataFrame, row_count: int, key: str | list[str] | None = None) -> tuple[int, float]:
    if row_count == 0:
        return 0, 0.0
    subset = [key] if isinstance(key, str) else key
    if subset:
        missing = [c for c in subset if c not in df.columns]
        if missing:
            raise ValueError(f"Unkown identifier columns : {missing}. Have {df.columns}")
    unique = df.dropDuplicates(subset).count() if subset else df.dropDuplicates().count()
    extra = row_count - unique
    return extra, extra/row_count

def uniqueness(distinct_count : int, non_null_count: int) -> float | None:
    if non_null_count == 0:
        return None
    return distinct_count/non_null_count

def looks_boolean(spark_type: str, values: list[str | None]) -> bool:
    if spark_type == "boolean":
        return True
    tokens = {v.strip().lower() for v in values if v is not None and str(v).strip()}
    return bool(tokens) and tokens.issubset(BOOL_TOKENS)

def distinct_strings(df: DataFrame, col: str, limit: int = 20) -> list[str | None]:
    rows = df.select(F.col(col).cast("string").alias(col)).distinct().limit(limit).collect()
    return [r[0] for r in rows]

def true_rate(df : DataFrame, col:str) -> float | None:
    labeled = df.filter(F.col(col).isNotNull() & (F.trim(F.col(col).cast("string")) != ""))
    n = labeled.count()
    if n == 0:
        return None
    is_true = F.lower(F.trim(F.col(col).cast("string"))).isin(list(TRUE_TOKEN_TYPES))
    return labeled.filter(is_true).count() / n

def numeric_stats(df: DataFrame, col : str) -> NumericStats:
    agg = df.agg(
        F.min(col).alias("min_value"),
        F.max(col).alias("max_value"),
        F.mean(col).alias("mean"),
        F.stddev(col).alias("stddev"),
    ).collect()[0]
    q1 = q3 = outlier_rate = None
    try:
        q1, q3 = df.approxQuantile(col, [0.25,0.75], 0.01)
        inter_quartile = q3 - q1
        if inter_quartile > 0:
            lower, upper= q1 - 1.5 * inter_quartile, q3 + 1.5 * inter_quartile
            non_null = df.filter(F.col(col).isNotNull())
            n = non_null.count()
            if n:
                outliers = non_null.filter((F.col(col) < lower) | (F.col(col) > upper)).count()
                outlier_rate = outliers/n
    except Exception:
        q1 = q3 = None

    def _f(v):
        return None if v is None else float(v)
    return NumericStats(
        min_value = _f(agg['min_value']),
        max_value = _f(agg['max_value']),
        mean=_f(agg["mean"]),
        stddev=_f(agg["stddev"]),
        q1=_f(q1),
        q3=_f(q3),
        iqr_outlier_rate=_f(outlier_rate),
    )

def string_stats(df: DataFrame, col: str, row_count: int) -> StringStats:
    if row_count == 0:
        return StringStats()
    empty = F.col(col).isNotNull() & (F.trim(F.col(col)) == "")
    row = df.agg(
        F.sum(empty.cast("long")).alias("empties"),
        F.min(F.length(col)).alias("min_length"),
        F.max(F.length(col)).alias("max_length"),
    ).collect()[0]
    return StringStats(
        empty_rate=float(row["empties"] or 0) / row_count,
        min_length=int(row["min_length"]) if row["min_length"] is not None else None,
        max_length=int(row["max_length"]) if row["max_length"] is not None else None,
    )

def looks_date(name: str, dtype) -> bool:
    if isinstance(dtype, (DateType, TimestampType)):
        return True
    if not isinstance(dtype, StringType):
        return False
    lowered = name.lower()
    return any(hint in lowered for hint in DATE_HINTS)


def date_fail_rate(df: DataFrame, col: str, original_nulls: int, row_count: int) -> float | None:
    if row_count == 0:
        return None
    # Spark 4 ANSI mode raises on malformed values; try_to_timestamp returns NULL instead.
    parsed_nulls = df.select(F.sum(F.try_to_timestamp(F.col(col)).isNull().cast("long"))).first()[0] or 0
    return max(int(parsed_nulls) - original_nulls, 0) / row_count

def build_flags(columns: list[ColumnProfile], settings: Settings) -> list[str]:
    flags: list[str] = []
    for col in columns:
        if col.fill_rate < settings.fill_rate_threshold:
            flags.append(f"low_fill:{col.name}")
        if col.is_constant:
            flags.append(f"constant:{col.name}")
        if col.numeric and (col.numeric.iqr_outlier_rate or 0) > settings.outlier_rate_threshold:
            flags.append(f"outliers:{col.name}")
        if col.true_rate is not None and col.true_rate in {0.0, 1.0}:
            flags.append(f"degenerate_boolean:{col.name}")
        if col.date_parse_failure_rate and col.date_parse_failure_rate > 0:
            flags.append(f"bad_dates:{col.name}")
        if col.uniqueness == 1.0 and col.name.endswith("_id"):
            flags.append(f"pk_candidate:{col.name}")
    return flags

def recommend(flags: list[str]) -> list[str]:
    recs: list[str] = []
    if any(f.startswith("low_fill:") for f in flags):
        recs.append("Trace low-fill columns to the source system.")
    if any(f.startswith("outliers:") for f in flags):
        recs.append("Spot-check IQR outliers vs real heavy tails.")
    if any(f.startswith("constant:") for f in flags):
        recs.append("Drop or partition on constant columns.")
    if any(f.startswith("bad_dates:") for f in flags):
        recs.append("Cast dates in ingestion and quarantine failures.")
    if not any(f.startswith("pk_candidate:") for f in flags):
        recs.append("Measure duplicate-key rate on a natural key.")
    return recs


def is_numeric(dtype) -> bool:
    return isinstance(dtype, NumericType)
def is_string(dtype) -> bool:
    return isinstance(dtype, StringType)