from __future__ import annotations
from pyspark.sql import SparkSession

from data_inspect import checks
from data_inspect.config import Settings, get_settings
from data_inspect.source_specifications import sourceSpecs, infer_spec, load_source
from data_inspect.models import ColumnProfile, DatasetProfile

def _as_spec(source: sourceSpecs | dict | str) -> sourceSpecs:
    if isinstance(source, sourceSpecs):
        return source
    if isinstance(source, str):
        return infer_spec(source)
    return sourceSpecs.model_validate(source)

def run_profile(spark: SparkSession, source: sourceSpecs | dict | str, backend: str = "local", settings: Settings | None = None, key:str | list[str] | None = None) -> DatasetProfile:
    settings = settings or get_settings()
    spec = _as_spec(source)
    df = load_source(spark, spec)

    row_count = df.count()
    dtypes = {f.name : f.dataType for f in df.schema.fields}
    type_names = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    
    complete = checks.completeness(df)
    dup_count, dup_rate = checks.duplicate_rows(df, row_count)

    key_count: int | None = None
    key_rate: float | None = None
    if key is not None:
        key_count, key_rate = checks.duplicate_rows(df, row_count, key = key)

    columns: list[ColumnProfile] = []
    for name in df.columns:
        null_count, distinct_count = complete.get(name, (0,0))
        null_rate = (null_count/row_count) if row_count else 0.0
        non_null = row_count - null_count
        dtype = dtypes[name]
        spark_type = type_names[name]

        true = None
        cheap_boolean = spark_type == "boolean" or distinct_count <= len(checks.BOOL_TOKENS)
        if cheap_boolean and checks.looks_boolean(spark_type, checks.distinct_strings(df, name)):
            true = checks.true_rate(df, name)
        
        uniq = checks.uniqueness(distinct_count, non_null)
        date_fail = None
        if checks.looks_date(name, dtype) and checks.is_string(dtype):
            date_fail = checks.date_fail_rate(df, name, null_count, row_count)
        
        columns.append(
            ColumnProfile(
                name = name,
                spark_type = spark_type,
                null_count = null_count,
                null_rate = round(null_rate, 6),
                fill_rate = round(1.0 - null_rate, 6),
                distinct_count = distinct_count,
                uniqueness = None if uniq is None else round(uniq, 6),
                is_constant = distinct_count <= 1,
                true_rate = None if true is None else round(true, 6),
                numeric=checks.numeric_stats(df, name) if checks.is_numeric(dtype) else None,
                string=checks.string_stats(df, name, row_count) if checks.is_string(dtype) else None,
                date_parse_failure_rate = None if date_fail is None else round(date_fail, 6),
            )
        )

    flags = checks.build_flags(columns, settings)
    if key_rate and key_rate > 0:
        label = key if isinstance(key, str) else "+".join(key)
        flags.append(f"dup_key: {label}")
    
    source_label = spec.path or spec.table or spec.query or spec.type
    sample = [r.asDict(recursive = True) for r in df.limit(settings.sample_rows).collect()]

    return DatasetProfile(
        source_path=str(source_label),
        source_type=spec.type.lower(),
        backend=backend,
        row_count=row_count,
        column_count=len(df.columns),
        duplicate_row_count=dup_count,
        duplicate_row_rate=round(dup_rate, 6),
        identifier=key,
        duplicate_key_count=key_count,
        duplicate_key_rate=None if key_rate is None else round(key_rate, 6),
        columns=columns,
        flags=flags,
        sample_rows=sample,
        recommended_checks=checks.recommend(flags),
    )

