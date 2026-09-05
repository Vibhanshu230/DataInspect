from __future__ import annotations
from typing import Any
from pydantic import BaseModel, Field, model_validator

class sourceSpecs(BaseModel):
    """
    Pydantic model to accept various formats of sources
    """
    type: str = Field(description="csv | parquet | json | records | pickle | jdbc")
    path: str | None = None
    records: list[dict[str, Any]] | None = None
    options: dict[str, Any] = Field(default_factory=dict)

    url: str | None = None
    query: str | None = None
    table: str | None = None

    @model_validator(mode = "after")
    def payload(self) -> sourceSpecs:
        kind = self.type.lower()
        if kind == "records" and not self.records:
            raise ValueError("type=records requires records=[...]")
        if kind == "jdbc" and not self.url:
            raise ValueError("type=jdbc requires url")
        if kind == "jdbc" and not (self.query or self.table)
            raise ValueError("type=jdbc requires query or table")
        if kind in {"csv", "parquet", "json", "pickle"} and not self.path:
            raise ValueError(f"type={kind} requires path")
        return self


SUFFIX_TO_TYPE = {
    ".csv": "csv",
    ".tsv": "csv",
    ".parquet": "parquet",
    ".pq": "parquet",
    ".json": "json",
    ".jsonl": "json",
    ".pkl": "pickle",
    ".pickle": "pickle",
}

def infer_spec(path: str, **options) -> sourceSpecs:
    suffix = Path(path.split("?")[0]).suffix.lower()
    kind = SUFFIX_TO_TYPE.get(suffix)
    if kind is None:
        raise ValueError(f"Vannot infer type from {path!r}. Set source.type correctly")
    if suffix == ".tsv":
        options.setdefault("sep", "\t")
    return sourceSpecs(type = kind, path = path, options = options)

def _option(reader, options: dict):
    for key, value in options.items():
        reader = reader.option(key, str(value))
    return reader

def read_csv(spark: SparkSession, spec: SourceSpec) -> DataFrame:
    reader = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("nullValue", "")
        .option("emptyValue", "")
    )
    return _options(reader, spec.options).load(spec.path)
def read_parquet(spark: SparkSession, spec: SourceSpec) -> DataFrame:
    return _options(spark.read.format("parquet"), spec.options).load(spec.path)
def read_json(spark: SparkSession, spec: SourceSpec) -> DataFrame:
    return _options(spark.read.format("json"), spec.options).load(spec.path)
def read_records(spark: SparkSession, spec: SourceSpec) -> DataFrame:
    rows = spec.records or []
    if not rows:
        return spark.createDataFrame([], StructType([]))
    keys: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                keys.append(key)
    normalized = [{k: row.get(k) for k in keys} for row in rows]
    try:
        return spark.createDataFrame(normalized)
    except TypeError:
        schema = StructType([StructField(k, StringType(), True) for k in keys])
        as_str = [{k: None if r[k] is None else str(r[k]) for k in keys} for r in normalized]
        return spark.createDataFrame(as_str, schema=schema)
def read_pickle(spark: SparkSession, spec: SourceSpec) -> DataFrame:
    obj = pickle.loads(Path(spec.path).read_bytes())  # trusted local files only
    if hasattr(obj, "to_dict") and hasattr(obj, "columns"):
        return read_records(spark, SourceSpec(type="records", records=obj.to_dict(orient="records")))
    if isinstance(obj, list) and (not obj or isinstance(obj[0], dict)):
        return read_records(spark, SourceSpec(type="records", records=obj))
    raise TypeError(f"Pickle must be list[dict] or pandas.DataFrame, got {type(obj).__name__}")
def read_jdbc(spark: SparkSession, spec: SourceSpec) -> DataFrame:
    reader = spark.read.format("jdbc").option("url", spec.url)
    if spec.query:
        reader = reader.option("query", spec.query)
    else:
        reader = reader.option("dbtable", spec.table)
    return _options(reader, spec.options).load()
# New format: write read_excel() above, then add "excel": read_excel
READERS = {
    "csv": read_csv,
    "parquet": read_parquet,
    "json": read_json,
    "records": read_records,
    "pickle": read_pickle,
    "jdbc": read_jdbc,
}
def load_source(spark: SparkSession, spec: SourceSpec | dict | str) -> DataFrame:
    if isinstance(spec, str):
        spec = infer_spec(spec)
    elif isinstance(spec, dict):
        spec = SourceSpec.model_validate(spec)
    reader = READERS.get(spec.type.lower())
    if reader is None:
        raise ValueError(f"Unknown source type {spec.type!r}. Known: {', '.join(sorted(READERS))}")
    return reader(spark, spec)