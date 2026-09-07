from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from data_inspect.source_specifications import load_source


def test_records(spark: SparkSession):
    df = load_source(spark, {"type": "records", "records": [{"a": 1}, {"a": 2, "b": "x"}]})
    assert set(df.columns) == {"a", "b"}
    assert df.count() == 2


def test_records_all_null_column(spark: SparkSession):
    df = load_source(spark, {"type": "records", "records": [{"id": 1}, {"id": 2, "ssn": None}]})
    assert set(df.columns) == {"id", "ssn"}
    assert df.count() == 2


def test_csv(spark: SparkSession, messy_csv: str):
    assert load_source(spark, messy_csv).count() == 15


def test_unknown(spark: SparkSession):
    with pytest.raises(ValueError, match="Unknown source type"):
        load_source(spark, {"type": "xlsx", "path": "a.xlsx"})