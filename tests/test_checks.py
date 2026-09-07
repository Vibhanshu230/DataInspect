from __future__ import annotations

from pyspark.sql import SparkSession

from data_inspect.checks import date_fail_rate, looks_boolean, true_rate
from data_inspect.config import Settings
from data_inspect.profile import run_profile


def test_true_rate(spark: SparkSession):
    df = spark.createDataFrame([("Y",), ("N",), ("true",), (None,), ("1",)], ["default_flag"])
    assert true_rate(df, "default_flag") == 0.75


def test_looks_boolean():
    assert looks_boolean("string", ["Y", "N", "true", None])
    assert not looks_boolean("string", ["Y", "maybe"])


def test_date_fail_rate(spark: SparkSession):
    df = spark.createDataFrame(
        [("2024-01-15",), ("not-a-date",), (None,)],
        ["application_date"],
    )
    assert date_fail_rate(df, "application_date", original_nulls=1, row_count=3) == 1 / 3


def test_messy(spark: SparkSession, messy_csv: str):
    # 4/15 SSNs are missing (~73% fill); the default 0.7 threshold would miss it.
    p = run_profile(spark, messy_csv, settings=Settings(fill_rate_threshold=0.8))
    assert p.row_count == 15
    assert p.duplicate_row_count >= 1
    assert "constant:country" in p.flags
    assert any(f.startswith("low_fill:ssn") for f in p.flags)
    assert "bad_dates:application_date" in p.flags


def test_clean(spark: SparkSession, clean_csv: str):
    p = run_profile(spark, clean_csv)
    assert next(c for c in p.columns if c.name == "ssn").fill_rate == 1.0
    assert p.duplicate_row_count == 0