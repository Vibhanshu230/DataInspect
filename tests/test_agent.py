from __future__ import annotations

from data_inspect.agents import MockAgent
from data_inspect.models import ColumnProfile, DatasetProfile


def _p() -> DatasetProfile:
    return DatasetProfile(
        source_path="memory",
        source_type="records",
        backend="local",
        row_count=10,
        column_count=1,
        duplicate_row_count=0,
        duplicate_row_rate=0.0,
        columns=[
            ColumnProfile(
                name="ssn",
                spark_type="string",
                null_count=3,
                null_rate=0.3,
                fill_rate=0.7,
                distinct_count=7,
            )
        ],
        flags=["low_fill:ssn"],
    )


def test_summary():
    assert "10 rows" in MockAgent().summarize(_p()).key_findings[0]


def test_ask_fill():
    assert "ssn" in MockAgent().answer(_p(), "which columns are below 95% fill rate?")