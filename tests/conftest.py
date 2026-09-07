from __future__ import annotations

from pathlib import Path
import sys

import pytest
from pyspark.sql import SparkSession

from data_inspect.spark import pin_worker_python



SAMPLES = Path(__file__).resolve().parents[1] / "samples"


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    pin_worker_python()
    session = (
        SparkSession.builder.master("local[1]")
        .appName("dq-tests")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def messy_csv() -> str:
    return str(SAMPLES / "messy_credit.csv")


@pytest.fixture
def clean_csv() -> str:
    return str(SAMPLES / "clean_credit.csv")