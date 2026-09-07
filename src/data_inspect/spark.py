from __future__ import annotations
import os
import sys

from pyspark.sql import SparkSession
from data_inspect.config import get_settings


def pin_worker_python() -> None:
    """Keep Spark workers on the same interpreter as the driver."""
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def get_spark(backend: str | None = None) -> SparkSession:
    settings = get_settings()
    chosen = (backend or settings.spark).lower()

    if chosen == "local":
        pin_worker_python()
        builder = (
            SparkSession.builder.master("local[*]")
            .appName(settings.spark_app_name)
            .config("spark.pyspark.python", sys.executable)
            .config("spark.pyspark.driver.python", sys.executable)
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
        )

        if settings.spark_jars:
            builder = builder.config("spark.jars", settings.spark_jars)
        return builder.getOrCreate()
    
    if chosen == "databricks":
        try:
            from databricks.connect import DatabricksSession
        except ImportError as exc:
            raise RuntimeError(
                "This env has no Databricks Connect."
                "Use .venv + local pyspark, or a separate .venv-databricks"
            )
        return DatabricksSession.builder.getOrCreate()
    raise ValueError(f"Unknown spark backend : {chosen!r}")