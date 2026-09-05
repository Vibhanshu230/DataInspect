from __future__ import annotations  
from pyspark.sql import SparkSession
from data_inspect.config import get_settings

def get_spark(backend: str | None = None) -> SparkSession:
    settings = get_settings()
    chosen = (backend or settings.spark).lower()

    if chosen == "local":
        builder = (
            SparkSession.builder
            .master("local[*]")
            .appName(settings.spark_app_name)
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