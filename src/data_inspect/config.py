from __future__ import annotations
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    spark: str = os.getenv("SPARK_KEY", "local")
    agent: str = os.getenv("AGENT_API_KEY", "abc1234")
    sample_rows: int = int(os.getenv("SAMPLE_ROWS"), "100")
    fill_rate_threshold: float = float(os.getenv("FILL_RATE_THRESHOLD", "0.7"))
    outlier_rate_threshold: float = float(os.getenv("OUTLIER_RATE_THRESHOLD", "0.2"))
    spark_app_name : str = os.getenv("SPARK_APP_NAME", "data_insights")
    spark_jars: str | None = os.getenv("SPARK_JARS")

def get_settings() -> Settings:
    return Settings()
    