from __future__ import annotations

import os
import sys
from pathlib import Path

src = Path(__file__).resolve().parents[1] / "src"
if str(src) not in sys.path:
    sys.path.insert(0, str(src))

from pyspark.sql import SparkSession

from src.data_inspect.source_specifications import sourceSpecs, infer_spec
from src.data_inspect.profile import run_profile


def main() -> None:
    kind = os.environ.get("SOURCE_TYPE", "csv")
    path = os.environ.get("SOURCE_PATH")
    if path:
        spec = infer_spec(path) if kind == "infer" else sourceSpecs(type=kind, path=path)
    else:
        spec = sourceSpecs(
            type=kind,
            url=os.environ.get("SOURCE_URL"),
            query=os.environ.get("SOURCE_QUERY"),
            table=os.environ.get("SOURCE_TABLE"),
        )
    spark = SparkSession.builder.getOrCreate()
    profile = run_profile(spark, spec, backend="databricks")
    out = os.environ.get("OUTPUT_PATH")
    text = profile.model_dump_json(indent=2)
    Path(out).write_text(text) if out else print(text)


if __name__ == "__main__":
    main()