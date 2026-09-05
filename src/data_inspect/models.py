from __future__ import annotations
from typing import Any, Literal
from pydantic import BaseModel, Field
from data_inspect.source_specifications import sourceSpecs

class NumericState(BaseModel):
    min_value: float | None = None
    max_value: float | None = None
    mean: float | None = None
    stddev: float | None = None
    q1: float | None = None
    q3: float | None = None
    iqr_outlier_rate: float | None = None

class StringStats(BaseModel):
    empty_rate: float = 0.0
    min_length: int | None = None
    max_length: int | None = None

class ColumnProfile(BaseModel):
    name: str
    spark_type: str
    null_count: int
    null_rate: float
    fill_rate: float
    distinct_count: int
    uniqueness: float | None = None
    is_constant: bool = False
    true_rate: float | None = None
    numeric: NumericStats | None = None
    string: StringStats | None = None
    date_parse_failure_rate: float | None = None
class DatasetProfile(BaseModel):
    source_path: str
    source_type: str
    backend: str
    row_count: int
    column_count: int
    duplicate_row_count: int
    duplicate_row_rate: float
    columns: list[ColumnProfile]
    flags: list[str] = Field(default_factory=list)
    sample_rows: list[dict[str, Any]] = Field(default_factory=list)
    recommended_checks: list[str] = Field(default_factory=list)
class QualitySummary(BaseModel):
    headline: str
    narrative: str
    key_findings: list[str] = Field(default_factory=list)
    questions_to_ask: list[str] = Field(default_factory=list)
    recommended_checks: list[str] = Field(default_factory=list)

class AnalyzeRequest(BaseModel):
    source: sourceSpecs
    backend: str = "local"
    summarize: bool = True
    async_run: bool = False
class AskRequest(BaseModel):
    job_id: str
    question: str
class JobStatus(BaseModel):
    job_id: str
    status: Literal["pending", "running", "succeeded", "failed"]
    backend: str
    source_type: str
    path: str | None = None
    profile: DatasetProfile | None = None
    summary: QualitySummary | None = None
    error: str | None = None
class AnalyzeResponse(JobStatus):
    pass
class AskResponse(BaseModel):
    job_id: str
    question: str
    answer: str
    backend: str