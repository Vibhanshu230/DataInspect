from __future__ import annotations

import tempfile
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import RedirectResponse

from data_inspect import __version__
from data_inspect.agents import get_agent
from data_inspect.config import get_settings
from data_inspect.source_specifications import READERS, infer_spec
from data_inspect.jobs import STORE, run_job, submit_async
from data_inspect.models import AnalyzeRequest, AnalyzeResponse, AskRequest, AskResponse

app = FastAPI(
    title="data_inspect",
    description="Spark computes metrics. The agent only reads that JSON.",
    version=__version__,
)


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse("/docs")


@app.get("/health")
def health():
    s = get_settings()
    return {
        "status": "ok",
        "version": __version__,
        "spark_backend": s.spark,
        "agent_backend": s.agent,
        "source_types": sorted(READERS),
    }


@app.post("/v1/analyze", response_model=AnalyzeResponse)
def analyze(payload: AnalyzeRequest) -> AnalyzeResponse:
    if payload.async_run:
        return AnalyzeResponse(**submit_async(payload.source, payload.backend, payload.summarize).model_dump())
    job = STORE.create(payload.source, payload.backend)
    return AnalyzeResponse(**run_job(job.job_id, payload.source, payload.backend, payload.summarize).model_dump())


@app.post("/v1/analyze/upload", response_model=AnalyzeResponse)
async def analyze_upload(
    file: UploadFile = File(...),
    backend: str = Form("local"),
    summarize: bool = Form(True),
    async_run: bool = Form(False),
) -> AnalyzeResponse:
    suffix = Path(file.filename or "upload.csv").suffix or ".csv"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(await file.read())
        path = tmp.name
    return analyze(AnalyzeRequest(source=infer_spec(path), backend=backend, summarize=summarize, async_run=async_run))


@app.get("/v1/jobs/{job_id}", response_model=AnalyzeResponse)
def get_job(job_id: str) -> AnalyzeResponse:
    job = STORE.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Unknown job_id")
    return AnalyzeResponse(**job.model_dump())


@app.post("/v1/ask", response_model=AskResponse)
def ask(payload: AskRequest) -> AskResponse:
    job = STORE.get(payload.job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Unknown job_id")
    if job.status != "succeeded" or job.profile is None:
        raise HTTPException(status_code=409, detail=f"Job is {job.status}")
    agent = get_agent()
    return AskResponse(
        job_id=payload.job_id,
        question=payload.question,
        answer=agent.answer(job.profile, payload.question),
        backend=agent.name,
    )