from __future__ import annotations

import threading
import uuid

from data_inspect.agents import get_agent
from data_inspect.source_specifications import sourceSpecs
from data_inspect.models import DatasetProfile, JobStatus, QualitySummary
from data_inspect.profile import run_profile
from data_inspect.spark import get_spark


class JobStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._jobs: dict[str, JobStatus] = {}

    def create(self, spec: sourceSpecs, backend: str) -> JobStatus:
        job = JobStatus(
            job_id=str(uuid.uuid4()),
            status="pending",
            backend=backend,
            source_type=spec.type,
            path=spec.path,
        )
        with self._lock:
            self._jobs[job.job_id] = job
        return job

    def get(self, job_id: str) -> JobStatus | None:
        with self._lock:
            job = self._jobs.get(job_id)
            return job.model_copy(deep=True) if job else None

    def update(self, job_id: str, **kwargs) -> JobStatus:
        with self._lock:
            updated = self._jobs[job_id].model_copy(update=kwargs)
            self._jobs[job_id] = updated
            return updated.model_copy(deep=True)

    def put_result(self, job_id: str, profile: DatasetProfile, summary: QualitySummary | None) -> JobStatus:
        return self.update(job_id, status="succeeded", profile=profile, summary=summary, error=None)


STORE = JobStore()


def run_job(job_id: str, spec: sourceSpecs, backend: str, summarize: bool) -> JobStatus:
    STORE.update(job_id, status="running")
    try:
        profile = run_profile(get_spark(backend), spec, backend=backend)
        summary = get_agent().summarize(profile) if summarize else None
        return STORE.put_result(job_id, profile, summary)
    except Exception as exc:
        return STORE.update(job_id, status="failed", error=str(exc))


def submit_async(spec: sourceSpecs, backend: str, summarize: bool) -> JobStatus:
    job = STORE.create(spec, backend)
    threading.Thread(target=run_job, args=(job.job_id, spec, backend, summarize), daemon=True).start()
    return job