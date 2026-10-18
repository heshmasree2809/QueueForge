"""HTTP API and dashboard."""
import hmac
import json
import logging
import re
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Response
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import text

from . import jobs
from .config import settings
from .db import engine, init_db
from .handlers import HANDLERS
from .worker import WorkerPool

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
STATIC = Path(__file__).parent / "static"
QUEUE_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    pool = None
    if settings.EMBEDDED_WORKERS:
        pool = WorkerPool()
        pool.start()
    yield
    if pool:
        pool.stop()


app = FastAPI(title="QueueForge", version="1.0.0", lifespan=lifespan)


def require_key(x_api_key: Optional[str] = Header(default=None)) -> None:
    """If API_KEY is configured, state-changing endpoints require a matching X-API-Key header."""
    if settings.API_KEY and not hmac.compare_digest(x_api_key or "", settings.API_KEY):
        raise HTTPException(status_code=401, detail="invalid or missing API key")


class JobIn(BaseModel):
    type: str = Field(min_length=1, max_length=64)
    payload: dict[str, Any] = Field(default_factory=dict)
    queue: str = "default"
    priority: int = Field(default=0, ge=-100, le=100)
    max_attempts: int = Field(default=5, ge=1, le=20)
    delay_seconds: float = Field(default=0, ge=0, le=86400)
    idempotency_key: Optional[str] = Field(default=None, min_length=1, max_length=200)

    @field_validator("type")
    @classmethod
    def known_type(cls, v: str) -> str:
        if v not in HANDLERS:
            raise ValueError(f"unknown job type; available: {sorted(HANDLERS)}")
        return v

    @field_validator("queue")
    @classmethod
    def valid_queue(cls, v: str) -> str:
        if not QUEUE_RE.match(v):
            raise ValueError("queue must match [A-Za-z0-9_.-]{1,64}")
        return v

    @field_validator("payload")
    @classmethod
    def small_payload(cls, v: dict) -> dict:
        if len(json.dumps(v)) > 16384:
            raise ValueError("payload must be at most 16 KB")
        return v


@app.post("/api/v1/jobs", status_code=201, dependencies=[Depends(require_key)])
def create_job(body: JobIn, response: Response) -> dict:
    job, created = jobs.enqueue(
        body.type,
        body.payload,
        queue=body.queue,
        priority=body.priority,
        max_attempts=body.max_attempts,
        delay_seconds=body.delay_seconds,
        idempotency_key=body.idempotency_key,
    )
    if not created:
        response.status_code = 200  # same idempotency key: return the original job
    return job


@app.get("/api/v1/jobs/{job_id}")
def read_job(job_id: int) -> dict:
    job = jobs.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return job


@app.get("/api/v1/jobs")
def read_jobs(
    status: Optional[str] = Query(default=None, pattern="^(queued|running|succeeded|dead)$"),
    limit: int = Query(default=50, ge=1, le=200),
) -> list[dict]:
    return jobs.list_jobs(status, limit)


@app.post("/api/v1/jobs/{job_id}/retry", dependencies=[Depends(require_key)])
def retry_job(job_id: int) -> dict:
    if jobs.get_job(job_id) is None:
        raise HTTPException(status_code=404, detail="job not found")
    job = jobs.retry_dead(job_id)
    if job is None:
        raise HTTPException(status_code=409, detail="only dead jobs can be retried")
    return job


@app.get("/api/v1/stats")
def read_stats(queue: str = Query(default="default", pattern=r"^[A-Za-z0-9_.-]{1,64}$")) -> dict:
    return jobs.stats(queue)


@app.get("/health")
def health() -> dict:
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception:
        raise HTTPException(status_code=503, detail="database unavailable")
    return {"status": "ok"}


@app.get("/", include_in_schema=False)
def dashboard() -> FileResponse:
    return FileResponse(STATIC / "index.html")
