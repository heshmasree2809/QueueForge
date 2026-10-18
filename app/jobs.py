"""Queue operations. All state lives in PostgreSQL; every function is safe to call from many processes.

Delivery semantics: AT-LEAST-ONCE. A job can run more than once (worker dies after finishing but
before acknowledging), so handlers must be idempotent.
"""
import json
import random
from typing import Any, Optional

from sqlalchemy import text

from .config import settings
from .db import engine

ENQUEUE_SQL = text(
    """
    INSERT INTO jobs (queue, type, payload, priority, max_attempts, run_at, idempotency_key)
    VALUES (:queue, :type, CAST(:payload AS JSONB), :priority, :max_attempts,
            now() + CAST(:delay AS double precision) * interval '1 second', :key)
    ON CONFLICT (idempotency_key) DO NOTHING
    RETURNING *
    """
)

# FOR UPDATE SKIP LOCKED lets many workers pull from the same table without blocking each other
# and without two workers ever receiving the same row.
CLAIM_SQL = text(
    """
    WITH next AS (
        SELECT id FROM jobs
        WHERE status = 'queued' AND queue = :queue AND run_at <= now()
        ORDER BY priority DESC, run_at, id
        LIMIT :limit
        FOR UPDATE SKIP LOCKED
    )
    UPDATE jobs j
    SET status = 'running',
        attempts = j.attempts + 1,
        locked_by = :worker,
        lease_expires_at = now() + CAST(:lease AS double precision) * interval '1 second',
        updated_at = now()
    FROM next
    WHERE j.id = next.id
    RETURNING j.*
    """
)

REAP_SQL = text(
    """
    UPDATE jobs
    SET status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END,
        run_at = now(),
        last_error = 'lease expired (worker presumed dead)',
        locked_by = NULL,
        lease_expires_at = NULL,
        updated_at = now(),
        finished_at = CASE WHEN attempts >= max_attempts THEN now() ELSE NULL END
    WHERE status = 'running' AND queue = :queue AND lease_expires_at < now()
    """
)

# The locked_by guard means a worker whose lease was already reclaimed cannot overwrite the new owner.
COMPLETE_SQL = text(
    """
    UPDATE jobs
    SET status = 'succeeded', result = CAST(:result AS JSONB), last_error = NULL,
        locked_by = NULL, lease_expires_at = NULL, updated_at = now(), finished_at = now()
    WHERE id = :id AND status = 'running' AND locked_by = :worker
    """
)

FAIL_SQL = text(
    """
    UPDATE jobs
    SET status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END,
        run_at = now() + CAST(:delay AS double precision) * interval '1 second',
        last_error = :error,
        locked_by = NULL,
        lease_expires_at = NULL,
        updated_at = now(),
        finished_at = CASE WHEN attempts >= max_attempts THEN now() ELSE NULL END
    WHERE id = :id AND status = 'running' AND locked_by = :worker
    """
)


def backoff_seconds(attempts: int, base: Optional[float] = None, cap: Optional[float] = None) -> float:
    """Exponential backoff with jitter: base * 2^(attempts-1), capped, then scaled by U(0.5, 1.0)."""
    base = settings.BACKOFF_BASE_SECONDS if base is None else base
    cap = settings.BACKOFF_CAP_SECONDS if cap is None else cap
    delay = min(cap, base * (2 ** max(0, attempts - 1)))
    return delay * random.uniform(0.5, 1.0)


def enqueue(
    type_: str,
    payload: Optional[dict] = None,
    queue: str = "default",
    priority: int = 0,
    max_attempts: int = 5,
    delay_seconds: float = 0.0,
    idempotency_key: Optional[str] = None,
) -> tuple[dict, bool]:
    """Insert a job. Returns (job, created). A repeated idempotency_key returns the original job."""
    params = {
        "queue": queue,
        "type": type_,
        "payload": json.dumps(payload or {}),
        "priority": priority,
        "max_attempts": max_attempts,
        "delay": float(delay_seconds),
        "key": idempotency_key,
    }
    with engine.begin() as conn:
        row = conn.execute(ENQUEUE_SQL, params).mappings().first()
        if row is not None:
            return dict(row), True
        row = conn.execute(
            text("SELECT * FROM jobs WHERE idempotency_key = :key"), {"key": idempotency_key}
        ).mappings().first()
        return dict(row), False


def claim(worker_id: str, queue: str = "default", lease_seconds: Optional[float] = None, limit: int = 1) -> list[dict]:
    lease = settings.LEASE_SECONDS if lease_seconds is None else lease_seconds
    with engine.begin() as conn:
        rows = conn.execute(
            CLAIM_SQL, {"queue": queue, "limit": limit, "worker": worker_id, "lease": float(lease)}
        ).mappings().all()
    return [dict(r) for r in rows]


def complete(job_id: int, worker_id: str, result: Optional[Any] = None) -> bool:
    with engine.begin() as conn:
        res = conn.execute(
            COMPLETE_SQL, {"id": job_id, "worker": worker_id, "result": json.dumps(result)}
        )
    return res.rowcount == 1


def fail(job: dict, worker_id: str, error: str) -> bool:
    """Record a failure. Requeues with backoff, or marks the job dead once attempts are exhausted."""
    delay = backoff_seconds(job["attempts"])
    with engine.begin() as conn:
        res = conn.execute(
            FAIL_SQL, {"id": job["id"], "worker": worker_id, "error": error[:2000], "delay": delay}
        )
    return res.rowcount == 1


def reap_expired(queue: str = "default") -> int:
    """Return jobs from crashed/stalled workers to the queue (or to 'dead' if out of attempts)."""
    with engine.begin() as conn:
        return conn.execute(REAP_SQL, {"queue": queue}).rowcount


def get_job(job_id: int) -> Optional[dict]:
    with engine.connect() as conn:
        row = conn.execute(text("SELECT * FROM jobs WHERE id = :id"), {"id": job_id}).mappings().first()
    return dict(row) if row else None


def list_jobs(status: Optional[str] = None, limit: int = 50) -> list[dict]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT * FROM jobs WHERE (CAST(:status AS TEXT) IS NULL OR status = :status) "
                "ORDER BY id DESC LIMIT :limit"
            ),
            {"status": status, "limit": limit},
        ).mappings().all()
    return [dict(r) for r in rows]


def retry_dead(job_id: int) -> Optional[dict]:
    """Manually requeue a dead job with a fresh attempt budget."""
    with engine.begin() as conn:
        row = conn.execute(
            text(
                "UPDATE jobs SET status = 'queued', attempts = 0, run_at = now(), "
                "finished_at = NULL, updated_at = now() WHERE id = :id AND status = 'dead' RETURNING *"
            ),
            {"id": job_id},
        ).mappings().first()
    return dict(row) if row else None


def stats(queue: str = "default") -> dict:
    with engine.connect() as conn:
        counts = {
            r[0]: r[1]
            for r in conn.execute(
                text("SELECT status, count(*) FROM jobs WHERE queue = :q GROUP BY status"), {"q": queue}
            )
        }
        row = conn.execute(
            text(
                """
                SELECT
                  (SELECT EXTRACT(EPOCH FROM now() - min(run_at)) FROM jobs
                    WHERE queue = :q AND status = 'queued' AND run_at <= now()) AS oldest_ready_age_s,
                  (SELECT count(*) FROM jobs
                    WHERE queue = :q AND status = 'succeeded'
                      AND finished_at > now() - interval '60 seconds') AS succeeded_last_minute
                """
            ),
            {"q": queue},
        ).mappings().first()
    age = row["oldest_ready_age_s"]
    return {
        "queue": queue,
        "counts": {s: counts.get(s, 0) for s in ("queued", "running", "succeeded", "dead")},
        "oldest_ready_age_s": float(age) if age is not None else None,
        "succeeded_last_minute": int(row["succeeded_last_minute"]),
    }
