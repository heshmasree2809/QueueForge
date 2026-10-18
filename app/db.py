"""Database engine and idempotent schema setup."""
from sqlalchemy import create_engine, text

from .config import settings

engine = create_engine(
    settings.DATABASE_URL,
    pool_size=settings.POOL_SIZE,
    max_overflow=5,
    pool_pre_ping=True,
)

SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS jobs (
        id               BIGSERIAL PRIMARY KEY,
        queue            TEXT        NOT NULL DEFAULT 'default',
        type             TEXT        NOT NULL,
        payload          JSONB       NOT NULL DEFAULT '{}',
        status           TEXT        NOT NULL DEFAULT 'queued'
                         CHECK (status IN ('queued', 'running', 'succeeded', 'dead')),
        priority         INT         NOT NULL DEFAULT 0,
        attempts         INT         NOT NULL DEFAULT 0,
        max_attempts     INT         NOT NULL DEFAULT 5,
        run_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
        locked_by        TEXT,
        lease_expires_at TIMESTAMPTZ,
        idempotency_key  TEXT UNIQUE,
        result           JSONB,
        last_error       TEXT,
        created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
        finished_at      TIMESTAMPTZ
    )
    """,
    # Claim path: only rows that are ready to run are indexed, ordered the way workers pick them.
    """
    CREATE INDEX IF NOT EXISTS jobs_ready_idx
        ON jobs (queue, priority DESC, run_at, id) WHERE status = 'queued'
    """,
    # Reaper path: find running jobs whose lease has expired.
    """
    CREATE INDEX IF NOT EXISTS jobs_lease_idx
        ON jobs (lease_expires_at) WHERE status = 'running'
    """,
]


def init_db() -> None:
    """Create tables/indexes. The advisory lock stops concurrent processes racing on startup."""
    with engine.begin() as conn:
        conn.execute(text("SELECT pg_advisory_xact_lock(727274)"))
        for statement in SCHEMA:
            conn.execute(text(statement))
