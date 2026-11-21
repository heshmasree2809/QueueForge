import os

# Tests TRUNCATE the jobs table: point them at a throwaway database, never one you care about.
os.environ["DATABASE_URL"] = (
    os.environ.get("TEST_DATABASE_URL")
    or os.environ.get("DATABASE_URL")
    or "postgresql://queue:queue@localhost:5432/queue"
)
os.environ["EMBEDDED_WORKERS"] = "false"

import pytest  # noqa: E402
from sqlalchemy import text  # noqa: E402

from app.db import engine, init_db  # noqa: E402


@pytest.fixture(autouse=True)
def clean_db():
    init_db()
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE jobs RESTART IDENTITY"))
    yield
