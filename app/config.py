"""Runtime configuration, read once from environment variables."""
import os


def _normalise_db_url(url: str) -> str:
    # Hosted Postgres URLs start with postgres:// or postgresql://; SQLAlchemy needs a driver name.
    for prefix in ("postgres://", "postgresql://"):
        if url.startswith(prefix):
            return "postgresql+psycopg://" + url[len(prefix):]
    return url


def _bool(name: str, default: bool) -> bool:
    return os.environ.get(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}


class Settings:
    def __init__(self) -> None:
        self.DATABASE_URL = _normalise_db_url(
            os.environ.get("DATABASE_URL", "postgresql://queue:queue@localhost:5432/queue")
        )
        self.API_KEY = os.environ.get("API_KEY", "")
        self.EMBEDDED_WORKERS = _bool("EMBEDDED_WORKERS", True)
        self.WORKER_THREADS = int(os.environ.get("WORKER_THREADS", "9"))
        self.QUEUE = os.environ.get("QUEUE", "default")
        self.LEASE_SECONDS = float(os.environ.get("LEASE_SECONDS", "30"))
        self.POLL_SECONDS = float(os.environ.get("POLL_SECONDS", "0.5"))
        self.REAP_INTERVAL_SECONDS = float(os.environ.get("REAP_INTERVAL_SECONDS", "10"))
        self.BACKOFF_BASE_SECONDS = float(os.environ.get("BACKOFF_BASE_SECONDS", "2"))
        self.BACKOFF_CAP_SECONDS = float(os.environ.get("BACKOFF_CAP_SECONDS", "300"))
        self.POOL_SIZE = int(os.environ.get("POOL_SIZE", "10"))


settings = Settings()
