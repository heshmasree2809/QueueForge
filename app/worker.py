"""Worker pool: threads that claim jobs, run handlers, and acknowledge results.

Run standalone with `python -m app.worker`, or embedded in the API process (EMBEDDED_WORKERS=true).
"""
import logging
import os
import random
import signal
import socket
import threading
import time
from typing import Optional

from . import jobs
from .config import settings
from .db import init_db
from .handlers import HANDLERS

log = logging.getLogger("queueforge.worker")


class WorkerPool:
    def __init__(
        self,
        threads: Optional[int] = None,
        queue: Optional[str] = None,
        lease_seconds: Optional[float] = None,
        poll_seconds: Optional[float] = None,
    ) -> None:
        self.threads = threads or settings.WORKER_THREADS
        self.queue = queue or settings.QUEUE
        self.lease_seconds = lease_seconds or settings.LEASE_SECONDS
        self.poll_seconds = poll_seconds or settings.POLL_SECONDS
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self._prefix = f"{socket.gethostname()}-{os.getpid()}"

    def start(self) -> None:
        for i in range(self.threads):
            t = threading.Thread(target=self._loop, args=(f"{self._prefix}-{i}",), daemon=True)
            t.start()
            self._threads.append(t)
        log.info("started %d worker threads on queue %r", self.threads, self.queue)

    def stop(self, timeout: float = 10.0) -> None:
        self._stop.set()
        for t in self._threads:
            t.join(timeout)

    def wait(self) -> None:
        self._stop.wait()

    def _loop(self, worker_id: str) -> None:
        last_reap = 0.0
        while not self._stop.is_set():
            try:
                if time.monotonic() - last_reap >= settings.REAP_INTERVAL_SECONDS:
                    reclaimed = jobs.reap_expired(self.queue)
                    if reclaimed:
                        log.warning("%s reclaimed %d expired job(s)", worker_id, reclaimed)
                    last_reap = time.monotonic()
                batch = jobs.claim(worker_id, self.queue, self.lease_seconds, limit=1)
            except Exception:
                log.exception("%s: database error while claiming; retrying", worker_id)
                self._stop.wait(1.0)
                continue
            if not batch:
                self._stop.wait(self.poll_seconds * random.uniform(0.8, 1.2))
                continue
            for job in batch:
                self._run(job, worker_id)

    def _run(self, job: dict, worker_id: str) -> None:
        try:
            handler = HANDLERS.get(job["type"])
            if handler is None:
                raise LookupError(f"no handler registered for type {job['type']!r}")
            result = handler(job["payload"])
            if not jobs.complete(job["id"], worker_id, result):
                log.warning("job %s: lease was lost before completion; result discarded", job["id"])
        except Exception as exc:
            log.info("job %s attempt %s failed: %s", job["id"], job["attempts"], exc)
            try:
                jobs.fail(job, worker_id, f"{type(exc).__name__}: {exc}")
            except Exception:
                # If even this fails, the lease will expire and the reaper requeues the job.
                log.exception("job %s: could not record failure", job["id"])


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    init_db()
    pool = WorkerPool()
    signal.signal(signal.SIGTERM, lambda *_: pool._stop.set())
    signal.signal(signal.SIGINT, lambda *_: pool._stop.set())
    pool.start()
    pool.wait()
    pool.stop()


if __name__ == "__main__":
    main()
