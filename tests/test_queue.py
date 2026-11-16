import threading
import time

from sqlalchemy import text

from app import jobs
from app.db import engine
from app.worker import WorkerPool


def _make_ready(job_id: int) -> None:
    with engine.begin() as conn:
        conn.execute(text("UPDATE jobs SET run_at = now() WHERE id = :id"), {"id": job_id})


def test_enqueue_is_idempotent():
    first, created1 = jobs.enqueue("echo", {"a": 1}, idempotency_key="order-42")
    second, created2 = jobs.enqueue("echo", {"a": 999}, idempotency_key="order-42")
    assert created1 is True and created2 is False
    assert first["id"] == second["id"]
    assert second["payload"] == {"a": 1}  # the original job is returned untouched


def test_concurrent_claims_never_double_deliver():
    for i in range(200):
        jobs.enqueue("echo", {"i": i})
    claimed, lock = [], threading.Lock()

    def drain(name: str) -> None:
        while True:
            batch = jobs.claim(name, limit=1)
            if not batch:
                return
            with lock:
                claimed.extend(j["id"] for j in batch)

    threads = [threading.Thread(target=drain, args=(f"w{i}",)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(claimed) == 200
    assert len(set(claimed)) == 200  # no job handed to two workers


def test_priority_order_and_delayed_jobs():
    low, _ = jobs.enqueue("echo", priority=0)
    high, _ = jobs.enqueue("echo", priority=10)
    delayed, _ = jobs.enqueue("echo", priority=100, delay_seconds=3600)
    order = [jobs.claim("w")[0]["id"], jobs.claim("w")[0]["id"]]
    assert order == [high["id"], low["id"]]
    assert jobs.claim("w") == []  # the delayed job is not ready yet
    assert jobs.get_job(delayed["id"])["status"] == "queued"


def test_failure_backs_off_then_goes_dead():
    job, _ = jobs.enqueue("fail", max_attempts=2)
    claimed = jobs.claim("w")[0]
    assert claimed["attempts"] == 1
    assert jobs.fail(claimed, "w", "boom")
    after_first = jobs.get_job(job["id"])
    assert after_first["status"] == "queued"
    assert jobs.claim("w") == []  # backoff: not claimable immediately

    _make_ready(job["id"])
    claimed = jobs.claim("w")[0]
    assert claimed["attempts"] == 2
    assert jobs.fail(claimed, "w", "boom again")
    final = jobs.get_job(job["id"])
    assert final["status"] == "dead"
    assert final["last_error"] == "boom again"


def test_dead_job_can_be_retried():
    job, _ = jobs.enqueue("fail", max_attempts=1)
    jobs.fail(jobs.claim("w")[0], "w", "x")
    assert jobs.get_job(job["id"])["status"] == "dead"
    revived = jobs.retry_dead(job["id"])
    assert revived["status"] == "queued" and revived["attempts"] == 0
    assert jobs.retry_dead(job["id"]) is None  # no longer dead


def test_expired_lease_is_reclaimed():
    job, _ = jobs.enqueue("echo")
    jobs.claim("crashed-worker", lease_seconds=0)  # lease expires immediately: simulates a crash
    time.sleep(0.05)
    assert jobs.reap_expired() == 1
    again = jobs.claim("healthy-worker")[0]
    assert again["id"] == job["id"] and again["attempts"] == 2


def test_stale_worker_cannot_acknowledge_after_lease_loss():
    job, _ = jobs.enqueue("echo")
    jobs.claim("a", lease_seconds=0)
    time.sleep(0.05)
    jobs.reap_expired()
    jobs.claim("b")
    assert jobs.complete(job["id"], "a", {"late": True}) is False
    assert jobs.complete(job["id"], "b", {"ok": True}) is True
    assert jobs.get_job(job["id"])["result"] == {"ok": True}


def test_exhausted_attempts_on_lease_expiry_go_dead():
    job, _ = jobs.enqueue("echo", max_attempts=1)
    jobs.claim("a", lease_seconds=0)
    time.sleep(0.05)
    jobs.reap_expired()
    assert jobs.get_job(job["id"])["status"] == "dead"


def test_worker_pool_processes_jobs_end_to_end():
    ids = [jobs.enqueue("add", {"numbers": [i, 1]})[0]["id"] for i in range(20)]
    pool = WorkerPool(threads=2, queue="default", poll_seconds=0.05)
    pool.start()
    try:
        deadline = time.time() + 15
        while time.time() < deadline:
            if jobs.stats()["counts"]["succeeded"] == 20:
                break
            time.sleep(0.1)
    finally:
        pool.stop()
    assert jobs.stats()["counts"]["succeeded"] == 20
    assert jobs.get_job(ids[3])["result"] == {"sum": 5}
