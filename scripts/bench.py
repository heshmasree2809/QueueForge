"""Throughput and latency benchmark.

WARNING: deletes all jobs in the 'bench' queue. Run against a local or throwaway database.

    python -m scripts.bench --jobs 5000 --threads 8
"""
import argparse
import time

from sqlalchemy import text

from app import jobs
from app.db import engine, init_db
from app.worker import WorkerPool


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--jobs", type=int, default=2000)
    parser.add_argument("--threads", type=int, default=8, help="keep <= POOL_SIZE (default 10)")
    args = parser.parse_args()

    init_db()
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM jobs WHERE queue = 'bench'"))

    t0 = time.perf_counter()
    for i in range(args.jobs):
        jobs.enqueue("echo", {"i": i}, queue="bench")
    enqueue_s = time.perf_counter() - t0

    pool = WorkerPool(threads=args.threads, queue="bench", poll_seconds=0.05)
    t1 = time.perf_counter()
    pool.start()
    while True:
        counts = jobs.stats("bench")["counts"]
        if counts["queued"] + counts["running"] == 0:
            break
        time.sleep(0.1)
    drain_s = time.perf_counter() - t1
    pool.stop()

    with engine.connect() as conn:
        p50, p95, p99 = conn.execute(
            text(
                """
                SELECT percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM finished_at - created_at)),
                       percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM finished_at - created_at)),
                       percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM finished_at - created_at))
                FROM jobs WHERE queue = 'bench' AND status = 'succeeded'
                """
            )
        ).one()
    final = jobs.stats("bench")["counts"]
    print(f"jobs: {args.jobs}  worker threads: {args.threads}")
    print(f"enqueue rate : {args.jobs / enqueue_s:,.0f} jobs/s (single producer thread)")
    print(f"process rate : {args.jobs / drain_s:,.0f} jobs/s")
    print(f"end-to-end latency (create -> finish) p50/p95/p99: {p50*1000:.0f} / {p95*1000:.0f} / {p99*1000:.0f} ms")
    print(f"final counts : {final}")
    print("Note: latency includes time spent queued behind the other jobs enqueued first.")


if __name__ == "__main__":
    main()
