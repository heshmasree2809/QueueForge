# QueueForge

A fault-tolerant background job queue built on PostgreSQL, with a FastAPI HTTP API, a worker pool, and a live dashboard.

Jobs survive worker crashes (lease-based recovery), retry with exponential backoff and jitter, and move to a dead-letter state when attempts run out. State lives in one Postgres table, so there is no extra broker to run.

## How it works

| Concern | Approach |
|---|---|
| No double delivery | Workers claim rows with `SELECT ... FOR UPDATE SKIP LOCKED`, so concurrent workers never block each other or receive the same job. |
| Worker crashes | Each claim sets a lease (`lease_expires_at`). A reaper requeues running jobs whose lease expired. A `locked_by` guard stops a stale worker from overwriting the new owner's result. |
| Retries | Exponential backoff with jitter (`base * 2^(attempt-1)`, capped, scaled by 0.5-1.0), then `dead` after `max_attempts`. Dead jobs can be retried from the API or dashboard. |
| Duplicate submissions | Optional `idempotency_key` (unique index) returns the original job instead of creating a second one. |
| Ordering | Priority first, then scheduled time, then ID. `delay_seconds` schedules a job for later. |
| Fast claims | A partial index on `(queue, priority DESC, run_at, id) WHERE status = 'queued'` covers only runnable rows. |
| Delivery guarantee | **At-least-once.** A job may run twice (worker finishes, then dies before acknowledging), so handlers must be idempotent. |
| Abuse control | Optional `API_KEY` protects write endpoints; payloads are capped at 16 KB; job types are validated against registered handlers. |

## Run locally

```bash
docker compose up --build            # Postgres + API + 1 worker
docker compose up --scale worker=3   # three worker containers
```

Open http://localhost:8000 for the dashboard and http://localhost:8000/docs for the API.

```bash
curl -X POST localhost:8000/api/v1/jobs -H 'content-type: application/json' \
  -d '{"type":"add","payload":{"numbers":[1,2,3]},"idempotency_key":"demo-1"}'
curl localhost:8000/api/v1/jobs/1
curl localhost:8000/api/v1/stats
```

**Watch crash recovery:** enqueue some `sleep` jobs, then run `docker compose kill worker`. After the lease (30 s by default) expires, start a worker again and the jobs are picked up.

Without Docker: `pip install -r requirements.txt`, set `DATABASE_URL`, then `uvicorn app.main:app` (workers run inside the API process by default) or `python -m app.worker` for a separate worker.

## API

| Method | Path | Notes |
|---|---|---|
| POST | `/api/v1/jobs` | Body: `type`, `payload`, `queue`, `priority`, `max_attempts`, `delay_seconds`, `idempotency_key`. 201 if created, 200 if the key already existed. |
| GET | `/api/v1/jobs/{id}` | Status, attempts, result, last error. |
| GET | `/api/v1/jobs?status=dead&limit=50` | Recent jobs. |
| POST | `/api/v1/jobs/{id}/retry` | Requeue a dead job. |
| GET | `/api/v1/stats` | Counts by status, oldest ready job age, completions in the last minute. |
| GET | `/health` | Checks database connectivity. |

Built-in job types: `echo`, `add`, `sleep`, `flaky`, `fail`. Add your own in `app/handlers.py`.

## Tests and benchmark

```bash
export TEST_DATABASE_URL=postgresql://queue:queue@localhost:5432/queue   # tests TRUNCATE the jobs table
pip install -r requirements-dev.txt && pytest -v
python -m scripts.bench --jobs 5000 --threads 8
```

The tests cover idempotency, an 8-thread concurrent-claim check (no job delivered twice), priority and delay ordering, backoff and dead-lettering, lease expiry and reaping, stale-worker rejection, and an end-to-end worker run. CI runs them against Postgres on every push.

## Limitations and next steps

- Workers poll the table; `LISTEN/NOTIFY` would cut idle latency and load.
- No lease heartbeat: handlers must finish within `LEASE_SECONDS`, otherwise the job may be run twice.
- One Postgres instance is the throughput ceiling; partitioning by queue or moving to a dedicated broker is the next scaling step.
- Finished jobs are never purged; add a retention job for production use.
- A single shared API key is basic; real deployments need per-client credentials.
