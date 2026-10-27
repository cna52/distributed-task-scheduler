# Distributed Task Scheduler

A Go background-job service with a REST API, PostgreSQL job state, Redis ready queues, and independently scalable workers. Jobs have **at-least-once** execution semantics. PostgreSQL owns claims, leases, retries, and terminal state; Redis is a fast, recoverable delivery signal.

## Start

```sh
docker compose up --build -d
docker compose up --scale worker=3 -d
curl -sS http://localhost:8080/healthz
```

Submit and inspect a job:

```sh
curl -sS -X POST http://localhost:8080/jobs -H 'Content-Type: application/json' \
  -d '{"type":"sleep","payload":{"duration_ms":100},"priority":"high","max_attempts":3}'
curl -sS http://localhost:8080/jobs/JOB_ID
```

The Go CLI provides the same actions when run on the host:

```sh
go run . submit -type record_event -payload '{"name":"example"}' -key example-1
go run . status JOB_ID
```

`POST /jobs` accepts `type`, `payload`, optional `priority` (`high`, `normal`, or `low`, default `normal`), `run_at` (RFC 3339 timestamp), `max_attempts` (1–20, default 3), and `idempotency_key` (up to 128 characters). It returns `202` for a new job, `200` for a repeat with the same key and request, `409` if that key is reused for a different request, and `400` for invalid input. `GET /jobs/{id}` returns the current job or `404`. Results and errors remain in PostgreSQL. There is no retention cleanup in this version.

Registered handlers:

| Type | Payload | Behavior |
| --- | --- | --- |
| `sleep` | `{"duration_ms":100}` | Waits 0–300000 ms and succeeds. |
| `fail` | `{"failures":2}` | Fails its first two attempts, then succeeds. |
| `record_event` | `{"name":"example"}` | Inserts one event keyed by job ID in the completion transaction. |
| `resize_image` | `{"source_key":"inputs/photo.png","width":128,"height":128}` | Reads PNG/JPEG input from shared S3-compatible storage and writes a PNG thumbnail under `jobs/{job_id}/thumbnail.png`. |

Try the image handler with the local S3Mock service and an existing PNG/JPEG file:

```sh
curl -sS -X PUT -H 'Content-Type: image/png' --data-binary @photo.png \
  http://localhost:9090/scheduler-images/inputs/photo.png
curl -sS -X POST http://localhost:8080/jobs -H 'Content-Type: application/json' \
  -d '{"type":"resize_image","payload":{"source_key":"inputs/photo.png","width":128,"height":128}}'
curl -sS http://localhost:8080/jobs/JOB_ID
curl -sS http://localhost:9090/scheduler-images/jobs/JOB_ID/thumbnail.png -o thumbnail.png
```

Inputs are limited to 10 MiB, 8,000 pixels per side, and 20 million pixels total; requested dimensions are 1–1024 pixels. The handler preserves aspect ratio and does not enlarge small images. Input keys must stay under `inputs/`; outputs use a deterministic job-ID key, so replay overwrites the same object. Keep input objects immutable while their jobs may retry. The local S3Mock service stores objects on a named Docker volume. `OBJECT_ENDPOINT`, `OBJECT_ACCESS_KEY`, `OBJECT_SECRET_KEY`, and `OBJECT_BUCKET` configure workers for a different S3-compatible store.

The API is intended for local development: it has no authentication or TLS. Compose binds its published ports to `127.0.0.1`.

## Reliability model

1. Submission inserts a job and an outbox event in the same PostgreSQL transaction. The API can accept jobs while Redis is unavailable. The dispatcher publishes due events to Redis when it returns. An interrupted publish may be repeated; Redis sorted-set membership deduplicates a queued ID.
2. Workers atomically remove IDs from Redis, then conditionally claim the corresponding PostgreSQL row. Only one worker receives the active lease. The worker renews it every third of its lease period (30 seconds by default). On `SIGTERM` or `SIGINT`, a worker stops claiming jobs and allows active handlers to finish for 20 seconds by default; after that, unfinished jobs recover through lease expiry.
3. A completion or failure update must present the active, unexpired lease token. The dispatcher requeues expired leases with exponential delay or marks the job failed when its attempt limit is reached. A periodic reconciliation scan restores eligible jobs lost after a Redis pop or Redis data loss; its normal recovery window is about 30 seconds.
4. Ready jobs use three Redis sorted sets. FIFO is based on `run_at` within a priority. A shared seven-slot selection cycle gives four high, two normal, and one low slot while all three remain populated. Empty priorities yield their slot to available work.

Each handler may run more than once. `record_event` is duplicate-safe because its side effect and completion share one fenced PostgreSQL transaction and `recorded_events.job_id` is unique. A future handler that calls an external system must give that system the job ID as an idempotency key or otherwise tolerate replay. A lease prevents two *current claims*, but it cannot undo an external effect produced by a worker whose lease later expires.

## Development and tests

Requires Go 1.26 and Docker Compose. Start only the dependencies and create a separate test database:

```sh
docker compose up -d postgres redis s3mock
docker compose exec -T postgres createdb -U scheduler scheduler_test
TEST_DATABASE_URL='postgres://scheduler:scheduler@localhost:5432/scheduler_test?sslmode=disable' \
TEST_REDIS_URL='redis://localhost:6379/1' \
TEST_OBJECT_ENDPOINT='localhost:9090' TEST_OBJECT_ACCESS_KEY='test' \
TEST_OBJECT_SECRET_KEY='test' TEST_OBJECT_BUCKET='scheduler-test-images' go test -race ./...
```

The integration tests truncate the configured test database and flush the configured Redis database. Leave the `TEST_*` variables unset for unit-only tests. GitHub Actions runs the integration suite against service containers.

The binaries accept `DATABASE_URL`, `REDIS_URL`, `HTTP_ADDR`, `WORKER_CONCURRENCY`, `LEASE_SECONDS`, and `SHUTDOWN_GRACE_SECONDS`. Compose gives workers a 25-second stop grace for the default 20-second drain. Use `go run . migrate`, `serve`, `dispatch`, or `worker` to run individual processes. `GET /metrics` exposes Prometheus-format submission, rejection, job-status, Redis-ready, and outbox metrics. `scheduler_outbox_oldest_pending_seconds` is the age of the oldest undelivered event (0 when empty). `scheduler_submission_to_first_start_seconds_p95` is a rolling five-minute p95 from creation to first claim, including intentional scheduling delay; `scheduler_submission_to_first_start_samples` reports its sample count. JSON logs include job IDs and errors.

## Failure drills and benchmark

With the full stack running, submit a long `sleep` job, wait for `running`, then force-kill the worker holding it and start a replacement. The dispatcher will detect its expired lease and retry it. To test a Redis outage, stop Redis, submit a job, restart Redis, and observe pending outbox publication. To test a lost pop, the integration suite removes a ready ID before claiming it and verifies reconciliation. The integration suite also checks that an old lease token cannot complete a job.

Observed local failure drills on 2026-10-24:

| Drill | State during failure | Recovery observed |
| --- | --- | --- |
| Worker crash | A 15-second `sleep` job was `running` on attempt 1 with a 5-second lease when its worker received `SIGKILL`. | The dispatcher logged one expired lease; a replacement worker claimed attempt 2, and the job `succeeded` at 18:29:23 UTC. |
| Redis outage | With Redis stopped, `POST /jobs` accepted a `record_event` job. PostgreSQL showed `queued`, 0 attempts, and one pending outbox event. | After Redis restarted, the job `succeeded` on attempt 1, the pending outbox count became 0, and exactly one event was recorded. |

```sh
go run . bench -jobs 1000 -concurrency 20 -duration-ms 0
docker compose up --scale worker=1 -d
go run . bench -jobs 200 -concurrency 20 -duration-ms 100
docker compose up --scale worker=3 -d
go run . bench -jobs 200 -concurrency 20 -duration-ms 100
```

The benchmark submits `sleep` jobs, polls until all finish, then calculates throughput and p95 submission-to-completion latency from PostgreSQL completion timestamps. Client, API, and database clocks must be synchronized for latency values to be meaningful. Run benchmarks on an otherwise idle machine and record CPU, memory, worker count, concurrency, and job count with each result.

Local measurements on 2026-10-24, Docker Desktop on arm64 with 10 assigned CPUs and 7.65 GiB assigned memory, 4 execution goroutines per worker:

| Workload | Workers | Throughput | p95 latency |
| --- | ---: | ---: | ---: |
| 1,000 jobs, 0 ms, 20 submitters | 1 | 1,363 jobs/s | 537 ms |
| 1,000 jobs, 0 ms, 20 submitters | 3 | 1,326 jobs/s | 514 ms |
| 200 jobs, 100 ms, 20 submitters | 1 | 37.5 jobs/s | 5.06 s |
| 200 jobs, 100 ms, 20 submitters | 3 | 99.8 jobs/s | 1.91 s |

The zero-duration workload is limited by dispatch, database, and API overhead, so extra workers did not improve its throughput in this run. The 100 ms workload shows the expected increase in execution capacity. These are single-run local measurements, not production capacity guarantees.

After batching outbox publication, a fixed before/after comparison on 2026-10-26 used the same host API/dispatcher/worker setup, one worker with four goroutines, Docker PostgreSQL and Redis, 1,000 zero-duration jobs, and 20 submitters:

| Dispatcher | Throughput | p95 completion latency |
| --- | ---: | ---: |
| Prior commit `3aa7e2c`, per-job Redis and SQL writes | 681 jobs/s | 1.12 s |
| Batched Redis pipeline and SQL updates | 954 jobs/s | 696 ms |

These are single-run measurements on a development machine. The host-process setup differs from the earlier all-Compose measurements, so compare only rows within the same table.
