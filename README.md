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

The API is intended for local development: it has no authentication or TLS. Compose binds its published ports to `127.0.0.1`.

## Reliability model

1. Submission inserts a job and an outbox event in the same PostgreSQL transaction. The API can accept jobs while Redis is unavailable. The dispatcher publishes due events to Redis when it returns. An interrupted publish may be repeated; Redis sorted-set membership deduplicates a queued ID.
2. Workers atomically remove IDs from Redis, then conditionally claim the corresponding PostgreSQL row. Only one worker receives the active lease. The worker renews it every third of its lease period (30 seconds by default).
3. A completion or failure update must present the active, unexpired lease token. The dispatcher requeues expired leases with exponential delay or marks the job failed when its attempt limit is reached. A periodic reconciliation scan restores eligible jobs lost after a Redis pop or Redis data loss; its normal recovery window is about 30 seconds.
4. Ready jobs use three Redis sorted sets. FIFO is based on `run_at` within a priority. A shared seven-slot selection cycle gives four high, two normal, and one low slot while all three remain populated. Empty priorities yield their slot to available work.

Each handler may run more than once. `record_event` is duplicate-safe because its side effect and completion share one fenced PostgreSQL transaction and `recorded_events.job_id` is unique. A future handler that calls an external system must give that system the job ID as an idempotency key or otherwise tolerate replay. A lease prevents two *current claims*, but it cannot undo an external effect produced by a worker whose lease later expires.

## Development and tests

Requires Go 1.26 and Docker Compose. Start only the dependencies and create a separate test database:

```sh
docker compose up -d postgres redis
docker compose exec -T postgres createdb -U scheduler scheduler_test
TEST_DATABASE_URL='postgres://scheduler:scheduler@localhost:5432/scheduler_test?sslmode=disable' \
TEST_REDIS_URL='redis://localhost:6379/1' go test -race ./...
```

The integration tests truncate the configured test database and flush the configured Redis database. Leave the `TEST_*` variables unset for unit-only tests. GitHub Actions runs the integration suite against service containers.

The binaries accept `DATABASE_URL`, `REDIS_URL`, `HTTP_ADDR`, `WORKER_CONCURRENCY`, and `LEASE_SECONDS`. Use `go run . migrate`, `serve`, `dispatch`, or `worker` to run individual processes. `GET /metrics` exposes Prometheus-format submission, rejection, job-status, and Redis-ready metrics. JSON logs include job IDs and errors.

## Failure drills and benchmark

With the full stack running, submit a long `sleep` job, wait for `running`, then stop a worker container without a graceful shutdown. The dispatcher will detect its expired lease, retry it, and reject any completion from the old token. To test Redis loss, stop Redis, submit a job, restart Redis, and observe pending outbox publication. To test a lost pop, the integration suite removes a ready ID before claiming it and verifies reconciliation.

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
