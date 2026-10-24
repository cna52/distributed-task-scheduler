CREATE TABLE IF NOT EXISTS jobs (
    id uuid PRIMARY KEY,
    type text NOT NULL,
    payload jsonb NOT NULL,
    status text NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'failed')),
    priority text NOT NULL CHECK (priority IN ('high', 'normal', 'low')),
    max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 20),
    attempts integer NOT NULL DEFAULT 0,
    run_at timestamptz NOT NULL,
    lease_token uuid,
    leased_until timestamptz,
    result jsonb,
    error text,
    idempotency_key text UNIQUE,
    request_hash text NOT NULL,
    enqueued_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    finished_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS jobs_ready_idx ON jobs (run_at, created_at) WHERE status = 'queued';
CREATE INDEX IF NOT EXISTS jobs_leases_idx ON jobs (leased_until) WHERE status = 'running';

CREATE TABLE IF NOT EXISTS outbox (
    id bigserial PRIMARY KEY,
    job_id uuid NOT NULL REFERENCES jobs(id),
    available_at timestamptz NOT NULL,
    delivered_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS outbox_pending_idx ON outbox (available_at, id) WHERE delivered_at IS NULL;

CREATE TABLE IF NOT EXISTS recorded_events (
    job_id uuid PRIMARY KEY REFERENCES jobs(id),
    name text NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT now()
);
