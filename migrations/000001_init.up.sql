CREATE TABLE jobs (
 id uuid PRIMARY KEY,
 queue text NOT NULL, type text NOT NULL, payload jsonb NOT NULL,
 status text NOT NULL CHECK(status IN ('SCHEDULED','READY','RUNNING','SUCCEEDED','RETRY_WAIT','DEAD_LETTERED','CANCELLED')),
 priority integer NOT NULL DEFAULT 0,
 scheduled_at timestamptz NOT NULL,
 max_attempts integer NOT NULL CHECK(max_attempts BETWEEN 1 AND 100),
 attempts integer NOT NULL DEFAULT 0,
 timeout_seconds integer NOT NULL CHECK(timeout_seconds BETWEEN 1 AND 86400),
 idempotency_key text,
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
 started_at timestamptz, completed_at timestamptz, result jsonb, last_error text NOT NULL DEFAULT '',
 worker_id text NOT NULL DEFAULT '', lease_token uuid, lease_expires_at timestamptz,
 request_id text NOT NULL, trace_parent text NOT NULL DEFAULT '', trace_state text NOT NULL DEFAULT '',
 UNIQUE(queue,idempotency_key),
 CHECK ((status='RUNNING') = (lease_token IS NOT NULL AND lease_expires_at IS NOT NULL))
);
CREATE INDEX jobs_schedule ON jobs(status,scheduled_at) WHERE status IN ('SCHEDULED','RETRY_WAIT');
CREATE INDEX jobs_claim ON jobs(queue,priority DESC,created_at,id) WHERE status='READY';
CREATE INDEX jobs_expired ON jobs(lease_expires_at) WHERE status='RUNNING';
CREATE INDEX jobs_page ON jobs(created_at DESC,id DESC);
CREATE TABLE workers (
 id text PRIMARY KEY, hostname text NOT NULL, queues text[] NOT NULL,
 concurrency integer NOT NULL CHECK(concurrency>0), version text NOT NULL,
 started_at timestamptz NOT NULL DEFAULT now(), last_heartbeat timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX workers_heartbeat ON workers(last_heartbeat);
CREATE TABLE job_attempts (
 id bigserial PRIMARY KEY, job_id uuid NOT NULL REFERENCES jobs(id),
 attempt_number integer NOT NULL, worker_id text NOT NULL, lease_token uuid NOT NULL UNIQUE,
 started_at timestamptz NOT NULL DEFAULT now(), finished_at timestamptz,
 status text NOT NULL, error text NOT NULL DEFAULT '', duration_ms bigint
);
CREATE INDEX attempts_job ON job_attempts(job_id,id);
