-- Leases make claimed work recoverable after a crash: an order or job whose
-- lease expired can be claimed again, and completions are fenced by token.

ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS attempts        INT         NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS pending_since   TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS last_error      TEXT,
    ADD COLUMN IF NOT EXISTS lease_owner     TEXT,
    ADD COLUMN IF NOT EXISTS lease_token     UUID,
    ADD COLUMN IF NOT EXISTS lease_until     TIMESTAMPTZ;

ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_status_check;
ALTER TABLE orders ADD CONSTRAINT orders_status_check
    CHECK (status IN ('pending', 'syncing', 'synced', 'failed', 'dead'));

DROP INDEX IF EXISTS idx_orders_status;
DROP INDEX IF EXISTS idx_orders_updated_at;
CREATE INDEX IF NOT EXISTS idx_orders_claimable
    ON orders (next_attempt_at)
    WHERE status IN ('pending', 'failed');
CREATE INDEX IF NOT EXISTS idx_orders_lease
    ON orders (lease_until)
    WHERE status = 'syncing';

ALTER TABLE sync_jobs RENAME COLUMN batch_size TO last_batch_size;
ALTER TABLE sync_jobs RENAME COLUMN orders_queued TO orders_claimed;
ALTER TABLE sync_jobs
    ADD COLUMN IF NOT EXISTS batches     INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS worker_id   TEXT,
    ADD COLUMN IF NOT EXISTS lease_until TIMESTAMPTZ;

-- At most one queued job: concurrent triggers and schedulers converge on it.
CREATE UNIQUE INDEX IF NOT EXISTS uq_sync_jobs_single_queued
    ON sync_jobs (status)
    WHERE status = 'queued';
CREATE INDEX IF NOT EXISTS idx_sync_jobs_running_lease
    ON sync_jobs (lease_until)
    WHERE status = 'running';
CREATE INDEX IF NOT EXISTS idx_sync_jobs_created_at ON sync_jobs (created_at DESC);

ALTER TABLE idempotency_keys
    ADD COLUMN IF NOT EXISTS request_hash TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_outbox_published_at
    ON outbox_events (published_at)
    WHERE published_at IS NOT NULL;
