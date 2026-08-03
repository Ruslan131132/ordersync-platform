-- Portfolio / learning project schema for ERP ↔ marketplace order sync.
-- gen_random_uuid() is built into PostgreSQL 13+.

CREATE TABLE IF NOT EXISTS orders (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    external_id     TEXT NOT NULL,
    marketplace     TEXT NOT NULL CHECK (marketplace IN ('wb_like', 'ozon_like')),
    status          TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'syncing', 'synced', 'failed')),
    payload         JSONB NOT NULL DEFAULT '{}'::jsonb,
    content_hash    TEXT NOT NULL,
    version         BIGINT NOT NULL DEFAULT 1,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (marketplace, external_id)
);

CREATE INDEX IF NOT EXISTS idx_orders_status ON orders (status);
CREATE INDEX IF NOT EXISTS idx_orders_updated_at ON orders (updated_at);

CREATE TABLE IF NOT EXISTS sync_jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    status          TEXT NOT NULL DEFAULT 'queued'
                        CHECK (status IN ('queued', 'running', 'completed', 'failed', 'cancelled')),
    trigger_source  TEXT NOT NULL DEFAULT 'manual',
    fresh_volume    INT NOT NULL DEFAULT 0,
    batch_size      INT NOT NULL DEFAULT 0,
    orders_queued   INT NOT NULL DEFAULT 0,
    orders_done     INT NOT NULL DEFAULT 0,
    orders_failed   INT NOT NULL DEFAULT 0,
    error_message   TEXT,
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_sync_jobs_status ON sync_jobs (status);

-- Ledger of successful marketplace side-effects for idempotent retries.
CREATE TABLE IF NOT EXISTS sync_ledger (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id        UUID NOT NULL REFERENCES orders (id) ON DELETE CASCADE,
    marketplace     TEXT NOT NULL,
    method          TEXT NOT NULL,
    content_hash    TEXT NOT NULL,
    remote_ref      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (order_id, marketplace, method, content_hash)
);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    key             TEXT NOT NULL,
    scope           TEXT NOT NULL,
    result_ref      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (scope, key)
);

CREATE INDEX IF NOT EXISTS idx_idempotency_expires ON idempotency_keys (expires_at);

CREATE TABLE IF NOT EXISTS outbox_events (
    id              BIGSERIAL PRIMARY KEY,
    aggregate_type  TEXT NOT NULL,
    aggregate_id    TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    payload         JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_outbox_unpublished
    ON outbox_events (id)
    WHERE published_at IS NULL;
