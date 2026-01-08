-- V2 Schema: UUIDv7 + Type Hash System
-- This migration creates the v2 schema with improved type safety and better database types.
--
-- Breaking changes:
-- - Job IDs changed from TEXT (Xid) to BLOB (16-byte UUID)
-- - Job types now use type_hash (INTEGER/i64) for lookups + type_name (TEXT) for debugging
-- - Timestamps stored as INTEGER (Unix timestamp in milliseconds) for better precision
-- - Payload stored as TEXT (JSON) for better SQLite compatibility
-- - Added error_message and error_context to dead_queue for better debugging

-- New main queue table
CREATE TABLE IF NOT EXISTS adc_queue_v2 (
    jid BLOB PRIMARY KEY NOT NULL,
    queue TEXT NOT NULL DEFAULT 'default',
    type_hash INTEGER NOT NULL,
    type_name TEXT NOT NULL,
    payload TEXT NOT NULL,
    retries INTEGER NOT NULL DEFAULT 0,
    scheduled_at INTEGER NOT NULL,
    started_at INTEGER,
    enqueued_at INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
    priority INTEGER NOT NULL DEFAULT 0,

    -- Constraints
    CHECK (retries >= 0),
    CHECK (priority >= -128 AND priority <= 127),
    CHECK (length(jid) = 16)
);

-- Indexes for efficient polling
CREATE INDEX IF NOT EXISTS adc_queue_v2_poll_idx ON adc_queue_v2 (
    queue,
    type_hash,
    scheduled_at ASC,
    priority DESC
) WHERE started_at IS NULL;

CREATE INDEX IF NOT EXISTS adc_queue_v2_scheduled_idx ON adc_queue_v2 (
    scheduled_at ASC
) WHERE started_at IS NULL;

-- New dead queue table with error context
CREATE TABLE IF NOT EXISTS adc_dead_queue_v2 (
    jid BLOB PRIMARY KEY NOT NULL,
    queue TEXT NOT NULL,
    type_hash INTEGER NOT NULL,
    type_name TEXT NOT NULL,
    payload TEXT NOT NULL,
    retries INTEGER NOT NULL,
    scheduled_at INTEGER NOT NULL,
    started_at INTEGER NOT NULL,
    enqueued_at INTEGER NOT NULL,
    died_at INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
    priority INTEGER NOT NULL,
    error_message TEXT,
    error_context TEXT,

    -- Constraints
    CHECK (priority >= -128 AND priority <= 127),
    CHECK (length(jid) = 16)
);

CREATE INDEX IF NOT EXISTS adc_dead_queue_v2_died_idx ON adc_dead_queue_v2 (
    died_at DESC
);

CREATE INDEX IF NOT EXISTS adc_dead_queue_v2_type_idx ON adc_dead_queue_v2 (
    type_hash
);

-- New cron jobs table
CREATE TABLE IF NOT EXISTS adc_cron_jobs_v2 (
    cron_id BLOB PRIMARY KEY NOT NULL,
    queue TEXT NOT NULL DEFAULT 'default',
    type_hash INTEGER NOT NULL,
    type_name TEXT NOT NULL,
    payload TEXT NOT NULL,
    cron_expression TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
    last_enqueued_at INTEGER,
    next_execution_at INTEGER NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    max_runs INTEGER,
    run_count INTEGER NOT NULL DEFAULT 0,

    -- Constraints
    CHECK (length(cron_id) = 16),
    CHECK (enabled IN (0, 1)),
    CHECK (run_count >= 0),
    CHECK (max_runs IS NULL OR max_runs > 0),
    CHECK (priority >= -128 AND priority <= 127)
);

CREATE INDEX IF NOT EXISTS adc_cron_jobs_v2_next_execution_idx ON adc_cron_jobs_v2 (
    enabled,
    next_execution_at ASC,
    queue
) WHERE enabled = 1;

CREATE INDEX IF NOT EXISTS adc_cron_jobs_v2_type_idx ON adc_cron_jobs_v2 (
    type_hash
);
