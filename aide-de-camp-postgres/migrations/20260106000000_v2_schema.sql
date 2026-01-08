-- V2 Schema: UUIDv7 + Type Hash System
-- This migration creates the v2 schema with improved type safety and better database types.
--
-- Breaking changes:
-- - Job IDs changed from TEXT (Xid) to UUID (UUIDv7)
-- - Job types now use type_hash (BIGINT) for lookups + type_name (TEXT) for debugging
-- - Timestamps use native TIMESTAMPTZ instead of BIGINT
-- - Added error_message and error_context to dead_queue for better debugging

-- UUIDv7 generation: Use pg_uuidv7 extension if available, otherwise fallback
-- Install extension with: CREATE EXTENSION pg_uuidv7;
-- See https://pgxn.org/dist/pg_uuidv7/
DO $$
BEGIN
    -- Try to create the extension (will silently fail if not available)
    CREATE EXTENSION IF NOT EXISTS pg_uuidv7;
EXCEPTION WHEN OTHERS THEN
    -- Extension not available, create our own function as fallback
    NULL;
END
$$;

-- Create fallback function if extension function doesn't exist
-- This provides a compatible uuid_generate_v7() for testing/development
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'uuid_generate_v7') THEN
        EXECUTE '
CREATE OR REPLACE FUNCTION uuid_generate_v7()
RETURNS uuid
AS $BODY$
DECLARE
    unix_ts_ms bytea;
    uuid_bytes bytea;
BEGIN
    -- Get Unix timestamp in milliseconds (48 bits)
    unix_ts_ms = substring(int8send(floor(extract(epoch from clock_timestamp()) * 1000)::bigint) from 3);

    -- Generate random bytes for the rest
    uuid_bytes = unix_ts_ms || gen_random_bytes(10);

    -- Set version (4 bits) to 7: 0111
    uuid_bytes = set_byte(uuid_bytes, 6, (get_byte(uuid_bytes, 6) & 15) | 112);

    -- Set variant (2 bits) to RFC 4122: 10
    uuid_bytes = set_byte(uuid_bytes, 8, (get_byte(uuid_bytes, 8) & 63) | 128);

    RETURN encode(uuid_bytes, ''hex'')::uuid;
END;
$BODY$ LANGUAGE plpgsql VOLATILE;
        ';
    END IF;
END
$$;

-- New main queue table
CREATE TABLE IF NOT EXISTS adc_queue_v2 (
    jid UUID PRIMARY KEY DEFAULT uuid_generate_v7(),
    queue TEXT NOT NULL DEFAULT 'default',
    type_hash BIGINT NOT NULL,
    type_name TEXT NOT NULL,
    payload JSONB NOT NULL,
    retries INT NOT NULL DEFAULT 0,
    scheduled_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    enqueued_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    priority SMALLINT NOT NULL DEFAULT 0,

    -- Constraints
    CHECK (retries >= 0),
    CHECK (priority >= -128 AND priority <= 127)
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
    jid UUID PRIMARY KEY,
    queue TEXT NOT NULL,
    type_hash BIGINT NOT NULL,
    type_name TEXT NOT NULL,
    payload JSONB NOT NULL,
    retries INT NOT NULL,
    scheduled_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    enqueued_at TIMESTAMPTZ NOT NULL,
    died_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    priority SMALLINT NOT NULL,
    error_message TEXT,
    error_context JSONB,

    -- Constraints
    CHECK (priority >= -128 AND priority <= 127)
);

CREATE INDEX IF NOT EXISTS adc_dead_queue_v2_died_idx ON adc_dead_queue_v2 (
    died_at DESC
);

CREATE INDEX IF NOT EXISTS adc_dead_queue_v2_type_idx ON adc_dead_queue_v2 (
    type_hash
);

-- New cron jobs table
CREATE TABLE IF NOT EXISTS adc_cron_jobs_v2 (
    cron_id UUID PRIMARY KEY DEFAULT uuid_generate_v7(),
    queue TEXT NOT NULL DEFAULT 'default',
    type_hash BIGINT NOT NULL,
    type_name TEXT NOT NULL,
    payload JSONB NOT NULL,
    cron_expression TEXT NOT NULL,
    priority SMALLINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_enqueued_at TIMESTAMPTZ,
    next_execution_at TIMESTAMPTZ NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT true,
    max_runs INTEGER,
    run_count INT NOT NULL DEFAULT 0,

    -- Constraints
    CHECK (run_count >= 0),
    CHECK (max_runs IS NULL OR max_runs > 0),
    CHECK (priority >= -128 AND priority <= 127)
);

CREATE INDEX IF NOT EXISTS adc_cron_jobs_v2_next_execution_idx ON adc_cron_jobs_v2 (
    enabled,
    next_execution_at ASC,
    queue
) WHERE enabled = true;

CREATE INDEX IF NOT EXISTS adc_cron_jobs_v2_type_idx ON adc_cron_jobs_v2 (
    type_hash
);
