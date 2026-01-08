CREATE TABLE IF NOT EXISTS adc_cron_jobs (
 cron_id TEXT PRIMARY KEY,
 queue TEXT NOT NULL DEFAULT 'default',
 job_type TEXT NOT NULL,
 payload JSONB NOT NULL,
 cron_expression TEXT NOT NULL,
 priority SMALLINT NOT NULL DEFAULT 0,
 created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT,
 last_enqueued_at BIGINT,
 next_execution_at BIGINT NOT NULL,
 enabled BOOLEAN NOT NULL DEFAULT true,
 max_runs INTEGER,
 run_count INT NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS adc_cron_jobs_next_execution ON adc_cron_jobs (
    enabled,
    next_execution_at ASC,
    queue
) WHERE enabled = true;
