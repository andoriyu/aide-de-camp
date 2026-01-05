CREATE TABLE IF NOT EXISTS adc_queue (
 jid TEXT PRIMARY KEY,
 queue TEXT NOT NULL DEFAULT 'default',
 job_type TEXT NOT NULL,
 payload JSONB NOT NULL,
 retries INT NOT NULL DEFAULT 0,
 scheduled_at BIGINT NOT NULL,
 started_at BIGINT,
 enqueued_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT
);

CREATE TABLE IF NOT EXISTS adc_dead_queue (
 jid TEXT PRIMARY KEY,
 queue TEXT NOT NULL,
 job_type TEXT NOT NULL,
 payload JSONB NOT NULL,
 retries INT NOT NULL,
 scheduled_at BIGINT NOT NULL,
 started_at BIGINT NOT NULL,
 enqueued_at BIGINT NOT NULL,
 died_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT
);

CREATE INDEX IF NOT EXISTS adc_queue_jobs ON adc_queue (
    scheduled_at ASC,
    started_at ASC,
    queue,
    job_type
);
