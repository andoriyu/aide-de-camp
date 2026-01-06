CREATE TABLE IF NOT EXISTS adc_cron_jobs (
 cron_id TEXT PRIMARY KEY,
 queue TEXT NOT NULL default 'default',
 job_type TEXT not null,
 payload blob not null,
 cron_expression TEXT not null,
 priority int not null default 0,
 created_at INTEGER not null default (strftime('%s', 'now')),
 last_enqueued_at INTEGER,
 next_execution_at INTEGER not null,
 enabled int not null default 1,
 max_runs INTEGER,
 run_count int not null default 0
);

CREATE INDEX IF NOT EXISTS adc_cron_jobs_next_execution ON adc_cron_jobs (
    enabled,
    next_execution_at asc,
    queue
) WHERE enabled = 1;
