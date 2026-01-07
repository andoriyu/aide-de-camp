//! CronQueue implementation for PostgreSQL.

use crate::queue::PostgresQueue;
use aide_de_camp::core::cron::CronSchedule;
use aide_de_camp::core::cron_queue::{CronError, CronJob, CronQueue};
use aide_de_camp::core::job_processor::JobProcessor;
use aide_de_camp::core::{DateTime, Utc};
use anyhow::Context;
use async_trait::async_trait;
use serde::Serialize;
use tracing::instrument;

// Helper to convert PostgreSQL BIGINT (Unix timestamp) to DateTime
fn timestamp_to_datetime(ts: i64) -> DateTime {
    use sqlx::types::chrono::TimeZone;
    Utc.timestamp_opt(ts, 0).unwrap()
}

// Helper to convert DateTime to PostgreSQL BIGINT (Unix timestamp)
fn datetime_to_timestamp(dt: DateTime) -> i64 {
    dt.timestamp()
}

#[async_trait]
impl CronQueue for PostgresQueue {
    #[instrument(skip_all, err, fields(job_type = J::name(), cron_expression))]
    async fn schedule_cron<J>(
        &self,
        cron_expression: &str,
        payload: J::Payload,
        priority: i8,
    ) -> Result<String, CronError>
    where
        J: JobProcessor + 'static,
        J::Payload: Serialize,
    {
        tracing::Span::current().record("cron_expression", cron_expression);

        // Validate cron expression and calculate next execution
        let schedule = CronSchedule::parse(cron_expression)?;
        let next_execution_at = schedule
            .next_from_now()
            .ok_or(CronError::NoUpcomingExecution)?;

        // Serialize payload to JSON
        let payload_json =
            serde_json::to_value(&payload).map_err(|e| CronError::ParseError(e.to_string()))?;

        // Generate unique ID
        let cron_id = aide_de_camp::core::new_xid().to_string();
        let job_type = J::name();
        let priority_i16 = priority as i16;

        // Insert into database
        sqlx::query(
            "INSERT INTO adc_cron_jobs (
                cron_id, queue, job_type, payload, cron_expression,
                priority, next_execution_at, enabled, run_count
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
        .bind(&cron_id)
        .bind("default")
        .bind(job_type)
        .bind(&payload_json)
        .bind(cron_expression)
        .bind(priority_i16)
        .bind(datetime_to_timestamp(next_execution_at))
        .bind(true) // enabled = true
        .bind(0) // run_count = 0
        .execute(&self.pool)
        .await
        .context("Failed to insert cron job")?;

        Ok(cron_id)
    }

    #[instrument(skip_all, err)]
    async fn list_cron_jobs(&self, job_type: Option<&str>) -> Result<Vec<CronJob>, CronError> {
        let rows = if let Some(job_type) = job_type {
            sqlx::query_as::<_, CronJobRow>(
                "SELECT cron_id, queue, job_type, payload, cron_expression, priority,
                        created_at, last_enqueued_at, next_execution_at, enabled, max_runs, run_count
                 FROM adc_cron_jobs
                 WHERE job_type = $1
                 ORDER BY next_execution_at ASC",
            )
            .bind(job_type)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query_as::<_, CronJobRow>(
                "SELECT cron_id, queue, job_type, payload, cron_expression, priority,
                        created_at, last_enqueued_at, next_execution_at, enabled, max_runs, run_count
                 FROM adc_cron_jobs
                 ORDER BY next_execution_at ASC",
            )
            .fetch_all(&self.pool)
            .await
        };

        let rows = rows.context("Failed to list cron jobs")?;
        Ok(rows.into_iter().map(|row| row.into()).collect())
    }

    #[instrument(skip_all, err, fields(cron_id))]
    async fn get_cron_job(&self, cron_id: &str) -> Result<CronJob, CronError> {
        let row = sqlx::query_as::<_, CronJobRow>(
            "SELECT cron_id, queue, job_type, payload, cron_expression, priority,
                    created_at, last_enqueued_at, next_execution_at, enabled, max_runs, run_count
             FROM adc_cron_jobs
             WHERE cron_id = $1",
        )
        .bind(cron_id)
        .fetch_optional(&self.pool)
        .await
        .context("Failed to fetch cron job")?
        .ok_or_else(|| CronError::ParseError(format!("Cron job not found: {}", cron_id)))?;

        Ok(row.into())
    }

    #[instrument(skip_all, err, fields(cron_id, cron_expression))]
    async fn update_cron_schedule(
        &self,
        cron_id: &str,
        cron_expression: &str,
    ) -> Result<(), CronError> {
        // Validate cron expression and calculate next execution
        let schedule = CronSchedule::parse(cron_expression)?;
        let next_execution_at = schedule
            .next_from_now()
            .ok_or(CronError::NoUpcomingExecution)?;

        let result = sqlx::query(
            "UPDATE adc_cron_jobs
             SET cron_expression = $1, next_execution_at = $2
             WHERE cron_id = $3",
        )
        .bind(cron_expression)
        .bind(datetime_to_timestamp(next_execution_at))
        .bind(cron_id)
        .execute(&self.pool)
        .await
        .context("Failed to update cron schedule")?;

        if result.rows_affected() == 0 {
            return Err(CronError::ParseError(format!(
                "Cron job not found: {}",
                cron_id
            )));
        }

        Ok(())
    }

    #[instrument(skip_all, err, fields(cron_id, enabled))]
    async fn set_cron_enabled(&self, cron_id: &str, enabled: bool) -> Result<(), CronError> {
        let result = sqlx::query(
            "UPDATE adc_cron_jobs
             SET enabled = $1
             WHERE cron_id = $2",
        )
        .bind(enabled)
        .bind(cron_id)
        .execute(&self.pool)
        .await
        .context("Failed to update cron enabled status")?;

        if result.rows_affected() == 0 {
            return Err(CronError::ParseError(format!(
                "Cron job not found: {}",
                cron_id
            )));
        }

        Ok(())
    }

    #[instrument(skip_all, err, fields(cron_id))]
    async fn delete_cron_job(&self, cron_id: &str) -> Result<(), CronError> {
        let result = sqlx::query("DELETE FROM adc_cron_jobs WHERE cron_id = $1")
            .bind(cron_id)
            .execute(&self.pool)
            .await
            .context("Failed to delete cron job")?;

        if result.rows_affected() == 0 {
            return Err(CronError::ParseError(format!(
                "Cron job not found: {}",
                cron_id
            )));
        }

        Ok(())
    }

    #[instrument(skip_all, err, fields(cron_id, job_type = J::name()))]
    async fn update_cron_payload<J>(
        &self,
        cron_id: &str,
        payload: J::Payload,
    ) -> Result<(), CronError>
    where
        J: JobProcessor + 'static,
        J::Payload: Serialize,
    {
        // Serialize payload to JSON
        let payload_json =
            serde_json::to_value(&payload).map_err(|e| CronError::ParseError(e.to_string()))?;

        let result = sqlx::query(
            "UPDATE adc_cron_jobs
             SET payload = $1
             WHERE cron_id = $2",
        )
        .bind(&payload_json)
        .bind(cron_id)
        .execute(&self.pool)
        .await
        .context("Failed to update cron payload")?;

        if result.rows_affected() == 0 {
            return Err(CronError::ParseError(format!(
                "Cron job not found: {}",
                cron_id
            )));
        }

        Ok(())
    }

    #[instrument(skip_all, err, fields(now, limit))]
    async fn poll_due_cron_jobs(
        &self,
        now: DateTime,
        limit: usize,
    ) -> Result<Vec<CronJob>, CronError> {
        let now_ts = datetime_to_timestamp(now);
        let limit_i64 = limit as i64;

        let rows = sqlx::query_as::<_, CronJobRow>(
            "SELECT cron_id, queue, job_type, payload, cron_expression, priority,
                    created_at, last_enqueued_at, next_execution_at, enabled, max_runs, run_count
             FROM adc_cron_jobs
             WHERE enabled = true AND next_execution_at <= $1
             ORDER BY next_execution_at ASC, priority DESC
             LIMIT $2",
        )
        .bind(now_ts)
        .bind(limit_i64)
        .fetch_all(&self.pool)
        .await
        .context("Failed to poll due cron jobs")?;

        Ok(rows.into_iter().map(|row| row.into()).collect())
    }

    #[instrument(skip_all, err, fields(cron_id, next_execution_at))]
    async fn mark_cron_executed(
        &self,
        cron_id: &str,
        next_execution_at: DateTime,
    ) -> Result<(), CronError> {
        let now_ts = datetime_to_timestamp(Utc::now());
        let next_ts = datetime_to_timestamp(next_execution_at);

        let result = sqlx::query(
            "UPDATE adc_cron_jobs
             SET last_enqueued_at = $1,
                 next_execution_at = $2,
                 run_count = run_count + 1
             WHERE cron_id = $3",
        )
        .bind(now_ts)
        .bind(next_ts)
        .bind(cron_id)
        .execute(&self.pool)
        .await
        .context("Failed to mark cron job as executed")?;

        if result.rows_affected() == 0 {
            return Err(CronError::ParseError(format!(
                "Cron job not found: {}",
                cron_id
            )));
        }

        Ok(())
    }
}

// Internal struct for PostgreSQL row mapping
#[derive(sqlx::FromRow)]
struct CronJobRow {
    cron_id: String,
    queue: String,
    job_type: String,
    payload: serde_json::Value,
    cron_expression: String,
    priority: i16,
    created_at: i64,
    last_enqueued_at: Option<i64>,
    next_execution_at: i64,
    enabled: bool,
    max_runs: Option<i32>,
    run_count: i32,
}

impl From<CronJobRow> for CronJob {
    fn from(row: CronJobRow) -> Self {
        CronJob {
            cron_id: row.cron_id,
            queue: row.queue,
            job_type: row.job_type,
            payload: row.payload,
            cron_expression: row.cron_expression,
            priority: row.priority as i8,
            created_at: timestamp_to_datetime(row.created_at),
            last_enqueued_at: row.last_enqueued_at.map(timestamp_to_datetime),
            next_execution_at: timestamp_to_datetime(row.next_execution_at),
            enabled: row.enabled,
            max_runs: row.max_runs,
            run_count: row.run_count,
        }
    }
}
