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
use uuid::Uuid;

#[async_trait]
impl CronQueue for PostgresQueue {
    #[instrument(skip_all, err, fields(job_type = J::type_name(), cron_expression))]
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
            .ok_or_else(|| CronError::NoUpcomingExecution(cron_expression.to_string()))?;

        // Serialize payload to JSON
        let payload_json = serde_json::to_value(&payload).map_err(|e| {
            CronError::DatabaseError(anyhow::anyhow!("Failed to serialize payload: {}", e))
        })?;

        // Generate unique ID
        let cron_id_uuid = Uuid::now_v7();
        let type_hash = J::type_hash();
        let type_name = J::type_name();
        let priority_i16 = priority as i16;

        // Insert into database
        sqlx::query(
            "INSERT INTO adc_cron_jobs_v2 (
                cron_id, queue, type_hash, type_name, payload, cron_expression,
                priority, next_execution_at, enabled, run_count
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        )
        .bind(cron_id_uuid)
        .bind("default")
        .bind(type_hash as i64)
        .bind(type_name)
        .bind(&payload_json)
        .bind(cron_expression)
        .bind(priority_i16)
        .bind(next_execution_at)
        .bind(true) // enabled = true
        .bind(0) // run_count = 0
        .execute(&self.pool)
        .await
        .context("Failed to insert cron job")?;

        Ok(cron_id_uuid.to_string())
    }

    #[instrument(skip_all, err)]
    async fn list_cron_jobs(&self, job_type: Option<&str>) -> Result<Vec<CronJob>, CronError> {
        let rows = if let Some(job_type) = job_type {
            sqlx::query_as::<_, CronJobRow>(
                "SELECT cron_id, queue, type_hash, type_name, payload, cron_expression, priority,
                        created_at, last_enqueued_at, next_execution_at, enabled, max_runs, run_count
                 FROM adc_cron_jobs_v2
                 WHERE type_name = $1
                 ORDER BY next_execution_at ASC",
            )
            .bind(job_type)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query_as::<_, CronJobRow>(
                "SELECT cron_id, queue, type_hash, type_name, payload, cron_expression, priority,
                        created_at, last_enqueued_at, next_execution_at, enabled, max_runs, run_count
                 FROM adc_cron_jobs_v2
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
        let cron_uuid = Uuid::parse_str(cron_id).map_err(|_| CronError::CronNotFound {
            cron_id: Uuid::nil(),
            expected_type: None,
        })?;

        let row = sqlx::query_as::<_, CronJobRow>(
            "SELECT cron_id, queue, type_hash, type_name, payload, cron_expression, priority,
                    created_at, last_enqueued_at, next_execution_at, enabled, max_runs, run_count
             FROM adc_cron_jobs_v2
             WHERE cron_id = $1",
        )
        .bind(cron_uuid)
        .fetch_optional(&self.pool)
        .await
        .context("Failed to fetch cron job")?
        .ok_or_else(|| CronError::CronNotFound {
            cron_id: cron_uuid,
            expected_type: None,
        })?;

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
            .ok_or_else(|| CronError::NoUpcomingExecution(cron_expression.to_string()))?;

        let cron_uuid = Uuid::parse_str(cron_id)
            .map_err(|e| CronError::DatabaseError(anyhow::anyhow!("Invalid UUID: {}", e)))?;

        let result = sqlx::query(
            "UPDATE adc_cron_jobs_v2
             SET cron_expression = $1, next_execution_at = $2
             WHERE cron_id = $3",
        )
        .bind(cron_expression)
        .bind(next_execution_at)
        .bind(cron_uuid)
        .execute(&self.pool)
        .await
        .context("Failed to update cron schedule")?;

        if result.rows_affected() == 0 {
            let cron_uuid = Uuid::parse_str(cron_id)
                .map_err(|e| CronError::DatabaseError(anyhow::anyhow!("Invalid UUID: {}", e)))?;
            return Err(CronError::CronNotFound {
                cron_id: cron_uuid,
                expected_type: None,
            });
        }

        Ok(())
    }

    #[instrument(skip_all, err, fields(cron_id, enabled))]
    async fn set_cron_enabled(&self, cron_id: &str, enabled: bool) -> Result<(), CronError> {
        let cron_uuid = Uuid::parse_str(cron_id)
            .map_err(|e| CronError::DatabaseError(anyhow::anyhow!("Invalid UUID: {}", e)))?;

        let result = sqlx::query(
            "UPDATE adc_cron_jobs_v2
             SET enabled = $1
             WHERE cron_id = $2",
        )
        .bind(enabled)
        .bind(cron_uuid)
        .execute(&self.pool)
        .await
        .context("Failed to update cron enabled status")?;

        if result.rows_affected() == 0 {
            let cron_uuid = Uuid::parse_str(cron_id)
                .map_err(|e| CronError::DatabaseError(anyhow::anyhow!("Invalid UUID: {}", e)))?;
            return Err(CronError::CronNotFound {
                cron_id: cron_uuid,
                expected_type: None,
            });
        }

        Ok(())
    }

    #[instrument(skip_all, err, fields(cron_id))]
    async fn delete_cron_job(&self, cron_id: &str) -> Result<(), CronError> {
        let cron_uuid = Uuid::parse_str(cron_id)
            .map_err(|e| CronError::DatabaseError(anyhow::anyhow!("Invalid UUID: {}", e)))?;

        let result = sqlx::query("DELETE FROM adc_cron_jobs_v2 WHERE cron_id = $1")
            .bind(cron_uuid)
            .execute(&self.pool)
            .await
            .context("Failed to delete cron job")?;

        if result.rows_affected() == 0 {
            let cron_uuid = Uuid::parse_str(cron_id)
                .map_err(|e| CronError::DatabaseError(anyhow::anyhow!("Invalid UUID: {}", e)))?;
            return Err(CronError::CronNotFound {
                cron_id: cron_uuid,
                expected_type: None,
            });
        }

        Ok(())
    }

    #[instrument(skip_all, err, fields(cron_id, job_type = J::type_name()))]
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
        let payload_json = serde_json::to_value(&payload).map_err(|e| {
            CronError::DatabaseError(anyhow::anyhow!("Failed to serialize payload: {}", e))
        })?;

        let cron_uuid = Uuid::parse_str(cron_id)
            .map_err(|e| CronError::DatabaseError(anyhow::anyhow!("Invalid UUID: {}", e)))?;

        let result = sqlx::query(
            "UPDATE adc_cron_jobs_v2
             SET payload = $1
             WHERE cron_id = $2",
        )
        .bind(&payload_json)
        .bind(cron_uuid)
        .execute(&self.pool)
        .await
        .context("Failed to update cron payload")?;

        if result.rows_affected() == 0 {
            let cron_uuid = Uuid::parse_str(cron_id)
                .map_err(|e| CronError::DatabaseError(anyhow::anyhow!("Invalid UUID: {}", e)))?;
            return Err(CronError::CronNotFound {
                cron_id: cron_uuid,
                expected_type: Some(J::type_name().to_string()),
            });
        }

        Ok(())
    }

    #[instrument(skip_all, err, fields(now, limit))]
    async fn poll_due_cron_jobs(
        &self,
        now: DateTime,
        limit: usize,
    ) -> Result<Vec<CronJob>, CronError> {
        let limit_i64 = limit as i64;

        let rows = sqlx::query_as::<_, CronJobRow>(
            "SELECT cron_id, queue, type_hash, type_name, payload, cron_expression, priority,
                    created_at, last_enqueued_at, next_execution_at, enabled, max_runs, run_count
             FROM adc_cron_jobs_v2
             WHERE enabled = true AND next_execution_at <= $1
             ORDER BY next_execution_at ASC, priority DESC
             LIMIT $2",
        )
        .bind(now)
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
        let now = Utc::now();
        let cron_uuid = Uuid::parse_str(cron_id)
            .map_err(|e| CronError::DatabaseError(anyhow::anyhow!("Invalid UUID: {}", e)))?;

        let result = sqlx::query(
            "UPDATE adc_cron_jobs_v2
             SET last_enqueued_at = $1,
                 next_execution_at = $2,
                 run_count = run_count + 1
             WHERE cron_id = $3",
        )
        .bind(now)
        .bind(next_execution_at)
        .bind(cron_uuid)
        .execute(&self.pool)
        .await
        .context("Failed to mark cron job as executed")?;

        if result.rows_affected() == 0 {
            let cron_uuid = Uuid::parse_str(cron_id)
                .map_err(|e| CronError::DatabaseError(anyhow::anyhow!("Invalid UUID: {}", e)))?;
            return Err(CronError::CronNotFound {
                cron_id: cron_uuid,
                expected_type: None,
            });
        }

        Ok(())
    }
}

// Internal struct for PostgreSQL row mapping
#[derive(sqlx::FromRow)]
struct CronJobRow {
    cron_id: Uuid,
    queue: String,
    type_hash: i64,
    type_name: String,
    payload: serde_json::Value,
    cron_expression: String,
    priority: i16,
    created_at: DateTime,
    last_enqueued_at: Option<DateTime>,
    next_execution_at: DateTime,
    enabled: bool,
    max_runs: Option<i32>,
    run_count: i32,
}

impl From<CronJobRow> for CronJob {
    fn from(row: CronJobRow) -> Self {
        CronJob {
            cron_id: row.cron_id.to_string(),
            queue: row.queue,
            type_hash: row.type_hash as u64,
            type_name: row.type_name,
            payload: row.payload,
            cron_expression: row.cron_expression,
            priority: row.priority as i8,
            created_at: row.created_at,
            last_enqueued_at: row.last_enqueued_at,
            next_execution_at: row.next_execution_at,
            enabled: row.enabled,
            max_runs: row.max_runs,
            run_count: row.run_count,
        }
    }
}
