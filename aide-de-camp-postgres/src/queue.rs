use crate::job_handle::PostgresJobHandle;
use crate::types::JobRow;
use aide_de_camp::core::job_processor::JobProcessor;
use aide_de_camp::core::queue::{InternalQueue, Queue, QueueError, ScheduleOptions};
use aide_de_camp::core::DateTime;
use anyhow::Context;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool, QueryBuilder};
use tracing::instrument;
use uuid::Uuid;

/// An implementation of the Queue backed by PostgreSQL
#[derive(Clone)]
pub struct PostgresQueue {
    pub(crate) pool: PgPool,
}

impl PostgresQueue {
    pub fn with_pool(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl Queue for PostgresQueue {
    type JobHandle = PostgresJobHandle;

    #[instrument(skip_all, err, ret, fields(type_name = J::type_name(), type_hash = J::type_hash()))]
    async fn schedule<J>(
        &self,
        payload: J::Payload,
        options: ScheduleOptions,
    ) -> Result<Uuid, QueueError>
    where
        J: JobProcessor + 'static,
        J::Payload: Serialize + for<'de> Deserialize<'de>,
    {
        let type_hash = J::type_hash();
        let type_name = J::type_name();
        let payload_value = serde_json::to_value(&payload)
            .map_err(|e| QueueError::serialize_error(type_name.to_string(), e))?;

        let jid = Uuid::now_v7();
        let priority_i16 = options.priority() as i16;

        sqlx::query(
            "INSERT INTO adc_queue_v2 (jid, type_hash, type_name, payload, scheduled_at, priority)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(jid)
        .bind(type_hash as i64)
        .bind(type_name)
        .bind(payload_value)
        .bind(options.scheduled_at())
        .bind(priority_i16)
        .execute(&self.pool)
        .await
        .context("Failed to add job to the queue")?;

        Ok(jid)
    }

    #[instrument(skip_all, err)]
    async fn poll_next(
        &self,
        type_hashes: &[u64],
        now: DateTime,
    ) -> Result<Option<PostgresJobHandle>, QueueError> {
        let type_hashes_i64: Vec<i64> = type_hashes.iter().map(|&h| h as i64).collect();

        let mut builder = QueryBuilder::new("UPDATE adc_queue_v2 SET started_at=NOW() ");
        let query = {
            builder.push(
                "WHERE jid IN (SELECT jid FROM adc_queue_v2 WHERE started_at IS NULL AND queue='default' AND scheduled_at <="
            );
            builder.push_bind(now);
            builder.push(" AND type_hash IN (");
            {
                let mut separated = builder.separated(",");
                for &type_hash in &type_hashes_i64 {
                    separated.push_bind(type_hash);
                }
            }
            builder.push(") ORDER BY priority DESC, scheduled_at ASC FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING *");
            builder.build()
        };
        let row = query
            .try_map(|row| JobRow::from_row(&row))
            .fetch_optional(&self.pool)
            .await
            .context("Failed to check out a job from the queue")?;

        if let Some(row) = row {
            Ok(Some(PostgresJobHandle::new(row, self.pool.clone())))
        } else {
            Ok(None)
        }
    }

    #[instrument(skip_all, err)]
    async fn cancel_job(&self, job_id: Uuid) -> Result<bool, QueueError> {
        let result = sqlx::query("DELETE FROM adc_queue_v2 WHERE started_at IS NULL AND jid = $1")
            .bind(job_id)
            .execute(&self.pool)
            .await
            .context("Failed to remove job from the queue")?;
        Ok(result.rows_affected() > 0)
    }

    #[allow(clippy::or_fun_call)]
    #[instrument(skip_all, err)]
    async fn unschedule_job<J>(&self, job_id: Uuid) -> Result<J::Payload, QueueError>
    where
        J: JobProcessor + 'static,
        J::Payload: for<'de> Deserialize<'de>,
    {
        let type_hash = J::type_hash() as i64;
        let type_name = J::type_name();

        let payload = sqlx::query_scalar::<_, serde_json::Value>(
            "DELETE FROM adc_queue_v2 WHERE started_at IS NULL AND jid = $1 AND type_hash = $2 RETURNING payload"
        )
        .bind(job_id)
        .bind(type_hash)
        .fetch_optional(&self.pool)
        .await
        .context("Failed to remove job from the queue")?
        .ok_or_else(|| QueueError::job_not_found_with_type(job_id, type_name.to_string()))?;

        let payload_bytes = serde_json::to_vec(&payload)
            .map_err(|e| QueueError::serialize_error(type_name.to_string(), e))?;

        let decoded = serde_json::from_value(payload).map_err(|e| {
            QueueError::deserialize_error(job_id, type_name.to_string(), &payload_bytes, e)
        })?;
        Ok(decoded)
    }
}

#[async_trait]
impl InternalQueue for PostgresQueue {
    #[instrument(skip_all, err, ret, fields(type_name = %type_name, type_hash = %type_hash))]
    async fn schedule_untyped(
        &self,
        type_hash: u64,
        type_name: &str,
        payload: serde_json::Value,
        options: ScheduleOptions,
    ) -> Result<Uuid, QueueError> {
        let jid = Uuid::now_v7();
        let priority_i16 = options.priority() as i16;

        sqlx::query(
            "INSERT INTO adc_queue_v2 (jid, type_hash, type_name, payload, scheduled_at, priority)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(jid)
        .bind(type_hash as i64)
        .bind(type_name)
        .bind(payload)
        .bind(options.scheduled_at())
        .bind(priority_i16)
        .execute(&self.pool)
        .await
        .context("Failed to add job to the queue")?;

        Ok(jid)
    }
}
