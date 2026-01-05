use crate::job_handle::PostgresJobHandle;
use crate::types::JobRow;
use aide_de_camp::core::job_processor::JobProcessor;
use aide_de_camp::core::queue::{Queue, QueueError};
use aide_de_camp::core::{new_xid, DateTime, Xid};
use anyhow::Context;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool, QueryBuilder};
use tracing::instrument;

/// An implementation of the Queue backed by PostgreSQL
#[derive(Clone)]
pub struct PostgresQueue {
    pool: PgPool,
}

impl PostgresQueue {
    pub fn with_pool(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl Queue for PostgresQueue {
    type JobHandle = PostgresJobHandle;

    #[instrument(skip_all, err, ret, fields(job_type = J::name(), payload_size))]
    async fn schedule_at<J>(
        &self,
        payload: J::Payload,
        scheduled_at: DateTime,
        priority: i8,
    ) -> Result<Xid, QueueError>
    where
        J: JobProcessor + 'static,
        J::Payload: Serialize,
    {
        let payload = serde_json::to_value(&payload).map_err(QueueError::SerializeError)?;
        let jid = new_xid();
        let jid_string = jid.to_string();
        let job_type = J::name();
        let scheduled_at_ts = scheduled_at.timestamp();
        let priority_i16 = priority as i16;

        tracing::Span::current().record("payload_size", payload.to_string().len());

        sqlx::query(
            "INSERT INTO adc_queue (jid,job_type,payload,scheduled_at,priority) VALUES ($1,$2,$3,$4,$5)"
        )
        .bind(jid_string)
        .bind(job_type)
        .bind(payload)
        .bind(scheduled_at_ts)
        .bind(priority_i16)
        .execute(&self.pool)
        .await
        .context("Failed to add job to the queue")?;
        Ok(jid)
    }

    #[instrument(skip_all, err)]
    async fn poll_next_with_instant(
        &self,
        job_types: &[&str],
        now: DateTime,
    ) -> Result<Option<PostgresJobHandle>, QueueError> {
        let now_ts = now.timestamp();
        let mut builder =
            QueryBuilder::new("UPDATE adc_queue SET started_at=EXTRACT(EPOCH FROM NOW())::BIGINT ");
        let query = {
            builder.push(
                "WHERE jid IN (SELECT jid FROM adc_queue WHERE started_at IS NULL AND queue='default' AND scheduled_at <="
            );
            builder.push_bind(now_ts);
            builder.push(" AND job_type IN (");
            {
                let mut separated = builder.separated(",");
                for job_type in job_types {
                    separated.push_bind(job_type);
                }
            }
            builder.push(") ORDER BY priority DESC LIMIT 1) RETURNING *");
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
    async fn cancel_job(&self, job_id: Xid) -> Result<(), QueueError> {
        let jid = job_id.to_string();
        let result = sqlx::query("DELETE FROM adc_queue WHERE started_at IS NULL and jid = $1")
            .bind(jid)
            .execute(&self.pool)
            .await
            .context("Failed to remove job from the queue")?;
        if result.rows_affected() == 0 {
            Err(QueueError::JobNotFound(job_id))
        } else {
            Ok(())
        }
    }

    #[allow(clippy::or_fun_call)]
    #[instrument(skip_all, err)]
    async fn unschedule_job<J>(&self, job_id: Xid) -> Result<J::Payload, QueueError>
    where
        J: JobProcessor + 'static,
        J::Payload: for<'de> Deserialize<'de>,
    {
        let jid = job_id.to_string();
        let job_type = J::name();
        let payload = sqlx::query_scalar::<_, serde_json::Value>(
            "DELETE FROM adc_queue WHERE started_at IS NULL and jid = $1 AND job_type = $2 RETURNING payload"
        )
        .bind(jid)
        .bind(job_type)
        .fetch_optional(&self.pool)
        .await
        .context("Failed to remove job from the queue")?
        .ok_or(QueueError::JobNotFound(job_id))?;
        let decoded = serde_json::from_value(payload).map_err(QueueError::DeserializeError)?;
        Ok(decoded)
    }
}
