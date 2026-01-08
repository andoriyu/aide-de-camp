use crate::types::JobRow;
use aide_de_camp::core::job_handle::{ErrorContext, JobHandle};
use aide_de_camp::core::queue::QueueError;
use aide_de_camp::core::Bytes;
use anyhow::Context;
use async_trait::async_trait;
use sqlx::PgPool;
use uuid::Uuid;

pub struct PostgresJobHandle {
    pool: PgPool,
    row: JobRow,
}

#[async_trait]
impl JobHandle for PostgresJobHandle {
    fn id(&self) -> Uuid {
        self.row.id()
    }

    fn type_hash(&self) -> u64 {
        self.row.type_hash()
    }

    fn type_name(&self) -> &str {
        self.row.type_name()
    }

    fn payload(&self) -> Bytes {
        self.row.payload_bytes()
    }

    fn retries(&self) -> u32 {
        self.row.retries()
    }

    async fn complete(self) -> Result<(), QueueError> {
        let jid = self.row.id();
        sqlx::query("DELETE FROM adc_queue_v2 WHERE jid = $1")
            .bind(jid)
            .execute(&self.pool)
            .await
            .context("Failed to mark job as completed")?;
        Ok(())
    }

    async fn fail(self) -> Result<(), QueueError> {
        let jid = self.row.id();
        sqlx::query("UPDATE adc_queue_v2 SET started_at=NULL, retries=retries+1 WHERE jid = $1")
            .bind(jid)
            .execute(&self.pool)
            .await
            .context("Failed to mark job as failed")?;
        Ok(())
    }

    async fn dead_queue(self, error_context: Option<ErrorContext>) -> Result<(), QueueError> {
        let jid = self.row.id();

        let mut tx = self
            .pool
            .begin()
            .await
            .context("Failed to start transaction")?;

        // Serialize error context if provided
        let (error_message, error_context_json) = if let Some(ctx) = error_context {
            let json = serde_json::to_value(&ctx).ok();
            (Some(ctx.error_message), json)
        } else {
            (None, None)
        };

        // Move the job to dead queue with error context
        // ON CONFLICT DO NOTHING handles race conditions in concurrent dead_queue calls
        let insert_result = sqlx::query(
            "INSERT INTO adc_dead_queue_v2 (jid, queue, type_hash, type_name, payload, retries, scheduled_at, started_at, enqueued_at, priority, error_message, error_context)
             SELECT jid, queue, type_hash, type_name, payload, retries, scheduled_at, started_at, enqueued_at, priority, $2, $3
             FROM adc_queue_v2 WHERE jid = $1
             ON CONFLICT (jid) DO NOTHING"
        )
        .bind(jid)
        .bind(error_message)
        .bind(error_context_json)
        .execute(&mut *tx)
        .await
        .context("Failed to move job to dead queue")?;

        // Only delete if we actually inserted (rows_affected > 0)
        // If rows_affected is 0, either the job doesn't exist or it's already in dead queue
        if insert_result.rows_affected() > 0 {
            sqlx::query("DELETE FROM adc_queue_v2 WHERE jid = $1")
                .bind(jid)
                .execute(&mut *tx)
                .await
                .context("Failed to delete job from the queue")?;
        }

        tx.commit().await.context("Failed to commit transaction")?;
        Ok(())
    }
}

impl PostgresJobHandle {
    pub(crate) fn new(row: JobRow, pool: PgPool) -> Self {
        Self { row, pool }
    }
}
