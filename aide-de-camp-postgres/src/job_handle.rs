use crate::types::JobRow;
use aide_de_camp::core::job_handle::JobHandle;
use aide_de_camp::core::queue::QueueError;
use aide_de_camp::core::{Bytes, Xid};
use anyhow::Context;
use async_trait::async_trait;
use sqlx::PgPool;

pub struct PostgresJobHandle {
    pool: PgPool,
    row: JobRow,
}

#[async_trait]
impl JobHandle for PostgresJobHandle {
    fn id(&self) -> Xid {
        self.row.jid()
    }

    fn job_type(&self) -> &str {
        &self.row.job_type
    }

    fn payload(&self) -> Bytes {
        self.row.clone().into_payload()
    }

    fn retries(&self) -> u32 {
        self.row.retries as u32
    }

    async fn complete(mut self) -> Result<(), QueueError> {
        let jid = &self.row.jid;
        sqlx::query("DELETE FROM adc_queue where jid = $1")
            .bind(jid)
            .execute(&self.pool)
            .await
            .context("Failed to mark job as completed")?;
        Ok(())
    }

    async fn fail(mut self) -> Result<(), QueueError> {
        let jid = &self.row.jid;
        sqlx::query("UPDATE adc_queue SET started_at=null, retries=retries+1 WHERE jid = $1")
            .bind(jid)
            .execute(&self.pool)
            .await
            .context("Failed to mark job as failed")?;
        Ok(())
    }

    async fn dead_queue(mut self) -> Result<(), QueueError> {
        let jid = &self.row.jid;

        let mut tx = self
            .pool
            .begin()
            .await
            .context("Failed to start transaction")?;

        // Move the job to dead queue
        sqlx::query(
            "INSERT INTO adc_dead_queue (jid, queue, job_type, payload, retries, scheduled_at, started_at, enqueued_at)
             SELECT jid, queue, job_type, payload, retries, scheduled_at, started_at, enqueued_at
             FROM adc_queue WHERE jid = $1"
        )
        .bind(jid)
        .execute(&mut *tx)
        .await
        .context("Failed to move job to dead queue")?;

        sqlx::query("DELETE FROM adc_queue WHERE jid = $1")
            .bind(jid)
            .execute(&mut *tx)
            .await
            .context("Failed to delete job from the queue")?;

        tx.commit().await.context("Failed to commit transaction")?;
        Ok(())
    }
}

impl PostgresJobHandle {
    pub(crate) fn new(row: JobRow, pool: PgPool) -> Self {
        Self { row, pool }
    }
}
