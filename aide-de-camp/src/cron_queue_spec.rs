//! Shared test specifications for CronQueue implementations.
//!
//! These test functions ensure consistent behavior across all CronQueue implementations.

/// Generate all cron queue spec test wrappers for a backend.
///
/// # Usage
///
/// ```ignore
/// // PostgreSQL example
/// aide_de_camp::generate_cron_queue_spec_tests! {
///     backend = "pg",
///     test_attr = #[sqlx::test(migrator = "MIGRATOR")],
///     setup = |pool: PgPool| PostgresCronQueue::with_pool(pool)
/// }
/// ```
#[macro_export]
macro_rules! generate_cron_queue_spec_tests {
    (
        backend = $backend:literal,
        test_attr = $test_attr:meta,
        setup = |$pool:ident: $pool_type:ty| $setup_expr:expr
    ) => {
        paste::paste! {
            #[$test_attr]
            async fn [<schedule_cron_basic_ $backend>]($pool: $pool_type) {
                let queue = $setup_expr;
                $crate::cron_queue_spec::test_schedule_cron_basic(queue).await;
            }

            #[$test_attr]
            async fn [<schedule_cron_invalid_expression_ $backend>]($pool: $pool_type) {
                let queue = $setup_expr;
                $crate::cron_queue_spec::test_schedule_cron_invalid_expression(queue).await;
            }

            #[$test_attr]
            async fn [<schedule_cron_calculates_next_execution_ $backend>]($pool: $pool_type) {
                let queue = $setup_expr;
                $crate::cron_queue_spec::test_schedule_cron_calculates_next_execution(queue).await;
            }

            #[$test_attr]
            async fn [<schedule_cron_with_priority_ $backend>]($pool: $pool_type) {
                let queue = $setup_expr;
                $crate::cron_queue_spec::test_schedule_cron_with_priority(queue).await;
            }

            #[$test_attr]
            async fn [<list_cron_jobs_empty_ $backend>]($pool: $pool_type) {
                let queue = $setup_expr;
                $crate::cron_queue_spec::test_list_cron_jobs_empty(queue).await;
            }

            #[$test_attr]
            async fn [<list_cron_jobs_all_ $backend>]($pool: $pool_type) {
                let queue = $setup_expr;
                $crate::cron_queue_spec::test_list_cron_jobs_all(queue).await;
            }

            #[$test_attr]
            async fn [<list_cron_jobs_filtered_by_type_ $backend>]($pool: $pool_type) {
                let queue = $setup_expr;
                $crate::cron_queue_spec::test_list_cron_jobs_filtered_by_type(queue).await;
            }

            #[$test_attr]
            async fn [<list_cron_jobs_ordered_by_next_execution_ $backend>]($pool: $pool_type) {
                let queue = $setup_expr;
                $crate::cron_queue_spec::test_list_cron_jobs_ordered_by_next_execution(queue).await;
            }

            #[$test_attr]
            async fn [<get_cron_job_found_ $backend>]($pool: $pool_type) {
                let queue = $setup_expr;
                $crate::cron_queue_spec::test_get_cron_job_found(queue).await;
            }

            #[$test_attr]
            async fn [<get_cron_job_not_found_ $backend>]($pool: $pool_type) {
                let queue = $setup_expr;
                $crate::cron_queue_spec::test_get_cron_job_not_found(queue).await;
            }

            #[$test_attr]
            async fn [<update_cron_schedule_valid_ $backend>]($pool: $pool_type) {
                let queue = $setup_expr;
                $crate::cron_queue_spec::test_update_cron_schedule_valid(queue).await;
            }

            #[$test_attr]
            async fn [<update_cron_schedule_invalid_ $backend>]($pool: $pool_type) {
                let queue = $setup_expr;
                $crate::cron_queue_spec::test_update_cron_schedule_invalid(queue).await;
            }

            #[$test_attr]
            async fn [<set_cron_enabled_true_ $backend>]($pool: $pool_type) {
                let queue = $setup_expr;
                $crate::cron_queue_spec::test_set_cron_enabled_true(queue).await;
            }

            #[$test_attr]
            async fn [<set_cron_enabled_false_ $backend>]($pool: $pool_type) {
                let queue = $setup_expr;
                $crate::cron_queue_spec::test_set_cron_enabled_false(queue).await;
            }

            #[$test_attr]
            async fn [<delete_cron_job_ $backend>]($pool: $pool_type) {
                let queue = $setup_expr;
                $crate::cron_queue_spec::test_delete_cron_job(queue).await;
            }

            #[$test_attr]
            async fn [<update_cron_payload_ $backend>]($pool: $pool_type) {
                let queue = $setup_expr;
                $crate::cron_queue_spec::test_update_cron_payload(queue).await;
            }

            #[$test_attr]
            async fn [<poll_due_cron_jobs_none_due_ $backend>]($pool: $pool_type) {
                let queue = $setup_expr;
                $crate::cron_queue_spec::test_poll_due_cron_jobs_none_due(queue).await;
            }

            #[$test_attr]
            async fn [<poll_due_cron_jobs_some_due_ $backend>]($pool: $pool_type) {
                let queue = $setup_expr;
                $crate::cron_queue_spec::test_poll_due_cron_jobs_some_due(queue).await;
            }

            #[$test_attr]
            async fn [<poll_due_cron_jobs_respects_limit_ $backend>]($pool: $pool_type) {
                let queue = $setup_expr;
                $crate::cron_queue_spec::test_poll_due_cron_jobs_respects_limit(queue).await;
            }

            #[$test_attr]
            async fn [<mark_cron_executed_ $backend>]($pool: $pool_type) {
                let queue = $setup_expr;
                $crate::cron_queue_spec::test_mark_cron_executed(queue).await;
            }
        }
    };
}

use crate::core::{
    cron::CronSchedule,
    cron_queue::{CronError, CronQueue},
    job_processor::JobProcessor,
    job_type_id::JobTypeId,
    Utc,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

// Test job type for cron integration tests
#[derive(Clone)]
pub struct CronTestJob;

#[derive(Debug, thiserror::Error)]
#[error("cron test job error")]
pub struct CronTestJobError;

impl From<CronTestJobError> for crate::core::job_processor::JobError {
    fn from(e: CronTestJobError) -> Self {
        crate::core::job_processor::JobError::HandlerError(anyhow::anyhow!(e.to_string()))
    }
}

impl JobTypeId for CronTestJob {
    fn type_name() -> &'static str {
        "aide_de_camp::cron_queue_spec::CronTestJob"
    }
}

#[async_trait]
impl JobProcessor for CronTestJob {
    type Payload = CronTestPayload;
    type Error = CronTestJobError;

    async fn handle(
        &self,
        _jid: Uuid,
        _payload: Self::Payload,
        _cancellation_token: CancellationToken,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CronTestPayload {
    pub message: String,
}

/// Test scheduling a basic cron job.
pub async fn test_schedule_cron_basic<C>(cron_queue: C)
where
    C: CronQueue + Send + Sync,
{
    let payload = CronTestPayload {
        message: "test".to_string(),
    };

    let cron_id = cron_queue
        .schedule_cron::<CronTestJob>("0 0 * * * *", payload, 0)
        .await
        .unwrap();

    assert!(!cron_id.is_empty());
}

/// Test scheduling with invalid cron expression.
pub async fn test_schedule_cron_invalid_expression<C>(cron_queue: C)
where
    C: CronQueue + Send + Sync,
{
    let payload = CronTestPayload {
        message: "test".to_string(),
    };

    let result = cron_queue
        .schedule_cron::<CronTestJob>("invalid cron", payload, 0)
        .await;

    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), CronError::ParseError { .. }));
}

/// Test that next_execution_at is calculated correctly.
pub async fn test_schedule_cron_calculates_next_execution<C>(cron_queue: C)
where
    C: CronQueue + Send + Sync,
{
    let payload = CronTestPayload {
        message: "test".to_string(),
    };

    let before = Utc::now();
    let cron_id = cron_queue
        .schedule_cron::<CronTestJob>("0 0 * * * *", payload, 0)
        .await
        .unwrap();

    let cron_job = cron_queue.get_cron_job(&cron_id).await.unwrap();

    // Next execution should be in the future
    assert!(cron_job.next_execution_at > before);

    // Verify the schedule is correct (top of next hour)
    let schedule = CronSchedule::parse("0 0 * * * *").unwrap();
    let expected_next = schedule.next_after(before).unwrap();

    // Should be within a few seconds of the calculated time
    let diff = (cron_job.next_execution_at - expected_next)
        .num_seconds()
        .abs();
    assert!(diff < 2);
}

/// Test scheduling with different priorities.
pub async fn test_schedule_cron_with_priority<C>(cron_queue: C)
where
    C: CronQueue + Send + Sync,
{
    let payload = CronTestPayload {
        message: "high priority".to_string(),
    };

    let cron_id = cron_queue
        .schedule_cron::<CronTestJob>("0 0 * * * *", payload, 10)
        .await
        .unwrap();

    let cron_job = cron_queue.get_cron_job(&cron_id).await.unwrap();
    assert_eq!(cron_job.priority, 10);
}

/// Test listing when no cron jobs exist.
pub async fn test_list_cron_jobs_empty<C>(cron_queue: C)
where
    C: CronQueue + Send + Sync,
{
    let jobs = cron_queue.list_cron_jobs(None).await.unwrap();
    assert!(jobs.is_empty());
}

/// Test listing all cron jobs.
pub async fn test_list_cron_jobs_all<C>(cron_queue: C)
where
    C: CronQueue + Send + Sync,
{
    // Schedule 3 cron jobs
    for i in 0..3 {
        let payload = CronTestPayload {
            message: format!("job{}", i),
        };
        cron_queue
            .schedule_cron::<CronTestJob>("0 0 * * * *", payload, 0)
            .await
            .unwrap();
    }

    let jobs = cron_queue.list_cron_jobs(None).await.unwrap();
    assert_eq!(jobs.len(), 3);
}

/// Test listing cron jobs filtered by type.
pub async fn test_list_cron_jobs_filtered_by_type<C>(cron_queue: C)
where
    C: CronQueue + Send + Sync,
{
    let payload = CronTestPayload {
        message: "test".to_string(),
    };

    cron_queue
        .schedule_cron::<CronTestJob>("0 0 * * * *", payload, 0)
        .await
        .unwrap();

    // Filter by correct type
    let jobs = cron_queue
        .list_cron_jobs(Some(CronTestJob::type_name()))
        .await
        .unwrap();
    assert_eq!(jobs.len(), 1);

    // Filter by non-existent type
    let jobs = cron_queue
        .list_cron_jobs(Some("NonExistentJob"))
        .await
        .unwrap();
    assert_eq!(jobs.len(), 0);
}

/// Test that listed jobs are ordered by next_execution_at.
pub async fn test_list_cron_jobs_ordered_by_next_execution<C>(cron_queue: C)
where
    C: CronQueue + Send + Sync,
{
    // Schedule jobs with different next execution times
    let _id1 = cron_queue
        .schedule_cron::<CronTestJob>(
            "0 0 * * * *", // Top of every hour
            CronTestPayload {
                message: "hourly".to_string(),
            },
            0,
        )
        .await
        .unwrap();

    let _id2 = cron_queue
        .schedule_cron::<CronTestJob>(
            "0 * * * * *", // Every minute
            CronTestPayload {
                message: "minutely".to_string(),
            },
            0,
        )
        .await
        .unwrap();

    let jobs = cron_queue.list_cron_jobs(None).await.unwrap();
    assert_eq!(jobs.len(), 2);

    // Minute job should come first (sooner execution)
    assert!(jobs[0].next_execution_at <= jobs[1].next_execution_at);
}

/// Test getting a specific cron job.
pub async fn test_get_cron_job_found<C>(cron_queue: C)
where
    C: CronQueue + Send + Sync,
{
    let payload = CronTestPayload {
        message: "get test".to_string(),
    };

    let cron_id = cron_queue
        .schedule_cron::<CronTestJob>("0 0 * * * *", payload.clone(), 5)
        .await
        .unwrap();

    let cron_job = cron_queue.get_cron_job(&cron_id).await.unwrap();

    assert_eq!(cron_job.cron_id, cron_id);
    assert_eq!(cron_job.type_name, CronTestJob::type_name());
    assert_eq!(cron_job.priority, 5);
}

/// Test getting a non-existent cron job.
pub async fn test_get_cron_job_not_found<C>(cron_queue: C)
where
    C: CronQueue + Send + Sync,
{
    let result = cron_queue.get_cron_job("non-existent-id").await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        CronError::CronNotFound { .. }
    ));
}

/// Test updating cron schedule with valid expression.
pub async fn test_update_cron_schedule_valid<C>(cron_queue: C)
where
    C: CronQueue + Send + Sync,
{
    let payload = CronTestPayload {
        message: "update test".to_string(),
    };

    let cron_id = cron_queue
        .schedule_cron::<CronTestJob>("0 0 * * * *", payload, 0)
        .await
        .unwrap();

    let original = cron_queue.get_cron_job(&cron_id).await.unwrap();

    // Update to a different schedule
    cron_queue
        .update_cron_schedule(&cron_id, "0 * * * * *")
        .await
        .unwrap();

    let updated = cron_queue.get_cron_job(&cron_id).await.unwrap();

    assert_eq!(updated.cron_expression, "0 * * * * *");
    assert_ne!(updated.next_execution_at, original.next_execution_at);
}

/// Test updating cron schedule with invalid expression.
pub async fn test_update_cron_schedule_invalid<C>(cron_queue: C)
where
    C: CronQueue + Send + Sync,
{
    let payload = CronTestPayload {
        message: "test".to_string(),
    };

    let cron_id = cron_queue
        .schedule_cron::<CronTestJob>("0 0 * * * *", payload, 0)
        .await
        .unwrap();

    let result = cron_queue
        .update_cron_schedule(&cron_id, "invalid expression")
        .await;

    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), CronError::ParseError { .. }));
}

/// Test enabling a cron job.
pub async fn test_set_cron_enabled_true<C>(cron_queue: C)
where
    C: CronQueue + Send + Sync,
{
    let payload = CronTestPayload {
        message: "test".to_string(),
    };

    let cron_id = cron_queue
        .schedule_cron::<CronTestJob>("0 0 * * * *", payload, 0)
        .await
        .unwrap();

    // Disable first
    cron_queue.set_cron_enabled(&cron_id, false).await.unwrap();

    let disabled = cron_queue.get_cron_job(&cron_id).await.unwrap();
    assert!(!disabled.enabled);

    // Re-enable
    cron_queue.set_cron_enabled(&cron_id, true).await.unwrap();

    let enabled = cron_queue.get_cron_job(&cron_id).await.unwrap();
    assert!(enabled.enabled);
}

/// Test disabling a cron job.
pub async fn test_set_cron_enabled_false<C>(cron_queue: C)
where
    C: CronQueue + Send + Sync,
{
    let payload = CronTestPayload {
        message: "test".to_string(),
    };

    let cron_id = cron_queue
        .schedule_cron::<CronTestJob>("0 0 * * * *", payload, 0)
        .await
        .unwrap();

    cron_queue.set_cron_enabled(&cron_id, false).await.unwrap();

    let cron_job = cron_queue.get_cron_job(&cron_id).await.unwrap();
    assert!(!cron_job.enabled);
}

/// Test deleting a cron job.
pub async fn test_delete_cron_job<C>(cron_queue: C)
where
    C: CronQueue + Send + Sync,
{
    let payload = CronTestPayload {
        message: "delete test".to_string(),
    };

    let cron_id = cron_queue
        .schedule_cron::<CronTestJob>("0 0 * * * *", payload, 0)
        .await
        .unwrap();

    // Verify it exists
    assert!(cron_queue.get_cron_job(&cron_id).await.is_ok());

    // Delete it
    cron_queue.delete_cron_job(&cron_id).await.unwrap();

    // Verify it's gone
    let result = cron_queue.get_cron_job(&cron_id).await;
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        CronError::CronNotFound { .. }
    ));
}

/// Test updating cron job payload.
pub async fn test_update_cron_payload<C>(cron_queue: C)
where
    C: CronQueue + Send + Sync,
{
    let original_payload = CronTestPayload {
        message: "original".to_string(),
    };

    let cron_id = cron_queue
        .schedule_cron::<CronTestJob>("0 0 * * * *", original_payload, 0)
        .await
        .unwrap();

    let new_payload = CronTestPayload {
        message: "updated".to_string(),
    };

    cron_queue
        .update_cron_payload::<CronTestJob>(&cron_id, new_payload.clone())
        .await
        .unwrap();

    let cron_job = cron_queue.get_cron_job(&cron_id).await.unwrap();

    // Deserialize payload to verify update
    let stored_payload: CronTestPayload = serde_json::from_value(cron_job.payload).unwrap();
    assert_eq!(stored_payload, new_payload);
}

/// Test polling for due cron jobs when none are due.
pub async fn test_poll_due_cron_jobs_none_due<C>(cron_queue: C)
where
    C: CronQueue + Send + Sync,
{
    let payload = CronTestPayload {
        message: "future".to_string(),
    };

    // Schedule for far in the future (January 1st at midnight - runs once per year)
    cron_queue
        .schedule_cron::<CronTestJob>("0 0 0 1 1 *", payload, 0)
        .await
        .unwrap();

    let due_jobs = cron_queue.poll_due_cron_jobs(Utc::now(), 10).await.unwrap();
    assert_eq!(due_jobs.len(), 0);
}

/// Test polling for due cron jobs when some are due.
pub async fn test_poll_due_cron_jobs_some_due<C>(cron_queue: C)
where
    C: CronQueue + Send + Sync,
{
    let payload = CronTestPayload {
        message: "due now".to_string(),
    };

    // Schedule a job that's already due (every second)
    let _cron_id = cron_queue
        .schedule_cron::<CronTestJob>("* * * * * *", payload, 0)
        .await
        .unwrap();

    // Wait a moment to ensure it's due
    tokio::time::sleep(tokio::time::Duration::from_millis(1100)).await;

    let due_jobs = cron_queue.poll_due_cron_jobs(Utc::now(), 10).await.unwrap();
    assert!(!due_jobs.is_empty());
}

/// Test that poll_due_cron_jobs respects the limit parameter.
pub async fn test_poll_due_cron_jobs_respects_limit<C>(cron_queue: C)
where
    C: CronQueue + Send + Sync,
{
    // Schedule 5 jobs that are all due
    for i in 0..5 {
        let payload = CronTestPayload {
            message: format!("job{}", i),
        };
        cron_queue
            .schedule_cron::<CronTestJob>("* * * * * *", payload, 0)
            .await
            .unwrap();
    }

    // Wait for them to be due
    tokio::time::sleep(tokio::time::Duration::from_millis(1100)).await;

    // Poll with limit of 3
    let due_jobs = cron_queue.poll_due_cron_jobs(Utc::now(), 3).await.unwrap();
    assert_eq!(due_jobs.len(), 3);
}

/// Test marking a cron job as executed.
pub async fn test_mark_cron_executed<C>(cron_queue: C)
where
    C: CronQueue + Send + Sync,
{
    let payload = CronTestPayload {
        message: "execute test".to_string(),
    };

    let cron_id = cron_queue
        .schedule_cron::<CronTestJob>("0 0 * * * *", payload, 0)
        .await
        .unwrap();

    let original = cron_queue.get_cron_job(&cron_id).await.unwrap();

    // Calculate next execution
    let schedule = CronSchedule::parse("0 0 * * * *").unwrap();
    let next_execution = schedule.next_after(original.next_execution_at).unwrap();

    // Mark as executed
    cron_queue
        .mark_cron_executed(&cron_id, next_execution)
        .await
        .unwrap();

    let updated = cron_queue.get_cron_job(&cron_id).await.unwrap();

    // Verify updates
    assert_eq!(updated.next_execution_at, next_execution);
    assert_eq!(updated.run_count, original.run_count + 1);
    assert!(updated.last_enqueued_at.is_some());
    assert!(updated.last_enqueued_at.unwrap() > original.created_at);
}
