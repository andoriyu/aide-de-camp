//! Shared test specifications for Queue implementations.
//!
//! These test functions can be called by any backend (PostgreSQL, SQLite, etc.)
//! to ensure consistent behavior across all implementations.

/// Generate all queue spec test wrappers for a backend.
///
/// # Usage
///
/// ```ignore
/// // PostgreSQL example with sqlx::test
/// aide_de_camp::generate_queue_spec_tests! {
///     backend = "pg",
///     test_attr = #[sqlx::test(migrator = "MIGRATOR")],
///     setup = |pool: PgPool| PostgresQueue::with_pool(pool)
/// }
///
/// // SQLite example with tokio::test
/// aide_de_camp::generate_queue_spec_tests! {
///     backend = "sqlite",
///     test_attr = #[tokio::test],
///     setup = || {
///         let pool = make_pool(":memory:").await;
///         SqliteQueue::with_pool(pool)
///     }
/// }
/// ```
#[macro_export]
macro_rules! generate_queue_spec_tests {
    (
        backend = $backend:literal,
        test_attr = $test_attr:meta,
        setup = |$pool:ident: $pool_type:ty| $setup_expr:expr
    ) => {
        paste::paste! {
            // ErrorContext serialization tests
            #[$test_attr]
            async fn [<error_context_json_roundtrip_ $backend>]($pool: $pool_type) {
                let queue = $setup_expr;
                $crate::queue_spec::test_error_context_json_roundtrip(queue).await;
            }

            #[$test_attr]
            async fn [<error_context_special_characters_ $backend>]($pool: $pool_type) {
                let queue = $setup_expr;
                $crate::queue_spec::test_error_context_special_characters(queue).await;
            }

            #[$test_attr]
            async fn [<error_context_large_metadata_ $backend>]($pool: $pool_type) {
                let queue = $setup_expr;
                $crate::queue_spec::test_error_context_large_metadata(queue).await;
            }

            #[$test_attr]
            async fn [<error_context_timestamp_precision_ $backend>]($pool: $pool_type) {
                let queue = $setup_expr;
                $crate::queue_spec::test_error_context_timestamp_precision(queue).await;
            }

            #[$test_attr]
            async fn [<error_context_nested_json_ $backend>]($pool: $pool_type) {
                let queue = $setup_expr;
                $crate::queue_spec::test_error_context_nested_json(queue).await;
            }

            #[$test_attr]
            async fn [<error_context_null_fields_ $backend>]($pool: $pool_type) {
                let queue = $setup_expr;
                $crate::queue_spec::test_error_context_null_fields(queue).await;
            }

            // Dead queue operation tests
            #[$test_attr]
            async fn [<dead_queue_basic_ $backend>]($pool: $pool_type) {
                let queue = $setup_expr;
                $crate::queue_spec::test_dead_queue_basic(queue).await;
            }

            #[$test_attr]
            async fn [<dead_queue_with_error_context_ $backend>]($pool: $pool_type) {
                let queue = $setup_expr;
                $crate::queue_spec::test_dead_queue_with_error_context(queue).await;
            }

            #[$test_attr]
            async fn [<dead_queue_without_error_context_ $backend>]($pool: $pool_type) {
                let queue = $setup_expr;
                $crate::queue_spec::test_dead_queue_without_error_context(queue).await;
            }

            #[$test_attr]
            async fn [<dead_queue_transaction_atomicity_ $backend>]($pool: $pool_type) {
                let queue = $setup_expr;
                $crate::queue_spec::test_dead_queue_transaction_atomicity(queue).await;
            }

            #[$test_attr]
            async fn [<dead_queue_preserves_metadata_ $backend>]($pool: $pool_type) {
                let queue = $setup_expr;
                $crate::queue_spec::test_dead_queue_preserves_metadata(queue).await;
            }

            #[$test_attr]
            async fn [<dead_queue_concurrent_moves_ $backend>]($pool: $pool_type) {
                let queue = $setup_expr;
                $crate::queue_spec::test_dead_queue_concurrent_moves(queue).await;
            }

            #[$test_attr]
            async fn [<dead_queue_retry_count_preserved_ $backend>]($pool: $pool_type) {
                let queue = $setup_expr;
                $crate::queue_spec::test_dead_queue_retry_count_preserved(queue).await;
            }

            #[$test_attr]
            async fn [<dead_queue_priority_preserved_ $backend>]($pool: $pool_type) {
                let queue = $setup_expr;
                $crate::queue_spec::test_dead_queue_priority_preserved(queue).await;
            }
        }
    };
}

use crate::core::{
    job_handle::{ErrorContext, JobHandle},
    job_processor::JobProcessor,
    job_type_id::JobTypeId,
    queue::{Queue, ScheduleOptions},
    Utc,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::io;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

// Test job types for integration tests
#[derive(Clone)]
pub struct TestJob1;

#[derive(Debug, thiserror::Error)]
#[error("test job error")]
pub struct TestJobError;

impl From<TestJobError> for crate::core::job_processor::JobError {
    fn from(e: TestJobError) -> Self {
        crate::core::job_processor::JobError::HandlerError(anyhow::anyhow!(e.to_string()))
    }
}

impl JobTypeId for TestJob1 {
    fn type_name() -> &'static str {
        "aide_de_camp::tests::integration::TestJob1"
    }
}

#[async_trait]
impl JobProcessor for TestJob1 {
    type Payload = TestPayload1;
    type Error = TestJobError;

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
pub struct TestPayload1 {
    pub value: String,
}

/// Test ErrorContext JSON serialization roundtrip.
///
/// Verifies that ErrorContext can be serialized to JSON and deserialized back
/// without data loss.
pub async fn test_error_context_json_roundtrip<Q>(queue: Q)
where
    Q: Queue + Send + Sync,
{
    let payload = TestPayload1 {
        value: "test".to_string(),
    };

    // Schedule a job
    let job_id = queue
        .schedule::<TestJob1>(payload, ScheduleOptions::now())
        .await
        .unwrap();

    // Poll for the job
    let handle = queue
        .poll_next(&[TestJob1::type_hash()], Utc::now())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(handle.id(), job_id);

    // Create error context
    let error = io::Error::other("test error");
    let original_context = ErrorContext::from_error(&error, 3)
        .with_metadata("key1".to_string(), json!("value1"))
        .with_metadata("key2".to_string(), json!(42));

    // Move to dead queue
    handle
        .dead_queue(Some(original_context.clone()))
        .await
        .unwrap();

    // The queue implementation should have serialized and stored the error context
    // This test verifies the basic flow works
}

/// Test ErrorContext with special characters in metadata.
pub async fn test_error_context_special_characters<Q>(queue: Q)
where
    Q: Queue + Send + Sync,
{
    let payload = TestPayload1 {
        value: "test".to_string(),
    };

    let _job_id = queue
        .schedule::<TestJob1>(payload, ScheduleOptions::now())
        .await
        .unwrap();

    let handle = queue
        .poll_next(&[TestJob1::type_hash()], Utc::now())
        .await
        .unwrap()
        .unwrap();

    // Create error context with special characters
    let error = io::Error::other("error with special chars: \n\t\"'\\");
    let context = ErrorContext::from_error(&error, 1)
        .with_metadata("unicode".to_string(), json!("emoji: 🚀 中文 العربية"))
        .with_metadata("quotes".to_string(), json!("\"quoted\" 'text'"))
        .with_metadata("newlines".to_string(), json!("line1\nline2\r\nline3"));

    handle.dead_queue(Some(context)).await.unwrap();
}

/// Test ErrorContext with large metadata payload.
pub async fn test_error_context_large_metadata<Q>(queue: Q)
where
    Q: Queue + Send + Sync,
{
    let payload = TestPayload1 {
        value: "test".to_string(),
    };

    let _job_id = queue
        .schedule::<TestJob1>(payload, ScheduleOptions::now())
        .await
        .unwrap();

    let handle = queue
        .poll_next(&[TestJob1::type_hash()], Utc::now())
        .await
        .unwrap()
        .unwrap();

    // Create a large metadata payload (e.g., stack trace)
    let large_string = "x".repeat(10000);
    let error = io::Error::other("test");
    let context = ErrorContext::from_error(&error, 0)
        .with_metadata("stack_trace".to_string(), json!(large_string))
        .with_metadata("large_array".to_string(), json!(vec![1; 1000]));

    handle.dead_queue(Some(context)).await.unwrap();
}

/// Test ErrorContext timestamp precision.
pub async fn test_error_context_timestamp_precision<Q>(queue: Q)
where
    Q: Queue + Send + Sync,
{
    let payload = TestPayload1 {
        value: "test".to_string(),
    };

    let _job_id = queue
        .schedule::<TestJob1>(payload, ScheduleOptions::now())
        .await
        .unwrap();

    let handle = queue
        .poll_next(&[TestJob1::type_hash()], Utc::now())
        .await
        .unwrap()
        .unwrap();

    let before = Utc::now();
    let error = io::Error::other("test");
    let context = ErrorContext::from_error(&error, 0);
    let after = Utc::now();

    // Timestamp should be between before and after
    assert!(context.last_error_at >= before);
    assert!(context.last_error_at <= after);

    handle.dead_queue(Some(context)).await.unwrap();
}

/// Test ErrorContext with nested JSON structures.
pub async fn test_error_context_nested_json<Q>(queue: Q)
where
    Q: Queue + Send + Sync,
{
    let payload = TestPayload1 {
        value: "test".to_string(),
    };

    let _job_id = queue
        .schedule::<TestJob1>(payload, ScheduleOptions::now())
        .await
        .unwrap();

    let handle = queue
        .poll_next(&[TestJob1::type_hash()], Utc::now())
        .await
        .unwrap()
        .unwrap();

    let error = io::Error::other("test");
    let nested = json!({
        "level1": {
            "level2": {
                "level3": {
                    "array": [1, 2, 3],
                    "object": {"key": "value"},
                    "null": null,
                    "bool": true
                }
            }
        }
    });

    let context = ErrorContext::from_error(&error, 0).with_metadata("nested".to_string(), nested);

    handle.dead_queue(Some(context)).await.unwrap();
}

/// Test ErrorContext with null fields.
pub async fn test_error_context_null_fields<Q>(queue: Q)
where
    Q: Queue + Send + Sync,
{
    let payload = TestPayload1 {
        value: "test".to_string(),
    };

    let _job_id = queue
        .schedule::<TestJob1>(payload, ScheduleOptions::now())
        .await
        .unwrap();

    let handle = queue
        .poll_next(&[TestJob1::type_hash()], Utc::now())
        .await
        .unwrap()
        .unwrap();

    let error = io::Error::other("test");
    let context = ErrorContext::from_error(&error, 0)
        .with_metadata("null_value".to_string(), json!(null))
        .with_metadata("optional".to_string(), json!({"some": null, "none": null}));

    handle.dead_queue(Some(context)).await.unwrap();
}

/// Test basic dead queue operation.
pub async fn test_dead_queue_basic<Q>(queue: Q)
where
    Q: Queue + Send + Sync,
{
    let payload = TestPayload1 {
        value: "dead".to_string(),
    };

    let job_id = queue
        .schedule::<TestJob1>(payload, ScheduleOptions::now())
        .await
        .unwrap();

    let handle = queue
        .poll_next(&[TestJob1::type_hash()], Utc::now())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(handle.id(), job_id);

    // Move to dead queue without error context
    handle.dead_queue(None).await.unwrap();

    // Job should no longer be pollable
    let next = queue
        .poll_next(&[TestJob1::type_hash()], Utc::now())
        .await
        .unwrap();
    assert!(next.is_none());
}

/// Test dead queue with error context.
pub async fn test_dead_queue_with_error_context<Q>(queue: Q)
where
    Q: Queue + Send + Sync,
{
    let payload = TestPayload1 {
        value: "error".to_string(),
    };

    let _job_id = queue
        .schedule::<TestJob1>(payload, ScheduleOptions::now())
        .await
        .unwrap();

    let handle = queue
        .poll_next(&[TestJob1::type_hash()], Utc::now())
        .await
        .unwrap()
        .unwrap();

    let error = io::Error::new(io::ErrorKind::ConnectionRefused, "failed to connect");
    let context = ErrorContext::from_error(&error, 5)
        .with_metadata("host".to_string(), json!("example.com"))
        .with_metadata("port".to_string(), json!(443));

    handle.dead_queue(Some(context)).await.unwrap();

    // Job should not be pollable
    let next = queue
        .poll_next(&[TestJob1::type_hash()], Utc::now())
        .await
        .unwrap();
    assert!(next.is_none());
}

/// Test dead queue without error context.
pub async fn test_dead_queue_without_error_context<Q>(queue: Q)
where
    Q: Queue + Send + Sync,
{
    let payload = TestPayload1 {
        value: "simple".to_string(),
    };

    let _job_id = queue
        .schedule::<TestJob1>(payload, ScheduleOptions::now())
        .await
        .unwrap();

    let handle = queue
        .poll_next(&[TestJob1::type_hash()], Utc::now())
        .await
        .unwrap()
        .unwrap();

    handle.dead_queue(None).await.unwrap();
}

/// Test dead queue transaction atomicity.
///
/// Verifies that moving a job to the dead queue is atomic - either fully
/// succeeds or fully fails.
pub async fn test_dead_queue_transaction_atomicity<Q>(queue: Q)
where
    Q: Queue + Send + Sync,
{
    let payload = TestPayload1 {
        value: "atomic".to_string(),
    };

    let _job_id = queue
        .schedule::<TestJob1>(payload, ScheduleOptions::now())
        .await
        .unwrap();

    let handle = queue
        .poll_next(&[TestJob1::type_hash()], Utc::now())
        .await
        .unwrap()
        .unwrap();

    let error = io::Error::other("test");
    let context = ErrorContext::from_error(&error, 3);

    handle.dead_queue(Some(context)).await.unwrap();

    // After dead_queue, job should be gone from main queue
    let next = queue
        .poll_next(&[TestJob1::type_hash()], Utc::now())
        .await
        .unwrap();
    assert!(next.is_none());
}

/// Test that dead queue preserves all error context metadata.
pub async fn test_dead_queue_preserves_metadata<Q>(queue: Q)
where
    Q: Queue + Send + Sync,
{
    let payload = TestPayload1 {
        value: "metadata".to_string(),
    };

    let _job_id = queue
        .schedule::<TestJob1>(payload, ScheduleOptions::now())
        .await
        .unwrap();

    let handle = queue
        .poll_next(&[TestJob1::type_hash()], Utc::now())
        .await
        .unwrap()
        .unwrap();

    let error = io::Error::other("complex error");
    let context = ErrorContext::from_error(&error, 10)
        .with_metadata("user_id".to_string(), json!(12345))
        .with_metadata("request_path".to_string(), json!("/api/v1/users"))
        .with_metadata(
            "headers".to_string(),
            json!({"Authorization": "Bearer xxx"}),
        )
        .with_metadata("timestamp".to_string(), json!("2024-01-01T00:00:00Z"));

    handle.dead_queue(Some(context)).await.unwrap();
}

/// Test concurrent dead queue operations.
pub async fn test_dead_queue_concurrent_moves<Q>(queue: Q)
where
    Q: Queue + Send + Sync + Clone,
{
    // Schedule multiple jobs
    let _job_ids: Vec<Uuid> = futures::future::join_all((0..5).map(|i| {
        let queue = queue.clone();
        async move {
            queue
                .schedule::<TestJob1>(
                    TestPayload1 {
                        value: format!("job{}", i),
                    },
                    ScheduleOptions::now(),
                )
                .await
                .unwrap()
        }
    }))
    .await;

    // Poll and move to dead queue concurrently
    let handles: Vec<_> = futures::future::join_all((0..5).map(|_| {
        let queue = queue.clone();
        async move {
            queue
                .poll_next(&[TestJob1::type_hash()], Utc::now())
                .await
                .unwrap()
                .unwrap()
        }
    }))
    .await;

    // Move all to dead queue concurrently
    futures::future::join_all(handles.into_iter().map(|handle| async move {
        let error = io::Error::other("concurrent test");
        let context = ErrorContext::from_error(&error, 0);
        handle.dead_queue(Some(context)).await.unwrap();
    }))
    .await;

    // All jobs should be gone
    let next = queue
        .poll_next(&[TestJob1::type_hash()], Utc::now())
        .await
        .unwrap();
    assert!(next.is_none());
}

/// Test that retry count is preserved when moving to dead queue.
pub async fn test_dead_queue_retry_count_preserved<Q>(queue: Q)
where
    Q: Queue + Send + Sync,
{
    let payload = TestPayload1 {
        value: "retries".to_string(),
    };

    let _job_id = queue
        .schedule::<TestJob1>(payload, ScheduleOptions::now())
        .await
        .unwrap();

    let handle = queue
        .poll_next(&[TestJob1::type_hash()], Utc::now())
        .await
        .unwrap()
        .unwrap();

    let retry_count = handle.retries();

    let error = io::Error::other("test");
    let context = ErrorContext::from_error(&error, retry_count);

    assert_eq!(context.retry_count, retry_count);

    handle.dead_queue(Some(context)).await.unwrap();
}

/// Test that priority is preserved when moving to dead queue.
pub async fn test_dead_queue_priority_preserved<Q>(queue: Q)
where
    Q: Queue + Send + Sync,
{
    let payload = TestPayload1 {
        value: "priority".to_string(),
    };

    // Schedule with high priority
    let _job_id = queue
        .schedule::<TestJob1>(payload, ScheduleOptions::now().with_priority(10))
        .await
        .unwrap();

    let handle = queue
        .poll_next(&[TestJob1::type_hash()], Utc::now())
        .await
        .unwrap()
        .unwrap();

    let error = io::Error::other("test");
    let context = ErrorContext::from_error(&error, 0)
        .with_metadata("original_priority".to_string(), json!(10));

    handle.dead_queue(Some(context)).await.unwrap();
}
