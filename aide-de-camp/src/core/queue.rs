use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::core::job_handle::JobHandle;
use crate::core::job_processor::JobProcessor;
use crate::core::{DateTime, Duration};

/// Scheduling options for enqueuing jobs.
///
/// Provides a builder pattern for configuring when and how a job should be scheduled.
///
/// # Examples
///
/// ```rust
/// use aide_de_camp::core::queue::ScheduleOptions;
/// use chrono::Duration;
///
/// // Schedule immediately
/// let opts = ScheduleOptions::now();
///
/// // Schedule in 5 minutes with high priority
/// let opts = ScheduleOptions::new()
///     .in_duration(Duration::minutes(5))
///     .with_priority(10);
/// ```
#[derive(Debug, Clone)]
pub struct ScheduleOptions {
    pub(crate) scheduled_at: DateTime,
    pub(crate) priority: i8,
}

impl ScheduleOptions {
    /// Create new scheduling options with default values (immediate, priority 0).
    pub fn new() -> Self {
        Self::default()
    }

    /// Create options for immediate scheduling.
    pub fn now() -> Self {
        Self::default()
    }

    /// Set the scheduled time to a specific datetime.
    pub fn at(mut self, scheduled_at: DateTime) -> Self {
        self.scheduled_at = scheduled_at;
        self
    }

    /// Schedule the job to run after a duration from now.
    pub fn in_duration(mut self, duration: Duration) -> Self {
        self.scheduled_at = Utc::now() + duration;
        self
    }

    /// Set the job priority (higher values run first).
    ///
    /// Priority is clamped to i8 range (-128 to 127).
    pub fn with_priority(mut self, priority: i8) -> Self {
        self.priority = priority;
        self
    }

    /// Get the scheduled time.
    pub fn scheduled_at(&self) -> DateTime {
        self.scheduled_at
    }

    /// Get the priority.
    pub fn priority(&self) -> i8 {
        self.priority
    }
}

impl Default for ScheduleOptions {
    fn default() -> Self {
        Self {
            scheduled_at: Utc::now(),
            priority: 0,
        }
    }
}

/// An interface to queue implementation. Responsible for pushing jobs into the queue and pulling
/// jobs out of the queue.
///
/// ### Priority
///
/// When a job is enqueued one can specify priority. Jobs with higher priority will get polled first even if submitted after lower priority jobs.
#[async_trait]
pub trait Queue: Send + Sync {
    type JobHandle: JobHandle;

    /// Schedule a job with the given options.
    ///
    /// This is the primary method for enqueueing jobs. Use `ScheduleOptions` builder
    /// to configure when and with what priority the job should run.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// // Schedule immediately
    /// queue.schedule::<MyJob>(payload, ScheduleOptions::now()).await?;
    ///
    /// // Schedule in 5 minutes with high priority
    /// queue.schedule::<MyJob>(
    ///     payload,
    ///     ScheduleOptions::new()
    ///         .in_duration(Duration::minutes(5))
    ///         .with_priority(10)
    /// ).await?;
    /// ```
    async fn schedule<J>(
        &self,
        payload: J::Payload,
        options: ScheduleOptions,
    ) -> Result<Uuid, QueueError>
    where
        J: JobProcessor + 'static,
        J::Payload: Serialize + for<'de> Deserialize<'de>;

    /// Poll for the next job matching the given type hashes.
    ///
    /// Returns `None` if no jobs are ready. Does not wait.
    ///
    /// # Arguments
    ///
    /// * `type_hashes` - List of job type hashes to poll for
    /// * `now` - Current time for checking scheduled_at
    async fn poll_next(
        &self,
        type_hashes: &[u64],
        now: DateTime,
    ) -> Result<Option<Self::JobHandle>, QueueError>;

    /// Await the next job. Polls the queue at the specified interval until a job is available.
    ///
    /// # Arguments
    ///
    /// * `type_hashes` - List of job type hashes to wait for
    /// * `interval` - How often to poll
    async fn next(
        &self,
        type_hashes: &[u64],
        interval: Duration,
    ) -> Result<Self::JobHandle, QueueError> {
        let duration = interval
            .to_std()
            .map_err(|_| QueueError::InvalidSchedule("Invalid poll interval".to_string()))?;
        let mut interval = tokio::time::interval(duration);
        loop {
            interval.tick().await;
            let job = self.poll_next(type_hashes, Utc::now()).await?;
            if let Some(job) = job {
                break Ok(job);
            }
        }
    }

    /// Cancel a job that has been scheduled.
    ///
    /// Returns `true` if the job was cancelled, `false` if it wasn't found or already started.
    async fn cancel_job(&self, job_id: Uuid) -> Result<bool, QueueError>;

    /// Cancel a job and return its payload.
    ///
    /// If the job is not found or has already started, returns `JobNotFound` error.
    /// If deserialization fails, the job is not cancelled.
    async fn unschedule_job<J>(&self, job_id: Uuid) -> Result<J::Payload, QueueError>
    where
        J: JobProcessor + 'static,
        J::Payload: for<'de> Deserialize<'de>;
}

/// Internal trait for type-erased scheduling (used by cron scheduler).
///
/// This is intentionally not public - cron scheduler uses it internally but
/// users should use the type-safe `schedule()` method.
#[doc(hidden)]
#[async_trait]
pub trait InternalQueue: Queue {
    async fn schedule_untyped(
        &self,
        type_hash: u64,
        type_name: &str,
        payload: serde_json::Value,
        options: ScheduleOptions,
    ) -> Result<Uuid, QueueError>;
}

/// Errors related to queue operations.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum QueueError {
    /// Failed to serialize payload.
    #[error("Failed to serialize payload for job type '{job_type}': {error}")]
    SerializeError {
        job_type: String,
        #[source]
        error: serde_json::Error,
    },

    /// Failed to deserialize payload with context for debugging.
    #[error("Failed to deserialize payload for job {job_id} (type: '{job_type}'): {error}\nPayload sample (first {payload_size} bytes): {payload_sample}")]
    DeserializeError {
        job_id: Uuid,
        job_type: String,
        #[source]
        error: serde_json::Error,
        payload_sample: String,
        payload_size: usize,
    },

    /// Invalid schedule options.
    #[error("Invalid schedule options: {0}")]
    InvalidSchedule(String),

    /// Job not found.
    #[error("Job {job_id} not found{}", expected_type.as_ref().map(|t| format!(" (expected type: '{}')", t)).unwrap_or_default())]
    JobNotFound {
        job_id: Uuid,
        expected_type: Option<String>,
    },

    /// Database or other backend error.
    #[error("Database error: {0}")]
    DatabaseError(#[from] anyhow::Error),
}

impl QueueError {
    /// Create a deserialization error with payload context.
    ///
    /// Includes a sample of the payload (first 500 chars) for debugging.
    pub fn deserialize_error(
        job_id: Uuid,
        job_type: String,
        payload: &[u8],
        error: serde_json::Error,
    ) -> Self {
        let payload_size = payload.len();
        let payload_sample = String::from_utf8_lossy(payload).chars().take(500).collect();

        Self::DeserializeError {
            job_id,
            job_type,
            error,
            payload_sample,
            payload_size,
        }
    }

    /// Create a serialize error with type context.
    pub fn serialize_error(job_type: String, error: serde_json::Error) -> Self {
        Self::SerializeError { job_type, error }
    }

    /// Create a job not found error.
    pub fn job_not_found(job_id: Uuid) -> Self {
        Self::JobNotFound {
            job_id,
            expected_type: None,
        }
    }

    /// Create a job not found error with expected type.
    pub fn job_not_found_with_type(job_id: Uuid, expected_type: String) -> Self {
        Self::JobNotFound {
            job_id,
            expected_type: Some(expected_type),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schedule_options_new_defaults() {
        let before = Utc::now();
        let opts = ScheduleOptions::new();
        let after = Utc::now();

        assert_eq!(opts.priority(), 0);
        assert!(opts.scheduled_at() >= before);
        assert!(opts.scheduled_at() <= after);
    }

    #[test]
    fn test_schedule_options_now_equals_new() {
        let new_opts = ScheduleOptions::new();
        let now_opts = ScheduleOptions::now();

        assert_eq!(new_opts.priority(), now_opts.priority());
        // Both should be approximately now (within 1 second)
        let diff = (new_opts.scheduled_at() - now_opts.scheduled_at())
            .num_milliseconds()
            .abs();
        assert!(diff < 1000);
    }

    #[test]
    fn test_schedule_options_at_sets_time() {
        use chrono::TimeZone;

        let specific_time = Utc.with_ymd_and_hms(2024, 6, 15, 14, 30, 0).unwrap();
        let opts = ScheduleOptions::new().at(specific_time);

        assert_eq!(opts.scheduled_at(), specific_time);
        assert_eq!(opts.priority(), 0);
    }

    #[test]
    fn test_schedule_options_in_duration_future() {
        let before = Utc::now();
        let opts = ScheduleOptions::new().in_duration(Duration::minutes(5));
        let expected = before + Duration::minutes(5);

        // Should be approximately 5 minutes in the future
        let diff = (opts.scheduled_at() - expected).num_seconds().abs();
        assert!(diff < 2);
        assert_eq!(opts.priority(), 0);
    }

    #[test]
    fn test_schedule_options_in_duration_negative() {
        let before = Utc::now();
        let opts = ScheduleOptions::new().in_duration(Duration::hours(-1));
        let expected = before - Duration::hours(1);

        // Should be approximately 1 hour in the past
        let diff = (opts.scheduled_at() - expected).num_seconds().abs();
        assert!(diff < 2);
        assert_eq!(opts.priority(), 0);
    }

    #[test]
    fn test_schedule_options_with_priority_positive() {
        let opts = ScheduleOptions::new().with_priority(10);

        assert_eq!(opts.priority(), 10);
    }

    #[test]
    fn test_schedule_options_with_priority_negative() {
        let opts = ScheduleOptions::new().with_priority(-10);

        assert_eq!(opts.priority(), -10);
    }

    #[test]
    fn test_schedule_options_with_priority_max() {
        let opts = ScheduleOptions::new().with_priority(127);

        assert_eq!(opts.priority(), 127);
    }

    #[test]
    fn test_schedule_options_with_priority_min() {
        let opts = ScheduleOptions::new().with_priority(-128);

        assert_eq!(opts.priority(), -128);
    }

    #[test]
    fn test_schedule_options_chained() {
        use chrono::TimeZone;

        let specific_time = Utc.with_ymd_and_hms(2024, 12, 25, 9, 0, 0).unwrap();
        let opts = ScheduleOptions::new().at(specific_time).with_priority(15);

        assert_eq!(opts.scheduled_at(), specific_time);
        assert_eq!(opts.priority(), 15);

        // Test chaining in different order
        let opts2 = ScheduleOptions::new()
            .with_priority(20)
            .in_duration(Duration::hours(2));

        assert_eq!(opts2.priority(), 20);
        let expected = Utc::now() + Duration::hours(2);
        let diff = (opts2.scheduled_at() - expected).num_seconds().abs();
        assert!(diff < 2);
    }

    #[test]
    fn test_schedule_options_scheduled_at_getter() {
        use chrono::TimeZone;

        let time = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let opts = ScheduleOptions::new().at(time);

        assert_eq!(opts.scheduled_at(), time);
    }

    #[test]
    fn test_schedule_options_priority_getter() {
        let opts = ScheduleOptions::new().with_priority(42);

        assert_eq!(opts.priority(), 42);
    }

    #[test]
    fn test_queue_error_deserialize_error_formatting() {
        let job_id = Uuid::now_v7();
        let payload = b"{\"invalid\": json}";
        let serde_error = serde_json::from_slice::<serde_json::Value>(payload).unwrap_err();

        let error =
            QueueError::deserialize_error(job_id, "TestJob".to_string(), payload, serde_error);

        let error_msg = error.to_string();
        assert!(error_msg.contains(&job_id.to_string()));
        assert!(error_msg.contains("TestJob"));
        assert!(error_msg.contains("invalid"));
    }

    #[test]
    fn test_queue_error_deserialize_error_truncates() {
        let job_id = Uuid::now_v7();
        // Create a payload larger than 500 chars
        let large_payload = "x".repeat(1000);
        let serde_error =
            serde_json::from_slice::<serde_json::Value>(large_payload.as_bytes()).unwrap_err();

        let error = QueueError::deserialize_error(
            job_id,
            "TestJob".to_string(),
            large_payload.as_bytes(),
            serde_error,
        );

        let error_msg = error.to_string();
        // Should contain "1000 bytes" reference
        assert!(error_msg.contains("1000"));
        // But the sample should be truncated to 500 chars
        // The error message includes the sample plus some other text, so count should be 500 or slightly more
        let sample_count = error_msg.matches('x').count();
        assert!(
            (500..=510).contains(&sample_count),
            "Expected ~500 'x' chars, got {}",
            sample_count
        );
    }

    #[test]
    fn test_queue_error_deserialize_error_utf8_invalid() {
        let job_id = Uuid::now_v7();
        // Invalid UTF-8 sequence
        let invalid_utf8: &[u8] = &[0xFF, 0xFE, 0xFD];
        let serde_error = serde_json::from_slice::<serde_json::Value>(invalid_utf8).unwrap_err();

        let error =
            QueueError::deserialize_error(job_id, "TestJob".to_string(), invalid_utf8, serde_error);

        // Should not panic, should handle invalid UTF-8 gracefully
        let error_msg = error.to_string();
        assert!(error_msg.contains(&job_id.to_string()));
        assert!(error_msg.contains("TestJob"));
    }

    #[test]
    fn test_queue_error_serialize_error_formatting() {
        // Create a serde_json::Error by attempting invalid deserialization
        // (easier than forcing a serialization error)
        let serde_error = serde_json::from_str::<serde_json::Value>("invalid json").unwrap_err();

        let error = QueueError::serialize_error("MyJobType".to_string(), serde_error);

        let error_msg = error.to_string();
        assert!(error_msg.contains("MyJobType"));
        assert!(error_msg.contains("serialize"));
    }

    #[test]
    fn test_queue_error_job_not_found_no_type() {
        let job_id = Uuid::now_v7();
        let error = QueueError::job_not_found(job_id);

        let error_msg = error.to_string();
        assert!(error_msg.contains(&job_id.to_string()));
        assert!(error_msg.contains("not found"));
        assert!(!error_msg.contains("expected type"));
    }

    #[test]
    fn test_queue_error_job_not_found_with_type() {
        let job_id = Uuid::now_v7();
        let error = QueueError::job_not_found_with_type(job_id, "MyJob".to_string());

        let error_msg = error.to_string();
        assert!(error_msg.contains(&job_id.to_string()));
        assert!(error_msg.contains("not found"));
        assert!(error_msg.contains("expected type"));
        assert!(error_msg.contains("MyJob"));
    }

    #[test]
    fn test_queue_error_invalid_schedule() {
        let error = QueueError::InvalidSchedule("Invalid duration".to_string());

        let error_msg = error.to_string();
        assert!(error_msg.contains("Invalid schedule"));
        assert!(error_msg.contains("Invalid duration"));
    }

    #[test]
    fn test_queue_error_database_error() {
        let anyhow_error = anyhow::anyhow!("Database connection failed");
        let error = QueueError::DatabaseError(anyhow_error);

        let error_msg = error.to_string();
        assert!(error_msg.contains("Database error"));
        assert!(error_msg.contains("Database connection failed"));
    }

    #[test]
    fn test_queue_error_display_messages() {
        let job_id = Uuid::now_v7();

        // Test all error variant display messages
        let errors = vec![
            QueueError::InvalidSchedule("test".to_string()),
            QueueError::job_not_found(job_id),
            QueueError::job_not_found_with_type(job_id, "Type".to_string()),
            QueueError::DatabaseError(anyhow::anyhow!("test")),
        ];

        for error in errors {
            let msg = error.to_string();
            // Each error should have a non-empty message
            assert!(!msg.is_empty());
        }
    }

    #[test]
    fn test_queue_error_source_chain() {
        use std::error::Error;

        // DatabaseError preserves source
        let anyhow_error = anyhow::anyhow!("Root cause");
        let error = QueueError::DatabaseError(anyhow_error);
        assert!(error.source().is_some());

        // SerializeError preserves source
        let serde_error = serde_json::from_str::<serde_json::Value>("invalid").unwrap_err();
        let error = QueueError::serialize_error("Test".to_string(), serde_error);
        assert!(error.source().is_some());

        // DeserializeError preserves source
        let job_id = Uuid::now_v7();
        let payload = b"invalid";
        let serde_error = serde_json::from_slice::<serde_json::Value>(payload).unwrap_err();
        let error = QueueError::deserialize_error(job_id, "Test".to_string(), payload, serde_error);
        assert!(error.source().is_some());
    }
}
