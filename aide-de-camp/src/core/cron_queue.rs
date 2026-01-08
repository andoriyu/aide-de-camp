//! Cron job queue operations.
//!
//! This module provides traits and types for scheduling and managing cron jobs
//! (recurring jobs with cron expressions).

pub use crate::core::cron::CronError;
use crate::core::job_processor::JobProcessor;
use crate::core::DateTime;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// A cron job stored in the database.
///
/// Represents a recurring job with a cron schedule that will be automatically
/// enqueued when its execution time arrives.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CronJob {
    /// Unique identifier for this cron job (UUIDv7)
    pub cron_id: String,

    /// Queue name (e.g., "default")
    pub queue: String,

    /// Job type hash for routing
    pub type_hash: u64,

    /// Job type name for debugging
    pub type_name: String,

    /// Job payload as JSON
    pub payload: serde_json::Value,

    /// Cron expression (e.g., "0 0 * * *")
    pub cron_expression: String,

    /// Job priority (higher values run first)
    pub priority: i8,

    /// When the cron job was created
    pub created_at: DateTime,

    /// Last time a job was enqueued from this cron
    pub last_enqueued_at: Option<DateTime>,

    /// Next scheduled execution time
    pub next_execution_at: DateTime,

    /// Whether this cron job is enabled
    pub enabled: bool,

    /// Maximum number of times to run (None = unlimited)
    pub max_runs: Option<i32>,

    /// Number of times this cron has been executed
    pub run_count: i32,
}

/// Queue extension for cron job management.
///
/// Provides operations for scheduling recurring jobs with cron expressions.
/// Backend implementations (SQLite, PostgreSQL) implement this trait to provide
/// cron functionality on top of the regular job queue.
///
/// # Example
///
/// ```rust,no_run
/// use aide_de_camp::core::cron_queue::CronQueue;
/// use aide_de_camp::core::job_processor::JobProcessor;
/// # use aide_de_camp::core::queue::Queue;
/// # use serde::{Serialize, Deserialize};
///
/// # #[derive(Serialize, Deserialize)]
/// # struct MyJobPayload;
/// # struct MyJob;
/// # impl JobProcessor for MyJob {
/// #     type Payload = MyJobPayload;
/// #     fn name() -> &'static str { "MyJob" }
/// #     async fn process(&self, _: Self::Payload) -> Result<(), anyhow::Error> { Ok(()) }
/// # }
/// #
/// # async fn example(queue: impl CronQueue + Queue) -> Result<(), Box<dyn std::error::Error>> {
/// // Schedule a daily job at midnight
/// let cron_id = queue.schedule_cron::<MyJob>(
///     "0 0 * * *",
///     MyJobPayload,
///     0, // priority
/// ).await?;
///
/// // Disable the cron job temporarily
/// queue.set_cron_enabled(&cron_id, false).await?;
///
/// // Re-enable it later
/// queue.set_cron_enabled(&cron_id, true).await?;
/// # Ok(())
/// # }
/// ```
#[async_trait]
pub trait CronQueue: Send + Sync {
    /// Schedule a new cron job.
    ///
    /// # Arguments
    ///
    /// * `cron_expression` - Standard 5-field cron expression (min hour day month dow)
    /// * `payload` - Job payload to serialize and store
    /// * `priority` - Job priority (higher values run first)
    ///
    /// # Returns
    ///
    /// The unique cron job ID
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use aide_de_camp::core::cron_queue::CronQueue;
    /// # use aide_de_camp::core::job_processor::JobProcessor;
    /// # use serde::{Serialize, Deserialize};
    /// # #[derive(Serialize, Deserialize)]
    /// # struct ReportPayload { report_type: String }
    /// # struct DailyReportJob;
    /// # impl JobProcessor for DailyReportJob {
    /// #     type Payload = ReportPayload;
    /// #     fn name() -> &'static str { "DailyReportJob" }
    /// #     async fn process(&self, _: Self::Payload) -> Result<(), anyhow::Error> { Ok(()) }
    /// # }
    /// # async fn example(queue: impl CronQueue) -> Result<(), Box<dyn std::error::Error>> {
    /// // Run every day at 9 AM
    /// queue.schedule_cron::<DailyReportJob>(
    ///     "0 9 * * *",
    ///     ReportPayload { report_type: "sales".to_string() },
    ///     0,
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn schedule_cron<J>(
        &self,
        cron_expression: &str,
        payload: J::Payload,
        priority: i8,
    ) -> Result<String, CronError>
    where
        J: JobProcessor + 'static,
        J::Payload: Serialize;

    /// List all cron jobs, optionally filtered by job type.
    ///
    /// # Arguments
    ///
    /// * `job_type` - Optional job type filter. If None, returns all cron jobs.
    ///
    /// # Returns
    ///
    /// Vector of all matching cron jobs
    async fn list_cron_jobs(&self, job_type: Option<&str>) -> Result<Vec<CronJob>, CronError>;

    /// Get a specific cron job by ID.
    ///
    /// # Arguments
    ///
    /// * `cron_id` - The cron job ID
    ///
    /// # Returns
    ///
    /// The cron job, or an error if not found
    async fn get_cron_job(&self, cron_id: &str) -> Result<CronJob, CronError>;

    /// Update the cron schedule for an existing job.
    ///
    /// This recalculates the next execution time based on the new expression.
    ///
    /// # Arguments
    ///
    /// * `cron_id` - The cron job ID
    /// * `cron_expression` - New cron expression
    async fn update_cron_schedule(
        &self,
        cron_id: &str,
        cron_expression: &str,
    ) -> Result<(), CronError>;

    /// Enable or disable a cron job.
    ///
    /// Disabled cron jobs will not be executed by the scheduler.
    ///
    /// # Arguments
    ///
    /// * `cron_id` - The cron job ID
    /// * `enabled` - Whether to enable (true) or disable (false) the job
    async fn set_cron_enabled(&self, cron_id: &str, enabled: bool) -> Result<(), CronError>;

    /// Delete a cron job permanently.
    ///
    /// # Arguments
    ///
    /// * `cron_id` - The cron job ID to delete
    async fn delete_cron_job(&self, cron_id: &str) -> Result<(), CronError>;

    /// Update the payload of an existing cron job.
    ///
    /// # Arguments
    ///
    /// * `cron_id` - The cron job ID
    /// * `payload` - New payload to serialize and store
    async fn update_cron_payload<J>(
        &self,
        cron_id: &str,
        payload: J::Payload,
    ) -> Result<(), CronError>
    where
        J: JobProcessor + 'static,
        J::Payload: Serialize;

    // Internal methods for CronScheduler

    /// Poll for cron jobs that are due for execution.
    ///
    /// This is used internally by the CronScheduler to find jobs that need
    /// to be enqueued. Users should not typically call this directly.
    ///
    /// # Arguments
    ///
    /// * `now` - Current time to check against
    /// * `limit` - Maximum number of jobs to return
    ///
    /// # Returns
    ///
    /// Vector of cron jobs that are due
    async fn poll_due_cron_jobs(
        &self,
        now: DateTime,
        limit: usize,
    ) -> Result<Vec<CronJob>, CronError>;

    /// Mark a cron job as executed and update next execution time.
    ///
    /// This is used internally by the CronScheduler after successfully enqueuing
    /// a job. Users should not typically call this directly.
    ///
    /// # Arguments
    ///
    /// * `cron_id` - The cron job ID
    /// * `next_execution_at` - The next scheduled execution time
    async fn mark_cron_executed(
        &self,
        cron_id: &str,
        next_execution_at: DateTime,
    ) -> Result<(), CronError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::Utc;
    use serde_json::json;
    use uuid::Uuid;

    #[test]
    fn test_cron_error_parse_error_formatting() {
        let error = CronError::ParseError {
            expression: "invalid cron".to_string(),
            error: "Expected 6 fields".to_string(),
        };

        let error_msg = error.to_string();
        assert!(error_msg.contains("invalid cron"));
        assert!(error_msg.contains("Expected 6 fields"));
        assert!(error_msg.contains("Invalid cron expression"));
    }

    #[test]
    fn test_cron_error_no_upcoming_execution() {
        let expression = "0 0 31 2 *"; // Feb 31st (impossible)
        let error = CronError::NoUpcomingExecution(expression.to_string());

        let error_msg = error.to_string();
        assert!(error_msg.contains("0 0 31 2 *"));
        assert!(error_msg.contains("No upcoming execution"));
    }

    #[test]
    fn test_cron_error_cron_not_found() {
        let cron_id = Uuid::now_v7();
        let error = CronError::CronNotFound {
            cron_id,
            expected_type: None,
        };

        let error_msg = error.to_string();
        assert!(error_msg.contains(&cron_id.to_string()));
        assert!(error_msg.contains("not found"));
        assert!(!error_msg.contains("expected type"));

        // With expected type
        let error_with_type = CronError::CronNotFound {
            cron_id,
            expected_type: Some("MyJob".to_string()),
        };

        let error_msg = error_with_type.to_string();
        assert!(error_msg.contains("MyJob"));
        assert!(error_msg.contains("expected type"));
    }

    #[test]
    fn test_cron_error_type_mismatch() {
        let cron_id = Uuid::now_v7();
        let error = CronError::TypeMismatch {
            cron_id,
            actual_type: "JobA".to_string(),
            expected_type: "JobB".to_string(),
        };

        let error_msg = error.to_string();
        assert!(error_msg.contains(&cron_id.to_string()));
        assert!(error_msg.contains("JobA"));
        assert!(error_msg.contains("JobB"));
        assert!(error_msg.contains("has type"));
        assert!(error_msg.contains("but expected"));
    }

    #[test]
    fn test_cron_error_database_error() {
        use std::error::Error;

        let anyhow_error = anyhow::anyhow!("Connection failed");
        let error = CronError::DatabaseError(anyhow_error);

        let error_msg = error.to_string();
        assert!(error_msg.contains("Database error"));
        assert!(error_msg.contains("Connection failed"));

        // Check source chain is preserved
        assert!(error.source().is_some());
    }

    #[test]
    fn test_cron_job_serialization() {
        let now = Utc::now();
        let cron_job = CronJob {
            cron_id: "test-cron-123".to_string(),
            queue: "default".to_string(),
            type_hash: 12345,
            type_name: "TestJob".to_string(),
            payload: json!({"key": "value"}),
            cron_expression: "0 0 * * *".to_string(),
            priority: 5,
            created_at: now,
            last_enqueued_at: Some(now),
            next_execution_at: now,
            enabled: true,
            max_runs: Some(10),
            run_count: 3,
        };

        // Serialize to JSON
        let json = serde_json::to_string(&cron_job).unwrap();

        // Deserialize back
        let deserialized: CronJob = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.cron_id, cron_job.cron_id);
        assert_eq!(deserialized.queue, cron_job.queue);
        assert_eq!(deserialized.type_hash, cron_job.type_hash);
        assert_eq!(deserialized.type_name, cron_job.type_name);
        assert_eq!(deserialized.payload, cron_job.payload);
        assert_eq!(deserialized.cron_expression, cron_job.cron_expression);
        assert_eq!(deserialized.priority, cron_job.priority);
        assert_eq!(deserialized.created_at, cron_job.created_at);
        assert_eq!(deserialized.last_enqueued_at, cron_job.last_enqueued_at);
        assert_eq!(deserialized.next_execution_at, cron_job.next_execution_at);
        assert_eq!(deserialized.enabled, cron_job.enabled);
        assert_eq!(deserialized.max_runs, cron_job.max_runs);
        assert_eq!(deserialized.run_count, cron_job.run_count);
    }

    #[test]
    fn test_cron_job_json_payload() {
        let now = Utc::now();

        // Test with complex nested JSON payload
        let complex_payload = json!({
            "user_id": 42,
            "nested": {
                "array": [1, 2, 3],
                "string": "test"
            },
            "null_field": null
        });

        let cron_job = CronJob {
            cron_id: "test".to_string(),
            queue: "default".to_string(),
            type_hash: 1,
            type_name: "Test".to_string(),
            payload: complex_payload.clone(),
            cron_expression: "* * * * * *".to_string(),
            priority: 0,
            created_at: now,
            last_enqueued_at: None,
            next_execution_at: now,
            enabled: true,
            max_runs: None,
            run_count: 0,
        };

        assert_eq!(cron_job.payload, complex_payload);
        assert_eq!(cron_job.payload["user_id"], 42);
        assert_eq!(cron_job.payload["nested"]["array"][0], 1);
    }

    #[test]
    fn test_cron_job_enabled_flag() {
        let now = Utc::now();

        // Test enabled = true
        let enabled_job = CronJob {
            cron_id: "enabled".to_string(),
            queue: "default".to_string(),
            type_hash: 1,
            type_name: "Test".to_string(),
            payload: json!({}),
            cron_expression: "* * * * * *".to_string(),
            priority: 0,
            created_at: now,
            last_enqueued_at: None,
            next_execution_at: now,
            enabled: true,
            max_runs: None,
            run_count: 0,
        };

        assert!(enabled_job.enabled);

        // Test enabled = false
        let disabled_job = CronJob {
            enabled: false,
            ..enabled_job.clone()
        };

        assert!(!disabled_job.enabled);
    }

    #[test]
    fn test_cron_job_max_runs() {
        let now = Utc::now();

        // Test with max_runs = Some(5)
        let limited_job = CronJob {
            cron_id: "limited".to_string(),
            queue: "default".to_string(),
            type_hash: 1,
            type_name: "Test".to_string(),
            payload: json!({}),
            cron_expression: "* * * * * *".to_string(),
            priority: 0,
            created_at: now,
            last_enqueued_at: None,
            next_execution_at: now,
            enabled: true,
            max_runs: Some(5),
            run_count: 0,
        };

        assert_eq!(limited_job.max_runs, Some(5));

        // Test with max_runs = None (unlimited)
        let unlimited_job = CronJob {
            max_runs: None,
            ..limited_job.clone()
        };

        assert_eq!(unlimited_job.max_runs, None);
    }

    #[test]
    fn test_cron_job_run_count() {
        let now = Utc::now();

        let job = CronJob {
            cron_id: "counter".to_string(),
            queue: "default".to_string(),
            type_hash: 1,
            type_name: "Test".to_string(),
            payload: json!({}),
            cron_expression: "* * * * * *".to_string(),
            priority: 0,
            created_at: now,
            last_enqueued_at: None,
            next_execution_at: now,
            enabled: true,
            max_runs: Some(10),
            run_count: 0,
        };

        assert_eq!(job.run_count, 0);

        // Simulate incrementing run count
        let job_after_one_run = CronJob {
            run_count: 1,
            ..job.clone()
        };
        assert_eq!(job_after_one_run.run_count, 1);

        let job_after_five_runs = CronJob {
            run_count: 5,
            ..job.clone()
        };
        assert_eq!(job_after_five_runs.run_count, 5);

        // Test reaching max_runs
        let job_at_limit = CronJob {
            run_count: 10,
            max_runs: Some(10),
            ..job.clone()
        };
        assert_eq!(job_at_limit.run_count, job_at_limit.max_runs.unwrap());
    }
}
