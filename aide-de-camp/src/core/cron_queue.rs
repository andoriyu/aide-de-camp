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
    /// Unique identifier for this cron job
    pub cron_id: String,

    /// Queue name (e.g., "default")
    pub queue: String,

    /// Job type identifier
    pub job_type: String,

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
