//! Background service for scheduling cron jobs.
//!
//! The CronScheduler polls the cron jobs table and enqueues jobs when their
//! execution time arrives. It supports leader election for high-availability
//! deployments where only one instance should process cron jobs.

use crate::core::cron::CronSchedule;
use crate::core::cron_queue::{CronJob, CronQueue};
use crate::core::queue::{InternalQueue, ScheduleOptions};
use crate::core::Utc;
use anyhow::Context;
use std::{future::Future, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument};

/// Options for configuring the cron scheduler.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct CronSchedulerOptions {
    /// How often to poll for due cron jobs (default: 10 seconds)
    pub poll_interval: Duration,

    /// Maximum number of cron jobs to process per poll (default: 100)
    pub batch_size: usize,

    /// How long to wait for graceful shutdown (default: 5 seconds)
    pub shutdown_timeout: Duration,
}

impl Default for CronSchedulerOptions {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(10),
            batch_size: 100,
            shutdown_timeout: Duration::from_secs(5),
        }
    }
}

/// Background scheduler for cron jobs.
///
/// Polls the cron jobs table and enqueues jobs when they become due.
/// Supports leader election for HA deployments.
///
/// # Example
///
/// ```rust,no_run
/// use aide_de_camp::runner::cron_scheduler::{CronScheduler, CronSchedulerOptions};
/// use aide_de_camp::core::cron_queue::CronQueue;
/// use aide_de_camp::core::queue::Queue;
/// # use electorate::{Elector, AlwaysLeader};
///
/// # async fn example<Q: Queue + CronQueue + 'static>(queue: Q) {
/// let scheduler = CronScheduler::new(
///     queue,
///     Some(Box::new(AlwaysLeader)),
///     CronSchedulerOptions::default(),
/// );
///
/// // Run until shutdown signal
/// let shutdown = tokio::signal::ctrl_c();
/// scheduler.run_with_shutdown(shutdown).await.unwrap();
/// # }
/// ```
pub struct CronScheduler<Q>
where
    Q: InternalQueue + CronQueue,
{
    queue: Arc<Q>,
    elector: Option<Box<dyn electorate::Elector>>,
    options: CronSchedulerOptions,
}

impl<Q> CronScheduler<Q>
where
    Q: InternalQueue + CronQueue + 'static,
{
    /// Create a new cron scheduler.
    ///
    /// # Arguments
    ///
    /// * `queue` - Queue implementation that also supports cron operations
    /// * `elector` - Optional leader elector. If None, always processes cron jobs.
    /// * `options` - Scheduler configuration options
    pub fn new(
        queue: Q,
        elector: Option<Box<dyn electorate::Elector>>,
        options: CronSchedulerOptions,
    ) -> Self {
        info!(
            poll_interval = ?options.poll_interval,
            batch_size = options.batch_size,
            has_elector = elector.is_some(),
            "Initializing cron scheduler"
        );

        Self {
            queue: Arc::new(queue),
            elector,
            options,
        }
    }

    /// Run the cron scheduler. This method blocks forever.
    pub async fn run(&self) -> Result<(), anyhow::Error> {
        self.run_with_shutdown(std::future::pending()).await
    }

    /// Run the cron scheduler until the shutdown future completes.
    ///
    /// # Arguments
    ///
    /// * `shutdown` - Future that completes when graceful shutdown should begin
    pub async fn run_with_shutdown<F>(&self, shutdown: F) -> Result<(), anyhow::Error>
    where
        F: Future<Output = ()>,
    {
        info!("Cron scheduler starting");

        let shutdown_token = CancellationToken::new();
        let mut interval = tokio::time::interval(self.options.poll_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = self.poll_and_enqueue().await {
                        error!(error = %e, "Error polling and enqueuing cron jobs");
                    }
                }
                _ = &mut shutdown => {
                    info!("Shutdown signal received, stopping cron scheduler");
                    shutdown_token.cancel();
                    break;
                }
            }
        }

        info!("Cron scheduler stopped");
        Ok(())
    }

    /// Poll for due cron jobs and enqueue them.
    ///
    /// This checks if we're the leader (if elector configured), finds all due
    /// cron jobs, and enqueues them as one-time jobs.
    #[instrument(skip(self), fields(is_leader, jobs_processed))]
    async fn poll_and_enqueue(&self) -> Result<(), anyhow::Error> {
        // Check if we're the leader
        let is_leader = match &self.elector {
            Some(elector) => elector
                .is_leader()
                .await
                .context("Failed to check leader status")?,
            None => true, // No elector means always process
        };

        tracing::Span::current().record("is_leader", is_leader);

        if !is_leader {
            debug!("Not the leader, skipping cron poll");
            return Ok(());
        }

        // Poll for due cron jobs
        let now = Utc::now();
        let cron_jobs = self
            .queue
            .poll_due_cron_jobs(now, self.options.batch_size)
            .await
            .context("Failed to poll due cron jobs")?;

        if cron_jobs.is_empty() {
            debug!("No cron jobs due");
            return Ok(());
        }

        info!(count = cron_jobs.len(), "Found due cron jobs");
        tracing::Span::current().record("jobs_processed", cron_jobs.len());

        // Process each cron job
        for cron_job in cron_jobs {
            if let Err(e) = self.process_cron_job(cron_job).await {
                error!(error = %e, "Failed to process cron job");
                // Continue processing other jobs even if one fails
            }
        }

        Ok(())
    }

    /// Process a single cron job by enqueuing it and updating next execution time.
    #[instrument(skip(self, cron_job), fields(cron_id = %cron_job.cron_id, job_type = %cron_job.type_name))]
    async fn process_cron_job(&self, cron_job: CronJob) -> Result<(), anyhow::Error> {
        // Check if max_runs limit has been reached
        if let Some(max_runs) = cron_job.max_runs {
            if cron_job.run_count >= max_runs {
                info!(
                    cron_id = %cron_job.cron_id,
                    max_runs,
                    "Cron job has reached max_runs limit, disabling"
                );
                self.queue
                    .set_cron_enabled(&cron_job.cron_id, false)
                    .await
                    .context("Failed to disable cron job")?;
                return Ok(());
            }
        }

        // Enqueue the job using internal queue method
        let jid = self
            .queue
            .schedule_untyped(
                cron_job.type_hash,
                &cron_job.type_name,
                cron_job.payload.clone(),
                ScheduleOptions::now().with_priority(cron_job.priority),
            )
            .await
            .context("Failed to enqueue cron job")?;

        debug!(
            cron_id = %cron_job.cron_id,
            job_id = %jid,
            "Enqueued job from cron"
        );

        // Calculate next execution time
        let schedule = CronSchedule::parse(&cron_job.cron_expression)
            .map_err(|e| anyhow::anyhow!("Failed to parse cron expression: {}", e))?;

        let next_execution_at = schedule
            .next_from_now()
            .ok_or_else(|| anyhow::anyhow!("No upcoming execution time for cron expression"))?;

        // Update the cron job
        self.queue
            .mark_cron_executed(&cron_job.cron_id, next_execution_at)
            .await
            .context("Failed to mark cron job as executed")?;

        info!(
            cron_id = %cron_job.cron_id,
            next_execution_at = %next_execution_at,
            "Successfully processed cron job"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::cron_queue::CronError;
    use crate::core::job_handle::JobHandle;
    use crate::core::queue::{Queue, QueueError, ScheduleOptions};
    use crate::core::Bytes;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Mutex;
    use uuid::Uuid;

    #[test]
    fn test_default_options() {
        let opts = CronSchedulerOptions::default();
        assert_eq!(opts.poll_interval, Duration::from_secs(10));
        assert_eq!(opts.batch_size, 100);
        assert_eq!(opts.shutdown_timeout, Duration::from_secs(5));
    }

    // Mock JobHandle for testing
    #[derive(Clone)]
    struct MockJobHandle;

    #[async_trait]
    impl JobHandle for MockJobHandle {
        fn id(&self) -> Uuid {
            Uuid::now_v7()
        }

        fn type_hash(&self) -> u64 {
            0
        }

        fn type_name(&self) -> &str {
            "MockJob"
        }

        fn payload(&self) -> Bytes {
            Bytes::new()
        }

        fn retries(&self) -> u32 {
            0
        }

        async fn complete(self) -> Result<(), QueueError> {
            Ok(())
        }

        async fn fail(self) -> Result<(), QueueError> {
            Ok(())
        }

        async fn dead_queue(
            self,
            _error_context: Option<crate::core::job_handle::ErrorContext>,
        ) -> Result<(), QueueError> {
            Ok(())
        }
    }

    // Mock queue that tracks operations
    #[derive(Clone)]
    struct MockCronQueue {
        cron_jobs: Arc<Mutex<Vec<CronJob>>>,
        scheduled_jobs: Arc<Mutex<Vec<(u64, String, serde_json::Value)>>>,
        poll_called: Arc<AtomicUsize>,
        disabled_crons: Arc<Mutex<Vec<String>>>,
        executed_crons: Arc<Mutex<Vec<(String, crate::core::DateTime)>>>,
    }

    impl MockCronQueue {
        fn new() -> Self {
            Self {
                cron_jobs: Arc::new(Mutex::new(Vec::new())),
                scheduled_jobs: Arc::new(Mutex::new(Vec::new())),
                poll_called: Arc::new(AtomicUsize::new(0)),
                disabled_crons: Arc::new(Mutex::new(Vec::new())),
                executed_crons: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn add_cron_job(&self, cron_job: CronJob) {
            self.cron_jobs.lock().unwrap().push(cron_job);
        }

        fn get_poll_count(&self) -> usize {
            self.poll_called.load(Ordering::SeqCst)
        }

        fn get_scheduled_jobs(&self) -> Vec<(u64, String, serde_json::Value)> {
            self.scheduled_jobs.lock().unwrap().clone()
        }

        fn get_disabled_crons(&self) -> Vec<String> {
            self.disabled_crons.lock().unwrap().clone()
        }

        fn get_executed_crons(&self) -> Vec<(String, crate::core::DateTime)> {
            self.executed_crons.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl Queue for MockCronQueue {
        type JobHandle = MockJobHandle;

        async fn schedule<J>(
            &self,
            _payload: J::Payload,
            _options: ScheduleOptions,
        ) -> Result<Uuid, QueueError>
        where
            J: crate::core::job_processor::JobProcessor + 'static,
            J::Payload: serde::Serialize + for<'de> serde::Deserialize<'de>,
        {
            Ok(Uuid::now_v7())
        }

        async fn poll_next(
            &self,
            _type_hashes: &[u64],
            _now: crate::core::DateTime,
        ) -> Result<Option<Self::JobHandle>, QueueError> {
            Ok(None)
        }

        async fn cancel_job(&self, _job_id: Uuid) -> Result<bool, QueueError> {
            Ok(true)
        }

        async fn unschedule_job<J>(&self, _job_id: Uuid) -> Result<J::Payload, QueueError>
        where
            J: crate::core::job_processor::JobProcessor + 'static,
            J::Payload: for<'de> serde::Deserialize<'de>,
        {
            Err(QueueError::job_not_found(_job_id))
        }
    }

    #[async_trait]
    impl InternalQueue for MockCronQueue {
        async fn schedule_untyped(
            &self,
            type_hash: u64,
            type_name: &str,
            payload: serde_json::Value,
            _options: ScheduleOptions,
        ) -> Result<Uuid, QueueError> {
            self.scheduled_jobs
                .lock()
                .unwrap()
                .push((type_hash, type_name.to_string(), payload));
            Ok(Uuid::now_v7())
        }
    }

    #[async_trait]
    impl CronQueue for MockCronQueue {
        async fn schedule_cron<J>(
            &self,
            _cron_expression: &str,
            _payload: J::Payload,
            _priority: i8,
        ) -> Result<String, CronError>
        where
            J: crate::core::job_processor::JobProcessor + 'static,
            J::Payload: serde::Serialize,
        {
            Ok(Uuid::now_v7().to_string())
        }

        async fn list_cron_jobs(&self, _job_type: Option<&str>) -> Result<Vec<CronJob>, CronError> {
            Ok(self.cron_jobs.lock().unwrap().clone())
        }

        async fn get_cron_job(&self, _cron_id: &str) -> Result<CronJob, CronError> {
            Err(CronError::CronNotFound {
                cron_id: Uuid::nil(),
                expected_type: None,
            })
        }

        async fn update_cron_schedule(
            &self,
            _cron_id: &str,
            _cron_expression: &str,
        ) -> Result<(), CronError> {
            Ok(())
        }

        async fn set_cron_enabled(&self, cron_id: &str, _enabled: bool) -> Result<(), CronError> {
            self.disabled_crons
                .lock()
                .unwrap()
                .push(cron_id.to_string());
            Ok(())
        }

        async fn delete_cron_job(&self, _cron_id: &str) -> Result<(), CronError> {
            Ok(())
        }

        async fn update_cron_payload<J>(
            &self,
            _cron_id: &str,
            _payload: J::Payload,
        ) -> Result<(), CronError>
        where
            J: crate::core::job_processor::JobProcessor + 'static,
            J::Payload: serde::Serialize,
        {
            Ok(())
        }

        async fn poll_due_cron_jobs(
            &self,
            _now: crate::core::DateTime,
            _limit: usize,
        ) -> Result<Vec<CronJob>, CronError> {
            self.poll_called.fetch_add(1, Ordering::SeqCst);
            Ok(self.cron_jobs.lock().unwrap().clone())
        }

        async fn mark_cron_executed(
            &self,
            cron_id: &str,
            next_execution_at: crate::core::DateTime,
        ) -> Result<(), CronError> {
            self.executed_crons
                .lock()
                .unwrap()
                .push((cron_id.to_string(), next_execution_at));
            Ok(())
        }
    }

    // Mock elector implementations
    struct AlwaysLeader;

    #[async_trait]
    impl electorate::Elector for AlwaysLeader {
        async fn is_leader(&self) -> Result<bool, electorate::ElectorError> {
            Ok(true)
        }
    }

    struct NeverLeader;

    #[async_trait]
    impl electorate::Elector for NeverLeader {
        async fn is_leader(&self) -> Result<bool, electorate::ElectorError> {
            Ok(false)
        }
    }

    // Helper to create a test cron job
    fn create_test_cron_job(
        cron_id: &str,
        type_name: &str,
        cron_expression: &str,
        run_count: i32,
        max_runs: Option<i32>,
    ) -> CronJob {
        CronJob {
            cron_id: cron_id.to_string(),
            queue: "default".to_string(),
            type_hash: 12345,
            type_name: type_name.to_string(),
            payload: serde_json::json!({"test": "data"}),
            cron_expression: cron_expression.to_string(),
            priority: 0,
            created_at: Utc::now(),
            last_enqueued_at: None,
            next_execution_at: Utc::now(),
            enabled: true,
            max_runs,
            run_count,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_cron_scheduler_no_elector_processes() {
        let queue = MockCronQueue::new();
        queue.add_cron_job(create_test_cron_job(
            "test-1",
            "TestJob",
            "0 0 * * * *", // Every hour
            0,
            None,
        ));

        let scheduler = CronScheduler::new(
            queue.clone(),
            None, // No elector
            CronSchedulerOptions {
                poll_interval: Duration::from_secs(1),
                batch_size: 10,
                shutdown_timeout: Duration::from_secs(1),
            },
        );

        // Trigger one poll
        scheduler.poll_and_enqueue().await.unwrap();

        // Should have polled and scheduled job
        assert_eq!(queue.get_poll_count(), 1);
        assert_eq!(queue.get_scheduled_jobs().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn test_cron_scheduler_is_leader_processes() {
        let queue = MockCronQueue::new();
        queue.add_cron_job(create_test_cron_job(
            "test-1",
            "TestJob",
            "0 0 * * * *",
            0,
            None,
        ));

        let scheduler = CronScheduler::new(
            queue.clone(),
            Some(Box::new(AlwaysLeader)),
            CronSchedulerOptions {
                poll_interval: Duration::from_secs(1),
                batch_size: 10,
                shutdown_timeout: Duration::from_secs(1),
            },
        );

        scheduler.poll_and_enqueue().await.unwrap();

        assert_eq!(queue.get_poll_count(), 1);
        assert_eq!(queue.get_scheduled_jobs().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn test_cron_scheduler_not_leader_skips() {
        let queue = MockCronQueue::new();
        queue.add_cron_job(create_test_cron_job(
            "test-1",
            "TestJob",
            "0 0 * * * *",
            0,
            None,
        ));

        let scheduler = CronScheduler::new(
            queue.clone(),
            Some(Box::new(NeverLeader)),
            CronSchedulerOptions {
                poll_interval: Duration::from_secs(1),
                batch_size: 10,
                shutdown_timeout: Duration::from_secs(1),
            },
        );

        scheduler.poll_and_enqueue().await.unwrap();

        // Should not have polled or scheduled
        assert_eq!(queue.get_poll_count(), 0);
        assert_eq!(queue.get_scheduled_jobs().len(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn test_cron_scheduler_poll_and_enqueue_empty() {
        let queue = MockCronQueue::new();
        // No cron jobs added

        let scheduler = CronScheduler::new(
            queue.clone(),
            None,
            CronSchedulerOptions {
                poll_interval: Duration::from_secs(1),
                batch_size: 10,
                shutdown_timeout: Duration::from_secs(1),
            },
        );

        scheduler.poll_and_enqueue().await.unwrap();

        assert_eq!(queue.get_poll_count(), 1);
        assert_eq!(queue.get_scheduled_jobs().len(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn test_cron_scheduler_poll_and_enqueue_one() {
        let queue = MockCronQueue::new();
        queue.add_cron_job(create_test_cron_job(
            "test-1",
            "TestJob",
            "0 0 * * * *",
            0,
            None,
        ));

        let scheduler = CronScheduler::new(
            queue.clone(),
            None,
            CronSchedulerOptions {
                poll_interval: Duration::from_secs(1),
                batch_size: 10,
                shutdown_timeout: Duration::from_secs(1),
            },
        );

        scheduler.poll_and_enqueue().await.unwrap();

        let scheduled = queue.get_scheduled_jobs();
        assert_eq!(scheduled.len(), 1);
        assert_eq!(scheduled[0].0, 12345); // type_hash
        assert_eq!(scheduled[0].1, "TestJob"); // type_name
    }

    #[tokio::test(start_paused = true)]
    async fn test_cron_scheduler_poll_and_enqueue_batch() {
        let queue = MockCronQueue::new();

        // Add 3 cron jobs
        for i in 1..=3 {
            queue.add_cron_job(create_test_cron_job(
                &format!("test-{}", i),
                &format!("TestJob{}", i),
                "0 0 * * * *",
                0,
                None,
            ));
        }

        let scheduler = CronScheduler::new(
            queue.clone(),
            None,
            CronSchedulerOptions {
                poll_interval: Duration::from_secs(1),
                batch_size: 10,
                shutdown_timeout: Duration::from_secs(1),
            },
        );

        scheduler.poll_and_enqueue().await.unwrap();

        assert_eq!(queue.get_scheduled_jobs().len(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn test_cron_scheduler_max_runs_limit_reached() {
        let queue = MockCronQueue::new();
        queue.add_cron_job(create_test_cron_job(
            "test-1",
            "TestJob",
            "0 0 * * * *",
            5,       // run_count
            Some(5), // max_runs
        ));

        let scheduler = CronScheduler::new(
            queue.clone(),
            None,
            CronSchedulerOptions {
                poll_interval: Duration::from_secs(1),
                batch_size: 10,
                shutdown_timeout: Duration::from_secs(1),
            },
        );

        scheduler.poll_and_enqueue().await.unwrap();

        // Should not have scheduled the job
        assert_eq!(queue.get_scheduled_jobs().len(), 0);
        // Should have disabled the cron
        assert_eq!(queue.get_disabled_crons().len(), 1);
        assert_eq!(queue.get_disabled_crons()[0], "test-1");
    }

    #[tokio::test(start_paused = true)]
    async fn test_cron_scheduler_max_runs_limit_not_reached() {
        let queue = MockCronQueue::new();
        queue.add_cron_job(create_test_cron_job(
            "test-1",
            "TestJob",
            "0 0 * * * *",
            3,       // run_count
            Some(5), // max_runs
        ));

        let scheduler = CronScheduler::new(
            queue.clone(),
            None,
            CronSchedulerOptions {
                poll_interval: Duration::from_secs(1),
                batch_size: 10,
                shutdown_timeout: Duration::from_secs(1),
            },
        );

        scheduler.poll_and_enqueue().await.unwrap();

        // Should have scheduled the job
        assert_eq!(queue.get_scheduled_jobs().len(), 1);
        // Should not have disabled
        assert_eq!(queue.get_disabled_crons().len(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn test_cron_scheduler_updates_next_execution() {
        let queue = MockCronQueue::new();
        queue.add_cron_job(create_test_cron_job(
            "test-1",
            "TestJob",
            "0 0 * * * *",
            0,
            None,
        ));

        let scheduler = CronScheduler::new(
            queue.clone(),
            None,
            CronSchedulerOptions {
                poll_interval: Duration::from_secs(1),
                batch_size: 10,
                shutdown_timeout: Duration::from_secs(1),
            },
        );

        scheduler.poll_and_enqueue().await.unwrap();

        // Should have marked cron as executed with next execution time
        let executed = queue.get_executed_crons();
        assert_eq!(executed.len(), 1);
        assert_eq!(executed[0].0, "test-1");
        // Next execution should be in the future
        assert!(executed[0].1 > Utc::now());
    }

    #[tokio::test(start_paused = true)]
    async fn test_cron_scheduler_invalid_cron_expression() {
        let queue = MockCronQueue::new();

        // Create job with invalid expression (this will cause parse to fail)
        let mut job = create_test_cron_job("test-1", "TestJob", "invalid expression", 0, None);
        job.cron_expression = "invalid".to_string();
        queue.add_cron_job(job);

        let scheduler = CronScheduler::new(
            queue.clone(),
            None,
            CronSchedulerOptions {
                poll_interval: Duration::from_secs(1),
                batch_size: 10,
                shutdown_timeout: Duration::from_secs(1),
            },
        );

        // Should not panic, should log error and continue
        scheduler.poll_and_enqueue().await.unwrap();

        // Job should have been scheduled even though next execution couldn't be calculated
        // (the error happens after scheduling)
        assert_eq!(queue.get_scheduled_jobs().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn test_cron_scheduler_shutdown_signal() {
        let queue = MockCronQueue::new();

        let scheduler = CronScheduler::new(
            queue.clone(),
            None,
            CronSchedulerOptions {
                poll_interval: Duration::from_millis(100),
                batch_size: 10,
                shutdown_timeout: Duration::from_secs(1),
            },
        );

        let shutdown_signal = Arc::new(AtomicBool::new(false));
        let shutdown_signal_clone = shutdown_signal.clone();

        let handle = tokio::spawn(async move {
            scheduler
                .run_with_shutdown(async move {
                    while !shutdown_signal_clone.load(Ordering::SeqCst) {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await
        });

        // Let it run a bit
        tokio::time::advance(Duration::from_millis(50)).await;

        // Trigger shutdown
        shutdown_signal.store(true, Ordering::SeqCst);

        // Should complete successfully
        let result = tokio::time::timeout(Duration::from_secs(2), handle).await;
        assert!(result.is_ok());
        assert!(result.unwrap().unwrap().is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn test_cron_scheduler_continues_on_job_error() {
        let queue = MockCronQueue::new();

        // Add multiple jobs, one with invalid expression
        queue.add_cron_job(create_test_cron_job(
            "test-1", "TestJob1", "invalid", 0, None,
        ));
        queue.add_cron_job(create_test_cron_job(
            "test-2",
            "TestJob2",
            "0 0 * * * *",
            0,
            None,
        ));
        queue.add_cron_job(create_test_cron_job(
            "test-3",
            "TestJob3",
            "0 0 * * * *",
            0,
            None,
        ));

        let scheduler = CronScheduler::new(
            queue.clone(),
            None,
            CronSchedulerOptions {
                poll_interval: Duration::from_secs(1),
                batch_size: 10,
                shutdown_timeout: Duration::from_secs(1),
            },
        );

        scheduler.poll_and_enqueue().await.unwrap();

        // All 3 should have been scheduled (error happens after scheduling)
        assert_eq!(queue.get_scheduled_jobs().len(), 3);
    }
}
