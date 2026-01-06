//! Background service for scheduling cron jobs.
//!
//! The CronScheduler polls the cron jobs table and enqueues jobs when their
//! execution time arrives. It supports leader election for high-availability
//! deployments where only one instance should process cron jobs.

use crate::core::cron::CronSchedule;
use crate::core::cron_queue::{CronJob, CronQueue};
use crate::core::queue::Queue;
use crate::core::{Bytes, Utc};
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
    Q: Queue + CronQueue,
{
    queue: Arc<Q>,
    elector: Option<Box<dyn electorate::Elector>>,
    options: CronSchedulerOptions,
}

impl<Q> CronScheduler<Q>
where
    Q: Queue + CronQueue + 'static,
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
    #[instrument(skip(self, cron_job), fields(cron_id = %cron_job.cron_id, job_type = %cron_job.job_type))]
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

        // Enqueue the job using schedule_raw
        let payload = Bytes::from(cron_job.payload.clone());
        let jid = self
            .queue
            .schedule_raw(&cron_job.job_type, payload, Utc::now(), cron_job.priority)
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

    #[test]
    fn test_default_options() {
        let opts = CronSchedulerOptions::default();
        assert_eq!(opts.poll_interval, Duration::from_secs(10));
        assert_eq!(opts.batch_size, 100);
        assert_eq!(opts.shutdown_timeout, Duration::from_secs(5));
    }
}
