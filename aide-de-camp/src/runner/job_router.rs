use super::wrapped_job::{BoxedJobHandler, WrappedJobHandler};
use crate::core::job_handle::{ErrorContext, JobHandle};
use crate::core::job_processor::{JobError, JobProcessor};
use crate::core::queue::{Queue, QueueError};
use crate::core::Utc;
use chrono::Duration;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::instrument;

/// A job processor router. Matches job type to job processor implementation.
/// This type requires that your jobs implement `Serialize` + `Deserialize` from serde. Those traits are re-exported in prelude.
///
/// ## Example
/// ```rust
/// use aide_de_camp::prelude::{JobProcessor, RunnerRouter, Serialize, Deserialize, Xid, CancellationToken};
/// use async_trait::async_trait;
/// struct MyJob;
///
/// impl MyJob {
///     async fn do_work(&self) -> anyhow::Result<()> {
///         // ..do some work
///         Ok(())
///     }
/// }
///
/// #[derive(Serialize, Deserialize)]
/// struct MyJobPayload(u8, String);
///
/// #[async_trait::async_trait]
/// impl JobProcessor for MyJob {
///     type Payload = MyJobPayload;
///     type Error = anyhow::Error;
///
///     fn name() -> &'static str {
///         "my_job"
///     }
///
///     async fn handle(&self, jid: Xid, payload: Self::Payload, cancellation_token: CancellationToken) -> Result<(), Self::Error> {
///         tokio::select! {
///             result = self.do_work() => { result }
///             _ = cancellation_token.cancelled() => { Ok(()) }
///         }
///     }
/// }
///
/// let router = {
///     let mut r = RunnerRouter::default();
///     r.add_job_handler(MyJob);
///     r
/// };
///
///```
#[derive(Default)]
pub struct RunnerRouter {
    jobs: HashMap<u64, BoxedJobHandler>,
}

impl RunnerRouter {
    /// Register a job handler with the router. If job by that type already present, it will get replaced.
    pub fn add_job_handler<J>(&mut self, job: J)
    where
        J: JobProcessor + 'static,
        J::Payload: for<'de> Deserialize<'de> + Serialize,
        J::Error: Into<JobError>,
    {
        let type_hash = J::type_hash();
        let boxed = WrappedJobHandler::new(job).boxed();
        self.jobs.entry(type_hash).or_insert(boxed);
    }

    pub fn type_hashes(&self) -> Vec<u64> {
        self.jobs.keys().copied().collect()
    }

    /// Process job handle. This function reposible for job lifecycle. If you're implementing your
    /// own job runner, then this is what you should use to process job that is already pulled
    /// from the queue. In all other cases, you shouldn't use this function directly.
    #[instrument(skip_all, err, fields(job_type = %job_handle.type_name(), jid = %job_handle.id().to_string(), retries = job_handle.retries()))]
    pub async fn process<H: JobHandle>(
        &self,
        job_handle: H,
        cancellation_token: CancellationToken,
    ) -> Result<(), RunnerError> {
        let type_hash = job_handle.type_hash();
        if let Some(r) = self.jobs.get(&type_hash) {
            let job_shutdown_timeout = r.shutdown_timeout();

            let job_result = tokio::select! {
                job_result = r.handle(job_handle.id(), job_handle.payload(), cancellation_token.child_token()) => {
                    job_result
                }
                cancellation_result = cancellation_handler(job_shutdown_timeout, cancellation_token.child_token()) => {
                    cancellation_result
                }
            };
            handle_job_result(
                job_result,
                job_handle,
                r.max_retries(),
                cancellation_token.child_token(),
            )
            .await
        } else {
            Err(RunnerError::UnknownJobType {
                type_name: job_handle.type_name().to_string(),
                type_hash,
            })
        }
    }

    /// In a loop, poll the queue with interval and process incoming jobs.
    /// Function process jobs one-by-one without job-level concurrency. If you need
    /// concurrency, look at the `JobRunner` instead.
    pub async fn listen<Q, QR>(
        &self,
        queue: Q,
        poll_interval: Duration,
        cancellation_token: CancellationToken,
    ) where
        Q: AsRef<QR>,
        QR: Queue,
    {
        let type_hashes = self.type_hashes();
        let mut interval = tokio::time::interval(poll_interval.to_std().unwrap());
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let now = Utc::now();
                    match queue.as_ref().poll_next(&type_hashes, now).await {
                        Ok(Some(job_handle)) => {
                            let cancellation_token = cancellation_token.child_token();
                            self.handle_next_job_result::<QR>(Ok(job_handle), cancellation_token).await;
                        }
                        Ok(None) => {
                            // No jobs available
                        }
                        Err(e) => {
                            self.handle_next_job_result::<QR>(Err(e), cancellation_token.child_token()).await;
                        }
                    }
                }
                _ = cancellation_token.cancelled() => {
                    // If cancellation is requested while a job is processing, this block will execute on the next iteration.
                    tracing::debug!("Shutdown request received, stopping listener");
                    return
                }
            }
        }
    }

    async fn handle_next_job_result<QR>(
        &self,
        next: Result<QR::JobHandle, QueueError>,
        cancellation_token: CancellationToken,
    ) where
        QR: Queue,
    {
        match next {
            Ok(handle) => {
                match self.process(handle, cancellation_token.child_token()).await {
                    Ok(_) => {}
                    Err(RunnerError::QueueError(e)) => handle_queue_error(e).await,
                    Err(RunnerError::UnknownJobType {
                        type_name,
                        type_hash,
                    }) => {
                        tracing::error!("Unknown job type: {} (hash: {})", type_name, type_hash)
                    }
                };
            }
            Err(e) => {
                handle_queue_error(e).await;
            }
        }
    }
}

/// Errors returned by the router.
#[derive(Error, Debug)]
pub enum RunnerError {
    #[error("Runner is not configured to run job type '{type_name}' (hash: {type_hash})")]
    UnknownJobType { type_name: String, type_hash: u64 },
    #[error(transparent)]
    QueueError(#[from] QueueError),
}

async fn cancellation_handler(
    job_shutdown_timeout: std::time::Duration,
    cancellation_token: CancellationToken,
) -> Result<(), JobError> {
    cancellation_token.cancelled().await;
    // Wait for the duration of the shutdown timeout specified by the job configuration.
    // The job should complete within the timeout period, preventing this method from completing.
    tokio::time::sleep(job_shutdown_timeout).await;
    Err(JobError::ShutdownTimeout(job_shutdown_timeout))
}

async fn handle_job_result<H: JobHandle>(
    job_result: Result<(), JobError>,
    job_handle: H,
    max_retries: u32,
    cancellation_token: CancellationToken,
) -> Result<(), RunnerError> {
    if cancellation_token.is_cancelled() {
        tracing::info!("Cancellation was requested during job processing");
    }
    match job_result {
        Ok(_) => {
            job_handle.complete().await?;
            Ok(())
        }
        Err(e) => {
            tracing::error!("Error during job processing: {}", e);
            if job_handle.retries() >= max_retries {
                tracing::warn!("Moving job {} to dead queue", job_handle.id().to_string());

                // Create error context
                let error_context = ErrorContext {
                    error_message: e.to_string(),
                    error_type: match e {
                        JobError::Deserialization { .. } => "deserialization".to_string(),
                        JobError::ShutdownTimeout(_) => "shutdown_timeout".to_string(),
                        JobError::HandlerError(_) => "handler_error".to_string(),
                    },
                    retry_count: job_handle.retries(),
                    last_error_at: Utc::now(),
                    metadata: HashMap::new(),
                };

                job_handle.dead_queue(Some(error_context)).await?;
                Ok(())
            } else {
                job_handle.fail().await?;
                Ok(())
            }
        }
    }
}

async fn handle_queue_error(error: QueueError) {
    tracing::error!("Encountered QueueError: {}", error);
    tracing::warn!("Suspending worker for 5 seconds");
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::core::job_type_id::JobTypeId;
    use std::convert::Infallible;
    use uuid::Uuid;

    #[tokio::test]
    async fn it_is_object_safe_and_wrappable() {
        struct Example;

        impl JobTypeId for Example {
            fn type_name() -> &'static str {
                "test::Example"
            }
        }

        #[async_trait::async_trait]
        impl JobProcessor for Example {
            type Payload = Vec<i32>;
            type Error = Infallible;

            async fn handle(
                &self,
                _jid: Uuid,
                _payload: Self::Payload,
                _cancellation_token: CancellationToken,
            ) -> Result<(), Infallible> {
                dbg!("we did it patrick");
                Ok(())
            }
        }

        let payload = vec![1, 2, 3];

        let job: Box<dyn JobProcessor<Payload = _, Error = _>> = Box::new(Example);

        job.handle(Uuid::now_v7(), payload.clone(), CancellationToken::new())
            .await
            .unwrap();
        let wrapped: Box<dyn JobProcessor<Payload = _, Error = JobError>> =
            Box::new(WrappedJobHandler::new(Example));

        let payload = serde_json::to_vec(&payload).unwrap();

        wrapped
            .handle(Uuid::now_v7(), payload.into(), CancellationToken::new())
            .await
            .unwrap();
    }
}
