use async_trait::async_trait;
use std::convert::Infallible;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::core::job_type_id::JobTypeId;

/// A job-handler interface with type-safe identification.
///
/// Your job processor must implement both `JobProcessor` and `JobTypeId` traits.
/// The payload must implement `Serialize` and `Deserialize` for queue operations.
///
/// # Type Safety
///
/// The `JobTypeId` supertrait ensures each job type has a unique identifier,
/// preventing runtime type mismatches and enabling compile-time type checking.
///
/// ## Example
/// ```rust,ignore
/// use aide_de_camp::prelude::{JobProcessor, JobTypeId, Serialize, Deserialize, Uuid, CancellationToken};
/// use async_trait::async_trait;
///
/// struct MyJob;
///
/// impl JobTypeId for MyJob {
///     fn type_name() -> &'static str {
///         "myapp::jobs::MyJob"
///     }
/// }
///
/// #[derive(Serialize, Deserialize)]
/// struct MyJobPayload {
///     user_id: u64,
///     action: String,
/// }
///
/// #[async_trait::async_trait]
/// impl JobProcessor for MyJob {
///     type Payload = MyJobPayload;
///     type Error = anyhow::Error;
///
///     async fn handle(
///         &self,
///         jid: Uuid,
///         payload: Self::Payload,
///         cancellation_token: CancellationToken
///     ) -> Result<(), Self::Error> {
///         tokio::select! {
///             result = self.do_work(&payload) => { result }
///             _ = cancellation_token.cancelled() => { Ok(()) }
///         }
///     }
/// }
/// ```
///
/// ## Services
/// If your job processor requires external services (database client, REST client, etc.),
/// add them directly as struct fields.
#[async_trait]
pub trait JobProcessor: JobTypeId + Send + Sync {
    /// The input payload for this job handler.
    ///
    /// For queue operations, the payload should implement `Serialize` and `Deserialize`.
    /// Internal job processors (like `WrappedJobHandler`) may use `Bytes` directly.
    type Payload: Send;

    /// The error type returned by this job.
    ///
    /// Should implement `Into<JobError>` for proper error handling.
    type Error: Send + Into<JobError>;

    /// Execute the job with the given payload.
    ///
    /// Should listen for `cancellation_token.cancelled()` to handle graceful shutdown.
    ///
    /// # Arguments
    ///
    /// * `jid` - Unique job ID
    /// * `payload` - Deserialized job payload
    /// * `cancellation_token` - Token to signal shutdown requests
    async fn handle(
        &self,
        jid: Uuid,
        payload: Self::Payload,
        cancellation_token: CancellationToken,
    ) -> Result<(), Self::Error>;

    /// Maximum number of retries before moving to dead queue.
    ///
    /// Default: 0 (no retries)
    fn max_retries(&self) -> u32 {
        0
    }

    /// Timeout for graceful shutdown.
    ///
    /// How long to wait for the job to complete when shutdown is requested.
    /// After this timeout, the job is forcefully terminated.
    ///
    /// Default: 1 second
    fn shutdown_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs(1)
    }
}

/// Error types for job processing.
#[derive(Error, Debug)]
pub enum JobError {
    /// Failed to deserialize job payload with context.
    #[error("Failed to deserialize payload: {error}\nPayload sample: {payload_sample}")]
    Deserialization {
        #[source]
        error: serde_json::Error,
        payload_sample: String,
    },

    /// Job failed to complete within shutdown timeout.
    #[error("Job failed to complete within the shutdown timeout of {0:#?}")]
    ShutdownTimeout(std::time::Duration),

    /// Error from job handler implementation.
    #[error("Job handler error: {0}")]
    HandlerError(#[source] anyhow::Error),
}

impl JobError {
    /// Create a deserialization error with payload context.
    pub fn deserialization_error(error: serde_json::Error, payload: &[u8]) -> Self {
        let payload_sample = String::from_utf8_lossy(payload).chars().take(500).collect();

        Self::Deserialization {
            error,
            payload_sample,
        }
    }
}

impl From<Infallible> for JobError {
    fn from(_: Infallible) -> Self {
        unreachable!();
    }
}
