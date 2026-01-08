use async_trait::async_trait;
use thiserror::Error;

/// Trait for leader election in distributed systems.
///
/// Implementations determine whether the current instance should act as the leader
/// for coordinating distributed tasks like cron job scheduling.
#[async_trait]
pub trait Elector: Send + Sync {
    /// Check if this instance is currently the leader.
    ///
    /// This method should be lightweight and idempotent, as it may be called frequently.
    ///
    /// # Returns
    /// - `Ok(true)` if this instance is the leader
    /// - `Ok(false)` if another instance is the leader
    /// - `Err` if leadership status cannot be determined
    async fn is_leader(&self) -> Result<bool, ElectorError>;

    /// Optional: Release leadership explicitly (for graceful shutdown).
    /// Default implementation is a no-op.
    async fn release_leadership(&self) -> Result<(), ElectorError> {
        Ok(())
    }
}

/// Errors that can occur during leader election
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum ElectorError {
    #[error("Failed to acquire or check leader status: {0}")]
    AcquisitionError(String),

    #[error("Database error during leader election: {0}")]
    DatabaseError(#[from] anyhow::Error),

    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}
