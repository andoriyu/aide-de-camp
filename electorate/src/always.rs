use async_trait::async_trait;

use crate::elector::{Elector, ElectorError};

/// An elector that always returns true.
///
/// Use this for single-instance deployments (especially SQLite) where
/// leader election is unnecessary.
///
/// # Example
/// ```rust
/// use electorate::{Elector, AlwaysLeader};
///
/// # async fn example() {
/// let elector = AlwaysLeader;
/// assert!(elector.is_leader().await.unwrap());
/// # }
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct AlwaysLeader;

#[async_trait]
impl Elector for AlwaysLeader {
    async fn is_leader(&self) -> Result<bool, ElectorError> {
        Ok(true)
    }
}
