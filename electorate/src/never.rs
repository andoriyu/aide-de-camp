use async_trait::async_trait;

use crate::elector::{Elector, ElectorError};

/// An elector that always returns false.
///
/// Use this for follower nodes that should never attempt to become leader,
/// or for disabling leader-specific functionality in development/testing.
///
/// # Example
/// ```rust
/// use electorate::{Elector, NeverLeader};
///
/// # async fn example() {
/// let elector = NeverLeader;
/// assert!(!elector.is_leader().await.unwrap());
/// # }
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct NeverLeader;

#[async_trait]
impl Elector for NeverLeader {
    async fn is_leader(&self) -> Result<bool, ElectorError> {
        Ok(false)
    }
}
