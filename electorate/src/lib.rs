//! Leader election abstraction for distributed systems.
//!
//! This crate provides a trait-based abstraction for leader election in distributed
//! systems, along with several implementations for different use cases.
//!
//! # Features
//!
//! - `postgres` - Enables PostgreSQL advisory lock-based leader election
//!
//! # Examples
//!
//! ## Single-instance deployment (always leader)
//!
//! ```rust
//! use electorate::{Elector, AlwaysLeader};
//!
//! # async fn example() {
//! let elector = AlwaysLeader;
//! assert!(elector.is_leader().await.unwrap());
//! # }
//! ```
//!
//! ## Follower-only node (never leader)
//!
//! ```rust
//! use electorate::{Elector, NeverLeader};
//!
//! # async fn example() {
//! let elector = NeverLeader;
//! assert!(!elector.is_leader().await.unwrap());
//! # }
//! ```
//!
//! ## PostgreSQL advisory locks (distributed leader election)
//!
//! ```rust,no_run
//! use electorate::{Elector, PostgresAdvisoryLockElector};
//! use sqlx::PgPool;
//!
//! # async fn example() -> anyhow::Result<()> {
//! let pool = PgPool::connect("postgresql://localhost/mydb").await?;
//! let elector = PostgresAdvisoryLockElector::new(pool, None);
//!
//! if elector.is_leader().await? {
//!     println!("I am the leader!");
//! }
//! # Ok(())
//! # }
//! ```

pub mod always;
pub mod elector;
pub mod never;

#[cfg(feature = "postgres")]
pub mod postgres;

// Re-export main types
pub use always::AlwaysLeader;
pub use elector::{Elector, ElectorError};
pub use never::NeverLeader;

#[cfg(feature = "postgres")]
pub use postgres::PostgresAdvisoryLockElector;

/// Prelude module for convenient imports.
///
/// # Example
///
/// ```rust
/// use electorate::prelude::*;
/// ```
pub mod prelude {
    pub use crate::always::AlwaysLeader;
    pub use crate::elector::{Elector, ElectorError};
    pub use crate::never::NeverLeader;

    #[cfg(feature = "postgres")]
    pub use crate::postgres::PostgresAdvisoryLockElector;
}
