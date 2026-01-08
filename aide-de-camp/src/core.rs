//! Implementation agnostic traits for implementing queues and re-exports of 3rd party types/crates used in public interface.

pub use uuid::Uuid;

/// An alias for `chrono::DateTime<chrono::Utc>`
pub type DateTime = chrono::DateTime<chrono::Utc>;
pub use bytes::Bytes;
pub use chrono::{Duration, Utc};
pub use serde_json;
pub use tokio_util::sync::CancellationToken;

pub mod cron;
pub mod cron_queue;
pub mod job_handle;
pub mod job_processor;
pub mod job_type_id;
pub mod queue;
