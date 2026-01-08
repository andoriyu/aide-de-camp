#![doc = include_str!("../README.md")]

pub mod core;

/// Shared test specifications for backend implementations.
///
/// These test functions ensure consistent behavior across all Queue implementations
/// (PostgreSQL, SQLite, etc.). Backend tests should call these functions with their
/// queue instance.
#[doc(hidden)]
pub mod queue_spec;

/// Shared test specifications for CronQueue implementations.
///
/// These test functions ensure consistent cron behavior across all CronQueue
/// implementations. Backend tests should call these functions with their cron queue instance.
#[doc(hidden)]
pub mod cron_queue_spec;

/// Default implementation of job runner.
#[cfg(feature = "runner")]
pub mod runner {
    pub mod cron_scheduler;
    pub mod job_router;
    pub mod job_runner;
    pub mod wrapped_job;
}

/// Re-exports to simplify importing this crate types.
pub mod prelude {
    pub use super::core::{
        job_handle::{ErrorContext, JobHandle},
        job_processor::{JobError, JobProcessor},
        job_type_id::JobTypeId,
        queue::{Queue, QueueError, ScheduleOptions},
        CancellationToken, Duration, Uuid,
    };
    #[cfg(feature = "runner")]
    pub use super::runner::{
        job_router::RunnerRouter, job_runner::JobRunner, job_runner::RunnerOptions,
    };
    pub use serde::{Deserialize, Serialize};
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
