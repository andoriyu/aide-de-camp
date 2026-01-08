//! Cron expression parsing and scheduling.
//!
//! This module provides utilities for parsing standard 5-field cron expressions
//! and calculating next execution times.
//!
//! # Cron Format
//!
//! Supports standard 5-field cron format: `min hour day month dow`
//!
//! - **min**: Minute (0-59)
//! - **hour**: Hour (0-23)
//! - **day**: Day of month (1-31)
//! - **month**: Month (1-12)
//! - **dow**: Day of week (0-6, 0 = Sunday)
//!
//! # Examples
//!
//! ```rust
//! use aide_de_camp::core::cron::CronSchedule;
//! use chrono::Utc;
//!
//! // Every day at midnight
//! let schedule = CronSchedule::parse("0 0 * * *").unwrap();
//! let next = schedule.next_from_now().unwrap();
//! println!("Next execution: {}", next);
//!
//! // Every 15 minutes
//! let schedule = CronSchedule::parse("*/15 * * * *").unwrap();
//!
//! // Every Monday at 9 AM
//! let schedule = CronSchedule::parse("0 9 * * 1").unwrap();
//! ```

use crate::core::DateTime;
use cron::Schedule;
use std::str::FromStr;
use thiserror::Error;
use uuid::Uuid;

/// Error type for cron-related operations.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum CronError {
    /// Failed to parse cron expression with context.
    #[error("Invalid cron expression '{expression}': {error}")]
    ParseError { expression: String, error: String },

    /// No upcoming execution time could be calculated.
    #[error("No upcoming execution for cron expression '{0}'")]
    NoUpcomingExecution(String),

    /// Cron job not found.
    #[error("Cron job {cron_id} not found{}", expected_type.as_ref().map(|t| format!(" (expected type: '{}')", t)).unwrap_or_default())]
    CronNotFound {
        cron_id: Uuid,
        expected_type: Option<String>,
    },

    /// Type mismatch when retrieving cron job.
    #[error("Cron job {cron_id} has type '{actual_type}' but expected '{expected_type}'")]
    TypeMismatch {
        cron_id: Uuid,
        actual_type: String,
        expected_type: String,
    },

    /// Database or other error.
    #[error("Database error: {0}")]
    DatabaseError(#[from] anyhow::Error),
}

/// A parsed cron schedule that can calculate next execution times.
///
/// Wraps the `cron` crate's `Schedule` with aide-de-camp-specific types
/// and error handling.
#[derive(Debug, Clone)]
pub struct CronSchedule {
    schedule: Schedule,
    expression: String,
}

impl CronSchedule {
    /// Parse a cron expression into a schedule.
    ///
    /// # Arguments
    ///
    /// * `expression` - A standard 5-field cron expression (min hour day month dow)
    ///
    /// # Examples
    ///
    /// ```rust
    /// use aide_de_camp::core::cron::CronSchedule;
    ///
    /// // Every day at midnight
    /// let schedule = CronSchedule::parse("0 0 * * *").unwrap();
    ///
    /// // Every 5 minutes
    /// let schedule = CronSchedule::parse("*/5 * * * *").unwrap();
    ///
    /// // Every weekday at 9 AM
    /// let schedule = CronSchedule::parse("0 9 * * 1-5").unwrap();
    /// ```
    pub fn parse(expression: &str) -> Result<Self, CronError> {
        let schedule = Schedule::from_str(expression).map_err(|e| CronError::ParseError {
            expression: expression.to_string(),
            error: e.to_string(),
        })?;

        Ok(Self {
            schedule,
            expression: expression.to_string(),
        })
    }

    /// Get the next execution time after a given datetime.
    ///
    /// # Arguments
    ///
    /// * `after` - Calculate next execution after this time
    ///
    /// # Returns
    ///
    /// The next scheduled execution time, or `None` if there are no more executions
    /// (which shouldn't happen for typical cron expressions).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use aide_de_camp::core::cron::CronSchedule;
    /// use aide_de_camp::core::Utc;
    ///
    /// let schedule = CronSchedule::parse("0 0 * * *").unwrap();
    /// let now = Utc::now();
    /// let next = schedule.next_after(now).unwrap();
    /// assert!(next > now);
    /// ```
    pub fn next_after(&self, after: DateTime) -> Option<DateTime> {
        self.schedule.after(&after).next()
    }

    /// Get the next execution time from now.
    ///
    /// Convenience method equivalent to `next_after(Utc::now())`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use aide_de_camp::core::cron::CronSchedule;
    ///
    /// let schedule = CronSchedule::parse("*/15 * * * *").unwrap();
    /// let next = schedule.next_from_now().unwrap();
    /// println!("Next run at: {}", next);
    /// ```
    pub fn next_from_now(&self) -> Option<DateTime> {
        use crate::core::Utc;
        self.next_after(Utc::now())
    }

    /// Get the original cron expression string.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use aide_de_camp::core::cron::CronSchedule;
    ///
    /// let schedule = CronSchedule::parse("0 0 * * *").unwrap();
    /// assert_eq!(schedule.expression(), "0 0 * * *");
    /// ```
    pub fn expression(&self) -> &str {
        &self.expression
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::Utc;
    use chrono::{Datelike, TimeZone, Timelike};

    #[test]
    fn test_parse_valid_cron() {
        let schedule = CronSchedule::parse("0 0 0 * * *").unwrap();
        assert_eq!(schedule.expression(), "0 0 0 * * *");
    }

    #[test]
    fn test_parse_invalid_cron() {
        let result = CronSchedule::parse("invalid");
        assert!(result.is_err());
    }

    #[test]
    fn test_next_after() {
        let schedule = CronSchedule::parse("0 0 0 * * *").unwrap();

        // Test from a specific time: Jan 1, 2024 at 12:00 PM
        let start = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();
        let next = schedule.next_after(start).unwrap();

        // Next should be Jan 2, 2024 at 00:00
        assert_eq!(next.day(), 2);
        assert_eq!(next.hour(), 0);
        assert_eq!(next.minute(), 0);
        assert_eq!(next.second(), 0);
    }

    #[test]
    fn test_every_15_minutes() {
        let schedule = CronSchedule::parse("0 */15 * * * *").unwrap();

        let start = Utc.with_ymd_and_hms(2024, 1, 1, 12, 7, 30).unwrap();
        let next = schedule.next_after(start).unwrap();

        // Next should be 12:15:00
        assert_eq!(next.hour(), 12);
        assert_eq!(next.minute(), 15);
        assert_eq!(next.second(), 0);
    }

    #[test]
    fn test_next_from_now() {
        let schedule = CronSchedule::parse("0 0 0 * * *").unwrap();
        let next = schedule.next_from_now();

        assert!(next.is_some());
        let next = next.unwrap();
        assert!(next > Utc::now());
    }
}
