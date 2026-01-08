use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

use crate::core::queue::QueueError;
use crate::core::DateTime;

/// Structured error context for dead queue entries.
///
/// Provides rich debugging information when a job fails and is moved to the dead queue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorContext {
    /// The error message from the last failure.
    pub error_message: String,

    /// The type name of the error (e.g., "serde_json::Error").
    pub error_type: String,

    /// Number of times the job was retried before failing.
    pub retry_count: u32,

    /// When the last error occurred.
    pub last_error_at: DateTime,

    /// Additional metadata (e.g., stack traces, debug info).
    pub metadata: HashMap<String, serde_json::Value>,
}

impl ErrorContext {
    /// Create error context from a standard error.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use aide_de_camp::core::job_handle::ErrorContext;
    ///
    /// let error = std::io::Error::new(std::io::ErrorKind::Other, "test");
    /// let context = ErrorContext::from_error(&error, 3);
    /// assert_eq!(context.retry_count, 3);
    /// ```
    pub fn from_error<E: std::error::Error>(error: &E, retries: u32) -> Self {
        Self {
            error_message: error.to_string(),
            error_type: std::any::type_name::<E>().to_string(),
            retry_count: retries,
            last_error_at: chrono::Utc::now(),
            metadata: HashMap::new(),
        }
    }

    /// Add metadata to the error context.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use aide_de_camp::core::job_handle::ErrorContext;
    /// use serde_json::json;
    ///
    /// let error = std::io::Error::new(std::io::ErrorKind::Other, "test");
    /// let context = ErrorContext::from_error(&error, 0)
    ///     .with_metadata("request_id".to_string(), json!("req-12345"));
    /// ```
    pub fn with_metadata(mut self, key: String, value: serde_json::Value) -> Self {
        self.metadata.insert(key, value);
        self
    }
}

/// Job lifecycle handler and metadata provider.
///
/// Provides access to job metadata and methods for updating job status.
/// Implementations should handle database updates when status changes occur.
#[async_trait]
pub trait JobHandle: Send + Sync {
    /// Get the job ID.
    fn id(&self) -> Uuid;

    /// Get the job type hash for routing.
    fn type_hash(&self) -> u64;

    /// Get the job type name for debugging.
    fn type_name(&self) -> &str;

    /// Get the raw payload bytes.
    fn payload(&self) -> Bytes;

    /// How many times this job has been retried.
    fn retries(&self) -> u32;

    /// Mark the job as completed successfully.
    ///
    /// Consumes self to prevent further use after completion.
    async fn complete(self) -> Result<(), QueueError>;

    /// Mark the job as failed.
    ///
    /// The job may be retried based on retry policy.
    /// Consumes self to prevent further use after failure.
    async fn fail(self) -> Result<(), QueueError>;

    /// Move the job to the dead queue with optional error context.
    ///
    /// Use this when a job has exceeded retries or encountered an unrecoverable error.
    /// Consumes self to prevent further use.
    ///
    /// # Arguments
    ///
    /// * `error_context` - Optional structured error information for debugging
    async fn dead_queue(self, error_context: Option<ErrorContext>) -> Result<(), QueueError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;
    use std::io;

    #[test]
    fn test_error_context_from_error_basic() {
        let error = io::Error::other("test error message");
        let context = ErrorContext::from_error(&error, 3);

        assert_eq!(context.error_message, "test error message");
        assert_eq!(context.retry_count, 3);
        assert!(context.metadata.is_empty());
    }

    #[test]
    fn test_error_context_from_error_retry_count() {
        let error = io::Error::other("test");

        let context_0 = ErrorContext::from_error(&error, 0);
        assert_eq!(context_0.retry_count, 0);

        let context_5 = ErrorContext::from_error(&error, 5);
        assert_eq!(context_5.retry_count, 5);

        let context_100 = ErrorContext::from_error(&error, 100);
        assert_eq!(context_100.retry_count, 100);
    }

    #[test]
    fn test_error_context_from_error_timestamp() {
        let before = Utc::now();
        let error = io::Error::other("test");
        let context = ErrorContext::from_error(&error, 0);
        let after = Utc::now();

        assert!(context.last_error_at >= before);
        assert!(context.last_error_at <= after);
    }

    #[test]
    fn test_error_context_from_error_type_name() {
        let io_error = io::Error::other("test");
        let context = ErrorContext::from_error(&io_error, 0);

        assert!(
            context.error_type.contains("io::Error")
                || context.error_type.contains("std::io::error::Error")
        );

        // Test with a different error type
        let json_error = serde_json::from_str::<serde_json::Value>("invalid").unwrap_err();
        let context = ErrorContext::from_error(&json_error, 0);

        assert!(context.error_type.contains("serde_json"));
    }

    #[test]
    fn test_error_context_with_metadata_single() {
        let error = io::Error::other("test");
        let context = ErrorContext::from_error(&error, 0)
            .with_metadata("request_id".to_string(), json!("req-12345"));

        assert_eq!(context.metadata.len(), 1);
        assert_eq!(
            context.metadata.get("request_id").unwrap(),
            &json!("req-12345")
        );
    }

    #[test]
    fn test_error_context_with_metadata_multiple() {
        let error = io::Error::other("test");
        let context = ErrorContext::from_error(&error, 0)
            .with_metadata("request_id".to_string(), json!("req-12345"))
            .with_metadata("user_id".to_string(), json!(42))
            .with_metadata("trace".to_string(), json!(["step1", "step2"]));

        assert_eq!(context.metadata.len(), 3);
        assert_eq!(
            context.metadata.get("request_id").unwrap(),
            &json!("req-12345")
        );
        assert_eq!(context.metadata.get("user_id").unwrap(), &json!(42));
        assert_eq!(
            context.metadata.get("trace").unwrap(),
            &json!(["step1", "step2"])
        );
    }

    #[test]
    fn test_error_context_with_metadata_complex_json() {
        let error = io::Error::other("test");
        let complex_value = json!({
            "nested": {
                "array": [1, 2, 3],
                "object": {
                    "key": "value"
                }
            },
            "null_field": null
        });

        let context = ErrorContext::from_error(&error, 0)
            .with_metadata("debug_info".to_string(), complex_value.clone());

        assert_eq!(context.metadata.get("debug_info").unwrap(), &complex_value);
    }

    #[test]
    fn test_error_context_serialization_roundtrip() {
        let error = io::Error::other("roundtrip test");
        let original = ErrorContext::from_error(&error, 5)
            .with_metadata("key1".to_string(), json!("value1"))
            .with_metadata("key2".to_string(), json!(42));

        // Serialize to JSON
        let json = serde_json::to_string(&original).unwrap();

        // Deserialize back
        let deserialized: ErrorContext = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.error_message, original.error_message);
        assert_eq!(deserialized.error_type, original.error_type);
        assert_eq!(deserialized.retry_count, original.retry_count);
        assert_eq!(deserialized.last_error_at, original.last_error_at);
        assert_eq!(deserialized.metadata.len(), original.metadata.len());
        assert_eq!(
            deserialized.metadata.get("key1"),
            original.metadata.get("key1")
        );
        assert_eq!(
            deserialized.metadata.get("key2"),
            original.metadata.get("key2")
        );
    }
}
