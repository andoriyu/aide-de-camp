use crate::core::job_processor::{JobError, JobProcessor};
use crate::core::job_type_id::JobTypeId;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::instrument;
use uuid::Uuid;

/// Shorthand for boxed trait object for a WrappedJob.
pub type BoxedJobHandler = Box<dyn JobProcessor<Payload = Bytes, Error = JobError>>;

/// Object-safe implementation of a job that can be used in runner. Generally speaking, you don't
/// need to directly use this type, JobRouter takes care of everything related to it.
pub struct WrappedJobHandler<T: JobProcessor> {
    job: T,
}

impl<J> WrappedJobHandler<J>
where
    J: JobProcessor + 'static,
    J::Payload: for<'de> Deserialize<'de> + Serialize,
    J::Error: Into<JobError>,
{
    pub fn new(job: J) -> Self {
        Self { job }
    }

    pub fn boxed(self) -> BoxedJobHandler {
        Box::new(self) as BoxedJobHandler
    }
}

impl<J> JobTypeId for WrappedJobHandler<J>
where
    J: JobProcessor + 'static,
{
    fn type_name() -> &'static str {
        J::type_name()
    }
}

#[async_trait]
impl<J> JobProcessor for WrappedJobHandler<J>
where
    J: JobProcessor + 'static,
    J::Payload: for<'de> Deserialize<'de> + Serialize,
    J::Error: Into<JobError>,
{
    type Payload = Bytes;
    type Error = JobError;

    #[instrument(skip_all, err, fields(jid = %jid, job_type = %Self::type_name()))]
    async fn handle(
        &self,
        jid: Uuid,
        payload: Self::Payload,
        cancellation_token: CancellationToken,
    ) -> Result<(), Self::Error> {
        let payload_result: Result<J::Payload, _> = serde_json::from_slice(payload.as_ref());

        let typed_payload = payload_result
            .map_err(|error| JobError::deserialization_error(error, payload.as_ref()))?;

        self.job
            .handle(jid, typed_payload, cancellation_token)
            .await
            .map_err(Into::into)
    }

    fn max_retries(&self) -> u32 {
        self.job.max_retries()
    }

    fn shutdown_timeout(&self) -> std::time::Duration {
        self.job.shutdown_timeout()
    }
}

impl<J> From<J> for WrappedJobHandler<J>
where
    J: JobProcessor + 'static,
    J::Payload: for<'de> Deserialize<'de> + Serialize,
    J::Error: Into<JobError>,
{
    fn from(job: J) -> Self {
        Self::new(job)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc,
    };
    use std::time::Duration;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestPayload {
        value: String,
    }

    #[derive(Debug, thiserror::Error)]
    #[error("test error")]
    struct TestError;

    impl From<TestError> for JobError {
        fn from(e: TestError) -> Self {
            JobError::HandlerError(anyhow::anyhow!(e.to_string()))
        }
    }

    struct MockJobProcessor {
        should_fail: Arc<AtomicBool>,
        execution_count: Arc<AtomicU32>,
        max_retries: u32,
        shutdown_timeout_ms: u64,
    }

    impl MockJobProcessor {
        fn new() -> Self {
            Self {
                should_fail: Arc::new(AtomicBool::new(false)),
                execution_count: Arc::new(AtomicU32::new(0)),
                max_retries: 3,
                shutdown_timeout_ms: 5000,
            }
        }

        fn with_max_retries(mut self, retries: u32) -> Self {
            self.max_retries = retries;
            self
        }

        fn with_shutdown_timeout(mut self, ms: u64) -> Self {
            self.shutdown_timeout_ms = ms;
            self
        }

        fn fail_next(&self) {
            self.should_fail.store(true, Ordering::SeqCst);
        }
    }

    impl JobTypeId for MockJobProcessor {
        fn type_name() -> &'static str {
            "aide_de_camp::runner::wrapped_job::tests::MockJobProcessor"
        }
    }

    #[async_trait]
    impl JobProcessor for MockJobProcessor {
        type Payload = TestPayload;
        type Error = TestError;

        async fn handle(
            &self,
            _jid: Uuid,
            _payload: Self::Payload,
            _cancellation_token: CancellationToken,
        ) -> Result<(), Self::Error> {
            self.execution_count.fetch_add(1, Ordering::SeqCst);

            if self.should_fail.load(Ordering::SeqCst) {
                Err(TestError)
            } else {
                Ok(())
            }
        }

        fn max_retries(&self) -> u32 {
            self.max_retries
        }

        fn shutdown_timeout(&self) -> Duration {
            Duration::from_millis(self.shutdown_timeout_ms)
        }
    }

    #[test]
    fn test_wrapped_job_handler_type_name() {
        assert_eq!(
            WrappedJobHandler::<MockJobProcessor>::type_name(),
            MockJobProcessor::type_name()
        );
    }

    #[test]
    fn test_wrapped_job_handler_type_hash() {
        assert_eq!(
            WrappedJobHandler::<MockJobProcessor>::type_hash(),
            MockJobProcessor::type_hash()
        );
    }

    #[test]
    fn test_wrapped_job_handler_max_retries() {
        let mock = MockJobProcessor::new().with_max_retries(5);
        let wrapped = WrappedJobHandler::new(mock);

        assert_eq!(wrapped.max_retries(), 5);

        let mock2 = MockJobProcessor::new().with_max_retries(10);
        let wrapped2 = WrappedJobHandler::new(mock2);

        assert_eq!(wrapped2.max_retries(), 10);
    }

    #[test]
    fn test_wrapped_job_handler_shutdown_timeout() {
        let mock = MockJobProcessor::new().with_shutdown_timeout(1000);
        let wrapped = WrappedJobHandler::new(mock);

        assert_eq!(wrapped.shutdown_timeout(), Duration::from_millis(1000));

        let mock2 = MockJobProcessor::new().with_shutdown_timeout(3000);
        let wrapped2 = WrappedJobHandler::new(mock2);

        assert_eq!(wrapped2.shutdown_timeout(), Duration::from_millis(3000));
    }

    #[tokio::test]
    async fn test_wrapped_job_handler_deserialize_success() {
        let mock = MockJobProcessor::new();
        let wrapped = WrappedJobHandler::new(mock);

        let payload = TestPayload {
            value: "test".to_string(),
        };
        let payload_bytes = Bytes::from(serde_json::to_vec(&payload).unwrap());

        let result = wrapped
            .handle(Uuid::now_v7(), payload_bytes, CancellationToken::new())
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_wrapped_job_handler_deserialize_failure() {
        let mock = MockJobProcessor::new();
        let wrapped = WrappedJobHandler::new(mock);

        let invalid_json = Bytes::from("{invalid json}");

        let result = wrapped
            .handle(Uuid::now_v7(), invalid_json, CancellationToken::new())
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            JobError::Deserialization { .. } => {}
            other => panic!("Expected Deserialization error, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_wrapped_job_handler_deserialize_empty() {
        let mock = MockJobProcessor::new();
        let wrapped = WrappedJobHandler::new(mock);

        let empty_bytes = Bytes::from("");

        let result = wrapped
            .handle(Uuid::now_v7(), empty_bytes, CancellationToken::new())
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            JobError::Deserialization { .. } => {}
            other => panic!("Expected Deserialization error, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_wrapped_job_handler_deserialize_wrong_shape() {
        let mock = MockJobProcessor::new();
        let wrapped = WrappedJobHandler::new(mock);

        // Valid JSON but wrong shape (missing 'value' field)
        let wrong_shape = Bytes::from(r#"{"wrong_field": "value"}"#);

        let result = wrapped
            .handle(Uuid::now_v7(), wrong_shape, CancellationToken::new())
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            JobError::Deserialization { .. } => {}
            other => panic!("Expected Deserialization error, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_wrapped_job_handler_execution_success() {
        let mock = MockJobProcessor::new();
        let execution_count = mock.execution_count.clone();
        let wrapped = WrappedJobHandler::new(mock);

        let payload = TestPayload {
            value: "success".to_string(),
        };
        let payload_bytes = Bytes::from(serde_json::to_vec(&payload).unwrap());

        let result = wrapped
            .handle(Uuid::now_v7(), payload_bytes, CancellationToken::new())
            .await;

        assert!(result.is_ok());
        assert_eq!(execution_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_wrapped_job_handler_execution_failure() {
        let mock = MockJobProcessor::new();
        mock.fail_next();
        let wrapped = WrappedJobHandler::new(mock);

        let payload = TestPayload {
            value: "fail".to_string(),
        };
        let payload_bytes = Bytes::from(serde_json::to_vec(&payload).unwrap());

        let result = wrapped
            .handle(Uuid::now_v7(), payload_bytes, CancellationToken::new())
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            JobError::HandlerError(e) => {
                assert_eq!(e.to_string(), "test error");
            }
            other => panic!("Expected HandlerError, got {:?}", other),
        }
    }
}
