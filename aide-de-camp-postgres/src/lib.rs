//! PostgreSQL implementation of the Queue trait from aide-de-camp

pub mod cron_queue;
pub mod job_handle;
pub mod queue;
pub mod types;

pub use queue::PostgresQueue;
use sqlx::migrate::Migrator;
pub static MIGRATOR: Migrator = sqlx::migrate!();

#[cfg(test)]
mod test {
    use crate::queue::PostgresQueue;
    use crate::MIGRATOR;
    use aide_de_camp::core::job_handle::JobHandle;
    use aide_de_camp::core::job_processor::JobProcessor;
    use aide_de_camp::core::job_type_id::JobTypeId;
    use aide_de_camp::core::queue::{Queue, ScheduleOptions};
    use aide_de_camp::core::{CancellationToken, Duration, Utc};
    use aide_de_camp::prelude::QueueError;
    use async_trait::async_trait;
    use serde::{Deserialize, Serialize};
    use sqlx::PgPool;
    use std::convert::Infallible;
    use uuid::Uuid;

    #[allow(dead_code)]
    pub fn setup_logger() {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_test_writer()
            .init();
    }

    #[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
    struct TestPayload1 {
        arg1: i32,
        arg2: String,
    }

    impl Default for TestPayload1 {
        fn default() -> Self {
            Self {
                arg1: 1774,
                arg2: String::from("this is a test"),
            }
        }
    }

    struct TestJob1;

    impl JobTypeId for TestJob1 {
        fn type_name() -> &'static str {
            "postgres::tests::TestJob1"
        }
    }

    #[async_trait]
    impl JobProcessor for TestJob1 {
        type Payload = TestPayload1;
        type Error = Infallible;

        async fn handle(
            &self,
            _jid: Uuid,
            _payload: Self::Payload,
            _cancellation_token: CancellationToken,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
    struct TestPayload2 {
        arg1: i32,
        arg2: u64,
        arg3: String,
    }

    impl Default for TestPayload2 {
        fn default() -> Self {
            Self {
                arg1: 1774,
                arg2: 42,
                arg3: String::from("this is a test"),
            }
        }
    }

    struct TestJob2;

    impl JobTypeId for TestJob2 {
        fn type_name() -> &'static str {
            "postgres::tests::TestJob2"
        }
    }

    #[async_trait]
    impl JobProcessor for TestJob2 {
        type Payload = TestPayload2;
        type Error = Infallible;

        async fn handle(
            &self,
            _jid: Uuid,
            _payload: Self::Payload,
            _cancellation_token: CancellationToken,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    // Job with payload2 but different type
    struct TestJob3;

    impl JobTypeId for TestJob3 {
        fn type_name() -> &'static str {
            "postgres::tests::TestJob3"
        }
    }

    #[async_trait]
    impl JobProcessor for TestJob3 {
        type Payload = TestPayload2;
        type Error = Infallible;

        async fn handle(
            &self,
            _jid: Uuid,
            _payload: Self::Payload,
            _cancellation_token: CancellationToken,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[sqlx::test(migrator = "MIGRATOR")]
    async fn queue_smoke_test(pool: PgPool) {
        let queue = PostgresQueue::with_pool(pool);

        // If there are no jobs, this should return Ok(None);
        {
            let job = queue
                .poll_next(&[TestJob1::type_hash()], Utc::now())
                .await
                .unwrap();
            assert!(job.is_none());
        }
        // Schedule a job to run now
        let jid1 = queue
            .schedule::<TestJob1>(TestPayload1::default(), ScheduleOptions::now())
            .await
            .unwrap();

        // Now poll_next should return this job to us
        let job1 = queue
            .poll_next(&[TestJob1::type_hash()], Utc::now())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(jid1, job1.id());
        // Second time poll should not return anything
        {
            let job = queue
                .poll_next(&[TestJob1::type_hash()], Utc::now())
                .await
                .unwrap();
            assert!(job.is_none());
        }

        // Completed jobs should not show up in queue again
        job1.complete().await.unwrap();
        {
            let job = queue
                .poll_next(&[TestJob1::type_hash()], Utc::now())
                .await
                .unwrap();
            assert!(job.is_none());
        }
    }

    #[sqlx::test(migrator = "MIGRATOR")]
    async fn failed_jobs(pool: PgPool) {
        let queue = PostgresQueue::with_pool(pool);

        // Schedule a job to run now
        let _jid1 = queue
            .schedule::<TestJob1>(TestPayload1::default(), ScheduleOptions::now())
            .await
            .unwrap();

        // Now poll_next should return this job to us
        let job1 = queue
            .poll_next(&[TestJob1::type_hash()], Utc::now())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job1.retries(), 0);
        // Fail the job
        job1.fail().await.unwrap();

        // We should be able to get the same job again, but it should have increased retry count

        let job1 = queue
            .poll_next(&[TestJob1::type_hash()], Utc::now())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job1.retries(), 1);
    }

    #[sqlx::test(migrator = "MIGRATOR")]
    async fn scheduling_future_jobs(pool: PgPool) {
        setup_logger();
        let queue = PostgresQueue::with_pool(pool);

        // schedule to run job tomorrow
        // schedule a job to run now
        let tomorrow_jid = queue
            .schedule::<TestJob1>(
                TestPayload1::default(),
                ScheduleOptions::now().in_duration(Duration::days(1)),
            )
            .await
            .unwrap();

        // Should not be polled yet
        {
            let job = queue
                .poll_next(&[TestJob1::type_hash()], Utc::now())
                .await
                .unwrap();
            assert!(job.is_none());
        }

        let hour_ago = { Utc::now() - Duration::hours(1) };
        let hour_ago_jid = queue
            .schedule::<TestJob1>(TestPayload1::default(), ScheduleOptions::now().at(hour_ago))
            .await
            .unwrap();

        {
            let job = queue
                .poll_next(&[TestJob1::type_hash()], Utc::now())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(hour_ago_jid, job.id());
        }

        let tomorrow = Utc::now() + Duration::days(1) + Duration::minutes(1);
        {
            let job = queue
                .poll_next(&[TestJob1::type_hash()], tomorrow)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(tomorrow_jid, job.id());
        }

        // Everything should be in-progress, so None
        {
            let job = queue
                .poll_next(&[TestJob1::type_hash()], tomorrow)
                .await
                .unwrap();
            assert!(job.is_none());
        }
    }

    #[sqlx::test(migrator = "MIGRATOR")]
    async fn cancel_job_not_started(pool: PgPool) {
        let queue = PostgresQueue::with_pool(pool);
        let jid = queue
            .schedule::<TestJob1>(TestPayload1::default(), ScheduleOptions::now())
            .await
            .unwrap();
        queue.cancel_job(jid).await.unwrap();

        // Should return None
        {
            let job = queue
                .poll_next(&[TestJob1::type_hash()], Utc::now())
                .await
                .unwrap();
            assert!(job.is_none());
        }

        // Should return false (not found)
        let ret = queue.cancel_job(jid).await.unwrap();
        assert!(!ret);
    }

    #[sqlx::test(migrator = "MIGRATOR")]
    async fn cancel_job_return_payload(pool: PgPool) {
        let queue = PostgresQueue::with_pool(pool);
        let payload = TestPayload1::default();
        let jid = queue
            .schedule::<TestJob1>(payload.clone(), ScheduleOptions::now())
            .await
            .unwrap();

        let deleted_payload = queue.unschedule_job::<TestJob1>(jid).await.unwrap();
        assert_eq!(payload, deleted_payload);

        let ret = queue.unschedule_job::<TestJob1>(jid).await;
        assert!(matches!(ret, Err(QueueError::JobNotFound { .. })));
    }

    #[sqlx::test(migrator = "MIGRATOR")]
    async fn cancel_wrong_type(pool: PgPool) {
        let queue = PostgresQueue::with_pool(pool);
        let jid = queue
            .schedule::<TestJob1>(TestPayload1::default(), ScheduleOptions::now())
            .await
            .unwrap();

        let result = queue.unschedule_job::<TestJob2>(jid).await;
        assert!(matches!(result, Err(QueueError::JobNotFound { .. })));

        // TestJob3 has a different type_hash, so it won't find the job either
        let result = queue.unschedule_job::<TestJob3>(jid).await;
        assert!(matches!(result, Err(QueueError::JobNotFound { .. })));
    }

    #[sqlx::test(migrator = "MIGRATOR")]
    async fn cancel_job_started(pool: PgPool) {
        let queue = PostgresQueue::with_pool(pool);
        let payload = TestPayload1::default();
        let jid = queue
            .schedule::<TestJob1>(payload.clone(), ScheduleOptions::now())
            .await
            .unwrap();

        let _job = queue
            .poll_next(&[TestJob1::type_hash()], Utc::now())
            .await
            .unwrap()
            .unwrap();

        let ret = queue.cancel_job(jid).await.unwrap();
        assert!(!ret); // Returns false because job has already started

        let ret = queue.unschedule_job::<TestJob1>(jid).await;
        assert!(matches!(ret, Err(QueueError::JobNotFound { .. })));
    }
    #[sqlx::test(migrator = "MIGRATOR")]
    async fn priority_polling(pool: PgPool) {
        let queue = PostgresQueue::with_pool(pool);
        let hour_ago = { Utc::now() - Duration::hours(1) };
        let _hour_ago_jid = queue
            .schedule::<TestJob1>(TestPayload1::default(), ScheduleOptions::now().at(hour_ago))
            .await
            .unwrap();

        let higher_priority_jid = queue
            .schedule::<TestJob1>(
                TestPayload1::default(),
                ScheduleOptions::now().at(hour_ago).with_priority(3),
            )
            .await
            .unwrap();

        let job = queue
            .poll_next(&[TestJob1::type_hash()], Utc::now())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(higher_priority_jid, job.id());
    }

    // Generate all 14 shared integration test specs for PostgreSQL Queue
    aide_de_camp::generate_queue_spec_tests! {
        backend = "pg",
        test_attr = sqlx::test(migrator = "MIGRATOR"),
        setup = |pool: PgPool| PostgresQueue::with_pool(pool)
    }

    // Generate all 20 shared integration test specs for PostgreSQL CronQueue
    aide_de_camp::generate_cron_queue_spec_tests! {
        backend = "pg",
        test_attr = sqlx::test(migrator = "MIGRATOR"),
        setup = |pool: PgPool| PostgresQueue::with_pool(pool)
    }
}
