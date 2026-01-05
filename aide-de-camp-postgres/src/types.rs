use aide_de_camp::core::Xid;
use bytes::Bytes;
use sqlx::FromRow;
use std::str::FromStr;

#[derive(Debug, Clone, FromRow)]
pub(crate) struct JobRow {
    pub(crate) jid: String,
    pub(crate) job_type: String,
    pub(crate) payload: sqlx::types::JsonValue,
    pub(crate) retries: i32,
    /// Timestamp when the job is scheduled to run (not directly accessed - used in SQL SELECT)
    #[allow(dead_code)]
    pub(crate) scheduled_at: i64,
    /// Timestamp when the job was enqueued (not directly accessed - used in SQL SELECT)
    #[allow(dead_code)]
    pub(crate) enqueued_at: i64,
}

impl JobRow {
    pub(crate) fn into_payload(self) -> Bytes {
        // Convert JsonValue back to JSON bytes
        serde_json::to_vec(&self.payload).unwrap().into()
    }

    pub(crate) fn jid(&self) -> Xid {
        Xid::from_str(&self.jid).expect("Invalid XID in database")
    }
}
