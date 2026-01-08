use bytes::Bytes;
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, Clone, FromRow)]
pub(crate) struct JobRow {
    pub(crate) jid: Uuid,
    pub(crate) type_hash: i64,
    pub(crate) type_name: String,
    pub(crate) payload: sqlx::types::JsonValue,
    pub(crate) retries: i32,
    // Priority is used in SQL ORDER BY, not accessed in Rust after polling
    #[allow(dead_code)]
    pub(crate) priority: i16,
}

impl JobRow {
    /// Convert UUID to match trait signature
    pub(crate) fn id(&self) -> Uuid {
        self.jid
    }

    /// Convert i64 to u64 for type hash
    pub(crate) fn type_hash(&self) -> u64 {
        self.type_hash as u64
    }

    /// Get type name reference
    pub(crate) fn type_name(&self) -> &str {
        &self.type_name
    }

    /// Convert JsonValue to Bytes for JobHandle
    pub(crate) fn payload_bytes(&self) -> Bytes {
        serde_json::to_vec(&self.payload)
            .expect("Failed to serialize payload")
            .into()
    }

    /// Get retry count as u32
    pub(crate) fn retries(&self) -> u32 {
        self.retries as u32
    }
}
