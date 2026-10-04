use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const SCHEMA_VERSION: u32 = 1;
pub const ADAPTER_VERSION: &str = "1";

pub fn hash(bytes: impl AsRef<[u8]>) -> String {
    format!("{:x}", Sha256::digest(bytes.as_ref()))
}

pub fn stable_id(parts: &[&str]) -> String {
    hash(serde_json::to_vec(parts).expect("strings serialize"))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Record {
    pub id: String,
    pub harness: String,
    pub session_id: String,
    pub source_id: String,
    pub source_path: String,
    pub position: u64,
    pub captured_at: String,
    pub status: String,
    pub diagnostic: Option<String>,
    pub adapter_version: String,
    pub raw: Vec<u8>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Event {
    pub id: String,
    pub record_id: String,
    pub harness: String,
    pub session_id: String,
    pub source_id: String,
    pub parent_session_id: Option<String>,
    pub native_id: Option<String>,
    pub parent_id: Option<String>,
    pub position: u64,
    pub sub_index: u32,
    pub timestamp: Option<String>,
    pub kind: String,
    pub role: Option<String>,
    pub text: Option<String>,
    pub model: Option<String>,
    pub tool_call_id: Option<String>,
    pub payload_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InventoryFile {
    pub path: String,
    pub size: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionSummary {
    pub harness: String,
    pub session_id: String,
    pub records: u64,
    pub events: u64,
    pub first_timestamp: Option<String>,
    pub last_timestamp: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Manifest {
    pub schema_version: u32,
    pub collector_id: String,
    pub batch_id: String,
    pub created_at: String,
    pub sessions: Vec<SessionSummary>,
    pub files: Vec<InventoryFile>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Query {
    pub harness: Option<String>,
    pub session: Option<String>,
    pub since: Option<String>,
    pub until: Option<String>,
    pub kind: Option<String>,
    pub text: Option<String>,
}

impl Query {
    pub fn matches_session(&self, s: &SessionSummary) -> bool {
        self.harness.as_ref().is_none_or(|h| h == &s.harness)
            && self.session.as_ref().is_none_or(|id| id == &s.session_id)
    }
    pub fn matches(&self, e: &Event) -> bool {
        self.harness.as_ref().is_none_or(|h| h == &e.harness)
            && self.session.as_ref().is_none_or(|s| s == &e.session_id)
            && self.kind.as_ref().is_none_or(|k| k == &e.kind)
            && self
                .text
                .as_ref()
                .is_none_or(|t| e.text.as_ref().is_some_and(|v| v.contains(t)))
            && self
                .since
                .as_ref()
                .is_none_or(|s| e.timestamp.as_ref().is_some_and(|t| t >= s))
            && self
                .until
                .as_ref()
                .is_none_or(|s| e.timestamp.as_ref().is_some_and(|t| t <= s))
    }
}
