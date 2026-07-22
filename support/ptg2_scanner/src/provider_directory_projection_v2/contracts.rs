// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::io;

pub const PROVIDER_DIRECTORY_PROJECTION_COPY_MAGIC: &[u8] = b"HPPDCOPY\0\0\0\x02";
pub const COPY_SPOOL_CONTRACT_ID: &str =
    "healthporta.provider-directory.native-projection-copy-spool.v2";
pub const TRANSFORM_CONTRACT_ID: &str = "healthporta.provider-directory.fhir-profile-projection.v2";
pub const COPY_CONTRACT_ID: &str = "healthporta.postgresql.binary-copy.v1";
pub const COPY_COLUMN_CONTRACT_ID: &str =
    "healthporta.provider-directory.projection-stage-columns.v1";
pub const CANONICAL_ROW_CONTRACT_ID: &str =
    "healthporta.provider-directory.canonical-projection-row.v2";
pub const COPY_SUMMARY_RECORD_KIND: &str = "provider_directory_projection_copy_summary";
pub const SEMANTIC_TYPED_EVIDENCE_CONTRACT_ID: &str =
    "healthporta.provider-directory.semantic-typed-evidence.v2";
pub const SEMANTIC_EVIDENCE_HASH_DOMAIN: &str =
    "provider-directory-projection-semantic-typed-evidence-v2";
pub const MAX_COPY_SUMMARY_BYTES: usize = 1024 * 1024;
pub const MAX_COPY_STREAM_BYTES: usize = 128 * 1024 * 1024;
pub const PROVIDER_DIRECTORY_PROJECTION_COPY_MAX_OWNED_IO_BYTES: usize =
    32 * 1024 * 1024 + MAX_COPY_STREAM_BYTES + MAX_COPY_SUMMARY_BYTES + 32;
pub const COPY_FIELD_COUNT: i16 = 18;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionCopyContext {
    pub recipe_id: String,
    pub partition_id: String,
    pub partition_ordinal: u32,
}

#[derive(Clone, Debug)]
pub struct ProjectedResourceRow {
    pub physical_projection_id: String,
    pub resource_type: String,
    pub resource_id: String,
    pub proof_partition_id: String,
    pub payload_hash: String,
    pub payload_json: Value,
    pub source_rank: String,
    pub summary_npi: Option<i64>,
    pub summary_address_count: i32,
    pub summary_addressed_location: bool,
    pub summary_geocoded_location: bool,
    pub summary_network_link_count: i32,
    pub summary_affiliation_link_count: i32,
    pub active: Option<bool>,
    pub effective_start: Option<String>,
    pub effective_end: Option<String>,
    pub observed_at: Option<String>,
    pub profile_evidence_json: Option<Value>,
    pub semantic_evidence_sha256: String,
}

impl ProjectedResourceRow {
    pub fn sort_key(&self) -> (&str, &str, &str, &str) {
        (
            &self.resource_type,
            &self.resource_id,
            &self.source_rank,
            &self.payload_hash,
        )
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderDirectoryProjectionCopyTimings {
    pub input_read_seconds: f64,
    pub parse_seconds: f64,
    pub transform_seconds: f64,
    pub sort_seconds: f64,
    pub copy_encode_seconds: f64,
    pub total_before_stdout_seconds: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderDirectoryProjectionCopySummary {
    pub record_kind: String,
    pub contract_id: String,
    pub decoder_contract_id: String,
    pub transform_contract_id: String,
    pub copy_contract_id: String,
    pub copy_column_contract_id: String,
    pub canonical_row_contract_id: String,
    pub input_framing: String,
    pub recipe_id: String,
    pub partition_id: String,
    pub partition_ordinal: u32,
    pub input_byte_count: u64,
    pub input_sha256: String,
    pub canonical_input_byte_count: u64,
    pub canonical_input_sha256: String,
    pub resource_count: u64,
    pub resource_counts: BTreeMap<String, u64>,
    pub first_identity: [String; 2],
    pub last_identity: [String; 2],
    pub canonical_row_sha256: String,
    pub copy_byte_count: u64,
    pub copy_sha256: String,
    pub timings_seconds: ProviderDirectoryProjectionCopyTimings,
}

#[derive(Clone, Debug)]
pub struct ProviderDirectoryProjectionCopySpool {
    pub header_bytes: Vec<u8>,
    pub summary: ProviderDirectoryProjectionCopySummary,
    pub copy_bytes: Vec<u8>,
}

impl ProviderDirectoryProjectionCopySpool {
    pub fn wire_byte_count(&self) -> usize {
        self.header_bytes.len() + self.copy_bytes.len() + 1
    }
}

pub fn invalid_input(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

pub fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

pub fn is_sha256(candidate: &str) -> bool {
    candidate.len() == 64
        && candidate
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

pub fn timings_are_valid(timings: &ProviderDirectoryProjectionCopyTimings) -> bool {
    [
        timings.input_read_seconds,
        timings.parse_seconds,
        timings.transform_seconds,
        timings.sort_seconds,
        timings.copy_encode_seconds,
        timings.total_before_stdout_seconds,
    ]
    .into_iter()
    .all(|seconds| seconds.is_finite() && (0.0..=86_400.0).contains(&seconds))
}
