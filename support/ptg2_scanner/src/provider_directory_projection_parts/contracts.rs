// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io;

pub const PROVIDER_DIRECTORY_PROJECTION_SPOOL_CONTRACT_ID: &str =
    "healthporta.provider-directory.native-projection-spool.v1";
pub const PROVIDER_DIRECTORY_PROJECTION_DECODER_CONTRACT_ID: &str =
    "healthporta.provider-directory.fhir-json-decoder.v1";
pub const PROVIDER_DIRECTORY_PROJECTION_RECORD_CONTRACT_ID: &str =
    "healthporta.provider-directory.native-projection-record.v1";
pub const PROVIDER_DIRECTORY_PROJECTION_SPOOL_MAGIC: &[u8] = b"HPPDPROJ\0\0\0\x01";
pub const PROVIDER_DIRECTORY_PROJECTION_MAX_INPUT_BYTES: usize = 32 * 1024 * 1024;
pub const PROVIDER_DIRECTORY_PROJECTION_MAX_RESOURCE_BYTES: usize = 16 * 1024 * 1024;
pub const PROVIDER_DIRECTORY_PROJECTION_MAX_RESOURCE_COUNT: usize = 100_000;
pub const PROVIDER_DIRECTORY_PROJECTION_MAX_SPOOL_BYTES: usize = 128 * 1024 * 1024;

/// Input plus encoded-spool ceilings while both buffers coexist.
///
/// Deserialized JSON values and bounded Rayon work items add allocator overhead;
/// this is an owned-I/O bound, not a total resident-memory claim. A later slice
/// may stream records without changing the v1 spool wire contract.
pub const PROVIDER_DIRECTORY_PROJECTION_MAX_OWNED_IO_BYTES: usize =
    PROVIDER_DIRECTORY_PROJECTION_MAX_INPUT_BYTES + PROVIDER_DIRECTORY_PROJECTION_MAX_SPOOL_BYTES;

pub const PROVIDER_DIRECTORY_SUPPORTED_RESOURCE_TYPES: [&str; 8] = [
    "Endpoint",
    "HealthcareService",
    "InsurancePlan",
    "Location",
    "Organization",
    "OrganizationAffiliation",
    "Practitioner",
    "PractitionerRole",
];

pub(super) const RECORD_FRAME_KIND: u8 = 1;
pub(super) const SUMMARY_FRAME_KIND: u8 = 2;
pub(super) const END_FRAME_KIND: u8 = 255;
pub(super) const FRAME_HEADER_BYTES: usize = 5;
pub(super) const MAX_SUMMARY_BYTES: usize = 1024 * 1024;

pub(super) fn invalid_input(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

pub(super) fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

pub(super) fn sha256_hex(digest: Sha256) -> String {
    let bytes = digest.finalize();
    let mut encoded = String::with_capacity(64);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

pub(super) fn hash_bytes(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    sha256_hex(digest)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderDirectoryInputFraming {
    Ndjson,
    Bundle,
}

impl ProviderDirectoryInputFraming {
    pub(super) fn parse(value: &str) -> io::Result<Self> {
        match value {
            "ndjson" => Ok(Self::Ndjson),
            "bundle" => Ok(Self::Bundle),
            _ => Err(invalid_input(
                "provider-directory projection framing must be ndjson or bundle",
            )),
        }
    }

    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Ndjson => "ndjson",
            Self::Bundle => "bundle",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderDirectoryProjectionRecord {
    pub contract_id: String,
    pub input_ordinal: u64,
    pub resource_type: String,
    pub resource_id: String,
    pub payload_hash: String,
    pub payload_json: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderDirectoryProjectionTimings {
    pub input_read_seconds: f64,
    pub parse_seconds: f64,
    pub canonicalize_seconds: f64,
    pub sort_seconds: f64,
    pub record_spool_seconds: f64,
    pub total_before_stdout_seconds: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderDirectoryProjectionSummary {
    pub record_kind: String,
    pub contract_id: String,
    pub decoder_contract_id: String,
    pub input_framing: String,
    pub input_byte_count: u64,
    pub input_sha256: String,
    pub canonical_input_byte_count: u64,
    pub canonical_input_sha256: String,
    pub resource_count: u64,
    pub resource_counts: BTreeMap<String, u64>,
    pub first_identity: [String; 2],
    pub last_identity: [String; 2],
    pub record_frame_byte_count: u64,
    pub record_set_sha256: String,
    pub record_spool_sha256: String,
    pub timings_seconds: ProviderDirectoryProjectionTimings,
}

#[derive(Clone, Debug)]
pub struct ProviderDirectoryProjectionSpool {
    pub bytes: Vec<u8>,
    pub summary: ProviderDirectoryProjectionSummary,
}
