// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

use rayon::prelude::*;
use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize as DeriveDeserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::env;
use std::ffi::CString;
use std::fmt;
use std::fs::{self, File, Metadata, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub const UHC_RETAIN_CONTRACT_ID: &str = "healthporta.uhc.retained-json-ranges.v2";
pub const UHC_RETAIN_CONTRACT_VERSION: u64 = 2;
pub const UHC_RETAIN_CANONICALIZATION_ID: &str = "json-object-remove-crlf-append-lf.v1";

const READ_BUFFER_BYTES: usize = 1024 * 1024;
const MAX_PROVIDER_RECORD_BYTES: usize = 16 * 1024 * 1024;
const MIN_RANGE_COUNT: usize = 4;
const MAX_RANGE_COUNT: usize = 256;
const MAX_RANGE_WORKERS: usize = 8;
const RANGE_RECORD_BATCH_BYTES: usize = 512 * 1024;
const RANGE_BATCH_QUEUE_DEPTH: usize = 8;
const PUBLICATION_LINK_RETRIES: usize = 64;
const MAX_RECORD_COUNT: u64 = 250_000_000;
const MAX_MANIFEST_BYTES: u64 = 1024 * 1024;
const MAX_BUILD_ID_BYTES: usize = 160;
const SHA256_HEX_BYTES: usize = 64;
const SHA256_BYTES: usize = 32;

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn invalid_input(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn some_or_invalid_data<T>(value: Option<T>, message: &'static str) -> io::Result<T> {
    match value {
        Some(value) => Ok(value),
        None => Err(invalid_data(message)),
    }
}

fn current_build_id() -> &'static str {
    option_env!("HLTHPRT_UHC_RETAIN_BUILD_ID").unwrap_or(concat!(
        "ptg2_scanner/",
        env!("CARGO_PKG_VERSION"),
        "/uhc-retain-v2"
    ))
}

fn validate_build_id(build_id: &str) -> io::Result<()> {
    if build_id.is_empty()
        || build_id.len() > MAX_BUILD_ID_BYTES
        || build_id
            .bytes()
            .any(|byte| byte.is_ascii_control() || !byte.is_ascii())
    {
        return Err(invalid_data("UHC retained producer_build_id is invalid"));
    }
    Ok(())
}

fn parse_sha256_hex(value: &str) -> io::Result<[u8; SHA256_BYTES]> {
    if value.len() != SHA256_HEX_BYTES
        || value
            .bytes()
            .any(|byte| !matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(invalid_input(
            "expected UHC retained SHA-256 must be 64 lowercase hexadecimal characters",
        ));
    }
    let mut decoded = [0u8; SHA256_BYTES];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = hex_nibble(pair[0]);
        let low = hex_nibble(pair[1]);
        decoded[index] = (high << 4) | low;
    }
    Ok(decoded)
}

fn hex_nibble(value: u8) -> u8 {
    match value {
        b'0'..=b'9' => value - b'0',
        b'a'..=b'f' => value - b'a' + 10,
        _ => unreachable!("SHA-256 syntax is validated before decoding"),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(SHA256_HEX_BYTES);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn finalize_sha256(digest: Sha256) -> [u8; SHA256_BYTES] {
    digest.finalize().into()
}

fn checked_i64_domain(value: u64, field_name: &str) -> io::Result<()> {
    if value > i64::MAX as u64 {
        return Err(invalid_data(format!(
            "UHC retained {field_name} exceeds the signed 64-bit contract"
        )));
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct UHCRetainRequest {
    pub source_path: PathBuf,
    pub output_root: PathBuf,
    pub expected_sha256: String,
    pub expected_byte_count: u64,
    pub range_count: usize,
}

impl UHCRetainRequest {
    fn validate(&self) -> io::Result<[u8; SHA256_BYTES]> {
        let decoded_sha256 = parse_sha256_hex(&self.expected_sha256)?;
        if self.expected_byte_count == 0 || self.expected_byte_count > i64::MAX as u64 {
            return Err(invalid_input(
                "expected UHC retained byte count must be in 1..=i64::MAX",
            ));
        }
        if !(MIN_RANGE_COUNT..=MAX_RANGE_COUNT).contains(&self.range_count) {
            return Err(invalid_input(format!(
                "UHC retained range count must be in {MIN_RANGE_COUNT}..={MAX_RANGE_COUNT}"
            )));
        }
        Ok(decoded_sha256)
    }
}

#[derive(Clone, Debug, DeriveDeserialize, Eq, PartialEq, Serialize)]
pub struct UHCRawArtifactManifest {
    pub file_name: String,
    pub sha256: String,
    pub byte_count: u64,
    pub record_count: u64,
}

#[derive(Clone, Debug, DeriveDeserialize, Eq, PartialEq, Serialize)]
pub struct UHCRawRangeManifest {
    pub range_ordinal: u64,
    pub raw_byte_start: u64,
    pub raw_byte_end: u64,
    pub raw_byte_count: u64,
    pub raw_sha256: String,
    pub record_start: u64,
    pub record_end: u64,
    pub record_count: u64,
    pub canonical_sha256: String,
    pub canonical_byte_count: u64,
}

#[derive(Clone, Debug, DeriveDeserialize, Eq, PartialEq, Serialize)]
pub struct UHCRetainedManifest {
    pub contract_id: String,
    pub contract_version: u64,
    pub canonicalization_id: String,
    pub producer_build_id: String,
    pub raw_artifact: UHCRawArtifactManifest,
    pub range_count: u64,
    pub ranges: Vec<UHCRawRangeManifest>,
    pub range_set_sha256: String,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct UHCRetainTimings {
    pub total: f64,
    pub raw_read_hash_copy: f64,
    pub frame_offset_index: f64,
    pub concurrent_range_worker_max: f64,
    pub raw_fsync: f64,
    pub range_verification: f64,
    pub raw_reverification: f64,
    pub raw_publish: f64,
    pub manifest_verify_or_publish: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct UHCRetainSummary {
    pub record_kind: String,
    pub contract_id: String,
    pub contract_version: u64,
    pub canonicalization_id: String,
    pub producer_build_id: String,
    pub verifier_build_id: String,
    pub raw_artifact_path: String,
    pub raw_artifact_sha256: String,
    pub raw_artifact_byte_count: u64,
    pub record_count: u64,
    pub range_count: u64,
    pub manifest_path: String,
    pub manifest_sha256: String,
    pub manifest_byte_count: u64,
    pub raw_reused: bool,
    pub manifest_reused: bool,
    pub timings_seconds: UHCRetainTimings,
}

