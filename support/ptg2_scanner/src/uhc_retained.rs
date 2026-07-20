//! Strict, immutable admission of retained UHC JSON arrays as logical byte ranges.
//!
//! The native helper deliberately does not acquire the higher-level retained-source
//! lock. The Python admission transaction owns that lock across this subprocess and
//! its database commit. Atomic no-clobber publication still makes this layer safe
//! when it is invoked concurrently without the outer lock.

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FileIdentity {
    device: u64,
    inode: u64,
    byte_count: u64,
    mode: u32,
    link_count: u64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
}

impl FileIdentity {
    fn from_metadata(metadata: &Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            byte_count: metadata.len(),
            mode: metadata.mode(),
            link_count: metadata.nlink(),
            modified_seconds: metadata.mtime(),
            modified_nanoseconds: metadata.mtime_nsec(),
            changed_seconds: metadata.ctime(),
            changed_nanoseconds: metadata.ctime_nsec(),
        }
    }

    fn from_file(file: &File) -> io::Result<Self> {
        Ok(Self::from_metadata(&file.metadata()?))
    }
}

fn require_stable_regular_file(
    file: &File,
    identity: FileIdentity,
    expected_byte_count: u64,
    label: &str,
) -> io::Result<()> {
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(invalid_data(format!(
            "UHC retained {label} is not a regular file"
        )));
    }
    let observed = FileIdentity::from_metadata(&metadata);
    if observed != identity || observed.byte_count != expected_byte_count {
        return Err(invalid_data(format!(
            "UHC retained {label} changed while it was being verified"
        )));
    }
    Ok(())
}

fn open_regular_nofollow(path: &Path, label: &str) -> io::Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(path)?;
    if !file.metadata()?.is_file() {
        return Err(invalid_data(format!(
            "UHC retained {label} is not a regular file"
        )));
    }
    Ok(file)
}

fn c_string(value: &str, label: &str) -> io::Result<CString> {
    CString::new(value).map_err(|_| invalid_input(format!("{label} contains a NUL byte")))
}

struct RootDirectory {
    supplied_path: PathBuf,
    path: PathBuf,
    directory: File,
    identity: FileIdentity,
}

impl RootDirectory {
    fn open(path: &Path) -> io::Result<Arc<Self>> {
        let supplied_path = if path.is_absolute() {
            path.to_owned()
        } else {
            env::current_dir()?.join(path)
        };
        let directory = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW)
            .open(&supplied_path)?;
        let metadata = directory.metadata()?;
        if !metadata.is_dir() {
            return Err(invalid_input("UHC retained output root is not a directory"));
        }
        let canonical_path = supplied_path.canonicalize()?;
        let root = Arc::new(Self {
            supplied_path,
            path: canonical_path,
            directory,
            identity: FileIdentity::from_metadata(&metadata),
        });
        root.verify_path_identity()?;
        Ok(root)
    }

    fn descriptor(&self) -> RawFd {
        self.directory.as_raw_fd()
    }

    fn verify_path_identity(&self) -> io::Result<()> {
        let descriptor_metadata = self.directory.metadata()?;
        let descriptor_identity = FileIdentity::from_metadata(&descriptor_metadata);
        if !descriptor_metadata.is_dir() || !same_inode(descriptor_identity, self.identity) {
            return Err(invalid_data(
                "UHC retained output root identity changed during admission",
            ));
        }
        for candidate in [&self.supplied_path, &self.path] {
            let metadata = fs::symlink_metadata(candidate)?;
            let observed = FileIdentity::from_metadata(&metadata);
            if metadata.file_type().is_symlink()
                || !metadata.is_dir()
                || !same_inode(observed, self.identity)
            {
                return Err(invalid_data(
                    "UHC retained output root path changed during admission",
                ));
            }
        }
        Ok(())
    }

    fn output_path(&self, name: &str) -> PathBuf {
        self.path.join(name)
    }

    fn open_existing_regular(&self, name: &str) -> io::Result<Option<File>> {
        let encoded_name = c_string(name, "UHC retained file name")?;
        for attempt in 0..PUBLICATION_LINK_RETRIES {
            let descriptor = unsafe {
                libc::openat(
                    self.descriptor(),
                    encoded_name.as_ptr(),
                    libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                )
            };
            if descriptor < 0 {
                let error = io::Error::last_os_error();
                if error.raw_os_error() == Some(libc::ENOENT) {
                    return Ok(None);
                }
                return Err(error);
            }
            let file = unsafe { File::from_raw_fd(descriptor) };
            let metadata = file.metadata()?;
            if !metadata.is_file() {
                return Err(invalid_data(format!(
                    "UHC retained existing {name} is not a regular file"
                )));
            }
            if metadata.nlink() == 1 && metadata.mode() & 0o022 == 0 {
                return Ok(Some(file));
            }
            if metadata.nlink() == 2
                && metadata.mode() & 0o022 == 0
                && attempt + 1 < PUBLICATION_LINK_RETRIES
            {
                drop(file);
                thread::sleep(Duration::from_micros(50));
                continue;
            }
            return Err(invalid_data(format!(
                "UHC retained existing {name} must have one link and no group/other write bits"
            )));
        }
        unreachable!("publication-link retry loop always returns")
    }

    fn create_temporary(self: &Arc<Self>, label: &str) -> io::Result<RootTemporaryFile> {
        for _attempt in 0..64 {
            let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let name = format!(
                ".uhc-retain-{label}-{}-{timestamp}-{sequence}.partial",
                std::process::id()
            );
            let encoded_name = c_string(&name, "UHC retained temporary file name")?;
            let descriptor = unsafe {
                libc::openat(
                    self.descriptor(),
                    encoded_name.as_ptr(),
                    libc::O_RDWR
                        | libc::O_CREAT
                        | libc::O_EXCL
                        | libc::O_CLOEXEC
                        | libc::O_NOFOLLOW,
                    0o600,
                )
            };
            if descriptor >= 0 {
                return Ok(RootTemporaryFile {
                    root: Arc::clone(self),
                    name,
                    file: unsafe { File::from_raw_fd(descriptor) },
                    cleanup_required: true,
                });
            }
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::EEXIST) {
                return Err(error);
            }
        }
        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "unable to allocate a unique UHC retained temporary file",
        ))
    }

    fn unlink(&self, name: &str) -> io::Result<()> {
        let encoded_name = c_string(name, "UHC retained temporary file name")?;
        let result = unsafe { libc::unlinkat(self.descriptor(), encoded_name.as_ptr(), 0) };
        if result == 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ENOENT) {
            return Ok(());
        }
        Err(error)
    }

    fn sync(&self) -> io::Result<()> {
        self.directory.sync_all()
    }
}

struct RootTemporaryFile {
    root: Arc<RootDirectory>,
    name: String,
    file: File,
    cleanup_required: bool,
}

impl RootTemporaryFile {
    fn file(&self) -> &File {
        &self.file
    }

    fn file_mut(&mut self) -> &mut File {
        &mut self.file
    }

    fn publish_noclobber(&mut self, final_name: &str) -> io::Result<bool> {
        self.root.verify_path_identity()?;
        let temporary_name = c_string(&self.name, "UHC retained temporary file name")?;
        let encoded_final = c_string(final_name, "UHC retained final file name")?;
        let linked = unsafe {
            libc::linkat(
                self.root.descriptor(),
                temporary_name.as_ptr(),
                self.root.descriptor(),
                encoded_final.as_ptr(),
                0,
            )
        };
        if linked != 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::EEXIST) {
                return Ok(false);
            }
            return Err(error);
        }
        self.root.unlink(&self.name)?;
        self.cleanup_required = false;
        Ok(true)
    }
}

impl Drop for RootTemporaryFile {
    fn drop(&mut self) {
        if self.cleanup_required {
            let _ = self.root.unlink(&self.name);
        }
    }
}

struct StrictValue;

struct StrictValueVisitor;

impl<'de> Visitor<'de> for StrictValueVisitor {
    type Value = StrictValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a strict JSON value")
    }

    fn visit_bool<E>(self, _value: bool) -> Result<Self::Value, E> {
        Ok(StrictValue)
    }

    fn visit_i64<E>(self, _value: i64) -> Result<Self::Value, E> {
        Ok(StrictValue)
    }

    fn visit_u64<E>(self, _value: u64) -> Result<Self::Value, E> {
        Ok(StrictValue)
    }

    fn visit_f64<E>(self, _value: f64) -> Result<Self::Value, E> {
        Ok(StrictValue)
    }

    fn visit_str<E>(self, _value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(StrictValue)
    }

    fn visit_borrowed_str<E>(self, _value: &'de str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(StrictValue)
    }

    fn visit_string<E>(self, _value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(StrictValue)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(StrictValue)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while sequence.next_element::<StrictValue>()?.is_some() {}
        Ok(StrictValue)
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut keys = HashSet::with_capacity(map.size_hint().unwrap_or(8).min(128));
        while let Some(key) = map.next_key::<String>()? {
            if !keys.insert(key.clone()) {
                return Err(de::Error::custom(format!(
                    "duplicate JSON object key: {key:?}"
                )));
            }
            map.next_value::<StrictValue>()?;
        }
        Ok(StrictValue)
    }
}

impl<'de> Deserialize<'de> for StrictValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(StrictValueVisitor)
    }
}

struct StrictObject;

struct StrictObjectVisitor;

impl<'de> Visitor<'de> for StrictObjectVisitor {
    type Value = StrictObject;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut keys = HashSet::with_capacity(map.size_hint().unwrap_or(16).min(128));
        while let Some(key) = map.next_key::<String>()? {
            if !keys.insert(key.clone()) {
                return Err(de::Error::custom(format!(
                    "duplicate JSON object key: {key:?}"
                )));
            }
            map.next_value::<StrictValue>()?;
        }
        Ok(StrictObject)
    }
}

impl<'de> Deserialize<'de> for StrictObject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(StrictObjectVisitor)
    }
}

fn validate_strict_json_object(bytes: &[u8]) -> io::Result<()> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    StrictObject::deserialize(&mut deserializer)
        .map_err(|error| invalid_data(format!("retained UHC record is invalid JSON: {error}")))?;
    deserializer
        .end()
        .map_err(|error| invalid_data(format!("retained UHC record is invalid JSON: {error}")))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FramingMode {
    Array,
    Fragment,
}

struct JsonObjectFramer {
    mode: FramingMode,
    capture_records: bool,
    max_record_bytes: usize,
    started: bool,
    ended: bool,
    has_records: bool,
    post_comma: bool,
    expecting_value: bool,
    in_record: bool,
    depth: i64,
    in_string: bool,
    escaped: bool,
    record_start: u64,
    record_size: usize,
    record: Vec<u8>,
    absolute_offset: u64,
}

impl JsonObjectFramer {
    fn array(capture_records: bool, max_record_bytes: usize) -> Self {
        Self {
            mode: FramingMode::Array,
            capture_records,
            max_record_bytes,
            started: false,
            ended: false,
            has_records: false,
            post_comma: false,
            expecting_value: true,
            in_record: false,
            depth: 0,
            in_string: false,
            escaped: false,
            record_start: 0,
            record_size: 0,
            record: Vec::new(),
            absolute_offset: 0,
        }
    }

    fn fragment(base_offset: u64, max_record_bytes: usize) -> Self {
        Self {
            mode: FramingMode::Fragment,
            capture_records: true,
            max_record_bytes,
            started: true,
            ended: false,
            has_records: false,
            post_comma: false,
            expecting_value: true,
            in_record: false,
            depth: 0,
            in_string: false,
            escaped: false,
            record_start: base_offset,
            record_size: 0,
            record: Vec::new(),
            absolute_offset: base_offset,
        }
    }

    fn is_json_whitespace(byte: u8) -> bool {
        matches!(byte, b' ' | b'\t' | b'\r' | b'\n')
    }

    fn start_record(&mut self, byte: u8) -> io::Result<()> {
        if byte != b'{' {
            return Err(invalid_data(
                "retained UHC array entries must be JSON objects",
            ));
        }
        self.in_record = true;
        self.depth = 1;
        self.in_string = false;
        self.escaped = false;
        self.post_comma = false;
        self.record_start = self.absolute_offset - 1;
        self.record_size = 1;
        if self.capture_records {
            self.record.clear();
            self.record.push(byte);
        }
        Ok(())
    }

    fn append_record_byte(&mut self, byte: u8) -> io::Result<()> {
        self.record_size = some_or_invalid_data(
            self.record_size.checked_add(1),
            "retained UHC record byte count overflowed",
        )?;
        if self.record_size > self.max_record_bytes {
            return Err(invalid_data(format!(
                "retained UHC record exceeds the {} byte limit",
                self.max_record_bytes
            )));
        }
        if self.capture_records {
            self.record.push(byte);
        }
        Ok(())
    }

    fn feed<F>(&mut self, chunk: &[u8], mut accept: F) -> io::Result<()>
    where
        F: FnMut(&[u8], u64, u64) -> io::Result<()>,
    {
        for &byte in chunk {
            self.absolute_offset = some_or_invalid_data(
                self.absolute_offset.checked_add(1),
                "retained UHC source offset overflowed",
            )?;
            if self.ended {
                if !Self::is_json_whitespace(byte) {
                    return Err(invalid_data("retained UHC JSON has trailing content"));
                }
                continue;
            }
            if !self.started {
                if Self::is_json_whitespace(byte) {
                    continue;
                }
                if byte != b'[' {
                    return Err(invalid_data("retained UHC artifact must be a JSON array"));
                }
                self.started = true;
                continue;
            }
            if !self.in_record {
                if Self::is_json_whitespace(byte) {
                    continue;
                }
                if self.expecting_value {
                    if byte == b']'
                        && self.mode == FramingMode::Array
                        && !self.has_records
                        && !self.post_comma
                    {
                        self.ended = true;
                        continue;
                    }
                    self.start_record(byte)?;
                    continue;
                }
                if byte == b',' {
                    self.expecting_value = true;
                    self.post_comma = true;
                    continue;
                }
                if byte == b']' && self.mode == FramingMode::Array {
                    self.ended = true;
                    continue;
                }
                return Err(invalid_data(
                    "retained UHC JSON has an invalid array separator",
                ));
            }

            self.append_record_byte(byte)?;
            if self.in_string {
                if self.escaped {
                    self.escaped = false;
                } else if byte == b'\\' {
                    self.escaped = true;
                } else if byte == b'"' {
                    self.in_string = false;
                }
                continue;
            }
            match byte {
                b'"' => self.in_string = true,
                b'{' | b'[' => {
                    self.depth = some_or_invalid_data(
                        self.depth.checked_add(1),
                        "retained UHC JSON nesting overflowed",
                    )?;
                }
                b'}' | b']' => {
                    self.depth -= 1;
                    if self.depth < 0 {
                        return Err(invalid_data("retained UHC JSON frame is invalid"));
                    }
                    if self.depth == 0 {
                        if byte != b'}' {
                            return Err(invalid_data(
                                "retained UHC array entries must be JSON objects",
                            ));
                        }
                        let record_end = self.absolute_offset;
                        accept(&self.record, self.record_start, record_end)?;
                        self.in_record = false;
                        self.expecting_value = false;
                        self.has_records = true;
                        self.record_size = 0;
                        if self.capture_records {
                            self.record.clear();
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn finish(&self) -> io::Result<()> {
        let complete = match self.mode {
            FramingMode::Array => self.started && self.ended && !self.in_record,
            FramingMode::Fragment => {
                self.started
                    && !self.ended
                    && self.has_records
                    && !self.in_record
                    && !self.expecting_value
                    && !self.post_comma
            }
        };
        if !complete {
            return Err(invalid_data("retained UHC JSON frame is incomplete"));
        }
        Ok(())
    }
}

#[derive(Debug)]
struct RawScanResult {
    record_count: u64,
    ranges: Vec<UHCRawRangeManifest>,
    read_hash_copy: Duration,
    frame_offset_index: Duration,
    concurrent_range_worker_max: Duration,
    range_verification_tail: Duration,
    raw_fsync: Duration,
}

struct StreamingRangeBoundary {
    range_ordinal: u64,
    raw_byte_start: u64,
    raw_byte_end: u64,
    record_start: u64,
    record_end: u64,
    record_count: u64,
}

impl StreamingRangeBoundary {
    fn new(range_ordinal: u64, record_start: u64, raw_byte_start: u64) -> Self {
        Self {
            range_ordinal,
            raw_byte_start,
            raw_byte_end: raw_byte_start,
            record_start,
            record_end: record_start,
            record_count: 0,
        }
    }

    fn add_record(&mut self, record_end: u64) -> io::Result<()> {
        self.raw_byte_end = record_end;
        self.record_end = some_or_invalid_data(
            self.record_end.checked_add(1),
            "retained UHC range record count overflowed",
        )?;
        self.record_count = some_or_invalid_data(
            self.record_count.checked_add(1),
            "retained UHC range record count overflowed",
        )?;
        Ok(())
    }

    fn finish(self) -> io::Result<RawRangeBoundary> {
        if self.raw_byte_start >= self.raw_byte_end
            || self.record_start >= self.record_end
            || self.record_end - self.record_start != self.record_count
        {
            return Err(invalid_data(
                "retained UHC streaming range boundary is invalid",
            ));
        }
        Ok(RawRangeBoundary {
            range_ordinal: self.range_ordinal,
            raw_byte_start: self.raw_byte_start,
            raw_byte_end: self.raw_byte_end,
            record_start: self.record_start,
            record_end: self.record_end,
        })
    }
}

enum RangeWorkerMessage {
    Records(Vec<Vec<u8>>),
    Finish(RawRangeBoundary),
}

#[derive(Default)]
struct PendingRecordBatch {
    records: Vec<Vec<u8>>,
    byte_count: usize,
}

fn run_range_worker(
    input: Arc<File>,
    messages: Receiver<RangeWorkerMessage>,
) -> io::Result<Option<UHCRawRangeManifest>> {
    let mut canonical_digest = Sha256::new();
    let mut canonical_byte_count = 0u64;
    let mut observed_record_count = 0u64;
    let boundary = loop {
        match messages.recv() {
            Ok(RangeWorkerMessage::Records(records)) => {
                for record in records {
                    validate_strict_json_object(&record)?;
                    let mut record_byte_count = 1u64;
                    let mut segment_start = 0usize;
                    for (index, byte) in record.iter().copied().enumerate() {
                        if matches!(byte, b'\r' | b'\n') {
                            canonical_digest.update(&record[segment_start..index]);
                            segment_start = index + 1;
                        } else {
                            record_byte_count = some_or_invalid_data(
                                record_byte_count.checked_add(1),
                                "retained UHC canonical range byte count overflowed",
                            )?;
                        }
                    }
                    canonical_digest.update(&record[segment_start..]);
                    canonical_digest.update(b"\n");
                    canonical_byte_count = some_or_invalid_data(
                        canonical_byte_count.checked_add(record_byte_count),
                        "retained UHC canonical range byte count overflowed",
                    )?;
                    observed_record_count = some_or_invalid_data(
                        observed_record_count.checked_add(1),
                        "retained UHC range record count overflowed",
                    )?;
                }
            }
            Ok(RangeWorkerMessage::Finish(boundary)) => break boundary,
            Err(_) => return Ok(None),
        }
    };
    let expected_record_count = some_or_invalid_data(
        boundary.record_end.checked_sub(boundary.record_start),
        "retained UHC range record count underflowed",
    )?;
    if observed_record_count != expected_record_count || canonical_byte_count == 0 {
        return Err(invalid_data(
            "retained UHC concurrent range record proof is incomplete",
        ));
    }
    let (raw_sha256, raw_byte_count) = hash_raw_range(&input, boundary)?;
    for (field_name, value) in [
        ("range ordinal", boundary.range_ordinal),
        ("range raw byte start", boundary.raw_byte_start),
        ("range raw byte end", boundary.raw_byte_end),
        ("range raw byte count", raw_byte_count),
        ("range record start", boundary.record_start),
        ("range record end", boundary.record_end),
        ("range record count", observed_record_count),
        ("range canonical byte count", canonical_byte_count),
    ] {
        checked_i64_domain(value, field_name)?;
    }
    Ok(Some(UHCRawRangeManifest {
        range_ordinal: boundary.range_ordinal,
        raw_byte_start: boundary.raw_byte_start,
        raw_byte_end: boundary.raw_byte_end,
        raw_byte_count,
        raw_sha256,
        record_start: boundary.record_start,
        record_end: boundary.record_end,
        record_count: observed_record_count,
        canonical_sha256: sha256_hex(&finalize_sha256(canonical_digest)),
        canonical_byte_count,
    }))
}

struct ConcurrentRangeWorkers {
    senders: Vec<SyncSender<RangeWorkerMessage>>,
    pending: Vec<PendingRecordBatch>,
    handles: Vec<thread::JoinHandle<(io::Result<Option<UHCRawRangeManifest>>, Duration)>>,
}

impl ConcurrentRangeWorkers {
    fn spawn(input: Arc<File>, range_count: usize) -> io::Result<Self> {
        let mut workers = Self {
            senders: Vec::with_capacity(range_count),
            pending: (0..range_count)
                .map(|_| PendingRecordBatch::default())
                .collect(),
            handles: Vec::with_capacity(range_count),
        };
        for range_ordinal in 0..range_count {
            let (sender, receiver) = sync_channel(RANGE_BATCH_QUEUE_DEPTH);
            let worker_input = Arc::clone(&input);
            let handle = thread::Builder::new()
                .name(format!("uhc-range-{range_ordinal}"))
                .spawn(move || {
                    let started = Instant::now();
                    let result = run_range_worker(worker_input, receiver);
                    (result, started.elapsed())
                })?;
            workers.senders.push(sender);
            workers.handles.push(handle);
        }
        Ok(workers)
    }

    fn send_record(&mut self, range_ordinal: usize, record: &[u8]) -> io::Result<()> {
        let batch = some_or_invalid_data(
            self.pending.get_mut(range_ordinal),
            "retained UHC range worker is unavailable",
        )?;
        batch.byte_count = some_or_invalid_data(
            batch.byte_count.checked_add(record.len()),
            "retained UHC range worker batch overflowed",
        )?;
        batch.records.push(record.to_vec());
        if batch.byte_count >= RANGE_RECORD_BATCH_BYTES {
            self.flush(range_ordinal)?;
        }
        Ok(())
    }

    fn flush(&mut self, range_ordinal: usize) -> io::Result<()> {
        let batch = some_or_invalid_data(
            self.pending.get_mut(range_ordinal),
            "retained UHC range worker is unavailable",
        )?;
        if batch.records.is_empty() {
            return Ok(());
        }
        let records = std::mem::take(&mut batch.records);
        batch.byte_count = 0;
        self.senders[range_ordinal]
            .send(RangeWorkerMessage::Records(records))
            .map_err(|_| invalid_data("retained UHC range worker stopped unexpectedly"))
    }

    fn finish_range(&mut self, boundary: RawRangeBoundary) -> io::Result<()> {
        let range_ordinal = boundary.range_ordinal as usize;
        self.flush(range_ordinal)?;
        self.senders[range_ordinal]
            .send(RangeWorkerMessage::Finish(boundary))
            .map_err(|_| invalid_data("retained UHC range worker stopped unexpectedly"))
    }

    fn finish(mut self) -> io::Result<(Vec<UHCRawRangeManifest>, Duration)> {
        self.senders.clear();
        let mut ranges = Vec::with_capacity(self.handles.len());
        let mut worker_max = Duration::ZERO;
        for handle in self.handles.drain(..) {
            let (result, elapsed) = handle
                .join()
                .map_err(|_| io::Error::other("UHC concurrent range worker panicked"))?;
            worker_max = worker_max.max(elapsed);
            if let Some(range) = result? {
                ranges.push(range);
            }
        }
        ranges.sort_by_key(|range| range.range_ordinal);
        Ok((ranges, worker_max))
    }
}

impl Drop for ConcurrentRangeWorkers {
    fn drop(&mut self) {
        self.senders.clear();
        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct RawRangeBoundary {
    range_ordinal: u64,
    raw_byte_start: u64,
    raw_byte_end: u64,
    record_start: u64,
    record_end: u64,
}

fn scan_raw_and_build_ranges(
    input: &File,
    range_input: Arc<File>,
    raw_output: Option<&mut File>,
    offsets_output: &mut File,
    expected_sha256: &[u8; SHA256_BYTES],
    expected_byte_count: u64,
    range_count: usize,
) -> io::Result<RawScanResult> {
    let mut raw_output = raw_output;
    let mut offset_writer = BufWriter::with_capacity(READ_BUFFER_BYTES, offsets_output);
    let mut raw_digest = Sha256::new();
    let concurrent_ranges = range_count <= MAX_RANGE_WORKERS;
    let mut framer = JsonObjectFramer::array(concurrent_ranges, MAX_PROVIDER_RECORD_BYTES);
    let mut buffer = vec![0u8; READ_BUFFER_BYTES];
    let mut absolute_offset = 0u64;
    let mut record_count = 0u64;
    let mut read_hash_copy = Duration::ZERO;
    let mut frame_offset_index = Duration::ZERO;
    let mut active_range: Option<StreamingRangeBoundary> = None;
    let mut boundary_count = 0usize;
    let mut range_workers = if concurrent_ranges {
        Some(ConcurrentRangeWorkers::spawn(range_input, range_count)?)
    } else {
        None
    };

    while absolute_offset < expected_byte_count {
        let remaining =
            usize::try_from((expected_byte_count - absolute_offset).min(READ_BUFFER_BYTES as u64))
                .map_err(|_| invalid_data("UHC retained raw read size overflowed"))?;
        let started = Instant::now();
        let bytes_read = input.read_at(&mut buffer[..remaining], absolute_offset)?;
        if bytes_read == 0 {
            return Err(invalid_data(
                "UHC retained raw artifact ended before its expected byte count",
            ));
        }
        let chunk = &buffer[..bytes_read];
        raw_digest.update(chunk);
        if let Some(output) = raw_output.as_deref_mut() {
            output.write_all(chunk)?;
        }
        read_hash_copy += started.elapsed();

        let frame_started = Instant::now();
        framer.feed(chunk, |record, record_start, record_end| {
            if record_count >= MAX_RECORD_COUNT {
                return Err(invalid_data(format!(
                    "retained UHC artifact exceeds the {MAX_RECORD_COUNT} record limit"
                )));
            }
            offset_writer.write_all(&record_start.to_be_bytes())?;
            offset_writer.write_all(&record_end.to_be_bytes())?;
            if let Some(workers) = range_workers.as_mut() {
                workers.send_record(boundary_count, record)?;
                let range = active_range.get_or_insert_with(|| {
                    StreamingRangeBoundary::new(boundary_count as u64, record_count, record_start)
                });
                range.add_record(record_end)?;
            }
            record_count += 1;
            if concurrent_ranges
                && boundary_count + 1 < range_count
                && record_end
                    >= ceil_partition_boundary(
                        expected_byte_count,
                        boundary_count + 1,
                        range_count,
                    )?
            {
                let completed = some_or_invalid_data(
                    active_range.take(),
                    "retained UHC range state disappeared",
                )?;
                some_or_invalid_data(
                    range_workers.as_mut(),
                    "retained UHC range workers disappeared",
                )?
                .finish_range(completed.finish()?)?;
                boundary_count += 1;
            }
            Ok(())
        })?;
        frame_offset_index += frame_started.elapsed();
        absolute_offset = some_or_invalid_data(
            absolute_offset.checked_add(bytes_read as u64),
            "UHC retained raw byte count overflowed",
        )?;
    }
    let mut extra = [0u8; 1];
    if input.read_at(&mut extra, expected_byte_count)? != 0 {
        return Err(invalid_data(
            "UHC retained raw artifact exceeds its expected byte count",
        ));
    }
    framer.finish()?;
    if let Some(range) = active_range.take() {
        some_or_invalid_data(
            range_workers.as_mut(),
            "retained UHC range workers disappeared",
        )?
        .finish_range(range.finish()?)?;
        boundary_count += 1;
    }
    offset_writer.flush()?;
    if finalize_sha256(raw_digest) != *expected_sha256 {
        return Err(invalid_data(
            "UHC retained raw artifact SHA-256 does not match its expected identity",
        ));
    }
    if record_count == 0 {
        return Err(invalid_data("retained UHC artifact contains no records"));
    }
    checked_i64_domain(record_count, "record count")?;
    let raw_sync = if let Some(output) = raw_output.as_deref() {
        let sync_file = output.try_clone()?;
        Some(thread::spawn(move || {
            let started = Instant::now();
            let result = sync_file.sync_data();
            (result, started.elapsed())
        }))
    } else {
        None
    };
    let range_tail_started = Instant::now();
    let (ranges, concurrent_range_worker_max) = if let Some(workers) = range_workers {
        workers.finish()?
    } else {
        (Vec::new(), Duration::ZERO)
    };
    let range_verification_tail = range_tail_started.elapsed();
    let raw_fsync = if let Some(handle) = raw_sync {
        let (result, elapsed) = handle
            .join()
            .map_err(|_| io::Error::other("UHC retained raw fsync worker panicked"))?;
        result?;
        elapsed
    } else {
        Duration::ZERO
    };
    if ranges.len() != boundary_count {
        return Err(invalid_data(
            "retained UHC concurrent range proof count is incomplete",
        ));
    }
    Ok(RawScanResult {
        record_count,
        ranges,
        read_hash_copy,
        frame_offset_index,
        concurrent_range_worker_max,
        range_verification_tail,
        raw_fsync,
    })
}

fn read_u64_at(file: &File, offset: u64) -> io::Result<u64> {
    let mut encoded = [0u8; 8];
    let mut consumed = 0usize;
    while consumed < encoded.len() {
        let bytes_read = file.read_at(&mut encoded[consumed..], offset + consumed as u64)?;
        if bytes_read == 0 {
            return Err(invalid_data(
                "retained UHC record-offset spool ended unexpectedly",
            ));
        }
        consumed += bytes_read;
    }
    Ok(u64::from_be_bytes(encoded))
}

fn ceil_partition_boundary(total: u64, ordinal: usize, count: usize) -> io::Result<u64> {
    let numerator = some_or_invalid_data(
        u128::from(total)
            .checked_mul(ordinal as u128)
            .and_then(|value| value.checked_add(count as u128 - 1)),
        "retained UHC range boundary overflowed",
    )?;
    u64::try_from(numerator / count as u128)
        .map_err(|_| invalid_data("retained UHC range boundary exceeds u64"))
}

fn build_range_boundaries(
    offsets_file: &File,
    record_count: u64,
    range_count: usize,
) -> io::Result<Vec<RawRangeBoundary>> {
    if record_count < range_count as u64 {
        return Err(invalid_data(format!(
            "retained UHC artifact has {record_count} records for {range_count} ranges"
        )));
    }
    let expected_offset_bytes = some_or_invalid_data(
        record_count.checked_mul(16),
        "retained UHC offset spool size overflowed",
    )?;
    if offsets_file.metadata()?.len() != expected_offset_bytes {
        return Err(invalid_data(
            "retained UHC record-offset spool has an invalid byte count",
        ));
    }
    let mut boundaries = Vec::with_capacity(range_count);
    for ordinal in 0..range_count {
        let record_start = ceil_partition_boundary(record_count, ordinal, range_count)?;
        let record_end = ceil_partition_boundary(record_count, ordinal + 1, range_count)?;
        if record_start >= record_end {
            return Err(invalid_data("retained UHC logical range would be empty"));
        }
        let first_offset = some_or_invalid_data(
            record_start.checked_mul(16),
            "retained UHC range offset overflowed",
        )?;
        let last_offset = some_or_invalid_data(
            record_end
                .checked_sub(1)
                .and_then(|value| value.checked_mul(16))
                .and_then(|value| value.checked_add(8)),
            "retained UHC range offset overflowed",
        )?;
        let raw_byte_start = read_u64_at(offsets_file, first_offset)?;
        let raw_byte_end = read_u64_at(offsets_file, last_offset)?;
        if raw_byte_start >= raw_byte_end {
            return Err(invalid_data(
                "retained UHC logical range has invalid byte boundaries",
            ));
        }
        boundaries.push(RawRangeBoundary {
            range_ordinal: ordinal as u64,
            raw_byte_start,
            raw_byte_end,
            record_start,
            record_end,
        });
    }
    for pair in boundaries.windows(2) {
        if pair[0].record_end != pair[1].record_start
            || pair[0].raw_byte_end >= pair[1].raw_byte_start
        {
            return Err(invalid_data(
                "retained UHC logical ranges are not ordered and disjoint",
            ));
        }
    }
    Ok(boundaries)
}

fn hash_raw_range(input: &File, boundary: RawRangeBoundary) -> io::Result<(String, u64)> {
    let raw_byte_count = some_or_invalid_data(
        boundary.raw_byte_end.checked_sub(boundary.raw_byte_start),
        "retained UHC raw range byte count underflowed",
    )?;
    if raw_byte_count == 0 {
        return Err(invalid_data("retained UHC raw range is empty"));
    }
    let mut digest = Sha256::new();
    let mut buffer = vec![0u8; READ_BUFFER_BYTES];
    let mut absolute_offset = boundary.raw_byte_start;
    while absolute_offset < boundary.raw_byte_end {
        let remaining = usize::try_from(
            (boundary.raw_byte_end - absolute_offset).min(READ_BUFFER_BYTES as u64),
        )
        .map_err(|_| invalid_data("retained UHC range read size overflowed"))?;
        let bytes_read = input.read_at(&mut buffer[..remaining], absolute_offset)?;
        if bytes_read == 0 {
            return Err(invalid_data(
                "retained UHC raw range ended before its declared boundary",
            ));
        }
        digest.update(&buffer[..bytes_read]);
        absolute_offset = some_or_invalid_data(
            absolute_offset.checked_add(bytes_read as u64),
            "retained UHC range offset overflowed",
        )?;
    }
    Ok((sha256_hex(&finalize_sha256(digest)), raw_byte_count))
}

fn verify_raw_range(input: &File, boundary: RawRangeBoundary) -> io::Result<UHCRawRangeManifest> {
    let raw_byte_count = some_or_invalid_data(
        boundary.raw_byte_end.checked_sub(boundary.raw_byte_start),
        "retained UHC raw range byte count underflowed",
    )?;
    let expected_record_count = some_or_invalid_data(
        boundary.record_end.checked_sub(boundary.record_start),
        "retained UHC range record count underflowed",
    )?;
    let mut raw_digest = Sha256::new();
    let mut canonical_digest = Sha256::new();
    let mut canonical_byte_count = 0u64;
    let mut observed_record_count = 0u64;
    let mut canonical = Vec::with_capacity(64 * 1024);
    let mut framer = JsonObjectFramer::fragment(boundary.raw_byte_start, MAX_PROVIDER_RECORD_BYTES);
    let mut buffer = vec![0u8; READ_BUFFER_BYTES];
    let mut absolute_offset = boundary.raw_byte_start;
    while absolute_offset < boundary.raw_byte_end {
        let remaining = usize::try_from(
            (boundary.raw_byte_end - absolute_offset).min(READ_BUFFER_BYTES as u64),
        )
        .map_err(|_| invalid_data("retained UHC range read size overflowed"))?;
        let bytes_read = input.read_at(&mut buffer[..remaining], absolute_offset)?;
        if bytes_read == 0 {
            return Err(invalid_data(
                "retained UHC raw range ended before its declared boundary",
            ));
        }
        let chunk = &buffer[..bytes_read];
        raw_digest.update(chunk);
        framer.feed(chunk, |record, _record_start, _record_end| {
            validate_strict_json_object(record)?;
            canonical.clear();
            canonical.extend(
                record
                    .iter()
                    .copied()
                    .filter(|byte| !matches!(*byte, b'\r' | b'\n')),
            );
            canonical.push(b'\n');
            canonical_digest.update(&canonical);
            canonical_byte_count = some_or_invalid_data(
                canonical_byte_count.checked_add(canonical.len() as u64),
                "retained UHC canonical range byte count overflowed",
            )?;
            observed_record_count = some_or_invalid_data(
                observed_record_count.checked_add(1),
                "retained UHC range record count overflowed",
            )?;
            Ok(())
        })?;
        absolute_offset = some_or_invalid_data(
            absolute_offset.checked_add(bytes_read as u64),
            "retained UHC range offset overflowed",
        )?;
    }
    framer.finish()?;
    if observed_record_count != expected_record_count {
        return Err(invalid_data(format!(
            "retained UHC range {} record count changed: {} != {}",
            boundary.range_ordinal, observed_record_count, expected_record_count
        )));
    }
    for (field_name, value) in [
        ("range ordinal", boundary.range_ordinal),
        ("range raw byte start", boundary.raw_byte_start),
        ("range raw byte end", boundary.raw_byte_end),
        ("range raw byte count", raw_byte_count),
        ("range record start", boundary.record_start),
        ("range record end", boundary.record_end),
        ("range record count", observed_record_count),
        ("range canonical byte count", canonical_byte_count),
    ] {
        checked_i64_domain(value, field_name)?;
    }
    Ok(UHCRawRangeManifest {
        range_ordinal: boundary.range_ordinal,
        raw_byte_start: boundary.raw_byte_start,
        raw_byte_end: boundary.raw_byte_end,
        raw_byte_count,
        raw_sha256: sha256_hex(&finalize_sha256(raw_digest)),
        record_start: boundary.record_start,
        record_end: boundary.record_end,
        record_count: observed_record_count,
        canonical_sha256: sha256_hex(&finalize_sha256(canonical_digest)),
        canonical_byte_count,
    })
}

fn verify_ranges_parallel(
    input: Arc<File>,
    boundaries: Vec<RawRangeBoundary>,
) -> io::Result<Vec<UHCRawRangeManifest>> {
    let worker_count = thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(4)
        .min(MAX_RANGE_WORKERS)
        .min(boundaries.len())
        .max(1);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(worker_count)
        .thread_name(|index| format!("uhc-retain-{index}"))
        .build()
        .map_err(|error| io::Error::other(error.to_string()))?;
    let results = pool.install(|| {
        boundaries
            .into_par_iter()
            .map(|boundary| verify_raw_range(&input, boundary))
            .collect::<Vec<_>>()
    });
    results.into_iter().collect()
}

fn verify_whole_file_sha256(
    file: &File,
    expected_identity: FileIdentity,
    expected_sha256: &[u8; SHA256_BYTES],
    expected_byte_count: u64,
    label: &str,
) -> io::Result<()> {
    require_stable_regular_file(file, expected_identity, expected_byte_count, label)?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0u8; READ_BUFFER_BYTES];
    let mut absolute_offset = 0u64;
    while absolute_offset < expected_byte_count {
        let remaining =
            usize::try_from((expected_byte_count - absolute_offset).min(READ_BUFFER_BYTES as u64))
                .map_err(|_| invalid_data("retained UHC verification read size overflowed"))?;
        let bytes_read = file.read_at(&mut buffer[..remaining], absolute_offset)?;
        if bytes_read == 0 {
            return Err(invalid_data(format!(
                "UHC retained {label} ended during SHA-256 verification"
            )));
        }
        digest.update(&buffer[..bytes_read]);
        absolute_offset += bytes_read as u64;
    }
    if finalize_sha256(digest) != *expected_sha256 {
        return Err(invalid_data(format!(
            "UHC retained {label} SHA-256 does not match"
        )));
    }
    require_stable_regular_file(file, expected_identity, expected_byte_count, label)
}

fn validate_range_sequence(
    ranges: &[UHCRawRangeManifest],
    expected_record_count: u64,
    expected_range_count: usize,
) -> io::Result<()> {
    if ranges.len() != expected_range_count {
        return Err(invalid_data(
            "retained UHC manifest range count does not match its range list",
        ));
    }
    let mut next_record = 0u64;
    let mut previous_raw_end = None;
    for (ordinal, range) in ranges.iter().enumerate() {
        if range.range_ordinal != ordinal as u64
            || range.record_start != next_record
            || range.record_end < range.record_start
            || range.record_end - range.record_start != range.record_count
            || range.record_count == 0
            || range.raw_byte_start >= range.raw_byte_end
            || range.raw_byte_end - range.raw_byte_start != range.raw_byte_count
            || range.raw_byte_count == 0
            || range.canonical_byte_count == 0
        {
            return Err(invalid_data(
                "retained UHC manifest contains an invalid logical range",
            ));
        }
        if previous_raw_end.is_some_and(|end| end >= range.raw_byte_start) {
            return Err(invalid_data(
                "retained UHC manifest raw ranges overlap or are unordered",
            ));
        }
        parse_sha256_hex(&range.raw_sha256)
            .map_err(|_| invalid_data("retained UHC range raw SHA-256 is invalid"))?;
        parse_sha256_hex(&range.canonical_sha256)
            .map_err(|_| invalid_data("retained UHC range canonical SHA-256 is invalid"))?;
        next_record = range.record_end;
        previous_raw_end = Some(range.raw_byte_end);
    }
    if next_record != expected_record_count {
        return Err(invalid_data(
            "retained UHC manifest ranges do not cover every record",
        ));
    }
    Ok(())
}

fn range_set_sha256(
    raw_sha256: &[u8; SHA256_BYTES],
    raw_byte_count: u64,
    record_count: u64,
    ranges: &[UHCRawRangeManifest],
) -> io::Result<String> {
    validate_range_sequence(ranges, record_count, ranges.len())?;
    let mut digest = Sha256::new();
    digest.update(UHC_RETAIN_CONTRACT_ID.as_bytes());
    digest.update(b"\0");
    digest.update(UHC_RETAIN_CONTRACT_VERSION.to_be_bytes());
    digest.update(UHC_RETAIN_CANONICALIZATION_ID.as_bytes());
    digest.update(b"\0");
    digest.update(raw_sha256);
    digest.update(raw_byte_count.to_be_bytes());
    digest.update(record_count.to_be_bytes());
    digest.update((ranges.len() as u64).to_be_bytes());
    for range in ranges {
        for value in [
            range.range_ordinal,
            range.raw_byte_start,
            range.raw_byte_end,
            range.raw_byte_count,
            range.record_start,
            range.record_end,
            range.record_count,
            range.canonical_byte_count,
        ] {
            digest.update(value.to_be_bytes());
        }
        digest.update(
            parse_sha256_hex(&range.raw_sha256)
                .map_err(|_| invalid_data("retained UHC range raw SHA-256 is invalid"))?,
        );
        digest.update(
            parse_sha256_hex(&range.canonical_sha256)
                .map_err(|_| invalid_data("retained UHC range canonical SHA-256 is invalid"))?,
        );
    }
    Ok(sha256_hex(&finalize_sha256(digest)))
}

fn encode_manifest(manifest: &UHCRetainedManifest) -> io::Result<Vec<u8>> {
    let mut encoded = serde_json::to_vec(manifest)
        .map_err(|error| invalid_data(format!("unable to encode UHC manifest: {error}")))?;
    encoded.push(b'\n');
    if encoded.len() as u64 > MAX_MANIFEST_BYTES {
        return Err(invalid_data("retained UHC manifest exceeds its byte limit"));
    }
    Ok(encoded)
}

fn read_bounded_stable_file(file: &File, maximum_bytes: u64, label: &str) -> io::Result<Vec<u8>> {
    let before = FileIdentity::from_file(file)?;
    if before.byte_count == 0 || before.byte_count > maximum_bytes {
        return Err(invalid_data(format!(
            "UHC retained {label} has an invalid byte count"
        )));
    }
    let byte_count = usize::try_from(before.byte_count)
        .map_err(|_| invalid_data(format!("UHC retained {label} is too large")))?;
    let mut payload = vec![0u8; byte_count];
    let mut consumed = 0usize;
    while consumed < payload.len() {
        let bytes_read = file.read_at(&mut payload[consumed..], consumed as u64)?;
        if bytes_read == 0 {
            return Err(invalid_data(format!(
                "UHC retained {label} ended unexpectedly"
            )));
        }
        consumed += bytes_read;
    }
    if FileIdentity::from_file(file)? != before {
        return Err(invalid_data(format!(
            "UHC retained {label} changed while it was read"
        )));
    }
    Ok(payload)
}

fn parse_strict_manifest(bytes: &[u8]) -> io::Result<UHCRetainedManifest> {
    validate_strict_json_object(bytes)
        .map_err(|error| invalid_data(format!("retained UHC manifest is invalid: {error}")))?;
    let manifest: UHCRetainedManifest = serde_json::from_slice(bytes)
        .map_err(|error| invalid_data(format!("retained UHC manifest is invalid: {error}")))?;
    validate_build_id(&manifest.producer_build_id)?;
    if encode_manifest(&manifest)? != bytes {
        return Err(invalid_data(
            "retained UHC manifest is not in its deterministic canonical encoding",
        ));
    }
    Ok(manifest)
}

fn validate_existing_manifest(
    manifest: &UHCRetainedManifest,
    expected_raw: &UHCRawArtifactManifest,
    expected_ranges: &[UHCRawRangeManifest],
    expected_range_set_sha256: &str,
) -> io::Result<()> {
    if manifest.contract_id != UHC_RETAIN_CONTRACT_ID
        || manifest.contract_version != UHC_RETAIN_CONTRACT_VERSION
        || manifest.canonicalization_id != UHC_RETAIN_CANONICALIZATION_ID
        || manifest.raw_artifact != *expected_raw
        || manifest.range_count != expected_ranges.len() as u64
        || manifest.ranges != expected_ranges
        || manifest.range_set_sha256 != expected_range_set_sha256
    {
        return Err(invalid_data(
            "existing retained UHC manifest does not match the rederived raw-range proof",
        ));
    }
    validate_range_sequence(
        &manifest.ranges,
        manifest.raw_artifact.record_count,
        manifest.range_count as usize,
    )?;
    let raw_sha = parse_sha256_hex(&manifest.raw_artifact.sha256)
        .map_err(|_| invalid_data("retained UHC manifest raw SHA-256 is invalid"))?;
    if range_set_sha256(
        &raw_sha,
        manifest.raw_artifact.byte_count,
        manifest.raw_artifact.record_count,
        &manifest.ranges,
    )? != manifest.range_set_sha256
    {
        return Err(invalid_data(
            "existing retained UHC manifest has an invalid range-set proof",
        ));
    }
    Ok(())
}

struct ManifestPublication {
    producer_build_id: String,
    encoded: Vec<u8>,
    reused: bool,
    final_identity: FileIdentity,
}

fn verify_existing_manifest_file(
    file: &File,
    expected_raw: &UHCRawArtifactManifest,
    expected_ranges: &[UHCRawRangeManifest],
    expected_range_set_sha256: &str,
) -> io::Result<ManifestPublication> {
    let encoded = read_bounded_stable_file(file, MAX_MANIFEST_BYTES, "manifest")?;
    let manifest = parse_strict_manifest(&encoded)?;
    validate_existing_manifest(
        &manifest,
        expected_raw,
        expected_ranges,
        expected_range_set_sha256,
    )?;
    Ok(ManifestPublication {
        producer_build_id: manifest.producer_build_id,
        encoded,
        reused: true,
        final_identity: FileIdentity::from_file(file)?,
    })
}

fn verify_existing_raw_file(
    file: &File,
    expected_sha256: &[u8; SHA256_BYTES],
    expected_byte_count: u64,
) -> io::Result<()> {
    let identity = FileIdentity::from_file(file)?;
    verify_whole_file_sha256(
        file,
        identity,
        expected_sha256,
        expected_byte_count,
        "raw artifact",
    )
}

fn path_text(path: &Path, label: &str) -> io::Result<String> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| invalid_data(format!("UHC retained {label} path is not UTF-8")))
}

fn raw_file_name(expected_sha256: &str) -> String {
    format!("raw-{expected_sha256}.json")
}

fn manifest_file_name(expected_sha256: &str, range_count: usize) -> String {
    format!(
        "raw-{expected_sha256}-ranges-{range_count}-v{UHC_RETAIN_CONTRACT_VERSION}.manifest.json"
    )
}

fn build_manifest(
    producer_build_id: &str,
    raw_artifact: UHCRawArtifactManifest,
    ranges: Vec<UHCRawRangeManifest>,
    range_set_sha256: String,
) -> UHCRetainedManifest {
    UHCRetainedManifest {
        contract_id: UHC_RETAIN_CONTRACT_ID.to_owned(),
        contract_version: UHC_RETAIN_CONTRACT_VERSION,
        canonicalization_id: UHC_RETAIN_CANONICALIZATION_ID.to_owned(),
        producer_build_id: producer_build_id.to_owned(),
        raw_artifact,
        range_count: ranges.len() as u64,
        ranges,
        range_set_sha256,
    }
}

fn same_inode(left: FileIdentity, right: FileIdentity) -> bool {
    left.device == right.device && left.inode == right.inode
}

fn publish_or_verify_manifest(
    root: &Arc<RootDirectory>,
    manifest_name: &str,
    candidate: &UHCRetainedManifest,
    expected_raw: &UHCRawArtifactManifest,
    expected_ranges: &[UHCRawRangeManifest],
    expected_range_set_sha256: &str,
) -> io::Result<ManifestPublication> {
    if let Some(existing) = root.open_existing_regular(manifest_name)? {
        return verify_existing_manifest_file(
            &existing,
            expected_raw,
            expected_ranges,
            expected_range_set_sha256,
        );
    }

    let encoded = encode_manifest(candidate)?;
    let mut temporary = root.create_temporary("manifest")?;
    temporary.file_mut().write_all(&encoded)?;
    temporary.file().sync_all()?;
    let temporary_identity = FileIdentity::from_file(temporary.file())?;
    if temporary.publish_noclobber(manifest_name)? {
        root.sync()?;
        let final_file = some_or_invalid_data(
            root.open_existing_regular(manifest_name)?,
            "published retained UHC manifest is not visible in its root",
        )?;
        let final_identity = FileIdentity::from_file(&final_file)?;
        if !same_inode(temporary_identity, final_identity) {
            return Err(invalid_data(
                "published retained UHC manifest inode changed unexpectedly",
            ));
        }
        let observed = read_bounded_stable_file(&final_file, MAX_MANIFEST_BYTES, "manifest")?;
        if observed != encoded {
            return Err(invalid_data(
                "published retained UHC manifest bytes changed unexpectedly",
            ));
        }
        return Ok(ManifestPublication {
            producer_build_id: candidate.producer_build_id.clone(),
            encoded,
            reused: false,
            final_identity,
        });
    }

    let existing = some_or_invalid_data(
        root.open_existing_regular(manifest_name)?,
        "concurrently published retained UHC manifest is unavailable",
    )?;
    verify_existing_manifest_file(
        &existing,
        expected_raw,
        expected_ranges,
        expected_range_set_sha256,
    )
}

fn reverify_manifest_path(
    root: &RootDirectory,
    manifest_name: &str,
    publication: &ManifestPublication,
) -> io::Result<()> {
    let final_file = some_or_invalid_data(
        root.open_existing_regular(manifest_name)?,
        "retained UHC manifest disappeared after verification",
    )?;
    let identity = FileIdentity::from_file(&final_file)?;
    if identity != publication.final_identity {
        return Err(invalid_data(
            "retained UHC manifest identity changed after verification",
        ));
    }
    let observed = read_bounded_stable_file(&final_file, MAX_MANIFEST_BYTES, "manifest")?;
    if observed != publication.encoded {
        return Err(invalid_data(
            "retained UHC manifest bytes changed after verification",
        ));
    }
    Ok(())
}

pub fn retain_uhc_artifact(request: &UHCRetainRequest) -> io::Result<UHCRetainSummary> {
    let total_started = Instant::now();
    let expected_sha256 = request.validate()?;
    let verifier_build_id = current_build_id();
    validate_build_id(verifier_build_id)?;
    let root = RootDirectory::open(&request.output_root)?;
    let raw_name = raw_file_name(&request.expected_sha256);
    let manifest_name = manifest_file_name(&request.expected_sha256, request.range_count);

    let existing_raw = root.open_existing_regular(&raw_name)?;
    let raw_was_present = existing_raw.is_some();
    let mut raw_temporary = if raw_was_present {
        None
    } else {
        Some(root.create_temporary("raw")?)
    };
    let input = if let Some(existing) = existing_raw {
        Arc::new(existing)
    } else {
        Arc::new(open_regular_nofollow(
            &request.source_path,
            "source artifact",
        )?)
    };
    let input_identity = FileIdentity::from_file(&input)?;
    if input_identity.byte_count != request.expected_byte_count {
        return Err(invalid_data(format!(
            "UHC retained input byte count changed: {} != {}",
            input_identity.byte_count, request.expected_byte_count
        )));
    }

    let mut offsets_temporary = root.create_temporary("offsets")?;
    let concurrent_range_input = if let Some(temporary) = raw_temporary.as_ref() {
        Arc::new(temporary.file().try_clone()?)
    } else {
        Arc::clone(&input)
    };
    let scan = scan_raw_and_build_ranges(
        &input,
        concurrent_range_input,
        raw_temporary.as_mut().map(RootTemporaryFile::file_mut),
        offsets_temporary.file_mut(),
        &expected_sha256,
        request.expected_byte_count,
        request.range_count,
    )?;
    require_stable_regular_file(
        &input,
        input_identity,
        request.expected_byte_count,
        "input artifact",
    )?;
    let RawScanResult {
        record_count,
        ranges: streaming_ranges,
        read_hash_copy,
        frame_offset_index,
        concurrent_range_worker_max,
        range_verification_tail,
        raw_fsync,
    } = scan;
    if let Some(temporary) = raw_temporary.as_ref() {
        let identity = FileIdentity::from_file(temporary.file())?;
        if identity.byte_count != request.expected_byte_count {
            return Err(invalid_data(
                "retained UHC raw temporary byte count does not match its source",
            ));
        }
    }

    let (ranges, range_verification) = if streaming_ranges.len() == request.range_count {
        (streaming_ranges, range_verification_tail)
    } else {
        let boundaries =
            build_range_boundaries(offsets_temporary.file(), record_count, request.range_count)?;
        let range_input = if let Some(temporary) = raw_temporary.as_ref() {
            let identity = FileIdentity::from_file(temporary.file())?;
            let range_file = temporary.file().try_clone()?;
            if FileIdentity::from_file(&range_file)? != identity {
                return Err(invalid_data(
                    "retained UHC range workers did not receive the copied raw inode",
                ));
            }
            Arc::new(range_file)
        } else {
            Arc::clone(&input)
        };
        let range_input_identity = FileIdentity::from_file(&range_input)?;
        let range_started = Instant::now();
        let ranges = verify_ranges_parallel(Arc::clone(&range_input), boundaries)?;
        let range_verification = range_started.elapsed();
        require_stable_regular_file(
            &range_input,
            range_input_identity,
            request.expected_byte_count,
            "range input artifact",
        )?;
        (ranges, range_verification_tail + range_verification)
    };
    validate_range_sequence(&ranges, record_count, request.range_count)?;
    let raw_reverification = Duration::ZERO;

    let raw_publish_started = Instant::now();
    let (raw_reused, authoritative_raw_identity) = if let Some(mut temporary) = raw_temporary.take()
    {
        let temporary_identity = FileIdentity::from_file(temporary.file())?;
        if temporary.publish_noclobber(&raw_name)? {
            root.sync()?;
            let final_file = some_or_invalid_data(
                root.open_existing_regular(&raw_name)?,
                "published retained UHC raw artifact is unavailable",
            )?;
            let final_identity = FileIdentity::from_file(&final_file)?;
            if !same_inode(temporary_identity, final_identity)
                || final_identity.byte_count != request.expected_byte_count
            {
                return Err(invalid_data(
                    "published retained UHC raw artifact inode changed unexpectedly",
                ));
            }
            (false, final_identity)
        } else {
            let final_file = some_or_invalid_data(
                root.open_existing_regular(&raw_name)?,
                "concurrently published retained UHC raw artifact is unavailable",
            )?;
            verify_existing_raw_file(&final_file, &expected_sha256, request.expected_byte_count)?;
            (true, FileIdentity::from_file(&final_file)?)
        }
    } else {
        (true, input_identity)
    };
    let raw_publish = raw_publish_started.elapsed();
    root.verify_path_identity()?;
    let final_raw_file = some_or_invalid_data(
        root.open_existing_regular(&raw_name)?,
        "retained UHC raw artifact disappeared before manifest publication",
    )?;
    let final_raw_identity = FileIdentity::from_file(&final_raw_file)?;
    if final_raw_identity != authoritative_raw_identity {
        return Err(invalid_data(
            "retained UHC raw artifact identity changed before manifest publication",
        ));
    }

    let raw_artifact = UHCRawArtifactManifest {
        file_name: raw_name.clone(),
        sha256: request.expected_sha256.clone(),
        byte_count: request.expected_byte_count,
        record_count,
    };
    let range_set_sha256 = range_set_sha256(
        &expected_sha256,
        request.expected_byte_count,
        record_count,
        &ranges,
    )?;
    let candidate_manifest = build_manifest(
        verifier_build_id,
        raw_artifact.clone(),
        ranges.clone(),
        range_set_sha256.clone(),
    );

    let manifest_started = Instant::now();
    let manifest_publication = publish_or_verify_manifest(
        &root,
        &manifest_name,
        &candidate_manifest,
        &raw_artifact,
        &ranges,
        &range_set_sha256,
    )?;
    root.verify_path_identity()?;
    require_stable_regular_file(
        &final_raw_file,
        authoritative_raw_identity,
        request.expected_byte_count,
        "published raw artifact",
    )?;
    let observed_raw_file = some_or_invalid_data(
        root.open_existing_regular(&raw_name)?,
        "retained UHC raw artifact disappeared after manifest publication",
    )?;
    if FileIdentity::from_file(&observed_raw_file)? != authoritative_raw_identity {
        return Err(invalid_data(
            "retained UHC raw artifact identity changed after manifest publication",
        ));
    }
    reverify_manifest_path(&root, &manifest_name, &manifest_publication)?;
    let manifest_verify_or_publish = manifest_started.elapsed();

    let manifest_digest = Sha256::digest(&manifest_publication.encoded);
    let mut timings = UHCRetainTimings {
        total: 0.0,
        raw_read_hash_copy: read_hash_copy.as_secs_f64(),
        frame_offset_index: frame_offset_index.as_secs_f64(),
        concurrent_range_worker_max: concurrent_range_worker_max.as_secs_f64(),
        raw_fsync: raw_fsync.as_secs_f64(),
        range_verification: range_verification.as_secs_f64(),
        raw_reverification: raw_reverification.as_secs_f64(),
        raw_publish: raw_publish.as_secs_f64(),
        manifest_verify_or_publish: manifest_verify_or_publish.as_secs_f64(),
    };
    timings.total = total_started.elapsed().as_secs_f64();
    Ok(UHCRetainSummary {
        record_kind: "uhc_retained_summary".to_owned(),
        contract_id: UHC_RETAIN_CONTRACT_ID.to_owned(),
        contract_version: UHC_RETAIN_CONTRACT_VERSION,
        canonicalization_id: UHC_RETAIN_CANONICALIZATION_ID.to_owned(),
        producer_build_id: manifest_publication.producer_build_id,
        verifier_build_id: verifier_build_id.to_owned(),
        raw_artifact_path: path_text(&root.output_path(&raw_name), "raw artifact")?,
        raw_artifact_sha256: request.expected_sha256.clone(),
        raw_artifact_byte_count: request.expected_byte_count,
        record_count,
        range_count: request.range_count as u64,
        manifest_path: path_text(&root.output_path(&manifest_name), "manifest")?,
        manifest_sha256: sha256_hex(&manifest_digest),
        manifest_byte_count: manifest_publication.encoded.len() as u64,
        raw_reused,
        manifest_reused: manifest_publication.reused,
        timings_seconds: timings,
    })
}

pub fn run_uhc_retain_cli(arguments: &[String]) -> io::Result<()> {
    if arguments.len() != 5 {
        return Err(invalid_input(
            "usage: ptg2_scanner --uhc-retain <source_path> <output_root> <expected_sha256> <expected_byte_count> <range_count>",
        ));
    }
    let expected_byte_count = arguments[3].parse::<u64>().map_err(|_| {
        invalid_input("expected UHC retained byte count must be an unsigned integer")
    })?;
    let range_count = arguments[4]
        .parse::<usize>()
        .map_err(|_| invalid_input("UHC retained range count must be an unsigned integer"))?;
    let summary = retain_uhc_artifact(&UHCRetainRequest {
        source_path: PathBuf::from(&arguments[0]),
        output_root: PathBuf::from(&arguments[1]),
        expected_sha256: arguments[2].clone(),
        expected_byte_count,
        range_count,
    })?;
    let stdout = io::stdout();
    let mut output = stdout.lock();
    serde_json::to_writer(&mut output, &summary)
        .map_err(|error| io::Error::other(error.to_string()))?;
    output.write_all(b"\n")?;
    output.flush()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{symlink, PermissionsExt};

    const FIXTURE: &[u8] = br#"[
  {"id":1,"text":"brace } and bracket [ inside a string"},
  {"id":2,"nested":{"items":[1,2,{"ok":true}]}},
  {"id":3,"huge":1234567890123456789012345678901234567890},
  {"id":4,"escaped":"quote: \" and slash: \\"},
  {"id":5,"empty":null},
  {"id":6,"unicode":"provider-\u2603"},
  {"id":7,"array":[{"a":1},{"b":2}]},
  {"id":8,"active":false}
]"#;

    struct Fixture {
        _directory: tempfile::TempDir,
        source: PathBuf,
        output: PathBuf,
        sha256: String,
        byte_count: u64,
    }

    impl Fixture {
        fn new(payload: &[u8]) -> Self {
            let directory = tempfile::tempdir().expect("temporary fixture root");
            let source = directory.path().join("source.json");
            let output = directory.path().join("retained");
            fs::write(&source, payload).expect("write source fixture");
            fs::create_dir(&output).expect("create retained root");
            Self {
                _directory: directory,
                source,
                output,
                sha256: sha256_hex(&Sha256::digest(payload)),
                byte_count: payload.len() as u64,
            }
        }

        fn request(&self, range_count: usize) -> UHCRetainRequest {
            UHCRetainRequest {
                source_path: self.source.clone(),
                output_root: self.output.clone(),
                expected_sha256: self.sha256.clone(),
                expected_byte_count: self.byte_count,
                range_count,
            }
        }

        fn raw_path(&self) -> PathBuf {
            self.output.join(raw_file_name(&self.sha256))
        }

        fn manifest_path(&self, range_count: usize) -> PathBuf {
            self.output
                .join(manifest_file_name(&self.sha256, range_count))
        }
    }

    fn retained_files(root: &Path) -> Vec<String> {
        let mut files = fs::read_dir(root)
            .expect("read retained root")
            .map(|entry| {
                entry
                    .expect("retained entry")
                    .file_name()
                    .into_string()
                    .expect("UTF-8 retained file name")
            })
            .collect::<Vec<_>>();
        files.sort();
        files
    }

    #[test]
    fn retains_one_raw_artifact_and_deterministic_logical_ranges() {
        let fixture = Fixture::new(FIXTURE);
        let summary = retain_uhc_artifact(&fixture.request(4)).expect("retain fixture");
        assert!(!summary.raw_reused);
        assert!(!summary.manifest_reused);
        assert_eq!(summary.record_count, 8);
        assert_eq!(summary.range_count, 4);

        let raw = fs::read(fixture.raw_path()).expect("read retained raw");
        assert_eq!(raw, FIXTURE);
        let manifest_bytes = fs::read(fixture.manifest_path(4)).expect("read manifest");
        let manifest = parse_strict_manifest(&manifest_bytes).expect("strict manifest");
        assert_eq!(manifest.raw_artifact.record_count, 8);
        assert_eq!(manifest.ranges.len(), 4);
        assert_eq!(
            manifest
                .ranges
                .iter()
                .map(|range| range.record_count)
                .collect::<Vec<_>>(),
            vec![2, 2, 2, 2]
        );

        for range in &manifest.ranges {
            let bytes = &raw[range.raw_byte_start as usize..range.raw_byte_end as usize];
            assert_eq!(sha256_hex(&Sha256::digest(bytes)), range.raw_sha256);
            let mut framer =
                JsonObjectFramer::fragment(range.raw_byte_start, MAX_PROVIDER_RECORD_BYTES);
            let mut canonical = Vec::new();
            framer
                .feed(bytes, |record, _, _| {
                    canonical.extend(
                        record
                            .iter()
                            .copied()
                            .filter(|byte| !matches!(*byte, b'\r' | b'\n')),
                    );
                    canonical.push(b'\n');
                    Ok(())
                })
                .expect("frame retained range");
            framer.finish().expect("complete retained range");
            assert_eq!(
                sha256_hex(&Sha256::digest(&canonical)),
                range.canonical_sha256
            );
            assert_eq!(canonical.len() as u64, range.canonical_byte_count);
        }

        let files = retained_files(&fixture.output);
        assert_eq!(files.len(), 2);
        assert!(files.iter().any(|name| name.ends_with(".json")));
        assert!(!files.iter().any(|name| name.ends_with(".ndjson")));
        assert!(!files.iter().any(|name| name.ends_with(".partial")));
    }

    #[test]
    fn rejects_trailing_comma_and_cleans_every_temporary_file() {
        let fixture = Fixture::new(br#"[{"id":1},{"id":2},{"id":3},{"id":4},]"#);
        let error = retain_uhc_artifact(&fixture.request(4)).expect_err("trailing comma rejected");
        assert!(error.to_string().contains("JSON"));
        assert!(retained_files(&fixture.output).is_empty());
    }

    #[test]
    fn strict_json_rejects_duplicates_extensions_and_invalid_utf8() {
        for invalid in [
            br#"{"a":1,"a":2}"#.as_slice(),
            br#"{"nested":{"a":1,"a":2}}"#.as_slice(),
            br#"{"number":NaN}"#.as_slice(),
            br#"{"number":Infinity}"#.as_slice(),
            br#"{"number":-Infinity}"#.as_slice(),
            br#"{"ok":true} trailing"#.as_slice(),
            b"{\"bad\":\"\xff\"}".as_slice(),
        ] {
            assert!(
                validate_strict_json_object(invalid).is_err(),
                "accepted invalid JSON: {:?}",
                String::from_utf8_lossy(invalid)
            );
        }
        validate_strict_json_object(
            br#"{"huge":12345678901234567890123456789012345678901234567890}"#,
        )
        .expect("arbitrary precision JSON number remains valid");
        validate_strict_json_object(
            br#"{"negative":-42,"decimal":3.125,"null":null,"escaped":"provider-\u2603"}"#,
        )
        .expect("every supported JSON scalar remains valid");
    }

    #[test]
    fn cli_rejects_incomplete_and_non_numeric_arguments() {
        assert_eq!(
            run_uhc_retain_cli(&[])
                .expect_err("missing arguments")
                .kind(),
            io::ErrorKind::InvalidInput
        );

        let mut arguments = vec![
            "source.json".to_owned(),
            "retained".to_owned(),
            "0".repeat(64),
            "not-a-byte-count".to_owned(),
            "4".to_owned(),
        ];
        assert_eq!(
            run_uhc_retain_cli(&arguments)
                .expect_err("non-numeric byte count")
                .kind(),
            io::ErrorKind::InvalidInput
        );

        arguments[3] = "1".to_owned();
        arguments[4] = "not-a-range-count".to_owned();
        assert_eq!(
            run_uhc_retain_cli(&arguments)
                .expect_err("non-numeric range count")
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn framing_is_chunk_independent_and_enforces_object_size() {
        let payload = br#"[{"value":"escaped \\\" quote and } brace"},{"nested":[{}]}]"#;
        let mut records = Vec::new();
        let mut framer = JsonObjectFramer::array(true, 1024);
        for byte in payload {
            framer
                .feed(std::slice::from_ref(byte), |record, start, end| {
                    records.push((record.to_vec(), start, end));
                    Ok(())
                })
                .expect("single-byte framing");
        }
        framer.finish().expect("complete single-byte framing");
        assert_eq!(records.len(), 2);
        assert_eq!(
            records[0].0,
            br#"{"value":"escaped \\\" quote and } brace"}"#
        );

        let mut limited = JsonObjectFramer::array(true, 4);
        assert!(limited
            .feed(br#"[{"too":"large"}]"#, |_, _, _| Ok(()))
            .is_err());
    }

    #[test]
    fn trailing_padding_and_large_range_fanout_use_verified_fallback() {
        let mut padded = br#"[{"id":1},{"id":2},{"id":3},{"id":4}]"#.to_vec();
        padded.extend(std::iter::repeat_n(b' ', 16 * 1024));
        let padded_fixture = Fixture::new(&padded);
        let padded_summary =
            retain_uhc_artifact(&padded_fixture.request(4)).expect("retain padded fixture");
        assert_eq!(padded_summary.record_count, 4);
        assert!(padded_summary.timings_seconds.range_verification > 0.0);
        let padded_manifest = parse_strict_manifest(
            &fs::read(padded_fixture.manifest_path(4)).expect("padded manifest"),
        )
        .expect("parse padded manifest");
        assert_eq!(padded_manifest.ranges.len(), 4);
        assert!(padded_manifest
            .ranges
            .iter()
            .all(|range| range.record_count == 1));

        let records = (0..9)
            .map(|ordinal| format!(r#"{{"id":{ordinal}}}"#))
            .collect::<Vec<_>>()
            .join(",");
        let wide_fixture = Fixture::new(format!("[{records}]").as_bytes());
        let wide_summary =
            retain_uhc_artifact(&wide_fixture.request(9)).expect("retain nine ranges");
        assert_eq!(wide_summary.range_count, 9);
        assert!(wide_summary.timings_seconds.range_verification > 0.0);
    }

    #[test]
    fn rejects_non_objects_trailing_content_and_impossible_range_counts() {
        for payload in [
            br#"[1,2,3,4]"#.as_slice(),
            br#"[{"id":1},{"id":2},{"id":3},{"id":4}] trailing"#.as_slice(),
        ] {
            let fixture = Fixture::new(payload);
            assert!(retain_uhc_artifact(&fixture.request(4)).is_err());
            assert!(retained_files(&fixture.output).is_empty());
        }

        let fixture = Fixture::new(br#"[{"id":1},{"id":2},{"id":3},{"id":4}]"#);
        assert!(retain_uhc_artifact(&fixture.request(3)).is_err());
        assert!(retain_uhc_artifact(&fixture.request(5)).is_err());
        let mut unsafe_request = fixture.request(4);
        unsafe_request.expected_sha256 = "A".repeat(64);
        assert!(retain_uhc_artifact(&unsafe_request).is_err());
    }

    #[test]
    fn exact_existing_artifacts_are_reused_without_the_source() {
        let fixture = Fixture::new(FIXTURE);
        let first = retain_uhc_artifact(&fixture.request(4)).expect("initial retain");
        assert!(!first.raw_reused);
        fs::remove_file(&fixture.source).expect("remove original source");
        let second = retain_uhc_artifact(&fixture.request(4)).expect("reuse retained artifacts");
        assert!(second.raw_reused);
        assert!(second.manifest_reused);
    }

    #[test]
    fn preserves_a_valid_manifest_from_a_different_producer_build() {
        let fixture = Fixture::new(FIXTURE);
        retain_uhc_artifact(&fixture.request(4)).expect("initial retain");
        let manifest_path = fixture.manifest_path(4);
        let mut manifest = parse_strict_manifest(&fs::read(&manifest_path).expect("manifest"))
            .expect("parse manifest");
        manifest.producer_build_id = "ptg2_scanner/preseeded-test-producer".to_owned();
        let preseeded = encode_manifest(&manifest).expect("encode preseeded manifest");
        fs::write(&manifest_path, &preseeded).expect("preseed existing valid manifest");
        let before_identity = FileIdentity::from_metadata(
            &fs::metadata(&manifest_path).expect("preseeded manifest metadata"),
        );

        let summary = retain_uhc_artifact(&fixture.request(4)).expect("reuse preseeded manifest");
        let after_identity = FileIdentity::from_metadata(
            &fs::metadata(&manifest_path).expect("reused manifest metadata"),
        );
        assert!(summary.raw_reused);
        assert!(summary.manifest_reused);
        assert_eq!(
            summary.producer_build_id,
            "ptg2_scanner/preseeded-test-producer"
        );
        assert_eq!(summary.verifier_build_id, current_build_id());
        assert!(same_inode(before_identity, after_identity));
        assert_eq!(fs::read(&manifest_path).expect("reused bytes"), preseeded);
    }

    #[test]
    fn mismatching_existing_manifest_fails_closed_without_replacement() {
        let fixture = Fixture::new(FIXTURE);
        retain_uhc_artifact(&fixture.request(4)).expect("initial retain");
        let manifest_path = fixture.manifest_path(4);
        let mut manifest = parse_strict_manifest(&fs::read(&manifest_path).expect("manifest"))
            .expect("parse manifest");
        manifest.ranges[0].canonical_sha256 = "0".repeat(64);
        let poisoned = encode_manifest(&manifest).expect("encode poisoned manifest");
        fs::write(&manifest_path, &poisoned).expect("poison existing manifest");
        let before_identity = FileIdentity::from_metadata(
            &fs::metadata(&manifest_path).expect("poisoned manifest metadata"),
        );

        assert!(retain_uhc_artifact(&fixture.request(4)).is_err());
        let after_identity = FileIdentity::from_metadata(
            &fs::metadata(&manifest_path).expect("unchanged manifest metadata"),
        );
        assert!(same_inode(before_identity, after_identity));
        assert_eq!(fs::read(&manifest_path).expect("unchanged bytes"), poisoned);
        assert!(!retained_files(&fixture.output)
            .iter()
            .any(|name| name.ends_with(".partial")));
    }

    #[test]
    fn mismatching_existing_raw_fails_closed_without_replacement() {
        let fixture = Fixture::new(FIXTURE);
        let raw_path = fixture.raw_path();
        let mut poisoned = FIXTURE.to_vec();
        let digit = poisoned
            .iter_mut()
            .find(|byte| **byte == b'8')
            .expect("fixture contains a digit to poison");
        *digit = b'9';
        fs::write(&raw_path, &poisoned).expect("preseed mismatching raw");
        let before_identity =
            FileIdentity::from_metadata(&fs::metadata(&raw_path).expect("raw metadata"));

        assert!(retain_uhc_artifact(&fixture.request(4)).is_err());
        let after_identity =
            FileIdentity::from_metadata(&fs::metadata(&raw_path).expect("raw metadata"));
        assert!(same_inode(before_identity, after_identity));
        assert_eq!(fs::read(&raw_path).expect("unchanged raw"), poisoned);
        assert_eq!(
            retained_files(&fixture.output),
            vec![raw_file_name(&fixture.sha256)]
        );
    }

    #[test]
    fn rejects_symlink_roots_sources_and_final_artifacts() {
        let fixture = Fixture::new(FIXTURE);
        let linked_root = fixture._directory.path().join("linked-root");
        symlink(&fixture.output, &linked_root).expect("link output root");
        let mut linked_root_request = fixture.request(4);
        linked_root_request.output_root = linked_root;
        assert!(retain_uhc_artifact(&linked_root_request).is_err());

        let source_target = fixture._directory.path().join("source-target.json");
        fs::write(&source_target, FIXTURE).expect("write source target");
        let linked_source = fixture._directory.path().join("linked-source.json");
        symlink(&source_target, &linked_source).expect("link source");
        let mut linked_source_request = fixture.request(4);
        linked_source_request.source_path = linked_source;
        assert!(retain_uhc_artifact(&linked_source_request).is_err());

        let raw_link_target = fixture._directory.path().join("raw-link-target.json");
        fs::write(&raw_link_target, FIXTURE).expect("write raw link target");
        symlink(&raw_link_target, fixture.raw_path()).expect("link final raw");
        assert!(retain_uhc_artifact(&fixture.request(4)).is_err());
        assert!(fs::symlink_metadata(fixture.raw_path())
            .expect("raw symlink metadata")
            .file_type()
            .is_symlink());
    }

    #[test]
    fn rejects_aliased_or_group_writable_retained_artifacts() {
        let linked_fixture = Fixture::new(FIXTURE);
        fs::write(linked_fixture.raw_path(), FIXTURE).expect("preseed raw artifact");
        let alias = linked_fixture._directory.path().join("raw-alias.json");
        fs::hard_link(linked_fixture.raw_path(), &alias).expect("hard-link retained raw");
        assert!(retain_uhc_artifact(&linked_fixture.request(4)).is_err());
        assert_eq!(fs::metadata(&alias).expect("alias metadata").nlink(), 2);

        let writable_fixture = Fixture::new(FIXTURE);
        retain_uhc_artifact(&writable_fixture.request(4)).expect("initial retain");
        let manifest_path = writable_fixture.manifest_path(4);
        let mut permissions = fs::metadata(&manifest_path)
            .expect("manifest metadata")
            .permissions();
        permissions.set_mode(0o620);
        fs::set_permissions(&manifest_path, permissions).expect("make manifest group writable");
        assert!(retain_uhc_artifact(&writable_fixture.request(4)).is_err());
        assert_eq!(
            fs::metadata(&manifest_path)
                .expect("manifest metadata")
                .mode()
                & 0o777,
            0o620
        );
    }

    #[test]
    fn concurrent_admission_never_clobbers_final_artifacts() {
        let fixture = Fixture::new(FIXTURE);
        let request = Arc::new(fixture.request(4));
        let first_request = Arc::clone(&request);
        let second_request = Arc::clone(&request);
        let first = thread::spawn(move || retain_uhc_artifact(&first_request));
        let second = thread::spawn(move || retain_uhc_artifact(&second_request));
        let first = first
            .join()
            .expect("first admission thread")
            .expect("first admission");
        let second = second
            .join()
            .expect("second admission thread")
            .expect("second admission");
        assert_eq!(first.manifest_sha256, second.manifest_sha256);
        assert!(first.raw_reused || second.raw_reused);
        assert!(first.manifest_reused || second.manifest_reused);
        assert_eq!(retained_files(&fixture.output).len(), 2);
        assert!(!retained_files(&fixture.output)
            .iter()
            .any(|name| name.ends_with(".partial")));
    }

    #[test]
    fn range_set_hash_has_a_stable_golden_value() {
        let fixture = Fixture::new(FIXTURE);
        retain_uhc_artifact(&fixture.request(4)).expect("retain fixture");
        let manifest = parse_strict_manifest(
            &fs::read(fixture.manifest_path(4)).expect("read golden manifest"),
        )
        .expect("parse golden manifest");
        assert_eq!(
            manifest.range_set_sha256,
            "0cd8eb9ccef2be7d4abb442b48d51f56e76fb11670ce662568cba4dc6ee15bb8"
        );
    }
}
