use sha2::{Digest, Sha256};
use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BinaryHeap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

pub const SERVING_RUN_RECORD_BYTES: usize = 52;
pub const SERVING_RUN_FORMAT: &str = "ptg2_v3_serving_run";
pub const SERVING_RUN_FORMAT_VERSION: u32 = 1;
pub const CODE_DICTIONARY_FORMAT: &str = "ptg2_v3_serving_code_dictionary";
pub const CODE_DICTIONARY_FORMAT_VERSION: u32 = 4;
pub const CODE_IDENTITY_VERSION: u32 = 4;
pub const COVERAGE_SCOPE_ID_BYTES: usize = 32;
pub const AUDIT_CANDIDATE_FORMAT: &str = "ptg2_v3_audit_candidates_v2";
pub const AUDIT_CANDIDATE_FORMAT_VERSION: u32 = 2;
pub const AUDIT_CANDIDATE_MAX_RECORDS: usize = 4096;
pub const AUDIT_CANDIDATE_RECORD_BYTES: usize = 20;
pub const AUDIT_CANDIDATE_SELECTION: &str = "equal_interval_assigned_rows_v1";

const CODE_IDENTITY_DOMAIN: &[u8] = b"healthporta.ptg2.v3.serving-code-identity";
const CODE_DICTIONARY_MAGIC: &[u8; 8] = b"PTG2CDR4";
const DEFAULT_PARTITION_BUFFER_BYTES: usize = 64 * 1024;
const DEFAULT_SORT_BUFFER_BYTES: usize = 1024 * 1024;
const DEFAULT_SORT_MERGE_FAN_IN: usize = 64;
const MAX_TAGGED_SERVING_RUN_RECORD_BYTES: usize = SERVING_RUN_RECORD_BYTES + 4;
static NEXT_TEMP_FILE_ID: AtomicU64 = AtomicU64::new(0);

/// One bounded audit seed selected from the final source-multiset serving stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuditCandidateRecord {
    pub code_key: u32,
    pub provider_set_key: u32,
    pub price_key: u32,
    pub source_key: u32,
    pub provider_count: u32,
}

impl AuditCandidateRecord {
    pub fn encode(&self) -> [u8; AUDIT_CANDIDATE_RECORD_BYTES] {
        let mut encoded = [0u8; AUDIT_CANDIDATE_RECORD_BYTES];
        encoded[0..4].copy_from_slice(&self.code_key.to_be_bytes());
        encoded[4..8].copy_from_slice(&self.provider_set_key.to_be_bytes());
        encoded[8..12].copy_from_slice(&self.price_key.to_be_bytes());
        encoded[12..16].copy_from_slice(&self.source_key.to_be_bytes());
        encoded[16..20].copy_from_slice(&self.provider_count.to_be_bytes());
        encoded
    }

    pub fn decode(encoded: &[u8]) -> io::Result<Self> {
        if encoded.len() != AUDIT_CANDIDATE_RECORD_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "audit candidate record must contain {AUDIT_CANDIDATE_RECORD_BYTES} bytes, got {}",
                    encoded.len()
                ),
            ));
        }
        Ok(Self {
            code_key: u32::from_be_bytes(encoded[0..4].try_into().map_err(to_invalid_data)?),
            provider_set_key: u32::from_be_bytes(
                encoded[4..8].try_into().map_err(to_invalid_data)?,
            ),
            price_key: u32::from_be_bytes(encoded[8..12].try_into().map_err(to_invalid_data)?),
            source_key: u32::from_be_bytes(encoded[12..16].try_into().map_err(to_invalid_data)?),
            provider_count: u32::from_be_bytes(
                encoded[16..20].try_into().map_err(to_invalid_data)?,
            ),
        })
    }
}

/// Bounded equal-interval selection over an already ordered stream with a known row count.
pub struct AuditCandidateSelector {
    population_count: u64,
    target_count: u64,
    observed_count: u64,
    next_sample_index: u64,
    next_sample_ordinal: Option<u64>,
    records: Vec<AuditCandidateRecord>,
}

impl AuditCandidateSelector {
    pub fn new(population_count: u64) -> Self {
        let target_count = population_count.min(AUDIT_CANDIDATE_MAX_RECORDS as u64);
        let next_sample_ordinal = (target_count > 0).then_some(0);
        Self {
            population_count,
            target_count,
            observed_count: 0,
            next_sample_index: 0,
            next_sample_ordinal,
            records: Vec::with_capacity(target_count as usize),
        }
    }

    pub fn observe(
        &mut self,
        code_key: u32,
        provider_set_key: u32,
        price_key: u32,
        source_key: u32,
        provider_count: u32,
    ) -> io::Result<()> {
        if self.observed_count >= self.population_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "audit candidate selector observed more rows than declared",
            ));
        }
        let row_ordinal = self.observed_count;
        if self.next_sample_ordinal == Some(row_ordinal) {
            self.records.push(AuditCandidateRecord {
                code_key,
                provider_set_key,
                price_key,
                source_key,
                provider_count,
            });
            self.next_sample_index += 1;
            self.next_sample_ordinal = if self.next_sample_index < self.target_count {
                Some(stratified_audit_ordinal(
                    self.population_count,
                    self.target_count,
                    self.next_sample_index,
                ))
            } else {
                None
            };
        }
        self.observed_count += 1;
        Ok(())
    }

    pub fn finish(&self) -> io::Result<&[AuditCandidateRecord]> {
        if self.observed_count != self.population_count
            || self.records.len() as u64 != self.target_count
            || self.next_sample_ordinal.is_some()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "audit candidate selection mismatch: observed {}, expected {}, selected {}, expected {}",
                    self.observed_count,
                    self.population_count,
                    self.records.len(),
                    self.target_count
                ),
            ));
        }
        Ok(&self.records)
    }
}

fn stratified_audit_ordinal(population_count: u64, target_count: u64, index: u64) -> u64 {
    debug_assert!(population_count > 0);
    debug_assert!(target_count > 0);
    debug_assert!(target_count <= population_count);
    debug_assert!(index < target_count);
    if target_count == 1 {
        return 0;
    }
    let numerator = u128::from(index) * u128::from(population_count - 1);
    (numerator / u128::from(target_count - 1)) as u64
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuditCandidateFileSummary {
    pub byte_count: u64,
    pub sha256: [u8; 32],
}

pub fn write_audit_candidate_file(
    path: impl AsRef<Path>,
    population_count: u64,
    records: &[AuditCandidateRecord],
) -> io::Result<AuditCandidateFileSummary> {
    let expected_count = population_count.min(AUDIT_CANDIDATE_MAX_RECORDS as u64);
    if records.len() as u64 != expected_count {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "audit candidate file requires {expected_count} records for population {population_count}, got {}",
                records.len()
            ),
        ));
    }
    let path = path.as_ref();
    let file = OpenOptions::new().write(true).create_new(true).open(path)?;
    let mut writer = BufWriter::new(file);
    let mut digest = Sha256::new();
    for record in records {
        let encoded = record.encode();
        writer.write_all(&encoded)?;
        digest.update(encoded);
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    let byte_count = (records.len() as u64).saturating_mul(AUDIT_CANDIDATE_RECORD_BYTES as u64);
    Ok(AuditCandidateFileSummary {
        byte_count,
        sha256: digest.finalize().into(),
    })
}

pub fn read_audit_candidate_file(path: impl AsRef<Path>) -> io::Result<Vec<AuditCandidateRecord>> {
    let mut reader = BufReader::new(File::open(path)?);
    let byte_count = reader.get_ref().metadata()?.len();
    if byte_count % AUDIT_CANDIDATE_RECORD_BYTES as u64 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "audit candidate file is not record-aligned",
        ));
    }
    let record_count = byte_count / AUDIT_CANDIDATE_RECORD_BYTES as u64;
    if record_count > AUDIT_CANDIDATE_MAX_RECORDS as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "audit candidate file exceeds the bounded record count",
        ));
    }
    let mut records = Vec::with_capacity(record_count as usize);
    let mut encoded = [0u8; AUDIT_CANDIDATE_RECORD_BYTES];
    for _ in 0..record_count {
        reader.read_exact(&mut encoded)?;
        records.push(AuditCandidateRecord::decode(&encoded)?);
    }
    if reader.read(&mut [0u8; 1])? != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "audit candidate file has trailing bytes",
        ));
    }
    Ok(records)
}

fn to_invalid_data(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

pub fn parse_coverage_scope_id(value: &str) -> io::Result<[u8; COVERAGE_SCOPE_ID_BYTES]> {
    if value.len() != COVERAGE_SCOPE_ID_BYTES * 2
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "HLTHPRT_PTG2_V3_COVERAGE_SCOPE_ID must be exactly 64 lowercase hex characters",
        ));
    }
    let mut scope = [0u8; COVERAGE_SCOPE_ID_BYTES];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        scope[index] = (lower_hex_nibble(pair[0]) << 4) | lower_hex_nibble(pair[1]);
    }
    Ok(scope)
}

fn lower_hex_nibble(value: u8) -> u8 {
    match value {
        b'0'..=b'9' => value - b'0',
        b'a'..=b'f' => value - b'a' + 10,
        _ => unreachable!("coverage-scope syntax is validated before decoding"),
    }
}

/// Stable 128-bit identity for the exact strict-V3 code tuple.
pub fn natural_lean_code_identity(
    coverage_scope_id: &[u8; COVERAGE_SCOPE_ID_BYTES],
    reported_code_system: Option<&str>,
    reported_code: Option<&str>,
    negotiation_arrangement: Option<&str>,
    billing_code_type_version: Option<&str>,
    name: Option<&str>,
    description: Option<&str>,
) -> [u8; 16] {
    let mut hasher = Sha256::new();
    hasher.update(CODE_IDENTITY_DOMAIN);
    hasher.update([0]);
    hasher.update(CODE_IDENTITY_VERSION.to_be_bytes());
    update_required_identity_field(&mut hasher, coverage_scope_id);
    update_optional_identity_field(&mut hasher, reported_code_system);
    update_optional_identity_field(&mut hasher, reported_code);
    update_optional_identity_field(&mut hasher, negotiation_arrangement);
    update_optional_identity_field(&mut hasher, billing_code_type_version);
    update_optional_identity_field(&mut hasher, name);
    update_optional_identity_field(&mut hasher, description);
    let digest = hasher.finalize();
    let mut identity = [0u8; 16];
    identity.copy_from_slice(&digest[..16]);
    identity
}

fn update_required_identity_field(hasher: &mut Sha256, value: &[u8]) {
    hasher.update([1]);
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn update_optional_identity_field(hasher: &mut Sha256, value: Option<&str>) {
    match value {
        None => hasher.update([0]),
        Some(value) => update_required_identity_field(hasher, value.as_bytes()),
    }
}

/// One fixed-width V3 serving record. The three IDs form its identity.
#[derive(Clone, Copy, Debug)]
pub struct ServingRunRecord {
    pub code_id: [u8; 16],
    pub provider_set_id: [u8; 16],
    pub price_set_id: [u8; 16],
    pub provider_count: u32,
}

impl ServingRunRecord {
    pub fn encode(&self) -> [u8; SERVING_RUN_RECORD_BYTES] {
        let mut encoded = [0u8; SERVING_RUN_RECORD_BYTES];
        encoded[0..16].copy_from_slice(&self.code_id);
        encoded[16..32].copy_from_slice(&self.provider_set_id);
        encoded[32..48].copy_from_slice(&self.price_set_id);
        encoded[48..52].copy_from_slice(&self.provider_count.to_be_bytes());
        encoded
    }

    pub fn decode(encoded: &[u8]) -> io::Result<Self> {
        if encoded.len() != SERVING_RUN_RECORD_BYTES {
            let detail = if encoded.len() < SERVING_RUN_RECORD_BYTES {
                "truncated"
            } else {
                "oversized"
            };
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{detail} serving run record: expected {SERVING_RUN_RECORD_BYTES} bytes, got {}",
                    encoded.len()
                ),
            ));
        }

        let mut code_id = [0u8; 16];
        let mut provider_set_id = [0u8; 16];
        let mut price_set_id = [0u8; 16];
        let mut provider_count = [0u8; 4];
        code_id.copy_from_slice(&encoded[0..16]);
        provider_set_id.copy_from_slice(&encoded[16..32]);
        price_set_id.copy_from_slice(&encoded[32..48]);
        provider_count.copy_from_slice(&encoded[48..52]);

        Ok(Self {
            code_id,
            provider_set_id,
            price_set_id,
            provider_count: u32::from_be_bytes(provider_count),
        })
    }

    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(&self.encode())
    }

    /// Reads one record, returning `None` only for EOF at a record boundary.
    pub fn read_from<R: Read>(reader: &mut R) -> io::Result<Option<Self>> {
        let mut encoded = [0u8; SERVING_RUN_RECORD_BYTES];
        let mut bytes_read = 0;

        while bytes_read < encoded.len() {
            match reader.read(&mut encoded[bytes_read..]) {
                Ok(0) if bytes_read == 0 => return Ok(None),
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "truncated serving run record: expected {SERVING_RUN_RECORD_BYTES} bytes, got {bytes_read}"
                        ),
                    ));
                }
                Ok(count) => bytes_read += count,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
        }

        Self::decode(&encoded).map(Some)
    }

    pub fn same_identity(&self, other: &Self) -> bool {
        self.code_id == other.code_id
            && self.provider_set_id == other.provider_set_id
            && self.price_set_id == other.price_set_id
    }
}

impl PartialEq for ServingRunRecord {
    fn eq(&self, other: &Self) -> bool {
        self.same_identity(other)
    }
}

impl Eq for ServingRunRecord {}

impl PartialOrd for ServingRunRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ServingRunRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        self.code_id
            .cmp(&other.code_id)
            .then_with(|| self.provider_set_id.cmp(&other.provider_set_id))
            .then_with(|| self.price_set_id.cmp(&other.price_set_id))
    }
}

/// Finalizer-only serving record tagged with its dense source key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TaggedServingRunCodec {
    source_count: u64,
    source_key_bytes: u8,
    record_bytes: usize,
}

impl TaggedServingRunCodec {
    pub fn new(source_count: u64, source_key_bytes: u8) -> io::Result<Self> {
        validate_source_key_bytes(source_count, source_key_bytes)?;
        Ok(Self {
            source_count,
            source_key_bytes,
            record_bytes: SERVING_RUN_RECORD_BYTES + usize::from(source_key_bytes),
        })
    }

    pub fn source_count(self) -> u64 {
        self.source_count
    }

    pub fn source_key_bytes(self) -> u8 {
        self.source_key_bytes
    }

    pub fn record_bytes(self) -> usize {
        self.record_bytes
    }
}

#[derive(Clone, Copy, Debug)]
pub struct TaggedServingRunRecord {
    pub record: ServingRunRecord,
    pub source_key: u32,
}

impl TaggedServingRunRecord {
    pub fn encode(&self, codec: TaggedServingRunCodec) -> io::Result<Vec<u8>> {
        if u64::from(self.source_key) >= codec.source_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "tagged serving source_key {} is outside source_count {}",
                    self.source_key, codec.source_count,
                ),
            ));
        }
        let source_key_bytes = usize::from(codec.source_key_bytes);
        let mut encoded = vec![0u8; codec.record_bytes];
        encoded[0..16].copy_from_slice(&self.record.code_id);
        encoded[16..32].copy_from_slice(&self.record.provider_set_id);
        encoded[32..48].copy_from_slice(&self.record.price_set_id);
        if source_key_bytes > 0 {
            let source_key = self.source_key.to_be_bytes();
            encoded[48..48 + source_key_bytes].copy_from_slice(&source_key[4 - source_key_bytes..]);
        }
        encoded[48 + source_key_bytes..52 + source_key_bytes]
            .copy_from_slice(&self.record.provider_count.to_be_bytes());
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8], codec: TaggedServingRunCodec) -> io::Result<Self> {
        let source_key_bytes = usize::from(codec.source_key_bytes);
        let expected_bytes = codec.record_bytes;
        if encoded.len() != expected_bytes {
            let detail = if encoded.len() < expected_bytes {
                "truncated"
            } else {
                "oversized"
            };
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{detail} tagged serving run record: expected {expected_bytes} bytes, got {}",
                    encoded.len()
                ),
            ));
        }
        let mut code_id = [0u8; 16];
        let mut provider_set_id = [0u8; 16];
        let mut price_set_id = [0u8; 16];
        code_id.copy_from_slice(&encoded[0..16]);
        provider_set_id.copy_from_slice(&encoded[16..32]);
        price_set_id.copy_from_slice(&encoded[32..48]);
        let mut source_key = [0u8; 4];
        if source_key_bytes > 0 {
            source_key[4 - source_key_bytes..].copy_from_slice(&encoded[48..48 + source_key_bytes]);
        }
        let source_key = u32::from_be_bytes(source_key);
        if u64::from(source_key) >= codec.source_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "tagged serving source_key {source_key} is outside source_count {}",
                    codec.source_count
                ),
            ));
        }
        Ok(Self {
            record: ServingRunRecord {
                code_id,
                provider_set_id,
                price_set_id,
                provider_count: u32::from_be_bytes(
                    encoded[48 + source_key_bytes..52 + source_key_bytes]
                        .try_into()
                        .map_err(to_invalid_data)?,
                ),
            },
            source_key,
        })
    }

    pub fn write_to<W: Write>(
        &self,
        writer: &mut W,
        codec: TaggedServingRunCodec,
    ) -> io::Result<()> {
        if u64::from(self.source_key) >= codec.source_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "tagged serving source_key {} is outside source_count {}",
                    self.source_key, codec.source_count,
                ),
            ));
        }
        writer.write_all(&self.record.code_id)?;
        writer.write_all(&self.record.provider_set_id)?;
        writer.write_all(&self.record.price_set_id)?;
        let source_key_bytes = usize::from(codec.source_key_bytes);
        if source_key_bytes > 0 {
            let source_key = self.source_key.to_be_bytes();
            writer.write_all(&source_key[4 - source_key_bytes..])?;
        }
        writer.write_all(&self.record.provider_count.to_be_bytes())
    }

    pub fn read_from<R: Read>(
        reader: &mut R,
        codec: TaggedServingRunCodec,
    ) -> io::Result<Option<Self>> {
        let record_bytes = codec.record_bytes;
        let mut encoded = [0u8; MAX_TAGGED_SERVING_RUN_RECORD_BYTES];
        let mut bytes_read = 0usize;
        while bytes_read < record_bytes {
            match reader.read(&mut encoded[bytes_read..record_bytes]) {
                Ok(0) if bytes_read == 0 => return Ok(None),
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "truncated tagged serving run record: expected {record_bytes} bytes, got {bytes_read}"
                        ),
                    ));
                }
                Ok(count) => bytes_read += count,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
        }
        Self::decode(&encoded[..record_bytes], codec).map(Some)
    }

    fn same_identity(&self, other: &Self) -> bool {
        self.record.same_identity(&other.record) && self.source_key == other.source_key
    }
}

impl PartialEq for TaggedServingRunRecord {
    fn eq(&self, other: &Self) -> bool {
        self.same_identity(other)
    }
}

impl Eq for TaggedServingRunRecord {}

impl PartialOrd for TaggedServingRunRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TaggedServingRunRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        self.record
            .cmp(&other.record)
            .then_with(|| self.source_key.cmp(&other.source_key))
    }
}

pub fn source_key_bits(source_count: u64) -> io::Result<u8> {
    const POSTGRES_INTEGER_SOURCE_CAPACITY: u64 = (i32::MAX as u64) + 1;
    if source_count == 0 || source_count > POSTGRES_INTEGER_SOURCE_CAPACITY {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "source_count must be in 1..=2^31 for PostgreSQL integer source keys",
        ));
    }
    if source_count == 1 {
        return Ok(0);
    }
    Ok((u64::BITS - (source_count - 1).leading_zeros()) as u8)
}

pub fn source_key_bytes(source_count: u64) -> io::Result<u8> {
    Ok(source_key_bits(source_count)?.div_ceil(8))
}

pub fn tagged_serving_run_record_bytes(source_count: u64) -> io::Result<usize> {
    Ok(SERVING_RUN_RECORD_BYTES + usize::from(source_key_bytes(source_count)?))
}

fn validate_source_key_bytes(source_count: u64, configured_bytes: u8) -> io::Result<()> {
    let expected_bytes = source_key_bytes(source_count)?;
    if configured_bytes != expected_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "source_key_bytes mismatch for source_count {source_count}: expected {expected_bytes}, got {configured_bytes}"
            ),
        ));
    }
    Ok(())
}

trait ServingSortRecord: Copy + Ord {
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Option<Self>>;
    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()>;
    fn same_identity(&self, other: &Self) -> bool;
    fn provider_count(&self) -> u32;
}

impl ServingSortRecord for ServingRunRecord {
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Option<Self>> {
        Self::read_from(reader)
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        self.write_to(writer)
    }

    fn same_identity(&self, other: &Self) -> bool {
        self.same_identity(other)
    }

    fn provider_count(&self) -> u32 {
        self.provider_count
    }
}

pub fn validate_partition_count(partition_count: usize) -> io::Result<()> {
    if !(1..=256).contains(&partition_count) || !partition_count.is_power_of_two() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("partition_count must be a power of two in 1..=256, got {partition_count}"),
        ));
    }
    Ok(())
}

/// Selects a partition from the leading bits of the code ID.
pub fn partition_for_code_id(code_id: &[u8; 16], partition_count: usize) -> io::Result<usize> {
    validate_partition_count(partition_count)?;
    let partition_bits = partition_count.trailing_zeros();
    if partition_bits == 0 {
        return Ok(0);
    }
    Ok(usize::from(code_id[0] >> (8 - partition_bits)))
}

pub fn partition_for_record(
    record: &ServingRunRecord,
    partition_count: usize,
) -> io::Result<usize> {
    partition_for_code_id(&record.code_id, partition_count)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionFile {
    pub partition_index: usize,
    pub partition_count: usize,
    pub path: PathBuf,
    pub record_count: u64,
    pub bytes: u64,
    pub sha256: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NaturalLeanCode {
    pub code_id: [u8; 16],
    pub coverage_scope_id: [u8; COVERAGE_SCOPE_ID_BYTES],
    pub reported_code_system: Option<String>,
    pub reported_code: Option<String>,
    pub negotiation_arrangement: Option<String>,
    pub billing_code_type_version: Option<String>,
    pub name: Option<String>,
    pub description: Option<String>,
}

#[derive(Clone, Copy, Debug)]
pub struct NaturalLeanCodeFields<'a> {
    pub coverage_scope_id: &'a [u8; COVERAGE_SCOPE_ID_BYTES],
    pub reported_code_system: Option<&'a str>,
    pub reported_code: Option<&'a str>,
    pub negotiation_arrangement: Option<&'a str>,
    pub billing_code_type_version: Option<&'a str>,
    pub name: Option<&'a str>,
    pub description: Option<&'a str>,
}

impl NaturalLeanCodeFields<'_> {
    pub fn identity(&self) -> [u8; 16] {
        natural_lean_code_identity(
            self.coverage_scope_id,
            self.reported_code_system,
            self.reported_code,
            self.negotiation_arrangement,
            self.billing_code_type_version,
            self.name,
            self.description,
        )
    }

    fn into_owned(self, code_id: [u8; 16]) -> NaturalLeanCode {
        NaturalLeanCode {
            code_id,
            coverage_scope_id: *self.coverage_scope_id,
            reported_code_system: self.reported_code_system.map(str::to_owned),
            reported_code: self.reported_code.map(str::to_owned),
            negotiation_arrangement: self.negotiation_arrangement.map(str::to_owned),
            billing_code_type_version: self.billing_code_type_version.map(str::to_owned),
            name: self.name.map(str::to_owned),
            description: self.description.map(str::to_owned),
        }
    }

    fn matches(self, code: &NaturalLeanCode) -> bool {
        code.coverage_scope_id == *self.coverage_scope_id
            && code.reported_code_system.as_deref() == self.reported_code_system
            && code.reported_code.as_deref() == self.reported_code
            && code.negotiation_arrangement.as_deref() == self.negotiation_arrangement
            && code.billing_code_type_version.as_deref() == self.billing_code_type_version
            && code.name.as_deref() == self.name
            && code.description.as_deref() == self.description
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PreparedNaturalLeanCode {
    code_id: [u8; 16],
}

impl PreparedNaturalLeanCode {
    pub fn code_id(self) -> [u8; 16] {
        self.code_id
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CodeDictionaryFile {
    pub path: PathBuf,
    pub record_count: u64,
    pub bytes: u64,
    pub sha256: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServingRunOutput {
    pub partition_files: Vec<PartitionFile>,
    pub code_dictionary_file: Option<CodeDictionaryFile>,
}

struct PartitionSink {
    staged_path: PathBuf,
    ready_path: PathBuf,
    writer: BufWriter<File>,
    sha256: Sha256,
}

/// Buffered, lazily opened staged files for code-ID partitions.
pub struct ServingRunPartitionWriter {
    directory: PathBuf,
    writer_token: String,
    file_id: u64,
    buffer_capacity: usize,
    writers: Vec<Option<PartitionSink>>,
    counts: Vec<u64>,
    poisoned: bool,
    committed: bool,
    staged_cleanup_paths: Vec<PathBuf>,
    renamed_ready_paths: Vec<PathBuf>,
    code_dictionary: BTreeMap<[u8; 16], NaturalLeanCode>,
}

impl ServingRunPartitionWriter {
    pub fn new(
        directory: impl AsRef<Path>,
        partition_count: usize,
        writer_token: impl AsRef<str>,
    ) -> io::Result<Self> {
        Self::with_buffer_capacity(
            directory,
            partition_count,
            writer_token,
            DEFAULT_PARTITION_BUFFER_BYTES,
        )
    }

    pub fn with_buffer_capacity(
        directory: impl AsRef<Path>,
        partition_count: usize,
        writer_token: impl AsRef<str>,
        buffer_capacity: usize,
    ) -> io::Result<Self> {
        validate_partition_count(partition_count)?;
        if buffer_capacity == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "partition buffer capacity must be greater than zero",
            ));
        }

        let directory = directory.as_ref().to_path_buf();
        fs::create_dir_all(&directory)?;
        let writer_token = sanitize_writer_token(writer_token.as_ref())?;
        Ok(Self {
            directory,
            writer_token,
            file_id: NEXT_TEMP_FILE_ID.fetch_add(1, AtomicOrdering::Relaxed),
            buffer_capacity,
            writers: std::iter::repeat_with(|| None)
                .take(partition_count)
                .collect(),
            counts: vec![0; partition_count],
            poisoned: false,
            committed: false,
            staged_cleanup_paths: Vec::new(),
            renamed_ready_paths: Vec::new(),
            code_dictionary: BTreeMap::new(),
        })
    }

    pub fn partition_count(&self) -> usize {
        self.writers.len()
    }

    pub fn write_record(&mut self, record: &ServingRunRecord) -> io::Result<usize> {
        if self.poisoned {
            return Err(io::Error::other(
                "partition writer is unusable after an earlier write failure",
            ));
        }

        let partition_index = partition_for_record(record, self.partition_count())?;
        let next_count = self.counts[partition_index].checked_add(1).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "partition record count overflow",
            )
        })?;
        let encoded = record.encode();
        let write_result = self.sink_for_partition(partition_index).and_then(|sink| {
            sink.writer.write_all(&encoded)?;
            sink.sha256.update(encoded);
            Ok(())
        });
        if let Err(error) = write_result {
            self.poisoned = true;
            return Err(error);
        }
        self.counts[partition_index] = next_count;
        Ok(partition_index)
    }

    pub fn write_natural_lean_record(
        &mut self,
        record: &ServingRunRecord,
        code_fields: NaturalLeanCodeFields<'_>,
    ) -> io::Result<usize> {
        let expected_code_id = code_fields.identity();
        if record.code_id != expected_code_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "serving run code identity does not match its natural-lean tuple",
            ));
        }
        let prepared = self.register_natural_lean_code_with_id(expected_code_id, code_fields)?;
        self.write_prepared_natural_lean_record(record, prepared)
    }

    pub fn register_natural_lean_code(
        &mut self,
        code_fields: NaturalLeanCodeFields<'_>,
    ) -> io::Result<PreparedNaturalLeanCode> {
        let code_id = code_fields.identity();
        self.register_natural_lean_code_with_id(code_id, code_fields)
    }

    pub fn write_prepared_natural_lean_record(
        &mut self,
        record: &ServingRunRecord,
        prepared: PreparedNaturalLeanCode,
    ) -> io::Result<usize> {
        if record.code_id != prepared.code_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "serving run code identity does not match its prepared natural-lean code",
            ));
        }
        self.write_record(record)
    }

    fn register_natural_lean_code_with_id(
        &mut self,
        code_id: [u8; 16],
        code_fields: NaturalLeanCodeFields<'_>,
    ) -> io::Result<PreparedNaturalLeanCode> {
        if let Some(existing) = self.code_dictionary.get(&code_id) {
            if !code_fields.matches(existing) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "serving run code identity collision has conflicting natural-lean tuples",
                ));
            }
        } else {
            self.code_dictionary
                .insert(code_id, code_fields.into_owned(code_id));
        }
        Ok(PreparedNaturalLeanCode { code_id })
    }

    pub fn finish(mut self) -> io::Result<ServingRunOutput> {
        if self.poisoned {
            return Err(io::Error::other(
                "partition writer had an earlier write failure",
            ));
        }

        for sink in self.writers.iter_mut().flatten() {
            sink.writer.flush()?;
            sink.writer.get_ref().sync_all()?;
        }

        let code_dictionary_sink = self.stage_code_dictionary()?;

        let partition_count = self.partition_count();
        let mut files = Vec::new();
        for (partition_index, sink) in self.writers.iter_mut().enumerate() {
            let Some(sink) = sink.take() else {
                continue;
            };
            let bytes = sink.writer.get_ref().metadata()?.len();
            drop(sink.writer);
            fs::rename(&sink.staged_path, &sink.ready_path)?;
            self.renamed_ready_paths.push(sink.ready_path.clone());
            files.push(PartitionFile {
                partition_index,
                partition_count,
                path: sink.ready_path,
                record_count: self.counts[partition_index],
                bytes,
                sha256: sink.sha256.finalize().into(),
            });
        }
        let code_dictionary_file = if let Some(sink) = code_dictionary_sink {
            let bytes = sink.writer.get_ref().metadata()?.len();
            drop(sink.writer);
            fs::rename(&sink.staged_path, &sink.ready_path)?;
            self.renamed_ready_paths.push(sink.ready_path.clone());
            Some(CodeDictionaryFile {
                path: sink.ready_path,
                record_count: self.code_dictionary.len() as u64,
                bytes,
                sha256: sink.sha256.finalize().into(),
            })
        } else {
            None
        };
        sync_directory(&self.directory)?;
        self.committed = true;
        Ok(ServingRunOutput {
            partition_files: files,
            code_dictionary_file,
        })
    }

    fn staged_path(&self, partition_index: usize) -> PathBuf {
        self.directory.join(format!(
            ".ptg2-v3-serving-{}-{}-{:016x}-partition-{partition_index:03}.partial",
            self.writer_token,
            std::process::id(),
            self.file_id,
        ))
    }

    fn ready_path(&self, partition_index: usize) -> PathBuf {
        self.directory.join(format!(
            "ptg2-v3-serving-{}-{}-{:016x}-partition-{partition_index:03}.v{SERVING_RUN_FORMAT_VERSION}.ready",
            self.writer_token,
            std::process::id(),
            self.file_id,
        ))
    }

    fn code_dictionary_paths(&self) -> (PathBuf, PathBuf) {
        let stem = format!(
            "ptg2-v3-serving-{}-{}-{:016x}-codes.v{CODE_DICTIONARY_FORMAT_VERSION}",
            self.writer_token,
            std::process::id(),
            self.file_id,
        );
        (
            self.directory.join(format!(".{stem}.partial")),
            self.directory.join(format!("{stem}.ready")),
        )
    }

    fn stage_code_dictionary(&mut self) -> io::Result<Option<PartitionSink>> {
        if self.code_dictionary.is_empty() {
            return Ok(None);
        }
        let (staged_path, ready_path) = self.code_dictionary_paths();
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&staged_path)?;
        self.staged_cleanup_paths.push(staged_path.clone());
        let mut sink = PartitionSink {
            staged_path,
            ready_path,
            writer: BufWriter::with_capacity(self.buffer_capacity, file),
            sha256: Sha256::new(),
        };
        {
            let mut writer = Sha256Writer {
                writer: &mut sink.writer,
                sha256: &mut sink.sha256,
            };
            writer.write_all(CODE_DICTIONARY_MAGIC)?;
            writer.write_all(&CODE_DICTIONARY_FORMAT_VERSION.to_be_bytes())?;
            writer.write_all(&(self.code_dictionary.len() as u64).to_be_bytes())?;
            for code in self.code_dictionary.values() {
                write_code_dictionary_record(&mut writer, code)?;
            }
            writer.flush()?;
        }
        sink.writer.get_ref().sync_all()?;
        Ok(Some(sink))
    }

    fn sink_for_partition(&mut self, partition_index: usize) -> io::Result<&mut PartitionSink> {
        if self.writers[partition_index].is_none() {
            let staged_path = self.staged_path(partition_index);
            let ready_path = self.ready_path(partition_index);
            let file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&staged_path)?;
            self.staged_cleanup_paths.push(staged_path.clone());
            self.writers[partition_index] = Some(PartitionSink {
                staged_path,
                ready_path,
                writer: BufWriter::with_capacity(self.buffer_capacity, file),
                sha256: Sha256::new(),
            });
        }
        self.writers[partition_index]
            .as_mut()
            .ok_or_else(|| io::Error::other("partition writer was not initialized"))
    }
}

struct Sha256Writer<'a, W> {
    writer: &'a mut W,
    sha256: &'a mut Sha256,
}

impl<W: Write> Write for Sha256Writer<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let written = self.writer.write(bytes)?;
        self.sha256.update(&bytes[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl Drop for ServingRunPartitionWriter {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        self.writers.clear();
        for path in self.staged_cleanup_paths.iter().rev() {
            let _ = fs::remove_file(path);
        }
        for path in self.renamed_ready_paths.iter().rev() {
            let _ = fs::remove_file(path);
        }
    }
}

fn sanitize_writer_token(value: &str) -> io::Result<String> {
    let sanitized: String = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect();
    if sanitized.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "partition writer token must not be empty",
        ));
    }
    Ok(sanitized)
}

fn sync_directory(directory: &Path) -> io::Result<()> {
    File::open(directory)?.sync_all()
}

fn write_code_dictionary_record<W: Write>(
    writer: &mut W,
    code: &NaturalLeanCode,
) -> io::Result<()> {
    writer.write_all(&code.code_id)?;
    writer.write_all(&code.coverage_scope_id)?;
    write_dictionary_optional_text(writer, code.reported_code_system.as_deref())?;
    write_dictionary_optional_text(writer, code.reported_code.as_deref())?;
    write_dictionary_optional_text(writer, code.negotiation_arrangement.as_deref())?;
    write_dictionary_optional_text(writer, code.billing_code_type_version.as_deref())?;
    write_dictionary_optional_text(writer, code.name.as_deref())?;
    write_dictionary_optional_text(writer, code.description.as_deref())
}

fn write_dictionary_text<W: Write>(writer: &mut W, value: &str) -> io::Result<()> {
    let length = u32::try_from(value.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "serving code dictionary text exceeds u32 length",
        )
    })?;
    writer.write_all(&length.to_be_bytes())?;
    writer.write_all(value.as_bytes())
}

fn write_dictionary_optional_text<W: Write>(writer: &mut W, value: Option<&str>) -> io::Result<()> {
    match value {
        None => writer.write_all(&[0]),
        Some(value) => {
            writer.write_all(&[1])?;
            write_dictionary_text(writer, value)
        }
    }
}

pub fn read_code_dictionary(path: impl AsRef<Path>) -> io::Result<Vec<NaturalLeanCode>> {
    let path = path.as_ref();
    let file_bytes = path.metadata()?.len();
    read_code_dictionary_inner(path, file_bytes, None)
}

pub fn read_code_dictionary_exact(
    path: impl AsRef<Path>,
    expected_record_count: u64,
    expected_file_bytes: u64,
) -> io::Result<Vec<NaturalLeanCode>> {
    let path = path.as_ref();
    let actual_file_bytes = path.metadata()?.len();
    if actual_file_bytes != expected_file_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "serving code dictionary byte count changed before parsing",
        ));
    }
    read_code_dictionary_inner(path, actual_file_bytes, Some(expected_record_count))
}

fn read_code_dictionary_inner(
    path: &Path,
    file_bytes: u64,
    expected_record_count: Option<u64>,
) -> io::Result<Vec<NaturalLeanCode>> {
    const HEADER_BYTES: u64 = 8 + 4 + 8;
    const MINIMUM_RECORD_BYTES: u64 = 16 + COVERAGE_SCOPE_ID_BYTES as u64 + 6;

    if file_bytes < HEADER_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "serving code dictionary is shorter than its header",
        ));
    }
    let mut reader = BufReader::new(File::open(path)?);
    let mut magic = [0u8; CODE_DICTIONARY_MAGIC.len()];
    reader.read_exact(&mut magic)?;
    if &magic != CODE_DICTIONARY_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "serving code dictionary has invalid magic",
        ));
    }
    let version = read_u32(&mut reader)?;
    if version != CODE_DICTIONARY_FORMAT_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "serving code dictionary version mismatch: expected {CODE_DICTIONARY_FORMAT_VERSION}, got {version}"
            ),
        ));
    }
    let record_count = read_u64(&mut reader)?;
    if expected_record_count.is_some_and(|expected| expected != record_count) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "serving code dictionary record count differs from its authenticated manifest",
        ));
    }
    let minimum_file_bytes = record_count
        .checked_mul(MINIMUM_RECORD_BYTES)
        .and_then(|bytes| bytes.checked_add(HEADER_BYTES))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "serving code dictionary minimum size overflow",
            )
        })?;
    if minimum_file_bytes > file_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "serving code dictionary record count cannot fit in the file",
        ));
    }
    let capacity = usize::try_from(record_count).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "serving code dictionary record count exceeds addressable memory",
        )
    })?;
    let mut records = Vec::new();
    records
        .try_reserve_exact(capacity.min(1_000_000))
        .map_err(|_| io::Error::other("serving code dictionary record allocation failed"))?;
    for _ in 0..record_count {
        let mut code_id = [0u8; 16];
        reader.read_exact(&mut code_id)?;
        let mut coverage_scope_id = [0u8; COVERAGE_SCOPE_ID_BYTES];
        reader.read_exact(&mut coverage_scope_id)?;
        let reported_code_system = read_dictionary_optional_text(&mut reader, file_bytes)?;
        let reported_code = read_dictionary_optional_text(&mut reader, file_bytes)?;
        let negotiation_arrangement = read_dictionary_optional_text(&mut reader, file_bytes)?;
        let billing_code_type_version = read_dictionary_optional_text(&mut reader, file_bytes)?;
        let name = read_dictionary_optional_text(&mut reader, file_bytes)?;
        let description = read_dictionary_optional_text(&mut reader, file_bytes)?;
        let expected = natural_lean_code_identity(
            &coverage_scope_id,
            reported_code_system.as_deref(),
            reported_code.as_deref(),
            negotiation_arrangement.as_deref(),
            billing_code_type_version.as_deref(),
            name.as_deref(),
            description.as_deref(),
        );
        if expected != code_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "serving code dictionary tuple does not match its code identity",
            ));
        }
        records.push(NaturalLeanCode {
            code_id,
            coverage_scope_id,
            reported_code_system,
            reported_code,
            negotiation_arrangement,
            billing_code_type_version,
            name,
            description,
        });
    }
    let mut trailing = [0u8; 1];
    if reader.read(&mut trailing)? != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "serving code dictionary has trailing bytes",
        ));
    }
    Ok(records)
}

fn read_u32<R: Read>(reader: &mut R) -> io::Result<u32> {
    let mut value = [0u8; 4];
    reader.read_exact(&mut value)?;
    Ok(u32::from_be_bytes(value))
}

fn read_u64<R: Read>(reader: &mut R) -> io::Result<u64> {
    let mut value = [0u8; 8];
    reader.read_exact(&mut value)?;
    Ok(u64::from_be_bytes(value))
}

fn read_dictionary_text<R: Read + Seek>(reader: &mut R, file_bytes: u64) -> io::Result<String> {
    let length = u64::from(read_u32(reader)?);
    let position = reader.stream_position()?;
    if position > file_bytes || length > file_bytes - position {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "serving code dictionary text length exceeds remaining file bytes",
        ));
    }
    let length = usize::try_from(length).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "serving code dictionary text length exceeds addressable memory",
        )
    })?;
    let mut value = Vec::new();
    value
        .try_reserve_exact(length)
        .map_err(|_| io::Error::other("serving code dictionary text allocation failed"))?;
    value.resize(length, 0);
    reader.read_exact(&mut value)?;
    String::from_utf8(value).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("serving code dictionary contains invalid UTF-8: {error}"),
        )
    })
}

fn read_dictionary_optional_text<R: Read + Seek>(
    reader: &mut R,
    file_bytes: u64,
) -> io::Result<Option<String>> {
    let mut tag = [0u8; 1];
    reader.read_exact(&mut tag)?;
    match tag[0] {
        0 => Ok(None),
        1 => read_dictionary_text(reader, file_bytes).map(Some),
        value => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("serving code dictionary has invalid nullable tag {value}"),
        )),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExternalSortStats {
    pub input_records: u64,
    pub unique_records: u64,
    pub duplicate_records: u64,
    pub output_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MultiFileSortStats {
    pub input_file_count: u64,
    pub input_records: u64,
    pub unique_records: u64,
    pub duplicate_records: u64,
    pub input_bytes: u64,
    pub spill_bytes: u64,
    pub chunk_count: u64,
    pub output_bytes: u64,
}

trait TemporaryRunMerger {
    fn merge_runs(&mut self, run_paths: Vec<PathBuf>) -> io::Result<PathBuf>;
}

struct BoundedRunAccumulator {
    fan_in: usize,
    levels: Vec<Vec<PathBuf>>,
    initial_run_count: u64,
    #[cfg(test)]
    peak_active_path_count: usize,
}

impl BoundedRunAccumulator {
    fn new(fan_in: usize) -> Self {
        assert!(
            fan_in >= 2,
            "external sort merge fan-in must be at least two"
        );
        Self {
            fan_in,
            levels: Vec::new(),
            initial_run_count: 0,
            #[cfg(test)]
            peak_active_path_count: 0,
        }
    }

    fn push_initial<M: TemporaryRunMerger>(
        &mut self,
        path: PathBuf,
        merger: &mut M,
    ) -> io::Result<()> {
        self.initial_run_count = self.initial_run_count.checked_add(1).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "initial run count overflow")
        })?;
        self.push_at_level(0, path, merger)
    }

    fn push_at_level<M: TemporaryRunMerger>(
        &mut self,
        level: usize,
        path: PathBuf,
        merger: &mut M,
    ) -> io::Result<()> {
        if self.levels.len() == level {
            self.levels.push(Vec::with_capacity(self.fan_in));
        }
        self.levels[level].push(path);
        #[cfg(test)]
        {
            self.peak_active_path_count = self
                .peak_active_path_count
                .max(self.levels.iter().map(Vec::len).sum());
        }
        if self.levels[level].len() < self.fan_in {
            return Ok(());
        }

        let group = std::mem::replace(&mut self.levels[level], Vec::with_capacity(self.fan_in));
        let merged_path = merger.merge_runs(group)?;
        self.push_at_level(level + 1, merged_path, merger)
    }

    fn initial_run_count(&self) -> u64 {
        self.initial_run_count
    }

    fn finish<M: TemporaryRunMerger>(self, merger: &mut M) -> io::Result<Vec<PathBuf>> {
        let mut run_paths = self.levels.into_iter().flatten().collect::<Vec<_>>();
        while run_paths.len() > self.fan_in {
            let group = run_paths.drain(..self.fan_in).collect();
            run_paths.push(merger.merge_runs(group)?);
        }
        Ok(run_paths)
    }

    #[cfg(test)]
    fn peak_active_path_count(&self) -> usize {
        self.peak_active_path_count
    }

    #[cfg(test)]
    fn maximum_active_path_metadata(fan_in: usize) -> usize {
        (fan_in - 1)
            .saturating_mul(u64::BITS as usize)
            .saturating_add(1)
    }
}

pub fn external_sort_dedupe_partition_files(
    input_paths: &[PathBuf],
    output_path: impl AsRef<Path>,
    temporary_directory: impl AsRef<Path>,
    in_memory_record_limit: usize,
    expected_partition: usize,
    partition_count: usize,
) -> io::Result<MultiFileSortStats> {
    external_sort_partition_files_with_mode(
        input_paths,
        output_path.as_ref(),
        temporary_directory.as_ref(),
        in_memory_record_limit,
        expected_partition,
        partition_count,
        true,
    )
}

/// Sorts partition files while retaining every source serving-row occurrence.
pub fn external_sort_partition_files(
    input_paths: &[PathBuf],
    output_path: impl AsRef<Path>,
    temporary_directory: impl AsRef<Path>,
    in_memory_record_limit: usize,
    expected_partition: usize,
    partition_count: usize,
) -> io::Result<MultiFileSortStats> {
    external_sort_partition_files_with_mode(
        input_paths,
        output_path.as_ref(),
        temporary_directory.as_ref(),
        in_memory_record_limit,
        expected_partition,
        partition_count,
        false,
    )
}

fn external_sort_partition_files_with_mode(
    input_paths: &[PathBuf],
    output_path: &Path,
    temporary_directory: &Path,
    in_memory_record_limit: usize,
    expected_partition: usize,
    partition_count: usize,
    dedupe: bool,
) -> io::Result<MultiFileSortStats> {
    if in_memory_record_limit == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "in-memory record limit must be greater than zero",
        ));
    }
    validate_partition_count(partition_count)?;
    if expected_partition >= partition_count {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "expected serving run partition is outside partition_count",
        ));
    }
    fs::create_dir_all(temporary_directory)?;
    let mut temporary_files = TemporaryFiles::default();
    let mut records = Vec::with_capacity(in_memory_record_limit.min(8192));
    let mut runs = BoundedRunAccumulator::new(DEFAULT_SORT_MERGE_FAN_IN);
    let mut merger = ServingRunMerger::<ServingRunRecord> {
        temporary_directory,
        temporary_files: &mut temporary_files,
        dedupe,
        duplicate_records: 0,
        spill_bytes: 0,
        record: std::marker::PhantomData,
    };
    let mut input_records = 0u64;
    let mut input_bytes = 0u64;
    for input_path in input_paths {
        let metadata = input_path.metadata()?;
        if metadata.len() % SERVING_RUN_RECORD_BYTES as u64 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "serving run file {} is not aligned to {SERVING_RUN_RECORD_BYTES} bytes",
                    input_path.display()
                ),
            ));
        }
        input_bytes = input_bytes.saturating_add(metadata.len());
        let mut reader = BufReader::new(File::open(input_path)?);
        while let Some(record) = ServingRunRecord::read_from(&mut reader)? {
            let actual_partition = partition_for_record(&record, partition_count)?;
            if actual_partition != expected_partition {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "serving run record belongs to partition {actual_partition}, expected {expected_partition}"
                    ),
                ));
            }
            input_records = input_records.checked_add(1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "input record count overflow")
            })?;
            records.push(record);
            if records.len() == in_memory_record_limit {
                let path =
                    spill_sorted_run(&mut records, temporary_directory, merger.temporary_files)?;
                merger.record_initial_spill(&path)?;
                runs.push_initial(path, &mut merger)?;
            }
        }
    }
    if !records.is_empty() {
        let path = spill_sorted_run(&mut records, temporary_directory, merger.temporary_files)?;
        merger.record_initial_spill(&path)?;
        runs.push_initial(path, &mut merger)?;
    }
    let initial_chunk_count = runs.initial_run_count();
    let run_paths = runs.finish(&mut merger)?;
    let intermediate_duplicates = merger.duplicate_records;
    let spill_bytes = merger.spill_bytes;
    let output_parent = usable_parent(output_path);
    fs::create_dir_all(output_parent)?;
    let (staged_output_path, staged_output) =
        create_unique_file(output_parent, "partition-output")?;
    temporary_files.track(staged_output_path.clone());
    let mut output = BufWriter::with_capacity(DEFAULT_SORT_BUFFER_BYTES, staged_output);
    let (output_records, final_duplicates) =
        merge_sorted_runs::<ServingRunRecord, _>(&run_paths, &mut output, dedupe)?;
    let duplicate_records = if dedupe {
        intermediate_duplicates.saturating_add(final_duplicates)
    } else {
        final_duplicates
    };
    output.flush()?;
    output.get_ref().sync_all()?;
    drop(output);
    temporary_files.remove_all(&run_paths)?;
    let merged_input_records = if dedupe {
        output_records.saturating_add(duplicate_records)
    } else {
        output_records
    };
    if merged_input_records != input_records {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "serving run partition merge count mismatch",
        ));
    }
    let unique_records = output_records.saturating_sub(if dedupe { 0 } else { duplicate_records });
    let output_bytes = output_records
        .checked_mul(SERVING_RUN_RECORD_BYTES as u64)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "output byte count overflow"))?;
    fs::rename(&staged_output_path, output_path)?;
    temporary_files.release(&staged_output_path);
    Ok(MultiFileSortStats {
        input_file_count: input_paths.len() as u64,
        input_records,
        unique_records,
        duplicate_records,
        input_bytes,
        spill_bytes,
        chunk_count: initial_chunk_count,
        output_bytes,
    })
}

/// Tags scanner run records with manifest provenance and sorts without deduplication.
pub fn external_sort_tagged_partition_files(
    input_paths: &[(PathBuf, u32)],
    output_path: impl AsRef<Path>,
    temporary_directory: impl AsRef<Path>,
    in_memory_record_limit: usize,
    expected_partition: usize,
    partition_count: usize,
    codec: TaggedServingRunCodec,
) -> io::Result<MultiFileSortStats> {
    if in_memory_record_limit == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "in-memory record limit must be greater than zero",
        ));
    }
    validate_partition_count(partition_count)?;
    if expected_partition >= partition_count {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "expected serving run partition is outside partition_count",
        ));
    }
    let output_path = output_path.as_ref();
    let temporary_directory = temporary_directory.as_ref();
    fs::create_dir_all(temporary_directory)?;
    let mut temporary_files = TemporaryFiles::default();
    let mut records = Vec::with_capacity(in_memory_record_limit.min(8192));
    let mut runs = BoundedRunAccumulator::new(DEFAULT_SORT_MERGE_FAN_IN);
    let mut merger = TaggedRunMerger {
        temporary_directory,
        temporary_files: &mut temporary_files,
        codec,
        spill_bytes: 0,
    };
    let mut input_records = 0u64;
    let mut input_bytes = 0u64;
    for (input_path, source_key) in input_paths {
        if u64::from(*source_key) >= codec.source_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "serving run source_key {source_key} is outside source_count {}",
                    codec.source_count
                ),
            ));
        }
        let metadata = input_path.metadata()?;
        if metadata.len() % SERVING_RUN_RECORD_BYTES as u64 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "serving run file {} is not aligned to {SERVING_RUN_RECORD_BYTES} bytes",
                    input_path.display()
                ),
            ));
        }
        input_bytes = input_bytes.saturating_add(metadata.len());
        let mut reader = BufReader::new(File::open(input_path)?);
        while let Some(record) = ServingRunRecord::read_from(&mut reader)? {
            let actual_partition = partition_for_record(&record, partition_count)?;
            if actual_partition != expected_partition {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "serving run record belongs to partition {actual_partition}, expected {expected_partition}"
                    ),
                ));
            }
            input_records = input_records.checked_add(1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "input record count overflow")
            })?;
            records.push(TaggedServingRunRecord {
                record,
                source_key: *source_key,
            });
            if records.len() == in_memory_record_limit {
                let path = spill_tagged_sorted_run(
                    &mut records,
                    temporary_directory,
                    merger.temporary_files,
                    codec,
                )?;
                merger.record_initial_spill(&path)?;
                runs.push_initial(path, &mut merger)?;
            }
        }
    }
    if !records.is_empty() {
        let path = spill_tagged_sorted_run(
            &mut records,
            temporary_directory,
            merger.temporary_files,
            codec,
        )?;
        merger.record_initial_spill(&path)?;
        runs.push_initial(path, &mut merger)?;
    }
    let initial_chunk_count = runs.initial_run_count();
    let run_paths = runs.finish(&mut merger)?;
    let spill_bytes = merger.spill_bytes;
    let output_parent = usable_parent(output_path);
    fs::create_dir_all(output_parent)?;
    let (staged_output_path, staged_output) =
        create_unique_file(output_parent, "tagged-partition-output")?;
    temporary_files.track(staged_output_path.clone());
    let mut output = BufWriter::with_capacity(DEFAULT_SORT_BUFFER_BYTES, staged_output);
    let (output_records, duplicate_records) =
        merge_tagged_sorted_runs(&run_paths, &mut output, codec)?;
    output.flush()?;
    output.get_ref().sync_all()?;
    drop(output);
    temporary_files.remove_all(&run_paths)?;
    if output_records != input_records {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "tagged serving run partition merge count mismatch",
        ));
    }
    let tagged_record_bytes = codec.record_bytes as u64;
    let output_bytes = output_records
        .checked_mul(tagged_record_bytes)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "output byte count overflow"))?;
    fs::rename(&staged_output_path, output_path)?;
    temporary_files.release(&staged_output_path);
    Ok(MultiFileSortStats {
        input_file_count: input_paths.len() as u64,
        input_records,
        unique_records: output_records.saturating_sub(duplicate_records),
        duplicate_records,
        input_bytes,
        spill_bytes,
        chunk_count: initial_chunk_count,
        output_bytes,
    })
}

pub const DENSE_ID_RECORD_BYTES: usize = 16;
pub const PROVIDER_IDENTITY_RECORD_BYTES: usize = 20;
pub const ASSIGNED_SERVING_RECORD_BYTES: usize = 20;
pub const PROVIDER_CODE_PAIR_RECORD_BYTES: usize = 8;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct DenseIdRecord(pub [u8; 16]);

#[derive(Clone, Copy, Debug)]
pub struct ProviderIdentityRecord {
    pub id: [u8; 16],
    pub provider_count: u32,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct AssignedServingRecord(pub [u8; ASSIGNED_SERVING_RECORD_BYTES]);

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ProviderCodePairRecord([u8; PROVIDER_CODE_PAIR_RECORD_BYTES]);

impl PartialEq for ProviderIdentityRecord {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for ProviderIdentityRecord {}

impl PartialOrd for ProviderIdentityRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ProviderIdentityRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

trait FixedSortRecord: Copy + Ord {
    const BYTE_COUNT: usize;

    fn read_from<R: Read>(reader: &mut R) -> io::Result<Option<Self>>;
    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()>;
    fn validate_duplicate(&self, duplicate: &Self) -> io::Result<()>;
}

impl FixedSortRecord for DenseIdRecord {
    const BYTE_COUNT: usize = DENSE_ID_RECORD_BYTES;

    fn read_from<R: Read>(reader: &mut R) -> io::Result<Option<Self>> {
        read_fixed_bytes(reader, "dense identity").map(|value| value.map(Self))
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(&self.0)
    }

    fn validate_duplicate(&self, _duplicate: &Self) -> io::Result<()> {
        Ok(())
    }
}

impl FixedSortRecord for ProviderIdentityRecord {
    const BYTE_COUNT: usize = PROVIDER_IDENTITY_RECORD_BYTES;

    fn read_from<R: Read>(reader: &mut R) -> io::Result<Option<Self>> {
        let Some(encoded) =
            read_fixed_bytes::<_, PROVIDER_IDENTITY_RECORD_BYTES>(reader, "provider identity")?
        else {
            return Ok(None);
        };
        let mut id = [0u8; 16];
        id.copy_from_slice(&encoded[..16]);
        let mut provider_count = [0u8; 4];
        provider_count.copy_from_slice(&encoded[16..]);
        Ok(Some(Self {
            id,
            provider_count: u32::from_be_bytes(provider_count),
        }))
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(&self.id)?;
        writer.write_all(&self.provider_count.to_be_bytes())
    }

    fn validate_duplicate(&self, duplicate: &Self) -> io::Result<()> {
        if self.provider_count != duplicate.provider_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "provider identity has conflicting provider_count values {} and {}",
                    self.provider_count, duplicate.provider_count
                ),
            ));
        }
        Ok(())
    }
}

impl FixedSortRecord for AssignedServingRecord {
    const BYTE_COUNT: usize = ASSIGNED_SERVING_RECORD_BYTES;

    fn read_from<R: Read>(reader: &mut R) -> io::Result<Option<Self>> {
        read_fixed_bytes(reader, "assigned serving record").map(|value| value.map(Self))
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(&self.0)
    }

    fn validate_duplicate(&self, _duplicate: &Self) -> io::Result<()> {
        Ok(())
    }
}

impl FixedSortRecord for ProviderCodePairRecord {
    const BYTE_COUNT: usize = PROVIDER_CODE_PAIR_RECORD_BYTES;

    fn read_from<R: Read>(reader: &mut R) -> io::Result<Option<Self>> {
        read_fixed_bytes(reader, "provider-code pair").map(|value| value.map(Self))
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(&self.0)
    }

    fn validate_duplicate(&self, _duplicate: &Self) -> io::Result<()> {
        Ok(())
    }
}

pub fn external_sort_dense_ids(
    input_paths: &[PathBuf],
    output_path: impl AsRef<Path>,
    temporary_directory: impl AsRef<Path>,
    in_memory_record_limit: usize,
) -> io::Result<MultiFileSortStats> {
    external_sort_fixed_records::<DenseIdRecord>(
        input_paths,
        output_path.as_ref(),
        temporary_directory.as_ref(),
        in_memory_record_limit,
        true,
    )
}

pub fn external_sort_provider_identities(
    input_paths: &[PathBuf],
    output_path: impl AsRef<Path>,
    temporary_directory: impl AsRef<Path>,
    in_memory_record_limit: usize,
) -> io::Result<MultiFileSortStats> {
    external_sort_fixed_records::<ProviderIdentityRecord>(
        input_paths,
        output_path.as_ref(),
        temporary_directory.as_ref(),
        in_memory_record_limit,
        true,
    )
}

/// Merge provider-identity files that were independently sorted and deduplicated.
///
/// The finalizer uses this after partition-local sorting. It avoids sorting the
/// complete source-row population a second time while still deduplicating
/// provider identities that occur in more than one code partition.
pub fn merge_sorted_provider_identities(
    input_paths: &[PathBuf],
    output_path: impl AsRef<Path>,
) -> io::Result<MultiFileSortStats> {
    let output_path = output_path.as_ref();
    let output_parent = usable_parent(output_path);
    fs::create_dir_all(output_parent)?;

    let mut input_records = 0u64;
    let mut input_bytes = 0u64;
    for input_path in input_paths {
        let bytes = input_path.metadata()?.len();
        if bytes % PROVIDER_IDENTITY_RECORD_BYTES as u64 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "sorted provider identity file {} is not aligned to {} bytes",
                    input_path.display(),
                    PROVIDER_IDENTITY_RECORD_BYTES
                ),
            ));
        }
        input_bytes = input_bytes.saturating_add(bytes);
        input_records = input_records.saturating_add(bytes / PROVIDER_IDENTITY_RECORD_BYTES as u64);
    }

    let mut temporary_files = TemporaryFiles::default();
    let (staged_path, staged_file) = create_unique_file(output_parent, "provider-output")?;
    temporary_files.track(staged_path.clone());
    let mut writer = BufWriter::with_capacity(DEFAULT_SORT_BUFFER_BYTES, staged_file);
    let (unique_records, duplicate_records) =
        merge_fixed_sort_runs::<ProviderIdentityRecord, _>(input_paths, &mut writer, true)?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    drop(writer);
    if unique_records.saturating_add(duplicate_records) != input_records {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "sorted provider identity merge count mismatch",
        ));
    }
    let output_bytes = unique_records
        .checked_mul(PROVIDER_IDENTITY_RECORD_BYTES as u64)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "provider identity output byte count overflow",
            )
        })?;
    fs::rename(&staged_path, output_path)?;
    temporary_files.release(&staged_path);
    Ok(MultiFileSortStats {
        input_file_count: input_paths.len() as u64,
        input_records,
        unique_records,
        duplicate_records,
        input_bytes,
        spill_bytes: 0,
        chunk_count: 0,
        output_bytes,
    })
}

/// Sorts and deduplicates fixed-width provider/code key pairs by encoded byte order.
pub fn external_sort_provider_code_pairs(
    input_paths: &[PathBuf],
    output_path: impl AsRef<Path>,
    temporary_directory: impl AsRef<Path>,
    in_memory_record_limit: usize,
) -> io::Result<MultiFileSortStats> {
    external_sort_fixed_records::<ProviderCodePairRecord>(
        input_paths,
        output_path.as_ref(),
        temporary_directory.as_ref(),
        in_memory_record_limit,
        true,
    )
}

/// Sorts dense assigned rows while preserving every duplicate occurrence.
pub fn external_sort_assigned_serving_records(
    input_paths: &[PathBuf],
    output_path: impl AsRef<Path>,
    temporary_directory: impl AsRef<Path>,
    in_memory_record_limit: usize,
) -> io::Result<MultiFileSortStats> {
    external_sort_fixed_records::<AssignedServingRecord>(
        input_paths,
        output_path.as_ref(),
        temporary_directory.as_ref(),
        in_memory_record_limit,
        false,
    )
}

pub fn external_sort_lexicographic_records(
    input_paths: &[PathBuf],
    output_path: impl AsRef<Path>,
    temporary_directory: impl AsRef<Path>,
    record_bytes: usize,
    in_memory_record_limit: usize,
    dedupe: bool,
) -> io::Result<MultiFileSortStats> {
    if record_bytes == 0 || in_memory_record_limit == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "lexicographic external sort requires positive record and memory limits",
        ));
    }
    let temporary_directory = temporary_directory.as_ref();
    fs::create_dir_all(temporary_directory)?;
    let mut temporary_files = TemporaryFiles::default();
    let mut records: Vec<Vec<u8>> = Vec::with_capacity(in_memory_record_limit.min(8192));
    let mut runs = BoundedRunAccumulator::new(DEFAULT_SORT_MERGE_FAN_IN);
    let mut merger = LexicographicRunMerger {
        temporary_directory,
        temporary_files: &mut temporary_files,
        record_bytes,
        dedupe,
        duplicate_records: 0,
        spill_bytes: 0,
    };
    let mut input_records = 0u64;
    let mut input_bytes = 0u64;
    for input_path in input_paths {
        let bytes = input_path.metadata()?.len();
        if bytes % record_bytes as u64 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "lexicographic input {} is not aligned to {record_bytes} bytes",
                    input_path.display()
                ),
            ));
        }
        input_bytes = input_bytes.saturating_add(bytes);
        let mut reader = BufReader::new(File::open(input_path)?);
        while let Some(record) = read_dynamic_fixed_record(&mut reader, record_bytes)? {
            input_records = input_records.saturating_add(1);
            records.push(record);
            if records.len() == in_memory_record_limit {
                let path = spill_lexicographic_run(
                    &mut records,
                    temporary_directory,
                    merger.temporary_files,
                    dedupe,
                )?;
                merger.record_initial_spill(&path)?;
                runs.push_initial(path, &mut merger)?;
            }
        }
    }
    if !records.is_empty() {
        let path = spill_lexicographic_run(
            &mut records,
            temporary_directory,
            merger.temporary_files,
            dedupe,
        )?;
        merger.record_initial_spill(&path)?;
        runs.push_initial(path, &mut merger)?;
    }
    let initial_chunk_count = runs.initial_run_count();
    let run_paths = runs.finish(&mut merger)?;
    let intermediate_duplicates = merger.duplicate_records;
    let spill_bytes = merger.spill_bytes;
    let output_path = output_path.as_ref();
    fs::create_dir_all(usable_parent(output_path))?;
    let (staged_path, staged_file) = create_unique_file(usable_parent(output_path), "lex-output")?;
    temporary_files.track(staged_path.clone());
    let mut writer = BufWriter::with_capacity(DEFAULT_SORT_BUFFER_BYTES, staged_file);
    let (unique_records, final_duplicates) =
        merge_lexicographic_runs(&run_paths, &mut writer, record_bytes, dedupe)?;
    let duplicate_records = intermediate_duplicates.saturating_add(final_duplicates);
    writer.flush()?;
    writer.get_ref().sync_all()?;
    drop(writer);
    temporary_files.remove_all(&run_paths)?;
    if unique_records.saturating_add(duplicate_records) != input_records {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "lexicographic external sort count mismatch",
        ));
    }
    let output_bytes = unique_records
        .checked_mul(record_bytes as u64)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "output byte count overflow"))?;
    fs::rename(&staged_path, output_path)?;
    temporary_files.release(&staged_path);
    Ok(MultiFileSortStats {
        input_file_count: input_paths.len() as u64,
        input_records,
        unique_records,
        duplicate_records,
        input_bytes,
        spill_bytes,
        chunk_count: initial_chunk_count,
        output_bytes,
    })
}

fn read_dynamic_fixed_record<R: Read>(
    reader: &mut R,
    record_bytes: usize,
) -> io::Result<Option<Vec<u8>>> {
    let mut record = vec![0u8; record_bytes];
    let mut offset = 0usize;
    while offset < record_bytes {
        match reader.read(&mut record[offset..]) {
            Ok(0) if offset == 0 => return Ok(None),
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "lexicographic input ended with a partial record",
                ));
            }
            Ok(read_bytes) => offset += read_bytes,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(Some(record))
}

fn spill_lexicographic_run(
    records: &mut Vec<Vec<u8>>,
    temporary_directory: &Path,
    temporary_files: &mut TemporaryFiles,
    _dedupe: bool,
) -> io::Result<PathBuf> {
    records.sort_unstable();
    let (path, file) = create_unique_file(temporary_directory, "lex-run")?;
    temporary_files.track(path.clone());
    let mut writer = BufWriter::with_capacity(DEFAULT_SORT_BUFFER_BYTES, file);
    for record in records.iter() {
        writer.write_all(record)?;
    }
    writer.flush()?;
    records.clear();
    Ok(path)
}

fn merge_lexicographic_runs<W: Write>(
    run_paths: &[PathBuf],
    writer: &mut W,
    record_bytes: usize,
    dedupe: bool,
) -> io::Result<(u64, u64)> {
    let mut readers = run_paths
        .iter()
        .map(|path| File::open(path).map(BufReader::new))
        .collect::<io::Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::with_capacity(readers.len());
    for (reader_index, reader) in readers.iter_mut().enumerate() {
        if let Some(record) = read_dynamic_fixed_record(reader, record_bytes)? {
            heap.push(Reverse((record, reader_index)));
        }
    }
    let mut previous: Option<Vec<u8>> = None;
    let mut output_records = 0u64;
    let mut duplicate_records = 0u64;
    while let Some(Reverse((record, reader_index))) = heap.pop() {
        if dedupe && previous.as_ref() == Some(&record) {
            duplicate_records = duplicate_records.saturating_add(1);
        } else {
            writer.write_all(&record)?;
            output_records = output_records.saturating_add(1);
            previous = Some(record);
        }
        if let Some(next_record) =
            read_dynamic_fixed_record(&mut readers[reader_index], record_bytes)?
        {
            heap.push(Reverse((next_record, reader_index)));
        }
    }
    Ok((output_records, duplicate_records))
}

struct LexicographicRunMerger<'a> {
    temporary_directory: &'a Path,
    temporary_files: &'a mut TemporaryFiles,
    record_bytes: usize,
    dedupe: bool,
    duplicate_records: u64,
    spill_bytes: u64,
}

impl LexicographicRunMerger<'_> {
    fn record_initial_spill(&mut self, path: &Path) -> io::Result<()> {
        self.spill_bytes = self.spill_bytes.saturating_add(path.metadata()?.len());
        Ok(())
    }
}

impl TemporaryRunMerger for LexicographicRunMerger<'_> {
    fn merge_runs(&mut self, run_paths: Vec<PathBuf>) -> io::Result<PathBuf> {
        let (path, file) = create_unique_file(self.temporary_directory, "lex-merge")?;
        self.temporary_files.track(path.clone());
        let mut writer = BufWriter::with_capacity(DEFAULT_SORT_BUFFER_BYTES, file);
        let (_output_records, group_duplicates) =
            merge_lexicographic_runs(&run_paths, &mut writer, self.record_bytes, self.dedupe)?;
        writer.flush()?;
        drop(writer);
        let output_bytes = path.metadata()?.len();
        self.temporary_files.remove_all(&run_paths)?;
        self.duplicate_records = self.duplicate_records.saturating_add(group_duplicates);
        self.spill_bytes = self.spill_bytes.saturating_add(output_bytes);
        Ok(path)
    }
}

fn read_fixed_bytes<R: Read, const N: usize>(
    reader: &mut R,
    label: &str,
) -> io::Result<Option<[u8; N]>> {
    let mut encoded = [0u8; N];
    let mut offset = 0usize;
    while offset < N {
        match reader.read(&mut encoded[offset..]) {
            Ok(0) if offset == 0 => return Ok(None),
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{label} file ended with a partial record"),
                ));
            }
            Ok(read_bytes) => offset += read_bytes,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(Some(encoded))
}

fn external_sort_fixed_records<T: FixedSortRecord>(
    input_paths: &[PathBuf],
    output_path: &Path,
    temporary_directory: &Path,
    in_memory_record_limit: usize,
    dedupe: bool,
) -> io::Result<MultiFileSortStats> {
    if in_memory_record_limit == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "in-memory record limit must be greater than zero",
        ));
    }
    fs::create_dir_all(temporary_directory)?;
    let mut temporary_files = TemporaryFiles::default();
    let allocation_bytes = in_memory_record_limit
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "fixed-record sort allocation size overflow",
            )
        })?;
    let mut records = Vec::new();
    records
        .try_reserve_exact(in_memory_record_limit)
        .map_err(|error| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!(
                    "unable to reserve {allocation_bytes} bytes for fixed-record sorting: {error}"
                ),
            )
        })?;
    let mut runs = BoundedRunAccumulator::new(DEFAULT_SORT_MERGE_FAN_IN);
    let mut merger = FixedRunMerger::<T> {
        temporary_directory,
        temporary_files: &mut temporary_files,
        dedupe,
        duplicate_records: 0,
        spill_bytes: 0,
        record: std::marker::PhantomData,
    };
    let mut input_records = 0u64;
    let mut input_bytes = 0u64;
    for input_path in input_paths {
        let bytes = input_path.metadata()?.len();
        if bytes % T::BYTE_COUNT as u64 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "fixed record file {} is not aligned to {} bytes",
                    input_path.display(),
                    T::BYTE_COUNT
                ),
            ));
        }
        input_bytes = input_bytes.saturating_add(bytes);
        let mut reader = BufReader::new(File::open(input_path)?);
        while let Some(record) = T::read_from(&mut reader)? {
            input_records = input_records.saturating_add(1);
            records.push(record);
            if records.len() == in_memory_record_limit {
                let path = spill_fixed_sort_run(
                    &mut records,
                    temporary_directory,
                    merger.temporary_files,
                )?;
                merger.record_initial_spill(&path)?;
                runs.push_initial(path, &mut merger)?;
            }
        }
    }
    if !records.is_empty() {
        let path = spill_fixed_sort_run(&mut records, temporary_directory, merger.temporary_files)?;
        merger.record_initial_spill(&path)?;
        runs.push_initial(path, &mut merger)?;
    }
    drop(records);
    let initial_chunk_count = runs.initial_run_count();
    let run_paths = runs.finish(&mut merger)?;
    let intermediate_duplicates = merger.duplicate_records;
    let spill_bytes = merger.spill_bytes;
    fs::create_dir_all(usable_parent(output_path))?;
    let (staged_path, staged_file) =
        create_unique_file(usable_parent(output_path), "dense-output")?;
    temporary_files.track(staged_path.clone());
    let mut writer = BufWriter::with_capacity(DEFAULT_SORT_BUFFER_BYTES, staged_file);
    let (unique_records, final_duplicates) =
        merge_fixed_sort_runs::<T, _>(&run_paths, &mut writer, dedupe)?;
    let duplicate_records = intermediate_duplicates.saturating_add(final_duplicates);
    writer.flush()?;
    writer.get_ref().sync_all()?;
    drop(writer);
    temporary_files.remove_all(&run_paths)?;
    if unique_records.saturating_add(duplicate_records) != input_records {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "fixed record external sort count mismatch",
        ));
    }
    let output_bytes = unique_records
        .checked_mul(T::BYTE_COUNT as u64)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "output byte count overflow"))?;
    fs::rename(&staged_path, output_path)?;
    temporary_files.release(&staged_path);
    Ok(MultiFileSortStats {
        input_file_count: input_paths.len() as u64,
        input_records,
        unique_records,
        duplicate_records,
        input_bytes,
        spill_bytes,
        chunk_count: initial_chunk_count,
        output_bytes,
    })
}

fn spill_fixed_sort_run<T: FixedSortRecord>(
    records: &mut Vec<T>,
    temporary_directory: &Path,
    temporary_files: &mut TemporaryFiles,
) -> io::Result<PathBuf> {
    records.sort_unstable();
    let (path, file) = create_unique_file(temporary_directory, "dense-run")?;
    temporary_files.track(path.clone());
    let mut writer = BufWriter::with_capacity(DEFAULT_SORT_BUFFER_BYTES, file);
    for record in records.iter() {
        record.write_to(&mut writer)?;
    }
    writer.flush()?;
    records.clear();
    Ok(path)
}

fn merge_fixed_sort_runs<T: FixedSortRecord, W: Write>(
    run_paths: &[PathBuf],
    writer: &mut W,
    dedupe: bool,
) -> io::Result<(u64, u64)> {
    let mut readers = run_paths
        .iter()
        .map(|path| File::open(path).map(BufReader::new))
        .collect::<io::Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::with_capacity(readers.len());
    for (reader_index, reader) in readers.iter_mut().enumerate() {
        if let Some(record) = T::read_from(reader)? {
            heap.push(Reverse((record, reader_index)));
        }
    }
    let mut previous: Option<T> = None;
    let mut unique_records = 0u64;
    let mut duplicate_records = 0u64;
    while let Some(Reverse((record, reader_index))) = heap.pop() {
        if let Some(previous_record) = previous {
            if dedupe && record == previous_record {
                previous_record.validate_duplicate(&record)?;
                duplicate_records = duplicate_records.saturating_add(1);
            } else {
                record.write_to(writer)?;
                unique_records = unique_records.saturating_add(1);
                previous = Some(record);
            }
        } else {
            record.write_to(writer)?;
            unique_records = 1;
            previous = Some(record);
        }
        if let Some(next_record) = T::read_from(&mut readers[reader_index])? {
            if next_record < record {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "fixed-record merge input is not sorted",
                ));
            }
            heap.push(Reverse((next_record, reader_index)));
        }
    }
    Ok((unique_records, duplicate_records))
}

struct FixedRunMerger<'a, T> {
    temporary_directory: &'a Path,
    temporary_files: &'a mut TemporaryFiles,
    dedupe: bool,
    duplicate_records: u64,
    spill_bytes: u64,
    record: std::marker::PhantomData<T>,
}

impl<T: FixedSortRecord> FixedRunMerger<'_, T> {
    fn record_initial_spill(&mut self, path: &Path) -> io::Result<()> {
        self.spill_bytes = self.spill_bytes.saturating_add(path.metadata()?.len());
        Ok(())
    }
}

impl<T: FixedSortRecord> TemporaryRunMerger for FixedRunMerger<'_, T> {
    fn merge_runs(&mut self, run_paths: Vec<PathBuf>) -> io::Result<PathBuf> {
        let (path, file) = create_unique_file(self.temporary_directory, "fixed-merge")?;
        self.temporary_files.track(path.clone());
        let mut writer = BufWriter::with_capacity(DEFAULT_SORT_BUFFER_BYTES, file);
        let (_output_records, group_duplicates) =
            merge_fixed_sort_runs::<T, _>(&run_paths, &mut writer, self.dedupe)?;
        writer.flush()?;
        drop(writer);
        let output_bytes = path.metadata()?.len();
        self.temporary_files.remove_all(&run_paths)?;
        self.duplicate_records = self.duplicate_records.saturating_add(group_duplicates);
        self.spill_bytes = self.spill_bytes.saturating_add(output_bytes);
        Ok(path)
    }
}

/// Sorts and strictly deduplicates one fixed-width partition.
pub fn external_sort_dedupe_partition(
    input_path: impl AsRef<Path>,
    output_path: impl AsRef<Path>,
    temporary_directory: impl AsRef<Path>,
    in_memory_record_limit: usize,
) -> io::Result<ExternalSortStats> {
    if in_memory_record_limit == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "in-memory record limit must be greater than zero",
        ));
    }

    let input_path = input_path.as_ref();
    let output_path = output_path.as_ref();
    let temporary_directory = temporary_directory.as_ref();
    fs::create_dir_all(temporary_directory)?;
    let mut temporary_files = TemporaryFiles::default();

    let (run_paths, input_records, intermediate_duplicates) = create_sorted_runs(
        input_path,
        temporary_directory,
        in_memory_record_limit,
        &mut temporary_files,
    )?;

    let output_parent = usable_parent(output_path);
    fs::create_dir_all(output_parent)?;
    let (staged_output_path, staged_output) = create_unique_file(output_parent, "output")?;
    temporary_files.track(staged_output_path.clone());
    let mut output = BufWriter::with_capacity(DEFAULT_SORT_BUFFER_BYTES, staged_output);
    let (unique_records, final_duplicates) =
        merge_sorted_runs::<ServingRunRecord, _>(&run_paths, &mut output, true)?;
    let duplicate_records = intermediate_duplicates.saturating_add(final_duplicates);
    output.flush()?;
    drop(output);
    temporary_files.remove_all(&run_paths)?;

    let merged_records = unique_records
        .checked_add(duplicate_records)
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "merged record count overflow")
        })?;
    if merged_records != input_records {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("merged record count mismatch: expected {input_records}, got {merged_records}"),
        ));
    }

    let output_bytes = unique_records
        .checked_mul(SERVING_RUN_RECORD_BYTES as u64)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "output byte count overflow"))?;
    fs::rename(&staged_output_path, output_path)?;
    temporary_files.release(&staged_output_path);

    Ok(ExternalSortStats {
        input_records,
        unique_records,
        duplicate_records,
        output_bytes,
    })
}

fn create_sorted_runs(
    input_path: &Path,
    temporary_directory: &Path,
    in_memory_record_limit: usize,
    temporary_files: &mut TemporaryFiles,
) -> io::Result<(Vec<PathBuf>, u64, u64)> {
    let input = File::open(input_path)?;
    let mut input = BufReader::with_capacity(DEFAULT_SORT_BUFFER_BYTES, input);
    let mut records = Vec::with_capacity(in_memory_record_limit.min(8192));
    let mut runs = BoundedRunAccumulator::new(DEFAULT_SORT_MERGE_FAN_IN);
    let mut merger = ServingRunMerger::<ServingRunRecord> {
        temporary_directory,
        temporary_files,
        dedupe: true,
        duplicate_records: 0,
        spill_bytes: 0,
        record: std::marker::PhantomData,
    };
    let mut input_records = 0u64;

    while let Some(record) = ServingRunRecord::read_from(&mut input)? {
        input_records = input_records.checked_add(1).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "input record count overflow")
        })?;
        records.push(record);
        if records.len() == in_memory_record_limit {
            let path = spill_sorted_run(&mut records, temporary_directory, merger.temporary_files)?;
            merger.record_initial_spill(&path)?;
            runs.push_initial(path, &mut merger)?;
        }
    }
    if !records.is_empty() {
        let path = spill_sorted_run(&mut records, temporary_directory, merger.temporary_files)?;
        merger.record_initial_spill(&path)?;
        runs.push_initial(path, &mut merger)?;
    }

    let run_paths = runs.finish(&mut merger)?;
    Ok((run_paths, input_records, merger.duplicate_records))
}

fn spill_sorted_run<T: ServingSortRecord>(
    records: &mut Vec<T>,
    temporary_directory: &Path,
    temporary_files: &mut TemporaryFiles,
) -> io::Result<PathBuf> {
    records.sort_unstable();
    let (path, file) = create_unique_file(temporary_directory, "run")?;
    temporary_files.track(path.clone());
    let mut writer = BufWriter::with_capacity(DEFAULT_SORT_BUFFER_BYTES, file);
    for record in records.iter() {
        record.write_to(&mut writer)?;
    }
    writer.flush()?;
    records.clear();
    Ok(path)
}

fn merge_sorted_runs<T: ServingSortRecord, W: Write>(
    run_paths: &[PathBuf],
    output: &mut W,
    dedupe: bool,
) -> io::Result<(u64, u64)> {
    let mut readers = run_paths
        .iter()
        .map(|path| File::open(path).map(BufReader::new))
        .collect::<io::Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::with_capacity(readers.len());
    for (run_index, reader) in readers.iter_mut().enumerate() {
        if let Some(record) = T::read_from(reader)? {
            heap.push(Reverse((record, run_index)));
        }
    }

    let mut previous = None;
    let mut output_records = 0u64;
    let mut duplicate_records = 0u64;
    while let Some(Reverse((record, run_index))) = heap.pop() {
        let is_duplicate =
            previous.is_some_and(|previous_record| record.same_identity(&previous_record));
        if let Some(previous_record) = previous {
            if is_duplicate {
                if record.provider_count() != previous_record.provider_count() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "duplicate serving identity has conflicting provider_count values {} and {}",
                            previous_record.provider_count(), record.provider_count()
                        ),
                    ));
                }
                duplicate_records = duplicate_records.checked_add(1).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "duplicate record count overflow",
                    )
                })?;
            }
        }
        if !is_duplicate || !dedupe {
            record.write_to(output)?;
            output_records = output_records.checked_add(1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "output record count overflow")
            })?;
        }
        previous = Some(record);

        if let Some(next_record) = T::read_from(&mut readers[run_index])? {
            heap.push(Reverse((next_record, run_index)));
        }
    }

    Ok((output_records, duplicate_records))
}

struct ServingRunMerger<'a, T> {
    temporary_directory: &'a Path,
    temporary_files: &'a mut TemporaryFiles,
    dedupe: bool,
    duplicate_records: u64,
    spill_bytes: u64,
    record: std::marker::PhantomData<T>,
}

impl<T: ServingSortRecord> ServingRunMerger<'_, T> {
    fn record_initial_spill(&mut self, path: &Path) -> io::Result<()> {
        self.spill_bytes = self.spill_bytes.saturating_add(path.metadata()?.len());
        Ok(())
    }
}

impl<T: ServingSortRecord> TemporaryRunMerger for ServingRunMerger<'_, T> {
    fn merge_runs(&mut self, run_paths: Vec<PathBuf>) -> io::Result<PathBuf> {
        let (path, file) = create_unique_file(self.temporary_directory, "serving-merge")?;
        self.temporary_files.track(path.clone());
        let mut writer = BufWriter::with_capacity(DEFAULT_SORT_BUFFER_BYTES, file);
        let (_output_records, group_duplicates) =
            merge_sorted_runs::<T, _>(&run_paths, &mut writer, self.dedupe)?;
        writer.flush()?;
        drop(writer);
        let output_bytes = path.metadata()?.len();
        self.temporary_files.remove_all(&run_paths)?;
        if self.dedupe {
            self.duplicate_records = self.duplicate_records.saturating_add(group_duplicates);
        }
        self.spill_bytes = self.spill_bytes.saturating_add(output_bytes);
        Ok(path)
    }
}

fn spill_tagged_sorted_run(
    records: &mut Vec<TaggedServingRunRecord>,
    temporary_directory: &Path,
    temporary_files: &mut TemporaryFiles,
    codec: TaggedServingRunCodec,
) -> io::Result<PathBuf> {
    records.sort_unstable();
    let (path, file) = create_unique_file(temporary_directory, "tagged-run")?;
    temporary_files.track(path.clone());
    let mut writer = BufWriter::with_capacity(DEFAULT_SORT_BUFFER_BYTES, file);
    for record in records.iter() {
        record.write_to(&mut writer, codec)?;
    }
    writer.flush()?;
    records.clear();
    Ok(path)
}

fn merge_tagged_sorted_runs<W: Write>(
    run_paths: &[PathBuf],
    output: &mut W,
    codec: TaggedServingRunCodec,
) -> io::Result<(u64, u64)> {
    let mut readers = run_paths
        .iter()
        .map(|path| File::open(path).map(BufReader::new))
        .collect::<io::Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::with_capacity(readers.len());
    for (run_index, reader) in readers.iter_mut().enumerate() {
        if let Some(record) = TaggedServingRunRecord::read_from(reader, codec)? {
            heap.push(Reverse((record, run_index)));
        }
    }

    let mut previous = None;
    let mut output_records = 0u64;
    let mut duplicate_records = 0u64;
    while let Some(Reverse((record, run_index))) = heap.pop() {
        if let Some(previous_record) = previous {
            if record.same_identity(&previous_record) {
                if record.record.provider_count != previous_record.record.provider_count {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "duplicate tagged serving identity has conflicting provider_count values {} and {}",
                            previous_record.record.provider_count, record.record.provider_count
                        ),
                    ));
                }
                duplicate_records = duplicate_records.checked_add(1).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "duplicate tagged record count overflow",
                    )
                })?;
            }
        }
        record.write_to(output, codec)?;
        output_records = output_records.checked_add(1).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "tagged output record count overflow",
            )
        })?;
        previous = Some(record);

        if let Some(next_record) =
            TaggedServingRunRecord::read_from(&mut readers[run_index], codec)?
        {
            heap.push(Reverse((next_record, run_index)));
        }
    }
    Ok((output_records, duplicate_records))
}

struct TaggedRunMerger<'a> {
    temporary_directory: &'a Path,
    temporary_files: &'a mut TemporaryFiles,
    codec: TaggedServingRunCodec,
    spill_bytes: u64,
}

impl TaggedRunMerger<'_> {
    fn record_initial_spill(&mut self, path: &Path) -> io::Result<()> {
        self.spill_bytes = self.spill_bytes.saturating_add(path.metadata()?.len());
        Ok(())
    }
}

impl TemporaryRunMerger for TaggedRunMerger<'_> {
    fn merge_runs(&mut self, run_paths: Vec<PathBuf>) -> io::Result<PathBuf> {
        let (path, file) = create_unique_file(self.temporary_directory, "tagged-merge")?;
        self.temporary_files.track(path.clone());
        let mut writer = BufWriter::with_capacity(DEFAULT_SORT_BUFFER_BYTES, file);
        merge_tagged_sorted_runs(&run_paths, &mut writer, self.codec)?;
        writer.flush()?;
        drop(writer);
        let output_bytes = path.metadata()?.len();
        self.temporary_files.remove_all(&run_paths)?;
        self.spill_bytes = self.spill_bytes.saturating_add(output_bytes);
        Ok(path)
    }
}

#[derive(Default)]
struct TemporaryFiles {
    paths: Vec<PathBuf>,
    #[cfg(test)]
    peak_path_count: usize,
}

impl TemporaryFiles {
    fn track(&mut self, path: PathBuf) {
        debug_assert!(!self.paths.contains(&path));
        self.paths.push(path);
        #[cfg(test)]
        {
            self.peak_path_count = self.peak_path_count.max(self.paths.len());
        }
    }

    fn untrack(&mut self, path: &Path) -> bool {
        let Some(index) = self.paths.iter().position(|tracked| tracked == path) else {
            return false;
        };
        self.paths.swap_remove(index);
        true
    }

    fn remove_all(&mut self, paths: &[PathBuf]) -> io::Result<()> {
        for path in paths {
            match fs::remove_file(path) {
                Ok(()) => {
                    let was_tracked = self.untrack(path);
                    debug_assert!(was_tracked);
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    self.untrack(path);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    fn release(&mut self, path: &Path) {
        let was_tracked = self.untrack(path);
        debug_assert!(was_tracked);
    }

    #[cfg(test)]
    fn active_path_count(&self) -> usize {
        self.paths.len()
    }

    #[cfg(test)]
    fn peak_path_count(&self) -> usize {
        self.peak_path_count
    }
}

impl Drop for TemporaryFiles {
    fn drop(&mut self) {
        for path in self.paths.iter().rev() {
            let _ = fs::remove_file(path);
        }
    }
}

fn create_unique_file(directory: &Path, label: &str) -> io::Result<(PathBuf, File)> {
    loop {
        let id = NEXT_TEMP_FILE_ID.fetch_add(1, AtomicOrdering::Relaxed);
        let path = directory.join(format!(
            ".ptg2-v3-{label}-{}-{id:016x}.tmp",
            std::process::id()
        ));
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => return Ok((path, file)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
}

fn usable_parent(path: &Path) -> &Path {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

    static NEXT_TEST_DIRECTORY_ID: AtomicU64 = AtomicU64::new(0);

    struct TestDirectory {
        path: PathBuf,
    }

    impl TestDirectory {
        fn new(label: &str) -> Self {
            loop {
                let id = NEXT_TEST_DIRECTORY_ID.fetch_add(1, AtomicOrdering::Relaxed);
                let path = std::env::temp_dir().join(format!(
                    "ptg2-v3-runs-{label}-{}-{id:016x}",
                    std::process::id()
                ));
                match fs::create_dir(&path) {
                    Ok(()) => return Self { path },
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                    Err(error) => panic!("failed to create test directory: {error}"),
                }
            }
        }

        fn join(&self, path: &str) -> PathBuf {
            self.path.join(path)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    struct MetadataTestRunMerger<'a> {
        directory: &'a Path,
        temporary_files: &'a mut TemporaryFiles,
    }

    impl TemporaryRunMerger for MetadataTestRunMerger<'_> {
        fn merge_runs(&mut self, run_paths: Vec<PathBuf>) -> io::Result<PathBuf> {
            let (path, file) = create_unique_file(self.directory, "metadata-merge")?;
            drop(file);
            self.temporary_files.track(path.clone());
            self.temporary_files.remove_all(&run_paths)?;
            Ok(path)
        }
    }

    fn record(code: u8, provider: u8, price: u8, provider_count: u32) -> ServingRunRecord {
        ServingRunRecord {
            code_id: [code; 16],
            provider_set_id: [provider; 16],
            price_set_id: [price; 16],
            provider_count,
        }
    }

    fn write_records(path: &Path, records: &[ServingRunRecord]) {
        let file = File::create(path).unwrap();
        let mut writer = BufWriter::new(file);
        for record in records {
            record.write_to(&mut writer).unwrap();
        }
        writer.flush().unwrap();
    }

    fn read_records(path: &Path) -> Vec<ServingRunRecord> {
        let file = File::open(path).unwrap();
        let mut reader = BufReader::new(file);
        let mut records = Vec::new();
        while let Some(record) = ServingRunRecord::read_from(&mut reader).unwrap() {
            records.push(record);
        }
        records
    }

    fn directory_is_empty(path: &Path) -> bool {
        fs::read_dir(path).unwrap().next().is_none()
    }

    #[test]
    fn audit_candidates_include_every_small_stream_row_in_raw_big_endian_format() {
        let base = TestDirectory::new("audit-small");
        let path = base.join("audit-candidates.bin");
        let mut selector = AuditCandidateSelector::new(3);
        selector.observe(1, 11, 21, 41, 31).unwrap();
        selector.observe(2, 12, 22, 42, 32).unwrap();
        selector.observe(3, 13, 23, 43, 33).unwrap();
        let records = selector.finish().unwrap();

        let summary = write_audit_candidate_file(&path, 3, records).unwrap();

        assert_eq!(summary.byte_count, 3 * AUDIT_CANDIDATE_RECORD_BYTES as u64);
        let bytes = fs::read(&path).unwrap();
        assert_eq!(bytes.len(), 3 * AUDIT_CANDIDATE_RECORD_BYTES);
        assert_eq!(
            &bytes[..AUDIT_CANDIDATE_RECORD_BYTES],
            &[0, 0, 0, 1, 0, 0, 0, 11, 0, 0, 0, 21, 0, 0, 0, 41, 0, 0, 0, 31,]
        );
        let expected_digest: [u8; 32] = Sha256::digest(&bytes).into();
        assert_eq!(summary.sha256, expected_digest);
        assert_eq!(read_audit_candidate_file(&path).unwrap(), records);
    }

    #[test]
    fn audit_candidates_are_deterministic_equal_interval_and_bounded() {
        let population_count = 8_193u64;
        let collect = || {
            let mut selector = AuditCandidateSelector::new(population_count);
            for ordinal in 0..population_count {
                selector.observe(ordinal as u32, 7, 9, 10, 11).unwrap();
            }
            selector.finish().unwrap().to_vec()
        };

        let first = collect();
        let second = collect();

        assert_eq!(first, second);
        assert_eq!(first.len(), AUDIT_CANDIDATE_MAX_RECORDS);
        assert_eq!(first.first().unwrap().code_key, 0);
        assert_eq!(first.last().unwrap().code_key, 8_192);
        for (index, record) in first.iter().enumerate() {
            assert_eq!(
                u64::from(record.code_key),
                stratified_audit_ordinal(
                    population_count,
                    AUDIT_CANDIDATE_MAX_RECORDS as u64,
                    index as u64,
                )
            );
        }
    }

    #[test]
    fn audit_candidate_selector_rejects_incomplete_or_excess_streams() {
        let mut incomplete = AuditCandidateSelector::new(2);
        incomplete.observe(1, 2, 3, 4, 5).unwrap();
        assert!(incomplete.finish().is_err());

        let mut excess = AuditCandidateSelector::new(1);
        excess.observe(1, 2, 3, 4, 5).unwrap();
        assert!(excess.observe(6, 7, 8, 9, 10).is_err());
    }

    #[test]
    fn codec_roundtrip_and_exact_big_endian_bytes() {
        let record = ServingRunRecord {
            code_id: [
                0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
                0x0e, 0x0f,
            ],
            provider_set_id: [
                0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d,
                0x1e, 0x1f,
            ],
            price_set_id: [
                0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d,
                0x2e, 0x2f,
            ],
            provider_count: 0x0102_0304,
        };
        let expected = [
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b,
            0x1c, 0x1d, 0x1e, 0x1f, 0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29,
            0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x01, 0x02, 0x03, 0x04,
        ];

        assert_eq!(record.encode(), expected);
        let decoded = ServingRunRecord::decode(&expected).unwrap();
        assert_eq!(decoded.code_id, record.code_id);
        assert_eq!(decoded.provider_set_id, record.provider_set_id);
        assert_eq!(decoded.price_set_id, record.price_set_id);
        assert_eq!(decoded.provider_count, record.provider_count);

        let different_payload = ServingRunRecord {
            provider_count: 99,
            ..record
        };
        assert_eq!(record.cmp(&different_payload), Ordering::Equal);
        assert_eq!(record, different_payload);
    }

    #[test]
    fn tagged_sort_orders_by_source_and_preserves_duplicate_multiplicity_across_spills() {
        let base = TestDirectory::new("tagged-sort");
        let source_one_path = base.join("source-one.ready");
        let source_zero_path = base.join("source-zero.ready");
        let output_path = base.join("tagged.sorted");
        let row = record(0, 1, 2, 3);
        write_records(&source_one_path, &[row]);
        write_records(&source_zero_path, &vec![row; 65]);
        let codec = TaggedServingRunCodec::new(2, 1).unwrap();

        let stats = external_sort_tagged_partition_files(
            &[(source_one_path, 1), (source_zero_path, 0)],
            &output_path,
            &base.path,
            1,
            0,
            4,
            codec,
        )
        .unwrap();

        let mut reader = BufReader::new(File::open(&output_path).unwrap());
        let mut tagged = Vec::new();
        while let Some(record) = TaggedServingRunRecord::read_from(&mut reader, codec).unwrap() {
            tagged.push(record);
        }
        assert_eq!(tagged.len(), 66);
        assert!(tagged[..65].iter().all(|record| record.source_key == 0));
        assert_eq!(tagged[65].source_key, 1);
        assert!(tagged
            .iter()
            .all(|record| record.record.encode() == row.encode()));
        assert_eq!(stats.input_records, 66);
        assert_eq!(stats.unique_records, 2);
        assert_eq!(stats.duplicate_records, 64);
        assert_eq!(stats.chunk_count, 66);
        assert_eq!(stats.input_bytes, 66 * SERVING_RUN_RECORD_BYTES as u64);
        assert_eq!(stats.output_bytes, 66 * 53);
    }

    #[test]
    fn tagged_codec_uses_minimum_source_width_at_boundaries() {
        let row = record(0, 1, 2, 3);
        for (source_count, expected_bits, expected_bytes, source_key) in [
            (1, 0, 0, 0),
            (2, 1, 1, 1),
            (256, 8, 1, 255),
            (257, 9, 2, 256),
            (65_536, 16, 2, 65_535),
            (65_537, 17, 3, 65_536),
            (1 << 24, 24, 3, (1 << 24) - 1),
            ((1 << 24) + 1, 25, 4, 1 << 24),
            (1_u64 << 31, 31, 4, i32::MAX as u32),
        ] {
            assert_eq!(source_key_bits(source_count).unwrap(), expected_bits);
            assert_eq!(source_key_bytes(source_count).unwrap(), expected_bytes);
            assert_eq!(
                tagged_serving_run_record_bytes(source_count).unwrap(),
                SERVING_RUN_RECORD_BYTES + usize::from(expected_bytes)
            );
            let tagged = TaggedServingRunRecord {
                record: row,
                source_key,
            };
            let codec = TaggedServingRunCodec::new(source_count, expected_bytes).unwrap();
            let encoded = tagged.encode(codec).unwrap();
            assert_eq!(
                encoded.len(),
                SERVING_RUN_RECORD_BYTES + usize::from(expected_bytes)
            );
            if source_count == 1 {
                assert_eq!(encoded, row.encode());
            }
            assert_eq!(
                TaggedServingRunRecord::decode(&encoded, codec).unwrap(),
                tagged
            );
            assert!(TaggedServingRunRecord::decode(&encoded[..encoded.len() - 1], codec).is_err());
            let mut truncated_reader = io::Cursor::new(&encoded[..encoded.len() - 1]);
            assert!(TaggedServingRunRecord::read_from(&mut truncated_reader, codec).is_err());
            let mut oversized = encoded.clone();
            oversized.push(0);
            assert!(TaggedServingRunRecord::decode(&oversized, codec).is_err());
        }
        let row = TaggedServingRunRecord {
            record: row,
            source_key: 2,
        };
        assert!(row
            .encode(TaggedServingRunCodec::new(2, 1).unwrap())
            .is_err());
        assert!(TaggedServingRunCodec::new(2, 2).is_err());
        let mut out_of_range = TaggedServingRunRecord {
            record: row.record,
            source_key: 0,
        }
        .encode(TaggedServingRunCodec::new(2, 1).unwrap())
        .unwrap();
        out_of_range[48] = 2;
        assert!(TaggedServingRunRecord::decode(
            &out_of_range,
            TaggedServingRunCodec::new(2, 1).unwrap(),
        )
        .is_err());
        for source_count in [0, (1_u64 << 31) + 1] {
            assert!(source_key_bits(source_count).is_err());
            assert!(source_key_bytes(source_count).is_err());
            assert!(tagged_serving_run_record_bytes(source_count).is_err());
        }
    }

    #[test]
    fn coverage_scope_id_requires_exact_lowercase_hex() {
        let encoded = "00112233445566778899aabbccddeeff0123456789abcdef0011223344556677";
        assert_eq!(
            parse_coverage_scope_id(encoded).unwrap(),
            [
                0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
                0xee, 0xff, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x00, 0x11, 0x22, 0x33,
                0x44, 0x55, 0x66, 0x77,
            ]
        );
        for invalid in [
            "",
            "00112233445566778899aabbccddeeff0123456789abcdef001122334455667",
            "00112233445566778899aabbccddeeff0123456789abcdef00112233445566770",
            "00112233445566778899aabbccddeeff0123456789abcdef001122334455667A",
            "00112233445566778899aabbccddeeff0123456789abcdef001122334455667g",
            " 0112233445566778899aabbccddeeff0123456789abcdef0011223344556677",
        ] {
            let error = parse_coverage_scope_id(invalid).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        }
    }

    #[test]
    fn natural_lean_code_identity_covers_scope_code_and_arrangement() {
        let scope = [0x11; COVERAGE_SCOPE_ID_BYTES];
        let other_scope = [0x22; COVERAGE_SCOPE_ID_BYTES];
        let identity = natural_lean_code_identity(
            &scope,
            Some("CPT"),
            Some("99213"),
            Some("FFS"),
            Some("2026"),
            Some("Office visit"),
            Some("Established patient visit"),
        );
        assert_eq!(
            identity,
            natural_lean_code_identity(
                &scope,
                Some("CPT"),
                Some("99213"),
                Some("FFS"),
                Some("2026"),
                Some("Office visit"),
                Some("Established patient visit"),
            )
        );
        assert_ne!(
            identity,
            natural_lean_code_identity(
                &other_scope,
                Some("CPT"),
                Some("99213"),
                Some("FFS"),
                Some("2026"),
                Some("Office visit"),
                Some("Established patient visit"),
            )
        );
        assert_ne!(
            natural_lean_code_identity(&scope, None, Some("99213"), Some("FFS"), None, None, None),
            natural_lean_code_identity(
                &scope,
                Some(""),
                Some("99213"),
                Some("FFS"),
                None,
                None,
                None,
            )
        );
        assert_ne!(
            natural_lean_code_identity(&scope, Some("CPT"), None, Some("FFS"), None, None, None),
            natural_lean_code_identity(
                &scope,
                Some("CPT"),
                Some(""),
                Some("FFS"),
                None,
                None,
                None,
            )
        );
        assert_ne!(
            identity,
            natural_lean_code_identity(
                &scope,
                Some("CPT"),
                Some("99213"),
                Some("BUNDLE"),
                Some("2026"),
                Some("Office visit"),
                Some("Established patient visit"),
            )
        );
        assert_ne!(
            natural_lean_code_identity(&scope, Some("CPT"), Some("99213"), None, None, None, None),
            natural_lean_code_identity(
                &scope,
                Some("CPT"),
                Some("99213"),
                Some(""),
                None,
                None,
                None,
            )
        );
        for variant in [
            natural_lean_code_identity(
                &scope,
                Some("CPT"),
                Some("99213"),
                Some("FFS"),
                Some("2025"),
                Some("Office visit"),
                Some("Established patient visit"),
            ),
            natural_lean_code_identity(
                &scope,
                Some("CPT"),
                Some("99213"),
                Some("FFS"),
                Some("2026"),
                Some("Different source name"),
                Some("Established patient visit"),
            ),
            natural_lean_code_identity(
                &scope,
                Some("CPT"),
                Some("99213"),
                Some("FFS"),
                Some("2026"),
                Some("Office visit"),
                Some("Different source description"),
            ),
        ] {
            assert_ne!(identity, variant);
        }
        assert_ne!(
            natural_lean_code_identity(
                &scope,
                Some("CPT"),
                Some("99213"),
                Some("FFS"),
                None,
                None,
                None,
            ),
            natural_lean_code_identity(
                &scope,
                Some("CPT"),
                Some("99213"),
                Some("FFS"),
                Some(""),
                Some(""),
                Some(""),
            )
        );
    }

    #[test]
    fn invalid_partition_configurations_are_rejected() {
        for partition_count in [0, 3, 255, 257] {
            let error = validate_partition_count(partition_count).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        }
        for partition_count in [1, 2, 4, 8, 16, 32, 64, 128, 256] {
            validate_partition_count(partition_count).unwrap();
        }
    }

    #[test]
    fn partitioning_uses_leading_code_id_bits_deterministically() {
        let mut code_id = [0u8; 16];
        code_id[0] = 0b1011_0110;

        let expected = [(1, 0), (2, 1), (4, 2), (8, 5), (16, 11), (256, 182)];
        for (partition_count, partition_index) in expected {
            assert_eq!(
                partition_for_code_id(&code_id, partition_count).unwrap(),
                partition_index
            );
            assert_eq!(
                partition_for_code_id(&code_id, partition_count).unwrap(),
                partition_index
            );
        }
    }

    #[test]
    fn partition_writer_opens_only_used_files() {
        let base = TestDirectory::new("partition-writer");
        let directory = base.join("partitions");
        let mut writer =
            ServingRunPartitionWriter::with_buffer_capacity(&directory, 4, "worker0001", 52)
                .unwrap();
        assert!(directory_is_empty(&directory));

        assert_eq!(writer.write_record(&record(0x00, 1, 1, 2)).unwrap(), 0);
        assert_eq!(writer.write_record(&record(0x01, 2, 2, 3)).unwrap(), 0);
        assert_eq!(writer.write_record(&record(0xff, 3, 3, 4)).unwrap(), 3);
        assert_eq!(fs::read_dir(&directory).unwrap().count(), 2);

        let output = writer.finish().unwrap();
        let files = output.partition_files;
        assert!(output.code_dictionary_file.is_none());
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].partition_index, 0);
        assert_eq!(files[0].record_count, 2);
        assert_eq!(files[1].partition_index, 3);
        assert_eq!(files[1].record_count, 1);
        assert_eq!(files[0].bytes, 104);
        assert_eq!(files[1].bytes, 52);
        assert_eq!(fs::metadata(&files[0].path).unwrap().len(), 104);
        assert_eq!(fs::metadata(&files[1].path).unwrap().len(), 52);
        assert!(files
            .iter()
            .all(|file| file.path.extension().is_some_and(|value| value == "ready")));
        assert!(fs::read_dir(&directory).unwrap().all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains("partial")));
    }

    #[test]
    fn code_dictionary_roundtrips_nullable_tuple_and_rejects_corruption() {
        let base = TestDirectory::new("code-dictionary");
        let directory = base.join("runs");
        let mut writer =
            ServingRunPartitionWriter::with_buffer_capacity(&directory, 4, "worker-codes", 52)
                .unwrap();
        let coverage_scope_id = [0x5a; COVERAGE_SCOPE_ID_BYTES];
        let code_fields = NaturalLeanCodeFields {
            coverage_scope_id: &coverage_scope_id,
            reported_code_system: None,
            reported_code: Some(""),
            negotiation_arrangement: Some("FFS"),
            billing_code_type_version: Some("2026"),
            name: Some("Source name"),
            description: Some("Source description"),
        };
        let code_id = code_fields.identity();
        let record = ServingRunRecord {
            code_id,
            provider_set_id: [1; 16],
            price_set_id: [2; 16],
            provider_count: 3,
        };
        let prepared = writer.register_natural_lean_code(code_fields).unwrap();
        assert_eq!(prepared.code_id(), code_id);
        writer
            .write_prepared_natural_lean_record(&record, prepared)
            .unwrap();
        writer
            .write_prepared_natural_lean_record(&record, prepared)
            .unwrap();
        let output = writer.finish().unwrap();
        let dictionary = output.code_dictionary_file.unwrap();
        assert_eq!(dictionary.record_count, 1);
        assert_eq!(
            read_code_dictionary_exact(
                &dictionary.path,
                dictionary.record_count,
                dictionary.bytes,
            )
            .unwrap(),
            vec![NaturalLeanCode {
                code_id,
                coverage_scope_id,
                reported_code_system: None,
                reported_code: Some("".to_string()),
                negotiation_arrangement: Some("FFS".to_string()),
                billing_code_type_version: Some("2026".to_string()),
                name: Some("Source name".to_string()),
                description: Some("Source description".to_string()),
            }]
        );

        let bytes = fs::read(&dictionary.path).unwrap();
        assert_eq!(&bytes[..8], b"PTG2CDR4");
        assert_eq!(&bytes[8..12], &CODE_DICTIONARY_FORMAT_VERSION.to_be_bytes());
        assert_eq!(&bytes[36..68], &coverage_scope_id);
        let truncated = base.join("truncated.codes");
        fs::write(&truncated, &bytes[..bytes.len() - 1]).unwrap();
        assert!(read_code_dictionary(&truncated).is_err());
        let trailing = base.join("trailing.codes");
        let mut trailing_bytes = bytes;
        trailing_bytes.push(0);
        fs::write(&trailing, trailing_bytes).unwrap();
        assert!(read_code_dictionary(&trailing).is_err());

        let mut impossible_count_bytes = fs::read(&dictionary.path).unwrap();
        impossible_count_bytes[12..20].copy_from_slice(&u64::MAX.to_be_bytes());
        let impossible_count = base.join("impossible-count.codes");
        fs::write(&impossible_count, &impossible_count_bytes).unwrap();
        let count_error = read_code_dictionary_exact(
            &impossible_count,
            dictionary.record_count,
            impossible_count_bytes.len() as u64,
        )
        .unwrap_err();
        assert_eq!(count_error.kind(), io::ErrorKind::InvalidData);
        assert!(count_error.to_string().contains("authenticated manifest"));

        let mut impossible_text_bytes = fs::read(&dictionary.path).unwrap();
        impossible_text_bytes[70..74].copy_from_slice(&u32::MAX.to_be_bytes());
        let impossible_text = base.join("impossible-text.codes");
        fs::write(&impossible_text, &impossible_text_bytes).unwrap();
        let text_error = read_code_dictionary_exact(
            &impossible_text,
            dictionary.record_count,
            impossible_text_bytes.len() as u64,
        )
        .unwrap_err();
        assert_eq!(text_error.kind(), io::ErrorKind::InvalidData);
        assert!(text_error.to_string().contains("remaining file bytes"));
    }

    #[test]
    fn arrangement_variants_produce_distinct_code_ids_and_dictionary_rows() {
        let base = TestDirectory::new("code-arrangements");
        let mut writer = ServingRunPartitionWriter::with_buffer_capacity(
            base.join("runs"),
            4,
            "arrangements",
            SERVING_RUN_RECORD_BYTES,
        )
        .unwrap();
        let coverage_scope_id = [0x33; COVERAGE_SCOPE_ID_BYTES];
        let ffs_id = natural_lean_code_identity(
            &coverage_scope_id,
            Some("CPT"),
            Some("99213"),
            Some("FFS"),
            None,
            None,
            None,
        );
        let bundle_id = natural_lean_code_identity(
            &coverage_scope_id,
            Some("CPT"),
            Some("99213"),
            Some("BUNDLE"),
            None,
            None,
            None,
        );
        assert_ne!(ffs_id, bundle_id);

        for (code_id, arrangement) in [(ffs_id, "FFS"), (bundle_id, "BUNDLE")] {
            let code_fields = NaturalLeanCodeFields {
                coverage_scope_id: &coverage_scope_id,
                reported_code_system: Some("CPT"),
                reported_code: Some("99213"),
                negotiation_arrangement: Some(arrangement),
                billing_code_type_version: None,
                name: None,
                description: None,
            };
            writer
                .write_natural_lean_record(
                    &ServingRunRecord {
                        code_id,
                        provider_set_id: [1; 16],
                        price_set_id: [2; 16],
                        provider_count: 3,
                    },
                    code_fields,
                )
                .unwrap();
        }

        let output = writer.finish().unwrap();
        assert_eq!(
            output
                .partition_files
                .iter()
                .map(|file| file.record_count)
                .sum::<u64>(),
            2
        );
        assert!(output
            .partition_files
            .iter()
            .all(|file| file.bytes == file.record_count * SERVING_RUN_RECORD_BYTES as u64));
        let dictionary = output.code_dictionary_file.unwrap();
        assert_eq!(dictionary.record_count, 2);
        let rows = read_code_dictionary(&dictionary.path).unwrap();
        assert_eq!(rows.len(), 2);
        for (code_id, arrangement) in [(ffs_id, "FFS"), (bundle_id, "BUNDLE")] {
            let row = rows.iter().find(|row| row.code_id == code_id).unwrap();
            assert_eq!(row.coverage_scope_id, coverage_scope_id);
            assert_eq!(row.reported_code_system.as_deref(), Some("CPT"));
            assert_eq!(row.reported_code.as_deref(), Some("99213"));
            assert_eq!(row.negotiation_arrangement.as_deref(), Some(arrangement));
        }
    }

    #[test]
    fn code_dictionary_detects_identity_tuple_collision() {
        let base = TestDirectory::new("code-collision");
        let mut writer =
            ServingRunPartitionWriter::with_buffer_capacity(base.join("runs"), 4, "collision", 52)
                .unwrap();
        let coverage_scope_id = [0x11; COVERAGE_SCOPE_ID_BYTES];
        let code_id = natural_lean_code_identity(
            &coverage_scope_id,
            Some("CPT"),
            Some("99213"),
            Some("FFS"),
            None,
            None,
            None,
        );
        writer.code_dictionary.insert(
            code_id,
            NaturalLeanCode {
                code_id,
                coverage_scope_id,
                reported_code_system: Some("CPT".to_string()),
                reported_code: Some("99213".to_string()),
                negotiation_arrangement: Some("BUNDLE".to_string()),
                billing_code_type_version: None,
                name: None,
                description: None,
            },
        );
        let code_fields = NaturalLeanCodeFields {
            coverage_scope_id: &coverage_scope_id,
            reported_code_system: Some("CPT"),
            reported_code: Some("99213"),
            negotiation_arrangement: Some("FFS"),
            billing_code_type_version: None,
            name: None,
            description: None,
        };
        let error = writer
            .write_natural_lean_record(
                &ServingRunRecord {
                    code_id,
                    provider_set_id: [1; 16],
                    price_set_id: [2; 16],
                    provider_count: 1,
                },
                code_fields,
            )
            .unwrap_err();
        assert!(error.to_string().contains("collision"));
    }

    #[test]
    fn multi_file_partition_sort_spills_and_dedupes_overlaps() {
        let base = TestDirectory::new("multi-partition-sort");
        let input_a = base.join("a.run");
        let input_b = base.join("b.run");
        let output = base.join("sorted.run");
        let temporary = base.join("temporary");
        let first = record(0x01, 1, 1, 2);
        let second = record(0x02, 2, 2, 3);
        let third = record(0x03, 3, 3, 4);
        write_records(&input_a, &[second, first]);
        write_records(&input_b, &[third, first]);

        let stats = external_sort_dedupe_partition_files(
            &[input_b.clone(), input_a.clone()],
            &output,
            &temporary,
            1,
            0,
            4,
        )
        .unwrap();

        assert_eq!(stats.input_file_count, 2);
        assert_eq!(stats.input_records, 4);
        assert_eq!(stats.unique_records, 3);
        assert_eq!(stats.duplicate_records, 1);
        assert!(stats.chunk_count > 1);
        assert_eq!(read_records(&output), vec![first, second, third]);
    }

    #[test]
    fn multi_file_partition_sort_retains_duplicate_serving_occurrences() {
        let base = TestDirectory::new("multi-partition-multiset");
        let input_a = base.join("a.run");
        let input_b = base.join("b.run");
        let output = base.join("sorted.run");
        let first = record(0x01, 1, 1, 2);
        let second = record(0x02, 2, 2, 3);
        let third = record(0x03, 3, 3, 4);
        write_records(&input_a, &[second, first]);
        write_records(&input_b, &[third, first]);

        let stats = external_sort_partition_files(
            &[input_b, input_a],
            &output,
            base.join("temporary"),
            1,
            0,
            4,
        )
        .unwrap();

        assert_eq!(stats.input_records, 4);
        assert_eq!(stats.unique_records, 3);
        assert_eq!(stats.duplicate_records, 1);
        assert_eq!(stats.output_bytes, 4 * SERVING_RUN_RECORD_BYTES as u64);
        assert_eq!(read_records(&output), vec![first, first, second, third]);
    }

    #[test]
    fn assigned_sort_is_fixed_width_and_preserves_duplicate_multiplicity() {
        let base = TestDirectory::new("assigned-multiset");
        let input = base.join("assigned.bin");
        let output = base.join("assigned.sorted");
        let mut first = [0u8; ASSIGNED_SERVING_RECORD_BYTES];
        first[3] = 1;
        let mut second = [0u8; ASSIGNED_SERVING_RECORD_BYTES];
        second[3] = 2;
        let mut writer = BufWriter::new(File::create(&input).unwrap());
        for record in [second, first, first, second, first] {
            writer.write_all(&record).unwrap();
        }
        writer.flush().unwrap();

        let stats =
            external_sort_assigned_serving_records(&[input], &output, base.join("temporary"), 1)
                .unwrap();

        assert_eq!(stats.input_records, 5);
        assert_eq!(stats.unique_records, 5);
        assert_eq!(stats.duplicate_records, 0);
        assert_eq!(stats.output_bytes, 5 * ASSIGNED_SERVING_RECORD_BYTES as u64);
        assert_eq!(
            fs::read(output).unwrap(),
            [first, first, first, second, second].concat()
        );
    }

    #[test]
    fn provider_code_pair_sort_is_fixed_width_deduped_and_limit_exact() {
        let base = TestDirectory::new("provider-code-pairs");
        let input = base.join("pairs.bin");
        let output = base.join("pairs.sorted");
        let pair = |provider_key: i32, code_key: i32| {
            let mut encoded = [0u8; PROVIDER_CODE_PAIR_RECORD_BYTES];
            encoded[..4].copy_from_slice(&provider_key.to_be_bytes());
            encoded[4..].copy_from_slice(&code_key.to_be_bytes());
            encoded
        };
        let first = pair(1, 4);
        let second = pair(1, 9);
        let third = pair(2, 3);
        let mut writer = BufWriter::new(File::create(&input).unwrap());
        for record in [third, second, third, first, second] {
            writer.write_all(&record).unwrap();
        }
        writer.flush().unwrap();

        let stats = external_sort_provider_code_pairs(&[input], &output, base.join("temporary"), 2)
            .unwrap();

        assert_eq!(stats.input_records, 5);
        assert_eq!(stats.unique_records, 3);
        assert_eq!(stats.duplicate_records, 2);
        assert_eq!(stats.chunk_count, 3);
        assert_eq!(
            stats.input_bytes,
            5 * PROVIDER_CODE_PAIR_RECORD_BYTES as u64
        );
        assert_eq!(stats.spill_bytes, stats.input_bytes);
        assert_eq!(
            stats.output_bytes,
            3 * PROVIDER_CODE_PAIR_RECORD_BYTES as u64
        );
        assert_eq!(fs::read(output).unwrap(), [first, second, third].concat());
    }

    #[test]
    fn sorted_provider_identity_merge_dedupes_across_partitions() {
        let base = TestDirectory::new("provider-identity-merge");
        let input_a = base.join("providers-a.sorted");
        let input_b = base.join("providers-b.sorted");
        let output = base.join("providers.sorted");
        let provider = |identity_byte: u8, provider_count: u32| {
            let mut encoded = [0u8; PROVIDER_IDENTITY_RECORD_BYTES];
            encoded[..16].fill(identity_byte);
            encoded[16..].copy_from_slice(&provider_count.to_be_bytes());
            encoded
        };
        let first = provider(1, 2);
        let second = provider(2, 3);
        let third = provider(3, 4);
        fs::write(&input_a, [first, second].concat()).unwrap();
        fs::write(&input_b, [first, third].concat()).unwrap();

        let stats = merge_sorted_provider_identities(&[input_a, input_b], &output).unwrap();

        assert_eq!(stats.input_file_count, 2);
        assert_eq!(stats.input_records, 4);
        assert_eq!(stats.unique_records, 3);
        assert_eq!(stats.duplicate_records, 1);
        assert_eq!(stats.spill_bytes, 0);
        assert_eq!(
            stats.output_bytes,
            3 * PROVIDER_IDENTITY_RECORD_BYTES as u64
        );
        assert_eq!(fs::read(output).unwrap(), [first, second, third].concat());
    }

    #[test]
    fn sorted_provider_identity_merge_rejects_unsorted_or_partial_inputs() {
        let base = TestDirectory::new("provider-identity-merge-invalid");
        let provider = |identity_byte: u8| {
            let mut encoded = [0u8; PROVIDER_IDENTITY_RECORD_BYTES];
            encoded[..16].fill(identity_byte);
            encoded[16..].copy_from_slice(&1u32.to_be_bytes());
            encoded
        };
        let unsorted = base.join("unsorted.bin");
        fs::write(&unsorted, [provider(2), provider(1)].concat()).unwrap();
        let error = merge_sorted_provider_identities(&[unsorted], base.join("unsorted-output.bin"))
            .unwrap_err();
        assert!(error.to_string().contains("not sorted"));

        let partial = base.join("partial.bin");
        fs::write(&partial, [0u8; PROVIDER_IDENTITY_RECORD_BYTES - 1]).unwrap();
        let error = merge_sorted_provider_identities(&[partial], base.join("partial-output.bin"))
            .unwrap_err();
        assert!(error.to_string().contains("not aligned"));
    }

    #[test]
    fn external_sorts_use_bounded_fan_in_across_many_spills() {
        let base = TestDirectory::new("bounded-fan-in");
        let serving_input = base.join("serving.run");
        let mut serving_records = (0u8..130)
            .rev()
            .map(|value| record(0x01, value, value, 2))
            .collect::<Vec<_>>();
        let duplicate_records = serving_records.iter().take(5).copied().collect::<Vec<_>>();
        serving_records.extend_from_slice(&duplicate_records);
        write_records(&serving_input, &serving_records);
        let serving_stats = external_sort_partition_files(
            &[serving_input],
            base.join("serving.sorted"),
            base.join("serving-tmp"),
            1,
            0,
            4,
        )
        .unwrap();
        assert!(serving_stats.chunk_count > DEFAULT_SORT_MERGE_FAN_IN as u64);
        assert_eq!(serving_stats.unique_records, 130);
        assert_eq!(serving_stats.duplicate_records, 5);
        assert!(serving_stats.spill_bytes > serving_stats.input_bytes);
        let sorted_serving = read_records(&base.join("serving.sorted"));
        assert_eq!(sorted_serving.len(), 135);
        assert!(sorted_serving.windows(2).all(|pair| pair[0] <= pair[1]));

        let dense_input = base.join("dense.bin");
        let mut dense_writer = BufWriter::new(File::create(&dense_input).unwrap());
        for value in (0u8..130).rev() {
            DenseIdRecord([value; 16])
                .write_to(&mut dense_writer)
                .unwrap();
        }
        dense_writer.flush().unwrap();
        let dense_output = base.join("dense.sorted");
        let dense_stats =
            external_sort_dense_ids(&[dense_input], &dense_output, base.join("dense-tmp"), 1)
                .unwrap();
        assert!(dense_stats.chunk_count > DEFAULT_SORT_MERGE_FAN_IN as u64);
        assert_eq!(dense_stats.unique_records, 130);
        assert!(dense_stats.spill_bytes > dense_stats.input_bytes);
        let dense_bytes = fs::read(dense_output).unwrap();
        assert!(dense_bytes
            .chunks_exact(DENSE_ID_RECORD_BYTES)
            .collect::<Vec<_>>()
            .windows(2)
            .all(|pair| pair[0] < pair[1]));

        let lex_input = base.join("lex.bin");
        let mut lex_writer = BufWriter::new(File::create(&lex_input).unwrap());
        for value in (0u32..130).rev() {
            lex_writer.write_all(&value.to_be_bytes()).unwrap();
        }
        lex_writer.flush().unwrap();
        let lex_output = base.join("lex.sorted");
        let lex_stats = external_sort_lexicographic_records(
            &[lex_input],
            &lex_output,
            base.join("lex-tmp"),
            4,
            1,
            false,
        )
        .unwrap();
        assert!(lex_stats.chunk_count > DEFAULT_SORT_MERGE_FAN_IN as u64);
        assert_eq!(lex_stats.unique_records, 130);
        assert_eq!(lex_stats.duplicate_records, 0);
        assert!(lex_stats.spill_bytes > lex_stats.input_bytes);
        let lex_bytes = fs::read(lex_output).unwrap();
        assert!(lex_bytes
            .chunks_exact(4)
            .collect::<Vec<_>>()
            .windows(2)
            .all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn leveled_run_accumulator_bounds_and_releases_path_metadata() {
        const FAN_IN: usize = 4;
        const INITIAL_RUNS: usize = 1024;

        let base = TestDirectory::new("bounded-run-metadata");
        let mut temporary_files = TemporaryFiles::default();
        let mut runs = BoundedRunAccumulator::new(FAN_IN);
        let mut merger = MetadataTestRunMerger {
            directory: &base.path,
            temporary_files: &mut temporary_files,
        };

        for _ in 0..INITIAL_RUNS {
            let (path, mut file) = create_unique_file(&base.path, "metadata-run").unwrap();
            file.write_all(&[0]).unwrap();
            drop(file);
            merger.temporary_files.track(path.clone());
            runs.push_initial(path, &mut merger).unwrap();
        }

        assert_eq!(runs.initial_run_count(), INITIAL_RUNS as u64);
        assert!(
            runs.peak_active_path_count()
                <= BoundedRunAccumulator::maximum_active_path_metadata(FAN_IN)
        );
        let final_paths = runs.finish(&mut merger).unwrap();
        assert!(final_paths.len() <= FAN_IN);
        assert!(
            merger.temporary_files.peak_path_count()
                <= BoundedRunAccumulator::maximum_active_path_metadata(FAN_IN) + 1
        );
        assert!(merger.temporary_files.peak_path_count() < INITIAL_RUNS);
        assert_eq!(
            merger.temporary_files.active_path_count(),
            final_paths.len()
        );
        assert!(final_paths.iter().all(|path| path.exists()));

        merger.temporary_files.remove_all(&final_paths).unwrap();
        assert_eq!(merger.temporary_files.active_path_count(), 0);
    }

    #[test]
    fn multi_file_partition_sort_rejects_count_conflict_and_wrong_partition() {
        let base = TestDirectory::new("multi-partition-errors");
        let conflict_a = base.join("conflict-a.run");
        let conflict_b = base.join("conflict-b.run");
        write_records(&conflict_a, &[record(0x01, 1, 1, 2)]);
        write_records(&conflict_b, &[record(0x01, 1, 1, 3)]);
        let error = external_sort_dedupe_partition_files(
            &[conflict_a, conflict_b],
            base.join("conflict.out"),
            base.join("conflict-tmp"),
            1,
            0,
            4,
        )
        .unwrap_err();
        assert!(error.to_string().contains("provider_count"));

        let wrong = base.join("wrong.run");
        write_records(&wrong, &[record(0xff, 1, 1, 2)]);
        let error = external_sort_dedupe_partition_files(
            &[wrong],
            base.join("wrong.out"),
            base.join("wrong-tmp"),
            1,
            0,
            4,
        )
        .unwrap_err();
        assert!(error.to_string().contains("belongs to partition"));
    }

    #[test]
    fn provider_identity_sort_rejects_count_conflict() {
        let base = TestDirectory::new("provider-conflict");
        let input = base.join("providers.bin");
        let mut writer = BufWriter::new(File::create(&input).unwrap());
        ProviderIdentityRecord {
            id: [7; 16],
            provider_count: 2,
        }
        .write_to(&mut writer)
        .unwrap();
        ProviderIdentityRecord {
            id: [7; 16],
            provider_count: 3,
        }
        .write_to(&mut writer)
        .unwrap();
        writer.flush().unwrap();
        let error = external_sort_provider_identities(
            &[input],
            base.join("providers.sorted"),
            base.join("temporary"),
            1,
        )
        .unwrap_err();
        assert!(error.to_string().contains("provider_count"));
    }

    #[test]
    fn dropped_or_failed_partition_writer_leaves_no_ready_partial() {
        let base = TestDirectory::new("partition-cancel");
        let cancelled_directory = base.join("cancelled");
        {
            let mut writer = ServingRunPartitionWriter::with_buffer_capacity(
                &cancelled_directory,
                4,
                "cancelled",
                52,
            )
            .unwrap();
            writer.write_record(&record(0x00, 1, 1, 2)).unwrap();
        }
        assert!(directory_is_empty(&cancelled_directory));

        let failed_directory = base.join("failed");
        let mut writer =
            ServingRunPartitionWriter::with_buffer_capacity(&failed_directory, 4, "failed", 52)
                .unwrap();
        writer.write_record(&record(0x00, 1, 1, 2)).unwrap();
        let conflicting_ready_path = writer.ready_path(0);
        fs::create_dir(&conflicting_ready_path).unwrap();
        let error = writer.finish().unwrap_err();
        assert!(matches!(
            error.kind(),
            io::ErrorKind::AlreadyExists | io::ErrorKind::IsADirectory
        ));
        assert!(fs::read_dir(&failed_directory).unwrap().all(|entry| {
            let entry = entry.unwrap();
            entry.path() == conflicting_ready_path
                || !entry.file_name().to_string_lossy().ends_with(".ready")
        }));
        assert!(fs::read_dir(&failed_directory).unwrap().all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains("partial")));
    }

    #[test]
    fn truncated_input_is_invalid_data() {
        let base = TestDirectory::new("truncated");
        let input = base.join("input.run");
        let output = base.join("output.run");
        let temporary = base.join("temporary");
        fs::write(&input, [7u8; SERVING_RUN_RECORD_BYTES - 1]).unwrap();

        let error = external_sort_dedupe_partition(&input, &output, &temporary, 2).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(!output.exists());
        assert!(directory_is_empty(&temporary));
        assert_eq!(
            ServingRunRecord::decode(&[0u8; SERVING_RUN_RECORD_BYTES - 1])
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn multi_run_sort_deduplicates_in_identity_order() {
        let base = TestDirectory::new("multi-run");
        let input = base.join("input.run");
        let output = base.join("output.run");
        let temporary = base.join("temporary");
        let records = [
            record(3, 1, 1, 30),
            record(1, 2, 2, 10),
            record(2, 1, 2, 20),
            record(1, 2, 2, 10),
            record(0, 9, 9, 5),
            record(3, 1, 1, 30),
            record(1, 2, 2, 10),
        ];
        write_records(&input, &records);

        let stats = external_sort_dedupe_partition(&input, &output, &temporary, 2).unwrap();

        assert_eq!(
            stats,
            ExternalSortStats {
                input_records: 7,
                unique_records: 4,
                duplicate_records: 3,
                output_bytes: 4 * SERVING_RUN_RECORD_BYTES as u64,
            }
        );
        let output_records = read_records(&output);
        assert_eq!(
            output_records,
            vec![records[4], records[1], records[2], records[0]]
        );
        assert_eq!(
            output_records
                .iter()
                .map(|record| record.provider_count)
                .collect::<Vec<_>>(),
            vec![5, 10, 20, 30]
        );
        assert!(directory_is_empty(&temporary));
    }

    #[test]
    fn conflicting_duplicate_is_rejected_and_cleaned_up() {
        let base = TestDirectory::new("conflict");
        let input = base.join("input.run");
        let output = base.join("output.run");
        let temporary = base.join("temporary");
        write_records(&input, &[record(1, 2, 3, 10), record(1, 2, 3, 11)]);
        fs::write(&output, b"existing-output").unwrap();

        let error = external_sort_dedupe_partition(&input, &output, &temporary, 1).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("provider_count"));
        assert_eq!(fs::read(&output).unwrap(), b"existing-output");
        assert!(directory_is_empty(&temporary));
    }

    #[test]
    fn empty_input_produces_empty_output() {
        let base = TestDirectory::new("empty");
        let input = base.join("input.run");
        let output = base.join("output.run");
        let temporary = base.join("temporary");
        fs::write(&input, []).unwrap();
        fs::write(&output, b"old-output").unwrap();

        let stats = external_sort_dedupe_partition(&input, &output, &temporary, 4).unwrap();

        assert_eq!(
            stats,
            ExternalSortStats {
                input_records: 0,
                unique_records: 0,
                duplicate_records: 0,
                output_bytes: 0,
            }
        );
        assert!(fs::read(&output).unwrap().is_empty());
        assert!(directory_is_empty(&temporary));
    }
}
