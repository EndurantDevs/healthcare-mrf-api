use crate::source_witness_spool::{
    ProviderSourceLocator, ProviderSourceSpools, RateSourceLocator, RateSourceSpools,
    SourceWitnessScratchBudget,
};
use flate2::write::ZlibEncoder;
use flate2::Compression;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::fs::{self, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, OnceLock};
use xxhash_rust::xxh3::Xxh3;

pub const SOURCE_WITNESS_CONTRACT: &str = "ptg2_v3_source_witness_v3";
pub const SOURCE_WITNESS_RECORD_CONTRACT: &str = "ptg2_v3_source_witness_record_v2";
pub const SOURCE_WITNESS_SELECTION: &str = "bottom_k_independent_occurrence_provider_cohorts_v3";
pub const SOURCE_WITNESS_FORMAT_VERSION: u32 = 3;
pub const SOURCE_WITNESS_OCCURRENCE_TARGET: usize = 10_000;
pub const SOURCE_WITNESS_PROVIDER_QUOTA: usize = 1_000;
pub const SOURCE_WITNESS_TOTAL_TARGET: usize =
    SOURCE_WITNESS_OCCURRENCE_TARGET + SOURCE_WITNESS_PROVIDER_QUOTA;
pub const SOURCE_WITNESS_UNQUERYABLE_POLICY: &str = "count_but_exclude_from_npi_api_challenges_v1";
const SOURCE_WITNESS_MAGIC: &[u8; 8] = b"PTG2SW03";
const SOURCE_WITNESS_RECORD_MAGIC: &[u8; 8] = b"PTG2SWR2";
const SOURCE_WITNESS_MAX_COMPRESSED_RECORD_BYTES: usize = 8 * 1024 * 1024;
const SOURCE_WITNESS_MAX_DECODED_EVIDENCE_BYTES: usize = 64 * 1024 * 1024;
const SOURCE_WITNESS_MAX_DECODED_TOTAL_BYTES: u64 = 512 * 1024 * 1024;
// Scanner bundles externalize and deduplicate exact source tokens before applying
// the same 512 MiB safety bound as the persisted logical payload.
const SOURCE_WITNESS_MAX_INTERMEDIATE_BUNDLE_BYTES: u64 = 512 * 1024 * 1024;
const SOURCE_WITNESS_MAX_COMPRESSED_RATE_CANDIDATE_BYTES: u64 =
    SOURCE_WITNESS_MAX_INTERMEDIATE_BUNDLE_BYTES;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct SourceWitnessCoordinate {
    pub object_ordinal: u64,
    pub rate_ordinal: u64,
}

impl SourceWitnessCoordinate {
    pub const fn new(object_ordinal: u64, rate_ordinal: u64) -> Self {
        Self {
            object_ordinal,
            rate_ordinal,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RateOccurrenceCandidate {
    pub priority: u64,
    pub occurrence_index: u64,
}

pub struct RateOccurrenceWitnessInput<'a> {
    pub candidate: RateOccurrenceCandidate,
    pub coordinate: SourceWitnessCoordinate,
    pub price_ordinal: u64,
    pub provider_ordinal: u64,
    pub provider_evidence: &'a Value,
    pub rate_source_locator: RateSourceLocator,
    pub raw_rate: &'a [u8],
    pub linked_provider_locator: Option<ProviderSourceLocator>,
    pub linked_provider_sha256: Option<[u8; 32]>,
    pub expected: &'a Value,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SourceWitnessKind {
    RateOccurrence,
    ProviderReference,
}

struct SourceWitnessRecordInput<'a> {
    kind: SourceWitnessKind,
    priority: u64,
    tie_breaker: [u8; 32],
    coordinate: SourceWitnessCoordinate,
    price_ordinal: u64,
    provider_ordinal: u64,
    provider_evidence: Option<&'a Value>,
    procedure: Option<&'a Map<String, Value>>,
    raw: &'a [u8],
    linked_provider_raw: Option<&'a [u8]>,
    expected: &'a Value,
}

impl SourceWitnessKind {
    fn label(self) -> &'static str {
        match self {
            Self::RateOccurrence => "rate_occurrence",
            Self::ProviderReference => "provider_reference",
        }
    }

    fn domain(self) -> &'static [u8] {
        match self {
            Self::RateOccurrence => b"PTG2_SOURCE_RATE_OCCURRENCE_WITNESS_V2\0",
            Self::ProviderReference => b"PTG2_SOURCE_PROVIDER_WITNESS_V2\0",
        }
    }
}

struct SelectedRateWitness {
    priority: u64,
    tie_breaker: [u8; 32],
    coordinate: SourceWitnessCoordinate,
    price_ordinal: u64,
    provider_ordinal: u64,
    provider_evidence: Value,
    rate_source_locator: RateSourceLocator,
    linked_provider_locator: Option<ProviderSourceLocator>,
    expected: Value,
}

struct PreparedSourceEvidence {
    sha256: [u8; 32],
    raw_byte_count: u32,
    compressed: Vec<u8>,
}

#[cfg(test)]
struct SelectedSourceEvidence {
    raw_byte_count: u32,
    compressed: Vec<u8>,
    reference_count: usize,
}

#[derive(Clone, Copy)]
struct StagedPayloadLocator {
    offset: u64,
    length: u32,
}

struct StagedSourceEvidence {
    raw_byte_count: u32,
    compressed_sha256: [u8; 32],
    payload: StagedPayloadLocator,
    reference_count: usize,
}

struct StagedSourceRecord {
    payload: StagedPayloadLocator,
}

struct SourceWitnessBundleStage {
    payload_file: tempfile::NamedTempFile,
    scratch_budget: Arc<SourceWitnessScratchBudget>,
    reserved_bytes: u64,
    logical_body_bytes: u64,
    decoded_evidence_bytes: u64,
    decoded_evidence_byte_count_by_sha256: HashMap<[u8; 32], u32>,
    evidence_by_sha256: HashMap<[u8; 32], StagedSourceEvidence>,
    rate_records: Vec<StagedSourceRecord>,
    provider_records: Vec<StagedSourceRecord>,
    rate_record_bytes: u64,
    provider_record_bytes: u64,
}

struct SelectedProviderWitness {
    priority: u64,
    tie_breaker: [u8; 32],
    coordinate: SourceWitnessCoordinate,
    source_locator: ProviderSourceLocator,
    expected: Value,
}

impl SelectedProviderWitness {
    fn key(&self) -> (u64, [u8; 32]) {
        (self.priority, self.tie_breaker)
    }
}

impl PartialEq for SelectedProviderWitness {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}

impl Eq for SelectedProviderWitness {}

impl PartialOrd for SelectedProviderWitness {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SelectedProviderWitness {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key().cmp(&other.key())
    }
}

impl SelectedRateWitness {
    fn key(&self) -> (u64, [u8; 32]) {
        (self.priority, self.tie_breaker)
    }
}

impl PartialEq for SelectedRateWitness {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}

impl Eq for SelectedRateWitness {}

impl PartialOrd for SelectedRateWitness {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SelectedRateWitness {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key().cmp(&other.key())
    }
}

#[derive(Default)]
struct SourceWitnessSamplerState {
    selected: BinaryHeap<SelectedRateWitness>,
}

struct SourceWitnessSampler {
    target: usize,
    compressed_byte_limit: u64,
    population: AtomicU64,
    evaluated: AtomicU64,
    oversized: AtomicU64,
    threshold: AtomicU64,
    state: Mutex<SourceWitnessSamplerState>,
}

#[derive(Default)]
struct ProviderWitnessSamplerState {
    selected: BinaryHeap<SelectedProviderWitness>,
}

struct ProviderWitnessSampler {
    target: usize,
    population: AtomicU64,
    evaluated: AtomicU64,
    threshold: AtomicU64,
    state: Mutex<ProviderWitnessSamplerState>,
}

impl SourceWitnessSampler {
    fn new(target: usize, compressed_byte_limit: u64) -> Self {
        Self {
            target,
            compressed_byte_limit,
            population: AtomicU64::new(0),
            evaluated: AtomicU64::new(0),
            oversized: AtomicU64::new(0),
            threshold: AtomicU64::new(u64::MAX),
            state: Mutex::new(SourceWitnessSamplerState::default()),
        }
    }

    fn add_population(&self, count: u64) -> io::Result<()> {
        io_result(
            self.population.fetch_update(
                AtomicOrdering::Relaxed,
                AtomicOrdering::Relaxed,
                |current| current.checked_add(count),
            ),
            io::ErrorKind::Other,
            "source witness population overflow",
        )
        .map(|_| ())
    }

    fn population(&self) -> u64 {
        self.population.load(AtomicOrdering::Relaxed)
    }

    fn threshold(&self) -> u64 {
        self.threshold.load(AtomicOrdering::Acquire)
    }

    fn mark_evaluated(&self) {
        self.evaluated.fetch_add(1, AtomicOrdering::Relaxed);
    }

    fn insert(&self, candidate: SelectedRateWitness) -> io::Result<()> {
        let mut state = io_result(
            self.state.lock(),
            io::ErrorKind::Other,
            "source witness sampler mutex is poisoned",
        )?;
        if state.selected.len() >= self.target
            && state
                .selected
                .peek()
                .is_some_and(|current| candidate.key() >= current.key())
        {
            return Ok(());
        }
        state.selected.push(candidate);
        if state.selected.len() > self.target {
            state.selected.pop();
        }
        let threshold = if state.selected.len() < self.target {
            u64::MAX
        } else {
            state
                .selected
                .peek()
                .map_or(u64::MAX, |record| record.priority)
        };
        self.threshold.store(threshold, AtomicOrdering::Release);
        Ok(())
    }

    fn take_sorted(&self) -> io::Result<Vec<SelectedRateWitness>> {
        let mut state = io_result(
            self.state.lock(),
            io::ErrorKind::Other,
            "source witness sampler mutex is poisoned",
        )?;
        let mut selected = std::mem::take(&mut state.selected).into_vec();
        selected.sort_unstable_by_key(SelectedRateWitness::key);
        Ok(selected)
    }

    fn metrics(&self, selected_count: usize, compressed_bytes: u64) -> Value {
        json!({
            "local_candidate_target": self.target,
            "population_count": self.population(),
            "candidate_evaluated_count": self.evaluated.load(AtomicOrdering::Relaxed),
            "oversized_record_count": self.oversized.load(AtomicOrdering::Relaxed),
            "selected_count": selected_count,
            "compressed_bytes": compressed_bytes,
            "compressed_record_byte_limit": SOURCE_WITNESS_MAX_COMPRESSED_RECORD_BYTES,
            "compressed_byte_limit": self.compressed_byte_limit,
        })
    }
}

impl ProviderWitnessSampler {
    fn new(target: usize) -> Self {
        Self {
            target,
            population: AtomicU64::new(0),
            evaluated: AtomicU64::new(0),
            threshold: AtomicU64::new(u64::MAX),
            state: Mutex::new(ProviderWitnessSamplerState::default()),
        }
    }

    fn add_population(&self, count: u64) -> io::Result<()> {
        io_result(
            self.population.fetch_update(
                AtomicOrdering::Relaxed,
                AtomicOrdering::Relaxed,
                |current| current.checked_add(count),
            ),
            io::ErrorKind::Other,
            "source witness population overflow",
        )
        .map(|_| ())
    }

    fn population(&self) -> u64 {
        self.population.load(AtomicOrdering::Relaxed)
    }

    fn threshold(&self) -> u64 {
        self.threshold.load(AtomicOrdering::Acquire)
    }

    fn mark_evaluated(&self) {
        self.evaluated.fetch_add(1, AtomicOrdering::Relaxed);
    }

    fn insert(&self, candidate: SelectedProviderWitness) -> io::Result<()> {
        let mut state = io_result(
            self.state.lock(),
            io::ErrorKind::Other,
            "provider witness sampler mutex is poisoned",
        )?;
        if state.selected.len() >= self.target
            && state
                .selected
                .peek()
                .is_some_and(|current| candidate.key() >= current.key())
        {
            return Ok(());
        }
        state.selected.push(candidate);
        if state.selected.len() > self.target {
            state.selected.pop();
        }
        let threshold = if state.selected.len() < self.target {
            u64::MAX
        } else {
            state
                .selected
                .peek()
                .map_or(u64::MAX, |record| record.priority)
        };
        self.threshold.store(threshold, AtomicOrdering::Release);
        Ok(())
    }

    fn take_sorted(&self) -> io::Result<Vec<SelectedProviderWitness>> {
        let mut state = io_result(
            self.state.lock(),
            io::ErrorKind::Other,
            "provider witness sampler mutex is poisoned",
        )?;
        let mut selected = std::mem::take(&mut state.selected).into_vec();
        selected.sort_unstable_by_key(SelectedProviderWitness::key);
        Ok(selected)
    }

    fn metrics(&self, selected_count: usize, compressed_bytes: u64) -> Value {
        json!({
            "local_candidate_target": self.target,
            "population_count": self.population(),
            "candidate_evaluated_count": self.evaluated.load(AtomicOrdering::Relaxed),
            "selected_count": selected_count,
            "compressed_bytes": compressed_bytes,
            "compressed_record_byte_limit": SOURCE_WITNESS_MAX_COMPRESSED_RECORD_BYTES,
            "compressed_byte_limit": SOURCE_WITNESS_MAX_INTERMEDIATE_BUNDLE_BYTES,
        })
    }
}

struct HashingWriter<W: Write> {
    inner: W,
    digest: Sha256,
    byte_count: u64,
}

impl<W: Write> HashingWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            digest: Sha256::new(),
            byte_count: 0,
        }
    }

    fn finish(mut self) -> io::Result<(W, [u8; 32], u64)> {
        self.inner.flush()?;
        Ok((self.inner, self.digest.finalize().into(), self.byte_count))
    }
}

impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(buffer)?;
        if written > 0 {
            self.digest.update(&buffer[..written]);
            self.byte_count = self.byte_count.saturating_add(written as u64);
        }
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl SourceWitnessBundleStage {
    fn new(scratch_budget: Arc<SourceWitnessScratchBudget>) -> io::Result<Self> {
        let payload_file = tempfile::Builder::new()
            .prefix("ptg2-source-witness-bundle-stage-")
            .tempfile_in(scratch_budget.scratch_root())?;
        Ok(Self {
            payload_file,
            scratch_budget,
            reserved_bytes: 0,
            // Evidence-count and record-count framing are always present.
            logical_body_bytes: 8,
            decoded_evidence_bytes: 0,
            decoded_evidence_byte_count_by_sha256: HashMap::new(),
            evidence_by_sha256: HashMap::new(),
            rate_records: Vec::new(),
            provider_records: Vec::new(),
            rate_record_bytes: 0,
            provider_record_bytes: 0,
        })
    }

    fn reserve_logical_body(&mut self, byte_count: u64) -> io::Result<()> {
        let projected = io_option(
            self.logical_body_bytes.checked_add(byte_count),
            io::ErrorKind::Other,
            "source witness bundle byte count overflow",
        )?;
        if projected > SOURCE_WITNESS_MAX_INTERMEDIATE_BUNDLE_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "strict V3 source witness candidate bundle would use at least {projected} bytes, exceeding the fail-closed {SOURCE_WITNESS_MAX_INTERMEDIATE_BUNDLE_BYTES}-byte intermediate budget",
                ),
            ));
        }
        self.logical_body_bytes = projected;
        Ok(())
    }

    fn append_payload(&mut self, payload: &[u8]) -> io::Result<StagedPayloadLocator> {
        let payload_length = io_result(
            u32::try_from(payload.len()),
            io::ErrorKind::InvalidData,
            "source witness staged payload exceeds the locator width",
        )?;
        let payload_file = self.payload_file.as_file_mut();
        let offset = payload_file.seek(SeekFrom::End(0))?;
        self.scratch_budget.reserve(u64::from(payload_length))?;
        if let Err(error) = payload_file.write_all(payload) {
            self.scratch_budget.release(u64::from(payload_length));
            return Err(error);
        }
        self.reserved_bytes = self
            .reserved_bytes
            .saturating_add(u64::from(payload_length));
        Ok(StagedPayloadLocator {
            offset,
            length: payload_length,
        })
    }

    fn reserve_decoded_evidence(
        &mut self,
        sha256: [u8; 32],
        raw_byte_count: u32,
    ) -> io::Result<()> {
        validate_decoded_evidence_size("token", raw_byte_count as usize)?;
        if let Some(existing_count) = self.decoded_evidence_byte_count_by_sha256.get(&sha256) {
            if *existing_count != raw_byte_count {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "source witness evidence digest is inconsistent",
                ));
            }
            return Ok(());
        }
        let projected_decoded_bytes = io_option(
            self.decoded_evidence_bytes
                .checked_add(u64::from(raw_byte_count)),
            io::ErrorKind::Other,
            "source witness decoded byte count overflow",
        )?;
        if projected_decoded_bytes > SOURCE_WITNESS_MAX_DECODED_TOTAL_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "strict V3 source witness evidence would decode to {projected_decoded_bytes} bytes, exceeding the fail-closed {SOURCE_WITNESS_MAX_DECODED_TOTAL_BYTES}-byte aggregate decoded budget",
                ),
            ));
        }
        self.decoded_evidence_byte_count_by_sha256
            .insert(sha256, raw_byte_count);
        self.decoded_evidence_bytes = projected_decoded_bytes;
        Ok(())
    }

    fn preflight_raw_evidence(&mut self, raw_evidence: &[u8]) -> io::Result<()> {
        validate_decoded_evidence_size("token", raw_evidence.len())?;
        let raw_byte_count = io_result(
            u32::try_from(raw_evidence.len()),
            io::ErrorKind::InvalidData,
            "source witness raw JSON token is too large",
        )?;
        self.reserve_decoded_evidence(Sha256::digest(raw_evidence).into(), raw_byte_count)
    }

    fn merge_evidence(&mut self, evidence: PreparedSourceEvidence) -> io::Result<()> {
        self.reserve_decoded_evidence(evidence.sha256, evidence.raw_byte_count)?;
        validate_compressed_record_size(evidence.compressed.len())?;
        let compressed_sha256: [u8; 32] = Sha256::digest(&evidence.compressed).into();
        if let Some(existing) = self.evidence_by_sha256.get_mut(&evidence.sha256) {
            if existing.raw_byte_count != evidence.raw_byte_count
                || existing.payload.length as usize != evidence.compressed.len()
                || existing.compressed_sha256 != compressed_sha256
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "source witness evidence digest is inconsistent",
                ));
            }
            existing.reference_count = existing.reference_count.saturating_add(1);
            return Ok(());
        }
        let framing_bytes = 32u64
            .saturating_add(4)
            .saturating_add(4)
            .saturating_add(evidence.compressed.len() as u64);
        self.reserve_logical_body(framing_bytes)?;
        let payload = self.append_payload(&evidence.compressed)?;
        self.evidence_by_sha256.insert(
            evidence.sha256,
            StagedSourceEvidence {
                raw_byte_count: evidence.raw_byte_count,
                compressed_sha256,
                payload,
                reference_count: 1,
            },
        );
        Ok(())
    }

    fn push_record(
        &mut self,
        kind: SourceWitnessKind,
        compressed_record: Vec<u8>,
    ) -> io::Result<()> {
        validate_compressed_record_size(compressed_record.len())?;
        self.reserve_logical_body(4u64.saturating_add(compressed_record.len() as u64))?;
        let payload = self.append_payload(&compressed_record)?;
        let record = StagedSourceRecord { payload };
        match kind {
            SourceWitnessKind::RateOccurrence => {
                self.rate_record_bytes = self
                    .rate_record_bytes
                    .saturating_add(u64::from(payload.length));
                self.rate_records.push(record);
            }
            SourceWitnessKind::ProviderReference => {
                self.provider_record_bytes = self
                    .provider_record_bytes
                    .saturating_add(u64::from(payload.length));
                self.provider_records.push(record);
            }
        }
        Ok(())
    }

    fn copy_payload<W: Write>(
        &mut self,
        writer: &mut W,
        locator: StagedPayloadLocator,
    ) -> io::Result<()> {
        let payload_file = self.payload_file.as_file_mut();
        payload_file.seek(SeekFrom::Start(locator.offset))?;
        let copied = io::copy(&mut payload_file.take(u64::from(locator.length)), writer)?;
        if copied != u64::from(locator.length) {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "source witness staged payload is truncated",
            ));
        }
        Ok(())
    }
}

impl Drop for SourceWitnessBundleStage {
    fn drop(&mut self) {
        self.scratch_budget.release(self.reserved_bytes);
    }
}

pub struct SourceWitnessCollector {
    raw_source_sha256: [u8; 32],
    raw_source_sha256_hex: String,
    rate_occurrences: SourceWitnessSampler,
    provider_references: ProviderWitnessSampler,
    rate_rows: AtomicU64,
    unqueryable_rate_rows: AtomicU64,
    provider_spools: OnceLock<ProviderSourceSpools>,
    rate_spools: OnceLock<RateSourceSpools>,
    scratch_budget: Arc<SourceWitnessScratchBudget>,
}

impl SourceWitnessCollector {
    pub fn new(raw_source_sha256: &str) -> io::Result<Self> {
        let normalized = raw_source_sha256.trim().to_ascii_lowercase();
        if normalized.len() != 64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "strict V3 source witness requires a 64-character raw source SHA-256",
            ));
        }
        let mut digest = [0u8; 32];
        for (index, chunk) in normalized.as_bytes().chunks_exact(2).enumerate() {
            let text = io_result(
                std::str::from_utf8(chunk),
                io::ErrorKind::InvalidInput,
                "raw source SHA-256 is invalid",
            )?;
            digest[index] = io_result(
                u8::from_str_radix(text, 16),
                io::ErrorKind::InvalidInput,
                "raw source SHA-256 is invalid",
            )?;
        }
        let scratch_budget = SourceWitnessScratchBudget::from_env()?;
        Ok(Self {
            raw_source_sha256: digest,
            raw_source_sha256_hex: normalized,
            rate_occurrences: SourceWitnessSampler::new(
                SOURCE_WITNESS_OCCURRENCE_TARGET,
                SOURCE_WITNESS_MAX_COMPRESSED_RATE_CANDIDATE_BYTES,
            ),
            provider_references: ProviderWitnessSampler::new(SOURCE_WITNESS_PROVIDER_QUOTA),
            rate_rows: AtomicU64::new(0),
            unqueryable_rate_rows: AtomicU64::new(0),
            provider_spools: OnceLock::new(),
            rate_spools: OnceLock::new(),
            scratch_budget,
        })
    }

    pub fn configure_provider_spools(&self, shard_count: usize) -> io::Result<()> {
        io_result(
            self.provider_spools
                .set(ProviderSourceSpools::new_provider_with_budget(
                    shard_count,
                    Arc::clone(&self.scratch_budget),
                )?),
            io::ErrorKind::Other,
            "provider source spools already configured",
        )
    }

    pub fn configure_rate_spools(&self, shard_count: usize) -> io::Result<()> {
        io_result(
            self.rate_spools.set(RateSourceSpools::new_rate_with_budget(
                shard_count,
                Arc::clone(&self.scratch_budget),
            )?),
            io::ErrorKind::Other,
            "rate source spools already configured",
        )
    }

    pub fn store_rate_source(
        &self,
        coordinate: SourceWitnessCoordinate,
        procedure: &Map<String, Value>,
        raw_rate: &[u8],
    ) -> io::Result<RateSourceLocator> {
        validate_decoded_evidence_size("rate token", raw_rate.len())?;
        let procedure_bytes = serde_json::to_vec(procedure).map_err(to_io_error)?;
        if procedure_bytes.len() > SOURCE_WITNESS_MAX_COMPRESSED_RECORD_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "source witness procedure is {} bytes, exceeding the fail-closed {}-byte metadata limit",
                    procedure_bytes.len(),
                    SOURCE_WITNESS_MAX_COMPRESSED_RECORD_BYTES,
                ),
            ));
        }
        let procedure_length = io_result(
            u32::try_from(procedure_bytes.len()),
            io::ErrorKind::InvalidData,
            "source witness procedure exceeds the locator width",
        )?;
        let mut fragment = Vec::with_capacity(
            4usize
                .saturating_add(procedure_bytes.len())
                .saturating_add(raw_rate.len()),
        );
        fragment.extend_from_slice(&procedure_length.to_be_bytes());
        fragment.extend_from_slice(&procedure_bytes);
        fragment.extend_from_slice(raw_rate);
        let spools = io_option(
            self.rate_spools.get(),
            io::ErrorKind::Other,
            "rate source spools are not configured",
        )?;
        let shard = coordinate.object_ordinal as usize % spools.shard_count();
        spools.append(shard, &fragment)
    }

    fn seal_rate_sources(&self) -> io::Result<()> {
        io_option(
            self.rate_spools.get(),
            io::ErrorKind::Other,
            "rate source spools are not configured",
        )?
        .seal()
    }

    fn read_rate_source(
        &self,
        locator: RateSourceLocator,
    ) -> io::Result<(Map<String, Value>, Vec<u8>)> {
        let fragment = io_option(
            self.rate_spools.get(),
            io::ErrorKind::Other,
            "rate source spools are not configured",
        )?
        .read_bounded(
            locator,
            SOURCE_WITNESS_MAX_DECODED_EVIDENCE_BYTES
                .saturating_add(SOURCE_WITNESS_MAX_COMPRESSED_RECORD_BYTES)
                .saturating_add(4),
        )?;
        let procedure_length_bytes: [u8; 4] = io_option(
            fragment.get(..4),
            io::ErrorKind::InvalidData,
            "rate source fragment is truncated",
        )?
        .try_into()
        .expect("four-byte slice");
        let procedure_length = u32::from_be_bytes(procedure_length_bytes) as usize;
        let procedure_end = io_option(
            4usize.checked_add(procedure_length),
            io::ErrorKind::InvalidData,
            "rate source fragment length overflow",
        )?;
        let procedure_bytes = io_option(
            fragment.get(4..procedure_end),
            io::ErrorKind::InvalidData,
            "rate source procedure is truncated",
        )?;
        let procedure = serde_json::from_slice::<Value>(procedure_bytes)
            .map_err(to_io_error)?
            .as_object()
            .cloned();
        let procedure = io_option(
            procedure,
            io::ErrorKind::InvalidData,
            "rate source procedure is not a JSON object",
        )?;
        let raw_rate = io_option(
            fragment.get(procedure_end..),
            io::ErrorKind::InvalidData,
            "rate source record is truncated",
        )?;
        Ok((procedure, raw_rate.to_vec()))
    }

    pub fn store_provider_source(
        &self,
        shard: usize,
        raw_provider: &[u8],
    ) -> io::Result<ProviderSourceLocator> {
        io_option(
            self.provider_spools.get(),
            io::ErrorKind::Other,
            "provider source spools are not configured",
        )?
        .append(shard, raw_provider)
    }

    pub fn seal_provider_sources(&self) -> io::Result<()> {
        io_option(
            self.provider_spools.get(),
            io::ErrorKind::Other,
            "provider source spools are not configured",
        )?
        .seal()
    }

    pub fn read_provider_source(&self, locator: ProviderSourceLocator) -> io::Result<Vec<u8>> {
        io_option(
            self.provider_spools.get(),
            io::ErrorKind::Other,
            "provider source spools are not configured",
        )?
        .read_bounded(locator, SOURCE_WITNESS_MAX_DECODED_EVIDENCE_BYTES)
    }

    #[cfg(test)]
    pub fn rate_occurrence_candidates(
        &self,
        coordinate: SourceWitnessCoordinate,
        procedure_json: &[u8],
        raw_rate: &[u8],
        occurrence_count: u64,
    ) -> io::Result<Vec<RateOccurrenceCandidate>> {
        self.record_rate_population(1, u64::from(occurrence_count == 0), occurrence_count)?;
        self.rate_occurrence_candidates_untracked(
            coordinate,
            procedure_json,
            raw_rate,
            occurrence_count,
        )
    }

    pub fn record_rate_population(
        &self,
        rate_rows: u64,
        unqueryable_rate_rows: u64,
        occurrence_count: u64,
    ) -> io::Result<()> {
        io_result(
            self.rate_rows.fetch_update(
                AtomicOrdering::Relaxed,
                AtomicOrdering::Relaxed,
                |current| current.checked_add(rate_rows),
            ),
            io::ErrorKind::Other,
            "source witness rate-row population overflow",
        )?;
        io_result(
            self.unqueryable_rate_rows.fetch_update(
                AtomicOrdering::Relaxed,
                AtomicOrdering::Relaxed,
                |current| current.checked_add(unqueryable_rate_rows),
            ),
            io::ErrorKind::Other,
            "source witness unqueryable population overflow",
        )?;
        self.rate_occurrences.add_population(occurrence_count)
    }

    pub fn rate_occurrence_candidates_untracked(
        &self,
        coordinate: SourceWitnessCoordinate,
        procedure_json: &[u8],
        raw_rate: &[u8],
        occurrence_count: u64,
    ) -> io::Result<Vec<RateOccurrenceCandidate>> {
        if occurrence_count == 0 {
            return Ok(Vec::new());
        }
        let seed = self.rate_block_seed(coordinate, procedure_json, raw_rate);
        let limit = occurrence_count.min(SOURCE_WITNESS_OCCURRENCE_TARGET as u64);
        let mut permutation = HashMap::<u64, u64>::new();
        let mut elapsed_priority = 0.0f64;
        let mut candidates = Vec::new();
        for rank in 0..limit {
            let remaining = occurrence_count - rank;
            let uniform = deterministic_open_unit(&seed, b"gap", rank);
            elapsed_priority += -uniform.ln() / remaining as f64;
            if !elapsed_priority.is_finite() || elapsed_priority < 0.0 {
                return Err(io::Error::other(
                    "source witness occurrence priority is invalid",
                ));
            }
            let priority = elapsed_priority.to_bits();
            if priority > self.rate_occurrences.threshold() {
                break;
            }
            let occurrence_index =
                lazy_permutation_value(&seed, rank, occurrence_count, &mut permutation);
            self.rate_occurrences.mark_evaluated();
            candidates.push(RateOccurrenceCandidate {
                priority,
                occurrence_index,
            });
        }
        Ok(candidates)
    }

    pub fn provider_priority_if_candidate(
        &self,
        coordinate: SourceWitnessCoordinate,
        raw_provider: &[u8],
    ) -> io::Result<Option<u64>> {
        self.provider_references.add_population(1)?;
        let priority = self.content_priority(
            SourceWitnessKind::ProviderReference,
            coordinate,
            raw_provider,
        );
        if priority > self.provider_references.threshold() {
            return Ok(None);
        }
        self.provider_references.mark_evaluated();
        Ok(Some(priority))
    }

    pub fn commit_rate_occurrence(&self, input: RateOccurrenceWitnessInput<'_>) -> io::Result<()> {
        let tie_breaker = record_tie_breaker(
            SourceWitnessKind::RateOccurrence,
            input.coordinate,
            input.price_ordinal,
            input.provider_ordinal,
            input.raw_rate,
            input.linked_provider_sha256,
        );
        self.rate_occurrences.insert(SelectedRateWitness {
            priority: input.candidate.priority,
            tie_breaker,
            coordinate: input.coordinate,
            price_ordinal: input.price_ordinal,
            provider_ordinal: input.provider_ordinal,
            provider_evidence: input.provider_evidence.clone(),
            rate_source_locator: input.rate_source_locator,
            linked_provider_locator: input.linked_provider_locator,
            expected: input.expected.clone(),
        })
    }

    pub fn commit_provider_reference(
        &self,
        priority: u64,
        coordinate: SourceWitnessCoordinate,
        source_locator: ProviderSourceLocator,
        raw_provider: &[u8],
        expected: &Value,
    ) -> io::Result<()> {
        let tie_breaker = record_tie_breaker(
            SourceWitnessKind::ProviderReference,
            coordinate,
            0,
            0,
            raw_provider,
            None,
        );
        self.provider_references.insert(SelectedProviderWitness {
            priority,
            tie_breaker,
            coordinate,
            source_locator,
            expected: expected.clone(),
        })
    }

    pub fn write_bundle(&self, directory: &Path) -> io::Result<Value> {
        fs::create_dir_all(directory)?;
        self.seal_rate_sources()?;
        let mut rate_candidates = self.rate_occurrences.take_sorted()?;
        let mut provider_candidates = self.provider_references.take_sorted()?;
        validate_local_candidate_selection(
            "rate occurrence",
            self.rate_occurrences.population(),
            rate_candidates.len(),
            self.rate_occurrences.target,
        )?;
        validate_local_candidate_selection(
            "provider reference",
            self.provider_references.population(),
            provider_candidates.len(),
            self.provider_references.target,
        )?;
        if rate_candidates.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "strict V3 scan produced no queryable source occurrence witnesses",
            ));
        }
        let (rate_target, provider_target, _total_target) = local_source_witness_targets(
            self.rate_occurrences.population(),
            self.provider_references.population(),
        )?;
        rate_candidates.truncate(rate_target);
        provider_candidates.truncate(provider_target);
        let mut bundle_stage = SourceWitnessBundleStage::new(Arc::clone(&self.scratch_budget))?;
        for candidate in rate_candidates {
            let (procedure, raw_rate) = self.read_rate_source(candidate.rate_source_locator)?;
            let linked_provider_raw = candidate
                .linked_provider_locator
                .map(|locator| self.read_provider_source(locator))
                .transpose()?;
            bundle_stage.preflight_raw_evidence(&raw_rate)?;
            if let Some(linked_provider_raw) = linked_provider_raw.as_deref() {
                bundle_stage.preflight_raw_evidence(linked_provider_raw)?;
            }
            let (compressed_record, evidence) =
                self.externalized_record(SourceWitnessRecordInput {
                    kind: SourceWitnessKind::RateOccurrence,
                    priority: candidate.priority,
                    tie_breaker: candidate.tie_breaker,
                    coordinate: candidate.coordinate,
                    price_ordinal: candidate.price_ordinal,
                    provider_ordinal: candidate.provider_ordinal,
                    provider_evidence: Some(&candidate.provider_evidence),
                    procedure: Some(&procedure),
                    raw: &raw_rate,
                    linked_provider_raw: linked_provider_raw.as_deref(),
                    expected: &candidate.expected,
                })?;
            if let Err(error) = validate_compressed_record_size(compressed_record.len()) {
                self.rate_occurrences
                    .oversized
                    .fetch_add(1, AtomicOrdering::Relaxed);
                return Err(error);
            }
            for evidence_entry in evidence {
                bundle_stage.merge_evidence(evidence_entry)?;
            }
            bundle_stage.push_record(SourceWitnessKind::RateOccurrence, compressed_record)?;
        }
        for candidate in provider_candidates {
            let raw_provider = self.read_provider_source(candidate.source_locator)?;
            bundle_stage.preflight_raw_evidence(&raw_provider)?;
            let (compressed_record, evidence) =
                self.externalized_record(SourceWitnessRecordInput {
                    kind: SourceWitnessKind::ProviderReference,
                    priority: candidate.priority,
                    tie_breaker: candidate.tie_breaker,
                    coordinate: candidate.coordinate,
                    price_ordinal: 0,
                    provider_ordinal: 0,
                    provider_evidence: None,
                    procedure: None,
                    raw: &raw_provider,
                    linked_provider_raw: None,
                    expected: &candidate.expected,
                })?;
            validate_compressed_record_size(compressed_record.len())?;
            for evidence_entry in evidence {
                bundle_stage.merge_evidence(evidence_entry)?;
            }
            bundle_stage.push_record(SourceWitnessKind::ProviderReference, compressed_record)?;
        }
        let rate_bytes = bundle_stage.rate_record_bytes;
        let provider_bytes = bundle_stage.provider_record_bytes;
        let mut rate_metrics = self
            .rate_occurrences
            .metrics(bundle_stage.rate_records.len(), rate_bytes);
        let rate_metrics_map = io_option(
            rate_metrics.as_object_mut(),
            io::ErrorKind::Other,
            "source witness rate metrics must be a JSON object",
        )?;
        rate_metrics_map.insert(
            "emitted_rate_row_count".to_string(),
            json!(self.rate_rows.load(AtomicOrdering::Relaxed)),
        );
        rate_metrics_map.insert(
            "unqueryable_rate_row_count".to_string(),
            json!(self.unqueryable_rate_rows.load(AtomicOrdering::Relaxed)),
        );
        let header = json!({
            "contract": SOURCE_WITNESS_CONTRACT,
            "format_version": SOURCE_WITNESS_FORMAT_VERSION,
            "selection_method": SOURCE_WITNESS_SELECTION,
            "unqueryable_rate_policy": SOURCE_WITNESS_UNQUERYABLE_POLICY,
            "raw_source_sha256": self.raw_source_sha256_hex,
        "occurrence_target": SOURCE_WITNESS_OCCURRENCE_TARGET,
        "total_target": SOURCE_WITNESS_TOTAL_TARGET,
            "provider_quota": SOURCE_WITNESS_PROVIDER_QUOTA,
            "scratch": {
                "spool_bytes": self.scratch_budget.used_bytes(),
                "spool_byte_limit": self.scratch_budget.byte_limit(),
                "representation": "source_fragment_locators_v1",
            },
            "rate_occurrence": rate_metrics,
            "provider_reference": self.provider_references.metrics(bundle_stage.provider_records.len(), provider_bytes),
        });
        let header_bytes = serde_json::to_vec(&header).map_err(to_io_error)?;
        let header_length = io_result(
            u32::try_from(header_bytes.len()),
            io::ErrorKind::InvalidData,
            "source witness header is too large",
        )?;
        let record_count = bundle_stage
            .rate_records
            .len()
            .saturating_add(bundle_stage.provider_records.len());
        let record_count_u32 = io_result(
            u32::try_from(record_count),
            io::ErrorKind::InvalidData,
            "source witness count is too large",
        )?;
        let projected_bundle_bytes = (SOURCE_WITNESS_MAGIC.len() as u64)
            .saturating_add(4)
            .saturating_add(header_bytes.len() as u64)
            .saturating_add(bundle_stage.logical_body_bytes);
        if projected_bundle_bytes > SOURCE_WITNESS_MAX_INTERMEDIATE_BUNDLE_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "strict V3 source witness candidate bundle would use {projected_bundle_bytes} bytes, exceeding the fail-closed {}-byte intermediate budget",
                    SOURCE_WITNESS_MAX_INTERMEDIATE_BUNDLE_BYTES,
                ),
            ));
        }
        let path = unique_bundle_path(directory)?;
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        let mut writer = HashingWriter::new(BufWriter::new(file));
        writer.write_all(SOURCE_WITNESS_MAGIC)?;
        writer.write_all(&header_length.to_be_bytes())?;
        writer.write_all(&header_bytes)?;
        let evidence_count = io_result(
            u32::try_from(bundle_stage.evidence_by_sha256.len()),
            io::ErrorKind::InvalidData,
            "source witness evidence dictionary is too large",
        )?;
        writer.write_all(&evidence_count.to_be_bytes())?;
        let mut evidence_entries = bundle_stage
            .evidence_by_sha256
            .iter()
            .map(|(digest, entry)| (*digest, entry.raw_byte_count, entry.payload))
            .collect::<Vec<_>>();
        evidence_entries.sort_unstable_by_key(|(digest, _raw_byte_count, _payload)| *digest);
        for (digest, raw_byte_count, payload) in evidence_entries {
            writer.write_all(&digest)?;
            writer.write_all(&raw_byte_count.to_be_bytes())?;
            writer.write_all(&payload.length.to_be_bytes())?;
            bundle_stage.copy_payload(&mut writer, payload)?;
        }
        writer.write_all(&record_count_u32.to_be_bytes())?;
        let record_payloads = bundle_stage
            .rate_records
            .iter()
            .chain(bundle_stage.provider_records.iter())
            .map(|record| record.payload)
            .collect::<Vec<_>>();
        for payload in record_payloads {
            writer.write_all(&payload.length.to_be_bytes())?;
            bundle_stage.copy_payload(&mut writer, payload)?;
        }
        let (writer, digest, byte_count) = writer.finish()?;
        if byte_count != projected_bundle_bytes {
            return Err(io::Error::other(
                "source witness candidate bundle byte count is inconsistent",
            ));
        }
        writer.get_ref().sync_all()?;
        drop(writer);
        Ok(json!({
            "path": path.display().to_string(),
            "contract": SOURCE_WITNESS_CONTRACT,
            "format_version": SOURCE_WITNESS_FORMAT_VERSION,
            "selection_method": SOURCE_WITNESS_SELECTION,
            "raw_source_sha256": self.raw_source_sha256_hex,
            "row_count": record_count,
            "occurrence_witness_count": bundle_stage.rate_records.len(),
            "provider_witness_count": bundle_stage.provider_records.len(),
            "byte_count": byte_count,
            "sha256": hex_digest(&digest),
            "queryable_occurrence_population_count": self.rate_occurrences.population(),
            "provider_population_count": self.provider_references.population(),
            "scratch_spool_bytes": self.scratch_budget.used_bytes(),
            "scratch_spool_byte_limit": self.scratch_budget.byte_limit(),
        }))
    }

    fn rate_block_seed(
        &self,
        coordinate: SourceWitnessCoordinate,
        procedure_json: &[u8],
        raw_rate: &[u8],
    ) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(SourceWitnessKind::RateOccurrence.domain());
        digest.update(self.raw_source_sha256);
        digest.update(coordinate.object_ordinal.to_be_bytes());
        digest.update(coordinate.rate_ordinal.to_be_bytes());
        digest.update((procedure_json.len() as u64).to_be_bytes());
        digest.update(procedure_json);
        digest.update((raw_rate.len() as u64).to_be_bytes());
        digest.update(raw_rate);
        digest.finalize().into()
    }

    fn content_priority(
        &self,
        kind: SourceWitnessKind,
        coordinate: SourceWitnessCoordinate,
        raw: &[u8],
    ) -> u64 {
        let mut digest = Xxh3::new();
        digest.update(kind.domain());
        digest.update(&self.raw_source_sha256);
        digest.update(&coordinate.object_ordinal.to_be_bytes());
        digest.update(&coordinate.rate_ordinal.to_be_bytes());
        digest.update(&(raw.len() as u64).to_be_bytes());
        digest.update(raw);
        digest.digest()
    }

    fn externalized_record(
        &self,
        input: SourceWitnessRecordInput<'_>,
    ) -> io::Result<(Vec<u8>, Vec<PreparedSourceEvidence>)> {
        let SourceWitnessRecordInput {
            kind,
            priority,
            tie_breaker,
            coordinate,
            price_ordinal,
            provider_ordinal,
            provider_evidence,
            procedure,
            raw,
            linked_provider_raw,
            expected,
        } = input;
        validate_decoded_evidence_size("raw token", raw.len())?;
        if let Some(linked_provider_raw) = linked_provider_raw {
            validate_decoded_evidence_size("linked provider token", linked_provider_raw.len())?;
        }
        let raw_digest: [u8; 32] = Sha256::digest(raw).into();
        let linked_provider_digest = linked_provider_raw.map(Sha256::digest);
        let metadata = json!({
            "contract": SOURCE_WITNESS_RECORD_CONTRACT,
            "kind": kind.label(),
            "priority": format!("{priority:016x}"),
            "tie_breaker": hex_digest(&tie_breaker),
            "coordinate": {
                "object_ordinal": coordinate.object_ordinal,
                "rate_ordinal": coordinate.rate_ordinal,
                "price_ordinal": price_ordinal,
                "provider_ordinal": provider_ordinal,
            },
            "raw_sha256": hex_digest(&raw_digest),
            "linked_provider_sha256": linked_provider_digest.map(|digest| hex_digest(&digest.into())),
            "provider_evidence": provider_evidence,
            "procedure": procedure,
            "expected": expected,
        });
        let metadata_bytes = serde_json::to_vec(&metadata).map_err(to_io_error)?;
        let metadata_length = io_result(
            u32::try_from(metadata_bytes.len()),
            io::ErrorKind::InvalidData,
            "source witness record metadata is too large",
        )?;
        let raw_length = io_result(
            u32::try_from(raw.len()),
            io::ErrorKind::InvalidData,
            "source witness raw JSON token is too large",
        )?;
        let linked_provider_raw = linked_provider_raw.unwrap_or_default();
        let linked_provider_length = io_result(
            u32::try_from(linked_provider_raw.len()),
            io::ErrorKind::InvalidData,
            "source witness linked provider token is too large",
        )?;
        let mut record = Vec::with_capacity(
            SOURCE_WITNESS_RECORD_MAGIC
                .len()
                .saturating_add(metadata_bytes.len())
                .saturating_add(12),
        );
        record.extend_from_slice(SOURCE_WITNESS_RECORD_MAGIC);
        record.extend_from_slice(&metadata_length.to_be_bytes());
        record.extend_from_slice(&metadata_bytes);
        record.extend_from_slice(&0u32.to_be_bytes());
        record.extend_from_slice(&0u32.to_be_bytes());
        let mut evidence = vec![PreparedSourceEvidence {
            sha256: raw_digest,
            raw_byte_count: raw_length,
            compressed: compress_source_witness_record(raw)?,
        }];
        if let Some(linked_provider_digest) = linked_provider_digest {
            let linked_provider_digest: [u8; 32] = linked_provider_digest.into();
            if linked_provider_digest == raw_digest {
                if linked_provider_raw != raw {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "source witness evidence digest is inconsistent",
                    ));
                }
            } else {
                evidence.push(PreparedSourceEvidence {
                    sha256: linked_provider_digest,
                    raw_byte_count: linked_provider_length,
                    compressed: compress_source_witness_record(linked_provider_raw)?,
                });
            }
        }
        Ok((compress_source_witness_record(&record)?, evidence))
    }
}

fn zlib_compress_record(record: &[u8], compression: Compression) -> io::Result<Vec<u8>> {
    let mut encoder = ZlibEncoder::new(Vec::new(), compression);
    encoder.write_all(record)?;
    encoder.finish()
}

fn compress_source_witness_record_with_limit(
    record: &[u8],
    fast_limit: usize,
) -> io::Result<Vec<u8>> {
    let fast_record = zlib_compress_record(record, Compression::fast())?;
    if fast_record.len() <= fast_limit {
        return Ok(fast_record);
    }
    zlib_compress_record(record, Compression::best())
}

fn compress_source_witness_record(record: &[u8]) -> io::Result<Vec<u8>> {
    compress_source_witness_record_with_limit(record, SOURCE_WITNESS_MAX_COMPRESSED_RECORD_BYTES)
}

fn deterministic_u64(seed: &[u8; 32], domain: &[u8], counter: u64, attempt: u64) -> u64 {
    let mut digest = Xxh3::new();
    digest.update(seed);
    digest.update(domain);
    digest.update(&counter.to_be_bytes());
    digest.update(&attempt.to_be_bytes());
    digest.digest()
}

fn deterministic_open_unit(seed: &[u8; 32], domain: &[u8], counter: u64) -> f64 {
    let value = deterministic_u64(seed, domain, counter, 0) >> 11;
    (value as f64 + 1.0) / ((1u64 << 53) as f64 + 1.0)
}

fn deterministic_bounded(seed: &[u8; 32], counter: u64, bound: u64) -> u64 {
    debug_assert!(bound > 0);
    let rejection_floor = u64::MAX - (u64::MAX % bound);
    let mut attempt = 0u64;
    loop {
        let value = deterministic_u64(seed, b"permutation", counter, attempt);
        if value < rejection_floor {
            return value % bound;
        }
        attempt = attempt.saturating_add(1);
    }
}

fn lazy_permutation_value(
    seed: &[u8; 32],
    rank: u64,
    population: u64,
    swaps: &mut HashMap<u64, u64>,
) -> u64 {
    let selected_position = rank + deterministic_bounded(seed, rank, population - rank);
    let selected_value = swaps
        .get(&selected_position)
        .copied()
        .unwrap_or(selected_position);
    let rank_value = swaps.get(&rank).copied().unwrap_or(rank);
    if selected_position != rank {
        swaps.insert(selected_position, rank_value);
    }
    swaps.remove(&rank);
    selected_value
}

fn record_tie_breaker(
    kind: SourceWitnessKind,
    coordinate: SourceWitnessCoordinate,
    price_ordinal: u64,
    provider_ordinal: u64,
    raw: &[u8],
    linked_provider_sha256: Option<[u8; 32]>,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(kind.domain());
    digest.update(coordinate.object_ordinal.to_be_bytes());
    digest.update(coordinate.rate_ordinal.to_be_bytes());
    digest.update(price_ordinal.to_be_bytes());
    digest.update(provider_ordinal.to_be_bytes());
    digest.update(Sha256::digest(raw));
    if let Some(linked_provider_sha256) = linked_provider_sha256 {
        digest.update(linked_provider_sha256);
    }
    digest.finalize().into()
}

fn local_source_witness_targets(
    occurrence_population: u64,
    provider_population: u64,
) -> io::Result<(usize, usize, usize)> {
    let occurrence_target = occurrence_population.min(SOURCE_WITNESS_OCCURRENCE_TARGET as u64);
    let provider_target = provider_population.min(SOURCE_WITNESS_PROVIDER_QUOTA as u64);
    let total_target = io_option(
        occurrence_target.checked_add(provider_target),
        io::ErrorKind::Other,
        "source witness target overflow",
    )?;
    Ok((
        occurrence_target as usize,
        provider_target as usize,
        total_target as usize,
    ))
}

fn validate_local_candidate_selection(
    label: &str,
    population: u64,
    selected: usize,
    candidate_target: usize,
) -> io::Result<()> {
    let expected = population.min(candidate_target as u64) as usize;
    if selected != expected {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "strict V3 {label} witness coverage is incomplete: selected={selected}, expected={expected}, population={population}"
            ),
        ));
    }
    Ok(())
}

fn validate_decoded_evidence_size(label: &str, decoded_bytes: usize) -> io::Result<()> {
    if decoded_bytes <= SOURCE_WITNESS_MAX_DECODED_EVIDENCE_BYTES {
        return Ok(());
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "source witness {label} is {decoded_bytes} bytes, exceeding the fail-closed {}-byte decoded evidence limit",
            SOURCE_WITNESS_MAX_DECODED_EVIDENCE_BYTES,
        ),
    ))
}

fn validate_compressed_record_size(compressed_record_bytes: usize) -> io::Result<()> {
    if compressed_record_bytes <= SOURCE_WITNESS_MAX_COMPRESSED_RECORD_BYTES {
        return Ok(());
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "selected source witness record is {compressed_record_bytes} bytes, exceeding the fail-closed {}-byte limit",
            SOURCE_WITNESS_MAX_COMPRESSED_RECORD_BYTES,
        ),
    ))
}

#[cfg(test)]
fn merge_selected_evidence(
    evidence_by_sha256: &mut HashMap<[u8; 32], SelectedSourceEvidence>,
    evidence_entry: PreparedSourceEvidence,
) -> io::Result<()> {
    if let Some(existing) = evidence_by_sha256.get_mut(&evidence_entry.sha256) {
        if existing.raw_byte_count != evidence_entry.raw_byte_count
            || existing.compressed != evidence_entry.compressed
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "source witness evidence digest is inconsistent",
            ));
        }
        existing.reference_count = existing.reference_count.saturating_add(1);
        return Ok(());
    }
    evidence_by_sha256.insert(
        evidence_entry.sha256,
        SelectedSourceEvidence {
            raw_byte_count: evidence_entry.raw_byte_count,
            compressed: evidence_entry.compressed,
            reference_count: 1,
        },
    );
    Ok(())
}

fn unique_bundle_path(directory: &Path) -> io::Result<PathBuf> {
    for attempt in 0..1_000u32 {
        let path = directory.join(format!(
            "ptg2-v3-source-witness-{}-{attempt:04}.bin",
            std::process::id()
        ));
        if !path.exists() {
            return Ok(path);
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "unable to allocate source witness bundle",
    ))
}

fn hex_digest(digest: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in digest {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn to_io_error(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

fn io_result<T, E>(
    result: Result<T, E>,
    kind: io::ErrorKind,
    message: &'static str,
) -> io::Result<T> {
    match result {
        Ok(value) => Ok(value),
        Err(_) => Err(io::Error::new(kind, message)),
    }
}

fn io_option<T>(value: Option<T>, kind: io::ErrorKind, message: &'static str) -> io::Result<T> {
    match value {
        Some(value) => Ok(value),
        None => Err(io::Error::new(kind, message)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::read::ZlibDecoder;
    use std::fs::File;
    use std::io::Read;

    type SelectedOccurrenceDigest = ((u64, [u8; 32]), [u8; 32]);

    fn selected_rate(priority: u64) -> SelectedRateWitness {
        SelectedRateWitness {
            priority,
            tie_breaker: [priority as u8; 32],
            coordinate: SourceWitnessCoordinate::new(priority, 0),
            price_ordinal: 0,
            provider_ordinal: 0,
            provider_evidence: json!({}),
            rate_source_locator: RateSourceLocator {
                shard: 0,
                offset: priority,
                length: 1,
            },
            linked_provider_locator: None,
            expected: json!({}),
        }
    }

    #[test]
    fn sampler_is_order_independent_and_bounded() {
        let sampler = SourceWitnessSampler::new(3, 1_024);
        for priority in [9, 2, 7, 1, 3, 8] {
            sampler.add_population(1).unwrap();
            sampler.insert(selected_rate(priority)).unwrap();
        }
        let selected = sampler.take_sorted().unwrap();
        assert_eq!(
            selected
                .iter()
                .map(|record| record.priority)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(sampler.population(), 6);

        let provider_sampler = ProviderWitnessSampler::new(1);
        for priority in [2, 3, 1] {
            provider_sampler
                .insert(SelectedProviderWitness {
                    priority,
                    tie_breaker: [priority as u8; 32],
                    coordinate: SourceWitnessCoordinate::new(priority, 0),
                    source_locator: ProviderSourceLocator {
                        shard: 0,
                        offset: priority,
                        length: 1,
                    },
                    expected: json!({}),
                })
                .unwrap();
        }
        assert_eq!(provider_sampler.take_sorted().unwrap()[0].priority, 1,);
    }

    #[test]
    fn witness_ordering_metrics_and_population_overflow_are_explicit() {
        let source = selected_rate;
        let _ = source(1).eq(&source(1));
        let _ = source(1).eq(&source(2));

        let provider = |priority| SelectedProviderWitness {
            priority,
            tie_breaker: [priority as u8; 32],
            coordinate: SourceWitnessCoordinate::new(priority, 0),
            source_locator: ProviderSourceLocator {
                shard: 0,
                offset: priority,
                length: 1,
            },
            expected: json!({}),
        };
        let _ = provider(1).eq(&provider(1));
        let _ = provider(1).eq(&provider(2));
        let _ = source(1).partial_cmp(&source(2));
        let _ = provider(1).partial_cmp(&provider(2));

        let source_sampler = SourceWitnessSampler::new(1, 32);
        source_sampler.add_population(u64::MAX).unwrap();
        source_sampler.mark_evaluated();
        source_sampler.add_population(1).unwrap_err();
        assert_eq!(source_sampler.metrics(0, 0)["candidate_evaluated_count"], 1);

        let provider_sampler = ProviderWitnessSampler::new(1);
        provider_sampler.add_population(u64::MAX).unwrap();
        provider_sampler.mark_evaluated();
        provider_sampler.add_population(1).unwrap_err();
        assert_eq!(
            provider_sampler.metrics(0, 0)["candidate_evaluated_count"],
            1
        );
    }

    #[test]
    fn hashing_writer_hashes_exactly_the_written_bytes() {
        let mut writer = HashingWriter::new(Vec::new());
        writer.write_all(b"source-witness").unwrap();
        writer.flush().unwrap();
        let (bytes, digest, byte_count) = writer.finish().unwrap();

        assert_eq!(bytes, b"source-witness");
        let expected_digest: [u8; 32] = Sha256::digest(&bytes).into();
        assert_eq!(digest, expected_digest);
        assert_eq!(byte_count, bytes.len() as u64);
    }

    #[test]
    fn collector_rejects_invalid_identity_and_enforces_spool_lifecycle() {
        SourceWitnessCollector::new("short").err().unwrap();
        SourceWitnessCollector::new(&format!("{}a", "€".repeat(21)))
            .err()
            .unwrap();
        SourceWitnessCollector::new(&"gg".repeat(32)).err().unwrap();

        let collector = SourceWitnessCollector::new(&"ab".repeat(32)).unwrap();
        collector
            .provider_references
            .threshold
            .store(0, AtomicOrdering::Relaxed);
        collector
            .provider_priority_if_candidate(SourceWitnessCoordinate::new(0, 0), b"p")
            .unwrap();
        let locator = ProviderSourceLocator {
            shard: 0,
            offset: 0,
            length: 1,
        };
        collector.store_provider_source(0, b"x").unwrap_err();
        collector.seal_provider_sources().unwrap_err();
        collector.read_provider_source(locator).unwrap_err();
        collector.configure_provider_spools(0).unwrap_err();
        collector.configure_provider_spools(1).unwrap();
        collector.configure_provider_spools(1).unwrap_err();
        collector.store_provider_source(1, b"x").unwrap_err();

        let locator = collector.store_provider_source(0, b"provider").unwrap();
        collector.read_provider_source(locator).unwrap_err();
        collector.seal_provider_sources().unwrap();
        collector.seal_provider_sources().unwrap();
        collector.store_provider_source(0, b"late").unwrap_err();
        assert_eq!(
            collector.read_provider_source(locator).unwrap(),
            b"provider"
        );
        collector
            .read_provider_source(ProviderSourceLocator {
                shard: 1,
                offset: 0,
                length: 1,
            })
            .unwrap_err();

        collector.rate_rows.store(u64::MAX, AtomicOrdering::Relaxed);
        collector.record_rate_population(1, 0, 0).unwrap_err();

        let collector = SourceWitnessCollector::new(&"ab".repeat(32)).unwrap();
        collector
            .unqueryable_rate_rows
            .store(u64::MAX, AtomicOrdering::Relaxed);
        collector.record_rate_population(0, 1, 0).unwrap_err();

        let collector = SourceWitnessCollector::new(&"ab".repeat(32)).unwrap();
        collector
            .rate_occurrences
            .population
            .store(u64::MAX, AtomicOrdering::Relaxed);
        collector.record_rate_population(0, 0, 1).unwrap_err();

        let collector = SourceWitnessCollector::new(&"ab".repeat(32)).unwrap();
        collector
            .provider_references
            .population
            .store(u64::MAX, AtomicOrdering::Relaxed);
        collector
            .provider_priority_if_candidate(SourceWitnessCoordinate::new(0, 0), b"provider")
            .unwrap_err();
    }

    #[test]
    fn poisoned_samplers_and_unconfigured_rate_spools_fail_closed() {
        let source_sampler = SourceWitnessSampler::new(1, 32);
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let _guard = source_sampler.state.lock().unwrap();
                    panic!("poison source sampler");
                })
                .join()
                .unwrap_err();
        });
        assert!(source_sampler.insert(selected_rate(1)).is_err());
        assert!(source_sampler.take_sorted().is_err());

        let provider_sampler = ProviderWitnessSampler::new(1);
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let _guard = provider_sampler.state.lock().unwrap();
                    panic!("poison provider sampler");
                })
                .join()
                .unwrap_err();
        });
        let selected_provider = SelectedProviderWitness {
            priority: 1,
            tie_breaker: [1; 32],
            coordinate: SourceWitnessCoordinate::new(1, 0),
            source_locator: ProviderSourceLocator {
                shard: 0,
                offset: 0,
                length: 1,
            },
            expected: json!({}),
        };
        assert!(provider_sampler.insert(selected_provider).is_err());
        assert!(provider_sampler.take_sorted().is_err());

        let collector = SourceWitnessCollector::new(&"ab".repeat(32)).unwrap();
        let coordinate = SourceWitnessCoordinate::new(0, 0);
        let procedure = Map::from_iter([("billing_code".to_owned(), json!("99213"))]);
        assert!(collector
            .store_rate_source(coordinate, &procedure, b"{}")
            .is_err());
        assert!(collector.seal_rate_sources().is_err());
        assert!(collector
            .read_rate_source(RateSourceLocator {
                shard: 0,
                offset: 0,
                length: 1,
            })
            .is_err());
        collector.configure_rate_spools(1).unwrap();
        assert!(collector.configure_rate_spools(1).is_err());
        collector
            .rate_occurrences
            .threshold
            .store(0, AtomicOrdering::Relaxed);
        assert!(collector
            .rate_occurrence_candidates_untracked(coordinate, b"{}", b"{}", 10)
            .unwrap()
            .is_empty());

        let output = tempfile::tempdir().unwrap();
        let empty = SourceWitnessCollector::new(&"ab".repeat(32)).unwrap();
        empty.configure_rate_spools(1).unwrap();
        let error = empty.write_bundle(output.path()).unwrap_err();
        assert!(error.to_string().contains("no queryable source occurrence"));
    }

    #[test]
    fn deterministic_rejection_and_identical_linked_evidence_are_explicit() {
        let seed = [7; 32];
        let bound = u64::MAX / 2 + 1;
        let rejection_floor = u64::MAX - (u64::MAX % bound);
        let counter = (0..10_000)
            .find(|counter| {
                deterministic_u64(&seed, b"permutation", *counter, 0) >= rejection_floor
            })
            .expect("fixture must exercise rejection sampling");
        assert!(deterministic_bounded(&seed, counter, bound) < bound);

        let collector = SourceWitnessCollector::new(&"ab".repeat(32)).unwrap();
        let raw = br#"{"provider_references":[7]}"#;
        let (_record, evidence) = collector
            .externalized_record(SourceWitnessRecordInput {
                kind: SourceWitnessKind::RateOccurrence,
                priority: 1,
                tie_breaker: [2; 32],
                coordinate: SourceWitnessCoordinate::new(0, 0),
                price_ordinal: 0,
                provider_ordinal: 0,
                provider_evidence: Some(&json!({"source_kind": "provider_reference"})),
                procedure: Some(&Map::new()),
                raw,
                linked_provider_raw: Some(raw),
                expected: &json!({}),
            })
            .unwrap();
        assert_eq!(evidence.len(), 1);
    }

    #[test]
    fn witness_validation_helpers_fail_closed_on_inconsistent_inputs() {
        validate_local_candidate_selection("rate", 2, 2, 10).unwrap();
        validate_local_candidate_selection("rate", 2, 1, 10).unwrap_err();
        validate_decoded_evidence_size("token", SOURCE_WITNESS_MAX_DECODED_EVIDENCE_BYTES).unwrap();
        let decoded_limit_error =
            validate_decoded_evidence_size("token", SOURCE_WITNESS_MAX_DECODED_EVIDENCE_BYTES + 1)
                .unwrap_err();
        assert!(decoded_limit_error.to_string().contains("fail-closed"));
        validate_compressed_record_size(SOURCE_WITNESS_MAX_COMPRESSED_RECORD_BYTES).unwrap();
        validate_compressed_record_size(SOURCE_WITNESS_MAX_COMPRESSED_RECORD_BYTES + 1)
            .unwrap_err();

        let mut evidence = HashMap::new();
        let entry = PreparedSourceEvidence {
            sha256: [7; 32],
            raw_byte_count: 3,
            compressed: vec![1, 2, 3],
        };
        merge_selected_evidence(&mut evidence, entry).unwrap();
        merge_selected_evidence(
            &mut evidence,
            PreparedSourceEvidence {
                sha256: [7; 32],
                raw_byte_count: 3,
                compressed: vec![1, 2, 3],
            },
        )
        .unwrap();
        assert_eq!(evidence[&[7; 32]].reference_count, 2);
        merge_selected_evidence(
            &mut evidence,
            PreparedSourceEvidence {
                sha256: [7; 32],
                raw_byte_count: 4,
                compressed: vec![1, 2, 3],
            },
        )
        .unwrap_err();

        let directory = tempfile::tempdir().unwrap();
        for attempt in 0..1_000u32 {
            File::create(directory.path().join(format!(
                "ptg2-v3-source-witness-{}-{attempt:04}.bin",
                std::process::id()
            )))
            .unwrap();
        }
        assert_eq!(
            unique_bundle_path(directory.path()).unwrap_err().kind(),
            io::ErrorKind::AlreadyExists,
        );
        let _ = to_io_error("invalid witness value");
        let empty = SourceWitnessCollector::new(&"ab".repeat(32)).unwrap();
        empty
            .write_bundle(tempfile::tempdir().unwrap().path())
            .unwrap_err();
    }

    #[test]
    fn bundle_stage_rejects_aggregate_budgets_before_writing_payload() {
        let scratch_budget = SourceWitnessScratchBudget::from_env().unwrap();
        let mut logical_stage = SourceWitnessBundleStage::new(Arc::clone(&scratch_budget)).unwrap();
        logical_stage
            .reserve_logical_body(
                SOURCE_WITNESS_MAX_INTERMEDIATE_BUNDLE_BYTES - logical_stage.logical_body_bytes,
            )
            .unwrap();
        let logical_error = logical_stage.reserve_logical_body(1).unwrap_err();
        assert!(logical_error.to_string().contains("intermediate budget"));

        let mut decoded_stage = SourceWitnessBundleStage::new(scratch_budget).unwrap();
        decoded_stage.decoded_evidence_bytes = SOURCE_WITNESS_MAX_DECODED_TOTAL_BYTES - 1;
        let decoded_error = decoded_stage.preflight_raw_evidence(b"xx").unwrap_err();

        assert!(decoded_error
            .to_string()
            .contains("aggregate decoded budget"));
        assert_eq!(
            decoded_stage
                .payload_file
                .as_file()
                .metadata()
                .unwrap()
                .len(),
            0
        );
        assert!(decoded_stage.evidence_by_sha256.is_empty());
    }

    #[test]
    fn bundle_stage_rejects_inconsistent_and_truncated_staged_payloads() {
        let scratch_budget = SourceWitnessScratchBudget::from_env().unwrap();

        let mut overflow = SourceWitnessBundleStage::new(Arc::clone(&scratch_budget)).unwrap();
        overflow.logical_body_bytes = u64::MAX;
        assert!(overflow.reserve_logical_body(1).is_err());
        overflow.decoded_evidence_bytes = u64::MAX;
        assert!(overflow.reserve_decoded_evidence([1; 32], 1).is_err());

        let mut inconsistent = SourceWitnessBundleStage::new(Arc::clone(&scratch_budget)).unwrap();
        inconsistent.reserve_decoded_evidence([2; 32], 1).unwrap();
        assert!(inconsistent.reserve_decoded_evidence([2; 32], 2).is_err());
        inconsistent
            .merge_evidence(PreparedSourceEvidence {
                sha256: [3; 32],
                raw_byte_count: 3,
                compressed: vec![1, 2, 3],
            })
            .unwrap();
        assert!(inconsistent
            .merge_evidence(PreparedSourceEvidence {
                sha256: [3; 32],
                raw_byte_count: 3,
                compressed: vec![3, 2, 1],
            })
            .is_err());

        let mut truncated = SourceWitnessBundleStage::new(scratch_budget).unwrap();
        let locator = truncated.append_payload(b"payload").unwrap();
        truncated.payload_file.as_file_mut().set_len(1).unwrap();
        assert!(truncated.copy_payload(&mut Vec::new(), locator).is_err());
    }

    #[test]
    fn corrupt_rate_source_fragments_fail_at_the_exact_boundary() {
        fn read_fragment(fragment: &[u8]) -> io::Result<(Map<String, Value>, Vec<u8>)> {
            let collector = SourceWitnessCollector::new(&"ab".repeat(32))?;
            collector.configure_rate_spools(1)?;
            let locator = collector.rate_spools.get().unwrap().append(0, fragment)?;
            collector.seal_rate_sources()?;
            collector.read_rate_source(locator)
        }

        assert!(read_fragment(b"x").is_err());
        assert!(read_fragment(&[0, 0, 0, 5, b'{', b'}']).is_err());
        let mut scalar = 4u32.to_be_bytes().to_vec();
        scalar.extend_from_slice(b"null");
        assert!(read_fragment(&scalar).is_err());
    }

    #[test]
    fn oversized_procedure_metadata_is_rejected_before_spooling() {
        let collector = SourceWitnessCollector::new(&"ab".repeat(32)).unwrap();
        let procedure = Map::from_iter([(
            "oversized".to_owned(),
            Value::String("x".repeat(SOURCE_WITNESS_MAX_COMPRESSED_RECORD_BYTES)),
        )]);
        let error = collector
            .store_rate_source(SourceWitnessCoordinate::new(0, 0), &procedure, b"{}")
            .unwrap_err();
        assert!(error.to_string().contains("metadata limit"));
    }

    #[test]
    fn occurrence_candidates_are_stable_across_repeated_calls() {
        let first = SourceWitnessCollector::new(&"ab".repeat(32)).unwrap();
        let second = SourceWitnessCollector::new(&"ab".repeat(32)).unwrap();
        let coordinate = SourceWitnessCoordinate::new(7, 11);
        let procedure = br#"{"billing_code":"99213"}"#;
        let rate =
            br#"{"provider_references":[7],"negotiated_prices":[{"negotiated_rate":1.20e2}]}"#;

        assert_eq!(
            first
                .rate_occurrence_candidates(coordinate, procedure, rate, 10_000)
                .unwrap(),
            second
                .rate_occurrence_candidates(coordinate, procedure, rate, 10_000)
                .unwrap(),
        );
    }

    #[test]
    fn batched_rate_population_preserves_candidates_and_exact_counts() {
        let direct = SourceWitnessCollector::new(&"ef".repeat(32)).unwrap();
        let batched = SourceWitnessCollector::new(&"ef".repeat(32)).unwrap();
        let procedure = br#"{"billing_code":"99213"}"#;
        let occurrence_counts = [0, 1, 8, 64, 10_000];
        let mut direct_candidates = Vec::new();
        let mut batched_candidates = Vec::new();
        let mut occurrence_population = 0u64;

        for (object_ordinal, occurrence_count) in occurrence_counts.into_iter().enumerate() {
            let coordinate = SourceWitnessCoordinate::new(object_ordinal as u64, 0);
            let raw_rate = format!(
                "{{\"provider_references\":[7],\"negotiated_prices\":[{{\"negotiated_rate\":{object_ordinal}}}]}}"
            );
            direct_candidates.extend(
                direct
                    .rate_occurrence_candidates(
                        coordinate,
                        procedure,
                        raw_rate.as_bytes(),
                        occurrence_count,
                    )
                    .unwrap(),
            );
            batched_candidates.extend(
                batched
                    .rate_occurrence_candidates_untracked(
                        coordinate,
                        procedure,
                        raw_rate.as_bytes(),
                        occurrence_count,
                    )
                    .unwrap(),
            );
            occurrence_population += occurrence_count;
        }
        batched
            .record_rate_population(
                occurrence_counts.len() as u64,
                occurrence_counts
                    .iter()
                    .filter(|occurrence_count| **occurrence_count == 0)
                    .count() as u64,
                occurrence_population,
            )
            .unwrap();

        assert_eq!(direct_candidates, batched_candidates);
        assert_eq!(
            direct.rate_rows.load(AtomicOrdering::Relaxed),
            batched.rate_rows.load(AtomicOrdering::Relaxed)
        );
        assert_eq!(
            direct.unqueryable_rate_rows.load(AtomicOrdering::Relaxed),
            batched.unqueryable_rate_rows.load(AtomicOrdering::Relaxed)
        );
        assert_eq!(
            direct.rate_occurrences.population(),
            batched.rate_occurrences.population()
        );
    }

    fn selected_occurrences_for_object_order(
        object_order: impl IntoIterator<Item = u64>,
    ) -> Vec<SelectedOccurrenceDigest> {
        let collector = SourceWitnessCollector::new(&"cd".repeat(32)).unwrap();
        collector.configure_rate_spools(2).unwrap();
        let procedure = Map::from_iter([
            ("billing_code_type".to_string(), json!("CPT")),
            ("billing_code".to_string(), json!("99213")),
        ]);
        let procedure_json = serde_json::to_vec(&procedure).unwrap();
        for object_ordinal in object_order {
            let raw_rate = format!(
                "{{\"provider_references\":[7],\"negotiated_prices\":[{{\"negotiated_rate\":{object_ordinal}}}]}}"
            );
            let coordinate = SourceWitnessCoordinate::new(object_ordinal, 0);
            let candidates = collector
                .rate_occurrence_candidates(coordinate, &procedure_json, raw_rate.as_bytes(), 64)
                .unwrap();
            let rate_source_locator = collector
                .store_rate_source(coordinate, &procedure, raw_rate.as_bytes())
                .unwrap();
            for candidate in candidates {
                let price_ordinal = candidate.occurrence_index / 8;
                let provider_ordinal = candidate.occurrence_index % 8;
                collector
                    .commit_rate_occurrence(RateOccurrenceWitnessInput {
                        candidate,
                        coordinate,
                        price_ordinal,
                        provider_ordinal,
                        provider_evidence: &json!({
                            "source_kind": "inline_provider_group",
                            "provider_group_ordinal": 0,
                            "npi_ordinal": provider_ordinal,
                        }),
                        rate_source_locator,
                        raw_rate: raw_rate.as_bytes(),
                        linked_provider_locator: None,
                        linked_provider_sha256: None,
                        expected: &json!({"contract": "ptg2_v3_source_rate_occurrence_expected_v2"}),
                    })
                    .unwrap();
            }
        }
        collector
            .rate_occurrences
            .take_sorted()
            .unwrap()
            .into_iter()
            .map(|record| (record.key(), record.tie_breaker))
            .collect()
    }

    #[test]
    fn occurrence_sample_is_invariant_across_worker_partition_order() {
        let even_then_odd = (0..64).step_by(2).chain((1..64).step_by(2));
        let reverse_partition_order = (0..64).rev();

        assert_eq!(
            selected_occurrences_for_object_order(even_then_odd),
            selected_occurrences_for_object_order(reverse_partition_order),
        );
    }

    #[test]
    fn witness_record_preserves_rate_and_linked_provider_tokens() {
        let collector = SourceWitnessCollector::new(&"ab".repeat(32)).unwrap();
        let raw_rate =
            br#"{"provider_references":[7],"negotiated_prices":[{"negotiated_rate":1.20e2}]}"#;
        let raw_provider = br#"{"provider_group_id":7,"provider_groups":[{"npi":[1234567890]}]}"#;
        let coordinate = SourceWitnessCoordinate::new(4, 5);
        let procedure = Map::from_iter([("billing_code".to_string(), json!("99213"))]);
        let provider_evidence = json!({"source_kind": "provider_reference"});
        let expected = json!({"price_atom_global_id": "00"});
        let (compressed, evidence) = collector
            .externalized_record(SourceWitnessRecordInput {
                kind: SourceWitnessKind::RateOccurrence,
                priority: 9,
                tie_breaker: [7; 32],
                coordinate,
                price_ordinal: 0,
                provider_ordinal: 0,
                provider_evidence: Some(&provider_evidence),
                procedure: Some(&procedure),
                raw: raw_rate,
                linked_provider_raw: Some(raw_provider),
                expected: &expected,
            })
            .unwrap();
        let mut decoded = Vec::new();
        ZlibDecoder::new(compressed.as_slice())
            .read_to_end(&mut decoded)
            .unwrap();
        assert!(decoded.starts_with(SOURCE_WITNESS_RECORD_MAGIC));
        let metadata_length = u32::from_be_bytes(decoded[8..12].try_into().unwrap()) as usize;
        let metadata_end = 12 + metadata_length;
        let raw_length =
            u32::from_be_bytes(decoded[metadata_end..metadata_end + 4].try_into().unwrap())
                as usize;
        let raw_end = metadata_end + 4 + raw_length;
        assert_eq!(raw_length, 0);
        let provider_length =
            u32::from_be_bytes(decoded[raw_end..raw_end + 4].try_into().unwrap()) as usize;
        assert_eq!(provider_length, 0);
        assert_eq!(evidence.len(), 2);
        let mut decoded_evidence = evidence
            .iter()
            .map(|entry| {
                let mut raw = Vec::new();
                ZlibDecoder::new(entry.compressed.as_slice())
                    .read_to_end(&mut raw)
                    .unwrap();
                raw
            })
            .collect::<Vec<_>>();
        decoded_evidence.sort();
        let mut expected_evidence = vec![raw_rate.to_vec(), raw_provider.to_vec()];
        expected_evidence.sort();
        assert_eq!(decoded_evidence, expected_evidence);
    }

    #[test]
    fn selected_oversized_record_fails_closed() {
        let error = validate_compressed_record_size(SOURCE_WITNESS_MAX_COMPRESSED_RECORD_BYTES + 1)
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("exceeding the fail-closed"));
    }

    #[test]
    fn selected_rate_heap_retains_locators_instead_of_materialized_records() {
        let sampler = SourceWitnessSampler::new(1, u64::MAX);
        sampler.insert(selected_rate(1)).unwrap();
        let selected = sampler.take_sorted().unwrap();
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].rate_source_locator.offset, 1);
    }

    #[test]
    fn sampler_keeps_exact_bottom_k_without_materializing_evidence() {
        let sampler = SourceWitnessSampler::new(2, 12);
        for priority in [1, 2] {
            sampler.insert(selected_rate(priority)).unwrap();
        }
        sampler.insert(selected_rate(0)).unwrap();
        let selected = sampler.take_sorted().unwrap();
        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0].priority, 0);
        assert_eq!(selected[1].priority, 1);
    }

    #[test]
    fn sampler_replacement_discards_the_worse_locator() {
        let sampler = SourceWitnessSampler::new(1, 64);
        sampler.insert(selected_rate(2)).unwrap();
        sampler.insert(selected_rate(1)).unwrap();
        let selected = sampler.take_sorted().unwrap();
        assert_eq!(selected[0].priority, 1);
    }

    #[test]
    fn final_materialization_rejects_oversized_evidence() {
        let error = validate_compressed_record_size(SOURCE_WITNESS_MAX_COMPRESSED_RECORD_BYTES + 1)
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("exceeding the fail-closed"));
    }

    #[test]
    fn rate_source_spool_round_trips_procedure_and_raw_rate() {
        let collector = SourceWitnessCollector::new(&"ab".repeat(32)).unwrap();
        collector.configure_rate_spools(2).unwrap();
        let coordinate = SourceWitnessCoordinate::new(3, 4);
        let procedure = Map::from_iter([("billing_code".to_string(), json!("99213"))]);
        let raw_rate = br#"{"provider_references":[7]}"#;
        let locator = collector
            .store_rate_source(coordinate, &procedure, raw_rate)
            .unwrap();
        collector.read_rate_source(locator).unwrap_err();
        collector.seal_rate_sources().unwrap();
        let (restored_procedure, restored_rate) = collector.read_rate_source(locator).unwrap();
        assert_eq!(restored_procedure, procedure);
        assert_eq!(restored_rate, raw_rate);
    }

    #[test]
    fn intermediate_candidate_budget_matches_persisted_safety_bound() {
        assert_eq!(
            SOURCE_WITNESS_MAX_INTERMEDIATE_BUNDLE_BYTES,
            512 * 1024 * 1024,
        );
        assert_eq!(
            SOURCE_WITNESS_MAX_COMPRESSED_RATE_CANDIDATE_BYTES,
            SOURCE_WITNESS_MAX_INTERMEDIATE_BUNDLE_BYTES,
        );
    }

    #[test]
    fn samplers_use_independent_release_targets() {
        let collector = SourceWitnessCollector::new(&"ab".repeat(32)).unwrap();

        assert_eq!(
            collector.rate_occurrences.target,
            SOURCE_WITNESS_OCCURRENCE_TARGET,
        );
        assert_eq!(
            collector.provider_references.target,
            SOURCE_WITNESS_PROVIDER_QUOTA,
        );
    }

    #[test]
    fn local_targets_preserve_independent_cohort_contract() {
        assert_eq!(
            local_source_witness_targets(20_000, 80).unwrap(),
            (10_000, 80, 10_080),
        );
        assert_eq!(
            local_source_witness_targets(2_040, 8).unwrap(),
            (2_040, 8, 2_048),
        );
        assert_eq!(
            local_source_witness_targets(10, 100).unwrap(),
            (10, 100, 110),
        );
        assert_eq!(
            local_source_witness_targets(10, 10_000).unwrap(),
            (10, 1_000, 1_010),
        );
    }

    #[test]
    fn large_witness_records_retry_with_best_compression() {
        let mut record = br#"{"provider_groups":[{"npi":["#.to_vec();
        for index in 0..225_000 {
            if index > 0 {
                record.push(b',');
            }
            write!(&mut record, "{}", 1_234_567_890 + index % 10_000).unwrap();
        }
        record.extend_from_slice(br#"]}]}"#);
        let fast_record = zlib_compress_record(&record, Compression::fast()).unwrap();
        let best_record = zlib_compress_record(&record, Compression::best()).unwrap();
        let retry_limit = fast_record.len().saturating_sub(1);

        assert!(best_record.len() < fast_record.len());
        assert_eq!(
            compress_source_witness_record_with_limit(&record, retry_limit).unwrap(),
            best_record,
        );
    }
}
