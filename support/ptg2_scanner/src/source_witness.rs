use crate::source_witness_spool::{ProviderSourceLocator, ProviderSourceSpools};
use flate2::write::ZlibEncoder;
use flate2::Compression;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::fs::{self, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Mutex, OnceLock};
use xxhash_rust::xxh3::Xxh3;

pub const SOURCE_WITNESS_CONTRACT: &str = "ptg2_v3_source_witness_v2";
pub const SOURCE_WITNESS_RECORD_CONTRACT: &str = "ptg2_v3_source_witness_record_v2";
pub const SOURCE_WITNESS_SELECTION: &str = "bottom_k_atomic_occurrence_exponential_priority_v2";
pub const SOURCE_WITNESS_FORMAT_VERSION: u32 = 2;
pub const SOURCE_WITNESS_TOTAL_TARGET: usize = 2_048;
pub const SOURCE_WITNESS_PROVIDER_QUOTA: usize = 48;
pub const SOURCE_WITNESS_UNQUERYABLE_POLICY: &str = "count_but_exclude_from_npi_api_challenges_v1";
const SOURCE_WITNESS_LOCAL_CANDIDATE_TARGET: usize = SOURCE_WITNESS_TOTAL_TARGET;
const SOURCE_WITNESS_MAGIC: &[u8; 8] = b"PTG2SW02";
const SOURCE_WITNESS_RECORD_MAGIC: &[u8; 8] = b"PTG2SWR2";
const SOURCE_WITNESS_MAX_COMPRESSED_RECORD_BYTES: usize = 8 * 1024 * 1024;
const SOURCE_WITNESS_MAX_COMPRESSED_RATE_COHORT_BYTES: u64 = 112 * 1024 * 1024;
const SOURCE_WITNESS_MAX_COMPRESSED_PROVIDER_COHORT_BYTES: u64 = 12 * 1024 * 1024;

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
    pub procedure: &'a Map<String, Value>,
    pub raw_rate: &'a [u8],
    pub linked_provider_raw: Option<&'a [u8]>,
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

struct SelectedSourceWitness {
    priority: u64,
    tie_breaker: [u8; 32],
    compressed_record: Vec<u8>,
}

impl SelectedSourceWitness {
    fn key(&self) -> (u64, [u8; 32]) {
        (self.priority, self.tie_breaker)
    }
}

impl PartialEq for SelectedSourceWitness {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}

impl Eq for SelectedSourceWitness {}

impl PartialOrd for SelectedSourceWitness {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SelectedSourceWitness {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key().cmp(&other.key())
    }
}

#[derive(Default)]
struct SourceWitnessSamplerState {
    selected: BinaryHeap<SelectedSourceWitness>,
    compressed_bytes: u64,
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
        self.population
            .fetch_update(
                AtomicOrdering::Relaxed,
                AtomicOrdering::Relaxed,
                |current| current.checked_add(count),
            )
            .map(|_| ())
            .map_err(|_| io::Error::other("source witness population overflow"))
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

    fn insert(
        &self,
        priority: u64,
        tie_breaker: [u8; 32],
        compressed_record: Vec<u8>,
    ) -> io::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("source witness sampler mutex is poisoned"))?;
        if state.selected.len() >= self.target
            && state
                .selected
                .peek()
                .is_some_and(|current| (priority, tie_breaker) >= current.key())
        {
            return Ok(());
        }
        if compressed_record.len() > SOURCE_WITNESS_MAX_COMPRESSED_RECORD_BYTES {
            self.oversized.fetch_add(1, AtomicOrdering::Relaxed);
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "selected source witness record is {} bytes, exceeding the fail-closed {}-byte limit",
                    compressed_record.len(),
                    SOURCE_WITNESS_MAX_COMPRESSED_RECORD_BYTES,
                ),
            ));
        }
        let evicted_bytes = if state.selected.len() >= self.target {
            state
                .selected
                .peek()
                .map_or(0, |record| record.compressed_record.len() as u64)
        } else {
            0
        };
        let projected_bytes = state
            .compressed_bytes
            .saturating_sub(evicted_bytes)
            .saturating_add(compressed_record.len() as u64);
        if projected_bytes > self.compressed_byte_limit {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "selected source witness cohort would use {projected_bytes} bytes, exceeding the fail-closed {}-byte payload budget",
                    self.compressed_byte_limit,
                ),
            ));
        }
        state.compressed_bytes = state
            .compressed_bytes
            .saturating_add(compressed_record.len() as u64);
        state.selected.push(SelectedSourceWitness {
            priority,
            tie_breaker,
            compressed_record,
        });
        if state.selected.len() > self.target {
            if let Some(evicted) = state.selected.pop() {
                state.compressed_bytes = state
                    .compressed_bytes
                    .saturating_sub(evicted.compressed_record.len() as u64);
            }
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

    fn take_sorted(&self) -> io::Result<Vec<SelectedSourceWitness>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("source witness sampler mutex is poisoned"))?;
        let mut selected = std::mem::take(&mut state.selected).into_vec();
        selected.sort_unstable_by_key(SelectedSourceWitness::key);
        state.compressed_bytes = 0;
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

pub struct SourceWitnessCollector {
    raw_source_sha256: [u8; 32],
    raw_source_sha256_hex: String,
    rate_occurrences: SourceWitnessSampler,
    provider_references: SourceWitnessSampler,
    rate_rows: AtomicU64,
    unqueryable_rate_rows: AtomicU64,
    provider_spools: OnceLock<ProviderSourceSpools>,
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
            let text = std::str::from_utf8(chunk).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "raw source SHA-256 is invalid")
            })?;
            digest[index] = u8::from_str_radix(text, 16).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "raw source SHA-256 is invalid")
            })?;
        }
        Ok(Self {
            raw_source_sha256: digest,
            raw_source_sha256_hex: normalized,
            rate_occurrences: SourceWitnessSampler::new(
                SOURCE_WITNESS_LOCAL_CANDIDATE_TARGET,
                SOURCE_WITNESS_MAX_COMPRESSED_RATE_COHORT_BYTES,
            ),
            provider_references: SourceWitnessSampler::new(
                SOURCE_WITNESS_PROVIDER_QUOTA,
                SOURCE_WITNESS_MAX_COMPRESSED_PROVIDER_COHORT_BYTES,
            ),
            rate_rows: AtomicU64::new(0),
            unqueryable_rate_rows: AtomicU64::new(0),
            provider_spools: OnceLock::new(),
        })
    }

    pub fn configure_provider_spools(&self, shard_count: usize) -> io::Result<()> {
        self.provider_spools
            .set(ProviderSourceSpools::new(shard_count)?)
            .map_err(|_| io::Error::other("provider source spools already configured"))
    }

    pub fn store_provider_source(
        &self,
        shard: usize,
        raw_provider: &[u8],
    ) -> io::Result<ProviderSourceLocator> {
        self.provider_spools
            .get()
            .ok_or_else(|| io::Error::other("provider source spools are not configured"))?
            .append(shard, raw_provider)
    }

    pub fn seal_provider_sources(&self) -> io::Result<()> {
        self.provider_spools
            .get()
            .ok_or_else(|| io::Error::other("provider source spools are not configured"))?
            .seal()
    }

    pub fn read_provider_source(&self, locator: ProviderSourceLocator) -> io::Result<Vec<u8>> {
        self.provider_spools
            .get()
            .ok_or_else(|| io::Error::other("provider source spools are not configured"))?
            .read(locator)
    }

    pub fn rate_occurrence_candidates(
        &self,
        coordinate: SourceWitnessCoordinate,
        procedure_json: &[u8],
        raw_rate: &[u8],
        occurrence_count: u64,
    ) -> io::Result<Vec<RateOccurrenceCandidate>> {
        self.rate_rows.fetch_add(1, AtomicOrdering::Relaxed);
        if occurrence_count == 0 {
            self.unqueryable_rate_rows
                .fetch_add(1, AtomicOrdering::Relaxed);
            return Ok(Vec::new());
        }
        self.rate_occurrences.add_population(occurrence_count)?;
        let seed = self.rate_block_seed(coordinate, procedure_json, raw_rate);
        let limit = occurrence_count.min(SOURCE_WITNESS_LOCAL_CANDIDATE_TARGET as u64);
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
            input.linked_provider_raw,
        );
        let compressed = self.compressed_record(SourceWitnessRecordInput {
            kind: SourceWitnessKind::RateOccurrence,
            priority: input.candidate.priority,
            tie_breaker,
            coordinate: input.coordinate,
            price_ordinal: input.price_ordinal,
            provider_ordinal: input.provider_ordinal,
            provider_evidence: Some(input.provider_evidence),
            procedure: Some(input.procedure),
            raw: input.raw_rate,
            linked_provider_raw: input.linked_provider_raw,
            expected: input.expected,
        })?;
        self.rate_occurrences
            .insert(input.candidate.priority, tie_breaker, compressed)
    }

    pub fn commit_provider_reference(
        &self,
        priority: u64,
        coordinate: SourceWitnessCoordinate,
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
        let compressed = self.compressed_record(SourceWitnessRecordInput {
            kind: SourceWitnessKind::ProviderReference,
            priority,
            tie_breaker,
            coordinate,
            price_ordinal: 0,
            provider_ordinal: 0,
            provider_evidence: None,
            procedure: None,
            raw: raw_provider,
            linked_provider_raw: None,
            expected,
        })?;
        self.provider_references
            .insert(priority, tie_breaker, compressed)
    }

    pub fn write_bundle(&self, directory: &Path) -> io::Result<Value> {
        fs::create_dir_all(directory)?;
        let rate_records = self.rate_occurrences.take_sorted()?;
        let provider_records = self.provider_references.take_sorted()?;
        validate_local_selection(
            "rate occurrence",
            self.rate_occurrences.population(),
            rate_records.len(),
        )?;
        validate_local_selection(
            "provider reference",
            self.provider_references.population(),
            provider_records.len(),
        )?;
        if rate_records.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "strict V3 scan produced no queryable source occurrence witnesses",
            ));
        }
        let rate_bytes = compressed_bytes(&rate_records);
        let provider_bytes = compressed_bytes(&provider_records);
        let mut rate_metrics = self
            .rate_occurrences
            .metrics(rate_records.len(), rate_bytes);
        let rate_metrics_map = rate_metrics
            .as_object_mut()
            .ok_or_else(|| io::Error::other("source witness rate metrics must be a JSON object"))?;
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
            "total_target": SOURCE_WITNESS_TOTAL_TARGET,
            "provider_quota": SOURCE_WITNESS_PROVIDER_QUOTA,
            "rate_occurrence": rate_metrics,
            "provider_reference": self.provider_references.metrics(provider_records.len(), provider_bytes),
        });
        let header_bytes = serde_json::to_vec(&header).map_err(to_io_error)?;
        let header_length = u32::try_from(header_bytes.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "source witness header is too large",
            )
        })?;
        let record_count = rate_records.len().saturating_add(provider_records.len());
        let record_count_u32 = u32::try_from(record_count).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "source witness count is too large",
            )
        })?;
        let path = unique_bundle_path(directory)?;
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        let mut writer = HashingWriter::new(BufWriter::new(file));
        writer.write_all(SOURCE_WITNESS_MAGIC)?;
        writer.write_all(&header_length.to_be_bytes())?;
        writer.write_all(&header_bytes)?;
        writer.write_all(&record_count_u32.to_be_bytes())?;
        for record in rate_records.iter().chain(provider_records.iter()) {
            let length = u32::try_from(record.compressed_record.len()).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "source witness record is too large",
                )
            })?;
            writer.write_all(&length.to_be_bytes())?;
            writer.write_all(&record.compressed_record)?;
        }
        let (writer, digest, byte_count) = writer.finish()?;
        writer.get_ref().sync_all()?;
        drop(writer);
        Ok(json!({
            "path": path.display().to_string(),
            "contract": SOURCE_WITNESS_CONTRACT,
            "format_version": SOURCE_WITNESS_FORMAT_VERSION,
            "selection_method": SOURCE_WITNESS_SELECTION,
            "raw_source_sha256": self.raw_source_sha256_hex,
            "row_count": record_count,
            "occurrence_witness_count": rate_records.len(),
            "provider_witness_count": provider_records.len(),
            "byte_count": byte_count,
            "sha256": hex_digest(&digest),
            "queryable_occurrence_population_count": self.rate_occurrences.population(),
            "provider_population_count": self.provider_references.population(),
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

    fn compressed_record(&self, input: SourceWitnessRecordInput<'_>) -> io::Result<Vec<u8>> {
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
        let metadata_length = u32::try_from(metadata_bytes.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "source witness record metadata is too large",
            )
        })?;
        let raw_length = u32::try_from(raw.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "source witness raw JSON token is too large",
            )
        })?;
        let linked_provider_raw = linked_provider_raw.unwrap_or_default();
        let linked_provider_length = u32::try_from(linked_provider_raw.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "source witness linked provider token is too large",
            )
        })?;
        let mut record = Vec::with_capacity(
            SOURCE_WITNESS_RECORD_MAGIC
                .len()
                .saturating_add(metadata_bytes.len())
                .saturating_add(raw.len())
                .saturating_add(linked_provider_raw.len())
                .saturating_add(12),
        );
        record.extend_from_slice(SOURCE_WITNESS_RECORD_MAGIC);
        record.extend_from_slice(&metadata_length.to_be_bytes());
        record.extend_from_slice(&metadata_bytes);
        record.extend_from_slice(&raw_length.to_be_bytes());
        record.extend_from_slice(raw);
        record.extend_from_slice(&linked_provider_length.to_be_bytes());
        record.extend_from_slice(linked_provider_raw);
        compress_source_witness_record(&record)
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
    linked_provider_raw: Option<&[u8]>,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(kind.domain());
    digest.update(coordinate.object_ordinal.to_be_bytes());
    digest.update(coordinate.rate_ordinal.to_be_bytes());
    digest.update(price_ordinal.to_be_bytes());
    digest.update(provider_ordinal.to_be_bytes());
    digest.update(Sha256::digest(raw));
    if let Some(linked_provider_raw) = linked_provider_raw {
        digest.update(Sha256::digest(linked_provider_raw));
    }
    digest.finalize().into()
}

fn validate_local_selection(label: &str, population: u64, selected: usize) -> io::Result<()> {
    let expected = population.min(SOURCE_WITNESS_LOCAL_CANDIDATE_TARGET as u64) as usize;
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

fn compressed_bytes(records: &[SelectedSourceWitness]) -> u64 {
    records
        .iter()
        .map(|record| record.compressed_record.len() as u64)
        .sum()
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

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::read::ZlibDecoder;
    use std::io::Read;

    type SelectedOccurrenceDigest = ((u64, [u8; 32]), [u8; 32]);

    #[test]
    fn sampler_is_order_independent_and_bounded() {
        let sampler = SourceWitnessSampler::new(3, 1_024);
        for priority in [9, 2, 7, 1, 3, 8] {
            sampler.add_population(1).unwrap();
            sampler
                .insert(priority, [priority as u8; 32], vec![priority as u8])
                .unwrap();
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

    fn selected_occurrences_for_object_order(
        object_order: impl IntoIterator<Item = u64>,
    ) -> Vec<SelectedOccurrenceDigest> {
        let collector = SourceWitnessCollector::new(&"cd".repeat(32)).unwrap();
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
            for candidate in collector
                .rate_occurrence_candidates(coordinate, &procedure_json, raw_rate.as_bytes(), 64)
                .unwrap()
            {
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
                        procedure: &procedure,
                        raw_rate: raw_rate.as_bytes(),
                        linked_provider_raw: None,
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
            .map(|record| {
                let digest: [u8; 32] = Sha256::digest(&record.compressed_record).into();
                (record.key(), digest)
            })
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
        let compressed = collector
            .compressed_record(SourceWitnessRecordInput {
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
        assert_eq!(&decoded[metadata_end + 4..raw_end], raw_rate);
        let provider_length =
            u32::from_be_bytes(decoded[raw_end..raw_end + 4].try_into().unwrap()) as usize;
        assert_eq!(
            &decoded[raw_end + 4..raw_end + 4 + provider_length],
            raw_provider
        );
    }

    #[test]
    fn selected_oversized_record_fails_closed() {
        let sampler = SourceWitnessSampler::new(1, u64::MAX);
        let error = sampler
            .insert(
                1,
                [0; 32],
                vec![0; SOURCE_WITNESS_MAX_COMPRESSED_RECORD_BYTES + 1],
            )
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("exceeding the fail-closed"));
    }

    #[test]
    fn selected_record_above_the_old_limit_remains_bounded_and_is_accepted() {
        let sampler = SourceWitnessSampler::new(1, u64::MAX);
        let compressed_record = vec![0; 512 * 1024 + 1];

        sampler
            .insert(1, [0; 32], compressed_record.clone())
            .unwrap();

        let selected = sampler.take_sorted().unwrap();
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].compressed_record, compressed_record);
    }

    #[test]
    fn provider_sampler_keeps_only_the_global_provider_quota() {
        let collector = SourceWitnessCollector::new(&"ab".repeat(32)).unwrap();

        assert_eq!(
            collector.provider_references.target,
            SOURCE_WITNESS_PROVIDER_QUOTA
        );
        assert_eq!(
            collector.provider_references.compressed_byte_limit,
            SOURCE_WITNESS_MAX_COMPRESSED_PROVIDER_COHORT_BYTES,
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
