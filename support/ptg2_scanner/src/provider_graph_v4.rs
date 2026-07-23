//! Adaptive strict-V4 provider graph compiler.
//!
//! The source scanner already emits the two exact factor relations
//! `provider_set -> component -> group` and the reciprocal exact NPI/group
//! relations.  This compiler keeps those factors intact, derives a
//! snapshot-local quotient of identical group/set incidence vectors, and
//! chooses the smaller *complete* direct or pattern projection.  It never
//! materializes the flat group/set expansion: every group or set union is
//! composed into one reusable scratch vector and immediately counted,
//! hashed, or emitted.

use memmap2::{Mmap, MmapOptions};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::borrow::Cow;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

const MANIFEST_VERSION: u32 = 1;
const STANDARD_MAGIC: &[u8; 8] = b"PTG2MNSC";
const DENSE_MAGIC: &[u8; 8] = b"PTG2MNDS";
const STANDARD_FORMAT: &str = "magic8:uint32_le_version:uint64_le_entry_count:index(owner16:uint64_le_offset:uint32_le_count):members16";
const DENSE_FORMAT: &str = "magic8:uint32_le_version:uint64_le_entry_count:uint64_le_member_global_count:index(owner16:uint64_le_offset:uint32_le_count):member_globals16:members_uint32_le";
const STANDARD_HEADER_BYTES: usize = 20;
const DENSE_HEADER_BYTES: usize = 28;
const OWNER_RECORD_BYTES: usize = 28;
const GLOBAL_ID_BYTES: usize = 16;
const LOCATOR_BYTES: usize = 12;
const HEAVY_BITMAP_HEADER_BYTES: usize = 24;
const HEAVY_BITMAP_FRAGMENT_HEADER_BYTES: usize = 32;
const HEAVY_BITMAP_FRAGMENT_MAGIC: &[u8; 8] = b"PTG2V4BF";
const DEFAULT_PAGE_BYTES: usize = 16 * 1024;
const DEFAULT_MAX_SET_PATTERNS_PER_SET: usize = 1_024;
const DEFAULT_MAX_SET_COMPONENTS_PER_FALLBACK_SET: usize = 4_096;
const DEFAULT_MAX_ONLINE_GROUP_KEYS_PER_SET: usize = 4_096;
const DEFAULT_MAX_ONLINE_SOURCE_OWNERS_PER_SET: usize = 4_096;
const DEFAULT_MAX_ONLINE_SOURCE_MEMBERS_PER_SET: usize = 16_384;
const DEFAULT_MAX_ONLINE_SOURCE_PAGES_PER_SET: usize = 64;
const DEFAULT_MAX_ONLINE_SOURCE_BYTES_PER_SET: u64 = 1024 * 1024;
const DEFAULT_ONLINE_GROUP_NPI_BATCH_SIZE: usize = 32;
const DEFAULT_MAX_ONLINE_GROUP_NPI_MEMBERS_PER_SET: usize = 32_768;
const DEFAULT_MAX_ONLINE_GROUP_NPI_LOCATOR_PAGES_PER_SET: usize = 16;
const DEFAULT_MAX_ONLINE_GROUP_NPI_MEMBER_PAGES_PER_SET: usize = 128;
const DEFAULT_MAX_ONLINE_GROUP_NPI_BYTES_PER_SET: u64 = 4 * 1024 * 1024;
const DEFAULT_MAX_ONLINE_GROUP_NPI_BATCHES_PER_SET: usize = 4;
const ONLINE_NPI_DICTIONARY_ENTRY_BYTES: u64 = 16;
const DEFAULT_PROVIDER_EXPANSION_RATE_PAGE_ROWS: usize = 64;
const DEFAULT_MAX_ONLINE_PROVIDER_EXPANSION_RATE_ROWS: usize = 256;
const DEFAULT_MAX_ONLINE_PROVIDER_EXPANSION_PROVIDER_SETS: usize = 64;
const DEFAULT_MAX_ONLINE_PROVIDER_EXPANSION_GRAPH_BATCHES: usize = 64;
const DEFAULT_NPI_PREFIX_TARGET: usize = 201;
const DEFAULT_MAX_NPI_PREFIX_OVERRIDE_OWNERS: usize = 50_000;
const DEFAULT_MAX_NPI_PREFIX_OVERRIDE_BYTES: u64 = 64 * 1024 * 1024;
const REFERENCE_ENCODE_BUFFER_BYTES: u64 = 4 * 1024;
const REFERENCE_SPOOL_WRITER_BYTES: u64 = 8 * 1024;
const MAX_REFERENCE_OBJECT_KINDS: usize = 32;
const REFERENCE_SPOOL_FIXED_BYTES: u64 = REFERENCE_ENCODE_BUFFER_BYTES
    + REFERENCE_SPOOL_WRITER_BYTES * MAX_REFERENCE_OBJECT_KINDS as u64;
// Vec capacity can grow to just under twice its logical length. These
// admission estimates deliberately account for that allocator slack rather
// than relying on the current allocator's exact growth strategy.
const ESTIMATED_U32_CAPACITY_BYTES: u64 = 8;
const ESTIMATED_VEC_OWNER_BYTES: u64 = 64;
const ESTIMATED_PATTERN_INDEX_BYTES: u64 = 256;
// V4 is a logical graph generation. Immutable CAS blocks intentionally retain
// the existing physical V3 wire contract so they can share `ptg2_v3_block`.
const SHARED_FORMAT_VERSION: i16 = 2;
const BLOCK_HASH_DOMAIN: &[u8] = b"PTG2V3BLOCK\x01";
const PATTERN_HASH_DOMAIN: &[u8] = b"PTG2V4PATTERN\x01";
const NPI_PREFIX_HASH_DOMAIN: &[u8] = b"PTG2V4NPI-PREFIX\x01";
const EDGE_XOR_DOMAIN: &[u8] = b"PTG2V4EDGE-XOR\x01";
const EDGE_SUM_DOMAIN: &[u8] = b"PTG2V4EDGE-SUM\x01";
const PG_COPY_HEADER: &[u8] = b"PGCOPY\n\xff\r\n\0\0\0\0\0\0\0\0\0";
const PROGRESS_PREFIX: &str = "PTG2_V4_PROGRESS\t";
const PROGRESS_VERSION: u8 = 1;
const PROGRESS_MAX_PERIODIC_EVENTS: u64 = 256;

const OUTPUT_NAMES: [&str; 9] = [
    "v4-graph-blocks.copy",
    "v4-graph-references.jsonl",
    "v4-provider-groups.copy",
    "v4-provider-components.copy",
    "v4-npi-scope.copy",
    "v4-provider-set-audit-npi.copy",
    "v4-provider-set-npi-prefix-overrides.copy",
    "v4-patterns.copy",
    "v4-summary.json",
];

type GlobalId = [u8; GLOBAL_ID_BYTES];

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
struct V4ProgressEvent {
    version: u8,
    seq: u64,
    phase: &'static str,
    done: u64,
    total: u64,
    unit: &'static str,
    elapsed_ms: u64,
    terminal: bool,
}

struct ProgressReporter<'a> {
    sink: &'a mut dyn FnMut(&V4ProgressEvent),
    started_at: Instant,
    next_seq: u64,
    last_phase: Option<&'static str>,
    last_done: u64,
}

impl<'a> ProgressReporter<'a> {
    fn new(sink: &'a mut dyn FnMut(&V4ProgressEvent)) -> Self {
        Self {
            sink,
            started_at: Instant::now(),
            next_seq: 1,
            last_phase: None,
            last_done: 0,
        }
    }

    fn emit(
        &mut self,
        phase: &'static str,
        done: u64,
        total: u64,
        unit: &'static str,
        terminal: bool,
    ) {
        debug_assert!(done <= total);
        let event = V4ProgressEvent {
            version: PROGRESS_VERSION,
            seq: self.next_seq,
            phase,
            done,
            total,
            unit,
            elapsed_ms: self
                .started_at
                .elapsed()
                .as_millis()
                .min(u128::from(u64::MAX)) as u64,
            terminal,
        };
        self.next_seq = self.next_seq.saturating_add(1);
        self.last_phase = Some(phase);
        self.last_done = done;
        (self.sink)(&event);
    }

    fn periodic(&mut self, phase: &'static str, done: u64, total: u64, unit: &'static str) {
        let phase_changed = self.last_phase != Some(phase);
        let interval = (total / PROGRESS_MAX_PERIODIC_EVENTS)
            .saturating_add(u64::from(
                !total.is_multiple_of(PROGRESS_MAX_PERIODIC_EVENTS),
            ))
            .max(1);
        let advanced = phase_changed || done.saturating_sub(self.last_done) >= interval;
        if phase_changed || done == 0 || done == total || advanced {
            self.emit(phase, done, total, unit, false);
        }
    }
}

fn stderr_progress_sink(event: &V4ProgressEvent) {
    if let Ok(payload) = serde_json::to_string(event) {
        let _ = writeln!(io::stderr().lock(), "{PROGRESS_PREFIX}{payload}");
    }
}

#[derive(Debug)]
pub enum ProviderGraphV4Error {
    Io(io::Error),
    InvalidData(Cow<'static, str>),
    Json(serde_json::Error),
}

impl fmt::Display for ProviderGraphV4Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => error.fmt(formatter),
            Self::InvalidData(message) => formatter.write_str(message),
            Self::Json(error) => error.fmt(formatter),
        }
    }
}

impl Error for ProviderGraphV4Error {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::InvalidData(_) => None,
        }
    }
}

impl From<io::Error> for ProviderGraphV4Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_json::Error> for ProviderGraphV4Error {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

pub type ProviderGraphV4Result<T> = Result<T, ProviderGraphV4Error>;

fn invalid(message: impl Into<Cow<'static, str>>) -> ProviderGraphV4Error {
    ProviderGraphV4Error::InvalidData(message.into())
}

fn invalid_conversion<T, E>(
    result: Result<T, E>,
    message: &'static str,
) -> ProviderGraphV4Result<T> {
    match result {
        Ok(value) => Ok(value),
        Err(_) => Err(invalid(message)),
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct V4MembershipMetadata {
    pub record_format: String,
    pub sha256: String,
    pub byte_count: u64,
    pub owner_count: u64,
    pub member_count: u64,
    #[serde(default)]
    pub member_global_count: Option<u64>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub source_shard_id: Option<String>,
    #[serde(default)]
    pub shard_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct V4MembershipArtifactDescriptor {
    pub path: PathBuf,
    pub metadata: V4MembershipMetadata,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct V4ProviderGraphShardDescriptor {
    pub shard_id: String,
    pub provider_set_component: V4MembershipArtifactDescriptor,
    pub provider_component_group: V4MembershipArtifactDescriptor,
    pub provider_group_npi: V4MembershipArtifactDescriptor,
    pub provider_npi_group: V4MembershipArtifactDescriptor,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(default)]
pub struct ProviderGraphV4Options {
    pub member_page_bytes: usize,
    pub locator_page_bytes: usize,
    pub heavy_owner_member_threshold: usize,
    pub heavy_bitmap_minimum_savings_bytes: usize,
    pub max_set_patterns_per_set: usize,
    pub max_set_components_per_fallback_set: usize,
    pub max_online_group_keys_per_set: usize,
    pub max_online_source_owners_per_set: usize,
    pub max_online_source_members_per_set: usize,
    pub max_online_source_pages_per_set: usize,
    pub max_online_source_bytes_per_set: u64,
    pub online_group_npi_batch_size: usize,
    pub max_online_group_npi_members_per_set: usize,
    pub max_online_group_npi_locator_pages_per_set: usize,
    pub max_online_group_npi_member_pages_per_set: usize,
    pub max_online_group_npi_bytes_per_set: u64,
    pub max_online_group_npi_batches_per_set: usize,
    pub provider_expansion_rate_page_rows: usize,
    pub max_online_provider_expansion_rate_rows: usize,
    pub max_online_provider_expansion_provider_sets: usize,
    pub max_online_provider_expansion_graph_batches: usize,
    pub npi_prefix_target: usize,
    pub max_npi_prefix_override_owners: usize,
    pub max_npi_prefix_override_bytes: u64,
    pub max_estimated_model_bytes: Option<u64>,
    pub max_factor_edges: Option<u64>,
}

impl Default for ProviderGraphV4Options {
    fn default() -> Self {
        Self {
            member_page_bytes: DEFAULT_PAGE_BYTES,
            locator_page_bytes: DEFAULT_PAGE_BYTES,
            heavy_owner_member_threshold: 4_096,
            heavy_bitmap_minimum_savings_bytes: 512,
            max_set_patterns_per_set: DEFAULT_MAX_SET_PATTERNS_PER_SET,
            max_set_components_per_fallback_set: DEFAULT_MAX_SET_COMPONENTS_PER_FALLBACK_SET,
            max_online_group_keys_per_set: DEFAULT_MAX_ONLINE_GROUP_KEYS_PER_SET,
            max_online_source_owners_per_set: DEFAULT_MAX_ONLINE_SOURCE_OWNERS_PER_SET,
            max_online_source_members_per_set: DEFAULT_MAX_ONLINE_SOURCE_MEMBERS_PER_SET,
            max_online_source_pages_per_set: DEFAULT_MAX_ONLINE_SOURCE_PAGES_PER_SET,
            max_online_source_bytes_per_set: DEFAULT_MAX_ONLINE_SOURCE_BYTES_PER_SET,
            online_group_npi_batch_size: DEFAULT_ONLINE_GROUP_NPI_BATCH_SIZE,
            max_online_group_npi_members_per_set: DEFAULT_MAX_ONLINE_GROUP_NPI_MEMBERS_PER_SET,
            max_online_group_npi_locator_pages_per_set:
                DEFAULT_MAX_ONLINE_GROUP_NPI_LOCATOR_PAGES_PER_SET,
            max_online_group_npi_member_pages_per_set:
                DEFAULT_MAX_ONLINE_GROUP_NPI_MEMBER_PAGES_PER_SET,
            max_online_group_npi_bytes_per_set: DEFAULT_MAX_ONLINE_GROUP_NPI_BYTES_PER_SET,
            max_online_group_npi_batches_per_set: DEFAULT_MAX_ONLINE_GROUP_NPI_BATCHES_PER_SET,
            provider_expansion_rate_page_rows: DEFAULT_PROVIDER_EXPANSION_RATE_PAGE_ROWS,
            max_online_provider_expansion_rate_rows:
                DEFAULT_MAX_ONLINE_PROVIDER_EXPANSION_RATE_ROWS,
            max_online_provider_expansion_provider_sets:
                DEFAULT_MAX_ONLINE_PROVIDER_EXPANSION_PROVIDER_SETS,
            max_online_provider_expansion_graph_batches:
                DEFAULT_MAX_ONLINE_PROVIDER_EXPANSION_GRAPH_BATCHES,
            npi_prefix_target: DEFAULT_NPI_PREFIX_TARGET,
            max_npi_prefix_override_owners: DEFAULT_MAX_NPI_PREFIX_OVERRIDE_OWNERS,
            max_npi_prefix_override_bytes: DEFAULT_MAX_NPI_PREFIX_OVERRIDE_BYTES,
            max_estimated_model_bytes: None,
            max_factor_edges: None,
        }
    }
}

impl ProviderGraphV4Options {
    fn validate(&self) -> ProviderGraphV4Result<()> {
        if self.member_page_bytes < 4 || self.member_page_bytes > i32::MAX as usize {
            return Err(invalid(
                "V4 member page bytes must be between 4 and int32::MAX",
            ));
        }
        if self.locator_page_bytes < LOCATOR_BYTES || self.locator_page_bytes > i32::MAX as usize {
            return Err(invalid(
                "V4 locator page bytes must be between 12 and int32::MAX",
            ));
        }
        if self.heavy_owner_member_threshold == 0 {
            return Err(invalid("V4 heavy-owner threshold must be positive"));
        }
        if self.max_set_patterns_per_set == 0 {
            return Err(invalid(
                "V4 maximum set-to-pattern serving degree must be positive",
            ));
        }
        if self.max_set_components_per_fallback_set == 0 {
            return Err(invalid(
                "V4 maximum set-to-component fallback degree must be positive",
            ));
        }
        if self.max_online_group_keys_per_set == 0 {
            return Err(invalid("V4 maximum online set/group work must be positive"));
        }
        if self.max_online_source_owners_per_set == 0 {
            return Err(invalid(
                "V4 maximum online source-owner work must be positive",
            ));
        }
        if self.max_online_source_members_per_set == 0 {
            return Err(invalid(
                "V4 maximum online source-member work must be positive",
            ));
        }
        if self.max_online_source_pages_per_set == 0 {
            return Err(invalid(
                "V4 maximum online source-page work must be positive",
            ));
        }
        if self.max_online_source_bytes_per_set == 0 {
            return Err(invalid(
                "V4 maximum online source-byte work must be positive",
            ));
        }
        if self.online_group_npi_batch_size == 0 {
            return Err(invalid(
                "V4 online group-to-NPI batch size must be positive",
            ));
        }
        if self.max_online_group_npi_members_per_set == 0 {
            return Err(invalid(
                "V4 maximum online group-to-NPI member work must be positive",
            ));
        }
        if self.max_online_group_npi_locator_pages_per_set == 0 {
            return Err(invalid(
                "V4 maximum online group-to-NPI locator-page work must be positive",
            ));
        }
        if self.max_online_group_npi_member_pages_per_set == 0 {
            return Err(invalid(
                "V4 maximum online group-to-NPI member-page work must be positive",
            ));
        }
        if self.max_online_group_npi_bytes_per_set == 0 {
            return Err(invalid(
                "V4 maximum online group-to-NPI byte work must be positive",
            ));
        }
        if self.max_online_group_npi_batches_per_set == 0 {
            return Err(invalid(
                "V4 maximum online group-to-NPI batch work must be positive",
            ));
        }
        if self.provider_expansion_rate_page_rows == 0 {
            return Err(invalid(
                "V4 provider-expansion rate page rows must be positive",
            ));
        }
        if self.max_online_provider_expansion_rate_rows == 0 {
            return Err(invalid(
                "V4 maximum online provider-expansion rate rows must be positive",
            ));
        }
        if self.max_online_provider_expansion_provider_sets == 0 {
            return Err(invalid(
                "V4 maximum online provider-expansion provider sets must be positive",
            ));
        }
        if self.max_online_provider_expansion_graph_batches == 0 {
            return Err(invalid(
                "V4 maximum online provider-expansion graph batches must be positive",
            ));
        }
        if self.npi_prefix_target == 0 {
            return Err(invalid("V4 NPI prefix target must be positive"));
        }
        if self.max_npi_prefix_override_owners == 0 {
            return Err(invalid(
                "V4 maximum NPI prefix override owners must be positive",
            ));
        }
        if self.max_npi_prefix_override_bytes == 0 {
            return Err(invalid(
                "V4 maximum NPI prefix override bytes must be positive",
            ));
        }
        if self.max_estimated_model_bytes == Some(0) {
            return Err(invalid("V4 estimated-model byte limit must be positive"));
        }
        if self.max_factor_edges == Some(0) {
            return Err(invalid("V4 factor-edge limit must be positive"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct ProviderGraphV4Manifest {
    pub shards: Vec<V4ProviderGraphShardDescriptor>,
    pub provider_set_key_map_path: PathBuf,
    pub output_directory: PathBuf,
    #[serde(default)]
    pub options: ProviderGraphV4Options,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderGraphV4Layout {
    Direct,
    Pattern,
}

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
pub struct V4RelationSummary {
    pub relation: String,
    pub member_object_kind: String,
    pub locator_object_kind: String,
    pub owner_base: u32,
    pub owner_count: u64,
    pub logical_member_count: u64,
    pub vector_member_count: u64,
    pub member_width: u8,
    pub member_page_bytes: u64,
    pub locator_page_bytes: u64,
    pub locator_owner_span: u32,
    pub member_block_count: u64,
    pub locator_block_count: u64,
    pub raw_vector_bytes: u64,
    pub raw_locator_bytes: u64,
    pub encoded_byte_count: u64,
}

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
pub struct V4HeavyBitmapSummary {
    pub relation: String,
    pub object_kind: String,
    pub owner_key: u32,
    pub member_count: u64,
    pub member_base: u32,
    pub member_span: u64,
    pub raw_byte_count: u64,
    pub vector_byte_count: u64,
    pub saved_decode_bytes: u64,
    pub block_count: u64,
}

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
pub struct V4OutputArtifactSummary {
    pub name: String,
    pub path: PathBuf,
    pub byte_count: u64,
    pub sha256: String,
    pub row_count: u64,
}

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
pub struct V4ObserveCounters {
    pub component_count: u64,
    pub group_count: u64,
    pub provider_set_count: u64,
    pub provider_set_audit_npi_count: u64,
    pub npi_count: u64,
    pub set_component_edge_count: u64,
    pub component_group_edge_count: u64,
    pub group_npi_edge_count: u64,
    pub group_component_edge_count: u64,
    pub group_set_incidence_count: u64,
    pub pattern_count: u64,
    pub pattern_set_edge_count: u64,
    pub set_pattern_edge_count: u64,
    pub npi_pattern_edge_count: u64,
    pub multi_component_group_count: u64,
    pub maximum_components_per_group: u64,
    pub maximum_sets_per_group: u64,
    pub maximum_groups_per_set: u64,
    pub maximum_groups_per_npi: u64,
    pub maximum_patterns_per_npi: u64,
    pub npi_patterns_per_npi_p50: u64,
    pub npi_patterns_per_npi_p95: u64,
    pub npi_patterns_per_npi_p99: u64,
    pub maximum_patterns_per_set: u64,
    pub maximum_components_per_set: u64,
    pub pattern_overflow_set_count: u64,
    pub maximum_components_per_pattern_overflow_set: u64,
    pub unsafe_pattern_component_set_count: u64,
    pub npi_prefix_group_unsafe_set_count: u64,
    pub npi_prefix_physical_unsafe_set_count: u64,
    pub npi_prefix_simulated_set_count: u64,
    pub npi_prefix_group_merge_member_visits: u64,
    pub npi_prefix_worst_online_probe_merge_member_visits: u64,
    pub npi_prefix_override_owner_count: u64,
    pub npi_prefix_override_member_count: u64,
    pub npi_prefix_override_raw_bytes: u64,
    pub npi_prefix_override_encoded_bytes: u64,
    pub npi_prefix_groups_to_target_p50: u64,
    pub npi_prefix_groups_to_target_p95: u64,
    pub npi_prefix_groups_to_target_p99: u64,
    pub npi_prefix_groups_to_target_max: u64,
    pub npi_prefix_worst_provider_set_key: Option<u32>,
    pub npi_prefix_worst_groups_to_target: u64,
    pub npi_prefix_worst_provider_set_uses_override: bool,
    pub npi_prefix_worst_uses_component_fallback: bool,
    pub npi_prefix_worst_member_count: u64,
    pub npi_prefix_worst_member_digest: Option<String>,
    pub npi_prefix_worst_source_owner_work: u64,
    pub npi_prefix_worst_source_member_work: u64,
    pub npi_prefix_worst_source_page_work: u64,
    pub npi_prefix_worst_source_byte_work: u64,
    pub npi_prefix_worst_group_npi_member_work: u64,
    pub npi_prefix_worst_group_npi_locator_page_work: u64,
    pub npi_prefix_worst_group_npi_member_page_work: u64,
    pub npi_prefix_worst_group_npi_byte_work: u64,
    pub npi_prefix_worst_group_npi_batch_work: u64,
    pub npi_prefix_worst_online_provider_set_key: Option<u32>,
    pub npi_prefix_worst_online_groups_to_target: u64,
    pub npi_prefix_worst_online_groups_to_target_exact: bool,
    pub npi_prefix_worst_online_uses_component_fallback: bool,
    pub npi_prefix_worst_online_group_work_bound: u64,
    pub npi_prefix_worst_online_member_count: u64,
    pub npi_prefix_worst_online_member_digest: Option<String>,
    pub npi_prefix_worst_online_source_owner_work: u64,
    pub npi_prefix_worst_online_source_member_work: u64,
    pub npi_prefix_worst_online_source_page_work: u64,
    pub npi_prefix_worst_online_source_byte_work: u64,
    pub npi_prefix_worst_online_group_npi_member_work: u64,
    pub npi_prefix_worst_online_group_npi_locator_page_work: u64,
    pub npi_prefix_worst_online_group_npi_member_page_work: u64,
    pub npi_prefix_worst_online_group_npi_byte_work: u64,
    pub npi_prefix_worst_online_group_npi_batch_work: u64,
    pub maximum_online_source_owner_work: u64,
    pub maximum_online_source_member_work: u64,
    pub maximum_online_source_page_work: u64,
    pub maximum_online_source_byte_work: u64,
    pub maximum_online_group_npi_member_work: u64,
    pub maximum_online_group_npi_locator_page_work: u64,
    pub maximum_online_group_npi_member_page_work: u64,
    pub maximum_online_group_npi_byte_work: u64,
    pub maximum_online_group_npi_batch_work: u64,
    pub empty_incidence_group_count: u64,
    pub maximum_groups_per_set_computed: u64,
    pub group_set_expansion_owner_visits: u64,
    pub group_set_expansion_edge_visits: u64,
    pub direct_group_set_emission_owner_visits: u64,
    pub direct_group_set_emission_edge_visits: u64,
    pub set_group_expansion_owner_visits: u64,
    pub set_group_expansion_edge_visits: u64,
    pub single_component_group_fast_path_count: u64,
    pub multi_component_group_union_count: u64,
    pub component_tuple_pattern_cache_owner_count: u64,
    pub component_tuple_pattern_cache_member_count: u64,
}

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
pub struct V4ResourceAdmissionSummary {
    pub formula: String,
    pub input_factor_bytes: u64,
    pub provider_set_key_map_bytes: u64,
    pub factor_edge_count: u64,
    pub factor_owner_count: u64,
    pub base_estimated_model_bytes: u64,
    pub derived_projection_bytes: u64,
    pub retained_scratch_high_water_bytes: u64,
    pub bounded_emission_buffer_bytes: u64,
    pub estimated_peak_bytes: u64,
    pub max_estimated_model_bytes: Option<u64>,
    pub max_factor_edges: Option<u64>,
}

#[derive(Debug)]
struct ResourceAdmissionTracker {
    summary: V4ResourceAdmissionSummary,
}

impl ResourceAdmissionTracker {
    fn checked_peak(
        &self,
        derived_projection_bytes: u64,
        retained_scratch_high_water_bytes: u64,
    ) -> ProviderGraphV4Result<u64> {
        self.summary
            .base_estimated_model_bytes
            .checked_add(derived_projection_bytes)
            .and_then(|value| value.checked_add(retained_scratch_high_water_bytes))
            .and_then(|value| value.checked_add(self.summary.bounded_emission_buffer_bytes))
            .ok_or(invalid(
                "resource_admission: estimated peak byte count overflows",
            ))
    }

    fn ensure_within_limit(
        &self,
        estimated_peak_bytes: u64,
        label: &str,
    ) -> ProviderGraphV4Result<()> {
        if self
            .summary
            .max_estimated_model_bytes
            .is_some_and(|limit| estimated_peak_bytes > limit)
        {
            return Err(invalid(format!(
                "resource_admission: estimated peak bytes {estimated_peak_bytes} exceeds configured limit {} while {label}",
                self.summary
                    .max_estimated_model_bytes
                    .expect("checked above")
            )));
        }
        Ok(())
    }

    fn reserve_projection(&mut self, label: &str, bytes: u64) -> ProviderGraphV4Result<()> {
        let derived_projection_bytes = self
            .summary
            .derived_projection_bytes
            .checked_add(bytes)
            .ok_or(invalid(
                "resource_admission: derived resident byte count overflows",
            ))?;
        let estimated_peak_bytes = self.checked_peak(
            derived_projection_bytes,
            self.summary.retained_scratch_high_water_bytes,
        )?;
        self.ensure_within_limit(
            estimated_peak_bytes,
            &format!("reserving {label}; derived resident bytes {derived_projection_bytes}"),
        )?;
        self.summary.derived_projection_bytes = derived_projection_bytes;
        self.summary.estimated_peak_bytes = estimated_peak_bytes;
        Ok(())
    }

    fn reserve_scratch_members(
        &mut self,
        label: &str,
        members: usize,
    ) -> ProviderGraphV4Result<()> {
        let requested = estimated_u32_capacity_bytes(members)?;
        self.reserve_scratch_bytes(label, requested)
    }

    fn reserve_scratch_bytes(&mut self, label: &str, requested: u64) -> ProviderGraphV4Result<()> {
        if requested <= self.summary.retained_scratch_high_water_bytes {
            return Ok(());
        }
        let estimated_peak_bytes =
            self.checked_peak(self.summary.derived_projection_bytes, requested)?;
        self.ensure_within_limit(
            estimated_peak_bytes,
            &format!("reserving {label}; scratch high-water bytes {requested}"),
        )?;
        self.summary.retained_scratch_high_water_bytes = requested;
        self.summary.estimated_peak_bytes = estimated_peak_bytes;
        Ok(())
    }

    fn into_summary(self) -> V4ResourceAdmissionSummary {
        self.summary
    }
}

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
pub struct ProviderGraphV4ConversionSummary {
    pub format: String,
    pub selected_layout: ProviderGraphV4Layout,
    pub pattern_layout_serving_degree_eligible: bool,
    pub max_set_patterns_per_set: u64,
    pub max_set_components_per_fallback_set: u64,
    pub max_online_group_keys_per_set: u64,
    pub max_online_source_owners_per_set: u64,
    pub max_online_source_members_per_set: u64,
    pub max_online_source_pages_per_set: u64,
    pub max_online_source_bytes_per_set: u64,
    pub online_group_npi_batch_size: u64,
    pub max_online_group_npi_members_per_set: u64,
    pub max_online_group_npi_locator_pages_per_set: u64,
    pub max_online_group_npi_member_pages_per_set: u64,
    pub max_online_group_npi_bytes_per_set: u64,
    pub max_online_group_npi_batches_per_set: u64,
    pub provider_expansion_rate_page_rows: u64,
    pub max_online_provider_expansion_rate_rows: u64,
    pub max_online_provider_expansion_provider_sets: u64,
    pub max_online_provider_expansion_graph_batches: u64,
    pub npi_prefix_target: u64,
    pub max_npi_prefix_override_owners: u64,
    pub max_npi_prefix_override_bytes: u64,
    pub direct_complete_encoded_bytes: u64,
    pub pattern_complete_encoded_bytes: u64,
    pub common_encoded_bytes: u64,
    pub selected_encoded_bytes: u64,
    pub block_copy_path: PathBuf,
    pub reference_manifest_path: PathBuf,
    pub group_copy_path: PathBuf,
    pub component_copy_path: PathBuf,
    pub npi_copy_path: PathBuf,
    pub provider_set_audit_npi_copy_path: PathBuf,
    pub provider_set_npi_prefix_override_copy_path: PathBuf,
    pub pattern_copy_path: Option<PathBuf>,
    pub summary_path: PathBuf,
    pub block_count: u64,
    pub block_copy_bytes: u64,
    pub relation_summaries: Vec<V4RelationSummary>,
    pub heavy_bitmaps: Vec<V4HeavyBitmapSummary>,
    pub output_artifacts: Vec<V4OutputArtifactSummary>,
    pub observe: V4ObserveCounters,
    pub resource_admission: V4ResourceAdmissionSummary,
    pub input_byte_count: u64,
    pub input_sha256: String,
}

#[derive(Clone, Copy, Debug)]
struct OwnerRecord {
    owner: GlobalId,
    member_offset: u64,
    member_count: u32,
}

struct ValidatedArtifact {
    _file: File,
    bytes: Mmap,
    owner_count: u64,
    member_count: u64,
    member_global_count: u64,
    index_start: usize,
    dictionary_start: usize,
    members_start: usize,
    dense: bool,
    byte_count: u64,
    sha256: [u8; 32],
}

impl ValidatedArtifact {
    fn open(descriptor: &V4MembershipArtifactDescriptor) -> ProviderGraphV4Result<Self> {
        let expected_digest = parse_sha256(&descriptor.metadata.sha256)?;
        let file = match File::open(&descriptor.path) {
            Ok(file) => file,
            Err(error) => {
                return Err(invalid(format!(
                    "V4 membership sidecar is unavailable ({}): {error}",
                    descriptor.path.display()
                )));
            }
        };
        let observed_size = file.metadata()?.len();
        if observed_size != descriptor.metadata.byte_count {
            return Err(invalid(format!(
                "V4 membership byte count mismatch for {}: expected {}, got {}",
                descriptor.path.display(),
                descriptor.metadata.byte_count,
                observed_size
            )));
        }
        // SAFETY: completed scanner sidecars are immutable for the conversion.
        let bytes = unsafe { MmapOptions::new().map(&file)? };
        let observed_digest: [u8; 32] = Sha256::digest(&bytes).into();
        if observed_digest != expected_digest {
            return Err(invalid(format!(
                "V4 membership checksum mismatch: {}",
                descriptor.path.display()
            )));
        }
        if bytes.len() < 8 {
            return Err(invalid("V4 membership sidecar is missing its header"));
        }
        let (dense, header_size, owner_count, member_global_count) =
            if &bytes[..8] == STANDARD_MAGIC {
                if descriptor.metadata.record_format != STANDARD_FORMAT {
                    return Err(invalid("V4 standard membership format metadata mismatch"));
                }
                if bytes.len() < STANDARD_HEADER_BYTES {
                    return Err(invalid("V4 membership sidecar has a truncated header"));
                }
                let version = read_u32_le(&bytes, 8)?;
                if version != MANIFEST_VERSION {
                    return Err(invalid(format!(
                        "unsupported V4 source membership version: {version}"
                    )));
                }
                (false, STANDARD_HEADER_BYTES, read_u64_le(&bytes, 12)?, 0)
            } else if &bytes[..8] == DENSE_MAGIC {
                if descriptor.metadata.record_format != DENSE_FORMAT {
                    return Err(invalid("V4 dense membership format metadata mismatch"));
                }
                if bytes.len() < DENSE_HEADER_BYTES {
                    return Err(invalid(
                        "V4 dense membership sidecar has a truncated header",
                    ));
                }
                let version = read_u32_le(&bytes, 8)?;
                if version != MANIFEST_VERSION {
                    return Err(invalid(format!(
                        "unsupported V4 source membership version: {version}"
                    )));
                }
                (
                    true,
                    DENSE_HEADER_BYTES,
                    read_u64_le(&bytes, 12)?,
                    read_u64_le(&bytes, 20)?,
                )
            } else {
                return Err(invalid("V4 membership sidecar has an invalid magic header"));
            };
        if owner_count != descriptor.metadata.owner_count {
            return Err(invalid("V4 membership owner count metadata mismatch"));
        }
        if dense && descriptor.metadata.member_global_count != Some(member_global_count) {
            return Err(invalid(
                "V4 dense membership dictionary count metadata mismatch",
            ));
        }
        let owner_count_usize = invalid_conversion(
            usize::try_from(owner_count),
            "V4 membership owner count exceeds addressable memory",
        )?;
        let member_count_usize = invalid_conversion(
            usize::try_from(descriptor.metadata.member_count),
            "V4 membership member count exceeds addressable memory",
        )?;
        let dictionary_count_usize = invalid_conversion(
            usize::try_from(member_global_count),
            "V4 membership dictionary exceeds addressable memory",
        )?;
        let index_bytes = owner_count_usize
            .checked_mul(OWNER_RECORD_BYTES)
            .ok_or(invalid("V4 membership index size overflows"))?;
        let dictionary_start = header_size
            .checked_add(index_bytes)
            .ok_or(invalid("V4 membership layout overflows"))?;
        let members_start = dictionary_start
            .checked_add(
                dictionary_count_usize
                    .checked_mul(GLOBAL_ID_BYTES)
                    .ok_or(invalid("V4 membership dictionary size overflows"))?,
            )
            .ok_or(invalid("V4 membership layout overflows"))?;
        let member_width = if dense { 4 } else { GLOBAL_ID_BYTES };
        let expected_size = members_start
            .checked_add(
                member_count_usize
                    .checked_mul(member_width)
                    .ok_or(invalid("V4 membership member size overflows"))?,
            )
            .ok_or(invalid("V4 membership layout overflows"))?;
        if expected_size != bytes.len() {
            return Err(invalid(format!(
                "V4 membership layout size mismatch: expected {expected_size}, got {}",
                bytes.len()
            )));
        }
        let artifact = Self {
            _file: file,
            bytes,
            owner_count,
            member_count: descriptor.metadata.member_count,
            member_global_count,
            index_start: header_size,
            dictionary_start,
            members_start,
            dense,
            byte_count: descriptor.metadata.byte_count,
            sha256: observed_digest,
        };
        artifact.validate()?;
        Ok(artifact)
    }

    fn validate(&self) -> ProviderGraphV4Result<()> {
        if self.dense {
            let mut previous = None;
            for index in 0..self.member_global_count {
                let current = self.dictionary_global(index)?;
                if previous.is_some_and(|value| current <= value) {
                    return Err(invalid(
                        "V4 dense membership dictionary must be sorted and unique",
                    ));
                }
                previous = Some(current);
            }
        }
        let mut previous_owner = None;
        let mut expected_offset = 0u64;
        for owner_index in 0..self.owner_count {
            let owner = self.owner(owner_index)?;
            if previous_owner.is_some_and(|value| owner.owner <= value) {
                return Err(invalid("V4 membership owners must be sorted and unique"));
            }
            if owner.member_offset != expected_offset {
                return Err(invalid("V4 membership offsets must be contiguous"));
            }
            let mut previous_member = None;
            for member_index in
                owner.member_offset..owner.member_offset + u64::from(owner.member_count)
            {
                let member = self.member_global(member_index)?;
                if previous_member.is_some_and(|value| member <= value) {
                    return Err(invalid("V4 membership members must be sorted and unique"));
                }
                previous_member = Some(member);
            }
            expected_offset = expected_offset
                .checked_add(u64::from(owner.member_count))
                .ok_or(invalid("V4 membership member count overflows"))?;
            previous_owner = Some(owner.owner);
        }
        if expected_offset != self.member_count {
            return Err(invalid("V4 membership member count mismatch"));
        }
        Ok(())
    }

    fn owner(&self, index: u64) -> ProviderGraphV4Result<OwnerRecord> {
        if index >= self.owner_count {
            return Err(invalid("V4 membership owner index is out of range"));
        }
        let index = invalid_conversion(
            usize::try_from(index),
            "V4 membership owner index exceeds addressable memory",
        )?;
        let offset = self
            .index_start
            .checked_add(index * OWNER_RECORD_BYTES)
            .ok_or(invalid("V4 membership owner offset overflows"))?;
        Ok(OwnerRecord {
            owner: read_global_id(&self.bytes, offset)?,
            member_offset: read_u64_le(&self.bytes, offset + 16)?,
            member_count: read_u32_le(&self.bytes, offset + 24)?,
        })
    }

    fn dictionary_global(&self, index: u64) -> ProviderGraphV4Result<GlobalId> {
        if index >= self.member_global_count {
            return Err(invalid("V4 dense membership ID is out of range"));
        }
        let index = invalid_conversion(
            usize::try_from(index),
            "V4 membership dictionary index is too large",
        )?;
        read_global_id(&self.bytes, self.dictionary_start + index * GLOBAL_ID_BYTES)
    }

    fn member_global(&self, index: u64) -> ProviderGraphV4Result<GlobalId> {
        if index >= self.member_count {
            return Err(invalid("V4 membership member index is out of range"));
        }
        let index = invalid_conversion(
            usize::try_from(index),
            "V4 membership member index is too large",
        )?;
        if self.dense {
            let local = read_u32_le(&self.bytes, self.members_start + index * 4)?;
            self.dictionary_global(u64::from(local))
        } else {
            read_global_id(&self.bytes, self.members_start + index * GLOBAL_ID_BYTES)
        }
    }

    fn for_each_pair(
        &self,
        mut consume: impl FnMut(GlobalId, GlobalId) -> ProviderGraphV4Result<()>,
    ) -> ProviderGraphV4Result<()> {
        for owner_index in 0..self.owner_count {
            let owner = self.owner(owner_index)?;
            for member_index in
                owner.member_offset..owner.member_offset + u64::from(owner.member_count)
            {
                consume(owner.owner, self.member_global(member_index)?)?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct EdgeFingerprint {
    xor: [u8; 32],
    sum: [u64; 4],
    count: u64,
}

impl EdgeFingerprint {
    fn add(&mut self, edge: &[u8]) {
        let mut xor_hasher = Sha256::new();
        xor_hasher.update(EDGE_XOR_DOMAIN);
        xor_hasher.update(edge);
        let xor_hash: [u8; 32] = xor_hasher.finalize().into();
        for (destination, value) in self.xor.iter_mut().zip(xor_hash) {
            *destination ^= value;
        }
        let mut sum_hasher = Sha256::new();
        sum_hasher.update(EDGE_SUM_DOMAIN);
        sum_hasher.update(edge);
        let sum_hash: [u8; 32] = sum_hasher.finalize().into();
        for (index, destination) in self.sum.iter_mut().enumerate() {
            let offset = index * 8;
            *destination = destination.wrapping_add(u64::from_be_bytes(
                sum_hash[offset..offset + 8]
                    .try_into()
                    .expect("fixed digest width"),
            ));
        }
        self.count = self.count.saturating_add(1);
    }
}

#[derive(Debug)]
struct ProviderSetMap {
    by_global: HashMap<GlobalId, u32>,
    globals_by_index: Vec<GlobalId>,
    key_base: u32,
}

impl ProviderSetMap {
    fn read(path: &Path) -> ProviderGraphV4Result<Self> {
        let mut reader = BufReader::new(File::open(path)?);
        let mut line = String::new();
        let mut by_global = HashMap::new();
        let mut globals_by_index = Vec::new();
        let mut previous = None;
        let mut key_base = None;
        loop {
            line.clear();
            if reader.read_line(&mut line)? == 0 {
                break;
            }
            while line.ends_with(['\n', '\r']) {
                line.pop();
            }
            let mut fields = line.split('\t');
            let global = parse_global_id_hex(
                fields
                    .next()
                    .ok_or(invalid("V4 provider-set map row is invalid"))?,
            )?;
            let key = invalid_conversion(
                fields
                    .next()
                    .ok_or(invalid("V4 provider-set map row is invalid"))?
                    .parse::<u32>(),
                "V4 provider-set key is invalid",
            )?;
            if fields.next().is_some() {
                return Err(invalid("V4 provider-set map row has extra fields"));
            }
            if previous.is_some_and(|value| global <= value) {
                return Err(invalid(
                    "V4 provider-set global IDs must be sorted and unique",
                ));
            }
            let base = *key_base.get_or_insert(key);
            if base != 0 && base != 1 {
                return Err(invalid("V4 provider-set keys must start at zero or one"));
            }
            let expected = base
                .checked_add(invalid_conversion(
                    u32::try_from(globals_by_index.len()),
                    "V4 provider-set map exceeds uint32",
                )?)
                .ok_or(invalid("V4 provider-set key overflows uint32"))?;
            if key != expected {
                return Err(invalid(
                    "V4 provider-set keys must be dense in global-ID order",
                ));
            }
            by_global.insert(global, key);
            globals_by_index.push(global);
            previous = Some(global);
        }
        let key_base = key_base.ok_or(invalid("V4 provider-set map is empty"))?;
        Ok(Self {
            by_global,
            globals_by_index,
            key_base,
        })
    }

    fn key(&self, global: GlobalId) -> ProviderGraphV4Result<u32> {
        match self.by_global.get(&global).copied() {
            Some(key) => Ok(key),
            None => Err(invalid(
                "V4 factor references a provider set absent from the authoritative map",
            )),
        }
    }

    fn index(&self, key: u32) -> ProviderGraphV4Result<usize> {
        let relative = key.checked_sub(self.key_base).ok_or(invalid(
            "V4 provider-set key precedes authoritative key base",
        ))?;
        let index = relative as usize;
        if index >= self.globals_by_index.len() {
            return Err(invalid("V4 provider-set key is outside authoritative map"));
        }
        Ok(index)
    }
}

#[derive(Debug)]
struct RawFactors {
    set_components: HashMap<GlobalId, Vec<GlobalId>>,
    component_groups: HashMap<GlobalId, Vec<GlobalId>>,
    group_npis: HashMap<GlobalId, Vec<GlobalId>>,
    input_byte_count: u64,
    input_digest: Sha256,
}

impl RawFactors {
    fn new() -> Self {
        let mut input_digest = Sha256::new();
        input_digest.update(b"PTG2V4INPUT\x01");
        Self {
            set_components: HashMap::new(),
            component_groups: HashMap::new(),
            group_npis: HashMap::new(),
            input_byte_count: 0,
            input_digest,
        }
    }

    fn record_artifact(&mut self, label: &str, artifact: &ValidatedArtifact) {
        self.input_digest.update((label.len() as u32).to_be_bytes());
        self.input_digest.update(label.as_bytes());
        self.input_digest.update(artifact.sha256);
        self.input_digest.update(artifact.byte_count.to_be_bytes());
        self.input_byte_count = self.input_byte_count.saturating_add(artifact.byte_count);
    }
}

fn normalize_map(map: &mut HashMap<GlobalId, Vec<GlobalId>>) {
    for members in map.values_mut() {
        members.sort_unstable();
        members.dedup();
    }
}

fn merge_artifact_into(
    artifact: &ValidatedArtifact,
    target: &mut HashMap<GlobalId, Vec<GlobalId>>,
    progress: &mut ProgressReporter<'_>,
    completed_edges: &mut u64,
    total_edges: u64,
) -> ProviderGraphV4Result<()> {
    artifact.for_each_pair(|owner, member| {
        target.entry(owner).or_default().push(member);
        *completed_edges = completed_edges
            .checked_add(1)
            .ok_or(invalid("V4 factor progress count overflows"))?;
        progress.periodic(
            "load_factors",
            *completed_edges,
            total_edges,
            "factor_edges",
        );
        Ok(())
    })
}

fn group_npi_fingerprint(
    artifact: &ValidatedArtifact,
    reversed: bool,
    progress: &mut ProgressReporter<'_>,
    completed_edges: &mut u64,
    total_edges: u64,
) -> ProviderGraphV4Result<EdgeFingerprint> {
    let mut fingerprint = EdgeFingerprint::default();
    artifact.for_each_pair(|owner, member| {
        let (group, npi) = if reversed {
            (member, owner)
        } else {
            (owner, member)
        };
        let npi_value = npi_from_global_id(npi)?;
        let mut edge = [0u8; 24];
        edge[..16].copy_from_slice(&group);
        edge[16..].copy_from_slice(&npi_value.to_be_bytes());
        fingerprint.add(&edge);
        *completed_edges = completed_edges
            .checked_add(1)
            .ok_or(invalid("V4 factor progress count overflows"))?;
        progress.periodic(
            "load_factors",
            *completed_edges,
            total_edges,
            "factor_edges",
        );
        Ok(())
    })?;
    Ok(fingerprint)
}

fn load_raw_factors(
    descriptors: &[V4ProviderGraphShardDescriptor],
    progress: &mut ProgressReporter<'_>,
) -> ProviderGraphV4Result<RawFactors> {
    if descriptors.is_empty() {
        return Err(invalid("V4 provider graph requires at least one shard"));
    }
    let total_edges = descriptors.iter().try_fold(0u64, |total, descriptor| {
        let reciprocal_group_npi_edges = descriptor
            .provider_group_npi
            .metadata
            .member_count
            .checked_mul(2)
            .ok_or(invalid("V4 factor progress total overflows"))?;
        total
            .checked_add(descriptor.provider_set_component.metadata.member_count)
            .and_then(|value| {
                value.checked_add(descriptor.provider_component_group.metadata.member_count)
            })
            .and_then(|value| value.checked_add(reciprocal_group_npi_edges))
            .and_then(|value| {
                value.checked_add(descriptor.provider_npi_group.metadata.member_count)
            })
            .ok_or(invalid("V4 factor progress total overflows"))
    })?;
    let mut completed_edges = 0u64;
    progress.periodic("load_factors", 0, total_edges, "factor_edges");
    let mut seen = HashSet::new();
    let mut raw = RawFactors::new();
    for descriptor in descriptors {
        let shard_id = descriptor.shard_id.trim();
        if shard_id.is_empty() || !seen.insert(shard_id.to_owned()) {
            return Err(invalid(
                "V4 provider graph shard IDs must be non-empty and unique",
            ));
        }
        for metadata in [
            &descriptor.provider_set_component.metadata,
            &descriptor.provider_component_group.metadata,
            &descriptor.provider_group_npi.metadata,
            &descriptor.provider_npi_group.metadata,
        ] {
            let source = metadata.source_shard_id.as_deref().map(str::trim);
            let alias = metadata.shard_id.as_deref().map(str::trim);
            if source.is_some() && alias.is_some() && source != alias {
                return Err(invalid("V4 membership has contradictory shard IDs"));
            }
            if source.or(alias).is_some_and(|value| value != shard_id) {
                return Err(invalid(format!(
                    "V4 membership shard ID does not match bundle {shard_id}"
                )));
            }
        }
        let set_component = ValidatedArtifact::open(&descriptor.provider_set_component)?;
        let component_group = ValidatedArtifact::open(&descriptor.provider_component_group)?;
        let group_npi = ValidatedArtifact::open(&descriptor.provider_group_npi)?;
        let npi_group = ValidatedArtifact::open(&descriptor.provider_npi_group)?;
        if group_npi_fingerprint(
            &group_npi,
            false,
            progress,
            &mut completed_edges,
            total_edges,
        )? != group_npi_fingerprint(
            &npi_group,
            true,
            progress,
            &mut completed_edges,
            total_edges,
        )? {
            return Err(invalid(format!(
                "V4 shard {shard_id} group/NPI directions are not reciprocal"
            )));
        }
        raw.record_artifact("provider_set_component", &set_component);
        raw.record_artifact("provider_component_group", &component_group);
        raw.record_artifact("provider_group_npi", &group_npi);
        raw.record_artifact("provider_npi_group", &npi_group);
        merge_artifact_into(
            &set_component,
            &mut raw.set_components,
            progress,
            &mut completed_edges,
            total_edges,
        )?;
        merge_artifact_into(
            &component_group,
            &mut raw.component_groups,
            progress,
            &mut completed_edges,
            total_edges,
        )?;
        // Store one exact direction only. The reciprocal sidecar has already
        // been validated and would otherwise double peak graph memory.
        merge_artifact_into(
            &group_npi,
            &mut raw.group_npis,
            progress,
            &mut completed_edges,
            total_edges,
        )?;
    }
    if completed_edges != total_edges {
        return Err(invalid("V4 factor progress count differs from metadata"));
    }
    normalize_map(&mut raw.set_components);
    normalize_map(&mut raw.component_groups);
    normalize_map(&mut raw.group_npis);
    Ok(raw)
}

fn resource_admission_preflight(
    descriptors: &[V4ProviderGraphShardDescriptor],
    provider_set_key_map_path: &Path,
    options: &ProviderGraphV4Options,
) -> ProviderGraphV4Result<ResourceAdmissionTracker> {
    if descriptors.is_empty() {
        return Err(invalid("V4 provider graph requires at least one shard"));
    }
    let mut input_factor_bytes = 0u64;
    let mut factor_edge_count = 0u64;
    let mut factor_owner_count = 0u64;
    for shard in descriptors {
        for artifact in [
            &shard.provider_set_component,
            &shard.provider_component_group,
            &shard.provider_group_npi,
            &shard.provider_npi_group,
        ] {
            input_factor_bytes = input_factor_bytes
                .checked_add(artifact.metadata.byte_count)
                .ok_or(invalid("resource_admission: input byte count overflows"))?;
            factor_edge_count = factor_edge_count
                .checked_add(artifact.metadata.member_count)
                .ok_or(invalid("resource_admission: factor edge count overflows"))?;
            factor_owner_count = factor_owner_count
                .checked_add(artifact.metadata.owner_count)
                .ok_or(invalid("resource_admission: factor owner count overflows"))?;
        }
    }
    let provider_set_key_map_bytes = match fs::metadata(provider_set_key_map_path) {
        Ok(metadata) => metadata.len(),
        Err(error) => {
            return Err(invalid(format!(
                "resource_admission: provider-set key map is unavailable: {error}"
            )));
        }
    };
    // This deliberately over-accounts both reciprocal NPI relations and all
    // immutable input mmaps.  The factor model never includes flat set/group
    // incidence, but HashMap/vector allocator overhead is budgeted at 128
    // bytes per declared edge and 256 bytes per declared owner.
    let base_estimated_model_bytes = input_factor_bytes
        .checked_add(provider_set_key_map_bytes.saturating_mul(4))
        .and_then(|value| value.checked_add(factor_edge_count.saturating_mul(128)))
        .and_then(|value| value.checked_add(factor_owner_count.saturating_mul(256)))
        .ok_or(invalid(
            "resource_admission: estimated peak byte count overflows",
        ))?;
    // Emission holds one relation-member page, one locator page, and at most
    // one streamed heavy-bitmap page at the same time. Reference rows are
    // externally spooled by object kind, so their resident memory is bounded
    // by a fixed number of small writer buffers rather than block count.
    let bounded_emission_buffer_bytes = invalid_conversion(
        u64::try_from(options.member_page_bytes),
        "resource_admission: member page bytes exceed uint64",
    )?
    .checked_mul(2)
    .and_then(|value| value.checked_add(options.locator_page_bytes as u64))
    .and_then(|value| value.checked_add(REFERENCE_SPOOL_FIXED_BYTES))
    .ok_or(invalid(
        "resource_admission: emission buffer byte count overflows",
    ))?;
    let estimated_peak_bytes = base_estimated_model_bytes
        .checked_add(bounded_emission_buffer_bytes)
        .ok_or(invalid(
            "resource_admission: estimated peak byte count overflows",
        ))?;
    if options
        .max_factor_edges
        .is_some_and(|limit| factor_edge_count > limit)
    {
        return Err(invalid(format!(
            "resource_admission: factor edge count {factor_edge_count} exceeds configured limit {}",
            options.max_factor_edges.expect("checked above")
        )));
    }
    if options
        .max_estimated_model_bytes
        .is_some_and(|limit| estimated_peak_bytes > limit)
    {
        return Err(invalid(format!(
            "resource_admission: estimated peak bytes {estimated_peak_bytes} exceeds configured limit {}",
            options.max_estimated_model_bytes.expect("checked above")
        )));
    }
    Ok(ResourceAdmissionTracker {
        summary: V4ResourceAdmissionSummary {
            formula: "base(input_factor_bytes + provider_set_key_map_bytes*4 + factor_edges*128 + factor_owners*256) + derived_projection_bytes + retained_scratch_high_water_bytes + bounded_emission_buffer_bytes"
                .to_string(),
            input_factor_bytes,
            provider_set_key_map_bytes,
            factor_edge_count,
            factor_owner_count,
            base_estimated_model_bytes,
            derived_projection_bytes: 0,
            retained_scratch_high_water_bytes: 0,
            bounded_emission_buffer_bytes,
            estimated_peak_bytes,
            max_estimated_model_bytes: options.max_estimated_model_bytes,
            max_factor_edges: options.max_factor_edges,
        },
    })
}

#[derive(Debug)]
struct GraphModel {
    set_base: u32,
    set_components: Vec<Vec<u32>>,
    component_groups: Vec<Vec<u32>>,
    component_sets: Vec<Vec<u32>>,
    group_components: Vec<Vec<u32>>,
    group_npis: Vec<Vec<u32>>,
    npi_groups: Vec<Vec<u32>>,
    group_globals: Vec<GlobalId>,
    component_globals: Vec<GlobalId>,
    npis: Vec<u64>,
    provider_set_audit_npis: Vec<(u32, u32, u64)>,
    set_npi_prefix_overrides: Vec<Vec<u32>>,
    provider_set_npi_prefix_override_metadata: Vec<(u32, u32, [u8; 32])>,
    group_patterns: Vec<u32>,
    pattern_groups: Vec<Vec<u32>>,
    pattern_sets: Vec<Vec<u32>>,
    pattern_digests: Vec<[u8; 32]>,
    set_patterns: Vec<Vec<u32>>,
    npi_patterns: Vec<Vec<u32>>,
    direct_edge_count: u64,
    observe: V4ObserveCounters,
}

fn dense_global_map(
    values: &[GlobalId],
    label: &str,
) -> ProviderGraphV4Result<HashMap<GlobalId, u32>> {
    if values.len() > u32::MAX as usize {
        return Err(invalid(format!("V4 {label} dictionary exceeds uint32")));
    }
    Ok(values
        .iter()
        .copied()
        .enumerate()
        .map(|(index, value)| (value, index as u32))
        .collect())
}

fn advance_build_progress(
    progress: &mut ProgressReporter<'_>,
    done: &mut u64,
    total: u64,
) -> ProviderGraphV4Result<()> {
    *done = done
        .checked_add(1)
        .ok_or(invalid("V4 model progress count overflows"))?;
    progress.periodic("build_model", *done, total, "factor_items");
    Ok(())
}

fn map_key(
    map: &HashMap<GlobalId, u32>,
    global: GlobalId,
    label: &str,
) -> ProviderGraphV4Result<u32> {
    match map.get(&global).copied() {
        Some(key) => Ok(key),
        None => Err(invalid(format!(
            "V4 {label} is absent from its dense dictionary"
        ))),
    }
}

fn estimated_u32_capacity_bytes(members: usize) -> ProviderGraphV4Result<u64> {
    invalid_conversion(
        u64::try_from(members),
        "resource_admission: vector member count exceeds uint64",
    )?
    .checked_mul(ESTIMATED_U32_CAPACITY_BYTES)
    .ok_or(invalid(
        "resource_admission: vector capacity byte count overflows",
    ))
}

fn estimated_vec_owner_bytes(owners: usize) -> ProviderGraphV4Result<u64> {
    invalid_conversion(
        u64::try_from(owners),
        "resource_admission: vector owner count exceeds uint64",
    )?
    .checked_mul(ESTIMATED_VEC_OWNER_BYTES)
    .ok_or(invalid(
        "resource_admission: vector owner byte count overflows",
    ))
}

fn checked_estimated_sum(
    values: impl IntoIterator<Item = u64>,
    message: &'static str,
) -> ProviderGraphV4Result<u64> {
    values.into_iter().try_fold(0u64, |total, value| {
        total.checked_add(value).ok_or_else(|| invalid(message))
    })
}

fn validate_factor_completeness(
    raw: &RawFactors,
    provider_sets: &ProviderSetMap,
) -> ProviderGraphV4Result<()> {
    let mut factor_sets = raw.set_components.keys().copied().collect::<Vec<_>>();
    factor_sets.sort_unstable();
    for factor_set in factor_sets {
        provider_sets.key(factor_set)?;
    }

    let mut referenced_components = Vec::new();
    for set_global in &provider_sets.globals_by_index {
        let Some(components) = raw.set_components.get(set_global) else {
            return Err(invalid(
                "V4 incomplete factor truth: authoritative provider set has no components",
            ));
        };
        if components.is_empty() {
            return Err(invalid(
                "V4 incomplete factor truth: authoritative provider set has no components",
            ));
        }
        referenced_components.extend_from_slice(components);
    }
    referenced_components.sort_unstable();
    referenced_components.dedup();
    for component in referenced_components {
        if raw
            .component_groups
            .get(&component)
            .is_none_or(Vec::is_empty)
        {
            return Err(invalid(
                "V4 incomplete factor truth: referenced component has no groups",
            ));
        }
    }
    // A referenced group is intentionally allowed to have no NPI edge.
    // TIN-only and quarantined-only source groups retain group identity while
    // contributing no exact NPI membership.
    Ok(())
}

fn sorted_union_into<'a>(lists: impl IntoIterator<Item = &'a [u32]>, scratch: &mut Vec<u32>) {
    scratch.clear();
    for list in lists {
        scratch.extend_from_slice(list);
    }
    scratch.sort_unstable();
    scratch.dedup();
}

struct OrderedNpiPrefix {
    members: Vec<u32>,
    unique_groups_visited: usize,
    source_members_visited: u64,
    source_exhausted: bool,
    group_npi_work: OnlineGroupNpiWork,
}

struct GroupNpiPhysicalLayout {
    regular_member_offsets: Vec<u64>,
    heavy_plans: Vec<Option<HeavyBitmapPlan>>,
    member_page_bytes: u64,
    locator_page_bytes: u64,
    members_per_page: u64,
    owners_per_locator_page: u64,
}

#[derive(Clone, Copy, Debug, Default)]
struct OnlineGroupNpiWork {
    relation_members: u64,
    dictionary_members: u64,
    locator_pages: u64,
    member_pages: u64,
    relation_bytes: u64,
    dictionary_bytes: u64,
    batches: u64,
}

impl OnlineGroupNpiWork {
    fn member_work(self) -> u64 {
        self.relation_members
            .saturating_add(self.dictionary_members)
    }

    fn byte_work(self) -> u64 {
        self.relation_bytes.saturating_add(self.dictionary_bytes)
    }

    fn page_work(self) -> u64 {
        self.locator_pages.saturating_add(self.member_pages)
    }

    fn round_trip_work(self) -> u64 {
        self.batches
            .saturating_add(u64::from(self.dictionary_members > 0))
    }

    fn add_dictionary_members(&mut self, member_count: usize) -> ProviderGraphV4Result<()> {
        let member_count = invalid_conversion(
            u64::try_from(member_count),
            "V4 NPI dictionary member work exceeds uint64",
        )?;
        self.dictionary_members = self
            .dictionary_members
            .checked_add(member_count)
            .ok_or(invalid("V4 NPI dictionary member work overflows"))?;
        self.dictionary_bytes = self
            .dictionary_bytes
            .checked_add(
                member_count
                    .checked_mul(ONLINE_NPI_DICTIONARY_ENTRY_BYTES)
                    .ok_or(invalid("V4 NPI dictionary byte work overflows"))?,
            )
            .ok_or(invalid("V4 NPI dictionary byte work overflows"))?;
        Ok(())
    }

    fn add_batch(&mut self, batch: OnlineGroupNpiWork) -> ProviderGraphV4Result<()> {
        self.relation_members = self
            .relation_members
            .checked_add(batch.relation_members)
            .ok_or(invalid("V4 group-to-NPI member work overflows"))?;
        self.locator_pages = self
            .locator_pages
            .checked_add(batch.locator_pages)
            .ok_or(invalid("V4 group-to-NPI locator-page work overflows"))?;
        self.member_pages = self
            .member_pages
            .checked_add(batch.member_pages)
            .ok_or(invalid("V4 group-to-NPI member-page work overflows"))?;
        self.relation_bytes = self
            .relation_bytes
            .checked_add(batch.relation_bytes)
            .ok_or(invalid("V4 group-to-NPI byte work overflows"))?;
        self.batches = self
            .batches
            .checked_add(batch.batches)
            .ok_or(invalid("V4 group-to-NPI batch work overflows"))?;
        Ok(())
    }
}

fn group_npi_physical_layout(
    group_npis: &[Vec<u32>],
    options: &ProviderGraphV4Options,
) -> ProviderGraphV4Result<GroupNpiPhysicalLayout> {
    let member_page_bytes = aligned_page_bytes(options.member_page_bytes, 4) as u64;
    let locator_page_bytes = aligned_page_bytes(options.locator_page_bytes, LOCATOR_BYTES) as u64;
    let mut regular_member_offsets = Vec::with_capacity(group_npis.len());
    let mut heavy_plans = Vec::with_capacity(group_npis.len());
    let mut regular_member_offset = 0u64;
    for (group_key, members) in group_npis.iter().enumerate() {
        let owner_key = invalid_conversion(
            u32::try_from(group_key),
            "V4 group-to-NPI owner key exceeds uint32",
        )?;
        let heavy_plan = maybe_heavy_bitmap("group_npis_exact", owner_key, members, options)?;
        regular_member_offsets.push(regular_member_offset);
        if heavy_plan.is_none() {
            regular_member_offset = regular_member_offset
                .checked_add(members.len() as u64)
                .ok_or(invalid("V4 group-to-NPI vector offset overflows"))?;
        }
        heavy_plans.push(heavy_plan);
    }
    Ok(GroupNpiPhysicalLayout {
        regular_member_offsets,
        heavy_plans,
        member_page_bytes,
        locator_page_bytes,
        members_per_page: member_page_bytes / 4,
        owners_per_locator_page: locator_page_bytes / LOCATOR_BYTES as u64,
    })
}

fn heavy_prefix_fragment_count(
    plan: HeavyBitmapPlan,
    members: &[u32],
    prefix_limit: usize,
    page_bytes: usize,
) -> ProviderGraphV4Result<u64> {
    let selected_count = members.len().min(prefix_limit);
    if selected_count == 0 {
        return Ok(0);
    }
    let fragment_content_bytes = page_bytes
        .checked_sub(HEAVY_BITMAP_FRAGMENT_HEADER_BYTES)
        .filter(|value| *value > 0)
        .ok_or(invalid(
            "V4 heavy bitmap page cannot contain its fragment frame",
        ))?;
    let selected_member = members[selected_count - 1];
    let logical_byte_offset = (HEAVY_BITMAP_HEADER_BYTES as u64)
        .checked_add(u64::from(selected_member - plan.member_base) / 8)
        .ok_or(invalid("V4 heavy bitmap prefix offset overflows"))?;
    logical_byte_offset
        .checked_div(fragment_content_bytes as u64)
        .and_then(|fragment| fragment.checked_add(1))
        .ok_or(invalid("V4 heavy bitmap prefix fragment count overflows"))
}

fn group_npi_batch_work(
    groups: &[u32],
    group_npis: &[Vec<u32>],
    physical: &GroupNpiPhysicalLayout,
    prefix_limit: usize,
    heavy_page_bytes: usize,
) -> ProviderGraphV4Result<OnlineGroupNpiWork> {
    let mut locator_pages = HashSet::new();
    let mut member_pages = HashSet::new();
    let mut heavy_member_pages = 0u64;
    let mut relation_members = 0u64;
    for group in groups {
        let group_index = *group as usize;
        let members = &group_npis[group_index];
        let selected_count = members.len().min(prefix_limit);
        relation_members = relation_members
            .checked_add(selected_count as u64)
            .ok_or(invalid("V4 group-to-NPI member work overflows"))?;
        if let Some(plan) = physical.heavy_plans[group_index] {
            heavy_member_pages = heavy_member_pages
                .checked_add(heavy_prefix_fragment_count(
                    plan,
                    members,
                    prefix_limit,
                    heavy_page_bytes,
                )?)
                .ok_or(invalid("V4 group-to-NPI member-page work overflows"))?;
            continue;
        }
        locator_pages.insert(u64::from(*group) / physical.owners_per_locator_page);
        if selected_count == 0 {
            continue;
        }
        let first_page = physical.regular_member_offsets[group_index] / physical.members_per_page;
        let last_page = (physical.regular_member_offsets[group_index] + selected_count as u64 - 1)
            / physical.members_per_page;
        member_pages.extend(first_page..=last_page);
    }
    let locator_page_count = locator_pages.len() as u64;
    let member_page_count = (member_pages.len() as u64)
        .checked_add(heavy_member_pages)
        .ok_or(invalid("V4 group-to-NPI member-page work overflows"))?;
    let relation_bytes = locator_page_count
        .checked_mul(physical.locator_page_bytes)
        .and_then(|bytes| {
            member_page_count
                .checked_mul(physical.member_page_bytes)
                .and_then(|member_bytes| bytes.checked_add(member_bytes))
        })
        .ok_or(invalid("V4 group-to-NPI byte work overflows"))?;
    Ok(OnlineGroupNpiWork {
        relation_members,
        locator_pages: locator_page_count,
        member_pages: member_page_count,
        relation_bytes,
        batches: u64::from(!groups.is_empty()),
        ..OnlineGroupNpiWork::default()
    })
}

fn ordered_npi_prefix_for_sources(
    sources: &[u32],
    source_groups: &[Vec<u32>],
    group_npis: &[Vec<u32>],
    group_npi_physical: &GroupNpiPhysicalLayout,
    options: &ProviderGraphV4Options,
    target: usize,
    group_limit: usize,
) -> ProviderGraphV4Result<OrderedNpiPrefix> {
    if target == 0 {
        return Ok(OrderedNpiPrefix {
            members: Vec::new(),
            unique_groups_visited: 0,
            source_members_visited: 0,
            source_exhausted: true,
            group_npi_work: OnlineGroupNpiWork::default(),
        });
    }
    let mut selected = Vec::with_capacity(target);
    let mut seen = HashSet::with_capacity(target);
    let mut heap = BinaryHeap::new();
    for (source_position, source) in sources.iter().copied().enumerate() {
        if let Some(group) = source_groups[source as usize].first().copied() {
            heap.push(Reverse((group, source_position, 0usize)));
        }
    }
    let mut previous_group = None;
    let mut unique_groups_visited = 0usize;
    let mut source_members_visited = 0u64;
    let mut group_npi_work = OnlineGroupNpiWork::default();
    let group_batch_size = options.online_group_npi_batch_size.min(target).max(1);
    while !heap.is_empty() && unique_groups_visited < group_limit {
        let mut group_batch = Vec::with_capacity(group_batch_size);
        while group_batch.len() < group_batch_size
            && unique_groups_visited + group_batch.len() < group_limit
        {
            let Some(Reverse((group, source_position, member_position))) = heap.pop() else {
                break;
            };
            source_members_visited = source_members_visited
                .checked_add(1)
                .ok_or(invalid("V4 NPI prefix group merge visits overflow"))?;
            let source = sources[source_position] as usize;
            let next_position = member_position + 1;
            if let Some(next_group) = source_groups[source].get(next_position).copied() {
                heap.push(Reverse((next_group, source_position, next_position)));
            }
            if previous_group == Some(group) {
                continue;
            }
            previous_group = Some(group);
            group_batch.push(group);
        }
        if group_batch.is_empty() {
            continue;
        }
        group_npi_work.add_batch(group_npi_batch_work(
            &group_batch,
            group_npis,
            group_npi_physical,
            target,
            options.member_page_bytes,
        )?)?;
        for group in group_batch {
            unique_groups_visited += 1;
            for npi in &group_npis[group as usize] {
                if seen.insert(*npi) {
                    selected.push(*npi);
                    if selected.len() >= target {
                        group_npi_work.add_dictionary_members(selected.len())?;
                        return Ok(OrderedNpiPrefix {
                            members: selected,
                            unique_groups_visited,
                            source_members_visited,
                            source_exhausted: heap.is_empty(),
                            group_npi_work,
                        });
                    }
                }
            }
        }
    }
    if heap.is_empty() {
        group_npi_work.add_dictionary_members(selected.len())?;
    }
    Ok(OrderedNpiPrefix {
        members: selected,
        unique_groups_visited,
        source_members_visited,
        source_exhausted: heap.is_empty(),
        group_npi_work,
    })
}

fn npi_prefix_digest(npi_keys: &[u32]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(NPI_PREFIX_HASH_DOMAIN);
    hasher.update((npi_keys.len() as u64).to_be_bytes());
    for npi_key in npi_keys {
        hasher.update(npi_key.to_be_bytes());
    }
    hasher.finalize().into()
}

fn nearest_rank(sorted_values: &[u64], percentile: usize) -> u64 {
    if sorted_values.is_empty() {
        return 0;
    }
    let rank = sorted_values
        .len()
        .saturating_mul(percentile)
        .saturating_add(99)
        / 100;
    sorted_values[rank.saturating_sub(1).min(sorted_values.len() - 1)]
}

struct NpiPrefixOverridePlan {
    lists: Vec<Vec<u32>>,
    metadata: Vec<(u32, u32, [u8; 32])>,
    groups_to_target: Vec<u64>,
    encoded_bytes: u64,
    group_unsafe_set_count: u64,
    physical_unsafe_set_count: u64,
    group_merge_member_visits: u64,
    worst_online_probe_merge_member_visits: u64,
    maximum_source_owner_work: u64,
    maximum_source_member_work: u64,
    maximum_source_page_work: u64,
    maximum_source_byte_work: u64,
    maximum_group_npi_member_work: u64,
    maximum_group_npi_locator_page_work: u64,
    maximum_group_npi_member_page_work: u64,
    maximum_group_npi_byte_work: u64,
    maximum_group_npi_batch_work: u64,
    worst_provider_set_key: Option<u32>,
    worst_groups_to_target: u64,
    worst_provider_set_uses_override: bool,
    worst_uses_component_fallback: bool,
    worst_prefix_member_count: u64,
    worst_prefix_member_digest: Option<[u8; 32]>,
    worst_source_work: OnlineSourceWork,
    worst_group_npi_work: OnlineGroupNpiWork,
    worst_online_provider_set_key: Option<u32>,
    worst_online_groups_to_target: u64,
    worst_online_groups_to_target_exact: bool,
    worst_online_uses_component_fallback: bool,
    worst_online_group_work_bound: u64,
    worst_online_prefix_member_count: u64,
    worst_online_prefix_member_digest: Option<[u8; 32]>,
    worst_online_source_work: OnlineSourceWork,
    worst_online_group_npi_work: OnlineGroupNpiWork,
}

#[derive(Clone, Copy, Debug, Default)]
struct OnlineSourceWork {
    owners: u64,
    members: u64,
    pages: u64,
    bytes: u64,
}

type OnlineOwnerRisk = (u64, u64, u64, u64, u64, u64, u64);

fn is_worse_owner(
    current_key: Option<u32>,
    current_risk: OnlineOwnerRisk,
    candidate_key: u32,
    candidate_risk: OnlineOwnerRisk,
) -> bool {
    candidate_risk > current_risk
        || (candidate_risk == current_risk && current_key.is_none_or(|key| candidate_key < key))
}

fn online_owner_risk(
    group_bound: u64,
    source_work: OnlineSourceWork,
    group_npi_work: OnlineGroupNpiWork,
) -> OnlineOwnerRisk {
    (
        group_npi_work.round_trip_work(),
        source_work.bytes.saturating_add(group_npi_work.byte_work()),
        source_work.pages.saturating_add(group_npi_work.page_work()),
        source_work
            .members
            .saturating_add(group_npi_work.member_work()),
        group_npi_work.batches,
        group_bound,
        source_work.owners,
    )
}

struct OnlineOwnerDiagnostic {
    key: u32,
    groups_to_target: u64,
    groups_to_target_exact: bool,
    uses_component_fallback: bool,
    group_work_bound: u64,
    source_work: OnlineSourceWork,
    group_npi_work: OnlineGroupNpiWork,
    prefix_members: Option<Vec<u32>>,
}

impl OnlineOwnerDiagnostic {
    fn risk(&self) -> OnlineOwnerRisk {
        online_owner_risk(self.group_work_bound, self.source_work, self.group_npi_work)
    }
}

fn retain_worst_online_owner(
    current: &mut Option<OnlineOwnerDiagnostic>,
    candidate: OnlineOwnerDiagnostic,
) {
    let should_replace = current.as_ref().is_none_or(|existing| {
        is_worse_owner(
            Some(existing.key),
            existing.risk(),
            candidate.key,
            candidate.risk(),
        )
    });
    if should_replace {
        *current = Some(candidate);
    }
}

fn dense_member_offsets(lists: &[Vec<u32>]) -> ProviderGraphV4Result<Vec<u64>> {
    let mut offsets = Vec::with_capacity(lists.len() + 1);
    offsets.push(0u64);
    for members in lists {
        let next = offsets
            .last()
            .copied()
            .expect("offset sentinel exists")
            .checked_add(members.len() as u64)
            .ok_or(invalid("V4 dense relation member offsets overflow"))?;
        offsets.push(next);
    }
    Ok(offsets)
}

fn pages_for_member_range(offset: u64, count: usize, members_per_page: u64) -> u64 {
    if count == 0 {
        return 0;
    }
    let first = offset / members_per_page;
    let last = (offset + count as u64 - 1) / members_per_page;
    last - first + 1
}

fn pages_for_owner_prefixes(
    owners: &[u32],
    lists: &[Vec<u32>],
    offsets: &[u64],
    prefix_limit: usize,
    members_per_page: u64,
) -> u64 {
    let mut pages = 0u64;
    let mut previous_last_page: Option<u64> = None;
    for owner in owners {
        let owner_index = *owner as usize;
        let count = lists[owner_index].len().min(prefix_limit);
        if count == 0 {
            continue;
        }
        let first_page = offsets[owner_index] / members_per_page;
        let last_page = (offsets[owner_index] + count as u64 - 1) / members_per_page;
        let new_first = previous_last_page
            .map(|previous| first_page.max(previous.saturating_add(1)))
            .unwrap_or(first_page);
        if new_first <= last_page {
            pages = pages.saturating_add(last_page - new_first + 1);
        }
        previous_last_page = Some(
            previous_last_page
                .map(|previous| previous.max(last_page))
                .unwrap_or(last_page),
        );
    }
    pages
}

fn locator_pages_for_owners(owners: &[u32], owners_per_page: u64) -> u64 {
    let mut previous = None;
    let mut pages = 0u64;
    for owner in owners {
        let page = u64::from(*owner) / owners_per_page;
        if previous != Some(page) {
            pages = pages.saturating_add(1);
            previous = Some(page);
        }
    }
    pages
}

struct SourceWorkInputs<'a> {
    set_index: usize,
    patterns: &'a [u32],
    components: &'a [u32],
    set_pattern_offsets: &'a [u64],
    set_component_offsets: &'a [u64],
    pattern_group_offsets: &'a [u64],
    component_group_offsets: &'a [u64],
    pattern_groups: &'a [Vec<u32>],
    component_groups: &'a [Vec<u32>],
}

struct NpiPrefixInputs<'a> {
    set_base: u32,
    set_components: &'a [Vec<u32>],
    component_groups: &'a [Vec<u32>],
    set_patterns: &'a [Vec<u32>],
    pattern_groups: &'a [Vec<u32>],
    group_npis: &'a [Vec<u32>],
}

fn online_source_work(
    inputs: SourceWorkInputs<'_>,
    options: &ProviderGraphV4Options,
) -> OnlineSourceWork {
    let member_page_bytes = aligned_page_bytes(options.member_page_bytes, 4);
    let locator_page_bytes = aligned_page_bytes(options.locator_page_bytes, LOCATOR_BYTES);
    let members_per_page = (member_page_bytes / 4) as u64;
    let owners_per_locator_page = (locator_page_bytes / LOCATOR_BYTES) as u64;
    let pattern_path = inputs.patterns.len() <= options.max_set_patterns_per_set;
    let (sources, source_groups, source_offsets) = if pattern_path {
        (
            inputs.patterns,
            inputs.pattern_groups,
            inputs.pattern_group_offsets,
        )
    } else {
        (
            inputs.components,
            inputs.component_groups,
            inputs.component_group_offsets,
        )
    };
    let first_pattern_count = inputs
        .patterns
        .len()
        .min(options.max_set_patterns_per_set.saturating_add(1));
    let mut locator_pages = 1u64;
    let mut member_pages = pages_for_member_range(
        inputs.set_pattern_offsets[inputs.set_index],
        first_pattern_count,
        members_per_page,
    );
    let mut owners = 1u64 + sources.len() as u64;
    let mut members = first_pattern_count as u64;
    if !pattern_path {
        locator_pages = locator_pages.saturating_add(1);
        member_pages = member_pages.saturating_add(pages_for_member_range(
            inputs.set_component_offsets[inputs.set_index],
            inputs.components.len(),
            members_per_page,
        ));
        owners = owners.saturating_add(1);
        members = members.saturating_add(inputs.components.len() as u64);
    }
    locator_pages =
        locator_pages.saturating_add(locator_pages_for_owners(sources, owners_per_locator_page));
    member_pages = member_pages.saturating_add(pages_for_owner_prefixes(
        sources,
        source_groups,
        source_offsets,
        options.max_online_group_keys_per_set.saturating_add(1),
        members_per_page,
    ));
    members = sources.iter().fold(members, |total, source| {
        total.saturating_add(
            source_groups[*source as usize]
                .len()
                .min(options.max_online_group_keys_per_set.saturating_add(1)) as u64,
        )
    });
    let pages = locator_pages.saturating_add(member_pages);
    let bytes = locator_pages
        .saturating_mul(locator_page_bytes as u64)
        .saturating_add(member_pages.saturating_mul(member_page_bytes as u64));
    OnlineSourceWork {
        owners,
        members,
        pages,
        bytes,
    }
}

fn group_npi_work_exceeds_limits(
    work: OnlineGroupNpiWork,
    options: &ProviderGraphV4Options,
) -> bool {
    work.member_work() > options.max_online_group_npi_members_per_set as u64
        || work.locator_pages > options.max_online_group_npi_locator_pages_per_set as u64
        || work.member_pages > options.max_online_group_npi_member_pages_per_set as u64
        || work.byte_work() > options.max_online_group_npi_bytes_per_set
        || work.batches > options.max_online_group_npi_batches_per_set as u64
}

fn derive_npi_prefix_overrides(
    inputs: NpiPrefixInputs<'_>,
    options: &ProviderGraphV4Options,
    admission: &mut ResourceAdmissionTracker,
) -> ProviderGraphV4Result<NpiPrefixOverridePlan> {
    let NpiPrefixInputs {
        set_base,
        set_components,
        component_groups,
        set_patterns,
        pattern_groups,
        group_npis,
    } = inputs;
    let online_cap = options.max_online_group_keys_per_set;
    let target = options.npi_prefix_target;
    let mut lists = vec![Vec::new(); set_components.len()];
    let mut metadata = Vec::new();
    let mut groups_to_target = Vec::new();
    let mut total_members = 0usize;
    let mut group_unsafe_set_count = 0u64;
    let mut physical_unsafe_set_count = 0u64;
    let mut group_merge_member_visits = 0u64;
    let mut maximum_source_owner_work = 0u64;
    let mut maximum_source_member_work = 0u64;
    let mut maximum_source_page_work = 0u64;
    let mut maximum_source_byte_work = 0u64;
    let mut maximum_group_npi_member_work = 0u64;
    let mut maximum_group_npi_locator_page_work = 0u64;
    let mut maximum_group_npi_member_page_work = 0u64;
    let mut maximum_group_npi_byte_work = 0u64;
    let mut maximum_group_npi_batch_work = 0u64;
    let mut worst_provider_set_key = None;
    let mut worst_groups_to_target = 0u64;
    let mut worst_provider_set_uses_override = false;
    let mut worst_uses_component_fallback = false;
    let mut worst_prefix_members = Vec::new();
    let mut worst_source_work = OnlineSourceWork::default();
    let mut worst_group_npi_work = OnlineGroupNpiWork::default();
    let mut worst_online_owner = None;
    let set_pattern_offsets = dense_member_offsets(set_patterns)?;
    let set_component_offsets = dense_member_offsets(set_components)?;
    let pattern_group_offsets = dense_member_offsets(pattern_groups)?;
    let component_group_offsets = dense_member_offsets(component_groups)?;
    let group_npi_physical = group_npi_physical_layout(group_npis, options)?;
    for (set_index, (components, patterns)) in set_components.iter().zip(set_patterns).enumerate() {
        let exact_group_degree = patterns.iter().try_fold(0usize, |total, pattern| {
            total
                .checked_add(pattern_groups[*pattern as usize].len())
                .ok_or(invalid("V4 exact set/group degree overflows"))
        })?;
        let group_unsafe = exact_group_degree > online_cap;
        group_unsafe_set_count = group_unsafe_set_count.saturating_add(u64::from(group_unsafe));
        let source_work = online_source_work(
            SourceWorkInputs {
                set_index,
                patterns,
                components,
                set_pattern_offsets: &set_pattern_offsets,
                set_component_offsets: &set_component_offsets,
                pattern_group_offsets: &pattern_group_offsets,
                component_group_offsets: &component_group_offsets,
                pattern_groups,
                component_groups,
            },
            options,
        );
        let owner_key = set_base
            .checked_add(set_index as u32)
            .ok_or(invalid("V4 NPI prefix override owner key overflows"))?;
        let uses_component_fallback = patterns.len() > options.max_set_patterns_per_set;
        let (sources, source_groups) = if patterns.len() <= options.max_set_patterns_per_set {
            (patterns.as_slice(), pattern_groups)
        } else {
            (components.as_slice(), component_groups)
        };
        admission.reserve_scratch_bytes(
            "NPI prefix simulation heap and uniqueness set",
            (sources.len() as u64)
                .saturating_mul(64)
                .saturating_add((target as u64).saturating_mul(32)),
        )?;
        let initial_bounded = ordered_npi_prefix_for_sources(
            sources,
            source_groups,
            group_npis,
            &group_npi_physical,
            options,
            target,
            online_cap,
        )?;
        group_merge_member_visits = group_merge_member_visits
            .checked_add(initial_bounded.source_members_visited)
            .ok_or(invalid("V4 NPI prefix group merge visits overflow"))?;
        let mut exact_discovery = None;
        let effective_target = if initial_bounded.members.len() >= target {
            target
        } else if initial_bounded.source_exhausted {
            initial_bounded.members.len()
        } else {
            let exact = ordered_npi_prefix_for_sources(
                sources,
                source_groups,
                group_npis,
                &group_npi_physical,
                options,
                target,
                usize::MAX,
            )?;
            group_merge_member_visits = group_merge_member_visits
                .checked_add(exact.source_members_visited)
                .ok_or(invalid("V4 NPI prefix group merge visits overflow"))?;
            let discovered_target = if exact.source_exhausted {
                exact.members.len()
            } else {
                target
            };
            exact_discovery = Some(exact);
            discovered_target
        };
        let bounded = if effective_target == target {
            initial_bounded
        } else {
            let adjusted = ordered_npi_prefix_for_sources(
                sources,
                source_groups,
                group_npis,
                &group_npi_physical,
                options,
                effective_target,
                online_cap,
            )?;
            group_merge_member_visits = group_merge_member_visits
                .checked_add(adjusted.source_members_visited)
                .ok_or(invalid("V4 NPI prefix group merge visits overflow"))?;
            adjusted
        };
        if group_unsafe && effective_target > 0 && bounded.source_exhausted {
            return Err(invalid(
                "V4 factored set/group degree disagrees with merged source groups",
            ));
        }
        let source_work = if effective_target == 0 {
            OnlineSourceWork::default()
        } else {
            source_work
        };
        maximum_source_owner_work = maximum_source_owner_work.max(source_work.owners);
        maximum_source_member_work = maximum_source_member_work.max(source_work.members);
        maximum_source_page_work = maximum_source_page_work.max(source_work.pages);
        maximum_source_byte_work = maximum_source_byte_work.max(source_work.bytes);
        let source_physical_unsafe = source_work.owners
            > options.max_online_source_owners_per_set as u64
            || source_work.members > options.max_online_source_members_per_set as u64
            || source_work.pages > options.max_online_source_pages_per_set as u64
            || source_work.bytes > options.max_online_source_bytes_per_set;
        maximum_group_npi_member_work =
            maximum_group_npi_member_work.max(bounded.group_npi_work.member_work());
        maximum_group_npi_locator_page_work =
            maximum_group_npi_locator_page_work.max(bounded.group_npi_work.locator_pages);
        maximum_group_npi_member_page_work =
            maximum_group_npi_member_page_work.max(bounded.group_npi_work.member_pages);
        maximum_group_npi_byte_work =
            maximum_group_npi_byte_work.max(bounded.group_npi_work.byte_work());
        maximum_group_npi_batch_work =
            maximum_group_npi_batch_work.max(bounded.group_npi_work.batches);
        let physical_unsafe = source_physical_unsafe
            || group_npi_work_exceeds_limits(bounded.group_npi_work, options);
        physical_unsafe_set_count =
            physical_unsafe_set_count.saturating_add(u64::from(physical_unsafe));
        let bounded_complete =
            bounded.members.len() >= effective_target || bounded.source_exhausted;
        if !physical_unsafe && bounded_complete {
            let visited_groups = bounded.unique_groups_visited as u64;
            groups_to_target.push(visited_groups);
            if is_worse_owner(
                worst_provider_set_key,
                online_owner_risk(
                    worst_groups_to_target,
                    worst_source_work,
                    worst_group_npi_work,
                ),
                owner_key,
                online_owner_risk(visited_groups, source_work, bounded.group_npi_work),
            ) {
                worst_provider_set_key = Some(owner_key);
                worst_groups_to_target = visited_groups;
                worst_provider_set_uses_override = false;
                worst_uses_component_fallback = uses_component_fallback;
                worst_prefix_members = bounded.members.clone();
                worst_source_work = source_work;
                worst_group_npi_work = bounded.group_npi_work;
            }
            retain_worst_online_owner(
                &mut worst_online_owner,
                OnlineOwnerDiagnostic {
                    key: owner_key,
                    groups_to_target: visited_groups,
                    groups_to_target_exact: true,
                    uses_component_fallback,
                    group_work_bound: exact_group_degree.min(online_cap) as u64,
                    source_work,
                    group_npi_work: bounded.group_npi_work,
                    prefix_members: Some(bounded.members.clone()),
                },
            );
            continue;
        }
        let exact = if bounded_complete {
            bounded
        } else if effective_target == target && exact_discovery.is_some() {
            exact_discovery.expect("exact discovery checked above")
        } else {
            let result = ordered_npi_prefix_for_sources(
                sources,
                source_groups,
                group_npis,
                &group_npi_physical,
                options,
                effective_target,
                usize::MAX,
            )?;
            group_merge_member_visits = group_merge_member_visits
                .checked_add(result.source_members_visited)
                .ok_or(invalid("V4 NPI prefix group merge visits overflow"))?;
            result
        };
        let visited_groups = exact.unique_groups_visited as u64;
        groups_to_target.push(visited_groups);
        maximum_group_npi_member_work =
            maximum_group_npi_member_work.max(exact.group_npi_work.member_work());
        maximum_group_npi_locator_page_work =
            maximum_group_npi_locator_page_work.max(exact.group_npi_work.locator_pages);
        maximum_group_npi_member_page_work =
            maximum_group_npi_member_page_work.max(exact.group_npi_work.member_pages);
        maximum_group_npi_byte_work =
            maximum_group_npi_byte_work.max(exact.group_npi_work.byte_work());
        maximum_group_npi_batch_work =
            maximum_group_npi_batch_work.max(exact.group_npi_work.batches);
        if is_worse_owner(
            worst_provider_set_key,
            online_owner_risk(
                worst_groups_to_target,
                worst_source_work,
                worst_group_npi_work,
            ),
            owner_key,
            online_owner_risk(visited_groups, source_work, exact.group_npi_work),
        ) {
            worst_provider_set_key = Some(owner_key);
            worst_groups_to_target = visited_groups;
            worst_provider_set_uses_override = true;
            worst_uses_component_fallback = uses_component_fallback;
            worst_prefix_members = exact.members.clone();
            worst_source_work = source_work;
            worst_group_npi_work = exact.group_npi_work;
        }
        total_members = total_members
            .checked_add(exact.members.len())
            .ok_or(invalid("V4 NPI prefix override member count overflows"))?;
        if metadata.len() >= options.max_npi_prefix_override_owners {
            return Err(invalid(
                "V4 NPI prefix override owner count exceeds configured maximum",
            ));
        }
        let raw_bytes = (total_members as u64)
            .checked_mul(4)
            .ok_or(invalid("V4 NPI prefix override byte count overflows"))?;
        if raw_bytes > options.max_npi_prefix_override_bytes {
            return Err(invalid(
                "V4 NPI prefix override bytes exceed configured maximum",
            ));
        }
        let member_count = invalid_conversion(
            u32::try_from(exact.members.len()),
            "V4 NPI prefix override member count exceeds uint32",
        )?;
        metadata.push((owner_key, member_count, npi_prefix_digest(&exact.members)));
        lists[set_index] = exact.members;
    }
    let worst_online_probe_merge_member_visits = 0u64;
    let worst_prefix_member_digest =
        worst_provider_set_key.map(|_| npi_prefix_digest(&worst_prefix_members));
    let worst_prefix_member_count = worst_prefix_members.len() as u64;
    let worst_online_prefix_members = worst_online_owner
        .as_ref()
        .and_then(|diagnostic| diagnostic.prefix_members.as_ref());
    let worst_online_prefix_member_digest =
        worst_online_prefix_members.map(|members| npi_prefix_digest(members));
    let worst_online_prefix_member_count =
        worst_online_prefix_members.map_or(0, |members| members.len() as u64);
    admission.reserve_projection(
        "sparse NPI prefix overrides",
        checked_estimated_sum(
            [
                estimated_vec_owner_bytes(lists.len())?,
                estimated_u32_capacity_bytes(total_members)?,
                estimated_vec_owner_bytes(metadata.len())?,
                (groups_to_target.len() as u64).saturating_mul(16),
                estimated_u32_capacity_bytes(worst_prefix_members.len())?,
                estimated_u32_capacity_bytes(worst_online_prefix_members.map_or(0, Vec::len))?,
            ],
            "resource_admission: NPI prefix override bytes overflow",
        )?,
    )?;
    let encoded_bytes = relation_encoded_bytes(
        &RelationShape {
            relation: "set_npi_prefix_override",
            owner_count: lists.len(),
            member_count: total_members as u64,
        },
        options,
    )?;
    let (
        worst_online_provider_set_key,
        worst_online_groups_to_target,
        worst_online_groups_to_target_exact,
        worst_online_uses_component_fallback,
        worst_online_group_work_bound,
        worst_online_source_work,
        worst_online_group_npi_work,
    ) = match worst_online_owner {
        Some(diagnostic) => (
            Some(diagnostic.key),
            diagnostic.groups_to_target,
            diagnostic.groups_to_target_exact,
            diagnostic.uses_component_fallback,
            diagnostic.group_work_bound,
            diagnostic.source_work,
            diagnostic.group_npi_work,
        ),
        None => (
            None,
            0,
            false,
            false,
            0,
            OnlineSourceWork::default(),
            OnlineGroupNpiWork::default(),
        ),
    };
    Ok(NpiPrefixOverridePlan {
        lists,
        metadata,
        groups_to_target,
        encoded_bytes,
        group_unsafe_set_count,
        physical_unsafe_set_count,
        group_merge_member_visits,
        maximum_source_owner_work,
        maximum_source_member_work,
        maximum_source_page_work,
        maximum_source_byte_work,
        maximum_group_npi_member_work,
        maximum_group_npi_locator_page_work,
        maximum_group_npi_member_page_work,
        maximum_group_npi_byte_work,
        maximum_group_npi_batch_work,
        worst_provider_set_key,
        worst_groups_to_target,
        worst_provider_set_uses_override,
        worst_uses_component_fallback,
        worst_prefix_member_count,
        worst_prefix_member_digest,
        worst_source_work,
        worst_group_npi_work,
        worst_online_provider_set_key,
        worst_online_groups_to_target,
        worst_online_groups_to_target_exact,
        worst_online_uses_component_fallback,
        worst_online_group_work_bound,
        worst_online_prefix_member_count,
        worst_online_prefix_member_digest,
        worst_online_source_work,
        worst_online_group_npi_work,
        worst_online_probe_merge_member_visits,
    })
}

fn pattern_digest(set_keys: &[u32]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(PATTERN_HASH_DOMAIN);
    hasher.update((set_keys.len() as u64).to_be_bytes());
    for key in set_keys {
        hasher.update(key.to_be_bytes());
    }
    hasher.finalize().into()
}

fn build_graph_model(
    raw: &RawFactors,
    provider_sets: &ProviderSetMap,
    progress: &mut ProgressReporter<'_>,
    admission: &mut ResourceAdmissionTracker,
    options: &ProviderGraphV4Options,
) -> ProviderGraphV4Result<GraphModel> {
    validate_factor_completeness(raw, provider_sets)?;
    let set_component_edges = raw
        .set_components
        .values()
        .try_fold(0u64, |total, members| {
            total.checked_add(members.len() as u64)
        })
        .ok_or(invalid("V4 set/component work count overflows"))?;
    let component_group_edges = raw
        .component_groups
        .values()
        .try_fold(0u64, |total, members| {
            total.checked_add(members.len() as u64)
        })
        .ok_or(invalid("V4 component/group work count overflows"))?;
    let group_npi_edges = raw
        .group_npis
        .values()
        .try_fold(0u64, |total, members| {
            total.checked_add(members.len() as u64)
        })
        .ok_or(invalid("V4 group/NPI work count overflows"))?;
    let build_total = [
        raw.component_groups.len() as u64,
        set_component_edges,
        component_group_edges,
        raw.group_npis.len() as u64,
        group_npi_edges,
        raw.set_components.len() as u64,
        set_component_edges,
        raw.component_groups.len() as u64,
        component_group_edges,
        provider_sets.globals_by_index.len() as u64,
        set_component_edges,
        component_group_edges,
        raw.group_npis.len() as u64,
        group_npi_edges,
        group_npi_edges,
        component_group_edges,
        provider_sets.globals_by_index.len() as u64,
        set_component_edges,
    ]
    .into_iter()
    .try_fold(0u64, |total, count| total.checked_add(count))
    .ok_or(invalid("V4 model progress total overflows"))?;
    let mut build_done = 0u64;
    progress.periodic("build_model", 0, build_total, "factor_items");

    let mut component_globals = Vec::new();
    for component in raw.component_groups.keys().copied() {
        component_globals.push(component);
        advance_build_progress(progress, &mut build_done, build_total)?;
    }
    for members in raw.set_components.values() {
        for component in members {
            component_globals.push(*component);
            advance_build_progress(progress, &mut build_done, build_total)?;
        }
    }
    component_globals.sort_unstable();
    component_globals.dedup();

    let mut group_globals = Vec::new();
    for members in raw.component_groups.values() {
        for group in members {
            group_globals.push(*group);
            advance_build_progress(progress, &mut build_done, build_total)?;
        }
    }
    for group in raw.group_npis.keys().copied() {
        group_globals.push(group);
        advance_build_progress(progress, &mut build_done, build_total)?;
    }
    group_globals.sort_unstable();
    group_globals.dedup();

    let mut npi_globals = Vec::new();
    for members in raw.group_npis.values() {
        for npi in members {
            npi_globals.push(*npi);
            advance_build_progress(progress, &mut build_done, build_total)?;
        }
    }
    npi_globals.sort_unstable();
    npi_globals.dedup();
    let npis = npi_globals
        .iter()
        .copied()
        .map(npi_from_global_id)
        .collect::<ProviderGraphV4Result<Vec<_>>>()?;
    let component_map = dense_global_map(&component_globals, "component")?;
    let group_map = dense_global_map(&group_globals, "group")?;
    let npi_map = dense_global_map(&npi_globals, "NPI")?;

    let mut set_components = vec![Vec::new(); provider_sets.globals_by_index.len()];
    for (set_global, component_globals_for_set) in &raw.set_components {
        let set_key = provider_sets.key(*set_global)?;
        let set_index = provider_sets.index(set_key)?;
        let members = &mut set_components[set_index];
        for component in component_globals_for_set {
            members.push(map_key(&component_map, *component, "component")?);
            advance_build_progress(progress, &mut build_done, build_total)?;
        }
        members.sort_unstable();
        members.dedup();
        advance_build_progress(progress, &mut build_done, build_total)?;
    }
    let mut component_groups = vec![Vec::new(); component_globals.len()];
    for (component_global, group_globals_for_component) in &raw.component_groups {
        let component = map_key(&component_map, *component_global, "component")? as usize;
        for group in group_globals_for_component {
            component_groups[component].push(map_key(&group_map, *group, "group")?);
            advance_build_progress(progress, &mut build_done, build_total)?;
        }
        component_groups[component].sort_unstable();
        component_groups[component].dedup();
        advance_build_progress(progress, &mut build_done, build_total)?;
    }
    let mut component_sets = vec![Vec::new(); component_globals.len()];
    for (set_index, components) in set_components.iter().enumerate() {
        let set_key = provider_sets
            .key_base
            .checked_add(set_index as u32)
            .ok_or(invalid("V4 provider-set key overflows"))?;
        for component in components {
            component_sets[*component as usize].push(set_key);
            advance_build_progress(progress, &mut build_done, build_total)?;
        }
        advance_build_progress(progress, &mut build_done, build_total)?;
    }
    let mut group_components = vec![Vec::new(); group_globals.len()];
    for (component, groups) in component_groups.iter().enumerate() {
        for group in groups {
            group_components[*group as usize].push(component as u32);
            advance_build_progress(progress, &mut build_done, build_total)?;
        }
    }
    for components in &mut group_components {
        components.sort_unstable();
        components.dedup();
    }
    let mut group_npis = vec![Vec::new(); group_globals.len()];
    for (group_global, npi_globals_for_group) in &raw.group_npis {
        let group = map_key(&group_map, *group_global, "group")? as usize;
        for npi in npi_globals_for_group {
            group_npis[group].push(map_key(&npi_map, *npi, "NPI")?);
            advance_build_progress(progress, &mut build_done, build_total)?;
        }
        group_npis[group].sort_unstable();
        group_npis[group].dedup();
        advance_build_progress(progress, &mut build_done, build_total)?;
    }
    let mut npi_groups = vec![Vec::new(); npis.len()];
    for (group, members) in group_npis.iter().enumerate() {
        for npi in members {
            npi_groups[*npi as usize].push(group as u32);
            advance_build_progress(progress, &mut build_done, build_total)?;
        }
    }
    let mut component_audit_npis = Vec::with_capacity(component_groups.len());
    for groups in &component_groups {
        let mut audit_npi: Option<(u64, u32)> = None;
        for group in groups {
            if let Some(npi) = group_npis[*group as usize].first() {
                let candidate = (npis[*npi as usize], *group);
                audit_npi = Some(audit_npi.map_or(candidate, |current| current.min(candidate)));
            }
            advance_build_progress(progress, &mut build_done, build_total)?;
        }
        component_audit_npis.push(audit_npi);
    }
    let mut provider_set_audit_npis = Vec::new();
    for (set_index, components) in set_components.iter().enumerate() {
        let mut audit_npi: Option<(u64, u32)> = None;
        for component in components {
            if let Some(candidate) = component_audit_npis[*component as usize] {
                audit_npi = Some(audit_npi.map_or(candidate, |current| current.min(candidate)));
            }
            advance_build_progress(progress, &mut build_done, build_total)?;
        }
        if let Some((npi, group)) = audit_npi {
            let set_key = provider_sets
                .key_base
                .checked_add(set_index as u32)
                .ok_or(invalid("V4 provider-set audit key overflows"))?;
            provider_set_audit_npis.push((set_key, group, npi));
        }
        advance_build_progress(progress, &mut build_done, build_total)?;
    }
    if build_done != build_total {
        return Err(invalid("V4 model progress count differs from factor work"));
    }

    let mut scratch = Vec::new();
    let mut digest_candidates: HashMap<[u8; 32], Vec<u32>> = HashMap::new();
    admission.reserve_projection(
        "component-to-pattern memo",
        checked_estimated_sum(
            [
                ESTIMATED_VEC_OWNER_BYTES,
                estimated_u32_capacity_bytes(component_sets.len())?,
            ],
            "resource_admission: component-pattern memo bytes overflow",
        )?,
    )?;
    let mut component_pattern = vec![None; component_sets.len()];
    let mut component_tuple_pattern: HashMap<Vec<u32>, u32> = HashMap::new();
    let mut component_tuple_pattern_cache_member_count = 0u64;
    admission.reserve_projection(
        "group-to-pattern owner mapping",
        checked_estimated_sum(
            [
                ESTIMATED_VEC_OWNER_BYTES,
                estimated_u32_capacity_bytes(group_globals.len())?,
            ],
            "resource_admission: group-pattern projection byte count overflows",
        )?,
    )?;
    let mut group_patterns = Vec::with_capacity(group_globals.len());
    let mut pattern_sets: Vec<Vec<u32>> = Vec::new();
    let mut pattern_digests = Vec::new();
    let mut direct_edge_count = 0u64;
    let mut maximum_sets_per_group = 0u64;
    let mut empty_incidence_group_count = 0u64;
    let mut group_set_expansion_owner_visits = 0u64;
    let mut group_set_expansion_edge_visits = 0u64;
    let mut single_component_group_fast_path_count = 0u64;
    let mut multi_component_group_union_count = 0u64;
    progress.periodic(
        "derive_patterns",
        0,
        group_components.len() as u64,
        "groups",
    );
    for (group_index, components) in group_components.iter().enumerate() {
        group_set_expansion_owner_visits = group_set_expansion_owner_visits
            .checked_add(1)
            .ok_or(invalid("V4 group/set owner traversal count overflows"))?;
        let cached_pattern = if let [component] = components.as_slice() {
            single_component_group_fast_path_count = single_component_group_fast_path_count
                .checked_add(1)
                .ok_or(invalid("V4 single-component fast-path count overflows"))?;
            component_pattern[*component as usize]
        } else {
            component_tuple_pattern.get(components.as_slice()).copied()
        };
        let pattern = if let Some(pattern) = cached_pattern {
            pattern
        } else {
            let incidence = if let [component] = components.as_slice() {
                component_sets[*component as usize].as_slice()
            } else {
                if components.len() > 1 {
                    multi_component_group_union_count = multi_component_group_union_count
                        .checked_add(1)
                        .ok_or(invalid("V4 multi-component union count overflows"))?;
                }
                let scratch_members = components.iter().try_fold(0usize, |total, component| {
                    total
                        .checked_add(component_sets[*component as usize].len())
                        .ok_or(invalid(
                            "resource_admission: group/set union scratch count overflows",
                        ))
                })?;
                admission.reserve_scratch_members("group/set incidence union", scratch_members)?;
                sorted_union_into(
                    components
                        .iter()
                        .map(|component| component_sets[*component as usize].as_slice()),
                    &mut scratch,
                );
                scratch.as_slice()
            };
            group_set_expansion_edge_visits = group_set_expansion_edge_visits
                .checked_add(incidence.len() as u64)
                .ok_or(invalid("V4 group/set edge traversal count overflows"))?;
            let digest = pattern_digest(incidence);
            let matched = digest_candidates.get(&digest).and_then(|candidates| {
                candidates
                    .iter()
                    .copied()
                    .find(|candidate| pattern_sets[*candidate as usize] == incidence)
            });
            let pattern = if let Some(matched_pattern) = matched {
                matched_pattern
            } else {
                let new_pattern = invalid_conversion(
                    u32::try_from(pattern_sets.len()),
                    "V4 pattern dictionary exceeds uint32",
                )?;
                admission.reserve_projection(
                    "a distinct pattern incidence",
                    checked_estimated_sum(
                        [
                            ESTIMATED_VEC_OWNER_BYTES,
                            estimated_u32_capacity_bytes(incidence.len())?,
                            ESTIMATED_PATTERN_INDEX_BYTES,
                        ],
                        "resource_admission: distinct pattern byte count overflows",
                    )?,
                )?;
                pattern_sets.push(incidence.to_vec());
                pattern_digests.push(digest);
                digest_candidates
                    .entry(digest)
                    .or_default()
                    .push(new_pattern);
                new_pattern
            };
            if let [component] = components.as_slice() {
                component_pattern[*component as usize] = Some(pattern);
            } else {
                admission.reserve_projection(
                    "a multi-component tuple-to-pattern memo",
                    ESTIMATED_PATTERN_INDEX_BYTES
                        .checked_add(estimated_u32_capacity_bytes(components.len())?)
                        .ok_or(invalid(
                            "resource_admission: component-tuple memo bytes overflow",
                        ))?,
                )?;
                component_tuple_pattern_cache_member_count =
                    component_tuple_pattern_cache_member_count
                        .checked_add(components.len() as u64)
                        .ok_or(invalid("V4 component-tuple memo member count overflows"))?;
                component_tuple_pattern.insert(components.clone(), pattern);
            }
            pattern
        };
        let incidence_count = pattern_sets[pattern as usize].len() as u64;
        direct_edge_count = direct_edge_count
            .checked_add(incidence_count)
            .ok_or(invalid("V4 group/set incidence count overflows"))?;
        maximum_sets_per_group = maximum_sets_per_group.max(incidence_count);
        empty_incidence_group_count += u64::from(pattern_sets[pattern as usize].is_empty());
        group_patterns.push(pattern);
        progress.periodic(
            "derive_patterns",
            group_index as u64 + 1,
            group_components.len() as u64,
            "groups",
        );
    }
    admission.reserve_projection(
        "pattern-to-group projection",
        checked_estimated_sum(
            [
                estimated_vec_owner_bytes(pattern_sets.len())?,
                estimated_u32_capacity_bytes(group_patterns.len())?,
            ],
            "resource_admission: pattern-group projection byte count overflows",
        )?,
    )?;
    let mut pattern_groups = vec![Vec::new(); pattern_sets.len()];
    for (group, pattern) in group_patterns.iter().copied().enumerate() {
        pattern_groups[pattern as usize].push(group as u32);
    }
    let pattern_set_members = pattern_sets.iter().try_fold(0usize, |total, sets| {
        total.checked_add(sets.len()).ok_or(invalid(
            "resource_admission: pattern/set projection count overflows",
        ))
    })?;
    admission.reserve_projection(
        "set-to-pattern projection",
        checked_estimated_sum(
            [
                estimated_vec_owner_bytes(provider_sets.globals_by_index.len())?,
                estimated_u32_capacity_bytes(pattern_set_members)?,
            ],
            "resource_admission: set-pattern projection byte count overflows",
        )?,
    )?;
    let mut set_patterns = vec![Vec::new(); provider_sets.globals_by_index.len()];
    for (pattern, sets) in pattern_sets.iter().enumerate() {
        for set in sets {
            let index = provider_sets.index(*set)?;
            set_patterns[index].push(pattern as u32);
        }
    }
    let npi_pattern_member_upper_bound = npi_groups.iter().try_fold(0usize, |total, groups| {
        total.checked_add(groups.len()).ok_or(invalid(
            "resource_admission: NPI-pattern projection count overflows",
        ))
    })?;
    admission.reserve_projection(
        "NPI-to-pattern projection upper bound",
        checked_estimated_sum(
            [
                estimated_vec_owner_bytes(npi_groups.len())?,
                estimated_u32_capacity_bytes(npi_pattern_member_upper_bound)?,
            ],
            "resource_admission: NPI-pattern projection byte count overflows",
        )?,
    )?;
    let mut npi_patterns = Vec::with_capacity(npi_groups.len());
    let mut maximum_patterns_per_npi = 0u64;
    let mut npi_pattern_edge_count = 0u64;
    progress.periodic("derive_npi_patterns", 0, npi_groups.len() as u64, "npis");
    for (npi_index, groups) in npi_groups.iter().enumerate() {
        admission.reserve_scratch_members("NPI-pattern union", groups.len())?;
        scratch.clear();
        scratch.extend(groups.iter().map(|group| group_patterns[*group as usize]));
        scratch.sort_unstable();
        scratch.dedup();
        maximum_patterns_per_npi = maximum_patterns_per_npi.max(scratch.len() as u64);
        npi_pattern_edge_count = npi_pattern_edge_count.saturating_add(scratch.len() as u64);
        npi_patterns.push(scratch.clone());
        progress.periodic(
            "derive_npi_patterns",
            npi_index as u64 + 1,
            npi_groups.len() as u64,
            "npis",
        );
    }
    drop(scratch);
    admission.reserve_scratch_bytes(
        "NPI-pattern degree percentile sample",
        (npi_patterns.len() as u64).saturating_mul(16),
    )?;
    let mut npi_pattern_degrees = npi_patterns
        .iter()
        .map(|patterns| patterns.len() as u64)
        .collect::<Vec<_>>();
    npi_pattern_degrees.sort_unstable();
    let npi_patterns_per_npi_p50 = nearest_rank(&npi_pattern_degrees, 50);
    let npi_patterns_per_npi_p95 = nearest_rank(&npi_pattern_degrees, 95);
    let npi_patterns_per_npi_p99 = nearest_rank(&npi_pattern_degrees, 99);
    drop(npi_pattern_degrees);

    let mut npi_prefix_override_plan = derive_npi_prefix_overrides(
        NpiPrefixInputs {
            set_base: provider_sets.key_base,
            set_components: &set_components,
            component_groups: &component_groups,
            set_patterns: &set_patterns,
            pattern_groups: &pattern_groups,
            group_npis: &group_npis,
        },
        options,
        admission,
    )?;
    npi_prefix_override_plan.groups_to_target.sort_unstable();
    let npi_prefix_override_member_count = npi_prefix_override_plan
        .metadata
        .iter()
        .map(|(_, member_count, _)| u64::from(*member_count))
        .sum::<u64>();
    let npi_prefix_override_raw_bytes = npi_prefix_override_member_count
        .checked_mul(4)
        .ok_or(invalid("V4 NPI prefix override raw bytes overflow"))?;

    let set_component_edge_count = set_components.iter().map(Vec::len).sum::<usize>() as u64;
    let component_group_edge_count = component_groups.iter().map(Vec::len).sum::<usize>() as u64;
    let group_npi_edge_count = group_npis.iter().map(Vec::len).sum::<usize>() as u64;
    let group_component_edge_count = group_components.iter().map(Vec::len).sum::<usize>() as u64;
    let pattern_set_edge_count = pattern_sets.iter().map(Vec::len).sum::<usize>() as u64;
    let set_pattern_edge_count = set_patterns.iter().map(Vec::len).sum::<usize>() as u64;
    let maximum_groups_per_npi = npi_groups.iter().map(Vec::len).max().unwrap_or(0) as u64;
    let maximum_patterns_per_set = set_patterns.iter().map(Vec::len).max().unwrap_or(0) as u64;
    let maximum_components_per_set = set_components.iter().map(Vec::len).max().unwrap_or(0) as u64;
    let maximum_components_per_group =
        group_components.iter().map(Vec::len).max().unwrap_or(0) as u64;
    let multi_component_group_count = group_components
        .iter()
        .filter(|components| components.len() > 1)
        .count() as u64;
    let observe = V4ObserveCounters {
        component_count: component_globals.len() as u64,
        group_count: group_globals.len() as u64,
        provider_set_count: provider_sets.globals_by_index.len() as u64,
        provider_set_audit_npi_count: provider_set_audit_npis.len() as u64,
        npi_count: npis.len() as u64,
        set_component_edge_count,
        component_group_edge_count,
        group_npi_edge_count,
        group_component_edge_count,
        group_set_incidence_count: direct_edge_count,
        pattern_count: pattern_sets.len() as u64,
        pattern_set_edge_count,
        set_pattern_edge_count,
        npi_pattern_edge_count,
        multi_component_group_count,
        maximum_components_per_group,
        maximum_sets_per_group,
        maximum_groups_per_set: 0,
        maximum_groups_per_npi,
        maximum_patterns_per_npi,
        npi_patterns_per_npi_p50,
        npi_patterns_per_npi_p95,
        npi_patterns_per_npi_p99,
        maximum_patterns_per_set,
        maximum_components_per_set,
        pattern_overflow_set_count: 0,
        maximum_components_per_pattern_overflow_set: 0,
        unsafe_pattern_component_set_count: 0,
        npi_prefix_group_unsafe_set_count: npi_prefix_override_plan.group_unsafe_set_count,
        npi_prefix_physical_unsafe_set_count: npi_prefix_override_plan.physical_unsafe_set_count,
        npi_prefix_simulated_set_count: npi_prefix_override_plan.groups_to_target.len() as u64,
        npi_prefix_group_merge_member_visits: npi_prefix_override_plan.group_merge_member_visits,
        npi_prefix_worst_online_probe_merge_member_visits: npi_prefix_override_plan
            .worst_online_probe_merge_member_visits,
        npi_prefix_override_owner_count: npi_prefix_override_plan.metadata.len() as u64,
        npi_prefix_override_member_count,
        npi_prefix_override_raw_bytes,
        npi_prefix_override_encoded_bytes: npi_prefix_override_plan.encoded_bytes,
        npi_prefix_groups_to_target_p50: nearest_rank(
            &npi_prefix_override_plan.groups_to_target,
            50,
        ),
        npi_prefix_groups_to_target_p95: nearest_rank(
            &npi_prefix_override_plan.groups_to_target,
            95,
        ),
        npi_prefix_groups_to_target_p99: nearest_rank(
            &npi_prefix_override_plan.groups_to_target,
            99,
        ),
        npi_prefix_groups_to_target_max: npi_prefix_override_plan
            .groups_to_target
            .last()
            .copied()
            .unwrap_or(0),
        npi_prefix_worst_provider_set_key: npi_prefix_override_plan.worst_provider_set_key,
        npi_prefix_worst_groups_to_target: npi_prefix_override_plan.worst_groups_to_target,
        npi_prefix_worst_provider_set_uses_override: npi_prefix_override_plan
            .worst_provider_set_uses_override,
        npi_prefix_worst_uses_component_fallback: npi_prefix_override_plan
            .worst_uses_component_fallback,
        npi_prefix_worst_member_count: npi_prefix_override_plan.worst_prefix_member_count,
        npi_prefix_worst_member_digest: npi_prefix_override_plan
            .worst_prefix_member_digest
            .map(|digest| hex(&digest)),
        npi_prefix_worst_source_owner_work: npi_prefix_override_plan.worst_source_work.owners,
        npi_prefix_worst_source_member_work: npi_prefix_override_plan.worst_source_work.members,
        npi_prefix_worst_source_page_work: npi_prefix_override_plan.worst_source_work.pages,
        npi_prefix_worst_source_byte_work: npi_prefix_override_plan.worst_source_work.bytes,
        npi_prefix_worst_group_npi_member_work: npi_prefix_override_plan
            .worst_group_npi_work
            .member_work(),
        npi_prefix_worst_group_npi_locator_page_work: npi_prefix_override_plan
            .worst_group_npi_work
            .locator_pages,
        npi_prefix_worst_group_npi_member_page_work: npi_prefix_override_plan
            .worst_group_npi_work
            .member_pages,
        npi_prefix_worst_group_npi_byte_work: npi_prefix_override_plan
            .worst_group_npi_work
            .byte_work(),
        npi_prefix_worst_group_npi_batch_work: npi_prefix_override_plan
            .worst_group_npi_work
            .batches,
        npi_prefix_worst_online_provider_set_key: npi_prefix_override_plan
            .worst_online_provider_set_key,
        npi_prefix_worst_online_groups_to_target: npi_prefix_override_plan
            .worst_online_groups_to_target,
        npi_prefix_worst_online_groups_to_target_exact: npi_prefix_override_plan
            .worst_online_groups_to_target_exact,
        npi_prefix_worst_online_uses_component_fallback: npi_prefix_override_plan
            .worst_online_uses_component_fallback,
        npi_prefix_worst_online_group_work_bound: npi_prefix_override_plan
            .worst_online_group_work_bound,
        npi_prefix_worst_online_member_count: npi_prefix_override_plan
            .worst_online_prefix_member_count,
        npi_prefix_worst_online_member_digest: npi_prefix_override_plan
            .worst_online_prefix_member_digest
            .map(|digest| hex(&digest)),
        npi_prefix_worst_online_source_owner_work: npi_prefix_override_plan
            .worst_online_source_work
            .owners,
        npi_prefix_worst_online_source_member_work: npi_prefix_override_plan
            .worst_online_source_work
            .members,
        npi_prefix_worst_online_source_page_work: npi_prefix_override_plan
            .worst_online_source_work
            .pages,
        npi_prefix_worst_online_source_byte_work: npi_prefix_override_plan
            .worst_online_source_work
            .bytes,
        npi_prefix_worst_online_group_npi_member_work: npi_prefix_override_plan
            .worst_online_group_npi_work
            .member_work(),
        npi_prefix_worst_online_group_npi_locator_page_work: npi_prefix_override_plan
            .worst_online_group_npi_work
            .locator_pages,
        npi_prefix_worst_online_group_npi_member_page_work: npi_prefix_override_plan
            .worst_online_group_npi_work
            .member_pages,
        npi_prefix_worst_online_group_npi_byte_work: npi_prefix_override_plan
            .worst_online_group_npi_work
            .byte_work(),
        npi_prefix_worst_online_group_npi_batch_work: npi_prefix_override_plan
            .worst_online_group_npi_work
            .batches,
        maximum_online_source_owner_work: npi_prefix_override_plan.maximum_source_owner_work,
        maximum_online_source_member_work: npi_prefix_override_plan.maximum_source_member_work,
        maximum_online_source_page_work: npi_prefix_override_plan.maximum_source_page_work,
        maximum_online_source_byte_work: npi_prefix_override_plan.maximum_source_byte_work,
        maximum_online_group_npi_member_work: npi_prefix_override_plan
            .maximum_group_npi_member_work,
        maximum_online_group_npi_locator_page_work: npi_prefix_override_plan
            .maximum_group_npi_locator_page_work,
        maximum_online_group_npi_member_page_work: npi_prefix_override_plan
            .maximum_group_npi_member_page_work,
        maximum_online_group_npi_byte_work: npi_prefix_override_plan.maximum_group_npi_byte_work,
        maximum_online_group_npi_batch_work: npi_prefix_override_plan.maximum_group_npi_batch_work,
        empty_incidence_group_count,
        maximum_groups_per_set_computed: 0,
        group_set_expansion_owner_visits,
        group_set_expansion_edge_visits,
        direct_group_set_emission_owner_visits: 0,
        direct_group_set_emission_edge_visits: 0,
        set_group_expansion_owner_visits: 0,
        set_group_expansion_edge_visits: 0,
        single_component_group_fast_path_count,
        multi_component_group_union_count,
        component_tuple_pattern_cache_owner_count: component_tuple_pattern.len() as u64,
        component_tuple_pattern_cache_member_count,
    };
    Ok(GraphModel {
        set_base: provider_sets.key_base,
        set_components,
        component_groups,
        component_sets,
        group_components,
        group_npis,
        npi_groups,
        group_globals,
        component_globals,
        npis,
        provider_set_audit_npis,
        set_npi_prefix_overrides: npi_prefix_override_plan.lists,
        provider_set_npi_prefix_override_metadata: npi_prefix_override_plan.metadata,
        group_patterns,
        pattern_groups,
        pattern_sets,
        pattern_digests,
        set_patterns,
        npi_patterns,
        direct_edge_count,
        observe,
    })
}

#[derive(Clone, Debug)]
struct RelationShape {
    relation: &'static str,
    owner_count: usize,
    member_count: u64,
}

fn member_kind(relation: &str) -> String {
    format!("v4_{relation}_members_v1")
}

fn locator_kind(relation: &str) -> String {
    format!("v4_{relation}_locators_v1")
}

fn heavy_bitmap_kind(relation: &str) -> String {
    format!("v4_{relation}_heavy_bitmap_v1")
}

fn aligned_page_bytes(page_bytes: usize, width: usize) -> usize {
    (page_bytes / width) * width
}

fn pg_copy_row_bytes(object_kind: &str, payload_bytes: usize) -> ProviderGraphV4Result<u64> {
    let fields = [
        32usize,
        2,
        object_kind.len(),
        8,
        4,
        8,
        4, // codec = "none"
        8,
        8,
        payload_bytes,
    ];
    fields.iter().try_fold(2u64, |total, field| {
        total
            .checked_add(4)
            .and_then(|value| value.checked_add(*field as u64))
            .ok_or(invalid("V4 PostgreSQL COPY encoded size overflows"))
    })
}

fn paged_encoded_bytes(
    object_kind: &str,
    total_bytes: u64,
    page_bytes: usize,
) -> ProviderGraphV4Result<u64> {
    if total_bytes == 0 {
        return Ok(0);
    }
    let full_pages = total_bytes / page_bytes as u64;
    let remainder = (total_bytes % page_bytes as u64) as usize;
    let full_row = pg_copy_row_bytes(object_kind, page_bytes)?;
    let mut total = full_pages
        .checked_mul(full_row)
        .ok_or(invalid("V4 paged encoded byte count overflows"))?;
    if remainder > 0 {
        total = total
            .checked_add(pg_copy_row_bytes(object_kind, remainder)?)
            .ok_or(invalid("V4 paged encoded byte count overflows"))?;
    }
    Ok(total)
}

fn relation_encoded_bytes(
    shape: &RelationShape,
    options: &ProviderGraphV4Options,
) -> ProviderGraphV4Result<u64> {
    let member_page_bytes = aligned_page_bytes(options.member_page_bytes, 4);
    let locator_page_bytes = aligned_page_bytes(options.locator_page_bytes, LOCATOR_BYTES);
    let member_bytes = shape
        .member_count
        .checked_mul(4)
        .ok_or(invalid("V4 relation member byte count overflows"))?;
    let locator_bytes = (shape.owner_count as u64)
        .checked_mul(LOCATOR_BYTES as u64)
        .ok_or(invalid("V4 relation locator byte count overflows"))?;
    paged_encoded_bytes(
        &member_kind(shape.relation),
        member_bytes,
        member_page_bytes,
    )?
    .checked_add(paged_encoded_bytes(
        &locator_kind(shape.relation),
        locator_bytes,
        locator_page_bytes,
    )?)
    .ok_or(invalid("V4 relation encoded byte count overflows"))
}

fn dictionary_copy_bytes(
    row_field_widths: &[usize],
    row_count: usize,
) -> ProviderGraphV4Result<u64> {
    let row_bytes = row_field_widths.iter().try_fold(2u64, |total, width| {
        total
            .checked_add(4)
            .and_then(|value| value.checked_add(*width as u64))
            .ok_or(invalid("V4 dictionary COPY row size overflows"))
    })?;
    (PG_COPY_HEADER.len() as u64)
        .checked_add(2)
        .and_then(|value| value.checked_add(row_bytes.checked_mul(row_count as u64)?))
        .ok_or(invalid("V4 dictionary COPY encoded size overflows"))
}

#[derive(Clone, Copy, Debug)]
struct HeavyBitmapPlan {
    relation: &'static str,
    owner_key: u32,
    member_count: u64,
    member_base: u32,
    member_span: u64,
    logical_byte_count: u64,
    raw_byte_count: u64,
    vector_byte_count: u64,
}

fn maybe_heavy_bitmap(
    relation: &'static str,
    owner_key: u32,
    members: &[u32],
    options: &ProviderGraphV4Options,
) -> ProviderGraphV4Result<Option<HeavyBitmapPlan>> {
    if members.len() < options.heavy_owner_member_threshold || members.is_empty() {
        return Ok(None);
    }
    let Some(fragment_content_bytes) = options
        .member_page_bytes
        .checked_sub(HEAVY_BITMAP_FRAGMENT_HEADER_BYTES)
        .filter(|value| *value > 0)
    else {
        return Ok(None);
    };
    let minimum = members[0];
    let maximum = *members.last().expect("non-empty members");
    let span = u64::from(maximum)
        .checked_sub(u64::from(minimum))
        .and_then(|value| value.checked_add(1))
        .ok_or(invalid("V4 heavy bitmap span overflows"))?;
    let bitmap_bytes = span
        .checked_add(7)
        .and_then(|value| value.checked_div(8))
        .ok_or(invalid("V4 heavy bitmap byte count overflows"))?;
    let logical_byte_count = (HEAVY_BITMAP_HEADER_BYTES as u64)
        .checked_add(bitmap_bytes)
        .ok_or(invalid("V4 heavy bitmap payload size overflows"))?;
    let fragment_count = logical_byte_count
        .checked_add(fragment_content_bytes as u64 - 1)
        .and_then(|value| value.checked_div(fragment_content_bytes as u64))
        .ok_or(invalid("V4 heavy bitmap fragment count overflows"))?;
    let payload_bytes = logical_byte_count
        .checked_add(
            fragment_count
                .checked_mul(HEAVY_BITMAP_FRAGMENT_HEADER_BYTES as u64)
                .ok_or(invalid("V4 heavy bitmap framing size overflows"))?,
        )
        .ok_or(invalid("V4 heavy bitmap payload size overflows"))?;
    let vector_bytes = (members.len() as u64)
        .checked_mul(4)
        .ok_or(invalid("V4 heavy vector size overflows"))?;
    let bitmap_encoded_bytes = paged_encoded_bytes(
        &heavy_bitmap_kind(relation),
        payload_bytes,
        options.member_page_bytes,
    )?;
    if bitmap_encoded_bytes
        .checked_add(options.heavy_bitmap_minimum_savings_bytes as u64)
        .is_none_or(|required| required > vector_bytes)
    {
        return Ok(None);
    }
    invalid_conversion(u32::try_from(span), "V4 heavy bitmap span exceeds uint32")?;
    invalid_conversion(
        u32::try_from(members.len()),
        "V4 heavy bitmap member count exceeds uint32",
    )?;
    Ok(Some(HeavyBitmapPlan {
        relation,
        owner_key,
        member_count: members.len() as u64,
        member_base: minimum,
        member_span: span,
        logical_byte_count,
        raw_byte_count: payload_bytes,
        vector_byte_count: vector_bytes,
    }))
}

#[derive(Debug)]
struct LayoutSizes {
    common: u64,
    direct: u64,
    pattern: u64,
}

fn compute_layout_sizes(
    model: &GraphModel,
    options: &ProviderGraphV4Options,
) -> ProviderGraphV4Result<LayoutSizes> {
    let common_shapes = [
        RelationShape {
            relation: "set_components",
            owner_count: model.set_components.len(),
            member_count: model.observe.set_component_edge_count,
        },
        RelationShape {
            relation: "component_groups",
            owner_count: model.component_groups.len(),
            member_count: model.observe.component_group_edge_count,
        },
        RelationShape {
            relation: "npi_groups_exact",
            owner_count: model.npi_groups.len(),
            member_count: model.observe.group_npi_edge_count,
        },
        RelationShape {
            relation: "group_npis_exact",
            owner_count: model.group_npis.len(),
            member_count: model.observe.group_npi_edge_count,
        },
        RelationShape {
            relation: "set_npi_prefix_override",
            owner_count: model.set_npi_prefix_overrides.len(),
            member_count: model.observe.npi_prefix_override_member_count,
        },
    ];
    let common_relations = common_shapes.iter().try_fold(0u64, |total, shape| {
        total
            .checked_add(relation_encoded_bytes(shape, options)?)
            .ok_or(invalid("V4 common encoded byte count overflows"))
    })?;
    let common_dictionaries = dictionary_copy_bytes(&[4, 16], model.group_globals.len())?
        .checked_add(dictionary_copy_bytes(
            &[4, 16],
            model.component_globals.len(),
        )?)
        .and_then(|value| {
            value.checked_add(
                dictionary_copy_bytes(&[4, 8], model.npis.len())
                    .expect("NPI dictionary size was validated"),
            )
        })
        .and_then(|value| {
            value.checked_add(
                dictionary_copy_bytes(&[4, 4, 8], model.provider_set_audit_npis.len())
                    .expect("provider-set audit dictionary size was validated"),
            )
        })
        .and_then(|value| {
            value.checked_add(
                dictionary_copy_bytes(
                    &[4, 4, 32],
                    model.provider_set_npi_prefix_override_metadata.len(),
                )
                .expect("NPI prefix override metadata size was validated"),
            )
        })
        .ok_or(invalid("V4 common dictionary byte count overflows"))?;

    let direct_shapes = [
        RelationShape {
            relation: "group_sets_direct",
            owner_count: model.group_globals.len(),
            member_count: model.direct_edge_count,
        },
        RelationShape {
            relation: "set_groups_direct",
            owner_count: model.set_components.len(),
            member_count: model.direct_edge_count,
        },
    ];
    let pattern_shapes = [
        RelationShape {
            relation: "group_patterns",
            owner_count: model.group_globals.len(),
            member_count: model.group_patterns.len() as u64,
        },
        RelationShape {
            relation: "pattern_groups",
            owner_count: model.pattern_groups.len(),
            member_count: model.group_patterns.len() as u64,
        },
        RelationShape {
            relation: "pattern_sets",
            owner_count: model.pattern_sets.len(),
            member_count: model.observe.pattern_set_edge_count,
        },
        RelationShape {
            relation: "set_patterns",
            owner_count: model.set_patterns.len(),
            member_count: model.observe.set_pattern_edge_count,
        },
        RelationShape {
            relation: "npi_patterns",
            owner_count: model.npi_patterns.len(),
            member_count: model.observe.npi_pattern_edge_count,
        },
    ];
    let direct_projection = direct_shapes.iter().try_fold(0u64, |total, shape| {
        total
            .checked_add(relation_encoded_bytes(shape, options)?)
            .ok_or(invalid("V4 direct encoded byte count overflows"))
    })?;
    let pattern_projection = pattern_shapes.iter().try_fold(0u64, |total, shape| {
        total
            .checked_add(relation_encoded_bytes(shape, options)?)
            .ok_or(invalid("V4 pattern encoded byte count overflows"))
    })?;
    let common = common_relations
        .checked_add(common_dictionaries)
        .and_then(|value| value.checked_add(PG_COPY_HEADER.len() as u64 + 2))
        .ok_or(invalid("V4 common encoded byte count overflows"))?;
    let direct = common
        .checked_add(direct_projection)
        .ok_or(invalid("V4 direct complete byte count overflows"))?;
    let pattern_dictionary = dictionary_copy_bytes(&[4, 32, 8], model.pattern_sets.len())?;
    let pattern = common
        .checked_add(pattern_projection)
        .and_then(|value| value.checked_add(pattern_dictionary))
        .ok_or(invalid("V4 pattern complete byte count overflows"))?;
    Ok(LayoutSizes {
        common,
        direct,
        pattern,
    })
}

fn record_pattern_fallback_diagnostics(
    model: &GraphModel,
    options: &ProviderGraphV4Options,
    observe: &mut V4ObserveCounters,
) -> bool {
    debug_assert_eq!(model.set_patterns.len(), model.set_components.len());
    let mut overflow_set_count = 0u64;
    let mut maximum_components_per_overflow_set = 0u64;
    let mut unsafe_set_count = 0u64;
    for (patterns, components) in model.set_patterns.iter().zip(&model.set_components) {
        if patterns.len() <= options.max_set_patterns_per_set {
            continue;
        }
        overflow_set_count = overflow_set_count.saturating_add(1);
        maximum_components_per_overflow_set =
            maximum_components_per_overflow_set.max(components.len() as u64);
        if components.len() > options.max_set_components_per_fallback_set {
            unsafe_set_count = unsafe_set_count.saturating_add(1);
        }
    }
    observe.pattern_overflow_set_count = overflow_set_count;
    observe.maximum_components_per_pattern_overflow_set = maximum_components_per_overflow_set;
    observe.unsafe_pattern_component_set_count = unsafe_set_count;
    unsafe_set_count == 0
}

fn choose_layout(
    direct_bytes: u64,
    pattern_bytes: u64,
    pattern_layout_serving_degree_eligible: bool,
) -> ProviderGraphV4Layout {
    if pattern_bytes < direct_bytes && pattern_layout_serving_degree_eligible {
        ProviderGraphV4Layout::Pattern
    } else {
        ProviderGraphV4Layout::Direct
    }
}

#[derive(Clone, Debug, Serialize)]
struct BlockReference {
    object_kind: String,
    block_key: i64,
    fragment_no: i32,
    entry_count: u64,
    raw_byte_count: u64,
    stored_byte_count: u64,
    codec: &'static str,
    hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    owner_base: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    owner_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    member_offset: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    owner_key: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    member_base: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    member_span: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default)]
struct BlockCoordinateMetadata {
    owner_base: Option<u32>,
    owner_count: Option<u32>,
    member_offset: Option<u64>,
    owner_key: Option<u32>,
    member_base: Option<u32>,
    member_span: Option<u64>,
}

struct ReferenceSpool {
    writer: BufWriter<tempfile::NamedTempFile>,
    last_coordinate: Option<(i64, i32)>,
}

struct CasBlockWriter {
    copy: BufWriter<File>,
    references: BufWriter<File>,
    reference_spools: BTreeMap<String, ReferenceSpool>,
    reference_spool_directory: PathBuf,
    reference_encode_buffer: Vec<u8>,
    block_count: u64,
    copy_path: PathBuf,
    finished: bool,
}

impl CasBlockWriter {
    fn create(copy_path: &Path, reference_path: &Path) -> ProviderGraphV4Result<Self> {
        let mut copy = BufWriter::new(File::create(copy_path)?);
        copy.write_all(PG_COPY_HEADER)?;
        let reference_spool_directory = reference_path
            .parent()
            .ok_or(invalid("V4 reference manifest has no parent directory"))?
            .to_path_buf();
        Ok(Self {
            copy,
            references: BufWriter::new(File::create(reference_path)?),
            reference_spools: BTreeMap::new(),
            reference_spool_directory,
            reference_encode_buffer: Vec::with_capacity(REFERENCE_ENCODE_BUFFER_BYTES as usize),
            block_count: 0,
            copy_path: copy_path.to_path_buf(),
            finished: false,
        })
    }

    fn write_block(
        &mut self,
        object_kind: &str,
        block_key: i64,
        fragment_no: i32,
        entry_count: u64,
        payload: &[u8],
        metadata: BlockCoordinateMetadata,
    ) -> ProviderGraphV4Result<()> {
        self.ensure_reference_coordinate(object_kind, block_key, fragment_no)?;
        let block_hash = shared_block_hash(object_kind, "none", payload)?;
        let raw_bytes = payload.len() as u64;
        let block_key_bytes = block_key.to_be_bytes();
        let fragment_bytes = fragment_no.to_be_bytes();
        let entry_bytes = invalid_conversion(
            i64::try_from(entry_count),
            "V4 block entry count exceeds PostgreSQL bigint",
        )?
        .to_be_bytes();
        let raw_byte_field = invalid_conversion(
            i64::try_from(raw_bytes),
            "V4 block byte count exceeds PostgreSQL bigint",
        )?
        .to_be_bytes();
        self.write_copy_row(&[
            &block_hash,
            &SHARED_FORMAT_VERSION.to_be_bytes(),
            object_kind.as_bytes(),
            &block_key_bytes,
            &fragment_bytes,
            &entry_bytes,
            b"none",
            &raw_byte_field,
            &raw_byte_field,
            payload,
        ])?;
        let reference = BlockReference {
            object_kind: object_kind.to_owned(),
            block_key,
            fragment_no,
            entry_count,
            raw_byte_count: raw_bytes,
            stored_byte_count: raw_bytes,
            codec: "none",
            hash: hex(&block_hash),
            owner_base: metadata.owner_base,
            owner_count: metadata.owner_count,
            member_offset: metadata.member_offset,
            owner_key: metadata.owner_key,
            member_base: metadata.member_base,
            member_span: metadata.member_span,
        };
        self.spool_reference(reference)?;
        self.block_count = self.block_count.saturating_add(1);
        Ok(())
    }

    fn ensure_reference_coordinate(
        &self,
        object_kind: &str,
        block_key: i64,
        fragment_no: i32,
    ) -> ProviderGraphV4Result<()> {
        if self
            .reference_spools
            .get(object_kind)
            .and_then(|spool| spool.last_coordinate)
            .is_some_and(|previous| (block_key, fragment_no) <= previous)
        {
            return Err(invalid(
                "V4 CAS output repeats or reorders a block coordinate",
            ));
        }
        Ok(())
    }

    fn spool_reference(&mut self, reference: BlockReference) -> ProviderGraphV4Result<()> {
        let coordinate = (reference.block_key, reference.fragment_no);
        if !self.reference_spools.contains_key(&reference.object_kind) {
            if self.reference_spools.len() >= MAX_REFERENCE_OBJECT_KINDS {
                return Err(invalid(
                    "V4 CAS output exceeds the bounded reference object-kind count",
                ));
            }
            let temporary = tempfile::Builder::new()
                .prefix(".v4-reference-spool-")
                .tempfile_in(&self.reference_spool_directory)?;
            self.reference_spools.insert(
                reference.object_kind.clone(),
                ReferenceSpool {
                    writer: BufWriter::with_capacity(
                        REFERENCE_SPOOL_WRITER_BYTES as usize,
                        temporary,
                    ),
                    last_coordinate: None,
                },
            );
        }
        let spool = self
            .reference_spools
            .get_mut(&reference.object_kind)
            .expect("reference spool was inserted");
        debug_assert!(spool
            .last_coordinate
            .is_none_or(|previous| coordinate > previous));
        self.reference_encode_buffer.clear();
        serde_json::to_writer(&mut self.reference_encode_buffer, &reference)?;
        self.reference_encode_buffer.push(b'\n');
        if self.reference_encode_buffer.len() > REFERENCE_ENCODE_BUFFER_BYTES as usize {
            return Err(invalid(
                "V4 CAS reference row exceeds its bounded encode buffer",
            ));
        }
        spool.writer.write_all(&self.reference_encode_buffer)?;
        spool.last_coordinate = Some(coordinate);
        Ok(())
    }

    fn write_copy_row(&mut self, fields: &[&[u8]]) -> ProviderGraphV4Result<()> {
        let field_count = invalid_conversion(
            i16::try_from(fields.len()),
            "V4 PostgreSQL COPY row has too many fields",
        )?;
        self.copy.write_all(&field_count.to_be_bytes())?;
        for field in fields {
            let field_size = invalid_conversion(
                i32::try_from(field.len()),
                "V4 PostgreSQL COPY field exceeds int32",
            )?;
            self.copy.write_all(&field_size.to_be_bytes())?;
            self.copy.write_all(field)?;
        }
        Ok(())
    }

    fn finish(mut self) -> ProviderGraphV4Result<(u64, u64)> {
        self.copy.write_all(&(-1i16).to_be_bytes())?;
        self.copy.flush()?;
        for spool in self.reference_spools.values_mut() {
            spool.writer.flush()?;
            spool.writer.get_mut().seek(SeekFrom::Start(0))?;
            io::copy(spool.writer.get_mut(), &mut self.references)?;
        }
        self.references.flush()?;
        self.finished = true;
        let byte_count = fs::metadata(&self.copy_path)?.len();
        Ok((self.block_count, byte_count))
    }
}

impl Drop for CasBlockWriter {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.copy.flush();
            let _ = self.references.flush();
        }
    }
}

struct RelationEmitter<'a> {
    relation: &'static str,
    member_object_kind: String,
    locator_object_kind: String,
    owner_base: u32,
    expected_owner_count: usize,
    member_page_bytes: usize,
    locator_page_bytes: usize,
    locator_owner_span: u32,
    cas: &'a mut CasBlockWriter,
    member_payload: Vec<u8>,
    locator_payload: Vec<u8>,
    next_owner_index: usize,
    logical_member_count: u64,
    vector_member_count: u64,
    member_block_count: u64,
    locator_block_count: u64,
    current_member_page_offset: u64,
    current_locator_page_base: u32,
}

impl<'a> RelationEmitter<'a> {
    fn new(
        relation: &'static str,
        owner_base: u32,
        owner_count: usize,
        cas: &'a mut CasBlockWriter,
        options: &ProviderGraphV4Options,
    ) -> ProviderGraphV4Result<Self> {
        let member_page_bytes = aligned_page_bytes(options.member_page_bytes, 4);
        let locator_page_bytes = aligned_page_bytes(options.locator_page_bytes, LOCATOR_BYTES);
        if member_page_bytes == 0 || locator_page_bytes == 0 {
            return Err(invalid("V4 relation page alignment produced an empty page"));
        }
        Ok(Self {
            relation,
            member_object_kind: member_kind(relation),
            locator_object_kind: locator_kind(relation),
            owner_base,
            expected_owner_count: owner_count,
            member_page_bytes,
            locator_page_bytes,
            locator_owner_span: (locator_page_bytes / LOCATOR_BYTES) as u32,
            cas,
            member_payload: Vec::with_capacity(member_page_bytes),
            locator_payload: Vec::with_capacity(locator_page_bytes),
            next_owner_index: 0,
            logical_member_count: 0,
            vector_member_count: 0,
            member_block_count: 0,
            locator_block_count: 0,
            current_member_page_offset: 0,
            current_locator_page_base: owner_base,
        })
    }

    fn push_owner(
        &mut self,
        members: &[u32],
        replace_with_bitmap: bool,
    ) -> ProviderGraphV4Result<()> {
        if members.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(invalid(format!(
                "V4 {} members must be sorted and unique",
                self.relation
            )));
        }
        self.push_owner_unchecked_order(members, replace_with_bitmap)
    }

    fn push_ordered_owner(&mut self, members: &[u32]) -> ProviderGraphV4Result<()> {
        let unique_count = members.iter().copied().collect::<HashSet<_>>().len();
        if unique_count != members.len() {
            return Err(invalid(format!(
                "V4 {} ordered members must be unique",
                self.relation
            )));
        }
        self.push_owner_unchecked_order(members, false)
    }

    fn push_owner_unchecked_order(
        &mut self,
        members: &[u32],
        replace_with_bitmap: bool,
    ) -> ProviderGraphV4Result<()> {
        if self.next_owner_index >= self.expected_owner_count {
            return Err(invalid(format!(
                "V4 {} received more owners than declared",
                self.relation
            )));
        }
        self.logical_member_count = self
            .logical_member_count
            .checked_add(members.len() as u64)
            .ok_or(invalid("V4 logical relation member count overflows"))?;
        let offset = self.vector_member_count;
        if !replace_with_bitmap {
            for member in members {
                if self.member_payload.len() + 4 > self.member_page_bytes {
                    self.flush_member_page()?;
                }
                self.member_payload.extend_from_slice(&member.to_le_bytes());
                self.vector_member_count = self
                    .vector_member_count
                    .checked_add(1)
                    .ok_or(invalid("V4 vector relation member count overflows"))?;
            }
        }
        if self.locator_payload.len() + LOCATOR_BYTES > self.locator_page_bytes {
            self.flush_locator_page()?;
        }
        self.locator_payload
            .extend_from_slice(&offset.to_le_bytes());
        self.locator_payload.extend_from_slice(
            &invalid_conversion(
                u32::try_from(if replace_with_bitmap {
                    0
                } else {
                    members.len()
                }),
                "V4 one-owner member count exceeds uint32",
            )?
            .to_le_bytes(),
        );
        self.next_owner_index += 1;
        Ok(())
    }

    fn emit_heavy_bitmap_streamed(
        &mut self,
        plan: HeavyBitmapPlan,
        members: &[u32],
        page_bytes: usize,
    ) -> ProviderGraphV4Result<V4HeavyBitmapSummary> {
        if plan.relation != self.relation
            || plan.member_count != members.len() as u64
            || members.first().copied() != Some(plan.member_base)
        {
            return Err(invalid(
                "V4 heavy bitmap plan differs from its relation owner",
            ));
        }
        let mut header = [0u8; HEAVY_BITMAP_HEADER_BYTES];
        header[..8].copy_from_slice(b"PTG2V4BM");
        header[8..12].copy_from_slice(&plan.owner_key.to_le_bytes());
        header[12..16].copy_from_slice(&plan.member_base.to_le_bytes());
        header[16..20].copy_from_slice(
            &invalid_conversion(
                u32::try_from(plan.member_span),
                "V4 heavy bitmap span exceeds uint32",
            )?
            .to_le_bytes(),
        );
        header[20..24].copy_from_slice(
            &invalid_conversion(
                u32::try_from(plan.member_count),
                "V4 heavy bitmap member count exceeds uint32",
            )?
            .to_le_bytes(),
        );

        let logical_byte_count = invalid_conversion(
            usize::try_from(plan.logical_byte_count),
            "V4 heavy bitmap exceeds addressable output",
        )?;
        let fragment_content_bytes = page_bytes
            .checked_sub(HEAVY_BITMAP_FRAGMENT_HEADER_BYTES)
            .filter(|value| *value > 0)
            .ok_or(invalid(
                "V4 heavy bitmap page cannot contain its fragment frame",
            ))?;
        let object_kind = heavy_bitmap_kind(plan.relation);
        let mut payload = Vec::with_capacity(page_bytes);
        let mut logical_offset = 0usize;
        let mut physical_raw_byte_count = 0u64;
        let mut fragment = 0usize;
        let mut member_index = 0usize;
        while logical_offset < logical_byte_count {
            let logical_end = logical_offset
                .checked_add(fragment_content_bytes)
                .map(|end| end.min(logical_byte_count))
                .ok_or(invalid("V4 heavy bitmap fragment boundary overflows"))?;
            payload.clear();
            payload.resize(
                HEAVY_BITMAP_FRAGMENT_HEADER_BYTES + logical_end - logical_offset,
                0,
            );

            let header_start = logical_offset.min(HEAVY_BITMAP_HEADER_BYTES);
            let header_end = logical_end.min(HEAVY_BITMAP_HEADER_BYTES);
            if header_start < header_end {
                payload[HEAVY_BITMAP_FRAGMENT_HEADER_BYTES
                    ..HEAVY_BITMAP_FRAGMENT_HEADER_BYTES + header_end - header_start]
                    .copy_from_slice(&header[header_start..header_end]);
            }

            let mut fragment_member_count = 0u64;
            while let Some(member) = members.get(member_index).copied() {
                let relative = u64::from(member - plan.member_base);
                let raw_byte_offset = (HEAVY_BITMAP_HEADER_BYTES as u64)
                    .checked_add(relative / 8)
                    .ok_or(invalid("V4 heavy bitmap member offset overflows"))?;
                let raw_byte_offset = invalid_conversion(
                    usize::try_from(raw_byte_offset),
                    "V4 heavy bitmap member offset exceeds addressable output",
                )?;
                if raw_byte_offset >= logical_end {
                    break;
                }
                if raw_byte_offset < logical_offset {
                    return Err(invalid("V4 heavy bitmap members are not sorted and unique"));
                }
                payload[HEAVY_BITMAP_FRAGMENT_HEADER_BYTES + raw_byte_offset - logical_offset] |=
                    1 << (relative % 8);
                fragment_member_count = fragment_member_count
                    .checked_add(1)
                    .ok_or(invalid("V4 heavy bitmap fragment count overflows"))?;
                member_index += 1;
            }
            payload[..8].copy_from_slice(HEAVY_BITMAP_FRAGMENT_MAGIC);
            payload[8..12].copy_from_slice(&plan.owner_key.to_le_bytes());
            payload[12..16].copy_from_slice(&plan.member_base.to_le_bytes());
            payload[16..20].copy_from_slice(
                &invalid_conversion(
                    u32::try_from(plan.member_span),
                    "V4 heavy bitmap span exceeds uint32",
                )?
                .to_le_bytes(),
            );
            payload[20..24].copy_from_slice(
                &invalid_conversion(
                    u32::try_from(plan.member_count),
                    "V4 heavy bitmap member count exceeds uint32",
                )?
                .to_le_bytes(),
            );
            payload[24..28].copy_from_slice(
                &invalid_conversion(
                    u32::try_from(fragment),
                    "V4 heavy bitmap fragment exceeds uint32",
                )?
                .to_le_bytes(),
            );
            payload[28..32].copy_from_slice(
                &invalid_conversion(
                    u32::try_from(fragment_member_count),
                    "V4 heavy bitmap fragment member count exceeds uint32",
                )?
                .to_le_bytes(),
            );

            self.cas.write_block(
                &object_kind,
                i64::from(plan.owner_key),
                invalid_conversion(
                    i32::try_from(fragment),
                    "V4 heavy bitmap fragment exceeds int32",
                )?,
                fragment_member_count,
                &payload,
                BlockCoordinateMetadata {
                    owner_key: Some(plan.owner_key),
                    member_base: Some(plan.member_base),
                    member_span: Some(plan.member_span),
                    ..BlockCoordinateMetadata::default()
                },
            )?;
            physical_raw_byte_count = physical_raw_byte_count
                .checked_add(payload.len() as u64)
                .ok_or(invalid("V4 heavy bitmap physical byte count overflows"))?;
            logical_offset = logical_end;
            fragment = fragment
                .checked_add(1)
                .ok_or(invalid("V4 heavy bitmap fragment count overflows"))?;
        }
        if member_index != members.len() {
            return Err(invalid(
                "V4 heavy bitmap did not consume every sorted member",
            ));
        }
        if physical_raw_byte_count != plan.raw_byte_count {
            return Err(invalid(
                "V4 heavy bitmap physical framing differs from its plan",
            ));
        }
        let block_count = invalid_conversion(
            u64::try_from(fragment),
            "V4 heavy bitmap block count exceeds uint64",
        )?;
        Ok(V4HeavyBitmapSummary {
            relation: plan.relation.to_owned(),
            object_kind,
            owner_key: plan.owner_key,
            member_count: plan.member_count,
            member_base: plan.member_base,
            member_span: plan.member_span,
            raw_byte_count: plan.raw_byte_count,
            vector_byte_count: plan.vector_byte_count,
            saved_decode_bytes: plan.vector_byte_count.saturating_sub(plan.raw_byte_count),
            block_count,
        })
    }

    fn flush_member_page(&mut self) -> ProviderGraphV4Result<()> {
        if self.member_payload.is_empty() {
            return Ok(());
        }
        let entry_count = (self.member_payload.len() / 4) as u64;
        let block_key = invalid_conversion(
            i64::try_from(self.current_member_page_offset),
            "V4 member page offset exceeds PostgreSQL bigint",
        )?;
        self.cas.write_block(
            &self.member_object_kind,
            block_key,
            0,
            entry_count,
            &self.member_payload,
            BlockCoordinateMetadata {
                member_offset: Some(self.current_member_page_offset),
                ..BlockCoordinateMetadata::default()
            },
        )?;
        self.current_member_page_offset = self
            .current_member_page_offset
            .checked_add(entry_count)
            .ok_or(invalid("V4 member page offset overflows"))?;
        self.member_block_count += 1;
        self.member_payload.clear();
        Ok(())
    }

    fn flush_locator_page(&mut self) -> ProviderGraphV4Result<()> {
        if self.locator_payload.is_empty() {
            return Ok(());
        }
        let owner_count = (self.locator_payload.len() / LOCATOR_BYTES) as u32;
        self.cas.write_block(
            &self.locator_object_kind,
            i64::from(self.current_locator_page_base),
            0,
            u64::from(owner_count),
            &self.locator_payload,
            BlockCoordinateMetadata {
                owner_base: Some(self.current_locator_page_base),
                owner_count: Some(owner_count),
                ..BlockCoordinateMetadata::default()
            },
        )?;
        self.current_locator_page_base = self
            .current_locator_page_base
            .checked_add(owner_count)
            .ok_or(invalid("V4 locator page owner base overflows"))?;
        self.locator_block_count += 1;
        self.locator_payload.clear();
        Ok(())
    }

    fn finish(mut self) -> ProviderGraphV4Result<V4RelationSummary> {
        if self.next_owner_index != self.expected_owner_count {
            return Err(invalid(format!(
                "V4 {} emitted {} owners, expected {}",
                self.relation, self.next_owner_index, self.expected_owner_count
            )));
        }
        self.flush_member_page()?;
        self.flush_locator_page()?;
        let raw_vector_bytes = self
            .vector_member_count
            .checked_mul(4)
            .ok_or(invalid("V4 relation raw member bytes overflow"))?;
        let raw_locator_bytes = (self.expected_owner_count as u64)
            .checked_mul(LOCATOR_BYTES as u64)
            .ok_or(invalid("V4 relation raw locator bytes overflow"))?;
        let encoded_byte_count = paged_encoded_bytes(
            &self.member_object_kind,
            raw_vector_bytes,
            self.member_page_bytes,
        )?
        .checked_add(paged_encoded_bytes(
            &self.locator_object_kind,
            raw_locator_bytes,
            self.locator_page_bytes,
        )?)
        .ok_or(invalid("V4 relation encoded byte count overflows"))?;
        Ok(V4RelationSummary {
            relation: self.relation.to_owned(),
            member_object_kind: self.member_object_kind,
            locator_object_kind: self.locator_object_kind,
            owner_base: self.owner_base,
            owner_count: self.expected_owner_count as u64,
            logical_member_count: self.logical_member_count,
            vector_member_count: self.vector_member_count,
            member_width: 4,
            member_page_bytes: self.member_page_bytes as u64,
            locator_page_bytes: self.locator_page_bytes as u64,
            locator_owner_span: self.locator_owner_span,
            member_block_count: self.member_block_count,
            locator_block_count: self.locator_block_count,
            raw_vector_bytes,
            raw_locator_bytes,
            encoded_byte_count,
        })
    }
}

struct RelationEmissionProgress<'a> {
    done: u64,
    total: u64,
    admission: &'a mut ResourceAdmissionTracker,
}

impl<'a> RelationEmissionProgress<'a> {
    fn new(
        total: u64,
        admission: &'a mut ResourceAdmissionTracker,
        progress: &mut ProgressReporter<'_>,
    ) -> Self {
        progress.periodic("emit_relations", 0, total, "owners");
        Self {
            done: 0,
            total,
            admission,
        }
    }

    fn owner(&mut self, progress: &mut ProgressReporter<'_>) -> ProviderGraphV4Result<()> {
        self.done = self
            .done
            .checked_add(1)
            .ok_or(invalid("V4 emitted owner progress overflows"))?;
        progress.periodic("emit_relations", self.done, self.total, "owners");
        Ok(())
    }
}

struct RelationListSpec<'a> {
    relation: &'static str,
    owner_base: u32,
    lists: &'a [Vec<u32>],
}

fn emit_relation_lists(
    cas: &mut CasBlockWriter,
    spec: RelationListSpec<'_>,
    options: &ProviderGraphV4Options,
    bitmaps: &mut Vec<V4HeavyBitmapSummary>,
    emission: &mut RelationEmissionProgress<'_>,
    progress: &mut ProgressReporter<'_>,
) -> ProviderGraphV4Result<V4RelationSummary> {
    let mut emitter = RelationEmitter::new(
        spec.relation,
        spec.owner_base,
        spec.lists.len(),
        cas,
        options,
    )?;
    for (index, members) in spec.lists.iter().enumerate() {
        let owner_key = spec
            .owner_base
            .checked_add(index as u32)
            .ok_or(invalid("V4 relation owner key overflows"))?;
        let bitmap_plan = maybe_heavy_bitmap(spec.relation, owner_key, members, options)?;
        emitter.push_owner(members, bitmap_plan.is_some())?;
        if let Some(plan) = bitmap_plan {
            bitmaps.push(emitter.emit_heavy_bitmap_streamed(
                plan,
                members,
                options.member_page_bytes,
            )?);
        }
        emission.owner(progress)?;
    }
    emitter.finish()
}

fn emit_single_member_relation(
    cas: &mut CasBlockWriter,
    relation: &'static str,
    members: &[u32],
    options: &ProviderGraphV4Options,
    bitmaps: &mut Vec<V4HeavyBitmapSummary>,
    emission: &mut RelationEmissionProgress<'_>,
    progress: &mut ProgressReporter<'_>,
) -> ProviderGraphV4Result<V4RelationSummary> {
    let mut emitter = RelationEmitter::new(relation, 0, members.len(), cas, options)?;
    for (owner_key, member) in members.iter().enumerate() {
        let owner_members = std::slice::from_ref(member);
        let bitmap_plan = maybe_heavy_bitmap(relation, owner_key as u32, owner_members, options)?;
        emitter.push_owner(owner_members, bitmap_plan.is_some())?;
        if let Some(plan) = bitmap_plan {
            bitmaps.push(emitter.emit_heavy_bitmap_streamed(
                plan,
                owner_members,
                options.member_page_bytes,
            )?);
        }
        emission.owner(progress)?;
    }
    emitter.finish()
}

fn emit_ordered_relation_lists(
    cas: &mut CasBlockWriter,
    relation: &'static str,
    owner_base: u32,
    lists: &[Vec<u32>],
    options: &ProviderGraphV4Options,
    emission: &mut RelationEmissionProgress<'_>,
    progress: &mut ProgressReporter<'_>,
) -> ProviderGraphV4Result<V4RelationSummary> {
    let mut emitter = RelationEmitter::new(relation, owner_base, lists.len(), cas, options)?;
    for members in lists {
        emitter.push_ordered_owner(members)?;
        emission.owner(progress)?;
    }
    emitter.finish()
}

fn emit_direct_relations(
    cas: &mut CasBlockWriter,
    model: &GraphModel,
    options: &ProviderGraphV4Options,
    bitmaps: &mut Vec<V4HeavyBitmapSummary>,
    observe: &mut V4ObserveCounters,
    emission: &mut RelationEmissionProgress<'_>,
    progress: &mut ProgressReporter<'_>,
) -> ProviderGraphV4Result<Vec<V4RelationSummary>> {
    let mut summaries = Vec::with_capacity(2);
    let mut scratch = Vec::new();
    let mut group_emitter = RelationEmitter::new(
        "group_sets_direct",
        0,
        model.group_components.len(),
        cas,
        options,
    )?;
    for (group, components) in model.group_components.iter().enumerate() {
        let scratch_members = components.iter().try_fold(0usize, |total, component| {
            total
                .checked_add(model.component_sets[*component as usize].len())
                .ok_or(invalid(
                    "resource_admission: direct group/set scratch count overflows",
                ))
        })?;
        emission
            .admission
            .reserve_scratch_members("direct group/set union", scratch_members)?;
        sorted_union_into(
            components
                .iter()
                .map(|component| model.component_sets[*component as usize].as_slice()),
            &mut scratch,
        );
        observe.direct_group_set_emission_owner_visits = observe
            .direct_group_set_emission_owner_visits
            .checked_add(1)
            .ok_or(invalid("V4 group/set owner traversal count overflows"))?;
        observe.direct_group_set_emission_edge_visits = observe
            .direct_group_set_emission_edge_visits
            .checked_add(scratch.len() as u64)
            .ok_or(invalid("V4 group/set edge traversal count overflows"))?;
        let bitmap_plan = maybe_heavy_bitmap("group_sets_direct", group as u32, &scratch, options)?;
        group_emitter.push_owner(&scratch, bitmap_plan.is_some())?;
        if let Some(plan) = bitmap_plan {
            bitmaps.push(group_emitter.emit_heavy_bitmap_streamed(
                plan,
                &scratch,
                options.member_page_bytes,
            )?);
        }
        emission.owner(progress)?;
    }
    summaries.push(group_emitter.finish()?);
    let mut set_emitter = RelationEmitter::new(
        "set_groups_direct",
        model.set_base,
        model.set_components.len(),
        cas,
        options,
    )?;
    let mut maximum_groups_per_set = 0u64;
    for (set_index, components) in model.set_components.iter().enumerate() {
        let scratch_members = components.iter().try_fold(0usize, |total, component| {
            total
                .checked_add(model.component_groups[*component as usize].len())
                .ok_or(invalid(
                    "resource_admission: direct set/group scratch count overflows",
                ))
        })?;
        emission
            .admission
            .reserve_scratch_members("direct set/group union", scratch_members)?;
        sorted_union_into(
            components
                .iter()
                .map(|component| model.component_groups[*component as usize].as_slice()),
            &mut scratch,
        );
        observe.set_group_expansion_owner_visits = observe
            .set_group_expansion_owner_visits
            .checked_add(1)
            .ok_or(invalid("V4 set/group owner traversal count overflows"))?;
        observe.set_group_expansion_edge_visits = observe
            .set_group_expansion_edge_visits
            .checked_add(scratch.len() as u64)
            .ok_or(invalid("V4 set/group edge traversal count overflows"))?;
        maximum_groups_per_set = maximum_groups_per_set.max(scratch.len() as u64);
        let owner_key = model
            .set_base
            .checked_add(set_index as u32)
            .ok_or(invalid("V4 provider-set owner key overflows"))?;
        let bitmap_plan = maybe_heavy_bitmap("set_groups_direct", owner_key, &scratch, options)?;
        set_emitter.push_owner(&scratch, bitmap_plan.is_some())?;
        if let Some(plan) = bitmap_plan {
            bitmaps.push(set_emitter.emit_heavy_bitmap_streamed(
                plan,
                &scratch,
                options.member_page_bytes,
            )?);
        }
        emission.owner(progress)?;
    }
    summaries.push(set_emitter.finish()?);
    observe.maximum_groups_per_set = maximum_groups_per_set;
    observe.maximum_groups_per_set_computed = 1;
    Ok(summaries)
}

fn emit_pattern_relations(
    cas: &mut CasBlockWriter,
    model: &GraphModel,
    options: &ProviderGraphV4Options,
    bitmaps: &mut Vec<V4HeavyBitmapSummary>,
    emission: &mut RelationEmissionProgress<'_>,
    progress: &mut ProgressReporter<'_>,
) -> ProviderGraphV4Result<Vec<V4RelationSummary>> {
    Ok(vec![
        emit_single_member_relation(
            cas,
            "group_patterns",
            &model.group_patterns,
            options,
            bitmaps,
            emission,
            progress,
        )?,
        emit_relation_lists(
            cas,
            RelationListSpec {
                relation: "pattern_groups",
                owner_base: 0,
                lists: &model.pattern_groups,
            },
            options,
            bitmaps,
            emission,
            progress,
        )?,
        emit_relation_lists(
            cas,
            RelationListSpec {
                relation: "pattern_sets",
                owner_base: 0,
                lists: &model.pattern_sets,
            },
            options,
            bitmaps,
            emission,
            progress,
        )?,
        emit_relation_lists(
            cas,
            RelationListSpec {
                relation: "set_patterns",
                owner_base: model.set_base,
                lists: &model.set_patterns,
            },
            options,
            bitmaps,
            emission,
            progress,
        )?,
        emit_relation_lists(
            cas,
            RelationListSpec {
                relation: "npi_patterns",
                owner_base: 0,
                lists: &model.npi_patterns,
            },
            options,
            bitmaps,
            emission,
            progress,
        )?,
    ])
}

struct PgCopyFileWriter {
    writer: BufWriter<File>,
    finished: bool,
}

impl PgCopyFileWriter {
    fn create(path: &Path) -> ProviderGraphV4Result<Self> {
        let mut writer = BufWriter::new(File::create(path)?);
        writer.write_all(PG_COPY_HEADER)?;
        Ok(Self {
            writer,
            finished: false,
        })
    }

    fn row(&mut self, fields: &[&[u8]]) -> ProviderGraphV4Result<()> {
        self.writer.write_all(
            &invalid_conversion(
                i16::try_from(fields.len()),
                "V4 dictionary row has too many fields",
            )?
            .to_be_bytes(),
        )?;
        for field in fields {
            self.writer.write_all(
                &invalid_conversion(
                    i32::try_from(field.len()),
                    "V4 dictionary field exceeds int32",
                )?
                .to_be_bytes(),
            )?;
            self.writer.write_all(field)?;
        }
        Ok(())
    }

    fn finish(mut self) -> ProviderGraphV4Result<()> {
        self.writer.write_all(&(-1i16).to_be_bytes())?;
        self.writer.flush()?;
        self.finished = true;
        Ok(())
    }
}

impl Drop for PgCopyFileWriter {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.writer.flush();
        }
    }
}

fn emit_dictionaries(
    output_directory: &Path,
    model: &GraphModel,
    layout: ProviderGraphV4Layout,
) -> ProviderGraphV4Result<(PathBuf, PathBuf, PathBuf, PathBuf, PathBuf, Option<PathBuf>)> {
    let group_path = output_directory.join("v4-provider-groups.copy");
    let mut groups = PgCopyFileWriter::create(&group_path)?;
    for (key, global) in model.group_globals.iter().enumerate() {
        groups.row(&[&(key as i32).to_be_bytes(), global])?;
    }
    groups.finish()?;
    let component_path = output_directory.join("v4-provider-components.copy");
    let mut components = PgCopyFileWriter::create(&component_path)?;
    for (key, global) in model.component_globals.iter().enumerate() {
        components.row(&[&(key as i32).to_be_bytes(), global])?;
    }
    components.finish()?;
    let npi_path = output_directory.join("v4-npi-scope.copy");
    let mut npis = PgCopyFileWriter::create(&npi_path)?;
    for (key, npi) in model.npis.iter().enumerate() {
        npis.row(&[&(key as i32).to_be_bytes(), &(*npi as i64).to_be_bytes()])?;
    }
    npis.finish()?;
    let provider_set_audit_npi_path = output_directory.join("v4-provider-set-audit-npi.copy");
    let mut provider_set_audit_npis = PgCopyFileWriter::create(&provider_set_audit_npi_path)?;
    for (provider_set_key, provider_group_key, npi) in &model.provider_set_audit_npis {
        provider_set_audit_npis.row(&[
            &(*provider_set_key as i32).to_be_bytes(),
            &(*provider_group_key as i32).to_be_bytes(),
            &(*npi as i64).to_be_bytes(),
        ])?;
    }
    provider_set_audit_npis.finish()?;
    let provider_set_npi_prefix_override_path =
        output_directory.join("v4-provider-set-npi-prefix-overrides.copy");
    let mut provider_set_npi_prefix_overrides =
        PgCopyFileWriter::create(&provider_set_npi_prefix_override_path)?;
    for (provider_set_key, member_count, member_digest) in
        &model.provider_set_npi_prefix_override_metadata
    {
        provider_set_npi_prefix_overrides.row(&[
            &(*provider_set_key as i32).to_be_bytes(),
            &(*member_count as i32).to_be_bytes(),
            member_digest,
        ])?;
    }
    provider_set_npi_prefix_overrides.finish()?;
    let pattern_path = if layout == ProviderGraphV4Layout::Pattern {
        let path = output_directory.join("v4-patterns.copy");
        let mut patterns = PgCopyFileWriter::create(&path)?;
        for (key, (digest, sets)) in model
            .pattern_digests
            .iter()
            .zip(&model.pattern_sets)
            .enumerate()
        {
            patterns.row(&[
                &(key as i32).to_be_bytes(),
                digest,
                &invalid_conversion(
                    i64::try_from(sets.len()),
                    "V4 pattern set count exceeds int64",
                )?
                .to_be_bytes(),
            ])?;
        }
        patterns.finish()?;
        Some(path)
    } else {
        None
    };
    Ok((
        group_path,
        component_path,
        npi_path,
        provider_set_audit_npi_path,
        provider_set_npi_prefix_override_path,
        pattern_path,
    ))
}

/// Compile complete factor sidecars into one deterministic adaptive V4 graph.
pub fn compile_provider_graph_v4(
    shards: &[V4ProviderGraphShardDescriptor],
    provider_set_key_map_path: impl AsRef<Path>,
    output_directory: impl AsRef<Path>,
    options: ProviderGraphV4Options,
) -> ProviderGraphV4Result<ProviderGraphV4ConversionSummary> {
    let mut sink = stderr_progress_sink;
    compile_provider_graph_v4_with_progress(
        shards,
        provider_set_key_map_path,
        output_directory,
        options,
        &mut sink,
    )
}

fn compile_provider_graph_v4_with_progress(
    shards: &[V4ProviderGraphShardDescriptor],
    provider_set_key_map_path: impl AsRef<Path>,
    output_directory: impl AsRef<Path>,
    options: ProviderGraphV4Options,
    sink: &mut dyn FnMut(&V4ProgressEvent),
) -> ProviderGraphV4Result<ProviderGraphV4ConversionSummary> {
    options.validate()?;
    fs::create_dir_all(output_directory.as_ref())?;
    let output_directory = fs::canonicalize(output_directory.as_ref())?;
    let paths: Vec<PathBuf> = OUTPUT_NAMES
        .iter()
        .map(|name| output_directory.join(name))
        .collect();
    for path in &paths {
        if path.exists() {
            return Err(invalid(format!(
                "V4 provider graph output already exists: {}",
                path.display()
            )));
        }
    }
    let mut progress = ProgressReporter::new(sink);
    let result = compile_provider_graph_v4_inner(
        shards,
        provider_set_key_map_path.as_ref(),
        &output_directory,
        &options,
        &mut progress,
    );
    if result.is_err() {
        for path in paths {
            let _ = fs::remove_file(path);
        }
    }
    result
}

fn compile_provider_graph_v4_inner(
    shards: &[V4ProviderGraphShardDescriptor],
    provider_set_key_map_path: &Path,
    output_directory: &Path,
    options: &ProviderGraphV4Options,
    progress: &mut ProgressReporter<'_>,
) -> ProviderGraphV4Result<ProviderGraphV4ConversionSummary> {
    progress.emit("resource_admission", 0, 1, "stage", false);
    let mut resource_admission =
        resource_admission_preflight(shards, provider_set_key_map_path, options)?;
    progress.emit("resource_admission", 1, 1, "stage", false);
    let raw = load_raw_factors(shards, progress)?;
    let provider_sets = ProviderSetMap::read(provider_set_key_map_path)?;
    let model = build_graph_model(
        &raw,
        &provider_sets,
        progress,
        &mut resource_admission,
        options,
    )?;
    let mut observe = model.observe.clone();
    let sizes = compute_layout_sizes(&model, options)?;
    let pattern_layout_serving_degree_eligible =
        record_pattern_fallback_diagnostics(&model, options, &mut observe);
    let selected_layout = choose_layout(
        sizes.direct,
        sizes.pattern,
        pattern_layout_serving_degree_eligible,
    );
    progress.emit("select_layout", 1, 1, "stage", false);

    let block_copy_path = output_directory.join("v4-graph-blocks.copy");
    let reference_manifest_path = output_directory.join("v4-graph-references.jsonl");
    let mut cas = CasBlockWriter::create(&block_copy_path, &reference_manifest_path)?;
    let mut selected_bitmaps = Vec::new();
    let selected_owner_count = [
        model.set_components.len(),
        model.component_groups.len(),
        model.npi_groups.len(),
        model.group_npis.len(),
        model.set_npi_prefix_overrides.len(),
    ]
    .into_iter()
    .chain(match selected_layout {
        ProviderGraphV4Layout::Direct => {
            vec![model.group_components.len(), model.set_components.len()]
        }
        ProviderGraphV4Layout::Pattern => vec![
            model.group_patterns.len(),
            model.pattern_groups.len(),
            model.pattern_sets.len(),
            model.set_patterns.len(),
            model.npi_patterns.len(),
        ],
    })
    .try_fold(0u64, |total, count| {
        total
            .checked_add(count as u64)
            .ok_or(invalid("V4 selected relation owner count overflows"))
    })?;
    let mut emission_progress =
        RelationEmissionProgress::new(selected_owner_count, &mut resource_admission, progress);
    let mut relation_summaries = vec![
        emit_relation_lists(
            &mut cas,
            RelationListSpec {
                relation: "set_components",
                owner_base: model.set_base,
                lists: &model.set_components,
            },
            options,
            &mut selected_bitmaps,
            &mut emission_progress,
            progress,
        )?,
        emit_relation_lists(
            &mut cas,
            RelationListSpec {
                relation: "component_groups",
                owner_base: 0,
                lists: &model.component_groups,
            },
            options,
            &mut selected_bitmaps,
            &mut emission_progress,
            progress,
        )?,
        emit_relation_lists(
            &mut cas,
            RelationListSpec {
                relation: "npi_groups_exact",
                owner_base: 0,
                lists: &model.npi_groups,
            },
            options,
            &mut selected_bitmaps,
            &mut emission_progress,
            progress,
        )?,
        emit_relation_lists(
            &mut cas,
            RelationListSpec {
                relation: "group_npis_exact",
                owner_base: 0,
                lists: &model.group_npis,
            },
            options,
            &mut selected_bitmaps,
            &mut emission_progress,
            progress,
        )?,
        emit_ordered_relation_lists(
            &mut cas,
            "set_npi_prefix_override",
            model.set_base,
            &model.set_npi_prefix_overrides,
            options,
            &mut emission_progress,
            progress,
        )?,
    ];
    relation_summaries.extend(match selected_layout {
        ProviderGraphV4Layout::Direct => emit_direct_relations(
            &mut cas,
            &model,
            options,
            &mut selected_bitmaps,
            &mut observe,
            &mut emission_progress,
            progress,
        )?,
        ProviderGraphV4Layout::Pattern => emit_pattern_relations(
            &mut cas,
            &model,
            options,
            &mut selected_bitmaps,
            &mut emission_progress,
            progress,
        )?,
    });
    if emission_progress.done != emission_progress.total {
        return Err(invalid("V4 selected relation owner progress is incomplete"));
    }
    selected_bitmaps.sort_by(|left, right| {
        left.relation
            .cmp(&right.relation)
            .then_with(|| left.owner_key.cmp(&right.owner_key))
    });
    // Bitmap fragments were streamed as part of their owner emission. This
    // terminal phase marker preserves monotonic dashboard phase ordering.
    progress.emit("emit_bitmaps", 1, 1, "stage", false);
    let heavy_bitmaps = selected_bitmaps;
    let (block_count, block_copy_bytes) = cas.finish()?;
    let (
        group_copy_path,
        component_copy_path,
        npi_copy_path,
        provider_set_audit_npi_copy_path,
        provider_set_npi_prefix_override_copy_path,
        pattern_copy_path,
    ) = {
        progress.emit("emit_dictionaries", 0, 1, "stage", false);
        let emitted = emit_dictionaries(output_directory, &model, selected_layout)?;
        progress.emit("emit_dictionaries", 1, 1, "stage", false);
        emitted
    };
    let summary_path = output_directory.join("v4-summary.json");
    let input_digest: [u8; 32] = raw.input_digest.finalize().into();
    let mut database_output_bytes = block_copy_bytes;
    for path in [
        &group_copy_path,
        &component_copy_path,
        &npi_copy_path,
        &provider_set_audit_npi_copy_path,
        &provider_set_npi_prefix_override_copy_path,
    ] {
        database_output_bytes = database_output_bytes
            .checked_add(fs::metadata(path)?.len())
            .ok_or(invalid("V4 database output byte count overflows"))?;
    }
    if let Some(path) = pattern_copy_path.as_ref() {
        database_output_bytes = database_output_bytes
            .checked_add(fs::metadata(path)?.len())
            .ok_or(invalid("V4 database output byte count overflows"))?;
    }
    let selected_base_bytes = match selected_layout {
        ProviderGraphV4Layout::Direct => sizes.direct,
        ProviderGraphV4Layout::Pattern => sizes.pattern,
    };
    if database_output_bytes > selected_base_bytes {
        return Err(invalid(format!(
            "V4 selected bitmap overrides expanded output: base {selected_base_bytes}, emitted {database_output_bytes}"
        )));
    }
    let selected_encoded_bytes = database_output_bytes;
    let mut output_artifacts = vec![
        output_artifact("graph_blocks", &block_copy_path, block_count)?,
        output_artifact("graph_references", &reference_manifest_path, block_count)?,
        output_artifact(
            "provider_groups",
            &group_copy_path,
            model.group_globals.len() as u64,
        )?,
        output_artifact(
            "provider_components",
            &component_copy_path,
            model.component_globals.len() as u64,
        )?,
        output_artifact("npi_scope", &npi_copy_path, model.npis.len() as u64)?,
        output_artifact(
            "provider_set_audit_npi",
            &provider_set_audit_npi_copy_path,
            model.provider_set_audit_npis.len() as u64,
        )?,
        output_artifact(
            "provider_set_npi_prefix_overrides",
            &provider_set_npi_prefix_override_copy_path,
            model.provider_set_npi_prefix_override_metadata.len() as u64,
        )?,
    ];
    if let Some(path) = pattern_copy_path.as_ref() {
        output_artifacts.push(output_artifact(
            "patterns",
            path,
            model.pattern_sets.len() as u64,
        )?);
    }
    let summary = ProviderGraphV4ConversionSummary {
        format: "ptg2_provider_graph_v4_factor_adaptive_v1".to_owned(),
        selected_layout,
        pattern_layout_serving_degree_eligible,
        max_set_patterns_per_set: options.max_set_patterns_per_set as u64,
        max_set_components_per_fallback_set: options.max_set_components_per_fallback_set as u64,
        max_online_group_keys_per_set: options.max_online_group_keys_per_set as u64,
        max_online_source_owners_per_set: options.max_online_source_owners_per_set as u64,
        max_online_source_members_per_set: options.max_online_source_members_per_set as u64,
        max_online_source_pages_per_set: options.max_online_source_pages_per_set as u64,
        max_online_source_bytes_per_set: options.max_online_source_bytes_per_set,
        online_group_npi_batch_size: options.online_group_npi_batch_size as u64,
        max_online_group_npi_members_per_set: options.max_online_group_npi_members_per_set as u64,
        max_online_group_npi_locator_pages_per_set: options
            .max_online_group_npi_locator_pages_per_set
            as u64,
        max_online_group_npi_member_pages_per_set: options.max_online_group_npi_member_pages_per_set
            as u64,
        max_online_group_npi_bytes_per_set: options.max_online_group_npi_bytes_per_set,
        max_online_group_npi_batches_per_set: options.max_online_group_npi_batches_per_set as u64,
        provider_expansion_rate_page_rows: options.provider_expansion_rate_page_rows as u64,
        max_online_provider_expansion_rate_rows: options.max_online_provider_expansion_rate_rows
            as u64,
        max_online_provider_expansion_provider_sets: options
            .max_online_provider_expansion_provider_sets
            as u64,
        max_online_provider_expansion_graph_batches: options
            .max_online_provider_expansion_graph_batches
            as u64,
        npi_prefix_target: options.npi_prefix_target as u64,
        max_npi_prefix_override_owners: options.max_npi_prefix_override_owners as u64,
        max_npi_prefix_override_bytes: options.max_npi_prefix_override_bytes,
        direct_complete_encoded_bytes: sizes.direct,
        pattern_complete_encoded_bytes: sizes.pattern,
        common_encoded_bytes: sizes.common,
        selected_encoded_bytes,
        block_copy_path,
        reference_manifest_path,
        group_copy_path,
        component_copy_path,
        npi_copy_path,
        provider_set_audit_npi_copy_path,
        provider_set_npi_prefix_override_copy_path,
        pattern_copy_path,
        summary_path: summary_path.clone(),
        block_count,
        block_copy_bytes,
        relation_summaries,
        heavy_bitmaps,
        output_artifacts,
        observe,
        resource_admission: resource_admission.into_summary(),
        input_byte_count: raw.input_byte_count,
        input_sha256: hex(&input_digest),
    };
    let summary_file = File::create(&summary_path)?;
    let mut summary_writer = BufWriter::new(summary_file);
    serde_json::to_writer_pretty(&mut summary_writer, &summary)?;
    summary_writer.flush()?;
    progress.emit("complete", 1, 1, "compile", true);
    Ok(summary)
}

pub fn compile_provider_graph_v4_manifest(
    manifest: ProviderGraphV4Manifest,
) -> ProviderGraphV4Result<ProviderGraphV4ConversionSummary> {
    compile_provider_graph_v4(
        &manifest.shards,
        manifest.provider_set_key_map_path,
        manifest.output_directory,
        manifest.options,
    )
}

fn shared_block_hash(
    object_kind: &str,
    codec: &str,
    payload: &[u8],
) -> ProviderGraphV4Result<[u8; 32]> {
    let mut hasher = Sha256::new();
    hasher.update(BLOCK_HASH_DOMAIN);
    hasher.update(SHARED_FORMAT_VERSION.to_be_bytes());
    update_length_prefixed(&mut hasher, object_kind.as_bytes())?;
    update_length_prefixed(&mut hasher, codec.as_bytes())?;
    update_length_prefixed(&mut hasher, payload)?;
    Ok(hasher.finalize().into())
}

fn output_artifact(
    name: &str,
    path: &Path,
    row_count: u64,
) -> ProviderGraphV4Result<V4OutputArtifactSummary> {
    let file = File::open(path)?;
    let byte_count = file.metadata()?.len();
    let mut reader = BufReader::new(file);
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = std::io::Read::read(&mut reader, &mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(V4OutputArtifactSummary {
        name: name.to_owned(),
        path: path.to_path_buf(),
        byte_count,
        sha256: hex(&hasher.finalize()),
        row_count,
    })
}

fn update_length_prefixed(hasher: &mut Sha256, value: &[u8]) -> ProviderGraphV4Result<()> {
    let length = invalid_conversion(
        u32::try_from(value.len()),
        "V4 shared block hash field exceeds uint32",
    )?;
    hasher.update(length.to_be_bytes());
    hasher.update(value);
    Ok(())
}

fn npi_from_global_id(global_id: GlobalId) -> ProviderGraphV4Result<u64> {
    if global_id[..8] != [0; 8] {
        return Err(invalid("V4 NPI membership uses an invalid global ID"));
    }
    let npi = u64::from_be_bytes(global_id[8..].try_into().expect("fixed global ID width"));
    if npi == 0 || npi > i64::MAX as u64 {
        return Err(invalid("V4 NPI membership uses an invalid NPI"));
    }
    Ok(npi)
}

fn parse_sha256(value: &str) -> ProviderGraphV4Result<[u8; 32]> {
    if value.len() != 64 {
        return Err(invalid("V4 membership metadata requires a SHA-256 digest"));
    }
    let mut result = [0u8; 32];
    for (index, destination) in result.iter_mut().enumerate() {
        *destination = (decode_hex(value.as_bytes()[index * 2])? << 4)
            | decode_hex(value.as_bytes()[index * 2 + 1])?;
    }
    Ok(result)
}

fn parse_global_id_hex(value: &str) -> ProviderGraphV4Result<GlobalId> {
    if value.len() != 32 {
        return Err(invalid("V4 global IDs must contain 32 hex characters"));
    }
    let mut result = [0u8; 16];
    for (index, destination) in result.iter_mut().enumerate() {
        *destination = (decode_hex(value.as_bytes()[index * 2])? << 4)
            | decode_hex(value.as_bytes()[index * 2 + 1])?;
    }
    Ok(result)
}

fn decode_hex(value: u8) -> ProviderGraphV4Result<u8> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(invalid("V4 metadata contains invalid hexadecimal text")),
    }
}

fn read_global_id(bytes: &[u8], offset: usize) -> ProviderGraphV4Result<GlobalId> {
    let end = offset
        .checked_add(GLOBAL_ID_BYTES)
        .ok_or(invalid("V4 global ID offset overflows"))?;
    invalid_conversion(
        bytes
            .get(offset..end)
            .ok_or(invalid("V4 membership global ID is truncated"))?
            .try_into(),
        "V4 membership global ID width changed",
    )
}

fn read_u32_le(bytes: &[u8], offset: usize) -> ProviderGraphV4Result<u32> {
    let value = bytes
        .get(offset..offset.saturating_add(4))
        .ok_or(invalid("V4 membership uint32 is truncated"))?;
    Ok(u32::from_le_bytes(
        value.try_into().expect("validated uint32 slice"),
    ))
}

fn read_u64_le(bytes: &[u8], offset: usize) -> ProviderGraphV4Result<u64> {
    let value = bytes
        .get(offset..offset.saturating_add(8))
        .ok_or(invalid("V4 membership uint64 is truncated"))?;
    Ok(u64::from_le_bytes(
        value.try_into().expect("validated uint64 slice"),
    ))
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(DIGITS[(byte >> 4) as usize] as char);
        output.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::collections::BTreeMap;
    use std::io::Read;
    use tempfile::TempDir;

    fn global(domain: u8, value: u64) -> GlobalId {
        let mut result = [0u8; 16];
        result[0] = domain;
        result[8..].copy_from_slice(&value.to_be_bytes());
        result
    }

    fn npi(value: u64) -> GlobalId {
        let mut result = [0u8; 16];
        result[8..].copy_from_slice(&value.to_be_bytes());
        result
    }

    fn normalized_pairs(
        pairs: impl IntoIterator<Item = (GlobalId, GlobalId)>,
    ) -> BTreeMap<GlobalId, Vec<GlobalId>> {
        let mut result: BTreeMap<GlobalId, Vec<GlobalId>> = BTreeMap::new();
        for (owner, member) in pairs {
            result.entry(owner).or_default().push(member);
        }
        for members in result.values_mut() {
            members.sort_unstable();
            members.dedup();
        }
        result
    }

    fn write_membership(
        path: &Path,
        shard_id: &str,
        name: &str,
        pairs: impl IntoIterator<Item = (GlobalId, GlobalId)>,
        dense: bool,
    ) -> V4MembershipArtifactDescriptor {
        let pairs = normalized_pairs(pairs);
        let member_count = pairs.values().map(Vec::len).sum::<usize>();
        let mut bytes = Vec::new();
        let dictionary = if dense {
            let mut values: Vec<GlobalId> = pairs
                .values()
                .flat_map(|members| members.iter().copied())
                .collect();
            values.sort_unstable();
            values.dedup();
            values
        } else {
            Vec::new()
        };
        bytes.extend_from_slice(if dense { DENSE_MAGIC } else { STANDARD_MAGIC });
        bytes.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
        bytes.extend_from_slice(&(pairs.len() as u64).to_le_bytes());
        if dense {
            bytes.extend_from_slice(&(dictionary.len() as u64).to_le_bytes());
        }
        let mut member_offset = 0u64;
        for (owner, members) in &pairs {
            bytes.extend_from_slice(owner);
            bytes.extend_from_slice(&member_offset.to_le_bytes());
            bytes.extend_from_slice(&(members.len() as u32).to_le_bytes());
            member_offset += members.len() as u64;
        }
        if dense {
            for member in &dictionary {
                bytes.extend_from_slice(member);
            }
            let local: HashMap<GlobalId, u32> = dictionary
                .iter()
                .copied()
                .enumerate()
                .map(|(index, value)| (value, index as u32))
                .collect();
            for members in pairs.values() {
                for member in members {
                    bytes.extend_from_slice(&local[member].to_le_bytes());
                }
            }
        } else {
            for members in pairs.values() {
                for member in members {
                    bytes.extend_from_slice(member);
                }
            }
        }
        fs::write(path, &bytes).unwrap();
        V4MembershipArtifactDescriptor {
            path: path.to_path_buf(),
            metadata: V4MembershipMetadata {
                record_format: if dense { DENSE_FORMAT } else { STANDARD_FORMAT }.to_owned(),
                sha256: hex(&Sha256::digest(&bytes)),
                byte_count: bytes.len() as u64,
                owner_count: pairs.len() as u64,
                member_count: member_count as u64,
                member_global_count: dense.then_some(dictionary.len() as u64),
                name: Some(name.to_owned()),
                source_shard_id: Some(shard_id.to_owned()),
                shard_id: None,
            },
        }
    }

    fn write_provider_map(path: &Path, sets: &[GlobalId], key_base: u32) {
        let mut sorted = sets.to_vec();
        sorted.sort_unstable();
        let mut output = String::new();
        for (index, set) in sorted.iter().enumerate() {
            output.push_str(&hex(set));
            output.push('\t');
            output.push_str(&(key_base + index as u32).to_string());
            output.push('\n');
        }
        fs::write(path, output).unwrap();
    }

    struct Fixture {
        _temporary: TempDir,
        shard: V4ProviderGraphShardDescriptor,
        provider_map: PathBuf,
        output: PathBuf,
    }

    fn shared_pattern_fixture(group_count: usize, set_count: usize) -> Fixture {
        let temporary = tempfile::tempdir().unwrap();
        let component = global(2, 1);
        let groups: Vec<GlobalId> = (0..group_count)
            .map(|index| global(3, index as u64 + 1))
            .collect();
        let sets: Vec<GlobalId> = (0..set_count)
            .map(|index| global(1, index as u64 + 1))
            .collect();
        let provider_npi = npi(1_234_567_890);
        let set_component = write_membership(
            &temporary.path().join("set-component.sidecar"),
            "shard-a",
            "provider_set_component",
            sets.iter().copied().map(|set| (set, component)),
            true,
        );
        let component_group = write_membership(
            &temporary.path().join("component-group.sidecar"),
            "shard-a",
            "provider_component_group",
            groups.iter().copied().map(|group| (component, group)),
            true,
        );
        let group_npi = write_membership(
            &temporary.path().join("group-npi.sidecar"),
            "shard-a",
            "provider_group_npi",
            groups.iter().copied().map(|group| (group, provider_npi)),
            true,
        );
        let npi_group = write_membership(
            &temporary.path().join("npi-group.sidecar"),
            "shard-a",
            "provider_npi_group",
            groups.iter().copied().map(|group| (provider_npi, group)),
            true,
        );
        let provider_map = temporary.path().join("provider-map.copy");
        write_provider_map(&provider_map, &sets, 1);
        let output = temporary.path().join("output");
        Fixture {
            _temporary: temporary,
            shard: V4ProviderGraphShardDescriptor {
                shard_id: "shard-a".to_owned(),
                provider_set_component: set_component,
                provider_component_group: component_group,
                provider_group_npi: group_npi,
                provider_npi_group: npi_group,
            },
            provider_map,
            output,
        }
    }

    fn mixed_pattern_component_fixture(groups_per_component: usize) -> Fixture {
        let temporary = tempfile::tempdir().unwrap();
        let sets = [global(1, 1), global(1, 2), global(1, 3)];
        let components = [global(2, 1), global(2, 2)];
        let first_groups = (0..groups_per_component)
            .map(|index| global(3, index as u64 + 1))
            .collect::<Vec<_>>();
        let second_groups = (0..groups_per_component)
            .map(|index| global(3, groups_per_component as u64 + index as u64 + 1))
            .collect::<Vec<_>>();
        let provider_npi = npi(1_234_567_890);
        let set_component = write_membership(
            &temporary.path().join("set-component.sidecar"),
            "shard-mixed",
            "provider_set_component",
            [
                (sets[0], components[0]),
                (sets[0], components[1]),
                (sets[1], components[0]),
                (sets[2], components[1]),
            ],
            true,
        );
        let component_group = write_membership(
            &temporary.path().join("component-group.sidecar"),
            "shard-mixed",
            "provider_component_group",
            first_groups
                .iter()
                .copied()
                .map(|group| (components[0], group))
                .chain(
                    second_groups
                        .iter()
                        .copied()
                        .map(|group| (components[1], group)),
                ),
            true,
        );
        let groups = first_groups
            .iter()
            .chain(&second_groups)
            .copied()
            .collect::<Vec<_>>();
        let group_npi = write_membership(
            &temporary.path().join("group-npi.sidecar"),
            "shard-mixed",
            "provider_group_npi",
            groups.iter().copied().map(|group| (group, provider_npi)),
            true,
        );
        let npi_group = write_membership(
            &temporary.path().join("npi-group.sidecar"),
            "shard-mixed",
            "provider_npi_group",
            groups.iter().copied().map(|group| (provider_npi, group)),
            true,
        );
        let provider_map = temporary.path().join("provider-map.copy");
        write_provider_map(&provider_map, &sets, 1);
        let output = temporary.path().join("output");
        Fixture {
            _temporary: temporary,
            shard: V4ProviderGraphShardDescriptor {
                shard_id: "shard-mixed".to_owned(),
                provider_set_component: set_component,
                provider_component_group: component_group,
                provider_group_npi: group_npi,
                provider_npi_group: npi_group,
            },
            provider_map,
            output,
        }
    }

    fn independent_fixture() -> Fixture {
        let temporary = tempfile::tempdir().unwrap();
        let sets = vec![global(1, 1), global(1, 2)];
        let components = [global(2, 1), global(2, 2)];
        let groups = [global(3, 1), global(3, 2)];
        let npis = [npi(1_111_111_111), npi(2_222_222_222)];
        let set_component = write_membership(
            &temporary.path().join("set-component.sidecar"),
            "shard-b",
            "provider_set_component",
            sets.iter().copied().zip(components),
            false,
        );
        let component_group = write_membership(
            &temporary.path().join("component-group.sidecar"),
            "shard-b",
            "provider_component_group",
            components.into_iter().zip(groups),
            false,
        );
        let group_npi = write_membership(
            &temporary.path().join("group-npi.sidecar"),
            "shard-b",
            "provider_group_npi",
            groups.into_iter().zip(npis),
            false,
        );
        let npi_group = write_membership(
            &temporary.path().join("npi-group.sidecar"),
            "shard-b",
            "provider_npi_group",
            npis.into_iter().zip(groups),
            false,
        );
        let provider_map = temporary.path().join("provider-map.copy");
        write_provider_map(&provider_map, &sets, 0);
        let output = temporary.path().join("output");
        Fixture {
            _temporary: temporary,
            shard: V4ProviderGraphShardDescriptor {
                shard_id: "shard-b".to_owned(),
                provider_set_component: set_component,
                provider_component_group: component_group,
                provider_group_npi: group_npi,
                provider_npi_group: npi_group,
            },
            provider_map,
            output,
        }
    }

    fn reference_kinds(path: &Path) -> Vec<String> {
        let references: Vec<Value> = BufReader::new(File::open(path).unwrap())
            .lines()
            .map(|line| {
                let value: Value = serde_json::from_str(&line.unwrap()).unwrap();
                assert_eq!(value["codec"], "none");
                value
            })
            .collect();
        let coordinates: Vec<(&str, i64, i64)> = references
            .iter()
            .map(|value| {
                (
                    value["object_kind"].as_str().unwrap(),
                    value["block_key"].as_i64().unwrap(),
                    value["fragment_no"].as_i64().unwrap(),
                )
            })
            .collect();
        assert!(coordinates.windows(2).all(|pair| pair[0] < pair[1]));
        references
            .iter()
            .map(|value| value["object_kind"].as_str().unwrap().to_owned())
            .collect()
    }

    fn copy_payloads_for_kind(path: &Path, expected_kind: &str) -> Vec<Vec<u8>> {
        let bytes = fs::read(path).unwrap();
        assert_eq!(&bytes[..PG_COPY_HEADER.len()], PG_COPY_HEADER);
        let mut offset = PG_COPY_HEADER.len();
        let mut payloads = Vec::new();
        loop {
            let field_count = i16::from_be_bytes(bytes[offset..offset + 2].try_into().unwrap());
            offset += 2;
            if field_count == -1 {
                assert_eq!(offset, bytes.len());
                return payloads;
            }
            assert_eq!(field_count, 10);
            let mut fields = Vec::with_capacity(10);
            for _ in 0..10 {
                let width = i32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap());
                offset += 4;
                assert!(width >= 0);
                let end = offset + width as usize;
                fields.push(&bytes[offset..end]);
                offset = end;
            }
            if fields[2] == expected_kind.as_bytes() {
                payloads.push(fields[9].to_vec());
            }
        }
    }

    #[test]
    fn factor_quotient_has_exact_direct_parity_without_flat_expansion() {
        let fixture = shared_pattern_fixture(64, 16);
        let mut sink = |_event: &V4ProgressEvent| {};
        let mut progress = ProgressReporter::new(&mut sink);
        let raw = load_raw_factors(std::slice::from_ref(&fixture.shard), &mut progress).unwrap();
        let provider_sets = ProviderSetMap::read(&fixture.provider_map).unwrap();
        let mut admission = resource_admission_preflight(
            std::slice::from_ref(&fixture.shard),
            &fixture.provider_map,
            &ProviderGraphV4Options::default(),
        )
        .unwrap();
        let model = build_graph_model(
            &raw,
            &provider_sets,
            &mut progress,
            &mut admission,
            &ProviderGraphV4Options::default(),
        )
        .unwrap();
        assert_eq!(model.pattern_sets.len(), 1);
        assert_eq!(model.pattern_groups[0].len(), 64);
        assert_eq!(model.pattern_sets[0], (1..=16).collect::<Vec<_>>());
        let mut scratch = Vec::new();
        for (group, components) in model.group_components.iter().enumerate() {
            sorted_union_into(
                components
                    .iter()
                    .map(|component| model.component_sets[*component as usize].as_slice()),
                &mut scratch,
            );
            assert_eq!(
                scratch, model.pattern_sets[model.group_patterns[group] as usize],
                "pattern projection changed exact group/set incidence"
            );
        }
        assert_eq!(model.npi_groups[0].len(), 64);
        assert_eq!(model.npi_patterns[0], vec![0]);
    }

    #[test]
    fn repeated_multi_component_tuple_is_unioned_and_charged_once() {
        let temporary = tempfile::tempdir().unwrap();
        let sets = [global(1, 1), global(1, 2)];
        let components = [global(2, 1), global(2, 2)];
        let groups = (0..64)
            .map(|index| global(3, index + 1))
            .collect::<Vec<_>>();
        let provider_npi = npi(1_234_567_890);
        let shard = V4ProviderGraphShardDescriptor {
            shard_id: "tuple-cache".to_owned(),
            provider_set_component: write_membership(
                &temporary.path().join("set-component.sidecar"),
                "tuple-cache",
                "provider_set_component",
                [(sets[0], components[0]), (sets[1], components[1])],
                true,
            ),
            provider_component_group: write_membership(
                &temporary.path().join("component-group.sidecar"),
                "tuple-cache",
                "provider_component_group",
                groups
                    .iter()
                    .copied()
                    .map(|group| (components[0], group))
                    .chain(groups.iter().copied().map(|group| (components[1], group))),
                true,
            ),
            provider_group_npi: write_membership(
                &temporary.path().join("group-npi.sidecar"),
                "tuple-cache",
                "provider_group_npi",
                groups.iter().copied().map(|group| (group, provider_npi)),
                true,
            ),
            provider_npi_group: write_membership(
                &temporary.path().join("npi-group.sidecar"),
                "tuple-cache",
                "provider_npi_group",
                groups.iter().copied().map(|group| (provider_npi, group)),
                true,
            ),
        };
        let provider_map = temporary.path().join("provider-map.copy");
        write_provider_map(&provider_map, &sets, 0);
        let mut sink = |_event: &V4ProgressEvent| {};
        let mut progress = ProgressReporter::new(&mut sink);
        let raw = load_raw_factors(std::slice::from_ref(&shard), &mut progress).unwrap();
        let provider_sets = ProviderSetMap::read(&provider_map).unwrap();
        let mut admission = resource_admission_preflight(
            std::slice::from_ref(&shard),
            &provider_map,
            &ProviderGraphV4Options::default(),
        )
        .unwrap();
        let model = build_graph_model(
            &raw,
            &provider_sets,
            &mut progress,
            &mut admission,
            &ProviderGraphV4Options::default(),
        )
        .unwrap();

        assert_eq!(model.observe.multi_component_group_count, 64);
        assert_eq!(model.observe.multi_component_group_union_count, 1);
        assert_eq!(model.observe.component_tuple_pattern_cache_owner_count, 1);
        assert_eq!(model.observe.component_tuple_pattern_cache_member_count, 2);
        assert_eq!(model.observe.group_set_expansion_edge_visits, 2);
        assert_eq!(model.observe.group_set_incidence_count, 128);
        assert!(model.group_patterns.iter().all(|pattern| *pattern == 0));
    }

    #[test]
    fn prefix_override_prescreen_skips_ordinary_sets_and_keeps_group_first_order() {
        let fixture = independent_fixture();
        let options = ProviderGraphV4Options {
            npi_prefix_target: 3,
            ..ProviderGraphV4Options::default()
        };
        let mut admission = resource_admission_preflight(
            std::slice::from_ref(&fixture.shard),
            &fixture.provider_map,
            &options,
        )
        .unwrap();
        let ordinary_groups = (0..4_000u32).collect::<Vec<_>>();
        let ordinary_set_count = 128usize;
        let ordinary_plan = derive_npi_prefix_overrides(
            NpiPrefixInputs {
                set_base: 0,
                set_components: &vec![vec![0]; ordinary_set_count],
                component_groups: std::slice::from_ref(&ordinary_groups),
                set_patterns: &vec![vec![0]; ordinary_set_count],
                pattern_groups: std::slice::from_ref(&ordinary_groups),
                group_npis: &vec![vec![9]; ordinary_groups.len()],
            },
            &options,
            &mut admission,
        )
        .unwrap();
        assert_eq!(ordinary_plan.group_unsafe_set_count, 0);
        assert_eq!(ordinary_plan.physical_unsafe_set_count, 0);
        assert_eq!(ordinary_plan.groups_to_target.len(), ordinary_set_count);
        assert!(ordinary_plan.group_merge_member_visits > 0);
        assert!(ordinary_plan.metadata.is_empty());
        assert_eq!(ordinary_plan.worst_online_provider_set_key, Some(0));
        assert_eq!(ordinary_plan.worst_online_group_work_bound, 4_000);
        assert_eq!(ordinary_plan.worst_online_prefix_member_count, 1);
        assert_eq!(ordinary_plan.worst_online_group_npi_work.batches, 1);
        assert_eq!(ordinary_plan.worst_online_group_npi_work.member_work(), 2);
        assert_eq!(
            ordinary_plan.worst_online_prefix_member_digest,
            Some(npi_prefix_digest(&[9]))
        );
        assert_eq!(ordinary_plan.worst_online_probe_merge_member_visits, 0);

        let high_groups = (0..5_000u32).collect::<Vec<_>>();
        let mut group_npis = vec![vec![9]; high_groups.len()];
        group_npis[4_096] = vec![2];
        group_npis[4_097] = vec![7];
        let mut high_admission = resource_admission_preflight(
            std::slice::from_ref(&fixture.shard),
            &fixture.provider_map,
            &options,
        )
        .unwrap();
        let high_plan = derive_npi_prefix_overrides(
            NpiPrefixInputs {
                set_base: 11,
                set_components: &[vec![0]],
                component_groups: std::slice::from_ref(&high_groups),
                set_patterns: &[vec![0]],
                pattern_groups: std::slice::from_ref(&high_groups),
                group_npis: &group_npis,
            },
            &options,
            &mut high_admission,
        )
        .unwrap();
        assert_eq!(high_plan.group_unsafe_set_count, 1);
        assert_eq!(high_plan.physical_unsafe_set_count, 1);
        assert_eq!(high_plan.metadata.len(), 1);
        assert_eq!(high_plan.lists[0], vec![9, 2, 7]);
        assert_eq!(high_plan.metadata[0].0, 11);
        assert_eq!(high_plan.metadata[0].1, 3);
        assert_eq!(high_plan.metadata[0].2, npi_prefix_digest(&[9, 2, 7]));
        assert_eq!(high_plan.groups_to_target, vec![4_098]);
        assert_eq!(high_plan.worst_provider_set_key, Some(11));
        assert_eq!(high_plan.worst_groups_to_target, 4_098);
        assert!(high_plan.worst_provider_set_uses_override);
        assert_eq!(high_plan.worst_prefix_member_count, 3);
        assert_eq!(
            high_plan.worst_prefix_member_digest,
            Some(npi_prefix_digest(&[9, 2, 7]))
        );
        assert!(
            high_plan.maximum_group_npi_member_work >= high_plan.worst_group_npi_work.member_work()
        );
        assert!(
            high_plan.maximum_group_npi_locator_page_work
                >= high_plan.worst_group_npi_work.locator_pages
        );
        assert!(
            high_plan.maximum_group_npi_member_page_work
                >= high_plan.worst_group_npi_work.member_pages
        );
        assert!(
            high_plan.maximum_group_npi_byte_work >= high_plan.worst_group_npi_work.byte_work()
        );
        assert!(high_plan.maximum_group_npi_batch_work >= high_plan.worst_group_npi_work.batches);
        assert_eq!(high_plan.worst_online_provider_set_key, None);
        assert!(high_plan.group_merge_member_visits > 4_096);
    }

    #[test]
    fn physical_work_budget_forces_sparse_override_before_online_traversal() {
        let fixture = independent_fixture();
        let options = ProviderGraphV4Options {
            npi_prefix_target: 3,
            max_online_source_members_per_set: 1,
            ..ProviderGraphV4Options::default()
        };
        let mut admission = resource_admission_preflight(
            std::slice::from_ref(&fixture.shard),
            &fixture.provider_map,
            &options,
        )
        .unwrap();
        let groups = (0..10u32).collect::<Vec<_>>();
        let group_npis = (0..10u32).map(|npi_key| vec![npi_key]).collect::<Vec<_>>();
        let plan = derive_npi_prefix_overrides(
            NpiPrefixInputs {
                set_base: 0,
                set_components: &[vec![0]],
                component_groups: std::slice::from_ref(&groups),
                set_patterns: &[vec![0]],
                pattern_groups: std::slice::from_ref(&groups),
                group_npis: &group_npis,
            },
            &options,
            &mut admission,
        )
        .unwrap();
        assert_eq!(plan.group_unsafe_set_count, 0);
        assert_eq!(plan.physical_unsafe_set_count, 1);
        assert_eq!(plan.groups_to_target, vec![3]);
        assert_eq!(plan.lists[0], vec![0, 1, 2]);
        assert_eq!(plan.metadata.len(), 1);
        assert!(plan.maximum_source_member_work > 1);
    }

    #[test]
    fn second_hop_work_accounts_exact_pages_dictionary_and_byte_admission() {
        let fixture = independent_fixture();
        let options = ProviderGraphV4Options {
            member_page_bytes: 16,
            locator_page_bytes: 24,
            online_group_npi_batch_size: 2,
            npi_prefix_target: 3,
            max_online_group_npi_bytes_per_set: 87,
            ..ProviderGraphV4Options::default()
        };
        let mut admission = resource_admission_preflight(
            std::slice::from_ref(&fixture.shard),
            &fixture.provider_map,
            &options,
        )
        .unwrap();
        let plan = derive_npi_prefix_overrides(
            NpiPrefixInputs {
                set_base: 0,
                set_components: &[vec![0]],
                component_groups: &[vec![0, 1]],
                set_patterns: &[vec![0]],
                pattern_groups: &[vec![0, 1]],
                group_npis: &[vec![0, 1], vec![1, 2]],
            },
            &options,
            &mut admission,
        )
        .unwrap();

        assert_eq!(plan.group_unsafe_set_count, 0);
        assert_eq!(plan.physical_unsafe_set_count, 1);
        assert_eq!(plan.maximum_group_npi_member_work, 7);
        assert_eq!(plan.maximum_group_npi_locator_page_work, 1);
        assert_eq!(plan.maximum_group_npi_member_page_work, 1);
        assert_eq!(plan.maximum_group_npi_byte_work, 88);
        assert_eq!(plan.maximum_group_npi_batch_work, 1);
        assert_eq!(plan.metadata.len(), 1);
        assert_eq!(plan.lists[0], vec![0, 1, 2]);
        assert_eq!(plan.worst_group_npi_work.relation_members, 4);
        assert_eq!(plan.worst_group_npi_work.dictionary_members, 3);
    }

    #[test]
    fn second_hop_batch_cap_overrides_the_129th_group_owner() {
        let fixture = independent_fixture();
        let options = ProviderGraphV4Options {
            npi_prefix_target: 129,
            ..ProviderGraphV4Options::default()
        };
        let mut admission = resource_admission_preflight(
            std::slice::from_ref(&fixture.shard),
            &fixture.provider_map,
            &options,
        )
        .unwrap();
        let groups = (0..129u32).collect::<Vec<_>>();
        let group_npis = groups
            .iter()
            .copied()
            .map(|npi_key| vec![npi_key])
            .collect::<Vec<_>>();
        let plan = derive_npi_prefix_overrides(
            NpiPrefixInputs {
                set_base: 0,
                set_components: &[vec![0]],
                component_groups: std::slice::from_ref(&groups),
                set_patterns: &[vec![0]],
                pattern_groups: std::slice::from_ref(&groups),
                group_npis: &group_npis,
            },
            &options,
            &mut admission,
        )
        .unwrap();

        assert_eq!(plan.group_unsafe_set_count, 0);
        assert_eq!(plan.physical_unsafe_set_count, 1);
        assert_eq!(plan.maximum_group_npi_batch_work, 5);
        assert_eq!(plan.metadata.len(), 1);
        assert_eq!(plan.lists[0].len(), 129);
        assert!(
            plan.maximum_group_npi_member_work
                < options.max_online_group_npi_members_per_set as u64
        );
        assert!(plan.maximum_group_npi_byte_work < options.max_online_group_npi_bytes_per_set);
    }

    #[test]
    fn compiler_selects_pattern_and_emits_only_pattern_hot_relations() {
        let fixture = shared_pattern_fixture(64, 16);
        let summary = compile_provider_graph_v4(
            std::slice::from_ref(&fixture.shard),
            &fixture.provider_map,
            &fixture.output,
            ProviderGraphV4Options::default(),
        )
        .unwrap();
        assert_eq!(summary.selected_layout, ProviderGraphV4Layout::Pattern);
        assert!(summary.pattern_layout_serving_degree_eligible);
        assert_eq!(
            summary.max_set_patterns_per_set,
            DEFAULT_MAX_SET_PATTERNS_PER_SET as u64
        );
        assert_eq!(
            summary.max_set_components_per_fallback_set,
            DEFAULT_MAX_SET_COMPONENTS_PER_FALLBACK_SET as u64
        );
        assert_eq!(
            summary.max_online_group_keys_per_set,
            DEFAULT_MAX_ONLINE_GROUP_KEYS_PER_SET as u64
        );
        assert_eq!(
            summary.max_online_source_members_per_set,
            DEFAULT_MAX_ONLINE_SOURCE_MEMBERS_PER_SET as u64
        );
        assert_eq!(
            summary.max_online_source_pages_per_set,
            DEFAULT_MAX_ONLINE_SOURCE_PAGES_PER_SET as u64
        );
        assert_eq!(
            summary.online_group_npi_batch_size,
            DEFAULT_ONLINE_GROUP_NPI_BATCH_SIZE as u64
        );
        assert_eq!(
            summary.max_online_group_npi_members_per_set,
            DEFAULT_MAX_ONLINE_GROUP_NPI_MEMBERS_PER_SET as u64
        );
        assert_eq!(
            summary.max_online_group_npi_locator_pages_per_set,
            DEFAULT_MAX_ONLINE_GROUP_NPI_LOCATOR_PAGES_PER_SET as u64
        );
        assert_eq!(
            summary.max_online_group_npi_member_pages_per_set,
            DEFAULT_MAX_ONLINE_GROUP_NPI_MEMBER_PAGES_PER_SET as u64
        );
        assert_eq!(
            summary.max_online_group_npi_bytes_per_set,
            DEFAULT_MAX_ONLINE_GROUP_NPI_BYTES_PER_SET
        );
        assert_eq!(
            summary.max_online_group_npi_batches_per_set,
            DEFAULT_MAX_ONLINE_GROUP_NPI_BATCHES_PER_SET as u64
        );
        assert_eq!(
            summary.provider_expansion_rate_page_rows,
            DEFAULT_PROVIDER_EXPANSION_RATE_PAGE_ROWS as u64
        );
        assert_eq!(
            summary.max_online_provider_expansion_rate_rows,
            DEFAULT_MAX_ONLINE_PROVIDER_EXPANSION_RATE_ROWS as u64
        );
        assert_eq!(
            summary.max_online_provider_expansion_provider_sets,
            DEFAULT_MAX_ONLINE_PROVIDER_EXPANSION_PROVIDER_SETS as u64
        );
        assert_eq!(
            summary.max_online_provider_expansion_graph_batches,
            DEFAULT_MAX_ONLINE_PROVIDER_EXPANSION_GRAPH_BATCHES as u64
        );
        assert_eq!(summary.npi_prefix_target, DEFAULT_NPI_PREFIX_TARGET as u64);
        assert_eq!(summary.observe.maximum_patterns_per_set, 1);
        assert_eq!(summary.observe.maximum_components_per_set, 1);
        assert_eq!(summary.observe.pattern_overflow_set_count, 0);
        assert_eq!(
            summary.observe.maximum_components_per_pattern_overflow_set,
            0
        );
        assert_eq!(summary.observe.unsafe_pattern_component_set_count, 0);
        assert!(summary.pattern_complete_encoded_bytes < summary.direct_complete_encoded_bytes);
        assert_eq!(summary.observe.pattern_count, 1);
        assert_eq!(summary.observe.maximum_patterns_per_npi, 1);
        assert_eq!(summary.observe.npi_patterns_per_npi_p50, 1);
        assert_eq!(summary.observe.npi_patterns_per_npi_p95, 1);
        assert_eq!(summary.observe.npi_patterns_per_npi_p99, 1);
        assert_eq!(summary.observe.group_set_expansion_owner_visits, 64);
        assert_eq!(summary.observe.group_set_expansion_edge_visits, 16);
        assert!(
            summary.observe.group_set_expansion_edge_visits
                < summary.observe.group_set_incidence_count
        );
        assert_eq!(summary.observe.set_group_expansion_owner_visits, 0);
        assert_eq!(summary.observe.set_group_expansion_edge_visits, 0);
        assert_eq!(summary.observe.single_component_group_fast_path_count, 64);
        assert_eq!(summary.observe.multi_component_group_union_count, 0);
        assert_eq!(summary.observe.maximum_groups_per_set_computed, 0);
        assert_eq!(summary.observe.npi_prefix_simulated_set_count, 16);
        assert!(summary.observe.npi_prefix_group_merge_member_visits > 0);
        assert_eq!(summary.observe.npi_prefix_override_owner_count, 0);
        assert_eq!(summary.observe.maximum_online_group_npi_batch_work, 1);
        assert_eq!(summary.observe.maximum_online_group_npi_member_work, 2);
        assert_eq!(summary.observe.provider_set_audit_npi_count, 16);
        assert_eq!(summary.resource_admission.factor_edge_count, 208);
        assert!(summary.resource_admission.estimated_peak_bytes > 0);
        assert_eq!(
            summary.resource_admission.estimated_peak_bytes,
            summary
                .resource_admission
                .base_estimated_model_bytes
                .checked_add(summary.resource_admission.derived_projection_bytes)
                .and_then(|value| {
                    value.checked_add(summary.resource_admission.retained_scratch_high_water_bytes)
                })
                .and_then(|value| {
                    value.checked_add(summary.resource_admission.bounded_emission_buffer_bytes)
                })
                .unwrap()
        );
        assert!(
            summary.resource_admission.derived_projection_bytes
                < summary.observe.group_set_incidence_count * 8,
            "shared incidence was charged like the flat expansion: derived={} flat={}",
            summary.resource_admission.derived_projection_bytes,
            summary.observe.group_set_incidence_count * 8
        );
        assert!(summary.provider_set_audit_npi_copy_path.is_file());
        assert!(summary.provider_set_npi_prefix_override_copy_path.is_file());
        assert!(summary.pattern_copy_path.as_ref().unwrap().is_file());
        let kinds = reference_kinds(&summary.reference_manifest_path);
        assert!(kinds
            .iter()
            .any(|kind| kind == "v4_group_patterns_members_v1"));
        assert!(kinds
            .iter()
            .any(|kind| kind == "v4_npi_groups_exact_members_v1"));
        assert!(!kinds.iter().any(|kind| kind.contains("sets_direct")));

        let references = fs::read_to_string(&summary.reference_manifest_path).unwrap();
        let locator: Value = references
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .find(|value: &Value| value["object_kind"] == "v4_set_components_locators_v1")
            .unwrap();
        assert_eq!(locator["block_key"], 1);
        assert_eq!(locator["owner_base"], 1);
        assert_eq!(locator["owner_count"], 16);

        let mut copy = Vec::new();
        File::open(&summary.block_copy_path)
            .unwrap()
            .read_to_end(&mut copy)
            .unwrap();
        assert_eq!(&copy[..PG_COPY_HEADER.len()], PG_COPY_HEADER);
        let mut offset = PG_COPY_HEADER.len();
        assert_eq!(
            i16::from_be_bytes(copy[offset..offset + 2].try_into().unwrap()),
            10
        );
        offset += 2;
        let hash_len = i32::from_be_bytes(copy[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4 + hash_len;
        let version_len = i32::from_be_bytes(copy[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        assert_eq!(version_len, 2);
        assert_eq!(
            i16::from_be_bytes(copy[offset..offset + 2].try_into().unwrap()),
            SHARED_FORMAT_VERSION
        );

        let audit = fs::read(&summary.provider_set_audit_npi_copy_path).unwrap();
        let mut audit_offset = PG_COPY_HEADER.len();
        assert_eq!(
            i16::from_be_bytes(audit[audit_offset..audit_offset + 2].try_into().unwrap()),
            3
        );
        audit_offset += 2;
        let mut audit_fields = Vec::new();
        for _ in 0..3 {
            let length =
                i32::from_be_bytes(audit[audit_offset..audit_offset + 4].try_into().unwrap())
                    as usize;
            audit_offset += 4;
            audit_fields.push(&audit[audit_offset..audit_offset + length]);
            audit_offset += length;
        }
        assert_eq!(i32::from_be_bytes(audit_fields[0].try_into().unwrap()), 1);
        assert_eq!(i32::from_be_bytes(audit_fields[1].try_into().unwrap()), 0);
        assert_eq!(
            i64::from_be_bytes(audit_fields[2].try_into().unwrap()),
            1_234_567_890
        );
    }

    #[test]
    fn compiler_selects_direct_for_unshared_incidence_and_direct_wins_ties() {
        let fixture = independent_fixture();
        let summary = compile_provider_graph_v4(
            std::slice::from_ref(&fixture.shard),
            &fixture.provider_map,
            &fixture.output,
            ProviderGraphV4Options::default(),
        )
        .unwrap();
        assert_eq!(summary.selected_layout, ProviderGraphV4Layout::Direct);
        assert!(summary.direct_complete_encoded_bytes < summary.pattern_complete_encoded_bytes);
        assert!(summary.pattern_copy_path.is_none());
        let kinds = reference_kinds(&summary.reference_manifest_path);
        assert!(kinds
            .iter()
            .any(|kind| kind == "v4_group_sets_direct_members_v1"));
        assert!(!kinds.iter().any(|kind| kind.contains("group_patterns")));
        assert_eq!(choose_layout(100, 100, true), ProviderGraphV4Layout::Direct);
        assert_eq!(summary.observe.group_set_expansion_owner_visits, 2);
        assert_eq!(summary.observe.group_set_expansion_edge_visits, 2);
        assert_eq!(summary.observe.direct_group_set_emission_owner_visits, 2);
        assert_eq!(summary.observe.direct_group_set_emission_edge_visits, 2);
        assert_eq!(summary.observe.set_group_expansion_owner_visits, 2);
        assert_eq!(summary.observe.set_group_expansion_edge_visits, 2);
        assert_eq!(summary.observe.maximum_groups_per_set_computed, 1);
        assert_eq!(summary.observe.maximum_groups_per_set, 1);
    }

    #[test]
    fn pattern_layout_requires_one_bounded_first_hop_per_set() {
        assert_eq!(
            choose_layout(1_000_000_000, 1_000_000, false),
            ProviderGraphV4Layout::Direct,
            "a smaller pattern projection is unsafe when neither exact first hop is bounded"
        );
        assert_eq!(
            choose_layout(1_000_000_000, 1_000_000, true),
            ProviderGraphV4Layout::Pattern,
        );
    }

    #[test]
    fn compiler_keeps_pattern_layout_when_only_overflow_sets_use_components() {
        let safe_fixture = mixed_pattern_component_fixture(128);
        let safe = compile_provider_graph_v4(
            std::slice::from_ref(&safe_fixture.shard),
            &safe_fixture.provider_map,
            &safe_fixture.output,
            ProviderGraphV4Options {
                max_set_patterns_per_set: 1,
                max_set_components_per_fallback_set: 2,
                ..ProviderGraphV4Options::default()
            },
        )
        .unwrap();
        assert_eq!(
            safe.selected_layout,
            ProviderGraphV4Layout::Pattern,
            "direct={} pattern={} eligible={}",
            safe.direct_complete_encoded_bytes,
            safe.pattern_complete_encoded_bytes,
            safe.pattern_layout_serving_degree_eligible,
        );
        assert!(safe.pattern_layout_serving_degree_eligible);
        assert_eq!(safe.observe.maximum_patterns_per_set, 2);
        assert_eq!(safe.observe.maximum_components_per_set, 2);
        assert_eq!(safe.observe.pattern_overflow_set_count, 1);
        assert_eq!(safe.observe.maximum_components_per_pattern_overflow_set, 2);
        assert_eq!(safe.observe.unsafe_pattern_component_set_count, 0);

        let unsafe_fixture = mixed_pattern_component_fixture(128);
        let unsafe_summary = compile_provider_graph_v4(
            std::slice::from_ref(&unsafe_fixture.shard),
            &unsafe_fixture.provider_map,
            &unsafe_fixture.output,
            ProviderGraphV4Options {
                max_set_patterns_per_set: 1,
                max_set_components_per_fallback_set: 1,
                ..ProviderGraphV4Options::default()
            },
        )
        .unwrap();
        assert_eq!(
            unsafe_summary.selected_layout,
            ProviderGraphV4Layout::Direct
        );
        assert!(!unsafe_summary.pattern_layout_serving_degree_eligible);
        assert_eq!(unsafe_summary.observe.unsafe_pattern_component_set_count, 1);
    }

    #[test]
    fn compiler_is_deterministic_for_identical_factor_inputs() {
        let fixture = shared_pattern_fixture(128, 8);
        let output_two = fixture._temporary.path().join("output-two");
        let options = ProviderGraphV4Options {
            member_page_bytes: 64,
            heavy_owner_member_threshold: 1,
            heavy_bitmap_minimum_savings_bytes: 0,
            ..ProviderGraphV4Options::default()
        };
        let first = compile_provider_graph_v4(
            std::slice::from_ref(&fixture.shard),
            &fixture.provider_map,
            &fixture.output,
            options.clone(),
        )
        .unwrap();
        let second = compile_provider_graph_v4(
            std::slice::from_ref(&fixture.shard),
            &fixture.provider_map,
            &output_two,
            options,
        )
        .unwrap();
        assert_eq!(
            fs::read(first.block_copy_path).unwrap(),
            fs::read(second.block_copy_path).unwrap()
        );
        assert_eq!(
            fs::read(first.reference_manifest_path).unwrap(),
            fs::read(second.reference_manifest_path).unwrap()
        );
        assert_eq!(first.input_sha256, second.input_sha256);
        assert_eq!(first.heavy_bitmaps, second.heavy_bitmaps);
        assert!(!first.heavy_bitmaps.is_empty());
    }

    #[test]
    fn heavy_bitmaps_are_exact_and_only_selected_when_dense_and_smaller() {
        let options = ProviderGraphV4Options::default();
        let dense: Vec<u32> = (10_000..15_000).collect();
        let bitmap = maybe_heavy_bitmap("npi_groups_exact", 7, &dense, &options)
            .unwrap()
            .unwrap();
        assert_eq!(bitmap.member_count, dense.len() as u64);
        assert_eq!(bitmap.member_base, 10_000);
        assert!(bitmap.raw_byte_count + 512 <= bitmap.vector_byte_count);
        assert_eq!(
            bitmap.raw_byte_count,
            24 + 625 + HEAVY_BITMAP_FRAGMENT_HEADER_BYTES as u64
        );
        let sparse: Vec<u32> = (0..5_000).map(|value| value * 100_000).collect();
        assert!(maybe_heavy_bitmap("npi_groups_exact", 7, &sparse, &options)
            .unwrap()
            .is_none());
    }

    #[test]
    fn compiler_heavy_bitmap_fragments_carry_physical_member_counts() {
        let fixture = shared_pattern_fixture(512, 16);
        let options = ProviderGraphV4Options {
            member_page_bytes: 64,
            heavy_owner_member_threshold: 1,
            heavy_bitmap_minimum_savings_bytes: 0,
            ..ProviderGraphV4Options::default()
        };
        let summary = compile_provider_graph_v4(
            std::slice::from_ref(&fixture.shard),
            &fixture.provider_map,
            &fixture.output,
            options,
        )
        .unwrap();
        let heavy = summary
            .heavy_bitmaps
            .iter()
            .find(|bitmap| bitmap.relation == "npi_groups_exact" && bitmap.owner_key == 0)
            .unwrap();
        assert!(heavy.block_count > 1);
        let references: Vec<Value> =
            BufReader::new(File::open(&summary.reference_manifest_path).unwrap())
                .lines()
                .map(|line| serde_json::from_str(&line.unwrap()).unwrap())
                .filter(|value: &Value| {
                    value["object_kind"] == heavy.object_kind
                        && value["block_key"].as_u64() == Some(u64::from(heavy.owner_key))
                })
                .collect();
        assert_eq!(references.len() as u64, heavy.block_count);
        assert_eq!(
            references
                .iter()
                .map(|value| value["entry_count"].as_u64().unwrap())
                .sum::<u64>(),
            heavy.member_count
        );
        assert_eq!(references[0]["entry_count"].as_u64(), Some(64));
        assert!(references[1..]
            .iter()
            .all(|value| value["entry_count"].as_u64().unwrap() > 0));
        assert!(references
            .iter()
            .all(|value| value["raw_byte_count"].as_u64().unwrap() <= 64));
        assert!(summary.heavy_bitmaps.len() >= 3);
        assert!(
            summary
                .heavy_bitmaps
                .iter()
                .map(|bitmap| bitmap.raw_byte_count)
                .sum::<u64>()
                > 64,
            "fixture must exceed the one-page streaming bound in aggregate"
        );
        let bitmap_payloads = copy_payloads_for_kind(&summary.block_copy_path, &heavy.object_kind);
        assert!(bitmap_payloads.iter().all(|payload| payload.len() <= 64));
        let bitmap_payload = bitmap_payloads
            .iter()
            .flat_map(|payload| {
                payload[HEAVY_BITMAP_FRAGMENT_HEADER_BYTES..]
                    .iter()
                    .copied()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            bitmap_payloads.iter().map(Vec::len).sum::<usize>() as u64,
            heavy.raw_byte_count
        );
        assert_eq!(&bitmap_payload[..8], b"PTG2V4BM");
        assert_eq!(
            bitmap_payload[HEAVY_BITMAP_HEADER_BYTES..]
                .iter()
                .map(|byte| byte.count_ones())
                .sum::<u32>(),
            512
        );
        let relation = summary
            .relation_summaries
            .iter()
            .find(|relation| relation.relation == "npi_groups_exact")
            .unwrap();
        assert_eq!(relation.logical_member_count, 512);
        assert_eq!(relation.vector_member_count, 0);
        assert!(summary.selected_encoded_bytes < summary.pattern_complete_encoded_bytes);
        assert!(!reference_kinds(&summary.reference_manifest_path)
            .iter()
            .any(|kind| kind == "v4_npi_groups_exact_members_v1"));
        let locator_payloads =
            copy_payloads_for_kind(&summary.block_copy_path, "v4_npi_groups_exact_locators_v1");
        assert_eq!(locator_payloads.len(), 1);
        assert_eq!(
            u64::from_le_bytes(locator_payloads[0][..8].try_into().unwrap()),
            0
        );
        assert_eq!(
            u32::from_le_bytes(locator_payloads[0][8..12].try_into().unwrap()),
            0
        );
    }

    #[test]
    fn bitmap_fragment_hash_commits_identical_head_body_content_to_entry_count() {
        let content = vec![0x5a; 32];
        let framed = |fragment_no: u32, entry_count: u32| {
            let mut payload = vec![0; HEAVY_BITMAP_FRAGMENT_HEADER_BYTES];
            payload[..8].copy_from_slice(HEAVY_BITMAP_FRAGMENT_MAGIC);
            payload[8..12].copy_from_slice(&7u32.to_le_bytes());
            payload[12..16].copy_from_slice(&100u32.to_le_bytes());
            payload[16..20].copy_from_slice(&256u32.to_le_bytes());
            payload[20..24].copy_from_slice(&9u32.to_le_bytes());
            payload[24..28].copy_from_slice(&fragment_no.to_le_bytes());
            payload[28..32].copy_from_slice(&entry_count.to_le_bytes());
            payload.extend_from_slice(&content);
            payload
        };
        let head = framed(0, 1);
        let body = framed(1, 8);
        assert_eq!(
            &head[HEAVY_BITMAP_FRAGMENT_HEADER_BYTES..],
            &body[HEAVY_BITMAP_FRAGMENT_HEADER_BYTES..],
            "the regression requires deliberately identical logical fragment content"
        );
        assert_ne!(
            shared_block_hash("v4_test_heavy_bitmap_v1", "none", &head).unwrap(),
            shared_block_hash("v4_test_heavy_bitmap_v1", "none", &body).unwrap(),
            "payload-derived fragment metadata must prevent a CAS entry-count alias"
        );
    }

    #[test]
    fn many_page_reference_spooling_is_bounded_and_low_budget_is_rejected() {
        let fixture = shared_pattern_fixture(512, 16);
        let options = ProviderGraphV4Options {
            member_page_bytes: 4,
            ..ProviderGraphV4Options::default()
        };
        let summary = compile_provider_graph_v4(
            std::slice::from_ref(&fixture.shard),
            &fixture.provider_map,
            &fixture.output,
            options.clone(),
        )
        .unwrap();
        assert!(summary.block_count > 1_000);
        assert!(
            summary.resource_admission.bounded_emission_buffer_bytes >= REFERENCE_SPOOL_FIXED_BYTES
        );

        let limited_output = fixture._temporary.path().join("limited-output");
        let limited = ProviderGraphV4Options {
            max_estimated_model_bytes: Some(
                summary
                    .resource_admission
                    .estimated_peak_bytes
                    .checked_sub(1)
                    .unwrap(),
            ),
            ..options
        };
        let error = compile_provider_graph_v4(
            std::slice::from_ref(&fixture.shard),
            &fixture.provider_map,
            &limited_output,
            limited,
        )
        .unwrap_err();
        assert!(error.to_string().contains("resource_admission"));
        assert!(!limited_output.join("v4-graph-blocks.copy").exists());
    }

    #[test]
    fn compiler_progress_protocol_is_ordered_and_terminal() {
        let fixture = shared_pattern_fixture(64, 16);
        let mut events = Vec::new();
        {
            let mut sink = |event: &V4ProgressEvent| events.push(event.clone());
            compile_provider_graph_v4_with_progress(
                std::slice::from_ref(&fixture.shard),
                &fixture.provider_map,
                &fixture.output,
                ProviderGraphV4Options::default(),
                &mut sink,
            )
            .unwrap();
        }
        assert!(!events.is_empty());
        assert!(events.iter().all(|event| event.done <= event.total));
        assert!(events.windows(2).all(|pair| pair[1].seq == pair[0].seq + 1));
        for (phase, unit) in [
            ("load_factors", "factor_edges"),
            ("build_model", "factor_items"),
        ] {
            let phase_events = events
                .iter()
                .filter(|event| event.phase == phase)
                .collect::<Vec<_>>();
            assert!(phase_events.len() > 2, "{phase} progress stayed static");
            let total = phase_events[0].total;
            assert!(total > 1);
            assert!(phase_events
                .iter()
                .all(|event| event.total == total && event.unit == unit));
            assert_eq!(phase_events[0].done, 0);
            assert!(phase_events
                .iter()
                .any(|event| event.done > 0 && event.done < total));
            assert_eq!(phase_events.last().unwrap().done, total);
        }
        let phases = [
            "resource_admission",
            "load_factors",
            "build_model",
            "derive_patterns",
            "derive_npi_patterns",
            "select_layout",
            "emit_relations",
            "emit_bitmaps",
            "emit_dictionaries",
            "complete",
        ];
        let mut previous_phase = 0usize;
        let mut previous_done = 0u64;
        for event in &events {
            let phase = phases
                .iter()
                .position(|phase| *phase == event.phase)
                .unwrap();
            assert!(phase >= previous_phase);
            if phase == previous_phase {
                assert!(event.done >= previous_done);
            } else {
                previous_phase = phase;
            }
            previous_done = event.done;
        }
        let terminal = events.last().unwrap();
        assert_eq!(terminal.version, PROGRESS_VERSION);
        assert_eq!(terminal.phase, "complete");
        assert!(terminal.terminal);
        assert_eq!((terminal.done, terminal.total), (1, 1));
    }

    #[test]
    fn compiler_rejects_nonreciprocal_npi_group_input() {
        let mut fixture = independent_fixture();
        fixture.shard.provider_npi_group = write_membership(
            &fixture._temporary.path().join("wrong-npi-group.sidecar"),
            "shard-b",
            "provider_npi_group",
            [(npi(1_111_111_111), global(3, 2))],
            true,
        );
        let error = compile_provider_graph_v4(
            std::slice::from_ref(&fixture.shard),
            &fixture.provider_map,
            &fixture.output,
            ProviderGraphV4Options::default(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("not reciprocal"));
        assert!(!fixture.output.join("v4-graph-blocks.copy").exists());
    }

    #[test]
    fn compiler_rejects_missing_authoritative_set_factors() {
        let mut fixture = independent_fixture();
        fixture.shard.provider_set_component = write_membership(
            &fixture
                ._temporary
                .path()
                .join("incomplete-set-component.sidecar"),
            "shard-b",
            "provider_set_component",
            [(global(1, 1), global(2, 1))],
            false,
        );
        let error = compile_provider_graph_v4(
            std::slice::from_ref(&fixture.shard),
            &fixture.provider_map,
            &fixture.output,
            ProviderGraphV4Options::default(),
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("authoritative provider set has no components"));
        assert!(!fixture.output.join("v4-graph-blocks.copy").exists());
    }

    #[test]
    fn compiler_rejects_missing_referenced_component_factors() {
        let mut fixture = independent_fixture();
        fixture.shard.provider_component_group = write_membership(
            &fixture
                ._temporary
                .path()
                .join("incomplete-component-group.sidecar"),
            "shard-b",
            "provider_component_group",
            [(global(2, 1), global(3, 1))],
            false,
        );
        let error = compile_provider_graph_v4(
            std::slice::from_ref(&fixture.shard),
            &fixture.provider_map,
            &fixture.output,
            ProviderGraphV4Options::default(),
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("referenced component has no groups"));
        assert!(!fixture.output.join("v4-graph-blocks.copy").exists());
    }

    #[test]
    fn compiler_accepts_complete_tin_only_groups_without_npi_edges() {
        let mut fixture = independent_fixture();
        fixture.shard.provider_group_npi = write_membership(
            &fixture._temporary.path().join("empty-group-npi.sidecar"),
            "shard-b",
            "provider_group_npi",
            std::iter::empty::<(GlobalId, GlobalId)>(),
            false,
        );
        fixture.shard.provider_npi_group = write_membership(
            &fixture._temporary.path().join("empty-npi-group.sidecar"),
            "shard-b",
            "provider_npi_group",
            std::iter::empty::<(GlobalId, GlobalId)>(),
            false,
        );
        let summary = compile_provider_graph_v4(
            std::slice::from_ref(&fixture.shard),
            &fixture.provider_map,
            &fixture.output,
            ProviderGraphV4Options::default(),
        )
        .unwrap();
        assert_eq!(summary.observe.group_count, 2);
        assert_eq!(summary.observe.npi_count, 0);
        assert_eq!(summary.observe.group_npi_edge_count, 0);
        assert_eq!(summary.observe.provider_set_audit_npi_count, 0);
    }

    #[test]
    fn derived_projection_is_admitted_after_factor_preflight() {
        let fixture = shared_pattern_fixture(64, 16);
        let baseline = resource_admission_preflight(
            std::slice::from_ref(&fixture.shard),
            &fixture.provider_map,
            &ProviderGraphV4Options::default(),
        )
        .unwrap();
        let limit = baseline
            .summary
            .estimated_peak_bytes
            .checked_add(1)
            .unwrap();
        let options = ProviderGraphV4Options {
            max_estimated_model_bytes: Some(limit),
            ..ProviderGraphV4Options::default()
        };
        assert!(resource_admission_preflight(
            std::slice::from_ref(&fixture.shard),
            &fixture.provider_map,
            &options,
        )
        .is_ok());
        let error = compile_provider_graph_v4(
            std::slice::from_ref(&fixture.shard),
            &fixture.provider_map,
            &fixture.output,
            options,
        )
        .unwrap_err();
        let message = error.to_string();
        assert!(message.contains("derived resident bytes"), "{message}");
        assert!(!fixture.output.join("v4-graph-blocks.copy").exists());
    }

    fn descriptor_for_bytes(
        path: &Path,
        bytes: &[u8],
        record_format: &str,
        owner_count: u64,
        member_count: u64,
        member_global_count: Option<u64>,
    ) -> V4MembershipArtifactDescriptor {
        fs::write(path, bytes).unwrap();
        V4MembershipArtifactDescriptor {
            path: path.to_path_buf(),
            metadata: V4MembershipMetadata {
                record_format: record_format.to_owned(),
                sha256: hex(&Sha256::digest(bytes)),
                byte_count: bytes.len() as u64,
                owner_count,
                member_count,
                member_global_count,
                name: Some("coverage".to_owned()),
                source_shard_id: Some("coverage-shard".to_owned()),
                shard_id: None,
            },
        }
    }

    fn standard_bytes(entries: &[(GlobalId, u64, u32)], members: &[GlobalId]) -> Vec<u8> {
        let mut bytes = STANDARD_MAGIC.to_vec();
        bytes.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
        bytes.extend_from_slice(&(entries.len() as u64).to_le_bytes());
        for (owner, offset, count) in entries {
            bytes.extend_from_slice(owner);
            bytes.extend_from_slice(&offset.to_le_bytes());
            bytes.extend_from_slice(&count.to_le_bytes());
        }
        for member in members {
            bytes.extend_from_slice(member);
        }
        bytes
    }

    fn dense_bytes(
        entries: &[(GlobalId, u64, u32)],
        dictionary: &[GlobalId],
        local_members: &[u32],
    ) -> Vec<u8> {
        let mut bytes = DENSE_MAGIC.to_vec();
        bytes.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
        bytes.extend_from_slice(&(entries.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&(dictionary.len() as u64).to_le_bytes());
        for (owner, offset, count) in entries {
            bytes.extend_from_slice(owner);
            bytes.extend_from_slice(&offset.to_le_bytes());
            bytes.extend_from_slice(&count.to_le_bytes());
        }
        for member in dictionary {
            bytes.extend_from_slice(member);
        }
        for member in local_members {
            bytes.extend_from_slice(&member.to_le_bytes());
        }
        bytes
    }

    #[test]
    fn membership_validation_and_error_contract_cover_every_fail_closed_shape() {
        let temporary = tempfile::tempdir().unwrap();
        let mut ordinal = 0usize;
        let mut open = |bytes: Vec<u8>, format: &str, owners, members, globals| {
            ordinal += 1;
            let path = temporary.path().join(format!("artifact-{ordinal}"));
            let descriptor = descriptor_for_bytes(&path, &bytes, format, owners, members, globals);
            ValidatedArtifact::open(&descriptor)
        };

        let io_error = ProviderGraphV4Error::from(io::Error::other("io-error"));
        assert_eq!(io_error.to_string(), "io-error");
        assert!(io_error.source().is_some());
        let json_error =
            ProviderGraphV4Error::from(serde_json::from_slice::<Value>(b"not-json").unwrap_err());
        assert!(json_error.source().is_some());
        let invalid_error = invalid(String::from("invalid-data"));
        assert_eq!(invalid_error.to_string(), "invalid-data");
        assert!(invalid_error.source().is_none());

        for options in [
            ProviderGraphV4Options {
                member_page_bytes: 3,
                ..ProviderGraphV4Options::default()
            },
            ProviderGraphV4Options {
                locator_page_bytes: 11,
                ..ProviderGraphV4Options::default()
            },
            ProviderGraphV4Options {
                heavy_owner_member_threshold: 0,
                ..ProviderGraphV4Options::default()
            },
            ProviderGraphV4Options {
                max_set_patterns_per_set: 0,
                ..ProviderGraphV4Options::default()
            },
            ProviderGraphV4Options {
                max_online_source_owners_per_set: 0,
                ..ProviderGraphV4Options::default()
            },
            ProviderGraphV4Options {
                max_online_source_members_per_set: 0,
                ..ProviderGraphV4Options::default()
            },
            ProviderGraphV4Options {
                max_online_source_pages_per_set: 0,
                ..ProviderGraphV4Options::default()
            },
            ProviderGraphV4Options {
                max_online_source_bytes_per_set: 0,
                ..ProviderGraphV4Options::default()
            },
            ProviderGraphV4Options {
                online_group_npi_batch_size: 0,
                ..ProviderGraphV4Options::default()
            },
            ProviderGraphV4Options {
                max_online_group_npi_members_per_set: 0,
                ..ProviderGraphV4Options::default()
            },
            ProviderGraphV4Options {
                max_online_group_npi_locator_pages_per_set: 0,
                ..ProviderGraphV4Options::default()
            },
            ProviderGraphV4Options {
                max_online_group_npi_member_pages_per_set: 0,
                ..ProviderGraphV4Options::default()
            },
            ProviderGraphV4Options {
                max_online_group_npi_bytes_per_set: 0,
                ..ProviderGraphV4Options::default()
            },
            ProviderGraphV4Options {
                max_online_group_npi_batches_per_set: 0,
                ..ProviderGraphV4Options::default()
            },
            ProviderGraphV4Options {
                provider_expansion_rate_page_rows: 0,
                ..ProviderGraphV4Options::default()
            },
            ProviderGraphV4Options {
                max_online_provider_expansion_rate_rows: 0,
                ..ProviderGraphV4Options::default()
            },
            ProviderGraphV4Options {
                max_online_provider_expansion_provider_sets: 0,
                ..ProviderGraphV4Options::default()
            },
            ProviderGraphV4Options {
                max_online_provider_expansion_graph_batches: 0,
                ..ProviderGraphV4Options::default()
            },
            ProviderGraphV4Options {
                max_estimated_model_bytes: Some(0),
                ..ProviderGraphV4Options::default()
            },
            ProviderGraphV4Options {
                max_factor_edges: Some(0),
                ..ProviderGraphV4Options::default()
            },
        ] {
            assert!(options.validate().is_err());
        }

        let missing = V4MembershipArtifactDescriptor {
            path: temporary.path().join("missing"),
            metadata: V4MembershipMetadata {
                record_format: STANDARD_FORMAT.to_owned(),
                sha256: "00".repeat(32),
                byte_count: 0,
                owner_count: 0,
                member_count: 0,
                member_global_count: None,
                name: None,
                source_shard_id: None,
                shard_id: None,
            },
        };
        assert!(ValidatedArtifact::open(&missing)
            .err()
            .unwrap()
            .to_string()
            .contains("unavailable"));

        let empty_standard = standard_bytes(&[], &[]);
        let path = temporary.path().join("wrong-size");
        let mut wrong_size =
            descriptor_for_bytes(&path, &empty_standard, STANDARD_FORMAT, 0, 0, None);
        wrong_size.metadata.byte_count += 1;
        assert!(ValidatedArtifact::open(&wrong_size).is_err());
        let mut wrong_digest = wrong_size.clone();
        wrong_digest.metadata.byte_count -= 1;
        wrong_digest.metadata.sha256 = "00".repeat(32);
        assert!(ValidatedArtifact::open(&wrong_digest).is_err());

        assert!(open(Vec::new(), STANDARD_FORMAT, 0, 0, None).is_err());
        assert!(open(b"BADMAGIC".to_vec(), STANDARD_FORMAT, 0, 0, None).is_err());
        assert!(open(STANDARD_MAGIC.to_vec(), STANDARD_FORMAT, 0, 0, None).is_err());
        assert!(open(empty_standard.clone(), "wrong-format", 0, 0, None).is_err());
        let mut bad_version = empty_standard.clone();
        bad_version[8..12].copy_from_slice(&2u32.to_le_bytes());
        assert!(open(bad_version, STANDARD_FORMAT, 0, 0, None).is_err());

        let empty_dense = dense_bytes(&[], &[], &[]);
        assert!(open(DENSE_MAGIC.to_vec(), DENSE_FORMAT, 0, 0, Some(0)).is_err());
        assert!(open(empty_dense.clone(), "wrong-format", 0, 0, Some(0)).is_err());
        let mut bad_dense_version = empty_dense.clone();
        bad_dense_version[8..12].copy_from_slice(&2u32.to_le_bytes());
        assert!(open(bad_dense_version, DENSE_FORMAT, 0, 0, Some(0)).is_err());
        assert!(open(empty_dense.clone(), DENSE_FORMAT, 1, 0, Some(0)).is_err());
        assert!(open(empty_dense.clone(), DENSE_FORMAT, 0, 0, Some(1)).is_err());

        let owner = global(1, 1);
        let member = global(2, 1);
        assert!(open(
            standard_bytes(&[(owner, 0, 1)], &[]),
            STANDARD_FORMAT,
            1,
            0,
            None,
        )
        .is_err());
        assert!(open(
            dense_bytes(&[], &[member, member], &[]),
            DENSE_FORMAT,
            0,
            0,
            Some(2),
        )
        .is_err());
        assert!(open(
            standard_bytes(&[(owner, 0, 0), (owner, 0, 0)], &[]),
            STANDARD_FORMAT,
            2,
            0,
            None,
        )
        .is_err());
        assert!(open(
            standard_bytes(&[(owner, 1, 0)], &[]),
            STANDARD_FORMAT,
            1,
            0,
            None,
        )
        .is_err());
        assert!(open(
            standard_bytes(&[(owner, 0, 2)], &[member, member]),
            STANDARD_FORMAT,
            1,
            2,
            None,
        )
        .is_err());
        assert!(open(
            standard_bytes(&[(owner, 0, 0)], &[member]),
            STANDARD_FORMAT,
            1,
            1,
            None,
        )
        .is_err());
        assert!(open(
            dense_bytes(&[(owner, 0, 1)], &[member], &[1]),
            DENSE_FORMAT,
            1,
            1,
            Some(1),
        )
        .is_err());

        let valid_path = temporary.path().join("valid-empty");
        let valid_descriptor =
            descriptor_for_bytes(&valid_path, &empty_standard, STANDARD_FORMAT, 0, 0, None);
        let valid = ValidatedArtifact::open(&valid_descriptor).unwrap();
        assert!(valid.owner(0).is_err());
        assert!(valid.member_global(0).is_err());
        assert!(valid.dictionary_global(0).is_err());
        assert!(read_u32_le(&[], 0).is_err());
        assert!(read_u64_le(&[], 0).is_err());
        assert!(read_global_id(&[], 0).is_err());
    }

    #[test]
    fn provider_set_map_rejects_every_ambiguous_coordinate_shape() {
        let temporary = tempfile::tempdir().unwrap();
        let mut ordinal = 0usize;
        let mut read = |payload: &str| {
            ordinal += 1;
            let path = temporary.path().join(format!("map-{ordinal}"));
            fs::write(&path, payload).unwrap();
            ProviderSetMap::read(&path)
        };
        for payload in [
            "",
            "not-hex\t0\n",
            "00000000000000000000000000000001\n",
            "00000000000000000000000000000001\tbad\n",
            "00000000000000000000000000000001\t0\textra\n",
            concat!(
                "00000000000000000000000000000001\t0\n",
                "00000000000000000000000000000001\t1\n"
            ),
            "00000000000000000000000000000001\t2\n",
            concat!(
                "00000000000000000000000000000001\t0\n",
                "00000000000000000000000000000002\t2\n"
            ),
        ] {
            assert!(read(payload).is_err(), "accepted map: {payload:?}");
        }
        let map = read(concat!(
            "00000000000000000000000000000001\t1\n",
            "00000000000000000000000000000002\t2\n"
        ))
        .unwrap();
        assert!(map.key(global(9, 9)).is_err());
        assert!(map.index(0).is_err());
        assert!(map.index(3).is_err());
    }

    #[test]
    fn multi_component_and_empty_incidence_groups_preserve_exact_unions() {
        let temporary = tempfile::tempdir().unwrap();
        let sets = [global(1, 1), global(1, 2)];
        let components = [global(2, 1), global(2, 2), global(2, 3)];
        let groups = [global(3, 1), global(3, 2), global(3, 3)];
        let provider_npi = npi(1_234_567_890);
        let shard = V4ProviderGraphShardDescriptor {
            shard_id: "multi-shard".to_owned(),
            provider_set_component: write_membership(
                &temporary.path().join("set-component.sidecar"),
                "multi-shard",
                "provider_set_component",
                [(sets[0], components[0]), (sets[1], components[1])],
                true,
            ),
            provider_component_group: write_membership(
                &temporary.path().join("component-group.sidecar"),
                "multi-shard",
                "provider_component_group",
                [
                    (components[0], groups[0]),
                    (components[0], groups[1]),
                    (components[1], groups[0]),
                    (components[2], groups[2]),
                ],
                true,
            ),
            provider_group_npi: write_membership(
                &temporary.path().join("group-npi.sidecar"),
                "multi-shard",
                "provider_group_npi",
                groups.into_iter().map(|group| (group, provider_npi)),
                true,
            ),
            provider_npi_group: write_membership(
                &temporary.path().join("npi-group.sidecar"),
                "multi-shard",
                "provider_npi_group",
                groups.into_iter().map(|group| (provider_npi, group)),
                true,
            ),
        };
        let provider_map = temporary.path().join("provider-map.copy");
        write_provider_map(&provider_map, &sets, 0);
        let mut sink = |_event: &V4ProgressEvent| {};
        let mut progress = ProgressReporter::new(&mut sink);
        let raw = load_raw_factors(std::slice::from_ref(&shard), &mut progress).unwrap();
        let provider_sets = ProviderSetMap::read(&provider_map).unwrap();
        let mut admission = resource_admission_preflight(
            std::slice::from_ref(&shard),
            &provider_map,
            &ProviderGraphV4Options::default(),
        )
        .unwrap();
        let model = build_graph_model(
            &raw,
            &provider_sets,
            &mut progress,
            &mut admission,
            &ProviderGraphV4Options::default(),
        )
        .unwrap();

        assert_eq!(model.observe.multi_component_group_count, 1);
        assert_eq!(model.observe.multi_component_group_union_count, 1);
        assert_eq!(model.observe.empty_incidence_group_count, 1);
        assert_eq!(
            model.pattern_sets[model.group_patterns[0] as usize],
            vec![0, 1]
        );
        assert!(model.pattern_sets[model.group_patterns[2] as usize].is_empty());
        assert_eq!(model.provider_set_audit_npis.len(), 2);
    }

    #[test]
    fn factor_loading_and_resource_admission_reject_ambiguous_metadata() {
        let fixture = independent_fixture();
        let mut sink = |_event: &V4ProgressEvent| {};

        let mut progress = ProgressReporter::new(&mut sink);
        assert!(load_raw_factors(&[], &mut progress).is_err());
        assert!(resource_admission_preflight(
            &[],
            &fixture.provider_map,
            &ProviderGraphV4Options::default(),
        )
        .is_err());

        let mut blank = fixture.shard.clone();
        blank.shard_id = "   ".to_owned();
        let mut progress = ProgressReporter::new(&mut sink);
        assert!(load_raw_factors(&[blank], &mut progress).is_err());

        let mut progress = ProgressReporter::new(&mut sink);
        assert!(load_raw_factors(
            &[fixture.shard.clone(), fixture.shard.clone()],
            &mut progress,
        )
        .is_err());

        let mut contradictory = fixture.shard.clone();
        contradictory.provider_set_component.metadata.shard_id = Some("other".to_owned());
        let mut progress = ProgressReporter::new(&mut sink);
        assert!(load_raw_factors(&[contradictory], &mut progress).is_err());

        let mut mismatched = fixture.shard.clone();
        mismatched.provider_set_component.metadata.source_shard_id = Some("other".to_owned());
        let mut progress = ProgressReporter::new(&mut sink);
        assert!(load_raw_factors(&[mismatched], &mut progress).is_err());

        let mut progress_overflow = fixture.shard.clone();
        progress_overflow.provider_group_npi.metadata.member_count = u64::MAX;
        let mut progress = ProgressReporter::new(&mut sink);
        assert!(load_raw_factors(&[progress_overflow], &mut progress).is_err());

        let limited_edges = ProviderGraphV4Options {
            max_factor_edges: Some(1),
            ..ProviderGraphV4Options::default()
        };
        assert!(resource_admission_preflight(
            std::slice::from_ref(&fixture.shard),
            &fixture.provider_map,
            &limited_edges,
        )
        .is_err());
        let limited_memory = ProviderGraphV4Options {
            max_estimated_model_bytes: Some(1),
            ..ProviderGraphV4Options::default()
        };
        assert!(resource_admission_preflight(
            std::slice::from_ref(&fixture.shard),
            &fixture.provider_map,
            &limited_memory,
        )
        .is_err());
        assert!(resource_admission_preflight(
            std::slice::from_ref(&fixture.shard),
            &temporary_missing_path(fixture._temporary.path()),
            &ProviderGraphV4Options::default(),
        )
        .is_err());

        for field in ["bytes", "edges", "owners"] {
            let mut overflow = fixture.shard.clone();
            let artifacts = [
                &mut overflow.provider_set_component,
                &mut overflow.provider_component_group,
                &mut overflow.provider_group_npi,
                &mut overflow.provider_npi_group,
            ];
            for artifact in artifacts {
                match field {
                    "bytes" => artifact.metadata.byte_count = u64::MAX,
                    "edges" => artifact.metadata.member_count = u64::MAX,
                    "owners" => artifact.metadata.owner_count = u64::MAX,
                    _ => unreachable!(),
                }
            }
            assert!(resource_admission_preflight(
                &[overflow],
                &fixture.provider_map,
                &ProviderGraphV4Options::default(),
            )
            .is_err());
        }
    }

    fn temporary_missing_path(root: &Path) -> PathBuf {
        root.join("missing-provider-map")
    }

    #[test]
    fn primitive_failures_and_existing_outputs_are_explicit() {
        assert!(parse_sha256("short").is_err());
        assert_eq!(decode_hex(b'A').unwrap(), 10);
        assert!(decode_hex(b'!').is_err());
        assert!(npi_from_global_id(global(9, 1)).is_err());
        assert!(npi_from_global_id([0; 16]).is_err());

        let temporary = tempfile::tempdir().unwrap();
        let mut bytes = standard_bytes(&[], &[]);
        bytes.push(0);
        let descriptor = descriptor_for_bytes(
            &temporary.path().join("trailing-byte.sidecar"),
            &bytes,
            STANDARD_FORMAT,
            0,
            0,
            None,
        );
        assert!(ValidatedArtifact::open(&descriptor).is_err());

        let fixture = independent_fixture();
        compile_provider_graph_v4(
            std::slice::from_ref(&fixture.shard),
            &fixture.provider_map,
            &fixture.output,
            ProviderGraphV4Options::default(),
        )
        .unwrap();
        assert!(compile_provider_graph_v4(
            std::slice::from_ref(&fixture.shard),
            &fixture.provider_map,
            &fixture.output,
            ProviderGraphV4Options::default(),
        )
        .is_err());

        let unfinished = temporary.path().join("unfinished.copy");
        drop(PgCopyFileWriter::create(&unfinished).unwrap());
        assert!(unfinished.is_file());
    }

    #[test]
    fn encoding_and_relation_error_boundaries_are_explicit() {
        let json_error =
            ProviderGraphV4Error::from(serde_json::from_slice::<Value>(b"not-json").unwrap_err());
        assert!(json_error.to_string().contains("expected"));
        assert!(invalid_conversion::<u32, _>(u32::try_from(-1_i64), "conversion").is_err());

        assert_eq!(paged_encoded_bytes("coverage", 0, 4).unwrap(), 0);
        assert!(pg_copy_row_bytes("coverage", usize::MAX).is_err());
        assert!(paged_encoded_bytes("coverage", u64::MAX, 4).is_err());
        assert!(relation_encoded_bytes(
            &RelationShape {
                relation: "coverage",
                owner_count: 1,
                member_count: u64::MAX,
            },
            &ProviderGraphV4Options::default(),
        )
        .is_err());
        assert!(dictionary_copy_bytes(&[usize::MAX], 1).is_err());
        assert!(dictionary_copy_bytes(&[1], usize::MAX).is_err());

        let below_threshold = ProviderGraphV4Options {
            heavy_owner_member_threshold: 3,
            ..ProviderGraphV4Options::default()
        };
        assert!(maybe_heavy_bitmap("coverage", 0, &[], &below_threshold)
            .unwrap()
            .is_none());
        assert!(maybe_heavy_bitmap("coverage", 0, &[1, 2], &below_threshold)
            .unwrap()
            .is_none());
        let wide = ProviderGraphV4Options {
            heavy_owner_member_threshold: 2,
            heavy_bitmap_minimum_savings_bytes: 0,
            ..ProviderGraphV4Options::default()
        };
        assert!(maybe_heavy_bitmap("coverage", 0, &[0, u32::MAX], &wide)
            .unwrap()
            .is_none());

        let temporary = tempfile::tempdir().unwrap();
        let copy = temporary.path().join("blocks.copy");
        let references = temporary.path().join("references.jsonl");
        let mut cas = CasBlockWriter::create(&copy, &references).unwrap();
        assert!(cas
            .write_block(
                "coverage",
                0,
                0,
                u64::MAX,
                b"payload",
                BlockCoordinateMetadata::default(),
            )
            .is_err());
        let too_many_fields = vec![&[][..]; i16::MAX as usize + 1];
        assert!(cas.write_copy_row(&too_many_fields).is_err());
        cas.write_block(
            "coverage",
            0,
            0,
            1,
            b"first",
            BlockCoordinateMetadata::default(),
        )
        .unwrap();
        assert!(cas
            .write_block(
                "coverage",
                0,
                0,
                1,
                b"second",
                BlockCoordinateMetadata::default(),
            )
            .is_err());
        cas.finish().unwrap();

        let dictionary_path = temporary.path().join("dictionary.copy");
        let mut dictionary = PgCopyFileWriter::create(&dictionary_path).unwrap();
        assert!(dictionary.row(&too_many_fields).is_err());
        drop(dictionary);

        let options = ProviderGraphV4Options::default();
        let mut cas = CasBlockWriter::create(
            &temporary.path().join("relations.copy"),
            &temporary.path().join("relations.jsonl"),
        )
        .unwrap();
        let invalid_pages = ProviderGraphV4Options {
            member_page_bytes: 3,
            ..ProviderGraphV4Options::default()
        };
        assert!(RelationEmitter::new("coverage", 0, 1, &mut cas, &invalid_pages).is_err());

        {
            let mut emitter = RelationEmitter::new("coverage", 0, 1, &mut cas, &options).unwrap();
            assert!(emitter.push_owner(&[2, 1], false).is_err());
            emitter.push_owner(&[1], false).unwrap();
            assert!(emitter.push_owner(&[2], false).is_err());
        }
        {
            let emitter = RelationEmitter::new("coverage", 0, 1, &mut cas, &options).unwrap();
            assert!(emitter.finish().is_err());
        }
        {
            let mut emitter = RelationEmitter::new("coverage", 0, 1, &mut cas, &options).unwrap();
            emitter.logical_member_count = u64::MAX;
            assert!(emitter.push_owner(&[1], true).is_err());
        }
        {
            let mut emitter = RelationEmitter::new("coverage", 0, 1, &mut cas, &options).unwrap();
            emitter.vector_member_count = u64::MAX;
            assert!(emitter.push_owner(&[1], false).is_err());
        }
        {
            let mut emitter = RelationEmitter::new("coverage", 0, 0, &mut cas, &options).unwrap();
            emitter.flush_member_page().unwrap();
            emitter.flush_locator_page().unwrap();
            emitter.vector_member_count = u64::MAX;
            assert!(emitter.finish().is_err());
        }
        {
            let mut emitter =
                RelationEmitter::new("coverage", 0, usize::MAX, &mut cas, &options).unwrap();
            emitter.next_owner_index = usize::MAX;
            assert!(emitter.finish().is_err());
        }

        let mut events = Vec::new();
        let mut sink = |event: &V4ProgressEvent| events.push(event.clone());
        let mut progress = ProgressReporter::new(&mut sink);
        let fixture = independent_fixture();
        let mut admission = resource_admission_preflight(
            std::slice::from_ref(&fixture.shard),
            &fixture.provider_map,
            &ProviderGraphV4Options::default(),
        )
        .unwrap();
        let mut emission = RelationEmissionProgress {
            done: u64::MAX,
            total: u64::MAX,
            admission: &mut admission,
        };
        assert!(emission.owner(&mut progress).is_err());
        assert!(!events.is_empty() || emission.done == u64::MAX);
    }

    #[test]
    fn bitmap_framing_and_reference_spools_fail_closed_at_runtime_boundaries() {
        let temporary = tempfile::tempdir().unwrap();

        assert!(
            CasBlockWriter::create(&temporary.path().join("parentless.copy"), Path::new("/"),)
                .is_err()
        );

        let mut kind_limited = CasBlockWriter::create(
            &temporary.path().join("kind-limited.copy"),
            &temporary.path().join("kind-limited.jsonl"),
        )
        .unwrap();
        for index in 0..MAX_REFERENCE_OBJECT_KINDS {
            kind_limited
                .write_block(
                    &format!("coverage_{index:02}"),
                    0,
                    0,
                    1,
                    b"x",
                    BlockCoordinateMetadata::default(),
                )
                .unwrap();
        }
        assert!(kind_limited
            .write_block(
                "coverage_over_limit",
                0,
                0,
                1,
                b"x",
                BlockCoordinateMetadata::default(),
            )
            .unwrap_err()
            .to_string()
            .contains("object-kind count"));

        let mut row_limited = CasBlockWriter::create(
            &temporary.path().join("row-limited.copy"),
            &temporary.path().join("row-limited.jsonl"),
        )
        .unwrap();
        assert!(row_limited
            .write_block(
                &"x".repeat(REFERENCE_ENCODE_BUFFER_BYTES as usize),
                0,
                0,
                1,
                b"x",
                BlockCoordinateMetadata::default(),
            )
            .unwrap_err()
            .to_string()
            .contains("encode buffer"));

        let dense = (100..612).collect::<Vec<_>>();
        let bitmap_options = ProviderGraphV4Options {
            member_page_bytes: 64,
            heavy_owner_member_threshold: 1,
            heavy_bitmap_minimum_savings_bytes: 0,
            ..ProviderGraphV4Options::default()
        };
        let plan = maybe_heavy_bitmap("coverage", 0, &dense, &bitmap_options)
            .unwrap()
            .unwrap();

        let mut mismatch_cas = CasBlockWriter::create(
            &temporary.path().join("mismatch.copy"),
            &temporary.path().join("mismatch.jsonl"),
        )
        .unwrap();
        let mut mismatch_emitter =
            RelationEmitter::new("other", 0, 1, &mut mismatch_cas, &bitmap_options).unwrap();
        assert!(mismatch_emitter
            .emit_heavy_bitmap_streamed(plan, &dense, 64)
            .unwrap_err()
            .to_string()
            .contains("differs from its relation owner"));

        let mut small_page_cas = CasBlockWriter::create(
            &temporary.path().join("small-page.copy"),
            &temporary.path().join("small-page.jsonl"),
        )
        .unwrap();
        let mut small_page_emitter =
            RelationEmitter::new("coverage", 0, 1, &mut small_page_cas, &bitmap_options).unwrap();
        assert!(small_page_emitter
            .emit_heavy_bitmap_streamed(plan, &dense, HEAVY_BITMAP_FRAGMENT_HEADER_BYTES)
            .unwrap_err()
            .to_string()
            .contains("cannot contain its fragment frame"));

        let mut incomplete_plan = plan;
        incomplete_plan.logical_byte_count = HEAVY_BITMAP_HEADER_BYTES as u64;
        incomplete_plan.raw_byte_count =
            (HEAVY_BITMAP_HEADER_BYTES + HEAVY_BITMAP_FRAGMENT_HEADER_BYTES) as u64;
        let mut incomplete_cas = CasBlockWriter::create(
            &temporary.path().join("incomplete.copy"),
            &temporary.path().join("incomplete.jsonl"),
        )
        .unwrap();
        let mut incomplete_emitter =
            RelationEmitter::new("coverage", 0, 1, &mut incomplete_cas, &bitmap_options).unwrap();
        assert!(incomplete_emitter
            .emit_heavy_bitmap_streamed(incomplete_plan, &dense, 64)
            .unwrap_err()
            .to_string()
            .contains("did not consume every sorted member"));

        let mut wrong_size_plan = plan;
        wrong_size_plan.raw_byte_count += 1;
        let mut wrong_size_cas = CasBlockWriter::create(
            &temporary.path().join("wrong-size.copy"),
            &temporary.path().join("wrong-size.jsonl"),
        )
        .unwrap();
        let mut wrong_size_emitter =
            RelationEmitter::new("coverage", 0, 1, &mut wrong_size_cas, &bitmap_options).unwrap();
        assert!(wrong_size_emitter
            .emit_heavy_bitmap_streamed(wrong_size_plan, &dense, 64)
            .unwrap_err()
            .to_string()
            .contains("physical framing differs"));

        let out_of_order = [100, 200, 101];
        let out_of_order_plan = HeavyBitmapPlan {
            relation: "coverage",
            owner_key: 0,
            member_count: out_of_order.len() as u64,
            member_base: 100,
            member_span: 101,
            logical_byte_count: 37,
            raw_byte_count: 357,
            vector_byte_count: 12,
        };
        let mut out_of_order_cas = CasBlockWriter::create(
            &temporary.path().join("out-of-order.copy"),
            &temporary.path().join("out-of-order.jsonl"),
        )
        .unwrap();
        let mut out_of_order_emitter =
            RelationEmitter::new("coverage", 0, 1, &mut out_of_order_cas, &bitmap_options).unwrap();
        assert!(out_of_order_emitter
            .emit_heavy_bitmap_streamed(out_of_order_plan, &out_of_order, 36)
            .unwrap_err()
            .to_string()
            .contains("not sorted and unique"));

        let mut wide_plan = plan;
        wide_plan.member_span = u64::from(u32::MAX) + 1;
        let mut wide_cas = CasBlockWriter::create(
            &temporary.path().join("wide.copy"),
            &temporary.path().join("wide.jsonl"),
        )
        .unwrap();
        let mut wide_emitter =
            RelationEmitter::new("coverage", 0, 1, &mut wide_cas, &bitmap_options).unwrap();
        assert!(wide_emitter
            .emit_heavy_bitmap_streamed(wide_plan, &dense, 64)
            .unwrap_err()
            .to_string()
            .contains("span exceeds uint32"));

        let bitmap_disabled_by_framing = ProviderGraphV4Options {
            member_page_bytes: HEAVY_BITMAP_FRAGMENT_HEADER_BYTES,
            heavy_owner_member_threshold: 1,
            heavy_bitmap_minimum_savings_bytes: 0,
            ..ProviderGraphV4Options::default()
        };
        assert!(
            maybe_heavy_bitmap("coverage", 0, &dense, &bitmap_disabled_by_framing,)
                .unwrap()
                .is_none()
        );
    }

    fn blank_admission(max_estimated_model_bytes: Option<u64>) -> ResourceAdmissionTracker {
        ResourceAdmissionTracker {
            summary: V4ResourceAdmissionSummary {
                formula: "coverage".to_owned(),
                input_factor_bytes: 0,
                provider_set_key_map_bytes: 0,
                factor_edge_count: 0,
                factor_owner_count: 0,
                base_estimated_model_bytes: 0,
                derived_projection_bytes: 0,
                retained_scratch_high_water_bytes: 0,
                bounded_emission_buffer_bytes: 0,
                estimated_peak_bytes: 0,
                max_estimated_model_bytes,
                max_factor_edges: None,
            },
        }
    }

    #[test]
    fn remaining_option_resource_and_factor_boundaries_fail_closed() {
        for options in [
            ProviderGraphV4Options {
                max_set_components_per_fallback_set: 0,
                ..ProviderGraphV4Options::default()
            },
            ProviderGraphV4Options {
                max_online_group_keys_per_set: 0,
                ..ProviderGraphV4Options::default()
            },
            ProviderGraphV4Options {
                npi_prefix_target: 0,
                ..ProviderGraphV4Options::default()
            },
            ProviderGraphV4Options {
                max_npi_prefix_override_owners: 0,
                ..ProviderGraphV4Options::default()
            },
            ProviderGraphV4Options {
                max_npi_prefix_override_bytes: 0,
                ..ProviderGraphV4Options::default()
            },
            ProviderGraphV4Options {
                max_estimated_model_bytes: Some(0),
                ..ProviderGraphV4Options::default()
            },
            ProviderGraphV4Options {
                max_factor_edges: Some(0),
                ..ProviderGraphV4Options::default()
            },
        ] {
            assert!(options.validate().is_err());
        }

        let mut tracker = blank_admission(Some(8));
        tracker.reserve_projection("within limit", 8).unwrap();
        assert!(tracker.reserve_projection("over limit", 1).is_err());

        let mut tracker = blank_admission(None);
        tracker.summary.derived_projection_bytes = u64::MAX;
        assert!(tracker.reserve_projection("overflow", 1).is_err());

        let mut tracker = blank_admission(None);
        tracker.summary.base_estimated_model_bytes = u64::MAX;
        assert!(tracker.checked_peak(1, 0).is_err());
        tracker.summary.base_estimated_model_bytes = 0;
        assert!(tracker.checked_peak(u64::MAX, 1).is_err());
        tracker.summary.bounded_emission_buffer_bytes = 1;
        assert!(tracker.checked_peak(0, u64::MAX).is_err());

        let mut tracker = blank_admission(None);
        tracker.reserve_scratch_bytes("new high water", 8).unwrap();
        tracker
            .reserve_scratch_bytes("retained high water", 4)
            .unwrap();
        assert_eq!(tracker.summary.retained_scratch_high_water_bytes, 8);

        assert!(estimated_u32_capacity_bytes(usize::MAX).is_err());
        assert!(estimated_vec_owner_bytes(usize::MAX).is_err());
        assert!(checked_estimated_sum([u64::MAX, 1], "coverage overflow").is_err());
        assert!(map_key(&HashMap::new(), global(3, 1), "group").is_err());

        let mut events = Vec::new();
        let mut sink = |event: &V4ProgressEvent| events.push(event.clone());
        let mut progress = ProgressReporter::new(&mut sink);
        let mut done = u64::MAX;
        assert!(advance_build_progress(&mut progress, &mut done, u64::MAX).is_err());

        let fixture = independent_fixture();
        let provider_sets = ProviderSetMap::read(&fixture.provider_map).unwrap();
        let mut progress = ProgressReporter::new(&mut sink);
        let mut raw =
            load_raw_factors(std::slice::from_ref(&fixture.shard), &mut progress).unwrap();
        let first_set = provider_sets.globals_by_index[0];
        raw.set_components.insert(first_set, Vec::new());
        assert!(validate_factor_completeness(&raw, &provider_sets).is_err());
    }

    #[test]
    fn heavy_prefix_and_online_work_boundaries_are_exact() {
        let dense = (100..612).collect::<Vec<_>>();
        let options = ProviderGraphV4Options {
            member_page_bytes: 64,
            locator_page_bytes: 48,
            heavy_owner_member_threshold: 1,
            heavy_bitmap_minimum_savings_bytes: 0,
            ..ProviderGraphV4Options::default()
        };
        let plan = maybe_heavy_bitmap("group_npis_exact", 0, &dense, &options)
            .unwrap()
            .unwrap();
        assert_eq!(
            heavy_prefix_fragment_count(plan, &dense, 0, options.member_page_bytes).unwrap(),
            0
        );
        assert!(
            heavy_prefix_fragment_count(plan, &dense, 1, HEAVY_BITMAP_FRAGMENT_HEADER_BYTES)
                .is_err()
        );
        assert_eq!(
            heavy_prefix_fragment_count(plan, &dense, 1, options.member_page_bytes).unwrap(),
            1
        );
        assert!(
            heavy_prefix_fragment_count(plan, &dense, dense.len(), options.member_page_bytes)
                .unwrap()
                > 1
        );

        let group_npis = vec![dense.clone(), Vec::new()];
        let physical = group_npi_physical_layout(&group_npis, &options).unwrap();
        let work = group_npi_batch_work(
            &[0, 1],
            &group_npis,
            &physical,
            16,
            options.member_page_bytes,
        )
        .unwrap();
        assert_eq!(work.relation_members, 16);
        assert_eq!(work.locator_pages, 1);
        assert!(work.member_pages > 0);
        assert_eq!(work.batches, 1);

        let empty = ordered_npi_prefix_for_sources(
            &[],
            &[],
            &[],
            &group_npi_physical_layout(&[], &options).unwrap(),
            &options,
            0,
            0,
        )
        .unwrap();
        assert!(empty.members.is_empty());
        assert!(empty.source_exhausted);

        let source_groups = vec![vec![0, 1], vec![0, 2], Vec::new()];
        let group_npis = vec![Vec::new(), vec![1], vec![2]];
        let physical = group_npi_physical_layout(&group_npis, &options).unwrap();
        let merged = ordered_npi_prefix_for_sources(
            &[0, 1, 2],
            &source_groups,
            &group_npis,
            &physical,
            &options,
            2,
            usize::MAX,
        )
        .unwrap();
        assert_eq!(merged.members, vec![1, 2]);
        assert_eq!(merged.unique_groups_visited, 3);
        assert!(merged.source_exhausted);
        assert!(merged.source_members_visited > merged.unique_groups_visited as u64);

        let bounded = ordered_npi_prefix_for_sources(
            &[0],
            &source_groups,
            &group_npis,
            &physical,
            &options,
            2,
            0,
        )
        .unwrap();
        assert!(!bounded.source_exhausted);
        assert!(bounded.members.is_empty());

        assert_eq!(nearest_rank(&[], 95), 0);
        assert_eq!(nearest_rank(&[1, 2, 3, 4], 50), 2);
        assert_eq!(pages_for_member_range(4, 0, 4), 0);
        assert_eq!(pages_for_member_range(3, 2, 4), 2);
        assert_eq!(
            pages_for_owner_prefixes(&[0, 1], &[Vec::new(), vec![1]], &[0, 0, 1], 1, 1),
            1
        );
        assert_eq!(locator_pages_for_owners(&[0, 1, 4, 5], 4), 2);

        for work in [
            OnlineGroupNpiWork {
                relation_members: 1,
                ..OnlineGroupNpiWork::default()
            },
            OnlineGroupNpiWork {
                locator_pages: 1,
                ..OnlineGroupNpiWork::default()
            },
            OnlineGroupNpiWork {
                member_pages: 1,
                ..OnlineGroupNpiWork::default()
            },
            OnlineGroupNpiWork {
                relation_bytes: 1,
                ..OnlineGroupNpiWork::default()
            },
            OnlineGroupNpiWork {
                batches: 1,
                ..OnlineGroupNpiWork::default()
            },
        ] {
            let zero_limits = ProviderGraphV4Options {
                max_online_group_npi_members_per_set: 0,
                max_online_group_npi_locator_pages_per_set: 0,
                max_online_group_npi_member_pages_per_set: 0,
                max_online_group_npi_bytes_per_set: 0,
                max_online_group_npi_batches_per_set: 0,
                ..ProviderGraphV4Options::default()
            };
            assert!(group_npi_work_exceeds_limits(work, &zero_limits));
        }
    }

    #[test]
    fn override_caps_manifest_and_direct_heavy_relations_are_exercised() {
        let set_components = vec![vec![0]];
        let component_groups = vec![vec![0]];
        let set_patterns = vec![vec![0]];
        let pattern_groups = vec![vec![0]];
        let group_npis = vec![vec![0]];

        for options in [
            ProviderGraphV4Options {
                max_online_group_keys_per_set: 0,
                max_npi_prefix_override_owners: 0,
                ..ProviderGraphV4Options::default()
            },
            ProviderGraphV4Options {
                max_online_group_keys_per_set: 0,
                max_npi_prefix_override_bytes: 1,
                ..ProviderGraphV4Options::default()
            },
        ] {
            let mut admission = blank_admission(None);
            assert!(derive_npi_prefix_overrides(
                NpiPrefixInputs {
                    set_base: 0,
                    set_components: &set_components,
                    component_groups: &component_groups,
                    set_patterns: &set_patterns,
                    pattern_groups: &pattern_groups,
                    group_npis: &group_npis,
                },
                &options,
                &mut admission,
            )
            .is_err());
        }

        let temporary = tempfile::tempdir().unwrap();
        let mut cas = CasBlockWriter::create(
            &temporary.path().join("ordered.copy"),
            &temporary.path().join("ordered.jsonl"),
        )
        .unwrap();
        let mut emitter = RelationEmitter::new(
            "ordered_coverage",
            0,
            1,
            &mut cas,
            &ProviderGraphV4Options::default(),
        )
        .unwrap();
        assert!(emitter.push_ordered_owner(&[1, 1]).is_err());

        let fixture = shared_pattern_fixture(1, 512);
        let options = ProviderGraphV4Options {
            member_page_bytes: 64,
            heavy_owner_member_threshold: 1,
            heavy_bitmap_minimum_savings_bytes: 0,
            ..ProviderGraphV4Options::default()
        };
        let summary = compile_provider_graph_v4_manifest(ProviderGraphV4Manifest {
            shards: vec![fixture.shard.clone()],
            provider_set_key_map_path: fixture.provider_map.clone(),
            output_directory: fixture.output.clone(),
            options,
        })
        .unwrap();
        assert_eq!(summary.selected_layout, ProviderGraphV4Layout::Direct);
        assert!(summary
            .heavy_bitmaps
            .iter()
            .any(|bitmap| bitmap.relation == "group_sets_direct"));
    }
}
