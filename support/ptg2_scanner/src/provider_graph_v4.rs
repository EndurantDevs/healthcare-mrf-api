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
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::borrow::Cow;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

const MANIFEST_VERSION: u32 = 1;
const STANDARD_MAGIC: &[u8; 8] = b"PTG2MNSC";
const DENSE_MAGIC: &[u8; 8] = b"PTG2MNDS";
const TAX_IDENTITY_MAGIC: &[u8; 8] = b"PTG2TAX1";
const TAX_IDENTITY_FORMAT: &str = "ptg2_provider_group_tax_identity_v1";
const TAX_IDENTITY_VERSION: u16 = 1;
const TAX_IDENTITY_RECORD_BYTES: u16 = 65;
const TAX_IDENTITY_FIXED_HEADER_BYTES: usize = 13;
const TAX_IDENTITY_NORMALIZATION_CONTRACT: &str = "ein_ascii_digits_or_2_7_hyphen_v1";
const TAX_IDENTITY_HMAC_CONTRACT: &str = "hmac_sha256_ptg_tin_v1";
const TAX_IDENTITY_CANDIDATE_PREFIX_CONTRACT: &str = "tin_id_128=first_16_bytes(tin_hmac_sha256)";
const TAX_IDENTITY_AUTHORITY_CONTRACT: &str = "tin_hmac_sha256_full_32_bytes_authoritative";
const TAX_IDENTITY_PROJECTION_CONTRACT: &str = "ptg2_provider_tax_identity_projection_v1";
const TAX_SOURCE_ORDINAL_CONTRACT: &str = "snapshot_shard_id_sorted_lsb0_bitmap_v1";
const TAX_SOURCE_ORDINAL_FIXED_UPPER_BOUND_BYTES: u64 = 256;
const TAX_SOURCE_IDENTITY_COPY_UPPER_BOUND: u64 = 2;
const TAX_IDENTITY_GROUP_ENTRY_UPPER_BOUND_BYTES: u64 = 256;
const TAX_IDENTITY_DICTIONARY_ENTRY_UPPER_BOUND_BYTES: u64 = 128;
const TAX_POLICY_DESCRIPTOR_HASH_DOMAIN: &[u8] = b"PTG2V4TINPOLICY\x01";
const TAX_SOURCE_ORDINAL_HASH_DOMAIN: &[u8] = b"PTG2V4TAXORD\x01";
const TAX_CONTENT_HASH_DOMAIN: &[u8] = b"PTG2V4TAXCONTENT\x01";
const STANDARD_FORMAT: &str = "magic8:uint32_le_version:uint64_le_entry_count:index(owner16:uint64_le_offset:uint32_le_count):members16";
const DENSE_FORMAT: &str = "magic8:uint32_le_version:uint64_le_entry_count:uint64_le_member_global_count:index(owner16:uint64_le_offset:uint32_le_count):member_globals16:members_uint32_le";
const STANDARD_HEADER_BYTES: usize = 20;
const DENSE_HEADER_BYTES: usize = 28;
const OWNER_RECORD_BYTES: usize = 28;
const GLOBAL_ID_BYTES: usize = 16;
const MIN_NPI: u64 = 1_000_000_000;
const MAX_NPI: u64 = 9_999_999_999;
const LOCATOR_BYTES: usize = 12;
const HEAVY_BITMAP_HEADER_BYTES: usize = 24;
const HEAVY_BITMAP_FRAGMENT_HEADER_BYTES: usize = 32;
const HEAVY_BITMAP_FRAGMENT_MAGIC: &[u8; 8] = b"PTG2V4BF";
const V4_MAP_HEADER_BYTES: u64 = 80;
const V4_MAP_RECORD_BYTES: u64 = 52;
const V4_MAP_COORDINATES_PER_PACK: u64 = 256;
const V4_MAP_BLOCK_KIND: &str = "snapshot_coordinate_map_v1";
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
const DEFAULT_MAX_NPI_PREFIX_OVERRIDE_OWNERS: usize = 250_000;
const DEFAULT_MAX_NPI_PREFIX_OVERRIDE_BYTES: u64 = 256 * 1024 * 1024;
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
const ESTIMATED_INFERRED_TAXONOMY_ROW_BYTES: u64 = 512;
const ESTIMATED_PATTERN_POSTING_SCRATCH_BYTES: u64 = 128;
const ESTIMATED_PATTERN_PAYLOAD_BYTES_PER_MEMBER: u64 = 24;
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
const NPI_SCOPE_FORMAT: &str = "ptg2_provider_graph_v4_npi_scope_v1";
const NPI_SCOPE_INPUT_HASH_DOMAIN: &[u8] = b"PTG2V4NPISCOPE\x01";
const NPI_SCOPE_ARTIFACT_FORMAT: &str = "ptg2_provider_npi_scope_pg_binary_int8_v1";
const NPI_SCOPE_BINDING_CONTRACT: &str = "provider_npi_scope_to_provider_npi_group_v1";
const NPI_SCOPE_BINDING_HASH_DOMAIN: &[u8] = b"ptg2:v4:provider-npi-scope-binding:v1\x00";
const NPI_SCOPE_SHARD_BINDING_CONTRACT: &str = "provider_npi_scope_shard_binding_v1";
const NPI_SCOPE_SHARD_BINDING_HASH_DOMAIN: &[u8] =
    b"ptg2:v4:provider-npi-scope-shard-binding:v1\x00";
const NPI_SCOPE_RETENTION_CONTRACT: &str = "shared_v4_publication_scratch_v1";
const MAX_NPI_SCOPE_AUTH_WORKERS: usize = 8;
const INFERRED_TAXONOMY_INPUT_CONTRACT: &str = "ptg2_v4_inferred_taxonomy_compiler_input_v1";
const INFERRED_TAXONOMY_CATALOG_CONTRACT: &str = "snapshot_npi_live_catalog_individual_v1";
const INFERRED_TAXONOMY_VECTOR_FORMAT: &str = "sorted_u32le_v1";
const INFERRED_TAXONOMY_DIRECT_REPRESENTATION: &str = "direct_v1";
const INFERRED_TAXONOMY_PATTERN_REPRESENTATION: &str = "pattern_v1";
const INFERRED_TAXONOMY_OBSERVE_REPRESENTATION: &str = "observe_v1";
const INFERRED_TAXONOMY_CANDIDATE_CAP_REASON: &str = "candidate_cap_exceeded";
const INFERRED_TAXONOMY_PATTERN_CAP_REASON: &str = "pattern_projection_cap_exceeded";
const INFERRED_TAXONOMY_MEMBER_DIGEST_DOMAIN: &[u8] = b"ptg2:v4:inferred-taxonomy-members:v1\x00";
const INFERRED_TAXONOMY_RULE_SET_DIGEST_DOMAIN: &[u8] =
    b"ptg2:v4:inferred-taxonomy-rule-set:v1\x00";
const INFERRED_TAXONOMY_PATTERN_MEMBER_DIGEST_DOMAIN: &[u8] =
    b"ptg2:v4:inferred-taxonomy-pattern-members:v2\x00";
const INFERRED_TAXONOMY_PATTERN_PAYLOAD_MAGIC: &[u8; 8] = b"PTG4TXP2";
const INFERRED_TAXONOMY_PATTERN_PAYLOAD_VERSION: u32 = 1;
const DEFAULT_MAX_ONLINE_INFERRED_TAXONOMY_CANDIDATES: usize = 37_000;
const DEFAULT_MAX_ONLINE_CANDIDATE_PATTERN_PROJECTION_MEMBERS: usize = 131_072;

const OUTPUT_NAMES: [&str; 12] = [
    "v4-graph-blocks.copy",
    "v4-graph-references.jsonl",
    "v4-provider-groups.copy",
    "v4-provider-components.copy",
    "v4-npi-scope.copy",
    "v4-provider-set-audit-npi.copy",
    "v4-provider-set-npi-prefix-overrides.copy",
    "v4-provider-tax-identities.copy",
    "v4-provider-group-tax-identities.copy",
    "v4-patterns.copy",
    "v4-inferred-taxonomy-candidates.copy",
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
#[serde(deny_unknown_fields)]
pub struct V4NpiScopeMetadata {
    pub record_format: String,
    pub sha256: String,
    pub byte_count: u64,
    pub row_count: u64,
    pub provider_npi_group_sha256: String,
    pub provider_npi_group_record_format: String,
    pub provider_npi_group_byte_count: u64,
    pub provider_npi_group_owner_count: u64,
    pub provider_npi_group_member_count: u64,
    pub provider_npi_group_member_global_count: u64,
    pub binding_contract: String,
    pub binding_sha256: String,
    pub shard_binding_contract: String,
    pub shard_binding_sha256: String,
    pub retention_contract: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub source_shard_id: Option<String>,
    #[serde(default)]
    pub shard_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct V4NpiScopeArtifactDescriptor {
    pub path: PathBuf,
    pub metadata: V4NpiScopeMetadata,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct V4TaxIdentityMetadata {
    pub record_format: String,
    pub sha256: String,
    pub byte_count: u64,
    pub row_count: u64,
    pub provider_group_count: u64,
    pub matched_ein_count: u64,
    pub missing_count: u64,
    pub malformed_count: u64,
    pub unsupported_type_count: u64,
    pub version: u16,
    pub record_bytes: u16,
    pub token_policy_id: String,
    pub normalization_contract: String,
    pub hmac_contract: String,
    #[serde(rename = "final")]
    pub final_file: bool,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub source_shard_id: Option<String>,
    #[serde(default)]
    pub shard_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct V4TaxIdentityArtifactDescriptor {
    pub path: PathBuf,
    pub metadata: V4TaxIdentityMetadata,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct V4ProviderGraphShardDescriptor {
    pub shard_id: String,
    pub provider_set_component: V4MembershipArtifactDescriptor,
    pub provider_component_group: V4MembershipArtifactDescriptor,
    pub provider_group_npi: V4MembershipArtifactDescriptor,
    pub provider_npi_group: V4MembershipArtifactDescriptor,
    pub provider_npi_scope: V4NpiScopeArtifactDescriptor,
    pub provider_group_tax_identity: V4TaxIdentityArtifactDescriptor,
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
    pub max_online_inferred_taxonomy_candidates: usize,
    pub max_online_candidate_pattern_projection_members: usize,
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
            max_online_inferred_taxonomy_candidates:
                DEFAULT_MAX_ONLINE_INFERRED_TAXONOMY_CANDIDATES,
            max_online_candidate_pattern_projection_members:
                DEFAULT_MAX_ONLINE_CANDIDATE_PATTERN_PROJECTION_MEMBERS,
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
        if self.max_online_inferred_taxonomy_candidates == 0 {
            return Err(invalid(
                "V4 maximum inferred-taxonomy candidates must be positive",
            ));
        }
        if self.max_online_candidate_pattern_projection_members == 0 {
            return Err(invalid(
                "V4 maximum inferred-taxonomy pattern members must be positive",
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
    pub npi_scope: ProviderGraphV4NpiScopeInput,
    pub inferred_taxonomy: ProviderGraphV4InferredTaxonomyInput,
    pub output_directory: PathBuf,
    #[serde(default)]
    pub options: ProviderGraphV4Options,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderGraphV4NpiScopeManifest {
    pub shards: Vec<V4ProviderGraphShardDescriptor>,
    pub output_path: PathBuf,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderGraphV4NpiScopeInput {
    pub format: String,
    pub row_count: u64,
    pub source_owner_count: u64,
    pub input_byte_count: u64,
    pub input_sha256: String,
    pub output_byte_count: u64,
    pub output_sha256: String,
    pub output_path: PathBuf,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderGraphV4InputArtifact {
    pub path: PathBuf,
    pub byte_count: u64,
    pub sha256: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderGraphV4InferredTaxonomyRuleInput {
    pub rule_digest: String,
    pub catalog_digest: String,
    pub member_count: u64,
    pub member_offset_bytes: u64,
    pub member_byte_count: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderGraphV4InferredTaxonomyInput {
    pub contract: String,
    pub catalog_contract: String,
    pub vector_format: String,
    pub npi_scope_sha256: String,
    pub rule_set_digest: String,
    pub members: ProviderGraphV4InputArtifact,
    pub rules: Vec<ProviderGraphV4InferredTaxonomyRuleInput>,
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
pub struct ProviderGraphV4NpiScopeSummary {
    pub format: String,
    pub row_count: u64,
    pub source_owner_count: u64,
    pub input_byte_count: u64,
    pub input_sha256: String,
    pub output_byte_count: u64,
    pub output_sha256: String,
    pub output_path: PathBuf,
}

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
pub struct V4TaxSourceOrdinal {
    pub shard_id: String,
    pub ordinal: u32,
}

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
pub struct V4TaxIdentitySummary {
    pub contract: String,
    pub token_policy_id: String,
    pub token_policy_descriptor_sha256: String,
    pub normalization_contract: String,
    pub hmac_contract: String,
    pub candidate_prefix_contract: String,
    pub authority_contract: String,
    pub source_ordinal_contract: String,
    pub source_ordinal_map: Vec<V4TaxSourceOrdinal>,
    pub source_ordinal_map_digest: String,
    pub source_shard_count: u64,
    pub source_bitmap_bytes: u64,
    pub provider_group_count: u64,
    pub tax_identity_count: u64,
    pub matched_ein_count: u64,
    pub missing_count: u64,
    pub malformed_count: u64,
    pub unsupported_type_count: u64,
    pub content_digest: String,
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
    pub pattern_component_over_cap_set_count: u64,
    pub pattern_component_over_cap_prefix_covered_set_count: u64,
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
    pub tax_identity_merge_bitmap_upper_bound_bytes: u64,
    pub tax_identity_source_ordinal_upper_bound_bytes: u64,
    pub tax_identity_projection_upper_bound_bytes: u64,
    pub base_estimated_model_bytes: u64,
    pub derived_projection_bytes: u64,
    pub tax_identity_projection_bytes: u64,
    pub retained_scratch_high_water_bytes: u64,
    pub bounded_emission_buffer_bytes: u64,
    pub estimated_peak_bytes: u64,
    pub max_estimated_model_bytes: Option<u64>,
    pub max_factor_edges: Option<u64>,
}

#[derive(Debug)]
struct ResourceAdmissionTracker {
    summary: V4ResourceAdmissionSummary,
    tax_identity_projection_reconciled: bool,
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
            .and_then(|value| value.checked_add(self.summary.tax_identity_projection_bytes))
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

    fn reconcile_tax_identity_projection(&mut self, bytes: u64) -> ProviderGraphV4Result<()> {
        if self.tax_identity_projection_reconciled {
            return Err(invalid(
                "resource_admission: tax identity projection was reconciled twice",
            ));
        }
        if bytes > self.summary.tax_identity_projection_upper_bound_bytes {
            return Err(invalid(
                "resource_admission: exact tax identity projection exceeds its preflight upper bound",
            ));
        }
        self.summary.tax_identity_projection_bytes = bytes;
        let estimated_peak_bytes = self.checked_peak(
            self.summary.derived_projection_bytes,
            self.summary.retained_scratch_high_water_bytes,
        )?;
        self.ensure_within_limit(
            estimated_peak_bytes,
            &format!("reserving provider tax identity projection; bytes {bytes}"),
        )?;
        self.summary.estimated_peak_bytes = estimated_peak_bytes;
        self.tax_identity_projection_reconciled = true;
        Ok(())
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
    pub member_page_bytes: u64,
    pub locator_page_bytes: u64,
    pub heavy_owner_member_threshold: u64,
    pub heavy_bitmap_minimum_savings_bytes: u64,
    pub pattern_layout_serving_degree_eligible: bool,
    pub pattern_layout_sparse_prefix_eligible: bool,
    pub direct_layout_complete_prefix_eligible: bool,
    pub pattern_sparse_prefix_owner_count: u64,
    pub pattern_sparse_prefix_member_count: u64,
    pub pattern_sparse_prefix_raw_bytes: u64,
    pub pattern_sparse_prefix_projection_encoded_bytes: u64,
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
    pub max_online_inferred_taxonomy_candidates: u64,
    pub max_online_candidate_pattern_projection_members: u64,
    pub direct_complete_prefix_projection_encoded_bytes: u64,
    pub direct_graph_encoded_bytes: u64,
    pub pattern_graph_encoded_bytes: u64,
    pub direct_mapping_persistence_encoded_bytes: u64,
    pub pattern_mapping_persistence_encoded_bytes: u64,
    pub direct_map_payload_encoded_bytes: u64,
    pub pattern_map_payload_encoded_bytes: u64,
    pub direct_map_coordinate_count: u64,
    pub pattern_map_coordinate_count: u64,
    pub direct_map_pack_count: u64,
    pub pattern_map_pack_count: u64,
    pub direct_map_object_kind_count: u64,
    pub pattern_map_object_kind_count: u64,
    pub direct_complete_encoded_bytes: u64,
    pub pattern_complete_encoded_bytes: u64,
    pub direct_inferred_taxonomy_encoded_bytes: u64,
    pub pattern_inferred_taxonomy_encoded_bytes: u64,
    pub direct_inferred_taxonomy_eligible: bool,
    pub pattern_inferred_taxonomy_eligible: bool,
    pub direct_inferred_taxonomy_rejection_reason: Option<String>,
    pub direct_inferred_taxonomy_rejection_rule_digest: Option<String>,
    pub direct_inferred_taxonomy_rejection_observed_count: Option<u64>,
    pub direct_inferred_taxonomy_rejection_cap: Option<u64>,
    pub pattern_inferred_taxonomy_rejection_reason: Option<String>,
    pub pattern_inferred_taxonomy_rejection_rule_digest: Option<String>,
    pub pattern_inferred_taxonomy_rejection_observed_count: Option<u64>,
    pub pattern_inferred_taxonomy_rejection_cap: Option<u64>,
    pub common_encoded_bytes: u64,
    pub selected_graph_encoded_bytes: u64,
    pub selected_encoded_bytes: u64,
    pub block_copy_path: PathBuf,
    pub reference_manifest_path: PathBuf,
    pub group_copy_path: PathBuf,
    pub component_copy_path: PathBuf,
    pub npi_copy_path: PathBuf,
    pub provider_set_audit_npi_copy_path: PathBuf,
    pub provider_set_npi_prefix_override_copy_path: PathBuf,
    pub provider_tax_identity_copy_path: PathBuf,
    pub provider_group_tax_identity_copy_path: PathBuf,
    pub pattern_copy_path: Option<PathBuf>,
    pub inferred_taxonomy_copy_path: PathBuf,
    pub summary_path: PathBuf,
    pub block_count: u64,
    pub block_copy_bytes: u64,
    pub relation_summaries: Vec<V4RelationSummary>,
    pub heavy_bitmaps: Vec<V4HeavyBitmapSummary>,
    pub output_artifacts: Vec<V4OutputArtifactSummary>,
    pub tax_identity: V4TaxIdentitySummary,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum V4TaxIdentityState {
    MatchedEin,
    Missing,
    Malformed,
    UnsupportedType,
}

impl V4TaxIdentityState {
    fn parse(value: u8) -> ProviderGraphV4Result<Self> {
        match value {
            1 => Ok(Self::MatchedEin),
            2 => Ok(Self::Missing),
            3 => Ok(Self::Malformed),
            4 => Ok(Self::UnsupportedType),
            _ => Err(invalid(
                "V4 provider tax identity record has an invalid state",
            )),
        }
    }

    fn priority(self) -> u8 {
        match self {
            Self::MatchedEin => 4,
            Self::UnsupportedType => 3,
            Self::Malformed => 2,
            Self::Missing => 1,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::MatchedEin => "matched_ein",
            Self::Missing => "missing",
            Self::Malformed => "malformed",
            Self::UnsupportedType => "unsupported_type",
        }
    }

    fn code(self) -> u8 {
        match self {
            Self::MatchedEin => 1,
            Self::Missing => 2,
            Self::Malformed => 3,
            Self::UnsupportedType => 4,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct V4TaxIdentityRecord {
    provider_group_global_id: GlobalId,
    state: V4TaxIdentityState,
    tin_id_128: [u8; 16],
    tin_hmac_sha256: [u8; 32],
}

#[derive(Debug)]
struct ValidatedTaxIdentityArtifact {
    _file: File,
    bytes: Mmap,
    header_bytes: usize,
    row_count: u64,
    token_policy_id: String,
    byte_count: u64,
    sha256: [u8; 32],
}

impl ValidatedTaxIdentityArtifact {
    fn open(descriptor: &V4TaxIdentityArtifactDescriptor) -> ProviderGraphV4Result<Self> {
        let metadata = &descriptor.metadata;
        if metadata.record_format != TAX_IDENTITY_FORMAT
            || metadata.name.as_deref() != Some("provider_group_tax_identity")
            || metadata.version != TAX_IDENTITY_VERSION
            || metadata.record_bytes != TAX_IDENTITY_RECORD_BYTES
            || metadata.normalization_contract != TAX_IDENTITY_NORMALIZATION_CONTRACT
            || metadata.hmac_contract != TAX_IDENTITY_HMAC_CONTRACT
            || !metadata.final_file
            || metadata.provider_group_count != metadata.row_count
            || metadata
                .matched_ein_count
                .checked_add(metadata.missing_count)
                .and_then(|value| value.checked_add(metadata.malformed_count))
                .and_then(|value| value.checked_add(metadata.unsupported_type_count))
                != Some(metadata.row_count)
        {
            return Err(invalid("V4 provider tax identity metadata is inconsistent"));
        }
        let policy_bytes = metadata.token_policy_id.as_bytes();
        if !valid_tax_token_policy_id(&metadata.token_policy_id) {
            return Err(invalid(
                "V4 provider tax identity token policy ID is invalid",
            ));
        }
        let expected_digest = parse_sha256(&metadata.sha256)?;
        let file = File::open(&descriptor.path).map_err(|error| {
            invalid(format!(
                "V4 provider tax identity sidecar is unavailable ({}): {error}",
                descriptor.path.display()
            ))
        })?;
        let observed_size = file.metadata()?.len();
        if observed_size != metadata.byte_count {
            return Err(invalid(
                "V4 provider tax identity byte count metadata mismatch",
            ));
        }
        // SAFETY: completed scanner sidecars are immutable for the conversion.
        let bytes = unsafe { MmapOptions::new().map(&file)? };
        let observed_digest: [u8; 32] = Sha256::digest(&bytes).into();
        if observed_digest != expected_digest {
            return Err(invalid(
                "V4 provider tax identity checksum metadata mismatch",
            ));
        }
        let header_bytes = TAX_IDENTITY_FIXED_HEADER_BYTES
            .checked_add(policy_bytes.len())
            .ok_or(invalid("V4 provider tax identity header size overflows"))?;
        let expected_size = header_bytes
            .checked_add(
                invalid_conversion(
                    usize::try_from(metadata.row_count),
                    "V4 provider tax identity row count exceeds addressable memory",
                )?
                .checked_mul(TAX_IDENTITY_RECORD_BYTES as usize)
                .ok_or(invalid("V4 provider tax identity artifact size overflows"))?,
            )
            .ok_or(invalid("V4 provider tax identity artifact size overflows"))?;
        if bytes.len() != expected_size
            || bytes.get(..8) != Some(TAX_IDENTITY_MAGIC)
            || bytes.get(8..10) != Some(TAX_IDENTITY_VERSION.to_le_bytes().as_slice())
            || bytes.get(10..12) != Some(TAX_IDENTITY_RECORD_BYTES.to_le_bytes().as_slice())
            || bytes.get(12).copied() != Some(policy_bytes.len() as u8)
            || bytes.get(13..header_bytes) != Some(policy_bytes)
        {
            return Err(invalid(
                "V4 provider tax identity artifact header or size is invalid",
            ));
        }
        let artifact = Self {
            _file: file,
            bytes,
            header_bytes,
            row_count: metadata.row_count,
            token_policy_id: metadata.token_policy_id.clone(),
            byte_count: metadata.byte_count,
            sha256: observed_digest,
        };
        artifact.validate(metadata)?;
        Ok(artifact)
    }

    fn record(&self, index: u64) -> ProviderGraphV4Result<V4TaxIdentityRecord> {
        if index >= self.row_count {
            return Err(invalid(
                "V4 provider tax identity row index is out of range",
            ));
        }
        let offset = self
            .header_bytes
            .checked_add(
                invalid_conversion(
                    usize::try_from(index),
                    "V4 provider tax identity row index exceeds addressable memory",
                )?
                .checked_mul(TAX_IDENTITY_RECORD_BYTES as usize)
                .ok_or(invalid("V4 provider tax identity row offset overflows"))?,
            )
            .ok_or(invalid("V4 provider tax identity row offset overflows"))?;
        let provider_group_global_id = read_global_id(&self.bytes, offset)?;
        let state = V4TaxIdentityState::parse(
            *self
                .bytes
                .get(offset + 16)
                .ok_or(invalid("V4 provider tax identity state is truncated"))?,
        )?;
        let tin_id_128: [u8; 16] = invalid_conversion(
            self.bytes
                .get(offset + 17..offset + 33)
                .ok_or(invalid("V4 provider tax identity candidate is truncated"))?
                .try_into(),
            "V4 provider tax identity candidate width changed",
        )?;
        let tin_hmac_sha256: [u8; 32] = invalid_conversion(
            self.bytes
                .get(offset + 33..offset + 65)
                .ok_or(invalid("V4 provider tax identity token is truncated"))?
                .try_into(),
            "V4 provider tax identity token width changed",
        )?;
        match state {
            V4TaxIdentityState::MatchedEin => {
                if tin_id_128 != tin_hmac_sha256[..16] {
                    return Err(invalid(
                        "V4 provider tax identity candidate does not match its full HMAC",
                    ));
                }
            }
            _ if tin_id_128 != [0; 16] || tin_hmac_sha256 != [0; 32] => {
                return Err(invalid(
                    "V4 unavailable provider tax identity carries a token",
                ));
            }
            _ => {}
        }
        Ok(V4TaxIdentityRecord {
            provider_group_global_id,
            state,
            tin_id_128,
            tin_hmac_sha256,
        })
    }

    fn validate(&self, metadata: &V4TaxIdentityMetadata) -> ProviderGraphV4Result<()> {
        let mut previous_group = None;
        let mut counts = [0u64; 4];
        for index in 0..self.row_count {
            let record = self.record(index)?;
            if previous_group.is_some_and(|previous| record.provider_group_global_id <= previous) {
                return Err(invalid(
                    "V4 provider tax identity groups must be sorted and unique",
                ));
            }
            match record.state {
                V4TaxIdentityState::MatchedEin => counts[0] += 1,
                V4TaxIdentityState::Missing => counts[1] += 1,
                V4TaxIdentityState::Malformed => counts[2] += 1,
                V4TaxIdentityState::UnsupportedType => counts[3] += 1,
            }
            previous_group = Some(record.provider_group_global_id);
        }
        if counts
            != [
                metadata.matched_ein_count,
                metadata.missing_count,
                metadata.malformed_count,
                metadata.unsupported_type_count,
            ]
        {
            return Err(invalid("V4 provider tax identity state counts changed"));
        }
        Ok(())
    }
}

fn valid_tax_token_policy_id(value: &str) -> bool {
    let Some(key_id) = value.strip_prefix("ptg-tin-hmac-sha256-v1:") else {
        return false;
    };
    value.len() <= 55
        && !key_id.is_empty()
        && key_id.as_bytes().iter().enumerate().all(|(index, byte)| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || (index > 0 && matches!(byte, b'.' | b'_' | b'-'))
        })
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
    tax_identities: V4TaxIdentityFactors,
    input_byte_count: u64,
    input_digest: Sha256,
}

#[derive(Clone, Debug)]
struct V4MergedTaxIdentity {
    state: V4TaxIdentityState,
    tin_hmac_sha256: Option<[u8; 32]>,
    source_bitmap: Vec<u8>,
}

#[derive(Clone, Debug, Default)]
struct V4TaxIdentityFactors {
    token_policy_id: String,
    source_ordinals: Vec<V4TaxSourceOrdinal>,
    source_ordinal_sha256: [u8; 32],
    source_bitmap_bytes: usize,
    by_group: BTreeMap<GlobalId, V4MergedTaxIdentity>,
}

impl RawFactors {
    fn new() -> Self {
        let mut input_digest = Sha256::new();
        input_digest.update(b"PTG2V4INPUT\x01");
        Self {
            set_components: HashMap::new(),
            component_groups: HashMap::new(),
            group_npis: HashMap::new(),
            tax_identities: V4TaxIdentityFactors::default(),
            input_byte_count: 0,
            input_digest,
        }
    }

    fn record_artifact(&mut self, label: &str, artifact: &ValidatedArtifact) {
        self.record_input(label, artifact.sha256, artifact.byte_count);
    }

    fn record_tax_artifact(&mut self, label: &str, artifact: &ValidatedTaxIdentityArtifact) {
        self.record_input(label, artifact.sha256, artifact.byte_count);
    }

    fn record_input(&mut self, label: &str, sha256: [u8; 32], byte_count: u64) {
        self.input_digest.update((label.len() as u32).to_be_bytes());
        self.input_digest.update(label.as_bytes());
        self.input_digest.update(sha256);
        self.input_digest.update(byte_count.to_be_bytes());
        self.input_byte_count = self.input_byte_count.saturating_add(byte_count);
    }
}

fn tax_source_ordinal_sha256(
    source_ordinals: &[V4TaxSourceOrdinal],
) -> ProviderGraphV4Result<[u8; 32]> {
    let mut hasher = Sha256::new();
    hasher.update(TAX_SOURCE_ORDINAL_HASH_DOMAIN);
    hasher.update(
        invalid_conversion(
            u32::try_from(source_ordinals.len()),
            "V4 tax identity source count exceeds uint32",
        )?
        .to_be_bytes(),
    );
    for source in source_ordinals {
        update_length_prefixed(&mut hasher, source.shard_id.as_bytes())?;
        hasher.update(source.ordinal.to_be_bytes());
    }
    Ok(hasher.finalize().into())
}

fn token_policy_descriptor_sha256(token_policy_id: &str) -> ProviderGraphV4Result<[u8; 32]> {
    token_policy_descriptor_sha256_fields([
        token_policy_id,
        TAX_IDENTITY_NORMALIZATION_CONTRACT,
        TAX_IDENTITY_HMAC_CONTRACT,
        TAX_IDENTITY_CANDIDATE_PREFIX_CONTRACT,
        TAX_IDENTITY_AUTHORITY_CONTRACT,
    ])
}

fn token_policy_descriptor_sha256_fields(fields: [&str; 5]) -> ProviderGraphV4Result<[u8; 32]> {
    let mut hasher = Sha256::new();
    hasher.update(TAX_POLICY_DESCRIPTOR_HASH_DOMAIN);
    for field in fields {
        update_length_prefixed(&mut hasher, field.as_bytes())?;
    }
    Ok(hasher.finalize().into())
}

fn merge_tax_identity_artifact(
    factors: &mut V4TaxIdentityFactors,
    artifact: &ValidatedTaxIdentityArtifact,
    source_ordinal: usize,
    progress: &mut ProgressReporter<'_>,
    completed_items: &mut u64,
    total_items: u64,
) -> ProviderGraphV4Result<()> {
    let bitmap_index = source_ordinal / 8;
    let bitmap_bit = 1u8 << (source_ordinal % 8);
    for index in 0..artifact.row_count {
        let record = artifact.record(index)?;
        let token =
            (record.state == V4TaxIdentityState::MatchedEin).then_some(record.tin_hmac_sha256);
        let merged = factors
            .by_group
            .entry(record.provider_group_global_id)
            .or_insert_with(|| V4MergedTaxIdentity {
                state: record.state,
                tin_hmac_sha256: token,
                source_bitmap: vec![0; factors.source_bitmap_bytes],
            });
        if let (Some(left), Some(right)) = (merged.tin_hmac_sha256, token) {
            if left != right {
                return Err(invalid(
                    "V4 provider group has conflicting full tax identity HMACs",
                ));
            }
        }
        if record.state.priority() > merged.state.priority() {
            merged.state = record.state;
            merged.tin_hmac_sha256 = token;
        }
        *merged.source_bitmap.get_mut(bitmap_index).ok_or(invalid(
            "V4 tax identity source bitmap ordinal is out of range",
        ))? |= bitmap_bit;
        *completed_items = completed_items
            .checked_add(1)
            .ok_or(invalid("V4 factor progress count overflows"))?;
        progress.periodic(
            "load_factors",
            *completed_items,
            total_items,
            "factor_items",
        );
    }
    Ok(())
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
            "factor_items",
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
            "factor_items",
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
    let mut ordered_descriptors = descriptors.iter().collect::<Vec<_>>();
    ordered_descriptors.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
    let total_edges = ordered_descriptors
        .iter()
        .try_fold(0u64, |total, descriptor| {
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
                .and_then(|value| {
                    value.checked_add(descriptor.provider_group_tax_identity.metadata.row_count)
                })
                .ok_or(invalid("V4 factor progress total overflows"))
        })?;
    let mut completed_edges = 0u64;
    progress.periodic("load_factors", 0, total_edges, "factor_items");
    let mut seen = HashSet::new();
    let mut raw = RawFactors::new();
    let source_bitmap_bytes = ordered_descriptors
        .len()
        .checked_add(7)
        .ok_or(invalid("V4 tax identity source bitmap width overflows"))?
        / 8;
    if source_bitmap_bytes == 0 {
        return Err(invalid(
            "V4 provider tax identity source set must not be empty",
        ));
    }
    raw.tax_identities.source_bitmap_bytes = source_bitmap_bytes;
    raw.tax_identities.source_ordinals = ordered_descriptors
        .iter()
        .enumerate()
        .map(|(ordinal, descriptor)| {
            Ok(V4TaxSourceOrdinal {
                shard_id: descriptor.shard_id.clone(),
                ordinal: invalid_conversion(
                    u32::try_from(ordinal),
                    "V4 tax identity source ordinal exceeds uint32",
                )?,
            })
        })
        .collect::<ProviderGraphV4Result<Vec<_>>>()?;
    raw.tax_identities.source_ordinal_sha256 =
        tax_source_ordinal_sha256(&raw.tax_identities.source_ordinals)?;
    for (source_ordinal, descriptor) in ordered_descriptors.into_iter().enumerate() {
        let shard_id = descriptor.shard_id.trim();
        if shard_id.is_empty()
            || descriptor.shard_id != shard_id
            || !seen.insert(shard_id.to_owned())
        {
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
        let tax_metadata = &descriptor.provider_group_tax_identity.metadata;
        let source = tax_metadata.source_shard_id.as_deref().map(str::trim);
        let alias = tax_metadata.shard_id.as_deref().map(str::trim);
        if source.is_some() && alias.is_some() && source != alias {
            return Err(invalid(
                "V4 provider tax identity has contradictory shard IDs",
            ));
        }
        if source.or(alias) != Some(shard_id) {
            return Err(invalid(format!(
                "V4 provider tax identity shard ID does not match bundle {shard_id}"
            )));
        }
        let set_component = ValidatedArtifact::open(&descriptor.provider_set_component)?;
        let component_group = ValidatedArtifact::open(&descriptor.provider_component_group)?;
        let group_npi = ValidatedArtifact::open(&descriptor.provider_group_npi)?;
        let npi_group = ValidatedArtifact::open(&descriptor.provider_npi_group)?;
        let tax_identity =
            ValidatedTaxIdentityArtifact::open(&descriptor.provider_group_tax_identity)?;
        if raw.tax_identities.token_policy_id.is_empty() {
            raw.tax_identities.token_policy_id = tax_identity.token_policy_id.clone();
        } else if raw.tax_identities.token_policy_id != tax_identity.token_policy_id {
            return Err(invalid(
                "V4 provider tax identity token policy differs across shards",
            ));
        }
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
        raw.record_tax_artifact("provider_group_tax_identity", &tax_identity);
        merge_tax_identity_artifact(
            &mut raw.tax_identities,
            &tax_identity,
            source_ordinal,
            progress,
            &mut completed_edges,
            total_edges,
        )?;
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
    if raw.tax_identities.token_policy_id.is_empty() {
        return Err(invalid(
            "V4 provider tax identity token policy is unavailable",
        ));
    }
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
    let mut matched_ein_occurrence_upper_bound = 0u64;
    let shard_count = invalid_conversion(
        u64::try_from(descriptors.len()),
        "resource_admission: shard count exceeds uint64",
    )?;
    let source_bitmap_bytes = shard_count.checked_add(7).ok_or(invalid(
        "resource_admission: tax identity bitmap width overflows",
    ))? / 8;
    let mut tax_identity_group_occurrence_upper_bound = 0u64;
    // This covers the ordinal vector, cloned shard IDs, and the temporary
    // uniqueness set before any factor mmap is opened.
    let mut tax_identity_source_ordinal_upper_bound_bytes = shard_count
        .checked_mul(TAX_SOURCE_ORDINAL_FIXED_UPPER_BOUND_BYTES)
        .ok_or(invalid(
            "resource_admission: tax identity source ordinal bytes overflow",
        ))?;
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
        input_factor_bytes = input_factor_bytes
            .checked_add(shard.provider_group_tax_identity.metadata.byte_count)
            .ok_or(invalid("resource_admission: input byte count overflows"))?;
        factor_edge_count = factor_edge_count
            .checked_add(shard.provider_group_tax_identity.metadata.row_count)
            .ok_or(invalid("resource_admission: factor edge count overflows"))?;
        factor_owner_count = factor_owner_count
            .checked_add(
                shard
                    .provider_group_tax_identity
                    .metadata
                    .provider_group_count,
            )
            .ok_or(invalid("resource_admission: factor owner count overflows"))?;
        tax_identity_group_occurrence_upper_bound = tax_identity_group_occurrence_upper_bound
            .checked_add(
                shard
                    .provider_group_tax_identity
                    .metadata
                    .provider_group_count,
            )
            .ok_or(invalid(
                "resource_admission: tax identity group occurrence count overflows",
            ))?;
        matched_ein_occurrence_upper_bound = matched_ein_occurrence_upper_bound
            .checked_add(shard.provider_group_tax_identity.metadata.matched_ein_count)
            .ok_or(invalid(
                "resource_admission: matched tax identity count overflows",
            ))?;
        let shard_id_bytes = invalid_conversion(
            u64::try_from(shard.shard_id.len()),
            "resource_admission: shard ID length exceeds uint64",
        )?;
        tax_identity_source_ordinal_upper_bound_bytes =
            tax_identity_source_ordinal_upper_bound_bytes
                .checked_add(
                    shard_id_bytes
                        .checked_mul(TAX_SOURCE_IDENTITY_COPY_UPPER_BOUND)
                        .ok_or(invalid(
                            "resource_admission: tax identity source ID bytes overflow",
                        ))?,
                )
                .ok_or(invalid(
                    "resource_admission: tax identity source ordinal bytes overflow",
                ))?;
    }
    let tax_identity_merge_bitmap_upper_bound_bytes = tax_identity_group_occurrence_upper_bound
        .checked_mul(source_bitmap_bytes)
        .ok_or(invalid(
            "resource_admission: tax identity merge bitmap bytes overflow",
        ))?;
    let tax_identity_projection_upper_bound_bytes = tax_identity_group_occurrence_upper_bound
        .checked_mul(TAX_IDENTITY_GROUP_ENTRY_UPPER_BOUND_BYTES.saturating_add(source_bitmap_bytes))
        .and_then(|value| {
            value.checked_add(
                matched_ein_occurrence_upper_bound
                    .saturating_mul(TAX_IDENTITY_DICTIONARY_ENTRY_UPPER_BOUND_BYTES),
            )
        })
        .ok_or(invalid(
            "resource_admission: tax identity projection upper bound overflows",
        ))?;
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
        .and_then(|value| value.checked_add(tax_identity_merge_bitmap_upper_bound_bytes))
        .and_then(|value| value.checked_add(tax_identity_source_ordinal_upper_bound_bytes))
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
        .checked_add(tax_identity_projection_upper_bound_bytes)
        .and_then(|value| value.checked_add(bounded_emission_buffer_bytes))
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
            formula: "base(input_factor_bytes + provider_set_key_map_bytes*4 + factor_edges*128 + factor_owners*256 + tax_identity_merge_bitmap_upper_bound_bytes + tax_identity_source_ordinal_upper_bound_bytes) + derived_projection_bytes + tax_identity_projection_bytes(preflight=tax_identity_projection_upper_bound_bytes,reconciled=exact) + retained_scratch_high_water_bytes + bounded_emission_buffer_bytes"
                .to_string(),
            input_factor_bytes,
            provider_set_key_map_bytes,
            factor_edge_count,
            factor_owner_count,
            tax_identity_merge_bitmap_upper_bound_bytes,
            tax_identity_source_ordinal_upper_bound_bytes,
            tax_identity_projection_upper_bound_bytes,
            base_estimated_model_bytes,
            derived_projection_bytes: 0,
            tax_identity_projection_bytes: tax_identity_projection_upper_bound_bytes,
            retained_scratch_high_water_bytes: 0,
            bounded_emission_buffer_bytes,
            estimated_peak_bytes,
            max_estimated_model_bytes: options.max_estimated_model_bytes,
            max_factor_edges: options.max_factor_edges,
        },
        tax_identity_projection_reconciled: false,
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
    npi_prefix_complete_member_count: u64,
    npi_prefix_complete_encoded_bytes: u64,
    npi_prefix_complete_projection_encoded_bytes: u64,
    npi_prefix_complete_eligible: bool,
    npi_prefix_sparse_eligible: bool,
    npi_prefix_sparse_owner_count: u64,
    npi_prefix_sparse_member_count: u64,
    npi_prefix_sparse_raw_bytes: u64,
    npi_prefix_sparse_projection_encoded_bytes: u64,
    group_patterns: Vec<u32>,
    pattern_groups: Vec<Vec<u32>>,
    pattern_sets: Vec<Vec<u32>>,
    pattern_digests: Vec<[u8; 32]>,
    set_patterns: Vec<Vec<u32>>,
    npi_patterns: Vec<Vec<u32>>,
    observe: V4ObserveCounters,
}

#[derive(Debug)]
struct V4TaxIdentityModel {
    token_policy_id: String,
    source_ordinals: Vec<V4TaxSourceOrdinal>,
    source_ordinal_sha256: [u8; 32],
    source_bitmap_bytes: usize,
    tin_hmacs: Vec<[u8; 32]>,
    group_rows: Vec<(GlobalId, V4TaxIdentityState, Option<u32>, Vec<u8>)>,
}

impl V4TaxIdentityModel {
    fn build(
        factors: &V4TaxIdentityFactors,
        provider_group_globals: &[GlobalId],
    ) -> ProviderGraphV4Result<Self> {
        let observed_groups = factors.by_group.keys().copied().collect::<Vec<_>>();
        if observed_groups != provider_group_globals {
            return Err(invalid(
                "V4 provider tax identity group set differs from provider-group dictionary",
            ));
        }
        let mut tin_hmacs = factors
            .by_group
            .values()
            .filter_map(|identity| identity.tin_hmac_sha256)
            .collect::<Vec<_>>();
        tin_hmacs.sort_unstable();
        tin_hmacs.dedup();
        if tin_hmacs.len() > u32::MAX as usize {
            return Err(invalid(
                "V4 provider tax identity dictionary exceeds uint32",
            ));
        }
        let tin_key_by_hmac = tin_hmacs
            .iter()
            .copied()
            .enumerate()
            .map(|(index, hmac)| (hmac, index as u32))
            .collect::<HashMap<_, _>>();
        let mut group_rows = Vec::with_capacity(provider_group_globals.len());
        for group in provider_group_globals {
            let identity = factors
                .by_group
                .get(group)
                .ok_or(invalid("V4 provider tax identity group disappeared"))?;
            if identity.source_bitmap.len() != factors.source_bitmap_bytes
                || identity.source_bitmap.iter().all(|byte| *byte == 0)
            {
                return Err(invalid(
                    "V4 provider tax identity source bitmap is not canonical",
                ));
            }
            let unused_bits = factors.source_bitmap_bytes * 8 - factors.source_ordinals.len();
            if unused_bits > 0 {
                let valid_bits = 8 - unused_bits;
                let invalid_mask = !((1u8 << valid_bits) - 1);
                if identity.source_bitmap.last().copied().unwrap_or_default() & invalid_mask != 0 {
                    return Err(invalid(
                        "V4 provider tax identity source bitmap has out-of-range bits",
                    ));
                }
            }
            let tin_key = match (identity.state, identity.tin_hmac_sha256) {
                (V4TaxIdentityState::MatchedEin, Some(hmac)) => {
                    Some(*tin_key_by_hmac.get(&hmac).ok_or(invalid(
                        "V4 provider tax identity dictionary lookup is inconsistent",
                    ))?)
                }
                (V4TaxIdentityState::MatchedEin, None) => {
                    return Err(invalid("V4 matched provider tax identity has no full HMAC"));
                }
                (_, None) => None,
                (_, Some(_)) => {
                    return Err(invalid(
                        "V4 unavailable provider tax identity has a full HMAC",
                    ));
                }
            };
            group_rows.push((
                *group,
                identity.state,
                tin_key,
                identity.source_bitmap.clone(),
            ));
        }
        Ok(Self {
            token_policy_id: factors.token_policy_id.clone(),
            source_ordinals: factors.source_ordinals.clone(),
            source_ordinal_sha256: factors.source_ordinal_sha256,
            source_bitmap_bytes: factors.source_bitmap_bytes,
            tin_hmacs,
            group_rows,
        })
    }

    fn content_digest(&self) -> ProviderGraphV4Result<[u8; 32]> {
        let mut hasher = Sha256::new();
        hasher.update(TAX_CONTENT_HASH_DOMAIN);
        hasher.update(token_policy_descriptor_sha256(&self.token_policy_id)?);
        hasher.update(self.source_ordinal_sha256);
        hasher.update(
            invalid_conversion(
                u64::try_from(self.tin_hmacs.len()),
                "V4 tax identity dictionary count exceeds uint64",
            )?
            .to_be_bytes(),
        );
        for hmac in &self.tin_hmacs {
            hasher.update(hmac);
        }
        hasher.update(
            invalid_conversion(
                u64::try_from(self.group_rows.len()),
                "V4 provider tax identity row count exceeds uint64",
            )?
            .to_be_bytes(),
        );
        for (group, state, tin_key, source_bitmap) in &self.group_rows {
            hasher.update(group);
            hasher.update([state.code()]);
            match tin_key {
                Some(value) => {
                    hasher.update([1]);
                    hasher.update(value.to_be_bytes());
                }
                None => hasher.update([0]),
            }
            update_length_prefixed(&mut hasher, source_bitmap)?;
        }
        Ok(hasher.finalize().into())
    }

    fn summary(&self) -> ProviderGraphV4Result<V4TaxIdentitySummary> {
        let mut counts = [0u64; 4];
        for (_, state, _, _) in &self.group_rows {
            match state {
                V4TaxIdentityState::MatchedEin => counts[0] += 1,
                V4TaxIdentityState::Missing => counts[1] += 1,
                V4TaxIdentityState::Malformed => counts[2] += 1,
                V4TaxIdentityState::UnsupportedType => counts[3] += 1,
            }
        }
        Ok(V4TaxIdentitySummary {
            contract: TAX_IDENTITY_PROJECTION_CONTRACT.to_owned(),
            token_policy_id: self.token_policy_id.clone(),
            token_policy_descriptor_sha256: hex(&token_policy_descriptor_sha256(
                &self.token_policy_id,
            )?),
            normalization_contract: TAX_IDENTITY_NORMALIZATION_CONTRACT.to_owned(),
            hmac_contract: TAX_IDENTITY_HMAC_CONTRACT.to_owned(),
            candidate_prefix_contract: TAX_IDENTITY_CANDIDATE_PREFIX_CONTRACT.to_owned(),
            authority_contract: TAX_IDENTITY_AUTHORITY_CONTRACT.to_owned(),
            source_ordinal_contract: TAX_SOURCE_ORDINAL_CONTRACT.to_owned(),
            source_ordinal_map: self.source_ordinals.clone(),
            source_ordinal_map_digest: hex(&self.source_ordinal_sha256),
            source_shard_count: self.source_ordinals.len() as u64,
            source_bitmap_bytes: self.source_bitmap_bytes as u64,
            provider_group_count: self.group_rows.len() as u64,
            tax_identity_count: self.tin_hmacs.len() as u64,
            matched_ein_count: counts[0],
            missing_count: counts[1],
            malformed_count: counts[2],
            unsupported_type_count: counts[3],
            content_digest: hex(&self.content_digest()?),
        })
    }
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
    complete_member_count: u64,
    complete_encoded_bytes: u64,
    complete_projection_encoded_bytes: u64,
    complete_eligible: bool,
    sparse_eligible: bool,
    sparse_projection_encoded_bytes: u64,
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
    let mut complete_total_members = 0usize;
    let mut sparse_eligible = true;
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
        let degree_over_cap = exact_group_degree > online_cap;
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
        if degree_over_cap && effective_target > 0 && bounded.source_exhausted {
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
        group_unsafe_set_count =
            group_unsafe_set_count.saturating_add(u64::from(!bounded_complete));
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
            complete_total_members = complete_total_members
                .checked_add(bounded.members.len())
                .ok_or(invalid("V4 complete NPI prefix member count overflows"))?;
            lists[set_index] = bounded.members;
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
        let raw_bytes = (total_members as u64)
            .checked_mul(4)
            .ok_or(invalid("V4 NPI prefix override byte count overflows"))?;
        let member_count = invalid_conversion(
            u32::try_from(exact.members.len()),
            "V4 NPI prefix override member count exceeds uint32",
        )?;
        metadata.push((owner_key, member_count, npi_prefix_digest(&exact.members)));
        sparse_eligible = metadata.len() <= options.max_npi_prefix_override_owners
            && raw_bytes <= options.max_npi_prefix_override_bytes;
        complete_total_members = complete_total_members
            .checked_add(exact.members.len())
            .ok_or(invalid("V4 complete NPI prefix member count overflows"))?;
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
        "complete direct-layout NPI prefix candidate",
        checked_estimated_sum(
            [
                estimated_vec_owner_bytes(lists.len())?,
                estimated_u32_capacity_bytes(complete_total_members)?,
                estimated_vec_owner_bytes(lists.len())?,
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
    let complete_encoded_bytes = relation_encoded_bytes(
        &RelationShape {
            relation: "set_npi_prefix_override",
            owner_count: lists.len(),
            member_count: complete_total_members as u64,
        },
        options,
    )?;
    let complete_projection_encoded_bytes = complete_encoded_bytes
        .checked_add(dictionary_copy_bytes(&[4, 4, 32], lists.len())?)
        .ok_or(invalid(
            "V4 complete NPI prefix projection byte count overflows",
        ))?;
    let complete_eligible =
        complete_prefix_projection_eligible(complete_projection_encoded_bytes, options);
    let sparse_projection_encoded_bytes = encoded_bytes
        .checked_add(dictionary_copy_bytes(&[4, 4, 32], metadata.len())?)
        .ok_or(invalid(
            "V4 sparse NPI prefix projection byte count overflows",
        ))?;
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
        complete_member_count: complete_total_members as u64,
        complete_encoded_bytes,
        complete_projection_encoded_bytes,
        complete_eligible,
        sparse_eligible,
        sparse_projection_encoded_bytes,
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
        pattern_component_over_cap_set_count: 0,
        pattern_component_over_cap_prefix_covered_set_count: 0,
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
    let npi_prefix_sparse_owner_count = npi_prefix_override_plan.metadata.len() as u64;
    let npi_prefix_sparse_member_count: u64 = npi_prefix_override_plan
        .metadata
        .iter()
        .map(|(_, member_count, _)| u64::from(*member_count))
        .sum();
    let npi_prefix_sparse_raw_bytes = npi_prefix_sparse_member_count
        .checked_mul(4)
        .ok_or(invalid("V4 sparse NPI prefix raw bytes overflow"))?;
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
        npi_prefix_complete_member_count: npi_prefix_override_plan.complete_member_count,
        npi_prefix_complete_encoded_bytes: npi_prefix_override_plan.complete_encoded_bytes,
        npi_prefix_complete_projection_encoded_bytes: npi_prefix_override_plan
            .complete_projection_encoded_bytes,
        npi_prefix_complete_eligible: npi_prefix_override_plan.complete_eligible,
        npi_prefix_sparse_eligible: npi_prefix_override_plan.sparse_eligible,
        npi_prefix_sparse_owner_count,
        npi_prefix_sparse_member_count,
        npi_prefix_sparse_raw_bytes,
        npi_prefix_sparse_projection_encoded_bytes: npi_prefix_override_plan
            .sparse_projection_encoded_bytes,
        group_patterns,
        pattern_groups,
        pattern_sets,
        pattern_digests,
        set_patterns,
        npi_patterns,
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

fn tax_identity_copy_bytes(model: &V4TaxIdentityModel) -> ProviderGraphV4Result<u64> {
    let token_bytes = dictionary_copy_bytes(&[4, 16, 32], model.tin_hmacs.len())?;
    let group_rows =
        model
            .group_rows
            .iter()
            .try_fold(0u64, |total, (_, state, tin_key, bitmap)| {
                let row_bytes = 2u64
                    .checked_add(4 + GLOBAL_ID_BYTES as u64)
                    .and_then(|value| value.checked_add(4 + state.as_str().len() as u64))
                    .and_then(|value| value.checked_add(4 + u64::from(tin_key.is_some()) * 4))
                    .and_then(|value| value.checked_add(4 + bitmap.len() as u64))
                    .ok_or(invalid("V4 tax identity COPY row size overflows"))?;
                total
                    .checked_add(row_bytes)
                    .ok_or(invalid("V4 tax identity COPY size overflows"))
            })?;
    token_bytes
        .checked_add(PG_COPY_HEADER.len() as u64)
        .and_then(|value| value.checked_add(2))
        .and_then(|value| value.checked_add(group_rows))
        .ok_or(invalid("V4 tax identity COPY size overflows"))
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
    encoded_byte_count: u64,
    block_count: u64,
}

fn maybe_heavy_bitmap_geometry(
    relation: &'static str,
    owner_key: u32,
    member_count: usize,
    member_bounds: Option<(u32, u32)>,
    options: &ProviderGraphV4Options,
) -> ProviderGraphV4Result<Option<HeavyBitmapPlan>> {
    if member_count < options.heavy_owner_member_threshold || member_count == 0 {
        return Ok(None);
    }
    let Some(fragment_content_bytes) = options
        .member_page_bytes
        .checked_sub(HEAVY_BITMAP_FRAGMENT_HEADER_BYTES)
        .filter(|value| *value > 0)
    else {
        return Ok(None);
    };
    let (minimum, maximum) = member_bounds.ok_or(invalid(
        "V4 heavy bitmap geometry lacks non-empty member bounds",
    ))?;
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
    let vector_bytes = (member_count as u64)
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
        u32::try_from(member_count),
        "V4 heavy bitmap member count exceeds uint32",
    )?;
    Ok(Some(HeavyBitmapPlan {
        relation,
        owner_key,
        member_count: member_count as u64,
        member_base: minimum,
        member_span: span,
        logical_byte_count,
        raw_byte_count: payload_bytes,
        vector_byte_count: vector_bytes,
        encoded_byte_count: bitmap_encoded_bytes,
        block_count: fragment_count,
    }))
}

fn maybe_heavy_bitmap(
    relation: &'static str,
    owner_key: u32,
    members: &[u32],
    options: &ProviderGraphV4Options,
) -> ProviderGraphV4Result<Option<HeavyBitmapPlan>> {
    maybe_heavy_bitmap_geometry(
        relation,
        owner_key,
        members.len(),
        members.first().copied().zip(members.last().copied()),
        options,
    )
}

#[derive(Debug)]
struct LayoutSizes {
    common: u64,
    direct_graph: u64,
    pattern_graph: u64,
    direct_inferred_taxonomy: u64,
    pattern_inferred_taxonomy: u64,
    direct_mapping: u64,
    pattern_mapping: u64,
    direct_map_payload: u64,
    pattern_map_payload: u64,
    direct_map_coordinate_count: u64,
    pattern_map_coordinate_count: u64,
    direct_map_pack_count: u64,
    pattern_map_pack_count: u64,
    direct_map_object_kind_count: u64,
    pattern_map_object_kind_count: u64,
    direct: u64,
    pattern: u64,
}

fn complete_prefix_projection_eligible(
    projection_encoded_bytes: u64,
    options: &ProviderGraphV4Options,
) -> bool {
    projection_encoded_bytes <= options.max_npi_prefix_override_bytes
        && options
            .max_estimated_model_bytes
            .is_none_or(|limit| projection_encoded_bytes <= limit)
}

#[derive(Debug)]
struct PlannedRelationPersistence {
    relation: &'static str,
    graph_encoded_bytes: u64,
    coordinate_count_by_kind: BTreeMap<String, u64>,
    logical_member_count: u64,
    vector_member_count: u64,
    heavy_owner_plans: Vec<HeavyBitmapPlan>,
}

#[derive(Debug, Eq, PartialEq)]
struct MappingPersistenceSizes {
    total_encoded_bytes: u64,
    map_payload_encoded_bytes: u64,
    coordinate_count: u64,
    pack_count: u64,
    object_kind_count: u64,
}

fn copy_row_encoded_bytes(field_widths: &[usize]) -> ProviderGraphV4Result<u64> {
    field_widths.iter().try_fold(2u64, |total, width| {
        total
            .checked_add(4)
            .and_then(|value| value.checked_add(*width as u64))
            .ok_or(invalid("V4 metadata row encoded size overflows"))
    })
}

fn paged_block_count(total_bytes: u64, page_bytes: usize) -> ProviderGraphV4Result<u64> {
    if total_bytes == 0 {
        return Ok(0);
    }
    total_bytes
        .checked_add(page_bytes as u64 - 1)
        .and_then(|value| value.checked_div(page_bytes as u64))
        .ok_or(invalid("V4 planned block count overflows"))
}

fn planned_relation_persistence(
    relation: &'static str,
    owner_base: u32,
    owner_count: usize,
    owner_geometries: impl IntoIterator<Item = ProviderGraphV4Result<(usize, Option<(u32, u32)>)>>,
    options: &ProviderGraphV4Options,
) -> ProviderGraphV4Result<PlannedRelationPersistence> {
    let mut vector_member_count = 0u64;
    let mut bitmap_encoded_bytes = 0u64;
    let mut logical_member_count = 0u64;
    let mut heavy_owner_plans = Vec::new();
    let mut observed_owner_count = 0usize;
    for (owner_index, geometry) in owner_geometries.into_iter().enumerate() {
        let (member_count, member_bounds) = geometry?;
        let owner_key = owner_base
            .checked_add(owner_index as u32)
            .ok_or(invalid("V4 planned relation owner key overflows"))?;
        logical_member_count = logical_member_count
            .checked_add(member_count as u64)
            .ok_or(invalid("V4 planned logical member count overflows"))?;
        if let Some(plan) =
            maybe_heavy_bitmap_geometry(relation, owner_key, member_count, member_bounds, options)?
        {
            bitmap_encoded_bytes = bitmap_encoded_bytes
                .checked_add(plan.encoded_byte_count)
                .ok_or(invalid("V4 planned bitmap byte count overflows"))?;
            heavy_owner_plans.push(plan);
        } else {
            vector_member_count = vector_member_count
                .checked_add(member_count as u64)
                .ok_or(invalid("V4 planned vector member count overflows"))?;
        }
        observed_owner_count = observed_owner_count
            .checked_add(1)
            .ok_or(invalid("V4 planned relation owner count overflows"))?;
    }
    if observed_owner_count != owner_count {
        return Err(invalid("V4 planned relation owner count changed"));
    }
    let graph_encoded_bytes = relation_encoded_bytes(
        &RelationShape {
            relation,
            owner_count,
            member_count: vector_member_count,
        },
        options,
    )?
    .checked_add(bitmap_encoded_bytes)
    .ok_or(invalid("V4 planned relation byte count overflows"))?;
    let mut coordinate_count_by_kind = BTreeMap::new();
    let member_bytes = vector_member_count
        .checked_mul(4)
        .ok_or(invalid("V4 planned member byte count overflows"))?;
    let member_blocks = paged_block_count(
        member_bytes,
        aligned_page_bytes(options.member_page_bytes, 4),
    )?;
    let locator_blocks = paged_block_count(
        (owner_count as u64)
            .checked_mul(LOCATOR_BYTES as u64)
            .ok_or(invalid("V4 planned locator byte count overflows"))?,
        aligned_page_bytes(options.locator_page_bytes, LOCATOR_BYTES),
    )?;
    if member_blocks > 0 {
        coordinate_count_by_kind.insert(member_kind(relation), member_blocks);
    }
    if locator_blocks > 0 {
        coordinate_count_by_kind.insert(locator_kind(relation), locator_blocks);
    }
    let heavy_blocks = heavy_owner_plans.iter().try_fold(0u64, |total, plan| {
        total
            .checked_add(plan.block_count)
            .ok_or(invalid("V4 planned heavy bitmap block count overflows"))
    })?;
    if heavy_blocks > 0 {
        coordinate_count_by_kind.insert(heavy_bitmap_kind(relation), heavy_blocks);
    }
    Ok(PlannedRelationPersistence {
        relation,
        graph_encoded_bytes,
        coordinate_count_by_kind,
        logical_member_count,
        vector_member_count,
        heavy_owner_plans,
    })
}

fn planned_list_relation_persistence(
    relation: &'static str,
    owner_base: u32,
    lists: &[Vec<u32>],
    options: &ProviderGraphV4Options,
) -> ProviderGraphV4Result<PlannedRelationPersistence> {
    planned_relation_persistence(
        relation,
        owner_base,
        lists.len(),
        lists.iter().map(|members| {
            Ok((
                members.len(),
                members.first().copied().zip(members.last().copied()),
            ))
        }),
        options,
    )
}

fn planned_ordered_relation_persistence(
    relation: &'static str,
    owner_base: u32,
    owner_count: usize,
    member_count: u64,
    options: &ProviderGraphV4Options,
) -> ProviderGraphV4Result<PlannedRelationPersistence> {
    let graph_encoded_bytes = relation_encoded_bytes(
        &RelationShape {
            relation,
            owner_count,
            member_count,
        },
        options,
    )?;
    let mut coordinate_count_by_kind = BTreeMap::new();
    let member_blocks = paged_block_count(
        member_count
            .checked_mul(4)
            .ok_or(invalid("V4 ordered relation member bytes overflow"))?,
        aligned_page_bytes(options.member_page_bytes, 4),
    )?;
    let locator_blocks = paged_block_count(
        (owner_count as u64)
            .checked_mul(LOCATOR_BYTES as u64)
            .ok_or(invalid("V4 ordered relation locator bytes overflow"))?,
        aligned_page_bytes(options.locator_page_bytes, LOCATOR_BYTES),
    )?;
    if member_blocks > 0 {
        coordinate_count_by_kind.insert(member_kind(relation), member_blocks);
    }
    if locator_blocks > 0 {
        coordinate_count_by_kind.insert(locator_kind(relation), locator_blocks);
    }
    let _ = owner_base;
    Ok(PlannedRelationPersistence {
        relation,
        graph_encoded_bytes,
        coordinate_count_by_kind,
        logical_member_count: member_count,
        vector_member_count: member_count,
        heavy_owner_plans: Vec::new(),
    })
}

fn planned_direct_set_group_persistence(
    model: &GraphModel,
    options: &ProviderGraphV4Options,
) -> ProviderGraphV4Result<PlannedRelationPersistence> {
    planned_relation_persistence(
        "set_groups_direct",
        model.set_base,
        model.set_patterns.len(),
        model.set_patterns.iter().map(|patterns| {
            let mut member_count = 0usize;
            let mut minimum = None;
            let mut maximum = None;
            for pattern in patterns {
                let groups = &model.pattern_groups[*pattern as usize];
                member_count = member_count
                    .checked_add(groups.len())
                    .ok_or(invalid("V4 planned set/group count overflows"))?;
                minimum = minimum.into_iter().chain(groups.first().copied()).min();
                maximum = maximum.into_iter().chain(groups.last().copied()).max();
            }
            Ok((member_count, minimum.zip(maximum)))
        }),
        options,
    )
}

fn relation_manifest_row_encoded_bytes(
    plan: &PlannedRelationPersistence,
) -> ProviderGraphV4Result<u64> {
    copy_row_encoded_bytes(&[
        8,
        plan.relation.len(),
        member_kind(plan.relation).len(),
        locator_kind(plan.relation).len(),
        8,
        8,
        8,
        8,
        2,
        4,
        4,
        4,
        8,
    ])
}

fn heavy_owner_rows_encoded_bytes(plan: &PlannedRelationPersistence) -> ProviderGraphV4Result<u64> {
    plan.heavy_owner_plans
        .iter()
        .try_fold(0u64, |total, owner| {
            total
                .checked_add(copy_row_encoded_bytes(&[
                    8,
                    owner.relation.len(),
                    8,
                    heavy_bitmap_kind(owner.relation).len(),
                    8,
                    8,
                    8,
                    4,
                    8,
                ])?)
                .ok_or(invalid("V4 heavy-owner metadata byte count overflows"))
        })
}

fn map_root_row_encoded_bytes(representation: &str) -> ProviderGraphV4Result<u64> {
    copy_row_encoded_bytes(&[
        8,
        "complete".len(),
        2,
        "packed_coordinate_hash_v1".len(),
        representation.len(),
        "snapshot_local_v1".len(),
        32,
        4,
        8,
        8,
        8,
        8,
        8,
        8,
        8,
        8,
        4,
        8,
        8,
        8,
    ])
}

fn layout_mapping_persistence_bytes(
    relations: &[PlannedRelationPersistence],
    representation: &str,
) -> ProviderGraphV4Result<MappingPersistenceSizes> {
    let mut coordinate_count_by_kind = BTreeMap::<String, u64>::new();
    for relation in relations {
        for (object_kind, coordinate_count) in &relation.coordinate_count_by_kind {
            let total = coordinate_count_by_kind
                .entry(object_kind.clone())
                .or_default();
            *total = total
                .checked_add(*coordinate_count)
                .ok_or(invalid("V4 map coordinate count overflows"))?;
        }
    }
    let relation_metadata_bytes = relations.iter().try_fold(0u64, |total, plan| {
        total
            .checked_add(relation_manifest_row_encoded_bytes(plan)?)
            .ok_or(invalid("V4 relation metadata bytes overflow"))
    })?;
    let heavy_metadata_bytes = relations.iter().try_fold(0u64, |total, plan| {
        total
            .checked_add(heavy_owner_rows_encoded_bytes(plan)?)
            .ok_or(invalid("V4 heavy metadata bytes overflow"))
    })?;
    mapping_persistence_bytes(
        coordinate_count_by_kind,
        relation_metadata_bytes,
        heavy_metadata_bytes,
        representation,
    )
}

fn mapping_persistence_bytes(
    coordinate_count_by_kind: BTreeMap<String, u64>,
    relation_metadata_bytes: u64,
    heavy_metadata_bytes: u64,
    representation: &str,
) -> ProviderGraphV4Result<MappingPersistenceSizes> {
    let mut map_block_bytes = 0u64;
    let mut map_payload_bytes = 0u64;
    let mut map_pack_metadata_bytes = 0u64;
    let mut map_pack_count = 0u64;
    let coordinate_count = coordinate_count_by_kind
        .values()
        .try_fold(0u64, |total, count| {
            total
                .checked_add(*count)
                .ok_or(invalid("V4 map coordinate count overflows"))
        })?;
    let object_kind_count = coordinate_count_by_kind.len() as u64;
    for (object_kind, coordinate_count) in coordinate_count_by_kind {
        let full_packs = coordinate_count / V4_MAP_COORDINATES_PER_PACK;
        let remainder = coordinate_count % V4_MAP_COORDINATES_PER_PACK;
        let full_pack_count = invalid_conversion(
            usize::try_from(full_packs),
            "V4 map pack count exceeds usize",
        )?;
        for pack_coordinates in std::iter::repeat_n(V4_MAP_COORDINATES_PER_PACK, full_pack_count)
            .chain((remainder > 0).then_some(remainder))
        {
            let payload_bytes = V4_MAP_HEADER_BYTES
                .checked_add(
                    pack_coordinates
                        .checked_mul(V4_MAP_RECORD_BYTES)
                        .ok_or(invalid("V4 map pack payload bytes overflow"))?,
                )
                .ok_or(invalid("V4 map pack payload bytes overflow"))?;
            map_block_bytes = map_block_bytes
                .checked_add(pg_copy_row_bytes(
                    V4_MAP_BLOCK_KIND,
                    invalid_conversion(
                        usize::try_from(payload_bytes),
                        "V4 map pack payload exceeds usize",
                    )?,
                )?)
                .ok_or(invalid("V4 map block persistence bytes overflow"))?;
            map_payload_bytes = map_payload_bytes
                .checked_add(payload_bytes)
                .ok_or(invalid("V4 map payload bytes overflow"))?;
            map_pack_count = map_pack_count
                .checked_add(1)
                .ok_or(invalid("V4 map pack count overflows"))?;
            map_pack_metadata_bytes = map_pack_metadata_bytes
                .checked_add(copy_row_encoded_bytes(&[
                    8,
                    object_kind.len(),
                    4,
                    8,
                    4,
                    8,
                    4,
                    4,
                    8,
                    8,
                    32,
                    8,
                ])?)
                .ok_or(invalid("V4 map pack metadata bytes overflow"))?;
        }
    }
    let total_encoded_bytes = map_root_row_encoded_bytes(representation)?
        .checked_add(map_block_bytes)
        .and_then(|value| value.checked_add(map_pack_metadata_bytes))
        .and_then(|value| value.checked_add(relation_metadata_bytes))
        .and_then(|value| value.checked_add(heavy_metadata_bytes))
        .ok_or(invalid("V4 mapping persistence bytes overflow"))?;
    Ok(MappingPersistenceSizes {
        total_encoded_bytes,
        map_payload_encoded_bytes: map_payload_bytes,
        coordinate_count,
        pack_count: map_pack_count,
        object_kind_count,
    })
}

fn emitted_mapping_persistence_bytes(
    relations: &[V4RelationSummary],
    heavy_owners: &[V4HeavyBitmapSummary],
    representation: &str,
) -> ProviderGraphV4Result<MappingPersistenceSizes> {
    let mut coordinate_count_by_kind = BTreeMap::<String, u64>::new();
    let mut relation_metadata_bytes = 0u64;
    for relation in relations {
        for (object_kind, block_count) in [
            (&relation.member_object_kind, relation.member_block_count),
            (&relation.locator_object_kind, relation.locator_block_count),
        ] {
            if block_count > 0 {
                coordinate_count_by_kind.insert(object_kind.clone(), block_count);
            }
        }
        relation_metadata_bytes = relation_metadata_bytes
            .checked_add(copy_row_encoded_bytes(&[
                8,
                relation.relation.len(),
                relation.member_object_kind.len(),
                relation.locator_object_kind.len(),
                8,
                8,
                8,
                8,
                2,
                4,
                4,
                4,
                8,
            ])?)
            .ok_or(invalid("V4 emitted relation metadata bytes overflow"))?;
    }
    let mut heavy_metadata_bytes = 0u64;
    for owner in heavy_owners {
        let total = coordinate_count_by_kind
            .entry(owner.object_kind.clone())
            .or_default();
        *total = total
            .checked_add(owner.block_count)
            .ok_or(invalid("V4 emitted bitmap coordinate count overflows"))?;
        heavy_metadata_bytes = heavy_metadata_bytes
            .checked_add(copy_row_encoded_bytes(&[
                8,
                owner.relation.len(),
                8,
                owner.object_kind.len(),
                8,
                8,
                8,
                4,
                8,
            ])?)
            .ok_or(invalid("V4 emitted heavy metadata bytes overflow"))?;
    }
    mapping_persistence_bytes(
        coordinate_count_by_kind,
        relation_metadata_bytes,
        heavy_metadata_bytes,
        representation,
    )
}

fn compute_layout_sizes(
    model: &GraphModel,
    tax_identity: &V4TaxIdentityModel,
    direct_inferred_taxonomy: &V4InferredTaxonomyProjection,
    pattern_inferred_taxonomy: &V4InferredTaxonomyProjection,
    options: &ProviderGraphV4Options,
) -> ProviderGraphV4Result<LayoutSizes> {
    let common_relation_plans = [
        planned_list_relation_persistence(
            "set_components",
            model.set_base,
            &model.set_components,
            options,
        )?,
        planned_list_relation_persistence("component_groups", 0, &model.component_groups, options)?,
        planned_list_relation_persistence("npi_groups_exact", 0, &model.npi_groups, options)?,
        planned_list_relation_persistence("group_npis_exact", 0, &model.group_npis, options)?,
    ];
    let common_relation_bytes = common_relation_plans.iter().try_fold(0u64, |total, plan| {
        total
            .checked_add(plan.graph_encoded_bytes)
            .ok_or(invalid("V4 common encoded byte count overflows"))
    })?;
    let tax_dictionary_bytes = tax_identity_copy_bytes(tax_identity)?;
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
        .and_then(|value| value.checked_add(tax_dictionary_bytes))
        .ok_or(invalid("V4 common dictionary byte count overflows"))?;
    let sparse_prefix_relation = planned_ordered_relation_persistence(
        "set_npi_prefix_override",
        model.set_base,
        model.set_npi_prefix_overrides.len(),
        model.npi_prefix_sparse_member_count,
        options,
    )?;
    let sparse_prefix_dictionary =
        dictionary_copy_bytes(&[4, 4, 32], model.npi_prefix_sparse_owner_count as usize)?;
    let complete_prefix_relation = planned_ordered_relation_persistence(
        "set_npi_prefix_override",
        model.set_base,
        model.set_npi_prefix_overrides.len(),
        model.npi_prefix_complete_member_count,
        options,
    )?;
    let complete_prefix_dictionary =
        dictionary_copy_bytes(&[4, 4, 32], model.set_npi_prefix_overrides.len())?;

    let group_set_geometries = model.group_patterns.iter().map(|pattern| {
        let members = &model.pattern_sets[*pattern as usize];
        Ok((
            members.len(),
            members.first().copied().zip(members.last().copied()),
        ))
    });
    let direct_relation_plans = [
        planned_relation_persistence(
            "group_sets_direct",
            0,
            model.group_patterns.len(),
            group_set_geometries,
            options,
        )?,
        planned_direct_set_group_persistence(model, options)?,
    ];
    let direct_projection = direct_relation_plans.iter().try_fold(0u64, |total, plan| {
        total
            .checked_add(plan.graph_encoded_bytes)
            .ok_or(invalid("V4 direct encoded byte count overflows"))
    })?;
    let group_pattern_geometries = model
        .group_patterns
        .iter()
        .map(|pattern| Ok((1usize, Some((*pattern, *pattern)))));
    let pattern_relation_plans = [
        planned_relation_persistence(
            "group_patterns",
            0,
            model.group_patterns.len(),
            group_pattern_geometries,
            options,
        )?,
        planned_list_relation_persistence("pattern_groups", 0, &model.pattern_groups, options)?,
        planned_list_relation_persistence("pattern_sets", 0, &model.pattern_sets, options)?,
        planned_list_relation_persistence(
            "set_patterns",
            model.set_base,
            &model.set_patterns,
            options,
        )?,
        planned_list_relation_persistence("npi_patterns", 0, &model.npi_patterns, options)?,
    ];
    let pattern_projection = pattern_relation_plans
        .iter()
        .try_fold(0u64, |total, plan| {
            total
                .checked_add(plan.graph_encoded_bytes)
                .ok_or(invalid("V4 pattern encoded byte count overflows"))
        })?;
    let common = common_relation_bytes
        .checked_add(common_dictionaries)
        .and_then(|value| value.checked_add(PG_COPY_HEADER.len() as u64 + 2))
        .ok_or(invalid("V4 common encoded byte count overflows"))?;
    let direct_graph = common
        .checked_add(complete_prefix_relation.graph_encoded_bytes)
        .and_then(|value| value.checked_add(complete_prefix_dictionary))
        .and_then(|value| value.checked_add(direct_projection))
        .ok_or(invalid("V4 direct complete byte count overflows"))?;
    let pattern_dictionary = dictionary_copy_bytes(&[4, 32, 8], model.pattern_sets.len())?;
    let pattern_graph = common
        .checked_add(sparse_prefix_relation.graph_encoded_bytes)
        .and_then(|value| value.checked_add(sparse_prefix_dictionary))
        .and_then(|value| value.checked_add(pattern_projection))
        .and_then(|value| value.checked_add(pattern_dictionary))
        .ok_or(invalid("V4 pattern complete byte count overflows"))?;
    let mut direct_all_relations = common_relation_plans
        .iter()
        .chain(std::iter::once(&complete_prefix_relation))
        .chain(direct_relation_plans.iter())
        .collect::<Vec<_>>();
    let mut pattern_all_relations = common_relation_plans
        .iter()
        .chain(std::iter::once(&sparse_prefix_relation))
        .chain(pattern_relation_plans.iter())
        .collect::<Vec<_>>();
    let direct_owned = direct_all_relations
        .drain(..)
        .map(|plan| PlannedRelationPersistence {
            relation: plan.relation,
            graph_encoded_bytes: plan.graph_encoded_bytes,
            coordinate_count_by_kind: plan.coordinate_count_by_kind.clone(),
            logical_member_count: plan.logical_member_count,
            vector_member_count: plan.vector_member_count,
            heavy_owner_plans: plan.heavy_owner_plans.clone(),
        })
        .collect::<Vec<_>>();
    let pattern_owned = pattern_all_relations
        .drain(..)
        .map(|plan| PlannedRelationPersistence {
            relation: plan.relation,
            graph_encoded_bytes: plan.graph_encoded_bytes,
            coordinate_count_by_kind: plan.coordinate_count_by_kind.clone(),
            logical_member_count: plan.logical_member_count,
            vector_member_count: plan.vector_member_count,
            heavy_owner_plans: plan.heavy_owner_plans.clone(),
        })
        .collect::<Vec<_>>();
    let direct_mapping = layout_mapping_persistence_bytes(&direct_owned, "direct_v1")?;
    let pattern_mapping = layout_mapping_persistence_bytes(&pattern_owned, "pattern_v1")?;
    let direct = direct_graph
        .checked_add(direct_mapping.total_encoded_bytes)
        .and_then(|value| value.checked_add(direct_inferred_taxonomy.encoded_bytes))
        .ok_or(invalid("V4 direct persistent byte count overflows"))?;
    let pattern = pattern_graph
        .checked_add(pattern_mapping.total_encoded_bytes)
        .and_then(|value| value.checked_add(pattern_inferred_taxonomy.encoded_bytes))
        .ok_or(invalid("V4 pattern persistent byte count overflows"))?;
    Ok(LayoutSizes {
        common,
        direct_graph,
        pattern_graph,
        direct_inferred_taxonomy: direct_inferred_taxonomy.encoded_bytes,
        pattern_inferred_taxonomy: pattern_inferred_taxonomy.encoded_bytes,
        direct_mapping: direct_mapping.total_encoded_bytes,
        pattern_mapping: pattern_mapping.total_encoded_bytes,
        direct_map_payload: direct_mapping.map_payload_encoded_bytes,
        pattern_map_payload: pattern_mapping.map_payload_encoded_bytes,
        direct_map_coordinate_count: direct_mapping.coordinate_count,
        pattern_map_coordinate_count: pattern_mapping.coordinate_count,
        direct_map_pack_count: direct_mapping.pack_count,
        pattern_map_pack_count: pattern_mapping.pack_count,
        direct_map_object_kind_count: direct_mapping.object_kind_count,
        pattern_map_object_kind_count: pattern_mapping.object_kind_count,
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
    let exact_prefix_owners = model
        .provider_set_npi_prefix_override_metadata
        .iter()
        .map(|(owner_key, _, _)| *owner_key)
        .collect::<HashSet<_>>();
    let mut overflow_set_count = 0u64;
    let mut maximum_components_per_overflow_set = 0u64;
    let mut component_over_cap_set_count = 0u64;
    let mut component_over_cap_prefix_covered_set_count = 0u64;
    let mut unsafe_set_count = 0u64;
    for (set_index, (patterns, components)) in model
        .set_patterns
        .iter()
        .zip(&model.set_components)
        .enumerate()
    {
        if patterns.len() <= options.max_set_patterns_per_set {
            continue;
        }
        overflow_set_count = overflow_set_count.saturating_add(1);
        maximum_components_per_overflow_set =
            maximum_components_per_overflow_set.max(components.len() as u64);
        if components.len() > options.max_set_components_per_fallback_set {
            component_over_cap_set_count = component_over_cap_set_count.saturating_add(1);
            let owner_key = model
                .set_base
                .checked_add(set_index as u32)
                .expect("provider-set key was validated while building the model");
            if exact_prefix_owners.contains(&owner_key) {
                component_over_cap_prefix_covered_set_count =
                    component_over_cap_prefix_covered_set_count.saturating_add(1);
            } else {
                unsafe_set_count = unsafe_set_count.saturating_add(1);
            }
        }
    }
    observe.pattern_overflow_set_count = overflow_set_count;
    observe.maximum_components_per_pattern_overflow_set = maximum_components_per_overflow_set;
    observe.pattern_component_over_cap_set_count = component_over_cap_set_count;
    observe.pattern_component_over_cap_prefix_covered_set_count =
        component_over_cap_prefix_covered_set_count;
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

fn choose_complete_layout(
    direct_bytes: u64,
    direct_eligible: bool,
    pattern_bytes: u64,
    pattern_eligible: bool,
) -> ProviderGraphV4Result<ProviderGraphV4Layout> {
    match (direct_eligible, pattern_eligible) {
        (true, true) => Ok(choose_layout(direct_bytes, pattern_bytes, true)),
        (true, false) => Ok(ProviderGraphV4Layout::Direct),
        (false, true) => Ok(ProviderGraphV4Layout::Pattern),
        (false, false) => Err(invalid(
            "V4 graph has no bounded complete online representation",
        )),
    }
}

fn select_npi_prefix_projection(
    model: &mut GraphModel,
    observe: &mut V4ObserveCounters,
    selected_layout: ProviderGraphV4Layout,
) -> ProviderGraphV4Result<()> {
    match selected_layout {
        ProviderGraphV4Layout::Direct => {
            if !model.npi_prefix_complete_eligible {
                return Err(invalid(
                    "V4 direct layout complete NPI prefix exceeds its configured bound",
                ));
            }
            model.provider_set_npi_prefix_override_metadata = model
                .set_npi_prefix_overrides
                .iter()
                .enumerate()
                .map(|(set_index, members)| {
                    let owner_key = model
                        .set_base
                        .checked_add(set_index as u32)
                        .ok_or(invalid("V4 complete NPI prefix owner key overflows"))?;
                    let member_count = invalid_conversion(
                        u32::try_from(members.len()),
                        "V4 complete NPI prefix member count exceeds uint32",
                    )?;
                    Ok((owner_key, member_count, npi_prefix_digest(members)))
                })
                .collect::<ProviderGraphV4Result<Vec<_>>>()?;
            observe.npi_prefix_override_owner_count =
                model.provider_set_npi_prefix_override_metadata.len() as u64;
            observe.npi_prefix_override_member_count = model.npi_prefix_complete_member_count;
            observe.npi_prefix_override_raw_bytes = model
                .npi_prefix_complete_member_count
                .checked_mul(4)
                .ok_or(invalid("V4 complete NPI prefix raw bytes overflow"))?;
            observe.npi_prefix_override_encoded_bytes = model.npi_prefix_complete_encoded_bytes;
            observe.npi_prefix_worst_provider_set_uses_override =
                observe.npi_prefix_worst_provider_set_key.is_some();
            observe.npi_prefix_worst_online_provider_set_key = None;
            observe.npi_prefix_worst_online_groups_to_target = 0;
            observe.npi_prefix_worst_online_groups_to_target_exact = false;
            observe.npi_prefix_worst_online_uses_component_fallback = false;
            observe.npi_prefix_worst_online_group_work_bound = 0;
            observe.npi_prefix_worst_online_member_count = 0;
            observe.npi_prefix_worst_online_member_digest = None;
            observe.npi_prefix_worst_online_source_owner_work = 0;
            observe.npi_prefix_worst_online_source_member_work = 0;
            observe.npi_prefix_worst_online_source_page_work = 0;
            observe.npi_prefix_worst_online_source_byte_work = 0;
            observe.npi_prefix_worst_online_group_npi_member_work = 0;
            observe.npi_prefix_worst_online_group_npi_locator_page_work = 0;
            observe.npi_prefix_worst_online_group_npi_member_page_work = 0;
            observe.npi_prefix_worst_online_group_npi_byte_work = 0;
            observe.npi_prefix_worst_online_group_npi_batch_work = 0;
        }
        ProviderGraphV4Layout::Pattern => {
            let exact_prefix_owners = model
                .provider_set_npi_prefix_override_metadata
                .iter()
                .map(|(owner_key, _, _)| *owner_key)
                .collect::<HashSet<_>>();
            for (set_index, members) in model.set_npi_prefix_overrides.iter_mut().enumerate() {
                let owner_key = model
                    .set_base
                    .checked_add(set_index as u32)
                    .ok_or(invalid("V4 sparse NPI prefix owner key overflows"))?;
                if !exact_prefix_owners.contains(&owner_key) {
                    members.clear();
                }
            }
        }
    }
    Ok(())
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

#[derive(Clone, Copy)]
struct OutputFileIdentity {
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}

impl OutputFileIdentity {
    fn from_file(file: &File) -> io::Result<Self> {
        Self::from_metadata(&file.metadata()?)
    }

    fn from_metadata(metadata: &fs::Metadata) -> io::Result<Self> {
        if !metadata.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "output identity requires a regular file",
            ));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;

            Ok(Self {
                device: metadata.dev(),
                inode: metadata.ino(),
            })
        }
        #[cfg(not(unix))]
        {
            let _ = metadata;
            Ok(Self {})
        }
    }

    fn matches_metadata(self, metadata: &fs::Metadata) -> bool {
        if !metadata.is_file() {
            return false;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;

            metadata.dev() == self.device && metadata.ino() == self.inode
        }
        #[cfg(not(unix))]
        {
            false
        }
    }

    fn matches_path(self, path: &Path) -> bool {
        let Ok(metadata) = fs::symlink_metadata(path) else {
            return false;
        };
        !metadata.file_type().is_symlink() && self.matches_metadata(&metadata)
    }
}

fn remove_owned_output(path: &Path, identity: OutputFileIdentity) {
    if identity.matches_path(path) {
        let _ = fs::remove_file(path);
    }
}

struct OwnedOutput {
    path: PathBuf,
    identity: OutputFileIdentity,
    #[cfg(unix)]
    _guard: File,
}

struct OutputOwnership {
    root: PathBuf,
    outputs: Vec<OwnedOutput>,
    committed: bool,
}

impl OutputOwnership {
    fn new(root: &Path) -> Self {
        Self {
            root: root.to_path_buf(),
            outputs: Vec::new(),
            committed: false,
        }
    }

    fn create(&mut self, path: &Path) -> ProviderGraphV4Result<File> {
        let name = path
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or(invalid("V4 provider graph output name is invalid"))?;
        if path.parent() != Some(self.root.as_path()) || !OUTPUT_NAMES.contains(&name) {
            return Err(invalid(
                "V4 provider graph output escaped its canonical output root",
            ));
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        let identity = OutputFileIdentity::from_file(&file)?;
        #[cfg(unix)]
        let guard = match file.try_clone() {
            Ok(guard) => guard,
            Err(error) => {
                remove_owned_output(path, identity);
                return Err(error.into());
            }
        };
        self.outputs.push(OwnedOutput {
            path: path.to_path_buf(),
            identity,
            #[cfg(unix)]
            _guard: guard,
        });
        Ok(file)
    }

    fn commit(&mut self) {
        self.committed = true;
    }
}

impl Drop for OutputOwnership {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        for output in self.outputs.iter().rev() {
            remove_owned_output(&output.path, output.identity);
        }
    }
}

struct CasBlockWriter {
    copy: BufWriter<File>,
    references: BufWriter<File>,
    reference_spools: BTreeMap<String, ReferenceSpool>,
    reference_spool_directory: PathBuf,
    reference_encode_buffer: Vec<u8>,
    block_count: u64,
    copy_path: PathBuf,
    reference_path: PathBuf,
    copy_identity: OutputFileIdentity,
    reference_identity: OutputFileIdentity,
    finished: bool,
}

impl CasBlockWriter {
    #[cfg(test)]
    fn create(copy_path: &Path, reference_path: &Path) -> ProviderGraphV4Result<Self> {
        let copy_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(copy_path)?;
        let copy_identity = OutputFileIdentity::from_file(&copy_file)?;
        let reference_file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(reference_path)
        {
            Ok(file) => file,
            Err(error) => {
                drop(copy_file);
                remove_owned_output(copy_path, copy_identity);
                return Err(error.into());
            }
        };
        let reference_identity = OutputFileIdentity::from_file(&reference_file)?;
        match Self::from_files(
            copy_path,
            reference_path,
            copy_file,
            reference_file,
            copy_identity,
            reference_identity,
        ) {
            Ok(writer) => Ok(writer),
            Err(error) => {
                remove_owned_output(copy_path, copy_identity);
                remove_owned_output(reference_path, reference_identity);
                Err(error)
            }
        }
    }

    fn create_tracked(
        copy_path: &Path,
        reference_path: &Path,
        ownership: &mut OutputOwnership,
    ) -> ProviderGraphV4Result<Self> {
        let copy_file = ownership.create(copy_path)?;
        let reference_file = ownership.create(reference_path)?;
        let copy_identity = OutputFileIdentity::from_file(&copy_file)?;
        let reference_identity = OutputFileIdentity::from_file(&reference_file)?;
        Self::from_files(
            copy_path,
            reference_path,
            copy_file,
            reference_file,
            copy_identity,
            reference_identity,
        )
    }

    fn from_files(
        copy_path: &Path,
        reference_path: &Path,
        copy_file: File,
        reference_file: File,
        copy_identity: OutputFileIdentity,
        reference_identity: OutputFileIdentity,
    ) -> ProviderGraphV4Result<Self> {
        let mut copy = BufWriter::new(copy_file);
        copy.write_all(PG_COPY_HEADER)?;
        let reference_spool_directory = reference_path
            .parent()
            .ok_or(invalid("V4 reference manifest has no parent directory"))?
            .to_path_buf();
        Ok(Self {
            copy,
            references: BufWriter::new(reference_file),
            reference_spools: BTreeMap::new(),
            reference_spool_directory,
            reference_encode_buffer: Vec::with_capacity(REFERENCE_ENCODE_BUFFER_BYTES as usize),
            block_count: 0,
            copy_path: copy_path.to_path_buf(),
            reference_path: reference_path.to_path_buf(),
            copy_identity,
            reference_identity,
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
        let byte_count = fs::metadata(&self.copy_path)?.len();
        self.finished = true;
        Ok((self.block_count, byte_count))
    }
}

impl Drop for CasBlockWriter {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.copy.flush();
            let _ = self.references.flush();
            remove_owned_output(&self.copy_path, self.copy_identity);
            remove_owned_output(&self.reference_path, self.reference_identity);
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
    path: PathBuf,
    identity: OutputFileIdentity,
    finished: bool,
}

impl PgCopyFileWriter {
    fn create(path: &Path) -> ProviderGraphV4Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        let identity = OutputFileIdentity::from_file(&file)?;
        Self::from_file(path, file, identity)
    }

    fn create_tracked(path: &Path, ownership: &mut OutputOwnership) -> ProviderGraphV4Result<Self> {
        let file = ownership.create(path)?;
        let identity = OutputFileIdentity::from_file(&file)?;
        Self::from_file(path, file, identity)
    }

    fn from_file(
        path: &Path,
        file: File,
        identity: OutputFileIdentity,
    ) -> ProviderGraphV4Result<Self> {
        let mut writer = BufWriter::new(file);
        if let Err(error) = writer.write_all(PG_COPY_HEADER) {
            drop(writer);
            remove_owned_output(path, identity);
            return Err(error.into());
        }
        Ok(Self {
            writer,
            path: path.to_path_buf(),
            identity,
            finished: false,
        })
    }

    fn row(&mut self, fields: &[&[u8]]) -> ProviderGraphV4Result<()> {
        let nullable = fields.iter().copied().map(Some).collect::<Vec<_>>();
        self.row_nullable(&nullable)
    }

    fn row_nullable(&mut self, fields: &[Option<&[u8]>]) -> ProviderGraphV4Result<()> {
        self.writer.write_all(
            &invalid_conversion(
                i16::try_from(fields.len()),
                "V4 dictionary row has too many fields",
            )?
            .to_be_bytes(),
        )?;
        for field in fields {
            let Some(field) = field else {
                self.writer.write_all(&(-1i32).to_be_bytes())?;
                continue;
            };
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

    fn npi_scope_row(&mut self, key: i32, npi: i64) -> ProviderGraphV4Result<()> {
        let mut row = [0u8; 22];
        row[..2].copy_from_slice(&2i16.to_be_bytes());
        row[2..6].copy_from_slice(&4i32.to_be_bytes());
        row[6..10].copy_from_slice(&key.to_be_bytes());
        row[10..14].copy_from_slice(&8i32.to_be_bytes());
        row[14..].copy_from_slice(&npi.to_be_bytes());
        self.writer.write_all(&row)?;
        Ok(())
    }

    fn finish(mut self) -> ProviderGraphV4Result<()> {
        self.writer.write_all(&(-1i16).to_be_bytes())?;
        self.writer.flush()?;
        self.finished = true;
        Ok(())
    }

    fn finish_and_digest_with_hook(
        mut self,
        after_flush: impl FnOnce(&Path),
    ) -> ProviderGraphV4Result<(u64, [u8; 32])> {
        self.writer.write_all(&(-1i16).to_be_bytes())?;
        self.writer.flush()?;
        after_flush(&self.path);
        let file = self.writer.get_mut();
        let before = file.metadata()?;
        if !self.identity.matches_metadata(&before) {
            return Err(invalid("V4 output descriptor identity changed"));
        }
        file.seek(SeekFrom::Start(0))?;
        let mut digest = Sha256::new();
        let mut buffer = [0u8; 64 * 1024];
        loop {
            let count = file.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            digest.update(&buffer[..count]);
        }
        let after = file.metadata()?;
        if before.len() != after.len()
            || !self.identity.matches_metadata(&after)
            || !self.identity.matches_path(&self.path)
        {
            return Err(invalid("V4 output path changed while sealing"));
        }
        self.finished = true;
        Ok((after.len(), digest.finalize().into()))
    }
}

impl Drop for PgCopyFileWriter {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.writer.flush();
            remove_owned_output(&self.path, self.identity);
        }
    }
}

fn validate_scope_shard<'a>(
    descriptor: &'a V4ProviderGraphShardDescriptor,
    seen_shards: &mut HashSet<String>,
) -> ProviderGraphV4Result<(
    &'a V4NpiScopeArtifactDescriptor,
    &'a V4MembershipArtifactDescriptor,
)> {
    let shard_id = descriptor.shard_id.trim();
    if shard_id.is_empty()
        || descriptor.shard_id != shard_id
        || !seen_shards.insert(shard_id.to_owned())
    {
        return Err(invalid(
            "V4 provider graph shard IDs must be non-empty and unique",
        ));
    }
    for (label, source, alias) in [
        (
            "membership",
            descriptor
                .provider_npi_group
                .metadata
                .source_shard_id
                .as_deref()
                .map(str::trim),
            descriptor
                .provider_npi_group
                .metadata
                .shard_id
                .as_deref()
                .map(str::trim),
        ),
        (
            "NPI scope",
            descriptor
                .provider_npi_scope
                .metadata
                .source_shard_id
                .as_deref()
                .map(str::trim),
            descriptor
                .provider_npi_scope
                .metadata
                .shard_id
                .as_deref()
                .map(str::trim),
        ),
    ] {
        if source.is_some() && alias.is_some() && source != alias {
            return Err(invalid(format!("V4 {label} has contradictory shard IDs")));
        }
        if source.or(alias).is_some_and(|value| value != shard_id) {
            return Err(invalid(format!(
                "V4 {label} shard ID does not match bundle {shard_id}"
            )));
        }
    }
    Ok((
        &descriptor.provider_npi_scope,
        &descriptor.provider_npi_group,
    ))
}

fn npi_scope_binding_digest(metadata: &V4NpiScopeMetadata) -> ProviderGraphV4Result<[u8; 32]> {
    let mut digest = Sha256::new();
    digest.update(NPI_SCOPE_BINDING_HASH_DOMAIN);
    update_length_prefixed(&mut digest, metadata.record_format.as_bytes())?;
    digest.update(parse_sha256(&metadata.sha256)?);
    digest.update(metadata.byte_count.to_be_bytes());
    digest.update(metadata.row_count.to_be_bytes());
    digest.update(parse_sha256(&metadata.provider_npi_group_sha256)?);
    update_length_prefixed(
        &mut digest,
        metadata.provider_npi_group_record_format.as_bytes(),
    )?;
    digest.update(metadata.provider_npi_group_byte_count.to_be_bytes());
    digest.update(metadata.provider_npi_group_owner_count.to_be_bytes());
    digest.update(metadata.provider_npi_group_member_count.to_be_bytes());
    digest.update(
        metadata
            .provider_npi_group_member_global_count
            .to_be_bytes(),
    );
    Ok(digest.finalize().into())
}

fn npi_scope_shard_binding_digest(
    metadata: &V4NpiScopeMetadata,
    shard_id: &str,
) -> ProviderGraphV4Result<[u8; 32]> {
    let mut digest = Sha256::new();
    digest.update(NPI_SCOPE_SHARD_BINDING_HASH_DOMAIN);
    digest.update(parse_sha256(&metadata.binding_sha256)?);
    update_length_prefixed(&mut digest, shard_id.as_bytes())?;
    Ok(digest.finalize().into())
}

fn reciprocal_npi_scope_member_global_count(
    reciprocal: &V4MembershipArtifactDescriptor,
) -> ProviderGraphV4Result<u64> {
    if reciprocal.metadata.record_format != DENSE_FORMAT {
        return Err(invalid(
            "V4 provider NPI scope reciprocal graph must use the dense format",
        ));
    }
    reciprocal.metadata.member_global_count.ok_or_else(|| {
        invalid("V4 provider NPI scope reciprocal graph must declare its global member count")
    })
}

struct ValidatedNpiScopeArtifact {
    reader: BufReader<File>,
    remaining_rows: u64,
    previous_npi: u64,
    finished: bool,
}

impl ValidatedNpiScopeArtifact {
    fn open(
        descriptor: &V4NpiScopeArtifactDescriptor,
        reciprocal: &V4MembershipArtifactDescriptor,
        shard_id: &str,
    ) -> ProviderGraphV4Result<Self> {
        let metadata = &descriptor.metadata;
        let reciprocal_member_global_count = reciprocal_npi_scope_member_global_count(reciprocal)?;
        if metadata.record_format != NPI_SCOPE_ARTIFACT_FORMAT
            || metadata.binding_contract != NPI_SCOPE_BINDING_CONTRACT
            || metadata.shard_binding_contract != NPI_SCOPE_SHARD_BINDING_CONTRACT
            || metadata.retention_contract != NPI_SCOPE_RETENTION_CONTRACT
            || metadata.provider_npi_group_sha256 != reciprocal.metadata.sha256
            || metadata.provider_npi_group_record_format != reciprocal.metadata.record_format
            || metadata.provider_npi_group_byte_count != reciprocal.metadata.byte_count
            || metadata.provider_npi_group_owner_count != reciprocal.metadata.owner_count
            || metadata.provider_npi_group_member_count != reciprocal.metadata.member_count
            || metadata.provider_npi_group_member_global_count != reciprocal_member_global_count
            || metadata.row_count != reciprocal.metadata.owner_count
            || parse_sha256(&metadata.binding_sha256)? != npi_scope_binding_digest(metadata)?
            || parse_sha256(&metadata.shard_binding_sha256)?
                != npi_scope_shard_binding_digest(metadata, shard_id)?
        {
            return Err(invalid(
                "V4 provider NPI scope binding does not match its reciprocal graph",
            ));
        }
        let expected_bytes = (PG_COPY_HEADER.len() as u64)
            .checked_add(
                metadata
                    .row_count
                    .checked_mul(14)
                    .ok_or(invalid("V4 provider NPI scope byte count overflows"))?,
            )
            .and_then(|value| value.checked_add(2))
            .ok_or(invalid("V4 provider NPI scope byte count overflows"))?;
        let path_metadata = fs::symlink_metadata(&descriptor.path)?;
        if path_metadata.file_type().is_symlink()
            || !path_metadata.is_file()
            || path_metadata.len() != metadata.byte_count
            || metadata.byte_count != expected_bytes
        {
            return Err(invalid("V4 provider NPI scope artifact changed"));
        }
        let mut file = File::open(&descriptor.path)?;
        let mut hasher = Sha256::new();
        let mut buffer = [0u8; 64 * 1024];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
        if parse_sha256(&metadata.sha256)? != hasher.finalize().as_slice() {
            return Err(invalid("V4 provider NPI scope checksum changed"));
        }
        file.seek(SeekFrom::Start(0))?;
        let mut reader = BufReader::new(file);
        let mut header = vec![0u8; PG_COPY_HEADER.len()];
        reader
            .read_exact(&mut header)
            .map_err(|_| invalid("V4 provider NPI scope header is truncated"))?;
        if header != PG_COPY_HEADER {
            return Err(invalid("V4 provider NPI scope header is invalid"));
        }
        Ok(Self {
            reader,
            remaining_rows: metadata.row_count,
            previous_npi: 0,
            finished: false,
        })
    }

    fn next_npi(&mut self) -> ProviderGraphV4Result<Option<u64>> {
        if self.remaining_rows == 0 {
            if !self.finished {
                let mut trailer = [0u8; 2];
                self.reader
                    .read_exact(&mut trailer)
                    .map_err(|_| invalid("V4 provider NPI scope trailer is truncated"))?;
                let mut extra = [0u8; 1];
                if trailer != (-1i16).to_be_bytes() || self.reader.read(&mut extra)? != 0 {
                    return Err(invalid("V4 provider NPI scope trailer is invalid"));
                }
                self.finished = true;
            }
            return Ok(None);
        }
        let mut row = [0u8; 14];
        self.reader
            .read_exact(&mut row)
            .map_err(|_| invalid("V4 provider NPI scope row is truncated"))?;
        if row[..2] != 1i16.to_be_bytes() {
            return Err(invalid(
                "V4 provider NPI scope row has an invalid field count",
            ));
        }
        if row[2..6] != 8i32.to_be_bytes() {
            return Err(invalid(
                "V4 provider NPI scope source NPI has an invalid width",
            ));
        }
        let npi = u64::from_be_bytes(row[6..].try_into().expect("source NPI width"));
        if !(MIN_NPI..=MAX_NPI).contains(&npi) || npi <= self.previous_npi {
            return Err(invalid(
                "V4 provider NPI scope rows must contain strict sorted ten-digit NPIs",
            ));
        }
        self.previous_npi = npi;
        self.remaining_rows -= 1;
        Ok(Some(npi))
    }
}

fn extract_provider_graph_v4_npi_scope_inner(
    shards: &[V4ProviderGraphShardDescriptor],
    output_path: &Path,
) -> ProviderGraphV4Result<ProviderGraphV4NpiScopeSummary> {
    extract_provider_graph_v4_npi_scope_inner_with_hook(shards, output_path, |_| {})
}

fn npi_scope_auth_worker_count(available_parallelism: usize, shard_count: usize) -> usize {
    available_parallelism
        .min(MAX_NPI_SCOPE_AUTH_WORKERS)
        .min(shard_count)
        .max(1)
}

fn extract_provider_graph_v4_npi_scope_inner_with_hook(
    shards: &[V4ProviderGraphShardDescriptor],
    output_path: &Path,
    after_output_flush: impl FnOnce(&Path),
) -> ProviderGraphV4Result<ProviderGraphV4NpiScopeSummary> {
    if shards.is_empty() {
        return Err(invalid("V4 provider graph requires at least one shard"));
    }
    let mut ordered = shards.iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
    let mut seen_shards = HashSet::new();
    let mut scope_inputs = Vec::with_capacity(ordered.len());
    let mut input_digest = Sha256::new();
    input_digest.update(NPI_SCOPE_INPUT_HASH_DOMAIN);
    let mut input_byte_count = 0u64;
    let mut source_owner_count = 0u64;
    for descriptor in ordered {
        let (scope_descriptor, reciprocal_descriptor) =
            validate_scope_shard(descriptor, &mut seen_shards)?;
        scope_inputs.push((descriptor, scope_descriptor, reciprocal_descriptor));
    }
    let worker_count = npi_scope_auth_worker_count(
        std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1),
        scope_inputs.len(),
    );
    let worker_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(worker_count)
        .thread_name(|index| format!("ptg2-v4-npi-auth-{index}"))
        .build()
        .map_err(io::Error::other)?;
    let opened = worker_pool.install(|| {
        scope_inputs
            .par_iter()
            .map(|(descriptor, scope_descriptor, reciprocal_descriptor)| {
                ValidatedNpiScopeArtifact::open(
                    scope_descriptor,
                    reciprocal_descriptor,
                    &descriptor.shard_id,
                )
            })
            .collect::<Vec<_>>()
    });
    let mut artifacts = opened
        .into_iter()
        .collect::<ProviderGraphV4Result<Vec<_>>>()?;
    for (descriptor, scope_descriptor, _reciprocal_descriptor) in scope_inputs {
        input_byte_count = input_byte_count
            .checked_add(scope_descriptor.metadata.byte_count)
            .ok_or(invalid("V4 NPI scope input byte count overflows"))?;
        source_owner_count = source_owner_count
            .checked_add(scope_descriptor.metadata.row_count)
            .ok_or(invalid("V4 NPI scope source owner count overflows"))?;
        update_length_prefixed(&mut input_digest, descriptor.shard_id.as_bytes())?;
        input_digest.update(parse_sha256(&scope_descriptor.metadata.sha256)?);
        input_digest.update(scope_descriptor.metadata.byte_count.to_be_bytes());
        input_digest.update(scope_descriptor.metadata.row_count.to_be_bytes());
        input_digest.update(parse_sha256(&scope_descriptor.metadata.binding_sha256)?);
    }

    let mut heap = BinaryHeap::new();
    for (artifact_index, artifact) in artifacts.iter_mut().enumerate() {
        if let Some(npi) = artifact.next_npi()? {
            heap.push(Reverse((npi, artifact_index)));
        }
    }
    let mut output = PgCopyFileWriter::create(output_path)?;
    let mut previous_npi = None;
    let mut row_count = 0u64;
    while let Some(Reverse((npi, artifact_index))) = heap.pop() {
        if previous_npi != Some(npi) {
            let key = invalid_conversion(
                i32::try_from(row_count),
                "V4 NPI scope row count exceeds int32",
            )?;
            let npi = invalid_conversion(i64::try_from(npi), "V4 NPI exceeds int64")?;
            output.npi_scope_row(key, npi)?;
            row_count = row_count
                .checked_add(1)
                .ok_or(invalid("V4 NPI scope row count overflows"))?;
            previous_npi = Some(npi as u64);
        }
        if let Some(next_npi) = artifacts[artifact_index].next_npi()? {
            heap.push(Reverse((next_npi, artifact_index)));
        }
    }
    for artifact in &mut artifacts {
        if artifact.next_npi()?.is_some() {
            return Err(invalid("V4 provider NPI scope merge is incomplete"));
        }
    }
    let (output_byte_count, output_sha256) =
        output.finish_and_digest_with_hook(after_output_flush)?;
    Ok(ProviderGraphV4NpiScopeSummary {
        format: NPI_SCOPE_FORMAT.to_owned(),
        row_count,
        source_owner_count,
        input_byte_count,
        input_sha256: hex(&input_digest.finalize()),
        output_byte_count,
        output_sha256: hex(&output_sha256),
        output_path: output_path.to_path_buf(),
    })
}

/// Extract the exact sorted snapshot NPI universe from authenticated reciprocal
/// NPI-to-group factor owners without loading their member edges.
pub fn extract_provider_graph_v4_npi_scope(
    shards: &[V4ProviderGraphShardDescriptor],
    output_path: impl AsRef<Path>,
) -> ProviderGraphV4Result<ProviderGraphV4NpiScopeSummary> {
    let output_path = output_path.as_ref();
    if fs::symlink_metadata(output_path).is_ok() {
        return Err(invalid(format!(
            "V4 NPI scope output already exists: {}",
            output_path.display()
        )));
    }
    extract_provider_graph_v4_npi_scope_inner(shards, output_path)
}

fn expected_npi_scope_input_identity(
    shards: &[V4ProviderGraphShardDescriptor],
) -> ProviderGraphV4Result<(u64, u64, [u8; 32])> {
    if shards.is_empty() {
        return Err(invalid("V4 provider graph requires at least one shard"));
    }
    let mut ordered = shards.iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
    let mut seen_shards = HashSet::new();
    let mut input_byte_count = 0u64;
    let mut source_owner_count = 0u64;
    let mut input_digest = Sha256::new();
    input_digest.update(NPI_SCOPE_INPUT_HASH_DOMAIN);
    for descriptor in ordered {
        let (artifact, reciprocal) = validate_scope_shard(descriptor, &mut seen_shards)?;
        let reciprocal_member_global_count = reciprocal_npi_scope_member_global_count(reciprocal)?;
        if artifact.metadata.record_format != NPI_SCOPE_ARTIFACT_FORMAT
            || artifact.metadata.binding_contract != NPI_SCOPE_BINDING_CONTRACT
            || artifact.metadata.shard_binding_contract != NPI_SCOPE_SHARD_BINDING_CONTRACT
            || artifact.metadata.retention_contract != NPI_SCOPE_RETENTION_CONTRACT
            || artifact.metadata.provider_npi_group_sha256 != reciprocal.metadata.sha256
            || artifact.metadata.provider_npi_group_record_format
                != reciprocal.metadata.record_format
            || artifact.metadata.provider_npi_group_byte_count != reciprocal.metadata.byte_count
            || artifact.metadata.provider_npi_group_owner_count != reciprocal.metadata.owner_count
            || artifact.metadata.provider_npi_group_member_count != reciprocal.metadata.member_count
            || artifact.metadata.provider_npi_group_member_global_count
                != reciprocal_member_global_count
            || artifact.metadata.row_count != reciprocal.metadata.owner_count
            || parse_sha256(&artifact.metadata.binding_sha256)?
                != npi_scope_binding_digest(&artifact.metadata)?
            || parse_sha256(&artifact.metadata.shard_binding_sha256)?
                != npi_scope_shard_binding_digest(&artifact.metadata, &descriptor.shard_id)?
        {
            return Err(invalid(
                "V4 provider NPI scope binding does not match its reciprocal graph",
            ));
        }
        input_byte_count = input_byte_count
            .checked_add(artifact.metadata.byte_count)
            .ok_or(invalid("V4 NPI scope input byte count overflows"))?;
        source_owner_count = source_owner_count
            .checked_add(artifact.metadata.row_count)
            .ok_or(invalid("V4 NPI scope source owner count overflows"))?;
        update_length_prefixed(&mut input_digest, descriptor.shard_id.as_bytes())?;
        input_digest.update(parse_sha256(&artifact.metadata.sha256)?);
        input_digest.update(artifact.metadata.byte_count.to_be_bytes());
        input_digest.update(artifact.metadata.row_count.to_be_bytes());
        input_digest.update(parse_sha256(&artifact.metadata.binding_sha256)?);
    }
    Ok((
        input_byte_count,
        source_owner_count,
        input_digest.finalize().into(),
    ))
}

fn validate_npi_scope_input(
    shards: &[V4ProviderGraphShardDescriptor],
    input: &ProviderGraphV4NpiScopeInput,
    model_npis: &[u64],
) -> ProviderGraphV4Result<()> {
    validate_npi_scope_input_with_hook(shards, input, model_npis, |_| {})
}

fn validate_npi_scope_input_with_hook(
    shards: &[V4ProviderGraphShardDescriptor],
    input: &ProviderGraphV4NpiScopeInput,
    model_npis: &[u64],
    after_open: impl FnOnce(&Path),
) -> ProviderGraphV4Result<()> {
    let (input_byte_count, source_owner_count, input_digest) =
        expected_npi_scope_input_identity(shards)?;
    if input.format != NPI_SCOPE_FORMAT
        || input.input_byte_count != input_byte_count
        || input.source_owner_count != source_owner_count
        || parse_sha256(&input.input_sha256)? != input_digest
        || input.row_count != model_npis.len() as u64
    {
        return Err(invalid("V4 NPI scope prepass identity changed"));
    }
    let path_metadata = fs::symlink_metadata(&input.output_path)?;
    if path_metadata.file_type().is_symlink()
        || !path_metadata.is_file()
        || path_metadata.len() != input.output_byte_count
    {
        return Err(invalid("V4 NPI scope prepass artifact changed"));
    }
    let path_identity = OutputFileIdentity::from_metadata(&path_metadata)?;
    let file = File::open(&input.output_path)?;
    let descriptor_metadata = file.metadata()?;
    if descriptor_metadata.len() != input.output_byte_count
        || !path_identity.matches_metadata(&descriptor_metadata)
    {
        return Err(invalid("V4 NPI scope prepass artifact changed"));
    }
    after_open(&input.output_path);
    let mut reader = BufReader::new(file);
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    if parse_sha256(&input.output_sha256)? != digest.finalize().as_slice() {
        return Err(invalid("V4 NPI scope prepass checksum changed"));
    }
    reader.seek(SeekFrom::Start(0))?;
    let rows = read_npi_scope_copy_reader(&mut reader)?;
    let final_metadata = reader.get_ref().metadata()?;
    let current_path_metadata = fs::symlink_metadata(&input.output_path)?;
    if final_metadata.len() != input.output_byte_count
        || current_path_metadata.len() != input.output_byte_count
        || current_path_metadata.file_type().is_symlink()
        || !path_identity.matches_metadata(&final_metadata)
        || !path_identity.matches_metadata(&current_path_metadata)
    {
        return Err(invalid("V4 NPI scope prepass artifact changed"));
    }
    if rows.len() != model_npis.len()
        || rows
            .iter()
            .zip(model_npis)
            .any(|((key, npi), expected_npi)| {
                *key as usize >= model_npis.len() || *npi != *expected_npi
            })
    {
        return Err(invalid(
            "V4 NPI scope prepass differs from the complete graph model",
        ));
    }
    Ok(())
}

fn read_copy_i32(reader: &mut BufReader<File>, label: &'static str) -> ProviderGraphV4Result<i32> {
    let mut bytes = [0u8; 4];
    reader
        .read_exact(&mut bytes)
        .map_err(|_| invalid(format!("V4 NPI scope {label} is truncated")))?;
    Ok(i32::from_be_bytes(bytes))
}

fn read_copy_field<const N: usize>(
    reader: &mut BufReader<File>,
    label: &'static str,
) -> ProviderGraphV4Result<[u8; N]> {
    let length = read_copy_i32(reader, label)?;
    if length != N as i32 {
        return Err(invalid(format!(
            "V4 NPI scope {label} has an invalid width"
        )));
    }
    let mut value = [0u8; N];
    reader
        .read_exact(&mut value)
        .map_err(|_| invalid(format!("V4 NPI scope {label} is truncated")))?;
    Ok(value)
}

#[cfg(test)]
fn read_npi_scope_copy(path: &Path) -> ProviderGraphV4Result<Vec<(u32, u64)>> {
    let mut reader = BufReader::new(File::open(path)?);
    read_npi_scope_copy_reader(&mut reader)
}

fn read_npi_scope_copy_reader(
    reader: &mut BufReader<File>,
) -> ProviderGraphV4Result<Vec<(u32, u64)>> {
    let mut header = vec![0u8; PG_COPY_HEADER.len()];
    reader
        .read_exact(&mut header)
        .map_err(|_| invalid("V4 NPI scope COPY header is truncated"))?;
    if header != PG_COPY_HEADER {
        return Err(invalid("V4 NPI scope COPY header is invalid"));
    }
    let mut rows = Vec::new();
    loop {
        let mut field_count_bytes = [0u8; 2];
        reader
            .read_exact(&mut field_count_bytes)
            .map_err(|_| invalid("V4 NPI scope COPY row header is truncated"))?;
        let field_count = i16::from_be_bytes(field_count_bytes);
        if field_count == -1 {
            let mut trailing = [0u8; 1];
            if reader.read(&mut trailing)? != 0 {
                return Err(invalid("V4 NPI scope COPY has trailing bytes"));
            }
            break;
        }
        if field_count != 2 {
            return Err(invalid("V4 NPI scope COPY row must have two fields"));
        }
        let key = i32::from_be_bytes(read_copy_field::<4>(reader, "key")?);
        let npi = i64::from_be_bytes(read_copy_field::<8>(reader, "NPI")?);
        if key < 0 || !(MIN_NPI..=MAX_NPI).contains(&(npi as u64)) {
            return Err(invalid("V4 NPI scope COPY row is outside its domain"));
        }
        let key = key as u32;
        let npi = npi as u64;
        if key as usize != rows.len()
            || rows
                .last()
                .is_some_and(|(_, previous_npi)| npi <= *previous_npi)
        {
            return Err(invalid(
                "V4 NPI scope COPY rows must be dense and NPI-sorted",
            ));
        }
        rows.push((key, npi));
    }
    Ok(rows)
}

#[derive(Clone, Debug)]
struct V4InferredTaxonomyRule {
    rule_digest: [u8; 32],
    catalog_digest: [u8; 32],
    member_keys: Vec<u32>,
}

#[derive(Clone, Debug)]
struct V4InferredTaxonomyModel {
    rules: Vec<V4InferredTaxonomyRule>,
}

#[derive(Clone, Debug)]
struct V4InferredTaxonomyRow {
    rule_digest: [u8; 32],
    catalog_digest: [u8; 32],
    member_digest: [u8; 32],
    member_keys: Vec<u8>,
    representation: &'static str,
    observe_reason: Option<&'static str>,
    observe_count_lower_bound: Option<u64>,
    pattern_count: u32,
    pattern_member_count: u64,
    pattern_member_digest: [u8; 32],
    pattern_member_payload: Vec<u8>,
}

#[derive(Clone, Debug)]
struct V4InferredTaxonomyRejection {
    reason: &'static str,
    rule_digest: [u8; 32],
    observed_count: u64,
    cap: u64,
}

#[derive(Clone, Debug)]
struct V4InferredTaxonomyProjection {
    rows: Vec<V4InferredTaxonomyRow>,
    encoded_bytes: u64,
    eligible: bool,
    rejection: Option<V4InferredTaxonomyRejection>,
}

fn strict_u32le_members(bytes: &[u8], npi_count: usize) -> ProviderGraphV4Result<Vec<u32>> {
    if !bytes.len().is_multiple_of(4) {
        return Err(invalid(
            "V4 inferred-taxonomy candidate member payload is misaligned",
        ));
    }
    let mut members = Vec::with_capacity(bytes.len() / 4);
    let mut previous = None;
    for member in bytes.chunks_exact(4) {
        let key = u32::from_le_bytes(member.try_into().expect("fixed uint32 width"));
        if previous.is_some_and(|value| key <= value) || key as usize >= npi_count {
            return Err(invalid(
                "V4 inferred-taxonomy candidate members are outside the NPI scope",
            ));
        }
        members.push(key);
        previous = Some(key);
    }
    Ok(members)
}

fn validate_inferred_taxonomy_rule_envelope(
    input: &ProviderGraphV4InferredTaxonomyInput,
) -> ProviderGraphV4Result<()> {
    let mut previous_digest = None;
    let mut expected_offset = 0u64;
    let mut rule_set_hasher = Sha256::new();
    rule_set_hasher.update(INFERRED_TAXONOMY_RULE_SET_DIGEST_DOMAIN);
    rule_set_hasher.update(
        invalid_conversion(
            u32::try_from(input.rules.len()),
            "V4 inferred-taxonomy rule count exceeds uint32",
        )?
        .to_be_bytes(),
    );
    for raw_rule in &input.rules {
        let rule_digest = parse_sha256(&raw_rule.rule_digest)?;
        parse_sha256(&raw_rule.catalog_digest)?;
        let expected_member_bytes = raw_rule.member_count.checked_mul(4).ok_or(invalid(
            "V4 inferred-taxonomy candidate member bytes overflow",
        ))?;
        if previous_digest.is_some_and(|value| rule_digest <= value)
            || raw_rule.member_offset_bytes != expected_offset
            || raw_rule.member_byte_count != expected_member_bytes
        {
            return Err(invalid(
                "V4 inferred-taxonomy rules must be strict and contiguous",
            ));
        }
        expected_offset = expected_offset
            .checked_add(raw_rule.member_byte_count)
            .ok_or(invalid("V4 inferred-taxonomy member range overflows"))?;
        rule_set_hasher.update(rule_digest);
        previous_digest = Some(rule_digest);
    }
    if expected_offset != input.members.byte_count
        || parse_sha256(&input.rule_set_digest)? != rule_set_hasher.finalize().as_slice()
    {
        return Err(invalid("V4 inferred-taxonomy compiler input is incomplete"));
    }
    Ok(())
}

fn reserve_inferred_taxonomy_memory(
    input: &ProviderGraphV4InferredTaxonomyInput,
    admission: &mut ResourceAdmissionTracker,
) -> ProviderGraphV4Result<()> {
    let rule_count = invalid_conversion(
        u64::try_from(input.rules.len()),
        "resource_admission: inferred-taxonomy rule count exceeds uint64",
    )?;
    let member_count = input.members.byte_count / 4;
    let model_member_bytes = member_count
        .checked_mul(ESTIMATED_U32_CAPACITY_BYTES)
        .ok_or(invalid(
            "resource_admission: inferred-taxonomy model bytes overflow",
        ))?;
    let retained_bytes = model_member_bytes
        .checked_add(rule_count.saturating_mul(ESTIMATED_VEC_OWNER_BYTES))
        .ok_or(invalid(
            "resource_admission: inferred-taxonomy model bytes overflow",
        ))?;
    admission.reserve_projection("inferred-taxonomy model", retained_bytes)?;
    admission.reserve_scratch_bytes(
        "authenticated inferred-taxonomy member input",
        input.members.byte_count.max(64 * 1024),
    )
}

fn reserve_inferred_taxonomy_projection_memory(
    npi_patterns: &[Vec<u32>],
    input: &V4InferredTaxonomyModel,
    options: &ProviderGraphV4Options,
    admission: &mut ResourceAdmissionTracker,
) -> ProviderGraphV4Result<()> {
    let observe_threshold = options
        .max_online_inferred_taxonomy_candidates
        .checked_add(1)
        .ok_or(invalid("V4 inferred-taxonomy candidate cap overflows"))?;
    let pattern_cap = invalid_conversion(
        u64::try_from(options.max_online_candidate_pattern_projection_members),
        "resource_admission: inferred-taxonomy pattern cap exceeds uint64",
    )?;
    let rule_count = invalid_conversion(
        u64::try_from(input.rules.len()),
        "resource_admission: inferred-taxonomy rule count exceeds uint64",
    )?;
    let member_payload_bytes = input.rules.iter().try_fold(0u64, |total, rule| {
        total
            .checked_add(
                invalid_conversion(
                    u64::try_from(rule.member_keys.len()),
                    "resource_admission: inferred-taxonomy candidate count exceeds uint64",
                )?
                .checked_mul(4)
                .ok_or(invalid(
                    "resource_admission: inferred-taxonomy candidate bytes overflow",
                ))?,
            )
            .ok_or(invalid(
                "resource_admission: inferred-taxonomy candidate bytes overflow",
            ))
    })?;
    let mut retained_pattern_payload_bytes = 0u64;
    let mut maximum_pattern_scratch_members = 0u64;
    let mut pattern_rejected = false;
    for rule in &input.rules {
        if pattern_rejected
            || rule.member_keys.is_empty()
            || rule.member_keys.len() == observe_threshold
        {
            continue;
        }
        let mut associations = 0u64;
        for npi_key in &rule.member_keys {
            let patterns = npi_patterns.get(*npi_key as usize).ok_or(invalid(
                "resource_admission: inferred-taxonomy candidate exceeds pattern scope",
            ))?;
            associations = associations
                .checked_add(invalid_conversion(
                    u64::try_from(patterns.len()),
                    "resource_admission: inferred-taxonomy pattern count exceeds uint64",
                )?)
                .ok_or(invalid(
                    "resource_admission: inferred-taxonomy pattern member count overflows",
                ))?;
            if associations > pattern_cap {
                maximum_pattern_scratch_members =
                    maximum_pattern_scratch_members.max(pattern_cap.saturating_add(1));
                pattern_rejected = true;
                break;
            }
        }
        if !pattern_rejected {
            maximum_pattern_scratch_members = maximum_pattern_scratch_members.max(associations);
            retained_pattern_payload_bytes = retained_pattern_payload_bytes
                .checked_add(
                    associations
                        .checked_mul(ESTIMATED_PATTERN_PAYLOAD_BYTES_PER_MEMBER)
                        .and_then(|value| value.checked_add(48))
                        .ok_or(invalid(
                            "resource_admission: inferred-taxonomy pattern payload bytes overflow",
                        ))?,
                )
                .ok_or(invalid(
                    "resource_admission: inferred-taxonomy pattern payload bytes overflow",
                ))?;
        }
    }
    let projection_bytes = member_payload_bytes
        .checked_mul(2)
        .and_then(|value| {
            value.checked_add(
                rule_count
                    .saturating_mul(2)
                    .saturating_mul(ESTIMATED_INFERRED_TAXONOMY_ROW_BYTES),
            )
        })
        .and_then(|value| value.checked_add(retained_pattern_payload_bytes))
        .ok_or(invalid(
            "resource_admission: inferred-taxonomy simultaneous projection bytes overflow",
        ))?;
    admission.reserve_projection(
        "simultaneous direct/pattern inferred-taxonomy projections",
        projection_bytes,
    )?;
    let pattern_scratch_bytes = maximum_pattern_scratch_members
        .checked_mul(ESTIMATED_PATTERN_POSTING_SCRATCH_BYTES)
        .ok_or(invalid(
            "resource_admission: inferred-taxonomy pattern scratch bytes overflow",
        ))?;
    admission.reserve_scratch_bytes("inferred-taxonomy pattern postings", pattern_scratch_bytes)
}

fn read_inferred_taxonomy_model(
    input: &ProviderGraphV4InferredTaxonomyInput,
    scope: &ProviderGraphV4NpiScopeInput,
    npi_count: usize,
    admission: &mut ResourceAdmissionTracker,
) -> ProviderGraphV4Result<V4InferredTaxonomyModel> {
    if input.contract != INFERRED_TAXONOMY_INPUT_CONTRACT
        || input.catalog_contract != INFERRED_TAXONOMY_CATALOG_CONTRACT
        || input.vector_format != INFERRED_TAXONOMY_VECTOR_FORMAT
        || parse_sha256(&input.npi_scope_sha256)? != parse_sha256(&scope.output_sha256)?
        || input.rules.is_empty()
    {
        return Err(invalid(
            "V4 inferred-taxonomy compiler input contract is incompatible",
        ));
    }
    validate_inferred_taxonomy_rule_envelope(input)?;
    let expected_members_digest = parse_sha256(&input.members.sha256)?;
    let mut member_file = File::open(&input.members.path).map_err(|error| {
        invalid(format!(
            "V4 inferred-taxonomy member input is unavailable ({}): {error}",
            input.members.path.display()
        ))
    })?;
    if member_file.metadata()?.len() != input.members.byte_count {
        return Err(invalid(
            "V4 inferred-taxonomy member input byte count changed",
        ));
    }
    let mut authenticated_digest = Sha256::new();
    let mut authentication_buffer = [0u8; 64 * 1024];
    loop {
        let count = member_file.read(&mut authentication_buffer)?;
        if count == 0 {
            break;
        }
        authenticated_digest.update(&authentication_buffer[..count]);
    }
    if authenticated_digest.finalize().as_slice() != expected_members_digest {
        return Err(invalid(
            "V4 inferred-taxonomy member input checksum changed",
        ));
    }
    reserve_inferred_taxonomy_memory(input, admission)?;
    member_file.seek(SeekFrom::Start(0))?;
    let member_len = invalid_conversion(
        usize::try_from(input.members.byte_count),
        "V4 inferred-taxonomy member input exceeds addressable memory",
    )?;
    let mut member_bytes = vec![0u8; member_len];
    member_file.read_exact(&mut member_bytes)?;
    if Sha256::digest(&member_bytes).as_slice() != expected_members_digest {
        return Err(invalid(
            "V4 inferred-taxonomy member input changed after authentication",
        ));
    }
    let mut trailing = [0u8; 1];
    if member_file.read(&mut trailing)? != 0 {
        return Err(invalid(
            "V4 inferred-taxonomy member input grew after authentication",
        ));
    }
    let mut rules = Vec::with_capacity(input.rules.len());
    let mut previous_digest = None;
    let mut expected_offset = 0u64;
    let mut rule_set_hasher = Sha256::new();
    rule_set_hasher.update(INFERRED_TAXONOMY_RULE_SET_DIGEST_DOMAIN);
    rule_set_hasher.update(
        invalid_conversion(
            u32::try_from(input.rules.len()),
            "V4 inferred-taxonomy rule count exceeds uint32",
        )?
        .to_be_bytes(),
    );
    for raw_rule in &input.rules {
        let rule_digest = parse_sha256(&raw_rule.rule_digest)?;
        let catalog_digest = parse_sha256(&raw_rule.catalog_digest)?;
        let expected_member_bytes = raw_rule.member_count.checked_mul(4).ok_or(invalid(
            "V4 inferred-taxonomy candidate member bytes overflow",
        ))?;
        if previous_digest.is_some_and(|value| rule_digest <= value)
            || raw_rule.member_offset_bytes != expected_offset
            || raw_rule.member_byte_count != expected_member_bytes
        {
            return Err(invalid(
                "V4 inferred-taxonomy rules must be strict and contiguous",
            ));
        }
        let start = invalid_conversion(
            usize::try_from(raw_rule.member_offset_bytes),
            "V4 inferred-taxonomy member offset exceeds addressable memory",
        )?;
        let end = invalid_conversion(
            usize::try_from(
                raw_rule
                    .member_offset_bytes
                    .checked_add(raw_rule.member_byte_count)
                    .ok_or(invalid("V4 inferred-taxonomy member range overflows"))?,
            ),
            "V4 inferred-taxonomy member range exceeds addressable memory",
        )?;
        let members = strict_u32le_members(
            member_bytes.get(start..end).ok_or(invalid(
                "V4 inferred-taxonomy member range is outside its artifact",
            ))?,
            npi_count,
        )?;
        if members.len() as u64 != raw_rule.member_count {
            return Err(invalid(
                "V4 inferred-taxonomy candidate member count changed",
            ));
        }
        rule_set_hasher.update(rule_digest);
        rules.push(V4InferredTaxonomyRule {
            rule_digest,
            catalog_digest,
            member_keys: members,
        });
        expected_offset = end as u64;
        previous_digest = Some(rule_digest);
    }
    if expected_offset != input.members.byte_count
        || parse_sha256(&input.rule_set_digest)? != rule_set_hasher.finalize().as_slice()
    {
        return Err(invalid("V4 inferred-taxonomy compiler input is incomplete"));
    }
    Ok(V4InferredTaxonomyModel { rules })
}

fn inferred_taxonomy_member_digest(
    rule_digest: &[u8; 32],
    member_payload: &[u8],
) -> ProviderGraphV4Result<[u8; 32]> {
    if !member_payload.len().is_multiple_of(4) {
        return Err(invalid(
            "V4 inferred-taxonomy candidate member payload is misaligned",
        ));
    }
    let mut digest = Sha256::new();
    digest.update(INFERRED_TAXONOMY_MEMBER_DIGEST_DOMAIN);
    digest.update(rule_digest);
    digest.update((member_payload.len() as u64 / 4).to_be_bytes());
    digest.update(member_payload);
    Ok(digest.finalize().into())
}

fn inferred_taxonomy_pattern_digest(
    rule_digest: &[u8; 32],
    representation: &str,
    pattern_count: u32,
    pattern_member_count: u64,
    pattern_payload: &[u8],
) -> ProviderGraphV4Result<[u8; 32]> {
    let representation_bytes = representation.as_bytes();
    let mut digest = Sha256::new();
    digest.update(INFERRED_TAXONOMY_PATTERN_MEMBER_DIGEST_DOMAIN);
    digest.update(rule_digest);
    digest.update(
        invalid_conversion(
            u16::try_from(representation_bytes.len()),
            "V4 inferred-taxonomy representation exceeds uint16",
        )?
        .to_be_bytes(),
    );
    digest.update(representation_bytes);
    digest.update(u64::from(pattern_count).to_be_bytes());
    digest.update(pattern_member_count.to_be_bytes());
    digest.update((pattern_payload.len() as u64).to_be_bytes());
    digest.update(pattern_payload);
    Ok(digest.finalize().into())
}

fn pack_pattern_postings(
    postings: &BTreeMap<u32, Vec<u32>>,
    member_count: u64,
) -> ProviderGraphV4Result<Vec<u8>> {
    if postings.is_empty() {
        return Ok(Vec::new());
    }
    let mut payload = Vec::new();
    payload.extend_from_slice(INFERRED_TAXONOMY_PATTERN_PAYLOAD_MAGIC);
    payload.extend_from_slice(&INFERRED_TAXONOMY_PATTERN_PAYLOAD_VERSION.to_le_bytes());
    payload.extend_from_slice(
        &invalid_conversion(
            u32::try_from(postings.len()),
            "V4 inferred-taxonomy pattern count exceeds uint32",
        )?
        .to_le_bytes(),
    );
    payload.extend_from_slice(&member_count.to_le_bytes());
    for (pattern_key, npi_keys) in postings {
        if npi_keys.is_empty() {
            return Err(invalid("V4 inferred-taxonomy pattern posting is empty"));
        }
        payload.extend_from_slice(&pattern_key.to_le_bytes());
        payload.extend_from_slice(
            &invalid_conversion(
                u32::try_from(npi_keys.len()),
                "V4 inferred-taxonomy pattern posting exceeds uint32",
            )?
            .to_le_bytes(),
        );
        for npi_key in npi_keys {
            payload.extend_from_slice(&npi_key.to_le_bytes());
        }
    }
    Ok(payload)
}

fn candidate_row_encoded_bytes(row: &V4InferredTaxonomyRow) -> ProviderGraphV4Result<u64> {
    copy_row_encoded_bytes(&[
        32,
        INFERRED_TAXONOMY_CATALOG_CONTRACT.len(),
        32,
        INFERRED_TAXONOMY_VECTOR_FORMAT.len(),
        4,
        32,
        row.member_keys.len(),
        row.representation.len(),
        row.observe_reason.map_or(0, str::len),
        row.observe_count_lower_bound.map_or(0, |_| 8),
        4,
        8,
        8,
        32,
        row.pattern_member_payload.len(),
    ])
}

fn inferred_taxonomy_projection(
    npi_patterns: &[Vec<u32>],
    input: &V4InferredTaxonomyModel,
    layout: ProviderGraphV4Layout,
    options: &ProviderGraphV4Options,
) -> ProviderGraphV4Result<V4InferredTaxonomyProjection> {
    let mut rows = Vec::with_capacity(input.rules.len());
    let mut encoded_bytes = (PG_COPY_HEADER.len() + 2) as u64;
    for rule in &input.rules {
        let observe_threshold = options
            .max_online_inferred_taxonomy_candidates
            .checked_add(1)
            .ok_or(invalid("V4 inferred-taxonomy candidate cap overflows"))?;
        if rule.member_keys.len() > observe_threshold {
            return Err(invalid(
                "V4 inferred-taxonomy candidate witness exceeds its bounded cap",
            ));
        }
        let mut member_payload = Vec::with_capacity(rule.member_keys.len() * 4);
        for npi_key in &rule.member_keys {
            member_payload.extend_from_slice(&npi_key.to_le_bytes());
        }
        let mut representation = INFERRED_TAXONOMY_DIRECT_REPRESENTATION;
        let mut observe_reason = None;
        let mut observe_count_lower_bound = None;
        let mut pattern_count = 0u32;
        let mut pattern_member_count = 0u64;
        let mut pattern_payload = Vec::new();
        if rule.member_keys.len() == observe_threshold {
            representation = INFERRED_TAXONOMY_OBSERVE_REPRESENTATION;
            observe_reason = Some(INFERRED_TAXONOMY_CANDIDATE_CAP_REASON);
            observe_count_lower_bound = Some(observe_threshold as u64);
        } else if layout == ProviderGraphV4Layout::Pattern && !rule.member_keys.is_empty() {
            let mut postings = BTreeMap::<u32, Vec<u32>>::new();
            for npi_key in &rule.member_keys {
                let patterns = npi_patterns.get(*npi_key as usize).ok_or(invalid(
                    "V4 inferred-taxonomy candidate exceeds pattern scope",
                ))?;
                if patterns.is_empty() {
                    return Err(invalid(
                        "V4 inferred-taxonomy candidate has no pattern evidence",
                    ));
                }
                for pattern_key in patterns {
                    pattern_member_count = pattern_member_count.checked_add(1).ok_or(invalid(
                        "V4 inferred-taxonomy pattern member count overflows",
                    ))?;
                    if pattern_member_count
                        > options.max_online_candidate_pattern_projection_members as u64
                    {
                        return Ok(V4InferredTaxonomyProjection {
                            rows: Vec::new(),
                            encoded_bytes: 0,
                            eligible: false,
                            rejection: Some(V4InferredTaxonomyRejection {
                                reason: INFERRED_TAXONOMY_PATTERN_CAP_REASON,
                                rule_digest: rule.rule_digest,
                                observed_count: pattern_member_count,
                                cap: options.max_online_candidate_pattern_projection_members as u64,
                            }),
                        });
                    }
                    postings.entry(*pattern_key).or_default().push(*npi_key);
                }
            }
            pattern_count = invalid_conversion(
                u32::try_from(postings.len()),
                "V4 inferred-taxonomy pattern count exceeds uint32",
            )?;
            pattern_payload = pack_pattern_postings(&postings, pattern_member_count)?;
            representation = INFERRED_TAXONOMY_PATTERN_REPRESENTATION;
        }
        let row = V4InferredTaxonomyRow {
            rule_digest: rule.rule_digest,
            catalog_digest: rule.catalog_digest,
            member_digest: inferred_taxonomy_member_digest(&rule.rule_digest, &member_payload)?,
            member_keys: member_payload,
            representation,
            observe_reason,
            observe_count_lower_bound,
            pattern_count,
            pattern_member_count,
            pattern_member_digest: inferred_taxonomy_pattern_digest(
                &rule.rule_digest,
                representation,
                pattern_count,
                pattern_member_count,
                &pattern_payload,
            )?,
            pattern_member_payload: pattern_payload,
        };
        encoded_bytes = encoded_bytes
            .checked_add(candidate_row_encoded_bytes(&row)?)
            .ok_or(invalid(
                "V4 inferred-taxonomy candidate encoded bytes overflow",
            ))?;
        rows.push(row);
    }
    Ok(V4InferredTaxonomyProjection {
        rows,
        encoded_bytes,
        eligible: true,
        rejection: None,
    })
}

fn emit_inferred_taxonomy_candidates(
    path: &Path,
    projection: &V4InferredTaxonomyProjection,
    ownership: &mut OutputOwnership,
) -> ProviderGraphV4Result<()> {
    if !projection.eligible {
        return Err(invalid(projection.rejection.as_ref().map_or(
            "V4 inferred-taxonomy projection is ineligible",
            |value| value.reason,
        )));
    }
    let mut output = PgCopyFileWriter::create_tracked(path, ownership)?;
    for row in &projection.rows {
        let member_count = invalid_conversion(
            i32::try_from(row.member_keys.len() / 4),
            "V4 inferred-taxonomy candidate count exceeds int32",
        )?;
        let pattern_count = invalid_conversion(
            i32::try_from(row.pattern_count),
            "V4 inferred-taxonomy pattern count exceeds int32",
        )?;
        let pattern_member_count = invalid_conversion(
            i64::try_from(row.pattern_member_count),
            "V4 inferred-taxonomy pattern member count exceeds int64",
        )?;
        let pattern_member_bytes = invalid_conversion(
            i64::try_from(row.pattern_member_payload.len()),
            "V4 inferred-taxonomy pattern payload exceeds int64",
        )?;
        let observe_count_lower_bound = row
            .observe_count_lower_bound
            .map(|value| {
                invalid_conversion(
                    i64::try_from(value),
                    "V4 inferred-taxonomy observe count exceeds int64",
                )
                .map(i64::to_be_bytes)
            })
            .transpose()?;
        output.row_nullable(&[
            Some(&row.rule_digest),
            Some(INFERRED_TAXONOMY_CATALOG_CONTRACT.as_bytes()),
            Some(&row.catalog_digest),
            Some(INFERRED_TAXONOMY_VECTOR_FORMAT.as_bytes()),
            Some(&member_count.to_be_bytes()),
            Some(&row.member_digest),
            Some(&row.member_keys),
            Some(row.representation.as_bytes()),
            row.observe_reason.map(str::as_bytes),
            observe_count_lower_bound.as_ref().map(<[u8; 8]>::as_slice),
            Some(&pattern_count.to_be_bytes()),
            Some(&pattern_member_count.to_be_bytes()),
            Some(&pattern_member_bytes.to_be_bytes()),
            Some(&row.pattern_member_digest),
            Some(&row.pattern_member_payload),
        ])?;
    }
    output.finish()
}

struct EmittedDictionaries {
    group_copy_path: PathBuf,
    component_copy_path: PathBuf,
    npi_copy_path: PathBuf,
    provider_set_audit_npi_copy_path: PathBuf,
    provider_set_npi_prefix_override_copy_path: PathBuf,
    provider_tax_identity_copy_path: PathBuf,
    provider_group_tax_identity_copy_path: PathBuf,
    pattern_copy_path: Option<PathBuf>,
}

fn emit_dictionaries(
    output_directory: &Path,
    model: &GraphModel,
    tax_identity: &V4TaxIdentityModel,
    layout: ProviderGraphV4Layout,
    progress: &mut ProgressReporter<'_>,
    ownership: &mut OutputOwnership,
) -> ProviderGraphV4Result<EmittedDictionaries> {
    let group_path = output_directory.join("v4-provider-groups.copy");
    let mut groups = PgCopyFileWriter::create_tracked(&group_path, ownership)?;
    for (key, global) in model.group_globals.iter().enumerate() {
        groups.row(&[&(key as i32).to_be_bytes(), global])?;
    }
    groups.finish()?;
    let component_path = output_directory.join("v4-provider-components.copy");
    let mut components = PgCopyFileWriter::create_tracked(&component_path, ownership)?;
    for (key, global) in model.component_globals.iter().enumerate() {
        components.row(&[&(key as i32).to_be_bytes(), global])?;
    }
    components.finish()?;
    let npi_path = output_directory.join("v4-npi-scope.copy");
    let mut npis = PgCopyFileWriter::create_tracked(&npi_path, ownership)?;
    for (key, npi) in model.npis.iter().enumerate() {
        npis.row(&[&(key as i32).to_be_bytes(), &(*npi as i64).to_be_bytes()])?;
    }
    npis.finish()?;
    let provider_set_audit_npi_path = output_directory.join("v4-provider-set-audit-npi.copy");
    let mut provider_set_audit_npis =
        PgCopyFileWriter::create_tracked(&provider_set_audit_npi_path, ownership)?;
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
        PgCopyFileWriter::create_tracked(&provider_set_npi_prefix_override_path, ownership)?;
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
    let provider_tax_identity_path = output_directory.join("v4-provider-tax-identities.copy");
    let mut provider_tax_identities =
        PgCopyFileWriter::create_tracked(&provider_tax_identity_path, ownership)?;
    let tax_row_total = tax_identity
        .tin_hmacs
        .len()
        .checked_add(tax_identity.group_rows.len())
        .ok_or(invalid(
            "V4 tax identity dictionary progress total overflows",
        ))? as u64;
    let mut tax_rows_done = 0u64;
    progress.periodic("emit_dictionaries", 0, tax_row_total, "tax_rows");
    for (tin_key, tin_hmac_sha256) in tax_identity.tin_hmacs.iter().enumerate() {
        provider_tax_identities.row(&[
            &(tin_key as i32).to_be_bytes(),
            &tin_hmac_sha256[..16],
            tin_hmac_sha256,
        ])?;
        tax_rows_done += 1;
        progress.periodic(
            "emit_dictionaries",
            tax_rows_done,
            tax_row_total,
            "tax_rows",
        );
    }
    provider_tax_identities.finish()?;
    let provider_group_tax_identity_path =
        output_directory.join("v4-provider-group-tax-identities.copy");
    let mut provider_group_tax_identities =
        PgCopyFileWriter::create_tracked(&provider_group_tax_identity_path, ownership)?;
    for (provider_group_global_id, state, tin_key, source_bitmap) in &tax_identity.group_rows {
        let tin_key_bytes = tin_key.map(|value| (value as i32).to_be_bytes());
        provider_group_tax_identities.row_nullable(&[
            Some(provider_group_global_id),
            Some(state.as_str().as_bytes()),
            tin_key_bytes.as_ref().map(|value| value.as_slice()),
            Some(source_bitmap),
        ])?;
        tax_rows_done += 1;
        progress.periodic(
            "emit_dictionaries",
            tax_rows_done,
            tax_row_total,
            "tax_rows",
        );
    }
    provider_group_tax_identities.finish()?;
    let pattern_path = if layout == ProviderGraphV4Layout::Pattern {
        let path = output_directory.join("v4-patterns.copy");
        let mut patterns = PgCopyFileWriter::create_tracked(&path, ownership)?;
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
    Ok(EmittedDictionaries {
        group_copy_path: group_path,
        component_copy_path: component_path,
        npi_copy_path: npi_path,
        provider_set_audit_npi_copy_path: provider_set_audit_npi_path,
        provider_set_npi_prefix_override_copy_path: provider_set_npi_prefix_override_path,
        provider_tax_identity_copy_path: provider_tax_identity_path,
        provider_group_tax_identity_copy_path: provider_group_tax_identity_path,
        pattern_copy_path: pattern_path,
    })
}

/// Compile complete factor sidecars into one deterministic adaptive V4 graph.
pub fn compile_provider_graph_v4(
    shards: &[V4ProviderGraphShardDescriptor],
    provider_set_key_map_path: impl AsRef<Path>,
    output_directory: impl AsRef<Path>,
    options: ProviderGraphV4Options,
) -> ProviderGraphV4Result<ProviderGraphV4ConversionSummary> {
    compile_provider_graph_v4_with_inputs(
        shards,
        provider_set_key_map_path,
        output_directory,
        options,
        None,
        None,
    )
}

fn compile_provider_graph_v4_with_inputs(
    shards: &[V4ProviderGraphShardDescriptor],
    provider_set_key_map_path: impl AsRef<Path>,
    output_directory: impl AsRef<Path>,
    options: ProviderGraphV4Options,
    npi_scope: Option<&ProviderGraphV4NpiScopeInput>,
    inferred_taxonomy: Option<&ProviderGraphV4InferredTaxonomyInput>,
) -> ProviderGraphV4Result<ProviderGraphV4ConversionSummary> {
    let mut sink = stderr_progress_sink;
    compile_provider_graph_v4_with_progress(
        shards,
        provider_set_key_map_path,
        output_directory,
        options,
        npi_scope,
        inferred_taxonomy,
        &mut sink,
    )
}

fn compile_provider_graph_v4_with_progress(
    shards: &[V4ProviderGraphShardDescriptor],
    provider_set_key_map_path: impl AsRef<Path>,
    output_directory: impl AsRef<Path>,
    options: ProviderGraphV4Options,
    npi_scope: Option<&ProviderGraphV4NpiScopeInput>,
    inferred_taxonomy: Option<&ProviderGraphV4InferredTaxonomyInput>,
    sink: &mut dyn FnMut(&V4ProgressEvent),
) -> ProviderGraphV4Result<ProviderGraphV4ConversionSummary> {
    options.validate()?;
    let taxonomy_inputs = match (npi_scope, inferred_taxonomy) {
        (Some(scope), Some(candidate_input)) => Some((scope, candidate_input)),
        (None, None) => None,
        _ => {
            return Err(invalid(
                "V4 NPI scope and inferred-taxonomy inputs must be supplied together",
            ));
        }
    };
    fs::create_dir_all(output_directory.as_ref())?;
    let output_directory = fs::canonicalize(output_directory.as_ref())?;
    for name in OUTPUT_NAMES {
        let path = output_directory.join(name);
        match fs::symlink_metadata(&path) {
            Ok(_) => {
                return Err(invalid(format!(
                    "V4 provider graph output already exists: {}",
                    path.display()
                )));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    let mut ownership = OutputOwnership::new(&output_directory);
    let mut progress = ProgressReporter::new(sink);
    let result = compile_provider_graph_v4_inner(
        shards,
        provider_set_key_map_path.as_ref(),
        &output_directory,
        &options,
        taxonomy_inputs,
        &mut progress,
        &mut ownership,
    );
    if result.is_ok() {
        ownership.commit();
    }
    result
}

fn compile_provider_graph_v4_inner(
    shards: &[V4ProviderGraphShardDescriptor],
    provider_set_key_map_path: &Path,
    output_directory: &Path,
    options: &ProviderGraphV4Options,
    taxonomy_inputs: Option<(
        &ProviderGraphV4NpiScopeInput,
        &ProviderGraphV4InferredTaxonomyInput,
    )>,
    progress: &mut ProgressReporter<'_>,
    ownership: &mut OutputOwnership,
) -> ProviderGraphV4Result<ProviderGraphV4ConversionSummary> {
    progress.emit("resource_admission", 0, 1, "stage", false);
    let mut resource_admission =
        resource_admission_preflight(shards, provider_set_key_map_path, options)?;
    progress.emit("resource_admission", 1, 1, "stage", false);
    let raw = load_raw_factors(shards, progress)?;
    let provider_sets = ProviderSetMap::read(provider_set_key_map_path)?;
    let mut model = build_graph_model(
        &raw,
        &provider_sets,
        progress,
        &mut resource_admission,
        options,
    )?;
    let inferred_taxonomy_model = match taxonomy_inputs {
        Some((scope, candidate_input)) => {
            validate_npi_scope_input(shards, scope, &model.npis)?;
            read_inferred_taxonomy_model(
                candidate_input,
                scope,
                model.npis.len(),
                &mut resource_admission,
            )?
        }
        None => V4InferredTaxonomyModel { rules: Vec::new() },
    };
    reserve_inferred_taxonomy_projection_memory(
        &model.npi_patterns,
        &inferred_taxonomy_model,
        options,
        &mut resource_admission,
    )?;
    let direct_inferred_taxonomy = inferred_taxonomy_projection(
        &model.npi_patterns,
        &inferred_taxonomy_model,
        ProviderGraphV4Layout::Direct,
        options,
    )?;
    let pattern_inferred_taxonomy = inferred_taxonomy_projection(
        &model.npi_patterns,
        &inferred_taxonomy_model,
        ProviderGraphV4Layout::Pattern,
        options,
    )?;
    let tax_identity = V4TaxIdentityModel::build(&raw.tax_identities, &model.group_globals)?;
    let tax_dictionary_projection_bytes = (tax_identity.tin_hmacs.len() as u64)
        .checked_mul(TAX_IDENTITY_DICTIONARY_ENTRY_UPPER_BOUND_BYTES)
        .ok_or(invalid(
            "resource_admission: tax identity projection byte count overflows",
        ))?;
    let tax_projection_bytes = (tax_identity.group_rows.len() as u64)
        .checked_mul(
            TAX_IDENTITY_GROUP_ENTRY_UPPER_BOUND_BYTES
                .saturating_add(tax_identity.source_bitmap_bytes as u64),
        )
        .and_then(|value| value.checked_add(tax_dictionary_projection_bytes))
        .ok_or(invalid(
            "resource_admission: tax identity projection byte count overflows",
        ))?;
    resource_admission.reconcile_tax_identity_projection(tax_projection_bytes)?;
    let mut observe = model.observe.clone();
    let sizes = compute_layout_sizes(
        &model,
        &tax_identity,
        &direct_inferred_taxonomy,
        &pattern_inferred_taxonomy,
        options,
    )?;
    let pattern_layout_component_fallback_eligible =
        record_pattern_fallback_diagnostics(&model, options, &mut observe);
    let pattern_layout_sparse_prefix_eligible = model.npi_prefix_sparse_eligible;
    let pattern_layout_serving_degree_eligible =
        pattern_layout_component_fallback_eligible && pattern_layout_sparse_prefix_eligible;
    let direct_layout_eligible =
        model.npi_prefix_complete_eligible && direct_inferred_taxonomy.eligible;
    let pattern_layout_eligible =
        pattern_layout_serving_degree_eligible && pattern_inferred_taxonomy.eligible;
    let selected_layout = choose_complete_layout(
        sizes.direct,
        direct_layout_eligible,
        sizes.pattern,
        pattern_layout_eligible,
    )?;
    select_npi_prefix_projection(&mut model, &mut observe, selected_layout)?;
    progress.emit("select_layout", 1, 1, "stage", false);

    let block_copy_path = output_directory.join("v4-graph-blocks.copy");
    let reference_manifest_path = output_directory.join("v4-graph-references.jsonl");
    let mut cas =
        CasBlockWriter::create_tracked(&block_copy_path, &reference_manifest_path, ownership)?;
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
    let selected_representation = match selected_layout {
        ProviderGraphV4Layout::Direct => "direct_v1",
        ProviderGraphV4Layout::Pattern => "pattern_v1",
    };
    let emitted_mapping_bytes = emitted_mapping_persistence_bytes(
        &relation_summaries,
        &heavy_bitmaps,
        selected_representation,
    )?;
    let selected_mapping = match selected_layout {
        ProviderGraphV4Layout::Direct => MappingPersistenceSizes {
            total_encoded_bytes: sizes.direct_mapping,
            map_payload_encoded_bytes: sizes.direct_map_payload,
            coordinate_count: sizes.direct_map_coordinate_count,
            pack_count: sizes.direct_map_pack_count,
            object_kind_count: sizes.direct_map_object_kind_count,
        },
        ProviderGraphV4Layout::Pattern => MappingPersistenceSizes {
            total_encoded_bytes: sizes.pattern_mapping,
            map_payload_encoded_bytes: sizes.pattern_map_payload,
            coordinate_count: sizes.pattern_map_coordinate_count,
            pack_count: sizes.pattern_map_pack_count,
            object_kind_count: sizes.pattern_map_object_kind_count,
        },
    };
    if emitted_mapping_bytes != selected_mapping {
        return Err(invalid(format!(
            "V4 selected packed-map plan differs from emitted coordinates: planned {selected_mapping:?}, emitted {emitted_mapping_bytes:?}"
        )));
    }
    let (block_count, block_copy_bytes) = cas.finish()?;
    let EmittedDictionaries {
        group_copy_path,
        component_copy_path,
        npi_copy_path,
        provider_set_audit_npi_copy_path,
        provider_set_npi_prefix_override_copy_path,
        provider_tax_identity_copy_path,
        provider_group_tax_identity_copy_path,
        pattern_copy_path,
    } = emit_dictionaries(
        output_directory,
        &model,
        &tax_identity,
        selected_layout,
        progress,
        ownership,
    )?;
    let inferred_taxonomy_copy_path = output_directory.join("v4-inferred-taxonomy-candidates.copy");
    let selected_inferred_taxonomy = match selected_layout {
        ProviderGraphV4Layout::Direct => &direct_inferred_taxonomy,
        ProviderGraphV4Layout::Pattern => &pattern_inferred_taxonomy,
    };
    emit_inferred_taxonomy_candidates(
        &inferred_taxonomy_copy_path,
        selected_inferred_taxonomy,
        ownership,
    )?;
    if fs::metadata(&inferred_taxonomy_copy_path)?.len() != selected_inferred_taxonomy.encoded_bytes
    {
        return Err(invalid(
            "V4 inferred-taxonomy planned bytes differ from emitted COPY",
        ));
    }
    let summary_path = output_directory.join("v4-summary.json");
    let input_digest: [u8; 32] = raw.input_digest.finalize().into();
    let mut database_output_bytes = block_copy_bytes;
    for path in [
        &group_copy_path,
        &component_copy_path,
        &npi_copy_path,
        &provider_set_audit_npi_copy_path,
        &provider_set_npi_prefix_override_copy_path,
        &provider_tax_identity_copy_path,
        &provider_group_tax_identity_copy_path,
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
    let selected_graph_bytes = match selected_layout {
        ProviderGraphV4Layout::Direct => sizes.direct_graph,
        ProviderGraphV4Layout::Pattern => sizes.pattern_graph,
    };
    if database_output_bytes != selected_graph_bytes {
        return Err(invalid(format!(
            "V4 selected bitmap plan differs from emitted graph output: planned {selected_graph_bytes}, emitted {database_output_bytes}"
        )));
    }
    let selected_encoded_bytes = match selected_layout {
        ProviderGraphV4Layout::Direct => sizes.direct,
        ProviderGraphV4Layout::Pattern => sizes.pattern,
    };
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
        output_artifact(
            "provider_tax_identities",
            &provider_tax_identity_copy_path,
            tax_identity.tin_hmacs.len() as u64,
        )?,
        output_artifact(
            "provider_group_tax_identities",
            &provider_group_tax_identity_copy_path,
            tax_identity.group_rows.len() as u64,
        )?,
        output_artifact(
            "inferred_taxonomy_candidates",
            &inferred_taxonomy_copy_path,
            selected_inferred_taxonomy.rows.len() as u64,
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
        member_page_bytes: options.member_page_bytes as u64,
        locator_page_bytes: options.locator_page_bytes as u64,
        heavy_owner_member_threshold: options.heavy_owner_member_threshold as u64,
        heavy_bitmap_minimum_savings_bytes: options.heavy_bitmap_minimum_savings_bytes as u64,
        pattern_layout_serving_degree_eligible,
        pattern_layout_sparse_prefix_eligible,
        direct_layout_complete_prefix_eligible: model.npi_prefix_complete_eligible,
        pattern_sparse_prefix_owner_count: model.npi_prefix_sparse_owner_count,
        pattern_sparse_prefix_member_count: model.npi_prefix_sparse_member_count,
        pattern_sparse_prefix_raw_bytes: model.npi_prefix_sparse_raw_bytes,
        pattern_sparse_prefix_projection_encoded_bytes: model
            .npi_prefix_sparse_projection_encoded_bytes,
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
        max_online_inferred_taxonomy_candidates: options.max_online_inferred_taxonomy_candidates
            as u64,
        max_online_candidate_pattern_projection_members: options
            .max_online_candidate_pattern_projection_members
            as u64,
        direct_complete_prefix_projection_encoded_bytes: model
            .npi_prefix_complete_projection_encoded_bytes,
        direct_graph_encoded_bytes: sizes.direct_graph,
        pattern_graph_encoded_bytes: sizes.pattern_graph,
        direct_mapping_persistence_encoded_bytes: sizes.direct_mapping,
        pattern_mapping_persistence_encoded_bytes: sizes.pattern_mapping,
        direct_map_payload_encoded_bytes: sizes.direct_map_payload,
        pattern_map_payload_encoded_bytes: sizes.pattern_map_payload,
        direct_map_coordinate_count: sizes.direct_map_coordinate_count,
        pattern_map_coordinate_count: sizes.pattern_map_coordinate_count,
        direct_map_pack_count: sizes.direct_map_pack_count,
        pattern_map_pack_count: sizes.pattern_map_pack_count,
        direct_map_object_kind_count: sizes.direct_map_object_kind_count,
        pattern_map_object_kind_count: sizes.pattern_map_object_kind_count,
        direct_complete_encoded_bytes: sizes.direct,
        pattern_complete_encoded_bytes: sizes.pattern,
        direct_inferred_taxonomy_encoded_bytes: sizes.direct_inferred_taxonomy,
        pattern_inferred_taxonomy_encoded_bytes: sizes.pattern_inferred_taxonomy,
        direct_inferred_taxonomy_eligible: direct_inferred_taxonomy.eligible,
        pattern_inferred_taxonomy_eligible: pattern_inferred_taxonomy.eligible,
        direct_inferred_taxonomy_rejection_reason: direct_inferred_taxonomy
            .rejection
            .as_ref()
            .map(|value| value.reason.to_owned()),
        direct_inferred_taxonomy_rejection_rule_digest: direct_inferred_taxonomy
            .rejection
            .as_ref()
            .map(|value| hex(&value.rule_digest)),
        direct_inferred_taxonomy_rejection_observed_count: direct_inferred_taxonomy
            .rejection
            .as_ref()
            .map(|value| value.observed_count),
        direct_inferred_taxonomy_rejection_cap: direct_inferred_taxonomy
            .rejection
            .as_ref()
            .map(|value| value.cap),
        pattern_inferred_taxonomy_rejection_reason: pattern_inferred_taxonomy
            .rejection
            .as_ref()
            .map(|value| value.reason.to_owned()),
        pattern_inferred_taxonomy_rejection_rule_digest: pattern_inferred_taxonomy
            .rejection
            .as_ref()
            .map(|value| hex(&value.rule_digest)),
        pattern_inferred_taxonomy_rejection_observed_count: pattern_inferred_taxonomy
            .rejection
            .as_ref()
            .map(|value| value.observed_count),
        pattern_inferred_taxonomy_rejection_cap: pattern_inferred_taxonomy
            .rejection
            .as_ref()
            .map(|value| value.cap),
        common_encoded_bytes: sizes.common,
        selected_graph_encoded_bytes: selected_graph_bytes,
        selected_encoded_bytes,
        block_copy_path,
        reference_manifest_path,
        group_copy_path,
        component_copy_path,
        npi_copy_path,
        provider_set_audit_npi_copy_path,
        provider_set_npi_prefix_override_copy_path,
        provider_tax_identity_copy_path,
        provider_group_tax_identity_copy_path,
        pattern_copy_path,
        inferred_taxonomy_copy_path,
        summary_path: summary_path.clone(),
        block_count,
        block_copy_bytes,
        relation_summaries,
        heavy_bitmaps,
        output_artifacts,
        tax_identity: tax_identity.summary()?,
        observe,
        resource_admission: resource_admission.into_summary(),
        input_byte_count: raw.input_byte_count,
        input_sha256: hex(&input_digest),
    };
    let summary_file = ownership.create(&summary_path)?;
    let mut summary_writer = BufWriter::new(summary_file);
    serde_json::to_writer_pretty(&mut summary_writer, &summary)?;
    summary_writer.flush()?;
    progress.emit("complete", 1, 1, "compile", true);
    Ok(summary)
}

pub fn compile_provider_graph_v4_manifest(
    manifest: ProviderGraphV4Manifest,
) -> ProviderGraphV4Result<ProviderGraphV4ConversionSummary> {
    compile_provider_graph_v4_with_inputs(
        &manifest.shards,
        manifest.provider_set_key_map_path,
        manifest.output_directory,
        manifest.options,
        Some(&manifest.npi_scope),
        Some(&manifest.inferred_taxonomy),
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
    if !(MIN_NPI..=MAX_NPI).contains(&npi) {
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

    fn write_tax_identity(
        path: &Path,
        shard_id: &str,
        policy_id: &str,
        records: impl IntoIterator<Item = (GlobalId, V4TaxIdentityState, Option<[u8; 32]>)>,
    ) -> V4TaxIdentityArtifactDescriptor {
        let mut records = records.into_iter().collect::<Vec<_>>();
        records.sort_unstable_by_key(|record| record.0);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(TAX_IDENTITY_MAGIC);
        bytes.extend_from_slice(&TAX_IDENTITY_VERSION.to_le_bytes());
        bytes.extend_from_slice(&TAX_IDENTITY_RECORD_BYTES.to_le_bytes());
        bytes.push(policy_id.len() as u8);
        bytes.extend_from_slice(policy_id.as_bytes());
        let mut counts = [0u64; 4];
        for (group, state, hmac) in &records {
            bytes.extend_from_slice(group);
            bytes.push(match state {
                V4TaxIdentityState::MatchedEin => 1,
                V4TaxIdentityState::Missing => 2,
                V4TaxIdentityState::Malformed => 3,
                V4TaxIdentityState::UnsupportedType => 4,
            });
            let hmac = hmac.unwrap_or([0; 32]);
            bytes.extend_from_slice(&hmac[..16]);
            bytes.extend_from_slice(&hmac);
            match state {
                V4TaxIdentityState::MatchedEin => counts[0] += 1,
                V4TaxIdentityState::Missing => counts[1] += 1,
                V4TaxIdentityState::Malformed => counts[2] += 1,
                V4TaxIdentityState::UnsupportedType => counts[3] += 1,
            }
        }
        fs::write(path, &bytes).unwrap();
        V4TaxIdentityArtifactDescriptor {
            path: path.to_path_buf(),
            metadata: V4TaxIdentityMetadata {
                record_format: TAX_IDENTITY_FORMAT.to_owned(),
                sha256: hex(&Sha256::digest(&bytes)),
                byte_count: bytes.len() as u64,
                row_count: records.len() as u64,
                provider_group_count: records.len() as u64,
                matched_ein_count: counts[0],
                missing_count: counts[1],
                malformed_count: counts[2],
                unsupported_type_count: counts[3],
                version: TAX_IDENTITY_VERSION,
                record_bytes: TAX_IDENTITY_RECORD_BYTES,
                token_policy_id: policy_id.to_owned(),
                normalization_contract: TAX_IDENTITY_NORMALIZATION_CONTRACT.to_owned(),
                hmac_contract: TAX_IDENTITY_HMAC_CONTRACT.to_owned(),
                final_file: true,
                name: Some("provider_group_tax_identity".to_owned()),
                source_shard_id: Some(shard_id.to_owned()),
                shard_id: None,
            },
        }
    }

    fn write_missing_tax_identity(
        path: &Path,
        shard_id: &str,
        groups: impl IntoIterator<Item = GlobalId>,
    ) -> V4TaxIdentityArtifactDescriptor {
        write_tax_identity(
            path,
            shard_id,
            "ptg-tin-hmac-sha256-v1:test",
            groups
                .into_iter()
                .map(|group| (group, V4TaxIdentityState::Missing, None)),
        )
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

    fn write_npi_scope(
        path: &Path,
        shard_id: &str,
        reciprocal: &V4MembershipArtifactDescriptor,
    ) -> V4NpiScopeArtifactDescriptor {
        let artifact = ValidatedArtifact::open(reciprocal).unwrap();
        let mut output = PgCopyFileWriter::create(path).unwrap();
        for owner_index in 0..artifact.owner_count {
            let npi = npi_from_global_id(artifact.owner(owner_index).unwrap().owner).unwrap();
            output.row(&[&(npi as i64).to_be_bytes()]).unwrap();
        }
        output.finish().unwrap();
        let bytes = fs::read(path).unwrap();
        let mut metadata = V4NpiScopeMetadata {
            record_format: NPI_SCOPE_ARTIFACT_FORMAT.to_owned(),
            sha256: hex(&Sha256::digest(&bytes)),
            byte_count: bytes.len() as u64,
            row_count: artifact.owner_count,
            provider_npi_group_sha256: reciprocal.metadata.sha256.clone(),
            provider_npi_group_record_format: reciprocal.metadata.record_format.clone(),
            provider_npi_group_byte_count: reciprocal.metadata.byte_count,
            provider_npi_group_owner_count: reciprocal.metadata.owner_count,
            provider_npi_group_member_count: reciprocal.metadata.member_count,
            provider_npi_group_member_global_count: reciprocal
                .metadata
                .member_global_count
                .unwrap_or(0),
            binding_contract: NPI_SCOPE_BINDING_CONTRACT.to_owned(),
            binding_sha256: String::new(),
            shard_binding_contract: NPI_SCOPE_SHARD_BINDING_CONTRACT.to_owned(),
            shard_binding_sha256: String::new(),
            retention_contract: NPI_SCOPE_RETENTION_CONTRACT.to_owned(),
            name: Some("provider_npi_scope".to_owned()),
            source_shard_id: Some(shard_id.to_owned()),
            shard_id: None,
        };
        metadata.binding_sha256 = hex(&npi_scope_binding_digest(&metadata).unwrap());
        metadata.shard_binding_sha256 =
            hex(&npi_scope_shard_binding_digest(&metadata, shard_id).unwrap());
        V4NpiScopeArtifactDescriptor {
            path: path.to_path_buf(),
            metadata,
        }
    }

    struct Fixture {
        _temporary: TempDir,
        shard: V4ProviderGraphShardDescriptor,
        provider_map: PathBuf,
        output: PathBuf,
    }

    fn shared_pattern_fixture_with_shard_id(
        group_count: usize,
        set_count: usize,
        shard_id: &str,
    ) -> Fixture {
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
            shard_id,
            "provider_set_component",
            sets.iter().copied().map(|set| (set, component)),
            true,
        );
        let component_group = write_membership(
            &temporary.path().join("component-group.sidecar"),
            shard_id,
            "provider_component_group",
            groups.iter().copied().map(|group| (component, group)),
            true,
        );
        let group_npi = write_membership(
            &temporary.path().join("group-npi.sidecar"),
            shard_id,
            "provider_group_npi",
            groups.iter().copied().map(|group| (group, provider_npi)),
            true,
        );
        let npi_group = write_membership(
            &temporary.path().join("npi-group.sidecar"),
            shard_id,
            "provider_npi_group",
            groups.iter().copied().map(|group| (provider_npi, group)),
            true,
        );
        let provider_npi_scope = write_npi_scope(
            &temporary.path().join("npi-scope.copy"),
            shard_id,
            &npi_group,
        );
        let provider_map = temporary.path().join("provider-map.copy");
        write_provider_map(&provider_map, &sets, 1);
        let output = temporary.path().join("output");
        let provider_group_tax_identity = write_missing_tax_identity(
            &temporary.path().join("group-tax-identity.sidecar"),
            shard_id,
            groups,
        );
        Fixture {
            _temporary: temporary,
            shard: V4ProviderGraphShardDescriptor {
                shard_id: shard_id.to_owned(),
                provider_set_component: set_component,
                provider_component_group: component_group,
                provider_group_npi: group_npi,
                provider_npi_group: npi_group,
                provider_npi_scope,
                provider_group_tax_identity,
            },
            provider_map,
            output,
        }
    }

    fn shared_pattern_fixture(group_count: usize, set_count: usize) -> Fixture {
        shared_pattern_fixture_with_shard_id(group_count, set_count, "source-a")
    }

    fn scope_input(summary: &ProviderGraphV4NpiScopeSummary) -> ProviderGraphV4NpiScopeInput {
        ProviderGraphV4NpiScopeInput {
            format: summary.format.clone(),
            row_count: summary.row_count,
            source_owner_count: summary.source_owner_count,
            input_byte_count: summary.input_byte_count,
            input_sha256: summary.input_sha256.clone(),
            output_byte_count: summary.output_byte_count,
            output_sha256: summary.output_sha256.clone(),
            output_path: summary.output_path.clone(),
        }
    }

    #[test]
    fn npi_scope_auth_workers_are_bounded_by_cpu_shards_and_cap() {
        assert_eq!(npi_scope_auth_worker_count(1, 192), 1);
        assert_eq!(npi_scope_auth_worker_count(64, 3), 3);
        assert_eq!(npi_scope_auth_worker_count(64, 192), 8);
    }

    #[test]
    fn npi_scope_prepass_merges_shards_in_model_key_order() {
        let shared = shared_pattern_fixture(4, 2);
        let independent = independent_fixture();
        let temporary = tempfile::tempdir().unwrap();
        let output_path = temporary.path().join("v4-npi-scope.copy");
        let summary = extract_provider_graph_v4_npi_scope(
            &[shared.shard.clone(), independent.shard.clone()],
            &output_path,
        )
        .unwrap();

        assert_eq!(summary.format, "ptg2_provider_graph_v4_npi_scope_v1");
        assert_eq!(summary.row_count, 3);
        let output_bytes = fs::read(&output_path).unwrap();
        assert_eq!(summary.output_sha256, hex(&Sha256::digest(&output_bytes)));
        let expected_rows = vec![(0, 1_111_111_111), (1, 1_234_567_890), (2, 2_222_222_222)];
        assert_eq!(read_npi_scope_copy(&output_path).unwrap(), expected_rows);
        let mut expected_bytes = PG_COPY_HEADER.to_vec();
        for (key, npi) in expected_rows {
            expected_bytes.extend_from_slice(&2i16.to_be_bytes());
            expected_bytes.extend_from_slice(&4i32.to_be_bytes());
            expected_bytes.extend_from_slice(&key.to_be_bytes());
            expected_bytes.extend_from_slice(&8i32.to_be_bytes());
            expected_bytes.extend_from_slice(&(npi as i64).to_be_bytes());
        }
        expected_bytes.extend_from_slice(&(-1i16).to_be_bytes());
        assert_eq!(output_bytes, expected_bytes);
    }

    #[test]
    fn npi_scope_output_refuses_existing_paths_without_mutation() {
        let fixture = shared_pattern_fixture(4, 2);
        let temporary = tempfile::tempdir().unwrap();
        let existing = temporary.path().join("existing.copy");
        fs::write(&existing, b"caller-owned").unwrap();

        assert!(extract_provider_graph_v4_npi_scope(
            std::slice::from_ref(&fixture.shard),
            &existing,
        )
        .is_err());
        assert_eq!(fs::read(&existing).unwrap(), b"caller-owned");
        assert!(PgCopyFileWriter::create(&existing).is_err());
        assert_eq!(fs::read(&existing).unwrap(), b"caller-owned");

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            let target = temporary.path().join("must-not-be-created.copy");
            let link = temporary.path().join("dangling.copy");
            symlink(&target, &link).unwrap();
            assert!(extract_provider_graph_v4_npi_scope(
                std::slice::from_ref(&fixture.shard),
                &link,
            )
            .is_err());
            assert!(fs::symlink_metadata(&link)
                .unwrap()
                .file_type()
                .is_symlink());
            assert!(!target.exists());
            assert!(PgCopyFileWriter::create(&link).is_err());
            assert!(fs::symlink_metadata(&link)
                .unwrap()
                .file_type()
                .is_symlink());
            assert!(!target.exists());
        }
    }

    #[test]
    fn npi_scope_output_is_removed_after_partial_merge_failure() {
        let mut fixture = independent_fixture();
        let scope = &mut fixture.shard.provider_npi_scope;
        let mut bytes = fs::read(&scope.path).unwrap();
        let second_npi_offset = PG_COPY_HEADER.len() + 14 + 2 + 4;
        bytes[second_npi_offset..second_npi_offset + 8]
            .copy_from_slice(&(MIN_NPI - 1).to_be_bytes());
        fs::write(&scope.path, &bytes).unwrap();
        scope.metadata.sha256 = hex(&Sha256::digest(&bytes));
        scope.metadata.binding_sha256 = hex(&npi_scope_binding_digest(&scope.metadata).unwrap());
        scope.metadata.shard_binding_sha256 =
            hex(&npi_scope_shard_binding_digest(&scope.metadata, &fixture.shard.shard_id).unwrap());
        let temporary = tempfile::tempdir().unwrap();
        let output = temporary.path().join("partial.copy");

        assert!(
            extract_provider_graph_v4_npi_scope(std::slice::from_ref(&fixture.shard), &output,)
                .is_err()
        );
        assert!(!output.exists());
    }

    #[test]
    fn npi_scope_post_finish_race_preserves_the_foreign_replacement() {
        let fixture = shared_pattern_fixture(4, 2);
        let temporary = tempfile::tempdir().unwrap();
        let output = temporary.path().join("raced.copy");
        let error = extract_provider_graph_v4_npi_scope_inner_with_hook(
            std::slice::from_ref(&fixture.shard),
            &output,
            |path| {
                fs::remove_file(path).unwrap();
                fs::write(path, b"foreign-output").unwrap();
            },
        )
        .unwrap_err();

        assert!(error.to_string().contains("output path changed"));
        assert_eq!(fs::read(&output).unwrap(), b"foreign-output");
    }

    #[test]
    fn npi_scope_validation_reads_one_descriptor_and_rejects_path_replacement() {
        let fixture = shared_pattern_fixture(4, 2);
        let temporary = tempfile::tempdir().unwrap();
        let output = temporary.path().join("scope.copy");
        let summary =
            extract_provider_graph_v4_npi_scope(std::slice::from_ref(&fixture.shard), &output)
                .unwrap();
        let input = scope_input(&summary);
        let error = validate_npi_scope_input_with_hook(
            std::slice::from_ref(&fixture.shard),
            &input,
            &[1_234_567_890],
            |path| {
                fs::remove_file(path).unwrap();
                fs::write(path, b"foreign-output").unwrap();
            },
        )
        .unwrap_err();

        assert!(error.to_string().contains("prepass artifact changed"));
        assert_eq!(fs::read(&output).unwrap(), b"foreign-output");
    }

    #[cfg(unix)]
    #[test]
    fn npi_scope_validation_rejects_dangling_symlink_without_following_it() {
        use std::os::unix::fs::symlink;

        let fixture = shared_pattern_fixture(4, 2);
        let temporary = tempfile::tempdir().unwrap();
        let output = temporary.path().join("scope.copy");
        let summary =
            extract_provider_graph_v4_npi_scope(std::slice::from_ref(&fixture.shard), &output)
                .unwrap();
        fs::remove_file(&output).unwrap();
        let target = temporary.path().join("must-not-be-created.copy");
        symlink(&target, &output).unwrap();

        assert!(validate_npi_scope_input(
            std::slice::from_ref(&fixture.shard),
            &scope_input(&summary),
            &[1_234_567_890],
        )
        .is_err());
        assert!(fs::symlink_metadata(&output)
            .unwrap()
            .file_type()
            .is_symlink());
        assert!(!target.exists());
    }

    #[test]
    fn npi_scope_requires_dense_reciprocal_global_count_metadata() {
        let fixture = shared_pattern_fixture(4, 2);
        let temporary = tempfile::tempdir().unwrap();
        for (name, record_format, member_global_count) in [
            (
                "standard",
                STANDARD_FORMAT,
                fixture
                    .shard
                    .provider_npi_group
                    .metadata
                    .member_global_count,
            ),
            ("missing-count", DENSE_FORMAT, None),
            (
                "wrong-count",
                DENSE_FORMAT,
                fixture
                    .shard
                    .provider_npi_group
                    .metadata
                    .member_global_count
                    .map(|count| count + 1),
            ),
        ] {
            let mut shard = fixture.shard.clone();
            shard.provider_npi_group.metadata.record_format = record_format.to_owned();
            shard.provider_npi_group.metadata.member_global_count = member_global_count;
            let output = temporary.path().join(format!("{name}.copy"));
            assert!(
                extract_provider_graph_v4_npi_scope(std::slice::from_ref(&shard), &output).is_err(),
                "{name}",
            );
            assert!(
                expected_npi_scope_input_identity(std::slice::from_ref(&shard)).is_err(),
                "{name}",
            );
            assert!(!output.exists(), "{name}");
        }
    }

    #[test]
    fn npi_scope_copy_reader_rejects_values_outside_exact_ten_digit_range() {
        let temporary = tempfile::tempdir().unwrap();
        for npi in [MIN_NPI - 1, MAX_NPI + 1] {
            let path = temporary.path().join(format!("{npi}.copy"));
            let mut writer = PgCopyFileWriter::create(&path).unwrap();
            writer
                .row(&[&0i32.to_be_bytes(), &(npi as i64).to_be_bytes()])
                .unwrap();
            writer.finish().unwrap();
            assert!(read_npi_scope_copy(&path).is_err(), "{npi}");
        }
    }

    #[test]
    fn npi_scope_copy_reader_rejects_every_framing_boundary() {
        let temporary = tempfile::tempdir().unwrap();
        let rejects = |name: &str, bytes: &[u8]| {
            let path = temporary.path().join(name);
            fs::write(&path, bytes).unwrap();
            assert!(read_npi_scope_copy(&path).is_err(), "{name}");
        };

        rejects("truncated-header.copy", b"");
        rejects("invalid-header.copy", &[0; PG_COPY_HEADER.len()]);

        let mut bytes = PG_COPY_HEADER.to_vec();
        rejects("truncated-row-header.copy", &bytes);
        bytes.extend_from_slice(&1i16.to_be_bytes());
        rejects("wrong-field-count.copy", &bytes);

        let mut bytes = PG_COPY_HEADER.to_vec();
        bytes.extend_from_slice(&(-1i16).to_be_bytes());
        bytes.push(0);
        rejects("trailing-byte.copy", &bytes);

        let mut bytes = PG_COPY_HEADER.to_vec();
        bytes.extend_from_slice(&2i16.to_be_bytes());
        rejects("truncated-key-length.copy", &bytes);
        bytes.extend_from_slice(&3i32.to_be_bytes());
        rejects("wrong-key-width.copy", &bytes);

        let mut bytes = PG_COPY_HEADER.to_vec();
        bytes.extend_from_slice(&2i16.to_be_bytes());
        bytes.extend_from_slice(&4i32.to_be_bytes());
        bytes.extend_from_slice(&[0; 2]);
        rejects("truncated-key.copy", &bytes);

        let mut bytes = PG_COPY_HEADER.to_vec();
        bytes.extend_from_slice(&2i16.to_be_bytes());
        bytes.extend_from_slice(&4i32.to_be_bytes());
        bytes.extend_from_slice(&0i32.to_be_bytes());
        rejects("truncated-npi-length.copy", &bytes);
        bytes.extend_from_slice(&7i32.to_be_bytes());
        rejects("wrong-npi-width.copy", &bytes);

        let mut bytes = PG_COPY_HEADER.to_vec();
        bytes.extend_from_slice(&2i16.to_be_bytes());
        bytes.extend_from_slice(&4i32.to_be_bytes());
        bytes.extend_from_slice(&(-1i32).to_be_bytes());
        bytes.extend_from_slice(&8i32.to_be_bytes());
        bytes.extend_from_slice(&(MIN_NPI as i64).to_be_bytes());
        bytes.extend_from_slice(&(-1i16).to_be_bytes());
        rejects("negative-key.copy", &bytes);

        let direct = |name: &str, bytes: &[u8], remaining_rows: u64| {
            let path = temporary.path().join(name);
            fs::write(&path, bytes).unwrap();
            let mut artifact = ValidatedNpiScopeArtifact {
                reader: BufReader::new(File::open(path).unwrap()),
                remaining_rows,
                previous_npi: 0,
                finished: false,
            };
            assert!(artifact.next_npi().is_err(), "{name}");
        };
        direct("truncated-trailer.copy", b"", 0);
        direct("truncated-source-row.copy", b"", 1);
        direct("truncated-source-length.copy", &1i16.to_be_bytes(), 1);
        let mut invalid_field_count = [0u8; 14];
        invalid_field_count[..2].copy_from_slice(&2i16.to_be_bytes());
        invalid_field_count[2..6].copy_from_slice(&8i32.to_be_bytes());
        invalid_field_count[6..].copy_from_slice(&MIN_NPI.to_be_bytes());
        direct("invalid-source-field-count.copy", &invalid_field_count, 1);
        let mut invalid_field_width = invalid_field_count;
        invalid_field_width[..2].copy_from_slice(&1i16.to_be_bytes());
        invalid_field_width[2..6].copy_from_slice(&7i32.to_be_bytes());
        direct("invalid-source-field-width.copy", &invalid_field_width, 1);
        let mut truncated_value = 1i16.to_be_bytes().to_vec();
        truncated_value.extend_from_slice(&8i32.to_be_bytes());
        truncated_value.extend_from_slice(&[0; 4]);
        direct("truncated-source-value.copy", &truncated_value, 1);
    }

    fn inferred_rule(rule_ordinal: u8, member_count: usize) -> V4InferredTaxonomyRule {
        V4InferredTaxonomyRule {
            rule_digest: [rule_ordinal; 32],
            catalog_digest: [rule_ordinal.saturating_add(32); 32],
            member_keys: (0..member_count as u32).collect(),
        }
    }

    fn one_rule_taxonomy_input(
        members_path: &Path,
        members: &[u8],
        scope_sha256: &str,
    ) -> ProviderGraphV4InferredTaxonomyInput {
        fs::write(members_path, members).unwrap();
        let rule_digest = [7u8; 32];
        let mut rule_set_digest = Sha256::new();
        rule_set_digest.update(INFERRED_TAXONOMY_RULE_SET_DIGEST_DOMAIN);
        rule_set_digest.update(1u32.to_be_bytes());
        rule_set_digest.update(rule_digest);
        ProviderGraphV4InferredTaxonomyInput {
            contract: INFERRED_TAXONOMY_INPUT_CONTRACT.to_owned(),
            catalog_contract: INFERRED_TAXONOMY_CATALOG_CONTRACT.to_owned(),
            vector_format: INFERRED_TAXONOMY_VECTOR_FORMAT.to_owned(),
            npi_scope_sha256: scope_sha256.to_owned(),
            rule_set_digest: hex(&rule_set_digest.finalize()),
            members: ProviderGraphV4InputArtifact {
                path: members_path.to_path_buf(),
                byte_count: members.len() as u64,
                sha256: hex(&Sha256::digest(members)),
            },
            rules: vec![ProviderGraphV4InferredTaxonomyRuleInput {
                rule_digest: hex(&rule_digest),
                catalog_digest: hex(&[8; 32]),
                member_count: members.len() as u64 / 4,
                member_offset_bytes: 0,
                member_byte_count: members.len() as u64,
            }],
        }
    }

    #[test]
    fn inferred_taxonomy_memory_is_authenticated_and_admitted_before_projection() {
        let temporary = tempfile::tempdir().unwrap();
        let scope_sha256 = hex(&[9; 32]);
        let scope = ProviderGraphV4NpiScopeInput {
            format: NPI_SCOPE_FORMAT.to_owned(),
            row_count: 1,
            source_owner_count: 1,
            input_byte_count: 0,
            input_sha256: hex(&[0; 32]),
            output_byte_count: 0,
            output_sha256: scope_sha256.clone(),
            output_path: temporary.path().join("unused.copy"),
        };
        let input = one_rule_taxonomy_input(
            &temporary.path().join("taxonomy-members.u32le"),
            &0u32.to_le_bytes(),
            &scope_sha256,
        );

        let mut corrupted = input.clone();
        corrupted.members.sha256 = hex(&[0; 32]);
        let mut tiny = blank_admission(Some(1));
        let error = read_inferred_taxonomy_model(&corrupted, &scope, 1, &mut tiny).unwrap_err();
        assert!(error.to_string().contains("checksum changed"));
        assert_eq!(tiny.summary.derived_projection_bytes, 0);

        let mut admitted = blank_admission(None);
        let model = read_inferred_taxonomy_model(&input, &scope, 1, &mut admitted).unwrap();
        assert_eq!(model.rules.len(), 1);
        assert!(admitted.summary.derived_projection_bytes >= ESTIMATED_VEC_OWNER_BYTES);
        assert!(admitted.summary.retained_scratch_high_water_bytes >= 4);
        reserve_inferred_taxonomy_projection_memory(
            &[vec![0, 1]],
            &model,
            &ProviderGraphV4Options::default(),
            &mut admitted,
        )
        .unwrap();
        let exact_peak = admitted.summary.estimated_peak_bytes;
        assert!(admitted.summary.derived_projection_bytes > input.members.byte_count);
        assert!(
            admitted.summary.retained_scratch_high_water_bytes
                >= 2 * ESTIMATED_PATTERN_POSTING_SCRATCH_BYTES
        );

        let mut limited = blank_admission(Some(exact_peak - 1));
        let limited_model = read_inferred_taxonomy_model(&input, &scope, 1, &mut limited).unwrap();
        let error = reserve_inferred_taxonomy_projection_memory(
            &[vec![0, 1]],
            &limited_model,
            &ProviderGraphV4Options::default(),
            &mut limited,
        )
        .unwrap_err();
        assert!(error.to_string().contains("resource_admission"));
    }

    #[test]
    fn inferred_taxonomy_rejects_contract_envelope_and_member_boundaries() {
        assert!(strict_u32le_members(&[0], 1).is_err());
        assert!(strict_u32le_members(&0u32.to_le_bytes(), 0).is_err());
        let duplicate_members = [0u32.to_le_bytes(), 0u32.to_le_bytes()].concat();
        assert!(strict_u32le_members(&duplicate_members, 1).is_err());

        let temporary = tempfile::tempdir().unwrap();
        let scope_sha256 = hex(&[9; 32]);
        let scope = ProviderGraphV4NpiScopeInput {
            format: NPI_SCOPE_FORMAT.to_owned(),
            row_count: 1,
            source_owner_count: 1,
            input_byte_count: 0,
            input_sha256: hex(&[0; 32]),
            output_byte_count: 0,
            output_sha256: scope_sha256.clone(),
            output_path: temporary.path().join("unused.copy"),
        };
        let input = one_rule_taxonomy_input(
            &temporary.path().join("taxonomy-members.u32le"),
            &0u32.to_le_bytes(),
            &scope_sha256,
        );

        let mut incompatible = input.clone();
        incompatible.contract = "other".to_owned();
        assert!(
            read_inferred_taxonomy_model(&incompatible, &scope, 1, &mut blank_admission(None),)
                .is_err()
        );

        let mut missing = input.clone();
        missing.members.path = temporary.path().join("missing.u32le");
        assert!(
            read_inferred_taxonomy_model(&missing, &scope, 1, &mut blank_admission(None))
                .unwrap_err()
                .to_string()
                .contains("unavailable")
        );

        let mut wrong_size = input.clone();
        wrong_size.members.byte_count += 4;
        wrong_size.rules[0].member_count += 1;
        wrong_size.rules[0].member_byte_count += 4;
        assert!(
            read_inferred_taxonomy_model(&wrong_size, &scope, 2, &mut blank_admission(None),)
                .unwrap_err()
                .to_string()
                .contains("byte count changed")
        );

        let mut noncontiguous = input.clone();
        noncontiguous.rules[0].member_offset_bytes = 4;
        assert!(validate_inferred_taxonomy_rule_envelope(&noncontiguous).is_err());

        let mut incomplete = input.clone();
        incomplete.rule_set_digest = hex(&[0; 32]);
        assert!(validate_inferred_taxonomy_rule_envelope(&incomplete).is_err());

        let mut overflowing = input.clone();
        overflowing.rules[0].member_count = u64::MAX;
        assert!(validate_inferred_taxonomy_rule_envelope(&overflowing).is_err());

        let model = V4InferredTaxonomyModel {
            rules: vec![V4InferredTaxonomyRule {
                rule_digest: [1; 32],
                catalog_digest: [2; 32],
                member_keys: vec![1],
            }],
        };
        assert!(reserve_inferred_taxonomy_projection_memory(
            &[vec![0]],
            &model,
            &ProviderGraphV4Options::default(),
            &mut blank_admission(None),
        )
        .unwrap_err()
        .to_string()
        .contains("exceeds pattern scope"));

        let options = ProviderGraphV4Options {
            max_online_inferred_taxonomy_candidates: usize::MAX,
            ..ProviderGraphV4Options::default()
        };
        assert!(reserve_inferred_taxonomy_projection_memory(
            &[vec![0]],
            &V4InferredTaxonomyModel {
                rules: vec![inferred_rule(1, 1)],
            },
            &options,
            &mut blank_admission(None),
        )
        .unwrap_err()
        .to_string()
        .contains("candidate cap overflows"));
    }

    #[test]
    fn manifest_compiler_consumes_authenticated_scope_and_taxonomy() {
        let fixture = shared_pattern_fixture(64, 16);
        let scope_summary = extract_provider_graph_v4_npi_scope(
            std::slice::from_ref(&fixture.shard),
            fixture._temporary.path().join("compiler-scope.copy"),
        )
        .unwrap();
        let taxonomy = one_rule_taxonomy_input(
            &fixture
                ._temporary
                .path()
                .join("compiler-taxonomy-members.u32le"),
            &0u32.to_le_bytes(),
            &scope_summary.output_sha256,
        );
        let summary = compile_provider_graph_v4_manifest(ProviderGraphV4Manifest {
            shards: vec![fixture.shard],
            provider_set_key_map_path: fixture.provider_map,
            npi_scope: scope_input(&scope_summary),
            inferred_taxonomy: taxonomy,
            output_directory: fixture.output,
            options: ProviderGraphV4Options {
                member_page_bytes: 64,
                locator_page_bytes: 48,
                heavy_owner_member_threshold: 8,
                heavy_bitmap_minimum_savings_bytes: 0,
                ..ProviderGraphV4Options::default()
            },
        })
        .unwrap();

        assert_eq!(summary.selected_layout, ProviderGraphV4Layout::Pattern);
        assert_eq!(summary.observe.pattern_count, 1);
        assert_eq!(
            summary
                .output_artifacts
                .iter()
                .find(|artifact| artifact.name == "inferred_taxonomy_candidates")
                .unwrap()
                .row_count,
            1,
        );
    }

    #[test]
    fn manifest_compiler_persists_direct_and_pattern_rejection_layouts() {
        let direct = independent_fixture();
        let direct_scope = extract_provider_graph_v4_npi_scope(
            std::slice::from_ref(&direct.shard),
            direct._temporary.path().join("direct-scope.copy"),
        )
        .unwrap();
        let direct_members = [0u32, 1u32]
            .into_iter()
            .flat_map(u32::to_le_bytes)
            .collect::<Vec<_>>();
        let direct_taxonomy = one_rule_taxonomy_input(
            &direct
                ._temporary
                .path()
                .join("direct-taxonomy-members.u32le"),
            &direct_members,
            &direct_scope.output_sha256,
        );
        let direct_summary = compile_provider_graph_v4_manifest(ProviderGraphV4Manifest {
            shards: vec![direct.shard],
            provider_set_key_map_path: direct.provider_map,
            npi_scope: scope_input(&direct_scope),
            inferred_taxonomy: direct_taxonomy,
            output_directory: direct.output,
            options: ProviderGraphV4Options {
                member_page_bytes: 64,
                locator_page_bytes: 48,
                heavy_owner_member_threshold: 1,
                heavy_bitmap_minimum_savings_bytes: 0,
                ..ProviderGraphV4Options::default()
            },
        })
        .unwrap();
        assert_eq!(
            direct_summary.selected_layout,
            ProviderGraphV4Layout::Direct
        );
        assert!(direct_summary.pattern_copy_path.is_none());
        assert!(direct_summary.direct_inferred_taxonomy_eligible);
        assert!(direct_summary.pattern_inferred_taxonomy_eligible);
        assert_eq!(
            direct_summary
                .output_artifacts
                .iter()
                .find(|artifact| artifact.name == "inferred_taxonomy_candidates")
                .unwrap()
                .row_count,
            1,
        );
        assert!(reference_kinds(&direct_summary.reference_manifest_path)
            .iter()
            .any(|kind| kind.contains("sets_direct")));

        let rejected = mixed_pattern_component_fixture(64);
        let rejected_scope = extract_provider_graph_v4_npi_scope(
            std::slice::from_ref(&rejected.shard),
            rejected._temporary.path().join("rejected-scope.copy"),
        )
        .unwrap();
        let rejected_taxonomy = one_rule_taxonomy_input(
            &rejected
                ._temporary
                .path()
                .join("rejected-taxonomy-members.u32le"),
            &0u32.to_le_bytes(),
            &rejected_scope.output_sha256,
        );
        let rejected_summary = compile_provider_graph_v4_manifest(ProviderGraphV4Manifest {
            shards: vec![rejected.shard],
            provider_set_key_map_path: rejected.provider_map,
            npi_scope: scope_input(&rejected_scope),
            inferred_taxonomy: rejected_taxonomy,
            output_directory: rejected.output,
            options: ProviderGraphV4Options {
                member_page_bytes: 64,
                locator_page_bytes: 48,
                heavy_owner_member_threshold: 8,
                heavy_bitmap_minimum_savings_bytes: 0,
                max_online_candidate_pattern_projection_members: 1,
                ..ProviderGraphV4Options::default()
            },
        })
        .unwrap();
        assert_eq!(
            rejected_summary.selected_layout,
            ProviderGraphV4Layout::Direct
        );
        assert!(rejected_summary.direct_inferred_taxonomy_eligible);
        assert!(!rejected_summary.pattern_inferred_taxonomy_eligible);
        assert_eq!(
            rejected_summary
                .pattern_inferred_taxonomy_rejection_reason
                .as_deref(),
            Some(INFERRED_TAXONOMY_PATTERN_CAP_REASON),
        );
        assert!(rejected_summary
            .pattern_inferred_taxonomy_rejection_rule_digest
            .is_some());
        assert_eq!(
            rejected_summary.pattern_inferred_taxonomy_rejection_cap,
            Some(1),
        );
        assert!(rejected_summary
            .output_artifacts
            .iter()
            .all(|artifact| artifact.byte_count > 0));

        let helper_temporary = tempfile::tempdir().unwrap();
        let range_input = helper_temporary.path().join("plain-range.json");
        fs::write(&range_input, b"abcdef").unwrap();
        let range_bytes = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut range_reader =
            crate::input::open_plain_range_json_reader(&range_input, 1, 3, range_bytes).unwrap();
        let mut range_text = String::new();
        range_reader.read_to_string(&mut range_text).unwrap();
        assert_eq!(range_text, "bcd");

        let full_scan_bytes = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut full_scan_reader = crate::input::open_full_scan_reader(
            &range_input,
            full_scan_bytes,
            &crate::input::RapidgzipConfig::default(),
        )
        .unwrap();
        let mut full_scan_text = String::new();
        full_scan_reader
            .read_to_string(&mut full_scan_text)
            .unwrap();
        assert_eq!(full_scan_text, "abcdef");

        let address_input = helper_temporary.path().join("addresses.copy");
        let address_output = helper_temporary.path().join("addresses.normalized.copy");
        fs::write(&address_input, b"").unwrap();
        crate::address_canon::canonicalize_copy_file(&address_input, &address_output).unwrap();
        assert!(fs::read(address_output).unwrap().is_empty());

        assert_eq!(
            crate::config::progress_interval("HLTHPRT_PTG2_TEST_COVERAGE_UNSET", 7),
            7,
        );
        assert_ne!(
            crate::manifest::procedure_global_id(&serde_json::json!({"billing_code": "70553"})).0,
            [0; 16],
        );
        crate::progress::emit_progress(
            &range_input,
            6,
            &std::sync::Arc::new(std::sync::atomic::AtomicU64::new(6)),
            &HashMap::from([("in_network".to_owned(), 1)]),
            std::time::Instant::now(),
            true,
        );
    }

    #[test]
    fn compact_candidate_shape_keeps_five_selected_and_five_observe_rules() {
        let retained_counts = [17_219, 25_126, 19_419, 32_148, 5_162];
        let mut rules = retained_counts
            .into_iter()
            .enumerate()
            .map(|(index, count)| inferred_rule(index as u8 + 1, count))
            .collect::<Vec<_>>();
        rules.extend((0..5).map(|index| inferred_rule(index as u8 + 6, 37_001)));
        let input = V4InferredTaxonomyModel { rules };
        let npi_patterns = vec![vec![0]; 37_001];
        let options = ProviderGraphV4Options::default();

        let direct = inferred_taxonomy_projection(
            &npi_patterns,
            &input,
            ProviderGraphV4Layout::Direct,
            &options,
        )
        .unwrap();
        let pattern = inferred_taxonomy_projection(
            &npi_patterns,
            &input,
            ProviderGraphV4Layout::Pattern,
            &options,
        )
        .unwrap();

        for projection in [&direct, &pattern] {
            assert!(projection.eligible);
            assert!(projection.rejection.is_none());
            assert_eq!(projection.rows.len(), 10);
            assert_eq!(
                projection
                    .rows
                    .iter()
                    .filter(|row| row.observe_reason.is_none())
                    .count(),
                5,
            );
            assert_eq!(
                projection
                    .rows
                    .iter()
                    .filter(|row| {
                        row.representation == INFERRED_TAXONOMY_OBSERVE_REPRESENTATION
                            && row.observe_reason == Some(INFERRED_TAXONOMY_CANDIDATE_CAP_REASON)
                            && row.observe_count_lower_bound == Some(37_001)
                    })
                    .count(),
                5,
            );
            assert_eq!(
                projection
                    .rows
                    .iter()
                    .map(|row| row.member_keys.len() / 4)
                    .sum::<usize>(),
                284_079,
            );
        }
    }

    #[test]
    fn pattern_candidate_cap_rejects_only_pattern_with_exact_witness() {
        let input = V4InferredTaxonomyModel {
            rules: vec![inferred_rule(7, 2)],
        };
        let npi_patterns = vec![vec![0, 1], vec![0, 1]];
        let options = ProviderGraphV4Options {
            max_online_candidate_pattern_projection_members: 3,
            ..ProviderGraphV4Options::default()
        };

        let direct = inferred_taxonomy_projection(
            &npi_patterns,
            &input,
            ProviderGraphV4Layout::Direct,
            &options,
        )
        .unwrap();
        let pattern = inferred_taxonomy_projection(
            &npi_patterns,
            &input,
            ProviderGraphV4Layout::Pattern,
            &options,
        )
        .unwrap();

        assert!(direct.eligible);
        assert!(direct.rejection.is_none());
        assert!(!pattern.eligible);
        let rejection = pattern.rejection.unwrap();
        assert_eq!(rejection.reason, INFERRED_TAXONOMY_PATTERN_CAP_REASON);
        assert_eq!(rejection.rule_digest, [7; 32]);
        assert_eq!(rejection.observed_count, 4);
        assert_eq!(rejection.cap, 3);
    }

    #[test]
    fn pattern_candidate_cap_stops_at_first_sorted_rule_breach() {
        let input = V4InferredTaxonomyModel {
            rules: vec![
                V4InferredTaxonomyRule {
                    rule_digest: [1; 32],
                    catalog_digest: [11; 32],
                    member_keys: vec![0],
                },
                V4InferredTaxonomyRule {
                    rule_digest: [2; 32],
                    catalog_digest: [12; 32],
                    member_keys: vec![1, 2],
                },
                V4InferredTaxonomyRule {
                    rule_digest: [3; 32],
                    catalog_digest: [13; 32],
                    member_keys: vec![2, 3],
                },
            ],
        };
        let npi_patterns = vec![vec![0], vec![0, 1], vec![0, 1], vec![0, 1]];
        let options = ProviderGraphV4Options {
            max_online_candidate_pattern_projection_members: 3,
            ..ProviderGraphV4Options::default()
        };

        let direct = inferred_taxonomy_projection(
            &npi_patterns,
            &input,
            ProviderGraphV4Layout::Direct,
            &options,
        )
        .unwrap();
        assert!(direct.eligible);
        assert_eq!(direct.rows.len(), input.rules.len());
        assert!(direct.encoded_bytes > 0);

        for _ in 0..3 {
            let pattern = inferred_taxonomy_projection(
                &npi_patterns,
                &input,
                ProviderGraphV4Layout::Pattern,
                &options,
            )
            .unwrap();
            assert!(!pattern.eligible);
            assert!(pattern.rows.is_empty());
            assert_eq!(pattern.encoded_bytes, 0);
            let rejection = pattern.rejection.unwrap();
            assert_eq!(rejection.reason, INFERRED_TAXONOMY_PATTERN_CAP_REASON);
            assert_eq!(rejection.rule_digest, [2; 32]);
            assert_eq!(rejection.observed_count, 4);
            assert_eq!(rejection.cap, 3);
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
        let provider_npi_scope = write_npi_scope(
            &temporary.path().join("npi-scope.copy"),
            "shard-mixed",
            &npi_group,
        );
        let provider_map = temporary.path().join("provider-map.copy");
        write_provider_map(&provider_map, &sets, 1);
        let output = temporary.path().join("output");
        let provider_group_tax_identity = write_missing_tax_identity(
            &temporary.path().join("group-tax-identity.sidecar"),
            "shard-mixed",
            groups,
        );
        Fixture {
            _temporary: temporary,
            shard: V4ProviderGraphShardDescriptor {
                shard_id: "shard-mixed".to_owned(),
                provider_set_component: set_component,
                provider_component_group: component_group,
                provider_group_npi: group_npi,
                provider_npi_group: npi_group,
                provider_npi_scope,
                provider_group_tax_identity,
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
            true,
        );
        let provider_npi_scope = write_npi_scope(
            &temporary.path().join("npi-scope.copy"),
            "shard-b",
            &npi_group,
        );
        let provider_map = temporary.path().join("provider-map.copy");
        write_provider_map(&provider_map, &sets, 0);
        let output = temporary.path().join("output");
        let provider_group_tax_identity = write_missing_tax_identity(
            &temporary.path().join("group-tax-identity.sidecar"),
            "shard-b",
            groups,
        );
        Fixture {
            _temporary: temporary,
            shard: V4ProviderGraphShardDescriptor {
                shard_id: "shard-b".to_owned(),
                provider_set_component: set_component,
                provider_component_group: component_group,
                provider_group_npi: group_npi,
                provider_npi_group: npi_group,
                provider_npi_scope,
                provider_group_tax_identity,
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
        let provider_npi_group = write_membership(
            &temporary.path().join("npi-group.sidecar"),
            "tuple-cache",
            "provider_npi_group",
            groups.iter().copied().map(|group| (provider_npi, group)),
            true,
        );
        let provider_npi_scope = write_npi_scope(
            &temporary.path().join("npi-scope.copy"),
            "tuple-cache",
            &provider_npi_group,
        );
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
            provider_npi_group,
            provider_npi_scope,
            provider_group_tax_identity: write_missing_tax_identity(
                &temporary.path().join("group-tax-identity.sidecar"),
                "tuple-cache",
                groups.iter().copied(),
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
    fn early_complete_prefix_is_safe_despite_total_group_degree() {
        let fixture = independent_fixture();
        let options = ProviderGraphV4Options::default();
        let mut admission = resource_admission_preflight(
            std::slice::from_ref(&fixture.shard),
            &fixture.provider_map,
            &options,
        )
        .unwrap();
        let groups = (0..4_097u32).collect::<Vec<_>>();
        let group_npis = groups
            .iter()
            .map(|group| vec![group * 2, group * 2 + 1])
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

        assert_eq!(plan.groups_to_target, vec![101]);
        assert_eq!(plan.group_unsafe_set_count, 0);
        assert_eq!(plan.physical_unsafe_set_count, 0);
        assert!(plan.metadata.is_empty());
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
        let owner_count = 50_001usize;
        let owner_factors = vec![vec![0]; owner_count];
        let plan = derive_npi_prefix_overrides(
            NpiPrefixInputs {
                set_base: 0,
                set_components: &owner_factors,
                component_groups: std::slice::from_ref(&groups),
                set_patterns: &owner_factors,
                pattern_groups: std::slice::from_ref(&groups),
                group_npis: &group_npis,
            },
            &options,
            &mut admission,
        )
        .unwrap();
        assert_eq!(plan.group_unsafe_set_count, 0);
        assert_eq!(plan.physical_unsafe_set_count, owner_count as u64);
        assert!(plan.groups_to_target.iter().all(|count| *count == 3));
        assert_eq!(plan.lists[0], vec![0, 1, 2]);
        assert_eq!(plan.metadata.len(), owner_count);
        assert!(plan.maximum_source_member_work > 1);
        assert_eq!(
            pages_for_owner_prefixes(&[0, 1], &group_npis, &[0, 1, 2], 1, 1),
            2
        );
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
        assert_eq!(summary.resource_admission.factor_edge_count, 272);
        assert_eq!(summary.resource_admission.factor_owner_count, 146);
        assert!(summary.resource_admission.tax_identity_projection_bytes > 0);
        assert!(summary.resource_admission.estimated_peak_bytes > 0);
        assert_eq!(
            summary.resource_admission.estimated_peak_bytes,
            summary
                .resource_admission
                .base_estimated_model_bytes
                .checked_add(summary.resource_admission.derived_projection_bytes)
                .and_then(|value| {
                    value.checked_add(summary.resource_admission.tax_identity_projection_bytes)
                })
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
        assert_eq!(
            summary
                .output_artifacts
                .iter()
                .find(|artifact| artifact.name == "provider_set_npi_prefix_overrides")
                .unwrap()
                .row_count,
            0,
            "pattern representation must retain only sparse unsafe-owner prefixes",
        );
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
        assert!(summary.direct_layout_complete_prefix_eligible);
        assert!(summary.direct_complete_encoded_bytes < summary.pattern_complete_encoded_bytes);
        assert!(summary.pattern_copy_path.is_none());
        assert_eq!(summary.observe.provider_set_count, 2);
        assert_eq!(summary.observe.npi_prefix_override_owner_count, 2);
        assert_eq!(summary.observe.npi_prefix_override_member_count, 2);
        assert!(summary.observe.npi_prefix_worst_provider_set_uses_override);
        assert_eq!(
            summary.observe.npi_prefix_worst_online_provider_set_key,
            None
        );
        assert_eq!(
            summary
                .output_artifacts
                .iter()
                .find(|artifact| artifact.name == "provider_set_npi_prefix_overrides")
                .unwrap()
                .row_count,
            2,
            "direct representation must authenticate a complete prefix for every set",
        );
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
    fn direct_planning_and_emission_deduplicate_overlapping_components() {
        let temporary = tempfile::tempdir().unwrap();
        let provider_set = global(1, 1);
        let components = [global(2, 1), global(2, 2)];
        let groups = [global(3, 1), global(3, 2), global(3, 3)];
        let provider_npi = npi(1_234_567_890);
        let provider_npi_group = write_membership(
            &temporary.path().join("npi-group.sidecar"),
            "overlap",
            "provider_npi_group",
            groups.into_iter().map(|group| (provider_npi, group)),
            true,
        );
        let provider_npi_scope = write_npi_scope(
            &temporary.path().join("npi-scope.copy"),
            "overlap",
            &provider_npi_group,
        );
        let shard = V4ProviderGraphShardDescriptor {
            shard_id: "overlap".to_owned(),
            provider_set_component: write_membership(
                &temporary.path().join("set-component.sidecar"),
                "overlap",
                "provider_set_component",
                [(provider_set, components[0]), (provider_set, components[1])],
                true,
            ),
            provider_component_group: write_membership(
                &temporary.path().join("component-group.sidecar"),
                "overlap",
                "provider_component_group",
                [
                    (components[0], groups[0]),
                    (components[0], groups[1]),
                    (components[1], groups[1]),
                    (components[1], groups[2]),
                ],
                true,
            ),
            provider_group_npi: write_membership(
                &temporary.path().join("group-npi.sidecar"),
                "overlap",
                "provider_group_npi",
                groups.into_iter().map(|group| (group, provider_npi)),
                true,
            ),
            provider_npi_group,
            provider_npi_scope,
            provider_group_tax_identity: write_missing_tax_identity(
                &temporary.path().join("group-tax-identity.sidecar"),
                "overlap",
                groups,
            ),
        };
        let provider_map = temporary.path().join("provider-map.copy");
        write_provider_map(&provider_map, &[provider_set], 0);
        let summary = compile_provider_graph_v4(
            std::slice::from_ref(&shard),
            &provider_map,
            temporary.path().join("output"),
            ProviderGraphV4Options::default(),
        )
        .unwrap();

        assert_eq!(summary.selected_layout, ProviderGraphV4Layout::Direct);
        for relation in ["group_sets_direct", "set_groups_direct"] {
            let geometry = summary
                .relation_summaries
                .iter()
                .find(|candidate| candidate.relation == relation)
                .unwrap();
            assert_eq!(
                geometry.logical_member_count, 3,
                "{relation} retained a duplicate component incidence"
            );
        }
        assert_eq!(summary.observe.direct_group_set_emission_edge_visits, 3);
        assert_eq!(summary.observe.set_group_expansion_edge_visits, 3);
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
    fn bitmap_planned_bytes_can_reverse_the_raw_vector_choice() {
        let options = ProviderGraphV4Options {
            heavy_owner_member_threshold: 1,
            heavy_bitmap_minimum_savings_bytes: 0,
            ..ProviderGraphV4Options::default()
        };
        let dense_direct = vec![(0..5_000).collect::<Vec<u32>>()];
        let sparse_pattern = vec![(0..1_200)
            .map(|value| value * 100_000)
            .collect::<Vec<u32>>()];
        let direct_raw = relation_encoded_bytes(
            &RelationShape {
                relation: "group_sets_direct",
                owner_count: 1,
                member_count: 5_000,
            },
            &options,
        )
        .unwrap();
        let pattern_raw = relation_encoded_bytes(
            &RelationShape {
                relation: "pattern_sets",
                owner_count: 1,
                member_count: 1_200,
            },
            &options,
        )
        .unwrap();
        let direct_planned =
            planned_list_relation_persistence("group_sets_direct", 0, &dense_direct, &options)
                .unwrap()
                .graph_encoded_bytes;
        let pattern_planned =
            planned_list_relation_persistence("pattern_sets", 0, &sparse_pattern, &options)
                .unwrap()
                .graph_encoded_bytes;

        assert!(pattern_raw < direct_raw);
        assert!(direct_planned < pattern_planned);
        assert_eq!(
            choose_layout(direct_planned, pattern_planned, true),
            ProviderGraphV4Layout::Direct,
        );
    }

    #[test]
    fn packed_map_payload_can_reverse_a_graph_only_choice() {
        let direct_coordinates = (0..4)
            .map(|index| (format!("direct-kind-{index}"), 1))
            .collect::<BTreeMap<_, _>>();
        let pattern_coordinates = (0..10)
            .map(|index| (format!("pattern-kind-{index}"), 1))
            .collect::<BTreeMap<_, _>>();
        let direct_mapping =
            mapping_persistence_bytes(direct_coordinates, 0, 0, "direct_v1").unwrap();
        let pattern_mapping =
            mapping_persistence_bytes(pattern_coordinates, 0, 0, "pattern_v1").unwrap();

        assert_eq!(direct_mapping.map_payload_encoded_bytes, 4 * (80 + 52));
        assert_eq!(pattern_mapping.map_payload_encoded_bytes, 10 * (80 + 52));
        assert_eq!(1_001 + direct_mapping.map_payload_encoded_bytes, 1_529);
        assert_eq!(1_000 + pattern_mapping.map_payload_encoded_bytes, 2_320);
        assert_eq!(
            choose_layout(
                1_001 + direct_mapping.total_encoded_bytes,
                1_000 + pattern_mapping.total_encoded_bytes,
                true,
            ),
            ProviderGraphV4Layout::Direct,
            "the extra persistent coordinate-map kinds must reverse the graph-only choice",
        );
    }

    #[test]
    fn complete_direct_prefix_admission_has_no_sparse_owner_cap() {
        let options = ProviderGraphV4Options::default();
        let owner_count = options
            .max_npi_prefix_override_owners
            .checked_add(1)
            .unwrap();
        let relation_bytes = relation_encoded_bytes(
            &RelationShape {
                relation: "set_npi_prefix_override",
                owner_count,
                member_count: 0,
            },
            &options,
        )
        .unwrap();
        let projection_bytes = relation_bytes
            .checked_add(dictionary_copy_bytes(&[4, 4, 32], owner_count).unwrap())
            .unwrap();

        assert_eq!(owner_count, 250_001);
        assert!(complete_prefix_projection_eligible(
            projection_bytes,
            &options
        ));
    }

    #[test]
    fn compiler_keeps_pattern_layout_when_only_overflow_sets_use_components() {
        let safe_fixture = mixed_pattern_component_fixture(1_024);
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

        let unsafe_fixture = mixed_pattern_component_fixture(1_024);
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
    fn pattern_over_cap_component_owner_is_safe_only_with_exact_prefix() {
        let fixture = mixed_pattern_component_fixture(1_024);
        let summary = compile_provider_graph_v4(
            std::slice::from_ref(&fixture.shard),
            &fixture.provider_map,
            &fixture.output,
            ProviderGraphV4Options {
                max_set_patterns_per_set: 1,
                max_set_components_per_fallback_set: 1,
                max_online_source_owners_per_set: 1,
                ..ProviderGraphV4Options::default()
            },
        )
        .unwrap();

        assert_eq!(summary.selected_layout, ProviderGraphV4Layout::Pattern);
        assert!(summary.pattern_layout_serving_degree_eligible);
        assert_eq!(summary.observe.pattern_component_over_cap_set_count, 1);
        assert_eq!(
            summary
                .observe
                .pattern_component_over_cap_prefix_covered_set_count,
            1,
        );
        assert_eq!(summary.observe.unsafe_pattern_component_set_count, 0);
        assert!(
            summary.observe.npi_prefix_override_owner_count
                >= summary
                    .observe
                    .pattern_component_over_cap_prefix_covered_set_count,
        );
    }

    #[test]
    fn compiler_falls_back_to_pattern_when_complete_direct_prefix_exceeds_cap() {
        let fixture = independent_fixture();
        let summary = compile_provider_graph_v4(
            std::slice::from_ref(&fixture.shard),
            &fixture.provider_map,
            &fixture.output,
            ProviderGraphV4Options {
                max_npi_prefix_override_bytes: 1,
                ..ProviderGraphV4Options::default()
            },
        )
        .unwrap();

        assert_eq!(summary.selected_layout, ProviderGraphV4Layout::Pattern);
        assert!(!summary.direct_layout_complete_prefix_eligible);
        assert!(summary.pattern_layout_serving_degree_eligible);
        assert_eq!(summary.observe.npi_prefix_override_owner_count, 0);
    }

    #[test]
    fn compiler_rejects_graph_when_neither_representation_is_bounded() {
        let fixture = mixed_pattern_component_fixture(128);
        let result = compile_provider_graph_v4(
            std::slice::from_ref(&fixture.shard),
            &fixture.provider_map,
            &fixture.output,
            ProviderGraphV4Options {
                max_set_patterns_per_set: 1,
                max_set_components_per_fallback_set: 1,
                max_npi_prefix_override_bytes: 1,
                ..ProviderGraphV4Options::default()
            },
        );

        assert!(matches!(
            result,
            Err(ProviderGraphV4Error::InvalidData(message))
                if message.contains("no bounded complete online representation")
        ));
    }

    #[test]
    fn automatic_layout_selection_uses_shape_not_source_identity() {
        let first = shared_pattern_fixture_with_shard_id(64, 16, "source-a");
        let second = shared_pattern_fixture_with_shard_id(64, 16, "renamed-source-b-long");
        let first_summary = compile_provider_graph_v4(
            std::slice::from_ref(&first.shard),
            &first.provider_map,
            &first.output,
            ProviderGraphV4Options::default(),
        )
        .unwrap();
        let second_summary = compile_provider_graph_v4(
            std::slice::from_ref(&second.shard),
            &second.provider_map,
            &second.output,
            ProviderGraphV4Options::default(),
        )
        .unwrap();

        assert_eq!(
            first_summary.selected_layout,
            second_summary.selected_layout
        );
        assert_eq!(
            first_summary.direct_complete_encoded_bytes,
            second_summary.direct_complete_encoded_bytes,
        );
        assert_eq!(
            first_summary.pattern_complete_encoded_bytes,
            second_summary.pattern_complete_encoded_bytes,
        );
        assert_eq!(
            first_summary.pattern_layout_serving_degree_eligible,
            second_summary.pattern_layout_serving_degree_eligible,
        );
        assert_eq!(
            first_summary.direct_layout_complete_prefix_eligible,
            second_summary.direct_layout_complete_prefix_eligible,
        );
        assert_eq!(
            first_summary.relation_summaries,
            second_summary.relation_summaries,
        );
        assert_ne!(
            first_summary.tax_identity.source_ordinal_map_digest,
            second_summary.tax_identity.source_ordinal_map_digest,
            "authenticated source provenance must retain the renamed shard",
        );

        let changed_shape = independent_fixture();
        let changed_summary = compile_provider_graph_v4(
            std::slice::from_ref(&changed_shape.shard),
            &changed_shape.provider_map,
            &changed_shape.output,
            ProviderGraphV4Options::default(),
        )
        .unwrap();
        assert_ne!(
            first_summary.selected_layout, changed_summary.selected_layout,
            "different measured graph geometry may select another representation",
        );
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
        assert_eq!(
            summary.selected_encoded_bytes,
            match summary.selected_layout {
                ProviderGraphV4Layout::Direct => summary.direct_complete_encoded_bytes,
                ProviderGraphV4Layout::Pattern => summary.pattern_complete_encoded_bytes,
            }
        );
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
    fn compiler_output_race_preserves_foreign_path_and_removes_owned_partial_outputs() {
        let fixture = shared_pattern_fixture(16, 4);
        let raced_path = fixture.output.join("v4-provider-groups.copy");
        let mut injected = false;
        let mut sink = |event: &V4ProgressEvent| {
            if !injected && event.phase == "emit_bitmaps" {
                fs::write(&raced_path, b"caller-owned race").unwrap();
                injected = true;
            }
        };

        let error = compile_provider_graph_v4_with_progress(
            std::slice::from_ref(&fixture.shard),
            &fixture.provider_map,
            &fixture.output,
            ProviderGraphV4Options::default(),
            None,
            None,
            &mut sink,
        )
        .unwrap_err();
        assert!(injected);
        assert!(matches!(
            error,
            ProviderGraphV4Error::Io(ref source)
                if source.kind() == io::ErrorKind::AlreadyExists
        ));
        assert_eq!(fs::read(&raced_path).unwrap(), b"caller-owned race");
        for name in OUTPUT_NAMES {
            if name != "v4-provider-groups.copy" {
                assert!(
                    fs::symlink_metadata(fixture.output.join(name)).is_err(),
                    "run-owned partial output survived: {name}",
                );
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn compiler_output_rejects_dangling_symlink_without_following_or_removing_it() {
        use std::os::unix::fs::symlink;

        let fixture = independent_fixture();
        fs::create_dir_all(&fixture.output).unwrap();
        let target = fixture._temporary.path().join("caller-target.copy");
        let link = fixture.output.join("v4-summary.json");
        symlink(&target, &link).unwrap();

        let error = compile_provider_graph_v4(
            std::slice::from_ref(&fixture.shard),
            &fixture.provider_map,
            &fixture.output,
            ProviderGraphV4Options::default(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("already exists"));
        assert!(fs::symlink_metadata(&link)
            .unwrap()
            .file_type()
            .is_symlink());
        assert!(!target.exists());
        for name in OUTPUT_NAMES {
            if name != "v4-summary.json" {
                assert!(fs::symlink_metadata(fixture.output.join(name)).is_err());
            }
        }
    }

    #[test]
    fn output_ownership_rejects_root_escape_and_preserves_raced_file() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("output");
        fs::create_dir(&root).unwrap();
        let mut ownership = OutputOwnership::new(&root);
        let raced = root.join("v4-summary.json");
        fs::write(&raced, b"caller-owned").unwrap();
        assert!(ownership.create(&raced).is_err());
        let replaced = root.join("v4-patterns.copy");
        drop(ownership.create(&replaced).unwrap());
        fs::remove_file(&replaced).unwrap();
        fs::write(&replaced, b"replacement after create").unwrap();
        let escaped = temporary.path().join("v4-summary.json");
        assert!(ownership.create(&escaped).is_err());
        drop(ownership);
        assert_eq!(fs::read(&raced).unwrap(), b"caller-owned");
        assert_eq!(fs::read(&replaced).unwrap(), b"replacement after create");
        assert!(!escaped.exists());
    }

    #[cfg(unix)]
    #[test]
    fn output_ownership_pins_original_inode_until_cleanup() {
        let temporary = tempfile::tempdir().unwrap();
        for attempt in 0..64 {
            let root = temporary.path().join(format!("output-{attempt}"));
            fs::create_dir(&root).unwrap();
            let path = root.join("v4-patterns.copy");
            let mut ownership = OutputOwnership::new(&root);
            drop(ownership.create(&path).unwrap());
            fs::remove_file(&path).unwrap();
            let replacement = format!("caller replacement {attempt}");
            fs::write(&path, replacement.as_bytes()).unwrap();

            drop(ownership);

            assert_eq!(fs::read(&path).unwrap(), replacement.as_bytes());
        }
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
                None,
                None,
                &mut sink,
            )
            .unwrap();
        }
        assert!(!events.is_empty());
        assert!(events.iter().all(|event| event.done <= event.total));
        assert!(events.windows(2).all(|pair| pair[1].seq == pair[0].seq + 1));
        for (phase, unit) in [
            ("load_factors", "factor_items"),
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
        let provider_npi_group = write_membership(
            &temporary.path().join("npi-group.sidecar"),
            "multi-shard",
            "provider_npi_group",
            groups.into_iter().map(|group| (provider_npi, group)),
            true,
        );
        let provider_npi_scope = write_npi_scope(
            &temporary.path().join("npi-scope.copy"),
            "multi-shard",
            &provider_npi_group,
        );
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
            provider_npi_group,
            provider_npi_scope,
            provider_group_tax_identity: write_missing_tax_identity(
                &temporary.path().join("group-tax-identity.sidecar"),
                "multi-shard",
                groups,
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

        let admitted = resource_admission_preflight(
            std::slice::from_ref(&fixture.shard),
            &fixture.provider_map,
            &ProviderGraphV4Options::default(),
        )
        .unwrap();
        let membership_edges = [
            &fixture.shard.provider_set_component,
            &fixture.shard.provider_component_group,
            &fixture.shard.provider_group_npi,
            &fixture.shard.provider_npi_group,
        ]
        .iter()
        .map(|artifact| artifact.metadata.member_count)
        .sum::<u64>();
        let membership_owners = [
            &fixture.shard.provider_set_component,
            &fixture.shard.provider_component_group,
            &fixture.shard.provider_group_npi,
            &fixture.shard.provider_npi_group,
        ]
        .iter()
        .map(|artifact| artifact.metadata.owner_count)
        .sum::<u64>();
        assert_eq!(
            admitted.summary.factor_edge_count,
            membership_edges + fixture.shard.provider_group_tax_identity.metadata.row_count
        );
        assert_eq!(
            admitted.summary.factor_owner_count,
            membership_owners
                + fixture
                    .shard
                    .provider_group_tax_identity
                    .metadata
                    .provider_group_count
        );
        assert_eq!(
            admitted.summary.tax_identity_merge_bitmap_upper_bound_bytes,
            fixture
                .shard
                .provider_group_tax_identity
                .metadata
                .provider_group_count
        );
        assert_eq!(
            admitted
                .summary
                .tax_identity_source_ordinal_upper_bound_bytes,
            TAX_SOURCE_ORDINAL_FIXED_UPPER_BOUND_BYTES
                + (fixture.shard.shard_id.len() as u64) * TAX_SOURCE_IDENTITY_COPY_UPPER_BOUND
        );

        let mut high_shard_factors = Vec::new();
        for ordinal in 0..9 {
            let mut shard = fixture.shard.clone();
            shard.shard_id = format!("disjoint-{ordinal}");
            shard
                .provider_group_tax_identity
                .metadata
                .provider_group_count = 10_000;
            shard.provider_group_tax_identity.metadata.row_count = 10_000;
            shard.provider_group_tax_identity.path =
                temporary_missing_path(fixture._temporary.path());
            high_shard_factors.push(shard);
        }
        let high_shard_admission = resource_admission_preflight(
            &high_shard_factors,
            &fixture.provider_map,
            &ProviderGraphV4Options::default(),
        )
        .unwrap();
        assert_eq!(
            high_shard_admission
                .summary
                .tax_identity_merge_bitmap_upper_bound_bytes,
            180_000
        );
        let pre_tax_limit = high_shard_admission
            .summary
            .estimated_peak_bytes
            .checked_sub(
                high_shard_admission
                    .summary
                    .tax_identity_merge_bitmap_upper_bound_bytes,
            )
            .and_then(|value| {
                value.checked_sub(
                    high_shard_admission
                        .summary
                        .tax_identity_source_ordinal_upper_bound_bytes,
                )
            })
            .unwrap();
        let adversarial_output = fixture._temporary.path().join("adversarial-output");
        let failure = compile_provider_graph_v4(
            &high_shard_factors,
            &fixture.provider_map,
            &adversarial_output,
            ProviderGraphV4Options {
                max_estimated_model_bytes: Some(pre_tax_limit),
                ..ProviderGraphV4Options::default()
            },
        )
        .unwrap_err();
        assert!(failure.to_string().contains("resource_admission"));
        assert!(fs::read_dir(adversarial_output).unwrap().next().is_none());
        let pre_projection_limit = high_shard_admission
            .summary
            .estimated_peak_bytes
            .checked_sub(
                high_shard_admission
                    .summary
                    .tax_identity_projection_upper_bound_bytes,
            )
            .unwrap();
        let projection_output = fixture._temporary.path().join("projection-output");
        let failure = compile_provider_graph_v4(
            &high_shard_factors,
            &fixture.provider_map,
            &projection_output,
            ProviderGraphV4Options {
                max_estimated_model_bytes: Some(pre_projection_limit),
                ..ProviderGraphV4Options::default()
            },
        )
        .unwrap_err();
        assert!(failure.to_string().contains("resource_admission"));
        assert!(fs::read_dir(projection_output).unwrap().next().is_none());

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

        let mut tax_edge_overflow = fixture.shard.clone();
        tax_edge_overflow
            .provider_group_tax_identity
            .metadata
            .row_count = u64::MAX;
        assert!(resource_admission_preflight(
            &[tax_edge_overflow],
            &fixture.provider_map,
            &ProviderGraphV4Options::default(),
        )
        .is_err());
        let mut tax_owner_overflow = fixture.shard.clone();
        tax_owner_overflow
            .provider_group_tax_identity
            .metadata
            .provider_group_count = u64::MAX;
        assert!(resource_admission_preflight(
            &[tax_owner_overflow],
            &fixture.provider_map,
            &ProviderGraphV4Options::default(),
        )
        .is_err());
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
        assert!(!unfinished.exists());
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
            encoded_byte_count: 0,
            block_count: 1,
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
                tax_identity_merge_bitmap_upper_bound_bytes: 0,
                tax_identity_source_ordinal_upper_bound_bytes: 0,
                tax_identity_projection_upper_bound_bytes: 0,
                base_estimated_model_bytes: 0,
                derived_projection_bytes: 0,
                tax_identity_projection_bytes: 0,
                retained_scratch_high_water_bytes: 0,
                bounded_emission_buffer_bytes: 0,
                estimated_peak_bytes: 0,
                max_estimated_model_bytes,
                max_factor_edges: None,
            },
            tax_identity_projection_reconciled: false,
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

        let mut admission = blank_admission(None);
        let sparse_capped = derive_npi_prefix_overrides(
            NpiPrefixInputs {
                set_base: 0,
                set_components: &set_components,
                component_groups: &component_groups,
                set_patterns: &set_patterns,
                pattern_groups: &pattern_groups,
                group_npis: &group_npis,
            },
            &ProviderGraphV4Options {
                max_online_group_keys_per_set: 0,
                max_npi_prefix_override_owners: 0,
                ..ProviderGraphV4Options::default()
            },
            &mut admission,
        )
        .unwrap();
        assert!(!sparse_capped.sparse_eligible);
        assert!(sparse_capped.complete_eligible);

        let mut admission = blank_admission(None);
        let complete_capped = derive_npi_prefix_overrides(
            NpiPrefixInputs {
                set_base: 0,
                set_components: &set_components,
                component_groups: &component_groups,
                set_patterns: &set_patterns,
                pattern_groups: &pattern_groups,
                group_npis: &group_npis,
            },
            &ProviderGraphV4Options {
                max_online_group_keys_per_set: 0,
                max_npi_prefix_override_bytes: 1,
                ..ProviderGraphV4Options::default()
            },
            &mut admission,
        )
        .unwrap();
        assert!(!complete_capped.sparse_eligible);
        assert!(!complete_capped.complete_eligible);

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

        let fixture = mixed_pattern_component_fixture(128);
        let options = ProviderGraphV4Options {
            member_page_bytes: 64,
            heavy_owner_member_threshold: 1,
            heavy_bitmap_minimum_savings_bytes: 0,
            max_set_patterns_per_set: 1,
            max_set_components_per_fallback_set: 1,
            ..ProviderGraphV4Options::default()
        };
        let summary = compile_provider_graph_v4(
            std::slice::from_ref(&fixture.shard),
            &fixture.provider_map,
            &fixture.output,
            options,
        )
        .unwrap();
        assert_eq!(summary.selected_layout, ProviderGraphV4Layout::Direct);
        assert!(summary
            .heavy_bitmaps
            .iter()
            .any(|bitmap| bitmap.relation == "set_groups_direct"));
    }

    #[test]
    fn tax_policy_descriptor_has_cross_language_vector_and_binds_every_field() {
        let fields = [
            "ptg-tin-hmac-sha256-v1:release-1",
            TAX_IDENTITY_NORMALIZATION_CONTRACT,
            TAX_IDENTITY_HMAC_CONTRACT,
            TAX_IDENTITY_CANDIDATE_PREFIX_CONTRACT,
            TAX_IDENTITY_AUTHORITY_CONTRACT,
        ];
        let expected = token_policy_descriptor_sha256_fields(fields).unwrap();
        assert_eq!(
            hex(&expected),
            "a0c06f5494f80663686be6861038a8804d9509d0fdc2d2c8cc56c259e53d761c"
        );
        for index in 0..fields.len() {
            let mut changed = fields;
            changed[index] = "changed";
            assert_ne!(
                token_policy_descriptor_sha256_fields(changed).unwrap(),
                expected,
                "descriptor field {index} was not authenticated"
            );
        }
    }

    #[test]
    fn tax_artifact_parser_rejects_reordering_and_candidate_drift() {
        let temporary = tempfile::tempdir().unwrap();
        let groups = [global(3, 1), global(3, 2)];
        let hmac = [0x5au8; 32];
        let descriptor = write_tax_identity(
            &temporary.path().join("tax.sidecar"),
            "tax-shard",
            "ptg-tin-hmac-sha256-v1:release-1",
            [
                (groups[0], V4TaxIdentityState::MatchedEin, Some(hmac)),
                (groups[1], V4TaxIdentityState::Missing, None),
            ],
        );
        let artifact = ValidatedTaxIdentityArtifact::open(&descriptor).unwrap();
        assert_eq!(artifact.record(0).unwrap().tin_hmac_sha256, hmac);

        let header_bytes =
            TAX_IDENTITY_FIXED_HEADER_BYTES + descriptor.metadata.token_policy_id.len();
        let mut candidate_drift = descriptor.clone();
        let mut bytes = fs::read(&candidate_drift.path).unwrap();
        bytes[header_bytes + 17] ^= 1;
        fs::write(&candidate_drift.path, &bytes).unwrap();
        candidate_drift.metadata.sha256 = hex(&Sha256::digest(&bytes));
        assert!(ValidatedTaxIdentityArtifact::open(&candidate_drift)
            .unwrap_err()
            .to_string()
            .contains("candidate"));

        let mut reordered = write_tax_identity(
            &temporary.path().join("tax-reordered.sidecar"),
            "tax-shard",
            "ptg-tin-hmac-sha256-v1:release-1",
            [
                (groups[0], V4TaxIdentityState::MatchedEin, Some(hmac)),
                (groups[1], V4TaxIdentityState::Missing, None),
            ],
        );
        let mut bytes = fs::read(&reordered.path).unwrap();
        let first = bytes[header_bytes..header_bytes + 65].to_vec();
        let second = bytes[header_bytes + 65..header_bytes + 130].to_vec();
        bytes[header_bytes..header_bytes + 65].copy_from_slice(&second);
        bytes[header_bytes + 65..header_bytes + 130].copy_from_slice(&first);
        fs::write(&reordered.path, &bytes).unwrap();
        reordered.metadata.sha256 = hex(&Sha256::digest(&bytes));
        assert!(ValidatedTaxIdentityArtifact::open(&reordered)
            .unwrap_err()
            .to_string()
            .contains("sorted and unique"));
    }

    #[test]
    fn tax_artifact_parser_rejects_metadata_file_and_state_boundaries() {
        let temporary = tempfile::tempdir().unwrap();
        let group = global(3, 1);
        let valid = || {
            write_tax_identity(
                &temporary.path().join(format!(
                    "tax-{}.sidecar",
                    fs::read_dir(temporary.path()).unwrap().count()
                )),
                "tax-shard",
                "ptg-tin-hmac-sha256-v1:release-1",
                [(group, V4TaxIdentityState::Missing, None)],
            )
        };

        let artifact = ValidatedTaxIdentityArtifact::open(&valid()).unwrap();
        assert!(artifact.record(1).is_err());

        let mut inconsistent = valid();
        inconsistent.metadata.final_file = false;
        assert!(ValidatedTaxIdentityArtifact::open(&inconsistent)
            .unwrap_err()
            .to_string()
            .contains("metadata is inconsistent"));

        let mut invalid_policy = valid();
        invalid_policy.metadata.token_policy_id = "invalid".to_owned();
        assert!(ValidatedTaxIdentityArtifact::open(&invalid_policy)
            .unwrap_err()
            .to_string()
            .contains("policy ID is invalid"));

        let mut missing_file = valid();
        missing_file.path = temporary.path().join("missing.sidecar");
        assert!(ValidatedTaxIdentityArtifact::open(&missing_file)
            .unwrap_err()
            .to_string()
            .contains("sidecar is unavailable"));

        let mut wrong_size = valid();
        wrong_size.metadata.byte_count += 1;
        assert!(ValidatedTaxIdentityArtifact::open(&wrong_size)
            .unwrap_err()
            .to_string()
            .contains("byte count metadata mismatch"));

        let mut wrong_checksum = valid();
        wrong_checksum.metadata.sha256 = hex(&[0; 32]);
        assert!(ValidatedTaxIdentityArtifact::open(&wrong_checksum)
            .unwrap_err()
            .to_string()
            .contains("checksum metadata mismatch"));

        let mut bad_header = valid();
        let mut bytes = fs::read(&bad_header.path).unwrap();
        bytes[0] ^= 1;
        fs::write(&bad_header.path, &bytes).unwrap();
        bad_header.metadata.sha256 = hex(&Sha256::digest(&bytes));
        assert!(ValidatedTaxIdentityArtifact::open(&bad_header)
            .unwrap_err()
            .to_string()
            .contains("header or size is invalid"));

        let mut token_on_unavailable = valid();
        let header_bytes =
            TAX_IDENTITY_FIXED_HEADER_BYTES + token_on_unavailable.metadata.token_policy_id.len();
        let mut bytes = fs::read(&token_on_unavailable.path).unwrap();
        bytes[header_bytes + 17] = 1;
        fs::write(&token_on_unavailable.path, &bytes).unwrap();
        token_on_unavailable.metadata.sha256 = hex(&Sha256::digest(&bytes));
        assert!(ValidatedTaxIdentityArtifact::open(&token_on_unavailable)
            .unwrap_err()
            .to_string()
            .contains("carries a token"));

        let mut invalid_state = valid();
        let mut bytes = fs::read(&invalid_state.path).unwrap();
        bytes[header_bytes + 16] = 0xff;
        fs::write(&invalid_state.path, &bytes).unwrap();
        invalid_state.metadata.sha256 = hex(&Sha256::digest(&bytes));
        assert!(ValidatedTaxIdentityArtifact::open(&invalid_state).is_err());

        let mut changed_counts = valid();
        changed_counts.metadata.missing_count = 0;
        changed_counts.metadata.malformed_count = 1;
        assert!(ValidatedTaxIdentityArtifact::open(&changed_counts)
            .unwrap_err()
            .to_string()
            .contains("state counts changed"));
    }

    #[test]
    fn tax_merge_uses_state_priority_fixed_bitmaps_and_dense_hmac_order() {
        let temporary = tempfile::tempdir().unwrap();
        let groups = [global(3, 1), global(3, 2)];
        let lower_hmac = [0x11u8; 32];
        let higher_hmac = [0x22u8; 32];
        let policy = "ptg-tin-hmac-sha256-v1:release-1";
        let first = write_tax_identity(
            &temporary.path().join("tax-first.sidecar"),
            "source-00",
            policy,
            [
                (groups[0], V4TaxIdentityState::Missing, None),
                (groups[1], V4TaxIdentityState::Malformed, None),
            ],
        );
        let last = write_tax_identity(
            &temporary.path().join("tax-last.sidecar"),
            "source-08",
            policy,
            [
                (groups[0], V4TaxIdentityState::MatchedEin, Some(higher_hmac)),
                (groups[1], V4TaxIdentityState::UnsupportedType, None),
            ],
        );
        let source_ordinals = (0..9)
            .map(|ordinal| V4TaxSourceOrdinal {
                shard_id: format!("source-{ordinal:02}"),
                ordinal,
            })
            .collect::<Vec<_>>();
        let mut factors = V4TaxIdentityFactors {
            token_policy_id: policy.to_owned(),
            source_ordinal_sha256: tax_source_ordinal_sha256(&source_ordinals).unwrap(),
            source_ordinals,
            source_bitmap_bytes: 2,
            by_group: BTreeMap::new(),
        };
        let mut sink = |_event: &V4ProgressEvent| {};
        let mut progress = ProgressReporter::new(&mut sink);
        let mut completed = 0;
        merge_tax_identity_artifact(
            &mut factors,
            &ValidatedTaxIdentityArtifact::open(&first).unwrap(),
            0,
            &mut progress,
            &mut completed,
            4,
        )
        .unwrap();
        merge_tax_identity_artifact(
            &mut factors,
            &ValidatedTaxIdentityArtifact::open(&last).unwrap(),
            8,
            &mut progress,
            &mut completed,
            4,
        )
        .unwrap();
        let model = V4TaxIdentityModel::build(&factors, &groups).unwrap();
        assert_eq!(model.tin_hmacs, vec![higher_hmac]);
        assert_eq!(
            model.group_rows,
            vec![
                (
                    groups[0],
                    V4TaxIdentityState::MatchedEin,
                    Some(0),
                    vec![1, 1],
                ),
                (
                    groups[1],
                    V4TaxIdentityState::UnsupportedType,
                    None,
                    vec![1, 1],
                ),
            ]
        );
        assert!(V4TaxIdentityModel::build(&factors, &groups[..1])
            .unwrap_err()
            .to_string()
            .contains("group set differs"));

        let conflict = write_tax_identity(
            &temporary.path().join("tax-conflict.sidecar"),
            "source-08",
            policy,
            [(groups[0], V4TaxIdentityState::MatchedEin, Some(lower_hmac))],
        );
        assert!(merge_tax_identity_artifact(
            &mut factors,
            &ValidatedTaxIdentityArtifact::open(&conflict).unwrap(),
            8,
            &mut progress,
            &mut completed,
            5,
        )
        .unwrap_err()
        .to_string()
        .contains("conflicting full tax identity HMACs"));
    }

    #[test]
    fn tax_dictionary_keeps_full_hmacs_distinct_after_candidate_collision() {
        let groups = [global(3, 1), global(3, 2)];
        let mut first = [0x44u8; 32];
        let mut second = first;
        first[31] = 1;
        second[31] = 2;
        let source_ordinals = vec![V4TaxSourceOrdinal {
            shard_id: "source".to_owned(),
            ordinal: 0,
        }];
        let factors = V4TaxIdentityFactors {
            token_policy_id: "ptg-tin-hmac-sha256-v1:release-1".to_owned(),
            source_ordinal_sha256: tax_source_ordinal_sha256(&source_ordinals).unwrap(),
            source_ordinals,
            source_bitmap_bytes: 1,
            by_group: BTreeMap::from([
                (
                    groups[0],
                    V4MergedTaxIdentity {
                        state: V4TaxIdentityState::MatchedEin,
                        tin_hmac_sha256: Some(second),
                        source_bitmap: vec![1],
                    },
                ),
                (
                    groups[1],
                    V4MergedTaxIdentity {
                        state: V4TaxIdentityState::MatchedEin,
                        tin_hmac_sha256: Some(first),
                        source_bitmap: vec![1],
                    },
                ),
            ]),
        };
        let model = V4TaxIdentityModel::build(&factors, &groups).unwrap();
        assert_eq!(first[..16], second[..16]);
        assert_eq!(model.tin_hmacs, vec![first, second]);
        assert_eq!(model.group_rows[0].2, Some(1));
        assert_eq!(model.group_rows[1].2, Some(0));
    }

    #[test]
    fn tax_model_rejects_noncanonical_state_token_and_source_combinations() {
        let groups = [global(3, 1), global(3, 2), global(3, 3), global(3, 4)];
        let hmac = [0x31; 32];
        let source_ordinals = vec![V4TaxSourceOrdinal {
            shard_id: "source".to_owned(),
            ordinal: 0,
        }];
        let make_factors = |rows| V4TaxIdentityFactors {
            token_policy_id: "ptg-tin-hmac-sha256-v1:release-1".to_owned(),
            source_ordinal_sha256: tax_source_ordinal_sha256(&source_ordinals).unwrap(),
            source_ordinals: source_ordinals.clone(),
            source_bitmap_bytes: 1,
            by_group: rows,
        };
        let valid = make_factors(BTreeMap::from([
            (
                groups[0],
                V4MergedTaxIdentity {
                    state: V4TaxIdentityState::MatchedEin,
                    tin_hmac_sha256: Some(hmac),
                    source_bitmap: vec![1],
                },
            ),
            (
                groups[1],
                V4MergedTaxIdentity {
                    state: V4TaxIdentityState::Missing,
                    tin_hmac_sha256: None,
                    source_bitmap: vec![1],
                },
            ),
            (
                groups[2],
                V4MergedTaxIdentity {
                    state: V4TaxIdentityState::Malformed,
                    tin_hmac_sha256: None,
                    source_bitmap: vec![1],
                },
            ),
            (
                groups[3],
                V4MergedTaxIdentity {
                    state: V4TaxIdentityState::UnsupportedType,
                    tin_hmac_sha256: None,
                    source_bitmap: vec![1],
                },
            ),
        ]));
        let model = V4TaxIdentityModel::build(&valid, &groups).unwrap();
        let summary = model.summary().unwrap();
        assert_eq!(summary.provider_group_count, 4);
        assert_eq!(summary.tax_identity_count, 1);
        assert_eq!(summary.matched_ein_count, 1);
        assert_eq!(summary.missing_count, 1);
        assert_eq!(summary.malformed_count, 1);
        assert_eq!(summary.unsupported_type_count, 1);
        assert_eq!(summary.content_digest.len(), 64);

        for (value, state, priority, label) in [
            (1, V4TaxIdentityState::MatchedEin, 4, "matched_ein"),
            (2, V4TaxIdentityState::Missing, 1, "missing"),
            (3, V4TaxIdentityState::Malformed, 2, "malformed"),
            (
                4,
                V4TaxIdentityState::UnsupportedType,
                3,
                "unsupported_type",
            ),
        ] {
            assert_eq!(V4TaxIdentityState::parse(value).unwrap(), state);
            assert_eq!(state.priority(), priority);
            assert_eq!(state.as_str(), label);
            assert_eq!(state.code(), value);
        }
        assert!(V4TaxIdentityState::parse(0).is_err());

        let invalid_cases = [
            V4MergedTaxIdentity {
                state: V4TaxIdentityState::Missing,
                tin_hmac_sha256: None,
                source_bitmap: vec![0],
            },
            V4MergedTaxIdentity {
                state: V4TaxIdentityState::Missing,
                tin_hmac_sha256: None,
                source_bitmap: vec![2],
            },
            V4MergedTaxIdentity {
                state: V4TaxIdentityState::MatchedEin,
                tin_hmac_sha256: None,
                source_bitmap: vec![1],
            },
            V4MergedTaxIdentity {
                state: V4TaxIdentityState::Malformed,
                tin_hmac_sha256: Some(hmac),
                source_bitmap: vec![1],
            },
        ];
        for invalid_identity in invalid_cases {
            let factors = make_factors(BTreeMap::from([(groups[0], invalid_identity)]));
            assert!(V4TaxIdentityModel::build(&factors, &groups[..1]).is_err());
        }
        let wrong_width = make_factors(BTreeMap::from([(
            groups[0],
            V4MergedTaxIdentity {
                state: V4TaxIdentityState::Missing,
                tin_hmac_sha256: None,
                source_bitmap: vec![1, 0],
            },
        )]));
        assert!(V4TaxIdentityModel::build(&wrong_width, &groups[..1]).is_err());
    }

    #[test]
    fn tax_projection_resource_admission_is_single_reservation_and_bounded() {
        let summary = V4ResourceAdmissionSummary {
            formula: "test".to_owned(),
            input_factor_bytes: 1,
            provider_set_key_map_bytes: 2,
            factor_edge_count: 3,
            factor_owner_count: 4,
            tax_identity_merge_bitmap_upper_bound_bytes: 0,
            tax_identity_source_ordinal_upper_bound_bytes: 0,
            tax_identity_projection_upper_bound_bytes: 8,
            base_estimated_model_bytes: 10,
            derived_projection_bytes: 0,
            tax_identity_projection_bytes: 8,
            retained_scratch_high_water_bytes: 0,
            bounded_emission_buffer_bytes: 5,
            estimated_peak_bytes: 23,
            max_estimated_model_bytes: Some(50),
            max_factor_edges: None,
        };
        let mut admission = ResourceAdmissionTracker {
            summary,
            tax_identity_projection_reconciled: false,
        };
        admission.reserve_projection("group dictionary", 4).unwrap();
        admission.reserve_scratch_members("group sort", 2).unwrap();
        admission.reconcile_tax_identity_projection(6).unwrap();
        assert!(admission.reconcile_tax_identity_projection(1).is_err());
        assert!(admission.reserve_projection("over limit", 100).is_err());
        let summary = admission.into_summary();
        assert_eq!(summary.tax_identity_projection_upper_bound_bytes, 8);
        assert_eq!(summary.tax_identity_projection_bytes, 6);
        assert_eq!(summary.derived_projection_bytes, 4);
        assert!(summary.retained_scratch_high_water_bytes >= 8);
        assert!(summary.estimated_peak_bytes <= 50);
    }
}
