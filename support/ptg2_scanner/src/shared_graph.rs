//! Bounded strict-V3 provider-graph conversion.
//!
//! Membership sidecars are already sorted by owner and contain sorted unique
//! members. This converter therefore performs a native k-way merge for each
//! direction. It never writes edge runs, transposes edges, or materializes the
//! graph. Dense sidecar dictionaries may produce dictionary-sized temporary
//! key translations; graph cardinality only affects the final block streams.
//!
//! Reciprocity uses exact input and globally unique edge counts plus two
//! independent commutative SHA-256 accumulators over canonical edges. One
//! accumulator XORs domain-separated hashes and the other adds four 64-bit
//! words from a separately domain-separated hash. Under the SHA-256 random
//! oracle model, an unequal equal-cardinality edge set passing validation has
//! negligible collision probability. This is probabilistic integrity, not a
//! cryptographic proof against an attacker capable of breaking SHA-256.

use memmap2::{Mmap, MmapOptions};
use sha2::{Digest, Sha256};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet};
use std::error::Error;
use std::fmt;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

const MANIFEST_VERSION: u32 = 1;
const STANDARD_MAGIC: &[u8; 8] = b"PTG2MNSC";
const DENSE_MAGIC: &[u8; 8] = b"PTG2MNDS";
const STANDARD_FORMAT: &str = "magic8:uint32_le_version:uint64_le_entry_count:index(owner16:uint64_le_offset:uint32_le_count):members16";
const DENSE_FORMAT: &str = "magic8:uint32_le_version:uint64_le_entry_count:uint64_le_member_global_count:index(owner16:uint64_le_offset:uint32_le_count):member_globals16:members_uint32_le";
const STANDARD_HEADER_BYTES: usize = 20;
const DENSE_HEADER_BYTES: usize = 28;
const OWNER_RECORD_BYTES: usize = 28;
const GLOBAL_ID_BYTES: usize = 16;
const DENSE_MAP_RECORD_BYTES: usize = 20;
const GRAPH_CHUNK_BYTES: usize = 64 * 1024;
const SHARED_FORMAT_VERSION: i16 = 2;
const BLOCK_HASH_DOMAIN: &[u8] = b"PTG2V3BLOCK\x01";
const SUPPORT_HASH_DOMAIN: &[u8] = b"PTG2V3GRAPHSUPPORT\x01";
const EDGE_XOR_DOMAIN: &[u8] = b"PTG2V3GRAPHEDGE-XOR\x01";
const EDGE_SUM_DOMAIN: &[u8] = b"PTG2V3GRAPHEDGE-SUM\x01";
const PG_COPY_HEADER: &[u8] = b"PGCOPY\n\xff\r\n\0\0\0\0\0\0\0\0\0";

const OUTPUT_NAMES: [&str; 8] = [
    "graph-blocks.copy",
    "graph-owners.copy",
    "provider-groups.copy",
    "npi-scope.copy",
    "graph-blocks.spool",
    "graph-owners.spool",
    "provider-group.map",
    "graph-references.run",
];

type GlobalId = [u8; GLOBAL_ID_BYTES];

/// Validation and conversion failure.
#[derive(Debug)]
pub enum SharedGraphError {
    Io(io::Error),
    InvalidData(String),
}

impl fmt::Display for SharedGraphError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => error.fmt(formatter),
            Self::InvalidData(message) => formatter.write_str(message),
        }
    }
}

impl Error for SharedGraphError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::InvalidData(_) => None,
        }
    }
}

impl From<io::Error> for SharedGraphError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

pub type SharedGraphResult<T> = Result<T, SharedGraphError>;

fn invalid(message: impl Into<String>) -> SharedGraphError {
    SharedGraphError::InvalidData(message.into())
}

/// Manifest metadata required to validate one membership sidecar.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MembershipMetadata {
    pub record_format: String,
    pub sha256: String,
    pub byte_count: u64,
    pub owner_count: u64,
    pub member_count: u64,
    pub member_global_count: Option<u64>,
    pub name: Option<String>,
    pub source_shard_id: Option<String>,
    pub shard_id: Option<String>,
}

/// A membership sidecar and its complete manifest metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MembershipArtifactDescriptor {
    pub path: PathBuf,
    pub metadata: MembershipMetadata,
}

/// Four reciprocal membership directions belonging to one source shard.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SharedGraphShardDescriptor {
    pub shard_id: String,
    pub group_npi: MembershipArtifactDescriptor,
    pub npi_group: MembershipArtifactDescriptor,
    pub group_provider_set: MembershipArtifactDescriptor,
    pub provider_set_group: MembershipArtifactDescriptor,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SharedGraphDirectionMetrics {
    pub direction: u8,
    pub object_kind: &'static str,
    pub member_width: u8,
    pub owner_count: u64,
    pub member_count: u64,
    pub empty_owner_count: u64,
    pub block_count: u64,
    pub raw_byte_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SharedGraphEdgeMetrics {
    pub edge_kind: &'static str,
    pub input_edge_count: u64,
    pub unique_edge_count: u64,
    pub duplicate_edge_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SharedGraphIntegrityMetrics {
    pub shard_count: u64,
    pub artifact_count: u64,
    pub checksum_byte_count: u64,
    pub reciprocal_pair_count: u64,
    pub reciprocal_edge_count: u64,
    pub input_edge_count: u64,
    pub unique_edge_count: u64,
    pub duplicate_edge_count: u64,
}

/// Fixed output files and the fields consumed by `SharedGraphConversionResult`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SharedGraphConversionSummary {
    pub scratch_directory: PathBuf,
    pub output_directory: PathBuf,
    pub block_copy_path: PathBuf,
    pub owner_copy_path: PathBuf,
    pub group_copy_path: PathBuf,
    pub npi_copy_path: PathBuf,
    pub block_spool_path: PathBuf,
    pub owner_spool_path: PathBuf,
    pub group_map_path: PathBuf,
    pub reference_path: PathBuf,
    pub block_count: u64,
    pub owner_count: u64,
    pub provider_group_count: u64,
    pub npi_count: u64,
    pub support_digest: [u8; 32],
    pub direction_metrics: Vec<SharedGraphDirectionMetrics>,
    pub edge_metrics: Vec<SharedGraphEdgeMetrics>,
    pub input_byte_count: u64,
    pub raw_block_byte_count: u64,
    pub stored_block_byte_count: u64,
    pub integrity: SharedGraphIntegrityMetrics,
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
    fingerprint: EdgeFingerprint,
}

impl ValidatedArtifact {
    fn open(
        descriptor: &MembershipArtifactDescriptor,
        direction: Direction,
        provider_map: Option<&DenseMap>,
    ) -> SharedGraphResult<Self> {
        let expected_digest = parse_sha256(&descriptor.metadata.sha256)?;
        let file = File::open(&descriptor.path).map_err(|error| {
            invalid(format!(
                "membership sidecar is unavailable ({}): {error}",
                descriptor.path.display()
            ))
        })?;
        let observed_size = file.metadata()?.len();
        if observed_size != descriptor.metadata.byte_count {
            return Err(invalid(format!(
                "membership sidecar byte count mismatch for {}: expected {}, got {}",
                descriptor.path.display(),
                descriptor.metadata.byte_count,
                observed_size
            )));
        }
        // SAFETY: the scanner owns immutable completed sidecars for the duration
        // of conversion. The open file is retained alongside the mapping.
        let bytes = unsafe { MmapOptions::new().map(&file)? };
        let observed_digest: [u8; 32] = Sha256::digest(&bytes).into();
        if observed_digest != expected_digest {
            return Err(invalid(format!(
                "membership sidecar checksum mismatch: {}",
                descriptor.path.display()
            )));
        }
        if bytes.len() < 8 {
            return Err(invalid("membership sidecar is missing its header"));
        }

        let (dense, header_size, owner_count, member_global_count) =
            if &bytes[..8] == STANDARD_MAGIC {
                if descriptor.metadata.record_format != STANDARD_FORMAT {
                    return Err(invalid(
                        "standard membership sidecar format metadata mismatch",
                    ));
                }
                if bytes.len() < STANDARD_HEADER_BYTES {
                    return Err(invalid("membership sidecar is missing its header"));
                }
                let version = read_u32_le(&bytes, 8)?;
                let owners = read_u64_le(&bytes, 12)?;
                if version != MANIFEST_VERSION {
                    return Err(invalid(format!(
                        "unsupported membership sidecar version: {version}"
                    )));
                }
                (false, STANDARD_HEADER_BYTES, owners, 0)
            } else if &bytes[..8] == DENSE_MAGIC {
                if descriptor.metadata.record_format != DENSE_FORMAT {
                    return Err(invalid("dense membership sidecar format metadata mismatch"));
                }
                if bytes.len() < DENSE_HEADER_BYTES {
                    return Err(invalid("dense membership sidecar is missing its header"));
                }
                let version = read_u32_le(&bytes, 8)?;
                let owners = read_u64_le(&bytes, 12)?;
                let globals = read_u64_le(&bytes, 20)?;
                if version != MANIFEST_VERSION {
                    return Err(invalid(format!(
                        "unsupported membership sidecar version: {version}"
                    )));
                }
                let expected_globals =
                    descriptor.metadata.member_global_count.ok_or_else(|| {
                        invalid("dense membership metadata requires member_global_count")
                    })?;
                if globals != expected_globals {
                    return Err(invalid("dense membership dictionary count mismatch"));
                }
                (true, DENSE_HEADER_BYTES, owners, globals)
            } else {
                return Err(invalid("membership sidecar has an invalid magic header"));
            };

        if owner_count != descriptor.metadata.owner_count {
            return Err(invalid("membership sidecar owner count mismatch"));
        }
        let owner_count_usize = usize::try_from(owner_count)
            .map_err(|_| invalid("membership owner count exceeds addressable memory"))?;
        let member_count_usize = usize::try_from(descriptor.metadata.member_count)
            .map_err(|_| invalid("membership member count exceeds addressable memory"))?;
        let member_global_count_usize = usize::try_from(member_global_count)
            .map_err(|_| invalid("membership dictionary count exceeds addressable memory"))?;
        let index_bytes = owner_count_usize
            .checked_mul(OWNER_RECORD_BYTES)
            .ok_or_else(|| invalid("membership owner index size overflows"))?;
        let dictionary_start = header_size
            .checked_add(index_bytes)
            .ok_or_else(|| invalid("membership layout size overflows"))?;
        let dictionary_bytes = member_global_count_usize
            .checked_mul(GLOBAL_ID_BYTES)
            .ok_or_else(|| invalid("membership dictionary size overflows"))?;
        let members_start = dictionary_start
            .checked_add(dictionary_bytes)
            .ok_or_else(|| invalid("membership layout size overflows"))?;
        let member_width = if dense { 4 } else { GLOBAL_ID_BYTES };
        let expected_size = members_start
            .checked_add(
                member_count_usize
                    .checked_mul(member_width)
                    .ok_or_else(|| invalid("membership member block size overflows"))?,
            )
            .ok_or_else(|| invalid("membership layout size overflows"))?;
        if expected_size != bytes.len() {
            return Err(invalid(format!(
                "membership sidecar layout size mismatch: expected {expected_size}, got {}",
                bytes.len()
            )));
        }

        let mut artifact = Self {
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
            fingerprint: EdgeFingerprint::default(),
        };
        artifact.validate_dictionary()?;
        artifact.fingerprint = artifact.validate_owners_and_members(direction, provider_map)?;
        Ok(artifact)
    }

    fn validate_dictionary(&self) -> SharedGraphResult<()> {
        if !self.dense {
            return Ok(());
        }
        let mut previous = None;
        for index in 0..self.member_global_count {
            let global_id = self.dictionary_global(index)?;
            if previous.is_some_and(|value| global_id <= value) {
                return Err(invalid(
                    "dense membership dictionary must be sorted and unique",
                ));
            }
            previous = Some(global_id);
        }
        Ok(())
    }

    fn validate_owners_and_members(
        &self,
        direction: Direction,
        provider_map: Option<&DenseMap>,
    ) -> SharedGraphResult<EdgeFingerprint> {
        let mut previous_owner = None;
        let mut expected_member_offset = 0u64;
        let mut fingerprint = EdgeFingerprint::default();
        for owner_index in 0..self.owner_count {
            let owner = self.owner(owner_index)?;
            if previous_owner.is_some_and(|value| owner.owner <= value) {
                return Err(invalid("membership owners must be sorted and unique"));
            }
            if direction == Direction::NpiToGroup {
                npi_from_global_id(owner.owner)?;
            } else if direction == Direction::ProviderSetToGroup {
                provider_map
                    .ok_or_else(|| invalid("provider-set map is required for validation"))?
                    .key(owner.owner)?;
            }
            if owner.member_offset != expected_member_offset {
                return Err(invalid("membership member offsets are not contiguous"));
            }
            expected_member_offset = expected_member_offset
                .checked_add(u64::from(owner.member_count))
                .ok_or_else(|| invalid("membership member count overflows"))?;
            let mut previous_member = None;
            for member_index in
                owner.member_offset..owner.member_offset + u64::from(owner.member_count)
            {
                let member = self.member_global(member_index)?;
                if previous_member.is_some_and(|value| member <= value) {
                    return Err(invalid("membership members must be sorted and unique"));
                }
                update_edge_fingerprint(&mut fingerprint, direction, owner.owner, member)?;
                if direction == Direction::GroupToProviderSet {
                    provider_map
                        .ok_or_else(|| invalid("provider-set map is required for validation"))?
                        .key(member)?;
                }
                previous_member = Some(member);
            }
            previous_owner = Some(owner.owner);
        }
        if expected_member_offset != self.member_count {
            return Err(invalid("membership member count mismatch"));
        }
        Ok(fingerprint)
    }

    fn owner(&self, index: u64) -> SharedGraphResult<OwnerRecord> {
        if index >= self.owner_count {
            return Err(invalid("membership owner index is out of range"));
        }
        let index = usize::try_from(index)
            .map_err(|_| invalid("membership owner index exceeds addressable memory"))?;
        let offset = self
            .index_start
            .checked_add(index * OWNER_RECORD_BYTES)
            .ok_or_else(|| invalid("membership owner offset overflows"))?;
        let owner = read_global_id(&self.bytes, offset)?;
        Ok(OwnerRecord {
            owner,
            member_offset: read_u64_le(&self.bytes, offset + 16)?,
            member_count: read_u32_le(&self.bytes, offset + 24)?,
        })
    }

    fn dictionary_global(&self, index: u64) -> SharedGraphResult<GlobalId> {
        if index >= self.member_global_count {
            return Err(invalid("dense membership member id is out of range"));
        }
        let index = usize::try_from(index)
            .map_err(|_| invalid("dense dictionary index exceeds addressable memory"))?;
        read_global_id(&self.bytes, self.dictionary_start + index * GLOBAL_ID_BYTES)
    }

    fn member_local_id(&self, index: u64) -> SharedGraphResult<Option<u32>> {
        if !self.dense {
            return Ok(None);
        }
        if index >= self.member_count {
            return Err(invalid("membership member index is out of range"));
        }
        let index = usize::try_from(index)
            .map_err(|_| invalid("membership member index exceeds addressable memory"))?;
        let local_id = read_u32_le(&self.bytes, self.members_start + index * 4)?;
        if u64::from(local_id) >= self.member_global_count {
            return Err(invalid("dense membership member id is out of range"));
        }
        Ok(Some(local_id))
    }

    fn member_global(&self, index: u64) -> SharedGraphResult<GlobalId> {
        if index >= self.member_count {
            return Err(invalid("membership member index is out of range"));
        }
        if let Some(local_id) = self.member_local_id(index)? {
            self.dictionary_global(u64::from(local_id))
        } else {
            let index = usize::try_from(index)
                .map_err(|_| invalid("membership member index exceeds addressable memory"))?;
            read_global_id(&self.bytes, self.members_start + index * GLOBAL_ID_BYTES)
        }
    }
}

struct ValidatedShard {
    shard_id: String,
    group_npi: ValidatedArtifact,
    npi_group: ValidatedArtifact,
    group_provider_set: ValidatedArtifact,
    provider_set_group: ValidatedArtifact,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Direction {
    NpiToGroup,
    GroupToNpi,
    GroupToProviderSet,
    ProviderSetToGroup,
}

impl Direction {
    const ALL: [Self; 4] = [
        Self::NpiToGroup,
        Self::GroupToNpi,
        Self::GroupToProviderSet,
        Self::ProviderSetToGroup,
    ];

    fn id(self) -> u8 {
        match self {
            Self::NpiToGroup => 1,
            Self::GroupToNpi => 2,
            Self::GroupToProviderSet => 3,
            Self::ProviderSetToGroup => 4,
        }
    }

    fn object_kind(self) -> &'static str {
        match self {
            Self::NpiToGroup => "graph_npi_groups_v1",
            Self::GroupToNpi => "graph_group_npis_v1",
            Self::GroupToProviderSet => "graph_group_provider_sets_v1",
            Self::ProviderSetToGroup => "graph_provider_set_groups_v1",
        }
    }

    fn member_width(self) -> usize {
        match self {
            Self::GroupToNpi => 8,
            _ => 4,
        }
    }

    fn is_group_npi(self) -> bool {
        matches!(self, Self::NpiToGroup | Self::GroupToNpi)
    }

    fn member_uses_group_map(self) -> bool {
        matches!(self, Self::NpiToGroup | Self::ProviderSetToGroup)
    }

    fn member_uses_provider_map(self) -> bool {
        self == Self::GroupToProviderSet
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct EdgeFingerprint {
    xor: [u8; 32],
    sum: [u64; 4],
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
    }
}

fn update_edge_fingerprint(
    fingerprint: &mut EdgeFingerprint,
    direction: Direction,
    owner: GlobalId,
    member: GlobalId,
) -> SharedGraphResult<()> {
    if direction.is_group_npi() {
        let (group, npi) = match direction {
            Direction::NpiToGroup => (member, npi_from_global_id(owner)?),
            Direction::GroupToNpi => (owner, npi_from_global_id(member)?),
            _ => unreachable!(),
        };
        let mut edge = [0u8; 24];
        edge[..16].copy_from_slice(&group);
        edge[16..].copy_from_slice(&npi.to_be_bytes());
        fingerprint.add(&edge);
    } else {
        let (group, provider_set) = match direction {
            Direction::GroupToProviderSet => (owner, member),
            Direction::ProviderSetToGroup => (member, owner),
            _ => unreachable!(),
        };
        let mut edge = [0u8; 32];
        edge[..16].copy_from_slice(&group);
        edge[16..].copy_from_slice(&provider_set);
        fingerprint.add(&edge);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OwnerHeapItem {
    owner: GlobalId,
    artifact_index: usize,
    owner_index: u64,
}

impl Ord for OwnerHeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .owner
            .cmp(&self.owner)
            .then_with(|| other.artifact_index.cmp(&self.artifact_index))
            .then_with(|| other.owner_index.cmp(&self.owner_index))
    }
}

impl PartialOrd for OwnerHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct MemberHeapItem {
    member: GlobalId,
    artifact_index: usize,
    member_index: u64,
    end_index: u64,
}

impl Ord for MemberHeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .member
            .cmp(&self.member)
            .then_with(|| other.artifact_index.cmp(&self.artifact_index))
            .then_with(|| other.member_index.cmp(&self.member_index))
    }
}

impl PartialOrd for MemberHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

trait DirectionSink {
    fn begin_owner(&mut self, owner: GlobalId) -> SharedGraphResult<()>;
    fn member(
        &mut self,
        member: GlobalId,
        artifact_index: usize,
        local_id: Option<u32>,
    ) -> SharedGraphResult<()>;
    fn end_owner(&mut self, member_count: u64) -> SharedGraphResult<()>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DirectionMergeMetrics {
    owner_count: u64,
    member_count: u64,
    empty_owner_count: u64,
    fingerprint: EdgeFingerprint,
}

fn merge_direction(
    artifacts: &[&ValidatedArtifact],
    direction: Direction,
    sink: &mut impl DirectionSink,
) -> SharedGraphResult<DirectionMergeMetrics> {
    let mut owners = BinaryHeap::new();
    for (artifact_index, artifact) in artifacts.iter().enumerate() {
        if artifact.owner_count > 0 {
            owners.push(OwnerHeapItem {
                owner: artifact.owner(0)?.owner,
                artifact_index,
                owner_index: 0,
            });
        }
    }

    let mut owner_count = 0u64;
    let mut member_count = 0u64;
    let mut empty_owner_count = 0u64;
    let mut fingerprint = EdgeFingerprint::default();
    while let Some(first_owner) = owners.pop() {
        let owner = first_owner.owner;
        let mut matching_owners = vec![first_owner];
        while owners.peek().is_some_and(|item| item.owner == owner) {
            matching_owners.push(owners.pop().expect("peeked owner exists"));
        }
        sink.begin_owner(owner)?;
        let mut members = BinaryHeap::new();
        for item in matching_owners {
            let artifact = artifacts[item.artifact_index];
            let record = artifact.owner(item.owner_index)?;
            if record.member_count > 0 {
                members.push(MemberHeapItem {
                    member: artifact.member_global(record.member_offset)?,
                    artifact_index: item.artifact_index,
                    member_index: record.member_offset,
                    end_index: record.member_offset + u64::from(record.member_count),
                });
            }
            let next_owner_index = item.owner_index + 1;
            if next_owner_index < artifact.owner_count {
                owners.push(OwnerHeapItem {
                    owner: artifact.owner(next_owner_index)?.owner,
                    artifact_index: item.artifact_index,
                    owner_index: next_owner_index,
                });
            }
        }

        let mut owner_member_count = 0u64;
        let mut previous_member = None;
        while let Some(item) = members.pop() {
            if previous_member != Some(item.member) {
                let local_id = artifacts[item.artifact_index].member_local_id(item.member_index)?;
                sink.member(item.member, item.artifact_index, local_id)?;
                update_edge_fingerprint(&mut fingerprint, direction, owner, item.member)?;
                owner_member_count += 1;
                member_count += 1;
                previous_member = Some(item.member);
            }
            let next_member_index = item.member_index + 1;
            if next_member_index < item.end_index {
                members.push(MemberHeapItem {
                    member: artifacts[item.artifact_index].member_global(next_member_index)?,
                    member_index: next_member_index,
                    ..item
                });
            }
        }
        sink.end_owner(owner_member_count)?;
        owner_count += 1;
        if owner_member_count == 0 {
            empty_owner_count += 1;
        }
    }
    Ok(DirectionMergeMetrics {
        owner_count,
        member_count,
        empty_owner_count,
        fingerprint,
    })
}

struct DenseMap {
    _file: File,
    bytes: Option<Mmap>,
    count: usize,
    label: &'static str,
}

impl DenseMap {
    fn open(path: &Path, label: &'static str, expected_starts: &[u32]) -> SharedGraphResult<Self> {
        let file = File::open(path)?;
        let size = usize::try_from(file.metadata()?.len())
            .map_err(|_| invalid(format!("{label} map is too large")))?;
        if size % DENSE_MAP_RECORD_BYTES != 0 {
            return Err(invalid(format!("{label} map is truncated")));
        }
        let count = size / DENSE_MAP_RECORD_BYTES;
        let bytes = if size == 0 {
            None
        } else {
            // SAFETY: the converter owns immutable map files while they are mapped.
            Some(unsafe { MmapOptions::new().map(&file)? })
        };
        let mut previous = None;
        let mut first_key: Option<u32> = None;
        for index in 0..count {
            let (global_id, key) = dense_map_record(
                bytes.as_deref().expect("non-empty dense map is mapped"),
                index,
            )?;
            if previous.is_some_and(|value| global_id <= value) {
                return Err(invalid(format!(
                    "{label} map global IDs must be sorted and unique"
                )));
            }
            let expected_key = if let Some(start) = first_key {
                start
                    .checked_add(
                        u32::try_from(index)
                            .map_err(|_| invalid(format!("{label} map exceeds uint32")))?,
                    )
                    .ok_or_else(|| invalid(format!("{label} map key overflows uint32")))?
            } else {
                if !expected_starts.contains(&key) {
                    return Err(invalid(format!(
                        "{label} map keys must be dense and follow sorted global IDs"
                    )));
                }
                first_key = Some(key);
                key
            };
            if key != expected_key {
                return Err(invalid(format!(
                    "{label} map keys must be dense and follow sorted global IDs"
                )));
            }
            previous = Some(global_id);
        }
        Ok(Self {
            _file: file,
            bytes,
            count,
            label,
        })
    }

    fn key(&self, target: GlobalId) -> SharedGraphResult<u32> {
        let mut low = 0usize;
        let mut high = self.count;
        let bytes = self.bytes.as_deref().ok_or_else(|| {
            invalid(format!(
                "graph references an ID absent from the authoritative {} map",
                self.label
            ))
        })?;
        while low < high {
            let middle = (low + high) / 2;
            let (candidate, _) = dense_map_record(bytes, middle)?;
            if candidate < target {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        if low >= self.count {
            return Err(invalid(format!(
                "graph references an ID absent from the authoritative {} map",
                self.label
            )));
        }
        let (candidate, key) = dense_map_record(bytes, low)?;
        if candidate != target {
            return Err(invalid(format!(
                "graph references an ID absent from the authoritative {} map",
                self.label
            )));
        }
        Ok(key)
    }

    fn record(&self, index: usize) -> SharedGraphResult<(GlobalId, u32)> {
        if index >= self.count {
            return Err(invalid(format!("{} map index is out of range", self.label)));
        }
        dense_map_record(
            self.bytes
                .as_deref()
                .ok_or_else(|| invalid(format!("{} map is empty", self.label)))?,
            index,
        )
    }
}

fn build_provider_set_map(source: &Path, destination: &Path) -> SharedGraphResult<DenseMap> {
    let source = File::open(source)?;
    let mut reader = BufReader::new(source);
    let mut destination_writer = BufWriter::new(File::create(destination)?);
    let mut line = String::new();
    let mut previous = None;
    let mut first_key = None;
    let mut row_index = 0u32;
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        while line.ends_with(['\n', '\r']) {
            line.pop();
        }
        let mut fields = line.split('\t');
        let global_hex = fields
            .next()
            .ok_or_else(|| invalid("provider-set dictionary export has an invalid row"))?;
        let key_text = fields
            .next()
            .ok_or_else(|| invalid("provider-set dictionary export has an invalid row"))?;
        if fields.next().is_some() {
            return Err(invalid("provider-set dictionary export has an invalid row"));
        }
        let global_id = parse_global_id_hex(global_hex)?;
        let key = key_text
            .parse::<u32>()
            .map_err(|_| invalid("provider-set dictionary export has an invalid row"))?;
        if previous.is_some_and(|value| global_id <= value) {
            return Err(invalid(
                "provider-set dictionary global IDs must be sorted and unique",
            ));
        }
        if first_key.is_none() {
            if key != 0 && key != 1 {
                return Err(invalid(
                    "provider-set dictionary keys must start at zero or one",
                ));
            }
            first_key = Some(key);
        }
        let expected = first_key
            .expect("first key assigned")
            .checked_add(row_index)
            .ok_or_else(|| invalid("provider-set dictionary key overflows uint32"))?;
        if key != expected {
            return Err(invalid(
                "provider-set dictionary keys must be dense and follow sorted global IDs",
            ));
        }
        destination_writer.write_all(&global_id)?;
        destination_writer.write_all(&key.to_be_bytes())?;
        previous = Some(global_id);
        row_index = row_index
            .checked_add(1)
            .ok_or_else(|| invalid("provider-set dictionary exceeds uint32 capacity"))?;
    }
    destination_writer.flush()?;
    drop(destination_writer);
    DenseMap::open(destination, "provider-set", &[0, 1])
}

fn merge_owner_ids(
    artifacts: &[&ValidatedArtifact],
    mut consume: impl FnMut(GlobalId, u64) -> SharedGraphResult<()>,
) -> SharedGraphResult<u64> {
    let mut owners = BinaryHeap::new();
    for (artifact_index, artifact) in artifacts.iter().enumerate() {
        if artifact.owner_count > 0 {
            owners.push(OwnerHeapItem {
                owner: artifact.owner(0)?.owner,
                artifact_index,
                owner_index: 0,
            });
        }
    }
    let mut count = 0u64;
    while let Some(item) = owners.pop() {
        let owner = item.owner;
        let mut matching = vec![item];
        while owners.peek().is_some_and(|next| next.owner == owner) {
            matching.push(owners.pop().expect("peeked owner exists"));
        }
        consume(owner, count)?;
        count = count
            .checked_add(1)
            .ok_or_else(|| invalid("merged owner count overflows uint64"))?;
        for matched in matching {
            let artifact = artifacts[matched.artifact_index];
            let next_index = matched.owner_index + 1;
            if next_index < artifact.owner_count {
                owners.push(OwnerHeapItem {
                    owner: artifact.owner(next_index)?.owner,
                    artifact_index: matched.artifact_index,
                    owner_index: next_index,
                });
            }
        }
    }
    Ok(count)
}

fn build_group_map(
    artifacts: &[&ValidatedArtifact],
    destination: &Path,
) -> SharedGraphResult<DenseMap> {
    let mut writer = BufWriter::new(File::create(destination)?);
    let count = merge_owner_ids(artifacts, |global_id, key| {
        let key = u32::try_from(key)
            .map_err(|_| invalid("provider group dictionary exceeds uint32 capacity"))?;
        writer.write_all(&global_id)?;
        writer.write_all(&key.to_be_bytes())?;
        Ok(())
    })?;
    if count > u64::from(u32::MAX) + 1 {
        return Err(invalid("provider group dictionary exceeds uint32 capacity"));
    }
    writer.flush()?;
    drop(writer);
    DenseMap::open(destination, "provider-group", &[0])
}

struct TranslationTable {
    _file: File,
    bytes: Option<Mmap>,
    count: u64,
}

impl TranslationTable {
    fn build(
        artifact: &ValidatedArtifact,
        dense_map: &DenseMap,
        path: &Path,
    ) -> SharedGraphResult<Option<Self>> {
        if !artifact.dense {
            return Ok(None);
        }
        let mut writer = BufWriter::new(File::create(path)?);
        for index in 0..artifact.member_global_count {
            let key = dense_map.key(artifact.dictionary_global(index)?)?;
            writer.write_all(&key.to_le_bytes())?;
        }
        writer.flush()?;
        drop(writer);
        let file = File::open(path)?;
        let expected_size = usize::try_from(artifact.member_global_count)
            .map_err(|_| invalid("translation table count exceeds addressable memory"))?
            .checked_mul(4)
            .ok_or_else(|| invalid("translation table size overflows"))?;
        let bytes = if expected_size == 0 {
            None
        } else {
            // SAFETY: the converter owns immutable translation tables while mapped.
            Some(unsafe { MmapOptions::new().map(&file)? })
        };
        if bytes.as_ref().map_or(0, |mapped| mapped.len()) != expected_size {
            return Err(invalid("translation table size mismatch"));
        }
        Ok(Some(Self {
            _file: file,
            bytes,
            count: artifact.member_global_count,
        }))
    }

    fn key(&self, local_id: u32) -> SharedGraphResult<u32> {
        if u64::from(local_id) >= self.count {
            return Err(invalid("translation table local id is out of range"));
        }
        read_u32_le(
            self.bytes
                .as_deref()
                .ok_or_else(|| invalid("translation table is empty"))?,
            local_id as usize * 4,
        )
    }
}

fn build_translation_tables(
    artifacts: &[&ValidatedArtifact],
    dense_map: Option<&DenseMap>,
    work_directory: &Path,
    direction: Direction,
) -> SharedGraphResult<Vec<Option<TranslationTable>>> {
    let Some(dense_map) = dense_map else {
        return Ok((0..artifacts.len()).map(|_| None).collect());
    };
    artifacts
        .iter()
        .enumerate()
        .map(|(index, artifact)| {
            TranslationTable::build(
                artifact,
                dense_map,
                &work_directory.join(format!(
                    "direction-{}-artifact-{index}.translation",
                    direction.id()
                )),
            )
        })
        .collect()
}

struct PgCopyWriter {
    writer: BufWriter<File>,
    finished: bool,
}

impl PgCopyWriter {
    fn create(path: &Path) -> SharedGraphResult<Self> {
        let mut writer = BufWriter::new(File::create(path)?);
        writer.write_all(PG_COPY_HEADER)?;
        Ok(Self {
            writer,
            finished: false,
        })
    }

    fn write_row(&mut self, fields: &[&[u8]]) -> SharedGraphResult<()> {
        let field_count = i16::try_from(fields.len())
            .map_err(|_| invalid("PostgreSQL COPY row has too many fields"))?;
        self.writer.write_all(&field_count.to_be_bytes())?;
        for field in fields {
            let field_size = i32::try_from(field.len())
                .map_err(|_| invalid("PostgreSQL COPY field exceeds int32 length"))?;
            self.writer.write_all(&field_size.to_be_bytes())?;
            self.writer.write_all(field)?;
        }
        Ok(())
    }

    fn finish(mut self) -> SharedGraphResult<()> {
        self.writer.write_all(&(-1i16).to_be_bytes())?;
        self.writer.flush()?;
        self.finished = true;
        Ok(())
    }
}

impl Drop for PgCopyWriter {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.writer.flush();
        }
    }
}

struct BlockStream<'a> {
    direction: Direction,
    block_copy: &'a mut PgCopyWriter,
    owner_copy: &'a mut PgCopyWriter,
    block_spool: &'a mut BufWriter<File>,
    owner_spool: &'a mut BufWriter<File>,
    reference_spool: &'a mut BufWriter<File>,
    support_hasher: &'a mut Sha256,
    group_map: &'a DenseMap,
    provider_map: &'a DenseMap,
    translations: &'a [Option<TranslationTable>],
    payload: Vec<u8>,
    block_count: u64,
    member_count: u64,
    owner_key: Option<u64>,
    owner_first_member: u64,
}

impl<'a> BlockStream<'a> {
    fn owner_key(&self, owner: GlobalId) -> SharedGraphResult<u64> {
        match self.direction {
            Direction::NpiToGroup => npi_from_global_id(owner),
            Direction::GroupToNpi | Direction::GroupToProviderSet => {
                Ok(u64::from(self.group_map.key(owner)?))
            }
            Direction::ProviderSetToGroup => Ok(u64::from(self.provider_map.key(owner)?)),
        }
    }

    fn mapped_member_key(
        &self,
        member: GlobalId,
        artifact_index: usize,
        local_id: Option<u32>,
        dense_map: &DenseMap,
    ) -> SharedGraphResult<u32> {
        if let Some(local_id) = local_id {
            if let Some(table) = self
                .translations
                .get(artifact_index)
                .and_then(Option::as_ref)
            {
                return table.key(local_id);
            }
        }
        dense_map.key(member)
    }

    fn append_u32(&mut self, value: u32) -> SharedGraphResult<()> {
        self.payload.extend_from_slice(&value.to_le_bytes());
        self.member_count = self
            .member_count
            .checked_add(1)
            .ok_or_else(|| invalid("graph member count overflows uint64"))?;
        if self.payload.len() == GRAPH_CHUNK_BYTES {
            self.flush_block()?;
        }
        Ok(())
    }

    fn append_u64(&mut self, value: u64) -> SharedGraphResult<()> {
        self.payload.extend_from_slice(&value.to_le_bytes());
        self.member_count = self
            .member_count
            .checked_add(1)
            .ok_or_else(|| invalid("graph member count overflows uint64"))?;
        if self.payload.len() == GRAPH_CHUNK_BYTES {
            self.flush_block()?;
        }
        Ok(())
    }

    fn flush_block(&mut self) -> SharedGraphResult<()> {
        if self.payload.is_empty() {
            return Ok(());
        }
        let block_hash = shared_block_hash(self.direction.object_kind(), "none", &self.payload)?;
        let block_key = i64::try_from(self.block_count)
            .map_err(|_| invalid("graph block key exceeds PostgreSQL bigint"))?;
        let entry_count = u64::try_from(self.payload.len() / self.direction.member_width())
            .expect("payload entry count fits u64");
        let entry_count_i64 = i64::try_from(entry_count)
            .map_err(|_| invalid("graph block entry count exceeds PostgreSQL bigint"))?;
        let raw_bytes = u64::try_from(self.payload.len()).expect("payload size fits u64");
        let raw_bytes_i64 = i64::try_from(raw_bytes)
            .map_err(|_| invalid("graph block size exceeds PostgreSQL bigint"))?;
        let fragment_no = 0i32.to_be_bytes();
        let format_version = SHARED_FORMAT_VERSION.to_be_bytes();
        let block_key_bytes = block_key.to_be_bytes();
        let entry_count_bytes = entry_count_i64.to_be_bytes();
        let raw_bytes_field = raw_bytes_i64.to_be_bytes();
        self.block_copy.write_row(&[
            &block_hash,
            &format_version,
            self.direction.object_kind().as_bytes(),
            &block_key_bytes,
            &fragment_no,
            &entry_count_bytes,
            b"none",
            &raw_bytes_field,
            &raw_bytes_field,
            &self.payload,
        ])?;
        self.block_spool.write_all(&[self.direction.id()])?;
        self.block_spool
            .write_all(&self.block_count.to_be_bytes())?;
        self.block_spool.write_all(&entry_count.to_be_bytes())?;
        self.block_spool.write_all(
            &u32::try_from(raw_bytes)
                .map_err(|_| invalid("graph spool block exceeds uint32"))?
                .to_be_bytes(),
        )?;
        self.block_spool.write_all(&self.payload)?;
        self.reference_spool.write_all(&[self.direction.id()])?;
        self.reference_spool
            .write_all(&self.block_count.to_be_bytes())?;
        self.reference_spool.write_all(&entry_count.to_be_bytes())?;
        self.reference_spool.write_all(&block_hash)?;
        self.reference_spool.write_all(&raw_bytes.to_be_bytes())?;
        self.block_count += 1;
        self.payload.clear();
        Ok(())
    }

    fn finish(mut self) -> SharedGraphResult<u64> {
        self.flush_block()?;
        Ok(self.block_count)
    }
}

impl DirectionSink for BlockStream<'_> {
    fn begin_owner(&mut self, owner: GlobalId) -> SharedGraphResult<()> {
        if self.owner_key.is_some() {
            return Err(invalid("graph owner stream is already open"));
        }
        self.owner_key = Some(self.owner_key(owner)?);
        self.owner_first_member = self.member_count;
        Ok(())
    }

    fn member(
        &mut self,
        member: GlobalId,
        artifact_index: usize,
        local_id: Option<u32>,
    ) -> SharedGraphResult<()> {
        match self.direction {
            Direction::NpiToGroup | Direction::ProviderSetToGroup => {
                let key =
                    self.mapped_member_key(member, artifact_index, local_id, self.group_map)?;
                self.append_u32(key)
            }
            Direction::GroupToNpi => self.append_u64(npi_from_global_id(member)?),
            Direction::GroupToProviderSet => {
                let key =
                    self.mapped_member_key(member, artifact_index, local_id, self.provider_map)?;
                self.append_u32(key)
            }
        }
    }

    fn end_owner(&mut self, member_count: u64) -> SharedGraphResult<()> {
        let owner_key = self
            .owner_key
            .take()
            .ok_or_else(|| invalid("graph owner stream is not open"))?;
        let observed_count = self
            .member_count
            .checked_sub(self.owner_first_member)
            .ok_or_else(|| invalid("graph owner member count underflows"))?;
        if observed_count != member_count {
            return Err(invalid("graph owner member count changed while encoding"));
        }
        let absolute_offset = self
            .owner_first_member
            .checked_mul(self.direction.member_width() as u64)
            .ok_or_else(|| invalid("graph owner byte offset overflows"))?;
        let first_chunk = absolute_offset / GRAPH_CHUNK_BYTES as u64;
        let member_offset = absolute_offset % GRAPH_CHUNK_BYTES as u64;
        let first_chunk_u32 =
            u32::try_from(first_chunk).map_err(|_| invalid("graph owner chunk exceeds uint32"))?;
        let member_offset_u32 = u32::try_from(member_offset)
            .map_err(|_| invalid("graph owner offset exceeds uint32"))?;
        let mut encoded = [0u8; 25];
        encoded[0] = self.direction.id();
        encoded[1..9].copy_from_slice(&owner_key.to_be_bytes());
        encoded[9..13].copy_from_slice(&first_chunk_u32.to_be_bytes());
        encoded[13..17].copy_from_slice(&member_offset_u32.to_be_bytes());
        encoded[17..25].copy_from_slice(&member_count.to_be_bytes());
        self.owner_spool.write_all(&encoded)?;
        self.support_hasher.update(encoded);

        let direction = i16::from(self.direction.id()).to_be_bytes();
        let owner_key_i64 = i64::try_from(owner_key)
            .map_err(|_| invalid("graph owner key exceeds PostgreSQL bigint"))?
            .to_be_bytes();
        let first_chunk_i32 = i32::try_from(first_chunk_u32)
            .map_err(|_| invalid("graph owner chunk exceeds PostgreSQL integer"))?
            .to_be_bytes();
        let member_offset_i32 = i32::try_from(member_offset_u32)
            .map_err(|_| invalid("graph owner offset exceeds PostgreSQL integer"))?
            .to_be_bytes();
        let member_count_i64 = i64::try_from(member_count)
            .map_err(|_| invalid("graph owner member count exceeds PostgreSQL bigint"))?
            .to_be_bytes();
        self.owner_copy.write_row(&[
            &direction,
            &owner_key_i64,
            &first_chunk_i32,
            &member_offset_i32,
            &member_count_i64,
        ])
    }
}

fn shared_block_hash(
    object_kind: &str,
    codec: &str,
    payload: &[u8],
) -> SharedGraphResult<[u8; 32]> {
    let mut hasher = Sha256::new();
    hasher.update(BLOCK_HASH_DOMAIN);
    hasher.update(SHARED_FORMAT_VERSION.to_be_bytes());
    update_length_prefixed(&mut hasher, object_kind.as_bytes())?;
    update_length_prefixed(&mut hasher, codec.as_bytes())?;
    update_length_prefixed(&mut hasher, payload)?;
    Ok(hasher.finalize().into())
}

fn update_length_prefixed(hasher: &mut Sha256, value: &[u8]) -> SharedGraphResult<()> {
    let length = u32::try_from(value.len())
        .map_err(|_| invalid("shared block hash field exceeds uint32"))?;
    hasher.update(length.to_be_bytes());
    hasher.update(value);
    Ok(())
}

fn validate_shards(
    descriptors: &[SharedGraphShardDescriptor],
    provider_map: &DenseMap,
) -> SharedGraphResult<Vec<ValidatedShard>> {
    if descriptors.is_empty() {
        return Err(invalid(
            "shared graph conversion requires at least one complete shard",
        ));
    }
    let mut seen_shards = HashSet::new();
    let mut validated = Vec::with_capacity(descriptors.len());
    for descriptor in descriptors {
        let shard_id = descriptor.shard_id.trim();
        if shard_id.is_empty() {
            return Err(invalid("shared graph shard identity is required"));
        }
        if !seen_shards.insert(shard_id.to_owned()) {
            return Err(invalid(format!(
                "duplicate shared graph shard identity: {shard_id}"
            )));
        }
        let artifacts = [
            (&descriptor.group_npi, "provider_group_npi"),
            (&descriptor.npi_group, "provider_npi_group"),
            (&descriptor.group_provider_set, "provider_inverted"),
            (&descriptor.provider_set_group, "provider_forward"),
        ];
        let mut paths = HashSet::new();
        for (artifact, expected_name) in artifacts {
            let canonical = fs::canonicalize(&artifact.path).map_err(|error| {
                invalid(format!(
                    "membership sidecar is unavailable ({}): {error}",
                    artifact.path.display()
                ))
            })?;
            if !paths.insert(canonical) {
                return Err(invalid(format!(
                    "shared graph shard {shard_id} reuses an artifact for multiple directions"
                )));
            }
            validate_artifact_identity(&artifact.metadata, shard_id)?;
            if artifact
                .metadata
                .name
                .as_deref()
                .is_some_and(|name| name != expected_name)
            {
                return Err(invalid(format!(
                    "shared graph shard {shard_id} has the wrong {expected_name} artifact"
                )));
            }
        }
        let shard = ValidatedShard {
            shard_id: shard_id.to_owned(),
            group_npi: ValidatedArtifact::open(&descriptor.group_npi, Direction::GroupToNpi, None)?,
            npi_group: ValidatedArtifact::open(&descriptor.npi_group, Direction::NpiToGroup, None)?,
            group_provider_set: ValidatedArtifact::open(
                &descriptor.group_provider_set,
                Direction::GroupToProviderSet,
                Some(provider_map),
            )?,
            provider_set_group: ValidatedArtifact::open(
                &descriptor.provider_set_group,
                Direction::ProviderSetToGroup,
                Some(provider_map),
            )?,
        };
        validate_shard_reciprocity(&shard)?;
        validated.push(shard);
    }
    Ok(validated)
}

fn validate_artifact_identity(
    metadata: &MembershipMetadata,
    shard_id: &str,
) -> SharedGraphResult<()> {
    let source = metadata.source_shard_id.as_deref().map(str::trim);
    let alias = metadata.shard_id.as_deref().map(str::trim);
    if source.is_some() && alias.is_some() && source != alias {
        return Err(invalid(
            "membership artifact has contradictory shard identities",
        ));
    }
    if source.or(alias).is_some_and(|value| value != shard_id) {
        return Err(invalid(format!(
            "shared graph shard identity mismatch: bundle={shard_id}"
        )));
    }
    Ok(())
}

fn validate_shard_reciprocity(shard: &ValidatedShard) -> SharedGraphResult<()> {
    validate_reciprocal_pair(
        &shard.shard_id,
        "group-npi",
        &shard.group_npi,
        &shard.npi_group,
    )?;
    validate_reciprocal_pair(
        &shard.shard_id,
        "group-provider-set",
        &shard.group_provider_set,
        &shard.provider_set_group,
    )
}

fn validate_reciprocal_pair(
    shard_id: &str,
    description: &str,
    forward: &ValidatedArtifact,
    reverse: &ValidatedArtifact,
) -> SharedGraphResult<()> {
    if forward.member_count != reverse.member_count {
        return Err(invalid(format!(
            "shared graph shard {shard_id} {description} directions have different edge counts"
        )));
    }
    if forward.fingerprint != reverse.fingerprint {
        return Err(invalid(format!(
            "shared graph shard {shard_id} {description} directions are not reciprocal"
        )));
    }
    Ok(())
}

/// Convert complete strict-V3 graph shards directly into shared graph outputs.
///
/// `provider_set_key_map_path` is the PostgreSQL text COPY export ordered by
/// provider-set global ID (`hex_id<TAB>dense_key`). `output_directory` is a
/// dedicated scratch directory; the eight fixed output names must not exist.
pub fn convert_shared_provider_graph(
    shards: &[SharedGraphShardDescriptor],
    provider_set_key_map_path: impl AsRef<Path>,
    output_directory: impl AsRef<Path>,
) -> SharedGraphResult<SharedGraphConversionSummary> {
    let output_directory = output_directory.as_ref().to_path_buf();
    fs::create_dir_all(&output_directory)?;
    let output_paths: Vec<PathBuf> = OUTPUT_NAMES
        .iter()
        .map(|name| output_directory.join(name))
        .collect();
    for path in &output_paths {
        if path.exists() {
            return Err(invalid(format!(
                "shared graph output already exists: {}",
                path.display()
            )));
        }
    }
    let result = convert_shared_provider_graph_inner(
        shards,
        provider_set_key_map_path.as_ref(),
        &output_directory,
    );
    if result.is_err() {
        for path in output_paths {
            let _ = fs::remove_file(path);
        }
    }
    result
}

fn convert_shared_provider_graph_inner(
    descriptors: &[SharedGraphShardDescriptor],
    provider_set_key_map_path: &Path,
    output_directory: &Path,
) -> SharedGraphResult<SharedGraphConversionSummary> {
    let work_directory = tempfile::Builder::new()
        .prefix(".shared-graph-work-")
        .tempdir_in(output_directory)?;
    let provider_map = build_provider_set_map(
        provider_set_key_map_path,
        &work_directory.path().join("provider-set.map"),
    )?;
    let shards = validate_shards(descriptors, &provider_map)?;

    let group_map_path = output_directory.join("provider-group.map");
    let group_owner_artifacts: Vec<&ValidatedArtifact> = shards
        .iter()
        .flat_map(|shard| [&shard.group_npi, &shard.group_provider_set])
        .collect();
    let group_map = build_group_map(&group_owner_artifacts, &group_map_path)?;

    let block_copy_path = output_directory.join("graph-blocks.copy");
    let owner_copy_path = output_directory.join("graph-owners.copy");
    let group_copy_path = output_directory.join("provider-groups.copy");
    let npi_copy_path = output_directory.join("npi-scope.copy");
    let block_spool_path = output_directory.join("graph-blocks.spool");
    let owner_spool_path = output_directory.join("graph-owners.spool");
    let reference_path = output_directory.join("graph-references.run");

    let mut block_copy = PgCopyWriter::create(&block_copy_path)?;
    let mut owner_copy = PgCopyWriter::create(&owner_copy_path)?;
    let mut block_spool = BufWriter::new(File::create(&block_spool_path)?);
    let mut owner_spool = BufWriter::new(File::create(&owner_spool_path)?);
    let mut reference_spool = BufWriter::new(File::create(&reference_path)?);
    let mut support_hasher = Sha256::new();
    support_hasher.update(SUPPORT_HASH_DOMAIN);
    let mut direction_metrics = Vec::with_capacity(4);
    let mut global_merges = Vec::with_capacity(4);

    for direction in Direction::ALL {
        let direction_work = tempfile::Builder::new()
            .prefix(&format!("direction-{}-", direction.id()))
            .tempdir_in(work_directory.path())?;
        let artifacts: Vec<&ValidatedArtifact> = match direction {
            Direction::NpiToGroup => shards.iter().map(|shard| &shard.npi_group).collect(),
            Direction::GroupToNpi => shards.iter().map(|shard| &shard.group_npi).collect(),
            Direction::GroupToProviderSet => shards
                .iter()
                .map(|shard| &shard.group_provider_set)
                .collect(),
            Direction::ProviderSetToGroup => shards
                .iter()
                .map(|shard| &shard.provider_set_group)
                .collect(),
        };
        let translation_map = if direction.member_uses_group_map() {
            Some(&group_map)
        } else if direction.member_uses_provider_map() {
            Some(&provider_map)
        } else {
            None
        };
        let translations = build_translation_tables(
            &artifacts,
            translation_map,
            direction_work.path(),
            direction,
        )?;
        let mut stream = BlockStream {
            direction,
            block_copy: &mut block_copy,
            owner_copy: &mut owner_copy,
            block_spool: &mut block_spool,
            owner_spool: &mut owner_spool,
            reference_spool: &mut reference_spool,
            support_hasher: &mut support_hasher,
            group_map: &group_map,
            provider_map: &provider_map,
            translations: &translations,
            payload: Vec::with_capacity(GRAPH_CHUNK_BYTES),
            block_count: 0,
            member_count: 0,
            owner_key: None,
            owner_first_member: 0,
        };
        let merged = merge_direction(&artifacts, direction, &mut stream)?;
        let block_count = stream.finish()?;
        let raw_byte_count = merged
            .member_count
            .checked_mul(direction.member_width() as u64)
            .ok_or_else(|| invalid("graph raw byte count overflows uint64"))?;
        direction_metrics.push(SharedGraphDirectionMetrics {
            direction: direction.id(),
            object_kind: direction.object_kind(),
            member_width: direction.member_width() as u8,
            owner_count: merged.owner_count,
            member_count: merged.member_count,
            empty_owner_count: merged.empty_owner_count,
            block_count,
            raw_byte_count,
        });
        global_merges.push(merged);
        drop(translations);
        direction_work.close()?;
    }

    validate_global_reciprocity(&global_merges)?;
    block_spool.flush()?;
    owner_spool.flush()?;
    reference_spool.flush()?;
    block_copy.finish()?;
    owner_copy.finish()?;

    let mut group_copy = PgCopyWriter::create(&group_copy_path)?;
    for index in 0..group_map.count {
        let (global_id, key) = group_map.record(index)?;
        let key_i32 = i32::try_from(key)
            .map_err(|_| invalid("provider group key exceeds PostgreSQL integer"))?;
        group_copy.write_row(&[&key_i32.to_be_bytes(), &global_id])?;
        support_hasher.update(key.to_be_bytes());
        support_hasher.update(global_id);
    }
    group_copy.finish()?;

    let mut npi_copy = PgCopyWriter::create(&npi_copy_path)?;
    let npi_artifacts: Vec<&ValidatedArtifact> =
        shards.iter().map(|shard| &shard.npi_group).collect();
    let npi_count = merge_owner_ids(&npi_artifacts, |global_id, _index| {
        let npi = npi_from_global_id(global_id)?;
        let npi_i64 =
            i64::try_from(npi).map_err(|_| invalid("provider NPI exceeds PostgreSQL bigint"))?;
        npi_copy.write_row(&[&npi_i64.to_be_bytes()])?;
        support_hasher.update(npi.to_be_bytes());
        Ok(())
    })?;
    npi_copy.finish()?;

    let group_npi_input = shards.iter().try_fold(0u64, |total, shard| {
        total
            .checked_add(shard.group_npi.member_count)
            .ok_or_else(|| invalid("group-NPI input edge count overflows uint64"))
    })?;
    let group_provider_input = shards.iter().try_fold(0u64, |total, shard| {
        total
            .checked_add(shard.group_provider_set.member_count)
            .ok_or_else(|| invalid("group-provider input edge count overflows uint64"))
    })?;
    let group_npi_unique = global_merges[0].member_count;
    let group_provider_unique = global_merges[2].member_count;
    let edge_metrics = vec![
        SharedGraphEdgeMetrics {
            edge_kind: "group_npi",
            input_edge_count: group_npi_input,
            unique_edge_count: group_npi_unique,
            duplicate_edge_count: group_npi_input
                .checked_sub(group_npi_unique)
                .ok_or_else(|| invalid("group-NPI unique edge count exceeds input count"))?,
        },
        SharedGraphEdgeMetrics {
            edge_kind: "group_provider_set",
            input_edge_count: group_provider_input,
            unique_edge_count: group_provider_unique,
            duplicate_edge_count: group_provider_input
                .checked_sub(group_provider_unique)
                .ok_or_else(|| invalid("group-provider unique edge count exceeds input count"))?,
        },
    ];
    let input_byte_count = shards.iter().try_fold(0u64, |total, shard| {
        [
            &shard.group_npi,
            &shard.npi_group,
            &shard.group_provider_set,
            &shard.provider_set_group,
        ]
        .into_iter()
        .try_fold(total, |subtotal, artifact| {
            subtotal
                .checked_add(artifact.byte_count)
                .ok_or_else(|| invalid("membership input byte count overflows uint64"))
        })
    })?;
    let raw_block_byte_count = direction_metrics.iter().try_fold(0u64, |total, metric| {
        total
            .checked_add(metric.raw_byte_count)
            .ok_or_else(|| invalid("graph raw block byte count overflows uint64"))
    })?;
    let input_edge_count = group_npi_input
        .checked_add(group_provider_input)
        .ok_or_else(|| invalid("graph input edge count overflows uint64"))?;
    let unique_edge_count = group_npi_unique
        .checked_add(group_provider_unique)
        .ok_or_else(|| invalid("graph unique edge count overflows uint64"))?;
    let block_count = direction_metrics.iter().try_fold(0u64, |total, metric| {
        total
            .checked_add(metric.block_count)
            .ok_or_else(|| invalid("graph block count overflows uint64"))
    })?;
    let owner_count = direction_metrics.iter().try_fold(0u64, |total, metric| {
        total
            .checked_add(metric.owner_count)
            .ok_or_else(|| invalid("graph owner count overflows uint64"))
    })?;
    let support_digest: [u8; 32] = support_hasher.finalize().into();

    Ok(SharedGraphConversionSummary {
        scratch_directory: output_directory.to_path_buf(),
        output_directory: output_directory.to_path_buf(),
        block_copy_path,
        owner_copy_path,
        group_copy_path,
        npi_copy_path,
        block_spool_path,
        owner_spool_path,
        group_map_path,
        reference_path,
        block_count,
        owner_count,
        provider_group_count: group_map.count as u64,
        npi_count,
        support_digest,
        direction_metrics,
        edge_metrics,
        input_byte_count,
        raw_block_byte_count,
        stored_block_byte_count: raw_block_byte_count,
        integrity: SharedGraphIntegrityMetrics {
            shard_count: shards.len() as u64,
            artifact_count: (shards.len() as u64)
                .checked_mul(4)
                .ok_or_else(|| invalid("artifact count overflows uint64"))?,
            checksum_byte_count: input_byte_count,
            reciprocal_pair_count: 2,
            reciprocal_edge_count: unique_edge_count,
            input_edge_count,
            unique_edge_count,
            duplicate_edge_count: input_edge_count
                .checked_sub(unique_edge_count)
                .ok_or_else(|| invalid("graph unique edge count exceeds input count"))?,
        },
    })
}

fn validate_global_reciprocity(merges: &[DirectionMergeMetrics]) -> SharedGraphResult<()> {
    if merges.len() != 4 {
        return Err(invalid("shared graph conversion omitted a direction"));
    }
    for (left, right, description) in [(0, 1, "group-NPI"), (2, 3, "group-provider-set")] {
        if merges[left].member_count != merges[right].member_count
            || merges[left].fingerprint != merges[right].fingerprint
        {
            return Err(invalid(format!(
                "globally merged {description} membership directions are not reciprocal"
            )));
        }
    }
    Ok(())
}

fn npi_from_global_id(global_id: GlobalId) -> SharedGraphResult<u64> {
    if global_id[..8] != [0; 8] {
        return Err(invalid("provider NPI membership uses an invalid global ID"));
    }
    let npi = u64::from_be_bytes(global_id[8..].try_into().expect("fixed global ID width"));
    if npi == 0 {
        return Err(invalid("provider NPI membership uses a non-positive NPI"));
    }
    Ok(npi)
}

fn parse_sha256(value: &str) -> SharedGraphResult<[u8; 32]> {
    if value.len() != 64 {
        return Err(invalid("membership sidecar metadata requires sha256"));
    }
    let mut result = [0u8; 32];
    for (index, destination) in result.iter_mut().enumerate() {
        *destination = (decode_hex(value.as_bytes()[index * 2])? << 4)
            | decode_hex(value.as_bytes()[index * 2 + 1])?;
    }
    Ok(result)
}

fn parse_global_id_hex(value: &str) -> SharedGraphResult<GlobalId> {
    if value.len() != 32 {
        return Err(invalid(
            "provider-set dictionary IDs must be 32-character hex",
        ));
    }
    let mut result = [0u8; 16];
    for (index, destination) in result.iter_mut().enumerate() {
        *destination = (decode_hex(value.as_bytes()[index * 2])? << 4)
            | decode_hex(value.as_bytes()[index * 2 + 1])?;
    }
    Ok(result)
}

fn decode_hex(value: u8) -> SharedGraphResult<u8> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(invalid("metadata contains invalid hexadecimal text")),
    }
}

fn read_global_id(bytes: &[u8], offset: usize) -> SharedGraphResult<GlobalId> {
    let end = offset
        .checked_add(GLOBAL_ID_BYTES)
        .ok_or_else(|| invalid("global ID offset overflows"))?;
    bytes
        .get(offset..end)
        .ok_or_else(|| invalid("membership sidecar is truncated"))?
        .try_into()
        .map_err(|_| invalid("membership sidecar has an invalid global ID"))
}

fn read_u32_le(bytes: &[u8], offset: usize) -> SharedGraphResult<u32> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| invalid("uint32 offset overflows"))?;
    Ok(u32::from_le_bytes(
        bytes
            .get(offset..end)
            .ok_or_else(|| invalid("membership sidecar is truncated"))?
            .try_into()
            .expect("fixed uint32 width"),
    ))
}

fn read_u64_le(bytes: &[u8], offset: usize) -> SharedGraphResult<u64> {
    let end = offset
        .checked_add(8)
        .ok_or_else(|| invalid("uint64 offset overflows"))?;
    Ok(u64::from_le_bytes(
        bytes
            .get(offset..end)
            .ok_or_else(|| invalid("membership sidecar is truncated"))?
            .try_into()
            .expect("fixed uint64 width"),
    ))
}

fn dense_map_record(bytes: &[u8], index: usize) -> SharedGraphResult<(GlobalId, u32)> {
    let offset = index
        .checked_mul(DENSE_MAP_RECORD_BYTES)
        .ok_or_else(|| invalid("dense map offset overflows"))?;
    let global_id = read_global_id(bytes, offset)?;
    let key_end = offset + DENSE_MAP_RECORD_BYTES;
    let key = u32::from_be_bytes(
        bytes
            .get(offset + GLOBAL_ID_BYTES..key_end)
            .ok_or_else(|| invalid("dense map is truncated"))?
            .try_into()
            .expect("fixed dense key width"),
    );
    Ok((global_id, key))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{
        normalized_sidecar_entries, write_dense_member_sidecar, write_global_sidecar, GlobalId128,
    };
    use std::collections::BTreeMap;

    fn global(value: u128) -> GlobalId {
        value.to_be_bytes()
    }

    fn npi(value: u64) -> GlobalId {
        let mut result = [0u8; 16];
        result[8..].copy_from_slice(&value.to_be_bytes());
        result
    }

    fn hex(value: impl AsRef<[u8]>) -> String {
        const DIGITS: &[u8; 16] = b"0123456789abcdef";
        let value = value.as_ref();
        let mut output = String::with_capacity(value.len() * 2);
        for &byte in value {
            output.push(DIGITS[(byte >> 4) as usize] as char);
            output.push(DIGITS[(byte & 0x0f) as usize] as char);
        }
        output
    }

    fn reverse(mapping: &BTreeMap<GlobalId, Vec<GlobalId>>) -> BTreeMap<GlobalId, Vec<GlobalId>> {
        let mut result: BTreeMap<GlobalId, Vec<GlobalId>> = BTreeMap::new();
        for (owner, members) in mapping {
            for member in members {
                result.entry(*member).or_default().push(*owner);
            }
        }
        result
    }

    fn artifact(
        directory: &Path,
        file_name: &str,
        name: &str,
        shard_id: &str,
        mapping: &BTreeMap<GlobalId, Vec<GlobalId>>,
        dense: bool,
    ) -> MembershipArtifactDescriptor {
        let entries = normalized_sidecar_entries(mapping.iter().map(|(owner, members)| {
            (
                GlobalId128(*owner),
                members.iter().copied().map(GlobalId128).collect(),
            )
        }));
        let path = directory.join(file_name);
        let mut output = File::create(&path).unwrap();
        if dense {
            write_dense_member_sidecar(&mut output, &entries).unwrap();
        } else {
            write_global_sidecar(&mut output, &entries).unwrap();
        }
        drop(output);
        let payload = fs::read(&path).unwrap();
        let member_global_count = if dense {
            Some(u64::from_le_bytes(payload[20..28].try_into().unwrap()))
        } else {
            None
        };
        MembershipArtifactDescriptor {
            path,
            metadata: MembershipMetadata {
                record_format: if dense {
                    DENSE_FORMAT.to_owned()
                } else {
                    STANDARD_FORMAT.to_owned()
                },
                sha256: hex(Sha256::digest(&payload)),
                byte_count: payload.len() as u64,
                owner_count: mapping.len() as u64,
                member_count: mapping.values().map(|members| members.len() as u64).sum(),
                member_global_count,
                name: Some(name.to_owned()),
                source_shard_id: Some(shard_id.to_owned()),
                shard_id: None,
            },
        }
    }

    fn bundle(
        directory: &Path,
        shard_id: &str,
        group_npi: BTreeMap<GlobalId, Vec<GlobalId>>,
        group_provider_set: BTreeMap<GlobalId, Vec<GlobalId>>,
        dense: [bool; 4],
    ) -> SharedGraphShardDescriptor {
        fs::create_dir_all(directory).unwrap();
        let npi_group = reverse(&group_npi);
        let provider_set_group = reverse(&group_provider_set);
        SharedGraphShardDescriptor {
            shard_id: shard_id.to_owned(),
            group_npi: artifact(
                directory,
                "group-npi.bin",
                "provider_group_npi",
                shard_id,
                &group_npi,
                dense[0],
            ),
            npi_group: artifact(
                directory,
                "npi-group.bin",
                "provider_npi_group",
                shard_id,
                &npi_group,
                dense[1],
            ),
            group_provider_set: artifact(
                directory,
                "group-provider.bin",
                "provider_inverted",
                shard_id,
                &group_provider_set,
                dense[2],
            ),
            provider_set_group: artifact(
                directory,
                "provider-group.bin",
                "provider_forward",
                shard_id,
                &provider_set_group,
                dense[3],
            ),
        }
    }

    fn provider_map(path: &Path, providers: &[(GlobalId, u32)]) {
        let mut output = BufWriter::new(File::create(path).unwrap());
        for (global_id, key) in providers {
            writeln!(output, "{}\t{key}", hex(*global_id)).unwrap();
        }
        output.flush().unwrap();
    }

    fn output_bytes(summary: &SharedGraphConversionSummary) -> Vec<Vec<u8>> {
        [
            &summary.block_copy_path,
            &summary.owner_copy_path,
            &summary.group_copy_path,
            &summary.npi_copy_path,
            &summary.block_spool_path,
            &summary.owner_spool_path,
            &summary.group_map_path,
            &summary.reference_path,
        ]
        .into_iter()
        .map(|path| fs::read(path).unwrap())
        .collect()
    }

    fn file_sha256(path: &Path) -> String {
        hex(Sha256::digest(fs::read(path).unwrap()))
    }

    #[test]
    fn one_shard_emits_python_compatible_metrics_and_files() {
        let root = tempfile::tempdir().unwrap();
        let group_a = global(0xa0);
        let group_b = global(0xb0);
        let group_empty = global(0xc0);
        let provider_a = global(0x1000);
        let provider_b = global(0x2000);
        let npi_a = npi(1_000_000_000);
        let npi_b = npi(1_000_000_001);
        let npi_c = npi(1_000_000_002);
        let shard = bundle(
            &root.path().join("source"),
            "source-a",
            BTreeMap::from([
                (group_a, vec![npi_a, npi_b, npi_c]),
                (group_b, vec![npi_a]),
                (group_empty, vec![]),
            ]),
            BTreeMap::from([
                (group_a, vec![provider_a, provider_b]),
                (group_b, vec![provider_b]),
                (group_empty, vec![]),
            ]),
            [false, true, true, false],
        );
        let provider_path = root.path().join("provider.tsv");
        provider_map(&provider_path, &[(provider_a, 0), (provider_b, 1)]);
        let summary =
            convert_shared_provider_graph(&[shard], &provider_path, root.path().join("output"))
                .unwrap();

        assert_eq!(
            summary
                .direction_metrics
                .iter()
                .map(|metric| metric.member_count)
                .collect::<Vec<_>>(),
            vec![4, 4, 3, 3]
        );
        assert_eq!(
            summary
                .direction_metrics
                .iter()
                .map(|metric| metric.owner_count)
                .collect::<Vec<_>>(),
            vec![3, 3, 3, 2]
        );
        assert_eq!(
            summary
                .direction_metrics
                .iter()
                .map(|metric| metric.empty_owner_count)
                .collect::<Vec<_>>(),
            vec![0, 1, 1, 0]
        );
        assert_eq!(summary.provider_group_count, 3);
        assert_eq!(summary.npi_count, 3);
        assert_eq!(summary.integrity.unique_edge_count, 7);
        assert_eq!(summary.integrity.duplicate_edge_count, 0);
        assert!(OUTPUT_NAMES
            .iter()
            .all(|name| summary.output_directory.join(name).is_file()));
        let mut output_names: Vec<String> = fs::read_dir(&summary.output_directory)
            .unwrap()
            .map(|entry| {
                entry
                    .unwrap()
                    .file_name()
                    .into_string()
                    .expect("test output names are ASCII")
            })
            .collect();
        output_names.sort();
        let mut expected_names: Vec<String> =
            OUTPUT_NAMES.iter().map(|name| (*name).to_owned()).collect();
        expected_names.sort();
        assert_eq!(output_names, expected_names);
        let expected_python_oracle = [
            (
                "graph-blocks.copy",
                "04f6b631e9d5e1b96656a3d26484068e62dcf9855bc5e3154dfedb685f71b89b",
            ),
            (
                "graph-owners.copy",
                "2cb4b3a8c8da0c6ac4fabf36132ad1885d52f81c24225c21f4258cf7d660240f",
            ),
            (
                "provider-groups.copy",
                "a5bc59529ac2513b81441bf12d12aec443a8a45aea60c66d0506ddaf762a267e",
            ),
            (
                "npi-scope.copy",
                "ac8037c1234945cf6214dc3685a68bdb88fd815add82024de1c70efe11d2a433",
            ),
            (
                "graph-blocks.spool",
                "835cbd18c2bb12bc05716dd1db1dd965c4820fecd69b1402268deb6d941d4cf7",
            ),
            (
                "graph-owners.spool",
                "09c01914a1e554a95b1d29505edc20430b4f0147d02c4ef965b1924ad58ca486",
            ),
            (
                "provider-group.map",
                "dcb945ca68c8b15eb46e494f43e4cbed952acbb02e1e94e6d63db047ad1d30e8",
            ),
            (
                "graph-references.run",
                "b7dd7f0d4239ed055ce83dd5e28a6a97491e5a20991933b0fd0662b79b0a515a",
            ),
        ];
        for (name, expected_sha256) in expected_python_oracle {
            assert_eq!(
                file_sha256(&summary.output_directory.join(name)),
                expected_sha256,
                "{name} differs from the Python byte oracle"
            );
        }
        assert_eq!(
            hex(summary.support_digest),
            "55f0238531fb6c1a8173fa3e4cc9ab7db6ad42ee6850c0d64eb2135b6a541b58"
        );
    }

    #[test]
    fn multiple_shards_merge_and_dedupe_without_order_dependence() {
        let root = tempfile::tempdir().unwrap();
        let group_a = global(0xa0);
        let group_b = global(0xb0);
        let group_empty = global(0xc0);
        let provider_a = global(0x1000);
        let provider_b = global(0x2000);
        let npi_a = npi(1_000_000_000);
        let npi_b = npi(1_000_000_001);
        let npi_c = npi(1_000_000_002);
        let npi_d = npi(1_000_000_003);
        let first = bundle(
            &root.path().join("first"),
            "source-a",
            BTreeMap::from([(group_a, vec![npi_a, npi_b]), (group_empty, vec![])]),
            BTreeMap::from([(group_a, vec![provider_a]), (group_empty, vec![])]),
            [true, false, true, false],
        );
        let second = bundle(
            &root.path().join("second"),
            "source-b",
            BTreeMap::from([(group_a, vec![npi_b, npi_c]), (group_b, vec![npi_d])]),
            BTreeMap::from([
                (group_a, vec![provider_a, provider_b]),
                (group_b, vec![provider_b]),
            ]),
            [false, true, false, true],
        );
        let provider_path = root.path().join("provider.tsv");
        provider_map(&provider_path, &[(provider_a, 0), (provider_b, 1)]);
        let forward = convert_shared_provider_graph(
            &[first.clone(), second.clone()],
            &provider_path,
            root.path().join("forward"),
        )
        .unwrap();
        let reverse = convert_shared_provider_graph(
            &[second, first],
            &provider_path,
            root.path().join("reverse"),
        )
        .unwrap();

        assert_eq!(forward.direction_metrics, reverse.direction_metrics);
        assert_eq!(forward.edge_metrics, reverse.edge_metrics);
        assert_eq!(forward.integrity, reverse.integrity);
        assert_eq!(forward.support_digest, reverse.support_digest);
        assert_eq!(output_bytes(&forward), output_bytes(&reverse));
        assert_eq!(forward.edge_metrics[0].input_edge_count, 5);
        assert_eq!(forward.edge_metrics[0].unique_edge_count, 4);
        assert_eq!(forward.edge_metrics[1].input_edge_count, 4);
        assert_eq!(forward.edge_metrics[1].unique_edge_count, 3);
        let expected_python_oracle = [
            (
                "graph-blocks.copy",
                "563cfbffc59b4d5c56a3e1315ff74aebcc889fcf154eec682ebd24051ae2a09f",
            ),
            (
                "graph-owners.copy",
                "a31e776c346e632f0750a51a24127b157dde48efc4e75f353286540d6d24c409",
            ),
            (
                "provider-groups.copy",
                "a5bc59529ac2513b81441bf12d12aec443a8a45aea60c66d0506ddaf762a267e",
            ),
            (
                "npi-scope.copy",
                "1ebc47a225d4bbb80cb4531dff35779a6d15b4cc277f2f45dacd01f8142e07af",
            ),
            (
                "graph-blocks.spool",
                "f0b7a750ffab4fdcdd27428821062636a22a35e6ebf3566d5f98485b91b5cd3d",
            ),
            (
                "graph-owners.spool",
                "d0ad782a671f6d36bbe40e00d077f250aa4c9b929bc07071940b71ac91b70fb4",
            ),
            (
                "provider-group.map",
                "dcb945ca68c8b15eb46e494f43e4cbed952acbb02e1e94e6d63db047ad1d30e8",
            ),
            (
                "graph-references.run",
                "b9714fde63cc436576aeed76432bc4cfa799a253c89ea0947567c322e5211e6d",
            ),
        ];
        for (name, expected_sha256) in expected_python_oracle {
            assert_eq!(
                file_sha256(&forward.output_directory.join(name)),
                expected_sha256,
                "multi-shard {name} differs from the Python byte oracle"
            );
        }
        assert_eq!(
            hex(forward.support_digest),
            "8e89e31b0ee91671f4d03ca4c60f83345b67000562358baa2c7baebdc4e3af53"
        );
    }

    #[test]
    fn empty_graph_emits_valid_empty_copy_streams() {
        let root = tempfile::tempdir().unwrap();
        let shard = bundle(
            &root.path().join("source"),
            "source-a",
            BTreeMap::new(),
            BTreeMap::new(),
            [false, true, false, true],
        );
        let provider_path = root.path().join("provider.tsv");
        provider_map(&provider_path, &[]);

        let summary =
            convert_shared_provider_graph(&[shard], &provider_path, root.path().join("output"))
                .unwrap();

        assert_eq!(summary.block_count, 0);
        assert_eq!(summary.owner_count, 0);
        assert_eq!(summary.provider_group_count, 0);
        assert_eq!(summary.npi_count, 0);
        assert_eq!(summary.integrity.input_edge_count, 0);
        assert!(summary
            .direction_metrics
            .iter()
            .all(|metric| metric.member_count == 0 && metric.owner_count == 0));
        for path in [
            &summary.block_copy_path,
            &summary.owner_copy_path,
            &summary.group_copy_path,
            &summary.npi_copy_path,
        ] {
            assert_eq!(fs::read(path).unwrap().len(), PG_COPY_HEADER.len() + 2);
        }
    }

    #[test]
    fn owner_locator_crosses_exact_64kib_block_boundary() {
        let root = tempfile::tempdir().unwrap();
        let group_a = global(0xa0);
        let group_b = global(0xb0);
        let provider = global(0x1000);
        let npis: Vec<GlobalId> = (0..8193).map(|index| npi(1_000_000_000 + index)).collect();
        let shard = bundle(
            &root.path().join("source"),
            "source-a",
            BTreeMap::from([(group_a, vec![npis[0]]), (group_b, npis.clone())]),
            BTreeMap::from([(group_a, vec![provider]), (group_b, vec![provider])]),
            [true; 4],
        );
        let provider_path = root.path().join("provider.tsv");
        provider_map(&provider_path, &[(provider, 0)]);

        let summary =
            convert_shared_provider_graph(&[shard], &provider_path, root.path().join("output"))
                .unwrap();

        let group_to_npi = &summary.direction_metrics[1];
        assert_eq!(group_to_npi.member_count, 8194);
        assert_eq!(group_to_npi.block_count, 2);
        assert_eq!(group_to_npi.raw_byte_count, 8194 * 8);
        let owners = fs::read(&summary.owner_spool_path).unwrap();
        let group_b_owner = owners
            .chunks_exact(25)
            .find(|record| {
                record[0] == Direction::GroupToNpi.id()
                    && u64::from_be_bytes(record[1..9].try_into().unwrap()) == 1
            })
            .unwrap();
        assert_eq!(
            u32::from_be_bytes(group_b_owner[9..13].try_into().unwrap()),
            0
        );
        assert_eq!(
            u32::from_be_bytes(group_b_owner[13..17].try_into().unwrap()),
            8
        );
        assert_eq!(
            u64::from_be_bytes(group_b_owner[17..25].try_into().unwrap()),
            8193
        );
    }

    #[test]
    fn malformed_sidecar_is_rejected_before_output_survives() {
        let root = tempfile::tempdir().unwrap();
        let group = global(0xa0);
        let provider = global(0x1000);
        let npi = npi(1_000_000_001);
        let mut shard = bundle(
            &root.path().join("source"),
            "source-a",
            BTreeMap::from([(group, vec![npi])]),
            BTreeMap::from([(group, vec![provider])]),
            [false; 4],
        );
        shard.group_npi.metadata.sha256 = "00".repeat(32);
        let provider_path = root.path().join("provider.tsv");
        provider_map(&provider_path, &[(provider, 0)]);
        let output = root.path().join("output");

        let error = convert_shared_provider_graph(&[shard], &provider_path, &output)
            .unwrap_err()
            .to_string();

        assert!(error.contains("checksum mismatch"));
        assert!(OUTPUT_NAMES.iter().all(|name| !output.join(name).exists()));
    }

    #[test]
    fn truncated_layout_is_rejected_even_with_matching_metadata_digest() {
        let root = tempfile::tempdir().unwrap();
        let group = global(0xa0);
        let provider = global(0x1000);
        let npi = npi(1_000_000_001);
        let mut shard = bundle(
            &root.path().join("source"),
            "source-a",
            BTreeMap::from([(group, vec![npi])]),
            BTreeMap::from([(group, vec![provider])]),
            [false; 4],
        );
        let path = shard.group_npi.path.clone();
        let mut payload = fs::read(&path).unwrap();
        payload.pop();
        fs::write(&path, &payload).unwrap();
        shard.group_npi.metadata.byte_count = payload.len() as u64;
        shard.group_npi.metadata.sha256 = hex(Sha256::digest(&payload));
        let provider_path = root.path().join("provider.tsv");
        provider_map(&provider_path, &[(provider, 0)]);

        let error =
            convert_shared_provider_graph(&[shard], &provider_path, root.path().join("output"))
                .unwrap_err()
                .to_string();

        assert!(error.contains("layout size mismatch"));
    }

    #[test]
    fn unsorted_dense_dictionary_is_rejected() {
        let root = tempfile::tempdir().unwrap();
        let group = global(0xa0);
        let provider = global(0x1000);
        let npi_a = npi(1_000_000_001);
        let npi_b = npi(1_000_000_002);
        let mut shard = bundle(
            &root.path().join("source"),
            "source-a",
            BTreeMap::from([(group, vec![npi_a, npi_b])]),
            BTreeMap::from([(group, vec![provider])]),
            [true; 4],
        );
        let path = shard.group_npi.path.clone();
        let mut payload = fs::read(&path).unwrap();
        let dictionary_start = DENSE_HEADER_BYTES + OWNER_RECORD_BYTES;
        let (left, right) = payload[dictionary_start..dictionary_start + 32].split_at_mut(16);
        left.swap_with_slice(right);
        fs::write(&path, &payload).unwrap();
        shard.group_npi.metadata.sha256 = hex(Sha256::digest(&payload));
        let provider_path = root.path().join("provider.tsv");
        provider_map(&provider_path, &[(provider, 0)]);

        let error =
            convert_shared_provider_graph(&[shard], &provider_path, root.path().join("output"))
                .unwrap_err()
                .to_string();

        assert!(error.contains("dictionary must be sorted and unique"));
    }

    #[test]
    fn mismatched_reciprocal_direction_is_rejected() {
        let root = tempfile::tempdir().unwrap();
        let group_a = global(0xa0);
        let group_b = global(0xb0);
        let provider = global(0x1000);
        let npi_a = npi(1_000_000_001);
        let npi_b = npi(1_000_000_002);
        let mut shard = bundle(
            &root.path().join("source"),
            "source-a",
            BTreeMap::from([(group_a, vec![npi_a]), (group_b, vec![npi_b])]),
            BTreeMap::from([(group_a, vec![provider])]),
            [false; 4],
        );
        let bad_reverse = BTreeMap::from([(npi_a, vec![group_b]), (npi_b, vec![group_a])]);
        shard.npi_group = artifact(
            &root.path().join("source"),
            "bad-reverse.bin",
            "provider_npi_group",
            "source-a",
            &bad_reverse,
            true,
        );
        let provider_path = root.path().join("provider.tsv");
        provider_map(&provider_path, &[(provider, 0)]);

        let error =
            convert_shared_provider_graph(&[shard], &provider_path, root.path().join("output"))
                .unwrap_err()
                .to_string();

        assert!(error.contains("not reciprocal"));
    }

    #[test]
    fn unknown_provider_set_id_is_rejected() {
        let root = tempfile::tempdir().unwrap();
        let group = global(0xa0);
        let provider = global(0x1000);
        let other_provider = global(0x2000);
        let npi = npi(1_000_000_001);
        let shard = bundle(
            &root.path().join("source"),
            "source-a",
            BTreeMap::from([(group, vec![npi])]),
            BTreeMap::from([(group, vec![provider])]),
            [true; 4],
        );
        let provider_path = root.path().join("provider.tsv");
        provider_map(&provider_path, &[(other_provider, 0)]);

        let error =
            convert_shared_provider_graph(&[shard], &provider_path, root.path().join("output"))
                .unwrap_err()
                .to_string();

        assert!(error.contains("absent from the authoritative provider-set map"));
    }

    #[test]
    fn block_hash_matches_python_format_two_vector() {
        let hash = shared_block_hash("graph_npi_groups_v1", "none", &[1, 2, 3]).unwrap();
        assert_eq!(
            hex(hash),
            "d1df104a844b74ff6a7f450d5075ce3669d1117d3081ea6d57bc245578c21261"
        );
    }
}
