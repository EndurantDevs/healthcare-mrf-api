//! Bounded checks of selected edges in retained V4 group/NPI graph pages.
//!
//! The caller must read the actual sealed snapshot, complete map/finalizer roots,
//! source assignments and producer authorization in one stable transaction.
//! Matching supplied context and content hashes proves codec consistency only;
//! it does not authorize the supplied graph or admit a producer.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

pub const MAX_SELECTED_EDGES: usize = 4096;
pub const MAX_PAGE_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_BATCH_BYTES: usize = 256 * 1024 * 1024;
pub const MAX_COORDINATES: usize = 65_536;
pub const MAX_GRAPH_PAGES: usize = 16_384;
const LOCATOR_KIND: &str = "v4_group_npis_exact_locators_v1";
const MEMBER_KIND: &str = "v4_group_npis_exact_members_v1";
const BITMAP_KIND: &str = "v4_group_npis_exact_heavy_bitmap_v1";
const MAP_KIND: &str = "snapshot_coordinate_map_v1";
type Result<T> = std::result::Result<T, &'static str>;
type Coordinate = (String, u64, u32);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphIdentity {
    pub snapshot_key: u64,
    pub layout_generation: String,
    pub layout_mapping_sha256: [u8; 32],
    pub map_sha256: [u8; 32],
    pub finalizer_map_sha256: [u8; 32],
    pub source_assignments_sha256: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchContext {
    pub graph_identity: GraphIdentity,
    pub after_ordinal: u64,
    pub last_ordinal: u64,
    pub row_count: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelationManifest {
    pub snapshot_key: u64,
    pub relation: String,
    pub member_object_kind: String,
    pub locator_object_kind: String,
    pub owner_base: u64,
    pub owner_count: u64,
    pub logical_member_count: u64,
    pub vector_member_count: u64,
    pub member_width: u64,
    pub member_page_bytes: u64,
    pub locator_page_bytes: u64,
    pub locator_owner_span: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeavyOwner {
    pub snapshot_key: u64,
    pub relation: String,
    pub owner_key: u32,
    pub object_kind: String,
    pub member_count: u32,
    pub member_base: u32,
    pub member_span: u32,
    pub fragment_count: u32,
}

#[derive(Clone, Copy, Debug)]
pub struct CasBlock<'a> {
    pub block_hash: [u8; 32],
    pub format_version: i16,
    pub object_kind: &'a str,
    pub codec: &'a str,
    pub entry_count: u64,
    pub raw_byte_count: u64,
    pub stored_byte_count: u64,
    pub payload: &'a [u8],
}

#[derive(Clone, Debug)]
pub struct MapPack<'a> {
    pub snapshot_key: u64,
    pub object_kind: &'a str,
    pub pack_no: u32,
    pub first_block_key: u64,
    pub first_fragment_no: u32,
    pub last_block_key: u64,
    pub last_fragment_no: u32,
    pub coordinate_count: u32,
    pub pack_entry_count: u64,
    pub block: CasBlock<'a>,
}

/// Read coordinates and selected owner keys only; never exposes NPI pairs.
#[derive(Debug, Serialize)]
pub struct GraphReadPlan {
    pub owner_keys: Vec<u32>,
    pub coordinates: Vec<(String, u64, u32)>,
}

#[derive(Debug, Serialize)]
pub struct GraphBatchProof {
    pub contract: &'static str,
    pub context: BatchContext,
    pub selected_edges_sha256: [u8; 32],
    pub edge_count: u32,
    pub verified_edge_count: u32,
    pub missing_edge_count: u32,
    pub selected_owner_count: u32,
    pub map_pack_count: u32,
    pub authenticated_graph_page_count: u32,
    pub authenticated_raw_bytes: u64,
    pub decoded_bytes: u64,
    pub checked_member_count: u64,
}

fn invalid<T>() -> Result<T> {
    Err("registry_ptg_graph_invalid")
}

fn add_bounded(total: &mut usize, amount: usize) -> Result<()> {
    *total = total
        .checked_add(amount)
        .ok_or("registry_ptg_graph_budget")?;
    if *total > MAX_BATCH_BYTES {
        return Err("registry_ptg_graph_budget");
    }
    Ok(())
}

fn validate_manifest(context: &BatchContext, manifest: &RelationManifest) -> Result<()> {
    let owner_end = manifest.owner_base.checked_add(manifest.owner_count);
    if context.graph_identity.snapshot_key == 0
        || context.graph_identity.snapshot_key > i64::MAX as u64
        || context.graph_identity.layout_generation.len() > 64
        || context.graph_identity.layout_generation != "shared_blocks_v4"
        || context.row_count == 0
        || context.row_count as usize > MAX_SELECTED_EDGES
        || context.last_ordinal > 1_000_000
        || context.last_ordinal.checked_sub(context.after_ordinal)
            != Some(u64::from(context.row_count))
        || manifest.snapshot_key != context.graph_identity.snapshot_key
        || manifest.relation != "group_npis_exact"
        || manifest.member_object_kind != MEMBER_KIND
        || manifest.locator_object_kind != LOCATOR_KIND
        || owner_end.is_none_or(|end| end > i32::MAX as u64 + 1)
        || manifest.owner_count == 0
        || manifest.vector_member_count > manifest.logical_member_count
        || manifest.logical_member_count > i64::MAX as u64
        || manifest.member_width != 4
        || !(4..=MAX_PAGE_BYTES as u64).contains(&manifest.member_page_bytes)
        || !manifest.member_page_bytes.is_multiple_of(4)
        || !(12..=MAX_PAGE_BYTES as u64).contains(&manifest.locator_page_bytes)
        || !manifest.locator_page_bytes.is_multiple_of(12)
        || manifest.locator_page_bytes / 12 != manifest.locator_owner_span
    {
        return invalid();
    }
    Ok(())
}

fn edges(
    payload: &[u8],
    context: &BatchContext,
    manifest: &RelationManifest,
) -> Result<Vec<(u32, u32)>> {
    validate_manifest(context, manifest)?;
    if payload.is_empty()
        || payload.len() > MAX_SELECTED_EDGES * 8
        || !payload.len().is_multiple_of(8)
        || payload.len() / 8 > context.row_count as usize
    {
        return invalid();
    }
    let mut selected = Vec::with_capacity(payload.len() / 8);
    for field in payload.as_chunks::<8>().0 {
        let owner = u32::from_be_bytes(field[..4].try_into().unwrap());
        let member = u32::from_be_bytes(field[4..].try_into().unwrap());
        if owner > i32::MAX as u32
            || member > i32::MAX as u32
            || u64::from(owner) < manifest.owner_base
            || u64::from(owner) >= manifest.owner_base + manifest.owner_count
            || selected
                .last()
                .is_some_and(|previous| *previous >= (owner, member))
        {
            return invalid();
        }
        selected.push((owner, member));
    }
    Ok(selected)
}

/// Plan the first batched read of locator pages and heavy-owner metadata.
pub fn plan_group_npi_locator_pages(
    context: &BatchContext,
    manifest: &RelationManifest,
    selected_edges: &[u8],
) -> Result<GraphReadPlan> {
    let selected = edges(selected_edges, context, manifest)?;
    let owners: BTreeSet<_> = selected.iter().map(|edge| edge.0).collect();
    let coordinates: BTreeSet<_> = owners
        .iter()
        .map(|owner| {
            let key = manifest.owner_base
                + (u64::from(*owner) - manifest.owner_base) / manifest.locator_owner_span
                    * manifest.locator_owner_span;
            (LOCATOR_KIND.to_owned(), key, 0)
        })
        .collect();
    Ok(GraphReadPlan {
        owner_keys: owners.into_iter().collect(),
        coordinates: coordinates.into_iter().collect(),
    })
}
// Match the retained V3 CAS framing shared by the V4 compiler and reader.
fn block_hash(block: &CasBlock<'_>) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"PTG2V3BLOCK\x01");
    digest.update(block.format_version.to_be_bytes());
    for field in [
        block.object_kind.as_bytes(),
        block.codec.as_bytes(),
        block.payload,
    ] {
        digest.update((field.len() as u32).to_be_bytes());
        digest.update(field);
    }
    digest.finalize().into()
}
fn authenticate(block: &CasBlock<'_>, kind: &str, cap: usize) -> Result<()> {
    if block.format_version != 2
        || block.object_kind != kind
        || block.codec != "none"
        || block.payload.len() > cap
        || block.raw_byte_count != block.payload.len() as u64
        || block.stored_byte_count != block.raw_byte_count
        || block.entry_count > i64::MAX as u64
        || block_hash(block) != block.block_hash
    {
        return invalid();
    }
    Ok(())
}
#[derive(Clone, Copy)]
struct Reference {
    hash: [u8; 32],
    count: u64,
}
struct Pages<'a> {
    coordinates: BTreeMap<Coordinate, Reference>,
    blocks: BTreeMap<[u8; 32], CasBlock<'a>>,
    used: BTreeSet<[u8; 32]>,
    member_pages: BTreeMap<u64, Vec<u32>>,
    checked_owner_pages: BTreeSet<(u32, u64)>,
    checked_members: u64,
    raw_bytes: usize,
    decoded_bytes: usize,
}
impl<'a> Pages<'a> {
    fn new(
        snapshot: u64,
        packs: &[MapPack<'a>],
        blocks: &[CasBlock<'a>],
        heavy: &[HeavyOwner],
    ) -> Result<Self> {
        if packs.len() > MAX_GRAPH_PAGES
            || blocks.len() > MAX_GRAPH_PAGES
            || heavy.len() > MAX_SELECTED_EDGES
        {
            return Err("registry_ptg_graph_budget");
        }
        let (mut raw_bytes, mut decoded_bytes, mut count) = (0, 0, 0usize);
        for pack in packs {
            if pack.block.payload.len() > MAX_PAGE_BYTES || pack.coordinate_count > 65_536 {
                return Err("registry_ptg_graph_budget");
            }
            count = count
                .checked_add(pack.coordinate_count as usize)
                .ok_or("registry_ptg_graph_budget")?;
            add_bounded(&mut raw_bytes, pack.block.payload.len())?;
        }
        if count > MAX_COORDINATES {
            return Err("registry_ptg_graph_budget");
        }
        add_bounded(
            &mut decoded_bytes,
            count * (std::mem::size_of::<Reference>() + std::mem::size_of::<Coordinate>() + 64),
        )?;
        add_bounded(
            &mut decoded_bytes,
            blocks.len() * 256
                + packs.len() * 128
                + MAX_SELECTED_EDGES * 256
                + MAX_GRAPH_PAGES * 192,
        )?;
        for block in blocks {
            if block.payload.len() > MAX_PAGE_BYTES {
                return Err("registry_ptg_graph_budget");
            }
            add_bounded(&mut raw_bytes, block.payload.len())?;
        }
        for owner in heavy {
            add_bounded(
                &mut decoded_bytes,
                24 + (owner.member_span as usize).div_ceil(8),
            )?;
        }
        let mut pages = Self {
            coordinates: BTreeMap::new(),
            blocks: BTreeMap::new(),
            used: BTreeSet::new(),
            member_pages: BTreeMap::new(),
            checked_owner_pages: BTreeSet::new(),
            checked_members: 0,
            raw_bytes,
            decoded_bytes,
        };
        let mut pack_numbers = BTreeSet::new();
        for pack in packs {
            if pack.snapshot_key != snapshot
                || ![LOCATOR_KIND, MEMBER_KIND, BITMAP_KIND].contains(&pack.object_kind)
                || pack.pack_no > i32::MAX as u32
                || !pack_numbers.insert((pack.object_kind, pack.pack_no))
            {
                return invalid();
            }
            pages.add_pack(pack)?;
        }
        for block in blocks {
            if ![LOCATOR_KIND, MEMBER_KIND, BITMAP_KIND].contains(&block.object_kind) {
                return invalid();
            }
            authenticate(block, block.object_kind, MAX_PAGE_BYTES)?;
            if pages.blocks.insert(block.block_hash, *block).is_some() {
                return invalid();
            }
        }
        Ok(pages)
    }
    fn add_pack(&mut self, pack: &MapPack<'_>) -> Result<()> {
        authenticate(&pack.block, MAP_KIND, MAX_PAGE_BYTES)?;
        let payload = pack.block.payload;
        if payload.len() < 80
            || &payload[..8] != b"PTG4MAP1"
            || u16::from_be_bytes(payload[8..10].try_into().unwrap()) != 1
        {
            return invalid();
        }
        let kind_len = u16::from_be_bytes(payload[10..12].try_into().unwrap()) as usize;
        let count = u32::from_be_bytes(payload[12..16].try_into().unwrap());
        if !(1..=64).contains(&kind_len)
            || &payload[16..16 + kind_len] != pack.object_kind.as_bytes()
            || payload[16 + kind_len..80].iter().any(|byte| *byte != 0)
            || count == 0
            || count > 65_536
            || count != pack.coordinate_count
            || payload.len() != 80 + count as usize * 52
            || pack.block.entry_count != u64::from(count)
        {
            return invalid();
        }
        let (mut previous, mut first, mut total) = (None, None, 0u64);
        for row in payload[80..].as_chunks::<52>().0 {
            let key = u64::from_be_bytes(row[..8].try_into().unwrap());
            let fragment = u32::from_be_bytes(row[8..12].try_into().unwrap());
            let entry_count = u64::from_be_bytes(row[12..20].try_into().unwrap());
            if key > i64::MAX as u64
                || fragment > i32::MAX as u32
                || entry_count > i64::MAX as u64
                || previous.is_some_and(|pair| pair >= (key, fragment))
            {
                return invalid();
            }
            first.get_or_insert((key, fragment));
            previous = Some((key, fragment));
            total = total
                .checked_add(entry_count)
                .ok_or("registry_ptg_graph_invalid")?;
            let reference = Reference {
                hash: row[20..52].try_into().unwrap(),
                count: entry_count,
            };
            if self
                .coordinates
                .insert((pack.object_kind.to_owned(), key, fragment), reference)
                .is_some()
            {
                return invalid();
            }
        }
        if first != Some((pack.first_block_key, pack.first_fragment_no))
            || previous != Some((pack.last_block_key, pack.last_fragment_no))
            || total != pack.pack_entry_count
        {
            return invalid();
        }
        Ok(())
    }
    fn get(&mut self, kind: &str, key: u64, fragment: u32, cap: usize) -> Result<CasBlock<'a>> {
        let reference = self
            .coordinates
            .get(&(kind.to_owned(), key, fragment))
            .ok_or("registry_ptg_graph_page_missing")?;
        let block = *self
            .blocks
            .get(&reference.hash)
            .ok_or("registry_ptg_graph_page_missing")?;
        if block.object_kind != kind
            || block.entry_count != reference.count
            || block.payload.len() > cap
        {
            return invalid();
        }
        self.used.insert(reference.hash);
        Ok(block)
    }
    fn members(&mut self, key: u64, manifest: &RelationManifest) -> Result<&[u32]> {
        if !self.member_pages.contains_key(&key) {
            let block = self.get(MEMBER_KIND, key, 0, manifest.member_page_bytes as usize)?;
            let expected = (manifest.member_page_bytes / 4).min(manifest.vector_member_count - key);
            if block.entry_count != expected || block.payload.len() != expected as usize * 4 {
                return invalid();
            }
            add_bounded(&mut self.decoded_bytes, block.payload.len())?;
            let members =
                crate::decode_u32_le(block.payload).map_err(|_| "registry_ptg_graph_invalid")?;
            self.member_pages.insert(key, members);
        }
        Ok(self.member_pages.get(&key).unwrap())
    }
    fn has(&self, kind: &str, key: u64, fragment: u32) -> bool {
        self.coordinates
            .get(&(kind.to_owned(), key, fragment))
            .is_some_and(|reference| self.blocks.contains_key(&reference.hash))
    }
}
fn heavy_owners<'a>(
    context: &BatchContext,
    manifest: &RelationManifest,
    selected: &[(u32, u32)],
    heavy: &'a [HeavyOwner],
) -> Result<BTreeMap<u32, &'a HeavyOwner>> {
    let owners: BTreeSet<_> = selected.iter().map(|edge| edge.0).collect();
    let mut result = BTreeMap::new();
    let mut fragments = 0usize;
    let mut members = 0u64;
    for owner in heavy {
        fragments = fragments
            .checked_add(owner.fragment_count as usize)
            .ok_or("registry_ptg_graph_budget")?;
        members = members
            .checked_add(u64::from(owner.member_count))
            .ok_or("registry_ptg_graph_invalid")?;
        if owner.snapshot_key != context.graph_identity.snapshot_key
            || owner.relation != manifest.relation
            || owner.object_kind != BITMAP_KIND
            || !owners.contains(&owner.owner_key)
            || owner.member_span == 0
            || owner.member_count == 0
            || owner.member_count > owner.member_span
            || u64::from(owner.member_base) + u64::from(owner.member_span) > i32::MAX as u64 + 1
            || owner.fragment_count == 0
            || owner.fragment_count > i32::MAX as u32
            || result.insert(owner.owner_key, owner).is_some()
        {
            return invalid();
        }
    }
    if fragments > MAX_GRAPH_PAGES {
        return Err("registry_ptg_graph_budget");
    }
    if members > manifest.logical_member_count - manifest.vector_member_count {
        return invalid();
    }
    Ok(result)
}
fn locators(
    manifest: &RelationManifest,
    selected: &[(u32, u32)],
    pages: &mut Pages<'_>,
) -> Result<BTreeMap<u32, (u64, u32)>> {
    let mut result = BTreeMap::new();
    let mut checked_pages = BTreeSet::new();
    for (owner, _) in selected {
        if result.contains_key(owner) {
            continue;
        }
        let key = manifest.owner_base
            + (u64::from(*owner) - manifest.owner_base) / manifest.locator_owner_span
                * manifest.locator_owner_span;
        let block = pages.get(LOCATOR_KIND, key, 0, manifest.locator_page_bytes as usize)?;
        let count = manifest
            .locator_owner_span
            .min(manifest.owner_base + manifest.owner_count - key);
        if block.entry_count != count || block.payload.len() != count as usize * 12 {
            return invalid();
        }
        if checked_pages.insert(key) {
            let mut previous_end = None;
            for locator in block.payload.as_chunks::<12>().0 {
                let offset = u64::from_le_bytes(locator[..8].try_into().unwrap());
                let width = u32::from_le_bytes(locator[8..12].try_into().unwrap());
                let end = offset
                    .checked_add(u64::from(width))
                    .ok_or("registry_ptg_graph_invalid")?;
                if u64::from(width) > i32::MAX as u64 + 1
                    || end > manifest.vector_member_count
                    || previous_end.is_some_and(|previous| previous != offset)
                {
                    return invalid();
                }
                previous_end = Some(end);
            }
        }
        let index = (u64::from(*owner) - key) as usize * 12;
        result.insert(
            *owner,
            (
                u64::from_le_bytes(block.payload[index..index + 8].try_into().unwrap()),
                u32::from_le_bytes(block.payload[index + 8..index + 12].try_into().unwrap()),
            ),
        );
    }
    Ok(result)
}
fn member_plan(
    locators: &BTreeMap<u32, (u64, u32)>,
    heavy: &BTreeMap<u32, &HeavyOwner>,
    pages: &Pages<'_>,
) -> Result<GraphReadPlan> {
    let mut coordinates = BTreeSet::new();
    let mut previous_end = 0;
    for (owner, (offset, count)) in locators {
        if *offset < previous_end {
            return invalid();
        }
        previous_end = offset + u64::from(*count);
        if let Some(bitmap) = heavy.get(owner) {
            if *count != 0 {
                return invalid();
            }
            for fragment in 0..bitmap.fragment_count {
                if !pages.has(BITMAP_KIND, u64::from(*owner), fragment) {
                    coordinates.insert((BITMAP_KIND.to_owned(), u64::from(*owner), fragment));
                }
            }
        }
        if coordinates.len() > MAX_GRAPH_PAGES {
            return Err("registry_ptg_graph_budget");
        }
    }
    Ok(GraphReadPlan {
        owner_keys: locators.keys().copied().collect(),
        coordinates: coordinates.into_iter().collect(),
    })
}
/// Return the next missing search pages; accumulate unique inputs until empty.
pub fn plan_group_npi_member_pages(
    context: &BatchContext,
    manifest: &RelationManifest,
    selected_edges: &[u8],
    map_packs: &[MapPack<'_>],
    blocks: &[CasBlock<'_>],
    heavy: &[HeavyOwner],
) -> Result<GraphReadPlan> {
    let selected = edges(selected_edges, context, manifest)?;
    let snapshot = context.graph_identity.snapshot_key;
    let mut pages = Pages::new(snapshot, map_packs, blocks, heavy)?;
    let heavy = heavy_owners(context, manifest, &selected, heavy)?;
    let locators = locators(manifest, &selected, &mut pages)?;
    let mut plan = member_plan(&locators, &heavy, &pages)?;
    let missing = search_regular_edges(&selected, manifest, &locators, &heavy, &mut pages)?;
    plan.coordinates.extend(missing);
    if plan.coordinates.len() > MAX_GRAPH_PAGES {
        return Err("registry_ptg_graph_budget");
    }
    for owner in heavy.values() {
        if (0..owner.fragment_count)
            .all(|fragment| pages.has(BITMAP_KIND, u64::from(owner.owner_key), fragment))
        {
            bitmap(owner, manifest, &mut pages)?;
        }
    }
    if pages.used.len() != pages.blocks.len() {
        return invalid();
    }
    Ok(plan)
}
fn bitmap(
    owner: &HeavyOwner,
    manifest: &RelationManifest,
    pages: &mut Pages<'_>,
) -> Result<Vec<u8>> {
    let size = 24 + (owner.member_span as usize).div_ceil(8);
    let page_bytes = manifest.member_page_bytes as usize;
    let content_capacity = page_bytes
        .checked_sub(32)
        .filter(|cap| *cap > 0)
        .ok_or("registry_ptg_graph_invalid")?;
    if size.div_ceil(content_capacity) != owner.fragment_count as usize {
        return invalid();
    }
    let mut logical = Vec::with_capacity(size);
    let mut observed_count = 0u64;
    for fragment in 0..owner.fragment_count {
        let block = pages.get(
            BITMAP_KIND,
            u64::from(owner.owner_key),
            fragment,
            page_bytes,
        )?;
        let expected = content_capacity.min(size - logical.len());
        let payload = block.payload;
        if payload.len() != 32 + expected || &payload[..8] != b"PTG2V4BF" {
            return invalid();
        }
        let header: [u32; 6] = std::array::from_fn(|index| {
            u32::from_le_bytes(payload[8 + index * 4..12 + index * 4].try_into().unwrap())
        });
        if header
            != [
                owner.owner_key,
                owner.member_base,
                owner.member_span,
                owner.member_count,
                fragment,
                block.entry_count as u32,
            ]
            || block.entry_count > u32::MAX as u64
        {
            return invalid();
        }
        let bitmap_start = 24usize.saturating_sub(logical.len()).min(expected);
        let count: u64 = payload[32 + bitmap_start..]
            .iter()
            .map(|byte| u64::from(byte.count_ones()))
            .sum();
        if count != block.entry_count {
            return invalid();
        }
        observed_count = observed_count
            .checked_add(count)
            .ok_or("registry_ptg_graph_invalid")?;
        logical.extend_from_slice(&payload[32..]);
    }
    let header: [u32; 4] = std::array::from_fn(|index| {
        u32::from_le_bytes(logical[8 + index * 4..12 + index * 4].try_into().unwrap())
    });
    if &logical[..8] != b"PTG2V4BM"
        || header
            != [
                owner.owner_key,
                owner.member_base,
                owner.member_span,
                owner.member_count,
            ]
        || observed_count != u64::from(owner.member_count)
        || (!owner.member_span.is_multiple_of(8)
            && logical[size - 1] >> (owner.member_span % 8) != 0)
    {
        return invalid();
    }
    Ok(logical)
}
fn search_regular_edges(
    selected: &[(u32, u32)],
    manifest: &RelationManifest,
    locators: &BTreeMap<u32, (u64, u32)>,
    heavy: &BTreeMap<u32, &HeavyOwner>,
    pages: &mut Pages<'_>,
) -> Result<BTreeSet<Coordinate>> {
    let mut missing = BTreeSet::new();
    let capacity = manifest.member_page_bytes / 4;
    for (owner, target) in selected {
        if heavy.contains_key(owner) {
            continue;
        }
        let (offset, count) = locators[owner];
        let (mut low, mut high, mut found, mut waiting) =
            (offset, offset + u64::from(count), false, false);
        let mut rounds = 0;
        while low < high {
            if rounds >= 32 {
                return invalid();
            }
            rounds += 1;
            let midpoint = low + (high - low) / 2;
            let key = midpoint / capacity * capacity;
            if !pages.has(MEMBER_KIND, key, 0) {
                missing.insert((MEMBER_KIND.to_owned(), key, 0));
                waiting = true;
                break;
            }
            let check = !pages.checked_owner_pages.contains(&(*owner, key));
            if check {
                add_bounded(&mut pages.decoded_bytes, 64)?;
                pages.checked_owner_pages.insert((*owner, key));
            }
            let members = pages.members(key, manifest)?;
            let start = offset.max(key) - key;
            let end = (offset + u64::from(count)).min(key + members.len() as u64) - key;
            let span = &members[start as usize..end as usize];
            if check
                && (span.iter().any(|member| *member > i32::MAX as u32)
                    || span.windows(2).any(|pair| pair[0] >= pair[1]))
            {
                return invalid();
            }
            let value = members[(midpoint - key) as usize];
            if check {
                pages.checked_members += end - start;
            }
            match value.cmp(target) {
                std::cmp::Ordering::Less => low = midpoint + 1,
                std::cmp::Ordering::Greater => high = midpoint,
                std::cmp::Ordering::Equal => {
                    found = true;
                    break;
                }
            }
        }
        if !found && !waiting {
            return Err("registry_ptg_graph_edge_missing");
        }
    }
    Ok(missing)
}
/// Verify every selected edge; malformed, missing or unsupported pages fail closed.
pub fn verify_group_npi_batch(
    expected_context: &BatchContext,
    actual_context: &BatchContext,
    manifest: &RelationManifest,
    selected_edges: &[u8],
    map_packs: &[MapPack<'_>],
    blocks: &[CasBlock<'_>],
    heavy: &[HeavyOwner],
) -> Result<GraphBatchProof> {
    if expected_context != actual_context {
        return Err("registry_ptg_graph_context_changed");
    }
    let selected = edges(selected_edges, actual_context, manifest)?;
    let snapshot = actual_context.graph_identity.snapshot_key;
    let mut pages = Pages::new(snapshot, map_packs, blocks, heavy)?;
    let heavy = heavy_owners(actual_context, manifest, &selected, heavy)?;
    let locators = locators(manifest, &selected, &mut pages)?;
    let plan = member_plan(&locators, &heavy, &pages)?;
    let missing = search_regular_edges(&selected, manifest, &locators, &heavy, &mut pages)?;
    if !plan.coordinates.is_empty() || !missing.is_empty() {
        return Err("registry_ptg_graph_page_missing");
    }
    for (owner, bitmap_owner) in &heavy {
        let first = selected.partition_point(|edge| edge.0 < *owner);
        let end = selected.partition_point(|edge| edge.0 <= *owner);
        let logical = bitmap(bitmap_owner, manifest, &mut pages)?;
        for (_, member) in &selected[first..end] {
            let relative = member
                .checked_sub(bitmap_owner.member_base)
                .filter(|relative| *relative < bitmap_owner.member_span)
                .ok_or("registry_ptg_graph_edge_missing")?;
            if logical[24 + relative as usize / 8] & (1 << (relative % 8)) == 0 {
                return Err("registry_ptg_graph_edge_missing");
            }
        }
        pages.checked_members = pages
            .checked_members
            .checked_add(u64::from(bitmap_owner.member_count))
            .ok_or("registry_ptg_graph_invalid")?;
    }
    if pages.checked_members > manifest.logical_member_count
        || pages.used.len() != pages.blocks.len()
    {
        return invalid();
    }
    Ok(GraphBatchProof {
        contract: "registry_ptg_graph_batch.v1",
        context: actual_context.clone(),
        selected_edges_sha256: Sha256::digest(selected_edges).into(),
        edge_count: selected.len() as u32,
        verified_edge_count: selected.len() as u32,
        missing_edge_count: 0,
        selected_owner_count: locators.len() as u32,
        map_pack_count: map_packs.len() as u32,
        authenticated_graph_page_count: pages.used.len() as u32,
        authenticated_raw_bytes: pages.raw_bytes as u64,
        decoded_bytes: pages.decoded_bytes as u64,
        checked_member_count: pages.checked_members,
    })
}
