#[path = "../src/registry_ptg_graph_witness.rs"]
mod witness;

use sha2::{Digest, Sha256};
use witness::*;

fn decode_u32_le(bytes: &[u8]) -> Result<Vec<u32>, &'static str> {
    ptg2_scanner::decode_u32_le(bytes)
}

#[derive(Clone)]
struct Block {
    kind: String,
    key: u64,
    fragment: u32,
    count: u64,
    bytes: Vec<u8>,
}

fn retained_hash(kind: &str, bytes: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"PTG2V3BLOCK\x01\x00\x02");
    for field in [kind.as_bytes(), b"none", bytes] {
        hash.update(u32::try_from(field.len()).unwrap().to_be_bytes());
        hash.update(field);
    }
    hash.finalize().into()
}

impl Block {
    fn cas(&self) -> CasBlock<'_> {
        CasBlock {
            block_hash: retained_hash(&self.kind, &self.bytes),
            format_version: 2,
            object_kind: &self.kind,
            codec: "none",
            entry_count: self.count,
            raw_byte_count: self.bytes.len() as u64,
            stored_byte_count: self.bytes.len() as u64,
            payload: &self.bytes,
        }
    }
}

#[derive(Clone)]
struct Fixture {
    context: BatchContext,
    manifest: RelationManifest,
    blocks: Vec<Block>,
    heavy: Vec<HeavyOwner>,
}

fn context() -> BatchContext {
    BatchContext {
        graph_identity: GraphIdentity {
            snapshot_key: 11,
            layout_generation: "shared_blocks_v4".into(),
            layout_mapping_sha256: [1; 32],
            map_sha256: [2; 32],
            finalizer_map_sha256: [3; 32],
            source_assignments_sha256: [4; 32],
        },
        after_ordinal: 0,
        last_ordinal: 4,
        row_count: 4,
    }
}

fn manifest(
    base: u64,
    count: u64,
    logical: u64,
    vector: u64,
    member_bytes: u64,
    locator_bytes: u64,
) -> RelationManifest {
    RelationManifest {
        snapshot_key: 11,
        relation: "group_npis_exact".into(),
        member_object_kind: "v4_group_npis_exact_members_v1".into(),
        locator_object_kind: "v4_group_npis_exact_locators_v1".into(),
        owner_base: base,
        owner_count: count,
        logical_member_count: logical,
        vector_member_count: vector,
        member_width: 4,
        member_page_bytes: member_bytes,
        locator_page_bytes: locator_bytes,
        locator_owner_span: locator_bytes / 12,
    }
}

fn locator(offset: u64, count: u32) -> Vec<u8> {
    [
        offset.to_le_bytes().as_slice(),
        count.to_le_bytes().as_slice(),
    ]
    .concat()
}

fn selected(values: &[(u32, u32)]) -> Vec<u8> {
    values
        .iter()
        .flat_map(|(owner, member)| [owner.to_be_bytes(), member.to_be_bytes()].concat())
        .collect()
}

fn regular() -> Fixture {
    let manifest = manifest(5, 3, 5, 5, 8, 24);
    let blocks = vec![
        Block {
            kind: manifest.locator_object_kind.clone(),
            key: 5,
            fragment: 0,
            count: 2,
            bytes: [locator(0, 3), locator(3, 1)].concat(),
        },
        Block {
            kind: manifest.locator_object_kind.clone(),
            key: 7,
            fragment: 0,
            count: 1,
            bytes: locator(4, 1),
        },
        Block {
            kind: manifest.member_object_kind.clone(),
            key: 0,
            fragment: 0,
            count: 2,
            bytes: [2u32.to_le_bytes(), 5u32.to_le_bytes()].concat(),
        },
        Block {
            kind: manifest.member_object_kind.clone(),
            key: 2,
            fragment: 0,
            count: 2,
            bytes: [9u32.to_le_bytes(), 5u32.to_le_bytes()].concat(),
        },
        Block {
            kind: manifest.member_object_kind.clone(),
            key: 4,
            fragment: 0,
            count: 1,
            bytes: 7u32.to_le_bytes().to_vec(),
        },
    ];
    Fixture {
        context: context(),
        manifest,
        blocks,
        heavy: vec![],
    }
}

fn heavy() -> Fixture {
    let manifest = manifest(8, 1, 3, 0, 40, 12);
    let owner = HeavyOwner {
        snapshot_key: 11,
        relation: manifest.relation.clone(),
        owner_key: 8,
        object_kind: "v4_group_npis_exact_heavy_bitmap_v1".into(),
        member_count: 3,
        member_base: 14,
        member_span: 23,
        fragment_count: 4,
    };
    let mut logical = b"PTG2V4BM".to_vec();
    for word in [8u32, 14, 23, 3] {
        logical.extend_from_slice(&word.to_le_bytes());
    }
    logical.extend_from_slice(&[1, 1, 64]);
    let mut blocks = vec![Block {
        kind: manifest.locator_object_kind.clone(),
        key: 8,
        fragment: 0,
        count: 1,
        bytes: locator(0, 0),
    }];
    for (fragment, content) in logical.chunks(8).enumerate() {
        let count = if fragment == 3 { 3 } else { 0 };
        let mut bytes = b"PTG2V4BF".to_vec();
        for word in [8u32, 14, 23, 3, fragment as u32, count] {
            bytes.extend_from_slice(&word.to_le_bytes());
        }
        bytes.extend_from_slice(content);
        blocks.push(Block {
            kind: owner.object_kind.clone(),
            key: 8,
            fragment: fragment as u32,
            count: u64::from(count),
            bytes,
        });
    }
    Fixture {
        context: context(),
        manifest,
        blocks,
        heavy: vec![owner],
    }
}

fn pack_blocks(blocks: &[Block]) -> Vec<Block> {
    let mut groups = std::collections::BTreeMap::<&str, Vec<&Block>>::new();
    for block in blocks {
        groups.entry(&block.kind).or_default().push(block);
    }
    groups
        .into_iter()
        .map(|(kind, mut blocks)| {
            blocks.sort_by_key(|block| (block.key, block.fragment));
            let mut bytes = b"PTG4MAP1".to_vec();
            bytes.extend_from_slice(&1u16.to_be_bytes());
            bytes.extend_from_slice(&(kind.len() as u16).to_be_bytes());
            bytes.extend_from_slice(&(blocks.len() as u32).to_be_bytes());
            bytes.extend_from_slice(kind.as_bytes());
            bytes.resize(80, 0);
            for block in &blocks {
                bytes.extend_from_slice(&block.key.to_be_bytes());
                bytes.extend_from_slice(&block.fragment.to_be_bytes());
                bytes.extend_from_slice(&block.count.to_be_bytes());
                bytes.extend_from_slice(&retained_hash(kind, &block.bytes));
            }
            Block {
                kind: "snapshot_coordinate_map_v1".into(),
                key: 0,
                fragment: 0,
                count: blocks.len() as u64,
                bytes,
            }
        })
        .collect()
}

fn maps<'a>(storage: &'a [Block]) -> Vec<MapPack<'a>> {
    storage
        .iter()
        .map(|block| {
            let bytes = &block.bytes;
            let kind_len = u16::from_be_bytes(bytes[10..12].try_into().unwrap()) as usize;
            let rows = &bytes[80..];
            let first = &rows[..52];
            let last = &rows[rows.len() - 52..];
            MapPack {
                snapshot_key: 11,
                object_kind: std::str::from_utf8(&bytes[16..16 + kind_len]).unwrap(),
                pack_no: 0,
                first_block_key: u64::from_be_bytes(first[..8].try_into().unwrap()),
                first_fragment_no: u32::from_be_bytes(first[8..12].try_into().unwrap()),
                last_block_key: u64::from_be_bytes(last[..8].try_into().unwrap()),
                last_fragment_no: u32::from_be_bytes(last[8..12].try_into().unwrap()),
                coordinate_count: block.count as u32,
                pack_entry_count: rows
                    .as_chunks::<52>()
                    .0
                    .iter()
                    .map(|row| u64::from_be_bytes(row[12..20].try_into().unwrap()))
                    .sum(),
                block: block.cas(),
            }
        })
        .collect()
}

impl Fixture {
    fn verify(&self, edges: &[u8]) -> Result<GraphBatchProof, &'static str> {
        let storage = pack_blocks(&self.blocks);
        verify_group_npi_batch(
            &self.context,
            &self.context,
            &self.manifest,
            edges,
            &maps(&storage),
            &self.blocks.iter().map(Block::cas).collect::<Vec<_>>(),
            &self.heavy,
        )
    }
}

#[test]
fn regular_spill_and_page_owner_restart_are_exact() {
    let fixture = regular();
    let edges = selected(&[(5, 2), (5, 9), (6, 5), (7, 7)]);
    let proof = fixture.verify(&edges).unwrap();
    assert_eq!(proof.verified_edge_count, 4);
    assert_eq!(proof.missing_edge_count, 0);
    assert_eq!(proof.checked_member_count, 5);
    assert_eq!(proof.authenticated_graph_page_count, 5);
    assert_eq!(
        proof.selected_edges_sha256,
        <[u8; 32]>::from(Sha256::digest(&edges))
    );
    let serialized = serde_json::to_value(proof).unwrap();
    assert!(serialized.get("selected_edges").is_none());
}

#[test]
fn planners_return_only_batched_page_coordinates() {
    let fixture = regular();
    let edges = selected(&[(5, 9), (7, 7)]);
    let first = plan_group_npi_locator_pages(&fixture.context, &fixture.manifest, &edges).unwrap();
    assert_eq!(first.owner_keys, [5, 7]);
    assert_eq!(first.coordinates.len(), 2);
    let locator_blocks: Vec<_> = fixture
        .blocks
        .iter()
        .filter(|block| block.kind.contains("locators"))
        .cloned()
        .collect();
    let storage = pack_blocks(&locator_blocks);
    let plan = plan_group_npi_member_pages(
        &fixture.context,
        &fixture.manifest,
        &edges,
        &maps(&storage),
        &locator_blocks.iter().map(Block::cas).collect::<Vec<_>>(),
        &[],
    )
    .unwrap();
    assert_eq!(
        plan.coordinates
            .iter()
            .map(|coordinate| coordinate.1)
            .collect::<Vec<_>>(),
        [0, 4]
    );
    let mut accumulated = locator_blocks;
    accumulated.extend(
        fixture
            .blocks
            .iter()
            .filter(|block| block.kind.contains("members") && [0, 4].contains(&block.key))
            .cloned(),
    );
    let storage = pack_blocks(&accumulated);
    let second = plan_group_npi_member_pages(
        &fixture.context,
        &fixture.manifest,
        &edges,
        &maps(&storage),
        &accumulated.iter().map(Block::cas).collect::<Vec<_>>(),
        &[],
    )
    .unwrap();
    assert_eq!(
        second
            .coordinates
            .iter()
            .map(|coordinate| coordinate.1)
            .collect::<Vec<_>>(),
        [2]
    );
    accumulated.push(fixture.blocks[3].clone());
    let storage = pack_blocks(&accumulated);
    let done = plan_group_npi_member_pages(
        &fixture.context,
        &fixture.manifest,
        &edges,
        &maps(&storage),
        &accumulated.iter().map(Block::cas).collect::<Vec<_>>(),
        &[],
    )
    .unwrap();
    assert!(done.coordinates.is_empty());
}

#[test]
fn sparse_search_does_not_materialize_a_large_owner_vector() {
    let count = 1_000_000_000u32;
    let midpoint = count / 2;
    let manifest = manifest(9, 1, u64::from(count), u64::from(count), 16_384, 12);
    let page_key = u64::from(midpoint) / 4096 * 4096;
    let blocks = vec![
        Block {
            kind: manifest.locator_object_kind.clone(),
            key: 9,
            fragment: 0,
            count: 1,
            bytes: locator(0, count),
        },
        Block {
            kind: manifest.member_object_kind.clone(),
            key: page_key,
            fragment: 0,
            count: 4096,
            bytes: (page_key..page_key + 4096)
                .flat_map(|value| (value as u32).to_le_bytes())
                .collect(),
        },
    ];
    let fixture = Fixture {
        context: context(),
        manifest,
        blocks,
        heavy: vec![],
    };
    let proof = fixture.verify(&selected(&[(9, midpoint)])).unwrap();
    assert_eq!(proof.authenticated_graph_page_count, 2);
    assert_eq!(proof.checked_member_count, 4096);
    assert!(proof.decoded_bytes < 5_000_000);
}

#[test]
fn retained_cas_hash_matches_fixed_format_fixture() {
    let bytes = [1u32.to_le_bytes(), 2u32.to_le_bytes()].concat();
    let hash = retained_hash("v4_group_npis_exact_members_v1", &bytes);
    let hex: String = hash.iter().map(|byte| format!("{byte:02x}")).collect();
    assert_eq!(
        hex,
        "efef34cbff174cde08c851d3226530b75b20cbc39d9093f3da4b12b9afbaa6f2"
    );
}

#[test]
fn bitmap_fragments_with_split_logical_header_are_exact() {
    let fixture = heavy();
    let proof = fixture
        .verify(&selected(&[(8, 14), (8, 22), (8, 36)]))
        .unwrap();
    assert_eq!(proof.checked_member_count, 3);
    assert_eq!(proof.authenticated_graph_page_count, 5);
    assert_eq!(
        fixture.verify(&selected(&[(8, 15)])).unwrap_err(),
        "registry_ptg_graph_edge_missing"
    );
    assert_eq!(
        fixture.verify(&selected(&[(8, 37)])).unwrap_err(),
        "registry_ptg_graph_edge_missing"
    );
}

#[test]
fn another_groups_member_does_not_prove_an_edge() {
    let fixture = regular();
    assert_eq!(
        fixture
            .verify(&selected(&[(5, 7), (6, 5), (7, 7)]))
            .unwrap_err(),
        "registry_ptg_graph_edge_missing"
    );
}

#[test]
fn selection_rejects_duplicate_unsorted_signed_overflow_and_framing() {
    let fixture = regular();
    for edges in [
        vec![],
        vec![0; 7],
        selected(&[(5, 2), (5, 2)]),
        selected(&[(6, 5), (5, 2)]),
        selected(&[(u32::MAX, 2)]),
        selected(&[(5, u32::MAX)]),
        selected(&[(4, 2)]),
        selected(&[(8, 2)]),
    ] {
        assert!(fixture.verify(&edges).is_err());
    }
    let oversized = vec![0; (MAX_SELECTED_EDGES + 1) * 8];
    assert!(fixture.verify(&oversized).is_err());
}

#[test]
fn each_graph_identity_and_ordinal_change_fails() {
    let fixture = regular();
    let edges = selected(&[(5, 2)]);
    let storage = pack_blocks(&fixture.blocks);
    let packs = maps(&storage);
    let blocks = fixture.blocks.iter().map(Block::cas).collect::<Vec<_>>();
    for index in 0..9 {
        let mut changed = fixture.context.clone();
        match index {
            0 => changed.graph_identity.snapshot_key += 1,
            1 => changed.graph_identity.layout_generation = "shared_blocks_v3".into(),
            2 => changed.graph_identity.layout_mapping_sha256[0] ^= 1,
            3 => changed.graph_identity.map_sha256[0] ^= 1,
            4 => changed.graph_identity.finalizer_map_sha256[0] ^= 1,
            5 => changed.graph_identity.source_assignments_sha256[0] ^= 1,
            6 => changed.after_ordinal += 1,
            7 => changed.last_ordinal += 1,
            _ => changed.row_count += 1,
        }
        assert_eq!(
            verify_group_npi_batch(
                &fixture.context,
                &changed,
                &fixture.manifest,
                &edges,
                &packs,
                &blocks,
                &[]
            )
            .unwrap_err(),
            "registry_ptg_graph_context_changed"
        );
    }
}

#[test]
fn authenticated_corrupt_locator_or_owner_slice_fails() {
    let edges = selected(&[(5, 2), (5, 9), (6, 5), (7, 7)]);
    for index in 0..5 {
        let mut fixture = regular();
        match index {
            0 => fixture.blocks[0].bytes[..8].copy_from_slice(&u64::MAX.to_le_bytes()),
            1 => fixture.blocks[0].bytes[8..12].copy_from_slice(&u32::MAX.to_le_bytes()),
            2 => fixture.blocks[0].bytes[12..20].copy_from_slice(&2u64.to_le_bytes()),
            3 => fixture.blocks[2].bytes[4..8].copy_from_slice(&2u32.to_le_bytes()),
            _ => {
                fixture.blocks[3].bytes[..4].copy_from_slice(&1u32.to_le_bytes());
            }
        }
        assert!(fixture.verify(&edges).is_err());
    }
}

#[test]
fn graph_page_missing_duplicate_misaligned_or_truncated_fails() {
    let edges = selected(&[(5, 2), (5, 9), (6, 5), (7, 7)]);
    for index in 0..4 {
        let mut fixture = regular();
        match index {
            0 => {
                fixture.blocks.remove(3);
            }
            1 => fixture.blocks.push(fixture.blocks[2].clone()),
            2 => fixture.blocks[3].key = 3,
            _ => {
                fixture.blocks[3].bytes.pop();
            }
        }
        assert!(fixture.verify(&edges).is_err());
    }
}

#[test]
fn bitmap_owner_fragment_count_padding_and_framing_corruption_fails() {
    let edges = selected(&[(8, 14), (8, 22), (8, 36)]);
    for index in 0..9 {
        let mut fixture = heavy();
        match index {
            0 => fixture.heavy[0].owner_key = 7,
            1 => fixture.heavy[0].member_span = u32::MAX,
            2 => fixture.heavy[0].fragment_count = u32::MAX,
            3 => fixture.blocks[4].bytes[24..28].copy_from_slice(&0u32.to_le_bytes()),
            4 => fixture.blocks[4].bytes[28..32].copy_from_slice(&2u32.to_le_bytes()),
            5 => fixture.blocks[4].bytes[34] |= 128,
            6 => fixture.blocks[1].bytes[32] = b'X',
            7 => fixture.blocks[0].bytes[8..12].copy_from_slice(&1u32.to_le_bytes()),
            _ => {
                fixture.blocks.remove(2);
            }
        }
        assert!(fixture.verify(&edges).is_err());
    }
}

#[test]
fn cas_metadata_and_payload_authentication_are_independent() {
    let fixture = regular();
    let edges = selected(&[(5, 2), (5, 9), (6, 5), (7, 7)]);
    let storage = pack_blocks(&fixture.blocks);
    let packs = maps(&storage);
    for index in 0..7 {
        let mut blocks = fixture.blocks.iter().map(Block::cas).collect::<Vec<_>>();
        match index {
            0 => blocks[0].block_hash[0] ^= 1,
            1 => blocks[0].format_version = 1,
            2 => blocks[0].codec = "gzip",
            3 => blocks[0].object_kind = "wrong",
            4 => blocks[0].entry_count += 1,
            5 => blocks[0].raw_byte_count += 1,
            _ => blocks[0].stored_byte_count += 1,
        }
        assert!(verify_group_npi_batch(
            &fixture.context,
            &fixture.context,
            &fixture.manifest,
            &edges,
            &packs,
            &blocks,
            &[]
        )
        .is_err());
    }
}

#[test]
fn map_pack_scope_metadata_overlap_and_header_fail_closed() {
    let fixture = regular();
    let edges = selected(&[(5, 2), (5, 9), (6, 5), (7, 7)]);
    let storage = pack_blocks(&fixture.blocks);
    let blocks = fixture.blocks.iter().map(Block::cas).collect::<Vec<_>>();
    for index in 0..8 {
        let mut packs = maps(&storage);
        match index {
            0 => packs[0].snapshot_key += 1,
            1 => packs[0].coordinate_count += 1,
            2 => packs[0].first_block_key += 1,
            3 => packs[0].last_fragment_no += 1,
            4 => packs[0].pack_entry_count += 1,
            5 => packs[0].block.block_hash[0] ^= 1,
            6 => packs.push(packs[0].clone()),
            _ => {
                let mut duplicate = packs[0].clone();
                duplicate.pack_no = 1;
                packs.push(duplicate);
            }
        }
        assert!(verify_group_npi_batch(
            &fixture.context,
            &fixture.context,
            &fixture.manifest,
            &edges,
            &packs,
            &blocks,
            &[]
        )
        .is_err());
    }
    let mut corrupt = storage.clone();
    corrupt[0].bytes[8..10].copy_from_slice(&2u16.to_be_bytes());
    assert!(verify_group_npi_batch(
        &fixture.context,
        &fixture.context,
        &fixture.manifest,
        &edges,
        &maps(&corrupt),
        &blocks,
        &[]
    )
    .is_err());
}

#[test]
fn only_the_actual_retained_text_generation_is_supported() {
    let edges = selected(&[(5, 2), (5, 9), (6, 5), (7, 7)]);
    for generation in [
        "",
        "shared_blocks_v3",
        "shared_blocks_v4 ",
        "shared_blocks_v4\0",
        &"x".repeat(65),
    ] {
        let mut fixture = regular();
        fixture.context.graph_identity.layout_generation = generation.into();
        assert_eq!(
            fixture.verify(&edges).unwrap_err(),
            "registry_ptg_graph_invalid"
        );
    }
    let fixture = regular();
    let proof = fixture.verify(&edges).unwrap();
    assert_eq!(
        proof.context.graph_identity.layout_generation,
        "shared_blocks_v4"
    );
    let mut document = serde_json::to_value(&fixture.context).unwrap();
    document["graph_identity"]["layout_generation"] = serde_json::json!(3);
    assert!(serde_json::from_value::<BatchContext>(document).is_err());
}

#[test]
fn manifest_extremes_and_allocation_budgets_fail_before_decode() {
    let edges = selected(&[(5, 2)]);
    for index in 0..7 {
        let mut fixture = regular();
        match index {
            0 => fixture.manifest.owner_base = u64::MAX,
            1 => fixture.manifest.owner_count = u64::MAX,
            2 => fixture.manifest.vector_member_count = u64::MAX,
            3 => fixture.manifest.member_page_bytes = 5,
            4 => fixture.manifest.locator_owner_span = 0,
            5 => fixture.manifest.member_width = 8,
            _ => fixture.manifest.member_page_bytes = MAX_PAGE_BYTES as u64 + 4,
        }
        assert!(fixture.verify(&edges).is_err());
    }
    let fixture = regular();
    let storage = pack_blocks(&fixture.blocks);
    let mut packs = maps(&storage);
    packs[0].coordinate_count = MAX_COORDINATES as u32;
    assert_eq!(
        verify_group_npi_batch(
            &fixture.context,
            &fixture.context,
            &fixture.manifest,
            &edges,
            &packs,
            &[],
            &[]
        )
        .unwrap_err(),
        "registry_ptg_graph_budget"
    );
    let bytes = vec![0; MAX_PAGE_BYTES + 1];
    let mut block = fixture.blocks[0].cas();
    block.payload = &bytes;
    assert_eq!(
        verify_group_npi_batch(
            &fixture.context,
            &fixture.context,
            &fixture.manifest,
            &edges,
            &[],
            &[block],
            &[]
        )
        .unwrap_err(),
        "registry_ptg_graph_budget"
    );
    let bytes = vec![0; MAX_PAGE_BYTES];
    block.payload = &bytes;
    let blocks = vec![block; MAX_BATCH_BYTES / MAX_PAGE_BYTES + 1];
    assert_eq!(
        verify_group_npi_batch(
            &fixture.context,
            &fixture.context,
            &fixture.manifest,
            &edges,
            &[],
            &blocks,
            &[]
        )
        .unwrap_err(),
        "registry_ptg_graph_budget"
    );
}
