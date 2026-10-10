use ptg2_scanner::registry_ptg_graph_python as boundary;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn context() -> Value {
    json!({"graph_identity": {
        "snapshot_key": 11, "layout_generation": "shared_blocks_v4",
        "layout_mapping_sha256": "01".repeat(32), "map_sha256": "02".repeat(32),
        "finalizer_map_sha256": "03".repeat(32), "source_assignments_sha256": "04".repeat(32)},
        "after_ordinal": 0, "last_ordinal": 1, "row_count": 1})
}

fn manifest() -> Value {
    json!({"snapshot_key": 11, "relation": "group_npis_exact",
        "member_object_kind": "v4_group_npis_exact_members_v1",
        "locator_object_kind": "v4_group_npis_exact_locators_v1",
        "owner_base": 5, "owner_count": 1, "logical_member_count": 3,
        "vector_member_count": 3, "member_width": 4, "member_page_bytes": 12,
        "locator_page_bytes": 12, "locator_owner_span": 1})
}

fn selected() -> Vec<u8> {
    [5u32.to_be_bytes(), 5u32.to_be_bytes()].concat()
}

fn cas(kind: &str, count: u64, bytes: &[u8], index: usize) -> Value {
    let mut digest = Sha256::new();
    digest.update(b"PTG2V3BLOCK\x01\x00\x02");
    for field in [kind.as_bytes(), b"none", bytes] {
        digest.update((field.len() as u32).to_be_bytes());
        digest.update(field);
    }
    json!({"block_hash": hex(&digest.finalize()), "format_version": 2,
        "object_kind": kind, "codec": "none", "entry_count": count,
        "raw_byte_count": bytes.len(), "stored_byte_count": bytes.len(), "payload_index": index})
}

fn pack(kind: &str, key: u64, count: u64, block: &Value, index: usize) -> (Value, Vec<u8>) {
    pack_rows(kind, &[(key, 0, count, block.clone())], index)
}

fn pack_rows(kind: &str, rows: &[(u64, u32, u64, Value)], index: usize) -> (Value, Vec<u8>) {
    let mut bytes = b"PTG4MAP1".to_vec();
    bytes.extend_from_slice(&1u16.to_be_bytes());
    bytes.extend_from_slice(&(kind.len() as u16).to_be_bytes());
    bytes.extend_from_slice(&(rows.len() as u32).to_be_bytes());
    bytes.extend_from_slice(kind.as_bytes());
    bytes.resize(80, 0);
    for (key, fragment, count, block) in rows {
        bytes.extend_from_slice(&key.to_be_bytes());
        bytes.extend_from_slice(&fragment.to_be_bytes());
        bytes.extend_from_slice(&count.to_be_bytes());
        let hash = block["block_hash"].as_str().unwrap();
        for index in 0..32 {
            bytes.push(u8::from_str_radix(&hash[index * 2..index * 2 + 2], 16).unwrap());
        }
    }
    let first = &rows[0];
    let last = rows.last().unwrap();
    let total: u64 = rows.iter().map(|row| row.2).sum();
    let metadata = json!({"snapshot_key":11, "object_kind":kind, "pack_no":0,
        "first_block_key":first.0, "first_fragment_no":first.1, "last_block_key":last.0,
        "last_fragment_no":last.1, "coordinate_count":rows.len(), "pack_entry_count":total,
        "block":cas("snapshot_coordinate_map_v1", rows.len() as u64, &bytes, index)});
    (metadata, bytes)
}

fn fixture(complete: bool, verification: bool) -> (Value, Vec<Vec<u8>>) {
    let locator = [0u64.to_le_bytes().as_slice(), 3u32.to_le_bytes().as_slice()].concat();
    let members = [2u32.to_le_bytes(), 5u32.to_le_bytes(), 9u32.to_le_bytes()].concat();
    let locator_kind = "v4_group_npis_exact_locators_v1";
    let member_kind = "v4_group_npis_exact_members_v1";
    let locator_block = cas(locator_kind, 1, &locator, if complete { 2 } else { 1 });
    let member_block = cas(member_kind, 3, &members, 3);
    let (locator_pack, locator_map) = pack(locator_kind, 5, 1, &locator_block, 0);
    let (member_pack, member_map) = pack(member_kind, 0, 3, &member_block, 1);
    let (packs, blocks, payloads) = if complete {
        (
            vec![locator_pack, member_pack],
            vec![locator_block, member_block],
            vec![locator_map, member_map, locator, members],
        )
    } else {
        (
            vec![locator_pack],
            vec![locator_block],
            vec![locator_map, locator],
        )
    };
    let mut metadata =
        json!({"manifest": manifest(), "map_packs":packs, "blocks":blocks, "heavy":[]});
    if verification {
        metadata["expected_context"] = context();
        metadata["actual_context"] = context();
    } else {
        metadata["context"] = context();
    }
    (metadata, payloads)
}

fn bytes(value: &Value) -> Vec<u8> {
    serde_json::to_vec(value).unwrap()
}

#[test]
fn retained_wire_plans_missing_pages_and_requires_final_verification() {
    let locator = json!({"context":context(), "manifest":manifest()});
    let plan: Value =
        serde_json::from_slice(&boundary::plan_locator(&bytes(&locator), &selected()).unwrap())
            .unwrap();
    assert_eq!(
        plan,
        json!({"owner_keys":[5], "coordinates":[["v4_group_npis_exact_locators_v1",5,0]]})
    );
    let (metadata, payloads) = fixture(false, false);
    let borrowed: Vec<_> = payloads.iter().map(Vec::as_slice).collect();
    let plan: Value = serde_json::from_slice(
        &boundary::plan_members(&bytes(&metadata), &selected(), &borrowed).unwrap(),
    )
    .unwrap();
    assert_eq!(
        plan["coordinates"],
        json!([["v4_group_npis_exact_members_v1", 0, 0]])
    );
    let (metadata, payloads) = fixture(false, true);
    let borrowed: Vec<_> = payloads.iter().map(Vec::as_slice).collect();
    assert_eq!(
        boundary::verify(&bytes(&metadata), &selected(), &borrowed).unwrap_err(),
        "registry_ptg_graph_page_missing"
    );
    let (metadata, payloads) = fixture(true, false);
    let borrowed: Vec<_> = payloads.iter().map(Vec::as_slice).collect();
    let plan: Value = serde_json::from_slice(
        &boundary::plan_members(&bytes(&metadata), &selected(), &borrowed).unwrap(),
    )
    .unwrap();
    assert_eq!(plan["coordinates"], json!([]));
    assert!(boundary::verify(&bytes(&plan), &selected(), &borrowed).is_err());
}

#[test]
fn final_proof_uses_actual_hex_identity_and_aggregate_counts_only() {
    let (mut metadata, payloads) = fixture(true, true);
    let borrowed: Vec<_> = payloads.iter().map(Vec::as_slice).collect();
    let result = boundary::verify(&bytes(&metadata), &selected(), &borrowed).unwrap();
    let proof: Value = serde_json::from_slice(&result).unwrap();
    assert_eq!(proof["context"], context());
    assert_eq!(
        proof["selected_edges_sha256"],
        hex(&Sha256::digest(selected()))
    );
    assert_eq!(proof["verified_edge_count"], 1);
    assert_eq!(proof["missing_edge_count"], 0);
    assert!(proof.get("rows").is_none());
    assert_eq!(result, bytes(&proof));
    metadata["expected_context"]["graph_identity"]["source_assignments_sha256"] =
        json!("ff".repeat(32));
    assert_eq!(
        boundary::verify(&bytes(&metadata), &selected(), &borrowed).unwrap_err(),
        "registry_ptg_graph_context_changed"
    );
    let expected = metadata["expected_context"].take();
    metadata["expected_context"] = metadata["actual_context"].take();
    metadata["actual_context"] = expected;
    assert_eq!(
        boundary::verify(&bytes(&metadata), &selected(), &borrowed).unwrap_err(),
        "registry_ptg_graph_context_changed"
    );
}

#[test]
fn retained_heavy_owner_metadata_and_fragment_buffers_verify() {
    let locator_kind = "v4_group_npis_exact_locators_v1";
    let bitmap_kind = "v4_group_npis_exact_heavy_bitmap_v1";
    let locator = [0u64.to_le_bytes().as_slice(), 0u32.to_le_bytes().as_slice()].concat();
    let locator_block = cas(locator_kind, 1, &locator, 2);
    let (locator_pack, locator_map) = pack(locator_kind, 5, 1, &locator_block, 0);
    let mut logical = b"PTG2V4BM".to_vec();
    for value in [5u32, 0, 8, 3] {
        logical.extend_from_slice(&value.to_le_bytes());
    }
    logical.push(0b00100110);
    let mut blocks = vec![locator_block];
    let mut fragments = Vec::new();
    let mut rows = Vec::new();
    for (fragment, content) in logical.chunks(8).enumerate() {
        let count = if fragment == 3 { 3 } else { 0 };
        let mut payload = b"PTG2V4BF".to_vec();
        for value in [5u32, 0, 8, 3, fragment as u32, count] {
            payload.extend_from_slice(&value.to_le_bytes());
        }
        payload.extend_from_slice(content);
        let block = cas(bitmap_kind, u64::from(count), &payload, fragment + 3);
        rows.push((5, fragment as u32, u64::from(count), block.clone()));
        blocks.push(block);
        fragments.push(payload);
    }
    let (bitmap_pack, bitmap_map) = pack_rows(bitmap_kind, &rows, 1);
    let mut payloads = vec![locator_map, bitmap_map, locator];
    payloads.extend(fragments);
    let mut relation = manifest();
    relation["vector_member_count"] = json!(0);
    relation["member_page_bytes"] = json!(40);
    let mut metadata = json!({"expected_context":context(), "actual_context":context(),
        "manifest":relation, "map_packs":[locator_pack,bitmap_pack], "blocks":blocks,
        "heavy":[{"snapshot_key":11,"relation":"group_npis_exact","owner_key":5,
        "object_kind":bitmap_kind,"member_count":3,"member_base":0,"member_span":8,"fragment_count":4}]});
    let borrowed: Vec<_> = payloads.iter().map(Vec::as_slice).collect();
    let proof: Value = serde_json::from_slice(
        &boundary::verify(&bytes(&metadata), &selected(), &borrowed).unwrap(),
    )
    .unwrap();
    assert_eq!(proof["authenticated_graph_page_count"], 5);
    assert_eq!(proof["checked_member_count"], 3);
    metadata["heavy"][0]["extra"] = json!(1);
    assert!(boundary::verify(&bytes(&metadata), &selected(), &borrowed).is_err());
}

#[test]
fn metadata_refuses_wrong_hash_generation_unknown_duplicate_and_type() {
    let original = json!({"context":context(), "manifest":manifest()});
    for invalid in [
        json!(7),
        json!(true),
        json!(vec![1; 32]),
        json!("AB".repeat(32)),
        json!("f".repeat(63)),
    ] {
        let mut metadata = original.clone();
        metadata["context"]["graph_identity"]["map_sha256"] = invalid;
        assert!(boundary::plan_locator(&bytes(&metadata), &selected()).is_err());
    }
    for invalid in [
        json!(4),
        json!("shared_blocks_v4 "),
        json!("shared_blocks_v5"),
    ] {
        let mut metadata = original.clone();
        metadata["context"]["graph_identity"]["layout_generation"] = invalid;
        assert!(boundary::plan_locator(&bytes(&metadata), &selected()).is_err());
    }
    for pointer in [
        "/context/extra",
        "/context/graph_identity/extra",
        "/manifest/extra",
        "/extra",
    ] {
        let mut metadata = original.clone();
        let (parent, key) = pointer.rsplit_once('/').unwrap();
        metadata
            .pointer_mut(parent)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert(key.into(), json!(1));
        assert!(boundary::plan_locator(&bytes(&metadata), &selected()).is_err());
    }
    let duplicate = format!(
        "{{\"context\":{},\"context\":{},\"manifest\":{}}}",
        context(),
        context(),
        manifest()
    );
    assert!(boundary::plan_locator(duplicate.as_bytes(), &selected()).is_err());
    assert!(boundary::plan_locator(b"{", &selected()).is_err());
    assert!(boundary::plan_locator(
        &vec![b' '; boundary::MAX_LOCATOR_METADATA_BYTES + 1],
        &selected()
    )
    .is_err());
}

#[test]
fn raw_and_metadata_counts_are_bounded_before_frame_parsing() {
    let too_large = vec![0; 4 * 1024 * 1024 + 1];
    assert_eq!(
        boundary::plan_members(b"invalid", &selected(), &[&too_large]).unwrap_err(),
        "registry_ptg_graph_budget"
    );
    let page = vec![0; 4 * 1024 * 1024];
    let aggregate = vec![page.as_slice(); 65];
    assert_eq!(
        boundary::verify(b"invalid", &selected(), &aggregate).unwrap_err(),
        "registry_ptg_graph_budget"
    );
    let many = vec![&[][..]; 32769];
    assert_eq!(
        boundary::plan_members(b"invalid", &selected(), &many).unwrap_err(),
        "registry_ptg_graph_budget"
    );
    let oversized = vec![b' '; boundary::MAX_METADATA_BYTES + 1];
    assert_eq!(
        boundary::verify(&oversized, &selected(), &[]).unwrap_err(),
        "registry_ptg_graph_budget"
    );
    let (mut metadata, _) = fixture(false, false);
    for field in ["map_packs", "blocks", "heavy"] {
        let original = metadata[field].take();
        let maximum = if field == "heavy" { 4096 } else { 16384 };
        metadata[field] = json!(vec![Value::Null; maximum + 1]);
        assert!(boundary::plan_members(&bytes(&metadata), &selected(), &[]).is_err());
        metadata[field] = original;
    }
    metadata["context"]["padding"] = json!("x".repeat(16384));
    assert!(boundary::plan_members(&bytes(&metadata), &selected(), &[]).is_err());
}

#[test]
fn exact_payload_mapping_refuses_omitted_unused_duplicate_or_reordered_indexes() {
    let (original, payloads) = fixture(true, true);
    let borrowed: Vec<_> = payloads.iter().map(Vec::as_slice).collect();
    assert!(boundary::verify(&bytes(&original), &selected(), &borrowed[..3]).is_err());
    let mut unused = borrowed.clone();
    unused.push(&[]);
    assert!(boundary::verify(&bytes(&original), &selected(), &unused).is_err());
    for index in [0, 1, 4, usize::MAX] {
        let mut metadata = original.clone();
        metadata["blocks"][0]["payload_index"] = json!(index);
        assert!(boundary::verify(&bytes(&metadata), &selected(), &borrowed).is_err());
    }
    for pointer in [
        "/blocks/0/extra",
        "/map_packs/0/extra",
        "/map_packs/0/block/extra",
    ] {
        let mut metadata = original.clone();
        let (parent, key) = pointer.rsplit_once('/').unwrap();
        metadata
            .pointer_mut(parent)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert(key.into(), json!(1));
        assert!(boundary::verify(&bytes(&metadata), &selected(), &borrowed).is_err());
    }
}

#[cfg(feature = "python")]
mod python {
    use super::*;
    use pyo3::{
        exceptions::PyTypeError,
        prelude::*,
        types::{PyBytes, PyTuple},
    };

    #[test]
    fn immutable_bytes_tuple_boundary_and_real_python_proof() {
        Python::initialize();
        Python::attach(|py| {
            let locator = pyo3::wrap_pyfunction!(boundary::locator, py).unwrap();
            let members = pyo3::wrap_pyfunction!(boundary::members, py).unwrap();
            let verify = pyo3::wrap_pyfunction!(boundary::verification, py).unwrap();
            assert_eq!(
                verify
                    .getattr("__name__")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "verify_registry_ptg_graph_batch"
            );
            for expression in [c"bytearray(b'{}')", c"memoryview(b'{}')", c"'{}'", c"[1]"] {
                let invalid = py.eval(expression, None, None).unwrap();
                let error = locator
                    .call1((&invalid, PyBytes::new(py, &selected())))
                    .unwrap_err();
                assert!(error.is_instance_of::<PyTypeError>(py));
                let error = locator
                    .call1((PyBytes::new(py, b"{}"), &invalid))
                    .unwrap_err();
                assert!(error.is_instance_of::<PyTypeError>(py));
            }
            for expression in [
                c"[]",
                c"(bytearray(b'a'),)",
                c"(memoryview(b'a'),)",
                c"('a',)",
            ] {
                let invalid = py.eval(expression, None, None).unwrap();
                let error = members
                    .call1((
                        PyBytes::new(py, b"{}"),
                        PyBytes::new(py, &selected()),
                        invalid,
                    ))
                    .unwrap_err();
                assert!(error.is_instance_of::<PyTypeError>(py));
            }
            let (metadata, payloads) = fixture(true, true);
            let buffers =
                PyTuple::new(py, payloads.iter().map(|payload| PyBytes::new(py, payload))).unwrap();
            let result = verify
                .call1((
                    PyBytes::new(py, &bytes(&metadata)),
                    PyBytes::new(py, &selected()),
                    buffers,
                ))
                .unwrap();
            let proof: Value =
                serde_json::from_slice(result.cast::<PyBytes>().unwrap().as_bytes()).unwrap();
            assert_eq!(proof["context"], context());
            assert_eq!(proof["verified_edge_count"], 1);
        });
    }
}
