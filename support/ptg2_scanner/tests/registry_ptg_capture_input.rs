use ptg2_scanner::registry_ptg_capture_input::{
    encode_registry_ptg_capture_batch, EncodedRegistryPTGCaptureBatch,
    RegistryPTGCaptureInputError, COPY_COLUMNS, MAX_CONTEXT_BYTES, MAX_INPUT_BYTES, MAX_ROWS,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

fn coordinates() -> Value {
    json!({
        "dataset_id": "11111111-1111-4111-8111-111111111111",
        "dataset_schema": "registry_ptg_capture_11111111111141118111111111111111",
        "edition_id": "synthetic-reviewed-edition-a",
        "producer_id": "synthetic-exact-office-producer",
        "source_id": "synthetic-ptg-office-source", "source_system": "ptg",
    })
}

fn context() -> Value {
    json!({
        "binding_coordinates": coordinates(), "binding_source_key": "synthetic-map-selector-a",
        "source_scope": {"cohort_id": "synthetic-cohort-a", "company_key": "synthetic-company-a",
                         "snapshot_id": "synthetic-sealed-snapshot-a"},
        "office_evidence_kind": "reviewed_exact_office", "snapshot_key": 31,
    })
}

fn row() -> Value {
    json!({
        "address_row_sha256": "07757eb7d385bb4544cbb02c5607c803d7fb6c8997f201f92db8be3752460730",
        "binding_source_key": "synthetic-map-selector-a", "cohort_id": "synthetic-cohort-a",
        "company_key": "synthetic-company-a", "dense_source_key": 0,
        "evidence_id": "daf2d9bc075c4de132cc429bc008cf11e4601573e7a5f2819b205611abe3b907",
        "location_hash": "entity_address_unified:32f8d9b5b9cef302f823521e97fa46849fdcd59f15f7658769e480ce720e4ab4",
        "location_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "location_key": "32f8d9b5b9cef302f823521e97fa46849fdcd59f15f7658769e480ce720e4ab4",
        "office_evidence_json": {
            "address_row_sha256": "07757eb7d385bb4544cbb02c5607c803d7fb6c8997f201f92db8be3752460730",
            "assertion_id": "synthetic-review-a", "binding_coordinates": coordinates(),
            "binding_source_key": "synthetic-map-selector-a", "contract": "registry_ptg_office_assertion.v1",
            "kind": "reviewed_exact_office", "location_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            "location_key": "32f8d9b5b9cef302f823521e97fa46849fdcd59f15f7658769e480ce720e4ab4",
            "provider_id": "1999999901", "provider_system": "npi", "source_record_key": "office:a",
            "source_scope": context()["source_scope"],
        },
        "office_evidence_kind": "reviewed_exact_office",
        "office_evidence_sha256": "19c73af41bd643ab6a75c77d0f49c31dd4721da84b5b1c56f6c1834d005252f7",
        "ordinal": 1, "provider_group_ref": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "provider_id": "1999999901", "provider_system": "npi",
        "provider_witness_sha256": "2e43a5aa0c575ac2a444c01fd28de5ec8fb6c8965071ec0c36dd5c20b2915bce",
        "snapshot_id": "synthetic-sealed-snapshot-a", "source_record_key": "office:a",
        "source_record_ordinal": 0,
    })
}

fn sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn rehash(row: &mut Value, context: &Value) {
    row["provider_witness_sha256"] = json!(sha256(&serde_json::to_vec(&json!({
        "snapshot_key": context["snapshot_key"], "dense_source_key": row["dense_source_key"],
        "source_record_ordinal": row["source_record_ordinal"], "provider_group_ref": row["provider_group_ref"],
        "provider_system": row["provider_system"], "provider_id": row["provider_id"],
    })).unwrap()));
    row["office_evidence_sha256"] = json!(sha256(
        &serde_json::to_vec(&row["office_evidence_json"]).unwrap()
    ));
    row["evidence_id"] = json!(sha256(
        &serde_json::to_vec(&json!([
            context["binding_coordinates"],
            context["source_scope"],
            row["binding_source_key"],
            row["provider_system"],
            row["provider_id"],
            row["location_id"],
            row["office_evidence_sha256"],
            row["provider_witness_sha256"],
            row["address_row_sha256"],
        ]))
        .unwrap()
    ));
}

fn office_b() -> Value {
    let mut second = row();
    for (field, value) in [
        ("source_record_key", "office:b"),
        ("location_id", "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
        (
            "location_key",
            "7c6eb63e27bfa873d92dc72ee194c54583de893e1c7411f9894c669e67081bcb",
        ),
        (
            "address_row_sha256",
            "c5b10363cea5bfedbf949cb32a2f4dc5c8dc24a5626a4fea569aea54402d4888",
        ),
    ] {
        second[field] = json!(value);
        second["office_evidence_json"][field] = json!(value);
    }
    second["location_hash"] = json!(format!(
        "entity_address_unified:{}",
        second["location_key"].as_str().unwrap()
    ));
    second["ordinal"] = json!(2);
    second["dense_source_key"] = json!(1);
    second["source_record_ordinal"] = json!(1);
    second["provider_group_ref"] = json!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    rehash(&mut second, &context());
    second
}

fn encode(
    rows: &[Value],
    context: &Value,
    after: u64,
) -> Result<EncodedRegistryPTGCaptureBatch, RegistryPTGCaptureInputError> {
    encode_registry_ptg_capture_batch(
        &serde_json::to_vec(rows).unwrap(),
        &serde_json::to_vec(context).unwrap(),
        after,
    )
}

fn copy_rows(bytes: &[u8]) -> Vec<Vec<&[u8]>> {
    assert_eq!(&bytes[..19], b"PGCOPY\n\xff\r\n\0\0\0\0\0\0\0\0\0");
    let mut offset = 19;
    let mut rows = Vec::new();
    loop {
        let count = i16::from_be_bytes(bytes[offset..offset + 2].try_into().unwrap());
        offset += 2;
        if count == -1 {
            break;
        }
        assert_eq!(count, 20);
        let mut fields = Vec::new();
        for _ in 0..count {
            let length = i32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap());
            assert!(length >= 0);
            offset += 4;
            let end = offset + length as usize;
            fields.push(&bytes[offset..end]);
            offset = end;
        }
        rows.push(fields);
    }
    assert_eq!(offset, bytes.len());
    rows
}

#[test]
fn frozen_synthetic_hashes_and_python_canonical_bytes_match() {
    let batch = encode(&[row()], &context(), 0).unwrap();
    assert_eq!((batch.row_count, batch.last_ordinal), (1, 1));
    assert_eq!(batch.canonical_ndjson.len(), 1928);
    assert_eq!(
        sha256(&batch.canonical_ndjson),
        "17626dd7f906d108448b6ab8e822a44e2170f6205b003086610bd6ec61a23c44"
    );
    let fields = &copy_rows(&batch.copy_bytes)[0];
    let raw = row();
    for (index, field) in COPY_COLUMNS.iter().enumerate() {
        match *field {
            "ordinal" => assert_eq!(i64::from_be_bytes(fields[index].try_into().unwrap()), 1),
            "dense_source_key" => {
                assert_eq!(i32::from_be_bytes(fields[index].try_into().unwrap()), 0)
            }
            "source_record_ordinal" => {
                assert_eq!(i64::from_be_bytes(fields[index].try_into().unwrap()), 0)
            }
            "location_id" => assert_eq!(
                fields[index],
                &[
                    0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0x4a, 0xaa, 0x8a, 0xaa, 0xaa, 0xaa, 0xaa,
                    0xaa, 0xaa, 0xaa
                ]
            ),
            "office_evidence_json" => {
                assert_eq!(fields[index][0], 1);
                assert_eq!(
                    serde_json::from_slice::<Value>(&fields[index][1..]).unwrap(),
                    raw[*field]
                );
            }
            _ => assert_eq!(fields[index], raw[*field].as_str().unwrap().as_bytes()),
        }
    }
}

#[test]
fn page_boundaries_and_input_key_order_do_not_change_canonical_stream() {
    let rows = [row(), office_b()];
    let combined = encode(&rows, &context(), 0).unwrap();
    let first = encode(&rows[..1], &context(), 0).unwrap();
    let second = encode(&rows[1..], &context(), first.last_ordinal).unwrap();
    assert_eq!(
        combined.canonical_ndjson,
        [first.canonical_ndjson, second.canonical_ndjson].concat()
    );
    assert_ne!(
        copy_rows(&combined.copy_bytes)[0][8],
        copy_rows(&combined.copy_bytes)[1][8]
    );
    let reverse: Vec<_> = rows[0]
        .as_object()
        .unwrap()
        .iter()
        .rev()
        .map(|(key, value)| format!("{}:{}", serde_json::to_string(key).unwrap(), value))
        .collect();
    let reordered = format!("[{{{}}}]", reverse.join(","));
    assert_eq!(
        encode_registry_ptg_capture_batch(
            reordered.as_bytes(),
            &serde_json::to_vec(&context()).unwrap(),
            0
        )
        .unwrap()
        .canonical_ndjson,
        encode(&rows[..1], &context(), 0).unwrap().canonical_ndjson
    );
}

#[test]
fn both_provenance_kinds_preserve_unicode_without_ascii_escaping() {
    for kind in ["payer_exact_office", "reviewed_exact_office"] {
        let mut ctx = context();
        ctx["office_evidence_kind"] = json!(kind);
        let mut raw = row();
        raw["office_evidence_kind"] = json!(kind);
        raw["office_evidence_json"]["kind"] = json!(kind);
        raw["office_evidence_json"]["assertion_id"] = json!("review-É東京");
        rehash(&mut raw, &ctx);
        let encoded = encode(&[raw], &ctx, 0).unwrap();
        let stream = std::str::from_utf8(&encoded.canonical_ndjson).unwrap();
        assert!(stream.contains("review-É東京"));
        assert!(!stream.contains("\\u00"));
    }
}

#[test]
fn scope_provider_and_exact_office_substitutions_reject_whole_batch() {
    let mut substituted = row();
    substituted["location_id"] = office_b()["location_id"].clone();
    assert_eq!(
        encode(&[substituted], &context(), 0).unwrap_err().code,
        "registry_ptg_office_invalid"
    );
    for field in [
        "binding_source_key",
        "company_key",
        "cohort_id",
        "snapshot_id",
        "provider_system",
        "provider_id",
        "location_id",
        "location_key",
        "location_hash",
        "address_row_sha256",
        "provider_group_ref",
        "office_evidence_kind",
    ] {
        let mut bad = office_b();
        bad[field] = json!("wrong");
        let failure = encode(&[row(), bad], &context(), 0).unwrap_err();
        assert_eq!(failure.row_ordinal, Some(2));
        assert!(!serde_json::to_string(&failure).unwrap().contains("wrong"));
    }
    for field in [
        "contract",
        "kind",
        "source_record_key",
        "binding_source_key",
        "provider_system",
        "provider_id",
        "location_id",
        "location_key",
        "address_row_sha256",
    ] {
        let mut bad = row();
        bad["office_evidence_json"][field] = json!("wrong");
        rehash(&mut bad, &context());
        assert_eq!(
            encode(&[bad], &context(), 0).unwrap_err().code,
            "registry_ptg_office_invalid"
        );
    }
    for field in [
        "dataset_id",
        "dataset_schema",
        "source_id",
        "source_system",
        "producer_id",
        "edition_id",
    ] {
        let mut bad = row();
        bad["office_evidence_json"]["binding_coordinates"][field] = json!("wrong");
        assert!(encode(&[bad], &context(), 0).is_err());
    }
    for field in ["company_key", "cohort_id", "snapshot_id"] {
        let mut bad = row();
        bad["office_evidence_json"]["source_scope"][field] = json!("wrong");
        assert!(encode(&[bad], &context(), 0).is_err());
    }
}

#[test]
fn exact_witness_office_and_evidence_digests_are_required() {
    for field in [
        "provider_witness_sha256",
        "office_evidence_sha256",
        "evidence_id",
    ] {
        let mut bad = row();
        bad[field] = json!("0".repeat(64));
        assert_eq!(
            encode(&[bad], &context(), 0).unwrap_err().code,
            "registry_ptg_digest_invalid"
        );
    }
    for value in ["f".repeat(63), "F".repeat(64), "g".repeat(64)] {
        for field in [
            "location_key",
            "address_row_sha256",
            "provider_witness_sha256",
            "office_evidence_sha256",
            "evidence_id",
        ] {
            let mut bad = row();
            bad[field] = json!(value);
            assert!(encode(&[bad], &context(), 0).is_err());
        }
    }
    for value in ["a".repeat(31), "A".repeat(32), "g".repeat(32)] {
        let mut bad = row();
        bad["provider_group_ref"] = json!(value);
        assert!(encode(&[bad], &context(), 0).is_err());
    }
    let mut bad_ctx = context();
    bad_ctx["snapshot_key"] = json!(32);
    assert_eq!(
        encode(&[row()], &bad_ctx, 0).unwrap_err().code,
        "registry_ptg_digest_invalid"
    );
}

#[test]
fn every_nested_object_is_closed_and_rejects_duplicate_fields() {
    let ctx_bytes = serde_json::to_vec(&context()).unwrap();
    let mut raw = row();
    for path in [
        vec![],
        vec!["office_evidence_json"],
        vec!["office_evidence_json", "binding_coordinates"],
        vec!["office_evidence_json", "source_scope"],
    ] {
        let mut bad = raw.clone();
        let mut object = &mut bad;
        for key in path {
            object = &mut object[key];
        }
        object["extra"] = json!(1);
        assert!(encode(&[bad], &context(), 0).is_err());
    }
    for marker in [
        "\"ordinal\":1",
        "\"contract\":\"registry_ptg_office_assertion.v1\"",
        "\"dataset_id\":\"11111111-1111-4111-8111-111111111111\"",
        "\"company_key\":\"synthetic-company-a\"",
    ] {
        let input = serde_json::to_string(&[raw.clone()]).unwrap().replacen(
            marker,
            &format!("{marker},{marker}"),
            1,
        );
        assert!(encode_registry_ptg_capture_batch(input.as_bytes(), &ctx_bytes, 0).is_err());
    }
    for path in [vec![], vec!["binding_coordinates"], vec!["source_scope"]] {
        let mut bad = context();
        let mut object = &mut bad;
        for key in path {
            object = &mut object[key];
        }
        object["extra"] = json!(1);
        assert!(encode(&[raw.clone()], &bad, 0).is_err());
    }
    for marker in [
        "\"snapshot_key\":31",
        "\"dataset_id\":\"11111111-1111-4111-8111-111111111111\"",
        "\"cohort_id\":\"synthetic-cohort-a\"",
    ] {
        let bad_ctx = serde_json::to_string(&context()).unwrap().replacen(
            marker,
            &format!("{marker},{marker}"),
            1,
        );
        assert!(encode_registry_ptg_capture_batch(
            &serde_json::to_vec(&[raw.clone()]).unwrap(),
            bad_ctx.as_bytes(),
            0
        )
        .is_err());
    }
    raw.as_object_mut().unwrap().remove("ordinal");
    assert!(encode(&[raw], &context(), 0).is_err());
}

#[test]
fn integers_are_never_coerced_and_keep_bigint_precision() {
    for value in [
        json!(true),
        json!(-1),
        json!(1.0),
        json!(1.5),
        json!("1"),
        json!(null),
        json!(u64::MAX),
    ] {
        for field in ["ordinal", "dense_source_key", "source_record_ordinal"] {
            let mut bad = row();
            bad[field] = value.clone();
            assert!(encode(&[bad], &context(), 0).is_err());
        }
        let mut bad = context();
        bad["snapshot_key"] = value;
        assert!(encode(&[row()], &bad, 0).is_err());
    }
    let mut bad = row();
    bad["dense_source_key"] = json!(i32::MAX as u64 + 1);
    assert!(encode(&[bad], &context(), 0).is_err());
    let mut ctx = context();
    ctx["snapshot_key"] = json!(i64::MAX);
    let mut raw = row();
    raw["ordinal"] = json!(i64::MAX);
    raw["source_record_ordinal"] = json!(i64::MAX);
    raw["dense_source_key"] = json!(i32::MAX);
    rehash(&mut raw, &ctx);
    let encoded = encode(&[raw.clone()], &ctx, i64::MAX as u64 - 1).unwrap();
    let fields = &copy_rows(&encoded.copy_bytes)[0];
    assert_eq!(i64::from_be_bytes(fields[0].try_into().unwrap()), i64::MAX);
    assert_eq!(i32::from_be_bytes(fields[12].try_into().unwrap()), i32::MAX);
    assert_eq!(i64::from_be_bytes(fields[13].try_into().unwrap()), i64::MAX);
    assert!(std::str::from_utf8(&encoded.canonical_ndjson)
        .unwrap()
        .contains("9223372036854775807"));
    assert!(encode(&[raw], &ctx, i64::MAX as u64).is_err());
    assert!(encode(&[row()], &context(), u64::MAX).is_err());
}

#[test]
fn ordinal_gaps_and_duplicate_keys_or_provider_offices_reject() {
    for ordinal in [0, 1, 3] {
        let mut second = office_b();
        second["ordinal"] = json!(ordinal);
        assert_eq!(
            encode(&[row(), second], &context(), 0).unwrap_err().code,
            "registry_ptg_accounting_invalid"
        );
    }
    let mut second = office_b();
    second["source_record_key"] = row()["source_record_key"].clone();
    second["office_evidence_json"]["source_record_key"] = second["source_record_key"].clone();
    rehash(&mut second, &context());
    assert_eq!(
        encode(&[row(), second], &context(), 0).unwrap_err().code,
        "registry_ptg_rows_invalid"
    );
    let mut second = row();
    second["ordinal"] = json!(2);
    second["source_record_key"] = json!("office:other");
    second["office_evidence_json"]["source_record_key"] = json!("office:other");
    rehash(&mut second, &context());
    assert_eq!(
        encode(&[row(), second], &context(), 0).unwrap_err().code,
        "registry_ptg_rows_invalid"
    );
}

#[test]
fn canonical_uuid_and_npi_structure_and_checksum_are_required() {
    for value in [
        "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA",
        "00000000-0000-0000-0000-000000000000",
        "aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa",
    ] {
        let mut bad = row();
        bad["location_id"] = json!(value);
        assert!(encode(&[bad], &context(), 0).is_err());
    }
    for value in [
        "1999999902",
        "3000000000",
        "0999999999",
        "0000000000",
        "１２３４５６７８９０",
    ] {
        let mut bad = row();
        bad["provider_id"] = json!(value);
        assert_eq!(
            encode(&[bad], &context(), 0).unwrap_err().code,
            "registry_ptg_provider_invalid"
        );
    }
}

#[test]
fn malformed_encoding_nonfinite_numbers_and_batch_limits_reject() {
    let ctx = serde_json::to_vec(&context()).unwrap();
    for input in [
        b"{}".as_slice(),
        b"[]",
        b"[null]",
        b"[",
        b"[] []",
        b"[NaN]",
        b"[Infinity]",
        &[b'[', 0xff, b']'],
    ] {
        assert!(encode_registry_ptg_capture_batch(input, &ctx, 0).is_err());
    }
    assert_eq!(
        encode_registry_ptg_capture_batch(&vec![b' '; MAX_INPUT_BYTES + 1], &ctx, 0)
            .unwrap_err()
            .code,
        "registry_ptg_batch_bounds"
    );
    assert_eq!(
        encode_registry_ptg_capture_batch(
            &serde_json::to_vec(&vec![json!({}); MAX_ROWS + 1]).unwrap(),
            &ctx,
            0
        )
        .unwrap_err()
        .code,
        "registry_ptg_batch_bounds"
    );
    for value in ["\0", " edge", "edge ", "line\nfeed", "\u{7f}"] {
        let mut bad = row();
        bad["source_record_key"] = json!(value);
        assert!(encode(&[bad], &context(), 0).is_err());
        let mut bad_ctx = context();
        bad_ctx["binding_source_key"] = json!(value);
        assert!(encode(&[row()], &bad_ctx, 0).is_err());
    }
    let mut bad = row();
    bad["office_evidence_json"]["assertion_id"] = json!("é".repeat(65));
    assert!(encode(&[bad], &context(), 0).is_err());
}

#[test]
fn existing_coordinate_text_interface_and_native_field_bounds_are_preserved() {
    let mut ctx = context();
    ctx["binding_coordinates"]["dataset_id"] = json!("synthetic-edition-key");
    let mut raw = row();
    raw["office_evidence_json"]["binding_coordinates"] = ctx["binding_coordinates"].clone();
    raw["source_record_key"] = json!("r".repeat(128));
    raw["office_evidence_json"]["source_record_key"] = raw["source_record_key"].clone();
    rehash(&mut raw, &ctx);
    assert!(encode(&[raw.clone()], &ctx, 0).is_ok());
    raw["source_record_key"] = json!("r".repeat(129));
    assert!(encode(&[raw], &ctx, 0).is_err());
    for (field, bound) in [
        ("company_key", 512),
        ("cohort_id", 128),
        ("snapshot_id", 96),
    ] {
        let mut bad = context();
        bad["source_scope"][field] = json!("x".repeat(bound + 1));
        assert!(encode(&[row()], &bad, 0).is_err());
    }
    let mut bad = context();
    bad["office_evidence_kind"] = json!("company_approval");
    assert!(encode(&[row()], &bad, 0).is_err());
}

#[test]
fn context_byte_cap_is_checked_before_decoding() {
    let input = serde_json::to_vec(&[row()]).unwrap();
    let mut context_bytes = serde_json::to_vec(&context()).unwrap();
    context_bytes.resize(MAX_CONTEXT_BYTES, b' ');
    assert!(encode_registry_ptg_capture_batch(&input, &context_bytes, 0).is_ok());
    context_bytes.push(b' ');
    let failure = encode_registry_ptg_capture_batch(&input, &context_bytes, 0).unwrap_err();
    assert_eq!(failure.code, "registry_ptg_batch_bounds");
    assert_eq!(failure.row_ordinal, None);
    context_bytes[0] = 0xff;
    assert_eq!(
        encode_registry_ptg_capture_batch(&input, &context_bytes, 0)
            .unwrap_err()
            .code,
        "registry_ptg_batch_bounds"
    );
}

#[test]
fn published_plan_keeps_two_distinct_offices_without_cohort_labels() {
    let mut ctx = context();
    ctx["source_scope"] = json!({"review_type":"published_complete_snapshot_plan", "scope_id":"11111111-1111-4111-8111-111111111111", "approval_sha256":"a".repeat(64), "snapshot_id":"synthetic-sealed-snapshot-a", "plan_id":"synthetic_plan", "plan_market_type":"individual", "selection_mode":"complete_snapshot_source_set"});
    let mut rows = [row(), office_b()];
    for raw in &mut rows {
        raw["company_key"] = Value::Null;
        raw["cohort_id"] = Value::Null;
        raw["office_evidence_json"]["source_scope"] = ctx["source_scope"].clone();
        rehash(raw, &ctx);
    }
    let batch = encode(&rows, &ctx, 0).unwrap();
    assert_eq!(batch.row_count, 2);
    let documents: Vec<Value> = batch
        .canonical_ndjson
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect();
    assert_ne!(documents[0]["location_id"], documents[1]["location_id"]);
    assert!(documents[0]["company_key"].is_null());
    // Native COPY nulls, not empty or invented labels.
    let mut offset = 21;
    for index in 0..20 {
        let length = i32::from_be_bytes(batch.copy_bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;
        if index == 3 || index == 4 {
            assert_eq!(length, -1);
        } else {
            assert!(length >= 0);
            offset += length as usize;
        }
    }
    rows[0]["company_key"] = json!("invented");
    assert_eq!(
        encode(&rows, &ctx, 0).unwrap_err().code,
        "registry_ptg_scope_changed"
    );
    rows[0]["company_key"] = Value::Null;
    ctx["source_scope"]["plan_id"] = json!("other");
    assert!(encode(&rows, &ctx, 0).is_err());
}
