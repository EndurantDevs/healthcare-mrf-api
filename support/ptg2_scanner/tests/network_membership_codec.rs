use ptg2_scanner::network_membership_codec::{
    encode_network_membership_batch, encode_network_membership_batch_cancellable, COPY_COLUMNS,
    MAX_FIELD_BYTES, MAX_INPUT_BYTES, MAX_ROWS,
};
use serde_json::{json, Value};
use std::sync::atomic::AtomicBool;

fn row() -> Value {
    json!({
        "network_id": 42,
        "provider_system": "npi",
        "provider_id": "1000000491",
        "location_id": "01234567-89ab-cdef-8123-456789abcdef",
        "evidence_id": "observation-1"
    })
}

fn encode(rows: &[Value]) -> Result<Vec<u8>, String> {
    encode_network_membership_batch(&serde_json::to_vec(rows).unwrap())
        .map(|batch| batch.copy_bytes)
        .map_err(|failure| serde_json::to_string(&failure).unwrap())
}

// Independent framing reader checks PostgreSQL binary COPY widths, order and trailer.
fn copy_rows(bytes: &[u8]) -> Vec<Vec<&[u8]>> {
    assert_eq!(&bytes[..11], b"PGCOPY\n\xff\r\n\0");
    assert_eq!(&bytes[11..19], &[0; 8]);
    let mut offset = 19;
    let mut rows = Vec::new();
    loop {
        let count = i16::from_be_bytes(bytes[offset..offset + 2].try_into().unwrap());
        offset += 2;
        if count == -1 {
            break;
        }
        assert_eq!(count, 5);
        let mut row = Vec::new();
        for _ in 0..count {
            let length = i32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap());
            offset += 4;
            assert!(length >= 0);
            let end = offset + length as usize;
            row.push(&bytes[offset..end]);
            offset = end;
        }
        rows.push(row);
    }
    assert_eq!(offset, bytes.len());
    rows
}

#[test]
fn copy_wire_preserves_network_provider_namespace_exact_site_and_evidence() {
    let mut second = row();
    second["network_id"] = json!(i32::MAX);
    second["provider_system"] = json!("manual");
    second["provider_id"] = json!("000123");
    second["location_id"] = json!("01234567-89AB-CDEF-8123-456789ABCDE0");
    second["evidence_id"] = json!("observation-2");
    let input = serde_json::to_vec(&[row(), second]).unwrap();
    let batch = encode_network_membership_batch(&input).unwrap();
    assert_eq!(batch.row_count, 2);
    assert_eq!(
        COPY_COLUMNS,
        [
            "network_id",
            "provider_system",
            "provider_id",
            "location_id",
            "evidence_id"
        ]
    );
    let rows = copy_rows(&batch.copy_bytes);
    assert_eq!(i32::from_be_bytes(rows[0][0].try_into().unwrap()), 42);
    assert_eq!(rows[0][1], b"npi");
    assert_eq!(rows[0][2], b"1000000491");
    assert_eq!(
        rows[0][3],
        &[
            0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x81, 0x23, 0x45, 0x67, 0x89, 0xab,
            0xcd, 0xef
        ]
    );
    assert_eq!(rows[0][4], b"observation-1");
    assert_eq!(i32::from_be_bytes(rows[1][0].try_into().unwrap()), i32::MAX);
    assert_eq!(rows[1][1], b"manual");
    assert_eq!(rows[1][2], b"000123");
    assert_ne!(rows[0][3], rows[1][3]);
    assert_eq!(rows[1][4], b"observation-2");
}

#[test]
fn invalid_network_ids_cannot_be_coerced_from_legacy_or_noninteger_values() {
    for invalid in [
        json!(0),
        json!(-1),
        json!(2_147_483_648u64),
        json!(u64::MAX),
        json!(42.0),
        json!("42"),
        json!(true),
        json!(null),
        json!("01234567-89ab-cdef-8123-456789abcdef"),
    ] {
        let mut bad = row();
        bad["network_id"] = invalid;
        let failure = encode(&[row(), bad]).unwrap_err();
        assert!(failure.contains("\"row_index\":1"), "{failure}");
    }
}

#[test]
fn malformed_and_unknown_fields_reject_whole_batch_without_leaking_values() {
    for malformed in [
        b"{}".as_slice(),
        b"[",
        b"[null]",
        b"[] []",
        b"[{},",
        b"[[]]",
        &[b'[', 0xff, b']'],
    ] {
        assert!(encode_network_membership_batch(malformed).is_err());
    }
    for field in ["network_label", "checksum_network", "legacy_uuid"] {
        let mut bad = row();
        bad[field] = json!("private-test-value");
        let failure = encode(&[row(), bad]).unwrap_err();
        assert!(failure.contains("\"row_index\":1"));
        assert!(!failure.contains("private-test-value"));
    }
    let duplicate = br#"[{"network_id":42,"network_id":43,"provider_system":"manual","provider_id":"p-1","location_id":"01234567-89ab-cdef-8123-456789abcdef","evidence_id":"e-1"}]"#;
    assert!(encode_network_membership_batch(duplicate).is_err());
    let mut missing = row();
    missing.as_object_mut().unwrap().remove("evidence_id");
    assert!(encode(&[missing]).is_err());
}

#[test]
fn npi_structure_and_checksum_are_reused_without_changing_source_identifiers() {
    for invalid in [
        "1000000492",
        "3000000000",
        "0999999999",
        "10/00/0491",
        "123",
        "100000049x",
        "0000000000",
        "１００００００４９１",
    ] {
        let mut bad = row();
        bad["provider_id"] = json!(invalid);
        assert!(encode(&[bad]).is_err());
    }
    for system in ["manual", "provider_directory"] {
        let mut opaque = row();
        opaque["provider_system"] = json!(system);
        opaque["provider_id"] = json!("resource-0001");
        assert!(encode(&[opaque]).is_ok());
    }
    let mut bad = row();
    bad["provider_system"] = json!("unknown");
    assert!(encode(&[bad]).is_err());
}

#[test]
fn location_and_bounded_text_validation_fail_closed() {
    for invalid in [
        "",
        "00000000-0000-0000-0000-000000000000",
        "0123456789abcdef8123456789abcdef",
        "{01234567-89ab-cdef-8123-456789abcdef}",
        "01234567_89ab-cdef-8123-456789abcdef",
        "01234567-89ab-cdef-8123-456789abcdeg",
    ] {
        let mut bad = row();
        bad["location_id"] = json!(invalid);
        assert!(encode(&[bad]).is_err());
    }
    for field in ["provider_id", "evidence_id"] {
        for invalid in [
            String::new(),
            " ".to_owned(),
            " x".to_owned(),
            "x ".to_owned(),
            "a\0b".to_owned(),
            "a\nb".to_owned(),
            "x".repeat(MAX_FIELD_BYTES + 1),
            "é".repeat(MAX_FIELD_BYTES / 2 + 1),
        ] {
            let mut bad = row();
            bad["provider_system"] = json!("manual");
            bad[field] = json!(invalid);
            assert!(encode(&[bad]).is_err());
        }
    }
    let mut exact = row();
    exact["provider_system"] = json!("manual");
    exact["provider_id"] = json!("é".repeat(MAX_FIELD_BYTES / 2));
    assert!(encode(&[exact]).is_ok());
}

#[test]
fn row_byte_and_cancellation_limits_preserve_batch_atomicity() {
    let exact = vec![row(); MAX_ROWS];
    let input = serde_json::to_vec(&exact).unwrap();
    assert_eq!(input.len(), 760_001);
    let batch = encode_network_membership_batch(&input).unwrap();
    assert_eq!(batch.row_count, MAX_ROWS);
    assert_eq!(batch.copy_bytes.len(), 340_021);
    assert_eq!(copy_rows(&batch.copy_bytes).len(), MAX_ROWS);
    assert!(encode(&vec![row(); MAX_ROWS + 1])
        .unwrap_err()
        .contains("row_limit"));
    let mut exact_bytes = vec![b' '; MAX_INPUT_BYTES];
    exact_bytes[..2].copy_from_slice(b"[]");
    assert_eq!(
        encode_network_membership_batch(&exact_bytes)
            .unwrap()
            .row_count,
        0
    );
    exact_bytes.push(b' ');
    assert_eq!(
        encode_network_membership_batch(&exact_bytes)
            .unwrap_err()
            .code,
        "input_limit"
    );
    assert_eq!(
        encode_network_membership_batch_cancellable(b"[]", &AtomicBool::new(true))
            .unwrap_err()
            .code,
        "cancelled"
    );
    assert!(encode_network_membership_batch_cancellable(b"[]", &AtomicBool::new(false)).is_ok());
}

#[test]
fn json_escapes_are_decoded_while_exact_duplicate_observations_are_retained() {
    let input = br#"[{"network_id":42,"provider_system":"manual","provider_id":"provider\u0031","location_id":"01234567-89ab-cdef-8123-456789abcdef","evidence_id":"proof\u0031"}]"#;
    let output = encode_network_membership_batch(input).unwrap();
    let rows = copy_rows(&output.copy_bytes);
    assert_eq!(rows[0][2], b"provider1");
    assert_eq!(rows[0][4], b"proof1");
    let repeated = encode(&[row(), row()]).unwrap();
    let repeated = copy_rows(&repeated);
    assert_eq!(repeated.len(), 2);
    assert_eq!(repeated[0], repeated[1]);
}
