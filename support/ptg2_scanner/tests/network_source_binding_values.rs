use bindings::{EncodedSourceBindingBatch, MAX_EXPECTED_REVISION, MAX_INPUT_BYTES, MAX_ROWS};
use ptg2_scanner::network_source_binding_values as bindings;
use serde_json::{json, Value};

fn row() -> Value {
    json!({
        "binding_id": "01234567-89ab-cdef-8123-456789abcdef",
        "source_system": "ptg", "source_id": "source-one", "dataset_schema": "source_snapshot",
        "dataset_id": "dataset-one", "producer_id": "producer-one", "edition_id": "edition-one",
        "source_key": "opaque-company-key",
        "source_scope_json": {"cohort_id": "cohort-one", "snapshot_id": "snapshot-one", "company_key": "company-one"},
        "network_id": 42, "evidence_id": "review-one", "evidence_sha256": "a".repeat(64),
        "operation": "bind", "expected_revision": 0, "expected_network_id": null
    })
}

fn aca() -> Value {
    let mut input = row();
    input["source_system"] = json!("aca");
    input["source_scope_json"] = json!({
        "issuer_id": "12345", "state": "CA", "plan_year": 2026,
        "plan_id": "12345CA0000001", "checksum_network": -17
    });
    input
}

fn fhir() -> Value {
    let mut input = row();
    input["source_system"] = json!("fhir");
    input["source_scope_json"] = json!({
        "organization_id": "Organization-1.v2", "legacy_uuid": "01234567-89ab-cdef-8123-456789abcde0",
        "alias_scope": "alias-scope-one"
    });
    input
}

fn encode(rows: &[Value]) -> EncodedSourceBindingBatch {
    bindings::encode_network_source_binding_batch(&serde_json::to_vec(rows).unwrap()).unwrap()
}

fn reject(rows: &[Value], code: &str) {
    let error = bindings::encode_network_source_binding_batch(&serde_json::to_vec(rows).unwrap())
        .unwrap_err();
    assert_eq!(error.code, code);
    assert!(!format!("{error:?} {error}").contains("opaque-company-key"));
}

fn copy_rows(bytes: &[u8]) -> Vec<Vec<Option<&[u8]>>> {
    assert_eq!(&bytes[..19], b"PGCOPY\n\xff\r\n\0\0\0\0\0\0\0\0\0");
    let mut offset = 19;
    let mut rows = Vec::new();
    loop {
        let columns = i16::from_be_bytes(bytes[offset..offset + 2].try_into().unwrap());
        offset += 2;
        if columns == -1 {
            break;
        }
        assert_eq!(columns, 16);
        let mut row = Vec::new();
        for _ in 0..columns {
            let length = i32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap());
            offset += 4;
            if length == -1 {
                row.push(None);
                continue;
            }
            assert!(length >= 0);
            let end = offset + length as usize;
            row.push(Some(&bytes[offset..end]));
            offset = end;
        }
        rows.push(row);
    }
    assert_eq!(offset, bytes.len());
    rows
}

fn uuid_text(bytes: &[u8]) -> String {
    assert_eq!(bytes.len(), 16);
    let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    )
}

#[test]
fn binary_copy_matches_validated_rows() {
    let first = row();
    let mut second = aca();
    second["binding_id"] = json!("01234567-89ab-cdef-8123-456789abcde0");
    second["network_id"] = json!(i32::MAX);
    second["operation"] = json!("rebind");
    second["expected_revision"] = json!(MAX_EXPECTED_REVISION);
    second["expected_network_id"] = json!(1);
    let mut third = fhir();
    third["binding_id"] = json!("01234567-89ab-cdef-8123-456789abcde1");
    let result = encode(&[first, second, third]);
    assert_eq!(result.row_count, 3);
    assert_eq!(result.rows.len(), 3);
    assert_eq!(
        bindings::COPY_COLUMNS,
        [
            "binding_id",
            "source_system",
            "source_id",
            "dataset_schema",
            "dataset_id",
            "producer_id",
            "edition_id",
            "source_key",
            "source_scope_json",
            "binding_key",
            "network_id",
            "evidence_id",
            "evidence_sha256",
            "operation",
            "expected_revision",
            "expected_network_id"
        ]
    );
    let wire = copy_rows(&result.copy_bytes);
    for (fields, row) in wire.iter().zip(&result.rows) {
        assert_eq!(uuid_text(fields[0].unwrap()), row.binding_id);
        let strings = [
            &row.source_system,
            &row.source_id,
            &row.dataset_schema,
            &row.dataset_id,
            &row.producer_id,
            &row.edition_id,
            &row.source_key,
        ];
        for (field, value) in fields[1..8].iter().zip(strings) {
            assert_eq!(field.unwrap(), value.as_bytes());
        }
        let scope = fields[8].unwrap();
        assert_eq!(scope[0], 1);
        assert_eq!(
            serde_json::from_slice::<Value>(&scope[1..]).unwrap(),
            row.source_scope_json
        );
        assert_eq!(fields[9].unwrap(), row.binding_key.as_bytes());
        assert_eq!(
            i32::from_be_bytes(fields[10].unwrap().try_into().unwrap()),
            row.network_id
        );
        assert_eq!(fields[11].unwrap(), row.evidence_id.as_bytes());
        assert_eq!(fields[12].unwrap(), row.evidence_sha256.as_bytes());
        assert_eq!(fields[13].unwrap(), row.operation.as_bytes());
        assert_eq!(
            i64::from_be_bytes(fields[14].unwrap().try_into().unwrap()),
            row.expected_revision
        );
        assert_eq!(
            fields[15].map(|bytes| i32::from_be_bytes(bytes.try_into().unwrap())),
            row.expected_network_id
        );
    }
}

#[test]
fn binding_key_has_stable_canonical_digest() {
    let original = row();
    let result = encode(std::slice::from_ref(&original));
    let key = &result.rows[0].binding_key;
    assert_eq!(
        key,
        "c6696a3d2001082b40849ba9daab61f6164d22ee9b536b17d91a0ae7276dab7d"
    );
    let mut changed = original;
    changed["binding_id"] = json!("01234567-89ab-cdef-8123-456789abcde0");
    changed["network_id"] = json!(1);
    changed["evidence_id"] = json!("other-review");
    changed["evidence_sha256"] = json!("b".repeat(64));
    changed["operation"] = json!("close");
    changed["expected_revision"] = json!(7);
    changed["expected_network_id"] = json!(1);
    assert_eq!(&encode(&[changed.clone()]).rows[0].binding_key, key);
    let raw = serde_json::to_string(&vec![changed]).unwrap();
    let raw = raw.replace(
        r#"{"cohort_id":"cohort-one","company_key":"company-one","snapshot_id":"snapshot-one"}"#,
        r#"{"snapshot_id":"snapshot-one", "company_key":"company-one", "cohort_id":"cohort-one"}"#,
    );
    assert_eq!(
        &bindings::encode_network_source_binding_batch(raw.as_bytes())
            .unwrap()
            .rows[0]
            .binding_key,
        key
    );
}

#[test]
fn complete_source_scope_prevents_collisions() {
    let base = row();
    let key = encode(std::slice::from_ref(&base))
        .rows
        .remove(0)
        .binding_key;
    for pointer in [
        "/source_id",
        "/dataset_schema",
        "/dataset_id",
        "/producer_id",
        "/edition_id",
        "/source_key",
        "/source_scope_json/cohort_id",
        "/source_scope_json/snapshot_id",
        "/source_scope_json/company_key",
    ] {
        let mut changed = base.clone();
        *changed.pointer_mut(pointer).unwrap() = json!("other");
        assert_ne!(encode(&[changed]).rows[0].binding_key, key);
    }
    let first = aca();
    let mut second = first.clone();
    second["source_scope_json"]["issuer_id"] = json!("54321");
    second["source_scope_json"]["plan_id"] = json!("54321CA0000001");
    assert_ne!(
        encode(&[first]).rows[0].binding_key,
        encode(&[second]).rows[0].binding_key
    );
    assert_eq!(
        encode(&[fhir()]).rows[0].source_scope_json["organization_id"],
        "Organization-1.v2"
    );
    let first = fhir();
    let mut second = first.clone();
    second["source_scope_json"]["alias_scope"] = json!("alias-scope-two");
    assert_ne!(
        encode(&[first]).rows[0].binding_key,
        encode(&[second]).rows[0].binding_key
    );
}

#[test]
fn duplicates_reject_entire_batch() {
    reject(&[row(), row()], "duplicate_binding");
    let mut same_scope = row();
    same_scope["binding_id"] = json!("01234567-89ab-cdef-8123-456789abcde0");
    same_scope["network_id"] = json!(1);
    reject(&[row(), same_scope], "duplicate_binding");
    let mut same_id = row();
    same_id["edition_id"] = json!("other-edition");
    reject(&[row(), same_id], "duplicate_binding");
}

#[test]
fn explicit_operations_enforce_revision_rules() {
    for (operation, revision, previous, network, valid) in [
        ("bind", 0, None, 1, true),
        ("bind", 1, None, 1, false),
        ("bind", 0, Some(1), 1, false),
        ("rebind", 1, Some(1), 2, true),
        ("rebind", MAX_EXPECTED_REVISION, Some(i32::MAX), 1, true),
        ("rebind", 0, Some(1), 2, false),
        ("rebind", 1, None, 2, false),
        ("rebind", 1, Some(0), 2, false),
        ("close", 1, Some(2), 2, true),
        ("close", 1, Some(1), 2, false),
        ("close", 0, Some(2), 2, false),
        ("other", 0, None, 1, false),
        ("rebind", -1, Some(1), 2, false),
        ("rebind", MAX_EXPECTED_REVISION + 1, Some(1), 2, false),
        ("bind", 0, None, 0, false),
    ] {
        let mut input = row();
        input["operation"] = json!(operation);
        input["expected_revision"] = json!(revision);
        input["expected_network_id"] = json!(previous);
        input["network_id"] = json!(network);
        if valid {
            assert_eq!(encode(&[input]).row_count, 1);
        } else {
            reject(&[input], "invalid_operation");
        }
    }
}

#[test]
fn aca_scope_checks_exact_coordinates() {
    for (field, value) in [
        ("issuer_id", json!("00000")),
        ("issuer_id", json!("1234")),
        ("issuer_id", json!(12345)),
        ("state", json!("ca")),
        ("state", json!("ZZ")),
        ("plan_year", json!(2009)),
        ("plan_year", json!(2101)),
        ("plan_year", json!(2026.0)),
        ("plan_year", json!(true)),
        ("plan_id", json!("54321CA0000001")),
        ("plan_id", json!("12345NY0000001")),
        ("plan_id", json!("12345CA0000001-1")),
        ("plan_id", json!("12345CA0000001-AAA")),
        ("checksum_network", json!(true)),
        ("checksum_network", json!(-2_147_483_649_i64)),
    ] {
        let mut input = aca();
        input["source_scope_json"][field] = value;
        reject(&[input], "invalid_scope");
    }
    for year in [2010, 2100] {
        let mut input = aca();
        input["source_scope_json"]["plan_year"] = json!(year);
        input["source_scope_json"]["checksum_network"] = json!(i32::MIN);
        input["source_scope_json"]["plan_id"] = json!("12345CA0000001-01");
        assert_eq!(
            encode(&[input]).rows[0].source_scope_json["checksum_network"],
            i32::MIN
        );
    }
    for state in "AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY AS GU MP PR VI".split_whitespace() {
        let mut input = aca();
        input["source_scope_json"]["state"] = json!(state);
        input["source_scope_json"]["plan_id"] = json!(format!("12345{state}0000001"));
        assert_eq!(encode(&[input]).row_count, 1);
    }
}

#[test]
fn fhir_ids_and_uuids_are_canonical() {
    for value in ["", "a_b", "a/b", "a b", "é", " a", "a\n"] {
        let mut input = fhir();
        input["source_scope_json"]["organization_id"] = json!(value);
        reject(&[input], "invalid_scope");
    }
    let mut input = fhir();
    input["source_scope_json"]["organization_id"] = json!("a".repeat(64));
    assert_eq!(encode(&[input.clone()]).row_count, 1);
    input["source_scope_json"]["organization_id"] = json!("a".repeat(65));
    reject(&[input], "invalid_scope");
    for value in [
        "00000000-0000-0000-0000-000000000000",
        "01234567-89AB-CDEF-8123-456789ABCDEF",
        "0123456789abcdef8123456789abcdef",
        "other",
    ] {
        let mut input = fhir();
        input["source_scope_json"]["legacy_uuid"] = json!(value);
        reject(&[input], "invalid_scope");
        let mut input = row();
        input["binding_id"] = json!(value);
        reject(&[input], "invalid_uuid");
    }
}

#[test]
fn scoped_text_bounds_are_exact() {
    for (template, field, maximum) in [
        (row(), "cohort_id", 128),
        (row(), "snapshot_id", 128),
        (row(), "company_key", 512),
        (fhir(), "alias_scope", 512),
    ] {
        let mut input = template.clone();
        input["source_scope_json"][field] = json!("x".repeat(maximum));
        assert_eq!(encode(&[input]).row_count, 1);
        for value in [
            json!("x".repeat(maximum + 1)),
            json!(""),
            json!(" x"),
            json!("x "),
            json!("a\nb"),
            json!(true),
        ] {
            let mut input = template.clone();
            input["source_scope_json"][field] = value;
            reject(&[input], "invalid_scope");
        }
    }
}

#[test]
fn strict_rows_reject_missing_extra_fields() {
    let base = row();
    for field in base.as_object().unwrap().keys() {
        let mut input = base.clone();
        input.as_object_mut().unwrap().remove(field);
        reject(&[input], "invalid_row");
    }
    let mut input = row();
    input["extra"] = json!(1);
    reject(&[input], "invalid_row");
    for base in [aca(), row(), fhir()] {
        let object = base["source_scope_json"].as_object().unwrap();
        for field in object.keys() {
            let mut input = base.clone();
            input["source_scope_json"]
                .as_object_mut()
                .unwrap()
                .remove(field);
            reject(&[input], "invalid_scope");
        }
        let mut input = base;
        input["source_scope_json"]["extra"] = json!(1);
        reject(&[input], "invalid_scope");
    }
}

#[test]
fn duplicate_json_fields_are_rejected() {
    let raw = serde_json::to_string(&vec![row()]).unwrap();
    let duplicate = raw.replace(r#""network_id":42"#, r#""network_id":42,"network_id":42"#);
    assert_eq!(
        bindings::encode_network_source_binding_batch(duplicate.as_bytes())
            .unwrap_err()
            .code,
        "invalid_row"
    );
    for base in [row(), aca(), fhir()] {
        let raw = serde_json::to_string(&vec![base.clone()]).unwrap();
        let (field, value) = base["source_scope_json"]
            .as_object()
            .unwrap()
            .iter()
            .next()
            .unwrap();
        let value = serde_json::to_string(value).unwrap();
        let field = format!("\"{field}\":{value}");
        let duplicate = raw.replace(&field, &format!("{field},{field}"));
        assert_eq!(
            bindings::encode_network_source_binding_batch(duplicate.as_bytes())
                .unwrap_err()
                .code,
            "invalid_scope"
        );
    }
}

#[test]
fn bounded_text_rejects_coercion_and_controls() {
    for (field, maximum) in [
        ("source_id", 128),
        ("dataset_id", 128),
        ("producer_id", 128),
        ("edition_id", 128),
        ("source_key", 512),
        ("evidence_id", 512),
    ] {
        for value in [
            json!(""),
            json!(" x"),
            json!("x "),
            json!("a\0b"),
            json!("a\nb"),
            json!("x".repeat(maximum + 1)),
            json!(1),
            json!(null),
        ] {
            let mut input = row();
            input[field] = value;
            let error = bindings::encode_network_source_binding_batch(
                &serde_json::to_vec(&vec![input]).unwrap(),
            )
            .unwrap_err();
            assert!(matches!(error.code, "invalid_text" | "invalid_row"));
        }
        let mut input = row();
        input[field] = json!("é".repeat(maximum / 2));
        assert_eq!(encode(&[input]).row_count, 1);
    }
    for field in ["cohort_id", "snapshot_id", "company_key"] {
        let mut input = row();
        input["source_scope_json"][field] = json!(" ");
        reject(&[input], "invalid_scope");
    }
}

#[test]
fn schema_and_digest_are_strict() {
    for value in [
        "",
        "1schema",
        "a-b",
        "a.b",
        "a b",
        "é",
        "x".repeat(64).as_str(),
    ] {
        let mut input = row();
        input["dataset_schema"] = json!(value);
        reject(&[input], "invalid_schema");
    }
    let mut input = row();
    input["dataset_schema"] = json!("_".repeat(63));
    assert_eq!(encode(&[input]).row_count, 1);
    for value in [
        "",
        "a".repeat(63).as_str(),
        "A".repeat(64).as_str(),
        "g".repeat(64).as_str(),
    ] {
        let mut input = row();
        input["evidence_sha256"] = json!(value);
        reject(&[input], "invalid_digest");
    }
}

#[test]
fn integers_reject_boolean_float_and_overflow() {
    for field in ["network_id", "expected_revision", "expected_network_id"] {
        for value in [json!(true), json!(1.0), json!("1"), json!(u64::MAX)] {
            let mut input = row();
            input[field] = value;
            reject(&[input], "invalid_row");
        }
    }
    let mut input = row();
    input["network_id"] = json!(2_147_483_648_u64);
    reject(std::slice::from_ref(&input), "invalid_row");
    input["network_id"] = json!(-1);
    reject(&[input], "invalid_operation");
}

#[test]
fn bad_final_row_exposes_no_partial_output() {
    let mut bad = row();
    bad["source_scope_json"]["cohort_id"] = json!(true);
    let error = bindings::encode_network_source_binding_batch(
        &serde_json::to_vec(&vec![row(), bad]).unwrap(),
    )
    .unwrap_err();
    assert_eq!(error.code, "invalid_scope");
    assert_eq!(error.row_index, Some(1));
    assert_eq!(error.field, Some("source_scope_json"));
}

#[test]
fn byte_and_row_boundaries_are_exact() {
    let mut bytes = b"[]".to_vec();
    bytes.resize(MAX_INPUT_BYTES, b' ');
    let empty = bindings::encode_network_source_binding_batch(&bytes).unwrap();
    assert_eq!(empty.copy_bytes.len(), 21);
    assert_eq!(empty.row_count, 0);
    bytes.push(b' ');
    assert_eq!(
        bindings::encode_network_source_binding_batch(&bytes)
            .unwrap_err()
            .code,
        "input_limit"
    );
    reject(&vec![row(); MAX_ROWS + 1], "row_limit");
    let rows: Vec<_> = (1..=MAX_ROWS)
        .map(|index| {
            let mut input = row();
            input["binding_id"] = json!(format!("01234567-89ab-cdef-8123-{index:012x}"));
            input["edition_id"] = json!(format!("edition-{index}"));
            input
        })
        .collect();
    let result = encode(&rows);
    assert_eq!(result.row_count, MAX_ROWS);
    assert!(result.copy_bytes.len() <= bindings::MAX_COPY_BYTES);
    assert_eq!(copy_rows(&result.copy_bytes).len(), MAX_ROWS);
}

#[test]
fn malformed_json_is_sanitized() {
    for bytes in [
        b"{}".as_slice(),
        b"null",
        b"[",
        b"[null]",
        b"[[]]",
        b"[] []",
        &[b'[', 0xff, b']'],
    ] {
        let error = bindings::encode_network_source_binding_batch(bytes).unwrap_err();
        assert!(matches!(error.code, "invalid_json" | "invalid_row"));
        assert_eq!(
            serde_json::to_value(error)
                .unwrap()
                .as_object()
                .unwrap()
                .len(),
            3
        );
    }
    let mut input = row();
    input["source_system"] = json!("label");
    reject(&[input], "invalid_scope");
    for scope in [json!([]), json!(null), json!("scope")] {
        let mut input = row();
        input["source_scope_json"] = scope;
        reject(&[input], "invalid_scope");
    }
}

#[test]
fn published_plan_namespace_is_closed_and_keeps_cohort_compatibility() {
    let mut published = row();
    published["source_scope_json"] = json!({
        "review_type": "published_complete_snapshot_plan",
        "scope_id": "01234567-89ab-cdef-8123-456789abcde0",
        "approval_sha256": "b".repeat(64), "snapshot_id": "snapshot-one",
        "plan_id": "plan-one", "plan_market_type": "group",
        "selection_mode": "complete_snapshot_source_set"
    });
    assert_eq!(
        encode(&[published.clone()]).rows[0].source_scope_json,
        published["source_scope_json"]
    );
    assert_eq!(encode(&[row()]).row_count, 1);
    for field in [
        "scope_id",
        "approval_sha256",
        "snapshot_id",
        "plan_id",
        "selection_mode",
    ] {
        let mut bad = published.clone();
        bad["source_scope_json"]
            .as_object_mut()
            .unwrap()
            .remove(field);
        reject(&[bad], "invalid_scope");
    }
    let mut mixed = published.clone();
    mixed["source_scope_json"]["company_key"] = json!("must-not-inherit");
    reject(&[mixed], "invalid_scope");
    published["source_scope_json"]["scope_id"] = json!("00000000-0000-0000-0000-000000000000");
    reject(&[published], "invalid_scope");
}
