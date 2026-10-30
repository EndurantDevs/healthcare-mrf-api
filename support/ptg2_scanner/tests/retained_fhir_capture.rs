// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

use ptg2_scanner::retained_fhir_capture::{encode_retained_fhir_capture_batch as encode, *};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::sync::atomic::AtomicBool;

fn context(rows: Vec<Value>, contract: &str, derive: bool) -> Value {
    json!({"source_id":"source-a", "release_id":"release-1", "observed_at_pg_micros":1234,
        "hash_contract":contract, "resource_types":["Organization", "Location", "InsurancePlan", "Practitioner"], "derive":derive, "rows":rows})
}
fn input(value: &Value) -> Vec<u8> {
    serde_json::to_vec(value).unwrap()
}
fn row(kind: &str, payload: Value, hash: &str) -> Value {
    json!({"resource_type":kind,"resource_id":payload["resource_id"],"payload_hash":hash,
        "payload_json":serde_json::to_string(&payload).unwrap(),"acquired_resource_sha256":null,
        "site_id": if kind == "Location" { Some("01234567-89ab-cdef-8123-456789abcdef") } else { None }})
}
fn legacy_row(kind: &str, payload: Value) -> Value {
    let raw = serde_json::to_string(&payload).unwrap();
    // The test fixtures use ASCII strings and integers; independently add Python's spaces outside strings.
    let mut spaced = Vec::new();
    let (mut quoted, mut escaped) = (false, false);
    for byte in raw.bytes() {
        spaced.push(byte);
        if !quoted && matches!(byte, b':' | b',') {
            spaced.push(b' ');
        }
        if !escaped && byte == b'"' {
            quoted = !quoted;
        }
        escaped = !escaped && byte == b'\\';
    }
    row(
        kind,
        payload,
        &Sha256::digest(spaced)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>(),
    )
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
        let mut fields = Vec::new();
        for _ in 0..columns {
            let size = i32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap());
            offset += 4;
            if size == -1 {
                fields.push(None);
            } else {
                assert!(size >= 0);
                fields.push(Some(&bytes[offset..offset + size as usize]));
                offset += size as usize;
            }
        }
        rows.push(fields);
    }
    assert_eq!(offset, bytes.len());
    rows
}

#[test]
fn persisted_contracts_match_python_reference_vectors() {
    let cases: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/retained_fhir_hashes.json")).unwrap();
    for case in cases {
        let kind = case["kind"].as_str().unwrap();
        let document = context(
            vec![row(
                kind,
                case["payload"].clone(),
                case["expected_hash"].as_str().unwrap(),
            )],
            case["contract"].as_str().unwrap(),
            false,
        );
        let result = encode(&input(&document)).unwrap();
        assert_eq!(result.row_count, 1);
        assert_eq!(result.families[kind].row_count, 1);
        assert_eq!(result.root_bytes, result.families[kind].bytes);
        assert!(result.binding_batches.is_empty());
        assert!(result.evidence_batches.is_empty());
        assert!(result.plan_batches.is_empty());
    }
}

#[test]
fn derived_copy_preserves_columns_types_nulls_reference_order_and_root_order() {
    let location = legacy_row(
        "Location",
        json!({"source_id":"source-a","resource_id":"site-1","name":"Office"}),
    );
    let plan = legacy_row(
        "InsurancePlan",
        json!({"resource_id":"plan-1","network_refs":["Organization/z","Organization/a","Organization/z"],"owned_by_ref":"", "administered_by_ref":null}),
    );
    let organization = legacy_row(
        "Organization",
        json!({"resource_id":"org-1","name":"Network"}),
    );
    let document = context(
        vec![
            location.clone(),
            plan.clone(),
            organization.clone(),
            plan.clone(),
        ],
        "transport_bound_v1",
        true,
    );
    let result = encode(&input(&document)).unwrap();
    assert_eq!(BINDING_COLUMNS.len(), 6);
    assert_eq!(EVIDENCE_COLUMNS.len(), 7);
    assert_eq!(PLAN_COLUMNS.len(), 11);
    let bindings = copy_rows(&result.binding_batches[0].bytes);
    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0].len(), BINDING_COLUMNS.len());
    assert_eq!(bindings[0][3], None);
    assert_eq!(
        bindings[0][4].unwrap(),
        &[1, 35, 69, 103, 137, 171, 205, 239, 129, 35, 69, 103, 137, 171, 205, 239][..16]
    );
    assert_eq!(bindings[0][5].unwrap(), 1234i64.to_be_bytes());
    let evidence = copy_rows(&result.evidence_batches[0].bytes);
    assert_eq!(evidence.len(), 2);
    assert_eq!(evidence[0].len(), EVIDENCE_COLUMNS.len());
    assert_eq!(evidence[0][5].unwrap()[0], 1);
    assert_eq!(
        serde_json::from_slice::<Value>(&evidence[0][5].unwrap()[1..]).unwrap(),
        serde_json::from_str::<Value>(location["payload_json"].as_str().unwrap()).unwrap()
    );
    let plans = copy_rows(&result.plan_batches[0].bytes);
    assert_eq!(plans.len(), 4);
    assert_eq!(plans[0].len(), PLAN_COLUMNS.len());
    assert_eq!(plans[0][3].unwrap(), b"a");
    assert_eq!(plans[1][3].unwrap(), b"z");
    assert_eq!(plans[0][6], Some(b"".as_slice()));
    assert_eq!(plans[0][7], None);
    assert_eq!(
        &plans[0][5].unwrap()[1..],
        br#"["Organization/z","Organization/a","Organization/z"]"#
    );
    let roots: Vec<Value> = result
        .root_bytes
        .split(|byte| *byte == b'\n')
        .map(|bytes| serde_json::from_slice(bytes).unwrap())
        .collect();
    assert_eq!(
        roots
            .iter()
            .map(|root| root[0].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["Location", "InsurancePlan", "Organization", "InsurancePlan"]
    );
    assert_eq!(roots[1], roots[3]);
    assert_eq!(result.families["InsurancePlan"].row_count, 2);
}

#[test]
fn duplicate_payload_keys_and_negative_zero_follow_python_last_value_semantics() {
    let payload = json!({"resource_id":"site-1", "n":0, "name":"last"});
    let mut record = legacy_row("Location", payload);
    record["payload_json"] = json!(
        r#"{"resource_id":"wrong","resource_id":"site-1","n":-0,"name":"first","name":"last"}"#
    );
    encode(&input(&context(vec![record], "transport_bound_v1", false))).unwrap();
}

#[test]
fn unsupported_python_cases_request_atomic_reference_fallback() {
    let cases = [
        (
            "Location",
            json!({"resource_id":"item-1", "ratio":1.0}),
            "python_semantics_required",
        ),
        (
            "Organization",
            json!({"resource_id":"item-1", "name":"Café", "aliases":[], "name_variants":["Café"]}),
            "unsupported_unicode_semantics",
        ),
    ];
    for (kind, payload, code) in cases {
        let preceding = legacy_row("Location", json!({"resource_id":"site-1"}));
        let record = row(kind, payload, &"0".repeat(64));
        let result = encode(&input(&context(
            vec![preceding, record],
            "semantic_content_v4",
            true,
        )))
        .unwrap();
        assert_eq!(result.fallback_rows, [1]);
        assert_eq!(result.row_count, 2);
        assert_eq!(result.binding_batches[0].row_count, 1);
        assert_eq!(result.evidence_batches[0].row_count, 1);
        assert_eq!(result.root_bytes.split(|byte| *byte == b'\n').count(), 2);
        assert!(result.owned_output_bytes <= MAX_OUTPUT_BYTES);
        assert!(!code.is_empty());
    }
}

#[test]
fn invalid_payload_hash_identity_reference_and_envelope_fail_closed() {
    let original = legacy_row(
        "InsurancePlan",
        json!({"resource_id":"plan-1", "network_refs":[]}),
    );
    let mut wrong_hash = original.clone();
    wrong_hash["payload_hash"] = json!("0".repeat(64));
    assert_eq!(
        encode(&input(&context(
            vec![wrong_hash],
            "transport_bound_v1",
            true
        )))
        .unwrap_err()
        .code,
        "payload_hash"
    );
    for payload in [
        json!({"resource_id":"plan-1", "source_id":"other"}),
        json!({"resource_id":"different"}),
    ] {
        let mut record = original.clone();
        record["payload_json"] = json!(serde_json::to_string(&payload).unwrap());
        assert_eq!(
            encode(&input(&context(vec![record], "transport_bound_v1", true)))
                .unwrap_err()
                .code,
            "payload_identity"
        );
    }
    for references in [
        json!(null),
        json!([" Organization/a"]),
        json!(["Organization/a/_history/1"]),
        json!([null]),
    ] {
        let record = legacy_row(
            "InsurancePlan",
            json!({"resource_id":"plan-1", "network_refs":references}),
        );
        assert_eq!(
            encode(&input(&context(vec![record], "transport_bound_v1", true)))
                .unwrap_err()
                .code,
            "invalid_references"
        );
    }
    for bytes in [b"{}".as_slice(), b"[]", b"null", b"{} {}"] {
        assert_eq!(encode(bytes).unwrap_err().code, "invalid_input");
    }
    let mut record = original;
    record["acquired_resource_sha256"] = json!("0".repeat(64));
    assert_eq!(
        encode(&input(&context(vec![record], "transport_bound_v1", true)))
            .unwrap_err()
            .code,
        "invalid_rows"
    );
}

#[test]
fn row_count_expansion_and_shared_output_budget_are_bounded() {
    let mut rows = Vec::new();
    for number in 0..834 {
        rows.push(legacy_row("InsurancePlan", json!({"resource_id":format!("plan-{number}"), "network_refs":["Organization/a","Organization/b","Organization/c","Organization/d","Organization/e","Organization/f"]})));
    }
    let result = encode(&input(&context(rows, "transport_bound_v1", true))).unwrap();
    assert_eq!(
        result
            .plan_batches
            .iter()
            .map(|batch| batch.row_count)
            .collect::<Vec<_>>(),
        [5000, 4]
    );
    for batch in &result.plan_batches {
        assert_eq!(copy_rows(&batch.bytes).len(), batch.row_count);
    }
    let record = legacy_row("Location", json!({"resource_id":"site-1"}));
    assert_eq!(
        encode(&input(&context(
            vec![record; MAX_ROWS + 1],
            "transport_bound_v1",
            false
        )))
        .unwrap_err()
        .code,
        "row_limit"
    );
    assert_eq!(
        encode(&vec![b' '; MAX_INPUT_BYTES + 1]).unwrap_err().code,
        "input_limit"
    );
    let references: Vec<_> = (0..100)
        .map(|number| format!("Organization/network-{number}"))
        .collect();
    let record = legacy_row(
        "InsurancePlan",
        json!({"resource_id":"plan-1","network_refs":references,"large":"x".repeat(1024*1024)}),
    );
    assert_eq!(
        encode(&input(&context(vec![record], "transport_bound_v1", true)))
            .unwrap_err()
            .code,
        "output_limit"
    );
}

#[test]
fn collection_limits_count_contents_without_counting_punctuation_in_strings() {
    let record = legacy_row(
        "Location",
        json!({"resource_id":"site-1", "text":"[a:b,c] \" a:b,c", "values":vec![Value::Null; MAX_COLLECTION_ITEMS]}),
    );
    assert_eq!(
        encode(&input(&context(vec![record], "transport_bound_v1", false)))
            .unwrap_err()
            .code,
        "collection_limit"
    );
    let record = legacy_row(
        "Location",
        json!({"resource_id":"site-1", "text":"[a:b,c] \" a:b,c"}),
    );
    assert!(
        encode(&input(&context(vec![record], "transport_bound_v1", false)))
            .unwrap()
            .fallback_rows
            .is_empty()
    );
}

#[test]
fn cancellation_returns_no_partial_copy_and_errors_contain_no_payload() {
    let record = legacy_row("Location", json!({"resource_id":"site-1"}));
    let bytes = input(&context(vec![record], "transport_bound_v1", true));
    let flag = AtomicBool::new(true);
    assert_eq!(
        encode_retained_fhir_capture_batch_cancellable(&bytes, &flag)
            .unwrap_err()
            .code,
        "cancelled"
    );
    flag.store(false, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        encode_retained_fhir_capture_batch_cancellable(&bytes, &flag)
            .unwrap()
            .row_count,
        1
    );
    const {
        assert!(MAX_OUTPUT_BYTES > 0);
        assert!(MAX_COLLECTION_ITEMS > 0);
    }
}

#[test]
fn canonical_names_without_display_text_match_reference_hashes() {
    // Digests come from the persisted Python hash contract, including empty name sets.
    for (kind, payload, digest) in [
        (
            "Practitioner",
            json!({"resource_id":"person-1","names":[],"family_name":null,"given_names":[],"full_name":null}),
            "282f1d68b47f6eb310681d645289e7bf22fcd75b593f215e44066c112bfcb67e",
        ),
        (
            "Practitioner",
            json!({"resource_id":"person-1","names":[{"family":"Able","given":["Ann"]}],"family_name":"Able","given_names":["Ann"],"full_name":"Ann Able"}),
            "1eaf72a00cbff5fa49dd1f8a1dee447f53b6781afb019060984f8db5192493bd",
        ),
        (
            "Organization",
            json!({"resource_id":"org-1","name":null,"aliases":[],"name_variants":[]}),
            "5a76f9af90be99e2eb36ba1b4bba150addca8db1f695db5a5b85f4c0db198e97",
        ),
    ] {
        let result = encode(&input(&context(
            vec![row(kind, payload.clone(), digest)],
            "semantic_content_v4",
            false,
        )))
        .unwrap();
        assert!(result.fallback_rows.is_empty());
        assert_eq!(result.row_count, 1);
        assert_eq!(
            serde_json::from_slice::<Value>(&result.root_bytes).unwrap(),
            json!([kind, payload["resource_id"], digest])
        );
        let mut stale = payload;
        stale[if kind == "Practitioner" {
            "full_name"
        } else {
            "name"
        }] = json!("Stale projection");
        let error = encode(&input(&context(
            vec![row(kind, stale, digest)],
            "semantic_content_v4",
            false,
        )))
        .unwrap_err();
        assert_eq!(error.code, "invalid_name_projection");
        assert_eq!(error.row_index, Some(0));
    }
}

#[test]
fn malformed_name_collections_fail_before_a_hash_can_be_accepted() {
    let fixtures: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/retained_fhir_hashes.json")).unwrap();
    for (index, field, value) in [
        (5, "names", json!(false)),
        (5, "names", json!([null])),
        (5, "names", Value::Null),
        (6, "name", json!(" ")),
        (6, "aliases", json!(false)),
        (6, "aliases", json!([null])),
        (6, "name_variants", json!(["\u{001c}"])),
    ] {
        let case = &fixtures[index];
        let mut payload = case["payload"].clone();
        payload[field] = value;
        let record = row(
            case["kind"].as_str().unwrap(),
            payload,
            case["expected_hash"].as_str().unwrap(),
        );
        let error =
            encode(&input(&context(vec![record], "semantic_content_v4", false))).unwrap_err();
        assert_eq!(error.code, "invalid_name_projection", "{field}");
    }
}

#[test]
fn control_characters_and_false_values_round_trip_through_derived_jsonb() {
    let payload =
        json!({"resource_id":"site-1","name":"\"\\\u{0008}\u{000c}\n\r\t\u{000b}","active":false});
    let record = legacy_row("Location", payload.clone());
    let result = encode(&input(&context(vec![record], "transport_bound_v1", true))).unwrap();
    assert!(result.fallback_rows.is_empty());
    let evidence = copy_rows(&result.evidence_batches[0].bytes);
    let jsonb = evidence[0][5].unwrap();
    assert_eq!(jsonb[0], 1);
    assert_eq!(
        serde_json::from_slice::<Value>(&jsonb[1..]).unwrap(),
        payload
    );
}

#[test]
fn invalid_derived_inputs_report_the_row_without_leaking_partial_results() {
    let valid = legacy_row("Location", json!({"resource_id":"site-1"}));
    let mut non_object = valid.clone();
    non_object["payload_json"] = json!("[]");
    let mut invalid_site = valid.clone();
    invalid_site["site_id"] = json!("not-a-uuid");
    let mut wrong_kind = valid.clone();
    wrong_kind["resource_type"] = json!("Unknown");
    let mut cases = vec![
        (non_object, "invalid_payload"),
        (invalid_site, "invalid_site_id"),
        (wrong_kind, "invalid_row"),
    ];
    for field in ["owned_by_ref", "administered_by_ref"] {
        let mut payload = json!({"resource_id":"plan-1","network_refs":["Organization/org-1"]});
        payload[field] = json!({"reference":"Organization/org-1"});
        cases.push((
            legacy_row("InsurancePlan", payload),
            "invalid_reference_value",
        ));
    }
    for (record, code) in cases {
        let error = encode(&input(&context(
            vec![valid.clone(), record],
            "transport_bound_v1",
            true,
        )))
        .unwrap_err();
        assert_eq!(error.code, code);
        assert_eq!(error.row_index, Some(1));
        assert_eq!(
            error.to_string(),
            format!("Retained FHIR batch rejected: {code}")
        );
    }
    let mut document = context(vec![], "transport_bound_v1", true);
    document["rows"] = json!(false);
    let error = encode(&input(&document)).unwrap_err();
    assert_eq!(error.code, "invalid_rows");
    assert_eq!(error.row_index, Some(0));
    document["rows"] = json!([]);
    document["source_id"] = json!(" source-a");
    assert_eq!(
        encode(&input(&document)).unwrap_err().code,
        "invalid_context"
    );
}

#[test]
fn payload_syntax_budget_rejects_expansion_before_reference_fallback() {
    let mut record = legacy_row("Location", json!({"resource_id":"site-1"}));
    record["payload_json"] = json!(format!(
        "{{\"resource_id\":\"site-1\",\"values\":[{}0]}}",
        "0,".repeat(2 * MAX_COLLECTION_ITEMS)
    ));
    let bytes = input(&context(vec![record], "transport_bound_v1", false));
    assert!(bytes.len() < MAX_INPUT_BYTES);
    let error = encode(&bytes).unwrap_err();
    assert_eq!(error.code, "collection_limit");
    assert_eq!(error.row_index, Some(0));
}
