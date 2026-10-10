// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

use ptg2_scanner::fhir_network_identity::{
    encode_fhir_network_identity_batch, extract_fhir_network_identity_batch,
    FhirNetworkIdentityBatch, COPY_COLUMNS, MAX_COPY_BYTES, MAX_INPUT_BYTES, MAX_OUTPUT_BYTES,
    MAX_PAYLOAD_COLLECTION_ITEMS, MAX_REFERENCE_BYTES, MAX_REFERENCE_ITEMS, MAX_RESOURCES,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

fn input(kind: &str, resources: &[Value]) -> Vec<u8> {
    serde_json::to_vec(&json!({"source_id":"source-a", "release_id":"release-1",
        "resource_type":kind, "resources":resources}))
    .unwrap()
}

fn raw_input(kind: &str, raw: &str) -> Vec<u8> {
    format!("{{\"source_id\":\"source-a\",\"release_id\":\"release-1\",\"resource_type\":\"{kind}\",\"resources\":[{raw}]}}").into_bytes()
}

fn plan() -> Value {
    json!({"resourceType":"InsurancePlan", "id":"plan-1", "name":"A plan",
        "network":[{"reference":"Organization/network-1"}],
        "ownedBy":{"reference":"Organization/insurer-1"},
        "administeredBy":{"reference":"Organization/administrator-1"}})
}

fn organization(id: &str, role: Value) -> Value {
    json!({"resourceType":"Organization", "id":id, "name":"Example Network", "type":role})
}

fn batch(kind: &str, resources: &[Value]) -> FhirNetworkIdentityBatch {
    extract_fhir_network_identity_batch(&input(kind, resources)).unwrap()
}

fn digest(raw: &[u8]) -> String {
    Sha256::digest(raw)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn rejected(input: &[u8], code: &str, ordinal: Option<usize>) {
    let error = extract_fhir_network_identity_batch(input).unwrap_err();
    assert_eq!(error.code, code);
    assert_eq!(error.source_row_ordinal, ordinal);
    assert_eq!(
        error.to_string(),
        format!("FHIR network batch rejected: {code}")
    );
    assert_eq!(
        serde_json::to_value(error)
            .unwrap()
            .as_object()
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn existing_plan_fixture_preserves_nested_reference_order_and_payers() {
    let mut resource = plan();
    resource["plan"] = json!([{"network":[{"reference":"Organization/network-2"}]}]);
    resource["coverage"] = json!([{"network":[{"reference":"Organization/network-3"}]}]);
    let result = batch("InsurancePlan", &[resource.clone()]);
    assert_eq!(result.input_count, 1);
    assert!(result.organizations.is_empty());
    let row = &result.plans[0];
    assert_eq!(row.source_row_ordinal, 1);
    assert_eq!(row.plan_resource_id, "plan-1");
    assert_eq!(
        row.network_refs,
        [
            "Organization/network-1",
            "Organization/network-2",
            "Organization/network-3"
        ]
    );
    assert_eq!(
        row.candidate_local_target_ids,
        ["network-1", "network-2", "network-3"]
    );
    assert_eq!(row.exact_local_target_ids, row.candidate_local_target_ids);
    assert_eq!(row.owned_by_ref.as_deref(), Some("Organization/insurer-1"));
    assert_eq!(
        row.administered_by_ref.as_deref(),
        Some("Organization/administrator-1")
    );
    assert_eq!(
        row.payload_sha256,
        digest(&serde_json::to_vec(&resource).unwrap())
    );
}

#[test]
fn roles_require_exact_source_declaration_and_never_merge_names() {
    let resources = vec![
        organization("network-1", json!([{"text":"ntwk"}])),
        organization(
            "network-2",
            json!([{"coding":[{"code":"ntwk","system":"unrelated"}]}]),
        ),
        organization("network-3", json!([{"text":"NTWK"}])),
        organization("network-4", json!({"text":"ntwk"})),
        organization("network-5", json!([null, {"coding":[true, {"code":1}]}])),
    ];
    let result = batch("Organization", &resources);
    assert_eq!(result.input_count, 5);
    assert!(result.plans.is_empty());
    assert_eq!(
        result
            .organizations
            .iter()
            .map(|row| row.resource_id.as_str())
            .collect::<Vec<_>>(),
        ["network-1", "network-2"]
    );
    assert_eq!(result.organizations[1].source_row_ordinal, 2);
    assert_eq!(result.duplicate_count, 0);
}

#[test]
fn raw_references_and_trimmed_candidates_remain_distinct() {
    let mut resource = plan();
    resource["network"] = json!([
        {"reference":" Organization/padded\u{001c}"},
        {"reference":"Organization/exact"}, {"reference":"Organization/exact"},
        {"reference":"https://example.org/Organization/external"},
        {"reference":"Organization/exact/_history/1"},
        {"reference":"Organization/exact?version=1"}, {"reference":"organization/wrong"},
        {"reference":"urn:uuid:unresolved"}, {"reference":""},
        {"reference":4}, {"display":"Unresolved"}, null, "Organization/ignored"
    ]);
    resource["plan"] = json!([null, {"network":{"reference":"Organization/other"}}]);
    resource["coverage"] = json!([{"network":[{"reference":"Organization/exact"}]}]);
    let result = batch("InsurancePlan", &[resource]);
    let row = &result.plans[0];
    assert_eq!(row.candidate_local_target_ids, ["padded", "exact", "other"]);
    assert_eq!(row.exact_local_target_ids, ["exact", "other"]);
    assert_eq!(row.network_refs.len(), 9);
    assert_eq!(
        row.network_refs.first().unwrap(),
        " Organization/padded\u{001c}"
    );
    assert_eq!(row.network_refs.last().unwrap(), "Organization/other");
    assert!(row.network_refs.contains(&String::new()));
}

#[test]
fn payer_evidence_preserves_raw_strings_without_relationship_inference() {
    let mut first = plan();
    first["ownedBy"] = json!({"reference":""});
    first["administeredBy"] = json!({"reference":" https://example.org/payer "});
    let mut second = plan();
    second["id"] = json!("plan-2");
    second["ownedBy"] = json!({"reference":1});
    second["administeredBy"] = json!([{"reference":"Organization/ignored"}]);
    let result = batch("InsurancePlan", &[first, second]);
    assert_eq!(result.plans[0].owned_by_ref.as_deref(), Some(""));
    assert_eq!(
        result.plans[0].administered_by_ref.as_deref(),
        Some(" https://example.org/payer ")
    );
    assert_eq!(result.plans[1].owned_by_ref, None);
    assert_eq!(result.plans[1].administered_by_ref, None);
}

#[test]
fn duplicates_retain_rows_but_different_payloads_reject_the_whole_batch() {
    let original = plan();
    let result = batch("InsurancePlan", &[original.clone(), original.clone()]);
    assert_eq!(result.input_count, 2);
    assert_eq!(result.duplicate_count, 1);
    assert_eq!(result.plans.len(), 2);
    assert_eq!(result.plans[1].source_row_ordinal, 2);
    let mut changed = original.clone();
    changed["network"] = json!([{"reference":"Organization/another"}]);
    rejected(
        &input("InsurancePlan", &[original, changed]),
        "payload_conflict",
        Some(2),
    );
    let organization = organization("not-network", json!([]));
    let mut changed = organization.clone();
    changed["name"] = json!("Changed");
    rejected(
        &input("Organization", &[organization, changed]),
        "payload_conflict",
        Some(2),
    );
}

#[test]
fn hashes_bind_received_unicode_and_float_spelling_without_reencoding() {
    let originals = [
        r#"{"id":"plan-1","name":"Café","ratio":1e+03,"resourceType":"InsurancePlan"}"#,
        r#"{"id":"plan-1","name":"Caf\u00e9","ratio":1000.0,"resourceType":"InsurancePlan"}"#,
    ];
    let mut hashes = Vec::new();
    for raw in originals {
        let result = extract_fhir_network_identity_batch(&raw_input("InsurancePlan", raw)).unwrap();
        assert_eq!(result.plans[0].payload_sha256, digest(raw.as_bytes()));
        hashes.push(result.plans[0].payload_sha256.clone());
    }
    assert_ne!(hashes[0], hashes[1]);
    let joined = format!("{},{}", originals[0], originals[1]);
    rejected(
        &raw_input("InsurancePlan", &joined),
        "payload_conflict",
        Some(2),
    );
}

#[test]
fn plan_ids_use_unicode_character_bounds_and_organization_network_ids_use_fhir_bounds() {
    let mut resource = plan();
    resource["id"] = json!("é".repeat(256));
    assert_eq!(
        batch("InsurancePlan", &[resource.clone()]).plans[0]
            .plan_resource_id
            .chars()
            .count(),
        256
    );
    resource["id"] = json!("é".repeat(257));
    rejected(
        &input("InsurancePlan", &[resource]),
        "invalid_identity",
        Some(1),
    );
    for id in ["", " padded", "trailing\u{001f}", "\u{00a0}plan"] {
        let mut resource = plan();
        resource["id"] = json!(id);
        rejected(
            &input("InsurancePlan", &[resource]),
            "invalid_identity",
            Some(1),
        );
    }
    assert_eq!(
        batch(
            "Organization",
            &[organization(&"a".repeat(64), json!([{"text":"ntwk"}]))]
        )
        .organizations
        .len(),
        1
    );
    for id in ["é", "with space", "a/b", &"a".repeat(65)] {
        rejected(
            &input(
                "Organization",
                &[organization(id, json!([{"text":"ntwk"}]))],
            ),
            "invalid_organization_id",
            Some(1),
        );
    }
}

#[test]
fn scope_is_exact_python_trimmed_unicode_text_and_never_name_based() {
    let mut value: Value = serde_json::from_slice(&input("InsurancePlan", &[plan()])).unwrap();
    value["source_id"] = json!("é".repeat(64));
    value["release_id"] = json!("界".repeat(256));
    let result = extract_fhir_network_identity_batch(&serde_json::to_vec(&value).unwrap()).unwrap();
    assert_eq!(result.source_id.chars().count(), 64);
    assert_eq!(result.release_id.chars().count(), 256);
    for (field, invalid) in [
        ("source_id", "é".repeat(65)),
        ("release_id", "界".repeat(257)),
        ("source_id", "\u{001c}source".into()),
        ("release_id", "release\u{2003}".into()),
    ] {
        let mut changed = value.clone();
        changed[field] = json!(invalid);
        rejected(
            &serde_json::to_vec(&changed).unwrap(),
            "invalid_identity",
            None,
        );
    }
    let mut other: Value = serde_json::from_slice(&input("InsurancePlan", &[plan()])).unwrap();
    other["source_id"] = json!("source-b");
    let other = extract_fhir_network_identity_batch(&serde_json::to_vec(&other).unwrap()).unwrap();
    assert_ne!(other.source_id, result.source_id);
    assert_eq!(
        other.plans[0].payload_sha256,
        result.plans[0].payload_sha256
    );
}

#[test]
fn homogeneous_rows_and_exact_envelope_are_required_with_static_late_errors() {
    for raw in [b"{}".as_slice(), b"null", b"{", b"\xff"] {
        rejected(raw, "invalid_batch", None);
    }
    let mut value: Value = serde_json::from_slice(&input("InsurancePlan", &[plan()])).unwrap();
    value["unknown"] = json!("not returned");
    rejected(&serde_json::to_vec(&value).unwrap(), "invalid_batch", None);
    value.as_object_mut().unwrap().remove("unknown");
    value["resource_type"] = json!("Patient");
    rejected(
        &serde_json::to_vec(&value).unwrap(),
        "invalid_resource_type",
        None,
    );
    value["resource_type"] = json!("InsurancePlan");
    value["resources"] = json!({});
    rejected(
        &serde_json::to_vec(&value).unwrap(),
        "invalid_resources",
        None,
    );
    rejected(&input("InsurancePlan", &[]), "resource_count", None);
    rejected(
        &input("InsurancePlan", &[plan(), json!(null)]),
        "invalid_resource",
        Some(2),
    );
    rejected(
        &input(
            "InsurancePlan",
            &[plan(), organization("network-1", json!([]))],
        ),
        "resource_type_mismatch",
        Some(2),
    );
    let mut invalid = plan();
    invalid["id"] = json!(" confidential-invalid ");
    let error = extract_fhir_network_identity_batch(&input("InsurancePlan", &[plan(), invalid]))
        .unwrap_err();
    assert!(!error.to_string().contains("confidential"));
    assert_eq!(error.code, "invalid_identity");
    assert_eq!(error.source_row_ordinal, Some(2));
}

#[test]
fn malformed_nested_collections_fail_or_remain_ignored_as_existing_extraction_requires() {
    for invalid in [json!(true), json!(1), json!(-1.5)] {
        let mut resource = plan();
        resource["plan"] = invalid;
        rejected(
            &input("InsurancePlan", &[resource]),
            "invalid_network_collection",
            Some(1),
        );
    }
    for ignored in [
        json!(false),
        json!(0),
        json!(null),
        json!({"network": []}),
        json!("ignored"),
    ] {
        let mut resource = plan();
        resource["coverage"] = ignored;
        assert_eq!(
            batch("InsurancePlan", &[resource]).plans[0]
                .network_refs
                .len(),
            1
        );
    }
}

#[test]
fn complete_thousand_resource_batch_retains_every_ordinal_and_rejects_overflow() {
    let resources: Vec<Value> = (0..MAX_RESOURCES)
        .map(|index| json!({"resourceType":"InsurancePlan", "id":format!("plan-{index}")}))
        .collect();
    let result = batch("InsurancePlan", &resources);
    assert_eq!(result.plans.len(), MAX_RESOURCES);
    assert_eq!(
        result.plans.last().unwrap().source_row_ordinal,
        MAX_RESOURCES
    );
    let mut overflow = resources;
    overflow.push(plan());
    rejected(
        &input("InsurancePlan", &overflow),
        "resource_count",
        Some(MAX_RESOURCES + 1),
    );
}

fn padded_raw_input(bytes: usize) -> Vec<u8> {
    let prefix = b"{\"source_id\":\"source-a\",\"release_id\":\"release-1\",\"resource_type\":\"InsurancePlan\",\"resources\":[{\"id\":\"plan-1\",\"resourceType\":\"InsurancePlan\",\"padding\":\"";
    let suffix = b"\"}]}";
    let mut input = prefix.to_vec();
    input.resize(bytes - suffix.len(), b'x');
    input.extend_from_slice(suffix);
    input
}

#[test]
fn encoded_input_boundary_is_enforced_before_parsing_and_unknown_payload_is_hashed() {
    let exact = padded_raw_input(MAX_INPUT_BYTES);
    let result = extract_fhir_network_identity_batch(&exact).unwrap();
    assert_eq!(result.plans.len(), 1);
    let raw = std::str::from_utf8(&exact)
        .unwrap()
        .split_once("\"resources\":[")
        .unwrap()
        .1;
    assert_eq!(
        result.plans[0].payload_sha256,
        digest(&raw.as_bytes()[..raw.len() - 2])
    );
    rejected(&padded_raw_input(MAX_INPUT_BYTES + 1), "input_limit", None);
}

#[test]
fn serialized_output_boundary_and_reference_byte_limit_are_independent() {
    let mut resource =
        json!({"resourceType":"InsurancePlan", "id":"plan-1", "network":{"reference":""}});
    let baseline = serde_json::to_vec(&batch("InsurancePlan", &[resource.clone()]))
        .unwrap()
        .len();
    let available = MAX_OUTPUT_BYTES - baseline;
    resource["network"]["reference"] = json!("x".repeat(available));
    let result = batch("InsurancePlan", &[resource.clone()]);
    assert_eq!(serde_json::to_vec(&result).unwrap().len(), MAX_OUTPUT_BYTES);
    resource["network"]["reference"] = json!("x".repeat(available + 1));
    rejected(
        &input("InsurancePlan", &[resource.clone()]),
        "output_limit",
        None,
    );
    resource["network"]["reference"] = json!("x".repeat(MAX_REFERENCE_BYTES + 1));
    rejected(
        &input("InsurancePlan", &[resource]),
        "reference_limit",
        Some(1),
    );
}

fn collection_input(field: &str, item: &[u8], count: usize) -> Vec<u8> {
    let mut raw = format!("{{\"id\":\"plan-1\",\"resourceType\":\"InsurancePlan\",\"{field}\":[")
        .into_bytes();
    for index in 0..count {
        if index > 0 {
            raw.push(b',');
        }
        raw.extend_from_slice(item);
    }
    raw.extend_from_slice(b"]}");
    raw_input("InsurancePlan", std::str::from_utf8(&raw).unwrap())
}

#[test]
fn payload_and_reference_collection_caps_bound_even_ignored_entries() {
    let exact = collection_input("padding", b"0", MAX_PAYLOAD_COLLECTION_ITEMS - 3);
    assert_eq!(
        extract_fhir_network_identity_batch(&exact)
            .unwrap()
            .plans
            .len(),
        1
    );
    drop(exact);
    rejected(
        &collection_input("padding", b"0", MAX_PAYLOAD_COLLECTION_ITEMS - 2),
        "payload_collection_limit",
        Some(1),
    );
    let exact = collection_input("network", b"null", MAX_REFERENCE_ITEMS);
    assert!(
        extract_fhir_network_identity_batch(&exact).unwrap().plans[0]
            .network_refs
            .is_empty()
    );
    drop(exact);
    rejected(
        &collection_input("network", b"null", MAX_REFERENCE_ITEMS + 1),
        "reference_count",
        Some(1),
    );
}

#[test]
fn json_escaping_is_included_in_output_budget() {
    let resource = json!({"resourceType":"InsurancePlan", "id":"plan-1", "network":{"reference":"\u{0000}".repeat(MAX_OUTPUT_BYTES / 6)}});
    let input = input("InsurancePlan", &[resource]);
    assert!(input.len() < MAX_INPUT_BYTES);
    rejected(&input, "output_limit", None);
}

fn copy_rows(bytes: &[u8]) -> Vec<Vec<&[u8]>> {
    assert_eq!(&bytes[..11], b"PGCOPY\n\xff\r\n\0");
    assert_eq!(&bytes[11..19], &[0; 8]);
    let mut cursor = 19;
    let mut rows = Vec::new();
    loop {
        let columns = i16::from_be_bytes(bytes[cursor..cursor + 2].try_into().unwrap());
        cursor += 2;
        if columns == -1 {
            break;
        }
        assert_eq!(columns, 5);
        let mut row = Vec::new();
        for _ in 0..columns {
            let length = i32::from_be_bytes(bytes[cursor..cursor + 4].try_into().unwrap());
            cursor += 4;
            assert!(length >= 0);
            let end = cursor + length as usize;
            row.push(&bytes[cursor..end]);
            cursor = end;
        }
        rows.push(row);
    }
    assert_eq!(cursor, bytes.len());
    rows
}

fn copy_rejected(input: &[u8], code: &str, ordinal: Option<usize>) {
    let error = encode_fhir_network_identity_batch(input).unwrap_err();
    assert_eq!(error.code, code);
    assert_eq!(error.source_row_ordinal, ordinal);
    assert_eq!(
        error.to_string(),
        format!("FHIR network batch rejected: {code}")
    );
}

#[test]
fn copy_framing_preserves_exact_resource_bytes_hash_and_plan_observation() {
    let raw = r#"{"id":"plan-1","name":"Caf\u00e9","network":[{"reference":" Organization/padded "},{"reference":"Organization/exact"}],"ratio":1e+03,"resourceType":"InsurancePlan","unknown":{"retained":true}}"#;
    let input = raw_input("InsurancePlan", raw);
    let extracted = extract_fhir_network_identity_batch(&input).unwrap();
    let encoded = encode_fhir_network_identity_batch(&input).unwrap();
    assert_eq!(
        COPY_COLUMNS,
        [
            "source_row_ordinal",
            "resource_id",
            "payload_sha256",
            "resource_json",
            "observation_json"
        ]
    );
    assert_eq!(
        (
            encoded.row_count,
            encoded.input_count,
            encoded.duplicate_count
        ),
        (1, 1, 0)
    );
    let rows = copy_rows(&encoded.copy_bytes);
    assert_eq!(rows.len(), 1);
    assert_eq!(i32::from_be_bytes(rows[0][0].try_into().unwrap()), 1);
    assert_eq!(rows[0][1], b"plan-1");
    assert_eq!(rows[0][2], digest(raw.as_bytes()).as_bytes());
    assert_eq!(rows[0][3][0], 1);
    assert_eq!(&rows[0][3][1..], raw.as_bytes());
    assert_eq!(rows[0][4][0], 1);
    let observation: Value = serde_json::from_slice(&rows[0][4][1..]).unwrap();
    assert_eq!(
        observation,
        serde_json::to_value(&extracted.plans[0]).unwrap()
    );
    assert_eq!(encode_fhir_network_identity_batch(&input).unwrap(), encoded);
}

#[test]
fn copy_keeps_non_network_organizations_and_exact_duplicates_as_source_rows() {
    let explicit = organization("network-1", json!([{"text":"ntwk"}]));
    let ignored = organization("ordinary-organization", json!([{"text":"ins"}]));
    let input = input(
        "Organization",
        &[explicit.clone(), ignored.clone(), explicit.clone()],
    );
    let encoded = encode_fhir_network_identity_batch(&input).unwrap();
    assert_eq!(
        (
            encoded.row_count,
            encoded.input_count,
            encoded.duplicate_count
        ),
        (3, 3, 1)
    );
    let rows = copy_rows(&encoded.copy_bytes);
    for (index, original) in [explicit.clone(), ignored, explicit].iter().enumerate() {
        let row = &rows[index];
        assert_eq!(
            i32::from_be_bytes(row[0].try_into().unwrap()),
            (index + 1) as i32
        );
        assert_eq!(&row[3][1..], serde_json::to_vec(original).unwrap());
        let observation: Value = serde_json::from_slice(&row[4][1..]).unwrap();
        assert_eq!(observation.as_object().unwrap().len(), 4);
        assert_eq!(observation["source_row_ordinal"], index + 1);
        assert_eq!(observation["resource_id"], original["id"]);
        assert_eq!(observation["network_role"], index != 1);
        assert_eq!(observation["payload_sha256"], digest(&row[3][1..]));
    }
}

#[test]
fn copy_rejects_all_nested_nul_locations_but_preserves_other_controls_and_literal_escape() {
    for field in [
        json!({"nested":[{"bad":"before\u{0000}after"}]}),
        json!({"bad\u{0000}key":1}),
    ] {
        let mut resource = plan();
        resource["id"] = json!("plan-2");
        resource["unknown"] = field;
        copy_rejected(
            &input("InsurancePlan", &[plan(), resource]),
            "invalid_jsonb",
            Some(2),
        );
    }
    for field in ["id", "ownedBy", "network"] {
        let mut resource = plan();
        resource[field] = match field {
            "id" => json!("plan\u{0000}id"),
            "ownedBy" => json!({"reference":"payer\u{0000}reference"}),
            _ => json!([{"reference":"network\u{0000}reference"}]),
        };
        copy_rejected(
            &input("InsurancePlan", &[resource]),
            "invalid_jsonb",
            Some(1),
        );
    }
    let mut resource = plan();
    resource["unknown"] = json!({"controls":"\u{0001}\t\n\r\u{001f}", "literal":r"\u0000"});
    let encoded =
        encode_fhir_network_identity_batch(&input("InsurancePlan", &[resource.clone()])).unwrap();
    let rows = copy_rows(&encoded.copy_bytes);
    assert_eq!(
        serde_json::from_slice::<Value>(&rows[0][3][1..]).unwrap(),
        resource
    );
    for field in ["source_id", "release_id"] {
        let mut envelope: Value =
            serde_json::from_slice(&input("InsurancePlan", &[plan()])).unwrap();
        envelope[field] = json!("scope\u{0000}value");
        copy_rejected(
            &serde_json::to_vec(&envelope).unwrap(),
            "invalid_jsonb",
            None,
        );
    }
}

#[test]
fn copy_validation_is_atomic_for_late_conflicts_malformed_unicode_and_missing_rows() {
    let mut changed = plan();
    changed["name"] = json!("do-not-emit");
    copy_rejected(
        &input("InsurancePlan", &[plan(), changed]),
        "payload_conflict",
        Some(2),
    );
    copy_rejected(
        &input("InsurancePlan", &[plan(), json!({})]),
        "resource_type_mismatch",
        Some(2),
    );
    copy_rejected(
        &raw_input(
            "InsurancePlan",
            r#"{"resourceType":"InsurancePlan","id":"plan-1","unknown":"\ud800"}"#,
        ),
        "invalid_resource",
        Some(1),
    );
    copy_rejected(&input("InsurancePlan", &[]), "resource_count", None);
    let error = encode_fhir_network_identity_batch(&input(
        "InsurancePlan",
        &[json!({"resourceType":"InsurancePlan","id":" padded-id "})],
    ))
    .unwrap_err();
    assert!(!serde_json::to_string(&error).unwrap().contains("padded-id"));
}

#[test]
fn copy_thousand_row_replay_retains_every_source_ordinal() {
    let input = input("InsurancePlan", &vec![plan(); MAX_RESOURCES]);
    let encoded = encode_fhir_network_identity_batch(&input).unwrap();
    assert_eq!(
        (
            encoded.row_count,
            encoded.input_count,
            encoded.duplicate_count
        ),
        (MAX_RESOURCES, MAX_RESOURCES, MAX_RESOURCES - 1)
    );
    let rows = copy_rows(&encoded.copy_bytes);
    assert_eq!(rows.len(), MAX_RESOURCES);
    let observation: Value = serde_json::from_slice(&rows[MAX_RESOURCES - 1][4][1..]).unwrap();
    assert_eq!(observation["source_row_ordinal"], MAX_RESOURCES);
    assert_eq!(rows[0][3], rows[MAX_RESOURCES - 1][3]);
    assert_eq!(rows[0][2], rows[MAX_RESOURCES - 1][2]);
    assert_eq!(encode_fhir_network_identity_batch(&input).unwrap(), encoded);
}

#[test]
fn copy_uses_existing_input_and_aggregate_extraction_output_boundaries() {
    let exact = padded_raw_input(MAX_INPUT_BYTES);
    let encoded = encode_fhir_network_identity_batch(&exact).unwrap();
    assert!(encoded.copy_bytes.len() < MAX_COPY_BYTES);
    assert_eq!(copy_rows(&encoded.copy_bytes).len(), 1);
    drop(encoded);
    drop(exact);
    copy_rejected(&padded_raw_input(MAX_INPUT_BYTES + 1), "input_limit", None);
    let mut resource =
        json!({"resourceType":"InsurancePlan", "id":"plan-1", "network":{"reference":""}});
    let baseline = serde_json::to_vec(&batch("InsurancePlan", &[resource.clone()]))
        .unwrap()
        .len();
    resource["network"]["reference"] = json!("x".repeat(MAX_OUTPUT_BYTES - baseline));
    let encoded =
        encode_fhir_network_identity_batch(&input("InsurancePlan", &[resource.clone()])).unwrap();
    assert!(encoded.copy_bytes.len() < MAX_COPY_BYTES);
    let rows = copy_rows(&encoded.copy_bytes);
    assert!(rows[0][4].len() < MAX_OUTPUT_BYTES);
    resource["network"]["reference"] = json!("x".repeat(MAX_OUTPUT_BYTES - baseline + 1));
    copy_rejected(&input("InsurancePlan", &[resource]), "output_limit", None);
}
