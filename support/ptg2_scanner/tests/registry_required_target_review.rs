// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

use ptg2_scanner::network_source_binding_values::encode_network_source_binding_batch;
use ptg2_scanner::registry_required_target_review::validate_registry_required_target_review_artifacts as validate_bundle;
use ptg2_scanner::registry_required_target_review::{
    encode_registry_required_target_review_artifact as encode,
    validate_registry_required_target_ledger_artifact as validate_ledger_artifact, MAX_INPUT_BYTES,
    MAX_LEDGER_INPUT_BYTES,
};
use ptg2_scanner::registry_target_ledger::{encode_registry_target_ledger_artifact, HEADERS};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

const LEDGER: &str = "34567890-3456-7890-8345-34567890abcd";
const REVIEW: &str = "45678901-4567-8901-8456-45678901abcd";
const RIBBON: &str = "12345678-1234-5678-8123-123456789abc";

fn sha(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn fields(copy: &[u8]) -> Vec<&[u8]> {
    assert_eq!(&copy[..19], b"PGCOPY\n\xff\r\n\0\0\0\0\0\0\0\0\0");
    assert_eq!(i16::from_be_bytes(copy[19..21].try_into().unwrap()), 6);
    let mut offset = 21;
    let values = (0..6)
        .map(|_| {
            let length = i32::from_be_bytes(copy[offset..offset + 4].try_into().unwrap());
            assert!(length >= 0);
            offset += 4;
            let value = &copy[offset..offset + length as usize];
            offset += length as usize;
            value
        })
        .collect();
    assert_eq!(&copy[offset..], &(-1i16).to_be_bytes());
    values
}

fn ledger(fc: &str) -> Value {
    let mut csv = csv::Writer::from_writer(Vec::new());
    csv.write_record(HEADERS).unwrap();
    for ribbon in [RIBBON, "23456789-2345-6789-8234-23456789abcd"] {
        csv.write_record([
            "Example",
            "Company",
            "Plan",
            "PPO",
            "Carrier",
            "Network",
            fc,
            &json!([ribbon]).to_string(),
        ])
        .unwrap();
    }
    let input = csv.into_inner().unwrap();
    let (copy, _) = encode_registry_target_ledger_artifact(&input, LEDGER).unwrap();
    serde_json::from_slice(&fields(&copy)[4][1..]).unwrap()
}

fn binding(system: &str) -> Value {
    let scope = match system {
        "aca" => {
            json!({"issuer_id":"12345","state":"CA","plan_year":2026,"plan_id":"12345CA0000001","checksum_network":-17})
        }
        "fhir" => {
            json!({"organization_id":"Organization-1","legacy_uuid":RIBBON,"alias_scope":"alias-one"})
        }
        _ => {
            json!({"cohort_id":"cohort-one","snapshot_id":"snapshot-one","company_key":"company-one"})
        }
    };
    json!({"binding_id":"01234567-89ab-cdef-8123-456789abcdef","source_system":system,
      "source_id":"source-one","dataset_schema":"source_snapshot","dataset_id":"dataset-one",
      "producer_id":"producer-one","edition_id":"edition-one","source_key":"opaque-source-key","source_scope_json":scope})
}

fn decision(target: &str, system: &str) -> Value {
    json!({"target_key":target,"resolution_status":"resolved","network_id":42,"source_binding":binding(system),
      "evidence_reference":"reviewer evidence é","evidence_sha256":"a".repeat(64),"reason":"Explicit source evidence"})
}

fn review(ledger: &Value) -> Value {
    json!({"ledger_snapshot_id":LEDGER,"ledger_artifact_sha256":sha(&serde_json::to_vec(ledger).unwrap()),
      "decisions":[decision(ledger["ledger"]["targets"][0]["target_key"].as_str().unwrap(),"ptg")]})
}

fn encode_values(input: &Value, ledger: &Value) -> (Vec<u8>, Vec<u8>) {
    encode(
        &serde_json::to_vec(input).unwrap(),
        &serde_json::to_vec(ledger).unwrap(),
        REVIEW,
    )
    .unwrap()
}

fn reject(input: &Value, ledger: &Value) {
    let error = encode(
        &serde_json::to_vec(input).unwrap(),
        &serde_json::to_vec(ledger).unwrap(),
        REVIEW,
    )
    .unwrap_err();
    assert!(error.code.starts_with("registry_target_review_"));
    assert!(!format!("{error:?} {error}").contains("opaque-source-key"));
}

#[test]
fn typed_copy_and_exact_canonical_hashes_preserve_partial_review() {
    let ledger = ledger("123");
    let input = review(&ledger);
    let source = serde_json::to_vec(&input).unwrap();
    let (copy, descriptor_bytes) = encode_values(&input, &ledger);
    assert_eq!(
        encode_values(&input, &ledger),
        (copy.clone(), descriptor_bytes.clone())
    );
    let fields = fields(&copy);
    assert_eq!(
        fields[0],
        &[
            0x45, 0x67, 0x89, 0x01, 0x45, 0x67, 0x89, 0x01, 0x84, 0x56, 0x45, 0x67, 0x89, 0x01,
            0xab, 0xcd
        ]
    );
    assert_eq!(fields[1], b"review:v1");
    assert_eq!(fields[2], &1i32.to_be_bytes());
    assert_eq!(fields[3], b"accepted");
    assert_eq!(fields[4][0], 1);
    assert_eq!(fields[5], b"\x01[]");
    let document: Value = serde_json::from_slice(&fields[4][1..]).unwrap();
    assert_eq!(document.as_object().unwrap().len(), 8);
    assert_eq!(document["component"], "registry_required_target_review");
    assert_eq!(document["revision"], 1);
    assert_eq!(document["parser_version"], "registry-target-review-v1");
    assert_eq!(document["source_sha256"], sha(&source));
    assert_eq!(
        document["ledger_source_sha256"],
        ledger["ledger"]["source_sha256"]
    );
    assert_eq!(document["decisions"].as_array().unwrap().len(), 1);
    assert_eq!(
        document["decisions"][0]["evidence_reference"],
        "reviewer evidence é"
    );
    let projection = &document["decisions"][0]["source_binding"];
    assert_eq!(projection.as_object().unwrap().len(), 10);
    for key in [
        "evidence_id",
        "evidence_sha256",
        "operation",
        "expected_revision",
        "expected_network_id",
    ] {
        assert!(projection.get(key).is_none());
    }
    let mut ordinary = binding("ptg");
    ordinary.as_object_mut().unwrap().extend(
        json!({"network_id":42,"evidence_id":"neutral","evidence_sha256":"b".repeat(64),
      "operation":"bind","expected_revision":0,"expected_network_id":null})
        .as_object()
        .unwrap()
        .clone(),
    );
    let native =
        encode_network_source_binding_batch(&serde_json::to_vec(&json!([ordinary])).unwrap())
            .unwrap();
    assert_eq!(projection["binding_key"], native.rows[0].binding_key);
    assert_eq!(serde_json::to_vec(&document).unwrap(), &fields[4][1..]);
    let descriptor: Value = serde_json::from_slice(&descriptor_bytes).unwrap();
    assert_eq!(descriptor.as_object().unwrap().len(), 11);
    assert_eq!(descriptor["artifact_sha256"], sha(&fields[4][1..]));
    assert_eq!(descriptor["snapshot_id"], REVIEW);
    assert_eq!(descriptor["ledger_snapshot_id"], LEDGER);
    assert_eq!(
        descriptor["ledger_artifact_sha256"],
        input["ledger_artifact_sha256"]
    );
    assert_eq!(descriptor["source_sha256"], sha(&source));
    assert_eq!(descriptor["decision_count"], 1);
    assert_eq!(descriptor["resolved_count"], 1);
    assert_eq!(descriptor["physical_records"], 1);
}

#[test]
fn retained_jsonb_whitespace_is_canonicalized_before_hashing() {
    let ledger = ledger("123");
    let input = review(&ledger);
    let bytes = serde_json::to_vec_pretty(&ledger).unwrap();
    assert_eq!(
        encode(&serde_json::to_vec(&input).unwrap(), &bytes, REVIEW).unwrap(),
        encode_values(&input, &ledger)
    );
    let mut padded = bytes;
    padded.resize(17 * 1024 * 1024, b' ');
    assert!(encode(&serde_json::to_vec(&input).unwrap(), &padded, REVIEW).is_ok());
    padded.resize(MAX_LEDGER_INPUT_BYTES + 1, b' ');
    assert_eq!(
        encode(&serde_json::to_vec(&input).unwrap(), &padded, REVIEW)
            .unwrap_err()
            .code,
        "registry_target_review_limit"
    );
}

#[test]
fn all_source_scope_rules_are_reused_and_last_invalid_decision_is_atomic() {
    let ledger = ledger("123");
    for system in ["aca", "fhir", "ptg"] {
        let mut input = review(&ledger);
        input["decisions"][0]["source_binding"] = binding(system);
        assert!(encode_values(&input, &ledger).0.len() > 21);
        input["decisions"][0]["source_binding"]["source_scope_json"]["unexpected"] = json!(1);
        reject(&input, &ledger);
    }
    let mut input = review(&ledger);
    let mut last = decision(
        ledger["ledger"]["targets"][1]["target_key"]
            .as_str()
            .unwrap(),
        "aca",
    );
    last["source_binding"]["source_scope_json"]["plan_id"] = json!("54321CA0000001");
    input["decisions"].as_array_mut().unwrap().push(last);
    reject(&input, &ledger);
}

#[test]
fn same_binding_can_cover_multiple_targets_only_when_coordinates_and_network_agree() {
    let ledger = ledger("123");
    let mut input = review(&ledger);
    input["decisions"].as_array_mut().unwrap().push(decision(
        ledger["ledger"]["targets"][1]["target_key"]
            .as_str()
            .unwrap(),
        "ptg",
    ));
    assert_eq!(
        serde_json::from_slice::<Value>(&encode_values(&input, &ledger).1).unwrap()
            ["resolved_count"],
        2
    );
    for pointer in [
        "/decisions/1/network_id",
        "/decisions/1/source_binding/edition_id",
        "/decisions/1/source_binding/source_scope_json/company_key",
    ] {
        let mut changed = input.clone();
        *changed.pointer_mut(pointer).unwrap() = if pointer.ends_with("network_id") {
            json!(43)
        } else {
            json!("other")
        };
        reject(&changed, &ledger);
    }
}

#[test]
fn unresolved_conflicting_reviews_are_explicit_and_empty_review_refuses() {
    let ledger = ledger("123");
    for status in ["unresolved", "conflicting"] {
        let mut input = review(&ledger);
        input["decisions"][0]["resolution_status"] = json!(status);
        input["decisions"][0]["network_id"] = Value::Null;
        input["decisions"][0]["source_binding"] = Value::Null;
        let descriptor: Value = serde_json::from_slice(&encode_values(&input, &ledger).1).unwrap();
        assert_eq!(descriptor["resolved_count"], 0);
    }
    let mut input = review(&ledger);
    input["decisions"] = json!([]);
    reject(&input, &ledger);
}

#[test]
fn closed_types_null_boolean_unknown_target_and_duplicate_decisions_refuse() {
    let ledger = ledger("123");
    let base = review(&ledger);
    for (pointer, value) in [
        ("/decisions/0/target_key", json!("unknown")),
        ("/decisions/0/network_id", json!(true)),
        ("/decisions/0/network_id", json!(0)),
        ("/decisions/0/network_id", json!(2_147_483_648i64)),
        ("/decisions/0/source_binding", Value::Null),
        ("/decisions/0/resolution_status", json!("unresolved")),
        ("/decisions/0/reason", json!("\u{0}")),
        ("/decisions/0/evidence_sha256", json!("A".repeat(64))),
        ("/ledger_artifact_sha256", json!("a".repeat(64))),
    ] {
        let mut input = base.clone();
        *input.pointer_mut(pointer).unwrap() = value;
        reject(&input, &ledger);
    }
    for pointer in ["", "/decisions/0", "/decisions/0/source_binding"] {
        let mut input = base.clone();
        input
            .pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("unknown".into(), json!(1));
        reject(&input, &ledger);
    }
    for field in ["network_id", "source_binding"] {
        let mut input = base.clone();
        input["decisions"][0].as_object_mut().unwrap().remove(field);
        reject(&input, &ledger);
    }
    let mut input = base.clone();
    input["decisions"]
        .as_array_mut()
        .unwrap()
        .push(base["decisions"][0].clone());
    reject(&input, &ledger);
    let mut oversized = serde_json::to_vec(&base).unwrap();
    oversized.resize(MAX_INPUT_BYTES + 1, b' ');
    assert_eq!(
        encode(&oversized, &serde_json::to_vec(&ledger).unwrap(), REVIEW)
            .unwrap_err()
            .code,
        "registry_target_review_limit"
    );
}

#[test]
fn both_snapshot_ids_are_canonical_lowercase_nonzero_uuid() {
    let ledger = ledger("123");
    let input = review(&ledger);
    for id in [
        "00000000-0000-0000-0000-000000000000",
        "45678901-4567-8901-8456-45678901ABCD",
        "invalid",
    ] {
        assert!(encode(
            &serde_json::to_vec(&input).unwrap(),
            &serde_json::to_vec(&ledger).unwrap(),
            id
        )
        .is_err());
        let mut changed = input.clone();
        changed["ledger_snapshot_id"] = json!(id);
        reject(&changed, &ledger);
    }
}

#[test]
fn ledger_metadata_counts_relations_and_maximum_fc_target_are_checked() {
    let ledger = ledger(&"9".repeat(128));
    let input = review(&ledger);
    assert!(
        ledger["ledger"]["targets"][0]["target_key"]
            .as_str()
            .unwrap()
            .len()
            > 128
    );
    encode_values(&input, &ledger);
    for (pointer, value) in [
        ("/component", json!("other")),
        ("/revision", json!(true)),
        ("/parser_version", json!("old")),
        ("/ledger/source_sha256", json!("x")),
        ("/ledger/row_count", json!(3)),
        ("/ledger/observations/1/source_row_ordinal", json!(1)),
        ("/ledger/targets/0/source_row_ordinals", json!([2])),
        ("/ledger/targets/0/target_key", json!("x".repeat(193))),
    ] {
        let mut changed = ledger.clone();
        *changed.pointer_mut(pointer).unwrap() = value;
        reject(&input, &changed);
    }
}

fn bundle() -> Value {
    let ledger = ledger("123");
    let (copy, descriptor) = encode_values(&review(&ledger), &ledger);
    let document: Value = serde_json::from_slice(&fields(&copy)[4][1..]).unwrap();
    let descriptor: Value = serde_json::from_slice(&descriptor).unwrap();
    json!({
        "reviews":[{"snapshot_id":REVIEW,"artifact_sha256":descriptor["artifact_sha256"],"document":document}],
        "ledgers":[{"snapshot_id":LEDGER,"artifact_sha256":sha(&serde_json::to_vec(&ledger).unwrap()),"document":ledger}]
    })
}

fn rehash(artifact: &mut Value) {
    artifact["artifact_sha256"] = json!(sha(&serde_json::to_vec(&artifact["document"]).unwrap()));
}

fn reject_bundle(input: &Value) {
    let error = validate_bundle(&serde_json::to_vec(input).unwrap()).unwrap_err();
    assert!(error.code.starts_with("registry_target_review_"));
}

#[test]
fn bundle_rechecks_body_hashes_and_accepts_empty_and_shared_ledger_reviews() {
    assert_eq!(
        serde_json::from_slice::<Value>(
            &validate_bundle(br#"{"reviews":[],"ledgers":[]}"#).unwrap()
        )
        .unwrap(),
        json!({"component":"registry_required_target_review_validation","revision":1,"review_count":0,"ledger_count":0})
    );
    let mut input = bundle();
    let mut second = input["reviews"][0].clone();
    second["snapshot_id"] = json!("56789012-5678-9012-8567-56789012abcd");
    input["reviews"].as_array_mut().unwrap().push(second);
    let descriptor = validate_bundle(&serde_json::to_vec_pretty(&input).unwrap()).unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&descriptor).unwrap(),
        json!({"component":"registry_required_target_review_validation","revision":1,"review_count":2,"ledger_count":1})
    );
}

#[test]
fn bundle_hashes_drift_missing_extraneous_and_duplicate_records_refuse() {
    for pointer in ["/reviews/0/artifact_sha256", "/ledgers/0/artifact_sha256"] {
        let mut input = bundle();
        *input.pointer_mut(pointer).unwrap() = json!("a".repeat(64));
        reject_bundle(&input);
    }
    let mut input = bundle();
    input["reviews"][0]["document"]["decisions"][0]["reason"] = json!("Changed after retention");
    reject_bundle(&input);
    for section in ["reviews", "ledgers"] {
        let mut input = bundle();
        let first = input[section][0].clone();
        input[section].as_array_mut().unwrap().push(first);
        reject_bundle(&input);
    }
    let mut input = bundle();
    input["ledgers"] = json!([]);
    reject_bundle(&input);
    let mut input = bundle();
    let mut extra = input["ledgers"][0].clone();
    extra["snapshot_id"] = json!("56789012-5678-9012-8567-56789012abcd");
    input["ledgers"].as_array_mut().unwrap().push(extra);
    reject_bundle(&input);
    let mut input = bundle();
    input["reviews"] = json!([]);
    reject_bundle(&input);
}

#[test]
fn bundle_recomputes_review_binding_keys_and_validates_rehashed_shapes() {
    for (pointer, value) in [
        ("/component", json!("other")),
        ("/revision", json!(true)),
        ("/parser_version", json!("old")),
        ("/ledger_source_sha256", json!("a".repeat(64))),
        ("/decisions/0/target_key", json!("unknown")),
        ("/decisions/0/network_id", json!(true)),
        (
            "/decisions/0/source_binding/binding_key",
            json!("b".repeat(64)),
        ),
        (
            "/decisions/0/source_binding/source_scope_json/company_key",
            json!("different-company"),
        ),
        (
            "/decisions/0/source_binding/evidence_id",
            json!("not-allowed"),
        ),
    ] {
        let mut input = bundle();
        if let Some(slot) = input["reviews"][0]["document"].pointer_mut(pointer) {
            *slot = value;
        } else {
            input["reviews"][0]["document"]["decisions"][0]["source_binding"]["evidence_id"] =
                value;
        }
        rehash(&mut input["reviews"][0]);
        reject_bundle(&input);
    }
    let mut input = bundle();
    input["ledgers"][0]["document"]["ledger"]["source_sha256"] = json!("b".repeat(64));
    rehash(&mut input["ledgers"][0]);
    input["reviews"][0]["document"]["ledger_artifact_sha256"] =
        input["ledgers"][0]["artifact_sha256"].clone();
    rehash(&mut input["reviews"][0]);
    reject_bundle(&input);
}

#[test]
fn bundle_closed_schema_duplicate_fields_and_bounds_are_atomic() {
    for pointer in [
        "",
        "/reviews/0",
        "/reviews/0/document",
        "/reviews/0/document/decisions/0",
        "/ledgers/0",
        "/ledgers/0/document/ledger",
    ] {
        let mut input = bundle();
        input
            .pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("unknown".into(), json!(1));
        reject_bundle(&input);
    }
    for section in ["reviews", "ledgers"] {
        let mut input = bundle();
        input[section] = json!(vec![input[section][0].clone(); 5001]);
        reject_bundle(&input);
    }
    assert!(validate_bundle(br#"{"reviews":[],"reviews":[],"ledgers":[]}"#).is_err());
    let input = serde_json::to_string(&bundle()).unwrap();
    let duplicate=input.replace("\"binding_key\":", "\"binding_key\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"binding_key\":");
    assert!(validate_bundle(duplicate.as_bytes()).is_err());
    let mut padded = br#"{"reviews":[],"ledgers":[]}"#.to_vec();
    padded.resize(
        ptg2_scanner::registry_required_target_review::MAX_BUNDLE_BYTES,
        b' ',
    );
    assert!(validate_bundle(&padded).is_ok());
    padded.push(b' ');
    assert_eq!(
        validate_bundle(&padded).unwrap_err().code,
        "registry_target_review_limit"
    );
}

#[test]
fn maximum_decision_count_is_complete_and_one_extra_refuses() {
    let mut csv = csv::Writer::from_writer(Vec::new());
    csv.write_record(HEADERS).unwrap();
    for network in 1..=5000 {
        csv.write_record([
            "Example",
            "Company",
            "Plan",
            "PPO",
            "Carrier",
            "Network",
            &network.to_string(),
            "[]",
        ])
        .unwrap();
    }
    let source = csv.into_inner().unwrap();
    let (copy, _) = encode_registry_target_ledger_artifact(&source, LEDGER).unwrap();
    let ledger: Value = serde_json::from_slice(&fields(&copy)[4][1..]).unwrap();
    let mut input = review(&ledger);
    input["decisions"] = Value::Array(
        ledger["ledger"]["targets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| decision(row["target_key"].as_str().unwrap(), "ptg"))
            .collect(),
    );
    let (copy, descriptor) = encode_values(&input, &ledger);
    let descriptor: Value = serde_json::from_slice(&descriptor).unwrap();
    assert_eq!(descriptor["decision_count"], 5000);
    assert_eq!(descriptor["resolved_count"], 5000);
    let document: Value = serde_json::from_slice(&fields(&copy)[4][1..]).unwrap();
    assert_eq!(document["decisions"].as_array().unwrap().len(), 5000);
    let last = input["decisions"][4999].clone();
    input["decisions"].as_array_mut().unwrap().push(last);
    reject(&input, &ledger);
}

#[test]
fn duplicate_scope_and_ledger_json_fields_are_not_canonicalized_away() {
    let ledger = ledger("123");
    let input = serde_json::to_string(&review(&ledger)).unwrap();
    let duplicate = input.replace("\"cohort_id\":", "\"cohort_id\":\"other\",\"cohort_id\":");
    assert!(encode(
        duplicate.as_bytes(),
        &serde_json::to_vec(&ledger).unwrap(),
        REVIEW
    )
    .is_err());
    let duplicate = serde_json::to_string(&ledger)
        .unwrap()
        .replace("\"row_count\":", "\"row_count\":2,\"row_count\":");
    assert!(encode(input.as_bytes(), duplicate.as_bytes(), REVIEW).is_err());
}

fn standalone_ledger() -> Value {
    let document = ledger("123");
    json!({"snapshot_id":LEDGER,"artifact_sha256":sha(&serde_json::to_vec(&document).unwrap()),"document":document})
}

fn validate_ledger(input: &Value) -> Value {
    serde_json::from_slice(&validate_ledger_artifact(&serde_json::to_vec(input).unwrap()).unwrap())
        .unwrap()
}

fn reject_ledger(input: &Value) {
    let error = validate_ledger_artifact(&serde_json::to_vec(input).unwrap()).unwrap_err();
    assert!(error.code.starts_with("registry_target_review_"));
}

#[test]
fn standalone_retained_ledger_has_closed_descriptor_without_review() {
    let input = standalone_ledger();
    assert_eq!(
        validate_ledger(&input),
        json!({
            "component":"registry_required_target_ledger_validation","revision":1,
            "snapshot_id":LEDGER,"artifact_sha256":input["artifact_sha256"],
            "source_sha256":input["document"]["ledger"]["source_sha256"],"source_rows":2,"target_count":2
        })
    );
    let pretty = serde_json::to_vec_pretty(&input).unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&validate_ledger_artifact(&pretty).unwrap()).unwrap(),
        validate_ledger(&input)
    );
    // Existing review ancestry remains closed; the standalone API creates no review.
    reject_bundle(&json!({"reviews":[],"ledgers":[input]}));
}

#[test]
fn standalone_ledger_refuses_hash_drift_and_invalid_rehashed_body() {
    let base = standalone_ledger();
    let mut changed = base.clone();
    changed["document"]["ledger"]["observations"][0]["raw_cells"][0] = json!("Changed");
    reject_ledger(&changed);
    for (pointer, value) in [
        ("/component", json!("other")),
        ("/revision", json!(true)),
        ("/parser_version", json!("old")),
        ("/ledger/source_sha256", json!("x")),
        ("/ledger/row_count", json!(3)),
        ("/ledger/observations/1/source_row_ordinal", json!(1)),
        ("/ledger/targets/0/source_row_ordinals", json!([2])),
        ("/ledger/targets/0/target_key", json!("x".repeat(193))),
    ] {
        let mut changed = base.clone();
        *changed["document"].pointer_mut(pointer).unwrap() = value;
        rehash(&mut changed);
        reject_ledger(&changed);
    }
}

#[test]
fn standalone_ledger_requires_exact_fields_types_and_identity() {
    let base = standalone_ledger();
    for pointer in [
        "",
        "/document",
        "/document/ledger",
        "/document/ledger/targets/0",
        "/document/ledger/observations/0",
    ] {
        let mut changed = base.clone();
        changed
            .pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("extra".into(), json!(1));
        reject_ledger(&changed);
    }
    for field in ["snapshot_id", "artifact_sha256", "document"] {
        let mut changed = base.clone();
        changed.as_object_mut().unwrap().remove(field);
        reject_ledger(&changed);
        let mut changed = base.clone();
        changed[field] = Value::Null;
        reject_ledger(&changed);
    }
    for id in [
        "00000000-0000-0000-0000-000000000000",
        "34567890-3456-7890-8345-34567890ABCD",
        "invalid",
    ] {
        let mut changed = base.clone();
        changed["snapshot_id"] = json!(id);
        reject_ledger(&changed);
    }
    for hash in [json!("A".repeat(64)), json!(true), json!("a".repeat(63))] {
        let mut changed = base.clone();
        changed["artifact_sha256"] = hash;
        reject_ledger(&changed);
    }
    let raw = serde_json::to_string(&base).unwrap();
    let duplicate = raw.replacen(
        "\"snapshot_id\":",
        &format!("\"snapshot_id\":\"{LEDGER}\",\"snapshot_id\":"),
        1,
    );
    assert!(validate_ledger_artifact(duplicate.as_bytes()).is_err());
}

#[test]
fn standalone_ledger_preserves_jsonb_input_and_canonical_document_limits() {
    let input = standalone_ledger();
    let mut padded = serde_json::to_vec(&input).unwrap();
    padded.resize(MAX_LEDGER_INPUT_BYTES, b' ');
    assert!(validate_ledger_artifact(&padded).is_ok());
    padded.push(b' ');
    assert_eq!(
        validate_ledger_artifact(&padded).unwrap_err().code,
        "registry_target_review_limit"
    );
    let mut oversized = input.clone();
    let mut rows = Vec::new();
    let mut ordinals = [Vec::new(), Vec::new()];
    for index in 0..5000 {
        let mut row = input["document"]["ledger"]["observations"][index % 2].clone();
        row["source_row_ordinal"] = json!(index + 1);
        row["raw_cells"][0] = json!("x".repeat(3500));
        ordinals[index % 2].push(index + 1);
        rows.push(row);
    }
    oversized["document"]["ledger"]["row_count"] = json!(5000);
    oversized["document"]["ledger"]["observations"] = json!(rows);
    for (index, rows) in ordinals.iter().enumerate() {
        oversized["document"]["ledger"]["targets"][index]["source_row_ordinals"] = json!(rows);
    }
    rehash(&mut oversized);
    let bytes = serde_json::to_vec(&oversized).unwrap();
    assert!(bytes.len() < MAX_LEDGER_INPUT_BYTES);
    assert_eq!(
        validate_ledger_artifact(&bytes).unwrap_err().code,
        "registry_target_review_limit"
    );
}
