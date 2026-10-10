#[path = "../src/registry_network_coverage.rs"]
mod coverage;

use coverage::{RegistryNetworkCoverage, SourceStatus, MAX_INPUT_BYTES, MAX_ROWS};
use serde_json::{json, Value};

fn fixture() -> Value {
    json!({
        "targets": [
            {"target_key": "required:three"},
            {"target_key": "required:two"},
            {"target_key": "required:one"}
        ],
        "bindings": [
            {"target_key": "required:one", "network_id": 1, "resolution_status": "resolved"},
            {"target_key": "required:two", "network_id": 2, "resolution_status": "resolved"}
        ],
        "evidence": [evidence(2, 0, 2, "unavailable"), evidence(1, 3, 0, "current")]
    })
}

fn evidence(network: i32, membership: u64, pricing: u64, status: &str) -> Value {
    json!({
        "network_id": network,
        "company_link_verified": true,
        "exact_membership_count": membership,
        "pricing_evidence_count": pricing,
        "source_status": status,
        "unresolved_location_count": 0
    })
}

fn report(input: &Value) -> RegistryNetworkCoverage {
    coverage::build_registry_network_coverage(&serde_json::to_vec(input).unwrap()).unwrap()
}

fn reject(input: &Value, code: &str) {
    let error =
        coverage::build_registry_network_coverage(&serde_json::to_vec(input).unwrap()).unwrap_err();
    assert_eq!(error.code, code);
}

#[test]
fn full_ledger_has_independent_dimensions() {
    let result = report(&fixture());
    assert_eq!(result.targets.len(), 3);
    let one = &result.targets[0];
    assert_eq!(one.target_key, "required:one");
    assert_eq!(one.network_id, Some(1));
    assert_eq!(one.mapping_status, "resolved");
    assert!(one.company_link_verified);
    assert!(one.directory_available);
    assert_eq!(one.priceable, Some(false));
    assert_eq!(one.source_status, Some(SourceStatus::Current));
    assert_eq!(one.gaps, ["pricing_evidence_missing"]);
    let three = &result.targets[1];
    assert_eq!(three.target_key, "required:three");
    assert_eq!(three.network_id, None);
    assert_eq!(three.gaps, ["mapping_missing"]);
    let two = &result.targets[2];
    assert_eq!(two.target_key, "required:two");
    assert!(!two.directory_available);
    assert_eq!(two.priceable, Some(true));
    assert_eq!(
        two.gaps,
        ["directory_membership_missing", "source_unavailable"]
    );
    let totals = result.totals;
    assert_eq!(totals.ledger_targets, 3);
    assert_eq!(totals.mapped_targets, 2);
    assert_eq!(totals.company_link_verified, 2);
    assert_eq!(totals.directory_available, 1);
    assert_eq!(totals.priceable, 1);
    assert_eq!(totals.targets_with_gaps, 3);
    assert_eq!(totals.exact_membership_count, 3);
    assert_eq!(totals.pricing_evidence_count, Some(2));
    assert_eq!(totals.unresolved_location_count, 0);
    assert_eq!(totals.source_status_counts.current, 1);
    assert_eq!(totals.source_status_counts.unavailable, 1);
    assert_eq!(totals.source_status_counts.historical, 0);
    assert_eq!(totals.source_status_counts.unresolved, 0);
}

#[test]
fn shared_network_preserves_each_target() {
    let mut input = fixture();
    input["bindings"][1]["network_id"] = json!(1);
    input["evidence"] = json!([evidence(1, u64::MAX, 7, "historical")]);
    input["evidence"][0]["unresolved_location_count"] = json!(4);
    let result = report(&input);
    assert_eq!(result.totals.mapped_targets, 2);
    assert_eq!(result.totals.company_link_verified, 2);
    assert_eq!(result.totals.directory_available, 2);
    assert_eq!(result.totals.priceable, 2);
    assert_eq!(result.totals.exact_membership_count, u64::MAX);
    assert_eq!(result.totals.pricing_evidence_count, Some(7));
    assert_eq!(result.totals.unresolved_location_count, 4);
    assert_eq!(result.totals.source_status_counts.historical, 1);
    for index in [0, 2] {
        assert_eq!(result.targets[index].network_id, Some(1));
        assert_eq!(result.targets[index].unresolved_location_count, 4);
        assert_eq!(
            result.targets[index].gaps,
            ["source_historical", "location_unresolved"]
        );
    }
}

#[test]
fn retained_membership_survives_other_gaps() {
    let mut input = fixture();
    input["evidence"][1]["company_link_verified"] = json!(false);
    input["evidence"][1]["source_status"] = json!("unavailable");
    let result = report(&input);
    let target = &result.targets[0];
    assert!(!target.company_link_verified);
    assert!(target.directory_available);
    assert_eq!(target.priceable, Some(false));
    assert_eq!(
        target.gaps,
        [
            "company_link_unresolved",
            "pricing_evidence_missing",
            "source_unavailable"
        ]
    );
    assert_eq!(result.targets[2].priceable, Some(true));
    assert!(!result.targets[2].directory_available);
}

#[test]
fn evidence_gaps_have_stable_order() {
    let mut input = fixture();
    input["evidence"][1] = evidence(1, 0, 0, "unresolved");
    input["evidence"][1]["company_link_verified"] = json!(false);
    input["evidence"][1]["unresolved_location_count"] = json!(9);
    let result = report(&input);
    assert_eq!(
        result.targets[0].gaps,
        [
            "company_link_unresolved",
            "directory_membership_missing",
            "pricing_evidence_missing",
            "source_unresolved",
            "location_unresolved"
        ]
    );
    assert_eq!(result.totals.source_status_counts.unresolved, 1);
    assert_eq!(result.totals.unresolved_location_count, 9);
}

#[test]
fn missing_evidence_does_not_invent_facts() {
    let input = json!({
        "targets": [
            {"target_key": "a"}, {"target_key": "b"}, {"target_key": "c"}, {"target_key": "d"}
        ],
        "bindings": [
            {"target_key": "a", "network_id": null, "resolution_status": "unresolved"},
            {"target_key": "b", "network_id": null, "resolution_status": "conflicting"},
            {"target_key": "c", "network_id": 5, "resolution_status": "resolved"}
        ],
        "evidence": []
    });
    let result = report(&input);
    let gaps = [
        "mapping_unresolved",
        "mapping_conflicting",
        "evidence_missing",
        "mapping_missing",
    ];
    for (target, gap) in result.targets.iter().zip(gaps) {
        assert_eq!(target.gaps, [gap]);
        assert!(!target.company_link_verified);
        assert!(!target.directory_available);
        assert_eq!(target.priceable, None);
        assert_eq!(target.source_status, None);
        assert_eq!(target.unresolved_location_count, 0);
    }
    assert_eq!(result.totals.mapped_targets, 1);
    assert_eq!(result.totals.exact_membership_count, 0);
    assert_eq!(result.totals.source_status_counts.current, 0);
}

#[test]
fn exact_schema_rejects_missing_extra_fields() {
    for pointer in ["", "/targets/0", "/bindings/0", "/evidence/0"] {
        let mut input = fixture();
        input
            .pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("extra".into(), json!(1));
        reject(&input, "registry_coverage_invalid");
        let mut input = fixture();
        let object = input.pointer_mut(pointer).unwrap().as_object_mut().unwrap();
        let fields: Vec<_> = object.keys().cloned().collect();
        for field in fields {
            let mut missing = fixture();
            missing
                .pointer_mut(pointer)
                .unwrap()
                .as_object_mut()
                .unwrap()
                .remove(&field);
            reject(&missing, "registry_coverage_invalid");
        }
    }
}

#[test]
fn repeated_json_fields_are_rejected() {
    for input in [
        r#"{"targets":[],"targets":[],"bindings":[],"evidence":[]}"#,
        r#"{"targets":[{"target_key":"a","target_key":"b"}],"bindings":[],"evidence":[]}"#,
        r#"{"targets":[{"target_key":"a"}],"bindings":[{"target_key":"a","network_id":1,"network_id":1,"resolution_status":"resolved"}],"evidence":[]}"#,
        r#"{"targets":[{"target_key":"a"}],"bindings":[{"target_key":"a","network_id":1,"resolution_status":"resolved"}],"evidence":[{"network_id":1,"company_link_verified":true,"exact_membership_count":0,"pricing_evidence_count":0,"source_status":"current","source_status":"current","unresolved_location_count":0}]}"#,
    ] {
        assert_eq!(
            coverage::build_registry_network_coverage(input.as_bytes())
                .unwrap_err()
                .code,
            "registry_coverage_invalid"
        );
    }
}

#[test]
fn duplicate_or_unledgered_rows_are_rejected() {
    for section in ["targets", "bindings", "evidence"] {
        let mut input = fixture();
        let row = input[section][0].clone();
        input[section].as_array_mut().unwrap().push(row);
        reject(&input, "registry_coverage_invalid");
    }
    for (pointer, value) in [
        ("/bindings/0/target_key", json!("unknown")),
        ("/evidence/0/network_id", json!(9)),
        ("/bindings/0/network_id", json!(null)),
        ("/bindings/0/resolution_status", json!("missing")),
        ("/bindings/0/resolution_status", json!("unresolved")),
        ("/bindings/0/resolution_status", json!("conflicting")),
    ] {
        let mut input = fixture();
        *input.pointer_mut(pointer).unwrap() = value;
        reject(&input, "registry_coverage_invalid");
    }
}

#[test]
fn integer_and_boolean_types_are_strict() {
    for pointer in ["/bindings/0/network_id", "/evidence/0/network_id"] {
        for value in [
            json!(true),
            json!(0),
            json!(-1),
            json!(2_147_483_648_u64),
            json!(1.0),
            json!("1"),
        ] {
            let mut input = fixture();
            *input.pointer_mut(pointer).unwrap() = value;
            reject(&input, "registry_coverage_invalid");
        }
    }
    for field in [
        "exact_membership_count",
        "pricing_evidence_count",
        "unresolved_location_count",
    ] {
        for value in [json!(true), json!(-1), json!(1.0), json!("1"), json!(null)] {
            if field == "pricing_evidence_count" && value.is_null() {
                continue;
            }
            let mut input = fixture();
            input["evidence"][0][field] = value;
            reject(&input, "registry_coverage_invalid");
        }
    }
    let mut input = fixture();
    input["evidence"][0]["company_link_verified"] = json!(1);
    reject(&input, "registry_coverage_invalid");
    input["evidence"][0]["company_link_verified"] = json!(true);
    input["evidence"][0]["source_status"] = json!("unknown");
    reject(&input, "registry_coverage_invalid");
}

#[test]
fn invalid_last_row_rejects_entire_batch() {
    let mut input = fixture();
    input["evidence"][1]["company_link_verified"] = json!("false");
    reject(&input, "registry_coverage_invalid");
    input = fixture();
    input["targets"][2]["target_key"] = json!("");
    reject(&input, "registry_coverage_invalid");
    input = fixture();
    input["bindings"][1]["network_id"] = json!(-2);
    reject(&input, "registry_coverage_invalid");
}

#[test]
fn checked_evidence_sums_reject_overflow() {
    for field in [
        "exact_membership_count",
        "pricing_evidence_count",
        "unresolved_location_count",
    ] {
        let mut input = fixture();
        input["evidence"][0][field] = json!(u64::MAX);
        input["evidence"][1][field] = json!(1);
        reject(&input, "registry_coverage_overflow");
    }
}

#[test]
fn target_keys_are_bounded_without_normalization() {
    for key in ["".to_owned(), "é".repeat(97)] {
        let mut input = fixture();
        input["targets"][0]["target_key"] = json!(key);
        reject(&input, "registry_coverage_invalid");
    }
    let key = "é".repeat(96);
    let input = json!({"targets": [{"target_key": key}], "bindings": [], "evidence": []});
    assert_eq!(report(&input).targets[0].target_key, key);
    let input = json!({
        "targets": [{"target_key": "a"}, {"target_key": " a"}, {"target_key": "a "},
            {"target_key": "é"}, {"target_key": "e\u{301}"}],
        "bindings": [], "evidence": []
    });
    let keys: Vec<_> = report(&input)
        .targets
        .into_iter()
        .map(|row| row.target_key)
        .collect();
    assert_eq!(keys, [" a", "a", "a ", "e\u{301}", "é"]);
}

#[test]
fn integer_extremes_preserve_native_bounds() {
    let mut input = fixture();
    input["bindings"][0]["network_id"] = json!(i32::MAX);
    input["evidence"][1]["network_id"] = json!(i32::MAX);
    assert_eq!(report(&input).targets[0].network_id, Some(i32::MAX));
    for field in [
        "exact_membership_count",
        "pricing_evidence_count",
        "unresolved_location_count",
    ] {
        let raw = r#"{"targets":[{"target_key":"a"}],"bindings":[{"target_key":"a","network_id":1,"resolution_status":"resolved"}],"evidence":[{"network_id":1,"company_link_verified":true,"exact_membership_count":0,"pricing_evidence_count":0,"source_status":"current","unresolved_location_count":0}]}"#;
        let raw = raw.replace(
            &format!("\"{field}\":0"),
            &format!("\"{field}\":18446744073709551616"),
        );
        assert_eq!(
            coverage::build_registry_network_coverage(raw.as_bytes())
                .unwrap_err()
                .code,
            "registry_coverage_invalid"
        );
    }
}

#[test]
fn row_limits_cover_each_array() {
    for section in ["targets", "bindings", "evidence"] {
        let mut input = fixture();
        let row = input[section][0].clone();
        input[section] = json!(vec![row; MAX_ROWS + 1]);
        reject(&input, "registry_coverage_limit");
    }
    let mut input = json!({"targets": [], "bindings": [], "evidence": []});
    for network in 1..=MAX_ROWS as i32 {
        let key = format!("target:{network:04}");
        input["targets"]
            .as_array_mut()
            .unwrap()
            .push(json!({"target_key": key}));
        input["bindings"].as_array_mut().unwrap().push(json!({
            "target_key": key, "network_id": network, "resolution_status": "resolved"
        }));
        input["evidence"]
            .as_array_mut()
            .unwrap()
            .push(evidence(network, 1, 1, "current"));
    }
    let result = report(&input);
    assert_eq!(result.targets.len(), MAX_ROWS);
    assert_eq!(result.totals.ledger_targets, MAX_ROWS as u64);
    assert_eq!(result.totals.directory_available, MAX_ROWS as u64);
    assert_eq!(result.totals.exact_membership_count, MAX_ROWS as u64);
    assert_eq!(result.totals.source_status_counts.current, MAX_ROWS as u64);
    assert_eq!(result.totals.targets_with_gaps, 0);
}

#[test]
fn input_size_boundary_is_exact() {
    let mut input = br#"{"targets":[],"bindings":[],"evidence":[]}"#.to_vec();
    input.resize(MAX_INPUT_BYTES, b' ');
    let result = coverage::build_registry_network_coverage(&input).unwrap();
    assert!(result.targets.is_empty());
    assert_eq!(result.totals.ledger_targets, 0);
    input.push(b' ');
    assert_eq!(
        coverage::build_registry_network_coverage(&input)
            .unwrap_err()
            .code,
        "registry_coverage_limit"
    );
}

#[test]
fn malformed_json_errors_are_sanitized() {
    for input in [
        "[]",
        "null",
        "{}",
        "{",
        r#"{"targets":{},"bindings":[],"evidence":[]}"#,
        r#"{"targets":[],"bindings":[],"evidence":[]} {}"#,
        r#"{"targets":[{"target_key":"synthetic-marker","private_field":1}],"bindings":[],"evidence":[]}"#,
    ] {
        let error = coverage::build_registry_network_coverage(input.as_bytes()).unwrap_err();
        assert_eq!(error.code, "registry_coverage_invalid");
        assert!(!format!("{error:?} {error}").contains("synthetic-marker"));
        assert_eq!(
            serde_json::to_value(error).unwrap(),
            json!({"code": "registry_coverage_invalid"})
        );
    }
}

#[test]
fn maximum_ledger_fc_ribbon_target_fits_shared_utf8_bound() {
    let key = format!(
        "fc:{}:ribbon:12345678-1234-5678-8123-123456789abc",
        "9".repeat(128)
    );
    assert!(key.len() > 128 && key.len() <= coverage::MAX_TARGET_KEY_BYTES);
    let mut input = json!({"targets":[{"target_key":key}],"bindings":[],"evidence":[]});
    assert_eq!(report(&input).totals.ledger_targets, 1);
    input["targets"][0]["target_key"] = json!("x".repeat(coverage::MAX_TARGET_KEY_BYTES + 1));
    reject(&input, "registry_coverage_invalid");
}

#[test]
fn nullable_pricing_is_required_and_independent_of_directory() {
    let mut input = fixture();
    input["evidence"][1]["pricing_evidence_count"] = Value::Null;
    let result = report(&input);
    assert!(result.targets[0].directory_available);
    assert_eq!(result.targets[0].priceable, None);
    assert_eq!(result.targets[0].gaps, ["pricing_not_assessed"]);
    assert_eq!(result.targets[2].priceable, Some(true));
    assert_eq!(result.totals.priceable, 1);
    assert_eq!(result.totals.pricing_evidence_count, None);
    let wire = serde_json::to_value(result).unwrap();
    assert!(wire["targets"][0]["priceable"].is_null());
    assert_eq!(wire["targets"][2]["priceable"], json!(true));
    assert!(wire["totals"]["pricing_evidence_count"].is_null());
    input["evidence"][1]
        .as_object_mut()
        .unwrap()
        .remove("pricing_evidence_count");
    reject(&input, "registry_coverage_invalid");
}

#[test]
fn assessed_zero_and_empty_evidence_have_different_totals() {
    let mut input = fixture();
    input["evidence"][0]["pricing_evidence_count"] = json!(0);
    let result = report(&input);
    assert_eq!(result.totals.pricing_evidence_count, Some(0));
    assert_eq!(result.totals.priceable, 0);
    assert_eq!(result.targets[0].priceable, Some(false));
    assert_eq!(result.targets[2].priceable, Some(false));
    assert_eq!(result.targets[1].priceable, None);
    let wire = serde_json::to_value(result).unwrap();
    assert_eq!(wire["targets"][0]["priceable"], json!(false));
    assert_eq!(wire["totals"]["pricing_evidence_count"], json!(0));
    input["evidence"] = json!([]);
    let result = report(&input);
    assert_eq!(result.totals.pricing_evidence_count, None);
    assert!(result
        .targets
        .iter()
        .all(|target| target.priceable.is_none()));
    assert_eq!(result.targets[0].gaps, ["evidence_missing"]);
}

#[test]
fn mixed_pricing_and_shared_network_preserve_exact_once_evidence() {
    let mut input = fixture();
    input["bindings"].as_array_mut().unwrap().push(json!({
        "target_key":"required:three","network_id":1,"resolution_status":"resolved"
    }));
    input["evidence"][0]["pricing_evidence_count"] = Value::Null;
    input["evidence"][1]["pricing_evidence_count"] = json!(u64::MAX);
    let result = report(&input);
    assert_eq!(result.totals.priceable, 2);
    assert_eq!(result.totals.pricing_evidence_count, None);
    assert_eq!(result.totals.exact_membership_count, 3);
    assert_eq!(result.totals.source_status_counts.current, 1);
    assert_eq!(result.totals.source_status_counts.unavailable, 1);
    for index in [0, 1] {
        assert_eq!(result.targets[index].priceable, Some(true));
    }
    assert_eq!(result.targets[2].priceable, None);
    assert_eq!(
        result.targets[2].gaps,
        [
            "directory_membership_missing",
            "pricing_not_assessed",
            "source_unavailable"
        ]
    );
}

#[test]
fn unassessed_pricing_never_hides_overflow_of_known_counts() {
    let mut input = fixture();
    input["bindings"].as_array_mut().unwrap().push(json!({
        "target_key":"required:three","network_id":3,"resolution_status":"resolved"
    }));
    input["evidence"] = json!([
        evidence(1, 0, u64::MAX, "current"),
        evidence(2, 0, 0, "current"),
        evidence(3, 0, 1, "current")
    ]);
    input["evidence"][1]["pricing_evidence_count"] = Value::Null;
    reject(&input, "registry_coverage_overflow");
}
