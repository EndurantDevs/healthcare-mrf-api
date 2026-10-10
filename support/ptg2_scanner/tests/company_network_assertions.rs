use ptg2_scanner::company_network_assertions::{
    validate_company_network_assertions, Applicability, BenefitDomain,
    CompanyNetworkAssertionError, RelationshipRole, ValidatedCompanyNetworkAssertions,
    MAX_EVIDENCE_CHARACTERS, MAX_INPUT_BYTES, MAX_ROWS,
};
use serde_json::{json, Value};

fn row() -> Value {
    json!({
        "company_id": "01234567-89ab-cdef-8123-456789abcdef", "network_id": 42,
        "relationship_role": "uses", "benefit_domain": null, "applicability": "national",
        "states": [], "valid_from": null, "valid_to": null,
        "evidence_text": "Reviewed explicit company-network relationship"
    })
}

fn validate(
    rows: &[Value],
) -> Result<ValidatedCompanyNetworkAssertions, CompanyNetworkAssertionError> {
    validate_company_network_assertions(&serde_json::to_vec(rows).unwrap())
}

fn period(start: Option<&str>, end: Option<&str>, evidence: &str) -> Value {
    let mut assertion = row();
    assertion["valid_from"] = json!(start);
    assertion["valid_to"] = json!(end);
    assertion["evidence_text"] = json!(evidence);
    assertion
}

#[test]
fn explicit_roles_unspecified_domain_and_evidence_survive_deterministic_sorting() {
    let mut rows = Vec::new();
    for role in ["publishes", "administers", "operates", "uses"] {
        let mut assertion = row();
        assertion["relationship_role"] = json!(role);
        assertion["evidence_text"] = json!("  Réviewed evidence — preserved exactly  ");
        rows.push(assertion);
    }
    let first = validate(&rows).unwrap();
    rows.reverse();
    let second = validate(&rows).unwrap();
    assert_eq!(first, second);
    assert_eq!(first.assertions.len(), 4);
    assert!(first.conflicts.is_empty());
    assert_eq!(
        first.assertions[0].relationship_role,
        RelationshipRole::Uses
    );
    for assertion in first.assertions {
        assert_eq!(assertion.benefit_domain, None);
        assert_eq!(
            assertion.evidence_text,
            "  Réviewed evidence — preserved exactly  "
        );
        assert_eq!(assertion.valid_from, None);
        assert_eq!(assertion.valid_to, None);
        assert_eq!(assertion.applicability, Applicability::National);
        assert!(assertion.states.is_empty());
    }
}

#[test]
fn positive_int4_identities_are_not_coerced_from_other_json_types() {
    for valid in [1, i32::MAX] {
        let mut assertion = row();
        assertion["network_id"] = json!(valid);
        assert_eq!(
            validate(&[assertion]).unwrap().assertions[0].network_id,
            valid
        );
    }
    for invalid in [
        json!(0),
        json!(-1),
        json!(2_147_483_648u64),
        json!(u64::MAX),
        json!(42.0),
        json!(true),
        json!(false),
        json!(null),
        json!("42"),
    ] {
        let mut assertion = row();
        assertion["network_id"] = invalid;
        let error = validate(&[row(), assertion]).unwrap_err();
        assert_eq!(error.row_index, Some(1));
    }
}

#[test]
fn companies_require_lowercase_nonzero_canonical_uuids() {
    for invalid in [
        "",
        "00000000-0000-0000-0000-000000000000",
        "0123456789abcdef8123456789abcdef",
        "01234567-89AB-CDEF-8123-456789ABCDEF",
        "{01234567-89ab-cdef-8123-456789abcdef}",
        "01234567-89ab-cdef-8123-456789abcdeg",
        " 01234567-89ab-cdef-8123-456789abcdef",
    ] {
        let mut assertion = row();
        assertion["company_id"] = json!(invalid);
        assert_eq!(
            validate(&[assertion]).unwrap_err().code,
            "invalid_company_id"
        );
    }
    for invalid in [json!(null), json!(42), json!(true)] {
        let mut assertion = row();
        assertion["company_id"] = invalid;
        assert!(validate(&[assertion]).is_err());
    }
}

#[test]
fn gregorian_dates_reject_noncanonical_and_impossible_calendar_values() {
    for field in ["valid_from", "valid_to"] {
        for valid in [
            "0001-01-01",
            "2000-02-29",
            "2024-02-29",
            "1900-02-28",
            "9999-12-31",
        ] {
            let mut assertion = row();
            assertion[field] = json!(valid);
            assert!(validate(&[assertion]).is_ok(), "{field}: {valid}");
        }
        for invalid in [
            "0000-01-01",
            "1900-02-29",
            "2100-02-29",
            "2023-02-29",
            "2024-04-31",
            "2024-00-01",
            "2024-13-01",
            "2024-01-00",
            "2024-01-32",
            "2024-2-01",
            "2024-02-1",
            "2024-02-29T00:00:00Z",
            "+2024-02-29",
            "２０２４-02-29",
            "2024/02/29",
        ] {
            let mut assertion = row();
            assertion[field] = json!(invalid);
            let error = validate(&[assertion]).unwrap_err();
            assert_eq!(error.code, "invalid_date", "{field}: {invalid}");
            assert_eq!(error.field, Some(field));
        }
        for invalid in [json!(20240229), json!(true), json!([])] {
            let mut assertion = row();
            assertion[field] = invalid;
            assert_eq!(validate(&[assertion]).unwrap_err().code, "invalid_date");
        }
    }
    assert_eq!(
        validate(&[period(Some("2024-02-29"), Some("2024-02-29"), "same day")])
            .unwrap_err()
            .code,
        "invalid_period"
    );
    assert_eq!(
        validate(&[period(Some("2024-03-01"), Some("2024-02-29"), "reversed")])
            .unwrap_err()
            .code,
        "invalid_period"
    );
}

#[test]
fn half_open_adjacent_periods_and_open_bounds_preserve_multiple_versions() {
    let rows = [
        period(Some("2025-01-01"), None, "later open period"),
        period(Some("2024-01-01"), Some("2025-01-01"), "bounded period"),
        period(None, Some("2024-01-01"), "earlier open period"),
    ];
    let output = validate(&rows).unwrap();
    assert_eq!(output.assertions.len(), 3);
    assert!(output.conflicts.is_empty());
    assert_eq!(output.assertions[0].valid_from, None);
    assert_eq!(
        output.assertions[1].valid_from.as_deref(),
        Some("2024-01-01")
    );
    assert_eq!(output.assertions[2].valid_to, None);
}

#[test]
fn overlapping_periods_remain_as_bounded_deterministic_review_components() {
    let rows = [
        period(Some("2030-01-01"), Some("2031-01-01"), "separate period"),
        period(Some("2024-02-01"), Some("2024-04-01"), "later evidence"),
        period(Some("2024-01-01"), Some("2024-03-01"), "earlier evidence"),
        period(Some("2024-03-01"), Some("2024-05-01"), "connected evidence"),
    ];
    let output = validate(&rows).unwrap();
    assert_eq!(output.assertions.len(), 4);
    assert_eq!(output.conflicts.len(), 1);
    assert_eq!(output.conflicts[0].code, "overlapping_periods");
    assert_eq!(output.conflicts[0].assertion_indices, [0, 1, 2]);
    let mut reversed = rows.to_vec();
    reversed.reverse();
    assert_eq!(
        serde_json::to_vec(&output).unwrap(),
        serde_json::to_vec(&validate(&reversed).unwrap()).unwrap()
    );
    let open = validate(&[
        period(None, None, "open evidence"),
        period(None, Some("2024-01-01"), "other evidence"),
    ])
    .unwrap();
    assert_eq!(open.conflicts[0].assertion_indices, [0, 1]);
    let same_period = validate(&[
        period(None, None, "first evidence"),
        period(None, None, "second evidence"),
    ])
    .unwrap();
    assert_eq!(same_period.assertions.len(), 2);
    assert_eq!(same_period.conflicts.len(), 1);
}

#[test]
fn nested_periods_keep_the_furthest_end_and_separate_disjoint_review_components() {
    let output = validate(&[
        period(Some("2024-01-01"), Some("2025-01-01"), "longer period"),
        period(Some("2024-02-01"), Some("2024-03-01"), "nested period"),
        period(Some("2024-12-01"), Some("2025-02-01"), "later overlap"),
        period(
            Some("2030-01-01"),
            Some("2031-01-01"),
            "separate longer period",
        ),
        period(
            Some("2030-02-01"),
            Some("2030-03-01"),
            "separate nested period",
        ),
    ])
    .unwrap();
    assert_eq!(output.conflicts.len(), 2);
    assert_eq!(output.conflicts[0].assertion_indices, [0, 1, 2]);
    assert_eq!(output.conflicts[1].assertion_indices, [3, 4]);
}

#[test]
fn conflict_scope_separates_roles_domains_companies_networks_and_exact_state_sets() {
    for (field, alternate) in [
        ("relationship_role", json!("operates")),
        ("benefit_domain", json!("medical")),
        ("company_id", json!("01234567-89ab-cdef-8123-456789abcde0")),
        ("network_id", json!(43)),
        ("applicability", json!("states")),
    ] {
        let mut different = row();
        different[field] = alternate;
        if field == "applicability" {
            different["states"] = json!(["CA"]);
        }
        let output = validate(&[row(), different]).unwrap();
        assert!(output.conflicts.is_empty(), "{field}");
    }
    let mut first = row();
    first["applicability"] = json!("states");
    first["states"] = json!(["CA"]);
    let mut second = first.clone();
    second["states"] = json!(["CA", "NY"]);
    assert!(validate(&[first, second]).unwrap().conflicts.is_empty());
    for (name, domain) in [
        ("medical", BenefitDomain::Medical),
        ("dental", BenefitDomain::Dental),
        ("vision", BenefitDomain::Vision),
    ] {
        let mut assertion = row();
        assertion["benefit_domain"] = json!(name);
        assert_eq!(
            validate(&[assertion]).unwrap().assertions[0].benefit_domain,
            Some(domain)
        );
    }
}

#[test]
fn national_and_explicit_state_jurisdictions_are_canonical_and_never_inferred() {
    let mut jurisdictions: Vec<_> = "AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY AS GU MP PR VI".split_ascii_whitespace().collect();
    jurisdictions.sort_unstable();
    assert_eq!(jurisdictions.len(), 56);
    let mut assertion = row();
    assertion["applicability"] = json!("states");
    assertion["states"] = json!(jurisdictions);
    assert_eq!(
        validate(&[assertion.clone()]).unwrap().assertions[0]
            .states
            .len(),
        56
    );
    for invalid in [
        json!([]),
        json!(["NY", "CA"]),
        json!(["CA", "CA"]),
        json!(["ca"]),
        json!(["ZZ"]),
        json!(["CA "]),
        json!(["US"]),
        json!(["AA"]),
        json!([true]),
        json!(null),
    ] {
        assertion["states"] = invalid;
        assert!(validate(&[assertion.clone()]).is_err());
    }
    assertion["applicability"] = json!("national");
    assertion["states"] = json!(["CA"]);
    assert_eq!(validate(&[assertion]).unwrap_err().code, "invalid_states");
}

#[test]
fn exact_fields_require_explicit_nulls_and_forbid_group_or_implicit_ownership_fields() {
    for field in [
        "company_id",
        "network_id",
        "relationship_role",
        "benefit_domain",
        "applicability",
        "states",
        "valid_from",
        "valid_to",
        "evidence_text",
    ] {
        let mut missing = row();
        missing.as_object_mut().unwrap().remove(field);
        assert!(validate(&[missing]).is_err(), "{field}");
    }
    for field in [
        "group_id",
        "group_ids",
        "owns",
        "retrieved_at",
        "source_date",
        "plan_option",
        "source_receipt_json",
    ] {
        let mut extra = row();
        extra[field] = json!("untrusted submitted value");
        let error = validate(&[extra]).unwrap_err();
        assert_eq!(error.code, "invalid_row");
        assert!(!serde_json::to_string(&error)
            .unwrap()
            .contains("untrusted submitted value"));
    }
    for (field, invalid) in [
        ("relationship_role", json!("owns")),
        ("benefit_domain", json!("unspecified")),
        ("benefit_domain", json!(true)),
        ("applicability", json!("global")),
    ] {
        let mut assertion = row();
        assertion[field] = invalid;
        assert!(validate(&[assertion]).is_err());
    }
    let duplicate_key = serde_json::to_string(&row()).unwrap().replacen(
        "\"network_id\":42",
        "\"network_id\":42,\"network_id\":43",
        1,
    );
    assert!(validate_company_network_assertions(format!("[{duplicate_key}]").as_bytes()).is_err());
    assert_eq!(
        validate(&[row(), row()]).unwrap_err().code,
        "duplicate_assertion"
    );
}

#[test]
fn evidence_is_nonblank_printable_and_bounded_by_characters() {
    for valid in [
        "é".repeat(MAX_EVIDENCE_CHARACTERS),
        "e".repeat(MAX_EVIDENCE_CHARACTERS),
    ] {
        let mut assertion = row();
        assertion["evidence_text"] = json!(valid.clone());
        assert_eq!(
            validate(&[assertion]).unwrap().assertions[0].evidence_text,
            valid
        );
    }
    for invalid in [
        String::new(),
        " \u{2003}".to_owned(),
        "line\nline".to_owned(),
        "a\0b".to_owned(),
        "a\tb".to_owned(),
        "a\u{7f}b".to_owned(),
        "é".repeat(MAX_EVIDENCE_CHARACTERS + 1),
    ] {
        let mut assertion = row();
        assertion["evidence_text"] = json!(invalid);
        assert_eq!(validate(&[assertion]).unwrap_err().code, "invalid_evidence");
    }
}

#[test]
fn batch_bounds_and_malformed_input_reject_without_partial_results() {
    for malformed in [
        b"{}".as_slice(),
        b"[",
        b"[null]",
        b"[[]]",
        b"[] []",
        &[b'[', 0xff, b']'],
    ] {
        assert!(validate_company_network_assertions(malformed).is_err());
    }
    let mut rows = Vec::new();
    for index in 0..MAX_ROWS {
        let mut assertion = row();
        assertion["evidence_text"] = json!(format!("evidence {index:04}"));
        rows.push(assertion);
    }
    let output = validate(&rows).unwrap();
    assert_eq!(output.assertions.len(), MAX_ROWS);
    assert_eq!(output.conflicts.len(), 1);
    assert_eq!(output.conflicts[0].assertion_indices.len(), MAX_ROWS);
    assert!(serde_json::to_vec(&output.conflicts).unwrap().len() < MAX_ROWS * 6);
    rows.push(row());
    assert_eq!(validate(&rows).unwrap_err().code, "row_limit");
    let mut exact_bytes = serde_json::to_vec(&[row()]).unwrap();
    exact_bytes.resize(MAX_INPUT_BYTES, b' ');
    assert_eq!(
        validate_company_network_assertions(&exact_bytes)
            .unwrap()
            .assertions
            .len(),
        1
    );
    exact_bytes.push(b' ');
    assert_eq!(
        validate_company_network_assertions(&exact_bytes)
            .unwrap_err()
            .code,
        "input_limit"
    );
    let cleared = validate_company_network_assertions(b"[]").unwrap();
    assert!(cleared.assertions.is_empty() && cleared.conflicts.is_empty());
}
