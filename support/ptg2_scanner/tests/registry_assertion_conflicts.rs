//! Independent review contract; the strict persistence validators remain unchanged.

use ptg2_scanner::{
    company_group_assertions, company_network_assertions, registry_assertion_conflicts,
};
use registry_assertion_conflicts::{
    classify_registry_assertions, AssertionReviewError, Disposition, ReviewBatch, ReviewPolicy,
    MAX_INPUT_BYTES, MAX_OUTPUT_BYTES, MAX_ROWS,
};
use serde_json::{json, Value};

fn identity(number: usize) -> String {
    format!("{number:08x}-0000-4000-8000-000000000001")
}

fn assertion(record: usize, object: usize, start: Value, end: Value) -> Value {
    json!({
        "subject_id": identity(100), "predicate": "reported_affiliation",
        "scope": {"system":"group_kind", "value":"corporate_parent"},
        "object_identifier": {"system":"group_uuid", "value":identity(object)},
        "valid_from":start, "valid_to":end,
        "evidence": {
            "source_edition": {"source_system":"synthetic", "source_id":"example-source",
                "edition_id":"example-edition", "snapshot_id":identity(200)},
            "record_key":format!("record-{record}"),
            "original_identifier":{"system":"external_reference", "value":"source-17"},
            "document_sha256":"a".repeat(64)
        },
        "source_label":"Shared source label"
    })
}

fn dated(record: usize, object: usize, start: &str, end: Option<&str>) -> Value {
    assertion(record, object, json!(start), json!(end))
}

fn review(rows: &[Value], policy: ReviewPolicy) -> ReviewBatch {
    let result = classify_registry_assertions(&serde_json::to_vec(rows).unwrap(), policy).unwrap();
    assert_eq!(result.assertions.len(), rows.len());
    assert_eq!(
        result.counts.input_rows,
        result.counts.accepted_rows
            + result.counts.duplicate_rows
            + result.counts.review_required_rows
    );
    assert!(serde_json::to_vec(&result).unwrap().len() <= MAX_OUTPUT_BYTES);
    result
}

fn reject(rows: &[Value], policy: ReviewPolicy) -> AssertionReviewError {
    classify_registry_assertions(&serde_json::to_vec(rows).unwrap(), policy).unwrap_err()
}

#[test]
fn corroborating_sources_keep_complete_original_assertions_without_a_winner() {
    let first = dated(1, 10, "2024-01-01", None);
    let mut second = first.clone();
    second["evidence"]["source_edition"]["source_id"] = json!("independent-source");
    second["evidence"]["source_edition"]["snapshot_id"] = json!(identity(201));
    second["evidence"]["document_sha256"] = json!("b".repeat(64));
    for policy in [ReviewPolicy::GroupInclusive, ReviewPolicy::NetworkHalfOpen] {
        let result = review(&[first.clone(), second.clone()], policy);
        assert_eq!(result.counts.accepted_rows, 2);
        assert_eq!(result.counts.duplicate_rows, 0);
        assert!(result.reviews.is_empty());
        let retained: Vec<Value> = result
            .assertions
            .iter()
            .map(|row| serde_json::from_str(row.assertion.get()).unwrap())
            .collect();
        assert!(retained.contains(&first) && retained.contains(&second));
    }
}

#[test]
fn duplicates_are_classified_once_each_and_original_rows_are_not_removed() {
    let row = dated(1, 10, "2024-01-01", None);
    let result = review(
        &[row.clone(), row.clone(), row.clone()],
        ReviewPolicy::GroupInclusive,
    );
    assert_eq!(result.counts.accepted_rows, 1);
    assert_eq!(result.counts.duplicate_rows, 2);
    assert_eq!(result.counts.review_required_rows, 0);
    assert_eq!(result.assertions[0].disposition, Disposition::Accepted);
    for duplicate in &result.assertions[1..] {
        assert_eq!(duplicate.disposition, Disposition::Duplicate);
        assert_eq!(duplicate.duplicate_of, Some(0));
        assert_eq!(
            serde_json::from_str::<Value>(duplicate.assertion.get()).unwrap(),
            row
        );
    }
}

#[test]
fn canonical_outputs_and_review_indices_are_stable_under_reordering() {
    let first = dated(1, 10, "2024-01-01", None);
    let second = dated(2, 11, "2024-02-01", None);
    let unresolved = assertion(3, 12, Value::Null, Value::Null);
    let mut separate = dated(4, 13, "2024-01-01", None);
    separate["subject_id"] = json!(identity(101));
    let rows = vec![first.clone(), first, second, unresolved, separate];
    let expected = serde_json::to_vec(&review(&rows, ReviewPolicy::GroupInclusive)).unwrap();
    let mut reversed = rows.clone();
    reversed.reverse();
    assert_eq!(
        expected,
        serde_json::to_vec(&review(&reversed, ReviewPolicy::GroupInclusive)).unwrap()
    );
    assert_eq!(
        expected,
        serde_json::to_vec(&review(&rows, ReviewPolicy::GroupInclusive)).unwrap()
    );
    let result = review(&rows, ReviewPolicy::GroupInclusive);
    assert_eq!(result.counts.accepted_rows, 1);
    assert_eq!(result.counts.review_required_rows, 4);
    assert_eq!(result.counts.duplicate_rows, 0);
    assert!(result
        .assertions
        .iter()
        .any(|row| row.disposition == Disposition::ReviewRequired && row.duplicate_of.is_some()));
}

#[test]
fn exact_subject_predicate_and_scoped_identity_keep_identical_labels_separate() {
    let first = dated(1, 10, "2024-01-01", None);
    let mut other_subject = dated(2, 11, "2024-01-01", None);
    other_subject["subject_id"] = json!(identity(101));
    let mut other_predicate = dated(3, 12, "2024-01-01", None);
    other_predicate["predicate"] = json!("verified_ownership");
    let mut other_scope_system = dated(4, 13, "2024-01-01", None);
    other_scope_system["scope"]["system"] = json!("other_group_kind");
    let mut other_scope_value = dated(5, 14, "2024-01-01", None);
    other_scope_value["scope"]["value"] = json!("naic_group");
    let result = review(
        &[
            first,
            other_subject,
            other_predicate,
            other_scope_system,
            other_scope_value,
        ],
        ReviewPolicy::GroupInclusive,
    );
    assert_eq!(result.counts.accepted_rows, 5);
    assert!(result.reviews.is_empty());
}

#[test]
fn inclusive_group_boundary_conflicts_but_half_open_network_boundary_does_not() {
    let rows = [
        dated(1, 10, "2024-01-01", Some("2024-02-01")),
        dated(2, 11, "2024-02-01", Some("2024-03-01")),
    ];
    let group = review(&rows, ReviewPolicy::GroupInclusive);
    assert_eq!(group.counts.review_required_rows, 2);
    assert_eq!(group.reviews[0].code, "contradictory_overlapping_objects");
    let mut same_network = rows[1].clone();
    same_network["object_identifier"] = rows[0]["object_identifier"].clone();
    let network = review(
        &[rows[0].clone(), same_network],
        ReviewPolicy::NetworkHalfOpen,
    );
    assert_eq!(network.counts.accepted_rows, 2);
    assert!(network.reviews.is_empty());
}

#[test]
fn distinct_networks_are_not_contradictions_and_distinct_overlapping_intervals_are_reviewed() {
    let first = dated(1, 10, "2024-01-01", None);
    let distinct = dated(2, 11, "2024-02-01", None);
    assert!(
        review(&[first.clone(), distinct], ReviewPolicy::NetworkHalfOpen)
            .reviews
            .is_empty()
    );
    let overlapping = dated(3, 10, "2024-02-01", Some("2024-03-01"));
    let result = review(&[first, overlapping], ReviewPolicy::NetworkHalfOpen);
    assert_eq!(result.counts.review_required_rows, 2);
    assert_eq!(result.reviews[0].code, "overlapping_periods");
}

#[test]
fn unresolved_object_and_start_stay_raw_and_review_precedes_duplicate() {
    let mut unresolved = assertion(1, 10, Value::Null, json!("2024-12-31"));
    unresolved["object_identifier"] = Value::Null;
    let result = review(
        &[unresolved.clone(), unresolved.clone()],
        ReviewPolicy::GroupInclusive,
    );
    assert_eq!(result.counts.review_required_rows, 2);
    assert_eq!(result.counts.duplicate_rows, 0);
    assert_eq!(
        result
            .reviews
            .iter()
            .map(|review| review.code)
            .collect::<Vec<_>>(),
        ["missing_effective_start", "missing_object"]
    );
    assert!(result.assertions[1].duplicate_of.is_some());
    for row in result.assertions {
        assert_eq!(
            serde_json::from_str::<Value>(row.assertion.get()).unwrap(),
            unresolved
        );
    }
}

#[test]
fn conflicting_digest_retains_both_claims_and_complete_evidence_identity_is_exact() {
    let first = dated(1, 10, "2024-01-01", None);
    let mut changed_digest = first.clone();
    changed_digest["evidence"]["document_sha256"] = json!("b".repeat(64));
    let result = review(
        &[first.clone(), changed_digest.clone()],
        ReviewPolicy::GroupInclusive,
    );
    assert_eq!(result.counts.review_required_rows, 2);
    assert_eq!(result.reviews[0].code, "evidence_digest_conflict");
    for field in ["source_system", "source_id", "edition_id", "snapshot_id"] {
        let mut independent = changed_digest.clone();
        independent["evidence"]["source_edition"][field] = if field == "snapshot_id" {
            json!(identity(201))
        } else {
            json!("independent")
        };
        assert!(
            review(&[first.clone(), independent], ReviewPolicy::GroupInclusive)
                .reviews
                .is_empty()
        );
    }
    let mut another_record = changed_digest.clone();
    another_record["evidence"]["record_key"] = json!("another-record");
    assert!(review(
        &[first.clone(), another_record],
        ReviewPolicy::GroupInclusive
    )
    .reviews
    .is_empty());
    for field in ["system", "value"] {
        let mut another_identifier = changed_digest.clone();
        another_identifier["evidence"]["original_identifier"][field] = json!("independent");
        assert!(review(
            &[first.clone(), another_identifier],
            ReviewPolicy::GroupInclusive
        )
        .reviews
        .is_empty());
    }
}

#[test]
fn raw_json_keeps_original_format_and_source_label_never_determines_identity() {
    let row = dated(1, 10, "2024-01-01", None);
    let raw = serde_json::to_string_pretty(&row).unwrap();
    let result =
        classify_registry_assertions(format!("[{raw}]").as_bytes(), ReviewPolicy::GroupInclusive)
            .unwrap();
    assert_eq!(result.assertions[0].assertion.get(), raw);
    let mut relabeled = row.clone();
    relabeled["source_label"] = json!("Different retained label");
    let result = review(&[row, relabeled], ReviewPolicy::GroupInclusive);
    assert_eq!(result.counts.accepted_rows, 1);
    assert_eq!(result.counts.duplicate_rows, 1);
    assert!(result.reviews.is_empty());
}

#[test]
fn strict_protocol_refuses_unknown_missing_and_duplicate_fields_atomically() {
    let row = dated(1, 10, "2024-01-01", None);
    for field in [
        "subject_id",
        "predicate",
        "scope",
        "object_identifier",
        "valid_from",
        "valid_to",
        "evidence",
    ] {
        let mut malformed = row.clone();
        malformed.as_object_mut().unwrap().remove(field);
        assert_eq!(
            reject(&[row.clone(), malformed], ReviewPolicy::GroupInclusive).code,
            "invalid_row"
        );
    }
    for field in [
        "source_edition",
        "record_key",
        "original_identifier",
        "document_sha256",
    ] {
        let mut malformed = row.clone();
        malformed["evidence"].as_object_mut().unwrap().remove(field);
        assert_eq!(
            reject(&[malformed], ReviewPolicy::GroupInclusive).code,
            "invalid_row"
        );
    }
    let mut malformed = row.clone();
    malformed["inferred_owner"] = json!(true);
    assert_eq!(
        reject(&[malformed], ReviewPolicy::GroupInclusive).code,
        "invalid_row"
    );
    let raw = serde_json::to_string(&row).unwrap();
    let duplicate = raw.replacen('{', "{\"predicate\":\"uses\",", 1);
    assert_eq!(
        classify_registry_assertions(
            format!("[{duplicate}]").as_bytes(),
            ReviewPolicy::GroupInclusive
        )
        .unwrap_err()
        .code,
        "invalid_row"
    );
    for raw in [
        b"null".as_slice(),
        b"{}",
        b"[null]",
        b"[] []",
        b"[",
        b"[{}]",
    ] {
        assert!(classify_registry_assertions(raw, ReviewPolicy::GroupInclusive).is_err());
    }
    let mut no_label = row;
    no_label.as_object_mut().unwrap().remove("source_label");
    assert_eq!(
        review(&[no_label], ReviewPolicy::GroupInclusive)
            .counts
            .accepted_rows,
        1
    );
}

#[test]
fn malformed_identifiers_digests_and_nullable_fields_refuse_the_batch() {
    let row = dated(1, 10, "2024-01-01", None);
    for value in [
        "not-a-uuid",
        "00000000-0000-0000-0000-000000000000",
        "0000000A-0000-4000-8000-000000000001",
    ] {
        let mut malformed = row.clone();
        malformed["subject_id"] = json!(value);
        assert_eq!(
            reject(&[malformed], ReviewPolicy::GroupInclusive).code,
            "invalid_uuid"
        );
    }
    for value in [
        "a".repeat(63),
        "A".repeat(64),
        "g".repeat(64),
        format!("sha256:{}", "a".repeat(64)),
    ] {
        let mut malformed = row.clone();
        malformed["evidence"]["document_sha256"] = json!(value);
        assert_eq!(
            reject(&[malformed], ReviewPolicy::GroupInclusive).code,
            "invalid_evidence"
        );
    }
    for field in ["object_identifier", "valid_from", "valid_to"] {
        let mut malformed = row.clone();
        malformed[field] = json!(17);
        assert_eq!(
            reject(&[malformed], ReviewPolicy::GroupInclusive).code,
            "invalid_field"
        );
    }
    for value in ["", " leading", "line\nbreak", "1invalid", "uses-network"] {
        let mut malformed = row.clone();
        malformed["predicate"] = json!(value);
        assert_eq!(
            reject(&[malformed], ReviewPolicy::GroupInclusive).code,
            "invalid_predicate"
        );
    }
}

fn existing_group_date(date: &str) -> bool {
    let row = json!({"assertion_id":identity(1), "company_id":identity(100), "group_id":identity(10),
        "group_kind":"corporate_parent", "relationship_kind":"reported_affiliation",
        "valid_from":date, "valid_to":null,
        "provenance":{"kind":"manual_reference", "evidence_ref":"Synthetic reference",
            "snapshot_id":null, "source_record_key":null}});
    company_group_assertions::reduce_company_group_assertions(&serde_json::to_vec(&[row]).unwrap())
        .is_ok()
}

fn existing_network_date(date: &str) -> bool {
    let row = json!({"company_id":identity(100), "network_id":42, "relationship_role":"uses",
        "benefit_domain":null, "applicability":"national", "states":[],
        "valid_from":date, "valid_to":null, "evidence_text":"Synthetic reference"});
    company_network_assertions::validate_company_network_assertions(
        &serde_json::to_vec(&[row]).unwrap(),
    )
    .is_ok()
}

#[test]
fn gregorian_validation_matches_existing_group_and_network_edge_dates() {
    for date in [
        "0001-01-01",
        "2000-02-29",
        "2024-02-29",
        "9999-12-31",
        "0000-01-01",
        "1900-02-29",
        "2023-02-29",
        "2024-13-01",
        "2024-01-00",
        "2024-1-01",
        "２０２４-01-01",
    ] {
        let row = dated(1, 10, date, None);
        let valid = classify_registry_assertions(
            &serde_json::to_vec(&[row]).unwrap(),
            ReviewPolicy::GroupInclusive,
        )
        .is_ok();
        assert_eq!(valid, existing_group_date(date), "{date}");
        assert_eq!(valid, existing_network_date(date), "{date}");
    }
    let one_day = dated(1, 10, "2024-02-29", Some("2024-02-29"));
    assert_eq!(
        review(std::slice::from_ref(&one_day), ReviewPolicy::GroupInclusive)
            .counts
            .accepted_rows,
        1
    );
    assert_eq!(
        reject(&[one_day], ReviewPolicy::NetworkHalfOpen).code,
        "invalid_period"
    );
    let reversed = dated(1, 10, "2024-03-01", Some("2024-02-29"));
    assert_eq!(
        reject(&[reversed], ReviewPolicy::GroupInclusive).code,
        "invalid_period"
    );
}

#[test]
fn row_and_byte_bounds_refuse_without_partial_classifications() {
    let row = dated(1, 10, "2024-01-01", None);
    let rows = vec![row.clone(); MAX_ROWS];
    let result = review(&rows, ReviewPolicy::GroupInclusive);
    assert_eq!(result.counts.accepted_rows, 1);
    assert_eq!(result.counts.duplicate_rows, MAX_ROWS - 1);
    let error = reject(&vec![row; MAX_ROWS + 1], ReviewPolicy::GroupInclusive);
    assert_eq!((error.code, error.row_index), ("row_limit", Some(MAX_ROWS)));
    assert_eq!(
        classify_registry_assertions(
            &vec![b' '; MAX_INPUT_BYTES + 1],
            ReviewPolicy::GroupInclusive
        )
        .unwrap_err()
        .code,
        "input_limit"
    );
    let empty = review(&[], ReviewPolicy::GroupInclusive);
    assert_eq!(empty.counts.input_rows, 0);
    assert!(empty.reviews.is_empty());
}

#[test]
fn structural_refusals_never_echo_submitted_values() {
    let mut row = dated(1, 10, "2024-01-01", None);
    row["subject_id"] = json!("synthetic-sensitive-value");
    let error = reject(&[row], ReviewPolicy::GroupInclusive);
    assert_eq!(
        serde_json::to_value(&error).unwrap(),
        json!({"code":"invalid_uuid", "row_index":0, "field":"subject_id"})
    );
    assert!(!error.to_string().contains("synthetic-sensitive-value"));
}

#[test]
fn complete_evidence_and_scoped_identifiers_are_required_and_byte_bounded() {
    let row = dated(1, 10, "2024-01-01", None);
    for field in ["source_system", "source_id", "edition_id", "snapshot_id"] {
        let mut malformed = row.clone();
        malformed["evidence"]["source_edition"]
            .as_object_mut()
            .unwrap()
            .remove(field);
        assert_eq!(
            reject(&[malformed], ReviewPolicy::GroupInclusive).code,
            "invalid_row"
        );
    }
    for container in [
        "/scope",
        "/object_identifier",
        "/evidence/original_identifier",
    ] {
        for field in ["system", "value"] {
            let mut malformed = row.clone();
            malformed
                .pointer_mut(container)
                .unwrap()
                .as_object_mut()
                .unwrap()
                .remove(field);
            assert!(classify_registry_assertions(
                &serde_json::to_vec(&[malformed]).unwrap(),
                ReviewPolicy::GroupInclusive
            )
            .is_err());
        }
        let mut malformed = row.clone();
        malformed.pointer_mut(container).unwrap()["unknown"] = json!(true);
        assert!(classify_registry_assertions(
            &serde_json::to_vec(&[malformed]).unwrap(),
            ReviewPolicy::GroupInclusive
        )
        .is_err());
    }
    for path in [
        "/scope/value",
        "/object_identifier/value",
        "/evidence/record_key",
        "/evidence/original_identifier/value",
        "/evidence/source_edition/edition_id",
    ] {
        for value in [
            String::new(),
            " leading".into(),
            "line\nbreak".into(),
            "x".repeat(129),
            "é".repeat(65),
        ] {
            let mut malformed = row.clone();
            *malformed.pointer_mut(path).unwrap() = json!(value);
            assert!(classify_registry_assertions(
                &serde_json::to_vec(&[malformed]).unwrap(),
                ReviewPolicy::GroupInclusive
            )
            .is_err());
        }
    }
    let mut exact = row.clone();
    exact["evidence"]["record_key"] = json!("é".repeat(64));
    assert_eq!(
        review(&[exact], ReviewPolicy::GroupInclusive)
            .counts
            .accepted_rows,
        1
    );
    for invalid in [
        "not-a-uuid",
        "00000000-0000-0000-0000-000000000000",
        "0000000A-0000-4000-8000-000000000001",
    ] {
        let mut malformed = row.clone();
        malformed["evidence"]["source_edition"]["snapshot_id"] = json!(invalid);
        assert_eq!(
            reject(&[malformed], ReviewPolicy::GroupInclusive).code,
            "invalid_evidence"
        );
    }
}

#[test]
fn connected_review_diagnostics_remain_linear_at_the_row_bound() {
    let rows: Vec<_> = (1..=MAX_ROWS)
        .map(|index| dated(index, index, "2024-01-01", None))
        .collect();
    let result = review(&rows, ReviewPolicy::GroupInclusive);
    assert_eq!(result.counts.review_required_rows, MAX_ROWS);
    assert_eq!(result.reviews.len(), 1);
    assert_eq!(result.reviews[0].assertion_indices.len(), MAX_ROWS);
    assert_eq!(result.reviews[0].code, "contradictory_overlapping_objects");
}
