//! Contract coverage for the reducer; public Python acceptance requires its batch Unicode gate.
use ptg2_scanner::company_group_assertions::{
    self, reduce_company_group_assertions, UncheckedCompanyGroupReduction, MAX_INPUT_BYTES,
    MAX_OUTPUT_BYTES, MAX_ROWS,
};
use serde_json::{json, Value};

fn identity(number: usize) -> String {
    format!("{number:08x}-0000-4000-8000-000000000001")
}
fn assertion(number: usize, group: usize, start: Value, end: Value) -> Value {
    json!({
        "assertion_id": identity(number), "company_id": identity(100_000),
        "group_id": identity(group), "group_kind": "corporate_parent",
        "relationship_kind": "reported_affiliation", "valid_from": start, "valid_to": end,
        "provenance": {"kind": "manual_reference", "evidence_ref": "Reviewed reference",
            "snapshot_id": null, "source_record_key": null}
    })
}
fn source(mut row: Value) -> Value {
    row["provenance"] = json!({"kind":"source_reference", "evidence_ref":"Retained reference",
        "snapshot_id":identity(200_000), "source_record_key":"record-17"});
    row
}
fn reduce(rows: Vec<Value>) -> UncheckedCompanyGroupReduction {
    reduce_company_group_assertions(&serde_json::to_vec(&rows).unwrap()).unwrap()
}
fn reject(rows: Vec<Value>) -> company_group_assertions::CompanyGroupAssertionError {
    reduce_company_group_assertions(&serde_json::to_vec(&rows).unwrap()).unwrap_err()
}

#[test]
fn independent_corporate_and_naic_periods_preserve_declared_evidence() {
    let corporate = assertion(1, 10, json!("2024-01-01"), Value::Null);
    let mut naic = assertion(2, 11, json!("2024-01-01"), Value::Null);
    naic["group_kind"] = json!("naic_group");
    naic["relationship_kind"] = json!("verified_ownership");
    let reduced = reduce(vec![naic.clone(), corporate.clone()]);
    assert!(reduced.reviews.is_empty());
    assert_eq!(reduced.effective_assertion_indices, [0, 1]);
    assert_eq!(reduced.counts.input_rows, 2);
    assert_eq!(
        serde_json::to_value(&reduced).unwrap()["assertions"],
        json!([corporate, naic])
    );
}

#[test]
fn inclusive_touching_and_single_day_periods_conflict_without_a_winner() {
    let reduced = reduce(vec![
        assertion(2, 11, json!("2024-02-29"), json!("2024-02-29")),
        assertion(1, 10, json!("2024-01-01"), json!("2024-02-29")),
    ]);
    assert_eq!(reduced.reviews.len(), 1);
    assert_eq!(reduced.reviews[0].code, "contradictory_overlapping_groups");
    assert_eq!(reduced.reviews[0].assertion_indices, [0, 1]);
    assert!(reduced.effective_assertion_indices.is_empty());
    assert_eq!(reduced.counts.review_rows, 2);
}

#[test]
fn disjoint_periods_and_same_group_references_remain_effective() {
    let reduced = reduce(vec![
        assertion(1, 10, json!("2024-01-01"), json!("2024-01-31")),
        assertion(2, 11, json!("2024-02-01"), json!("2024-03-31")),
        source(assertion(3, 11, json!("2024-02-15"), json!("2024-02-16"))),
    ]);
    assert!(reduced.reviews.is_empty());
    assert_eq!(reduced.counts.effective_rows, 3);
    assert_eq!(
        reduced.assertions[2]
            .provenance
            .source_record_key
            .as_deref(),
        Some("record-17")
    );
}

#[test]
fn open_end_conflict_components_are_stable_under_reordering() {
    let rows = vec![
        assertion(3, 12, json!("2030-01-01"), Value::Null),
        assertion(1, 10, json!("2020-01-01"), Value::Null),
        assertion(2, 11, json!("2024-01-01"), json!("2024-12-31")),
    ];
    let forward = reduce(rows.clone());
    let backward = reduce(rows.into_iter().rev().collect());
    assert_eq!(forward, backward);
    assert_eq!(forward.reviews[0].assertion_indices, [0, 1, 2]);
    assert_eq!(
        serde_json::to_vec(&forward).unwrap(),
        serde_json::to_vec(&backward).unwrap()
    );
}

#[test]
fn connected_conflicts_retain_all_related_assertions_and_exact_accounting() {
    let reduced = reduce(vec![
        assertion(1, 10, json!("2024-01-01"), json!("2024-01-10")),
        assertion(2, 10, json!("2024-01-10"), json!("2024-01-20")),
        assertion(3, 11, json!("2024-01-20"), json!("2024-01-30")),
        assertion(4, 12, json!("2024-02-01"), json!("2024-02-02")),
    ]);
    assert_eq!(reduced.reviews[0].assertion_indices, [0, 1, 2]);
    assert_eq!(reduced.effective_assertion_indices, [3]);
    assert_eq!(
        reduced.counts.input_rows,
        reduced.counts.effective_rows + reduced.counts.review_rows
    );
    assert_eq!(reduced.assertions.len(), 4);
}

#[test]
fn unknown_dates_and_unresolved_source_rows_are_preserved_for_review() {
    let mut missing_group = source(assertion(2, 11, json!("2024-01-01"), Value::Null));
    missing_group["group_id"] = Value::Null;
    let mut both_missing = missing_group.clone();
    both_missing["assertion_id"] = json!(identity(3));
    both_missing["valid_from"] = Value::Null;
    let reduced = reduce(vec![
        assertion(1, 10, Value::Null, json!("2024-12-31")),
        missing_group,
        both_missing,
        assertion(4, 12, json!("2024-01-01"), Value::Null),
    ]);
    assert_eq!(reduced.effective_assertion_indices, [3]);
    assert_eq!(reduced.counts.review_rows, 3);
    assert_eq!(
        reduced
            .reviews
            .iter()
            .map(|review| review.code)
            .collect::<Vec<_>>(),
        [
            "missing_effective_start",
            "missing_group",
            "missing_group_and_effective_start"
        ]
    );
    assert!(reduced.assertions[0].valid_from.is_none());
    assert_eq!(
        reduced.assertions[0].valid_to.as_deref(),
        Some("2024-12-31")
    );
    assert!(reduced.assertions[2].group_id.is_none());
}

#[test]
fn conflict_scope_keeps_company_group_kind_and_relationship_independent() {
    let base = assertion(1, 10, json!("2024-01-01"), Value::Null);
    let mut other_company = assertion(2, 11, json!("2024-01-01"), Value::Null);
    other_company["company_id"] = json!(identity(100_001));
    let mut other_kind = assertion(3, 12, json!("2024-01-01"), Value::Null);
    other_kind["group_kind"] = json!("naic_group");
    let mut other_relationship = assertion(4, 13, json!("2024-01-01"), Value::Null);
    other_relationship["relationship_kind"] = json!("verified_ownership");
    assert!(
        reduce(vec![base, other_company, other_kind, other_relationship])
            .reviews
            .is_empty()
    );
}

#[test]
fn missing_or_unknown_fields_and_nonstring_nullable_fields_refuse_whole_batch() {
    let original = assertion(2, 11, json!("2024-01-01"), Value::Null);
    for field in [
        "assertion_id",
        "company_id",
        "group_id",
        "group_kind",
        "relationship_kind",
        "valid_from",
        "valid_to",
        "provenance",
    ] {
        let mut malformed = original.clone();
        malformed.as_object_mut().unwrap().remove(field);
        assert_eq!(
            reject(vec![
                assertion(1, 10, json!("2024-01-01"), Value::Null),
                malformed
            ])
            .code,
            "invalid_row",
            "{field}"
        );
    }
    for field in ["snapshot_id", "source_record_key", "kind", "evidence_ref"] {
        let mut malformed = original.clone();
        malformed["provenance"]
            .as_object_mut()
            .unwrap()
            .remove(field);
        assert_eq!(reject(vec![malformed]).code, "invalid_row", "{field}");
    }
    for field in ["group_id", "valid_from", "valid_to"] {
        let mut malformed = original.clone();
        malformed[field] = json!(17);
        assert_eq!(reject(vec![malformed]).code, "invalid_field", "{field}");
    }
    let mut malformed = original.clone();
    malformed["company_name"] = json!("No inferred identity");
    assert_eq!(reject(vec![malformed]).code, "invalid_row");
    let mut malformed = original;
    malformed["provenance"]["approved"] = json!(true);
    assert_eq!(reject(vec![malformed]).code, "invalid_row");
}

#[test]
fn identity_enums_dates_and_periods_are_strict() {
    let original = assertion(1, 10, json!("2024-01-01"), Value::Null);
    for field in ["assertion_id", "company_id", "group_id"] {
        for invalid in [
            "00000000-0000-0000-0000-000000000000",
            "0000000A-0000-4000-8000-000000000001",
            "not-a-uuid",
        ] {
            let mut malformed = original.clone();
            malformed[field] = json!(invalid);
            assert_eq!(reject(vec![malformed]).code, "invalid_uuid");
        }
    }
    for (field, invalid) in [
        ("group_kind", "parent"),
        ("relationship_kind", "automatic_ownership"),
    ] {
        let mut malformed = original.clone();
        malformed[field] = json!(invalid);
        assert_eq!(reject(vec![malformed]).code, "invalid_row");
    }
    for invalid in [
        "0000-01-01",
        "1900-02-29",
        "2023-02-29",
        "2024-13-01",
        "2024-01-00",
        "2024-1-01",
        "２０２４-01-01",
    ] {
        let mut malformed = original.clone();
        malformed["valid_from"] = json!(invalid);
        assert_eq!(reject(vec![malformed]).code, "invalid_date", "{invalid}");
    }
    assert_eq!(
        reduce(vec![assertion(
            1,
            10,
            json!("2000-02-29"),
            json!("2000-02-29")
        )])
        .counts
        .effective_rows,
        1
    );
    assert_eq!(
        reject(vec![assertion(
            1,
            10,
            json!("2024-02-01"),
            json!("2024-01-31")
        )])
        .code,
        "invalid_period"
    );
}

#[test]
fn provenance_shape_requires_exact_declared_source_or_manual_reference() {
    let original = assertion(1, 10, json!("2024-01-01"), Value::Null);
    let mut manual_snapshot = original.clone();
    manual_snapshot["provenance"]["snapshot_id"] = json!(identity(200_000));
    let mut manual_key = original.clone();
    manual_key["provenance"]["source_record_key"] = json!("row");
    let mut manual_unresolved = original.clone();
    manual_unresolved["group_id"] = Value::Null;
    let mut source_missing_snapshot = source(original.clone());
    source_missing_snapshot["provenance"]["snapshot_id"] = Value::Null;
    let mut source_missing_key = source(original.clone());
    source_missing_key["provenance"]["source_record_key"] = Value::Null;
    let mut source_bad_snapshot = source(original.clone());
    source_bad_snapshot["provenance"]["snapshot_id"] = json!("not-a-uuid");
    for malformed in [
        manual_snapshot,
        manual_key,
        manual_unresolved,
        source_missing_snapshot,
        source_missing_key,
        source_bad_snapshot,
    ] {
        assert_eq!(reject(vec![malformed]).code, "invalid_provenance");
    }
}

#[test]
fn provenance_text_bounds_apply_to_utf8_not_only_character_counts() {
    let original = assertion(1, 10, json!("2024-01-01"), Value::Null);
    for invalid in [
        String::new(),
        " leading".into(),
        "trailing ".into(),
        "line\nbreak".into(),
        "x".repeat(129),
        "é".repeat(65),
    ] {
        let mut malformed = original.clone();
        malformed["provenance"]["evidence_ref"] = json!(invalid);
        assert_eq!(reject(vec![malformed]).code, "invalid_provenance");
        let mut malformed = source(original.clone());
        malformed["provenance"]["source_record_key"] = json!(invalid);
        assert_eq!(reject(vec![malformed]).code, "invalid_provenance");
    }
    let mut exact = original;
    exact["provenance"]["evidence_ref"] = json!("é".repeat(64));
    assert_eq!(reduce(vec![exact]).counts.effective_rows, 1);
}

#[test]
fn duplicate_assertion_identity_refuses_instead_of_silently_deduplicating() {
    let row = assertion(1, 10, json!("2024-01-01"), Value::Null);
    let error = reject(vec![row.clone(), row]);
    assert_eq!(error.code, "duplicate_assertion_id");
    assert_eq!(error.row_index, Some(1));
    assert_eq!(error.field, Some("assertion_id"));
}

#[test]
fn json_must_be_one_array_with_no_trailing_input_or_ambiguous_fields() {
    for invalid in [
        b"null".as_slice(),
        b"{}",
        b"[null]",
        b"[] []",
        b"[",
        b"[{}]",
    ] {
        assert!(reduce_company_group_assertions(invalid).is_err());
    }
    let row = serde_json::to_string(&assertion(1, 10, json!("2024-01-01"), Value::Null)).unwrap();
    let duplicate = row.replacen(
        "{",
        "{\"company_id\":\"00000001-0000-4000-8000-000000000001\",",
        1,
    );
    assert!(reduce_company_group_assertions(format!("[{duplicate}]").as_bytes()).is_err());
}

#[test]
fn row_and_byte_limits_refuse_before_exposing_a_partial_result() {
    let rows: Vec<_> = (1..=MAX_ROWS)
        .map(|number| assertion(number, 10, json!("2024-01-01"), Value::Null))
        .collect();
    let exact = reduce(rows.clone());
    assert_eq!(exact.counts.input_rows, MAX_ROWS);
    assert_eq!(exact.counts.effective_rows, MAX_ROWS);
    assert!(serde_json::to_vec(&exact).unwrap().len() <= MAX_OUTPUT_BYTES);
    let mut excess = rows;
    excess.push(assertion(
        MAX_ROWS + 1,
        10,
        json!("2024-01-01"),
        Value::Null,
    ));
    let error = reject(excess);
    assert_eq!((error.code, error.row_index), ("row_limit", Some(MAX_ROWS)));
    assert_eq!(
        reduce_company_group_assertions(&vec![b' '; MAX_INPUT_BYTES + 1])
            .unwrap_err()
            .code,
        "input_limit"
    );
}

#[test]
fn batch_unicode_material_includes_review_rows_and_is_never_wire_authority() {
    let mut row = source(assertion(1, 10, Value::Null, Value::Null));
    row["provenance"]["evidence_ref"] = json!("e\u{301}vidence");
    row["provenance"]["source_record_key"] = json!("source\u{200d}key");
    let reduced = reduce(vec![row]);
    // Rust retains format scalars provisionally; actual Python batch isprintable must reject U+200D.
    assert_eq!(
        reduced.provenance_text(),
        " e\u{301}vidence source\u{200d}key "
    );
    assert_eq!(reduced.counts.review_rows, 1);
    let wire = serde_json::to_value(&reduced).unwrap();
    assert!(wire.get("provenance_text").is_none());
    let empty = reduce(vec![]);
    assert_eq!(empty.provenance_text(), " ");
    assert_eq!(empty.counts.input_rows, 0);
}

#[test]
fn errors_expose_only_static_codes_and_row_coordinates() {
    let mut malformed = assertion(1, 10, json!("2024-01-01"), Value::Null);
    malformed["company_id"] = json!("synthetic-sensitive-value");
    let error = reject(vec![malformed]);
    assert!(!error.to_string().contains("synthetic-sensitive-value"));
    assert_eq!(
        serde_json::to_value(error).unwrap(),
        json!({"code":"invalid_uuid", "row_index":0, "field":"company_id"})
    );
}

fn no_affiliation(number: usize, start: Value, end: Value) -> Value {
    let mut row = assertion(number, 10, start, end);
    row["relationship_kind"] = json!("verified_no_affiliation");
    row["group_id"] = Value::Null;
    row
}

#[test]
fn evidenced_no_affiliation_is_effective_without_inventing_a_group() {
    let row = no_affiliation(1, json!("2024-01-01"), Value::Null);
    let mut naic = row.clone();
    naic["assertion_id"] = json!(identity(2));
    naic["group_kind"] = json!("naic_group");
    let reduced = reduce(vec![row.clone(), naic.clone()]);
    assert!(reduced.reviews.is_empty());
    assert_eq!(reduced.effective_assertion_indices, [0, 1]);
    assert_eq!(
        serde_json::to_value(reduced.assertions).unwrap(),
        json!([row, naic])
    );
}

#[test]
fn no_affiliation_refuses_group_unknown_start_and_source_inference() {
    let row = no_affiliation(1, json!("2024-01-01"), Value::Null);
    let mut group = row.clone();
    group["group_id"] = json!(identity(10));
    let mut undated = row.clone();
    undated["valid_from"] = Value::Null;
    for malformed in [group, undated, source(row)] {
        assert_eq!(reject(vec![malformed]).code, "invalid_no_affiliation");
    }
}

#[test]
fn no_affiliation_conflicts_with_both_positive_kinds_only_in_its_period_and_scope() {
    for kind in ["reported_affiliation", "verified_ownership"] {
        let none = no_affiliation(1, json!("2024-01-01"), json!("2024-02-29"));
        let mut positive = assertion(2, 10, json!("2024-02-29"), Value::Null);
        positive["relationship_kind"] = json!(kind);
        let reduced = reduce(vec![none.clone(), positive.clone()]);
        assert_eq!(reduced.reviews.len(), 1);
        assert_eq!(reduced.reviews[0].code, "contradictory_overlapping_groups");
        assert_eq!(reduced.reviews[0].assertion_indices, [0, 1]);
        assert!(reduced.effective_assertion_indices.is_empty());
        positive["valid_from"] = json!("2024-03-01");
        assert!(reduce(vec![none.clone(), positive.clone()])
            .reviews
            .is_empty());
        positive["valid_from"] = json!("2024-02-29");
        positive["group_kind"] = json!("naic_group");
        assert!(reduce(vec![none, positive]).reviews.is_empty());
    }
}
