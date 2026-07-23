use ptg2_scanner::contact_canon::{canonicalize_contact_number, canonicalize_contact_pair};
use ptg2_scanner::copy_format::{
    emit_compact_copy_row, pg_text_array_copy_field, write_copy_fields, CompactCopyRow,
};
use ptg2_scanner::dedupe::{
    dedupe_summary_payload, emit_dedupe_summary, ProviderIdentifierQuarantine, SharedDedupe,
};
use ptg2_scanner::hashing::{
    price_set_entry_key, provider_entry_component_key, provider_set_component_key,
    provider_set_entry_key,
};
use ptg2_scanner::manifest::{DenseIdMap, GlobalId128};
use ptg2_scanner::normalize::{
    int_list, normalize_catalog_code, normalize_money_text, normalized_scalar_from_reader,
    normalized_string_list_from_reader, strict_integer, strict_integer_text, strict_money_number,
};
use serde_json::json;
use std::collections::HashMap;
use struson::reader::JsonStreamReader;

#[test]
fn v4_provider_and_price_identity_dedupe_paths_remain_exact() {
    let dedupe = SharedDedupe::new_with_serving_rate_dedupe(1, true);
    assert_eq!(dedupe.insert_serving_rate("rate"), Some(true));
    assert_eq!(dedupe.insert_serving_rate("rate"), Some(false));
    assert!(dedupe.insert_procedure("procedure"));
    assert!(!dedupe.insert_procedure("procedure"));
    assert!(dedupe.insert_price_code_set("code-set"));
    assert!(!dedupe.insert_price_code_set("code-set"));

    let price_set = GlobalId128([1; 16]);
    let price_atom = GlobalId128([2; 16]);
    assert!(dedupe.insert_price_set(price_set));
    assert!(!dedupe.insert_price_set(price_set));
    dedupe.record_local_price_set_duplicates(2);
    assert!(dedupe.insert_price_atom(price_atom));
    assert!(!dedupe.insert_price_atom(price_atom));
    dedupe.record_local_price_atom_duplicates(2);
    let first_price_entry = dedupe.insert_price_set_entry(price_set, price_atom);
    let _second_price_entry = dedupe.insert_price_set_entry(price_set, price_atom);
    assert!(first_price_entry);

    let provider_set = GlobalId128([3; 16]);
    assert!(dedupe.insert_provider_set(provider_set));
    assert!(!dedupe.insert_provider_set(provider_set));
    dedupe.record_local_provider_set_duplicates(2);
    assert!(dedupe.insert_provider_set_component("provider-set", 11));
    assert!(!dedupe.insert_provider_set_component("provider-set", 11));
    assert!(dedupe.insert_provider_set_entry("provider-set", 12));
    assert!(!dedupe.insert_provider_set_entry("provider-set", 12));
    let first_component = dedupe.insert_provider_entry_component(12, 13);
    let _second_component = dedupe.insert_provider_entry_component(12, 13);
    assert!(first_component);
    assert!(dedupe.insert_provider_group(13));
    assert!(!dedupe.insert_provider_group(13));
    assert!(dedupe.insert_provider_group_member(13, 1_234_567_890));
    assert!(!dedupe.insert_provider_group_member(13, 1_234_567_890));

    dedupe
        .record_quarantined_provider_identifiers(&[-7, 10_000_000_000])
        .unwrap();
    let quarantine = dedupe.provider_identifier_quarantine().unwrap();
    assert_eq!(quarantine.payload().unwrap()["occurrence_count"], 2);
    let counts = HashMap::from([("negotiated_rates".to_owned(), 2)]);
    let summary = dedupe_summary_payload(&dedupe, &counts);
    assert_eq!(summary["serving_rate_duplicate"], 1);
    emit_dedupe_summary(&dedupe, &counts);

    let unmeasured = SharedDedupe::new_with_serving_rate_dedupe(1, false);
    assert_eq!(unmeasured.insert_serving_rate("rate"), None);
    unmeasured.record_unmeasured_serving_rates(3);
    let summary = dedupe_summary_payload(&unmeasured, &HashMap::new());
    assert!(summary["serving_rate_unique"].is_null());
    emit_dedupe_summary(&unmeasured, &HashMap::new());
}

#[test]
fn provider_identifier_quarantine_rejects_valid_and_unbounded_values() {
    let mut quarantine = ProviderIdentifierQuarantine::default();
    assert!(quarantine.record(&[0]).is_err());
    assert!(quarantine.record(&[1_234_567_890]).is_err());
    quarantine.record(&[-9, -9]).unwrap();

    let mut other = ProviderIdentifierQuarantine::default();
    other.record(&[-8]).unwrap();
    quarantine.merge(&other).unwrap();
    let payload = quarantine.payload().unwrap();
    assert_eq!(payload["occurrence_count"], 3);
    assert_eq!(payload["distinct_value_count"], 2);

    let mut full = ProviderIdentifierQuarantine::default();
    let values = (1..=1024)
        .map(|value| -i64::from(value))
        .collect::<Vec<_>>();
    full.record(&values).unwrap();
    assert!(full.record(&[-1025]).is_err());

    let mut merge_target = ProviderIdentifierQuarantine::default();
    merge_target.record(&values).unwrap();
    let mut extra = ProviderIdentifierQuarantine::default();
    extra.record(&[-1025]).unwrap();
    assert!(merge_target.merge(&extra).is_err());
}

#[test]
fn v4_coordinate_helpers_cover_empty_invalid_and_escaped_inputs() {
    let pair = canonicalize_contact_pair(None, Some("   "), Some("US"));
    assert!(pair.phone.number.is_none());
    assert!(pair.fax.number.is_none());
    for raw in [
        "letters only",
        "123x45",
        "3125551212ax12",
        "3125551212x12345678901234567",
        "3125551212x12a",
    ] {
        let _ = canonicalize_contact_number(Some(raw), Some("US"));
    }

    let network_names = vec!["name\\with\"quotes\0".to_owned()];
    assert!(pg_text_array_copy_field(&network_names).contains("\\\\"));
    let mut compact = Vec::new();
    emit_compact_copy_row(
        &mut compact,
        &CompactCopyRow {
            serving_rate_id: "rate\\id\tline\nreturn\r\0",
            snapshot_id: "snapshot",
            plan_id: "plan",
            procedure_hash: "procedure",
            procedure_code: None,
            reported_code_system: None,
            reported_code: Some("code"),
            provider_set_hash: "provider",
            provider_count: 1,
            price_set_hash: "price",
            source_trace_set_hash: "source",
            network_names: &network_names,
        },
    )
    .unwrap();
    assert!(String::from_utf8(compact).unwrap().contains("\\t"));
    let mut fields = Vec::new();
    write_copy_fields(&mut fields, &["left".to_owned(), "right".to_owned()]).unwrap();
    assert_eq!(fields, b"left\tright\n");

    let first = GlobalId128([1; 16]);
    let second = GlobalId128([2; 16]);
    let empty = DenseIdMap::from_global_ids([]);
    assert!(empty.is_empty());
    let dense = DenseIdMap::from_global_ids([second, first, second]);
    assert_eq!(
        dense.iter().collect::<Vec<_>>(),
        vec![(0, first), (1, second)]
    );
    assert_eq!(dense.dense_id(second), Some(1));
    assert_eq!(dense.global_id(2), None);

    assert_ne!(provider_set_entry_key("set", -1), 0);
    assert_ne!(provider_set_component_key("set", -2), 0);
    assert_ne!(price_set_entry_key("price", "atom"), 0);
    assert_ne!(provider_entry_component_key(-3, -4), 0);
}

#[test]
fn v4_normalization_rejects_ambiguous_numeric_and_json_shapes() {
    assert_eq!(
        normalize_catalog_code(Some(&json!("ABC")), Some("RC")),
        Some("ABC".to_owned())
    );
    assert!(int_list(Some(&json!(["bad", {}, []]))).is_empty());
    assert!(int_list(Some(&json!("bad"))).is_empty());
    assert!(strict_integer_text(&json!("1"), "field").is_err());
    assert!(strict_integer_text(&json!(1.5), "field").is_err());
    assert!(strict_integer(&json!(u64::MAX), "field").is_err());
    assert!(strict_integer(&json!(1.5), "field").is_err());
    assert_eq!(
        normalize_money_text("1e2e3".to_owned()),
        Some("1e2e3".to_owned())
    );
    assert_eq!(normalize_money_text("1e".to_owned()), Some("1e".to_owned()));
    assert_eq!(
        normalize_money_text("1e999999".to_owned()),
        Some("1e999999".to_owned())
    );
    assert!(strict_money_number(&json!("1.00")).is_err());

    let mut null_reader = JsonStreamReader::new(b"null".as_slice());
    assert_eq!(
        normalized_scalar_from_reader(&mut null_reader).unwrap(),
        None
    );
    let mut object_reader = JsonStreamReader::new(br#"{"skip":true}"#.as_slice());
    assert!(normalized_string_list_from_reader(&mut object_reader)
        .unwrap()
        .is_empty());
    let mut scalar_reader = JsonStreamReader::new(b"42".as_slice());
    assert_eq!(
        normalized_string_list_from_reader(&mut scalar_reader).unwrap(),
        vec!["42".to_owned()]
    );
}
