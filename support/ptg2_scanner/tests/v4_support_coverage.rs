use ptg2_scanner::address_canon::canonicalize_address;
use ptg2_scanner::contact_canon::{canonicalize_contact_number, canonicalize_contact_pair};
use ptg2_scanner::copy_format::{
    emit_compact_copy_row, pg_text_array_copy_field, write_copy_fields, CompactCopyRow,
};
use ptg2_scanner::dedupe::{
    dedupe_summary_payload, emit_dedupe_summary, ProviderIdentifierQuarantine, SharedDedupe,
};
use ptg2_scanner::hashing::{
    hash_text_key, price_set_entry_key, provider_entry_component_key, provider_set_component_key,
    provider_set_entry_key, xxh3_63,
};
use ptg2_scanner::input::{open_plain_range_json_reader, strict_utf8_reader};
use ptg2_scanner::manifest::{DenseIdMap, GlobalId128};
use ptg2_scanner::normalize::{
    int_list, normalize_catalog_code, normalize_money_text, normalized_scalar_from_reader,
    normalized_string_list_from_reader, npi_list, strict_integer, strict_integer_text,
    strict_money_number,
};
use ptg2_scanner::progress::{
    emit_progress, ScannerSemanticProgress, ScannerSemanticProgressReporter,
    SEMANTIC_PROGRESS_INTERVAL,
};
use ptg2_scanner::v3_dense::DenseIdentityMap;
use ptg2_scanner::{decode_u32_le, intersect_sorted_unique_u32};
use serde_json::json;
use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::path::Path;
use std::sync::{atomic::AtomicU64, Arc};
use std::time::{Duration, Instant};
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
fn candidate_address_units_and_unkeyable_inputs_are_exact() {
    let duplicate = canonicalize_address(
        Some("123 Main St Ste 7"),
        Some("Ste 7"),
        Some("Chicago"),
        Some("IL"),
        Some("60601"),
        Some("US"),
    );
    assert_eq!(duplicate.unit_norm, "ste7");
    assert!(duplicate
        .line1_norm
        .as_deref()
        .is_some_and(|line| !line.contains("ste7")));

    let spaced_suffix = canonicalize_address(
        Some("123 Main St"),
        Some("Ste 2 B"),
        Some("Chicago"),
        Some("IL"),
        Some("60601"),
        Some("US"),
    );
    assert_eq!(spaced_suffix.unit_norm, "ste2b");

    let hash_separated = canonicalize_address(
        Some("123 Main St Apt. #   12"),
        None,
        Some("Chicago"),
        Some("IL"),
        Some("60601"),
        Some("US"),
    );
    assert_eq!(hash_separated.unit_norm, "apt12");
    assert_eq!(hash_separated.line1_norm.as_deref(), Some("123mainst"));

    let foreign = canonicalize_address(
        Some("123 Main St"),
        None,
        Some("Example City"),
        Some("IL"),
        Some("60601"),
        Some("CA"),
    );
    assert!(foreign.address_key.is_none());
    assert!(foreign.identity_key.is_none());
    assert!(foreign.premise_key.is_none());
    assert!(foreign.premise_identity_key.is_none());

    let zip_without_place =
        canonicalize_address(None, None, None, Some("IL"), Some("60601"), Some("US"));
    assert!(zip_without_place.address_key.is_none());
    assert!(zip_without_place.identity_key.is_none());
    assert!(zip_without_place.premise_key.is_none());
    assert!(zip_without_place.premise_identity_key.is_none());

    let punctuated_floor = canonicalize_address(
        Some("123 Main St Floor. 2"),
        None,
        Some("Chicago"),
        Some("IL"),
        Some("60601"),
        Some("US"),
    );
    assert_eq!(punctuated_floor.unit_norm, "fl2");
    assert_eq!(punctuated_floor.line1_norm.as_deref(), Some("123mainst"));

    let duplicate_floor = canonicalize_address(
        Some("123 Main St 2 Floor"),
        Some("2 Floor"),
        Some("Chicago"),
        Some("IL"),
        Some("60601"),
        Some("US"),
    );
    assert_eq!(duplicate_floor.unit_norm, "fl2");
    assert_eq!(duplicate_floor.line1_norm.as_deref(), Some("123mainst"));

    let line_one_floor = canonicalize_address(
        Some("123 Main St 2 Floor"),
        None,
        Some("Chicago"),
        Some("IL"),
        Some("60601"),
        Some("US"),
    );
    assert_eq!(line_one_floor.unit_norm, "fl2");
    assert_eq!(line_one_floor.line1_norm.as_deref(), Some("123mainst"));

    let trailing_period_floor = canonicalize_address(
        Some("123 Main St"),
        Some("2 Floor."),
        Some("Chicago"),
        Some("IL"),
        Some("60601"),
        Some("US"),
    );
    assert_eq!(trailing_period_floor.unit_norm, "fl2");
    assert_eq!(
        trailing_period_floor.line1_norm.as_deref(),
        Some("123mainst")
    );

    let zero_floor = canonicalize_address(
        Some("123 Main St"),
        Some("000 Floor"),
        Some("Chicago"),
        Some("IL"),
        Some("60601"),
        Some("US"),
    );
    assert!(zero_floor.unit_norm.is_empty());

    let zero_ordinal = canonicalize_address(
        Some("000th Ave"),
        None,
        Some("Chicago"),
        Some("IL"),
        Some("60601"),
        Some("US"),
    );
    assert_eq!(zero_ordinal.line1_norm.as_deref(), Some("000thave"));

    let zero_typo_ordinal = canonicalize_address(
        Some("000h St"),
        None,
        Some("Chicago"),
        Some("IL"),
        Some("60601"),
        Some("US"),
    );
    assert_eq!(zero_typo_ordinal.line1_norm.as_deref(), Some("000hst"));

    let punctuation_only_line2 = canonicalize_address(
        Some("123 Main St ---"),
        Some("---"),
        Some("Chicago"),
        Some("IL"),
        Some("60601"),
        Some("US"),
    );
    assert!(punctuation_only_line2.unit_norm.is_empty());
}

#[test]
fn v4_coordinate_helpers_cover_empty_invalid_and_escaped_inputs() {
    assert_eq!(hash_text_key("not-hex"), xxh3_63(b"not-hex"));
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
            procedure_code: Some(42),
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
    // Exercise the public crate boundary so coverage includes the integration
    // instantiations used by scanner consumers, not only private unit paths.
    assert_eq!(
        intersect_sorted_unique_u32(&[1, 3, 7], &[2, 3, 7]).unwrap(),
        vec![3, 7]
    );
    assert!(intersect_sorted_unique_u32(&[1, 1], &[1]).is_err());
    assert!(intersect_sorted_unique_u32(&[2, 1], &[1]).is_err());
    assert_eq!(
        decode_u32_le(&[1, 0, 0, 0, 255, 0, 0, 0]).unwrap(),
        vec![1, 255]
    );
    assert!(decode_u32_le(&[1, 0, 0]).is_err());
    assert_eq!(
        npi_list(Some(&json!([
            1_234_567_890_u64,
            "1234567890",
            123_456_789_u64,
            10_000_000_000_u64
        ]))),
        vec![1_234_567_890]
    );
    assert_eq!(
        normalize_catalog_code(Some(&json!("ABC")), Some("RC")),
        Some("ABC".to_owned())
    );
    assert!(int_list(Some(&json!(["bad", {}, []]))).is_empty());
    assert!(int_list(Some(&json!("bad"))).is_empty());
    assert!(int_list(Some(&json!({}))).is_empty());
    assert!(int_list(None).is_empty());
    assert!(strict_integer_text(&json!("1"), "field").is_err());
    assert!(strict_integer_text(&json!(1.5), "field").is_err());
    assert_eq!(strict_integer_text(&json!(7), "field").unwrap(), "7");
    assert_eq!(
        strict_integer_text(&json!(u64::MAX), "field").unwrap(),
        u64::MAX.to_string()
    );
    assert!(strict_integer(&json!(u64::MAX), "field").is_err());
    assert!(strict_integer(&json!(1.5), "field").is_err());
    let excessive_exponent: serde_json::Value =
        serde_json::from_str("1e999999").expect("arbitrary-precision JSON number");
    assert!(strict_integer_text(&excessive_exponent, "field").is_err());
    assert!(strict_money_number(&excessive_exponent).is_err());
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

    let mut utf8_reader = strict_utf8_reader(b"ok".as_slice());
    assert_eq!(utf8_reader.read(&mut []).unwrap(), 0);
    let mut payload = String::new();
    utf8_reader.read_to_string(&mut payload).unwrap();
    assert_eq!(payload, "ok");
}

#[test]
fn semantic_progress_counts_each_scanner_work_family() {
    let progress = ScannerSemanticProgress::default();

    progress.record_provider_npi_union_visits(3);
    progress.record_rate_chunk_completed();
    progress.record_in_network_object_completed();

    let snapshot = progress.snapshot();
    assert_eq!(snapshot.provider_npi_union_visits, 3);
    assert_eq!(snapshot.rate_chunks_completed, 1);
    assert_eq!(snapshot.in_network_objects_completed, 1);
    assert_eq!(snapshot.semantic_work_completed, 5);
}

#[test]
fn scanner_progress_reporters_emit_interval_frame_and_stop_cleanly() {
    let compressed_bytes_read = Arc::new(AtomicU64::new(5));
    let semantic_progress = Arc::new(ScannerSemanticProgress::default());
    semantic_progress.record_rate_chunk_completed();
    let reporter = ScannerSemanticProgressReporter::start(
        Path::new("/tmp/scanner-progress.json.gz"),
        10,
        Arc::clone(&compressed_bytes_read),
        semantic_progress,
        Instant::now(),
    )
    .unwrap();

    std::thread::sleep(SEMANTIC_PROGRESS_INTERVAL + Duration::from_secs(1));
    emit_progress(
        Path::new("/tmp/scanner-progress.json.gz"),
        10,
        &compressed_bytes_read,
        &HashMap::from([("in_network".to_owned(), 1)]),
        Instant::now(),
        true,
    );
    emit_progress(
        Path::new("/tmp/scanner-progress-empty.json"),
        0,
        &compressed_bytes_read,
        &HashMap::new(),
        Instant::now(),
        true,
    );
    drop(reporter);
}

#[test]
fn plain_range_reader_rejects_overflow_before_seeking() {
    let workspace = tempfile::tempdir().unwrap();
    let source_path = workspace.path().join("rates.json");
    fs::write(&source_path, b"{}").unwrap();

    let error =
        open_plain_range_json_reader(&source_path, u64::MAX, 2, Arc::new(AtomicU64::new(0)))
            .err()
            .expect("overflowing range must be rejected");

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("overflows u64"));

    let gzip_path = workspace.path().join("rates.json.gz");
    fs::write(&gzip_path, b"not decoded").unwrap();
    let error = open_plain_range_json_reader(&gzip_path, 0, 1, Arc::new(AtomicU64::new(0)))
        .err()
        .expect("gzip range must be rejected");
    assert_eq!(error.kind(), std::io::ErrorKind::Unsupported);

    let error = open_plain_range_json_reader(&source_path, 1, 2, Arc::new(AtomicU64::new(0)))
        .err()
        .expect("past-end range must be rejected");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);

    assert!(DenseIdentityMap::with_capacity(0).unwrap().is_empty());
}
