// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

use super::contracts::{
    hash_bytes, ProviderDirectoryInputFraming, ProviderDirectoryProjectionSummary, END_FRAME_KIND,
    FRAME_HEADER_BYTES, PROVIDER_DIRECTORY_PROJECTION_SPOOL_MAGIC,
    PROVIDER_DIRECTORY_SUPPORTED_RESOURCE_TYPES, RECORD_FRAME_KIND, SUMMARY_FRAME_KIND,
};
use super::decode::decode_provider_directory_projection_spool;
use super::encode::project_provider_directory_bytes;
use super::wire::{frame_payload, write_frame};
use serde_json::Value;

fn practitioner(id: &str, family: &str) -> String {
    format!(r#"{{"resourceType":"Practitioner","id":"{id}","name":[{{"family":"{family}"}}]}}"#)
}

fn single_record_frame_payloads(spool: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let mut offset = PROVIDER_DIRECTORY_PROJECTION_SPOOL_MAGIC.len();
    let (record_kind, record_payload, _) = frame_payload(spool, &mut offset).unwrap();
    let (summary_kind, summary_payload, _) = frame_payload(spool, &mut offset).unwrap();
    assert_eq!(record_kind, RECORD_FRAME_KIND);
    assert_eq!(summary_kind, SUMMARY_FRAME_KIND);
    (record_payload.to_vec(), summary_payload.to_vec())
}

fn spool_with_payloads(record_payload: &[u8], summary_payload: &[u8]) -> Vec<u8> {
    let mut spool = PROVIDER_DIRECTORY_PROJECTION_SPOOL_MAGIC.to_vec();
    write_frame(&mut spool, RECORD_FRAME_KIND, record_payload).unwrap();
    write_frame(&mut spool, SUMMARY_FRAME_KIND, summary_payload).unwrap();
    write_frame(&mut spool, END_FRAME_KIND, &[]).unwrap();
    spool
}

fn one_practitioner_spool() -> Vec<u8> {
    project_provider_directory_bytes(
        practitioner("p-1", "Alpha").as_bytes(),
        ProviderDirectoryInputFraming::Ndjson,
    )
    .unwrap()
    .bytes
}

#[test]
fn ndjson_spool_is_sorted_and_self_verifying() {
    let input = format!(
        "{}\n{}\n",
        practitioner("z-last", "Zulu"),
        practitioner("a-first", "Alpha")
    );
    let spool =
        project_provider_directory_bytes(input.as_bytes(), ProviderDirectoryInputFraming::Ndjson)
            .unwrap();
    let (records, summary) = decode_provider_directory_projection_spool(&spool.bytes).unwrap();

    assert_eq!(records.len(), 2);
    assert_eq!(records[0].resource_id, "a-first");
    assert_eq!(records[0].input_ordinal, 1);
    assert_eq!(records[1].resource_id, "z-last");
    assert_eq!(summary.resource_counts["Practitioner"], 2);
    assert_eq!(summary.input_sha256, hash_bytes(input.as_bytes()));
    assert_eq!(summary.input_framing, "ndjson");
}

#[test]
fn bundle_and_ndjson_share_canonical_resource_proof() {
    let practitioner = practitioner("p-1", "Alpha");
    let ndjson = format!("{practitioner}\n");
    let bundle = format!(
        r#"{{"resourceType":"Bundle","type":"searchset","entry":[{{"resource":{practitioner}}}]}}"#
    );
    let ndjson_spool =
        project_provider_directory_bytes(ndjson.as_bytes(), ProviderDirectoryInputFraming::Ndjson)
            .unwrap();
    let bundle_spool =
        project_provider_directory_bytes(bundle.as_bytes(), ProviderDirectoryInputFraming::Bundle)
            .unwrap();

    assert_eq!(
        ndjson_spool.summary.canonical_input_sha256,
        bundle_spool.summary.canonical_input_sha256
    );
    assert_eq!(
        ndjson_spool.summary.record_set_sha256,
        bundle_spool.summary.record_set_sha256
    );
}

#[test]
fn strict_json_rejects_duplicates_and_unsupported_resources() {
    let duplicate = br#"{"resourceType":"Practitioner","id":"p-1","id":"p-2"}"#;
    let duplicate_error =
        project_provider_directory_bytes(duplicate, ProviderDirectoryInputFraming::Ndjson)
            .unwrap_err();
    assert!(duplicate_error
        .to_string()
        .contains("duplicate JSON object key"));

    let patient = br#"{"resourceType":"Patient","id":"p-1"}"#;
    let patient_error =
        project_provider_directory_bytes(patient, ProviderDirectoryInputFraming::Ndjson)
            .unwrap_err();
    assert!(patient_error
        .to_string()
        .contains("unsupported resourceType"));
}

#[test]
fn bundle_rejects_missing_resource_and_invalid_id() {
    let missing = br#"{"resourceType":"Bundle","entry":[{}]}"#;
    let missing_error =
        project_provider_directory_bytes(missing, ProviderDirectoryInputFraming::Bundle)
            .unwrap_err();
    assert!(missing_error.to_string().contains("lacks resource"));

    let invalid_id = br#"{"resourceType":"Practitioner","id":"bad/id"}"#;
    let invalid_error =
        project_provider_directory_bytes(invalid_id, ProviderDirectoryInputFraming::Ndjson)
            .unwrap_err();
    assert!(invalid_error.to_string().contains("invalid id"));
}

#[test]
fn every_supported_plan_net_resource_is_accepted() {
    let input = PROVIDER_DIRECTORY_SUPPORTED_RESOURCE_TYPES
        .iter()
        .enumerate()
        .map(|(ordinal, resource_type)| {
            format!(r#"{{"resourceType":"{resource_type}","id":"r-{ordinal}"}}"#)
        })
        .collect::<Vec<_>>()
        .join("\n");
    let spool =
        project_provider_directory_bytes(input.as_bytes(), ProviderDirectoryInputFraming::Ndjson)
            .unwrap();

    assert_eq!(spool.summary.resource_count, 8);
    assert!(PROVIDER_DIRECTORY_SUPPORTED_RESOURCE_TYPES
        .iter()
        .all(|resource_type| spool.summary.resource_counts[*resource_type] == 1));
}

#[test]
fn arbitrary_precision_fhir_numbers_survive_canonicalization() {
    let input = br#"{"resourceType":"Location","id":"l-1","extension":[{"valueDecimal":12345678901234567890.00000000000000000001}]}"#;
    let spool =
        project_provider_directory_bytes(input, ProviderDirectoryInputFraming::Ndjson).unwrap();
    let (records, _summary) = decode_provider_directory_projection_spool(&spool.bytes).unwrap();

    assert_eq!(
        records[0].payload_json["extension"][0]["valueDecimal"].to_string(),
        "12345678901234567890.00000000000000000001"
    );
}

#[test]
fn decoder_rejects_spool_corruption() {
    let mut spool = one_practitioner_spool();
    let record_payload_offset =
        PROVIDER_DIRECTORY_PROJECTION_SPOOL_MAGIC.len() + FRAME_HEADER_BYTES;
    spool[record_payload_offset] ^= 1;
    assert!(decode_provider_directory_projection_spool(&spool).is_err());
}

#[test]
fn decoder_rejects_unknown_record_summary_and_timing_fields() {
    let spool = one_practitioner_spool();
    let (record_payload, summary_payload) = single_record_frame_payloads(&spool);

    let mut record_json: Value = serde_json::from_slice(&record_payload).unwrap();
    record_json
        .as_object_mut()
        .unwrap()
        .insert("unknown_field".to_owned(), Value::Bool(true));
    let unknown_record = serde_json::to_vec(&record_json).unwrap();
    let record_error = decode_provider_directory_projection_spool(&spool_with_payloads(
        &unknown_record,
        &summary_payload,
    ))
    .unwrap_err();
    assert!(record_error.to_string().contains("unknown field"));

    let mut summary_json: Value = serde_json::from_slice(&summary_payload).unwrap();
    summary_json
        .as_object_mut()
        .unwrap()
        .insert("unknown_field".to_owned(), Value::Bool(true));
    let unknown_summary = serde_json::to_vec(&summary_json).unwrap();
    let summary_error = decode_provider_directory_projection_spool(&spool_with_payloads(
        &record_payload,
        &unknown_summary,
    ))
    .unwrap_err();
    assert!(summary_error.to_string().contains("unknown field"));

    let mut timing_json: Value = serde_json::from_slice(&summary_payload).unwrap();
    timing_json["timings_seconds"]
        .as_object_mut()
        .unwrap()
        .insert("unknown_field".to_owned(), Value::Bool(true));
    let unknown_timing = serde_json::to_vec(&timing_json).unwrap();
    let timing_error = decode_provider_directory_projection_spool(&spool_with_payloads(
        &record_payload,
        &unknown_timing,
    ))
    .unwrap_err();
    assert!(timing_error.to_string().contains("unknown field"));
}

#[test]
fn decoder_rejects_noncanonical_record_and_summary_frames() {
    let spool = one_practitioner_spool();
    let (record_payload, summary_payload) = single_record_frame_payloads(&spool);

    let mut noncanonical_record = vec![b' '];
    noncanonical_record.extend_from_slice(&record_payload);
    let record_error = decode_provider_directory_projection_spool(&spool_with_payloads(
        &noncanonical_record,
        &summary_payload,
    ))
    .unwrap_err();
    assert!(record_error.to_string().contains("not canonical JSON"));

    let mut noncanonical_summary = vec![b' '];
    noncanonical_summary.extend_from_slice(&summary_payload);
    let summary_error = decode_provider_directory_projection_spool(&spool_with_payloads(
        &record_payload,
        &noncanonical_summary,
    ))
    .unwrap_err();
    assert!(summary_error.to_string().contains("not canonical JSON"));
}

#[test]
fn decoder_rejects_nested_duplicate_and_noncanonical_payload_json() {
    let spool = one_practitioner_spool();
    let (record_payload, summary_payload) = single_record_frame_payloads(&spool);
    let record_text = String::from_utf8(record_payload.clone()).unwrap();

    let duplicate_text =
        record_text.replacen("\"payload_json\":{", "\"payload_json\":{\"id\":\"p-1\",", 1);
    let duplicate_error = decode_provider_directory_projection_spool(&spool_with_payloads(
        duplicate_text.as_bytes(),
        &summary_payload,
    ))
    .unwrap_err();
    assert!(duplicate_error
        .to_string()
        .contains("duplicate JSON object key"));

    let noncanonical_text = record_text.replacen("\"payload_json\":{", "\"payload_json\":{ ", 1);
    let canonical_error = decode_provider_directory_projection_spool(&spool_with_payloads(
        noncanonical_text.as_bytes(),
        &summary_payload,
    ))
    .unwrap_err();
    assert!(canonical_error.to_string().contains("not canonical JSON"));
}

#[test]
fn timing_changes_do_not_change_attesting_hashes() {
    let spool = one_practitioner_spool();
    let (record_payload, summary_payload) = single_record_frame_payloads(&spool);
    let original: ProviderDirectoryProjectionSummary =
        serde_json::from_slice(&summary_payload).unwrap();
    let mut changed = original.clone();
    changed.timings_seconds.input_read_seconds = 42.0;
    changed.timings_seconds.parse_seconds = 21.0;
    let changed_summary = serde_json::to_vec(&changed).unwrap();

    let (_, decoded) = decode_provider_directory_projection_spool(&spool_with_payloads(
        &record_payload,
        &changed_summary,
    ))
    .unwrap();
    assert_eq!(decoded.record_set_sha256, original.record_set_sha256);
    assert_eq!(decoded.record_spool_sha256, original.record_spool_sha256);
    assert_eq!(decoded.timings_seconds.input_read_seconds, 42.0);
}
