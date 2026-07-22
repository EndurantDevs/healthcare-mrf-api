// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

use super::*;
use serde_json::json;

fn context() -> ProjectionCopyContext {
    ProjectionCopyContext {
        recipe_id: "a".repeat(64),
        partition_id: "b".repeat(64),
        partition_ordinal: 7,
    }
}

fn organization_input() -> Vec<u8> {
    br#"{"resourceType":"Organization","id":"org-1","active":true}"#.to_vec()
}

fn valid_spool() -> ProviderDirectoryProjectionCopySpool {
    project_provider_directory_copy(
        &organization_input(),
        ProviderDirectoryInputFraming::Ndjson,
        &context(),
        0.0,
    )
    .expect("valid projection")
}

fn wire(
    summary: &ProviderDirectoryProjectionCopySummary,
    copy_bytes: &[u8],
    terminal: u8,
) -> Vec<u8> {
    let summary_bytes = serde_json::to_vec(summary).expect("summary JSON");
    let mut bytes = Vec::new();
    bytes.extend_from_slice(PROVIDER_DIRECTORY_PROJECTION_COPY_MAGIC);
    bytes.extend_from_slice(&(summary_bytes.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&summary_bytes);
    bytes.extend_from_slice(&(copy_bytes.len() as u64).to_be_bytes());
    bytes.extend_from_slice(copy_bytes);
    bytes.push(terminal);
    bytes
}

fn assert_summary_rejected(summary: ProviderDirectoryProjectionCopySummary, copy: &[u8]) {
    assert!(validate_summary(&summary, copy, copy.len() as u64).is_err());
}

#[test]
fn projection_rejects_empty_oversized_and_whitespace_inputs() {
    assert!(project_provider_directory_copy(
        b"",
        ProviderDirectoryInputFraming::Ndjson,
        &context(),
        0.0,
    )
    .is_err());
    assert!(project_provider_directory_copy(
        &vec![b' '; PROVIDER_DIRECTORY_PROJECTION_MAX_INPUT_BYTES + 1],
        ProviderDirectoryInputFraming::Ndjson,
        &context(),
        0.0,
    )
    .is_err());
    assert!(project_provider_directory_copy(
        b" \n\t ",
        ProviderDirectoryInputFraming::Ndjson,
        &context(),
        0.0,
    )
    .is_err());
    assert!(project_provider_directory_copy(
        b"{",
        ProviderDirectoryInputFraming::Ndjson,
        &context(),
        0.0,
    )
    .is_err());
}

#[test]
fn canonical_input_proof_enforces_resource_and_total_bounds() {
    let row = ProjectedResourceRow {
        physical_projection_id: "projection".to_owned(),
        resource_type: "Organization".to_owned(),
        resource_id: "org-1".to_owned(),
        proof_partition_id: "partition".to_owned(),
        payload_hash: "c".repeat(64),
        payload_json: json!({}),
        source_rank: "rank".to_owned(),
        summary_npi: None,
        summary_address_count: 0,
        summary_addressed_location: false,
        summary_geocoded_location: false,
        summary_network_link_count: 0,
        summary_affiliation_link_count: 0,
        active: None,
        effective_start: None,
        effective_end: None,
        observed_at: None,
        profile_evidence_json: None,
        semantic_evidence_sha256: "d".repeat(64),
    };
    let oversized_resource = vec![0; PROVIDER_DIRECTORY_PROJECTION_MAX_RESOURCE_BYTES + 1];
    assert!(canonical_input_proof(&[(row.clone(), oversized_resource)]).is_err());

    let exact_resource = vec![0; PROVIDER_DIRECTORY_PROJECTION_MAX_RESOURCE_BYTES];
    assert!(canonical_input_proof(
        &[(row.clone(), exact_resource.clone()), (row, exact_resource),]
    )
    .is_err());
}

#[test]
fn summary_and_header_require_nonempty_bounded_canonical_state() {
    let prepared = PreparedProjection {
        rows: Vec::new(),
        canonical_input_sha256: "e".repeat(64),
        canonical_input_byte_count: 1,
        transform_seconds: 0.0,
        sort_seconds: 0.0,
    };
    assert!(projection_summary(
        b"{}",
        ProviderDirectoryInputFraming::Ndjson,
        &context(),
        &prepared,
        b"copy",
        ProviderDirectoryProjectionCopyTimings {
            input_read_seconds: 0.0,
            parse_seconds: 0.0,
            transform_seconds: 0.0,
            sort_seconds: 0.0,
            copy_encode_seconds: 0.0,
            total_before_stdout_seconds: 0.0,
        },
    )
    .is_err());

    let spool = valid_spool();
    let mut oversized = spool.summary;
    oversized
        .resource_counts
        .insert("x".repeat(MAX_COPY_SUMMARY_BYTES), 1);
    assert!(encode_header(&oversized).is_err());
}

#[test]
fn spool_decoder_rejects_malformed_envelopes_and_accepts_canonical_wire() {
    let spool = valid_spool();
    let valid = wire(&spool.summary, &spool.copy_bytes, 0xff);
    let decoded = decode_provider_directory_projection_copy_spool(&valid)
        .expect("canonical wire must decode");
    assert_eq!(decoded.summary.copy_sha256, spool.summary.copy_sha256);
    assert_eq!(decoded.wire_byte_count(), valid.len());

    assert!(decode_provider_directory_projection_copy_spool(b"").is_err());
    let mut bad_magic = valid.clone();
    bad_magic[0] ^= 1;
    assert!(decode_provider_directory_projection_copy_spool(&bad_magic).is_err());

    let mut zero_summary = valid.clone();
    let prefix = PROVIDER_DIRECTORY_PROJECTION_COPY_MAGIC.len();
    zero_summary[prefix..prefix + 4].copy_from_slice(&0u32.to_be_bytes());
    assert!(decode_provider_directory_projection_copy_spool(&zero_summary).is_err());

    let mut huge_summary = valid.clone();
    huge_summary[prefix..prefix + 4]
        .copy_from_slice(&((MAX_COPY_SUMMARY_BYTES + 1) as u32).to_be_bytes());
    assert!(decode_provider_directory_projection_copy_spool(&huge_summary).is_err());

    assert!(decode_provider_directory_projection_copy_spool(
        &valid[..PROVIDER_DIRECTORY_PROJECTION_COPY_MAGIC.len() + 5]
    )
    .is_err());

    let summary_bytes = serde_json::to_vec(&spool.summary).expect("summary JSON");
    let mut noncanonical = Vec::new();
    noncanonical.extend_from_slice(PROVIDER_DIRECTORY_PROJECTION_COPY_MAGIC);
    noncanonical.extend_from_slice(&((summary_bytes.len() + 1) as u32).to_be_bytes());
    noncanonical.push(b' ');
    noncanonical.extend_from_slice(&summary_bytes);
    noncanonical.extend_from_slice(&(spool.copy_bytes.len() as u64).to_be_bytes());
    noncanonical.extend_from_slice(&spool.copy_bytes);
    noncanonical.push(0xff);
    assert!(decode_provider_directory_projection_copy_spool(&noncanonical).is_err());

    let mut invalid_json = noncanonical;
    invalid_json[prefix + 4] = b'!';
    assert!(decode_provider_directory_projection_copy_spool(&invalid_json).is_err());

    let mut zero_copy = spool.summary.clone();
    zero_copy.copy_byte_count = 0;
    let zero_copy = wire(&zero_copy, &[], 0xff);
    assert!(decode_provider_directory_projection_copy_spool(&zero_copy).is_err());

    let mut bad_terminal = valid.clone();
    *bad_terminal.last_mut().expect("terminal") = 0;
    assert!(decode_provider_directory_projection_copy_spool(&bad_terminal).is_err());

    let mut wrong_copy = spool.copy_bytes.clone();
    wrong_copy[0] ^= 1;
    let wrong_copy = wire(&spool.summary, &wrong_copy, 0xff);
    assert!(decode_provider_directory_projection_copy_spool(&wrong_copy).is_err());
}

#[test]
fn summary_validation_checks_every_contract_shape_timing_and_copy_frame() {
    let spool = valid_spool();
    validate_summary(
        &spool.summary,
        &spool.copy_bytes,
        spool.copy_bytes.len() as u64,
    )
    .expect("valid proof");

    macro_rules! reject {
        ($field:ident, $value:expr) => {{
            let mut summary = spool.summary.clone();
            summary.$field = $value;
            assert_summary_rejected(summary, &spool.copy_bytes);
        }};
    }
    reject!(record_kind, "wrong".to_owned());
    reject!(contract_id, "wrong".to_owned());
    reject!(decoder_contract_id, "wrong".to_owned());
    reject!(transform_contract_id, "wrong".to_owned());
    reject!(copy_contract_id, "wrong".to_owned());
    reject!(copy_column_contract_id, "wrong".to_owned());
    reject!(canonical_row_contract_id, "wrong".to_owned());
    reject!(input_framing, "xml".to_owned());
    reject!(recipe_id, "ABC".to_owned());
    reject!(partition_id, "ABC".to_owned());
    reject!(input_byte_count, 0);
    reject!(
        input_byte_count,
        PROVIDER_DIRECTORY_PROJECTION_MAX_INPUT_BYTES as u64 + 1
    );
    reject!(input_sha256, "ABC".to_owned());
    reject!(canonical_input_byte_count, 0);
    reject!(
        canonical_input_byte_count,
        PROVIDER_DIRECTORY_PROJECTION_MAX_INPUT_BYTES as u64 + 1
    );
    reject!(canonical_input_sha256, "ABC".to_owned());
    reject!(resource_count, 0);
    reject!(resource_count, spool.summary.resource_count + 1);
    reject!(first_identity, [String::new(), "id".to_owned()]);
    reject!(last_identity, ["Organization".to_owned(), String::new()]);
    reject!(canonical_row_sha256, "ABC".to_owned());
    reject!(copy_byte_count, 0);
    reject!(copy_sha256, "ABC".to_owned());

    let mut timing = spool.summary.clone();
    timing.timings_seconds.input_read_seconds = f64::NAN;
    assert_summary_rejected(timing, &spool.copy_bytes);
    let mut timing = spool.summary.clone();
    timing.timings_seconds.total_before_stdout_seconds = 86_400.1;
    assert_summary_rejected(timing, &spool.copy_bytes);

    let invalid_copy = vec![0; spool.copy_bytes.len()];
    let mut invalid_frame = spool.summary.clone();
    invalid_frame.copy_sha256 = sha256_bytes(&invalid_copy);
    assert!(validate_summary(&invalid_frame, &invalid_copy, invalid_copy.len() as u64).is_err());
}
