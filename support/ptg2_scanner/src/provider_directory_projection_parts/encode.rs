// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

use super::contracts::{
    hash_bytes, invalid_data, sha256_hex, ProviderDirectoryInputFraming,
    ProviderDirectoryProjectionRecord, ProviderDirectoryProjectionSpool,
    ProviderDirectoryProjectionSummary, ProviderDirectoryProjectionTimings, END_FRAME_KIND,
    MAX_SUMMARY_BYTES, PROVIDER_DIRECTORY_PROJECTION_DECODER_CONTRACT_ID,
    PROVIDER_DIRECTORY_PROJECTION_MAX_INPUT_BYTES,
    PROVIDER_DIRECTORY_PROJECTION_MAX_RESOURCE_BYTES,
    PROVIDER_DIRECTORY_PROJECTION_MAX_RESOURCE_COUNT,
    PROVIDER_DIRECTORY_PROJECTION_RECORD_CONTRACT_ID,
    PROVIDER_DIRECTORY_PROJECTION_SPOOL_CONTRACT_ID, PROVIDER_DIRECTORY_PROJECTION_SPOOL_MAGIC,
    PROVIDER_DIRECTORY_SUPPORTED_RESOURCE_TYPES, RECORD_FRAME_KIND, SUMMARY_FRAME_KIND,
};
use super::strict_json::strict_json;
use super::validation::{is_fhir_id, record_identity, record_sort_key, resource_counts};
use super::wire::write_frame;
use rayon::prelude::*;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::io;
use std::time::Instant;

struct PreparedProjection {
    records: Vec<ProviderDirectoryProjectionRecord>,
    canonical_input_sha256: String,
    canonical_input_byte_count: u64,
    canonicalize_seconds: f64,
    sort_seconds: f64,
}

struct EncodedRecordFrames {
    bytes: Vec<u8>,
    frame_byte_count: u64,
    record_set_sha256: String,
    record_spool_sha256: String,
    encode_seconds: f64,
}

fn trim_json_whitespace(mut bytes: &[u8]) -> &[u8] {
    while bytes
        .first()
        .is_some_and(|byte| matches!(byte, b' ' | b'\t' | b'\r'))
    {
        bytes = &bytes[1..];
    }
    while bytes
        .last()
        .is_some_and(|byte| matches!(byte, b' ' | b'\t' | b'\r'))
    {
        bytes = &bytes[..bytes.len() - 1];
    }
    bytes
}

fn ndjson_resources(input: &[u8]) -> io::Result<Vec<(usize, Value)>> {
    let raw_lines = input
        .split(|byte| *byte == b'\n')
        .map(trim_json_whitespace)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    if raw_lines.len() > PROVIDER_DIRECTORY_PROJECTION_MAX_RESOURCE_COUNT {
        return Err(invalid_data(
            "provider-directory projection resource count exceeds the limit",
        ));
    }
    let decoded_resources = raw_lines
        .par_iter()
        .enumerate()
        .map(|(ordinal, line)| {
            strict_json(
                line,
                &format!("provider-directory NDJSON resource {ordinal}"),
            )
            .map(|resource| (ordinal, resource))
        })
        .collect::<Vec<_>>();
    decoded_resources.into_iter().collect()
}

fn bundle_resources(input: &[u8]) -> io::Result<Vec<(usize, Value)>> {
    let bundle = strict_json(input, "provider-directory FHIR Bundle")?;
    let bundle_object = bundle
        .as_object()
        .ok_or_else(|| invalid_data("provider-directory FHIR Bundle must be an object"))?;
    if bundle_object.get("resourceType").and_then(Value::as_str) != Some("Bundle") {
        return Err(invalid_data(
            "provider-directory FHIR Bundle resourceType must be Bundle",
        ));
    }
    let entries = bundle_object
        .get("entry")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid_data("provider-directory FHIR Bundle entry must be an array"))?;
    if entries.len() > PROVIDER_DIRECTORY_PROJECTION_MAX_RESOURCE_COUNT {
        return Err(invalid_data(
            "provider-directory projection resource count exceeds the limit",
        ));
    }
    entries
        .iter()
        .enumerate()
        .map(|(ordinal, entry)| {
            let resource = entry
                .as_object()
                .and_then(|entry_object| entry_object.get("resource"))
                .cloned()
                .ok_or_else(|| {
                    invalid_data(format!(
                        "provider-directory FHIR Bundle entry {ordinal} lacks resource"
                    ))
                })?;
            Ok((ordinal, resource))
        })
        .collect()
}

pub(super) fn decoded_resources(
    input: &[u8],
    framing: ProviderDirectoryInputFraming,
) -> io::Result<Vec<(usize, Value)>> {
    match framing {
        ProviderDirectoryInputFraming::Ndjson => ndjson_resources(input),
        ProviderDirectoryInputFraming::Bundle => bundle_resources(input),
    }
}

fn prepare_record(
    input_ordinal: usize,
    payload_json: Value,
) -> io::Result<(ProviderDirectoryProjectionRecord, Vec<u8>)> {
    let fhir_resource = payload_json.as_object().ok_or_else(|| {
        invalid_data(format!(
            "provider-directory FHIR resource {input_ordinal} must be an object"
        ))
    })?;
    let resource_type = fhir_resource
        .get("resourceType")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            invalid_data(format!(
                "provider-directory FHIR resource {input_ordinal} lacks resourceType"
            ))
        })?;
    if PROVIDER_DIRECTORY_SUPPORTED_RESOURCE_TYPES
        .binary_search(&resource_type)
        .is_err()
    {
        return Err(invalid_data(format!(
            "provider-directory FHIR resource {input_ordinal} has unsupported resourceType"
        )));
    }
    let resource_id = fhir_resource
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            invalid_data(format!(
                "provider-directory FHIR resource {input_ordinal} lacks id"
            ))
        })?;
    if !is_fhir_id(resource_id) {
        return Err(invalid_data(format!(
            "provider-directory FHIR resource {input_ordinal} has invalid id"
        )));
    }
    let canonical_payload = serde_json::to_vec(&payload_json).map_err(|error| {
        invalid_data(format!(
            "provider-directory FHIR resource {input_ordinal} cannot be canonicalized: {error}"
        ))
    })?;
    if canonical_payload.len() > PROVIDER_DIRECTORY_PROJECTION_MAX_RESOURCE_BYTES {
        return Err(invalid_data(format!(
            "provider-directory FHIR resource {input_ordinal} exceeds the byte limit"
        )));
    }
    let projection_record = ProviderDirectoryProjectionRecord {
        contract_id: PROVIDER_DIRECTORY_PROJECTION_RECORD_CONTRACT_ID.to_owned(),
        input_ordinal: u64::try_from(input_ordinal)
            .map_err(|_| invalid_data("provider-directory input ordinal overflowed"))?,
        resource_type: resource_type.to_owned(),
        resource_id: resource_id.to_owned(),
        payload_hash: hash_bytes(&canonical_payload),
        payload_json,
    };
    Ok((projection_record, canonical_payload))
}

fn canonical_input_proof(
    prepared_records: &[(ProviderDirectoryProjectionRecord, Vec<u8>)],
) -> (String, u64) {
    let mut digest = Sha256::new();
    let mut byte_count = 0u64;
    for (_projection_record, canonical_payload) in prepared_records {
        digest.update(canonical_payload);
        digest.update(b"\n");
        byte_count = byte_count.saturating_add(canonical_payload.len() as u64 + 1);
    }
    (sha256_hex(digest), byte_count)
}

fn prepare_projection(decoded_resources: Vec<(usize, Value)>) -> io::Result<PreparedProjection> {
    let canonicalize_started = Instant::now();
    let prepared_results = decoded_resources
        .into_par_iter()
        .map(|(ordinal, resource)| prepare_record(ordinal, resource))
        .collect::<Vec<_>>();
    let prepared_records = prepared_results
        .into_iter()
        .collect::<io::Result<Vec<_>>>()?;
    let canonicalize_seconds = canonicalize_started.elapsed().as_secs_f64();
    let (canonical_input_sha256, canonical_input_byte_count) =
        canonical_input_proof(&prepared_records);

    let sort_started = Instant::now();
    let mut records = prepared_records
        .into_iter()
        .map(|(projection_record, _canonical_payload)| projection_record)
        .collect::<Vec<_>>();
    records.sort_by(|left, right| record_sort_key(left).cmp(&record_sort_key(right)));
    Ok(PreparedProjection {
        records,
        canonical_input_sha256,
        canonical_input_byte_count,
        canonicalize_seconds,
        sort_seconds: sort_started.elapsed().as_secs_f64(),
    })
}

fn encode_record_frames(
    input_size: usize,
    projection_records: &[ProviderDirectoryProjectionRecord],
) -> io::Result<EncodedRecordFrames> {
    let encode_started = Instant::now();
    let mut bytes = Vec::with_capacity(input_size.saturating_add(4096));
    bytes.extend_from_slice(PROVIDER_DIRECTORY_PROJECTION_SPOOL_MAGIC);
    let mut record_set_digest = Sha256::new();
    let mut record_spool_digest = Sha256::new();
    record_spool_digest.update(PROVIDER_DIRECTORY_PROJECTION_SPOOL_MAGIC);
    let record_frames_start = bytes.len();
    for projection_record in projection_records {
        let encoded_record = serde_json::to_vec(projection_record).map_err(|error| {
            invalid_data(format!(
                "provider-directory projection record cannot be encoded: {error}"
            ))
        })?;
        record_set_digest.update(&encoded_record);
        record_set_digest.update(b"\n");
        let frame_start = bytes.len();
        write_frame(&mut bytes, RECORD_FRAME_KIND, &encoded_record)?;
        record_spool_digest.update(&bytes[frame_start..]);
    }
    Ok(EncodedRecordFrames {
        frame_byte_count: (bytes.len() - record_frames_start) as u64,
        record_set_sha256: sha256_hex(record_set_digest),
        record_spool_sha256: sha256_hex(record_spool_digest),
        encode_seconds: encode_started.elapsed().as_secs_f64(),
        bytes,
    })
}

struct SummaryInputs<'a> {
    input: &'a [u8],
    framing: ProviderDirectoryInputFraming,
    input_read_seconds: f64,
    parse_seconds: f64,
    total_before_stdout_seconds: f64,
}

fn projection_summary(
    summary_inputs: SummaryInputs<'_>,
    prepared: &PreparedProjection,
    encoded: &EncodedRecordFrames,
) -> ProviderDirectoryProjectionSummary {
    ProviderDirectoryProjectionSummary {
        record_kind: "provider_directory_projection_summary".to_owned(),
        contract_id: PROVIDER_DIRECTORY_PROJECTION_SPOOL_CONTRACT_ID.to_owned(),
        decoder_contract_id: PROVIDER_DIRECTORY_PROJECTION_DECODER_CONTRACT_ID.to_owned(),
        input_framing: summary_inputs.framing.as_str().to_owned(),
        input_byte_count: summary_inputs.input.len() as u64,
        input_sha256: hash_bytes(summary_inputs.input),
        canonical_input_byte_count: prepared.canonical_input_byte_count,
        canonical_input_sha256: prepared.canonical_input_sha256.clone(),
        resource_count: prepared.records.len() as u64,
        resource_counts: resource_counts(&prepared.records),
        first_identity: record_identity(&prepared.records[0]),
        last_identity: record_identity(&prepared.records[prepared.records.len() - 1]),
        record_frame_byte_count: encoded.frame_byte_count,
        record_set_sha256: encoded.record_set_sha256.clone(),
        record_spool_sha256: encoded.record_spool_sha256.clone(),
        timings_seconds: ProviderDirectoryProjectionTimings {
            input_read_seconds: summary_inputs.input_read_seconds,
            parse_seconds: summary_inputs.parse_seconds,
            canonicalize_seconds: prepared.canonicalize_seconds,
            sort_seconds: prepared.sort_seconds,
            record_spool_seconds: encoded.encode_seconds,
            total_before_stdout_seconds: summary_inputs.total_before_stdout_seconds,
        },
    }
}

fn append_summary_frames(
    bytes: &mut Vec<u8>,
    summary: &ProviderDirectoryProjectionSummary,
) -> io::Result<()> {
    let encoded_summary = serde_json::to_vec(summary).map_err(|error| {
        invalid_data(format!(
            "provider-directory projection summary cannot be encoded: {error}"
        ))
    })?;
    if encoded_summary.len() > MAX_SUMMARY_BYTES {
        return Err(invalid_data(
            "provider-directory projection summary exceeds the byte limit",
        ));
    }
    write_frame(bytes, SUMMARY_FRAME_KIND, &encoded_summary)?;
    write_frame(bytes, END_FRAME_KIND, &[])
}

pub(super) fn project_provider_directory_bytes_with_read_time(
    input: &[u8],
    framing: ProviderDirectoryInputFraming,
    input_read_seconds: f64,
) -> io::Result<ProviderDirectoryProjectionSpool> {
    if input.is_empty() || input.len() > PROVIDER_DIRECTORY_PROJECTION_MAX_INPUT_BYTES {
        return Err(invalid_data(
            "provider-directory projection input byte count is invalid",
        ));
    }
    let total_started = Instant::now();
    let parse_started = Instant::now();
    let decoded = decoded_resources(input, framing)?;
    let parse_seconds = parse_started.elapsed().as_secs_f64();
    if decoded.is_empty() {
        return Err(invalid_data(
            "provider-directory projection input contains no resources",
        ));
    }
    let prepared = prepare_projection(decoded)?;
    let mut encoded = encode_record_frames(input.len(), &prepared.records)?;
    let summary = projection_summary(
        SummaryInputs {
            input,
            framing,
            input_read_seconds,
            parse_seconds,
            total_before_stdout_seconds: total_started.elapsed().as_secs_f64(),
        },
        &prepared,
        &encoded,
    );
    append_summary_frames(&mut encoded.bytes, &summary)?;
    Ok(ProviderDirectoryProjectionSpool {
        bytes: encoded.bytes,
        summary,
    })
}

pub fn project_provider_directory_bytes(
    input: &[u8],
    framing: ProviderDirectoryInputFraming,
) -> io::Result<ProviderDirectoryProjectionSpool> {
    project_provider_directory_bytes_with_read_time(input, framing, 0.0)
}
