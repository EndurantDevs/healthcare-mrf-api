// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

use super::contracts::{
    hash_bytes, invalid_data, ProviderDirectoryProjectionRecord,
    ProviderDirectoryProjectionTimings, PROVIDER_DIRECTORY_PROJECTION_MAX_RESOURCE_BYTES,
    PROVIDER_DIRECTORY_PROJECTION_RECORD_CONTRACT_ID, PROVIDER_DIRECTORY_SUPPORTED_RESOURCE_TYPES,
};
use serde_json::Value;
use std::collections::BTreeMap;
use std::io;

pub(super) fn is_fhir_id(candidate: &str) -> bool {
    !candidate.is_empty()
        && candidate.len() <= 64
        && candidate
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.'))
}

pub(super) fn is_sha256(candidate: &str) -> bool {
    candidate.len() == 64
        && candidate
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

pub(super) fn resource_counts(
    projection_records: &[ProviderDirectoryProjectionRecord],
) -> BTreeMap<String, u64> {
    let mut counts = BTreeMap::new();
    for projection_record in projection_records {
        *counts
            .entry(projection_record.resource_type.clone())
            .or_insert(0) += 1;
    }
    counts
}

pub(super) fn record_identity(
    projection_record: &ProviderDirectoryProjectionRecord,
) -> [String; 2] {
    [
        projection_record.resource_type.clone(),
        projection_record.resource_id.clone(),
    ]
}

pub(super) fn record_sort_key(
    projection_record: &ProviderDirectoryProjectionRecord,
) -> (&str, &str, &str, u64) {
    (
        &projection_record.resource_type,
        &projection_record.resource_id,
        &projection_record.payload_hash,
        projection_record.input_ordinal,
    )
}

pub(super) fn validated_record_payload(
    projection_record: &ProviderDirectoryProjectionRecord,
    frame_ordinal: usize,
) -> io::Result<Vec<u8>> {
    if projection_record.contract_id != PROVIDER_DIRECTORY_PROJECTION_RECORD_CONTRACT_ID
        || PROVIDER_DIRECTORY_SUPPORTED_RESOURCE_TYPES
            .binary_search(&projection_record.resource_type.as_str())
            .is_err()
        || !is_fhir_id(&projection_record.resource_id)
        || !is_sha256(&projection_record.payload_hash)
    {
        return Err(invalid_data(format!(
            "provider-directory projection record {frame_ordinal} identity is invalid"
        )));
    }
    let fhir_resource = projection_record.payload_json.as_object().ok_or_else(|| {
        invalid_data(format!(
            "provider-directory projection record {frame_ordinal} payload is invalid"
        ))
    })?;
    if fhir_resource.get("resourceType").and_then(Value::as_str)
        != Some(projection_record.resource_type.as_str())
        || fhir_resource.get("id").and_then(Value::as_str)
            != Some(projection_record.resource_id.as_str())
    {
        return Err(invalid_data(format!(
            "provider-directory projection record {frame_ordinal} payload identity changed"
        )));
    }
    let canonical_payload =
        serde_json::to_vec(&projection_record.payload_json).map_err(|error| {
            invalid_data(format!(
                "provider-directory projection record {frame_ordinal} payload is invalid: {error}"
            ))
        })?;
    if canonical_payload.len() > PROVIDER_DIRECTORY_PROJECTION_MAX_RESOURCE_BYTES
        || hash_bytes(&canonical_payload) != projection_record.payload_hash
    {
        return Err(invalid_data(format!(
            "provider-directory projection record {frame_ordinal} payload proof changed"
        )));
    }
    Ok(canonical_payload)
}

pub(super) fn summary_timings_are_valid(timings: &ProviderDirectoryProjectionTimings) -> bool {
    [
        timings.input_read_seconds,
        timings.parse_seconds,
        timings.canonicalize_seconds,
        timings.sort_seconds,
        timings.record_spool_seconds,
        timings.total_before_stdout_seconds,
    ]
    .into_iter()
    .all(|seconds| seconds.is_finite() && (0.0..=24.0 * 60.0 * 60.0).contains(&seconds))
}
