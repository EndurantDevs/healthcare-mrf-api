// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

use super::contracts::{
    invalid_data, sha256_hex, ProviderDirectoryProjectionRecord,
    ProviderDirectoryProjectionSummary, END_FRAME_KIND, MAX_SUMMARY_BYTES,
    PROVIDER_DIRECTORY_PROJECTION_DECODER_CONTRACT_ID,
    PROVIDER_DIRECTORY_PROJECTION_MAX_INPUT_BYTES,
    PROVIDER_DIRECTORY_PROJECTION_MAX_RESOURCE_BYTES,
    PROVIDER_DIRECTORY_PROJECTION_MAX_RESOURCE_COUNT,
    PROVIDER_DIRECTORY_PROJECTION_MAX_SPOOL_BYTES, PROVIDER_DIRECTORY_PROJECTION_SPOOL_CONTRACT_ID,
    PROVIDER_DIRECTORY_PROJECTION_SPOOL_MAGIC, RECORD_FRAME_KIND, SUMMARY_FRAME_KIND,
};
use super::strict_json::decoded_canonical_frame;
use super::validation::{
    is_sha256, record_identity, record_sort_key, resource_counts, summary_timings_are_valid,
    validated_record_payload,
};
use super::wire::frame_payload;
use sha2::{Digest, Sha256};
use std::io;

struct DecodedFrames {
    records: Vec<ProviderDirectoryProjectionRecord>,
    summary: ProviderDirectoryProjectionSummary,
    record_frame_byte_count: u64,
    record_set_sha256: String,
    record_spool_sha256: String,
}

fn decode_spool_frames(bytes: &[u8]) -> io::Result<DecodedFrames> {
    let mut offset = PROVIDER_DIRECTORY_PROJECTION_SPOOL_MAGIC.len();
    let mut records = Vec::new();
    let mut summary: Option<ProviderDirectoryProjectionSummary> = None;
    // Timings live only in the later summary frame. These stable attesting
    // hashes cover record frames and never timing measurements.
    let mut record_spool_digest = Sha256::new();
    record_spool_digest.update(PROVIDER_DIRECTORY_PROJECTION_SPOOL_MAGIC);
    let mut record_set_digest = Sha256::new();
    let mut record_frame_byte_count = 0u64;

    loop {
        let (frame_kind, payload, frame_start) = frame_payload(bytes, &mut offset)?;
        match frame_kind {
            RECORD_FRAME_KIND if summary.is_none() => {
                if records.len() >= PROVIDER_DIRECTORY_PROJECTION_MAX_RESOURCE_COUNT
                    || payload.len() > PROVIDER_DIRECTORY_PROJECTION_MAX_RESOURCE_BYTES + 4096
                {
                    return Err(invalid_data(
                        "provider-directory projection record frame exceeds the limit",
                    ));
                }
                record_spool_digest.update(&bytes[frame_start..offset]);
                record_frame_byte_count += (offset - frame_start) as u64;
                record_set_digest.update(payload);
                record_set_digest.update(b"\n");
                records.push(decoded_canonical_frame(
                    payload,
                    "provider-directory projection record frame",
                )?);
            }
            SUMMARY_FRAME_KIND if summary.is_none() && payload.len() <= MAX_SUMMARY_BYTES => {
                summary = Some(decoded_canonical_frame(
                    payload,
                    "provider-directory projection summary frame",
                )?);
            }
            END_FRAME_KIND if summary.is_some() && payload.is_empty() => break,
            _ => {
                return Err(invalid_data(
                    "provider-directory projection spool frame order is invalid",
                ));
            }
        }
    }
    if offset != bytes.len() || records.is_empty() {
        return Err(invalid_data(
            "provider-directory projection spool is incomplete",
        ));
    }
    Ok(DecodedFrames {
        records,
        summary: summary.ok_or_else(|| {
            invalid_data("provider-directory projection spool summary is missing")
        })?,
        record_frame_byte_count,
        record_set_sha256: sha256_hex(record_set_digest),
        record_spool_sha256: sha256_hex(record_spool_digest),
    })
}

fn canonical_resource_proof(
    projection_records: &[ProviderDirectoryProjectionRecord],
) -> io::Result<(String, u64)> {
    if projection_records
        .windows(2)
        .any(|pair| record_sort_key(&pair[0]) >= record_sort_key(&pair[1]))
    {
        return Err(invalid_data(
            "provider-directory projection record order is invalid",
        ));
    }
    let mut canonical_records = projection_records
        .iter()
        .enumerate()
        .map(|(frame_ordinal, projection_record)| {
            validated_record_payload(projection_record, frame_ordinal)
                .map(|payload| (projection_record.input_ordinal, payload))
        })
        .collect::<io::Result<Vec<_>>>()?;
    canonical_records.sort_by_key(|(input_ordinal, _payload)| *input_ordinal);

    let mut canonical_digest = Sha256::new();
    let mut canonical_byte_count = 0u64;
    for (expected_ordinal, (input_ordinal, canonical_payload)) in
        canonical_records.iter().enumerate()
    {
        if *input_ordinal != expected_ordinal as u64 {
            return Err(invalid_data(
                "provider-directory projection input ordinal census is invalid",
            ));
        }
        canonical_digest.update(canonical_payload);
        canonical_digest.update(b"\n");
        canonical_byte_count += canonical_payload.len() as u64 + 1;
    }
    Ok((sha256_hex(canonical_digest), canonical_byte_count))
}

fn summary_proof_is_valid(
    decoded: &DecodedFrames,
    canonical_input_sha256: &str,
    canonical_input_byte_count: u64,
) -> bool {
    let summary = &decoded.summary;
    summary.record_kind == "provider_directory_projection_summary"
        && summary.contract_id == PROVIDER_DIRECTORY_PROJECTION_SPOOL_CONTRACT_ID
        && summary.decoder_contract_id == PROVIDER_DIRECTORY_PROJECTION_DECODER_CONTRACT_ID
        && matches!(summary.input_framing.as_str(), "ndjson" | "bundle")
        && summary.input_byte_count > 0
        && summary.input_byte_count <= PROVIDER_DIRECTORY_PROJECTION_MAX_INPUT_BYTES as u64
        && is_sha256(&summary.input_sha256)
        && summary.canonical_input_byte_count == canonical_input_byte_count
        && summary.canonical_input_sha256 == canonical_input_sha256
        && summary.resource_count == decoded.records.len() as u64
        && summary.resource_counts == resource_counts(&decoded.records)
        && summary.first_identity == record_identity(&decoded.records[0])
        && summary.last_identity == record_identity(&decoded.records[decoded.records.len() - 1])
        && summary.record_frame_byte_count == decoded.record_frame_byte_count
        && summary.record_set_sha256 == decoded.record_set_sha256
        && summary.record_spool_sha256 == decoded.record_spool_sha256
        && summary_timings_are_valid(&summary.timings_seconds)
}

pub fn decode_provider_directory_projection_spool(
    bytes: &[u8],
) -> io::Result<(
    Vec<ProviderDirectoryProjectionRecord>,
    ProviderDirectoryProjectionSummary,
)> {
    if bytes.len() > PROVIDER_DIRECTORY_PROJECTION_MAX_SPOOL_BYTES
        || !bytes.starts_with(PROVIDER_DIRECTORY_PROJECTION_SPOOL_MAGIC)
    {
        return Err(invalid_data(
            "provider-directory projection spool header is incompatible",
        ));
    }
    let decoded = decode_spool_frames(bytes)?;
    let (canonical_input_sha256, canonical_input_byte_count) =
        canonical_resource_proof(&decoded.records)?;
    if !summary_proof_is_valid(
        &decoded,
        &canonical_input_sha256,
        canonical_input_byte_count,
    ) {
        return Err(invalid_data(
            "provider-directory projection spool proof is inconsistent",
        ));
    }
    Ok((decoded.records, decoded.summary))
}
