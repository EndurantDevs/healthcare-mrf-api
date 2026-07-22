// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

use super::canonical::{canonical_row_bytes, sha256_bytes};
use super::contracts::{
    invalid_data, is_sha256, timings_are_valid, ProjectedResourceRow, ProjectionCopyContext,
    ProviderDirectoryInputFraming, ProviderDirectoryProjectionCopySpool,
    ProviderDirectoryProjectionCopySummary, ProviderDirectoryProjectionCopyTimings,
    CANONICAL_ROW_CONTRACT_ID, COPY_COLUMN_CONTRACT_ID, COPY_CONTRACT_ID, COPY_SPOOL_CONTRACT_ID,
    COPY_SUMMARY_RECORD_KIND, MAX_COPY_STREAM_BYTES, MAX_COPY_SUMMARY_BYTES,
    PROVIDER_DIRECTORY_PROJECTION_COPY_MAGIC, PROVIDER_DIRECTORY_PROJECTION_DECODER_CONTRACT_ID,
    PROVIDER_DIRECTORY_PROJECTION_MAX_INPUT_BYTES,
    PROVIDER_DIRECTORY_PROJECTION_MAX_RESOURCE_BYTES, TRANSFORM_CONTRACT_ID,
};
use super::input::decoded_resources;
use super::pg_copy::encode_binary_copy;
use super::semantics::project_resource;
use rayon::prelude::*;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io;
use std::time::Instant;

struct PreparedProjection {
    rows: Vec<ProjectedResourceRow>,
    canonical_input_sha256: String,
    canonical_input_byte_count: u64,
    transform_seconds: f64,
    sort_seconds: f64,
}

pub fn project_provider_directory_copy(
    input: &[u8],
    framing: ProviderDirectoryInputFraming,
    context: &ProjectionCopyContext,
    input_read_seconds: f64,
) -> io::Result<ProviderDirectoryProjectionCopySpool> {
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
    let prepared = prepare_projection(decoded, context)?;
    let copy_started = Instant::now();
    let copy_bytes = encode_binary_copy(&prepared.rows)?;
    let copy_encode_seconds = copy_started.elapsed().as_secs_f64();
    let summary = projection_summary(
        input,
        framing,
        context,
        &prepared,
        &copy_bytes,
        ProviderDirectoryProjectionCopyTimings {
            input_read_seconds,
            parse_seconds,
            transform_seconds: prepared.transform_seconds,
            sort_seconds: prepared.sort_seconds,
            copy_encode_seconds,
            total_before_stdout_seconds: total_started.elapsed().as_secs_f64(),
        },
    )?;
    let header_bytes = encode_header(&summary)?;
    Ok(ProviderDirectoryProjectionCopySpool {
        header_bytes,
        summary,
        copy_bytes,
    })
}

fn prepare_projection(
    decoded: Vec<(usize, serde_json::Value)>,
    context: &ProjectionCopyContext,
) -> io::Result<PreparedProjection> {
    let transform_started = Instant::now();
    let projected = decoded
        .into_par_iter()
        .map(|(ordinal, resource)| project_resource(ordinal, resource, context))
        .collect::<Vec<_>>()
        .into_iter()
        .collect::<io::Result<Vec<_>>>()?;
    let transform_seconds = transform_started.elapsed().as_secs_f64();
    let (canonical_input_sha256, canonical_input_byte_count) = canonical_input_proof(&projected)?;
    let sort_started = Instant::now();
    let mut rows = projected
        .into_iter()
        .map(|(row, _canonical_payload)| row)
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.sort_key().cmp(&right.sort_key()));
    if rows
        .windows(2)
        .any(|pair| pair[0].sort_key() >= pair[1].sort_key())
    {
        return Err(invalid_data(
            "provider-directory projected row order is not strict",
        ));
    }
    Ok(PreparedProjection {
        rows,
        canonical_input_sha256,
        canonical_input_byte_count,
        transform_seconds,
        sort_seconds: sort_started.elapsed().as_secs_f64(),
    })
}

fn canonical_input_proof(
    projected: &[(ProjectedResourceRow, Vec<u8>)],
) -> io::Result<(String, u64)> {
    let mut digest = Sha256::new();
    let mut byte_count = 0u64;
    for (_row, payload) in projected {
        if payload.len() > PROVIDER_DIRECTORY_PROJECTION_MAX_RESOURCE_BYTES {
            return Err(invalid_data(
                "provider-directory canonical resource exceeds the byte limit",
            ));
        }
        digest.update(payload);
        digest.update(b"\n");
        byte_count = byte_count
            .checked_add(payload.len() as u64 + 1)
            .expect("bounded resource count and size fit u64");
    }
    if byte_count > PROVIDER_DIRECTORY_PROJECTION_MAX_INPUT_BYTES as u64 {
        return Err(invalid_data(
            "provider-directory canonical input exceeds the byte limit",
        ));
    }
    Ok((hex_digest(digest), byte_count))
}

fn canonical_row_sha256(rows: &[ProjectedResourceRow]) -> String {
    let mut digest = Sha256::new();
    for (ordinal, row) in rows.iter().enumerate() {
        if ordinal != 0 {
            digest.update(b"\n");
        }
        digest.update(canonical_row_bytes(row));
    }
    hex_digest(digest)
}

fn projection_summary(
    input: &[u8],
    framing: ProviderDirectoryInputFraming,
    context: &ProjectionCopyContext,
    prepared: &PreparedProjection,
    copy_bytes: &[u8],
    timings: ProviderDirectoryProjectionCopyTimings,
) -> io::Result<ProviderDirectoryProjectionCopySummary> {
    let resource_count =
        u64::try_from(prepared.rows.len()).expect("the 100,000-resource input bound fits u64");
    let first = prepared
        .rows
        .first()
        .ok_or_else(|| invalid_data("provider-directory projection contains no rows"))?;
    let last = prepared.rows.last().expect("nonempty projected rows");
    let mut resource_counts = BTreeMap::new();
    for row in &prepared.rows {
        *resource_counts
            .entry(row.resource_type.clone())
            .or_insert(0) += 1;
    }
    Ok(ProviderDirectoryProjectionCopySummary {
        record_kind: COPY_SUMMARY_RECORD_KIND.to_owned(),
        contract_id: COPY_SPOOL_CONTRACT_ID.to_owned(),
        decoder_contract_id: PROVIDER_DIRECTORY_PROJECTION_DECODER_CONTRACT_ID.to_owned(),
        transform_contract_id: TRANSFORM_CONTRACT_ID.to_owned(),
        copy_contract_id: COPY_CONTRACT_ID.to_owned(),
        copy_column_contract_id: COPY_COLUMN_CONTRACT_ID.to_owned(),
        canonical_row_contract_id: CANONICAL_ROW_CONTRACT_ID.to_owned(),
        input_framing: framing.as_str().to_owned(),
        recipe_id: context.recipe_id.clone(),
        partition_id: context.partition_id.clone(),
        partition_ordinal: context.partition_ordinal,
        input_byte_count: input.len() as u64,
        input_sha256: sha256_bytes(input),
        canonical_input_byte_count: prepared.canonical_input_byte_count,
        canonical_input_sha256: prepared.canonical_input_sha256.clone(),
        resource_count,
        resource_counts,
        first_identity: [first.resource_type.clone(), first.resource_id.clone()],
        last_identity: [last.resource_type.clone(), last.resource_id.clone()],
        canonical_row_sha256: canonical_row_sha256(&prepared.rows),
        copy_byte_count: copy_bytes.len() as u64,
        copy_sha256: sha256_bytes(copy_bytes),
        timings_seconds: timings,
    })
}

fn encode_header(summary: &ProviderDirectoryProjectionCopySummary) -> io::Result<Vec<u8>> {
    let summary_bytes =
        serde_json::to_vec(summary).expect("COPY summary serialization is infallible");
    if summary_bytes.len() > MAX_COPY_SUMMARY_BYTES {
        return Err(invalid_data(
            "provider-directory COPY summary exceeds the byte limit",
        ));
    }
    let summary_length =
        u32::try_from(summary_bytes.len()).expect("the 1 MiB summary bound fits u32");
    let mut header = Vec::with_capacity(
        PROVIDER_DIRECTORY_PROJECTION_COPY_MAGIC.len() + 4 + summary_bytes.len() + 8,
    );
    header.extend_from_slice(PROVIDER_DIRECTORY_PROJECTION_COPY_MAGIC);
    header.extend_from_slice(&summary_length.to_be_bytes());
    header.extend_from_slice(&summary_bytes);
    header.extend_from_slice(&summary.copy_byte_count.to_be_bytes());
    Ok(header)
}

pub fn decode_provider_directory_projection_copy_spool(
    bytes: &[u8],
) -> io::Result<ProviderDirectoryProjectionCopySpool> {
    let prefix_bytes = PROVIDER_DIRECTORY_PROJECTION_COPY_MAGIC.len() + 4;
    if bytes.len() < prefix_bytes + 8 + 1
        || !bytes.starts_with(PROVIDER_DIRECTORY_PROJECTION_COPY_MAGIC)
    {
        return Err(invalid_data(
            "provider-directory COPY spool header is incompatible",
        ));
    }
    let summary_length = u32::from_be_bytes(
        bytes[PROVIDER_DIRECTORY_PROJECTION_COPY_MAGIC.len()..prefix_bytes]
            .try_into()
            .expect("four-byte summary length"),
    ) as usize;
    if summary_length == 0 || summary_length > MAX_COPY_SUMMARY_BYTES {
        return Err(invalid_data(
            "provider-directory COPY summary length is invalid",
        ));
    }
    let summary_end = prefix_bytes + summary_length;
    let copy_length_end = summary_end + 8;
    if copy_length_end >= bytes.len() {
        return Err(invalid_data("provider-directory COPY spool is truncated"));
    }
    let summary_bytes = &bytes[prefix_bytes..summary_end];
    let summary: ProviderDirectoryProjectionCopySummary = serde_json::from_slice(summary_bytes)
        .map_err(|error| {
            invalid_data(format!(
                "provider-directory COPY summary is invalid: {error}"
            ))
        })?;
    if serde_json::to_vec(&summary).ok().as_deref() != Some(summary_bytes) {
        return Err(invalid_data(
            "provider-directory COPY summary is not canonical",
        ));
    }
    let declared_copy_length = u64::from_be_bytes(
        bytes[summary_end..copy_length_end]
            .try_into()
            .expect("eight-byte COPY length"),
    );
    if declared_copy_length == 0 || declared_copy_length > MAX_COPY_STREAM_BYTES as u64 {
        return Err(invalid_data("provider-directory COPY length is invalid"));
    }
    let copy_length =
        usize::try_from(declared_copy_length).expect("the 128 MiB COPY stream bound fits usize");
    let copy_end = copy_length_end + copy_length;
    if copy_end + 1 != bytes.len() || bytes[copy_end] != 0xff {
        return Err(invalid_data(
            "provider-directory COPY spool terminal is invalid",
        ));
    }
    let copy_bytes = bytes[copy_length_end..copy_end].to_vec();
    validate_summary(&summary, &copy_bytes, declared_copy_length)?;
    Ok(ProviderDirectoryProjectionCopySpool {
        header_bytes: bytes[..copy_length_end].to_vec(),
        summary,
        copy_bytes,
    })
}

fn validate_summary(
    summary: &ProviderDirectoryProjectionCopySummary,
    copy_bytes: &[u8],
    declared_copy_length: u64,
) -> io::Result<()> {
    let contracts_are_valid = summary.record_kind == COPY_SUMMARY_RECORD_KIND
        && summary.contract_id == COPY_SPOOL_CONTRACT_ID
        && summary.decoder_contract_id == PROVIDER_DIRECTORY_PROJECTION_DECODER_CONTRACT_ID
        && summary.transform_contract_id == TRANSFORM_CONTRACT_ID
        && summary.copy_contract_id == COPY_CONTRACT_ID
        && summary.copy_column_contract_id == COPY_COLUMN_CONTRACT_ID
        && summary.canonical_row_contract_id == CANONICAL_ROW_CONTRACT_ID;
    let shape_is_valid = matches!(summary.input_framing.as_str(), "ndjson" | "bundle")
        && is_sha256(&summary.recipe_id)
        && is_sha256(&summary.partition_id)
        && summary.input_byte_count > 0
        && summary.input_byte_count <= PROVIDER_DIRECTORY_PROJECTION_MAX_INPUT_BYTES as u64
        && is_sha256(&summary.input_sha256)
        && summary.canonical_input_byte_count > 0
        && summary.canonical_input_byte_count
            <= PROVIDER_DIRECTORY_PROJECTION_MAX_INPUT_BYTES as u64
        && is_sha256(&summary.canonical_input_sha256)
        && summary.resource_count > 0
        && summary.resource_counts.values().sum::<u64>() == summary.resource_count
        && summary
            .first_identity
            .iter()
            .all(|identity| !identity.is_empty())
        && summary
            .last_identity
            .iter()
            .all(|identity| !identity.is_empty())
        && is_sha256(&summary.canonical_row_sha256)
        && summary.copy_byte_count == declared_copy_length
        && summary.copy_byte_count == copy_bytes.len() as u64
        && summary.copy_sha256 == sha256_bytes(copy_bytes)
        && timings_are_valid(&summary.timings_seconds);
    if !contracts_are_valid || !shape_is_valid {
        return Err(invalid_data(
            "provider-directory COPY summary proof is inconsistent",
        ));
    }
    if !copy_bytes.starts_with(b"PGCOPY\n\xff\r\n\0")
        || !copy_bytes.ends_with(&(-1i16).to_be_bytes())
    {
        return Err(invalid_data(
            "provider-directory PostgreSQL COPY framing is invalid",
        ));
    }
    Ok(())
}

fn hex_digest(digest: Sha256) -> String {
    let bytes = digest.finalize();
    let mut encoded = String::with_capacity(64);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[cfg(test)]
#[path = "tests/encode_coverage_tests.rs"]
mod coverage_tests;
