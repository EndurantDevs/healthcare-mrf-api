// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

use super::contracts::{
    invalid_data, ProjectedResourceRow, COPY_FIELD_COUNT, MAX_COPY_STREAM_BYTES,
};
use std::io;

const POSTGRES_COPY_MAGIC: &[u8] = b"PGCOPY\n\xff\r\n\0";

pub const COPY_COLUMNS: [&str; 18] = [
    "physical_projection_id",
    "resource_type",
    "resource_id",
    "proof_partition_id",
    "payload_hash",
    "payload_json",
    "source_rank",
    "summary_npi",
    "summary_address_count",
    "summary_addressed_location",
    "summary_geocoded_location",
    "summary_network_link_count",
    "summary_affiliation_link_count",
    "active",
    "effective_start",
    "effective_end",
    "observed_at",
    "profile_evidence_json",
];

pub fn encode_binary_copy(rows: &[ProjectedResourceRow]) -> io::Result<Vec<u8>> {
    debug_assert_eq!(COPY_COLUMNS.len(), COPY_FIELD_COUNT as usize);
    let mut output = Vec::new();
    output.extend_from_slice(POSTGRES_COPY_MAGIC);
    output.extend_from_slice(&0i32.to_be_bytes());
    output.extend_from_slice(&0i32.to_be_bytes());
    for row in rows {
        encode_row(row, &mut output)?;
        assert_copy_bound(output.len())?;
    }
    output.extend_from_slice(&(-1i16).to_be_bytes());
    assert_copy_bound(output.len())?;
    Ok(output)
}

fn encode_row(row: &ProjectedResourceRow, output: &mut Vec<u8>) -> io::Result<()> {
    output.extend_from_slice(&COPY_FIELD_COUNT.to_be_bytes());
    write_text(&row.physical_projection_id, output)?;
    write_text(&row.resource_type, output)?;
    write_text(&row.resource_id, output)?;
    write_text(&row.proof_partition_id, output)?;
    write_text(&row.payload_hash, output)?;
    write_jsonb(&row.payload_json, output)?;
    write_text(&row.source_rank, output)?;
    write_optional_i64(row.summary_npi, output);
    write_i32(row.summary_address_count, output);
    write_bool(row.summary_addressed_location, output);
    write_bool(row.summary_geocoded_location, output);
    write_i32(row.summary_network_link_count, output);
    write_i32(row.summary_affiliation_link_count, output);
    write_optional_bool(row.active, output);
    write_optional_text(row.effective_start.as_deref(), output)?;
    write_optional_text(row.effective_end.as_deref(), output)?;
    write_optional_text(row.observed_at.as_deref(), output)?;
    write_optional_jsonb(row.profile_evidence_json.as_ref(), output)
}

fn write_length(length: usize, output: &mut Vec<u8>) -> io::Result<()> {
    let encoded_length = i32::try_from(length)
        .map_err(|_| invalid_data("provider-directory COPY field exceeds int32"))?;
    output.extend_from_slice(&encoded_length.to_be_bytes());
    Ok(())
}

fn write_text(text: &str, output: &mut Vec<u8>) -> io::Result<()> {
    write_length(text.len(), output)?;
    output.extend_from_slice(text.as_bytes());
    Ok(())
}

fn write_optional_text(text: Option<&str>, output: &mut Vec<u8>) -> io::Result<()> {
    match text {
        Some(text) => write_text(text, output),
        None => {
            output.extend_from_slice(&(-1i32).to_be_bytes());
            Ok(())
        }
    }
}

fn write_jsonb(value: &serde_json::Value, output: &mut Vec<u8>) -> io::Result<()> {
    let encoded = serde_json::to_vec(value).map_err(|error| {
        invalid_data(format!(
            "provider-directory COPY JSONB cannot be encoded: {error}"
        ))
    })?;
    write_length(encoded.len().saturating_add(1), output)?;
    output.push(1);
    output.extend_from_slice(&encoded);
    Ok(())
}

fn write_optional_jsonb(value: Option<&serde_json::Value>, output: &mut Vec<u8>) -> io::Result<()> {
    match value {
        Some(value) => write_jsonb(value, output),
        None => {
            output.extend_from_slice(&(-1i32).to_be_bytes());
            Ok(())
        }
    }
}

fn write_i32(value: i32, output: &mut Vec<u8>) {
    output.extend_from_slice(&4i32.to_be_bytes());
    output.extend_from_slice(&value.to_be_bytes());
}

fn write_optional_i64(value: Option<i64>, output: &mut Vec<u8>) {
    match value {
        Some(value) => {
            output.extend_from_slice(&8i32.to_be_bytes());
            output.extend_from_slice(&value.to_be_bytes());
        }
        None => output.extend_from_slice(&(-1i32).to_be_bytes()),
    }
}

fn write_bool(value: bool, output: &mut Vec<u8>) {
    output.extend_from_slice(&1i32.to_be_bytes());
    output.push(u8::from(value));
}

fn write_optional_bool(value: Option<bool>, output: &mut Vec<u8>) {
    match value {
        Some(value) => write_bool(value, output),
        None => output.extend_from_slice(&(-1i32).to_be_bytes()),
    }
}

fn assert_copy_bound(byte_count: usize) -> io::Result<()> {
    if byte_count > MAX_COPY_STREAM_BYTES {
        return Err(invalid_data(
            "provider-directory COPY stream exceeds the byte limit",
        ));
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn test_copy_columns() -> &'static [&'static str] {
    &COPY_COLUMNS
}
