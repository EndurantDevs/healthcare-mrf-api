// Licensed under the HealthPorta Non-Commercial License (see LICENSE).
//! Parallel Rust extraction and PostgreSQL binary COPY encoding for typed FHIR `source_cells`.
//!
//! Any row whose payload fails the exact `network_fhir_typed_sql.py` shape contract (or uses
//! float `npi`, NUL bytes, or unsupported JSON) causes the page to return `Ok(None)` so the
//! caller falls back to `PROJECT_SQL` with identical PostgreSQL semantics and error codes.

use crate::fhir_witness_page::{
    registry_location_uuid_bytes, validate_witness_page, WitnessPageRow, WitnessPageSummary,
    WitnessValidationOptions,
};
use crate::network_membership_codec::{write_field, COPY_HEADER};
use rayon::prelude::*;
use serde_json::{Map, Value};
use std::collections::BTreeSet;

const CELL_FIELD_COUNT: i16 = 18;
const PG_TEXT_OID: i32 = 25;
const PARALLEL_THRESHOLD: usize = 16;

const ADDRESS_KEYS: [&str; 6] = [
    "first_line",
    "second_line",
    "city_name",
    "state_name",
    "postal_code",
    "country_code",
];

enum CellExtractError {
    Fallback,
    Hard(&'static str),
}

fn is_pg_exact_int(text: &str) -> bool {
    if text == "-0" || text.is_empty() {
        return false;
    }
    let digits = text.strip_prefix('-').unwrap_or(text);
    !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
}

fn has_nul(text: &str) -> bool {
    text.as_bytes().contains(&0)
}

fn extract_opt_text<'a>(
    map: &'a Map<String, Value>,
    key: &str,
) -> Result<Option<&'a str>, CellExtractError> {
    match map.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) if !has_nul(text) => Ok(Some(text.as_str())),
        _ => Err(CellExtractError::Fallback),
    }
}

fn extract_opt_bool(map: &Map<String, Value>, key: &str) -> Result<Option<bool>, CellExtractError> {
    match map.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(flag)) => Ok(Some(*flag)),
        _ => Err(CellExtractError::Fallback),
    }
}

pub type PreparedTypedCopy = (Vec<u8>, usize, WitnessPageSummary);

fn extract_opt_npi(map: &Map<String, Value>) -> Result<Option<&str>, CellExtractError> {
    match map.get("npi") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) if !has_nul(text) => Ok(Some(text.as_str())),
        Some(Value::Number(number)) if is_pg_exact_int(number.as_str()) => {
            Ok(Some(number.as_str()))
        }
        _ => Err(CellExtractError::Fallback),
    }
}

fn extract_text_array<'a>(
    map: &'a Map<String, Value>,
    key: &str,
) -> Result<Vec<&'a str>, CellExtractError> {
    match map.get(key) {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(items)) => {
            let mut values = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Value::String(text) if !has_nul(text) => values.push(text.as_str()),
                    _ => return Err(CellExtractError::Fallback),
                }
            }
            Ok(values)
        }
        _ => Err(CellExtractError::Fallback),
    }
}

fn write_opt_text(value: Option<&str>, out: &mut Vec<u8>) -> Result<(), CellExtractError> {
    match value {
        Some(text) => {
            if i32::try_from(text.len()).is_err() {
                return Err(CellExtractError::Fallback);
            }
            write_field(text.as_bytes(), out);
        }
        None => out.extend_from_slice(&(-1i32).to_be_bytes()),
    }
    Ok(())
}

fn write_opt_bool(value: Option<bool>, out: &mut Vec<u8>) {
    match value {
        Some(flag) => {
            out.extend_from_slice(&1i32.to_be_bytes());
            out.push(u8::from(flag));
        }
        None => out.extend_from_slice(&(-1i32).to_be_bytes()),
    }
}

fn write_text_array(items: &[&str], out: &mut Vec<u8>) -> Result<(), CellExtractError> {
    if items.is_empty() {
        // PostgreSQL binary array format for empty 0-dimensional text[].
        out.extend_from_slice(&12i32.to_be_bytes());
        out.extend_from_slice(&0i32.to_be_bytes());
        out.extend_from_slice(&0i32.to_be_bytes());
        out.extend_from_slice(&PG_TEXT_OID.to_be_bytes());
        return Ok(());
    }
    let item_count = i32::try_from(items.len()).map_err(|_| CellExtractError::Fallback)?;
    let mut payload_len: usize = 20;
    for item in items {
        if i32::try_from(item.len()).is_err() {
            return Err(CellExtractError::Fallback);
        }
        payload_len = payload_len
            .checked_add(4)
            .and_then(|total| total.checked_add(item.len()))
            .ok_or(CellExtractError::Fallback)?;
    }
    let encoded_len = i32::try_from(payload_len).map_err(|_| CellExtractError::Fallback)?;
    out.extend_from_slice(&encoded_len.to_be_bytes());
    out.extend_from_slice(&1i32.to_be_bytes()); // ndim = 1
    out.extend_from_slice(&0i32.to_be_bytes()); // has_null = 0
    out.extend_from_slice(&PG_TEXT_OID.to_be_bytes()); // elem OID = TEXTOID
    out.extend_from_slice(&item_count.to_be_bytes()); // dim[0] length
    out.extend_from_slice(&1i32.to_be_bytes()); // dim[0] lower bound = 1
    for item in items {
        write_field(item.as_bytes(), out);
    }
    Ok(())
}

fn parse_payload_object(raw: &str) -> Result<Map<String, Value>, CellExtractError> {
    let parsed: Value = serde_json::from_str(raw).map_err(|_| CellExtractError::Fallback)?;
    match parsed {
        Value::Object(map) => Ok(map),
        Value::String(inner) => match serde_json::from_str::<Value>(&inner) {
            Ok(Value::Object(map)) => Ok(map),
            _ => Err(CellExtractError::Fallback),
        },
        _ => Err(CellExtractError::Fallback),
    }
}

fn encode_cell_row(row: &WitnessPageRow<'_>, source_id: &str) -> Result<Vec<u8>, CellExtractError> {
    let site_id = if row.resource_type == "Location" {
        Some(
            registry_location_uuid_bytes(source_id, row.resource_id)
                .map_err(CellExtractError::Hard)?,
        )
    } else {
        None
    };
    let map = parse_payload_object(row.payload_json)?;
    let mut npi: Option<&str> = None;
    let mut active: Option<bool> = None;
    let mut subject_ref: Option<&str> = None;
    let mut network_refs: Vec<&str> = Vec::new();
    let mut location_refs: Vec<&str> = Vec::new();
    let mut period_start: Option<&str> = None;
    let mut period_end: Option<&str> = None;
    let mut location_status: Option<&str> = None;
    let mut address_values: [Option<&str>; 6] = [None; 6];

    match row.resource_type {
        "Organization" | "Practitioner" => {
            npi = extract_opt_npi(&map)?;
            active = extract_opt_bool(&map, "active")?;
        }
        "Location" => {
            location_status = extract_opt_text(&map, "status")?;
            for (slot, key) in address_values.iter_mut().zip(ADDRESS_KEYS.iter()) {
                *slot = extract_opt_text(&map, key)?;
            }
        }
        "PractitionerRole" | "OrganizationAffiliation" => {
            npi = extract_opt_npi(&map)?;
            active = extract_opt_bool(&map, "active")?;
            let practitioner_ref = extract_opt_text(&map, "practitioner_ref")?;
            let participating_org_ref = extract_opt_text(&map, "participating_organization_ref")?;
            subject_ref = if row.resource_type == "PractitionerRole" {
                practitioner_ref
            } else {
                participating_org_ref
            };
            network_refs = extract_text_array(&map, "network_refs")?;
            location_refs = extract_text_array(&map, "location_refs")?;
            period_start = extract_opt_text(&map, "period_start")?;
            period_end = extract_opt_text(&map, "period_end")?;
        }
        _ => return Err(CellExtractError::Fallback),
    }

    let mut out = Vec::with_capacity(256);
    out.extend_from_slice(&CELL_FIELD_COUNT.to_be_bytes());
    write_opt_text(Some(row.resource_type), &mut out)?;
    write_opt_text(Some(row.resource_id), &mut out)?;
    write_opt_text(Some(row.payload_hash), &mut out)?;
    write_opt_text(npi, &mut out)?;
    write_opt_bool(active, &mut out);
    write_opt_text(subject_ref, &mut out)?;
    write_text_array(&network_refs, &mut out)?;
    write_text_array(&location_refs, &mut out)?;
    write_opt_text(period_start, &mut out)?;
    write_opt_text(period_end, &mut out)?;
    write_opt_text(location_status, &mut out)?;
    for value in address_values {
        write_opt_text(value, &mut out)?;
    }
    match site_id {
        Some(uuid) => write_field(&uuid, &mut out),
        None => out.extend_from_slice(&(-1i32).to_be_bytes()),
    }
    Ok(out)
}

/// Validate a typed witness page and emit a PostgreSQL binary COPY payload for `source_cells`.
///
/// Returns `Ok(None)` when any projected row needs the SQL fallback path (`PROJECT_SQL`), while
/// still raising witness-page or `invalid_site_id` errors directly.
pub fn prepare_typed_page_copy(
    rows: &[WitnessPageRow<'_>],
    options: &WitnessValidationOptions<'_>,
    source_id: &str,
    project_kinds: &BTreeSet<String>,
) -> Result<Option<PreparedTypedCopy>, &'static str> {
    let summary = validate_witness_page(rows, options)?;
    let projected: Vec<&WitnessPageRow<'_>> = rows
        .iter()
        .filter(|row| project_kinds.contains(row.resource_type))
        .collect();
    if projected.is_empty() {
        return Ok(Some((Vec::new(), 0, summary)));
    }
    let encoded_rows: Vec<Result<Vec<u8>, CellExtractError>> =
        if projected.len() >= PARALLEL_THRESHOLD {
            projected
                .par_iter()
                .map(|row| encode_cell_row(row, source_id))
                .collect()
        } else {
            projected
                .iter()
                .map(|row| encode_cell_row(row, source_id))
                .collect()
        };
    let mut total_bytes = COPY_HEADER.len() + 2;
    let mut row_buffers: Vec<Vec<u8>> = Vec::with_capacity(encoded_rows.len());
    for encoded in encoded_rows {
        match encoded {
            Ok(bytes) => {
                total_bytes += bytes.len();
                row_buffers.push(bytes);
            }
            Err(CellExtractError::Hard(code)) => return Err(code),
            Err(CellExtractError::Fallback) => return Ok(None),
        }
    }
    let mut copy_bytes = Vec::with_capacity(total_bytes);
    copy_bytes.extend_from_slice(COPY_HEADER);
    for row_buf in row_buffers {
        copy_bytes.extend_from_slice(&row_buf);
    }
    copy_bytes.extend_from_slice(&(-1i16).to_be_bytes());
    Ok(Some((copy_bytes, projected.len(), summary)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_options<'a>(
        dataset_id: &'a str,
        allowed: &'a BTreeSet<String>,
    ) -> WitnessValidationOptions<'a> {
        WitnessValidationOptions {
            dataset_id,
            allowed_kinds: Some(allowed),
            after: None,
            check_marker: true,
            completion_v3: false,
            validate_ids: true,
            validate_hashes: true,
            max_bytes: 8 * 1024 * 1024,
        }
    }

    #[test]
    fn encodes_valid_typed_cells_into_pg_binary_copy() {
        let hash = "a".repeat(64);
        let rows = [
            WitnessPageRow {
                dataset_id: "ds-1",
                resource_type: "HealthcareService",
                resource_id: "svc-1",
                payload_hash: &hash,
                payload_json: "{\"id\":\"svc-1\"}",
                acquired_resource_sha256: None,
            },
            WitnessPageRow {
                dataset_id: "ds-1",
                resource_type: "Location",
                resource_id: "loc-1",
                payload_hash: &hash,
                payload_json: "{\"status\":\"active\",\"first_line\":\"123 Main St\",\"city_name\":\"Austin\",\"state_name\":\"TX\",\"postal_code\":\"78701\"}",
                acquired_resource_sha256: None,
            },
            WitnessPageRow {
                dataset_id: "ds-1",
                resource_type: "PractitionerRole",
                resource_id: "role-1",
                payload_hash: &hash,
                payload_json: "{\"npi\":1234567893,\"active\":true,\"practitioner_ref\":\"Practitioner/p1\",\"network_refs\":[\"Organization/n1\"],\"location_refs\":[\"Location/loc-1\"],\"period_start\":\"2026-01-01\"}",
                acquired_resource_sha256: None,
            },
        ];
        let allowed = BTreeSet::from([
            "HealthcareService".to_owned(),
            "Location".to_owned(),
            "PractitionerRole".to_owned(),
        ]);
        let project = BTreeSet::from(["Location".to_owned(), "PractitionerRole".to_owned()]);
        let options = sample_options("ds-1", &allowed);
        let (copy_bytes, count, summary) =
            prepare_typed_page_copy(&rows, &options, "source-one", &project)
                .unwrap()
                .unwrap();
        assert_eq!(count, 2);
        assert_eq!(summary.last_type, "PractitionerRole");
        assert_eq!(summary.last_id, "role-1");
        assert!(copy_bytes.starts_with(COPY_HEADER));
        assert!(copy_bytes.ends_with(&(-1i16).to_be_bytes()));
    }

    #[test]
    fn falls_back_on_malformed_shape_or_float_npi_or_nul() {
        let hash = "a".repeat(64);
        let allowed = BTreeSet::from(["PractitionerRole".to_owned()]);
        let project = allowed.clone();
        let options = sample_options("ds-1", &allowed);
        for bad_payload in [
            "{\"active\":\"yes\"}",
            "{\"npi\":123.45}",
            "{\"network_refs\":[123]}",
            "{\"practitioner_ref\":\"bad\\u0000id\"}",
            "{\"participating_organization_ref\":42}",
        ] {
            let rows = [WitnessPageRow {
                dataset_id: "ds-1",
                resource_type: "PractitionerRole",
                resource_id: "role-1",
                payload_hash: &hash,
                payload_json: bad_payload,
                acquired_resource_sha256: None,
            }];
            assert!(
                prepare_typed_page_copy(&rows, &options, "source-one", &project)
                    .unwrap()
                    .is_none(),
                "expected fallback for {bad_payload}"
            );
        }
    }

    #[test]
    fn handles_double_encoded_json_string_payload() {
        let hash = "a".repeat(64);
        let allowed = BTreeSet::from(["Organization".to_owned()]);
        let project = allowed.clone();
        let options = sample_options("ds-1", &allowed);
        let rows = [WitnessPageRow {
            dataset_id: "ds-1",
            resource_type: "Organization",
            resource_id: "org-1",
            payload_hash: &hash,
            payload_json: "\"{\\\"npi\\\":\\\"1000000004\\\",\\\"active\\\":true}\"",
            acquired_resource_sha256: None,
        }];
        let (_, count, _) = prepare_typed_page_copy(&rows, &options, "source-one", &project)
            .unwrap()
            .unwrap();
        assert_eq!(count, 1);
    }
}
