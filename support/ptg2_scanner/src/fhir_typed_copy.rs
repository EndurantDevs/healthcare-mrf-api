// Licensed under the HealthPorta Non-Commercial License (see LICENSE).
//! Parallel Rust extraction and PostgreSQL binary COPY encoding for typed FHIR `source_cells`.
//!
//! Uses a zero-copy borrowing `serde::Deserialize` visitor that skips unused FHIR metadata
//! fields without building a `serde_json::Value` DOM. Any row whose payload fails the exact
//! `network_fhir_typed_sql.py` shape contract (or uses float `npi`, NUL bytes, or unsupported
//! JSON) causes the page to return `Ok(None)` so the caller falls back to `PROJECT_SQL` with
//! identical PostgreSQL semantics and error codes.

use crate::fhir_witness_page::{
    registry_location_uuid_bytes, validate_witness_page, WitnessPageRow, WitnessPageSummary,
    WitnessValidationOptions,
};
use crate::network_membership_codec::{write_field, COPY_HEADER};
use rayon::prelude::*;
use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde_json::value::RawValue;
use std::borrow::Cow;
use std::collections::BTreeSet;
use std::fmt;

const CELL_FIELD_COUNT: i16 = 18;
const PG_TEXT_OID: i32 = 25;
const PARALLEL_THRESHOLD: usize = 16;

enum CellExtractError {
    Fallback,
    Hard(&'static str),
}

#[derive(Clone, Copy)]
enum CellKind {
    Entity,
    Location,
    PractitionerRole,
    OrganizationAffiliation,
}

#[derive(Default)]
struct TypedCellFields<'de> {
    npi: Option<Cow<'de, str>>,
    active: Option<bool>,
    practitioner_ref: Option<Cow<'de, str>>,
    participating_organization_ref: Option<Cow<'de, str>>,
    network_refs: Vec<Cow<'de, str>>,
    location_refs: Vec<Cow<'de, str>>,
    period_start: Option<Cow<'de, str>>,
    period_end: Option<Cow<'de, str>>,
    status: Option<Cow<'de, str>>,
    address: [Option<Cow<'de, str>>; 6],
}

fn is_pg_exact_int(text: &str) -> bool {
    if text == "-0" || text.is_empty() {
        return false;
    }
    let digits = text.strip_prefix('-').unwrap_or(text);
    !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
}

#[inline]
fn has_nul(text: &str) -> bool {
    text.as_bytes().contains(&0)
}

struct OptTextSeed;

impl<'de> DeserializeSeed<'de> for OptTextSeed {
    type Value = Option<Cow<'de, str>>;

    fn deserialize<D: de::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        struct OptTextVisitor;
        impl<'de> Visitor<'de> for OptTextVisitor {
            type Value = Option<Cow<'de, str>>;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("null or NUL-free JSON string")
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(None)
            }
            fn visit_borrowed_str<E: de::Error>(self, text: &'de str) -> Result<Self::Value, E> {
                if has_nul(text) {
                    return Err(E::custom("nul"));
                }
                Ok(Some(Cow::Borrowed(text)))
            }
            fn visit_str<E: de::Error>(self, text: &str) -> Result<Self::Value, E> {
                if has_nul(text) {
                    return Err(E::custom("nul"));
                }
                Ok(Some(Cow::Owned(text.to_owned())))
            }
            fn visit_string<E: de::Error>(self, text: String) -> Result<Self::Value, E> {
                if has_nul(&text) {
                    return Err(E::custom("nul"));
                }
                Ok(Some(Cow::Owned(text)))
            }
        }
        deserializer.deserialize_any(OptTextVisitor)
    }
}

struct OptBoolSeed;

impl<'de> DeserializeSeed<'de> for OptBoolSeed {
    type Value = Option<bool>;

    fn deserialize<D: de::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        struct OptBoolVisitor;
        impl Visitor<'_> for OptBoolVisitor {
            type Value = Option<bool>;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("null or boolean")
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(None)
            }
            fn visit_bool<E: de::Error>(self, flag: bool) -> Result<Self::Value, E> {
                Ok(Some(flag))
            }
        }
        deserializer.deserialize_any(OptBoolVisitor)
    }
}

struct OptTextArraySeed;

impl<'de> DeserializeSeed<'de> for OptTextArraySeed {
    type Value = Vec<Cow<'de, str>>;

    fn deserialize<D: de::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        struct OptTextArrayVisitor;
        impl<'de> Visitor<'de> for OptTextArrayVisitor {
            type Value = Vec<Cow<'de, str>>;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("null or array of NUL-free strings")
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(Vec::new())
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut items = Vec::with_capacity(seq.size_hint().unwrap_or(0).min(64));
                while let Some(item) = seq.next_element_seed(TextItemSeed)? {
                    items.push(item);
                }
                Ok(items)
            }
        }
        deserializer.deserialize_any(OptTextArrayVisitor)
    }
}

struct TextItemSeed;

impl<'de> DeserializeSeed<'de> for TextItemSeed {
    type Value = Cow<'de, str>;

    fn deserialize<D: de::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        struct TextItemVisitor;
        impl<'de> Visitor<'de> for TextItemVisitor {
            type Value = Cow<'de, str>;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("NUL-free JSON string")
            }
            fn visit_borrowed_str<E: de::Error>(self, text: &'de str) -> Result<Self::Value, E> {
                if has_nul(text) {
                    return Err(E::custom("nul"));
                }
                Ok(Cow::Borrowed(text))
            }
            fn visit_str<E: de::Error>(self, text: &str) -> Result<Self::Value, E> {
                if has_nul(text) {
                    return Err(E::custom("nul"));
                }
                Ok(Cow::Owned(text.to_owned()))
            }
            fn visit_string<E: de::Error>(self, text: String) -> Result<Self::Value, E> {
                if has_nul(&text) {
                    return Err(E::custom("nul"));
                }
                Ok(Cow::Owned(text))
            }
        }
        deserializer.deserialize_str(TextItemVisitor)
    }
}

fn parse_raw_npi<'de, E: de::Error>(raw: &'de RawValue) -> Result<Option<Cow<'de, str>>, E> {
    let token = raw.get();
    if token == "null" {
        return Ok(None);
    }
    if token.starts_with('"') {
        if !token.as_bytes().contains(&b'\\') {
            let inner = &token[1..token.len() - 1];
            if has_nul(inner) {
                return Err(E::custom("nul"));
            }
            return Ok(Some(Cow::Borrowed(inner)));
        }
        let decoded: String = serde_json::from_str(token).map_err(E::custom)?;
        if has_nul(&decoded) {
            return Err(E::custom("nul"));
        }
        return Ok(Some(Cow::Owned(decoded)));
    }
    if is_pg_exact_int(token) {
        return Ok(Some(Cow::Borrowed(token)));
    }
    Err(E::custom("invalid_npi"))
}

struct TypedCellFieldsSeed {
    kind: CellKind,
}

impl<'de> DeserializeSeed<'de> for TypedCellFieldsSeed {
    type Value = TypedCellFields<'de>;

    fn deserialize<D: de::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        struct FieldsVisitor {
            kind: CellKind,
        }
        impl<'de> Visitor<'de> for FieldsVisitor {
            type Value = TypedCellFields<'de>;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("FHIR resource payload object")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut fields = TypedCellFields::default();
                while let Some(key) = map.next_key::<Cow<'de, str>>()? {
                    match (self.kind, key.as_ref()) {
                        (
                            CellKind::Entity
                            | CellKind::PractitionerRole
                            | CellKind::OrganizationAffiliation,
                            "npi",
                        ) => {
                            let raw: &'de RawValue = map.next_value()?;
                            fields.npi = parse_raw_npi(raw)?;
                        }
                        (
                            CellKind::Entity
                            | CellKind::PractitionerRole
                            | CellKind::OrganizationAffiliation,
                            "active",
                        ) => {
                            fields.active = map.next_value_seed(OptBoolSeed)?;
                        }
                        (CellKind::Location, "status") => {
                            fields.status = map.next_value_seed(OptTextSeed)?;
                        }
                        (CellKind::Location, "first_line") => {
                            fields.address[0] = map.next_value_seed(OptTextSeed)?;
                        }
                        (CellKind::Location, "second_line") => {
                            fields.address[1] = map.next_value_seed(OptTextSeed)?;
                        }
                        (CellKind::Location, "city_name") => {
                            fields.address[2] = map.next_value_seed(OptTextSeed)?;
                        }
                        (CellKind::Location, "state_name") => {
                            fields.address[3] = map.next_value_seed(OptTextSeed)?;
                        }
                        (CellKind::Location, "postal_code") => {
                            fields.address[4] = map.next_value_seed(OptTextSeed)?;
                        }
                        (CellKind::Location, "country_code") => {
                            fields.address[5] = map.next_value_seed(OptTextSeed)?;
                        }
                        (
                            CellKind::PractitionerRole | CellKind::OrganizationAffiliation,
                            "practitioner_ref",
                        ) => {
                            fields.practitioner_ref = map.next_value_seed(OptTextSeed)?;
                        }
                        (
                            CellKind::PractitionerRole | CellKind::OrganizationAffiliation,
                            "participating_organization_ref",
                        ) => {
                            fields.participating_organization_ref =
                                map.next_value_seed(OptTextSeed)?;
                        }
                        (
                            CellKind::PractitionerRole | CellKind::OrganizationAffiliation,
                            "network_refs",
                        ) => {
                            fields.network_refs = map.next_value_seed(OptTextArraySeed)?;
                        }
                        (
                            CellKind::PractitionerRole | CellKind::OrganizationAffiliation,
                            "location_refs",
                        ) => {
                            fields.location_refs = map.next_value_seed(OptTextArraySeed)?;
                        }
                        (
                            CellKind::PractitionerRole | CellKind::OrganizationAffiliation,
                            "period_start",
                        ) => {
                            fields.period_start = map.next_value_seed(OptTextSeed)?;
                        }
                        (
                            CellKind::PractitionerRole | CellKind::OrganizationAffiliation,
                            "period_end",
                        ) => {
                            fields.period_end = map.next_value_seed(OptTextSeed)?;
                        }
                        _ => {
                            let _ = map.next_value::<IgnoredAny>()?;
                        }
                    }
                }
                Ok(fields)
            }
        }
        deserializer.deserialize_map(FieldsVisitor { kind: self.kind })
    }
}

pub type PreparedTypedCopy = (Vec<u8>, usize, WitnessPageSummary);

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

fn write_text_array(items: &[Cow<'_, str>], out: &mut Vec<u8>) -> Result<(), CellExtractError> {
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

fn deserialize_object_fields<'de>(
    raw: &'de str,
    kind: CellKind,
) -> Result<TypedCellFields<'de>, CellExtractError> {
    let mut deserializer = serde_json::Deserializer::from_str(raw);
    let fields = TypedCellFieldsSeed { kind }
        .deserialize(&mut deserializer)
        .map_err(|_| CellExtractError::Fallback)?;
    deserializer.end().map_err(|_| CellExtractError::Fallback)?;
    Ok(fields)
}

fn encode_cell_fields(
    row: &WitnessPageRow<'_>,
    kind: CellKind,
    site_id: Option<[u8; 16]>,
    fields: &TypedCellFields<'_>,
) -> Result<Vec<u8>, CellExtractError> {
    let subject_ref = match kind {
        CellKind::PractitionerRole => fields.practitioner_ref.as_deref(),
        CellKind::OrganizationAffiliation => fields.participating_organization_ref.as_deref(),
        CellKind::Entity | CellKind::Location => None,
    };
    let mut out = Vec::with_capacity(256);
    out.extend_from_slice(&CELL_FIELD_COUNT.to_be_bytes());
    write_opt_text(Some(row.resource_type), &mut out)?;
    write_opt_text(Some(row.resource_id), &mut out)?;
    write_opt_text(Some(row.payload_hash), &mut out)?;
    write_opt_text(fields.npi.as_deref(), &mut out)?;
    write_opt_bool(fields.active, &mut out);
    write_opt_text(subject_ref, &mut out)?;
    write_text_array(&fields.network_refs, &mut out)?;
    write_text_array(&fields.location_refs, &mut out)?;
    write_opt_text(fields.period_start.as_deref(), &mut out)?;
    write_opt_text(fields.period_end.as_deref(), &mut out)?;
    write_opt_text(fields.status.as_deref(), &mut out)?;
    for value in &fields.address {
        write_opt_text(value.as_deref(), &mut out)?;
    }
    match site_id {
        Some(uuid) => write_field(&uuid, &mut out),
        None => out.extend_from_slice(&(-1i32).to_be_bytes()),
    }
    Ok(out)
}

fn encode_cell_row(row: &WitnessPageRow<'_>, source_id: &str) -> Result<Vec<u8>, CellExtractError> {
    let kind = match row.resource_type {
        "Organization" | "Practitioner" => CellKind::Entity,
        "Location" => CellKind::Location,
        "PractitionerRole" => CellKind::PractitionerRole,
        "OrganizationAffiliation" => CellKind::OrganizationAffiliation,
        _ => return Err(CellExtractError::Fallback),
    };
    let site_id = if matches!(kind, CellKind::Location) {
        Some(
            registry_location_uuid_bytes(source_id, row.resource_id)
                .map_err(CellExtractError::Hard)?,
        )
    } else {
        None
    };
    let trimmed = row.payload_json.trim_start();
    if trimmed.starts_with('{') {
        let fields = deserialize_object_fields(trimmed, kind)?;
        return encode_cell_fields(row, kind, site_id, &fields);
    }
    Err(CellExtractError::Fallback)
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
                payload_json: "{\"status\":\"active\",\"first_line\":\"123 \\\"Main\\\" St\",\"city_name\":\"Austin\",\"state_name\":\"TX\",\"postal_code\":\"78701\",\"unused_nested\":{\"a\":[1,2,3]}}",
                acquired_resource_sha256: None,
            },
            WitnessPageRow {
                dataset_id: "ds-1",
                resource_type: "PractitionerRole",
                resource_id: "role-1",
                payload_hash: &hash,
                payload_json: "{\"npi\":1234567893,\"active\":true,\"practitioner_ref\":\"Practitioner/p1\",\"network_refs\":[\"Organization/n1\"],\"location_refs\":[\"Location/loc-1\"],\"period_start\":\"2026-01-01\",\"extra_blob\":{\"nested\":true}}",
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
            "{\"npi\":-0}",
            "{\"npi\":\"123\\u00004\"}",
            "{\"network_refs\":[123]}",
            "{\"practitioner_ref\":\"bad\\u0000id\"}",
            "{\"participating_organization_ref\":42}",
            "{\"npi\":\"1000000004\"} trailing",
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
    fn falls_back_on_double_encoded_json_string_payload() {
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
        assert!(
            prepare_typed_page_copy(&rows, &options, "source-one", &project)
                .unwrap()
                .is_none()
        );
    }
}
