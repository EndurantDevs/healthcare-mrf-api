// Licensed under the HealthPorta Non-Commercial License (see LICENSE).
//! Shared FHIR witness-page validation, JSON byte bounding, UUIDv5 location identity,
//! typed projection serialization and legacy root folding.

use crate::retained_fhir_capture::hash::write_string;
use std::collections::{BTreeMap, BTreeSet};

pub const MAX_WITNESS_ROWS: usize = 1_000;

/// uuid5(NAMESPACE_URL, "urn:managed-network-registry:retained-fhir-location:v1")
const SITE_DOMAIN: [u8; 16] = [
    0xea, 0x83, 0xc5, 0x42, 0xbf, 0x27, 0x53, 0x1e, 0x9d, 0x0a, 0x64, 0x98, 0x06, 0x62, 0x55, 0xa8,
];

/// Fixed UTF-8 byte overhead of a compact 6-key witness row JSON object:
/// `{"acquired_resource_sha256":,"dataset_id":,"payload_hash":,"payload_json":,"resource_id":,"resource_type":}`
const WITNESS_ROW_JSON_OVERHEAD: usize = 107;

#[derive(Debug, Clone, Copy)]
pub struct WitnessPageRow<'a> {
    pub dataset_id: &'a str,
    pub resource_type: &'a str,
    pub resource_id: &'a str,
    pub payload_hash: &'a str,
    pub payload_json: &'a str,
    pub acquired_resource_sha256: Option<&'a str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WitnessPageSummary {
    pub last_type: String,
    pub last_id: String,
    pub counts: BTreeMap<String, usize>,
}

pub fn is_lower_hex_64(text: &str) -> bool {
    text.len() == 64
        && text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

pub fn is_fhir_id(text: &str) -> bool {
    (1..=64).contains(&text.len()) && id_bytes(text)
}

pub fn is_source_resource_id(text: &str) -> bool {
    (1..=256).contains(&text.len()) && id_bytes(text)
}

fn id_bytes(text: &str) -> bool {
    text.bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-'))
}

fn exact_source_text(text: &str, maximum: usize) -> bool {
    !text.is_empty()
        && text.len() <= maximum
        && text.trim() == text
        && !text.chars().any(char::is_control)
}

/// Exact UTF-8 byte length of `json.dumps(text, ensure_ascii=False)` for valid UTF-8 `text`.
#[inline]
pub fn json_str_utf8_len(text: &str) -> usize {
    let mut len = 2 + text.len();
    for &byte in text.as_bytes() {
        if byte < 0x20 {
            len += match byte {
                b'\x08' | b'\x0c' | b'\n' | b'\r' | b'\t' => 1,
                _ => 5,
            };
        } else if byte == b'"' || byte == b'\\' {
            len += 1;
        }
    }
    len
}

/// Exact UTF-8 byte length of one compact 6-field witness row dict in JSON.
#[inline]
pub fn witness_row_json_bytes(row: &WitnessPageRow<'_>) -> usize {
    WITNESS_ROW_JSON_OVERHEAD
        + json_str_utf8_len(row.dataset_id)
        + json_str_utf8_len(row.resource_type)
        + json_str_utf8_len(row.resource_id)
        + json_str_utf8_len(row.payload_hash)
        + json_str_utf8_len(row.payload_json)
        + row.acquired_resource_sha256.map_or(4, json_str_utf8_len)
}

/// Exact UTF-8 byte length of `json.dumps(rows, ensure_ascii=False, separators=(",", ":"))`.
pub fn witness_page_json_bytes(rows: &[WitnessPageRow<'_>]) -> usize {
    let mut total = 2usize;
    if rows.len() > 1 {
        total += rows.len() - 1;
    }
    for row in rows {
        total = total.saturating_add(witness_row_json_bytes(row));
    }
    total
}

/// RFC 3174 SHA-1 used exclusively for RFC 4122 UUIDv5 deterministic identifiers.
fn sha1_digest(data: &[u8]) -> [u8; 20] {
    let mut state: [u32; 5] = [
        0x6745_2301,
        0xefcd_ab89,
        0x98ba_dcfe,
        0x1032_5476,
        0xc3d2_e1f0,
    ];
    let bit_len = (data.len() as u64).wrapping_mul(8);
    let mut padded = Vec::with_capacity((data.len() + 9).next_multiple_of(64));
    padded.extend_from_slice(data);
    padded.push(0x80);
    while padded.len() % 64 != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_len.to_be_bytes());

    let mut w = [0u32; 80];
    for chunk in padded.as_chunks::<64>().0 {
        for (index, word) in chunk.as_chunks::<4>().0.iter().enumerate() {
            w[index] = u32::from_be_bytes(*word);
        }
        for index in 16..80 {
            w[index] = (w[index - 3] ^ w[index - 8] ^ w[index - 14] ^ w[index - 16]).rotate_left(1);
        }
        let [mut a, mut b, mut c, mut d, mut e] = state;
        for (index, &word) in w.iter().enumerate() {
            let (f, k) = match index {
                0..=19 => ((b & c) | ((!b) & d), 0x5a82_7999u32),
                20..=39 => (b ^ c ^ d, 0x6ed9_eba1u32),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8f1b_bcdcu32),
                _ => (b ^ c ^ d, 0xca62_c1d6u32),
            };
            let temp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(word);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = temp;
        }
        state[0] = state[0].wrapping_add(a);
        state[1] = state[1].wrapping_add(b);
        state[2] = state[2].wrapping_add(c);
        state[3] = state[3].wrapping_add(d);
        state[4] = state[4].wrapping_add(e);
    }

    let mut out = [0u8; 20];
    for (index, word) in state.iter().enumerate() {
        out[index * 4..index * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
    out
}

fn uuid5_bytes(namespace: &[u8; 16], name: &[u8]) -> [u8; 16] {
    let mut input = Vec::with_capacity(16 + name.len());
    input.extend_from_slice(namespace);
    input.extend_from_slice(name);
    let digest = sha1_digest(&input);
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    bytes
}

pub fn format_uuid(bytes: &[u8; 16]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(36);
    for (index, &byte) in bytes.iter().enumerate() {
        if matches!(index, 4 | 6 | 8 | 10) {
            out.push('-');
        }
        out.push(char::from(HEX[usize::from(byte >> 4)]));
        out.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    out
}

/// Deterministic registry Location UUID matching `registry_location_id(source_id, resource_id)`.
pub fn registry_location_uuid_bytes(
    source_id: &str,
    resource_id: &str,
) -> Result<[u8; 16], &'static str> {
    if !exact_source_text(source_id, 64) || !is_fhir_id(resource_id) {
        return Err("invalid_site_id");
    }
    let mut canonical = Vec::with_capacity(source_id.len() + resource_id.len() + 20);
    canonical.push(b'[');
    write_string(source_id, false, &mut canonical);
    canonical.extend_from_slice(b",\"Location\",");
    write_string(resource_id, false, &mut canonical);
    canonical.push(b']');
    Ok(uuid5_bytes(&SITE_DOMAIN, &canonical))
}

pub struct WitnessValidationOptions<'a> {
    pub dataset_id: &'a str,
    pub allowed_kinds: Option<&'a BTreeSet<String>>,
    pub after: Option<(&'a str, &'a str)>,
    pub check_marker: bool,
    pub completion_v3: bool,
    pub validate_ids: bool,
    pub validate_hashes: bool,
    pub max_bytes: usize,
}

/// Validate a bounded witness page (shape, byte budget, scope, marker, order, IDs/hashes).
pub fn validate_witness_page(
    rows: &[WitnessPageRow<'_>],
    options: &WitnessValidationOptions<'_>,
) -> Result<WitnessPageSummary, &'static str> {
    if rows.is_empty() || rows.len() > MAX_WITNESS_ROWS {
        return Err("page_bounds");
    }
    if witness_page_json_bytes(rows) > options.max_bytes {
        return Err("page_bounds");
    }
    let mut prev: Option<(&str, &str)> = options.after;
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for row in rows {
        if row.dataset_id != options.dataset_id {
            return Err("resource_scope");
        }
        if options.check_marker {
            let valid_marker = match (options.completion_v3, row.acquired_resource_sha256) {
                (true, Some(value)) => is_lower_hex_64(value),
                (false, None) => true,
                _ => false,
            };
            if !valid_marker {
                return Err("resource_scope");
            }
        }
        if let Some(allowed) = options.allowed_kinds
            && !allowed.contains(row.resource_type)
        {
            return Err("resource_identity");
        }
        if options.validate_ids {
            if !is_source_resource_id(row.resource_id) {
                return Err("resource_identity");
            }
        } else if !row.resource_type.is_ascii() || !row.resource_id.is_ascii() {
            return Err("resource_identity");
        }
        let cursor = (row.resource_type, row.resource_id);
        if let Some(previous) = prev
            && previous >= cursor
        {
            return Err("resource_order");
        }
        if options.validate_hashes && !is_lower_hex_64(row.payload_hash) {
            return Err("resource_hash");
        }
        *counts.entry(row.resource_type.to_owned()).or_insert(0) += 1;
        prev = Some(cursor);
    }
    let last = rows.last().unwrap();
    Ok(WitnessPageSummary {
        last_type: last.resource_type.to_owned(),
        last_id: last.resource_id.to_owned(),
        counts,
    })
}

/// Validate a typed witness page and emit compact projection JSON for `project_kinds` in one pass.
pub fn prepare_typed_page_json(
    rows: &[WitnessPageRow<'_>],
    options: &WitnessValidationOptions<'_>,
    source_id: &str,
    project_kinds: &BTreeSet<String>,
) -> Result<(String, usize, WitnessPageSummary), &'static str> {
    let summary = validate_witness_page(rows, options)?;
    let mut estimated_bytes = 2usize;
    let mut projected_count = 0usize;
    for row in rows {
        if project_kinds.contains(row.resource_type) {
            projected_count += 1;
            estimated_bytes = estimated_bytes
                .saturating_add(160 + row.resource_id.len() + row.payload_json.len());
        }
    }
    if projected_count == 0 {
        return Ok((String::new(), 0, summary));
    }
    let mut buffer = Vec::with_capacity(estimated_bytes);
    buffer.push(b'[');
    let mut emitted = 0usize;
    for row in rows {
        if !project_kinds.contains(row.resource_type) {
            continue;
        }
        if emitted > 0 {
            buffer.push(b',');
        }
        buffer.extend_from_slice(b"{\"resource_type\":");
        write_string(row.resource_type, false, &mut buffer);
        buffer.extend_from_slice(b",\"resource_id\":");
        write_string(row.resource_id, false, &mut buffer);
        buffer.extend_from_slice(b",\"payload_hash\":");
        write_string(row.payload_hash, false, &mut buffer);
        buffer.extend_from_slice(b",\"payload_json\":");
        write_string(row.payload_json, false, &mut buffer);
        buffer.extend_from_slice(b",\"site_id\":");
        if row.resource_type == "Location" {
            let uuid = registry_location_uuid_bytes(source_id, row.resource_id)?;
            let formatted = format_uuid(&uuid);
            buffer.push(b'"');
            buffer.extend_from_slice(formatted.as_bytes());
            buffer.push(b'"');
        } else {
            buffer.extend_from_slice(b"null");
        }
        buffer.push(b'}');
        emitted += 1;
    }
    buffer.push(b']');
    let encoded = String::from_utf8(buffer).map_err(|_| "utf8_invalid")?;
    Ok((encoded, emitted, summary))
}

pub type FoldedRootsPage = (Vec<u8>, BTreeMap<String, (Vec<u8>, usize)>);

/// Validate and serialize legacy stored-resource root triples for one bounded SQL page.
pub fn fold_stored_resource_roots_page(
    rows: &[(&str, &str, &str)],
    allowed_kinds: &BTreeSet<String>,
) -> Result<FoldedRootsPage, &'static str> {
    if rows.len() > MAX_WITNESS_ROWS {
        return Err("membership_page_bounds");
    }
    let mut root_bytes = Vec::with_capacity(rows.len() * 112);
    let mut families: BTreeMap<String, (Vec<u8>, usize)> = BTreeMap::new();
    let mut line = Vec::with_capacity(128);
    for (index, &(kind, identity, stored_hash)) in rows.iter().enumerate() {
        if !kind.is_ascii()
            || !allowed_kinds.contains(kind)
            || !is_fhir_id(identity)
            || !is_lower_hex_64(stored_hash)
        {
            return Err("membership_row_changed");
        }
        line.clear();
        line.push(b'[');
        write_string(kind, true, &mut line);
        line.push(b',');
        write_string(identity, true, &mut line);
        line.push(b',');
        write_string(stored_hash, true, &mut line);
        line.push(b']');
        if index > 0 {
            root_bytes.push(b'\n');
        }
        root_bytes.extend_from_slice(&line);
        let entry = families
            .entry(kind.to_owned())
            .or_insert_with(|| (Vec::new(), 0));
        if entry.1 > 0 {
            entry.0.push(b'\n');
        }
        entry.0.extend_from_slice(&line);
        entry.1 += 1;
    }
    Ok((root_bytes, families))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn site_domain_and_registry_location_uuid_match_python_uuid5() {
        // Verify SITE_DOMAIN itself matches uuid5(NAMESPACE_URL, ...).
        let namespace_url: [u8; 16] = [
            0x6b, 0xa7, 0xb8, 0x11, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4,
            0x30, 0xc8,
        ];
        let computed_domain = uuid5_bytes(
            &namespace_url,
            b"urn:managed-network-registry:retained-fhir-location:v1",
        );
        assert_eq!(computed_domain, SITE_DOMAIN);
        assert_eq!(
            format_uuid(&computed_domain),
            "ea83c542-bf27-531e-9d0a-6498066255a8"
        );
        let uuid = registry_location_uuid_bytes("source-one", "loc-1").unwrap();
        assert_eq!(format_uuid(&uuid).len(), 36);
        assert!(registry_location_uuid_bytes("source-one", "bad/id").is_err());
        assert!(registry_location_uuid_bytes(" padded ", "loc-1").is_err());
    }

    #[test]
    fn witness_row_byte_length_matches_compact_json_encoding() {
        let row = WitnessPageRow {
            dataset_id: "ds-1",
            resource_type: "Location",
            resource_id: "loc-1",
            payload_hash: &"a".repeat(64),
            payload_json: "{\"name\":\"O\\\"Brien\\n\\t\x00\x1f café 🚀\"}",
            acquired_resource_sha256: None,
        };
        let encoded = serde_json::json!([{
            "acquired_resource_sha256": serde_json::Value::Null,
            "dataset_id": row.dataset_id,
            "payload_hash": row.payload_hash,
            "payload_json": row.payload_json,
            "resource_id": row.resource_id,
            "resource_type": row.resource_type,
        }]);
        let bytes = crate::retained_fhir_capture::hash::json_bytes(&encoded, false, false);
        assert_eq!(witness_page_json_bytes(&[row]), bytes.len());
    }

    #[test]
    fn prepare_typed_page_validates_and_projects_in_one_pass() {
        let hash = "a".repeat(64);
        let rows = [
            WitnessPageRow {
                dataset_id: "ds-1",
                resource_type: "InsurancePlan",
                resource_id: "plan-1",
                payload_hash: &hash,
                payload_json: "{\"id\":\"plan-1\"}",
                acquired_resource_sha256: None,
            },
            WitnessPageRow {
                dataset_id: "ds-1",
                resource_type: "Location",
                resource_id: "loc-1",
                payload_hash: &hash,
                payload_json: "{\"id\":\"loc-1\"}",
                acquired_resource_sha256: None,
            },
        ];
        let allowed = BTreeSet::from(["InsurancePlan".to_owned(), "Location".to_owned()]);
        let project = BTreeSet::from(["Location".to_owned()]);
        let options = WitnessValidationOptions {
            dataset_id: "ds-1",
            allowed_kinds: Some(&allowed),
            after: None,
            check_marker: true,
            completion_v3: false,
            validate_ids: true,
            validate_hashes: true,
            max_bytes: 8 * 1024 * 1024,
        };
        let (json, count, summary) =
            prepare_typed_page_json(&rows, &options, "source-one", &project).unwrap();
        assert_eq!(count, 1);
        assert_eq!(summary.last_type, "Location");
        assert_eq!(summary.last_id, "loc-1");
        assert_eq!(summary.counts.get("InsurancePlan"), Some(&1));
        assert_eq!(summary.counts.get("Location"), Some(&1));
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0]["resource_type"], "Location");
        assert_eq!(
            parsed[0]["site_id"].as_str().unwrap(),
            format_uuid(&registry_location_uuid_bytes("source-one", "loc-1").unwrap())
        );
    }

    #[test]
    fn fold_stored_resource_roots_page_matches_json_triples() {
        let hash = "b".repeat(64);
        let allowed = BTreeSet::from(["Location".to_owned(), "Organization".to_owned()]);
        let rows = [
            ("Location", "loc-1", hash.as_str()),
            ("Organization", "org-1", hash.as_str()),
        ];
        let (root, families) = fold_stored_resource_roots_page(&rows, &allowed).unwrap();
        assert_eq!(
            String::from_utf8(root).unwrap(),
            format!("[\"Location\",\"loc-1\",\"{hash}\"]\n[\"Organization\",\"org-1\",\"{hash}\"]")
        );
        assert_eq!(families["Location"].1, 1);
        assert_eq!(families["Organization"].1, 1);
        assert!(fold_stored_resource_roots_page(
            &[("Location", "bad/id", hash.as_str())],
            &allowed
        )
        .is_err());
    }
}
