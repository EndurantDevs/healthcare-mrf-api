// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

use crate::retained_fhir_capture::hash::json_bytes;
use serde_json::{Map, Value};

pub(crate) type SourceRow<'a> = (&'a str, &'a str, &'a str, &'a str);

fn supported(value: &Value, int_max_digits: usize) -> bool {
    match value {
        // shortcut: Python controls float spelling and parse limits, use its reference path for these pages.
        Value::Number(number) => {
            let text = number.as_str();
            !text.contains(['.', 'e', 'E'])
                && (int_max_digits == 0 || text.trim_start_matches('-').len() <= int_max_digits)
        }
        Value::Array(values) => values.iter().all(|value| supported(value, int_max_digits)),
        Value::Object(values) => values
            .values()
            .all(|value| supported(value, int_max_digits)),
        _ => true,
    }
}

fn supported_raw(raw: &str) -> bool {
    let (mut quoted, mut escaped, mut items, mut previous) = (false, false, 0, 0u8);
    for byte in raw.bytes() {
        if !quoted {
            if byte == b'.' || (matches!(byte, b'e' | b'E') && previous.is_ascii_digit()) {
                return false;
            }
            if matches!(byte, b',' | b':') {
                items += 1;
                if items > 100_000 {
                    return false;
                }
            }
        }
        if !escaped && byte == b'"' {
            quoted = !quoted;
        }
        escaped = !escaped && byte == b'\\';
        previous = byte;
    }
    true
}

pub(crate) fn encode_page(
    rows: &[SourceRow<'_>],
    page_bytes: usize,
    max_references: usize,
    int_max_digits: usize,
) -> Option<Vec<Vec<u8>>> {
    if rows.is_empty() || rows.len() > 1_000 || page_bytes > 32 * 1024 * 1024 {
        return None;
    }
    let mut encoded = Vec::with_capacity(rows.len());
    let mut total_bytes = 1 + rows.len();
    for &(resource_type, resource_id, payload_hash, raw) in rows {
        if !matches!(
            resource_type,
            "InsurancePlan" | "Organization" | "OrganizationAffiliation" | "PractitionerRole"
        ) || resource_id.is_empty()
            || resource_id.len() > 64
            || !resource_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-'))
            || payload_hash.len() != 64
            || !payload_hash
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || raw.len() > 32 * 1024 * 1024
            // Escaped Unicode can spell serde arbitrary-precision marker keys too.
            || raw.contains("$serde_json::private::Number")
            || raw.contains(r"\u")
            || !supported_raw(raw)
        {
            return None;
        }
    }
    for &(resource_type, resource_id, payload_hash, raw) in rows {
        // Parse failures and invalid inputs return to Python for its exact rejection and diagnostic order.
        let payload: Value = serde_json::from_str(raw).ok()?;
        let object = payload.as_object()?;
        if !supported(&payload, int_max_digits) {
            return None;
        }
        if matches!(
            resource_type,
            "OrganizationAffiliation" | "PractitionerRole"
        ) {
            if let Some(references) = object.get("network_refs").filter(|value| !value.is_null()) {
                let references = references.as_array()?;
                if references.len() > max_references
                    || references.iter().any(|reference| {
                        !reference.is_null()
                            && reference.as_str().is_none_or(|text| {
                                text.len() > 2048
                                    || text
                                        .chars()
                                        .any(|character| character < ' ' || character == '\u{007f}')
                            })
                    })
                {
                    return None;
                }
            }
        }
        let row = Value::Object(Map::from_iter([
            ("resource_type".into(), Value::String(resource_type.into())),
            ("resource_id".into(), Value::String(resource_id.into())),
            ("payload_hash".into(), Value::String(payload_hash.into())),
            ("payload_json".into(), payload),
        ]));
        let bytes = json_bytes(&row, false, false);
        total_bytes = total_bytes.checked_add(bytes.len())?;
        if total_bytes > page_bytes {
            return None;
        }
        encoded.push(bytes);
    }
    Some(encoded)
}
