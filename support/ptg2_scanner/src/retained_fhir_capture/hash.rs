// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

use super::python_whitespace;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

fn digest(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    Sha256::digest(bytes)
        .iter()
        .flat_map(|byte| {
            [
                char::from(HEX[usize::from(byte >> 4)]),
                char::from(HEX[usize::from(byte & 15)]),
            ]
        })
        .collect()
}

fn sha256(value: &Value) -> String {
    let encoded = json_bytes(value, true, true);
    digest(&encoded)
}

pub(super) fn payload_hash(
    payload: &Map<String, Value>,
    contract: &str,
    resource_type: &str,
) -> std::result::Result<String, &'static str> {
    if contract == "transport_bound_v1" {
        return Ok(digest(&object_json_bytes(payload, true, true)));
    }
    let mut content = payload.clone();
    for field in [
        "resource_url",
        "fhir_self_url",
        "fhir_fetch_url",
        "fhir_fetch_mode",
    ] {
        content.remove(field);
    }
    if contract == "transport_neutral_v2" {
        return Ok(sha256(&Value::Object(content)));
    }
    if let Some(Value::Object(metadata)) = content.get_mut("fhir_meta") {
        metadata.remove("lastUpdated");
        if metadata.is_empty() {
            content.insert("fhir_meta".into(), Value::Null);
        }
    }
    if contract == "semantic_content_v4" && resource_type == "Organization" {
        return organization_hash(&content);
    }
    if ["names", "family_name", "given_names", "full_name"]
        .iter()
        .all(|field| content.contains_key(*field))
    {
        return practitioner_hash(&content);
    }
    Ok(sha256(&Value::Object(content)))
}

fn composed_hash(base: Map<String, Value>, hashes: BTreeSet<String>) -> String {
    sha256(&serde_json::json!({"base_hash": sha256(&Value::Object(base)), "name_hashes": hashes}))
}

fn practitioner_hash(payload: &Map<String, Value>) -> std::result::Result<String, &'static str> {
    let names = match &payload["names"] {
        Value::Null => &[][..],
        Value::Array(names) => names.as_slice(),
        _ => return Err("invalid_name_projection"),
    };
    let mut by_key = BTreeMap::new();
    for name in names {
        if !name.is_object() {
            return Err("invalid_name_projection");
        }
        by_key.insert(json_bytes(name, false, true), name.clone());
    }
    let canonical: Vec<Value> = by_key.into_values().collect();
    let primary = canonical.first();
    let family = primary
        .and_then(|name| name.get("family"))
        .cloned()
        .unwrap_or(Value::Null);
    let given = primary
        .and_then(|name| name.get("given"))
        .filter(|value| value.is_array())
        .cloned()
        .unwrap_or_else(|| Value::Array(Vec::new()));
    let full = match primary.and_then(|name| name.get("text")) {
        Some(value) if truthy(value) => value.clone(),
        _ => {
            let mut parts = given.as_array().unwrap().clone();
            if truthy(&family) {
                parts.push(family.clone());
            }
            let text = parts
                .iter()
                .map(|part| part.as_str().ok_or("python_semantics_required"))
                .collect::<std::result::Result<Vec<_>, _>>()?
                .join(" ");
            if text.is_empty() {
                Value::Null
            } else {
                Value::String(text)
            }
        }
    };
    for (field, expected) in [
        ("names", Value::Array(canonical.clone())),
        ("family_name", family),
        ("given_names", given),
        ("full_name", full),
    ] {
        if json_bytes(&payload[field], false, true) != json_bytes(&expected, false, true) {
            return Err("invalid_name_projection");
        }
    }
    let mut base = payload.clone();
    for field in ["names", "family_name", "given_names", "full_name"] {
        base.remove(field);
    }
    Ok(composed_hash(base, canonical.iter().map(sha256).collect()))
}

fn truthy(value: &Value) -> bool {
    match value {
        Value::Null | Value::Bool(false) => false,
        Value::String(value) => !value.is_empty(),
        Value::Array(value) => !value.is_empty(),
        Value::Object(value) => !value.is_empty(),
        Value::Number(value) => value.as_str() != "0" && value.as_str() != "-0",
        _ => true,
    }
}

fn organization_texts(
    value: Option<&Value>,
) -> std::result::Result<BTreeSet<String>, &'static str> {
    match value {
        None | Some(Value::Null) => Ok(BTreeSet::new()),
        Some(Value::Array(values)) => values
            .iter()
            .map(|value| {
                let text = value
                    .as_str()
                    .filter(|text| !text.trim_matches(python_whitespace).is_empty())
                    .ok_or("invalid_name_projection")?;
                // shortcut: full Python casefold ordering needs Unicode tables, use the reference path for non-ASCII labels.
                if !text.is_ascii() {
                    return Err("unsupported_unicode_semantics");
                }
                Ok(text.to_owned())
            })
            .collect(),
        _ => Err("invalid_name_projection"),
    }
}

fn organization_hash(payload: &Map<String, Value>) -> std::result::Result<String, &'static str> {
    if ["names", "family_name", "given_names", "full_name"]
        .iter()
        .all(|field| payload.contains_key(*field))
    {
        return Err("python_semantics_required");
    }
    let mut variants = organization_texts(payload.get("name_variants"))?;
    let name = match payload.get("name") {
        None | Some(Value::Null) => None,
        Some(Value::String(name)) if !name.trim().is_empty() => {
            if !name.is_ascii() {
                return Err("unsupported_unicode_semantics");
            }
            Some(name.clone())
        }
        _ => return Err("invalid_name_projection"),
    };
    if let Some(name) = name {
        variants.insert(name);
    }
    let labels: BTreeSet<String> = variants
        .union(&organization_texts(payload.get("aliases"))?)
        .cloned()
        .collect();
    let mut ordered: Vec<_> = variants.iter().cloned().collect();
    ordered.sort_by_cached_key(|text| {
        (
            text.to_ascii_lowercase(),
            text.bytes().filter(u8::is_ascii_uppercase).count(),
            text.clone(),
        )
    });
    let primary = ordered.first().cloned();
    let aliases: Vec<_> = labels
        .iter()
        .filter(|label| Some(*label) != primary.as_ref())
        .cloned()
        .collect();
    for (field, expected) in [
        ("name", serde_json::json!(primary)),
        ("aliases", serde_json::json!(aliases)),
        ("name_variants", serde_json::json!(ordered)),
    ] {
        let observed = payload.get(field).unwrap_or(&Value::Null);
        if json_bytes(observed, false, true) != json_bytes(&expected, false, true) {
            return Err("invalid_name_projection");
        }
    }
    let mut base = payload.clone();
    for field in ["name", "aliases", "name_variants"] {
        base.remove(field);
    }
    let hashes = variants
        .iter()
        .map(|name| sha256(&serde_json::json!({"kind": "name", "value": name})))
        .chain(
            labels
                .iter()
                .map(|label| sha256(&serde_json::json!({"kind": "label", "value": label}))),
        )
        .collect();
    Ok(composed_hash(base, hashes))
}

pub(crate) fn json_bytes(value: &Value, spaced: bool, ascii: bool) -> Vec<u8> {
    let mut output = Vec::new();
    write_json(value, spaced, ascii, &mut output);
    output
}

fn write_json(value: &Value, spaced: bool, ascii: bool, output: &mut Vec<u8>) {
    match value {
        Value::Null => output.extend_from_slice(b"null"),
        Value::Bool(true) => output.extend_from_slice(b"true"),
        Value::Bool(false) => output.extend_from_slice(b"false"),
        Value::Number(number) => output.extend_from_slice(
            if number.as_str() == "-0" {
                "0"
            } else {
                number.as_str()
            }
            .as_bytes(),
        ),
        Value::String(text) => write_string(text, ascii, output),
        Value::Array(values) => write_array(values, spaced, ascii, output),
        Value::Object(values) => write_object(values, spaced, ascii, output),
    }
}

pub(super) fn object_json_bytes(value: &Map<String, Value>, spaced: bool, ascii: bool) -> Vec<u8> {
    let mut output = Vec::new();
    write_object(value, spaced, ascii, &mut output);
    output
}

pub(super) fn write_array(values: &[Value], spaced: bool, ascii: bool, output: &mut Vec<u8>) {
    output.push(b'[');
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            output.extend_from_slice(if spaced { b", " } else { b"," });
        }
        write_json(value, spaced, ascii, output);
    }
    output.push(b']');
}

fn write_object(values: &Map<String, Value>, spaced: bool, ascii: bool, output: &mut Vec<u8>) {
    output.push(b'{');
    for (index, (key, value)) in values.iter().enumerate() {
        if index > 0 {
            output.extend_from_slice(if spaced { b", " } else { b"," });
        }
        write_string(key, ascii, output);
        output.extend_from_slice(if spaced { b": " } else { b":" });
        write_json(value, spaced, ascii, output);
    }
    output.push(b'}');
}

fn write_string(text: &str, ascii: bool, output: &mut Vec<u8>) {
    output.push(b'"');
    for character in text.chars() {
        match character {
            '"' => output.extend_from_slice(br#"\""#),
            '\\' => output.extend_from_slice(br#"\\"#),
            '\u{0008}' => output.extend_from_slice(br#"\b"#),
            '\u{000c}' => output.extend_from_slice(br#"\f"#),
            '\n' => output.extend_from_slice(br#"\n"#),
            '\r' => output.extend_from_slice(br#"\r"#),
            '\t' => output.extend_from_slice(br#"\t"#),
            character
                if character <= '\u{001f}' || (ascii && !(' '..='~').contains(&character)) =>
            {
                let mut units = [0; 2];
                for unit in character.encode_utf16(&mut units) {
                    output.extend_from_slice(b"\\u");
                    for shift in [12, 8, 4, 0] {
                        output.push(b"0123456789abcdef"[usize::from((*unit >> shift) & 15)]);
                    }
                }
            }
            character => {
                let mut bytes = [0; 4];
                output.extend_from_slice(character.encode_utf8(&mut bytes).as_bytes());
            }
        }
    }
    output.push(b'"');
}
