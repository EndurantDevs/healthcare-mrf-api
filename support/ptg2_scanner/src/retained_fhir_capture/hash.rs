// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

use super::python_whitespace;
use crate::provider_directory_rows::{is_sha256_hex, stored_merged_role_hash};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

const HEX: &[u8; 16] = b"0123456789abcdef";

trait JsonSink {
    fn push_byte(&mut self, byte: u8);
    fn extend_bytes(&mut self, bytes: &[u8]);
}

impl JsonSink for Vec<u8> {
    #[inline]
    fn push_byte(&mut self, byte: u8) {
        self.push(byte);
    }

    #[inline]
    fn extend_bytes(&mut self, bytes: &[u8]) {
        self.extend_from_slice(bytes);
    }
}

struct Sha256Sink {
    hasher: Sha256,
    buffer: [u8; 4096],
    len: usize,
}

impl Sha256Sink {
    #[inline]
    fn new() -> Self {
        Self {
            hasher: Sha256::new(),
            buffer: [0u8; 4096],
            len: 0,
        }
    }

    #[inline]
    fn flush(&mut self) {
        if self.len != 0 {
            self.hasher.update(&self.buffer[..self.len]);
            self.len = 0;
        }
    }

    fn finish_hex(mut self) -> String {
        self.flush();
        let digest = self.hasher.finalize();
        let mut out = String::with_capacity(64);
        for byte in digest {
            out.push(char::from(HEX[usize::from(byte >> 4)]));
            out.push(char::from(HEX[usize::from(byte & 15)]));
        }
        out
    }
}

impl JsonSink for Sha256Sink {
    #[inline]
    fn push_byte(&mut self, byte: u8) {
        if self.len == self.buffer.len() {
            self.flush();
        }
        self.buffer[self.len] = byte;
        self.len += 1;
    }

    #[inline]
    fn extend_bytes(&mut self, bytes: &[u8]) {
        if bytes.len() <= self.buffer.len() - self.len {
            self.buffer[self.len..self.len + bytes.len()].copy_from_slice(bytes);
            self.len += bytes.len();
            return;
        }
        self.flush();
        if bytes.len() >= self.buffer.len() {
            self.hasher.update(bytes);
        } else {
            self.buffer[..bytes.len()].copy_from_slice(bytes);
            self.len = bytes.len();
        }
    }
}

fn sha256(value: &Value) -> String {
    let mut sink = Sha256Sink::new();
    write_json_to(value, true, true, &mut sink);
    sink.finish_hex()
}

fn sha256_object(value: &Map<String, Value>, spaced: bool, ascii: bool) -> String {
    let mut sink = Sha256Sink::new();
    write_object_to(value, spaced, ascii, &mut sink);
    sink.finish_hex()
}

pub(super) fn payload_hash(
    payload: &Map<String, Value>,
    raw_payload: &str,
    contract: &str,
    resource_type: &str,
    completion_v3: bool,
) -> std::result::Result<String, &'static str> {
    if let Some(hash) =
        merged_or_transport_bound_hash(payload, raw_payload, contract, resource_type)?
    {
        return Ok(hash);
    }
    payload_hash_content(payload.clone(), contract, resource_type, completion_v3)
}

pub(super) fn payload_hash_owned(
    payload: Map<String, Value>,
    raw_payload: &str,
    contract: &str,
    resource_type: &str,
    completion_v3: bool,
) -> std::result::Result<String, &'static str> {
    if let Some(hash) =
        merged_or_transport_bound_hash(&payload, raw_payload, contract, resource_type)?
    {
        return Ok(hash);
    }
    payload_hash_content(payload, contract, resource_type, completion_v3)
}

fn merged_or_transport_bound_hash(
    payload: &Map<String, Value>,
    raw_payload: &str,
    contract: &str,
    resource_type: &str,
) -> std::result::Result<Option<String>, &'static str> {
    if matches!(contract, "semantic_content_v3" | "semantic_content_v4")
        && resource_type == "PractitionerRole"
        && let Some(id) = payload
            .get("resource_id")
            .and_then(Value::as_str)
            .filter(|id| is_sha256_hex(id))
        && let Some(hash) =
            stored_merged_role_hash(id, raw_payload).map_err(|_| "python_semantics_required")?
    {
        return Ok(Some(hash));
    }
    if contract == "transport_bound_v1" {
        return Ok(Some(sha256_object(payload, true, true)));
    }
    Ok(None)
}

fn payload_hash_content(
    mut content: Map<String, Value>,
    contract: &str,
    resource_type: &str,
    completion_v3: bool,
) -> std::result::Result<String, &'static str> {
    for field in [
        "resource_url",
        "fhir_self_url",
        "fhir_fetch_url",
        "fhir_fetch_mode",
    ] {
        content.remove(field);
    }
    if completion_v3 {
        return Ok(sha256_object(&content, false, false));
    }
    if contract == "transport_neutral_v2" {
        return Ok(sha256_object(&content, true, true));
    }
    if let Some(Value::Object(metadata)) = content.get_mut("fhir_meta") {
        metadata.remove("lastUpdated");
        if metadata.is_empty() {
            content.insert("fhir_meta".into(), Value::Null);
        }
    }
    if contract == "semantic_content_v4" && resource_type == "Organization" {
        return organization_hash(content);
    }
    if ["names", "family_name", "given_names", "full_name"]
        .iter()
        .all(|field| content.contains_key(*field))
    {
        return practitioner_hash(content);
    }
    Ok(sha256_object(&content, true, true))
}

fn composed_hash(base: &Map<String, Value>, hashes: &BTreeSet<String>) -> String {
    let base_hash = sha256_object(base, true, true);
    let mut sink = Sha256Sink::new();
    sink.extend_bytes(b"{\"base_hash\": ");
    write_string_to(&base_hash, true, &mut sink);
    sink.extend_bytes(b", \"name_hashes\": [");
    for (index, hash) in hashes.iter().enumerate() {
        if index > 0 {
            sink.extend_bytes(b", ");
        }
        write_string_to(hash, true, &mut sink);
    }
    sink.extend_bytes(b"]}");
    sink.finish_hex()
}

fn practitioner_hash(mut payload: Map<String, Value>) -> std::result::Result<String, &'static str> {
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
    for field in ["names", "family_name", "given_names", "full_name"] {
        payload.remove(field);
    }
    let name_hashes: BTreeSet<String> = canonical.iter().map(sha256).collect();
    Ok(composed_hash(&payload, &name_hashes))
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

fn hash_kind_value(kind: &str, value: &str) -> String {
    let mut sink = Sha256Sink::new();
    sink.extend_bytes(b"{\"kind\": ");
    write_string_to(kind, true, &mut sink);
    sink.extend_bytes(b", \"value\": ");
    write_string_to(value, true, &mut sink);
    sink.push_byte(b'}');
    sink.finish_hex()
}

fn organization_hash(mut payload: Map<String, Value>) -> std::result::Result<String, &'static str> {
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
    for field in ["name", "aliases", "name_variants"] {
        payload.remove(field);
    }
    let hashes: BTreeSet<String> = variants
        .iter()
        .map(|name| hash_kind_value("name", name))
        .chain(labels.iter().map(|label| hash_kind_value("label", label)))
        .collect();
    Ok(composed_hash(&payload, &hashes))
}

pub(crate) fn json_bytes(value: &Value, spaced: bool, ascii: bool) -> Vec<u8> {
    let mut output = Vec::new();
    write_json_to(value, spaced, ascii, &mut output);
    output
}

fn write_json_to<S: JsonSink>(value: &Value, spaced: bool, ascii: bool, output: &mut S) {
    match value {
        Value::Null => output.extend_bytes(b"null"),
        Value::Bool(true) => output.extend_bytes(b"true"),
        Value::Bool(false) => output.extend_bytes(b"false"),
        Value::Number(number) => output.extend_bytes(
            if number.as_str() == "-0" {
                "0"
            } else {
                number.as_str()
            }
            .as_bytes(),
        ),
        Value::String(text) => write_string_to(text, ascii, output),
        Value::Array(values) => write_array_to(values, spaced, ascii, output),
        Value::Object(values) => write_object_to(values, spaced, ascii, output),
    }
}

pub(super) fn object_json_bytes(value: &Map<String, Value>, spaced: bool, ascii: bool) -> Vec<u8> {
    let mut output = Vec::new();
    write_object_to(value, spaced, ascii, &mut output);
    output
}

pub(super) fn write_array(values: &[Value], spaced: bool, ascii: bool, output: &mut Vec<u8>) {
    write_array_to(values, spaced, ascii, output);
}

fn write_array_to<S: JsonSink>(values: &[Value], spaced: bool, ascii: bool, output: &mut S) {
    output.push_byte(b'[');
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            output.extend_bytes(if spaced { b", " } else { b"," });
        }
        write_json_to(value, spaced, ascii, output);
    }
    output.push_byte(b']');
}

fn write_object_to<S: JsonSink>(
    values: &Map<String, Value>,
    spaced: bool,
    ascii: bool,
    output: &mut S,
) {
    output.push_byte(b'{');
    for (index, (key, value)) in values.iter().enumerate() {
        if index > 0 {
            output.extend_bytes(if spaced { b", " } else { b"," });
        }
        write_string_to(key, ascii, output);
        output.extend_bytes(if spaced { b": " } else { b":" });
        write_json_to(value, spaced, ascii, output);
    }
    output.push_byte(b'}');
}

pub(crate) fn write_string(text: &str, ascii: bool, output: &mut Vec<u8>) {
    write_string_to(text, ascii, output);
}

fn write_string_to<S: JsonSink>(text: &str, ascii: bool, output: &mut S) {
    output.push_byte(b'"');
    let bytes = text.as_bytes();
    let mut start = 0usize;
    let mut index = 0usize;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte >= 0x20 && byte != b'"' && byte != b'\\' && (!ascii || byte < 0x7f) {
            index += 1;
            continue;
        }
        if start < index {
            output.extend_bytes(&bytes[start..index]);
        }
        match byte {
            b'"' => {
                output.extend_bytes(br#"\""#);
                index += 1;
            }
            b'\\' => {
                output.extend_bytes(br#"\\"#);
                index += 1;
            }
            0x08 => {
                output.extend_bytes(br#"\b"#);
                index += 1;
            }
            0x0c => {
                output.extend_bytes(br#"\f"#);
                index += 1;
            }
            b'\n' => {
                output.extend_bytes(br#"\n"#);
                index += 1;
            }
            b'\r' => {
                output.extend_bytes(br#"\r"#);
                index += 1;
            }
            b'\t' => {
                output.extend_bytes(br#"\t"#);
                index += 1;
            }
            0x00..=0x1f | 0x7f => {
                let escaped = [
                    b'\\',
                    b'u',
                    b'0',
                    b'0',
                    HEX[usize::from(byte >> 4)],
                    HEX[usize::from(byte & 15)],
                ];
                output.extend_bytes(&escaped);
                index += 1;
            }
            _ => {
                let character = text[index..].chars().next().unwrap();
                let mut units = [0u16; 2];
                for unit in character.encode_utf16(&mut units) {
                    let escaped = [
                        b'\\',
                        b'u',
                        HEX[usize::from((*unit >> 12) & 15)],
                        HEX[usize::from((*unit >> 8) & 15)],
                        HEX[usize::from((*unit >> 4) & 15)],
                        HEX[usize::from(*unit & 15)],
                    ];
                    output.extend_bytes(&escaped);
                }
                index += character.len_utf8();
            }
        }
        start = index;
    }
    if start < bytes.len() {
        output.extend_bytes(&bytes[start..]);
    }
    output.push_byte(b'"');
}
