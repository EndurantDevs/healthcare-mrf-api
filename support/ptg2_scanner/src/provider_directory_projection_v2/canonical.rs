// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

use super::contracts::ProjectedResourceRow;
use serde_json::Value;
use sha2::{Digest, Sha256};

pub fn canonical_row_bytes(row: &ProjectedResourceRow) -> Vec<u8> {
    let row_value = Value::Array(vec![
        Value::String(row.resource_type.clone()),
        Value::String(row.resource_id.clone()),
        Value::String(row.payload_hash.clone()),
        Value::String(row.source_rank.clone()),
        row.summary_npi.map(Value::from).unwrap_or(Value::Null),
        Value::from(row.summary_address_count),
        Value::Bool(row.summary_addressed_location),
        Value::Bool(row.summary_geocoded_location),
        Value::from(row.summary_network_link_count),
        Value::from(row.summary_affiliation_link_count),
        Value::String(row.semantic_evidence_sha256.clone()),
    ]);
    python_stable_json(&row_value)
}

pub fn stable_hash(value: &Value, domain: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(domain.as_bytes());
    digest.update(b"\0");
    digest.update(python_stable_json(value));
    hex_digest(digest)
}

pub fn sha256_bytes(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    hex_digest(digest)
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

pub fn python_stable_json(value: &Value) -> Vec<u8> {
    let mut encoded = Vec::new();
    encode_python_json(value, &mut encoded);
    encoded
}

fn encode_python_json(value: &Value, output: &mut Vec<u8>) {
    match value {
        Value::Null => output.extend_from_slice(b"null"),
        Value::Bool(true) => output.extend_from_slice(b"true"),
        Value::Bool(false) => output.extend_from_slice(b"false"),
        Value::Number(number) => output.extend_from_slice(number.to_string().as_bytes()),
        Value::String(text) => encode_python_string(text, output),
        Value::Array(entries) => encode_array(entries, output),
        Value::Object(object) => {
            output.push(b'{');
            for (ordinal, (key, entry)) in object.iter().enumerate() {
                if ordinal != 0 {
                    output.push(b',');
                }
                encode_python_string(key, output);
                output.push(b':');
                encode_python_json(entry, output);
            }
            output.push(b'}');
        }
    }
}

fn encode_array(entries: &[Value], output: &mut Vec<u8>) {
    output.push(b'[');
    for (ordinal, entry) in entries.iter().enumerate() {
        if ordinal != 0 {
            output.push(b',');
        }
        encode_python_json(entry, output);
    }
    output.push(b']');
}

fn encode_python_string(text: &str, output: &mut Vec<u8>) {
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
            character if character <= '\u{001f}' => write_unicode_escape(character as u16, output),
            character if character.is_ascii() => output.push(character as u8),
            character if (character as u32) <= 0xffff => {
                write_unicode_escape(character as u16, output)
            }
            character => {
                let scalar = character as u32 - 0x1_0000;
                write_unicode_escape((0xd800 + (scalar >> 10)) as u16, output);
                write_unicode_escape((0xdc00 + (scalar & 0x3ff)) as u16, output);
            }
        }
    }
    output.push(b'"');
}

fn write_unicode_escape(value: u16, output: &mut Vec<u8>) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    output.extend_from_slice(b"\\u");
    for shift in [12, 8, 4, 0] {
        output.push(HEX[usize::from((value >> shift) & 0xf)]);
    }
}

#[cfg(test)]
pub(super) fn test_python_stable_json(value: &Value) -> Vec<u8> {
    python_stable_json(value)
}
