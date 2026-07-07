//! Canonicalization and deterministic hash helpers for scanner records.

use serde_json::{json, Map, Value};
use xxhash_rust::xxh3::Xxh3;

const HASH_MASK_63: u64 = (1u64 << 63) - 1;
const LOWER_HEX: &[u8; 16] = b"0123456789abcdef";

pub fn hash_text_key(key: &str) -> u64 {
    u64::from_str_radix(key, 16).unwrap_or_else(|_| xxh3_63(key.as_bytes()))
}

pub fn provider_group_member_key(group_hash: i64, npi: i64) -> u128 {
    ((group_hash as u64 as u128) << 64) | (npi as u64 as u128)
}

pub fn provider_set_entry_key(provider_set_hash: &str, provider_entry_hash: i64) -> u128 {
    ((hash_text_key(provider_set_hash) as u128) << 64) | (provider_entry_hash as u64 as u128)
}

pub fn provider_set_component_key(provider_set_hash: &str, provider_group_hash: i64) -> u128 {
    ((hash_text_key(provider_set_hash) as u128) << 64) | (provider_group_hash as u64 as u128)
}

pub fn price_set_entry_key(price_set_hash: &str, price_atom_hash: &str) -> u128 {
    ((hash_text_key(price_set_hash) as u128) << 64) | (hash_text_key(price_atom_hash) as u128)
}

pub fn provider_entry_component_key(provider_entry_hash: i64, provider_group_hash: i64) -> u128 {
    ((provider_entry_hash as u64 as u128) << 64) | (provider_group_hash as u64 as u128)
}

pub fn shard_for_u64(key: u64, shard_count: usize) -> usize {
    (key as usize) % shard_count.max(1)
}

pub fn shard_for_u128(key: u128, shard_count: usize) -> usize {
    ((key ^ (key >> 64)) as usize) % shard_count.max(1)
}

pub fn canonical_value(value: &Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.iter().map(canonical_value).collect()),
        Value::Object(map) => {
            let mut sorted = Map::new();
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            for key in keys {
                sorted.insert(key.clone(), canonical_value(&map[key]));
            }
            Value::Object(sorted)
        }
        _ => value.clone(),
    }
}

pub fn canonical_json(value: &Value) -> String {
    serde_json::to_string(&canonical_value(value)).unwrap_or_else(|_| "null".to_string())
}

pub fn sha63_hex_from_json(value: &Value) -> String {
    let value = xxh3_63(canonical_json(value).as_bytes());
    hex_u64_16(value)
}

pub fn make_checksum(values: Vec<Value>) -> i64 {
    xxh3_63(canonical_json(&Value::Array(values)).as_bytes()) as i64
}

pub fn semantic_hash(domain: &str, payload: Value) -> String {
    sha63_hex_from_json(&json!({"domain": domain, "payload": payload}))
}

fn update_hash_field(hasher: &mut Xxh3, value: &[u8]) {
    hasher.update(b"\x1f");
    update_hash_usize(hasher, value.len());
    hasher.update(b":");
    hasher.update(value);
}

fn update_hash_usize(hasher: &mut Xxh3, value: usize) {
    let mut buffer = itoa::Buffer::new();
    hasher.update(buffer.format(value).as_bytes());
}

fn update_hash_usize_field(hasher: &mut Xxh3, value: usize) {
    let mut buffer = itoa::Buffer::new();
    update_hash_field(hasher, buffer.format(value).as_bytes());
}

fn update_hash_i64_field(hasher: &mut Xxh3, value: i64) {
    let mut buffer = itoa::Buffer::new();
    update_hash_field(hasher, buffer.format(value).as_bytes());
}

pub fn update_hash_optional_str(hasher: &mut Xxh3, value: Option<&str>) {
    match value {
        Some(text) => {
            hasher.update(b"S");
            update_hash_field(hasher, text.as_bytes());
        }
        None => hasher.update(b"N"),
    }
}

pub fn update_hash_string_list(hasher: &mut Xxh3, values: &[String]) {
    hasher.update(b"A");
    update_hash_usize_field(hasher, values.len());
    for value in values {
        update_hash_field(hasher, value.as_bytes());
    }
}

fn update_hash_i64_list(hasher: &mut Xxh3, values: &[i64]) {
    hasher.update(b"I");
    update_hash_usize_field(hasher, values.len());
    for value in values {
        update_hash_i64_field(hasher, *value);
    }
}

pub fn finish_hash_hex(hasher: Xxh3) -> String {
    hex_u64_16(hasher.digest() & HASH_MASK_63)
}

pub fn hash_string_list(domain: &str, values: &[String]) -> String {
    let mut hasher = Xxh3::new();
    hasher.update(domain.as_bytes());
    update_hash_string_list(&mut hasher, values);
    finish_hash_hex(hasher)
}

pub fn hash_i64_list(domain: &str, values: &[i64]) -> String {
    let mut hasher = Xxh3::new();
    hasher.update(domain.as_bytes());
    update_hash_i64_list(&mut hasher, values);
    finish_hash_hex(hasher)
}

pub fn checksum_i64_list(domain: &str, values: &[i64]) -> i64 {
    let mut hasher = Xxh3::new();
    hasher.update(domain.as_bytes());
    update_hash_i64_list(&mut hasher, values);
    (hasher.digest() & HASH_MASK_63) as i64
}

pub fn hash_text(domain: &str, parts: &[String]) -> String {
    let mut hasher = Xxh3::new();
    hasher.update(domain.as_bytes());
    for part in parts {
        update_hash_field(&mut hasher, part.as_bytes());
    }
    finish_hash_hex(hasher)
}

pub fn xxh3_63(bytes: &[u8]) -> u64 {
    let mut hasher = Xxh3::new();
    hasher.update(bytes);
    hasher.digest() & HASH_MASK_63
}

fn hex_u64_16(value: u64) -> String {
    let mut bytes = [0u8; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        let shift = (15 - index) * 4;
        *byte = LOWER_HEX[((value >> shift) & 0x0f) as usize];
    }
    String::from_utf8(bytes.to_vec()).expect("fixed hex alphabet is valid UTF-8")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn legacy_update_hash_field(hasher: &mut Xxh3, value: &[u8]) {
        hasher.update(b"\x1f");
        hasher.update(value.len().to_string().as_bytes());
        hasher.update(b":");
        hasher.update(value);
    }

    fn legacy_hash_string_list(domain: &str, values: &[String]) -> String {
        let mut hasher = Xxh3::new();
        hasher.update(domain.as_bytes());
        hasher.update(b"A");
        legacy_update_hash_field(&mut hasher, values.len().to_string().as_bytes());
        for value in values {
            legacy_update_hash_field(&mut hasher, value.as_bytes());
        }
        format!("{:016x}", hasher.digest() & HASH_MASK_63)
    }

    fn legacy_hash_i64_list(domain: &str, values: &[i64]) -> String {
        let mut hasher = Xxh3::new();
        hasher.update(domain.as_bytes());
        hasher.update(b"I");
        legacy_update_hash_field(&mut hasher, values.len().to_string().as_bytes());
        for value in values {
            legacy_update_hash_field(&mut hasher, value.to_string().as_bytes());
        }
        format!("{:016x}", hasher.digest() & HASH_MASK_63)
    }

    fn legacy_hash_text(domain: &str, parts: &[String]) -> String {
        let mut hasher = Xxh3::new();
        hasher.update(domain.as_bytes());
        for part in parts {
            let bytes = part.as_bytes();
            hasher.update(b"\x1f");
            hasher.update(bytes.len().to_string().as_bytes());
            hasher.update(b":");
            hasher.update(bytes);
        }
        format!("{:016x}", hasher.digest() & HASH_MASK_63)
    }

    #[test]
    fn canonical_json_orders_object_keys() {
        let left = json!({"b": 2, "a": [3, {"z": 1, "c": 2}]});
        let right = json!({"a": [3, {"c": 2, "z": 1}], "b": 2});
        assert_eq!(canonical_json(&left), canonical_json(&right));
    }

    #[test]
    fn semantic_hash_is_domain_sensitive_and_stable() {
        let payload = json!({"code": "450", "system": "RC"});
        assert_eq!(
            semantic_hash("procedure", payload.clone()),
            semantic_hash("procedure", payload.clone())
        );
        assert_ne!(
            semantic_hash("procedure", payload.clone()),
            semantic_hash("price", payload)
        );
    }

    #[test]
    fn composite_keys_are_deterministic() {
        assert_eq!(
            provider_group_member_key(12, 34),
            provider_group_member_key(12, 34)
        );
        assert_ne!(price_set_entry_key("a", "b"), price_set_entry_key("b", "a"));
    }

    #[test]
    fn stack_formatted_hashes_match_legacy_formatting() {
        let strings = vec![
            "".to_string(),
            "alpha".to_string(),
            "line\nquote\"slash\\".to_string(),
        ];
        assert_eq!(
            hash_string_list("price_code_set", &strings),
            legacy_hash_string_list("price_code_set", &strings)
        );

        let integers = vec![0, -1, i64::MIN, 1234567890123456789];
        assert_eq!(
            hash_i64_list("provider_set", &integers),
            legacy_hash_i64_list("provider_set", &integers)
        );
        assert_eq!(
            checksum_i64_list("provider_set", &integers),
            i64::from_str_radix(&legacy_hash_i64_list("provider_set", &integers), 16).unwrap()
        );

        assert_eq!(
            hash_text("serving_rate", &strings),
            legacy_hash_text("serving_rate", &strings)
        );

        let json_value = json!({"b": [2, 1], "a": {"code": "450"}});
        let legacy_json_hash = format!("{:016x}", xxh3_63(canonical_json(&json_value).as_bytes()));
        assert_eq!(sha63_hex_from_json(&json_value), legacy_json_hash);
    }

    #[test]
    fn finish_hash_hex_is_padded_lowercase_hex() {
        let mut hasher = Xxh3::new();
        hasher.update(b"pad-check");
        let value = finish_hash_hex(hasher);

        assert_eq!(value.len(), 16);
        assert!(value.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert_eq!(value, value.to_ascii_lowercase());
    }
}
