//! Complete bounded landing-payload validation without Python value recovery.

use serde_json::Value;

pub type PayloadField = (String, String, bool);
pub const MAX_PAYLOAD_DEPTH: usize = 32;
pub const MAX_PAYLOAD_NODES: usize = 10_000;

fn wire_value(value: &Value, depth: usize, nodes: &mut usize, integer_digits: usize) -> bool {
    *nodes += 1;
    if depth > MAX_PAYLOAD_DEPTH || *nodes > MAX_PAYLOAD_NODES {
        return false;
    }
    match value {
        Value::Number(number) => {
            let text = number.as_str();
            let digits = text.strip_prefix('-').unwrap_or(text);
            text != "-0"
                && digits.bytes().all(|byte| byte.is_ascii_digit())
                && (integer_digits == 0 || digits.len() <= integer_digits)
        }
        Value::Array(items) => items
            .iter()
            .all(|item| wire_value(item, depth + 1, nodes, integer_digits)),
        Value::Object(items) => items
            .values()
            .all(|item| wire_value(item, depth + 1, nodes, integer_digits)),
        _ => true,
    }
}

fn decimal_text(text: &str) -> bool {
    let unsigned = text.strip_prefix('-').unwrap_or(text);
    let digits = |part: &str| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit());
    match unsigned.split_once('.') {
        Some((integer, fraction)) => digits(integer) && digits(fraction),
        None => digits(unsigned),
    }
}

fn field_value(field: &PayloadField, encoded: &Value) -> Result<(), &'static str> {
    let (name, kind, nullable) = field;
    if !encoded.is_object() || encoded.get("field").and_then(Value::as_str) != Some(name) {
        return Err("field identity does not match the definition");
    }
    let value = encoded
        .get("value")
        .and_then(Value::as_object)
        .ok_or("field value does not match the definition")?;
    let state = value.get("state").and_then(Value::as_str);
    if state == Some("missing") {
        return if *nullable {
            Ok(())
        } else {
            Err("omits a required field")
        };
    }
    if value.get("type").and_then(Value::as_str) != Some(kind) {
        return Err("field value does not match the definition");
    }
    if state == Some("null") {
        return if *nullable {
            Ok(())
        } else {
            Err("nulls a required field")
        };
    }
    let scalar = value
        .get("value")
        .filter(|_| state == Some("value"))
        .ok_or("field value is malformed")?;
    match kind.as_str() {
        "string" if !scalar.is_string() => Err("string value is malformed"),
        "integer" if !scalar.is_number() => Err("integer value is malformed"),
        "boolean" if !scalar.is_boolean() => Err("boolean value is malformed"),
        "decimal" if !(scalar.is_number() || scalar.as_str().is_some_and(decimal_text)) => {
            Err("decimal value is malformed")
        }
        _ => Ok(()),
    }
}

pub fn validate_payload(
    fields: &[PayloadField],
    payload: &str,
    integer_digits: usize,
) -> Result<(), &'static str> {
    let value: Value = serde_json::from_str(payload).map_err(|_| "is malformed")?;
    if !wire_value(&value, 0, &mut 0, integer_digits)
        || serde_json::to_string(&value).ok().as_deref() != Some(payload)
    {
        return Err("is malformed");
    }
    if value.get("contract").and_then(Value::as_str) != Some("custom-import-record/v1") {
        return Err("has an unknown contract");
    }
    let encoded = value
        .get("fields")
        .and_then(Value::as_array)
        .filter(|encoded| encoded.len() == fields.len())
        .ok_or("fields do not match the definition")?;
    for (field, encoded) in fields.iter().zip(encoded) {
        field_value(field, encoded)?;
    }
    Ok(())
}
