// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

use super::contracts::invalid_data;
use serde_json::{Map, Number, Value};
use std::io;

pub fn normalized_text(value: Option<&Value>, limit: usize) -> Option<String> {
    let text = value?.as_str()?;
    let normalized = text.trim();
    (normalized.chars().count() <= limit && !normalized.is_empty()).then(|| normalized.to_owned())
}

pub fn optional_text(
    object: &Map<String, Value>,
    field: &str,
    limit: usize,
) -> io::Result<Option<String>> {
    if !object.contains_key(field) {
        return Ok(None);
    }
    normalized_text(object.get(field), limit)
        .map(Some)
        .ok_or_else(|| invalid_field(field))
}

pub fn fhir_list<'a>(object: &'a Map<String, Value>, field: &str) -> io::Result<&'a [Value]> {
    match object.get(field) {
        None => Ok(&[]),
        Some(Value::Array(entries)) => Ok(entries),
        Some(_) => Err(invalid_field(field)),
    }
}

pub fn normalized_period(value: Option<&Value>) -> io::Result<Option<Value>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let object = value.as_object().ok_or_else(|| invalid_field("period"))?;
    let mut period = Map::new();
    for field in ["start", "end"] {
        if let Some(text) = optional_text(object, field, 64)? {
            period.insert(field.to_owned(), Value::String(text));
        }
    }
    if period.is_empty() {
        return Err(invalid_field("period"));
    }
    Ok(Some(Value::Object(period)))
}

fn normalized_coding(value: &Value) -> io::Result<Value> {
    let object = value.as_object().ok_or_else(|| invalid_field("coding"))?;
    let mut coding = Map::new();
    for field in ["system", "version", "code", "display"] {
        if let Some(text) = optional_text(object, field, 2048)? {
            coding.insert(field.to_owned(), Value::String(text));
        }
    }
    if let Some(user_selected) = object.get("userSelected") {
        let selected = user_selected
            .as_bool()
            .ok_or_else(|| invalid_field("coding_user_selected"))?;
        coding.insert("userSelected".to_owned(), Value::Bool(selected));
    }
    if !coding.contains_key("code") && !coding.contains_key("display") {
        return Err(invalid_field("coding"));
    }
    Ok(Value::Object(coding))
}

fn normalized_concept(value: &Value) -> io::Result<Value> {
    let object = value
        .as_object()
        .ok_or_else(|| invalid_field("codeable_concept"))?;
    let mut concept = Map::new();
    for field in ["system", "version", "code", "display", "text"] {
        if let Some(text) = optional_text(object, field, 2048)? {
            concept.insert(field.to_owned(), Value::String(text));
        }
    }
    let codings = fhir_list(object, "coding")?
        .iter()
        .map(normalized_coding)
        .collect::<io::Result<Vec<_>>>()?;
    if !codings.is_empty() {
        concept.insert("coding".to_owned(), Value::Array(codings));
    }
    if !["code", "display", "text", "coding"]
        .iter()
        .any(|field| concept.contains_key(*field))
    {
        return Err(invalid_field("codeable_concept"));
    }
    Ok(Value::Object(concept))
}

pub fn profile_concepts(value: Option<&Value>) -> io::Result<Vec<Value>> {
    match value {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(entries)) => entries.iter().map(normalized_concept).collect(),
        Some(Value::Object(_)) => Ok(vec![normalized_concept(value.unwrap())?]),
        Some(_) => Err(invalid_field("codeable_concept")),
    }
}

fn normalized_name(value: &Value) -> io::Result<Value> {
    let object = value.as_object().ok_or_else(|| invalid_field("name"))?;
    let mut name = Map::new();
    for field in ["use", "text", "family"] {
        if let Some(text) = optional_text(object, field, 2048)? {
            name.insert(field.to_owned(), Value::String(text));
        }
    }
    for field in ["given", "prefix", "suffix"] {
        let parts = fhir_list(object, field)?
            .iter()
            .map(|part| {
                normalized_text(Some(part), 2048)
                    .map(Value::String)
                    .ok_or_else(|| invalid_field(&format!("name_{field}")))
            })
            .collect::<io::Result<Vec<_>>>()?;
        if !parts.is_empty() {
            name.insert(field.to_owned(), Value::Array(parts));
        }
    }
    if let Some(period) = normalized_period(object.get("period"))? {
        name.insert("period".to_owned(), period);
    }
    if !["text", "family", "given"]
        .iter()
        .any(|field| name.contains_key(*field))
    {
        return Err(invalid_field("name"));
    }
    Ok(Value::Object(name))
}

pub fn profile_names(resource: &Map<String, Value>) -> io::Result<Vec<Value>> {
    let mut names = match resource.get("name") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::String(_)) => vec![object_with_text(
            normalized_text(resource.get("name"), 2048).ok_or_else(|| invalid_field("name"))?,
        )],
        Some(Value::Object(_)) => vec![normalized_name(resource.get("name").unwrap())?],
        Some(Value::Array(entries)) => entries
            .iter()
            .map(normalized_name)
            .collect::<io::Result<Vec<_>>>()?,
        Some(_) => return Err(invalid_field("name")),
    };
    for alias in fhir_list(resource, "alias")? {
        let text = normalized_text(Some(alias), 2048).ok_or_else(|| invalid_field("alias"))?;
        names.push(object_with_text(text));
    }
    Ok(names)
}

pub fn profile_contacts(
    resource: &Map<String, Value>,
    resource_type: &str,
) -> io::Result<Vec<Value>> {
    let mut entries = fhir_list(resource, "telecom")?.iter().collect::<Vec<_>>();
    if resource_type == "Endpoint" {
        entries.extend(fhir_list(resource, "contact")?);
    }
    entries
        .into_iter()
        .map(|entry| {
            let object = entry.as_object().ok_or_else(|| invalid_field("telecom"))?;
            let value = optional_text(object, "value", 2048)?
                .ok_or_else(|| invalid_field("telecom_value"))?;
            let mut contact = Map::new();
            contact.insert("value".to_owned(), Value::String(value));
            for field in ["system", "use"] {
                if let Some(text) = optional_text(object, field, 64)? {
                    contact.insert(field.to_owned(), Value::String(text));
                }
            }
            if let Some(rank) = object.get("rank") {
                let rank = rank
                    .as_i64()
                    .filter(|rank| *rank >= 1)
                    .ok_or_else(|| invalid_field("telecom_rank"))?;
                contact.insert("rank".to_owned(), Value::Number(rank.into()));
            }
            if let Some(period) = normalized_period(object.get("period"))? {
                contact.insert("period".to_owned(), period);
            }
            Ok(Value::Object(contact))
        })
        .collect()
}

pub fn normalized_address(value: &Value) -> io::Result<Value> {
    let object = value.as_object().ok_or_else(|| invalid_field("address"))?;
    let mut address = Map::new();
    for field in [
        "use",
        "type",
        "text",
        "city",
        "district",
        "state",
        "postalCode",
        "country",
    ] {
        if let Some(text) = optional_text(object, field, 2048)? {
            address.insert(field.to_owned(), Value::String(text));
        }
    }
    if object.contains_key("line") {
        let lines = fhir_list(object, "line")?
            .iter()
            .map(|line| {
                normalized_text(Some(line), 2048)
                    .map(Value::String)
                    .ok_or_else(|| invalid_field("address_line"))
            })
            .collect::<io::Result<Vec<_>>>()?;
        if !lines.is_empty() {
            address.insert("line".to_owned(), Value::Array(lines));
        }
    }
    if let Some(period) = normalized_period(object.get("period"))? {
        address.insert("period".to_owned(), period);
    }
    if ![
        "text",
        "line",
        "city",
        "district",
        "state",
        "postalCode",
        "country",
    ]
    .iter()
    .any(|field| address.contains_key(*field))
    {
        return Err(invalid_field("address"));
    }
    Ok(Value::Object(address))
}

pub fn normalized_position(value: Option<&Value>) -> io::Result<Option<Value>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let object = value.as_object().ok_or_else(|| invalid_field("position"))?;
    let latitude = microdegrees(object.get("latitude"), 90)?;
    let longitude = microdegrees(object.get("longitude"), 180)?;
    let mut position = Map::new();
    position.insert(
        "latitude_microdegrees".to_owned(),
        Value::Number(latitude.into()),
    );
    position.insert(
        "longitude_microdegrees".to_owned(),
        Value::Number(longitude.into()),
    );
    Ok(Some(Value::Object(position)))
}

fn object_with_text(text: String) -> Value {
    let mut object = Map::new();
    object.insert("text".to_owned(), Value::String(text));
    Value::Object(object)
}

fn invalid_field(field: &str) -> io::Error {
    invalid_data(format!(
        "provider_directory_projection_fhir_{field}_invalid"
    ))
}

#[derive(Debug)]
struct DecimalParts {
    negative: bool,
    digits: String,
    scale: i64,
}

fn decimal_parts(number: &Number) -> io::Result<DecimalParts> {
    let token = number.to_string();
    let (negative, unsigned) = token
        .strip_prefix('-')
        .map_or((false, token.as_str()), |rest| (true, rest));
    let (mantissa, exponent_text) = unsigned
        .split_once(['e', 'E'])
        .map_or((unsigned, None), |(left, right)| (left, Some(right)));
    let exponent = exponent_text
        .map(|text| {
            text.parse::<i64>()
                .expect("a serde_json::Number exponent fits i64")
        })
        .unwrap_or(0);
    let (whole, fraction) = mantissa
        .split_once('.')
        .map_or((mantissa, ""), |(left, right)| (left, right));
    let digits = format!("{whole}{fraction}");
    debug_assert!(!digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit()));
    let normalized = digits.trim_start_matches('0');
    Ok(DecimalParts {
        negative,
        digits: if normalized.is_empty() {
            "0".to_owned()
        } else {
            normalized.to_owned()
        },
        scale: i64::try_from(fraction.len())
            .expect("a serde_json::Number token length fits i64")
            .checked_sub(exponent)
            .expect("a serde_json::Number scale fits i64"),
    })
}

fn within_integer_bound(parts: &DecimalParts, bound: i64) -> bool {
    if parts.digits == "0" {
        return true;
    }
    let bound_text = bound.to_string();
    if parts.scale <= 0 {
        let total_digits = i64::try_from(parts.digits.len()).unwrap_or(i64::MAX) - parts.scale;
        if total_digits != bound_text.len() as i64 {
            return total_digits < bound_text.len() as i64;
        }
        return parts.digits.as_str() <= &bound_text[..parts.digits.len()];
    }
    let integer_digits = i64::try_from(parts.digits.len()).unwrap_or(i64::MAX) - parts.scale;
    if integer_digits <= 0 {
        return true;
    }
    if integer_digits != bound_text.len() as i64 {
        return integer_digits < bound_text.len() as i64;
    }
    let split = usize::try_from(integer_digits).unwrap_or(usize::MAX);
    let integer = &parts.digits[..split];
    integer < bound_text.as_str()
        || (integer == bound_text && parts.digits[split..].bytes().all(|byte| byte == b'0'))
}

fn rounded_scaled_integer(parts: &DecimalParts, scale_delta: i64) -> io::Result<i64> {
    if parts.digits == "0" {
        return Ok(0);
    }
    let target_scale = parts
        .scale
        .checked_sub(scale_delta)
        .ok_or_else(|| invalid_field("position"))?;
    let magnitude = if target_scale <= 0 {
        let zero_count = usize::try_from(-target_scale).map_err(|_| invalid_field("position"))?;
        if zero_count > 32 {
            return Err(invalid_field("position"));
        }
        let mut integer = parts.digits.clone();
        integer.extend(std::iter::repeat_n('0', zero_count));
        integer
            .parse::<i64>()
            .map_err(|_| invalid_field("position"))?
    } else {
        let removed = usize::try_from(target_scale).map_err(|_| invalid_field("position"))?;
        if removed > parts.digits.len() {
            return Ok(0);
        }
        let split = parts.digits.len().saturating_sub(removed);
        let quotient_text = if split == 0 {
            "0"
        } else {
            &parts.digits[..split]
        };
        let quotient = quotient_text
            .parse::<i64>()
            .map_err(|_| invalid_field("position"))?;
        let remainder = &parts.digits[split..];
        let first = remainder.as_bytes().first().copied().unwrap_or(b'0');
        let round_up = first > b'5'
            || (first == b'5'
                && (remainder.as_bytes()[1..].iter().any(|digit| *digit != b'0')
                    || quotient % 2 == 1));
        quotient + i64::from(round_up)
    };
    Ok(if parts.negative && magnitude != 0 {
        -magnitude
    } else {
        magnitude
    })
}

fn microdegrees(value: Option<&Value>, bound: i64) -> io::Result<i64> {
    let number = value
        .and_then(Value::as_number)
        .ok_or_else(|| invalid_field("position"))?;
    let parts = decimal_parts(number)?;
    if !within_integer_bound(&parts, bound) {
        return Err(invalid_field("position"));
    }
    rounded_scaled_integer(&parts, 6)
}

#[cfg(test)]
pub(super) fn test_microdegrees(number: &str, bound: i64) -> io::Result<i64> {
    let value: Value = serde_json::from_str(number).map_err(|_| invalid_field("position"))?;
    microdegrees(Some(&value), bound)
}

#[cfg(test)]
#[path = "tests/fhir_values_coverage_tests.rs"]
mod coverage_tests;
