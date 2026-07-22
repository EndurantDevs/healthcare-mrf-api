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
mod coverage_tests {
    use super::*;
    use serde_json::json;

    fn object(value: Value) -> Map<String, Value> {
        value.as_object().expect("JSON object").clone()
    }

    #[test]
    fn text_list_period_and_concept_helpers_cover_valid_null_and_invalid_shapes() {
        assert_eq!(
            normalized_text(Some(&json!("  value  ")), 5).as_deref(),
            Some("value")
        );
        assert_eq!(normalized_text(None, 5), None);
        assert_eq!(normalized_text(Some(&Value::Null), 5), None);
        assert_eq!(normalized_text(Some(&json!("   ")), 5), None);
        assert_eq!(normalized_text(Some(&json!("sixsix")), 5), None);

        let mut fields = Map::new();
        assert_eq!(optional_text(&fields, "field", 8).unwrap(), None);
        fields.insert("field".to_owned(), json!(" value "));
        assert_eq!(
            optional_text(&fields, "field", 8).unwrap().as_deref(),
            Some("value")
        );
        fields.insert("field".to_owned(), Value::Null);
        assert!(optional_text(&fields, "field", 8).is_err());

        assert!(fhir_list(&Map::new(), "items").unwrap().is_empty());
        let list = object(json!({"items": [1, 2]}));
        assert_eq!(fhir_list(&list, "items").unwrap().len(), 2);
        assert!(fhir_list(&object(json!({"items": {}})), "items").is_err());

        assert_eq!(normalized_period(None).unwrap(), None);
        assert_eq!(normalized_period(Some(&Value::Null)).unwrap(), None);
        assert!(normalized_period(Some(&json!("bad"))).is_err());
        assert!(normalized_period(Some(&json!({}))).is_err());
        assert_eq!(
            normalized_period(Some(&json!({"start": " 2026-01-01 ", "end": "2026-12-31"})))
                .unwrap(),
            Some(json!({"end": "2026-12-31", "start": "2026-01-01"}))
        );

        let coding = normalized_coding(&json!({
            "system": "sys", "version": "v", "code": "code", "display": "Display",
            "userSelected": false
        }))
        .unwrap();
        assert_eq!(coding["userSelected"], false);
        assert!(normalized_coding(&json!([])).is_err());
        assert!(normalized_coding(&json!({"system": "sys"})).is_err());
        assert!(normalized_coding(&json!({"display": "x", "userSelected": "yes"})).is_err());

        let concept = normalized_concept(&json!({
            "system": "sys", "version": "v", "code": "c", "display": "D", "text": "T",
            "coding": [{"display": "nested"}]
        }))
        .unwrap();
        assert_eq!(concept["coding"][0]["display"], "nested");
        assert!(normalized_concept(&json!(1)).is_err());
        assert!(normalized_concept(&json!({})).is_err());
        assert!(normalized_concept(&json!({"text": "T", "coding": [false]})).is_err());
        assert!(profile_concepts(None).unwrap().is_empty());
        assert!(profile_concepts(Some(&Value::Null)).unwrap().is_empty());
        assert_eq!(
            profile_concepts(Some(&json!({"text": "T"}))).unwrap().len(),
            1
        );
        assert_eq!(
            profile_concepts(Some(&json!([{"code": "c"}])))
                .unwrap()
                .len(),
            1
        );
        assert!(profile_concepts(Some(&json!(true))).is_err());
    }

    #[test]
    fn names_contacts_and_addresses_preserve_supported_fields_and_fail_closed() {
        let name = normalized_name(&json!({
            "use": "official", "text": "Dr Example", "family": "Example",
            "given": ["Pat"], "prefix": ["Dr"], "suffix": ["III"],
            "period": {"start": "2020"}
        }))
        .unwrap();
        assert_eq!(name["given"][0], "Pat");
        assert!(normalized_name(&json!("name")).is_err());
        assert!(normalized_name(&json!({"prefix": ["Dr"]})).is_err());
        assert!(normalized_name(&json!({"family": "X", "given": [null]})).is_err());

        assert!(profile_names(&Map::new()).unwrap().is_empty());
        assert_eq!(
            profile_names(&object(json!({"name": " Clinic "})))
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            profile_names(&object(
                json!({"name": {"family": "X"}, "alias": ["Alias"]})
            ))
            .unwrap()
            .len(),
            2
        );
        assert_eq!(
            profile_names(&object(
                json!({"name": [{"text": "One"}, {"given": ["Two"]}]})
            ))
            .unwrap()
            .len(),
            2
        );
        assert!(profile_names(&object(json!({"name": 7}))).is_err());
        assert!(profile_names(&object(json!({"alias": [null]}))).is_err());

        let endpoint = object(json!({
            "telecom": [{"value": "555", "system": "phone", "use": "work", "rank": 1,
                "period": {"end": "2030"}}],
            "contact": [{"value": "ops@example.test", "system": "email"}]
        }));
        assert_eq!(profile_contacts(&endpoint, "Endpoint").unwrap().len(), 2);
        assert!(profile_contacts(&object(json!({"telecom": [false]})), "Organization").is_err());
        assert!(profile_contacts(&object(json!({"telecom": [{}]})), "Organization").is_err());
        assert!(profile_contacts(
            &object(json!({"telecom": [{"value": "555", "rank": 0}]})),
            "Organization"
        )
        .is_err());
        assert!(profile_contacts(
            &object(json!({"telecom": [{"value": "555", "rank": "first"}]})),
            "Organization"
        )
        .is_err());

        let address = normalized_address(&json!({
            "use": "work", "type": "physical", "text": "1 Main St", "line": ["1 Main St"],
            "city": "Town", "district": "District", "state": "IA", "postalCode": "50001",
            "country": "US", "period": {"start": "2026"}
        }))
        .unwrap();
        assert_eq!(address["line"][0], "1 Main St");
        assert!(normalized_address(&json!(false)).is_err());
        assert!(normalized_address(&json!({"use": "work"})).is_err());
        assert!(normalized_address(&json!({"city": "Town", "line": [null]})).is_err());
    }

    #[test]
    fn positions_and_decimal_helpers_cover_rounding_and_boundaries() {
        assert_eq!(normalized_position(None).unwrap(), None);
        assert_eq!(normalized_position(Some(&Value::Null)).unwrap(), None);
        assert!(normalized_position(Some(&json!(false))).is_err());
        assert!(normalized_position(Some(&json!({"latitude": 1}))).is_err());
        assert_eq!(
            normalized_position(Some(
                &json!({"latitude": 12.3456785, "longitude": -0.0000005})
            ))
            .unwrap(),
            Some(json!({"latitude_microdegrees": 12345678, "longitude_microdegrees": 0}))
        );

        let zero = decimal_parts(json!(0).as_number().unwrap()).unwrap();
        assert!(within_integer_bound(&zero, 90));
        assert_eq!(rounded_scaled_integer(&zero, 6).unwrap(), 0);
        let whole = decimal_parts(json!(9e1).as_number().unwrap()).unwrap();
        assert!(within_integer_bound(&whole, 90));
        assert_eq!(rounded_scaled_integer(&whole, 6).unwrap(), 90_000_000);
        assert!(test_microdegrees("90.000001", 90).is_err());
        assert!(test_microdegrees("-180.000001", 180).is_err());
        assert_eq!(test_microdegrees("0.0000016", 90).unwrap(), 2);
        assert_eq!(test_microdegrees("0.0000015", 90).unwrap(), 2);
        assert_eq!(test_microdegrees("0.0000025", 90).unwrap(), 2);
        assert_eq!(test_microdegrees("1e-40", 90).unwrap(), 0);
    }

    #[test]
    fn nested_shape_and_decimal_error_branches_are_explicit() {
        let mut fields = object(json!({"field": ""}));
        assert!(optional_text(&fields, "field", 8).is_err());
        fields.insert("field".to_owned(), json!("too-long"));
        assert!(optional_text(&fields, "field", 3).is_err());
        fields.insert("field".to_owned(), json!(7));
        assert!(optional_text(&fields, "field", 8).is_err());

        assert!(normalized_period(Some(&json!({"start": null}))).is_err());
        assert!(normalized_coding(&json!({"code": null})).is_err());
        assert!(normalized_concept(&json!({"text": "T", "coding": {}})).is_err());
        assert!(normalized_concept(&json!({"text": null})).is_err());
        assert!(normalized_name(&json!({"text": "Name", "given": "Pat"})).is_err());
        assert!(normalized_name(&json!({"text": "Name", "period": {}})).is_err());
        assert!(profile_names(&object(json!({"name": ""}))).is_err());
        assert!(profile_names(&object(json!({"name": [false]}))).is_err());

        assert!(profile_contacts(&object(json!({"telecom": {}})), "Organization").is_err());
        assert!(profile_contacts(&object(json!({"contact": {}})), "Endpoint").is_err());
        assert!(profile_contacts(
            &object(json!({"telecom": [{"value": "555", "system": null}]})),
            "Organization"
        )
        .is_err());
        assert!(profile_contacts(
            &object(json!({"telecom": [{"value": "555", "period": {}}]})),
            "Organization"
        )
        .is_err());

        assert!(normalized_address(&json!({"city": "Town", "line": "One"})).is_err());
        assert!(normalized_address(&json!({"city": null})).is_err());
        assert!(normalized_address(&json!({"city": "Town", "period": {}})).is_err());
        assert!(normalized_position(Some(&json!({
            "latitude": "north",
            "longitude": 1
        })))
        .is_err());
        assert!(normalized_position(Some(&json!({"latitude": 91, "longitude": 1}))).is_err());

        let checked_sub_overflow = DecimalParts {
            negative: false,
            digits: "1".to_owned(),
            scale: i64::MIN,
        };
        assert!(rounded_scaled_integer(&checked_sub_overflow, 6).is_err());
        let excessive_zero_padding = DecimalParts {
            negative: false,
            digits: "1".to_owned(),
            scale: -40,
        };
        assert!(rounded_scaled_integer(&excessive_zero_padding, 6).is_err());
        let integer_overflow = DecimalParts {
            negative: false,
            digits: "9999999999999999999999999999999999999999".to_owned(),
            scale: 6,
        };
        assert!(rounded_scaled_integer(&integer_overflow, 6).is_err());
        let quotient_overflow = DecimalParts {
            negative: false,
            digits: "9999999999999999999999999999999999999999".to_owned(),
            scale: 7,
        };
        assert!(rounded_scaled_integer(&quotient_overflow, 6).is_err());
        assert!(microdegrees(None, 90).is_err());
        assert!(test_microdegrees("not-json", 90).is_err());
    }
}
