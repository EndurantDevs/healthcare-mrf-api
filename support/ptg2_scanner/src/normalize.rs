use serde_json::Value;
use std::fmt::Display;
use std::io::{self, Read};
use struson::reader::{JsonReader, JsonStreamReader, ValueType};

pub const NPI_MIN: i64 = 1_000_000_000;
pub const NPI_MAX: i64 = 9_999_999_999;

fn to_io_error(error: impl Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

pub fn normalize_string(value: Option<&Value>) -> Option<String> {
    match value {
        Some(Value::String(text)) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }
        Some(Value::Number(number)) => Some(number.to_string()),
        Some(Value::Bool(value)) => Some(value.to_string()),
        _ => None,
    }
}

pub fn normalize_code(value: Option<&Value>) -> Option<String> {
    normalize_string(value).map(|value| value.to_uppercase())
}

pub fn normalize_tin_type(value: Option<&Value>) -> String {
    normalize_string(value)
        .unwrap_or_default()
        .trim()
        .to_lowercase()
}

pub fn normalize_tin_value(value: Option<&Value>) -> String {
    normalize_string(value)
        .unwrap_or_default()
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>()
        .to_uppercase()
}

pub fn int_list(value: Option<&Value>) -> Vec<i64> {
    let mut out = Vec::new();
    match value {
        Some(Value::Array(items)) => {
            for item in items {
                if let Some(text) = normalize_string(Some(item)) {
                    if let Some(number) = parse_integer_text(text.trim()) {
                        out.push(number);
                    }
                }
            }
        }
        Some(item) => {
            if let Some(text) = normalize_string(Some(item)) {
                if let Some(number) = parse_integer_text(text.trim()) {
                    out.push(number);
                }
            }
        }
        None => {}
    }
    out.sort_unstable();
    out.dedup();
    out
}

fn parse_integer_text(text: &str) -> Option<i64> {
    text.parse::<i64>().ok().or_else(|| {
        let canonical = canonical_decimal_text(text)?;
        (!canonical.contains('.'))
            .then(|| canonical.parse::<i64>().ok())
            .flatten()
    })
}

pub fn is_valid_npi(value: i64) -> bool {
    (NPI_MIN..=NPI_MAX).contains(&value)
}

pub fn npi_list(value: Option<&Value>) -> Vec<i64> {
    let mut out: Vec<i64> = int_list(value)
        .into_iter()
        .filter(|value| is_valid_npi(*value))
        .collect();
    out.sort_unstable();
    out.dedup();
    out
}

pub fn normalize_money_text(text: String) -> Option<String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        None
    } else {
        canonical_decimal_text(trimmed).or_else(|| Some(trimmed.to_string()))
    }
}

fn canonical_decimal_text(source_text: &str) -> Option<String> {
    let (is_negative, unsigned_text) = match source_text.as_bytes().first() {
        Some(b'-') => (true, &source_text[1..]),
        Some(b'+') => (false, &source_text[1..]),
        _ => (false, source_text),
    };
    let (mantissa, exponent) = decimal_mantissa_and_exponent(unsigned_text)?;
    let (integer_digits, fractional_digits) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if (integer_digits.is_empty() && fractional_digits.is_empty())
        || !integer_digits.bytes().all(|byte| byte.is_ascii_digit())
        || !fractional_digits.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let digits = format!("{integer_digits}{fractional_digits}");
    let decimal_position = i64::try_from(integer_digits.len())
        .ok()?
        .checked_add(exponent)?;
    let mut expanded = expand_decimal_digits(&digits, decimal_position)?;
    normalize_expanded_decimal(&mut expanded);
    if is_negative && expanded != "0" {
        expanded.insert(0, '-');
    }
    Some(expanded)
}

fn decimal_mantissa_and_exponent(source_text: &str) -> Option<(&str, i64)> {
    let mut exponent_markers = source_text.match_indices(['e', 'E']);
    let first_marker = exponent_markers.next();
    if exponent_markers.next().is_some() {
        return None;
    }
    let Some((marker_offset, _marker)) = first_marker else {
        return Some((source_text, 0));
    };
    let mantissa = &source_text[..marker_offset];
    let exponent_text = &source_text[marker_offset + 1..];
    if mantissa.is_empty() || exponent_text.is_empty() {
        return None;
    }
    Some((mantissa, exponent_text.parse::<i64>().ok()?))
}

fn expand_decimal_digits(digits: &str, decimal_position: i64) -> Option<String> {
    const MAX_CANONICAL_MONEY_CHARS: usize = 131_072;
    let digit_count = i64::try_from(digits.len()).ok()?;
    let output_size = if decimal_position <= 0 {
        digit_count
            .checked_add(decimal_position.checked_neg()?)?
            .checked_add(2)?
    } else if decimal_position >= digit_count {
        decimal_position
    } else {
        digit_count.checked_add(1)?
    };
    if output_size < 0 || usize::try_from(output_size).ok()? > MAX_CANONICAL_MONEY_CHARS {
        return None;
    }
    if decimal_position <= 0 {
        let zero_count = usize::try_from(decimal_position.checked_neg()?).ok()?;
        return Some(format!("0.{}{digits}", "0".repeat(zero_count)));
    }
    if decimal_position >= digit_count {
        let zero_count = usize::try_from(decimal_position - digit_count).ok()?;
        return Some(format!("{digits}{}", "0".repeat(zero_count)));
    }
    let split_offset = usize::try_from(decimal_position).ok()?;
    Some(format!(
        "{}.{}",
        &digits[..split_offset],
        &digits[split_offset..]
    ))
}

fn normalize_expanded_decimal(expanded: &mut String) {
    if let Some(decimal_offset) = expanded.find('.') {
        while expanded.ends_with('0') {
            expanded.pop();
        }
        if expanded.len() == decimal_offset + 1 {
            expanded.pop();
        }
    }
    let integer_end = expanded.find('.').unwrap_or(expanded.len());
    let leading_zero_count = expanded[..integer_end]
        .bytes()
        .take_while(|byte| *byte == b'0')
        .count();
    if leading_zero_count >= integer_end {
        expanded.replace_range(..integer_end, "0");
    } else if leading_zero_count > 0 {
        expanded.replace_range(..leading_zero_count, "");
    }
}

pub fn normalized_scalar_from_reader<R: Read>(
    json_reader: &mut JsonStreamReader<R>,
) -> io::Result<Option<String>> {
    match json_reader.peek().map_err(to_io_error)? {
        ValueType::String => {
            let text = json_reader.next_string().map_err(to_io_error)?;
            let trimmed = text.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(trimmed.to_string()))
            }
        }
        ValueType::Number => {
            let text = json_reader.next_number_as_string().map_err(to_io_error)?;
            if text.is_empty() {
                Ok(None)
            } else {
                Ok(Some(text))
            }
        }
        ValueType::Boolean => Ok(Some(
            json_reader.next_bool().map_err(to_io_error)?.to_string(),
        )),
        ValueType::Null => {
            json_reader.next_null().map_err(to_io_error)?;
            Ok(None)
        }
        ValueType::Array | ValueType::Object => {
            json_reader.skip_value().map_err(to_io_error)?;
            Ok(None)
        }
    }
}

pub fn normalized_money_from_reader<R: Read>(
    json_reader: &mut JsonStreamReader<R>,
) -> io::Result<Option<String>> {
    Ok(normalized_scalar_from_reader(json_reader)?.and_then(normalize_money_text))
}

pub fn normalized_string_list_from_reader<R: Read>(
    json_reader: &mut JsonStreamReader<R>,
) -> io::Result<Vec<String>> {
    let mut out = Vec::new();
    match json_reader.peek().map_err(to_io_error)? {
        ValueType::Array => {
            json_reader.begin_array().map_err(to_io_error)?;
            while json_reader.has_next().map_err(to_io_error)? {
                if let Some(text) = normalized_scalar_from_reader(json_reader)? {
                    out.push(text);
                }
            }
            json_reader.end_array().map_err(to_io_error)?;
        }
        ValueType::Object => {
            json_reader.skip_value().map_err(to_io_error)?;
        }
        _ => {
            if let Some(text) = normalized_scalar_from_reader(json_reader)? {
                out.push(text);
            }
        }
    }
    Ok(out)
}

pub fn canonical_text_list(values: Vec<String>, uppercase: bool) -> Vec<String> {
    let mut out: Vec<String> = values
        .into_iter()
        .filter_map(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else if uppercase {
                Some(trimmed.to_uppercase())
            } else {
                Some(trimmed.to_string())
            }
        })
        .collect();
    out.sort_unstable();
    out.dedup();
    out
}

#[cfg(test)]
mod tests {
    use super::{
        canonical_text_list, int_list, normalize_code, normalize_money_text, normalize_string,
        normalize_tin_type, normalize_tin_value, normalized_money_from_reader,
        normalized_scalar_from_reader, normalized_string_list_from_reader, npi_list,
    };
    use serde_json::json;
    use struson::reader::JsonStreamReader;

    #[test]
    fn normalizes_json_scalars_for_dictionary_keys() {
        assert_eq!(
            normalize_string(Some(&json!(" RC "))),
            Some("RC".to_string())
        );
        assert_eq!(normalize_string(Some(&json!("   "))), None);
        assert_eq!(normalize_string(Some(&json!(450))), Some("450".to_string()));
        assert_eq!(
            normalize_string(Some(&json!(true))),
            Some("true".to_string())
        );
        assert_eq!(normalize_code(Some(&json!(" rc "))), Some("RC".to_string()));
    }

    #[test]
    fn normalizes_tin_and_integer_lists() {
        assert_eq!(normalize_tin_type(Some(&json!(" EIN "))), "ein");
        assert_eq!(normalize_tin_value(Some(&json!(" 12-34 ab "))), "1234AB");
        assert_eq!(int_list(Some(&json!(["2", 1, "bad", 2]))), vec![1, 2]);
        assert_eq!(int_list(Some(&json!("42"))), vec![42]);
    }

    #[test]
    fn normalizes_npi_lists_to_ten_digit_values() {
        assert_eq!(
            npi_list(Some(&json!([
                "114911247",
                "1234567890",
                1234567890.0,
                1.23456789e9,
                9_999_999_999i64,
                10_000_000_000i64,
                "bad"
            ]))),
            vec![1_234_567_890, 9_999_999_999]
        );
    }

    #[test]
    fn normalizes_money_text_without_changing_integer_strings() {
        assert_eq!(
            normalize_money_text("10.5000".to_string()),
            Some("10.5".to_string())
        );
        assert_eq!(
            normalize_money_text("10.000".to_string()),
            Some("10".to_string())
        );
        assert_eq!(
            normalize_money_text("10".to_string()),
            Some("10".to_string())
        );
        assert_eq!(
            normalize_money_text("1.2300e10".to_string()),
            Some("12300000000".to_string())
        );
        assert_eq!(
            normalize_money_text("-1.2500E-3".to_string()),
            Some("-0.00125".to_string())
        );
        assert_eq!(
            normalize_money_text("+001.2300e2".to_string()),
            Some("123".to_string())
        );
        assert_eq!(
            normalize_money_text("-0e100".to_string()),
            Some("0".to_string())
        );
        assert_eq!(
            normalize_money_text("1e64".to_string()),
            Some(format!("1{}", "0".repeat(64)))
        );
        assert_eq!(normalize_money_text("".to_string()), None);
    }

    #[test]
    fn normalizes_scalar_values_from_streaming_reader() {
        let mut reader = JsonStreamReader::new(br#" " value " "#.as_slice());
        assert_eq!(
            normalized_scalar_from_reader(&mut reader).unwrap(),
            Some("value".to_string())
        );

        let mut reader = JsonStreamReader::new(br#"true"#.as_slice());
        assert_eq!(
            normalized_scalar_from_reader(&mut reader).unwrap(),
            Some("true".to_string())
        );
    }

    #[test]
    fn normalizes_money_and_lists_from_streaming_reader() {
        let mut reader = JsonStreamReader::new(br#"10.5000"#.as_slice());
        assert_eq!(
            normalized_money_from_reader(&mut reader).unwrap(),
            Some("10.5".to_string())
        );

        let mut reader = JsonStreamReader::new(br#"[" 26 ","",42,{"skip":true}]"#.as_slice());
        assert_eq!(
            normalized_string_list_from_reader(&mut reader).unwrap(),
            vec!["26".to_string(), "42".to_string()]
        );
    }

    #[test]
    fn canonical_text_lists_trim_sort_dedupe_and_optionally_uppercase() {
        assert_eq!(
            canonical_text_list(
                vec![" b ".to_string(), "A".to_string(), "b".to_string()],
                true
            ),
            vec!["A".to_string(), "B".to_string()]
        );
        assert_eq!(
            canonical_text_list(
                vec![" b ".to_string(), "a".to_string(), "".to_string()],
                false
            ),
            vec!["a".to_string(), "b".to_string()]
        );
    }
}
