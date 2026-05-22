use serde_json::Value;
use std::fmt::Display;
use std::io::{self, Read};
use struson::reader::{JsonReader, JsonStreamReader, ValueType};

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
                    if let Ok(number) = text.trim().parse::<i64>() {
                        out.push(number);
                    }
                }
            }
        }
        Some(item) => {
            if let Some(text) = normalize_string(Some(item)) {
                if let Ok(number) = text.trim().parse::<i64>() {
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

pub fn normalize_money_text(mut text: String) -> Option<String> {
    if text.contains('.') {
        while text.ends_with('0') {
            text.pop();
        }
        if text.ends_with('.') {
            text.pop();
        }
    }
    if text.is_empty() {
        None
    } else {
        Some(text)
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
        normalized_scalar_from_reader, normalized_string_list_from_reader,
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
