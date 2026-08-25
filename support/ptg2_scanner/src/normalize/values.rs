pub fn normalize_money_text(text: String) -> Option<String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        None
    } else {
        canonical_decimal_text(trimmed).or_else(|| Some(trimmed.to_string()))
    }
}

pub fn canonical_decimal_text(source_text: &str) -> Option<String> {
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

pub fn strict_money_number_from_reader<R: Read>(
    json_reader: &mut JsonStreamReader<R>,
) -> io::Result<Option<String>> {
    match json_reader.peek().map_err(to_io_error)? {
        ValueType::Number => {
            let text = json_reader.next_number_as_string().map_err(to_io_error)?;
            canonical_decimal_text(&text).map(Some).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "negotiated_rate cannot be represented by the canonical decimal contract",
                )
            })
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "negotiated_rate must be a JSON number",
        )),
    }
}

pub fn strict_money_number(value: &Value) -> io::Result<String> {
    let Value::Number(number) = value else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "negotiated_rate must be a JSON number",
        ));
    };
    canonical_decimal_text(number.as_str()).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "negotiated_rate cannot be represented by the canonical decimal contract",
        )
    })
}

/// Compare two values produced by the canonical decimal contract without
/// converting them to a bounded numeric type.
pub fn compare_canonical_decimal_text(left: &str, right: &str) -> Ordering {
    let (left_negative, left_magnitude) = match left.strip_prefix('-') {
        Some(magnitude) => (true, magnitude),
        None => (false, left),
    };
    let (right_negative, right_magnitude) = match right.strip_prefix('-') {
        Some(magnitude) => (true, magnitude),
        None => (false, right),
    };
    match (left_negative, right_negative) {
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        _ => {
            let compare_magnitude = |left: &str, right: &str| {
                let (left_integer, left_fraction) = left.split_once('.').unwrap_or((left, ""));
                let (right_integer, right_fraction) = right.split_once('.').unwrap_or((right, ""));
                left_integer
                    .len()
                    .cmp(&right_integer.len())
                    .then_with(|| left_integer.as_bytes().cmp(right_integer.as_bytes()))
                    .then_with(|| left_fraction.as_bytes().cmp(right_fraction.as_bytes()))
            };
            let magnitude_ordering = compare_magnitude(left_magnitude, right_magnitude);
            if left_negative {
                magnitude_ordering.reverse()
            } else {
                magnitude_ordering
            }
        }
    }
}

pub fn strict_string_array_from_reader<R: Read>(
    json_reader: &mut JsonStreamReader<R>,
    field_name: &str,
) -> io::Result<Vec<String>> {
    if json_reader.peek().map_err(to_io_error)? != ValueType::Array {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{field_name} must be an array of strings"),
        ));
    }
    let mut out = Vec::new();
    json_reader.begin_array().map_err(to_io_error)?;
    while json_reader.has_next().map_err(to_io_error)? {
        if json_reader.peek().map_err(to_io_error)? != ValueType::String {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{field_name} elements must be strings"),
            ));
        }
        out.push(json_reader.next_string().map_err(to_io_error)?);
    }
    json_reader.end_array().map_err(to_io_error)?;
    Ok(out)
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
            } else if trimmed.len() == value.len() {
                Some(value)
            } else {
                Some(trimmed.to_owned())
            }
        })
        .collect();
    out.sort_unstable();
    out.dedup();
    out
}

pub fn canonical_modifier_list(values: Vec<String>) -> Vec<String> {
    let mut out = Vec::new();
    for value in values {
        for modifier in value.split(',') {
            let trimmed = modifier.trim();
            if !trimmed.is_empty() {
                out.push(trimmed.to_uppercase());
            }
        }
    }
    out.sort_unstable();
    out.dedup();
    out
}
