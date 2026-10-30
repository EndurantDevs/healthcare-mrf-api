//! Bounded preparation of source address rows without changing database identity rules.

use crate::contact_canon::canonicalize_contact_number;
use crate::network_membership_codec::COPY_HEADER;

pub const CONTRACT: &str = "entity_address_rows.v1";
pub const MAX_ROWS: usize = 5_000;
pub const MAX_INPUT_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_COPY_BYTES: usize = 16 * 1024 * 1024;
const MAX_FIELD_BYTES: usize = 1024 * 1024;
const INPUT_WIDTH: i16 = 14;
const OUTPUT_WIDTH: i16 = 27;

#[derive(Debug, PartialEq, Eq)]
pub struct PreparedRows {
    pub copy_bytes: Vec<u8>,
    pub row_count: usize,
}

/// The first field is a positive bigint ordinal; all remaining input fields are text.
pub fn prepare_entity_address_rows(input: &[u8]) -> Result<PreparedRows, &'static str> {
    if input.len() > MAX_INPUT_BYTES {
        return Err("Address row input exceeds byte limit");
    }
    let mut reader = CopyReader { input, offset: 0 };
    if reader.take(COPY_HEADER.len())? != COPY_HEADER {
        return Err("Address row COPY header differs");
    }
    let mut output = COPY_HEADER.to_vec();
    let (mut row_count, mut previous_id) = (0, 0);
    loop {
        let width = i16::from_be_bytes(reader.take(2)?.try_into().unwrap());
        if width == -1 {
            if reader.offset != input.len() {
                return Err("Unexpected data after address row COPY trailer");
            }
            break;
        }
        if width != INPUT_WIDTH || row_count == MAX_ROWS {
            return Err("Address row COPY width or row limit differs");
        }
        let ordinal = reader.field()?.ok_or("Address row ordinal is null")?;
        let row_id = i64::from_be_bytes(
            ordinal
                .try_into()
                .map_err(|_| "Address row ordinal is not a bigint")?,
        );
        if row_id <= previous_id {
            return Err("Address row ordinals must be positive and strictly increasing");
        }
        previous_id = row_id;
        let mut fields = [None; 13];
        for field in &mut fields {
            *field = reader
                .field()?
                .map(|bytes| {
                    let text =
                        std::str::from_utf8(bytes).map_err(|_| "Address row text is not UTF-8")?;
                    if text.contains('\0') {
                        return Err("Address row text contains a null byte");
                    }
                    Ok(text)
                })
                .transpose()?;
        }
        let prepared = prepare_fields(fields);
        output.extend_from_slice(&OUTPUT_WIDTH.to_be_bytes());
        append_field(&mut output, Some(ordinal))?;
        for value in &prepared.text {
            append_field(&mut output, value.as_deref().map(str::as_bytes))?;
        }
        append_field(&mut output, Some(&[u8::from(prepared.case_safe)]))?;
        append_field(&mut output, Some(&[u8::from(prepared.contact_safe)]))?;
        row_count += 1;
    }
    output.extend_from_slice(&(-1i16).to_be_bytes());
    Ok(PreparedRows {
        copy_bytes: output,
        row_count,
    })
}

struct CopyReader<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> CopyReader<'a> {
    fn take(&mut self, length: usize) -> Result<&'a [u8], &'static str> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or("Address row COPY length overflow")?;
        let value = self
            .input
            .get(self.offset..end)
            .ok_or("Address row COPY is truncated")?;
        self.offset = end;
        Ok(value)
    }

    fn field(&mut self) -> Result<Option<&'a [u8]>, &'static str> {
        let length = i32::from_be_bytes(self.take(4)?.try_into().unwrap());
        if length == -1 {
            return Ok(None);
        }
        if length < 0 || length as usize > MAX_FIELD_BYTES {
            return Err("Address row COPY field length differs");
        }
        self.take(length as usize).map(Some)
    }
}

fn append_field(output: &mut Vec<u8>, value: Option<&[u8]>) -> Result<(), &'static str> {
    let bytes = value.unwrap_or_default();
    if output.len() + 4 + bytes.len() + 2 > MAX_COPY_BYTES {
        return Err("Prepared address rows exceed byte limit");
    }
    let length = value.map_or(-1, |value| value.len() as i32);
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(bytes);
    Ok(())
}

struct PreparedFields {
    text: Vec<Option<String>>,
    case_safe: bool,
    contact_safe: bool,
}

fn prepare_fields(fields: [Option<&str>; 13]) -> PreparedFields {
    let case_safe = [4, 5, 6, 7, 9]
        .iter()
        .all(|index| fields[*index].is_none_or(str::is_ascii));
    let contact_safe = [9, 10, 11]
        .iter()
        .all(|index| fields[*index].is_none_or(str::is_ascii));
    let mut text = fields.map(trim_text).to_vec();
    text[2].get_or_insert_with(|| "primary".to_owned());
    text[9].get_or_insert_with(|| "US".to_owned());
    text[10] = nullish_text(fields[10]);
    text[11] = nullish_text(fields[11]);
    let contacts = if contact_safe {
        let phone = prepare_contact(fields[10], fields[9]);
        let fax = prepare_contact(fields[11], fields[9]);
        [phone.0, phone.1, fax.0, fax.1]
    } else {
        [None, None, None, None]
    };
    let normalization = [
        case_safe.then(|| alnum_norm(text[4].as_deref())).flatten(),
        case_safe.then(|| alnum_norm(text[5].as_deref())).flatten(),
        case_safe.then(|| alnum_norm(text[6].as_deref())).flatten(),
        case_safe.then(|| upper_norm(text[7].as_deref())).flatten(),
        digits(text[8].as_deref(), 5),
        case_safe.then(|| upper_norm(text[9].as_deref())).flatten(),
        digits(text[10].as_deref(), usize::MAX),
    ];
    text.extend(contacts);
    text.extend(normalization);
    PreparedFields {
        text,
        case_safe,
        contact_safe,
    }
}

fn trim_text(value: Option<&str>) -> Option<String> {
    value
        .map(|value| value.trim_matches(' '))
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn nullish_text(value: Option<&str>) -> Option<String> {
    trim_text(value).filter(|value| {
        !matches!(
            value.to_ascii_lowercase().as_str(),
            "null" | "none" | "undefined"
        )
    })
}

fn alnum_norm(value: Option<&str>) -> Option<String> {
    let normalized: String = value
        .unwrap_or_default()
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .map(|ch| ch.to_ascii_lowercase())
        .collect();
    (!normalized.is_empty()).then_some(normalized)
}

fn upper_norm(value: Option<&str>) -> Option<String> {
    trim_text(value).map(|value| value.to_ascii_uppercase())
}

fn digits(value: Option<&str>, limit: usize) -> Option<String> {
    let normalized: String = value
        .unwrap_or_default()
        .chars()
        .filter(char::is_ascii_digit)
        .take(limit)
        .collect();
    (!normalized.is_empty()).then_some(normalized)
}

fn prepare_contact(raw: Option<&str>, country: Option<&str>) -> (Option<String>, Option<String>) {
    let raw = raw.unwrap_or_default();
    let (main, extension) = split_sql_extension(raw);
    let country: String = country
        .unwrap_or_default()
        .chars()
        .filter(char::is_ascii_alphabetic)
        .map(|ch| ch.to_ascii_uppercase())
        .collect();
    let default_us = matches!(
        country.as_str(),
        "" | "US" | "USA" | "UNITEDSTATES" | "UNITEDSTATESOFAMERICA"
    );
    let mut number = String::new();
    if main.trim_matches(' ').starts_with('+') {
        number.push('+');
    }
    number.extend(main.chars().filter(char::is_ascii_digit));
    // The shared primitive receives the SQL-compatible prefix and country classification.
    let canonical =
        canonicalize_contact_number(Some(&number), Some(if default_us { "US" } else { "other" }));
    (canonical.number, extension.map(str::to_owned))
}

fn split_sql_extension(value: &str) -> (&str, Option<&str>) {
    let end = value.trim_end_matches(sql_ascii_space).len();
    let digits_start = value[..end]
        .trim_end_matches(|ch: char| ch.is_ascii_digit())
        .len();
    if !(1..=16).contains(&(end - digits_start)) {
        return (value, None);
    }
    let prefix = value[..digits_start].trim_end_matches(sql_ascii_space);
    for marker in ["extension", "ext.", "ext", ";ext=", "#", "x"] {
        if prefix.len() >= marker.len()
            && prefix[prefix.len() - marker.len()..].eq_ignore_ascii_case(marker)
        {
            let main = prefix[..prefix.len() - marker.len()].trim_end_matches(sql_ascii_space);
            return (main, Some(&value[digits_start..end]));
        }
    }
    (value, None)
}

fn sql_ascii_space(ch: char) -> bool {
    matches!(ch, ' ' | '\t' | '\n' | '\r' | '\x0b' | '\x0c')
}

#[cfg(test)]
#[path = "entity_address_rows_tests.rs"]
mod tests;
