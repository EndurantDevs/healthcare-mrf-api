//! Address-canonical materialization helpers.

use crate::copy_format::{pg_text_copy_field, write_copy_fields};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::sync::OnceLock;

const PUB28_SOURCE: &str = include_str!("../../../process/ext/address_pub28.py");
pub const ADDRESS_IDENTITY_VERSION: u8 = 2;
pub const ADDRESS_IDENTITY_PREFIX: &str = "v2";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalAddress {
    pub address_key: Option<String>,
    pub identity_key: Option<String>,
    pub premise_key: Option<String>,
    pub premise_identity_key: Option<String>,
    pub line1_norm: Option<String>,
    pub unit_norm: String,
    pub city_norm: Option<String>,
    pub state_code: Option<String>,
    pub zip5: Option<String>,
    pub zip4: Option<String>,
    pub country_code: String,
}

#[derive(Debug, Clone)]
struct Pub28Tables {
    suffix: HashMap<String, String>,
    directional: HashMap<String, String>,
    state: HashMap<String, String>,
    unit: HashMap<String, String>,
    unit_no_range: HashSet<String>,
    invalid_unit_values: HashSet<String>,
    unit_keys_by_len: Vec<String>,
}

#[derive(Debug, Clone)]
struct UnitDecision {
    unit: String,
    street_text: String,
}

#[derive(Debug, Clone)]
struct UnitMatch {
    matched: bool,
    unit: String,
    has_value: bool,
    start: usize,
}

fn tables() -> &'static Pub28Tables {
    static TABLES: OnceLock<Pub28Tables> = OnceLock::new();
    TABLES.get_or_init(load_pub28_tables)
}

fn load_pub28_tables() -> Pub28Tables {
    let suffix = parse_py_dict(PUB28_SOURCE, "PUB28_STREET_SUFFIX_MAP");
    let directional = parse_py_dict(PUB28_SOURCE, "PUB28_DIRECTIONAL_MAP");
    let state = parse_py_dict(PUB28_SOURCE, "PUB28_STATE_MAP");
    let unit = parse_py_dict(PUB28_SOURCE, "PUB28_UNIT_DESIGNATOR_MAP");
    let unit_no_range = parse_py_set(PUB28_SOURCE, "PUB28_UNIT_NO_RANGE");
    let invalid_unit_values = parse_py_set(PUB28_SOURCE, "PUB28_INVALID_UNIT_VALUES");
    let mut unit_keys_by_len: Vec<String> = unit.keys().cloned().collect();
    unit_keys_by_len.sort_by(|a, b| b.len().cmp(&a.len()).then_with(|| a.cmp(b)));
    Pub28Tables {
        suffix,
        directional,
        state,
        unit,
        unit_no_range,
        invalid_unit_values,
        unit_keys_by_len,
    }
}

fn parse_py_dict(source: &str, name: &str) -> HashMap<String, String> {
    let body = assignment_body(source, name);
    let mut parser = PyLiteralParser::new(body);
    let mut out = HashMap::new();
    loop {
        parser.skip_noise();
        if parser.done() {
            break;
        }
        let key = parser.parse_string();
        parser.skip_noise();
        parser.expect(':');
        parser.skip_noise();
        let value = parser.parse_string();
        out.insert(key, value);
        parser.skip_noise();
        parser.consume(',');
    }
    out
}

fn parse_py_set(source: &str, name: &str) -> HashSet<String> {
    let body = assignment_body(source, name);
    let mut parser = PyLiteralParser::new(body);
    let mut out = HashSet::new();
    loop {
        parser.skip_noise();
        if parser.done() {
            break;
        }
        out.insert(parser.parse_string());
        parser.skip_noise();
        parser.consume(',');
    }
    out
}

fn assignment_body<'a>(source: &'a str, name: &str) -> &'a str {
    let start = source
        .find(name)
        .unwrap_or_else(|| panic!("missing Pub28 assignment: {name}"));
    let open = source[start..]
        .find('{')
        .map(|offset| start + offset)
        .unwrap_or_else(|| panic!("missing Pub28 body: {name}"));
    let mut depth = 0i32;
    let mut in_string = false;
    let mut escaped = false;
    for (offset, ch) in source[open..].char_indices() {
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '\'' {
                in_string = false;
            }
            continue;
        }
        if ch == '\'' {
            in_string = true;
            continue;
        }
        if ch == '{' {
            depth += 1;
            continue;
        }
        if ch == '}' {
            depth -= 1;
            if depth == 0 {
                return &source[open + 1..open + offset];
            }
        }
    }
    panic!("unterminated Pub28 body: {name}");
}

struct PyLiteralParser<'a> {
    value: &'a str,
    pos: usize,
}

impl<'a> PyLiteralParser<'a> {
    fn new(value: &'a str) -> Self {
        Self { value, pos: 0 }
    }

    fn done(&self) -> bool {
        self.pos >= self.value.len()
    }

    fn skip_noise(&mut self) {
        while let Some(ch) = self.peek() {
            if ch.is_whitespace() {
                self.pos += ch.len_utf8();
            } else {
                break;
            }
        }
    }

    fn peek(&self) -> Option<char> {
        self.value[self.pos..].chars().next()
    }

    fn consume(&mut self, expected: char) -> bool {
        if self.peek() == Some(expected) {
            self.pos += expected.len_utf8();
            true
        } else {
            false
        }
    }

    fn expect(&mut self, expected: char) {
        assert!(
            self.consume(expected),
            "expected {expected:?} in Python literal near {:?}",
            &self.value[self.pos..self.value.len().min(self.pos + 20)]
        );
    }

    fn parse_string(&mut self) -> String {
        self.expect('\'');
        let mut out = String::new();
        while let Some(ch) = self.peek() {
            self.pos += ch.len_utf8();
            if ch == '\'' {
                return out;
            }
            if ch == '\\' {
                let Some(next) = self.peek() else {
                    break;
                };
                self.pos += next.len_utf8();
                out.push(next);
            } else {
                out.push(ch);
            }
        }
        panic!("unterminated Python string literal");
    }
}

fn ascii_alnum_lower(value: &str) -> String {
    value
        .chars()
        .flat_map(|ch| ch.to_lowercase())
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect()
}

fn ascii_letters_upper(value: &str) -> String {
    value
        .chars()
        .filter(|ch| ch.is_ascii_alphabetic())
        .map(|ch| ch.to_ascii_uppercase())
        .collect()
}

fn digits(value: &str) -> String {
    value.chars().filter(|ch| ch.is_ascii_digit()).collect()
}

fn city_norm(value: Option<&str>) -> Option<String> {
    let cleaned = ascii_alnum_lower(value.unwrap_or(""));
    if cleaned.is_empty() {
        None
    } else {
        Some(cleaned)
    }
}

fn state_code(value: Option<&str>) -> Option<String> {
    let cleaned = ascii_letters_upper(value.unwrap_or(""));
    if cleaned.is_empty() {
        return None;
    }
    tables().state.get(&cleaned).cloned()
}

fn zip5_norm(value: Option<&str>) -> Option<String> {
    let cleaned = digits(value.unwrap_or(""));
    match cleaned.len() {
        0..=2 => None,
        3 | 4 => Some(format!("{:0>5}", cleaned)),
        _ => Some(cleaned.chars().take(5).collect()),
    }
}

fn zip4_norm(value: Option<&str>) -> Option<String> {
    let cleaned = digits(value.unwrap_or(""));
    let zip4: String = cleaned.chars().skip(5).take(4).collect();
    if zip4.is_empty() {
        None
    } else {
        Some(zip4)
    }
}

fn country_code(value: Option<&str>) -> String {
    let cleaned = ascii_letters_upper(value.unwrap_or(""));
    if cleaned.is_empty()
        || matches!(
            cleaned.as_str(),
            "US" | "USA" | "UNITEDSTATES" | "UNITEDSTATESOFAMERICA"
        )
    {
        "US".to_string()
    } else {
        cleaned
    }
}

fn unit_prefix(value: &str) -> Option<String> {
    let raw = value.trim().to_ascii_lowercase();
    if raw == "#" {
        return Some("ste".to_string());
    }
    let cleaned = ascii_alnum_lower(&raw);
    tables().unit.get(&cleaned).cloned()
}

fn valid_unit_value(value: &str) -> bool {
    let cleaned = ascii_alnum_lower(value);
    !cleaned.is_empty() && !tables().invalid_unit_values.contains(&cleaned)
}

fn unit_from_parts(prefix_raw: &str, value_raw: Option<&str>) -> String {
    let Some(prefix) = unit_prefix(prefix_raw) else {
        return String::new();
    };
    let cleaned = ascii_alnum_lower(value_raw.unwrap_or(""));
    if !cleaned.is_empty() {
        if !valid_unit_value(&cleaned) {
            return String::new();
        }
        return format!("{prefix}{cleaned}");
    }
    if tables().unit_no_range.contains(&prefix) {
        prefix
    } else {
        String::new()
    }
}

fn unit_remainder_allowed(value: &str) -> bool {
    let mut seen_trailing_whitespace = false;
    for ch in value.chars() {
        if ch.is_whitespace() {
            seen_trailing_whitespace = true;
            continue;
        }
        if !seen_trailing_whitespace && matches!(ch, '.' | ',' | ';' | ':') {
            continue;
        }
        return false;
    }
    true
}

fn parse_unit_at(value: &str, start: usize) -> UnitMatch {
    let tail = &value[start..];
    for key in &tables().unit_keys_by_len {
        if !tail.starts_with(key) {
            continue;
        }
        let mut pos = start + key.len();
        if value[pos..].starts_with('.') {
            pos += 1;
        }
        while let Some(ch) = value[pos..].chars().next() {
            if ch.is_whitespace() {
                pos += ch.len_utf8();
            } else {
                break;
            }
        }
        if value[pos..].starts_with('#') {
            pos += 1;
            while let Some(ch) = value[pos..].chars().next() {
                if ch.is_whitespace() {
                    pos += ch.len_utf8();
                } else {
                    break;
                }
            }
        }
        let value_start = pos;
        if let Some(ch) = value[pos..].chars().next() {
            if ch.is_ascii_alphanumeric() {
                pos += ch.len_utf8();
                while let Some(ch) = value[pos..].chars().next() {
                    if ch.is_ascii_alphanumeric() || ch == '-' {
                        pos += ch.len_utf8();
                    } else {
                        break;
                    }
                }
            }
        }
        let unit_value = if pos > value_start {
            Some(&value[value_start..pos])
        } else {
            None
        };
        if !unit_remainder_allowed(&value[pos..]) {
            continue;
        }
        return UnitMatch {
            matched: true,
            unit: unit_from_parts(key, unit_value),
            has_value: unit_value.is_some(),
            start,
        };
    }
    UnitMatch {
        matched: false,
        unit: String::new(),
        has_value: false,
        start,
    }
}

fn parse_full_unit(value: &str) -> UnitMatch {
    let trimmed = value.trim_start();
    let start = value.len() - trimmed.len();
    parse_unit_at(value, start)
}

fn parse_tail_unit(value: &str) -> UnitMatch {
    for (idx, ch) in value.char_indices() {
        if idx != 0 && !ch.is_whitespace() && ch != ',' {
            continue;
        }
        let start = idx + ch.len_utf8();
        let candidate = parse_unit_at(value, start);
        if candidate.matched {
            return UnitMatch {
                start: idx,
                ..candidate
            };
        }
    }
    UnitMatch {
        matched: false,
        unit: String::new(),
        has_value: false,
        start: 0,
    }
}

fn unit_decision(line1: Option<&str>, line2: Option<&str>) -> UnitDecision {
    let l1 = line1.unwrap_or("").to_lowercase();
    let l2 = line2.unwrap_or("").to_lowercase();
    let line2_match = parse_full_unit(&l2);
    if line2_match.matched {
        if !line2_match.unit.is_empty() {
            return UnitDecision {
                unit: line2_match.unit,
                street_text: format!(" {l1} "),
            };
        }
        return UnitDecision {
            unit: String::new(),
            street_text: format!(" {l1} {l2} "),
        };
    }

    let joined = format!(" {l1} {l2} ");
    let tail = parse_tail_unit(&joined);
    if tail.matched && !tail.unit.is_empty() && (tail.has_value || l2.trim().is_empty()) {
        return UnitDecision {
            unit: tail.unit,
            street_text: joined[..tail.start].to_string(),
        };
    }
    UnitDecision {
        unit: String::new(),
        street_text: joined,
    }
}

fn street_token_norm(value: &str) -> String {
    let token = ascii_alnum_lower(value);
    if token.is_empty() {
        return String::new();
    }
    if let Some(ordinal) = numeric_ordinal_value(&token) {
        return ordinal;
    }
    if token == "saint" {
        return "st".to_string();
    }
    let mapped = tables()
        .directional
        .get(&token)
        .or_else(|| tables().suffix.get(&token))
        .cloned()
        .unwrap_or(token);
    tables()
        .directional
        .get(&mapped)
        .or_else(|| tables().suffix.get(&mapped))
        .cloned()
        .unwrap_or(mapped)
}

fn numeric_ordinal_value(token: &str) -> Option<String> {
    let suffix = ["st", "nd", "rd", "th"]
        .iter()
        .find(|suffix| token.ends_with(**suffix))?;
    let digits = &token[..token.len() - suffix.len()];
    if digits.is_empty() || !digits.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }
    let trimmed = digits.trim_start_matches('0');
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn ordinal_h_typo_value(token: &str) -> Option<String> {
    let digits = token.strip_suffix('h')?;
    if digits.is_empty() || !digits.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }
    let trimmed = digits.trim_start_matches('0');
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn ordinal_word_value(token: &str) -> Option<&'static str> {
    match token {
        "first" => Some("1"),
        "second" => Some("2"),
        "third" => Some("3"),
        "fourth" => Some("4"),
        "fifth" => Some("5"),
        "sixth" => Some("6"),
        "seventh" => Some("7"),
        "eighth" => Some("8"),
        "ninth" => Some("9"),
        "tenth" => Some("10"),
        "eleventh" => Some("11"),
        "twelfth" => Some("12"),
        "thirteenth" => Some("13"),
        "fourteenth" => Some("14"),
        "fifteenth" => Some("15"),
        "sixteenth" => Some("16"),
        "seventeenth" => Some("17"),
        "eighteenth" => Some("18"),
        "nineteenth" => Some("19"),
        "twentieth" => Some("20"),
        "thirtieth" => Some("30"),
        _ => None,
    }
}

fn street_token_is_suffix(value: &str) -> bool {
    let token = ascii_alnum_lower(value);
    tables().suffix.contains_key(&token)
}

fn street_token_norm_context(token: &str, next_token: Option<&str>) -> String {
    let cleaned = ascii_alnum_lower(token);
    if cleaned.is_empty() {
        return String::new();
    }
    if next_token.is_some_and(street_token_is_suffix) {
        if let Some(value) = ordinal_word_value(&cleaned) {
            return value.to_string();
        }
        if let Some(value) = ordinal_h_typo_value(&cleaned) {
            return value;
        }
    }
    street_token_norm(&cleaned)
}

fn replace_po_box(raw: &str) -> String {
    let tokens: Vec<String> = raw
        .to_lowercase()
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(|token| token.to_string())
        .collect();
    let mut out: Vec<String> = Vec::with_capacity(tokens.len());
    let mut idx = 0usize;
    while idx < tokens.len() {
        if idx + 2 < tokens.len()
            && tokens[idx] == "p"
            && tokens[idx + 1] == "o"
            && tokens[idx + 2] == "box"
        {
            out.push("pobox".to_string());
            idx += 3;
        } else if tokens[idx] == "pob" {
            out.push("pobox".to_string());
            idx += 1;
        } else {
            out.push(tokens[idx].clone());
            idx += 1;
        }
    }
    out.join(" ")
}

fn street_norm(line1: Option<&str>, line2: Option<&str>) -> Option<String> {
    let raw = replace_po_box(&unit_decision(line1, line2).street_text);
    let mut tokens = Vec::new();
    let mut token = String::new();
    for ch in raw.chars().flat_map(|ch| ch.to_lowercase()) {
        if ch.is_ascii_alphanumeric() {
            token.push(ch);
        } else if !token.is_empty() {
            tokens.push(std::mem::take(&mut token));
            token.clear();
        }
    }
    if !token.is_empty() {
        tokens.push(token);
    }
    let mut out = String::new();
    for idx in 0..tokens.len() {
        out.push_str(&street_token_norm_context(
            &tokens[idx],
            tokens.get(idx + 1).map(|value| value.as_str()),
        ));
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

fn unit_norm(line1: Option<&str>, line2: Option<&str>) -> String {
    unit_decision(line1, line2).unit
}

fn key_from_identity(identity: Option<&str>) -> Option<String> {
    let identity = identity?;
    let digest = Sha256::digest(identity.as_bytes());
    Some(format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        digest[0],
        digest[1],
        digest[2],
        digest[3],
        digest[4],
        digest[5],
        digest[6],
        digest[7],
        digest[8],
        digest[9],
        digest[10],
        digest[11],
        digest[12],
        digest[13],
        digest[14],
        digest[15],
    ))
}

fn identity_key_v1(
    first_line: Option<&str>,
    second_line: Option<&str>,
    city: Option<&str>,
    state: Option<&str>,
    zip: Option<&str>,
    country: Option<&str>,
) -> Option<String> {
    let street = street_norm(first_line, second_line);
    let unit = unit_norm(first_line, second_line);
    let city_value = city_norm(city);
    let state_value = state_code(state)?;
    let zip_value = zip5_norm(zip)?;
    let country_value = country_code(country);
    if country_value != "US" {
        return None;
    }
    let has_street = street.is_some();
    let precision = if has_street {
        "street"
    } else if city_value.is_some() {
        "city_zip"
    } else {
        return None;
    };
    let identity_city = if has_street {
        String::new()
    } else {
        city_value.unwrap_or_default()
    };
    Some(format!(
        "{}|{}|{}|{}|{}|{}|{}|{}",
        ADDRESS_IDENTITY_PREFIX,
        street.unwrap_or_default(),
        unit,
        identity_city,
        state_value,
        zip_value,
        country_value,
        precision
    ))
}

fn premise_identity_key_v1(
    first_line: Option<&str>,
    second_line: Option<&str>,
    _city: Option<&str>,
    state: Option<&str>,
    zip: Option<&str>,
    country: Option<&str>,
) -> Option<String> {
    let street = street_norm(first_line, second_line)?;
    let state_value = state_code(state)?;
    let zip_value = zip5_norm(zip)?;
    let country_value = country_code(country);
    if country_value != "US" {
        return None;
    }
    Some(format!(
        "{}|{}|||{}|{}|{}|street",
        ADDRESS_IDENTITY_PREFIX, street, state_value, zip_value, country_value
    ))
}

pub fn pub28_sha256() -> String {
    let mut hasher = Sha256::new();
    hasher.update(PUB28_SOURCE.as_bytes());
    format!("{:x}", hasher.finalize())
}

pub fn canon_version_json() -> String {
    serde_json::json!({
        "identity_version": ADDRESS_IDENTITY_VERSION,
        "identity_prefix": ADDRESS_IDENTITY_PREFIX,
        "pub28_sha256": pub28_sha256(),
    })
    .to_string()
}

pub fn canonicalize_address(
    first_line: Option<&str>,
    second_line: Option<&str>,
    city: Option<&str>,
    state: Option<&str>,
    zip: Option<&str>,
    country: Option<&str>,
) -> CanonicalAddress {
    let identity_key = identity_key_v1(first_line, second_line, city, state, zip, country);
    let premise_identity_key =
        premise_identity_key_v1(first_line, second_line, city, state, zip, country);
    CanonicalAddress {
        address_key: key_from_identity(identity_key.as_deref()),
        identity_key,
        premise_key: key_from_identity(premise_identity_key.as_deref()),
        premise_identity_key,
        line1_norm: street_norm(first_line, second_line),
        unit_norm: unit_norm(first_line, second_line),
        city_norm: city_norm(city),
        state_code: state_code(state).map(|value| value.chars().take(32).collect()),
        zip5: zip5_norm(zip),
        zip4: zip4_norm(zip),
        country_code: country_code(country),
    }
}

fn decode_copy_field(value: &str) -> Option<String> {
    if value == "\\N" {
        return None;
    }
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        match chars.next() {
            Some('t') => out.push('\t'),
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('\\') => out.push('\\'),
            Some(other) => out.push(other),
            None => out.push('\\'),
        }
    }
    Some(out)
}

fn copy_field_ref(value: &Option<String>) -> Option<&str> {
    value.as_deref()
}

fn canonicalize_copy_line(line: &str) -> io::Result<Vec<String>> {
    let fields: Vec<Option<String>> = line
        .trim_end_matches(['\r', '\n'])
        .split('\t')
        .map(decode_copy_field)
        .collect();
    if fields.len() != 9 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "address canonical COPY row has {} fields, expected 9",
                fields.len()
            ),
        ));
    }
    let rn = fields[0].clone();
    let source_ctid = fields[1].clone();
    let staged_address_key = fields[2].clone();
    let first_line = fields[3].clone();
    let second_line = fields[4].clone();
    let city_name = fields[5].clone();
    let state_name = fields[6].clone();
    let postal_code = fields[7].clone();
    let country = fields[8].clone();
    let canonical = canonicalize_address(
        copy_field_ref(&first_line),
        copy_field_ref(&second_line),
        copy_field_ref(&city_name),
        copy_field_ref(&state_name),
        copy_field_ref(&postal_code),
        copy_field_ref(&country),
    );
    let address_key = staged_address_key
        .clone()
        .or_else(|| canonical.address_key.clone());
    Ok(vec![
        pg_text_copy_field(copy_field_ref(&rn)),
        pg_text_copy_field(copy_field_ref(&source_ctid)),
        pg_text_copy_field(copy_field_ref(&staged_address_key)),
        pg_text_copy_field(address_key.as_deref()),
        pg_text_copy_field(canonical.address_key.as_deref()),
        pg_text_copy_field(canonical.identity_key.as_deref()),
        pg_text_copy_field(canonical.premise_key.as_deref()),
        pg_text_copy_field(canonical.line1_norm.as_deref()),
        pg_text_copy_field(Some(&canonical.unit_norm)),
        pg_text_copy_field(canonical.city_norm.as_deref()),
        pg_text_copy_field(canonical.state_code.as_deref()),
        pg_text_copy_field(canonical.zip5.as_deref()),
        pg_text_copy_field(canonical.zip4.as_deref()),
        pg_text_copy_field(Some(&canonical.country_code)),
        pg_text_copy_field(copy_field_ref(&first_line)),
        pg_text_copy_field(copy_field_ref(&second_line)),
        pg_text_copy_field(copy_field_ref(&city_name)),
        pg_text_copy_field(copy_field_ref(&state_name)),
        pg_text_copy_field(copy_field_ref(&postal_code)),
    ])
}

pub fn canonicalize_copy_file(input_path: &Path, output_path: &Path) -> io::Result<()> {
    let input = BufReader::new(File::open(input_path)?);
    let output = File::create(output_path)?;
    let mut writer = BufWriter::new(output);
    for line in input.lines() {
        let line = line?;
        let fields = canonicalize_copy_line(&line)?;
        write_copy_fields(&mut writer, &fields)?;
    }
    writer.flush()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    const GOLDEN: &str = include_str!("../../../tests/fixtures/address_canonical_golden.json");

    #[test]
    fn canonicalizes_explicit_golden_cases() {
        let payload: Value = serde_json::from_str(GOLDEN).unwrap();
        let cases = payload["explicit_cases"].as_array().unwrap();
        assert!(cases.len() >= 270);
        for case in cases {
            let get = |key: &str| case.get(key).and_then(|value| value.as_str());
            let result = canonicalize_address(
                get("first_line"),
                get("second_line"),
                get("city"),
                get("state"),
                get("zip"),
                get("country"),
            );
            assert_eq!(
                result.identity_key.as_deref(),
                get("expected_identity_key"),
                "{}",
                get("id").unwrap_or("<missing>")
            );
            assert_eq!(
                result.address_key.as_deref(),
                get("expected_address_key"),
                "{}",
                get("id").unwrap_or("<missing>")
            );
            assert_eq!(
                result.premise_identity_key.as_deref(),
                get("expected_premise_identity_key"),
                "{}",
                get("id").unwrap_or("<missing>")
            );
            assert_eq!(
                result.premise_key.as_deref(),
                get("expected_premise_key"),
                "{}",
                get("id").unwrap_or("<missing>")
            );
        }
    }

    #[test]
    fn exposes_canonical_version_payload() {
        let payload: Value = serde_json::from_str(&canon_version_json()).unwrap();
        assert_eq!(payload["identity_version"], ADDRESS_IDENTITY_VERSION);
        assert_eq!(payload["identity_prefix"], ADDRESS_IDENTITY_PREFIX);
        assert_eq!(payload["pub28_sha256"].as_str().unwrap().len(), 64);
    }

    #[test]
    fn canonicalizes_postgres_copy_line() {
        let line =
            "1\t(0,1)\t\\N\t27 Dr Mellichamp Dr\u{00a0}Ste 100\t\\N\tBLUFFTON\tSC\t29910\tUS";
        let fields = canonicalize_copy_line(line).unwrap();
        assert_eq!(fields[0], "1");
        assert_eq!(fields[1], "(0,1)");
        assert_eq!(fields[3], "3e3ea29f-8c26-17ba-dcc8-74424e66fd32");
        assert_eq!(fields[5], "v2|27drmellichampdr|ste100||SC|29910|US|street");
    }

    #[test]
    fn canonicalizes_ordinal_and_saint_tokens() {
        assert_eq!(
            street_norm(Some("200 14 St"), None),
            street_norm(Some("200 14th St"), None)
        );
        assert_eq!(
            street_norm(Some("200 14h St"), None),
            street_norm(Some("200 14th St"), None)
        );
        assert_eq!(
            street_norm(Some("200 First Ave"), None),
            street_norm(Some("200 1st Ave"), None)
        );
        assert_eq!(
            street_norm(Some("200 Saint Clair Ave"), None),
            street_norm(Some("200 St Clair Ave"), None)
        );
        assert_ne!(
            street_norm(Some("14H Main St"), None),
            street_norm(Some("14 Main St"), None)
        );
    }

    #[test]
    fn preserves_unit_like_city_zip_values() {
        let dept_1234 = canonicalize_address(
            Some("DEPARTMENT 1234"),
            Some(""),
            Some("KNOXVILLE"),
            Some("TN"),
            Some("37995"),
            Some("US"),
        );
        let dept_5678 = canonicalize_address(
            Some("DEPARTMENT 5678"),
            Some(""),
            Some("KNOXVILLE"),
            Some("TN"),
            Some("37995"),
            Some("US"),
        );

        assert_eq!(
            dept_1234.identity_key.as_deref(),
            Some("v2||dept1234|knoxville|TN|37995|US|city_zip")
        );
        assert_eq!(
            dept_5678.identity_key.as_deref(),
            Some("v2||dept5678|knoxville|TN|37995|US|city_zip")
        );
        assert_ne!(dept_1234.address_key, dept_5678.address_key);
    }

    #[test]
    fn pads_short_zip_and_rejects_unknown_state() {
        let padded_zip = canonicalize_address(
            Some("1 Main St"),
            Some(""),
            Some("Cambridge"),
            Some("MA"),
            Some("2138"),
            Some("US"),
        );
        let unknown_state = canonicalize_address(
            Some("1 Main St"),
            Some(""),
            Some("Los Angeles"),
            Some("calif"),
            Some("90001"),
            Some("US"),
        );

        assert_eq!(
            padded_zip.identity_key.as_deref(),
            Some("v2|1mainst|||MA|02138|US|street")
        );
        assert!(unknown_state.identity_key.is_none());
        assert!(unknown_state.address_key.is_none());
    }

    #[test]
    fn canonicalizes_floor_hyphen_actual_npi_case_like_python_and_sql() {
        let result = canonicalize_address(
            Some("519 S. ROSELLE RD."),
            Some("2ND FLOOR-PULMANARY"),
            Some("SCHAUMBURG"),
            Some("IL"),
            Some("601932925"),
            Some("US"),
        );
        assert_eq!(
            result.identity_key.as_deref(),
            Some("v2|519srosellerd2|floorpulmanary||IL|60193|US|street")
        );
        assert_eq!(
            result.address_key.as_deref(),
            Some("241ff8cf-88c7-48b8-746f-031f75d8de8c")
        );
    }

    #[test]
    fn rejects_unit_value_with_whitespace_before_trailing_punctuation_like_python_and_sql() {
        let result = canonicalize_address(
            Some("4344 CONVOY ST."),
            Some("STE T ."),
            Some("SAN DIEGO"),
            Some("CA"),
            Some("921113737"),
            Some("US"),
        );
        assert_eq!(
            result.identity_key.as_deref(),
            Some("v2|4344convoyststet|||CA|92111|US|street")
        );
        assert_eq!(
            result.address_key.as_deref(),
            Some("df8e3c6e-cf4e-688c-c629-2cfc5c3be84a")
        );
    }
}
