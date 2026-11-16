//! SQL-v2 display labels for portable address inputs; other rows keep SQL rendering.

use super::{
    canonicalize_address, parse_py_dict, parse_py_set, street_norm_from_text,
    strip_duplicate_tail_unit, tables, unit_decision, CanonicalAddress,
};
use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

const DISPLAY_SOURCE: &str = include_str!("../../../process/ext/address_format_constants.py");
const MAX_LENGTH: usize = 1024;

pub struct PreparedAddress {
    pub canonical: CanonicalAddress,
    pub formatted_address: Option<String>,
    pub entity: crate::entity_address_rows::AddressGroupFields,
}

struct DisplayTables {
    suffix: HashMap<String, String>,
    unit: HashMap<String, String>,
    unit_prefixes: Vec<String>,
    acronyms: HashSet<String>,
    ambiguous_units: HashSet<String>,
}

fn display_tables() -> &'static DisplayTables {
    static DISPLAY: OnceLock<DisplayTables> = OnceLock::new();
    DISPLAY.get_or_init(|| {
        let unit = parse_py_dict(DISPLAY_SOURCE, "ADDRESS_UNIT_PREFIX_DISPLAY");
        let mut unit_prefixes: Vec<_> = unit.keys().cloned().collect();
        unit_prefixes.sort_by(|a, b| b.len().cmp(&a.len()).then_with(|| a.cmp(b)));
        DisplayTables {
            suffix: parse_py_dict(DISPLAY_SOURCE, "_STREET_SUFFIX_DISPLAY"),
            unit,
            unit_prefixes,
            acronyms: parse_py_set(DISPLAY_SOURCE, "_ADDRESS_ACRONYMS"),
            ambiguous_units: parse_py_set(DISPLAY_SOURCE, "_AMBIGUOUS_UNIT_WORDS"),
        }
    })
}

pub fn prepare_address(fields: [Option<&str>; 6]) -> Option<PreparedAddress> {
    let raw_fields = fields;
    let mut normalized = [None, None, None, None, None, None];
    for (target, source) in normalized.iter_mut().zip(fields) {
        *target = match source {
            Some(value) => Some(normalize_portable_input(value)?),
            None => None,
        };
    }
    let fields = normalized.each_ref().map(|value| value.as_deref());
    let [first, second, city, state, postal, country] = fields;
    Some(PreparedAddress {
        canonical: canonicalize_address(first, second, city, state, postal, country),
        formatted_address: render(fields),
        entity: crate::entity_address_rows::prepare_address_group_fields(raw_fields),
    })
}

fn normalize_portable_input(value: &str) -> Option<String> {
    let mut normalized = String::with_capacity(value.len());
    let mut space_pending = false;
    for character in value.chars() {
        match character {
            // These spaces are normalized by both the canonical and display SQL contracts.
            '\t'..='\r'
            | ' '
            | '\u{a0}'
            | '\u{2000}'..='\u{2007}'
            | '\u{2009}'
            | '\u{200a}'
            | '\u{202f}' => space_pending = !normalized.is_empty(),
            '!'..='~' => {
                if space_pending {
                    normalized.push(' ');
                    space_pending = false;
                }
                normalized.push(character);
            }
            _ => return None,
        }
    }
    Some(normalized)
}

fn clean(value: Option<&str>) -> Option<String> {
    let cleaned = value
        .unwrap_or_default()
        .split(|character| matches!(character, '\t'..='\r' | ' '))
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    (!cleaned.is_empty()).then_some(cleaned)
}

fn trim_punctuation(value: &str) -> Option<String> {
    let trimmed = value.trim_end_matches([' ', ',', ';', ':']);
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}

fn is_word(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn compound_directions(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = String::with_capacity(value.len());
    let mut index = 0;
    while index < bytes.len() {
        let first = bytes[index].to_ascii_uppercase();
        let mut next = index + 1;
        if matches!(first, b'N' | b'S') && (index == 0 || !is_word(bytes[index - 1])) {
            while next < bytes.len() && bytes[next] == b' ' {
                next += 1;
            }
            if next < bytes.len() && bytes[next] == b'.' {
                next += 1;
                while next < bytes.len() && bytes[next] == b' ' {
                    next += 1;
                }
            } else if next == index + 1 {
                next = bytes.len();
            }
            if next < bytes.len()
                && matches!(bytes[next].to_ascii_uppercase(), b'E' | b'W')
                && (next + 1 == bytes.len() || !is_word(bytes[next + 1]))
            {
                out.push(char::from(bytes[index]));
                out.push(char::from(bytes[next]));
                index = next + 1;
                continue;
            }
        }
        out.push(char::from(bytes[index]));
        index += 1;
    }
    out
}

fn ordinal(value: &str) -> bool {
    value.len() > 2
        && value[..value.len() - 2]
            .bytes()
            .all(|byte| byte.is_ascii_digit())
        && matches!(
            &value[value.len() - 2..].to_ascii_uppercase()[..],
            "ST" | "ND" | "RD" | "TH"
        )
}

fn safe_mixed_case(value: &str) -> bool {
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if !bytes[index].is_ascii_uppercase() {
            return false;
        }
        index += 1;
        let start = index;
        while index < bytes.len() && bytes[index].is_ascii_lowercase() {
            index += 1;
        }
        if index == start {
            return false;
        }
    }
    !value.is_empty()
}

fn title_word(value: &str) -> String {
    let mut bytes = value.to_ascii_lowercase().into_bytes();
    if let Some(first) = bytes.first_mut() {
        first.make_ascii_uppercase();
    }
    String::from_utf8(bytes).expect("portable display text is ASCII")
}

fn human_word(value: &str) -> String {
    let upper = value.to_ascii_uppercase();
    if ordinal(value) {
        return upper[..upper.len() - 2].to_owned()
            + &upper[upper.len() - 2..].to_ascii_lowercase();
    }
    if value.bytes().any(|byte| byte.is_ascii_digit())
        && value.bytes().any(|byte| byte.is_ascii_alphabetic())
        || display_tables().acronyms.contains(&upper)
    {
        return upper;
    }
    if safe_mixed_case(value) {
        return value.to_owned();
    }
    title_word(value)
}

fn tokens(value: &str) -> Vec<&str> {
    let bytes = value.as_bytes();
    let mut result = Vec::new();
    let mut start = 0;
    for index in 1..bytes.len() {
        if bytes[index].is_ascii_alphanumeric() != bytes[start].is_ascii_alphanumeric() {
            result.push(&value[start..index]);
            start = index;
        }
    }
    if !value.is_empty() {
        result.push(&value[start..]);
    }
    result
}

fn direction(value: &str) -> Option<&'static str> {
    match value {
        "N" => Some("North"),
        "S" => Some("South"),
        "E" => Some("East"),
        "W" => Some("West"),
        "NE" => Some("NE"),
        "NW" => Some("NW"),
        "SE" => Some("SE"),
        "SW" => Some("SW"),
        _ => None,
    }
}

fn display_suffix(value: &str) -> Option<&'static str> {
    let canonical = tables().suffix.get(&value.to_ascii_lowercase())?;
    display_tables()
        .suffix
        .get(&canonical.to_ascii_uppercase())
        .map(String::as_str)
}

fn render_text(value: &str, kind: &str, us_style: bool) -> String {
    let parts = tokens(value);
    let first_alpha = parts
        .iter()
        .position(|part| part.bytes().any(|byte| byte.is_ascii_alphabetic()));
    let suffix = parts.iter().rposition(|part| {
        part.len() > 1
            && part.bytes().any(|byte| byte.is_ascii_alphabetic())
            && direction(&part.to_ascii_uppercase()).is_none()
    });
    let mut out = String::new();
    let mut drop_period = false;
    for (index, part) in parts.iter().enumerate() {
        if !part.bytes().any(|byte| byte.is_ascii_alphanumeric()) {
            let stripped = part.trim_start_matches(' ');
            if drop_period && stripped.starts_with('.') {
                out.push_str(&part[..part.len() - stripped.len()]);
                out.push_str(&stripped[1..]);
                drop_period = false;
            } else {
                out.push_str(part);
                drop_period = drop_period && part.trim_matches(' ').is_empty();
            }
            continue;
        }
        if Some(index) != suffix {
            drop_period = false;
        }
        if us_style && matches!(kind, "line1" | "line2") && Some(index) == suffix {
            drop_period = false;
        }
        let upper = part.to_ascii_uppercase();
        let special = if us_style && matches!(kind, "line1" | "line2") {
            (Some(index) == suffix)
                .then(|| display_suffix(part))
                .flatten()
                .or_else(|| {
                    (Some(index) == first_alpha || suffix.is_some_and(|position| index > position))
                        .then(|| direction(&upper))
                        .flatten()
                })
        } else if us_style && kind == "city" && Some(index) == first_alpha {
            direction(&upper).or(match upper.as_str() {
                "FT" => Some("Fort"),
                "MT" => Some("Mount"),
                _ => None,
            })
        } else {
            None
        };
        if let Some(rendered) = special {
            out.push_str(rendered);
            drop_period = true;
        } else {
            out.push_str(&human_word(part));
        }
    }
    out
}

fn normalize_box_hash(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = String::with_capacity(value.len());
    let mut index = 0;
    while index < bytes.len() {
        if index + 3 <= bytes.len()
            && value[index..index + 3].eq_ignore_ascii_case("BOX")
            && (index == 0 || !is_word(bytes[index - 1]))
            && (index + 3 == bytes.len() || !is_word(bytes[index + 3]))
        {
            let mut next = index + 3;
            while next < bytes.len() && bytes[next] == b' ' {
                next += 1;
            }
            if next < bytes.len() && bytes[next] == b'#' {
                next += 1;
                while next < bytes.len() && bytes[next] == b' ' {
                    next += 1;
                }
                if next < bytes.len() && bytes[next].is_ascii_alphanumeric() {
                    out.push_str("BOX ");
                    out.push(char::from(bytes[next]));
                    index = next + 1;
                    continue;
                }
            }
        }
        out.push(char::from(bytes[index]));
        index += 1;
    }
    out
}

fn normalize_po_box(value: &str) -> String {
    let bytes = value.as_bytes();
    let upper = value.to_ascii_uppercase();
    let mut next = if upper.starts_with("POST OFFICE ") {
        12
    } else {
        if !upper.starts_with('P') {
            return value.to_owned();
        }
        let mut index = 1;
        while index < bytes.len() && bytes[index] == b' ' {
            index += 1;
        }
        if bytes.get(index) == Some(&b'.') {
            index += 1;
        }
        while index < bytes.len() && bytes[index] == b' ' {
            index += 1;
        }
        if bytes.get(index).map(u8::to_ascii_uppercase) != Some(b'O') {
            return value.to_owned();
        }
        index += 1;
        if bytes.get(index) == Some(&b'.') {
            index += 1;
        }
        if bytes.get(index) != Some(&b' ') {
            return value.to_owned();
        }
        while index < bytes.len() && bytes[index] == b' ' {
            index += 1;
        }
        index
    };
    if !upper[next..].starts_with("BOX") {
        return value.to_owned();
    }
    next += 3;
    if bytes.get(next) == Some(&b'.') {
        next += 1;
    }
    let separator = next;
    while next < bytes.len() && bytes[next] == b' ' {
        next += 1;
    }
    if bytes.get(next) == Some(&b'#') {
        next += 1;
        while next < bytes.len() && bytes[next] == b' ' {
            next += 1;
        }
    } else if next == separator && next != bytes.len() {
        return value.to_owned();
    }
    format!("PO Box {}", &value[next..]).trim().to_owned()
}

fn separate_unit_marker(value: &str) -> String {
    let lower = value.to_ascii_lowercase();
    let bytes = value.as_bytes();
    let mut out = String::with_capacity(value.len());
    let mut index = 0;
    while index < bytes.len() {
        if (index == 0 || matches!(bytes[index - 1], b' ' | b','))
            && let Some(prefix) = tables().unit_keys_by_len.iter().find(|prefix| {
                let end = index + prefix.len();
                lower[index..].starts_with(prefix.as_str())
                    && end + 1 < bytes.len()
                    && matches!(bytes[end], b'-' | b'/')
                    && (bytes[end + 1].is_ascii_alphanumeric() || bytes[end + 1] == b'#')
            })
        {
            out.push_str(&value[index..index + prefix.len()]);
            out.push(' ');
            index += prefix.len() + 1;
            continue;
        }
        out.push(char::from(bytes[index]));
        index += 1;
    }
    out
}

struct UnitSource<'a> {
    prefix: &'a str,
    value: Option<&'a str>,
    boundary: u8,
    glued: bool,
}

fn unit_source(value: &str) -> Option<UnitSource<'_>> {
    let lower = value.to_ascii_lowercase();
    let bytes = value.as_bytes();
    for start in 0..bytes.len() {
        if start > 0 && !matches!(bytes[start - 1], b' ' | b',') {
            continue;
        }
        for prefix in &tables().unit_keys_by_len {
            if !lower[start..].starts_with(prefix.as_str()) {
                continue;
            }
            let prefix_end = start + prefix.len();
            let mut next = prefix_end;
            if bytes.get(next) == Some(&b'.') {
                next += 1;
            }
            while bytes.get(next) == Some(&b' ') {
                next += 1;
            }
            if bytes.get(next) == Some(&b'#') {
                next += 1;
                while bytes.get(next) == Some(&b' ') {
                    next += 1;
                }
            }
            let begin = next;
            if bytes.get(next).is_some_and(u8::is_ascii_alphanumeric) {
                next += 1;
                while bytes
                    .get(next)
                    .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'-')
                {
                    next += 1;
                }
            }
            let mut end = next;
            while bytes.get(next) == Some(&b' ') {
                next += 1;
            }
            if next > end && bytes.get(next).is_some_and(u8::is_ascii_alphanumeric) {
                next += 1;
                end = next;
            }
            while bytes
                .get(next)
                .is_some_and(|byte| matches!(byte, b'.' | b',' | b';' | b':'))
            {
                next += 1;
            }
            while bytes.get(next) == Some(&b' ') {
                next += 1;
            }
            if next == bytes.len() {
                return Some(UnitSource {
                    prefix: &value[start..prefix_end],
                    value: (end > begin).then_some(&value[begin..end]),
                    boundary: if start == 0 { b' ' } else { bytes[start - 1] },
                    glued: prefix_end == begin,
                });
            }
        }
    }
    None
}

fn unit_value_display(value: &str) -> String {
    let upper = value.to_ascii_uppercase();
    if ordinal(value)
        || safe_mixed_case(value)
        || display_tables().acronyms.contains(&upper)
        || value.bytes().any(|byte| byte.is_ascii_digit())
            && value.bytes().any(|byte| byte.is_ascii_alphabetic())
    {
        return human_word(value);
    }
    tokens(value)
        .into_iter()
        .map(|part| {
            if part.bytes().any(|byte| byte.is_ascii_alphanumeric()) {
                title_word(part)
            } else {
                part.to_owned()
            }
        })
        .collect()
}

fn tail_unit_display(value: &str, line_one: bool) -> Option<(Option<String>, String)> {
    let parse_value = separate_unit_marker(value);
    let normalized = if line_one {
        unit_decision(Some(&parse_value), None).unit
    } else {
        unit_decision(None, Some(&parse_value)).unit
    };
    let prefix = display_tables()
        .unit_prefixes
        .iter()
        .find(|prefix| normalized.starts_with(prefix.as_str()))?;
    let raw_value = &normalized[prefix.len()..];
    let source = unit_source(&parse_value);
    if raw_value.bytes().all(|byte| byte.is_ascii_alphabetic())
        && !raw_value.is_empty()
        && source
            .as_ref()
            .is_some_and(|item| item.glued && item.prefix != "#")
    {
        return None;
    }
    let stripped = strip_duplicate_tail_unit(
        format!(" {} ", parse_value.to_ascii_lowercase()),
        &normalized,
    );
    let stripped = stripped.trim_matches([' ', '\t', '\n', '\r', ',', ';', ':']);
    let street = (!stripped.is_empty()).then(|| {
        value[..stripped.len().min(value.len())]
            .trim_matches([' ', ',', ';', ':'])
            .to_owned()
    });
    if line_one
        && source.as_ref().is_some_and(|item| {
            display_tables()
                .ambiguous_units
                .contains(&item.prefix.to_ascii_lowercase())
                && item.boundary != b','
        })
        && !has_street_suffix(street.as_deref().unwrap_or_default())
    {
        return None;
    }
    let source_value = source
        .as_ref()
        .and_then(|item| item.value)
        .unwrap_or(raw_value);
    let display = &display_tables().unit[prefix];
    let unit = if source_value.is_empty() {
        display.clone()
    } else {
        format!("{display} {}", unit_value_display(source_value))
    };
    Some((street.filter(|text| !text.is_empty()), unit))
}

fn has_street_suffix(value: &str) -> bool {
    let mut parts: Vec<_> = value.split_whitespace().collect();
    if parts
        .last()
        .is_some_and(|part| direction(&part.trim_end_matches('.').to_ascii_uppercase()).is_some())
    {
        parts.pop();
    }
    let value = parts.join(" ");
    let tokens = super::street_tokens(Some(&value), None);
    tokens.len() >= 2
        && tokens
            .last()
            .is_some_and(|token| tables().suffix.contains_key(token))
}

fn address_component(value: String, kind: &str, us_style: bool) -> Option<String> {
    let original = if us_style {
        normalize_box_hash(&value)
    } else {
        value
    };
    let mut remainder = Some(original.clone());
    let mut units = Vec::new();
    if us_style {
        while let Some(current) = remainder.as_deref() {
            let Some((street, unit)) = tail_unit_display(current, kind == "line1") else {
                break;
            };
            if street.as_deref() == Some(current) {
                break;
            }
            units.insert(0, unit);
            remainder = street;
        }
    }
    if kind == "line2" {
        if remainder.is_none() && !units.is_empty() {
            return Some(units.join(", "));
        }
        remainder = Some(original);
        units.clear();
    }
    let Some(remainder) = remainder else {
        return (!units.is_empty()).then(|| units.join(", "));
    };
    let cleaned = trim_punctuation(&normalize_po_box(&remainder));
    let rendered = cleaned.map(|text| render_text(&text, kind, us_style));
    let mut parts = Vec::new();
    if let Some(rendered) = rendered.filter(|text| !text.is_empty()) {
        parts.push(rendered);
    }
    if kind == "line1" {
        parts.extend(units);
    }
    (!parts.is_empty()).then(|| parts.join(", "))
}

fn component(value: Option<&str>, kind: &str, us_style: bool) -> Option<String> {
    let cleaned = trim_punctuation(&clean(value)?)?;
    let cleaned = if us_style && matches!(kind, "line1" | "line2" | "city") {
        compound_directions(&cleaned)
    } else {
        cleaned
    };
    if matches!(kind, "line1" | "line2") {
        return address_component(cleaned, kind, us_style);
    }
    if kind == "postal"
        || kind == "state"
            && cleaned.len() <= 3
            && cleaned.bytes().all(|byte| byte.is_ascii_alphabetic())
    {
        return Some(cleaned.to_ascii_uppercase());
    }
    Some(render_text(&cleaned, kind, us_style))
}

fn same_canonical_unit(first: Option<&str>, second: Option<&str>) -> bool {
    let first_unit = unit_decision(first, None);
    if first_unit.unit.is_empty() {
        return false;
    }
    let first_street = street_norm_from_text(&first_unit.street_text);
    if first_street.is_none() {
        return false;
    }
    let second_unit = unit_decision(second, None);
    let combined = unit_decision(first, second);
    (first_unit.unit == second_unit.unit
        && first_street == street_norm_from_text(&second_unit.street_text))
        || (first_unit.unit == combined.unit
            && first_street == street_norm_from_text(&combined.street_text))
}

fn render(fields: [Option<&str>; 6]) -> Option<String> {
    let [first, second, city, state, postal, country] = fields;
    let cleaned_country = clean(country);
    let country_key: String = cleaned_country
        .as_deref()
        .unwrap_or_default()
        .bytes()
        .filter(u8::is_ascii_alphabetic)
        .map(|byte| char::from(byte.to_ascii_uppercase()))
        .collect();
    let us_style = cleaned_country.is_none()
        || matches!(
            country_key.as_str(),
            "US" | "USA" | "UNITEDSTATES" | "UNITEDSTATESOFAMERICA"
        );
    let line_one = component(first, "line1", us_style);
    let mut line_two = component(second, "line2", us_style);
    if let (Some(one), Some(two)) = (&line_one, &line_two) {
        let suffix = one.len().checked_sub(two.len()).filter(|start| *start > 0);
        if one.eq_ignore_ascii_case(two)
            || suffix.is_some_and(|start| {
                one[start..].eq_ignore_ascii_case(two)
                    && matches!(one.as_bytes()[start - 1], b' ' | b',' | b';')
            })
            || same_canonical_unit(first, second)
        {
            line_two = None;
        }
    }
    let city = component(city, "city", us_style);
    let state = component(state, "state", us_style);
    let mut postal = component(postal, "postal", us_style);
    if us_style && let Some(value) = &postal {
        if value.len() == 9 && value.bytes().all(|byte| byte.is_ascii_digit()) {
            postal = Some(format!("{}-{}", &value[..5], &value[5..]));
        } else if value.len() == 10
            && matches!(value.as_bytes()[5], b'-' | b' ')
            && value[..5]
                .bytes()
                .chain(value[6..].bytes())
                .all(|byte| byte.is_ascii_digit())
        {
            postal = Some(format!("{}-{}", &value[..5], &value[6..]));
        }
    }
    let state_postal = [state, postal]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(" ");
    let locality = [city, (!state_postal.is_empty()).then_some(state_postal)]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(", ");
    let country = if us_style {
        None
    } else {
        cleaned_country
            .map(|value| {
                if value.len() <= 3 && value.bytes().all(|byte| byte.is_ascii_alphabetic()) {
                    value.to_ascii_uppercase()
                } else {
                    component(Some(&value), "country", false).unwrap_or_default()
                }
            })
            .filter(|value| !value.is_empty())
    };
    let mut rendered = [
        line_one,
        line_two,
        (!locality.is_empty()).then_some(locality),
        country,
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join(", ");
    if rendered.len() > MAX_LENGTH {
        rendered.truncate(MAX_LENGTH);
        rendered = rendered
            .trim_end_matches([' ', '\t', '\n', '\r', ',', ';'])
            .to_owned();
    }
    (!rendered.is_empty()).then_some(rendered)
}

#[cfg(test)]
#[path = "address_format/tests.rs"]
mod tests;
