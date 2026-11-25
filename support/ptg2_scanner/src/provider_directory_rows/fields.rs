//! FHIR field helpers shared by every resource type: text, references, codings, identifiers, Meta and URL identity.

use super::value::{get, Fields, Value, PYTHON_INT_STR_DIGITS_FLOOR};
use super::{unsupported, Unsupported};

const URL_IDENTITY_MAX_LENGTH: usize = 2048;
const URL_IDENTITY_INPUT_MAX_LENGTH: usize = 65536;
const SENSITIVE_PATH_KEYS: [&str; 15] = [
    "access-token",
    "access_token",
    "api-key",
    "api_key",
    "apikey",
    "authorization",
    "client-secret",
    "client_secret",
    "credential",
    "key",
    "password",
    "secret",
    "subscription-key",
    "subscription_key",
    "token",
];
const USES_NETLOC: [&str; 27] = [
    "",
    "ftp",
    "http",
    "gopher",
    "nntp",
    "telnet",
    "imap",
    "wais",
    "file",
    "mms",
    "https",
    "shttp",
    "snews",
    "prospero",
    "rtsp",
    "rtsps",
    "rtspu",
    "rsync",
    "svn",
    "svn+ssh",
    "sftp",
    "nfs",
    "git",
    "git+ssh",
    "ws",
    "wss",
    "itms-services",
];
const KELVIN_SIGN: char = '\u{212a}';

/// `str.isspace()`, which `str.strip()` removes.
pub fn python_whitespace(character: char) -> bool {
    character.is_whitespace() || matches!(character, '\u{001c}'..='\u{001f}')
}

pub fn python_strip(text: &str) -> &str {
    text.trim_matches(python_whitespace)
}

/// `text[:limit]` in code points.
pub fn truncate(mut text: String, limit: usize) -> String {
    if let Some((index, _)) = text.char_indices().nth(limit) {
        text.truncate(index);
    }
    text
}

/// `str(value)` where Python and Rust print the same text; containers and floats are refused.
pub fn python_str(value: &Value) -> Result<String, Unsupported> {
    match value {
        Value::Null => Ok("None".to_owned()),
        Value::Bool(true) => Ok("True".to_owned()),
        Value::Bool(false) => Ok("False".to_owned()),
        Value::Int(text) | Value::Str(text) => Ok(text.clone()),
        Value::Float(_) | Value::List(_) | Value::Object(_) => Err(unsupported("text_repr")),
    }
}

/// `_clean_text`: `str(value).strip() or None`.
pub fn clean_text(value: &Value) -> Result<Option<String>, Unsupported> {
    if let Value::Null = value {
        return Ok(None);
    }
    if let Value::Str(text) = value {
        let stripped = python_strip(text);
        return Ok((!stripped.is_empty()).then(|| stripped.to_owned()));
    }
    let text = python_str(value)?;
    Ok((!text.is_empty()).then_some(text))
}

/// `_profile_text`: cleaned text cut to `limit` code points.
pub fn profile_text(value: &Value, limit: usize) -> Result<Option<String>, Unsupported> {
    Ok(clean_text(value)?.map(|text| truncate(text, limit)))
}

/// The items of `value or []` in a `for` loop whose body keeps only dicts.
/// A truthy string or dict iterates characters or keys, which such a loop skips; a truthy
/// number or `True` raises `TypeError` in Python and is refused.
pub fn loop_items(value: &Value) -> Result<&[Value], Unsupported> {
    match value {
        Value::List(items) => Ok(items),
        Value::Null | Value::Bool(false) | Value::Str(_) | Value::Object(_) => Ok(&[]),
        Value::Int(text) if text == "0" => Ok(&[]),
        _ => Err(unsupported("not_iterable")),
    }
}

/// `[item for item in value or [] if isinstance(item, dict)]`.
pub fn dict_items(value: &Value) -> Result<Vec<Value>, Unsupported> {
    Ok(loop_items(value)?
        .iter()
        .filter(|item| item.is_object())
        .cloned()
        .collect())
}

/// Lowercase text compared against ASCII constants: `None` when it cannot equal one.
/// Only the Kelvin sign lowercases a non-ASCII character to ASCII; it is refused.
pub fn ascii_lower(text: &str) -> Result<Option<String>, Unsupported> {
    if text.is_ascii() {
        return Ok(Some(text.to_ascii_lowercase()));
    }
    if text.contains(KELVIN_SIGN) {
        return Err(unsupported("unicode_lowercase"));
    }
    Ok(None)
}

/// `(_clean_text(extension.get("url")) or "").split("|", 1)[0].rstrip("/").lower()`.
pub fn extension_url(extension: &Fields) -> Result<Option<String>, Unsupported> {
    let url = clean_text(get(extension, "url"))?.unwrap_or_default();
    let base = url.split('|').next().unwrap_or_default();
    ascii_lower(base.trim_end_matches('/'))
}

/// `_references`.
pub fn references(value: &Value) -> Result<Vec<String>, Unsupported> {
    let items = match value {
        Value::Object(_) => std::slice::from_ref(value),
        Value::List(items) => items.as_slice(),
        _ => return Ok(Vec::new()),
    };
    let mut found = Vec::new();
    for item in items {
        if let Value::Object(fields) = item
            && let Some(reference) = clean_text(get(fields, "reference"))?
        {
            found.push(reference);
        }
    }
    Ok(found)
}

pub fn first_reference(value: &Value) -> Result<Option<String>, Unsupported> {
    Ok(references(value)?.into_iter().next())
}

pub fn string_list(texts: Vec<String>) -> Value {
    Value::List(texts.into_iter().map(Value::Str).collect())
}

/// `_codings`.
pub fn codings(value: &Value) -> Result<Vec<Value>, Unsupported> {
    let concepts = match value {
        Value::Object(_) => std::slice::from_ref(value),
        Value::List(items) => items.as_slice(),
        _ => return Ok(Vec::new()),
    };
    let mut coding_entries = Vec::new();
    for concept in concepts {
        let Value::Object(concept) = concept else {
            continue;
        };
        let mut concept_items: Vec<Fields> = Vec::new();
        for coding in loop_items(get(concept, "coding"))? {
            let Value::Object(coding) = coding else {
                continue;
            };
            let system = clean_text(get(coding, "system"))?;
            let code = clean_text(get(coding, "code"))?;
            let display = clean_text(get(coding, "display"))?;
            if system.is_some() || code.is_some() || display.is_some() {
                concept_items.push(vec![
                    ("system".to_owned(), Value::str_or_none(system)),
                    ("code".to_owned(), Value::str_or_none(code)),
                    ("display".to_owned(), Value::str_or_none(display)),
                ]);
            }
        }
        if let Some(text) = clean_text(get(concept, "text"))? {
            match concept_items.first_mut() {
                Some(first) => first.push(("text".to_owned(), Value::Str(text))),
                None => concept_items.push(vec![("text".to_owned(), Value::Str(text))]),
            }
        }
        coding_entries.extend(concept_items.into_iter().map(Value::Object));
    }
    Ok(coding_entries)
}

/// `_normalize_identifier_record`: absent, empty-list and empty-dict values are dropped.
fn normalized_identifier(identifier: &Fields) -> Result<Option<Value>, Unsupported> {
    let period = get(identifier, "period");
    let assigner = get(identifier, "assigner");
    let assigner_display = match assigner {
        Value::Object(fields) => clean_text(get(fields, "display"))?,
        _ => None,
    };
    let type_codes = codings(get(identifier, "type"))?;
    let candidates = [
        (
            "use",
            Value::str_or_none(clean_text(get(identifier, "use"))?),
        ),
        ("type_codes", Value::List(type_codes)),
        (
            "system",
            Value::str_or_none(clean_text(get(identifier, "system"))?),
        ),
        (
            "value",
            Value::str_or_none(clean_text(get(identifier, "value"))?),
        ),
        (
            "period_start",
            Value::str_or_none(clean_text(period.get("start"))?),
        ),
        (
            "period_end",
            Value::str_or_none(clean_text(period.get("end"))?),
        ),
        (
            "assigner_ref",
            Value::str_or_none(first_reference(assigner)?),
        ),
        ("assigner_display", Value::str_or_none(assigner_display)),
    ];
    let kept: Fields = candidates
        .into_iter()
        .filter(|(_, value)| !matches!(value, Value::Null) && value != &Value::List(Vec::new()))
        .map(|(name, value)| (name.to_owned(), value))
        .collect();
    Ok((!kept.is_empty()).then_some(Value::Object(kept)))
}

/// `_normalized_identifiers`.
pub fn normalized_identifiers(value: &Value) -> Result<Vec<Value>, Unsupported> {
    let Value::List(items) = value else {
        return Ok(Vec::new());
    };
    let mut normalized = Vec::new();
    for item in items {
        if let Value::Object(identifier) = item
            && let Some(record) = normalized_identifier(identifier)?
        {
            normalized.push(record);
        }
    }
    Ok(normalized)
}

/// `identifier_descriptor` for matching ASCII NPI tokens.
fn identifier_descriptor(identifier: &Fields) -> Result<String, Unsupported> {
    let identifier_type = get(identifier, "type");
    let identifier_type = identifier_type
        .as_object()
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let mut parts: Vec<&Value> = vec![get(identifier, "system"), get(identifier_type, "text")];
    for coding in loop_items(get(identifier_type, "coding"))? {
        if let Value::Object(coding) = coding {
            parts.extend([
                get(coding, "system"),
                get(coding, "code"),
                get(coding, "display"),
            ]);
        }
    }
    let mut lowered = Vec::new();
    for part in parts {
        if part.truthy()? {
            let mut text = python_str(part)?;
            if !text.is_ascii() {
                // This dash remains a separator for the NPI tokens below.
                text = text.replace('\u{2013}', "-");
                if !text.is_ascii() {
                    return Err(unsupported("unicode_identifier"));
                }
            }
            text.make_ascii_lowercase();
            lowered.push(text);
        }
    }
    Ok(lowered.join(" "))
}

/// `is_valid_npi` for ten ASCII digits.
fn valid_npi_checksum(digits: &str) -> bool {
    let Ok(number) = digits.parse::<u64>() else {
        return false;
    };
    if !(1_000_000_000..=2_999_999_999).contains(&number) {
        return false;
    }
    let values: Vec<u32> = digits.bytes().map(|byte| u32::from(byte - b'0')).collect();
    let mut digit_sum = 24 + values[9];
    for (position, digit) in values[..9].iter().enumerate() {
        if position % 2 == 0 {
            let doubled = digit * 2;
            digit_sum += if doubled > 9 { doubled - 9 } else { doubled };
        } else {
            digit_sum += digit;
        }
    }
    digit_sum.is_multiple_of(10)
}

/// `explicit_npi`: the declared NPI system first, then input order; checksum only when `valid_only`.
pub fn explicit_npi(resource: &Fields, valid_only: bool) -> Result<Option<u64>, Unsupported> {
    let mut best: Option<(bool, usize, u64)> = None;
    for (index, identifier) in loop_items(get(resource, "identifier"))?.iter().enumerate() {
        let Value::Object(identifier) = identifier else {
            continue;
        };
        let Some(value) = clean_text(get(identifier, "value"))? else {
            continue;
        };
        let descriptor = identifier_descriptor(identifier)?;
        if !["us-npi", "npi", "national provider"]
            .iter()
            .any(|token| descriptor.contains(token))
        {
            continue;
        }
        // Python's isdigit accepts other scripts' digits; such values are not reproduced.
        if !value.is_ascii() {
            return Err(unsupported("unicode_identifier"));
        }
        let digits: String = value.chars().filter(char::is_ascii_digit).collect();
        if digits.len() != 10 || (valid_only && !valid_npi_checksum(&digits)) {
            continue;
        }
        let number: u64 = digits.parse().map_err(|_| unsupported("npi_digits"))?;
        let system = clean_text(get(identifier, "system"))?.unwrap_or_default();
        let declared = ascii_lower(&system)?.as_deref() == Some("http://hl7.org/fhir/sid/us-npi");
        let candidate = (!declared, index, number);
        if best.is_none_or(|current| candidate < current) {
            best = Some(candidate);
        }
    }
    Ok(best.map(|candidate| candidate.2))
}

/// The parts of `urllib.parse.urlsplit` the importer reads.
struct UrlParts<'a> {
    scheme: String,
    netloc: Option<&'a str>,
    path: &'a str,
}

/// `urllib.parse.urlsplit`, refusing inputs it rewrites (C0 controls, tabs, newlines) or treats specially
/// (brackets and non-ASCII text in the network location).
fn split_url(text: &str) -> Result<UrlParts<'_>, Unsupported> {
    if text.starts_with(|character: char| character <= ' ') || text.contains(['\t', '\r', '\n']) {
        return Err(unsupported("url_shape"));
    }
    let mut scheme = String::new();
    let mut rest = text;
    if let Some(colon) = text.find(':')
        && colon > 0
        && text.as_bytes()[0].is_ascii_alphabetic()
        && text[..colon]
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'-' | b'.'))
    {
        scheme = text[..colon].to_ascii_lowercase();
        rest = &text[colon + 1..];
    }
    let mut netloc = None;
    if let Some(after) = rest.strip_prefix("//") {
        let end = after.find(['/', '?', '#']).unwrap_or(after.len());
        let location = &after[..end];
        if location.contains(['[', ']']) || !location.is_ascii() {
            return Err(unsupported("url_network_location"));
        }
        netloc = Some(location);
        rest = &after[end..];
    }
    let path = rest.split('#').next().unwrap_or_default();
    let path = path.split('?').next().unwrap_or_default();
    Ok(UrlParts {
        scheme,
        netloc,
        path,
    })
}

/// `urllib.parse.unquote` as far as a comparison with ASCII needs: invalid UTF-8 only yields
/// replacement characters, which never compare equal to ASCII.
fn unquote(segment: &str) -> String {
    if !segment.contains('%') {
        return segment.to_owned();
    }
    let bytes = segment.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%'
            && let Some(hex) = segment.get(index + 1..index + 3)
            && let Ok(byte) = u8::from_str_radix(hex, 16)
            && hex.bytes().all(|digit| digit.is_ascii_hexdigit())
        {
            decoded.push(byte);
            index += 3;
            continue;
        }
        decoded.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&decoded).into_owned()
}

/// `_sanitized_fhir_url_path`.
fn sanitized_url_path(path: &str) -> Result<String, Unsupported> {
    let mut kept: Vec<&str> = Vec::new();
    let mut skip_credential_value = false;
    for raw_segment in path.split('/') {
        let segment = raw_segment.split(';').next().unwrap_or_default();
        if skip_credential_value {
            skip_credential_value = false;
            continue;
        }
        let decoded = unquote(segment);
        let decoded = python_strip(&decoded);
        let (key, has_separator) = match decoded.split_once('=') {
            Some((key, _)) => (key, true),
            None => (decoded, false),
        };
        if ascii_lower(key)?.is_some_and(|key| SENSITIVE_PATH_KEYS.contains(&key.as_str())) {
            skip_credential_value = !has_separator;
            continue;
        }
        kept.push(segment);
    }
    Ok(kept.join("/"))
}

/// `urlunsplit((scheme, netloc, path, "", ""))`.
fn unsplit(scheme: &str, netloc: &str, path: &str) -> String {
    let netloc = if !netloc.is_empty() {
        Some(netloc)
    } else if !scheme.is_empty()
        && USES_NETLOC.contains(&scheme)
        && (path.is_empty() || path.starts_with('/'))
    {
        Some("")
    } else {
        None
    };
    let mut url = String::new();
    match netloc {
        Some(netloc) => {
            url.push_str("//");
            url.push_str(netloc);
            if !path.is_empty() && !path.starts_with('/') {
                url.push('/');
            }
        }
        None if path.starts_with("//") => url.push_str("//"),
        None => {}
    }
    url.push_str(path);
    if scheme.is_empty() {
        url
    } else {
        format!("{scheme}:{url}")
    }
}

/// `_sanitized_fhir_url_identity`.
pub fn url_identity(value: &Value) -> Result<Option<String>, Unsupported> {
    let Some(text) = clean_text(value)? else {
        return Ok(None);
    };
    if text.chars().count() > URL_IDENTITY_INPUT_MAX_LENGTH {
        return Ok(None);
    }
    let parts = split_url(&text)?;
    let hostinfo = parts
        .netloc
        .unwrap_or_default()
        .rsplit_once('@')
        .map_or(parts.netloc.unwrap_or_default(), |(_, host)| host);
    let (hostname, port_text) = hostinfo.split_once(':').unwrap_or((hostinfo, ""));
    let port = if port_text.is_empty() {
        None
    } else if port_text.bytes().all(|byte| byte.is_ascii_digit()) {
        if port_text.len() > PYTHON_INT_STR_DIGITS_FLOOR {
            return Err(unsupported("port_digits"));
        }
        // Python raises ValueError for an out-of-range port, which the importer maps to no identity.
        match port_text.trim_start_matches('0').parse::<u32>() {
            Ok(port) if port <= 65535 => Some(port),
            Err(_) if port_text.trim_start_matches('0').is_empty() => Some(0),
            _ => return Ok(None),
        }
    } else {
        return Ok(None);
    };
    let sanitized_path = sanitized_url_path(parts.path)?;
    let hostname = hostname.to_ascii_lowercase();
    let sanitized = if !parts.scheme.is_empty() && hostname.is_empty() {
        unsplit(&parts.scheme, "", &sanitized_path)
    } else if parts.scheme.is_empty() {
        sanitized_path
    } else {
        let netloc = match port {
            Some(port) => format!("{hostname}:{port}"),
            None => hostname,
        };
        unsplit(&parts.scheme, &netloc, &sanitized_path)
    };
    profile_text(&Value::Str(sanitized), URL_IDENTITY_MAX_LENGTH)
}

/// `_organization_reference_identity`.
pub fn organization_reference_identity(reference: &str) -> Result<Option<String>, Unsupported> {
    let parts = split_url(reference)?;
    let segments: Vec<String> = parts
        .path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(unquote)
        .collect();
    for index in 0..segments.len().saturating_sub(1) {
        if ascii_lower(&segments[index])?.as_deref() != Some("organization") {
            continue;
        }
        let resource_id = &segments[index + 1];
        if (1..=64).contains(&resource_id.len())
            && resource_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.'))
        {
            return Ok(Some(format!("organization/{resource_id}")));
        }
    }
    Ok(None)
}

/// `_sanitized_fhir_meta_codings`.
fn meta_codings(value: &Value) -> Result<Vec<Value>, Unsupported> {
    let codings = match value {
        Value::Object(_) => std::slice::from_ref(value),
        Value::List(items) => items.as_slice(),
        _ => return Ok(Vec::new()),
    };
    let mut kept = Vec::new();
    for coding in codings.iter().take(32) {
        let Value::Object(coding) = coding else {
            continue;
        };
        let user_selected = match get(coding, "userSelected") {
            Value::Bool(flag) => Value::Bool(*flag),
            _ => Value::Null,
        };
        let fields: Fields = [
            (
                "system",
                Value::str_or_none(url_identity(get(coding, "system"))?),
            ),
            (
                "version",
                Value::str_or_none(profile_text(get(coding, "version"), 256)?),
            ),
            (
                "code",
                Value::str_or_none(profile_text(get(coding, "code"), 256)?),
            ),
            (
                "display",
                Value::str_or_none(profile_text(get(coding, "display"), 2048)?),
            ),
            ("userSelected", user_selected),
        ]
        .into_iter()
        .filter(|(_, value)| !matches!(value, Value::Null))
        .map(|(name, value)| (name.to_owned(), value))
        .collect();
        if !fields.is_empty() {
            kept.push(Value::Object(fields));
        }
    }
    Ok(kept)
}

fn non_empty_list(items: Vec<Value>) -> Value {
    if items.is_empty() {
        Value::Null
    } else {
        Value::List(items)
    }
}

/// `_sanitized_fhir_meta`.
pub fn sanitized_meta(value: &Value) -> Result<Value, Unsupported> {
    let Value::Object(meta) = value else {
        return Ok(Value::Null);
    };
    let profile_values = match get(meta, "profile") {
        text @ Value::Str(_) => std::slice::from_ref(text),
        Value::List(items) => items.as_slice(),
        _ => &[],
    };
    let mut profiles = Vec::new();
    for profile in profile_values.iter().take(32) {
        if let Some(identity) = url_identity(profile)? {
            profiles.push(Value::Str(identity));
        }
    }
    let fields: Fields = [
        (
            "versionId",
            Value::str_or_none(profile_text(get(meta, "versionId"), 256)?),
        ),
        (
            "lastUpdated",
            Value::str_or_none(profile_text(get(meta, "lastUpdated"), 64)?),
        ),
        (
            "source",
            Value::str_or_none(url_identity(get(meta, "source"))?),
        ),
        ("profile", non_empty_list(profiles)),
        (
            "security",
            non_empty_list(meta_codings(get(meta, "security"))?),
        ),
        ("tag", non_empty_list(meta_codings(get(meta, "tag"))?)),
    ]
    .into_iter()
    .filter(|(_, value)| !matches!(value, Value::Null))
    .map(|(name, value)| (name.to_owned(), value))
    .collect();
    Ok(if fields.is_empty() {
        Value::Null
    } else {
        Value::Object(fields)
    })
}

/// `_period`.
pub fn period(resource: &Fields) -> Result<(Value, Value), Unsupported> {
    match get(resource, "period") {
        Value::Object(period) => Ok((
            Value::str_or_none(clean_text(get(period, "start"))?),
            Value::str_or_none(clean_text(get(period, "end"))?),
        )),
        _ => Ok((Value::Null, Value::Null)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(text: &str) -> Option<String> {
        url_identity(&Value::Str(text.to_owned())).unwrap()
    }

    // Expected values printed by CPython 3.14 `_sanitized_fhir_url_identity`.
    #[test]
    fn url_identity_matches_python() {
        assert_eq!(
            identity(" HTTPS://User:pw@Example.ORG:0443/a;x/token/abc/b?q=1#f "),
            Some("https://example.org:443/a/b".into())
        );
        assert_eq!(
            identity("https://example.org/<redacted>:sha256:da17f50ff22b41a3"),
            Some("https://example.org/<redacted>:sha256:da17f50ff22b41a3".into())
        );
        assert_eq!(identity("urn:oid:1.2.3"), Some("urn:oid:1.2.3".into()));
        assert_eq!(identity("file:///tmp/x"), Some("file:///tmp/x".into()));
        assert_eq!(identity("http://:80/x"), Some("http:///x".into()));
        assert_eq!(identity("http://host:99999/x"), None);
        assert_eq!(identity("http://host:8a/x"), None);
        assert_eq!(identity("relative/api_key=1/b"), Some("relative/b".into()));
        assert_eq!(identity("//host/x"), Some("/x".into()));
        assert_eq!(identity("?only"), None);
        assert_eq!(
            identity("https://h/%74oken/v/w"),
            Some("https://h/w".into())
        );
        assert_eq!(
            url_identity(&Value::Str("http://[::1]/".into()))
                .unwrap_err()
                .0,
            "url_network_location"
        );
        assert_eq!(
            url_identity(&Value::Str("https://h/\u{212a}ey/v".into()))
                .unwrap_err()
                .0,
            "unicode_lowercase"
        );
    }

    #[test]
    fn organization_identity_matches_python() {
        let identity = |text: &str| organization_reference_identity(text).unwrap();
        assert_eq!(
            identity("Organization/abc-1"),
            Some("organization/abc-1".into())
        );
        assert_eq!(
            identity("https://h/fhir/ORGANIZATION/x%2Fy/Organization/ok"),
            Some("organization/ok".into())
        );
        assert_eq!(identity("Organization/"), None);
        assert_eq!(identity("Practitioner/1"), None);
    }

    #[test]
    fn npi_checksum_matches_python() {
        assert!(valid_npi_checksum("1234567893"));
        assert!(!valid_npi_checksum("1234567890"));
        assert!(!valid_npi_checksum("0234567893"));
    }
}
