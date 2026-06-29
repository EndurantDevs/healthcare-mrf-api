//! Contact-number canonicalization helpers.
//!
//! The storage contract is intentionally simple: phone/fax columns carry digits
//! only. US/NANP numbers are normalized to ten digits; explicitly international
//! values are preserved as 8-15 digits and marked as not safe for US fallback
//! matching. Raw formatting stays in source/provenance payloads.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalContactNumber {
    pub number: Option<String>,
    pub extension: Option<String>,
    pub is_international: bool,
    pub valid_for_fallback: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalContactPair {
    pub phone: CanonicalContactNumber,
    pub fax: CanonicalContactNumber,
}

impl CanonicalContactNumber {
    fn empty() -> Self {
        Self {
            number: None,
            extension: None,
            is_international: false,
            valid_for_fallback: false,
        }
    }
}

pub fn canonicalize_contact_pair(
    phone_raw: Option<&str>,
    fax_raw: Option<&str>,
    country_code: Option<&str>,
) -> CanonicalContactPair {
    CanonicalContactPair {
        phone: canonicalize_contact_number(phone_raw, country_code),
        fax: canonicalize_contact_number(fax_raw, country_code),
    }
}

pub fn canonicalize_contact_number(
    raw: Option<&str>,
    country_code: Option<&str>,
) -> CanonicalContactNumber {
    let Some(raw_value) = raw else {
        return CanonicalContactNumber::empty();
    };
    let text = raw_value.trim();
    if text.is_empty() {
        return CanonicalContactNumber::empty();
    }

    let (main_text, extension) = split_extension(text);
    let digits = digits_only(main_text);
    if digits.is_empty() {
        return CanonicalContactNumber::empty();
    }

    let country = country_code.unwrap_or("").trim().to_ascii_uppercase();
    let explicit_international = main_text.trim_start().starts_with('+');
    let default_us = country.is_empty()
        || matches!(
            country.as_str(),
            "US" | "USA" | "UNITED STATES" | "UNITEDSTATES" | "UNITED STATES OF AMERICA"
        );

    if digits.len() == 10 && default_us {
        return CanonicalContactNumber {
            number: Some(digits),
            extension,
            is_international: false,
            valid_for_fallback: true,
        };
    }
    if digits.len() == 11 && digits.starts_with('1') && default_us {
        return CanonicalContactNumber {
            number: Some(digits[1..].to_string()),
            extension,
            is_international: false,
            valid_for_fallback: true,
        };
    }

    if explicit_international && (8..=15).contains(&digits.len()) {
        return CanonicalContactNumber {
            number: Some(digits),
            extension,
            is_international: true,
            valid_for_fallback: false,
        };
    }

    CanonicalContactNumber {
        number: None,
        extension: None,
        is_international: false,
        valid_for_fallback: false,
    }
}

fn digits_only(value: &str) -> String {
    value.chars().filter(|ch| ch.is_ascii_digit()).collect()
}

fn split_extension(value: &str) -> (&str, Option<String>) {
    for marker in ["extension", "ext.", "ext", ";ext=", "#", " x ", " x", "x"] {
        if let Some((main, extension)) = split_extension_on_marker(value, marker) {
            return (main, Some(extension));
        }
    }
    (value, None)
}

fn split_extension_on_marker<'a>(value: &'a str, marker: &str) -> Option<(&'a str, String)> {
    let lower = value.to_ascii_lowercase();
    let index = lower.rfind(marker)?;
    if marker == "x" {
        let prev = value[..index].chars().next_back();
        if !matches!(prev, Some(ch) if ch.is_ascii_digit() || ch == ')' || ch.is_ascii_whitespace())
        {
            return None;
        }
    }
    let main = value[..index].trim_end();
    let suffix = value[index + marker.len()..].trim();
    if digits_only(main).len() < 7 {
        return None;
    }
    let extension = digits_only(suffix);
    if extension.is_empty() || extension.len() > 16 {
        return None;
    }
    if suffix.chars().any(|ch| ch.is_ascii_alphabetic()) {
        return None;
    }
    Some((main, extension))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_formatted_us_number_to_ten_digits() {
        let out = canonicalize_contact_number(Some("(312) 555-1212"), Some("US"));
        assert_eq!(out.number.as_deref(), Some("3125551212"));
        assert_eq!(out.extension, None);
        assert!(!out.is_international);
        assert!(out.valid_for_fallback);
    }

    #[test]
    fn strips_leading_one_for_us_number() {
        let out = canonicalize_contact_number(Some("+1 312-555-1212"), Some("US"));
        assert_eq!(out.number.as_deref(), Some("3125551212"));
        assert!(!out.is_international);
        assert!(out.valid_for_fallback);
    }

    #[test]
    fn splits_extension_without_polluting_number() {
        let out = canonicalize_contact_number(Some("312-555-1212 x45"), Some("US"));
        assert_eq!(out.number.as_deref(), Some("3125551212"));
        assert_eq!(out.extension.as_deref(), Some("45"));
    }

    #[test]
    fn rejects_local_seven_digit_number_for_matching() {
        let out = canonicalize_contact_number(Some("555-1212"), Some("US"));
        assert_eq!(out.number, None);
        assert!(!out.valid_for_fallback);
    }

    #[test]
    fn preserves_explicit_non_us_number_but_not_for_us_fallback() {
        let out = canonicalize_contact_number(Some("+44 20 7946 0958"), Some("GB"));
        assert_eq!(out.number.as_deref(), Some("442079460958"));
        assert!(out.is_international);
        assert!(!out.valid_for_fallback);
    }

    #[test]
    fn ignores_ambiguous_long_us_value() {
        let out = canonicalize_contact_number(Some("131255512123"), Some("US"));
        assert_eq!(out.number, None);
        assert!(!out.valid_for_fallback);
    }
}
