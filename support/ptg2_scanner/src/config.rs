//! Environment-backed scanner settings.

use std::env;

pub const READ_BUF_SIZE: usize = 8 * 1024 * 1024;
pub const DEFAULT_PROGRESS_BYTES: u64 = 256 * 1024 * 1024;
pub const DEFAULT_PROGRESS_OBJECTS: u64 = 2_000_000;
pub const DEFAULT_SPLIT_NEGOTIATED_RATES: usize = 8192;
pub const DEFAULT_COMPACT_RUST_WORKERS: usize = 16;
pub const DEFAULT_COMPACT_RUST_WORK_QUEUE: usize = 32;
pub const DEFAULT_COMPACT_COPY_ROTATE_BYTES: u64 = 128 * 1024 * 1024;
pub const DEFAULT_RAW_CHUNK_BYTES: usize = 32 * 1024 * 1024;
pub const DEFAULT_PARSE_IN_WORKERS: bool = true;
pub const DEFAULT_TOP_LEVEL_BYTE_SCAN: bool = true;

fn positive_usize(value: Option<&str>, default_value: usize) -> usize {
    value
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default_value)
}

fn usize_allow_zero(value: Option<&str>, default_value: usize) -> usize {
    value
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default_value)
}

fn bool_value(value: Option<&str>, default_value: bool) -> bool {
    match value.map(|value| value.trim().to_ascii_lowercase()) {
        Some(value) if matches!(value.as_str(), "1" | "true" | "yes" | "on") => true,
        Some(value) if matches!(value.as_str(), "0" | "false" | "no" | "off") => false,
        _ => default_value,
    }
}

pub fn split_interval(name: &str, default_value: usize) -> usize {
    let value = env::var(name).ok();
    positive_usize(value.as_deref(), default_value)
}

pub fn progress_interval(name: &str, default_value: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default_value)
}

pub fn env_usize(name: &str, default_value: usize) -> usize {
    let value = env::var(name).ok();
    positive_usize(value.as_deref(), default_value)
}

pub fn env_usize_allow_zero(name: &str, default_value: usize) -> usize {
    let value = env::var(name).ok();
    usize_allow_zero(value.as_deref(), default_value)
}

pub fn env_bool(name: &str, default_value: bool) -> bool {
    let value = env::var(name).ok();
    bool_value(value.as_deref(), default_value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_usize_uses_positive_integer_or_default() {
        assert_eq!(positive_usize(Some("12"), 4), 12);
        assert_eq!(positive_usize(Some("0"), 4), 4);
        assert_eq!(positive_usize(Some("nope"), 4), 4);
    }

    #[test]
    fn env_usize_allow_zero_preserves_explicit_disable() {
        assert_eq!(usize_allow_zero(Some("0"), 4), 0);
        assert_eq!(usize_allow_zero(Some("12"), 4), 12);
        assert_eq!(usize_allow_zero(Some("nope"), 4), 4);
    }

    #[test]
    fn env_bool_accepts_common_true_false_tokens() {
        assert!(bool_value(Some("yes"), false));
        assert!(!bool_value(Some("off"), true));
        assert!(bool_value(Some("unknown"), true));
    }

    #[test]
    fn parse_in_workers_defaults_on_but_can_be_disabled() {
        assert!(bool_value(None, DEFAULT_PARSE_IN_WORKERS));
        assert!(!bool_value(Some("false"), DEFAULT_PARSE_IN_WORKERS));
    }

    #[test]
    fn top_level_byte_scan_defaults_on_but_can_be_disabled() {
        assert!(bool_value(None, DEFAULT_TOP_LEVEL_BYTE_SCAN));
        assert!(!bool_value(Some("false"), DEFAULT_TOP_LEVEL_BYTE_SCAN));
    }

    #[test]
    fn scanner_chunk_defaults_are_promoted_and_can_be_overridden() {
        assert_eq!(DEFAULT_COMPACT_RUST_WORKERS, 16);
        assert_eq!(DEFAULT_COMPACT_RUST_WORK_QUEUE, 32);
        assert_eq!(DEFAULT_SPLIT_NEGOTIATED_RATES, 8192);
        assert_eq!(DEFAULT_RAW_CHUNK_BYTES, 33_554_432);
        assert_eq!(
            positive_usize(Some("4096"), DEFAULT_SPLIT_NEGOTIATED_RATES),
            4096
        );
        assert_eq!(
            positive_usize(Some("16777216"), DEFAULT_RAW_CHUNK_BYTES),
            16_777_216
        );
    }
}
