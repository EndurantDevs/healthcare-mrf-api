//! Environment-backed scanner settings.

use std::env;

pub const READ_BUF_SIZE: usize = 8 * 1024 * 1024;
pub const DEFAULT_PROGRESS_BYTES: u64 = 256 * 1024 * 1024;
pub const DEFAULT_PROGRESS_OBJECTS: u64 = 2_000_000;
pub const DEFAULT_SPLIT_NEGOTIATED_RATES: usize = 4096;
pub const DEFAULT_COMPACT_RUST_WORKERS: usize = 8;
pub const DEFAULT_COMPACT_RUST_WORK_QUEUE: usize = 16;
pub const DEFAULT_COMPACT_COPY_ROTATE_BYTES: u64 = 128 * 1024 * 1024;
pub const DEFAULT_PARSE_IN_WORKERS: bool = true;

pub fn split_interval(name: &str, default_value: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default_value)
}

pub fn progress_interval(name: &str, default_value: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default_value)
}

pub fn env_usize(name: &str, default_value: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default_value)
}

pub fn env_bool(name: &str, default_value: bool) -> bool {
    match env::var(name) {
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => true,
            "0" | "false" | "no" | "off" => false,
            _ => default_value,
        },
        Err(_) => default_value,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scoped_env<T>(name: &str, value: Option<&str>, callback: impl FnOnce() -> T) -> T {
        let previous = env::var(name).ok();
        match value {
            Some(value) => env::set_var(name, value),
            None => env::remove_var(name),
        }
        let result = callback();
        match previous {
            Some(value) => env::set_var(name, value),
            None => env::remove_var(name),
        }
        result
    }

    #[test]
    fn env_usize_uses_positive_integer_or_default() {
        scoped_env("PTG2_SCANNER_TEST_USIZE", Some("12"), || {
            assert_eq!(env_usize("PTG2_SCANNER_TEST_USIZE", 4), 12);
        });
        scoped_env("PTG2_SCANNER_TEST_USIZE", Some("0"), || {
            assert_eq!(env_usize("PTG2_SCANNER_TEST_USIZE", 4), 4);
        });
        scoped_env("PTG2_SCANNER_TEST_USIZE", Some("nope"), || {
            assert_eq!(env_usize("PTG2_SCANNER_TEST_USIZE", 4), 4);
        });
    }

    #[test]
    fn env_bool_accepts_common_true_false_tokens() {
        scoped_env("PTG2_SCANNER_TEST_BOOL", Some("yes"), || {
            assert!(env_bool("PTG2_SCANNER_TEST_BOOL", false));
        });
        scoped_env("PTG2_SCANNER_TEST_BOOL", Some("off"), || {
            assert!(!env_bool("PTG2_SCANNER_TEST_BOOL", true));
        });
        scoped_env("PTG2_SCANNER_TEST_BOOL", Some("unknown"), || {
            assert!(env_bool("PTG2_SCANNER_TEST_BOOL", true));
        });
    }

    #[test]
    fn parse_in_workers_defaults_on_but_can_be_disabled() {
        scoped_env("HLTHPRT_PTG2_RUST_PARSE_IN_WORKERS", None, || {
            assert!(env_bool(
                "HLTHPRT_PTG2_RUST_PARSE_IN_WORKERS",
                DEFAULT_PARSE_IN_WORKERS
            ));
        });
        scoped_env("HLTHPRT_PTG2_RUST_PARSE_IN_WORKERS", Some("false"), || {
            assert!(!env_bool(
                "HLTHPRT_PTG2_RUST_PARSE_IN_WORKERS",
                DEFAULT_PARSE_IN_WORKERS
            ));
        });
    }
}
