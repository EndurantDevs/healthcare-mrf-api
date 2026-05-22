//! Environment-backed scanner settings.

use std::env;

pub const READ_BUF_SIZE: usize = 8 * 1024 * 1024;
pub const DEFAULT_PROGRESS_BYTES: u64 = 256 * 1024 * 1024;
pub const DEFAULT_PROGRESS_OBJECTS: u64 = 2_000_000;
pub const DEFAULT_SPLIT_NEGOTIATED_RATES: usize = 4096;
pub const DEFAULT_COMPACT_RUST_WORKERS: usize = 8;
pub const DEFAULT_COMPACT_RUST_WORK_QUEUE: usize = 16;
pub const DEFAULT_COMPACT_COPY_ROTATE_BYTES: u64 = 128 * 1024 * 1024;

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
