pub use std::env::{args, current_dir, temp_dir, VarError};

#[cfg(not(test))]
pub use std::env::{var, var_os};

#[cfg(not(test))]
pub const READ_VAR: fn(&'static str) -> Result<String, VarError> = std::env::var::<&'static str>;

#[cfg(test)]
use std::collections::HashMap;
#[cfg(test)]
use std::ffi::OsString;
#[cfg(test)]
use std::sync::{OnceLock, RwLock};

#[cfg(test)]
fn overrides() -> &'static RwLock<HashMap<String, Option<OsString>>> {
    static OVERRIDES: OnceLock<RwLock<HashMap<String, Option<OsString>>>> = OnceLock::new();
    OVERRIDES.get_or_init(|| RwLock::new(HashMap::new()))
}

#[cfg(test)]
pub fn var_os(name: &str) -> Option<OsString> {
    overrides()
        .read()
        .unwrap()
        .get(name)
        .cloned()
        .unwrap_or_else(|| std::env::var_os(name))
}

#[cfg(test)]
pub fn var(name: &str) -> Result<String, VarError> {
    match var_os(name) {
        Some(value) => value.into_string().map_err(VarError::NotUnicode),
        None => Err(VarError::NotPresent),
    }
}

#[cfg(test)]
pub const READ_VAR: fn(&'static str) -> Result<String, VarError> = var;

#[cfg(test)]
pub fn replace_test_var(name: &'static str, value: Option<OsString>) -> Option<Option<OsString>> {
    overrides().write().unwrap().insert(name.to_string(), value)
}

#[cfg(test)]
pub fn restore_test_var(name: &'static str, previous: Option<Option<OsString>>) {
    let mut overrides = overrides().write().unwrap();
    match previous {
        Some(value) => {
            overrides.insert(name.to_string(), value);
        }
        None => {
            overrides.remove(name);
        }
    }
}

#[cfg(test)]
pub fn split_interval(name: &str, default_value: usize) -> usize {
    env_usize(name, default_value)
}

#[cfg(test)]
pub fn progress_interval(name: &str, default_value: u64) -> u64 {
    var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default_value)
}

#[cfg(test)]
pub fn env_usize(name: &str, default_value: usize) -> usize {
    var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default_value)
}

#[cfg(test)]
pub fn env_usize_allow_zero(name: &str, default_value: usize) -> usize {
    var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default_value)
}

#[cfg(test)]
pub fn env_bool(name: &str, default_value: bool) -> bool {
    match var(name) {
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => true,
            "0" | "false" | "no" | "off" => false,
            _ => default_value,
        },
        Err(_) => default_value,
    }
}
