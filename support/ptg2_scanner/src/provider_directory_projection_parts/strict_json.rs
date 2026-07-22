// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

use super::contracts::invalid_data;
use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::Serialize;
use serde_json::Value;
use std::collections::HashSet;
use std::fmt;
use std::io;

struct StrictJsonValue;

struct StrictJsonValueVisitor;

impl<'de> Visitor<'de> for StrictJsonValueVisitor {
    type Value = StrictJsonValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a strict JSON value")
    }

    fn visit_bool<E>(self, _value: bool) -> Result<Self::Value, E> {
        Ok(StrictJsonValue)
    }

    fn visit_i64<E>(self, _value: i64) -> Result<Self::Value, E> {
        Ok(StrictJsonValue)
    }

    fn visit_u64<E>(self, _value: u64) -> Result<Self::Value, E> {
        Ok(StrictJsonValue)
    }

    fn visit_f64<E>(self, _value: f64) -> Result<Self::Value, E> {
        Ok(StrictJsonValue)
    }

    fn visit_str<E>(self, _value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(StrictJsonValue)
    }

    fn visit_borrowed_str<E>(self, _value: &'de str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(StrictJsonValue)
    }

    fn visit_string<E>(self, _value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(StrictJsonValue)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(StrictJsonValue)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while sequence.next_element::<StrictJsonValue>()?.is_some() {}
        Ok(StrictJsonValue)
    }

    fn visit_map<A>(self, mut object: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut keys = HashSet::with_capacity(object.size_hint().unwrap_or(8).min(256));
        while let Some(key) = object.next_key::<String>()? {
            if !keys.insert(key.clone()) {
                return Err(de::Error::custom(format!(
                    "duplicate JSON object key: {key:?}"
                )));
            }
            object.next_value::<StrictJsonValue>()?;
        }
        Ok(StrictJsonValue)
    }
}

impl<'de> Deserialize<'de> for StrictJsonValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(StrictJsonValueVisitor)
    }
}

pub(super) fn strict_json(bytes: &[u8], label: &str) -> io::Result<Value> {
    let mut strict_deserializer = serde_json::Deserializer::from_slice(bytes);
    StrictJsonValue::deserialize(&mut strict_deserializer)
        .map_err(|error| invalid_data(format!("{label} is invalid JSON: {error}")))?;
    strict_deserializer
        .end()
        .map_err(|error| invalid_data(format!("{label} is invalid JSON: {error}")))?;
    serde_json::from_slice(bytes)
        .map_err(|error| invalid_data(format!("{label} is invalid JSON: {error}")))
}

pub(super) fn decoded_canonical_frame<T>(bytes: &[u8], label: &str) -> io::Result<T>
where
    T: de::DeserializeOwned + Serialize,
{
    let strict_value = strict_json(bytes, label)?;
    let decoded = serde_json::from_value(strict_value)
        .map_err(|error| invalid_data(format!("{label} shape is invalid: {error}")))?;
    let canonical_bytes = serde_json::to_vec(&decoded)
        .map_err(|error| invalid_data(format!("{label} cannot be canonicalized: {error}")))?;
    if canonical_bytes != bytes {
        return Err(invalid_data(format!("{label} is not canonical JSON")));
    }
    Ok(decoded)
}
