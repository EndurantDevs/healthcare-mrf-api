// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

use super::contracts::{
    invalid_data, ProviderDirectoryInputFraming, PROVIDER_DIRECTORY_PROJECTION_MAX_INPUT_BYTES,
    PROVIDER_DIRECTORY_PROJECTION_MAX_RESOURCE_COUNT,
};
use rayon::prelude::*;
use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::Value;
use std::collections::HashSet;
use std::fmt;
use std::io::{self, Read};

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

fn strict_json(bytes: &[u8], label: &str) -> io::Result<Value> {
    let mut strict_deserializer = serde_json::Deserializer::from_slice(bytes);
    StrictJsonValue::deserialize(&mut strict_deserializer)
        .map_err(|error| invalid_data(format!("{label} is invalid JSON: {error}")))?;
    strict_deserializer
        .end()
        .map_err(|error| invalid_data(format!("{label} is invalid JSON: {error}")))?;
    serde_json::from_slice(bytes)
        .map_err(|error| invalid_data(format!("{label} is invalid JSON: {error}")))
}

fn trim_json_whitespace(mut bytes: &[u8]) -> &[u8] {
    while bytes
        .first()
        .is_some_and(|byte| matches!(byte, b' ' | b'\t' | b'\r'))
    {
        bytes = &bytes[1..];
    }
    while bytes
        .last()
        .is_some_and(|byte| matches!(byte, b' ' | b'\t' | b'\r'))
    {
        bytes = &bytes[..bytes.len() - 1];
    }
    bytes
}

fn ndjson_resources(input: &[u8]) -> io::Result<Vec<(usize, Value)>> {
    let raw_lines = input
        .split(|byte| *byte == b'\n')
        .map(trim_json_whitespace)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    if raw_lines.len() > PROVIDER_DIRECTORY_PROJECTION_MAX_RESOURCE_COUNT {
        return Err(invalid_data(
            "provider-directory projection resource count exceeds the limit",
        ));
    }
    let decoded_resources = raw_lines
        .par_iter()
        .enumerate()
        .map(|(ordinal, line)| {
            strict_json(
                line,
                &format!("provider-directory NDJSON resource {ordinal}"),
            )
            .map(|resource| (ordinal, resource))
        })
        .collect::<Vec<_>>();
    decoded_resources.into_iter().collect()
}

fn bundle_resources(input: &[u8]) -> io::Result<Vec<(usize, Value)>> {
    let mut bundle = strict_json(input, "provider-directory FHIR Bundle")?;
    let bundle_object = bundle
        .as_object_mut()
        .ok_or_else(|| invalid_data("provider-directory FHIR Bundle must be an object"))?;
    if bundle_object.get("resourceType").and_then(Value::as_str) != Some("Bundle") {
        return Err(invalid_data(
            "provider-directory FHIR Bundle resourceType must be Bundle",
        ));
    }
    let entries = match bundle_object.remove("entry") {
        Some(Value::Array(entries)) => entries,
        _ => {
            return Err(invalid_data(
                "provider-directory FHIR Bundle entry must be an array",
            ));
        }
    };
    if entries.len() > PROVIDER_DIRECTORY_PROJECTION_MAX_RESOURCE_COUNT {
        return Err(invalid_data(
            "provider-directory projection resource count exceeds the limit",
        ));
    }
    entries
        .into_iter()
        .enumerate()
        .map(|(ordinal, entry)| {
            let resource = entry
                .as_object()
                .and_then(|entry_object| entry_object.get("resource"))
                .cloned()
                .ok_or_else(|| {
                    invalid_data(format!(
                        "provider-directory FHIR Bundle entry {ordinal} lacks resource"
                    ))
                })?;
            Ok((ordinal, resource))
        })
        .collect()
}

pub fn decoded_resources(
    input: &[u8],
    framing: ProviderDirectoryInputFraming,
) -> io::Result<Vec<(usize, Value)>> {
    match framing {
        ProviderDirectoryInputFraming::Ndjson => ndjson_resources(input),
        ProviderDirectoryInputFraming::Bundle => bundle_resources(input),
    }
}

pub fn read_bounded_input(mut input: impl Read) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    input
        .by_ref()
        .take((PROVIDER_DIRECTORY_PROJECTION_MAX_INPUT_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > PROVIDER_DIRECTORY_PROJECTION_MAX_INPUT_BYTES {
        return Err(invalid_data(
            "provider-directory projection input exceeds the byte limit",
        ));
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::de::value::Error as ValueError;

    struct FailingReader;

    impl Read for FailingReader {
        fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::other("read failed"))
        }
    }

    #[test]
    fn strict_json_accepts_every_scalar_and_rejects_ambiguous_documents() {
        let expected =
            <ValueError as de::Error>::invalid_type(de::Unexpected::Unit, &StrictJsonValueVisitor);
        assert!(expected.to_string().contains("a strict JSON value"));
        assert!(StrictJsonValueVisitor
            .visit_bool::<ValueError>(true)
            .is_ok());
        assert!(StrictJsonValueVisitor.visit_i64::<ValueError>(-1).is_ok());
        assert!(StrictJsonValueVisitor.visit_u64::<ValueError>(1).is_ok());
        assert!(StrictJsonValueVisitor.visit_f64::<ValueError>(1.5).is_ok());
        assert!(StrictJsonValueVisitor
            .visit_str::<ValueError>("text")
            .is_ok());
        assert!(StrictJsonValueVisitor
            .visit_borrowed_str::<ValueError>("borrowed")
            .is_ok());
        assert!(StrictJsonValueVisitor
            .visit_string::<ValueError>("owned".to_owned())
            .is_ok());
        assert!(StrictJsonValueVisitor.visit_unit::<ValueError>().is_ok());

        let representative = br#"{
            "array":[true,false,null,-1,2,3.5,"text"],
            "object":{"nested":"value"}
        }"#;
        assert!(strict_json(representative, "representative").is_ok());
        assert!(strict_json(br#"{"id":"one","id":"two"}"#, "duplicate").is_err());
        assert!(strict_json(br#"{} trailing"#, "trailing").is_err());
        assert!(strict_json(&[0xff], "invalid-utf8").is_err());
    }

    #[test]
    fn retained_input_decoding_fences_framing_shapes_and_limits() {
        let ndjson = b" \t\r{\"resourceType\":\"Organization\",\"id\":\"one\"}\r \n\n";
        let decoded = decoded_resources(ndjson, ProviderDirectoryInputFraming::Ndjson).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].0, 0);

        let bundle = br#"{
            "resourceType":"Bundle",
            "entry":[{"resource":{"resourceType":"Practitioner","id":"two"}}]
        }"#;
        let decoded = decoded_resources(bundle, ProviderDirectoryInputFraming::Bundle).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].1["id"], "two");

        for invalid_bundle in [
            br#"[]"#.as_slice(),
            br#"{"resourceType":"Parameters","entry":[]}"#.as_slice(),
            br#"{"resourceType":"Bundle"}"#.as_slice(),
            br#"{"resourceType":"Bundle","entry":{}}"#.as_slice(),
            br#"{"resourceType":"Bundle","entry":[{}]}"#.as_slice(),
        ] {
            assert!(
                decoded_resources(invalid_bundle, ProviderDirectoryInputFraming::Bundle).is_err()
            );
        }

        let too_many_lines = b"{}\n".repeat(PROVIDER_DIRECTORY_PROJECTION_MAX_RESOURCE_COUNT + 1);
        assert!(decoded_resources(&too_many_lines, ProviderDirectoryInputFraming::Ndjson).is_err());
    }

    #[test]
    fn bounded_reader_accepts_exact_limit_and_rejects_one_extra_byte() {
        let exact = vec![b'x'; PROVIDER_DIRECTORY_PROJECTION_MAX_INPUT_BYTES];
        assert_eq!(
            read_bounded_input(exact.as_slice()).unwrap().len(),
            exact.len()
        );

        let oversized = vec![b'x'; PROVIDER_DIRECTORY_PROJECTION_MAX_INPUT_BYTES + 1];
        assert!(read_bounded_input(oversized.as_slice()).is_err());
        assert!(read_bounded_input(FailingReader).is_err());
    }

    #[test]
    fn bundle_decoder_applies_json_and_resource_count_bounds() {
        assert!(decoded_resources(b"{", ProviderDirectoryInputFraming::Bundle).is_err());

        let entries = (0..=PROVIDER_DIRECTORY_PROJECTION_MAX_RESOURCE_COUNT)
            .map(|_| serde_json::json!({"resource": {}}))
            .collect::<Vec<_>>();
        let input = serde_json::to_vec(&serde_json::json!({
            "resourceType": "Bundle",
            "entry": entries
        }))
        .expect("Bundle JSON");
        assert!(decoded_resources(&input, ProviderDirectoryInputFraming::Bundle).is_err());
    }
}
