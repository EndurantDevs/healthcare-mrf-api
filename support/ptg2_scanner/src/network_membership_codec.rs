//! Bounded, atomic encoding of exact provider-location network membership.

use crate::npi_identifier::{npi_validity, NpiValidity};
use serde::de::{self, DeserializeSeed, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::borrow::Cow;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};

pub const MAX_ROWS: usize = 5_000;
pub const MAX_INPUT_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_FIELD_BYTES: usize = 1_024;
pub const MAX_COPY_BYTES: usize = 16 * 1024 * 1024;
pub const COPY_COLUMNS: [&str; 5] = [
    "network_id",
    "provider_system",
    "provider_id",
    "location_id",
    "evidence_id",
];
pub(crate) const COPY_HEADER: &[u8] = b"PGCOPY\n\xff\r\n\0\0\0\0\0\0\0\0\0";

#[derive(Debug, Eq, PartialEq)]
pub struct EncodedMembershipBatch {
    pub copy_bytes: Vec<u8>,
    pub row_count: usize,
}

/// Row indexes are zero-based. Messages never include submitted identifier values.
#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct MembershipBatchError {
    pub code: &'static str,
    pub row_index: Option<usize>,
    pub field: Option<&'static str>,
    pub message: &'static str,
}

impl fmt::Display for MembershipBatchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.message)
    }
}

impl std::error::Error for MembershipBatchError {}

fn error(
    code: &'static str,
    row_index: Option<usize>,
    field: Option<&'static str>,
    message: &'static str,
) -> MembershipBatchError {
    MembershipBatchError {
        code,
        row_index,
        field,
        message,
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MembershipRow<'a> {
    network_id: i32,
    #[serde(borrow)]
    provider_system: Cow<'a, str>,
    #[serde(borrow)]
    provider_id: Cow<'a, str>,
    #[serde(borrow)]
    location_id: Cow<'a, str>,
    #[serde(borrow)]
    evidence_id: Cow<'a, str>,
}

pub fn encode_network_membership_batch(
    input: &[u8],
) -> Result<EncodedMembershipBatch, MembershipBatchError> {
    encode_batch(input, None)
}

/// Cancellation discards the entire privately encoded batch.
pub fn encode_network_membership_batch_cancellable(
    input: &[u8],
    cancelled: &AtomicBool,
) -> Result<EncodedMembershipBatch, MembershipBatchError> {
    encode_batch(input, Some(cancelled))
}

fn encode_batch(
    input: &[u8],
    cancelled: Option<&AtomicBool>,
) -> Result<EncodedMembershipBatch, MembershipBatchError> {
    if input.len() > MAX_INPUT_BYTES {
        return Err(error(
            "input_limit",
            None,
            None,
            "Membership input exceeds byte limit",
        ));
    }
    let mut state = BatchEncoder {
        output: Vec::with_capacity(input.len().min(64 * 1024)),
        row_count: 0,
        failure: None,
        cancelled,
    };
    state.check_cancelled(None)?;
    state.output.extend_from_slice(COPY_HEADER);
    let mut decoder = serde_json::Deserializer::from_slice(input);
    if (&mut state).deserialize(&mut decoder).is_err() {
        return Err(state.failure.unwrap_or_else(|| {
            error(
                "invalid_json",
                Some(state.row_count),
                None,
                "Expected a complete JSON membership array",
            )
        }));
    }
    if decoder.end().is_err() {
        return Err(error(
            "invalid_json",
            None,
            None,
            "Unexpected data after membership array",
        ));
    }
    state.check_cancelled(None)?;
    state.output.extend_from_slice(&(-1i16).to_be_bytes());
    Ok(EncodedMembershipBatch {
        copy_bytes: state.output,
        row_count: state.row_count,
    })
}

struct BatchEncoder<'a> {
    output: Vec<u8>,
    row_count: usize,
    failure: Option<MembershipBatchError>,
    cancelled: Option<&'a AtomicBool>,
}

impl BatchEncoder<'_> {
    fn check_cancelled(&self, row: Option<usize>) -> Result<(), MembershipBatchError> {
        if self
            .cancelled
            .is_some_and(|flag| flag.load(Ordering::Relaxed))
        {
            return Err(error("cancelled", row, None, "Membership batch cancelled"));
        }
        Ok(())
    }

    fn encode_row(&mut self, raw: &RawValue) -> Result<(), MembershipBatchError> {
        let index = Some(self.row_count);
        self.check_cancelled(index)?;
        let row: MembershipRow<'_> = serde_json::from_str(raw.get()).map_err(|_| {
            error(
                "invalid_row",
                index,
                None,
                "Membership row has missing, unknown or invalid fields",
            )
        })?;
        if row.network_id <= 0 {
            return Err(error(
                "invalid_network_id",
                index,
                Some("network_id"),
                "Network ID must be a positive int32",
            ));
        }
        if !matches!(
            row.provider_system.as_ref(),
            "npi" | "provider_directory" | "manual"
        ) {
            return Err(error(
                "invalid_provider_system",
                index,
                Some("provider_system"),
                "Provider namespace is unsupported",
            ));
        }
        validate_text(&row.provider_id, index, "provider_id")?;
        validate_text(&row.evidence_id, index, "evidence_id")?;
        if row.provider_system == "npi" && npi_validity(&row.provider_id) != NpiValidity::Valid {
            return Err(error(
                "invalid_npi",
                index,
                Some("provider_id"),
                "NPI must have valid structure and checksum",
            ));
        }
        let location = uuid_bytes(&row.location_id).ok_or_else(|| {
            error(
                "invalid_location_id",
                index,
                Some("location_id"),
                "Location ID must be a nonzero hyphenated UUID",
            )
        })?;
        let row_bytes = 2
            + 5 * 4
            + 4
            + row.provider_system.len()
            + row.provider_id.len()
            + 16
            + row.evidence_id.len();
        if self.output.len() + row_bytes + 2 > MAX_COPY_BYTES {
            return Err(error(
                "output_limit",
                index,
                None,
                "Membership COPY output exceeds byte limit",
            ));
        }
        self.output.extend_from_slice(&5i16.to_be_bytes());
        write_field(&row.network_id.to_be_bytes(), &mut self.output);
        write_field(row.provider_system.as_bytes(), &mut self.output);
        write_field(row.provider_id.as_bytes(), &mut self.output);
        write_field(&location, &mut self.output);
        write_field(row.evidence_id.as_bytes(), &mut self.output);
        self.row_count += 1;
        Ok(())
    }
}

impl<'de> DeserializeSeed<'de> for &mut BatchEncoder<'_> {
    type Value = ();

    fn deserialize<D: de::Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        decoder.deserialize_seq(self)
    }
}

impl<'de> Visitor<'de> for &mut BatchEncoder<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an array of exact network membership rows")
    }

    fn visit_seq<S: SeqAccess<'de>>(self, mut sequence: S) -> Result<(), S::Error> {
        while let Some(raw) = sequence.next_element::<&RawValue>()? {
            let result = if self.row_count >= MAX_ROWS {
                Err(error(
                    "row_limit",
                    Some(self.row_count),
                    None,
                    "Membership batch exceeds row limit",
                ))
            } else {
                self.encode_row(raw)
            };
            if let Err(failure) = result {
                self.failure = Some(failure);
                return Err(de::Error::custom("membership batch rejected"));
            }
        }
        Ok(())
    }
}

fn validate_text(
    value: &str,
    row: Option<usize>,
    field: &'static str,
) -> Result<(), MembershipBatchError> {
    if value.is_empty()
        || value.len() > MAX_FIELD_BYTES
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(error(
            "invalid_text",
            row,
            Some(field),
            "Identifier text must be nonempty, bounded and contain no control or edge whitespace",
        ));
    }
    Ok(())
}

pub(crate) fn uuid_bytes(value: &str) -> Option<[u8; 16]> {
    let bytes = value.as_bytes();
    if bytes.len() != 36 {
        return None;
    }
    let mut result = [0u8; 16];
    let mut digit_index = 0;
    for (index, byte) in bytes.iter().copied().enumerate() {
        if matches!(index, 8 | 13 | 18 | 23) {
            if byte != b'-' {
                return None;
            }
            continue;
        }
        let nibble = match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            b'A'..=b'F' => byte - b'A' + 10,
            _ => return None,
        };
        result[digit_index / 2] |= nibble << if digit_index % 2 == 0 { 4 } else { 0 };
        digit_index += 1;
    }
    result.iter().any(|byte| *byte != 0).then_some(result)
}

pub(crate) fn write_field(bytes: &[u8], output: &mut Vec<u8>) {
    // Every field is validated against a smaller bound before encoding.
    output.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
    output.extend_from_slice(bytes);
}
