//! Bounded catalog observations with canonical integers separate from source aliases.

use serde::de::{self, DeserializeSeed, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::fmt;

pub const MAX_ROWS: usize = 5_000;
pub const MAX_INPUT_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_LABEL_BYTES: usize = 2_048;
pub const MAX_ALIASES: usize = 100;

/// Error fields are static and never echo a submitted name or source identifier.
#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct CatalogValueError {
    pub code: &'static str,
    pub row_index: Option<usize>,
    pub field: Option<&'static str>,
}

impl fmt::Display for CatalogValueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Network catalog batch rejected: {}", self.code)
    }
}

impl std::error::Error for CatalogValueError {}

fn failure(
    code: &'static str,
    row_index: Option<usize>,
    field: Option<&'static str>,
) -> CatalogValueError {
    CatalogValueError {
        code,
        row_index,
        field,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityStatus {
    Resolved,
    Unresolved,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceAliasType {
    LegacyFhirUuid,
    AcaChecksum,
    PtgLabel,
    RibbonNetworkId,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct ValidatedSourceAlias {
    pub source_system: String,
    pub source_id: String,
    pub alias_type: SourceAliasType,
    pub alias_value: String,
    /// Comparison spelling remains a source-scoped alias, never a canonical ID.
    pub canonical_alias_value: String,
    pub scope_key: String,
    pub evidence_id: String,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct ValidatedNetworkValue {
    pub network_id: Option<i32>,
    pub status: IdentityStatus,
    pub display_name: String,
    pub raw_display_name: String,
    pub aliases: Vec<String>,
    pub raw_aliases: Vec<String>,
    pub source_aliases: Vec<ValidatedSourceAlias>,
    pub directory_available: bool,
    pub priceable: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CatalogRow<'a> {
    #[serde(borrow)]
    network_id: &'a RawValue,
    display_name: String,
    aliases: Vec<String>,
    source_aliases: Vec<SourceAlias>,
    directory_available: bool,
    priceable: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceAlias {
    source_system: String,
    source_id: String,
    alias_type: SourceAliasType,
    alias_value: String,
    scope_key: String,
    evidence_id: String,
}

/// Preserve input order and repeated observations; any failure discards the batch.
pub fn validate_network_catalog_batch(
    input: &[u8],
) -> Result<Vec<ValidatedNetworkValue>, CatalogValueError> {
    if input.len() > MAX_INPUT_BYTES {
        return Err(failure("input_limit", None, None));
    }
    let mut state = CatalogValidator {
        observations: Vec::new(),
        error: None,
    };
    let mut decoder = serde_json::Deserializer::from_slice(input);
    if (&mut state).deserialize(&mut decoder).is_err() {
        return Err(state
            .error
            .unwrap_or_else(|| failure("invalid_json", Some(state.observations.len()), None)));
    }
    if decoder.end().is_err() {
        return Err(failure("invalid_json", None, None));
    }
    Ok(state.observations)
}

struct CatalogValidator {
    observations: Vec<ValidatedNetworkValue>,
    error: Option<CatalogValueError>,
}

impl<'de> DeserializeSeed<'de> for &mut CatalogValidator {
    type Value = ();

    fn deserialize<D: de::Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        decoder.deserialize_seq(self)
    }
}

impl<'de> Visitor<'de> for &mut CatalogValidator {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an array of bounded network catalog observations")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<(), A::Error> {
        while let Some(raw_observation) = sequence.next_element::<&RawValue>()? {
            if self.observations.len() == MAX_ROWS {
                self.error = Some(failure("row_limit", Some(MAX_ROWS), None));
                return Err(de::Error::custom("Network catalog row limit exceeded"));
            }
            match validate_row(raw_observation, self.observations.len()) {
                Ok(observation) => self.observations.push(observation),
                Err(error) => {
                    self.error = Some(error);
                    return Err(de::Error::custom("Network catalog observation rejected"));
                }
            }
        }
        Ok(())
    }
}

fn validate_row(
    raw_observation: &RawValue,
    row_index: usize,
) -> Result<ValidatedNetworkValue, CatalogValueError> {
    let observation: CatalogRow<'_> = serde_json::from_str(raw_observation.get())
        .map_err(|_| failure("invalid_row", Some(row_index), None))?;
    let network_id: Option<i32> = serde_json::from_str(observation.network_id.get())
        .map_err(|_| failure("invalid_network_id", Some(row_index), Some("network_id")))?;
    if network_id.is_some_and(|identifier| identifier <= 0) {
        return Err(failure(
            "invalid_network_id",
            Some(row_index),
            Some("network_id"),
        ));
    }
    validate_text(
        &observation.display_name,
        MAX_LABEL_BYTES,
        row_index,
        "display_name",
    )?;
    if observation.aliases.len() > MAX_ALIASES || observation.source_aliases.len() > MAX_ALIASES {
        let field = if observation.aliases.len() > MAX_ALIASES {
            "aliases"
        } else {
            "source_aliases"
        };
        return Err(failure("alias_limit", Some(row_index), Some(field)));
    }
    let mut aliases = Vec::with_capacity(observation.aliases.len());
    for alias in &observation.aliases {
        validate_text(alias, MAX_LABEL_BYTES, row_index, "aliases")?;
        aliases.push(alias.trim().to_owned());
    }
    let source_aliases = observation
        .source_aliases
        .into_iter()
        .map(|alias| validate_source_alias(alias, row_index))
        .collect::<Result<_, _>>()?;
    Ok(ValidatedNetworkValue {
        network_id,
        status: if network_id.is_some() {
            IdentityStatus::Resolved
        } else {
            IdentityStatus::Unresolved
        },
        display_name: observation.display_name.trim().to_owned(),
        raw_display_name: observation.display_name,
        aliases,
        raw_aliases: observation.aliases,
        source_aliases,
        directory_available: observation.directory_available,
        priceable: observation.priceable,
    })
}

fn validate_text(
    text: &str,
    maximum_bytes: usize,
    row_index: usize,
    field: &'static str,
) -> Result<(), CatalogValueError> {
    if text.len() > maximum_bytes {
        return Err(failure("field_limit", Some(row_index), Some(field)));
    }
    if text.trim().is_empty() {
        return Err(failure("blank_field", Some(row_index), Some(field)));
    }
    if text.contains('\0') {
        return Err(failure("invalid_text", Some(row_index), Some(field)));
    }
    Ok(())
}

fn validate_source_alias(
    alias: SourceAlias,
    row_index: usize,
) -> Result<ValidatedSourceAlias, CatalogValueError> {
    for (text, maximum_bytes, field) in [
        (
            alias.source_system.as_str(),
            64,
            "source_aliases.source_system",
        ),
        (alias.source_id.as_str(), 128, "source_aliases.source_id"),
        (
            alias.alias_value.as_str(),
            512,
            "source_aliases.alias_value",
        ),
        (alias.scope_key.as_str(), 512, "source_aliases.scope_key"),
        (
            alias.evidence_id.as_str(),
            512,
            "source_aliases.evidence_id",
        ),
    ] {
        validate_text(text, maximum_bytes, row_index, field)?;
    }
    let canonical_alias_value = match alias.alias_type {
        SourceAliasType::LegacyFhirUuid => canonical_uuid(&alias.alias_value).ok_or_else(|| {
            failure(
                "invalid_legacy_uuid",
                Some(row_index),
                Some("source_aliases.alias_value"),
            )
        })?,
        SourceAliasType::AcaChecksum => {
            canonical_checksum(&alias.alias_value).ok_or_else(|| {
                failure(
                    "invalid_aca_checksum",
                    Some(row_index),
                    Some("source_aliases.alias_value"),
                )
            })?
        }
        SourceAliasType::PtgLabel | SourceAliasType::RibbonNetworkId => alias.alias_value.clone(),
    };
    Ok(ValidatedSourceAlias {
        source_system: alias.source_system,
        source_id: alias.source_id,
        alias_type: alias.alias_type,
        alias_value: alias.alias_value,
        canonical_alias_value,
        scope_key: alias.scope_key,
        evidence_id: alias.evidence_id,
    })
}

fn canonical_uuid(alias_value: &str) -> Option<String> {
    let spelling = alias_value.as_bytes();
    if spelling.len() != 36 {
        return None;
    }
    let mut is_nonzero = false;
    for (index, byte) in spelling.iter().enumerate() {
        if matches!(index, 8 | 13 | 18 | 23) {
            if *byte != b'-' {
                return None;
            }
        } else if !byte.is_ascii_hexdigit() {
            return None;
        } else {
            is_nonzero |= *byte != b'0';
        }
    }
    is_nonzero.then(|| alias_value.to_ascii_lowercase())
}

fn canonical_checksum(alias_value: &str) -> Option<String> {
    let digits = alias_value.strip_prefix('-').unwrap_or(alias_value);
    if digits.is_empty() || !digits.bytes().all(|digit| digit.is_ascii_digit()) {
        return None;
    }
    alias_value
        .parse::<i32>()
        .ok()
        .map(|checksum| checksum.to_string())
}
