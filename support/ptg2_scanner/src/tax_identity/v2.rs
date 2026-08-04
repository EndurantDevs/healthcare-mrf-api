use super::{
    optional_trimmed_ascii_string, strict_normalized_ein, OptionalText, TaxIdentityToken,
    TinTokenPolicy,
};
use crate::npi_identifier::{npi_validity, NpiValidity};
use serde_json::Value;
use std::fmt;
use std::io;

const INVALID_NPI_TOKEN_MESSAGE: &str = "PTG NPI billing identity is invalid";
const INVALID_OBSERVATION_MESSAGE: &str = "provider group has invalid tax identity observation";
const CONFLICTING_OBSERVATION_MESSAGE: &str =
    "provider group has conflicting supported tax identities";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum TaxIdentityStateV2 {
    MatchedEin = 1,
    Missing = 2,
    Malformed = 3,
    UnsupportedType = 4,
    MatchedNpi = 5,
}

impl TaxIdentityStateV2 {
    fn is_matched(self) -> bool {
        matches!(self, Self::MatchedEin | Self::MatchedNpi)
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum NormalizedTaxIdentity {
    Ein([u8; 9]),
    Npi([u8; 10]),
}

impl fmt::Debug for NormalizedTaxIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ein(_) => formatter.write_str("Ein(<redacted>)"),
            Self::Npi(_) => formatter.write_str("Npi(<redacted>)"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClassifiedTaxIdentityV2 {
    pub state: TaxIdentityStateV2,
    pub normalized_identity: Option<NormalizedTaxIdentity>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TaxIdentityObservationV2 {
    pub state: TaxIdentityStateV2,
    pub tin_hmac_sha256: Option<[u8; 32]>,
}

impl TinTokenPolicy {
    pub fn token_for_npi(&self, normalized_npi: &[u8; 10]) -> io::Result<TaxIdentityToken> {
        if !normalized_npi_bytes_are_valid(normalized_npi) {
            return Err(invalid_npi_token_error());
        }
        Ok(self.token_for_validated_npi(normalized_npi))
    }

    fn token_for_validated_npi(&self, normalized_npi: &[u8; 10]) -> TaxIdentityToken {
        debug_assert!(normalized_npi_bytes_are_valid(normalized_npi));
        self.token_for_normalized_identity(b"npi", normalized_npi)
    }

    pub fn observe_v2(&self, tin: Option<&Value>) -> TaxIdentityObservationV2 {
        let classified = classify_provider_group_tin_v2(tin);
        let tin_hmac_sha256 =
            classified
                .normalized_identity
                .as_ref()
                .map(|identity| match identity {
                    NormalizedTaxIdentity::Ein(normalized_ein) => {
                        self.token_for_ein(normalized_ein).tin_hmac_sha256
                    }
                    NormalizedTaxIdentity::Npi(normalized_npi) => {
                        self.token_for_validated_npi(normalized_npi).tin_hmac_sha256
                    }
                });
        TaxIdentityObservationV2 {
            state: classified.state,
            tin_hmac_sha256,
        }
    }
}

impl TaxIdentityObservationV2 {
    pub fn merge(self, other: Self) -> io::Result<Self> {
        self.validate_shape()?;
        other.validate_shape()?;
        if self.state.is_matched()
            && other.state.is_matched()
            && (self.state != other.state || self.tin_hmac_sha256 != other.tin_hmac_sha256)
        {
            return Err(conflicting_observation_error());
        }
        if state_priority(other.state) > state_priority(self.state) {
            Ok(other)
        } else {
            Ok(self)
        }
    }

    fn validate_shape(self) -> io::Result<()> {
        let valid = match (self.state, self.tin_hmac_sha256) {
            (TaxIdentityStateV2::MatchedEin | TaxIdentityStateV2::MatchedNpi, Some(hmac)) => {
                hmac != [0u8; 32]
            }
            (
                TaxIdentityStateV2::Missing
                | TaxIdentityStateV2::Malformed
                | TaxIdentityStateV2::UnsupportedType,
                None,
            ) => true,
            _ => false,
        };
        if valid {
            Ok(())
        } else {
            Err(invalid_observation_error())
        }
    }
}

/// Classify a source tax identity without altering the v1 EIN-only path.
pub fn classify_provider_group_tin_v2(tin: Option<&Value>) -> ClassifiedTaxIdentityV2 {
    let Some(tin) = tin else {
        return classified(TaxIdentityStateV2::Missing, None);
    };
    if tin.is_null() {
        return classified(TaxIdentityStateV2::Missing, None);
    }
    let Some(tin) = tin.as_object() else {
        return classified(TaxIdentityStateV2::Malformed, None);
    };
    let tin_type = optional_trimmed_ascii_string(tin.get("type"));
    let tin_value = optional_trimmed_ascii_string(tin.get("value"));
    if matches!(tin_type, OptionalText::Missing) && matches!(tin_value, OptionalText::Missing) {
        return classified(TaxIdentityStateV2::Missing, None);
    }
    let (OptionalText::Present(tin_type), OptionalText::Present(tin_value)) = (tin_type, tin_value)
    else {
        return classified(TaxIdentityStateV2::Malformed, None);
    };
    if tin_type.eq_ignore_ascii_case("ein") {
        return match strict_normalized_ein(tin_value) {
            Some(normalized_ein) => classified(
                TaxIdentityStateV2::MatchedEin,
                Some(NormalizedTaxIdentity::Ein(normalized_ein)),
            ),
            None => classified(TaxIdentityStateV2::Malformed, None),
        };
    }
    if tin_type.eq_ignore_ascii_case("npi") {
        return match strict_normalized_npi(tin_value) {
            Some(normalized_npi) => classified(
                TaxIdentityStateV2::MatchedNpi,
                Some(NormalizedTaxIdentity::Npi(normalized_npi)),
            ),
            None => classified(TaxIdentityStateV2::Malformed, None),
        };
    }
    classified(TaxIdentityStateV2::UnsupportedType, None)
}

fn normalized_npi_bytes_are_valid(normalized_npi: &[u8; 10]) -> bool {
    std::str::from_utf8(normalized_npi).is_ok_and(|value| npi_validity(value) == NpiValidity::Valid)
}

fn strict_normalized_npi(value: &str) -> Option<[u8; 10]> {
    if npi_validity(value) != NpiValidity::Valid {
        return None;
    }
    let mut normalized = [0u8; 10];
    normalized.copy_from_slice(value.as_bytes());
    Some(normalized)
}

fn classified(
    state: TaxIdentityStateV2,
    normalized_identity: Option<NormalizedTaxIdentity>,
) -> ClassifiedTaxIdentityV2 {
    ClassifiedTaxIdentityV2 {
        state,
        normalized_identity,
    }
}

fn state_priority(state: TaxIdentityStateV2) -> u8 {
    match state {
        TaxIdentityStateV2::MatchedEin | TaxIdentityStateV2::MatchedNpi => 4,
        TaxIdentityStateV2::UnsupportedType => 3,
        TaxIdentityStateV2::Malformed => 2,
        TaxIdentityStateV2::Missing => 1,
    }
}

fn invalid_npi_token_error() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, INVALID_NPI_TOKEN_MESSAGE)
}

fn invalid_observation_error() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, INVALID_OBSERVATION_MESSAGE)
}

fn conflicting_observation_error() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, CONFLICTING_OBSERVATION_MESSAGE)
}

#[cfg(test)]
mod tests;
