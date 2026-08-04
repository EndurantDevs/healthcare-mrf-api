use crate::npi_identifier::{npi_validity, NpiValidity};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::env;
use std::fmt;
use std::fs::File;
use std::io::{self, Read};
use std::sync::atomic::{compiler_fence, Ordering};

const TOKEN_DOMAIN: &[u8] = b"healthporta.ptg.tin.v1";
const POLICY_ID_PREFIX: &str = "ptg-tin-hmac-sha256-v1:";
const MAX_POLICY_ID_BYTES: usize = 55;
const TOKEN_SECRET_BYTES: usize = 32;
const HMAC_BLOCK_BYTES: usize = 64;

pub const TIN_TOKEN_POLICY_ID_ENV: &str = "HLTHPRT_PTG2_TIN_TOKEN_POLICY_ID";
pub const TIN_TOKEN_SECRET_FILE_ENV: &str = "HLTHPRT_PTG2_TIN_TOKEN_SECRET_FILE";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum TaxIdentityState {
    MatchedEin = 1,
    Missing = 2,
    Malformed = 3,
    UnsupportedType = 4,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClassifiedTaxIdentity {
    pub state: TaxIdentityState,
    pub normalized_ein: Option<[u8; 9]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum TaxIdentityStateV2 {
    MatchedEin = 1,
    Missing = 2,
    Malformed = 3,
    UnsupportedType = 4,
    MatchedNpi = 5,
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
pub struct TaxIdentityToken {
    pub tin_id_128: [u8; 16],
    pub tin_hmac_sha256: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TaxIdentityObservation {
    pub state: TaxIdentityState,
    pub tin_hmac_sha256: Option<[u8; 32]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TaxIdentityObservationV2 {
    pub state: TaxIdentityStateV2,
    pub tin_hmac_sha256: Option<[u8; 32]>,
}

pub struct TinTokenPolicy {
    policy_id: String,
    secret: [u8; TOKEN_SECRET_BYTES],
}

impl fmt::Debug for TinTokenPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TinTokenPolicy")
            .field("policy_id", &self.policy_id)
            .finish_non_exhaustive()
    }
}

impl Drop for TinTokenPolicy {
    fn drop(&mut self) {
        clear_secret_bytes(&mut self.secret);
    }
}

impl TinTokenPolicy {
    pub fn from_secret(
        policy_id: String,
        mut secret: [u8; TOKEN_SECRET_BYTES],
    ) -> io::Result<Self> {
        if let Err(error) = validate_token_policy_id(&policy_id) {
            clear_secret_bytes(&mut secret);
            return Err(error);
        }
        Ok(Self { policy_id, secret })
    }

    pub fn policy_id(&self) -> &str {
        &self.policy_id
    }

    pub fn token_for_ein(&self, normalized_ein: &[u8; 9]) -> TaxIdentityToken {
        self.token_for_normalized_identity(b"ein", normalized_ein)
    }

    pub fn token_for_npi(&self, normalized_npi: &[u8; 10]) -> TaxIdentityToken {
        self.token_for_normalized_identity(b"npi", normalized_npi)
    }

    fn token_for_normalized_identity(
        &self,
        identity_type: &[u8],
        normalized_identity: &[u8],
    ) -> TaxIdentityToken {
        let mut message = canonical_tin_token_message(identity_type, normalized_identity);
        let tin_hmac_sha256 = hmac_sha256(&self.secret, &message);
        clear_secret_bytes(&mut message);
        let mut tin_id_128 = [0u8; 16];
        tin_id_128.copy_from_slice(&tin_hmac_sha256[..16]);
        TaxIdentityToken {
            tin_id_128,
            tin_hmac_sha256,
        }
    }

    pub fn observe(&self, tin: Option<&Value>) -> TaxIdentityObservation {
        let classified = classify_provider_group_tin(tin);
        let tin_hmac_sha256 = classified
            .normalized_ein
            .as_ref()
            .map(|normalized_ein| self.token_for_ein(normalized_ein).tin_hmac_sha256);
        TaxIdentityObservation {
            state: classified.state,
            tin_hmac_sha256,
        }
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
                        self.token_for_npi(normalized_npi).tin_hmac_sha256
                    }
                });
        TaxIdentityObservationV2 {
            state: classified.state,
            tin_hmac_sha256,
        }
    }
}

impl TaxIdentityObservation {
    pub fn merge(self, other: Self) -> io::Result<Self> {
        if let (Some(left), Some(right)) = (self.tin_hmac_sha256, other.tin_hmac_sha256) {
            if left != right {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "provider group has conflicting supported tax identities",
                ));
            }
        }
        if state_priority(other.state) > state_priority(self.state) {
            Ok(other)
        } else {
            Ok(self)
        }
    }
}

impl TaxIdentityObservationV2 {
    pub fn merge(self, other: Self) -> io::Result<Self> {
        if let (Some(left), Some(right)) = (self.tin_hmac_sha256, other.tin_hmac_sha256) {
            if left != right {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "provider group has conflicting supported tax identities",
                ));
            }
        }
        if state_priority_v2(other.state) > state_priority_v2(self.state) {
            Ok(other)
        } else {
            Ok(self)
        }
    }
}

pub fn load_tin_token_policy_from_env() -> io::Result<TinTokenPolicy> {
    let policy_id = env::var(TIN_TOKEN_POLICY_ID_ENV).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "PTG TIN token policy id must be configured",
        )
    })?;
    validate_token_policy_id(&policy_id)?;
    let secret_path = env::var(TIN_TOKEN_SECRET_FILE_ENV).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "PTG TIN token secret file must be configured",
        )
    })?;
    let secret = read_tin_token_secret(&secret_path)?;
    TinTokenPolicy::from_secret(policy_id, secret)
}

fn read_tin_token_secret(path: &str) -> io::Result<[u8; TOKEN_SECRET_BYTES]> {
    let mut file = File::open(path).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "PTG TIN token secret file could not be read",
        )
    })?;
    let mut secret = [0u8; TOKEN_SECRET_BYTES];
    if file.read_exact(&mut secret).is_err() {
        clear_secret_bytes(&mut secret);
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "PTG TIN token secret file must contain exactly 32 raw bytes",
        ));
    }
    let mut trailing = [0u8; 1];
    let trailing_count = match file.read(&mut trailing) {
        Ok(count) => count,
        Err(_) => {
            clear_secret_bytes(&mut secret);
            clear_secret_bytes(&mut trailing);
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "PTG TIN token secret file could not be read",
            ));
        }
    };
    clear_secret_bytes(&mut trailing);
    if trailing_count != 0 {
        clear_secret_bytes(&mut secret);
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "PTG TIN token secret file must contain exactly 32 raw bytes",
        ));
    }
    Ok(secret)
}

pub fn validate_token_policy_id(policy_id: &str) -> io::Result<()> {
    let key_id = policy_id.strip_prefix(POLICY_ID_PREFIX).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "PTG TIN token policy id is invalid",
        )
    })?;
    let valid_length = policy_id.len() <= MAX_POLICY_ID_BYTES && !key_id.is_empty();
    let valid_key = key_id.as_bytes().iter().enumerate().all(|(index, byte)| {
        byte.is_ascii_lowercase()
            || byte.is_ascii_digit()
            || (index > 0 && matches!(byte, b'.' | b'_' | b'-'))
    });
    if valid_length && valid_key {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "PTG TIN token policy id is invalid",
        ))
    }
}

pub fn classify_provider_group_tin(tin: Option<&Value>) -> ClassifiedTaxIdentity {
    let Some(tin) = tin else {
        return classified(TaxIdentityState::Missing, None);
    };
    if tin.is_null() {
        return classified(TaxIdentityState::Missing, None);
    }
    let Some(tin) = tin.as_object() else {
        return classified(TaxIdentityState::Malformed, None);
    };
    let tin_type = optional_trimmed_ascii_string(tin.get("type"));
    let tin_value = optional_trimmed_ascii_string(tin.get("value"));
    if matches!(tin_type, OptionalText::Missing) && matches!(tin_value, OptionalText::Missing) {
        return classified(TaxIdentityState::Missing, None);
    }
    let (OptionalText::Present(tin_type), OptionalText::Present(tin_value)) = (tin_type, tin_value)
    else {
        return classified(TaxIdentityState::Malformed, None);
    };
    if !tin_type.eq_ignore_ascii_case("ein") {
        return classified(TaxIdentityState::UnsupportedType, None);
    }
    match strict_normalized_ein(tin_value) {
        Some(normalized_ein) => classified(TaxIdentityState::MatchedEin, Some(normalized_ein)),
        None => classified(TaxIdentityState::Malformed, None),
    }
}

/// Classify a source tax identity without altering the v1 EIN-only path.
pub fn classify_provider_group_tin_v2(tin: Option<&Value>) -> ClassifiedTaxIdentityV2 {
    let Some(tin) = tin else {
        return classified_v2(TaxIdentityStateV2::Missing, None);
    };
    if tin.is_null() {
        return classified_v2(TaxIdentityStateV2::Missing, None);
    }
    let Some(tin) = tin.as_object() else {
        return classified_v2(TaxIdentityStateV2::Malformed, None);
    };
    let tin_type = optional_trimmed_ascii_string(tin.get("type"));
    let tin_value = optional_trimmed_ascii_string(tin.get("value"));
    if matches!(tin_type, OptionalText::Missing) && matches!(tin_value, OptionalText::Missing) {
        return classified_v2(TaxIdentityStateV2::Missing, None);
    }
    let (OptionalText::Present(tin_type), OptionalText::Present(tin_value)) = (tin_type, tin_value)
    else {
        return classified_v2(TaxIdentityStateV2::Malformed, None);
    };
    if tin_type.eq_ignore_ascii_case("ein") {
        return match strict_normalized_ein(tin_value) {
            Some(normalized_ein) => classified_v2(
                TaxIdentityStateV2::MatchedEin,
                Some(NormalizedTaxIdentity::Ein(normalized_ein)),
            ),
            None => classified_v2(TaxIdentityStateV2::Malformed, None),
        };
    }
    if tin_type.eq_ignore_ascii_case("npi") {
        return match strict_normalized_npi(tin_value) {
            Some(normalized_npi) => classified_v2(
                TaxIdentityStateV2::MatchedNpi,
                Some(NormalizedTaxIdentity::Npi(normalized_npi)),
            ),
            None => classified_v2(TaxIdentityStateV2::Malformed, None),
        };
    }
    classified_v2(TaxIdentityStateV2::UnsupportedType, None)
}

pub fn canonical_tin_token_message(tin_type: &[u8], normalized_tin: &[u8]) -> Vec<u8> {
    let type_length =
        u16::try_from(tin_type.len()).expect("canonical TIN type length must fit u16");
    let value_length =
        u16::try_from(normalized_tin.len()).expect("canonical TIN value length must fit u16");
    let mut message =
        Vec::with_capacity(TOKEN_DOMAIN.len() + 1 + 2 + tin_type.len() + 2 + normalized_tin.len());
    message.extend_from_slice(TOKEN_DOMAIN);
    message.push(0);
    message.extend_from_slice(&type_length.to_be_bytes());
    message.extend_from_slice(tin_type);
    message.extend_from_slice(&value_length.to_be_bytes());
    message.extend_from_slice(normalized_tin);
    message
}

#[derive(Clone, Copy)]
enum OptionalText<'a> {
    Missing,
    Present(&'a str),
    Invalid,
}

fn optional_trimmed_ascii_string(value: Option<&Value>) -> OptionalText<'_> {
    match value {
        None | Some(Value::Null) => OptionalText::Missing,
        Some(Value::String(value)) => {
            let value = value.trim_matches(|character: char| character.is_ascii_whitespace());
            if value.is_empty() {
                OptionalText::Missing
            } else {
                OptionalText::Present(value)
            }
        }
        Some(_) => OptionalText::Invalid,
    }
}

fn strict_normalized_ein(value: &str) -> Option<[u8; 9]> {
    let bytes = value.as_bytes();
    let mut normalized = [0u8; 9];
    if bytes.len() == 9 && bytes.iter().all(u8::is_ascii_digit) {
        normalized.copy_from_slice(bytes);
        return Some(normalized);
    }
    match bytes {
        [first, second, b'-', rest @ ..]
            if rest.len() == 7
                && first.is_ascii_digit()
                && second.is_ascii_digit()
                && rest.iter().all(u8::is_ascii_digit) =>
        {
            normalized[0] = *first;
            normalized[1] = *second;
            normalized[2..].copy_from_slice(rest);
            Some(normalized)
        }
        _ => None,
    }
}

fn strict_normalized_npi(value: &str) -> Option<[u8; 10]> {
    match npi_validity(value) {
        NpiValidity::Valid => {
            let mut normalized = [0u8; 10];
            normalized.copy_from_slice(value.as_bytes());
            Some(normalized)
        }
        NpiValidity::ChecksumInvalid | NpiValidity::StructuralInvalid | NpiValidity::Invalid => {
            None
        }
    }
}

fn classified(state: TaxIdentityState, normalized_ein: Option<[u8; 9]>) -> ClassifiedTaxIdentity {
    ClassifiedTaxIdentity {
        state,
        normalized_ein,
    }
}

fn classified_v2(
    state: TaxIdentityStateV2,
    normalized_identity: Option<NormalizedTaxIdentity>,
) -> ClassifiedTaxIdentityV2 {
    ClassifiedTaxIdentityV2 {
        state,
        normalized_identity,
    }
}

fn state_priority(state: TaxIdentityState) -> u8 {
    match state {
        TaxIdentityState::MatchedEin => 4,
        TaxIdentityState::UnsupportedType => 3,
        TaxIdentityState::Malformed => 2,
        TaxIdentityState::Missing => 1,
    }
}

fn state_priority_v2(state: TaxIdentityStateV2) -> u8 {
    match state {
        TaxIdentityStateV2::MatchedEin | TaxIdentityStateV2::MatchedNpi => 4,
        TaxIdentityStateV2::UnsupportedType => 3,
        TaxIdentityStateV2::Malformed => 2,
        TaxIdentityStateV2::Missing => 1,
    }
}

fn hmac_sha256(secret: &[u8; TOKEN_SECRET_BYTES], message: &[u8]) -> [u8; 32] {
    let mut inner_key = [0x36u8; HMAC_BLOCK_BYTES];
    let mut outer_key = [0x5cu8; HMAC_BLOCK_BYTES];
    for (index, secret_byte) in secret.iter().enumerate() {
        inner_key[index] ^= secret_byte;
        outer_key[index] ^= secret_byte;
    }
    let mut inner = Sha256::new();
    inner.update(inner_key.as_slice());
    inner.update(message);
    let mut inner_digest: [u8; 32] = inner.finalize().into();
    let mut outer = Sha256::new();
    outer.update(outer_key.as_slice());
    outer.update(inner_digest.as_slice());
    let result = outer.finalize().into();
    clear_secret_bytes(&mut inner_key);
    clear_secret_bytes(&mut outer_key);
    clear_secret_bytes(&mut inner_digest);
    result
}

fn clear_secret_bytes(bytes: &mut [u8]) {
    for byte in bytes {
        // SAFETY: `byte` is a valid unique mutable reference. A volatile
        // store prevents release LTO from eliding the secret wipe.
        unsafe { std::ptr::write_volatile(byte, 0) };
    }
    compiler_fence(Ordering::SeqCst);
}

#[cfg(test)]
mod tests;
