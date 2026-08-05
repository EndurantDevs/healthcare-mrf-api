//! Pure codec and bounded validation for the existing tax-identity v1 sidecar.
//!
//! This module authenticates binary framing, token shape, record limits, and
//! strict provider-group ordering. Artifact digests, scanner metadata, and
//! provider-group-universe parity belong to a bundle validator.

use crate::tax_identity::{validate_token_policy_id, TaxIdentityState};
use std::fmt;
use std::io::{self, Read};
use std::str;

pub const TAX_IDENTITY_SIDECAR_V1_MAGIC: &[u8; 8] = b"PTG2TAX1";
pub const TAX_IDENTITY_SIDECAR_V1_FORMAT: &str = "ptg2_provider_group_tax_identity_v1";
pub const TAX_IDENTITY_SIDECAR_V1_FORMAT_VERSION: u16 = 1;
pub const TAX_IDENTITY_SIDECAR_V1_RECORD_BYTES: usize = 65;

const FIXED_HEADER_BYTES: usize = 8 + 2 + 2 + 1;
const MAX_ENCODED_POLICY_ID_BYTES: usize = u8::MAX as usize;

/// Snapshot-local provider-group global identifier encoded in every record.
pub type ProviderGroupGlobalId = [u8; 16];
/// Bounded lookup locator, equal to the first 16 bytes of the full HMAC.
pub type TinId128 = [u8; 16];
/// Full domain-separated tax-identity HMAC used for collision verification.
pub type TinHmacSha256 = [u8; 32];

const ZERO_TIN_ID_128: TinId128 = [0; 16];
const ZERO_TIN_HMAC_SHA256: TinHmacSha256 = [0; 32];

const INVALID_HEADER: &str = "PTG tax identity sidecar v1 header is invalid";
const TRUNCATED_HEADER: &str = "PTG tax identity sidecar v1 header is truncated";
const INVALID_RECORD: &str = "PTG tax identity sidecar v1 record is invalid";
const TRUNCATED_RECORD: &str = "PTG tax identity sidecar v1 record is truncated";
const INVALID_ORDER: &str = "PTG tax identity sidecar v1 group ids must be strictly increasing";
const RECORD_LIMIT_EXCEEDED: &str = "PTG tax identity sidecar v1 record limit exceeded";
const VALIDATOR_POISONED: &str = "PTG tax identity sidecar v1 validator is poisoned";

#[derive(Clone, Eq, PartialEq)]
pub struct TaxIdentitySidecarV1Header {
    policy_id: String,
}

impl fmt::Debug for TaxIdentitySidecarV1Header {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaxIdentitySidecarV1Header")
            .field("policy_id", &"<validated>")
            .finish()
    }
}

impl TaxIdentitySidecarV1Header {
    pub fn new(policy_id: String) -> io::Result<Self> {
        validate_token_policy_id(&policy_id)?;
        Ok(Self { policy_id })
    }

    pub fn policy_id(&self) -> &str {
        &self.policy_id
    }

    pub fn encode(&self) -> Vec<u8> {
        let policy_id = self.policy_id.as_bytes();
        let policy_id_length = u8::try_from(policy_id.len())
            .expect("validated PTG tax identity policy ids fit in one byte");
        let mut encoded = Vec::with_capacity(FIXED_HEADER_BYTES + policy_id.len());
        encoded.extend_from_slice(TAX_IDENTITY_SIDECAR_V1_MAGIC);
        encoded.extend_from_slice(&TAX_IDENTITY_SIDECAR_V1_FORMAT_VERSION.to_le_bytes());
        encoded.extend_from_slice(&(TAX_IDENTITY_SIDECAR_V1_RECORD_BYTES as u16).to_le_bytes());
        encoded.push(policy_id_length);
        encoded.extend_from_slice(policy_id);
        encoded
    }

    pub fn decode(encoded: &[u8]) -> io::Result<Self> {
        if encoded.len() < FIXED_HEADER_BYTES {
            return Err(invalid_data(TRUNCATED_HEADER));
        }
        if &encoded[..8] != TAX_IDENTITY_SIDECAR_V1_MAGIC {
            return Err(invalid_data(INVALID_HEADER));
        }
        let version = u16::from_le_bytes([encoded[8], encoded[9]]);
        let record_bytes = u16::from_le_bytes([encoded[10], encoded[11]]);
        let policy_id_length = usize::from(encoded[12]);
        if version != TAX_IDENTITY_SIDECAR_V1_FORMAT_VERSION
            || usize::from(record_bytes) != TAX_IDENTITY_SIDECAR_V1_RECORD_BYTES
            || encoded.len() != FIXED_HEADER_BYTES + policy_id_length
        {
            return Err(invalid_data(INVALID_HEADER));
        }
        let policy_id = str::from_utf8(&encoded[FIXED_HEADER_BYTES..])
            .map_err(|_| invalid_data(INVALID_HEADER))?;
        validate_token_policy_id(policy_id).map_err(|_| invalid_data(INVALID_HEADER))?;
        Ok(Self {
            policy_id: policy_id.to_owned(),
        })
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct TaxIdentitySidecarV1Record {
    provider_group_global_id: ProviderGroupGlobalId,
    state: TaxIdentityState,
    tin_id_128: TinId128,
    tin_hmac_sha256: TinHmacSha256,
}

impl fmt::Debug for TaxIdentitySidecarV1Record {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaxIdentitySidecarV1Record")
            .field("provider_group_global_id", &"<opaque>")
            .field("state", &self.state)
            .field("tin_id_128", &"<redacted>")
            .field("tin_hmac_sha256", &"<redacted>")
            .finish()
    }
}

impl TaxIdentitySidecarV1Record {
    pub fn new(
        provider_group_global_id: ProviderGroupGlobalId,
        state: TaxIdentityState,
        tin_id_128: TinId128,
        tin_hmac_sha256: TinHmacSha256,
    ) -> io::Result<Self> {
        validate_token_shape(
            state,
            &tin_id_128,
            &tin_hmac_sha256,
            io::ErrorKind::InvalidInput,
        )?;
        Ok(Self {
            provider_group_global_id,
            state,
            tin_id_128,
            tin_hmac_sha256,
        })
    }

    pub fn provider_group_global_id(&self) -> &ProviderGroupGlobalId {
        &self.provider_group_global_id
    }

    pub fn state(&self) -> TaxIdentityState {
        self.state
    }

    pub fn tin_id_128(&self) -> &TinId128 {
        &self.tin_id_128
    }

    pub fn tin_hmac_sha256(&self) -> &TinHmacSha256 {
        &self.tin_hmac_sha256
    }

    pub fn encode(&self) -> [u8; TAX_IDENTITY_SIDECAR_V1_RECORD_BYTES] {
        let mut encoded = [0u8; TAX_IDENTITY_SIDECAR_V1_RECORD_BYTES];
        encoded[..16].copy_from_slice(&self.provider_group_global_id);
        encoded[16] = self.state as u8;
        encoded[17..33].copy_from_slice(&self.tin_id_128);
        encoded[33..].copy_from_slice(&self.tin_hmac_sha256);
        encoded
    }

    pub fn decode(encoded: &[u8]) -> io::Result<Self> {
        if encoded.len() != TAX_IDENTITY_SIDECAR_V1_RECORD_BYTES {
            return Err(invalid_data(INVALID_RECORD));
        }
        let state = decode_state(encoded[16])?;
        let mut provider_group_global_id = [0u8; 16];
        let mut tin_id_128 = [0u8; 16];
        let mut tin_hmac_sha256 = [0u8; 32];
        provider_group_global_id.copy_from_slice(&encoded[..16]);
        tin_id_128.copy_from_slice(&encoded[17..33]);
        tin_hmac_sha256.copy_from_slice(&encoded[33..]);
        validate_token_shape(
            state,
            &tin_id_128,
            &tin_hmac_sha256,
            io::ErrorKind::InvalidData,
        )?;
        Ok(Self {
            provider_group_global_id,
            state,
            tin_id_128,
            tin_hmac_sha256,
        })
    }
}

/// Fixed-memory stream validator for framing, record limits, and group order.
pub struct TaxIdentitySidecarV1StreamValidator<R> {
    reader: R,
    header: TaxIdentitySidecarV1Header,
    previous_group_id: Option<ProviderGroupGlobalId>,
    records_validated: u64,
    record_limit: u64,
    finished: bool,
    poisoned: bool,
}

impl<R: Read> TaxIdentitySidecarV1StreamValidator<R> {
    pub fn new(mut reader: R, record_limit: u64) -> io::Result<Self> {
        let header = read_header(&mut reader)?;
        Ok(Self {
            reader,
            header,
            previous_group_id: None,
            records_validated: 0,
            record_limit,
            finished: false,
            poisoned: false,
        })
    }

    pub fn header(&self) -> &TaxIdentitySidecarV1Header {
        &self.header
    }

    pub fn records_validated(&self) -> u64 {
        self.records_validated
    }

    pub fn next_record(&mut self) -> io::Result<Option<TaxIdentitySidecarV1Record>> {
        if self.poisoned {
            return Err(invalid_data(VALIDATOR_POISONED));
        }
        let result = self.next_record_inner();
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    fn next_record_inner(&mut self) -> io::Result<Option<TaxIdentitySidecarV1Record>> {
        if self.finished {
            return Ok(None);
        }
        let Some(encoded) = read_record_or_eof(&mut self.reader)? else {
            self.finished = true;
            return Ok(None);
        };
        if self.records_validated >= self.record_limit {
            return Err(invalid_data(RECORD_LIMIT_EXCEEDED));
        }
        let record = TaxIdentitySidecarV1Record::decode(&encoded)?;
        if self
            .previous_group_id
            .is_some_and(|previous| previous >= record.provider_group_global_id)
        {
            return Err(invalid_data(INVALID_ORDER));
        }
        self.previous_group_id = Some(record.provider_group_global_id);
        // The strict pre-increment limit check proves this cannot overflow.
        self.records_validated += 1;
        Ok(Some(record))
    }

    pub fn validate_to_end(&mut self) -> io::Result<u64> {
        while self.next_record()?.is_some() {}
        Ok(self.records_validated)
    }
}

fn read_header<R: Read>(reader: &mut R) -> io::Result<TaxIdentitySidecarV1Header> {
    let mut fixed = [0u8; FIXED_HEADER_BYTES];
    read_exact_or_invalid(reader, &mut fixed, TRUNCATED_HEADER)?;
    let policy_id_length = usize::from(fixed[12]);
    let mut policy_id = [0u8; MAX_ENCODED_POLICY_ID_BYTES];
    read_exact_or_invalid(reader, &mut policy_id[..policy_id_length], TRUNCATED_HEADER)?;
    let mut encoded = Vec::with_capacity(FIXED_HEADER_BYTES + policy_id_length);
    encoded.extend_from_slice(&fixed);
    encoded.extend_from_slice(&policy_id[..policy_id_length]);
    TaxIdentitySidecarV1Header::decode(&encoded)
}

fn read_record_or_eof<R: Read>(
    reader: &mut R,
) -> io::Result<Option<[u8; TAX_IDENTITY_SIDECAR_V1_RECORD_BYTES]>> {
    let mut encoded = [0u8; TAX_IDENTITY_SIDECAR_V1_RECORD_BYTES];
    let mut bytes_read = 0usize;
    while bytes_read < encoded.len() {
        match reader.read(&mut encoded[bytes_read..]) {
            Ok(0) if bytes_read == 0 => return Ok(None),
            Ok(0) => return Err(invalid_data(TRUNCATED_RECORD)),
            Ok(count) => bytes_read += count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(Some(encoded))
}

fn read_exact_or_invalid<R: Read>(
    reader: &mut R,
    output: &mut [u8],
    message: &'static str,
) -> io::Result<()> {
    reader.read_exact(output).map_err(|error| {
        if error.kind() == io::ErrorKind::UnexpectedEof {
            invalid_data(message)
        } else {
            error
        }
    })
}

fn decode_state(value: u8) -> io::Result<TaxIdentityState> {
    match value {
        1 => Ok(TaxIdentityState::MatchedEin),
        2 => Ok(TaxIdentityState::Missing),
        3 => Ok(TaxIdentityState::Malformed),
        4 => Ok(TaxIdentityState::UnsupportedType),
        _ => Err(invalid_data(INVALID_RECORD)),
    }
}

fn validate_token_shape(
    state: TaxIdentityState,
    tin_id_128: &TinId128,
    tin_hmac_sha256: &TinHmacSha256,
    error_kind: io::ErrorKind,
) -> io::Result<()> {
    let valid = match state {
        TaxIdentityState::MatchedEin => {
            tin_hmac_sha256 != &ZERO_TIN_HMAC_SHA256
                && tin_id_128.as_slice() == &tin_hmac_sha256[..16]
        }
        TaxIdentityState::Missing
        | TaxIdentityState::Malformed
        | TaxIdentityState::UnsupportedType => {
            tin_id_128 == &ZERO_TIN_ID_128 && tin_hmac_sha256 == &ZERO_TIN_HMAC_SHA256
        }
    };
    if valid {
        Ok(())
    } else {
        Err(io::Error::new(error_kind, INVALID_RECORD))
    }
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests;
