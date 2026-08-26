//! Bounded validation for one v1/v2 tax-identity sidecar pair.
//!
//! The validator proves stream-local migration parity only: both streams use
//! the same token policy, contain the same strictly ordered provider groups,
//! and follow the frozen v1-to-v2 state transition contract. It does not
//! authenticate artifact digests or manifest metadata, prove the authoritative
//! provider-group universe, detect cross-row full-HMAC/type collisions, or
//! activate v2 data for publication. Those remain fail-closed bundle gates.

use crate::tax_identity::{TaxIdentityState, TaxIdentityStateV2};
use crate::tax_identity_sidecar_v1::{
    TaxIdentitySidecarV1Record, TaxIdentitySidecarV1StreamValidator,
};
use crate::tax_identity_sidecar_v2::{
    TaxIdentitySidecarV2Record, TaxIdentitySidecarV2StreamValidator,
};
use std::fmt;
use std::io::{self, Read};

const POLICY_MISMATCH: &str = "PTG tax identity sidecar policies do not match";
const GROUP_MISMATCH: &str = "PTG tax identity sidecar group ids do not match";
const ROW_COUNT_MISMATCH: &str = "PTG tax identity sidecar row counts do not match";
const INVALID_TRANSITION: &str = "PTG tax identity sidecar state transition is invalid";
const COUNT_OVERFLOW: &str = "PTG tax identity sidecar pair count overflow";
const STATE_TOTAL_MISMATCH: &str = "PTG tax identity sidecar pair state total is invalid";
const VALIDATOR_POISONED: &str = "PTG tax identity sidecar pair validator is poisoned";

/// One row returned only after v1/v2 ordinal and transition parity succeeds.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct TaxIdentitySidecarPairRecord {
    v1: TaxIdentitySidecarV1Record,
    v2: TaxIdentitySidecarV2Record,
}

impl fmt::Debug for TaxIdentitySidecarPairRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaxIdentitySidecarPairRecord")
            .field("v1", &self.v1)
            .field("v2", &self.v2)
            .finish()
    }
}

impl TaxIdentitySidecarPairRecord {
    pub fn v1(&self) -> &TaxIdentitySidecarV1Record {
        &self.v1
    }

    pub fn v2(&self) -> &TaxIdentitySidecarV2Record {
        &self.v2
    }
}

/// Checked five-state v2 summary for one completely validated pair.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TaxIdentitySidecarPairSummary {
    row_count: u64,
    matched_ein_count: u64,
    matched_npi_count: u64,
    missing_count: u64,
    malformed_count: u64,
    unsupported_type_count: u64,
}

impl TaxIdentitySidecarPairSummary {
    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    pub fn matched_ein_count(&self) -> u64 {
        self.matched_ein_count
    }

    pub fn matched_npi_count(&self) -> u64 {
        self.matched_npi_count
    }

    pub fn missing_count(&self) -> u64 {
        self.missing_count
    }

    pub fn malformed_count(&self) -> u64 {
        self.malformed_count
    }

    pub fn unsupported_type_count(&self) -> u64 {
        self.unsupported_type_count
    }

    fn observe(&mut self, state: TaxIdentityStateV2) -> io::Result<()> {
        let next_row_count = checked_increment(self.row_count)?;
        let state_count = match state {
            TaxIdentityStateV2::MatchedEin => &mut self.matched_ein_count,
            TaxIdentityStateV2::MatchedNpi => &mut self.matched_npi_count,
            TaxIdentityStateV2::Missing => &mut self.missing_count,
            TaxIdentityStateV2::Malformed => &mut self.malformed_count,
            TaxIdentityStateV2::UnsupportedType => &mut self.unsupported_type_count,
        };
        let next_state_count = checked_increment(*state_count)?;
        self.row_count = next_row_count;
        *state_count = next_state_count;
        Ok(())
    }

    fn validate_total(self) -> io::Result<()> {
        let total = [
            self.matched_ein_count,
            self.matched_npi_count,
            self.missing_count,
            self.malformed_count,
            self.unsupported_type_count,
        ]
        .into_iter()
        .try_fold(0u64, |total, count| {
            total
                .checked_add(count)
                .ok_or_else(|| invalid_data(COUNT_OVERFLOW))
        })?;
        if total != self.row_count {
            return Err(invalid_data(STATE_TOTAL_MISMATCH));
        }
        Ok(())
    }
}

/// Fixed-memory lockstep validator for a v1 stream and its additive v2 stream.
pub struct TaxIdentitySidecarPairValidator<V1, V2> {
    v1: TaxIdentitySidecarV1StreamValidator<V1>,
    v2: TaxIdentitySidecarV2StreamValidator<V2>,
    summary: TaxIdentitySidecarPairSummary,
    finished: bool,
    poisoned: bool,
}

impl<V1: Read, V2: Read> TaxIdentitySidecarPairValidator<V1, V2> {
    pub fn new(v1: V1, v2: V2, record_limit: u64) -> io::Result<Self> {
        let v1 = TaxIdentitySidecarV1StreamValidator::new(v1, record_limit)?;
        let v2 = TaxIdentitySidecarV2StreamValidator::new(v2, record_limit)?;
        if v1.header().policy_id() != v2.header().policy_id() {
            return Err(invalid_data(POLICY_MISMATCH));
        }
        Ok(Self {
            v1,
            v2,
            summary: TaxIdentitySidecarPairSummary::default(),
            finished: false,
            poisoned: false,
        })
    }

    pub fn policy_id(&self) -> &str {
        self.v1.header().policy_id()
    }

    pub fn records_validated(&self) -> u64 {
        self.summary.row_count
    }

    pub fn validated_summary(&self) -> Option<TaxIdentitySidecarPairSummary> {
        self.finished.then_some(self.summary)
    }

    pub fn next_record(&mut self) -> io::Result<Option<TaxIdentitySidecarPairRecord>> {
        if self.poisoned {
            return Err(invalid_data(VALIDATOR_POISONED));
        }
        let result = self.next_record_inner();
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    fn next_record_inner(&mut self) -> io::Result<Option<TaxIdentitySidecarPairRecord>> {
        if self.finished {
            return Ok(None);
        }
        let v1 = self.v1.next_record()?;
        let v2 = self.v2.next_record()?;
        let (v1, v2) = match (v1, v2) {
            (None, None) => {
                self.finish()?;
                return Ok(None);
            }
            (Some(v1), Some(v2)) => (v1, v2),
            _ => return Err(invalid_data(ROW_COUNT_MISMATCH)),
        };
        if v1.provider_group_global_id() != v2.provider_group_global_id() {
            return Err(invalid_data(GROUP_MISMATCH));
        }
        validate_transition(&v1, &v2)?;
        self.summary.observe(v2.state())?;
        Ok(Some(TaxIdentitySidecarPairRecord { v1, v2 }))
    }

    pub fn validate_to_end(&mut self) -> io::Result<TaxIdentitySidecarPairSummary> {
        while self.next_record()?.is_some() {}
        self.validated_summary()
            .ok_or(invalid_data(STATE_TOTAL_MISMATCH))
    }

    fn finish(&mut self) -> io::Result<()> {
        if self.v1.records_validated() != self.v2.records_validated()
            || self.v1.records_validated() != self.summary.row_count
        {
            return Err(invalid_data(ROW_COUNT_MISMATCH));
        }
        self.summary.validate_total()?;
        self.finished = true;
        Ok(())
    }
}

fn validate_transition(
    v1: &TaxIdentitySidecarV1Record,
    v2: &TaxIdentitySidecarV2Record,
) -> io::Result<()> {
    let valid = match (v1.state(), v2.state()) {
        (TaxIdentityState::MatchedEin, TaxIdentityStateV2::MatchedEin) => {
            v1.tin_id_128() == v2.tin_id_128() && v1.tin_hmac_sha256() == v2.tin_hmac_sha256()
        }
        (TaxIdentityState::Missing, TaxIdentityStateV2::Missing)
        | (TaxIdentityState::Malformed, TaxIdentityStateV2::Malformed)
        | (
            TaxIdentityState::UnsupportedType,
            TaxIdentityStateV2::MatchedNpi
            | TaxIdentityStateV2::Malformed
            | TaxIdentityStateV2::UnsupportedType,
        ) => true,
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(invalid_data(INVALID_TRANSITION))
    }
}

fn checked_increment(value: u64) -> io::Result<u64> {
    value
        .checked_add(1)
        .ok_or_else(|| invalid_data(COUNT_OVERFLOW))
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests;
