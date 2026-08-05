use super::{
    invalid_data, FULL_HMAC_CROSS_TYPE_COLLISION, INVALID_AUDIT_RECORD, LOCATOR_PREFIX_COLLISION,
    NONCANONICAL_AUDIT_ORDER, TAX_IDENTITY_COLLISION_AUDIT_RECORD_BYTES,
};
use crate::tax_identity::TaxIdentityStateV2;
use crate::tax_identity_sidecar_v2::TaxIdentitySidecarV2Record;
use sha2::{Digest, Sha256};
use std::fmt;
use std::io;

#[cfg(test)]
mod record_guard_tests;

const OCCURRENCE_MULTISET_DOMAIN: &[u8] = b"PTG2TAXCOLLISIONOCCURRENCES\x01";
const EIN_TYPE_TAG: u8 = TaxIdentityStateV2::MatchedEin as u8;
const NPI_TYPE_TAG: u8 = TaxIdentityStateV2::MatchedNpi as u8;

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct CollisionAuditRecord {
    full_hmac: [u8; 32],
    type_tag: u8,
}

impl fmt::Debug for CollisionAuditRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CollisionAuditRecord")
            .field("full_hmac", &"<redacted>")
            .field("type_tag", &self.type_tag)
            .finish()
    }
}

impl CollisionAuditRecord {
    pub(super) fn from_sidecar(record: &TaxIdentitySidecarV2Record) -> Option<Self> {
        let type_tag = match record.state() {
            TaxIdentityStateV2::MatchedEin => EIN_TYPE_TAG,
            TaxIdentityStateV2::MatchedNpi => NPI_TYPE_TAG,
            TaxIdentityStateV2::Missing
            | TaxIdentityStateV2::Malformed
            | TaxIdentityStateV2::UnsupportedType => return None,
        };
        Some(Self {
            full_hmac: *record.tin_hmac_sha256(),
            type_tag,
        })
    }

    pub(super) fn encode(self) -> [u8; TAX_IDENTITY_COLLISION_AUDIT_RECORD_BYTES] {
        let mut encoded = [0u8; TAX_IDENTITY_COLLISION_AUDIT_RECORD_BYTES];
        encoded[..32].copy_from_slice(&self.full_hmac);
        encoded[32] = self.type_tag;
        encoded
    }

    pub(super) fn decode(
        encoded: [u8; TAX_IDENTITY_COLLISION_AUDIT_RECORD_BYTES],
    ) -> io::Result<Self> {
        let mut full_hmac = [0u8; 32];
        full_hmac.copy_from_slice(&encoded[..32]);
        let type_tag = encoded[32];
        if full_hmac == [0; 32] || !matches!(type_tag, EIN_TYPE_TAG | NPI_TYPE_TAG) {
            return Err(invalid_data(INVALID_AUDIT_RECORD));
        }
        Ok(Self {
            full_hmac,
            type_tag,
        })
    }

    fn locator(&self) -> &[u8] {
        &self.full_hmac[..16]
    }
}

pub(super) struct CollisionAuditSummary {
    pub(super) matched_row_count: u64,
    pub(super) matched_ein_count: u64,
    pub(super) matched_npi_count: u64,
    pub(super) unique_identity_count: u64,
    pub(super) repeated_identity_count: u64,
    pub(super) repeated_occurrence_count: u64,
    pub(super) occurrence_multiset_sha256: [u8; 32],
}

pub(super) struct CollisionAuditAccumulator {
    expected_rows: u64,
    previous: Option<CollisionAuditRecord>,
    current_identity_occurrences: u64,
    matched_row_count: u64,
    matched_ein_count: u64,
    matched_npi_count: u64,
    unique_identity_count: u64,
    repeated_identity_count: u64,
    repeated_occurrence_count: u64,
    locator_collision_detected: bool,
    occurrence_digest: Sha256,
}

impl CollisionAuditAccumulator {
    pub(super) fn new(expected_rows: u64) -> Self {
        let mut occurrence_digest = Sha256::new();
        occurrence_digest.update(OCCURRENCE_MULTISET_DOMAIN);
        occurrence_digest.update(expected_rows.to_be_bytes());
        Self {
            expected_rows,
            previous: None,
            current_identity_occurrences: 0,
            matched_row_count: 0,
            matched_ein_count: 0,
            matched_npi_count: 0,
            unique_identity_count: 0,
            repeated_identity_count: 0,
            repeated_occurrence_count: 0,
            locator_collision_detected: false,
            occurrence_digest,
        }
    }

    pub(super) fn observe(&mut self, record: CollisionAuditRecord) -> io::Result<()> {
        match record.type_tag {
            EIN_TYPE_TAG | NPI_TYPE_TAG => {}
            _ => return Err(invalid_data(INVALID_AUDIT_RECORD)),
        }
        if let Some(previous) = self.previous {
            if record.full_hmac == previous.full_hmac && record.type_tag != previous.type_tag {
                return Err(invalid_data(FULL_HMAC_CROSS_TYPE_COLLISION));
            }
            if record < previous {
                return Err(invalid_data(NONCANONICAL_AUDIT_ORDER));
            }
            if record.full_hmac != previous.full_hmac && record.locator() == previous.locator() {
                self.locator_collision_detected = true;
            }
        }

        self.matched_row_count = checked_increment(self.matched_row_count)?;
        match record.type_tag {
            EIN_TYPE_TAG => self.matched_ein_count = checked_increment(self.matched_ein_count)?,
            NPI_TYPE_TAG => self.matched_npi_count = checked_increment(self.matched_npi_count)?,
            _ => unreachable!("the type tag was validated before state mutation"),
        }
        self.occurrence_digest.update(record.encode());

        let Some(previous) = self.previous else {
            self.previous = Some(record);
            self.current_identity_occurrences = 1;
            self.unique_identity_count = 1;
            return Ok(());
        };
        if record == previous {
            self.current_identity_occurrences =
                checked_increment(self.current_identity_occurrences)?;
            self.repeated_occurrence_count = checked_increment(self.repeated_occurrence_count)?;
            return Ok(());
        }
        self.finish_identity()?;
        self.previous = Some(record);
        self.current_identity_occurrences = 1;
        self.unique_identity_count = checked_increment(self.unique_identity_count)?;
        Ok(())
    }

    pub(super) fn finish(mut self) -> io::Result<CollisionAuditSummary> {
        self.finish_identity()?;
        if self.matched_row_count != self.expected_rows
            || self
                .unique_identity_count
                .checked_add(self.repeated_occurrence_count)
                != Some(self.matched_row_count)
        {
            return Err(invalid_data(INVALID_AUDIT_RECORD));
        }
        if self.locator_collision_detected {
            return Err(invalid_data(LOCATOR_PREFIX_COLLISION));
        }
        Ok(CollisionAuditSummary {
            matched_row_count: self.matched_row_count,
            matched_ein_count: self.matched_ein_count,
            matched_npi_count: self.matched_npi_count,
            unique_identity_count: self.unique_identity_count,
            repeated_identity_count: self.repeated_identity_count,
            repeated_occurrence_count: self.repeated_occurrence_count,
            occurrence_multiset_sha256: self.occurrence_digest.finalize().into(),
        })
    }

    fn finish_identity(&mut self) -> io::Result<()> {
        if self.current_identity_occurrences > 1 {
            self.repeated_identity_count = checked_increment(self.repeated_identity_count)?;
        }
        Ok(())
    }
}

fn checked_increment(value: u64) -> io::Result<u64> {
    value
        .checked_add(1)
        .ok_or_else(|| invalid_data(INVALID_AUDIT_RECORD))
}
