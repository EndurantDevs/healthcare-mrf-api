use super::contracts::TaxIdentitySidecarAuditedBundleCheckpoint;
use super::{
    invalid_data, TAX_IDENTITY_COLLISION_AUDIT_CHECKPOINT_CONTRACT,
    TAX_IDENTITY_COLLISION_AUDIT_OCCURRENCE_DIGEST_CONTRACT,
    TAX_IDENTITY_COLLISION_AUDIT_RECORD_CONTRACT, TAX_IDENTITY_COLLISION_CHECK_PASSED,
    TAX_IDENTITY_FULL_HMAC_TYPE_COLLISION_POLICY, TAX_IDENTITY_LOCATOR_COLLISION_POLICY,
    TAX_IDENTITY_MULTI_CANDIDATE_LOCATOR_SUPPORT, TAX_IDENTITY_SAME_IDENTITY_REPETITION_POLICY,
};
use crate::tax_identity_sidecar_bundle::digests::{decode_sha256, encode_hex};
use sha2::{Digest, Sha256};
use std::io;

const AUDIT_DIGEST_DOMAIN: &[u8] = b"PTG2TAXCOLLISIONAUDIT\x01";
const INVALID_CHECKPOINT: &str = "PTG tax identity collision audit checkpoint is invalid";

pub(super) struct AuditDigestInput<'a> {
    pub(super) source_bundle_sha256: &'a str,
    pub(super) projection_authority: &'a str,
    pub(super) matched_row_count: u64,
    pub(super) matched_ein_count: u64,
    pub(super) matched_npi_count: u64,
    pub(super) unique_identity_count: u64,
    pub(super) repeated_identity_count: u64,
    pub(super) repeated_occurrence_count: u64,
    pub(super) occurrence_multiset_sha256: [u8; 32],
}

pub(super) fn build_checkpoint(
    input: AuditDigestInput<'_>,
) -> io::Result<TaxIdentitySidecarAuditedBundleCheckpoint> {
    let mut hasher = Sha256::new();
    hasher.update(AUDIT_DIGEST_DOMAIN);
    update_text(
        &mut hasher,
        TAX_IDENTITY_COLLISION_AUDIT_CHECKPOINT_CONTRACT,
    )?;
    hasher.update([0]);
    update_text(&mut hasher, input.projection_authority)?;
    hasher.update(decode_sha256(input.source_bundle_sha256)?);
    update_text(&mut hasher, TAX_IDENTITY_COLLISION_AUDIT_RECORD_CONTRACT)?;
    update_text(
        &mut hasher,
        TAX_IDENTITY_COLLISION_AUDIT_OCCURRENCE_DIGEST_CONTRACT,
    )?;
    update_text(&mut hasher, TAX_IDENTITY_LOCATOR_COLLISION_POLICY)?;
    update_text(&mut hasher, TAX_IDENTITY_FULL_HMAC_TYPE_COLLISION_POLICY)?;
    update_text(&mut hasher, TAX_IDENTITY_SAME_IDENTITY_REPETITION_POLICY)?;
    update_text(&mut hasher, TAX_IDENTITY_MULTI_CANDIDATE_LOCATOR_SUPPORT)?;
    update_text(&mut hasher, TAX_IDENTITY_COLLISION_CHECK_PASSED)?;
    update_text(&mut hasher, TAX_IDENTITY_COLLISION_CHECK_PASSED)?;
    for count in [
        input.matched_row_count,
        input.matched_ein_count,
        input.matched_npi_count,
        input.unique_identity_count,
        input.repeated_identity_count,
        input.repeated_occurrence_count,
    ] {
        hasher.update(count.to_be_bytes());
    }
    hasher.update(input.occurrence_multiset_sha256);
    let audit_sha256 = encode_hex(&hasher.finalize());
    Ok(TaxIdentitySidecarAuditedBundleCheckpoint {
        contract: TAX_IDENTITY_COLLISION_AUDIT_CHECKPOINT_CONTRACT.to_owned(),
        publication_admissible: false,
        projection_authority: input.projection_authority.to_owned(),
        source_bundle_sha256: input.source_bundle_sha256.to_owned(),
        record_contract: TAX_IDENTITY_COLLISION_AUDIT_RECORD_CONTRACT.to_owned(),
        occurrence_digest_contract: TAX_IDENTITY_COLLISION_AUDIT_OCCURRENCE_DIGEST_CONTRACT
            .to_owned(),
        locator_collision_policy: TAX_IDENTITY_LOCATOR_COLLISION_POLICY.to_owned(),
        full_hmac_type_collision_policy: TAX_IDENTITY_FULL_HMAC_TYPE_COLLISION_POLICY.to_owned(),
        same_identity_repetition_policy: TAX_IDENTITY_SAME_IDENTITY_REPETITION_POLICY.to_owned(),
        multi_candidate_locator_support: TAX_IDENTITY_MULTI_CANDIDATE_LOCATOR_SUPPORT.to_owned(),
        locator_prefix_collision_check: TAX_IDENTITY_COLLISION_CHECK_PASSED.to_owned(),
        full_hmac_cross_type_collision_check: TAX_IDENTITY_COLLISION_CHECK_PASSED.to_owned(),
        matched_row_count: input.matched_row_count,
        matched_ein_count: input.matched_ein_count,
        matched_npi_count: input.matched_npi_count,
        unique_identity_count: input.unique_identity_count,
        repeated_identity_count: input.repeated_identity_count,
        repeated_occurrence_count: input.repeated_occurrence_count,
        occurrence_multiset_sha256: encode_hex(&input.occurrence_multiset_sha256),
        audit_sha256,
    })
}

fn update_text(hasher: &mut Sha256, value: &str) -> io::Result<()> {
    let length = match u32::try_from(value.len()) {
        Ok(length) => length,
        Err(_) => return Err(invalid_data(INVALID_CHECKPOINT)),
    };
    hasher.update(length.to_be_bytes());
    hasher.update(value.as_bytes());
    Ok(())
}
