//! Bounded, validation-only cross-row collision audit for v2 tax identities.
//!
//! The audit is deliberately separate from provider-graph compilation. It
//! produces only a nonpublishable checkpoint bound to an already validated
//! sidecar bundle; v1 remains the sole projection authority.

mod artifacts;
mod contracts;
mod digests;
mod records;
mod scratch;
mod sort;

use artifacts::{
    scan_artifacts, validate_and_order_artifacts, validate_artifact_count, validate_base_checkpoint,
};
use digests::{build_checkpoint, AuditDigestInput};
use sort::{CollisionSortPlan, CollisionSorter};
use std::io;

use super::{
    TaxIdentitySidecarBundleCheckpoint, TaxIdentitySidecarV2ArtifactDescriptor,
    TAX_IDENTITY_SIDECAR_PROJECTION_AUTHORITY,
};

pub use contracts::{
    TaxIdentityCollisionAuditConfig, TaxIdentityCollisionAuditLimits,
    TaxIdentityCollisionAuditPhase, TaxIdentityCollisionAuditProgress,
    TaxIdentityCollisionAuditResult, TaxIdentityCollisionAuditStats,
    TaxIdentitySidecarAuditedBundleCheckpoint,
};

pub const TAX_IDENTITY_COLLISION_AUDIT_CHECKPOINT_CONTRACT: &str =
    "ptg2_tax_identity_sidecar_collision_audit_v1";
pub const TAX_IDENTITY_COLLISION_AUDIT_RECORD_CONTRACT: &str =
    "full_hmac_sha256_then_v2_type_tag_33_bytes_v1";
pub const TAX_IDENTITY_COLLISION_AUDIT_OCCURRENCE_DIGEST_CONTRACT: &str =
    "sha256_ptg2_tax_collision_occurrences_v1_with_expected_count_prefix";
pub const TAX_IDENTITY_LOCATOR_COLLISION_POLICY: &str =
    "reject_different_full_hmac_same_128_bit_prefix_v1";
pub const TAX_IDENTITY_FULL_HMAC_TYPE_COLLISION_POLICY: &str =
    "reject_same_full_hmac_different_type_v1";
pub const TAX_IDENTITY_SAME_IDENTITY_REPETITION_POLICY: &str =
    "allow_same_full_hmac_same_type_across_groups_v1";
pub const TAX_IDENTITY_MULTI_CANDIDATE_LOCATOR_SUPPORT: &str = "deferred_phase1";
pub const TAX_IDENTITY_COLLISION_CHECK_PASSED: &str = "passed";
pub const TAX_IDENTITY_COLLISION_AUDIT_RECORD_BYTES: usize = 33;

const LOCATOR_PREFIX_COLLISION: &str = "PTG tax identity locator prefix collision detected";
const FULL_HMAC_CROSS_TYPE_COLLISION: &str =
    "PTG tax identity full HMAC cross-type collision detected";
const INVALID_AUDIT_RECORD: &str = "PTG tax identity collision audit record is invalid";
const NONCANONICAL_AUDIT_ORDER: &str =
    "PTG tax identity collision audit records are not canonically ordered";

fn invalid_data(message: &'static str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message)
}

/// Run the bounded, validation-only v2 collision audit without progress events.
pub fn audit_tax_identity_sidecar_bundle(
    source_checkpoint: &TaxIdentitySidecarBundleCheckpoint,
    v2_artifacts: &[TaxIdentitySidecarV2ArtifactDescriptor],
    config: &TaxIdentityCollisionAuditConfig,
) -> io::Result<TaxIdentityCollisionAuditResult> {
    audit_tax_identity_sidecar_bundle_with_progress(source_checkpoint, v2_artifacts, config, |_| {
        Ok(())
    })
}

/// Run the bounded audit and report cancellation-safe, phase-global progress.
///
/// Returning an error from `progress` cancels the audit before publication of
/// a checkpoint. Scratch artifacts are owned by an RAII cleanup boundary.
pub fn audit_tax_identity_sidecar_bundle_with_progress<
    P: FnMut(TaxIdentityCollisionAuditProgress) -> io::Result<()>,
>(
    source_checkpoint: &TaxIdentitySidecarBundleCheckpoint,
    v2_artifacts: &[TaxIdentitySidecarV2ArtifactDescriptor],
    config: &TaxIdentityCollisionAuditConfig,
    mut progress: P,
) -> io::Result<TaxIdentityCollisionAuditResult> {
    let mut cancellation_poll_count = 0u64;
    let mut tracked_progress = |phase, completed, total| {
        cancellation_poll_count = cancellation_poll_count
            .checked_add(1)
            .ok_or_else(|| invalid_data("PTG tax identity collision audit count overflow"))?;
        progress(TaxIdentityCollisionAuditProgress {
            phase,
            completed,
            total,
        })
    };
    tracked_progress(TaxIdentityCollisionAuditPhase::Admission, 0, 1)?;
    validate_artifact_count(source_checkpoint, v2_artifacts.len())?;
    CollisionSortPlan::preflight_artifact_count(v2_artifacts.len(), config.limits())?;
    validate_base_checkpoint(source_checkpoint)?;
    let expected_matched_rows = source_checkpoint
        .matched_ein_count
        .checked_add(source_checkpoint.matched_npi_count)
        .ok_or_else(|| invalid_data("PTG tax identity collision audit count overflow"))?;
    let sort_plan = CollisionSortPlan::admit(
        source_checkpoint.row_count,
        expected_matched_rows,
        v2_artifacts.len(),
        config.limits(),
    )?;
    let ordered_artifacts = validate_and_order_artifacts(
        source_checkpoint,
        v2_artifacts,
        sort_plan.artifact_capacity(),
    )?;
    tracked_progress(TaxIdentityCollisionAuditPhase::Admission, 1, 1)?;
    let mut sorter = CollisionSorter::create(config.scratch_root(), sort_plan)?;

    let scan = scan_artifacts(
        source_checkpoint,
        &ordered_artifacts,
        &mut sorter,
        &mut tracked_progress,
    )?;
    let sorted = sorter.finish(&mut tracked_progress)?;
    if scan.source_rows != source_checkpoint.row_count
        || scan.matched_rows != expected_matched_rows
        || scan.matched_ein_rows != sorted.summary.matched_ein_count
        || scan.matched_npi_rows != sorted.summary.matched_npi_count
        || sorted.summary.matched_row_count != expected_matched_rows
    {
        return Err(invalid_data(
            "PTG tax identity collision audit source and sort counts do not match",
        ));
    }
    let checkpoint = build_checkpoint(AuditDigestInput {
        source_bundle_sha256: &source_checkpoint.bundle_sha256,
        projection_authority: TAX_IDENTITY_SIDECAR_PROJECTION_AUTHORITY,
        matched_row_count: sorted.summary.matched_row_count,
        matched_ein_count: sorted.summary.matched_ein_count,
        matched_npi_count: sorted.summary.matched_npi_count,
        unique_identity_count: sorted.summary.unique_identity_count,
        repeated_identity_count: sorted.summary.repeated_identity_count,
        repeated_occurrence_count: sorted.summary.repeated_occurrence_count,
        occurrence_multiset_sha256: sorted.summary.occurrence_multiset_sha256,
    })?;
    tracked_progress(TaxIdentityCollisionAuditPhase::Complete, 1, 1)?;
    Ok(TaxIdentityCollisionAuditResult {
        checkpoint,
        stats: TaxIdentityCollisionAuditStats {
            source_rows: scan.source_rows,
            matched_rows: scan.matched_rows,
            initial_run_count: sorted.initial_run_count,
            merge_operation_count: sorted.merge_operation_count,
            peak_scratch_bytes: sorted.peak_scratch_bytes,
            maximum_merge_fan_in: sorted.maximum_merge_fan_in,
            cancellation_poll_count,
        },
    })
}

#[cfg(test)]
mod tests;
