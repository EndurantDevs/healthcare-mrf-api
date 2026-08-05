use serde::Serialize;
use std::fmt;
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TaxIdentityCollisionAuditLimits {
    pub max_artifacts: usize,
    pub max_source_rows: u64,
    pub max_matched_rows: u64,
    pub max_memory_bytes: u64,
    pub max_scratch_bytes: u64,
    pub minimum_free_scratch_bytes: u64,
    pub merge_fan_in: usize,
    pub max_open_files: usize,
}

#[derive(Clone, Eq, PartialEq)]
pub struct TaxIdentityCollisionAuditConfig {
    scratch_root: PathBuf,
    limits: TaxIdentityCollisionAuditLimits,
}

impl TaxIdentityCollisionAuditConfig {
    pub fn new(scratch_root: PathBuf, limits: TaxIdentityCollisionAuditLimits) -> Self {
        Self {
            scratch_root,
            limits,
        }
    }

    pub fn scratch_root(&self) -> &Path {
        &self.scratch_root
    }

    pub const fn limits(&self) -> TaxIdentityCollisionAuditLimits {
        self.limits
    }
}

impl fmt::Debug for TaxIdentityCollisionAuditConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaxIdentityCollisionAuditConfig")
            .field("scratch_root", &"<redacted>")
            .field("limits", &self.limits)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TaxIdentityCollisionAuditPhase {
    Admission,
    Authenticate,
    Scan,
    Spill,
    Merge,
    Verify,
    Complete,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TaxIdentityCollisionAuditProgress {
    pub phase: TaxIdentityCollisionAuditPhase,
    pub completed: u64,
    pub total: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TaxIdentityCollisionAuditStats {
    pub source_rows: u64,
    pub matched_rows: u64,
    pub initial_run_count: u64,
    pub merge_operation_count: u64,
    pub peak_scratch_bytes: u64,
    pub maximum_merge_fan_in: usize,
    pub cancellation_poll_count: u64,
}

#[derive(Clone, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TaxIdentitySidecarAuditedBundleCheckpoint {
    pub(super) contract: String,
    pub(super) publication_admissible: bool,
    pub(super) projection_authority: String,
    pub(super) source_bundle_sha256: String,
    pub(super) record_contract: String,
    pub(super) occurrence_digest_contract: String,
    pub(super) locator_collision_policy: String,
    pub(super) full_hmac_type_collision_policy: String,
    pub(super) same_identity_repetition_policy: String,
    pub(super) multi_candidate_locator_support: String,
    pub(super) locator_prefix_collision_check: String,
    pub(super) full_hmac_cross_type_collision_check: String,
    pub(super) matched_row_count: u64,
    pub(super) matched_ein_count: u64,
    pub(super) matched_npi_count: u64,
    pub(super) unique_identity_count: u64,
    pub(super) repeated_identity_count: u64,
    pub(super) repeated_occurrence_count: u64,
    pub(super) occurrence_multiset_sha256: String,
    pub(super) audit_sha256: String,
}

impl TaxIdentitySidecarAuditedBundleCheckpoint {
    pub fn contract(&self) -> &str {
        &self.contract
    }

    pub const fn publication_admissible(&self) -> bool {
        self.publication_admissible
    }

    pub fn projection_authority(&self) -> &str {
        &self.projection_authority
    }

    pub fn source_bundle_sha256(&self) -> &str {
        &self.source_bundle_sha256
    }

    pub fn record_contract(&self) -> &str {
        &self.record_contract
    }

    pub fn occurrence_digest_contract(&self) -> &str {
        &self.occurrence_digest_contract
    }

    pub fn locator_collision_policy(&self) -> &str {
        &self.locator_collision_policy
    }

    pub fn full_hmac_type_collision_policy(&self) -> &str {
        &self.full_hmac_type_collision_policy
    }

    pub fn same_identity_repetition_policy(&self) -> &str {
        &self.same_identity_repetition_policy
    }

    pub fn multi_candidate_locator_support(&self) -> &str {
        &self.multi_candidate_locator_support
    }

    pub fn locator_prefix_collision_check(&self) -> &str {
        &self.locator_prefix_collision_check
    }

    pub fn full_hmac_cross_type_collision_check(&self) -> &str {
        &self.full_hmac_cross_type_collision_check
    }

    pub const fn matched_row_count(&self) -> u64 {
        self.matched_row_count
    }

    pub const fn matched_ein_count(&self) -> u64 {
        self.matched_ein_count
    }

    pub const fn matched_npi_count(&self) -> u64 {
        self.matched_npi_count
    }

    pub const fn unique_identity_count(&self) -> u64 {
        self.unique_identity_count
    }

    pub const fn repeated_occurrence_count(&self) -> u64 {
        self.repeated_occurrence_count
    }

    pub const fn repeated_identity_count(&self) -> u64 {
        self.repeated_identity_count
    }

    pub fn occurrence_multiset_sha256(&self) -> &str {
        &self.occurrence_multiset_sha256
    }

    pub fn audit_sha256(&self) -> &str {
        &self.audit_sha256
    }
}

impl fmt::Debug for TaxIdentitySidecarAuditedBundleCheckpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaxIdentitySidecarAuditedBundleCheckpoint")
            .field("publication_admissible", &self.publication_admissible)
            .field("projection_authority", &self.projection_authority)
            .field(
                "locator_prefix_collision_check",
                &self.locator_prefix_collision_check,
            )
            .field(
                "full_hmac_cross_type_collision_check",
                &self.full_hmac_cross_type_collision_check,
            )
            .field("matched_row_count", &self.matched_row_count)
            .field("unique_identity_count", &self.unique_identity_count)
            .field("source_bundle_sha256", &"<redacted>")
            .field("occurrence_multiset_sha256", &"<redacted>")
            .field("audit_sha256", &"<redacted>")
            .finish_non_exhaustive()
    }
}

pub struct TaxIdentityCollisionAuditResult {
    pub(super) checkpoint: TaxIdentitySidecarAuditedBundleCheckpoint,
    pub(super) stats: TaxIdentityCollisionAuditStats,
}

impl TaxIdentityCollisionAuditResult {
    pub fn checkpoint(&self) -> &TaxIdentitySidecarAuditedBundleCheckpoint {
        &self.checkpoint
    }

    pub const fn stats(&self) -> TaxIdentityCollisionAuditStats {
        self.stats
    }
}

impl fmt::Debug for TaxIdentityCollisionAuditResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaxIdentityCollisionAuditResult")
            .field("checkpoint", &self.checkpoint)
            .field("stats", &self.stats)
            .finish()
    }
}
