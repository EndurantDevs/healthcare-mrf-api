use super::records::CollisionAuditRecord;
use super::sort::{AuditProgressCallback, CollisionSorter};
use super::{invalid_data, TaxIdentityCollisionAuditPhase};
use crate::tax_identity::TaxIdentityStateV2;
use crate::tax_identity_sidecar_bundle::digests::{
    bundle_digest, checked_add, decode_sha256, derived_v2_resource_identity, encode_hex,
};
use crate::tax_identity_sidecar_bundle::{
    validate_v2_metadata, TaxIdentitySidecarBundleCheckpoint, TaxIdentitySidecarShardCheckpoint,
    TaxIdentitySidecarV2ArtifactDescriptor, TAX_IDENTITY_SIDECAR_BUNDLE_CHECKPOINT_CONTRACT,
    TAX_IDENTITY_SIDECAR_COLLISION_AUDIT, TAX_IDENTITY_SIDECAR_PROJECTION_AUTHORITY,
};
use crate::tax_identity_sidecar_v2::TaxIdentitySidecarV2StreamValidator;
use std::collections::HashSet;
use std::io::{self, BufReader};

mod files;
use files::{
    file_identity, hash_open_file, open_source_artifact, physical_file_identity,
    reauthenticate_source_path,
};

pub(super) const HASH_BUFFER_BYTES: usize = 64 * 1024;
const POLL_ROW_INTERVAL: u64 = 4096;
pub(super) const INVALID_BASE_CHECKPOINT: &str =
    "PTG tax identity collision audit source checkpoint is invalid";
pub(super) const ARTIFACT_SET_MISMATCH: &str =
    "PTG tax identity collision audit artifact set does not match checkpoint";
pub(super) const ARTIFACT_UNAVAILABLE: &str =
    "PTG tax identity collision audit source artifact is unavailable";
pub(super) const ARTIFACT_VERIFICATION_FAILED: &str =
    "PTG tax identity collision audit source artifact verification failed";
pub(super) const ARTIFACT_CONTENT_MISMATCH: &str =
    "PTG tax identity collision audit source artifact content does not match checkpoint";
const COUNT_OVERFLOW: &str = "PTG tax identity collision audit count overflow";

pub(super) struct ValidatedArtifact<'a> {
    descriptor: &'a TaxIdentitySidecarV2ArtifactDescriptor,
}

pub(super) struct ArtifactScanSummary {
    pub(super) source_rows: u64,
    pub(super) matched_rows: u64,
    pub(super) matched_ein_rows: u64,
    pub(super) matched_npi_rows: u64,
}

pub(super) fn validate_and_order_artifacts<'a>(
    checkpoint: &'a TaxIdentitySidecarBundleCheckpoint,
    descriptors: &'a [TaxIdentitySidecarV2ArtifactDescriptor],
    artifact_capacity: usize,
) -> io::Result<Vec<ValidatedArtifact<'a>>> {
    if descriptors.len() != checkpoint.shards.len() || descriptors.len() > artifact_capacity {
        return Err(invalid_data(ARTIFACT_SET_MISMATCH));
    }
    let mut ordered = Vec::new();
    if ordered.try_reserve_exact(descriptors.len()).is_err() {
        return Err(invalid_data(ARTIFACT_SET_MISMATCH));
    }
    ordered.extend(
        descriptors
            .iter()
            .map(|descriptor| ValidatedArtifact { descriptor }),
    );
    ordered.sort_unstable_by(|left, right| {
        left.descriptor
            .metadata
            .source_shard_id
            .cmp(&right.descriptor.metadata.source_shard_id)
    });
    let duplicate_source_id = ordered.windows(2).any(|pair| {
        pair[0].descriptor.metadata.source_shard_id == pair[1].descriptor.metadata.source_shard_id
    });
    let mut paths = HashSet::new();
    if paths.try_reserve(ordered.len()).is_err() {
        return Err(invalid_data(ARTIFACT_SET_MISMATCH));
    }
    let duplicate_path = ordered
        .iter()
        .any(|artifact| !paths.insert(artifact.descriptor.path.as_path()));
    if duplicate_source_id || duplicate_path {
        return Err(invalid_data(ARTIFACT_SET_MISMATCH));
    }

    for (shard, artifact) in checkpoint.shards.iter().zip(&ordered) {
        validate_descriptor_binding(checkpoint, shard, artifact.descriptor)?;
    }
    Ok(ordered)
}

pub(super) fn validate_artifact_count(
    checkpoint: &TaxIdentitySidecarBundleCheckpoint,
    descriptor_count: usize,
) -> io::Result<()> {
    if descriptor_count != checkpoint.shards.len() {
        return Err(invalid_data(ARTIFACT_SET_MISMATCH));
    }
    Ok(())
}

pub(super) fn scan_artifacts(
    checkpoint: &TaxIdentitySidecarBundleCheckpoint,
    artifacts: &[ValidatedArtifact<'_>],
    sorter: &mut CollisionSorter,
    progress: &mut AuditProgressCallback<'_>,
) -> io::Result<ArtifactScanSummary> {
    let verification_total = checkpoint
        .v2_byte_count
        .checked_mul(3)
        .ok_or(invalid_data(COUNT_OVERFLOW))?;
    let mut verification_completed = 0u64;
    let mut scan_completed = 0u64;
    let mut aggregate_counts = [0u64; 5];
    let mut source_identities = HashSet::new();
    if source_identities.try_reserve(artifacts.len()).is_err() {
        return Err(invalid_data(ARTIFACT_SET_MISMATCH));
    }

    for (shard, artifact) in checkpoint.shards.iter().zip(artifacts) {
        let expected_digest = match decode_sha256(&artifact.descriptor.metadata.sha256) {
            Ok(digest) => digest,
            Err(_) => return Err(invalid_data(ARTIFACT_CONTENT_MISMATCH)),
        };
        let mut file = open_source_artifact(&artifact.descriptor.path)?;
        let identity = file_identity(&file)?;
        let physical_identity = physical_file_identity(&file)?;
        if identity.byte_count != artifact.descriptor.metadata.byte_count
            || !source_identities.insert(physical_identity)
        {
            return Err(invalid_data(ARTIFACT_CONTENT_MISMATCH));
        }
        let first_digest = hash_open_file(
            &mut file,
            identity.byte_count,
            verification_completed,
            verification_total,
            progress,
        )?;
        verification_completed = checked_add(verification_completed, identity.byte_count)?;
        if first_digest != expected_digest || file_identity(&file)? != identity {
            return Err(invalid_data(ARTIFACT_VERIFICATION_FAILED));
        }

        let mut shard_counts = [0u64; 5];
        let shard_row_count = artifact.descriptor.metadata.row_count;
        {
            let validator = TaxIdentitySidecarV2StreamValidator::new(
                BufReader::with_capacity(HASH_BUFFER_BYTES, &mut file),
                shard_row_count,
            );
            let mut validator = match validator {
                Ok(validator) => validator,
                Err(_) => return Err(invalid_data(ARTIFACT_CONTENT_MISMATCH)),
            };
            if validator.header().policy_id() != checkpoint.token_policy_id {
                return Err(invalid_data(ARTIFACT_CONTENT_MISMATCH));
            }
            loop {
                let next_record = match validator.next_record() {
                    Ok(record) => record,
                    Err(_) => return Err(invalid_data(ARTIFACT_CONTENT_MISMATCH)),
                };
                let Some(record) = next_record else {
                    break;
                };
                let state_index = state_index(record.state());
                shard_counts[state_index] = checked_add(shard_counts[state_index], 1)?;
                if let Some(collision_record) = CollisionAuditRecord::from_sidecar(&record) {
                    sorter.push(collision_record, progress)?;
                }
                scan_completed = checked_add(scan_completed, 1)?;
                if scan_completed == checkpoint.row_count
                    || scan_completed.is_multiple_of(POLL_ROW_INTERVAL)
                    || validator.records_validated() == shard_row_count
                {
                    progress(
                        TaxIdentityCollisionAuditPhase::Scan,
                        scan_completed,
                        checkpoint.row_count,
                    )?;
                }
            }
            if validator.records_validated() != shard_row_count {
                return Err(invalid_data(ARTIFACT_CONTENT_MISMATCH));
            }
        }
        validate_shard_counts(shard, shard_counts)?;
        for (aggregate, observed) in aggregate_counts.iter_mut().zip(shard_counts) {
            *aggregate = checked_add(*aggregate, observed)?;
        }
        if file_identity(&file)? != identity {
            return Err(invalid_data(ARTIFACT_VERIFICATION_FAILED));
        }
        let second_digest = hash_open_file(
            &mut file,
            identity.byte_count,
            verification_completed,
            verification_total,
            progress,
        )?;
        verification_completed = checked_add(verification_completed, identity.byte_count)?;
        if second_digest != expected_digest || file_identity(&file)? != identity {
            return Err(invalid_data(ARTIFACT_VERIFICATION_FAILED));
        }
        reauthenticate_source_path(
            &artifact.descriptor.path,
            &identity,
            expected_digest,
            verification_completed,
            verification_total,
            progress,
        )?;
        verification_completed = checked_add(verification_completed, identity.byte_count)?;
    }

    if verification_completed != verification_total
        || scan_completed != checkpoint.row_count
        || aggregate_counts
            != [
                checkpoint.matched_ein_count,
                checkpoint.matched_npi_count,
                checkpoint.missing_count,
                checkpoint.malformed_count,
                checkpoint.unsupported_type_count,
            ]
    {
        return Err(invalid_data(ARTIFACT_CONTENT_MISMATCH));
    }
    Ok(ArtifactScanSummary {
        source_rows: scan_completed,
        matched_rows: checked_add(aggregate_counts[0], aggregate_counts[1])?,
        matched_ein_rows: aggregate_counts[0],
        matched_npi_rows: aggregate_counts[1],
    })
}

pub(super) fn validate_base_checkpoint(
    checkpoint: &TaxIdentitySidecarBundleCheckpoint,
) -> io::Result<()> {
    let shard_count = match u64::try_from(checkpoint.shards.len()) {
        Ok(shard_count) => shard_count,
        Err(_) => return Err(invalid_data(COUNT_OVERFLOW)),
    };
    if checkpoint.contract != TAX_IDENTITY_SIDECAR_BUNDLE_CHECKPOINT_CONTRACT
        || checkpoint.publication_admissible
        || checkpoint.projection_authority != TAX_IDENTITY_SIDECAR_PROJECTION_AUTHORITY
        || checkpoint.cross_row_full_hmac_type_collision_check
            != TAX_IDENTITY_SIDECAR_COLLISION_AUDIT
        || checkpoint.shards.is_empty()
        || checkpoint.shard_count != shard_count
        || checkpoint.row_count == 0
        || checkpoint.row_count != checkpoint.authoritative_provider_group_count
        || checkpoint.bundle_sha256
            != encode_hex(&bundle_digest(
                &checkpoint.token_policy_id,
                shard_count,
                &checkpoint.shards,
            )?)
    {
        return Err(invalid_data(INVALID_BASE_CHECKPOINT));
    }
    let mut totals = [0u64; 8];
    let mut previous_shard_id: Option<&str> = None;
    for shard in &checkpoint.shards {
        if shard.token_policy_id != checkpoint.token_policy_id
            || previous_shard_id.is_some_and(|previous| previous >= shard.shard_id.as_str())
        {
            return Err(invalid_data(INVALID_BASE_CHECKPOINT));
        }
        previous_shard_id = Some(&shard.shard_id);
        for (total, value) in totals.iter_mut().zip([
            shard.row_count,
            shard.matched_ein_count,
            shard.matched_npi_count,
            shard.missing_count,
            shard.malformed_count,
            shard.unsupported_type_count,
            shard.v1_byte_count,
            shard.v2_byte_count,
        ]) {
            *total = checked_add(*total, value)?;
        }
    }
    if totals
        != [
            checkpoint.row_count,
            checkpoint.matched_ein_count,
            checkpoint.matched_npi_count,
            checkpoint.missing_count,
            checkpoint.malformed_count,
            checkpoint.unsupported_type_count,
            checkpoint.v1_byte_count,
            checkpoint.v2_byte_count,
        ]
    {
        return Err(invalid_data(INVALID_BASE_CHECKPOINT));
    }
    Ok(())
}

fn validate_descriptor_binding(
    bundle: &TaxIdentitySidecarBundleCheckpoint,
    shard: &TaxIdentitySidecarShardCheckpoint,
    descriptor: &TaxIdentitySidecarV2ArtifactDescriptor,
) -> io::Result<()> {
    if validate_v2_metadata(&shard.shard_id, descriptor).is_err() {
        return Err(invalid_data(ARTIFACT_SET_MISMATCH));
    }
    let metadata = &descriptor.metadata;
    let digest = match decode_sha256(&metadata.sha256) {
        Ok(digest) => digest,
        Err(_) => return Err(invalid_data(ARTIFACT_SET_MISMATCH)),
    };
    let resource_identity = match derived_v2_resource_identity(&shard.shard_id, metadata, &digest) {
        Ok(identity) => identity,
        Err(_) => return Err(invalid_data(ARTIFACT_SET_MISMATCH)),
    };
    if metadata.source_shard_id != shard.shard_id
        || metadata.token_policy_id != bundle.token_policy_id
        || metadata.row_count != shard.row_count
        || metadata.provider_group_count != shard.authoritative_provider_group_count
        || metadata.matched_ein_count != shard.matched_ein_count
        || metadata.matched_npi_count != shard.matched_npi_count
        || metadata.missing_count != shard.missing_count
        || metadata.malformed_count != shard.malformed_count
        || metadata.unsupported_type_count != shard.unsupported_type_count
        || metadata.byte_count != shard.v2_byte_count
        || metadata.sha256 != shard.v2_sha256
        || resource_identity != shard.v2_resource_identity
    {
        return Err(invalid_data(ARTIFACT_SET_MISMATCH));
    }
    Ok(())
}

fn validate_shard_counts(
    checkpoint: &TaxIdentitySidecarShardCheckpoint,
    counts: [u64; 5],
) -> io::Result<()> {
    let expected = [
        checkpoint.matched_ein_count,
        checkpoint.matched_npi_count,
        checkpoint.missing_count,
        checkpoint.malformed_count,
        checkpoint.unsupported_type_count,
    ];
    if counts != expected {
        return Err(invalid_data(ARTIFACT_CONTENT_MISMATCH));
    }
    Ok(())
}

fn state_index(state: TaxIdentityStateV2) -> usize {
    match state {
        TaxIdentityStateV2::MatchedEin => 0,
        TaxIdentityStateV2::MatchedNpi => 1,
        TaxIdentityStateV2::Missing => 2,
        TaxIdentityStateV2::Malformed => 3,
        TaxIdentityStateV2::UnsupportedType => 4,
    }
}
