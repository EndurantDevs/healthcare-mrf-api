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
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom};

const HASH_BUFFER_BYTES: usize = 64 * 1024;
const POLL_ROW_INTERVAL: u64 = 4096;
pub(super) const INVALID_BASE_CHECKPOINT: &str =
    "PTG tax identity collision audit source checkpoint is invalid";
pub(super) const ARTIFACT_SET_MISMATCH: &str =
    "PTG tax identity collision audit artifact set does not match checkpoint";
pub(super) const ARTIFACT_UNAVAILABLE: &str =
    "PTG tax identity collision audit source artifact is unavailable";
pub(super) const ARTIFACT_AUTHENTICATION_FAILED: &str =
    "PTG tax identity collision audit source artifact authentication failed";
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
    ordered
        .try_reserve_exact(descriptors.len())
        .map_err(|_| invalid_data(ARTIFACT_SET_MISMATCH))?;
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
    paths
        .try_reserve(ordered.len())
        .map_err(|_| invalid_data(ARTIFACT_SET_MISMATCH))?;
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
    let authentication_total = checkpoint
        .v2_byte_count
        .checked_mul(2)
        .ok_or_else(|| invalid_data(COUNT_OVERFLOW))?;
    let mut authentication_completed = 0u64;
    let mut scan_completed = 0u64;
    let mut aggregate_counts = [0u64; 5];
    let mut source_identities = HashSet::new();
    source_identities
        .try_reserve(artifacts.len())
        .map_err(|_| invalid_data(ARTIFACT_SET_MISMATCH))?;

    for (shard, artifact) in checkpoint.shards.iter().zip(artifacts) {
        let expected_digest = decode_sha256(&artifact.descriptor.metadata.sha256)
            .map_err(|_| invalid_data(ARTIFACT_CONTENT_MISMATCH))?;
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
            authentication_completed,
            authentication_total,
            progress,
        )?;
        authentication_completed = checked_add(authentication_completed, identity.byte_count)?;
        if first_digest != expected_digest || file_identity(&file)? != identity {
            return Err(invalid_data(ARTIFACT_AUTHENTICATION_FAILED));
        }

        let mut shard_counts = [0u64; 5];
        let shard_row_count = artifact.descriptor.metadata.row_count;
        {
            let mut validator = TaxIdentitySidecarV2StreamValidator::new(
                BufReader::with_capacity(HASH_BUFFER_BYTES, &mut file),
                shard_row_count,
            )
            .map_err(|_| invalid_data(ARTIFACT_CONTENT_MISMATCH))?;
            if validator.header().policy_id() != checkpoint.token_policy_id {
                return Err(invalid_data(ARTIFACT_CONTENT_MISMATCH));
            }
            while let Some(record) = validator
                .next_record()
                .map_err(|_| invalid_data(ARTIFACT_CONTENT_MISMATCH))?
            {
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
            return Err(invalid_data(ARTIFACT_AUTHENTICATION_FAILED));
        }
        let second_digest = hash_open_file(
            &mut file,
            identity.byte_count,
            authentication_completed,
            authentication_total,
            progress,
        )?;
        authentication_completed = checked_add(authentication_completed, identity.byte_count)?;
        if second_digest != expected_digest
            || file_identity(&file)? != identity
            || !path_still_binds_identity(&artifact.descriptor.path, &identity)
        {
            return Err(invalid_data(ARTIFACT_AUTHENTICATION_FAILED));
        }
    }

    if authentication_completed != authentication_total
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

fn path_still_binds_identity(path: &std::path::Path, expected: &FileIdentity) -> bool {
    open_source_artifact(path)
        .and_then(|file| file_identity(&file))
        .is_ok_and(|observed| &observed == expected)
}

pub(super) fn validate_base_checkpoint(
    checkpoint: &TaxIdentitySidecarBundleCheckpoint,
) -> io::Result<()> {
    let shard_count =
        u64::try_from(checkpoint.shards.len()).map_err(|_| invalid_data(COUNT_OVERFLOW))?;
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
    validate_v2_metadata(&shard.shard_id, descriptor)
        .map_err(|_| invalid_data(ARTIFACT_SET_MISMATCH))?;
    let metadata = &descriptor.metadata;
    let digest =
        decode_sha256(&metadata.sha256).map_err(|_| invalid_data(ARTIFACT_SET_MISMATCH))?;
    let resource_identity = derived_v2_resource_identity(&shard.shard_id, metadata, &digest)
        .map_err(|_| invalid_data(ARTIFACT_SET_MISMATCH))?;
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

fn hash_open_file(
    file: &mut File,
    expected_byte_count: u64,
    progress_base: u64,
    progress_total: u64,
    progress: &mut AuditProgressCallback<'_>,
) -> io::Result<[u8; 32]> {
    file.seek(SeekFrom::Start(0))
        .map_err(|_| invalid_data(ARTIFACT_UNAVAILABLE))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; HASH_BUFFER_BYTES];
    let mut observed = 0u64;
    while observed < expected_byte_count {
        let requested =
            usize::try_from((expected_byte_count - observed).min(HASH_BUFFER_BYTES as u64))
                .map_err(|_| invalid_data(ARTIFACT_CONTENT_MISMATCH))?;
        let read = file
            .read(&mut buffer[..requested])
            .map_err(|_| invalid_data(ARTIFACT_UNAVAILABLE))?;
        if read == 0 {
            return Err(invalid_data(ARTIFACT_CONTENT_MISMATCH));
        }
        hasher.update(&buffer[..read]);
        observed = checked_add(observed, read as u64)?;
        progress(
            TaxIdentityCollisionAuditPhase::Authenticate,
            checked_add(progress_base, observed)?,
            progress_total,
        )?;
    }
    let mut trailing = [0u8; 1];
    if file
        .read(&mut trailing)
        .map_err(|_| invalid_data(ARTIFACT_UNAVAILABLE))?
        != 0
    {
        return Err(invalid_data(ARTIFACT_CONTENT_MISMATCH));
    }
    file.seek(SeekFrom::Start(0))
        .map_err(|_| invalid_data(ARTIFACT_UNAVAILABLE))?;
    Ok(hasher.finalize().into())
}

fn open_source_artifact(path: &std::path::Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options
        .open(path)
        .map_err(|_| invalid_data(ARTIFACT_UNAVAILABLE))?;
    if !file
        .metadata()
        .map_err(|_| invalid_data(ARTIFACT_UNAVAILABLE))?
        .is_file()
    {
        return Err(invalid_data(ARTIFACT_UNAVAILABLE));
    }
    Ok(file)
}

#[derive(Clone, Eq, Hash, PartialEq)]
struct FileIdentity {
    byte_count: u64,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    modified_seconds: i64,
    #[cfg(unix)]
    modified_nanoseconds: i64,
    #[cfg(unix)]
    changed_seconds: i64,
    #[cfg(unix)]
    changed_nanoseconds: i64,
    #[cfg(not(unix))]
    modified: Option<std::time::SystemTime>,
}

#[cfg(unix)]
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
struct PhysicalFileIdentity {
    device: u64,
    inode: u64,
}

#[cfg(not(unix))]
#[derive(Eq, Hash, PartialEq)]
struct PhysicalFileIdentity;

#[cfg(unix)]
fn physical_file_identity(file: &File) -> io::Result<PhysicalFileIdentity> {
    use std::os::unix::fs::MetadataExt;

    let metadata = file
        .metadata()
        .map_err(|_| invalid_data(ARTIFACT_UNAVAILABLE))?;
    if !metadata.is_file() {
        return Err(invalid_data(ARTIFACT_UNAVAILABLE));
    }
    Ok(PhysicalFileIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

#[cfg(not(unix))]
fn physical_file_identity(_file: &File) -> io::Result<PhysicalFileIdentity> {
    Err(invalid_data(ARTIFACT_AUTHENTICATION_FAILED))
}

fn file_identity(file: &File) -> io::Result<FileIdentity> {
    let metadata = file
        .metadata()
        .map_err(|_| invalid_data(ARTIFACT_UNAVAILABLE))?;
    if !metadata.is_file() {
        return Err(invalid_data(ARTIFACT_UNAVAILABLE));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        Ok(FileIdentity {
            byte_count: metadata.len(),
            device: metadata.dev(),
            inode: metadata.ino(),
            modified_seconds: metadata.mtime(),
            modified_nanoseconds: metadata.mtime_nsec(),
            changed_seconds: metadata.ctime(),
            changed_nanoseconds: metadata.ctime_nsec(),
        })
    }
    #[cfg(not(unix))]
    {
        Ok(FileIdentity {
            byte_count: metadata.len(),
            modified: metadata.modified().ok(),
        })
    }
}
