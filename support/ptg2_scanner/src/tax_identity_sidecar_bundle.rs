//! Validation-only admission for paired tax-identity v1/v2 artifacts.
//!
//! This gate authenticates artifact metadata and bytes, runs the fixed-memory
//! v1/v2 pair validator, and proves exact ordinal parity with an authoritative
//! provider-group universe. It deliberately leaves v1 as the only projection
//! authority. Cross-row full-HMAC/type collision detection requires a separate
//! bounded external pass and is not claimed here.

use crate::tax_identity::TaxIdentityState;
use crate::tax_identity_sidecar_pair::{
    TaxIdentitySidecarPairSummary, TaxIdentitySidecarPairValidator,
};
use crate::tax_identity_sidecar_v1::{
    TAX_IDENTITY_SIDECAR_V1_FORMAT, TAX_IDENTITY_SIDECAR_V1_FORMAT_VERSION,
    TAX_IDENTITY_SIDECAR_V1_RECORD_BYTES,
};
use crate::tax_identity_sidecar_v2::{
    TAX_IDENTITY_SIDECAR_V2_FORMAT, TAX_IDENTITY_SIDECAR_V2_FORMAT_VERSION,
    TAX_IDENTITY_SIDECAR_V2_RECORD_BYTES,
};
use std::collections::HashSet;
use std::io::{self, BufReader};

mod collision_audit;
mod contracts;
mod digests;
mod files;

pub use collision_audit::{
    audit_tax_identity_sidecar_bundle, audit_tax_identity_sidecar_bundle_with_progress,
    TaxIdentityCollisionAuditConfig, TaxIdentityCollisionAuditLimits,
    TaxIdentityCollisionAuditPhase, TaxIdentityCollisionAuditProgress,
    TaxIdentityCollisionAuditResult, TaxIdentityCollisionAuditStats,
    TaxIdentitySidecarAuditedBundleCheckpoint, TAX_IDENTITY_COLLISION_AUDIT_CHECKPOINT_CONTRACT,
    TAX_IDENTITY_COLLISION_AUDIT_OCCURRENCE_DIGEST_CONTRACT,
    TAX_IDENTITY_COLLISION_AUDIT_RECORD_BYTES, TAX_IDENTITY_COLLISION_AUDIT_RECORD_CONTRACT,
    TAX_IDENTITY_COLLISION_CHECK_PASSED, TAX_IDENTITY_FULL_HMAC_TYPE_COLLISION_POLICY,
    TAX_IDENTITY_LOCATOR_COLLISION_POLICY, TAX_IDENTITY_MULTI_CANDIDATE_LOCATOR_SUPPORT,
    TAX_IDENTITY_SAME_IDENTITY_REPETITION_POLICY,
};
pub use contracts::{
    ProviderGroupUniverse, TaxIdentitySidecarBundleCheckpoint, TaxIdentitySidecarShardCheckpoint,
    TaxIdentitySidecarV1Admission, TaxIdentitySidecarV2ArtifactDescriptor,
    TaxIdentitySidecarV2Metadata,
};
use digests::{
    bundle_digest, checked_add, decode_sha256, derived_v1_resource_identity,
    derived_v2_resource_identity, encode_hex, sum,
};
use files::{open_authentic_artifact, reauthenticate_artifact};

pub const TAX_IDENTITY_SIDECAR_V1_NORMALIZATION_CONTRACT: &str =
    "ein_ascii_digits_or_2_7_hyphen_v1";
pub const TAX_IDENTITY_SIDECAR_V2_NORMALIZATION_CONTRACT: &str =
    "ein_ascii_digits_or_2_7_hyphen_and_npi_10_ascii_digits_cms_80840_luhn_v2";
pub const TAX_IDENTITY_SIDECAR_TOKEN_MESSAGE_CONTRACT: &str =
    "healthporta_ptg_tin_v1_nul_u16be_type_length_type_u16be_value_length_value";
pub const TAX_IDENTITY_SIDECAR_HMAC_CONTRACT: &str = "hmac_sha256_ptg_tin_v1";
pub const TAX_IDENTITY_SIDECAR_LOCATOR_CONTRACT: &str = "first_16_bytes(tin_hmac_sha256)";
pub const TAX_IDENTITY_SIDECAR_FULL_HMAC_AUTHORITY_CONTRACT: &str =
    "tin_hmac_sha256_full_32_bytes_authoritative";
pub const TAX_IDENTITY_SIDECAR_BUNDLE_CHECKPOINT_CONTRACT: &str =
    "ptg2_tax_identity_sidecar_bundle_validation_v1";
pub const TAX_IDENTITY_SIDECAR_PROJECTION_AUTHORITY: &str = "v1_only";
pub const TAX_IDENTITY_SIDECAR_COLLISION_AUDIT: &str = "required_external_pass";

const V1_RESOURCE_NAME: &str = "provider_group_tax_identity";
const V2_RESOURCE_NAME: &str = "provider_group_tax_identity_v2";

const INVALID_METADATA: &str = "PTG tax identity sidecar bundle metadata is invalid";
const UNIVERSE_MISMATCH: &str = "PTG tax identity sidecar provider-group universe does not match";
const ZERO_ROW_REJECTED: &str = "PTG tax identity paired bundle must not be empty";
const DUPLICATE_SHARD: &str = "PTG tax identity paired bundle contains duplicate shards";
const DUPLICATE_RESOURCE: &str = "PTG tax identity paired bundle contains duplicate resources";
const POLICY_MISMATCH: &str = "PTG tax identity paired bundle policies do not match";
const COUNT_OVERFLOW: &str = "PTG tax identity paired bundle count overflow";

/// Authenticate and compare one v1/v2 pair against its exact group universe.
pub fn validate_tax_identity_sidecar_shard<U: ProviderGroupUniverse>(
    shard_id: &str,
    v1: TaxIdentitySidecarV1Admission<'_>,
    v2: &TaxIdentitySidecarV2ArtifactDescriptor,
    universe: &U,
) -> io::Result<TaxIdentitySidecarShardCheckpoint> {
    validate_tax_identity_sidecar_shard_with_progress(shard_id, v1, v2, universe, |_| Ok(()))
}

/// Variant that reports every authenticated pair row for progress heartbeats.
pub fn validate_tax_identity_sidecar_shard_with_progress<
    U: ProviderGroupUniverse,
    P: FnMut(u64) -> io::Result<()>,
>(
    shard_id: &str,
    v1: TaxIdentitySidecarV1Admission<'_>,
    v2: &TaxIdentitySidecarV2ArtifactDescriptor,
    universe: &U,
    mut record_validated: P,
) -> io::Result<TaxIdentitySidecarShardCheckpoint> {
    validate_shard_id(shard_id)?;
    validate_v1_metadata(shard_id, &v1)?;
    validate_v2_metadata(shard_id, v2)?;

    let universe_count = universe.provider_group_count();
    if universe_count == 0 || v1.row_count == 0 || v2.metadata.row_count == 0 {
        return Err(invalid_data(ZERO_ROW_REJECTED));
    }
    if v1.row_count != universe_count
        || v1.provider_group_count != universe_count
        || v2.metadata.row_count != universe_count
        || v2.metadata.provider_group_count != universe_count
    {
        return Err(invalid_data(UNIVERSE_MISMATCH));
    }
    if v1.token_policy_id != v2.metadata.token_policy_id {
        return Err(invalid_data(POLICY_MISMATCH));
    }

    let (mut v1_file, v1_digest, v1_file_identity) =
        open_authentic_artifact(v1.path, v1.byte_count, decode_sha256(v1.sha256)?)?;
    let (mut v2_file, v2_digest, v2_file_identity) = open_authentic_artifact(
        &v2.path,
        v2.metadata.byte_count,
        decode_sha256(&v2.metadata.sha256)?,
    )?;
    let mut v1_counts = [0u64; 4];
    let mut ordinal = 0u64;
    let summary = {
        let mut pair = TaxIdentitySidecarPairValidator::new(
            BufReader::new(&mut v1_file),
            BufReader::new(&mut v2_file),
            universe_count,
        )?;
        if pair.policy_id() != v1.token_policy_id {
            return Err(invalid_data(POLICY_MISMATCH));
        }
        while let Some(record) = pair.next_record()? {
            let authoritative_group = universe
                .provider_group_at(ordinal)
                .map_err(|_| invalid_data(UNIVERSE_MISMATCH))?;
            if *record.v1().provider_group_global_id() != authoritative_group {
                return Err(invalid_data(UNIVERSE_MISMATCH));
            }
            let state_index = match record.v1().state() {
                TaxIdentityState::MatchedEin => 0,
                TaxIdentityState::Missing => 1,
                TaxIdentityState::Malformed => 2,
                TaxIdentityState::UnsupportedType => 3,
            };
            v1_counts[state_index] = checked_add(v1_counts[state_index], 1)?;
            ordinal = checked_add(ordinal, 1)?;
            record_validated(ordinal)?;
        }
        pair.validated_summary()
            .ok_or_else(|| invalid_data(UNIVERSE_MISMATCH))?
    };
    if ordinal != universe_count {
        return Err(invalid_data(UNIVERSE_MISMATCH));
    }
    reauthenticate_artifact(&mut v1_file, &v1_file_identity, v1.byte_count, v1_digest)?;
    reauthenticate_artifact(
        &mut v2_file,
        &v2_file_identity,
        v2.metadata.byte_count,
        v2_digest,
    )?;
    validate_state_metadata(&v1, v1_counts, &v2.metadata, summary)?;
    let v1_resource_identity = derived_v1_resource_identity(shard_id, &v1, &v1_digest)?;
    let v2_resource_identity = derived_v2_resource_identity(shard_id, &v2.metadata, &v2_digest)?;

    Ok(TaxIdentitySidecarShardCheckpoint {
        shard_id: shard_id.to_owned(),
        v1_resource_identity,
        v2_resource_identity,
        token_policy_id: v1.token_policy_id.to_owned(),
        authoritative_provider_group_count: universe_count,
        row_count: summary.row_count(),
        matched_ein_count: summary.matched_ein_count(),
        matched_npi_count: summary.matched_npi_count(),
        missing_count: summary.missing_count(),
        malformed_count: summary.malformed_count(),
        unsupported_type_count: summary.unsupported_type_count(),
        v1_sha256: encode_hex(&v1_digest),
        v1_byte_count: v1.byte_count,
        v2_sha256: encode_hex(&v2_digest),
        v2_byte_count: v2.metadata.byte_count,
    })
}

/// Compute the only accepted path-independent identity for typed v2 metadata.
#[cfg(test)]
fn derive_tax_identity_sidecar_v2_resource_identity(
    shard_id: &str,
    metadata: &TaxIdentitySidecarV2Metadata,
) -> io::Result<String> {
    let digest = decode_sha256(&metadata.sha256)?;
    derived_v2_resource_identity(shard_id, metadata, &digest)
}

/// Reduce validated shard checkpoints into one deterministic bundle checkpoint.
pub fn finalize_tax_identity_sidecar_bundle(
    mut shards: Vec<TaxIdentitySidecarShardCheckpoint>,
) -> io::Result<TaxIdentitySidecarBundleCheckpoint> {
    if shards.is_empty() {
        return Err(invalid_data(ZERO_ROW_REJECTED));
    }
    shards.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
    if shards
        .windows(2)
        .any(|pair| pair[0].shard_id == pair[1].shard_id)
    {
        return Err(invalid_data(DUPLICATE_SHARD));
    }
    let mut v1_resources = HashSet::with_capacity(shards.len());
    if shards
        .iter()
        .any(|shard| !v1_resources.insert(shard.v1_resource_identity.as_str()))
    {
        return Err(invalid_data(DUPLICATE_RESOURCE));
    }
    let mut v2_resources = HashSet::with_capacity(shards.len());
    if shards
        .iter()
        .any(|shard| !v2_resources.insert(shard.v2_resource_identity.as_str()))
    {
        return Err(invalid_data(DUPLICATE_RESOURCE));
    }
    let token_policy_id = shards[0].token_policy_id.clone();
    if shards
        .iter()
        .any(|shard| shard.token_policy_id != token_policy_id)
    {
        return Err(invalid_data(POLICY_MISMATCH));
    }

    let shard_count = u64::try_from(shards.len()).map_err(|_| invalid_data(COUNT_OVERFLOW))?;
    let authoritative_provider_group_count =
        sum(&shards, |shard| shard.authoritative_provider_group_count)?;
    let row_count = sum(&shards, |shard| shard.row_count)?;
    if authoritative_provider_group_count == 0 || row_count != authoritative_provider_group_count {
        return Err(invalid_data(ZERO_ROW_REJECTED));
    }
    let matched_ein_count = sum(&shards, |shard| shard.matched_ein_count)?;
    let matched_npi_count = sum(&shards, |shard| shard.matched_npi_count)?;
    let missing_count = sum(&shards, |shard| shard.missing_count)?;
    let malformed_count = sum(&shards, |shard| shard.malformed_count)?;
    let unsupported_type_count = sum(&shards, |shard| shard.unsupported_type_count)?;
    let v1_byte_count = sum(&shards, |shard| shard.v1_byte_count)?;
    let v2_byte_count = sum(&shards, |shard| shard.v2_byte_count)?;
    let state_total = [
        matched_ein_count,
        matched_npi_count,
        missing_count,
        malformed_count,
        unsupported_type_count,
    ]
    .into_iter()
    .try_fold(0u64, checked_add)?;
    if state_total != row_count {
        return Err(invalid_data(INVALID_METADATA));
    }

    let bundle_digest = bundle_digest(&token_policy_id, &shards)?;
    Ok(TaxIdentitySidecarBundleCheckpoint {
        contract: TAX_IDENTITY_SIDECAR_BUNDLE_CHECKPOINT_CONTRACT.to_owned(),
        publication_admissible: false,
        projection_authority: TAX_IDENTITY_SIDECAR_PROJECTION_AUTHORITY.to_owned(),
        cross_row_full_hmac_type_collision_check: TAX_IDENTITY_SIDECAR_COLLISION_AUDIT.to_owned(),
        token_policy_id,
        shard_count,
        authoritative_provider_group_count,
        row_count,
        matched_ein_count,
        matched_npi_count,
        missing_count,
        malformed_count,
        unsupported_type_count,
        v1_byte_count,
        v2_byte_count,
        bundle_sha256: encode_hex(&bundle_digest),
        shards,
    })
}

fn validate_v1_metadata(
    shard_id: &str,
    metadata: &TaxIdentitySidecarV1Admission<'_>,
) -> io::Result<()> {
    let source = resolved_shard_id(metadata.source_shard_id, metadata.shard_id)?;
    let state_total = [
        metadata.matched_ein_count,
        metadata.missing_count,
        metadata.malformed_count,
        metadata.unsupported_type_count,
    ]
    .into_iter()
    .try_fold(0u64, checked_add)?;
    if metadata.record_format != TAX_IDENTITY_SIDECAR_V1_FORMAT
        || metadata.version != TAX_IDENTITY_SIDECAR_V1_FORMAT_VERSION
        || usize::from(metadata.record_bytes) != TAX_IDENTITY_SIDECAR_V1_RECORD_BYTES
        || metadata.provider_group_count != metadata.row_count
        || state_total != metadata.row_count
        || metadata.normalization_contract != TAX_IDENTITY_SIDECAR_V1_NORMALIZATION_CONTRACT
        || metadata.hmac_contract != TAX_IDENTITY_SIDECAR_HMAC_CONTRACT
        || !metadata.final_file
        || metadata.name != Some(V1_RESOURCE_NAME)
        || source != Some(shard_id)
    {
        return Err(invalid_data(INVALID_METADATA));
    }
    Ok(())
}

fn validate_v2_metadata(
    shard_id: &str,
    descriptor: &TaxIdentitySidecarV2ArtifactDescriptor,
) -> io::Result<()> {
    let metadata = &descriptor.metadata;
    let state_total = [
        metadata.matched_ein_count,
        metadata.matched_npi_count,
        metadata.missing_count,
        metadata.malformed_count,
        metadata.unsupported_type_count,
    ]
    .into_iter()
    .try_fold(0u64, checked_add)?;
    if metadata.record_format != TAX_IDENTITY_SIDECAR_V2_FORMAT
        || metadata.version != TAX_IDENTITY_SIDECAR_V2_FORMAT_VERSION
        || usize::from(metadata.record_bytes) != TAX_IDENTITY_SIDECAR_V2_RECORD_BYTES
        || metadata.provider_group_count != metadata.row_count
        || state_total != metadata.row_count
        || metadata.normalization_contract != TAX_IDENTITY_SIDECAR_V2_NORMALIZATION_CONTRACT
        || metadata.token_message_contract != TAX_IDENTITY_SIDECAR_TOKEN_MESSAGE_CONTRACT
        || metadata.hmac_contract != TAX_IDENTITY_SIDECAR_HMAC_CONTRACT
        || metadata.tin_id_128_contract != TAX_IDENTITY_SIDECAR_LOCATOR_CONTRACT
        || metadata.full_hmac_authority_contract
            != TAX_IDENTITY_SIDECAR_FULL_HMAC_AUTHORITY_CONTRACT
        || !metadata.final_file
        || metadata.name != V2_RESOURCE_NAME
        || metadata.source_shard_id != shard_id
    {
        return Err(invalid_data(INVALID_METADATA));
    }
    Ok(())
}

fn validate_state_metadata(
    v1: &TaxIdentitySidecarV1Admission<'_>,
    v1_counts: [u64; 4],
    v2: &TaxIdentitySidecarV2Metadata,
    summary: TaxIdentitySidecarPairSummary,
) -> io::Result<()> {
    if v1_counts
        != [
            v1.matched_ein_count,
            v1.missing_count,
            v1.malformed_count,
            v1.unsupported_type_count,
        ]
        || summary.row_count() != v2.row_count
        || summary.matched_ein_count() != v2.matched_ein_count
        || summary.matched_npi_count() != v2.matched_npi_count
        || summary.missing_count() != v2.missing_count
        || summary.malformed_count() != v2.malformed_count
        || summary.unsupported_type_count() != v2.unsupported_type_count
    {
        return Err(invalid_data(INVALID_METADATA));
    }
    Ok(())
}

fn resolved_shard_id<'a>(
    source_shard_id: Option<&'a str>,
    shard_id: Option<&'a str>,
) -> io::Result<Option<&'a str>> {
    if source_shard_id.is_some_and(|value| value.is_empty() || value.trim() != value)
        || shard_id.is_some_and(|value| value.is_empty() || value.trim() != value)
    {
        return Err(invalid_data(INVALID_METADATA));
    }
    let source = source_shard_id.map(str::trim);
    let alias = shard_id.map(str::trim);
    if source.is_some() && alias.is_some() && source != alias {
        return Err(invalid_data(INVALID_METADATA));
    }
    Ok(source.or(alias))
}

fn validate_shard_id(shard_id: &str) -> io::Result<()> {
    if shard_id.is_empty() || shard_id.trim() != shard_id {
        return Err(invalid_data(INVALID_METADATA));
    }
    Ok(())
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests;
