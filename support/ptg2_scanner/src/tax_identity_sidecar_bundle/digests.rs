use super::{
    invalid_data, TaxIdentitySidecarShardCheckpoint, TaxIdentitySidecarV1Admission,
    TaxIdentitySidecarV2Metadata, TAX_IDENTITY_SIDECAR_BUNDLE_CHECKPOINT_CONTRACT,
    TAX_IDENTITY_SIDECAR_COLLISION_AUDIT, TAX_IDENTITY_SIDECAR_PROJECTION_AUTHORITY,
};
use sha2::{Digest, Sha256};
use std::io;

const BUNDLE_DIGEST_DOMAIN: &[u8] = b"PTG2TAXBUNDLE\x01";
const RESOURCE_IDENTITY_DIGEST_DOMAIN: &[u8] = b"PTG2TAXRESOURCE\x01";
const RESOURCE_IDENTITY_PREFIX: &str = "ptg2-tax-sidecar-resource-v1:";
const INVALID_METADATA: &str = "PTG tax identity sidecar bundle metadata is invalid";
const COUNT_OVERFLOW: &str = "PTG tax identity paired bundle count overflow";

pub(super) fn derived_v1_resource_identity(
    shard_id: &str,
    metadata: &TaxIdentitySidecarV1Admission<'_>,
    digest: &[u8; 32],
) -> io::Result<String> {
    let mut hasher = resource_identity_hasher(1, shard_id, metadata.token_policy_id, digest)?;
    hasher.update(metadata.byte_count.to_be_bytes());
    hasher.update(metadata.row_count.to_be_bytes());
    hasher.update(metadata.provider_group_count.to_be_bytes());
    hasher.update(metadata.matched_ein_count.to_be_bytes());
    hasher.update(metadata.missing_count.to_be_bytes());
    hasher.update(metadata.malformed_count.to_be_bytes());
    hasher.update(metadata.unsupported_type_count.to_be_bytes());
    update_length_prefixed(&mut hasher, metadata.record_format)?;
    hasher.update(metadata.version.to_be_bytes());
    hasher.update(metadata.record_bytes.to_be_bytes());
    update_length_prefixed(&mut hasher, metadata.normalization_contract)?;
    update_length_prefixed(&mut hasher, metadata.hmac_contract)?;
    update_length_prefixed(&mut hasher, metadata.name.unwrap_or_default())?;
    update_length_prefixed(&mut hasher, metadata.source_shard_id.unwrap_or_default())?;
    update_length_prefixed(&mut hasher, metadata.shard_id.unwrap_or_default())?;
    hasher.update([u8::from(metadata.final_file)]);
    Ok(format!(
        "{RESOURCE_IDENTITY_PREFIX}{}",
        encode_hex(&hasher.finalize())
    ))
}

pub(super) fn derived_v2_resource_identity(
    shard_id: &str,
    metadata: &TaxIdentitySidecarV2Metadata,
    digest: &[u8; 32],
) -> io::Result<String> {
    let mut hasher = resource_identity_hasher(2, shard_id, &metadata.token_policy_id, digest)?;
    hasher.update(metadata.byte_count.to_be_bytes());
    hasher.update(metadata.row_count.to_be_bytes());
    hasher.update(metadata.provider_group_count.to_be_bytes());
    hasher.update(metadata.matched_ein_count.to_be_bytes());
    hasher.update(metadata.matched_npi_count.to_be_bytes());
    hasher.update(metadata.missing_count.to_be_bytes());
    hasher.update(metadata.malformed_count.to_be_bytes());
    hasher.update(metadata.unsupported_type_count.to_be_bytes());
    update_length_prefixed(&mut hasher, &metadata.record_format)?;
    hasher.update(metadata.version.to_be_bytes());
    hasher.update(metadata.record_bytes.to_be_bytes());
    update_length_prefixed(&mut hasher, &metadata.normalization_contract)?;
    update_length_prefixed(&mut hasher, &metadata.token_message_contract)?;
    update_length_prefixed(&mut hasher, &metadata.hmac_contract)?;
    update_length_prefixed(&mut hasher, &metadata.tin_id_128_contract)?;
    update_length_prefixed(&mut hasher, &metadata.full_hmac_authority_contract)?;
    update_length_prefixed(&mut hasher, &metadata.name)?;
    update_length_prefixed(&mut hasher, &metadata.source_shard_id)?;
    hasher.update([u8::from(metadata.final_file)]);
    Ok(format!(
        "{RESOURCE_IDENTITY_PREFIX}{}",
        encode_hex(&hasher.finalize())
    ))
}

pub(super) fn bundle_digest(
    token_policy_id: &str,
    shards: &[TaxIdentitySidecarShardCheckpoint],
) -> io::Result<[u8; 32]> {
    let mut hasher = Sha256::new();
    hasher.update(BUNDLE_DIGEST_DOMAIN);
    update_length_prefixed(&mut hasher, TAX_IDENTITY_SIDECAR_BUNDLE_CHECKPOINT_CONTRACT)?;
    hasher.update([0]);
    update_length_prefixed(&mut hasher, TAX_IDENTITY_SIDECAR_PROJECTION_AUTHORITY)?;
    update_length_prefixed(&mut hasher, TAX_IDENTITY_SIDECAR_COLLISION_AUDIT)?;
    update_length_prefixed(&mut hasher, token_policy_id)?;
    hasher.update(
        u64::try_from(shards.len())
            .map_err(|_| invalid_data(COUNT_OVERFLOW))?
            .to_be_bytes(),
    );
    for shard in shards {
        update_length_prefixed(&mut hasher, &shard.shard_id)?;
        update_length_prefixed(&mut hasher, &shard.v1_resource_identity)?;
        update_length_prefixed(&mut hasher, &shard.v2_resource_identity)?;
        update_length_prefixed(&mut hasher, &shard.token_policy_id)?;
        hasher.update(shard.authoritative_provider_group_count.to_be_bytes());
        hasher.update(shard.row_count.to_be_bytes());
        hasher.update(shard.matched_ein_count.to_be_bytes());
        hasher.update(shard.matched_npi_count.to_be_bytes());
        hasher.update(shard.missing_count.to_be_bytes());
        hasher.update(shard.malformed_count.to_be_bytes());
        hasher.update(shard.unsupported_type_count.to_be_bytes());
        hasher.update(decode_sha256(&shard.v1_sha256)?);
        hasher.update(shard.v1_byte_count.to_be_bytes());
        hasher.update(decode_sha256(&shard.v2_sha256)?);
        hasher.update(shard.v2_byte_count.to_be_bytes());
    }
    Ok(hasher.finalize().into())
}

pub(super) fn decode_sha256(value: &str) -> io::Result<[u8; 32]> {
    if value.len() != 64 {
        return Err(invalid_data(INVALID_METADATA));
    }
    let mut digest = [0u8; 32];
    for (index, destination) in digest.iter_mut().enumerate() {
        let high = decode_hex(value.as_bytes()[index * 2])?;
        let low = decode_hex(value.as_bytes()[index * 2 + 1])?;
        *destination = (high << 4) | low;
    }
    Ok(digest)
}

pub(super) fn encode_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push(DIGITS[(byte >> 4) as usize] as char);
        result.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    result
}

pub(super) fn sum(
    shards: &[TaxIdentitySidecarShardCheckpoint],
    field: impl Fn(&TaxIdentitySidecarShardCheckpoint) -> u64,
) -> io::Result<u64> {
    shards.iter().map(field).try_fold(0u64, checked_add)
}

pub(super) fn checked_add(left: u64, right: u64) -> io::Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| invalid_data(COUNT_OVERFLOW))
}

fn resource_identity_hasher(
    sidecar_version: u8,
    shard_id: &str,
    token_policy_id: &str,
    digest: &[u8; 32],
) -> io::Result<Sha256> {
    let mut hasher = Sha256::new();
    hasher.update(RESOURCE_IDENTITY_DIGEST_DOMAIN);
    hasher.update([sidecar_version]);
    update_length_prefixed(&mut hasher, shard_id)?;
    update_length_prefixed(&mut hasher, token_policy_id)?;
    hasher.update(digest);
    Ok(hasher)
}

fn update_length_prefixed(hasher: &mut Sha256, value: &str) -> io::Result<()> {
    let length = u32::try_from(value.len()).map_err(|_| invalid_data(COUNT_OVERFLOW))?;
    hasher.update(length.to_be_bytes());
    hasher.update(value.as_bytes());
    Ok(())
}

fn decode_hex(value: u8) -> io::Result<u8> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(invalid_data(INVALID_METADATA)),
    }
}
