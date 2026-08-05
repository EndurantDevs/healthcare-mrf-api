use serde::{Deserialize, Serialize};
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

/// Strict compiler descriptor metadata for one additive v2 sidecar.
#[derive(Clone, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TaxIdentitySidecarV2Metadata {
    pub record_format: String,
    pub sha256: String,
    pub byte_count: u64,
    pub row_count: u64,
    pub provider_group_count: u64,
    pub matched_ein_count: u64,
    pub matched_npi_count: u64,
    pub missing_count: u64,
    pub malformed_count: u64,
    pub unsupported_type_count: u64,
    pub version: u16,
    pub record_bytes: u16,
    pub token_policy_id: String,
    pub normalization_contract: String,
    pub token_message_contract: String,
    pub hmac_contract: String,
    pub tin_id_128_contract: String,
    pub full_hmac_authority_contract: String,
    #[serde(rename = "final")]
    pub final_file: bool,
    pub name: String,
    pub source_shard_id: String,
}

impl fmt::Debug for TaxIdentitySidecarV2Metadata {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaxIdentitySidecarV2Metadata")
            .field("record_format", &self.record_format)
            .field("byte_count", &self.byte_count)
            .field("row_count", &self.row_count)
            .field("provider_group_count", &self.provider_group_count)
            .field("token_policy_id", &"<redacted>")
            .field("sha256", &"<redacted>")
            .field("source_shard_id", &"<opaque>")
            .finish_non_exhaustive()
    }
}

/// A v2 artifact whose stable resource identity is derived after validation.
#[derive(Clone, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TaxIdentitySidecarV2ArtifactDescriptor {
    pub path: PathBuf,
    pub metadata: TaxIdentitySidecarV2Metadata,
}

impl fmt::Debug for TaxIdentitySidecarV2ArtifactDescriptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaxIdentitySidecarV2ArtifactDescriptor")
            .field("path", &"<redacted>")
            .field("metadata", &self.metadata)
            .finish()
    }
}

/// Borrowed v1 admission fields from the compiler's existing typed descriptor.
#[derive(Clone, Copy)]
pub struct TaxIdentitySidecarV1Admission<'a> {
    pub path: &'a Path,
    pub record_format: &'a str,
    pub sha256: &'a str,
    pub byte_count: u64,
    pub row_count: u64,
    pub provider_group_count: u64,
    pub matched_ein_count: u64,
    pub missing_count: u64,
    pub malformed_count: u64,
    pub unsupported_type_count: u64,
    pub version: u16,
    pub record_bytes: u16,
    pub token_policy_id: &'a str,
    pub normalization_contract: &'a str,
    pub hmac_contract: &'a str,
    pub final_file: bool,
    pub name: Option<&'a str>,
    pub source_shard_id: Option<&'a str>,
    pub shard_id: Option<&'a str>,
}

impl fmt::Debug for TaxIdentitySidecarV1Admission<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaxIdentitySidecarV1Admission")
            .field("path", &"<redacted>")
            .field("sha256", &"<redacted>")
            .field("token_policy_id", &"<redacted>")
            .field("source_shard_id", &"<opaque>")
            .field("shard_id", &"<opaque>")
            .field("row_count", &self.row_count)
            .finish_non_exhaustive()
    }
}

/// Ordered provider-group owners from the authoritative membership artifact.
pub trait ProviderGroupUniverse {
    fn provider_group_count(&self) -> u64;

    fn provider_group_at(&self, index: u64) -> io::Result<[u8; 16]>;
}

/// Immutable validation evidence for one admitted shard pair.
#[derive(Clone, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TaxIdentitySidecarShardCheckpoint {
    pub(super) shard_id: String,
    pub(super) v1_resource_identity: String,
    pub(super) v2_resource_identity: String,
    pub(super) token_policy_id: String,
    pub(super) authoritative_provider_group_count: u64,
    pub(super) row_count: u64,
    pub(super) matched_ein_count: u64,
    pub(super) matched_npi_count: u64,
    pub(super) missing_count: u64,
    pub(super) malformed_count: u64,
    pub(super) unsupported_type_count: u64,
    pub(super) v1_sha256: String,
    pub(super) v1_byte_count: u64,
    pub(super) v2_sha256: String,
    pub(super) v2_byte_count: u64,
}

impl fmt::Debug for TaxIdentitySidecarShardCheckpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaxIdentitySidecarShardCheckpoint")
            .field("shard_id", &"<opaque>")
            .field("v1_resource_identity", &"<redacted>")
            .field("v2_resource_identity", &"<redacted>")
            .field("token_policy_id", &"<redacted>")
            .field("row_count", &self.row_count)
            .field("v1_sha256", &"<redacted>")
            .field("v2_sha256", &"<redacted>")
            .finish_non_exhaustive()
    }
}

/// Deterministic, validation-only checkpoint for an all-paired bundle.
#[derive(Clone, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TaxIdentitySidecarBundleCheckpoint {
    pub(super) contract: String,
    pub(super) publication_admissible: bool,
    pub(super) projection_authority: String,
    pub(super) cross_row_full_hmac_type_collision_check: String,
    pub(super) token_policy_id: String,
    pub(super) shard_count: u64,
    pub(super) authoritative_provider_group_count: u64,
    pub(super) row_count: u64,
    pub(super) matched_ein_count: u64,
    pub(super) matched_npi_count: u64,
    pub(super) missing_count: u64,
    pub(super) malformed_count: u64,
    pub(super) unsupported_type_count: u64,
    pub(super) v1_byte_count: u64,
    pub(super) v2_byte_count: u64,
    pub(super) bundle_sha256: String,
    pub(super) shards: Vec<TaxIdentitySidecarShardCheckpoint>,
}

impl TaxIdentitySidecarBundleCheckpoint {
    pub fn publication_admissible(&self) -> bool {
        self.publication_admissible
    }

    pub fn projection_authority(&self) -> &str {
        &self.projection_authority
    }

    pub fn cross_row_full_hmac_type_collision_check(&self) -> &str {
        &self.cross_row_full_hmac_type_collision_check
    }

    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    pub fn bundle_sha256(&self) -> &str {
        &self.bundle_sha256
    }

    pub fn v2_byte_count(&self) -> u64 {
        self.v2_byte_count
    }
}

impl fmt::Debug for TaxIdentitySidecarBundleCheckpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaxIdentitySidecarBundleCheckpoint")
            .field("publication_admissible", &self.publication_admissible)
            .field("projection_authority", &self.projection_authority)
            .field(
                "cross_row_full_hmac_type_collision_check",
                &self.cross_row_full_hmac_type_collision_check,
            )
            .field("token_policy_id", &"<redacted>")
            .field("shard_count", &self.shard_count)
            .field("row_count", &self.row_count)
            .field("bundle_sha256", &"<redacted>")
            .field("shards", &"<redacted>")
            .finish_non_exhaustive()
    }
}
