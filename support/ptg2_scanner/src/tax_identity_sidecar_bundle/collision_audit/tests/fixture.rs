use super::super::invalid_data;
use crate::tax_identity::{TaxIdentityState, TaxIdentityStateV2};
use crate::tax_identity_sidecar_bundle::digests::encode_hex;
use crate::tax_identity_sidecar_bundle::{
    finalize_tax_identity_sidecar_bundle, validate_tax_identity_sidecar_shard,
    ProviderGroupUniverse, TaxIdentitySidecarBundleCheckpoint, TaxIdentitySidecarV1Admission,
    TaxIdentitySidecarV2ArtifactDescriptor, TaxIdentitySidecarV2Metadata,
    TAX_IDENTITY_SIDECAR_FULL_HMAC_AUTHORITY_CONTRACT, TAX_IDENTITY_SIDECAR_HMAC_CONTRACT,
    TAX_IDENTITY_SIDECAR_LOCATOR_CONTRACT, TAX_IDENTITY_SIDECAR_TOKEN_MESSAGE_CONTRACT,
    TAX_IDENTITY_SIDECAR_V1_NORMALIZATION_CONTRACT, TAX_IDENTITY_SIDECAR_V2_NORMALIZATION_CONTRACT,
    V1_RESOURCE_NAME, V2_RESOURCE_NAME,
};
use crate::tax_identity_sidecar_v1::{
    TaxIdentitySidecarV1Header, TaxIdentitySidecarV1Record, TAX_IDENTITY_SIDECAR_V1_FORMAT,
    TAX_IDENTITY_SIDECAR_V1_FORMAT_VERSION, TAX_IDENTITY_SIDECAR_V1_RECORD_BYTES,
};
use crate::tax_identity_sidecar_v2::{
    TaxIdentitySidecarV2Header, TaxIdentitySidecarV2Record, TAX_IDENTITY_SIDECAR_V2_FORMAT,
    TAX_IDENTITY_SIDECAR_V2_FORMAT_VERSION, TAX_IDENTITY_SIDECAR_V2_RECORD_BYTES,
};
use sha2::{Digest, Sha256};
use std::fs;
use std::io;
use std::path::PathBuf;
use tempfile::TempDir;

const POLICY: &str = "ptg-tin-hmac-sha256-v1:collision-audit-synthetic";
type RowSpec = (TaxIdentityStateV2, Option<[u8; 32]>);
type V1Row = ([u8; 16], TaxIdentityState, Option<[u8; 32]>);
type V2Row = ([u8; 16], TaxIdentityStateV2, Option<[u8; 32]>);

pub(super) struct BundleFixture {
    pub(super) temporary: TempDir,
    pub(super) checkpoint: TaxIdentitySidecarBundleCheckpoint,
    pub(super) descriptors: Vec<TaxIdentitySidecarV2ArtifactDescriptor>,
}

impl BundleFixture {
    pub(super) fn new(shards: Vec<Vec<RowSpec>>) -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let mut checkpoints = Vec::new();
        let mut descriptors = Vec::new();
        for (shard_index, rows) in shards.into_iter().enumerate() {
            let shard_id = format!("synthetic-{shard_index:04}");
            let groups = rows
                .iter()
                .enumerate()
                .map(|(row_index, _)| group(shard_index, row_index))
                .collect::<Vec<_>>();
            let v1_rows = rows
                .iter()
                .zip(&groups)
                .map(|((state, hmac), group)| v1_row(*group, *state, *hmac))
                .collect::<Vec<_>>();
            let v2_rows = rows
                .iter()
                .zip(&groups)
                .map(|((state, hmac), group)| (*group, *state, *hmac))
                .collect::<Vec<_>>();
            let v1_bytes = encode_v1(&v1_rows);
            let v2_bytes = encode_v2(&v2_rows);
            let v1_path = temporary.path().join(format!("{shard_id}.v1"));
            let v2_path = temporary.path().join(format!("{shard_id}.v2"));
            fs::write(&v1_path, &v1_bytes).unwrap();
            fs::write(&v2_path, &v2_bytes).unwrap();
            let descriptor = descriptor(&shard_id, &rows, &v2_path, &v2_bytes);
            let v1_counts = v1_state_counts(&v1_rows);
            let v1_sha256 = encode_hex(&Sha256::digest(&v1_bytes));
            let admission = TaxIdentitySidecarV1Admission {
                path: &v1_path,
                record_format: TAX_IDENTITY_SIDECAR_V1_FORMAT,
                sha256: &v1_sha256,
                byte_count: v1_bytes.len() as u64,
                row_count: rows.len() as u64,
                provider_group_count: rows.len() as u64,
                matched_ein_count: v1_counts[0],
                missing_count: v1_counts[1],
                malformed_count: v1_counts[2],
                unsupported_type_count: v1_counts[3],
                version: TAX_IDENTITY_SIDECAR_V1_FORMAT_VERSION,
                record_bytes: TAX_IDENTITY_SIDECAR_V1_RECORD_BYTES as u16,
                token_policy_id: POLICY,
                normalization_contract: TAX_IDENTITY_SIDECAR_V1_NORMALIZATION_CONTRACT,
                hmac_contract: TAX_IDENTITY_SIDECAR_HMAC_CONTRACT,
                final_file: true,
                name: Some(V1_RESOURCE_NAME),
                source_shard_id: Some(&shard_id),
                shard_id: None,
            };
            checkpoints.push(
                validate_tax_identity_sidecar_shard(
                    &shard_id,
                    admission,
                    &descriptor,
                    &VecUniverse(groups),
                )
                .unwrap(),
            );
            descriptors.push(descriptor);
        }
        Self {
            temporary,
            checkpoint: finalize_tax_identity_sidecar_bundle(checkpoints).unwrap(),
            descriptors,
        }
    }

    pub(super) fn scratch_root(&self, suffix: &str) -> PathBuf {
        let path = self.temporary.path().join(format!("scratch-{suffix}"));
        fs::create_dir(&path).unwrap();
        path
    }
}

fn descriptor(
    shard_id: &str,
    rows: &[RowSpec],
    path: &std::path::Path,
    bytes: &[u8],
) -> TaxIdentitySidecarV2ArtifactDescriptor {
    let counts = state_counts(rows);
    TaxIdentitySidecarV2ArtifactDescriptor {
        path: path.to_owned(),
        metadata: TaxIdentitySidecarV2Metadata {
            record_format: TAX_IDENTITY_SIDECAR_V2_FORMAT.to_owned(),
            sha256: encode_hex(&Sha256::digest(bytes)),
            byte_count: bytes.len() as u64,
            row_count: rows.len() as u64,
            provider_group_count: rows.len() as u64,
            matched_ein_count: counts[0],
            matched_npi_count: counts[1],
            missing_count: counts[2],
            malformed_count: counts[3],
            unsupported_type_count: counts[4],
            version: TAX_IDENTITY_SIDECAR_V2_FORMAT_VERSION,
            record_bytes: TAX_IDENTITY_SIDECAR_V2_RECORD_BYTES as u16,
            token_policy_id: POLICY.to_owned(),
            normalization_contract: TAX_IDENTITY_SIDECAR_V2_NORMALIZATION_CONTRACT.to_owned(),
            token_message_contract: TAX_IDENTITY_SIDECAR_TOKEN_MESSAGE_CONTRACT.to_owned(),
            hmac_contract: TAX_IDENTITY_SIDECAR_HMAC_CONTRACT.to_owned(),
            tin_id_128_contract: TAX_IDENTITY_SIDECAR_LOCATOR_CONTRACT.to_owned(),
            full_hmac_authority_contract: TAX_IDENTITY_SIDECAR_FULL_HMAC_AUTHORITY_CONTRACT
                .to_owned(),
            final_file: true,
            name: V2_RESOURCE_NAME.to_owned(),
            source_shard_id: shard_id.to_owned(),
        },
    }
}

struct VecUniverse(Vec<[u8; 16]>);

impl ProviderGroupUniverse for VecUniverse {
    fn provider_group_count(&self) -> u64 {
        self.0.len() as u64
    }

    fn provider_group_at(&self, index: u64) -> io::Result<[u8; 16]> {
        self.0
            .get(index as usize)
            .copied()
            .ok_or_else(|| invalid_data("synthetic universe mismatch"))
    }
}

fn v1_row(group: [u8; 16], state: TaxIdentityStateV2, hmac: Option<[u8; 32]>) -> V1Row {
    match state {
        TaxIdentityStateV2::MatchedEin => (group, TaxIdentityState::MatchedEin, hmac),
        TaxIdentityStateV2::MatchedNpi | TaxIdentityStateV2::UnsupportedType => {
            (group, TaxIdentityState::UnsupportedType, None)
        }
        TaxIdentityStateV2::Missing => (group, TaxIdentityState::Missing, None),
        TaxIdentityStateV2::Malformed => (group, TaxIdentityState::Malformed, None),
    }
}

fn state_counts(rows: &[RowSpec]) -> [u64; 5] {
    let mut counts = [0; 5];
    for (state, _) in rows {
        let index = match state {
            TaxIdentityStateV2::MatchedEin => 0,
            TaxIdentityStateV2::MatchedNpi => 1,
            TaxIdentityStateV2::Missing => 2,
            TaxIdentityStateV2::Malformed => 3,
            TaxIdentityStateV2::UnsupportedType => 4,
        };
        counts[index] += 1;
    }
    counts
}

fn v1_state_counts(rows: &[V1Row]) -> [u64; 4] {
    let mut counts = [0; 4];
    for (_, state, _) in rows {
        counts[match state {
            TaxIdentityState::MatchedEin => 0,
            TaxIdentityState::Missing => 1,
            TaxIdentityState::Malformed => 2,
            TaxIdentityState::UnsupportedType => 3,
        }] += 1;
    }
    counts
}

fn encode_v1(rows: &[V1Row]) -> Vec<u8> {
    let mut bytes = TaxIdentitySidecarV1Header::new(POLICY.to_owned())
        .unwrap()
        .encode();
    for (group, state, hmac) in rows {
        let hmac = hmac.unwrap_or([0; 32]);
        let record =
            TaxIdentitySidecarV1Record::new(*group, *state, hmac[..16].try_into().unwrap(), hmac)
                .unwrap();
        bytes.extend_from_slice(&record.encode());
    }
    bytes
}

fn encode_v2(rows: &[V2Row]) -> Vec<u8> {
    let mut bytes = TaxIdentitySidecarV2Header::new(POLICY.to_owned())
        .unwrap()
        .encode();
    for (group, state, hmac) in rows {
        let hmac = hmac.unwrap_or([0; 32]);
        let record =
            TaxIdentitySidecarV2Record::new(*group, *state, hmac[..16].try_into().unwrap(), hmac)
                .unwrap();
        bytes.extend_from_slice(&record.encode());
    }
    bytes
}

fn group(shard_index: usize, row_index: usize) -> [u8; 16] {
    let mut group = [0; 16];
    group[..8].copy_from_slice(&(shard_index as u64 + 1).to_be_bytes());
    group[8..].copy_from_slice(&(row_index as u64 + 1).to_be_bytes());
    group
}

pub(super) fn token(value: u8) -> [u8; 32] {
    [value; 32]
}
