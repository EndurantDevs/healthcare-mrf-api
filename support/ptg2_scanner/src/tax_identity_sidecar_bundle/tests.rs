use super::*;
use crate::tax_identity::{TaxIdentityState, TaxIdentityStateV2};
use crate::tax_identity_sidecar_bundle::files::{
    ARTIFACT_DIGEST_MISMATCH, ARTIFACT_SIZE_MISMATCH, HASH_BUFFER_BYTES, UNAVAILABLE_ARTIFACT,
};
use crate::tax_identity_sidecar_v1::{TaxIdentitySidecarV1Header, TaxIdentitySidecarV1Record};
use crate::tax_identity_sidecar_v2::{TaxIdentitySidecarV2Header, TaxIdentitySidecarV2Record};
use sha2::{Digest, Sha256};
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use tempfile::TempDir;

mod file_tests;
mod metadata_tests;
mod reducer_tests;

const POLICY: &str = "ptg-tin-hmac-sha256-v1:synthetic";

type V1Row = ([u8; 16], TaxIdentityState, Option<[u8; 32]>);
type V2Row = ([u8; 16], TaxIdentityStateV2, Option<[u8; 32]>);

#[derive(Clone)]
struct VecUniverse(Vec<[u8; 16]>);

impl ProviderGroupUniverse for VecUniverse {
    fn provider_group_count(&self) -> u64 {
        self.0.len() as u64
    }

    fn provider_group_at(&self, index: u64) -> io::Result<[u8; 16]> {
        self.0
            .get(index as usize)
            .copied()
            .ok_or_else(|| invalid_data(UNIVERSE_MISMATCH))
    }
}

struct FailingUniverse {
    groups: Vec<[u8; 16]>,
    fail_at: u64,
}

impl ProviderGroupUniverse for FailingUniverse {
    fn provider_group_count(&self) -> u64 {
        self.groups.len() as u64
    }

    fn provider_group_at(&self, index: u64) -> io::Result<[u8; 16]> {
        if index == self.fail_at {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "sensitive authoritative universe failure",
            ));
        }
        self.groups
            .get(index as usize)
            .copied()
            .ok_or_else(|| invalid_data(UNIVERSE_MISMATCH))
    }
}

struct PairFixture {
    _temporary: TempDir,
    groups: Vec<[u8; 16]>,
    v1_path: PathBuf,
    v1_sha256: String,
    v1_byte_count: u64,
    v2: TaxIdentitySidecarV2ArtifactDescriptor,
}

impl PairFixture {
    fn new() -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let groups = vec![group(1), group(2), group(3)];
        let ein_hmac = token(0x31);
        let npi_hmac = token(0x72);
        let v1_bytes = encode_v1(&[
            (groups[0], TaxIdentityState::MatchedEin, Some(ein_hmac)),
            (groups[1], TaxIdentityState::UnsupportedType, None),
            (groups[2], TaxIdentityState::Missing, None),
        ]);
        let v2_bytes = encode_v2(&[
            (groups[0], TaxIdentityStateV2::MatchedEin, Some(ein_hmac)),
            (groups[1], TaxIdentityStateV2::MatchedNpi, Some(npi_hmac)),
            (groups[2], TaxIdentityStateV2::Missing, None),
        ]);
        let v1_path = temporary.path().join("v1.sidecar");
        let v2_path = temporary.path().join("v2.sidecar");
        fs::write(&v1_path, &v1_bytes).unwrap();
        fs::write(&v2_path, &v2_bytes).unwrap();
        let metadata = TaxIdentitySidecarV2Metadata {
            record_format: TAX_IDENTITY_SIDECAR_V2_FORMAT.to_owned(),
            sha256: encode_hex(&Sha256::digest(&v2_bytes)),
            byte_count: v2_bytes.len() as u64,
            row_count: 3,
            provider_group_count: 3,
            matched_ein_count: 1,
            matched_npi_count: 1,
            missing_count: 1,
            malformed_count: 0,
            unsupported_type_count: 0,
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
            source_shard_id: "synthetic-a".to_owned(),
        };
        let v2 = TaxIdentitySidecarV2ArtifactDescriptor {
            path: v2_path,
            metadata,
        };
        Self {
            _temporary: temporary,
            groups,
            v1_path,
            v1_sha256: encode_hex(&Sha256::digest(&v1_bytes)),
            v1_byte_count: v1_bytes.len() as u64,
            v2,
        }
    }

    fn v1(&self) -> TaxIdentitySidecarV1Admission<'_> {
        TaxIdentitySidecarV1Admission {
            path: &self.v1_path,
            record_format: TAX_IDENTITY_SIDECAR_V1_FORMAT,
            sha256: &self.v1_sha256,
            byte_count: self.v1_byte_count,
            row_count: 3,
            provider_group_count: 3,
            matched_ein_count: 1,
            missing_count: 1,
            malformed_count: 0,
            unsupported_type_count: 1,
            version: TAX_IDENTITY_SIDECAR_V1_FORMAT_VERSION,
            record_bytes: TAX_IDENTITY_SIDECAR_V1_RECORD_BYTES as u16,
            token_policy_id: POLICY,
            normalization_contract: TAX_IDENTITY_SIDECAR_V1_NORMALIZATION_CONTRACT,
            hmac_contract: TAX_IDENTITY_SIDECAR_HMAC_CONTRACT,
            final_file: true,
            name: Some(V1_RESOURCE_NAME),
            source_shard_id: Some("synthetic-a"),
            shard_id: None,
        }
    }

    fn validate(&self) -> io::Result<TaxIdentitySidecarShardCheckpoint> {
        validate_tax_identity_sidecar_shard(
            "synthetic-a",
            self.v1(),
            &self.v2,
            &VecUniverse(self.groups.clone()),
        )
    }
}

#[test]
fn validates_pair_and_emits_non_publishable_v1_only_checkpoint() {
    let fixture = PairFixture::new();
    let shard = fixture.validate().unwrap();
    let checkpoint = finalize_tax_identity_sidecar_bundle(vec![shard]).unwrap();

    assert_eq!(
        checkpoint.contract,
        TAX_IDENTITY_SIDECAR_BUNDLE_CHECKPOINT_CONTRACT
    );
    assert!(!checkpoint.publication_admissible());
    assert_eq!(checkpoint.projection_authority(), "v1_only");
    assert_eq!(
        checkpoint.cross_row_full_hmac_type_collision_check(),
        "required_external_pass"
    );
    assert_eq!(checkpoint.row_count(), 3);
    assert_eq!(checkpoint.matched_ein_count, 1);
    assert_eq!(checkpoint.matched_npi_count, 1);
    assert_eq!(checkpoint.missing_count, 1);
    assert_eq!(checkpoint.bundle_sha256().len(), 64);
    assert_eq!(checkpoint.v2_byte_count(), fixture.v2.metadata.byte_count);
    let serialized = serde_json::to_value(&checkpoint).unwrap();
    assert_eq!(serialized["publication_admissible"], false);
    assert_eq!(serialized["projection_authority"], "v1_only");
}

#[test]
fn v1_shard_aliases_have_one_frozen_resource_and_bundle_identity() {
    let fixture = PairFixture::new();
    let source_only = fixture.v1();
    let mut alias_only = fixture.v1();
    alias_only.source_shard_id = None;
    alias_only.shard_id = Some("synthetic-a");
    let mut both_equal = fixture.v1();
    both_equal.shard_id = Some("synthetic-a");

    let checkpoints = [source_only, alias_only, both_equal].map(|v1| {
        validate_tax_identity_sidecar_shard(
            "synthetic-a",
            v1,
            &fixture.v2,
            &VecUniverse(fixture.groups.clone()),
        )
        .unwrap()
    });
    assert!(checkpoints.windows(2).all(|pair| pair[0] == pair[1]));
    let resource_identity = checkpoints[0].v1_resource_identity.clone();
    let bundles = checkpoints
        .iter()
        .cloned()
        .map(|checkpoint| finalize_tax_identity_sidecar_bundle(vec![checkpoint]).unwrap())
        .collect::<Vec<_>>();
    assert!(bundles.windows(2).all(|pair| pair[0] == pair[1]));
    let bundle_sha256 = bundles[0].bundle_sha256.clone();
    assert_eq!(
        resource_identity,
        "ptg2-tax-sidecar-resource-v1:d8a61495a3ca38b235688e65170ac09f6557c4528c3781d4d7a2be5913893fbf"
    );
    assert_eq!(
        bundle_sha256,
        "6528b562bb91bd8ca37e9b7755325c4f5c162e07a433e527fccaf3c6773fd271"
    );

    for (source_shard_id, shard_id) in [(None, None), (Some("synthetic-a"), Some("synthetic-b"))] {
        let mut v1 = fixture.v1();
        v1.source_shard_id = source_shard_id;
        v1.shard_id = shard_id;
        let error = validate_tax_identity_sidecar_shard(
            "synthetic-a",
            v1,
            &fixture.v2,
            &VecUniverse(fixture.groups.clone()),
        )
        .unwrap_err();
        assert_eq!(error.to_string(), INVALID_METADATA);
    }
}

#[test]
fn bundle_checkpoint_is_deterministic_and_rejects_duplicate_resources() {
    let fixture = PairFixture::new();
    let first = fixture.validate().unwrap();
    let mut second = first.clone();
    second.shard_id = "synthetic-b".to_owned();
    second.v1_resource_identity = "ptg2-tax-v1:synthetic-b".to_owned();
    second.v2_resource_identity = "ptg2-tax-v2:synthetic-b".to_owned();

    let forward =
        finalize_tax_identity_sidecar_bundle(vec![first.clone(), second.clone()]).unwrap();
    let reverse = finalize_tax_identity_sidecar_bundle(vec![second.clone(), first]).unwrap();
    assert_eq!(forward, reverse);
    assert_eq!(forward.shard_count, 2);

    let mut duplicate_v1 = second.clone();
    duplicate_v1.v1_resource_identity = fixture.validate().unwrap().v1_resource_identity;
    let error =
        finalize_tax_identity_sidecar_bundle(vec![fixture.validate().unwrap(), duplicate_v1])
            .unwrap_err();
    assert_eq!(error.to_string(), DUPLICATE_RESOURCE);

    second.v2_resource_identity = fixture.validate().unwrap().v2_resource_identity;
    let error = finalize_tax_identity_sidecar_bundle(vec![fixture.validate().unwrap(), second])
        .unwrap_err();
    assert_eq!(error.to_string(), DUPLICATE_RESOURCE);
}

#[test]
fn rejects_empty_or_inexact_authoritative_universe() {
    let fixture = PairFixture::new();
    let error = validate_tax_identity_sidecar_shard(
        "synthetic-a",
        fixture.v1(),
        &fixture.v2,
        &VecUniverse(Vec::new()),
    )
    .unwrap_err();
    assert_eq!(error.to_string(), ZERO_ROW_REJECTED);

    let error = validate_tax_identity_sidecar_shard(
        "synthetic-a",
        fixture.v1(),
        &fixture.v2,
        &VecUniverse(fixture.groups[..2].to_vec()),
    )
    .unwrap_err();
    assert_eq!(error.to_string(), UNIVERSE_MISMATCH);

    let mut groups = fixture.groups.clone();
    groups[1] = group(9);
    let error = validate_tax_identity_sidecar_shard(
        "synthetic-a",
        fixture.v1(),
        &fixture.v2,
        &VecUniverse(groups),
    )
    .unwrap_err();
    assert_eq!(error.to_string(), UNIVERSE_MISMATCH);

    let error = validate_tax_identity_sidecar_shard(
        "synthetic-a",
        fixture.v1(),
        &fixture.v2,
        &FailingUniverse {
            groups: fixture.groups.clone(),
            fail_at: 1,
        },
    )
    .unwrap_err();
    assert_eq!(error.to_string(), UNIVERSE_MISMATCH);
    assert!(!error.to_string().contains("sensitive"));
}

#[test]
fn artifact_errors_are_path_free_and_token_free() {
    let fixture = PairFixture::new();
    let mut v2 = fixture.v2.clone();
    v2.metadata.sha256 = "00".repeat(32);
    let error = validate_tax_identity_sidecar_shard(
        "synthetic-a",
        fixture.v1(),
        &v2,
        &VecUniverse(fixture.groups.clone()),
    )
    .unwrap_err();
    let message = error.to_string();
    assert_eq!(message, ARTIFACT_DIGEST_MISMATCH);
    assert!(!message.contains(fixture._temporary.path().to_string_lossy().as_ref()));
    assert!(!message.contains(&encode_hex(&token(0x31))));

    v2.path = fixture._temporary.path().join("sensitive-name.sidecar");
    v2.metadata.sha256 = fixture.v2.metadata.sha256.clone();
    let error = validate_tax_identity_sidecar_shard(
        "synthetic-a",
        fixture.v1(),
        &v2,
        &VecUniverse(fixture.groups.clone()),
    )
    .unwrap_err();
    assert_eq!(error.to_string(), UNAVAILABLE_ARTIFACT);
    assert!(!error.to_string().contains("sensitive-name"));
}

#[test]
fn same_file_descriptors_are_reauthenticated_after_pair_validation() {
    let fixture = PairFixture::new();
    let path = fixture.v2.path.clone();
    let mut mutated = fs::read(&path).unwrap();
    *mutated.last_mut().unwrap() ^= 1;
    let error = validate_tax_identity_sidecar_shard_with_progress(
        "synthetic-a",
        fixture.v1(),
        &fixture.v2,
        &VecUniverse(fixture.groups.clone()),
        |ordinal| {
            if ordinal == 1 {
                fs::write(&path, &mutated)?;
            }
            Ok(())
        },
    )
    .unwrap_err();
    assert_eq!(error.to_string(), ARTIFACT_DIGEST_MISMATCH);
}

#[test]
fn appended_bytes_are_rejected_before_a_checkpoint_can_be_emitted() {
    let fixture = PairFixture::new();
    let path = fixture.v2.path.clone();
    let error = validate_tax_identity_sidecar_shard_with_progress(
        "synthetic-a",
        fixture.v1(),
        &fixture.v2,
        &VecUniverse(fixture.groups.clone()),
        |ordinal| {
            if ordinal == 1 {
                std::fs::OpenOptions::new()
                    .append(true)
                    .open(&path)?
                    .write_all(&vec![0x7b; HASH_BUFFER_BYTES * 2])?;
            }
            Ok(())
        },
    )
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(!error.to_string().contains(path.to_string_lossy().as_ref()));
}

#[test]
fn debug_views_redact_paths_tokens_digests_and_resource_identities() {
    let fixture = PairFixture::new();
    let shard = fixture.validate().unwrap();
    let forbidden = [
        fixture.v1_path.to_string_lossy().into_owned(),
        fixture.v2.path.to_string_lossy().into_owned(),
        fixture.v1_sha256.clone(),
        fixture.v2.metadata.sha256.clone(),
        POLICY.to_owned(),
        shard.v1_resource_identity.clone(),
        shard.v2_resource_identity.clone(),
        shard.shard_id.clone(),
    ];
    let views = [
        format!("{:?}", fixture.v1()),
        format!("{:?}", fixture.v2),
        format!("{shard:?}"),
        format!(
            "{:?}",
            finalize_tax_identity_sidecar_bundle(vec![shard]).unwrap()
        ),
    ];
    for view in views {
        for secret in &forbidden {
            assert!(!view.contains(secret), "debug view exposed protected data");
        }
    }
}

#[test]
fn v2_resource_identity_has_a_frozen_path_independent_vector() {
    let fixture = PairFixture::new();
    let derived =
        derive_tax_identity_sidecar_v2_resource_identity("synthetic-a", &fixture.v2.metadata)
            .unwrap();
    assert_eq!(
        derived,
        "ptg2-tax-sidecar-resource-v1:4d4b02200ce1402b2d9d89d8096a0f533e4d7423bb1cc349b74de9ee0c9ac990"
    );

    let mut relocated = fixture.v2.clone();
    relocated.path = PathBuf::from("/different/location/v2.sidecar");
    assert_eq!(
        derive_tax_identity_sidecar_v2_resource_identity("synthetic-a", &relocated.metadata)
            .unwrap(),
        derived
    );
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

fn group(value: u8) -> [u8; 16] {
    let mut group = [0u8; 16];
    group[15] = value;
    group
}

fn token(value: u8) -> [u8; 32] {
    [value; 32]
}
