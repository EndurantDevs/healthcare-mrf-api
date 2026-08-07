use super::*;
use crate::tax_identity_sidecar_bundle::{
    TaxIdentitySidecarBundleCheckpoint, TaxIdentitySidecarV2ArtifactDescriptor,
};
use sha2::{Digest, Sha256};

#[test]
fn full_hmac_cross_type_failure_precedes_an_earlier_locator_failure() {
    let first = token(0x31);
    let mut second = first;
    second[31] = 0x32;
    let fixture = BundleFixture::new(vec![vec![
        (TaxIdentityStateV2::MatchedEin, Some(first)),
        (TaxIdentityStateV2::MatchedEin, Some(second)),
        (TaxIdentityStateV2::MatchedNpi, Some(second)),
    ]]);

    let error = audit_fixture(&fixture, "combined-collisions").unwrap_err();

    assert_eq!(error.to_string(), FULL_HMAC_CROSS_TYPE_COLLISION);
}

#[test]
fn source_checkpoint_contract_and_released_bytes_are_frozen() {
    let fixture = BundleFixture::new(vec![vec![
        (TaxIdentityStateV2::MatchedEin, Some(token(0x21))),
        (TaxIdentityStateV2::MatchedEin, Some(token(0x21))),
    ]]);
    let source_json = serde_json::to_vec(&fixture.checkpoint).unwrap();

    assert_eq!(
        fixture.checkpoint.bundle_sha256(),
        "3ab130b5361d7bd78648045ba69c4fe0f61d3f49a09048e794e382fd665e6335"
    );
    assert_eq!(source_json.len(), 1271);
    assert_eq!(
        encode_hex(&Sha256::digest(&source_json)),
        "89e3c80aafc24dc1916dfad2053b3afc2ef2662d5d1f783afd71d4dfcb532e86"
    );

    let source_before = source_json;
    audit_fixture(&fixture, "released-bytes").unwrap();
    assert_eq!(
        serde_json::to_vec(&fixture.checkpoint).unwrap(),
        source_before
    );
}

#[test]
fn source_checkpoint_single_field_tampering_fails_closed() {
    let fixture = BundleFixture::new(vec![vec![(
        TaxIdentityStateV2::MatchedEin,
        Some(token(0x41)),
    )]]);
    let mutations: [fn(&mut TaxIdentitySidecarBundleCheckpoint); 12] = [
        |value| value.contract.push_str("-changed"),
        |value| value.publication_admissible = true,
        |value| value.projection_authority = "v2".to_owned(),
        |value| value.cross_row_full_hmac_type_collision_check = "passed".to_owned(),
        |value| value.shard_count += 1,
        |value| value.authoritative_provider_group_count += 1,
        |value| value.row_count += 1,
        |value| value.matched_ein_count += 1,
        |value| value.v1_byte_count += 1,
        |value| value.v2_byte_count += 1,
        |value| value.bundle_sha256 = "00".repeat(32),
        |value| value.shards[0].v2_resource_identity.push_str("-changed"),
    ];

    for (index, mutate) in mutations.into_iter().enumerate() {
        let mut checkpoint = fixture.checkpoint.clone();
        mutate(&mut checkpoint);
        let scratch = fixture.scratch_root(&format!("checkpoint-tamper-{index}"));
        let error = audit_tax_identity_sidecar_bundle(
            &checkpoint,
            &fixture.descriptors,
            &config(scratch.clone(), 1_000_000, 2, 6),
        )
        .unwrap_err();
        assert_eq!(error.to_string(), artifacts::INVALID_BASE_CHECKPOINT);
        assert!(directory_is_empty(&scratch));
    }
}

#[test]
fn descriptor_resource_metadata_single_field_tampering_fails_closed() {
    let fixture = BundleFixture::new(vec![vec![(
        TaxIdentityStateV2::MatchedNpi,
        Some(token(0x51)),
    )]]);
    let mutations: [fn(&mut TaxIdentitySidecarV2ArtifactDescriptor); 10] = [
        |value| value.metadata.source_shard_id.push_str("-changed"),
        |value| value.metadata.token_policy_id.push_str("-changed"),
        |value| value.metadata.row_count += 1,
        |value| value.metadata.provider_group_count += 1,
        |value| value.metadata.matched_npi_count += 1,
        |value| value.metadata.byte_count += 1,
        |value| value.metadata.sha256 = "00".repeat(32),
        |value| value.metadata.record_format.push_str("-changed"),
        |value| value.metadata.version += 1,
        |value| {
            value
                .metadata
                .full_hmac_authority_contract
                .push_str("-changed")
        },
    ];

    for (index, mutate) in mutations.into_iter().enumerate() {
        let mut descriptors = fixture.descriptors.clone();
        mutate(&mut descriptors[0]);
        let scratch = fixture.scratch_root(&format!("descriptor-tamper-{index}"));
        let error = audit_tax_identity_sidecar_bundle(
            &fixture.checkpoint,
            &descriptors,
            &config(scratch.clone(), 1_000_000, 2, 6),
        )
        .unwrap_err();
        assert_eq!(error.to_string(), artifacts::ARTIFACT_SET_MISMATCH);
        assert!(directory_is_empty(&scratch));
    }
}

#[test]
fn public_contracts_and_checkpoint_accessors_are_externally_nameable() {
    let fixture = BundleFixture::new(vec![vec![(
        TaxIdentityStateV2::MatchedEin,
        Some(token(0x61)),
    )]]);
    let result = audit_fixture(&fixture, "public-contracts").unwrap();
    external_consumer::assert_contract(result.checkpoint());
}

mod external_consumer {
    use crate::tax_identity_sidecar_bundle::{
        TaxIdentitySidecarAuditedBundleCheckpoint,
        TAX_IDENTITY_COLLISION_AUDIT_CHECKPOINT_CONTRACT,
        TAX_IDENTITY_COLLISION_AUDIT_OCCURRENCE_DIGEST_CONTRACT,
        TAX_IDENTITY_COLLISION_AUDIT_RECORD_BYTES, TAX_IDENTITY_COLLISION_AUDIT_RECORD_CONTRACT,
        TAX_IDENTITY_FULL_HMAC_TYPE_COLLISION_POLICY, TAX_IDENTITY_LOCATOR_COLLISION_POLICY,
        TAX_IDENTITY_MULTI_CANDIDATE_LOCATOR_SUPPORT, TAX_IDENTITY_SAME_IDENTITY_REPETITION_POLICY,
    };

    pub(super) fn assert_contract(checkpoint: &TaxIdentitySidecarAuditedBundleCheckpoint) {
        assert_eq!(
            checkpoint.contract(),
            TAX_IDENTITY_COLLISION_AUDIT_CHECKPOINT_CONTRACT
        );
        assert_eq!(
            checkpoint.record_contract(),
            TAX_IDENTITY_COLLISION_AUDIT_RECORD_CONTRACT
        );
        assert_eq!(
            checkpoint.occurrence_digest_contract(),
            TAX_IDENTITY_COLLISION_AUDIT_OCCURRENCE_DIGEST_CONTRACT
        );
        assert_eq!(
            checkpoint.locator_collision_policy(),
            TAX_IDENTITY_LOCATOR_COLLISION_POLICY
        );
        assert_eq!(
            checkpoint.full_hmac_type_collision_policy(),
            TAX_IDENTITY_FULL_HMAC_TYPE_COLLISION_POLICY
        );
        assert_eq!(
            checkpoint.same_identity_repetition_policy(),
            TAX_IDENTITY_SAME_IDENTITY_REPETITION_POLICY
        );
        assert_eq!(
            checkpoint.multi_candidate_locator_support(),
            TAX_IDENTITY_MULTI_CANDIDATE_LOCATOR_SUPPORT
        );
        assert_eq!(TAX_IDENTITY_COLLISION_AUDIT_RECORD_BYTES, 33);
    }
}
