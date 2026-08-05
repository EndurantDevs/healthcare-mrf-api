use super::*;

#[test]
fn rejects_typed_metadata_contract_count_and_size_drift() {
    let fixture = PairFixture::new();
    let mutations: [fn(&mut TaxIdentitySidecarV2Metadata); 8] = [
        |metadata| metadata.record_format.push_str("-changed"),
        |metadata| metadata.normalization_contract.push_str("-changed"),
        |metadata| metadata.token_message_contract.push_str("-changed"),
        |metadata| metadata.hmac_contract.push_str("-changed"),
        |metadata| metadata.tin_id_128_contract.push_str("-changed"),
        |metadata| metadata.full_hmac_authority_contract.push_str("-changed"),
        |metadata| metadata.matched_npi_count += 1,
        |metadata| metadata.source_shard_id = "synthetic-b".to_owned(),
    ];
    for mutate in mutations {
        let mut v2 = fixture.v2.clone();
        mutate(&mut v2.metadata);
        let error = validate_tax_identity_sidecar_shard(
            "synthetic-a",
            fixture.v1(),
            &v2,
            &VecUniverse(fixture.groups.clone()),
        )
        .unwrap_err();
        assert_eq!(error.to_string(), INVALID_METADATA);
    }

    let mut wrong_size = fixture.v2.clone();
    wrong_size.metadata.byte_count += 1;
    let error = validate_tax_identity_sidecar_shard(
        "synthetic-a",
        fixture.v1(),
        &wrong_size,
        &VecUniverse(fixture.groups.clone()),
    )
    .unwrap_err();
    assert_eq!(error.to_string(), ARTIFACT_SIZE_MISMATCH);

    let mut uppercase_digest = fixture.v2.clone();
    uppercase_digest.metadata.sha256 = uppercase_digest.metadata.sha256.to_ascii_uppercase();
    let error = validate_tax_identity_sidecar_shard(
        "synthetic-a",
        fixture.v1(),
        &uppercase_digest,
        &VecUniverse(fixture.groups.clone()),
    )
    .unwrap_err();
    assert_eq!(error.to_string(), INVALID_METADATA);

    let mut v1 = fixture.v1();
    v1.source_shard_id = Some(" synthetic-a");
    let error = validate_tax_identity_sidecar_shard(
        "synthetic-a",
        v1,
        &fixture.v2,
        &VecUniverse(fixture.groups.clone()),
    )
    .unwrap_err();
    assert_eq!(error.to_string(), INVALID_METADATA);

    let mut compensated_count_drift = fixture.v2.clone();
    compensated_count_drift.metadata.matched_npi_count -= 1;
    compensated_count_drift.metadata.missing_count += 1;
    let error = validate_tax_identity_sidecar_shard(
        "synthetic-a",
        fixture.v1(),
        &compensated_count_drift,
        &VecUniverse(fixture.groups.clone()),
    )
    .unwrap_err();
    assert_eq!(error.to_string(), INVALID_METADATA);

    let mut compensated_v1_drift = fixture.v1();
    compensated_v1_drift.unsupported_type_count -= 1;
    compensated_v1_drift.malformed_count += 1;
    let error = validate_tax_identity_sidecar_shard(
        "synthetic-a",
        compensated_v1_drift,
        &fixture.v2,
        &VecUniverse(fixture.groups.clone()),
    )
    .unwrap_err();
    assert_eq!(error.to_string(), INVALID_METADATA);

    let other_policy = "ptg-tin-hmac-sha256-v1:other";
    let mut policy_mismatch = fixture.v2.clone();
    policy_mismatch.metadata.token_policy_id = other_policy.to_owned();
    let error = validate_tax_identity_sidecar_shard(
        "synthetic-a",
        fixture.v1(),
        &policy_mismatch,
        &VecUniverse(fixture.groups.clone()),
    )
    .unwrap_err();
    assert_eq!(error.to_string(), POLICY_MISMATCH);

    let mut v1 = fixture.v1();
    v1.token_policy_id = other_policy;
    let error = validate_tax_identity_sidecar_shard(
        "synthetic-a",
        v1,
        &policy_mismatch,
        &VecUniverse(fixture.groups.clone()),
    )
    .unwrap_err();
    assert_eq!(error.to_string(), POLICY_MISMATCH);

    let invalid_policy = "ptg-tin-hmac-sha256-v1:UPPER";
    let mut invalid_policy_v1 = fixture.v1();
    invalid_policy_v1.token_policy_id = invalid_policy;
    let mut invalid_policy_v2 = fixture.v2.clone();
    invalid_policy_v2.metadata.token_policy_id = invalid_policy.to_owned();
    let error = validate_tax_identity_sidecar_shard(
        "synthetic-a",
        invalid_policy_v1,
        &invalid_policy_v2,
        &VecUniverse(fixture.groups.clone()),
    )
    .unwrap_err();
    assert_eq!(error.to_string(), INVALID_METADATA);

    let invalid_digest = "g".repeat(64);
    let mut v1 = fixture.v1();
    v1.sha256 = &invalid_digest;
    let error = validate_tax_identity_sidecar_shard(
        "synthetic-a",
        v1,
        &fixture.v2,
        &VecUniverse(fixture.groups.clone()),
    )
    .unwrap_err();
    assert_eq!(error.to_string(), INVALID_METADATA);

    let mut invalid_v2_digest = fixture.v2.clone();
    invalid_v2_digest.metadata.sha256 = invalid_digest;
    let error = validate_tax_identity_sidecar_shard(
        "synthetic-a",
        fixture.v1(),
        &invalid_v2_digest,
        &VecUniverse(fixture.groups.clone()),
    )
    .unwrap_err();
    assert_eq!(error.to_string(), INVALID_METADATA);
}

#[test]
fn v2_descriptor_denies_unknown_contract_fields() {
    let fixture = PairFixture::new();
    let mut value = serde_json::to_value(&fixture.v2).unwrap();
    value
        .as_object_mut()
        .unwrap()
        .insert("unexpected".to_owned(), serde_json::Value::Bool(true));
    assert!(serde_json::from_value::<TaxIdentitySidecarV2ArtifactDescriptor>(value).is_err());

    let mut value = serde_json::to_value(&fixture.v2).unwrap();
    value["metadata"]
        .as_object_mut()
        .unwrap()
        .insert("unexpected".to_owned(), serde_json::Value::Bool(true));
    assert!(serde_json::from_value::<TaxIdentitySidecarV2ArtifactDescriptor>(value).is_err());
}

#[test]
fn public_shard_validation_rejects_empty_or_untrimmed_ids() {
    let fixture = PairFixture::new();
    for shard_id in ["", " synthetic-a", "synthetic-a "] {
        let mut v1 = fixture.v1();
        v1.source_shard_id = Some(shard_id);
        let mut v2 = fixture.v2.clone();
        v2.metadata.source_shard_id = shard_id.to_owned();

        let error = validate_tax_identity_sidecar_shard(
            shard_id,
            v1,
            &v2,
            &VecUniverse(fixture.groups.clone()),
        )
        .unwrap_err();
        assert_eq!(error.to_string(), INVALID_METADATA);
    }

    let maximum = "s".repeat(MAX_SHARD_ID_BYTES);
    let mut v1 = fixture.v1();
    v1.source_shard_id = Some(&maximum);
    let mut v2 = fixture.v2.clone();
    v2.metadata.source_shard_id = maximum.clone();
    validate_tax_identity_sidecar_shard(&maximum, v1, &v2, &VecUniverse(fixture.groups.clone()))
        .unwrap();

    let oversized = "s".repeat(MAX_SHARD_ID_BYTES + 1);
    let mut v1 = fixture.v1();
    v1.source_shard_id = Some(&oversized);
    v2.metadata.source_shard_id = oversized.clone();
    let error = validate_tax_identity_sidecar_shard(
        &oversized,
        v1,
        &v2,
        &VecUniverse(fixture.groups.clone()),
    )
    .unwrap_err();
    assert_eq!(error.to_string(), INVALID_METADATA);
}
