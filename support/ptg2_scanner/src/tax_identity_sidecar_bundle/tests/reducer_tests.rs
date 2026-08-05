use super::*;

fn independent_shards() -> (
    TaxIdentitySidecarShardCheckpoint,
    TaxIdentitySidecarShardCheckpoint,
) {
    let first = PairFixture::new().validate().unwrap();
    let mut second = first.clone();
    second.shard_id = "synthetic-b".to_owned();
    second.v1_resource_identity = "ptg2-tax-v1:synthetic-b".to_owned();
    second.v2_resource_identity = "ptg2-tax-v2:synthetic-b".to_owned();
    (first, second)
}

fn assert_reducer_error(shards: Vec<TaxIdentitySidecarShardCheckpoint>, expected_message: &str) {
    let error = finalize_tax_identity_sidecar_bundle(shards).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(error.to_string(), expected_message);
}

#[test]
fn reducer_rejects_empty_duplicate_and_mixed_policy_inputs() {
    assert_reducer_error(Vec::new(), ZERO_ROW_REJECTED);

    let (first, _) = independent_shards();
    assert_reducer_error(vec![first.clone(), first], DUPLICATE_SHARD);

    let (first, mut second) = independent_shards();
    second.token_policy_id = "ptg-tin-hmac-sha256-v1:other".to_owned();
    assert_reducer_error(vec![first, second], POLICY_MISMATCH);
}

#[test]
fn reducer_rejects_zero_mismatched_or_internally_inconsistent_counts() {
    let (first, _) = independent_shards();

    let mut zero_authority = first.clone();
    zero_authority.authoritative_provider_group_count = 0;
    assert_reducer_error(vec![zero_authority], ZERO_ROW_REJECTED);

    let mut mismatched_rows = first.clone();
    mismatched_rows.row_count -= 1;
    assert_reducer_error(vec![mismatched_rows], ZERO_ROW_REJECTED);

    let mut invalid_state_total = first;
    invalid_state_total.missing_count -= 1;
    assert_reducer_error(vec![invalid_state_total], INVALID_METADATA);
}

#[test]
fn reducer_rejects_overflow_in_every_summed_counter() {
    type CounterMutation = fn(&mut TaxIdentitySidecarShardCheckpoint);
    let mutations: [CounterMutation; 9] = [
        |shard| shard.authoritative_provider_group_count = u64::MAX,
        |shard| shard.row_count = u64::MAX,
        |shard| shard.matched_ein_count = u64::MAX,
        |shard| shard.matched_npi_count = u64::MAX,
        |shard| shard.missing_count = u64::MAX,
        |shard| shard.malformed_count = u64::MAX,
        |shard| shard.unsupported_type_count = u64::MAX,
        |shard| shard.v1_byte_count = u64::MAX,
        |shard| shard.v2_byte_count = u64::MAX,
    ];

    for mutate in mutations {
        let (mut first, second) = independent_shards();
        mutate(&mut first);
        assert_reducer_error(vec![first, second], COUNT_OVERFLOW);
    }
}

#[test]
fn reducer_reauthenticates_digest_and_bounded_text_fields() {
    for corrupt in [
        |shard: &mut TaxIdentitySidecarShardCheckpoint| shard.v1_sha256 = "g".repeat(64),
        |shard: &mut TaxIdentitySidecarShardCheckpoint| shard.v2_sha256 = "g".repeat(64),
    ] {
        let (mut shard, _) = independent_shards();
        corrupt(&mut shard);
        assert_reducer_error(vec![shard], INVALID_METADATA);
    }

    let (mut shard, _) = independent_shards();
    shard.v1_resource_identity = "r".repeat(MAX_BUNDLE_TEXT_BYTES + 1);
    assert_reducer_error(vec![shard], INVALID_METADATA);
}
