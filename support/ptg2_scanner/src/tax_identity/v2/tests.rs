use super::*;
use crate::tax_identity::{
    canonical_tin_token_message, classify_provider_group_tin, TaxIdentityObservation,
    TaxIdentityState, TinTokenPolicy,
};
use serde_json::{json, Value};
use std::io;

fn policy() -> TinTokenPolicy {
    let mut secret = [0u8; 32];
    for (index, byte) in secret.iter_mut().enumerate() {
        *byte = index as u8;
    }
    TinTokenPolicy::from_secret("ptg-tin-hmac-sha256-v1:release-1".to_string(), secret).unwrap()
}

fn observation(
    state: TaxIdentityStateV2,
    tin_hmac_sha256: Option<[u8; 32]>,
) -> TaxIdentityObservationV2 {
    TaxIdentityObservationV2 {
        state,
        tin_hmac_sha256,
    }
}

fn assert_generic_error(error: io::Error, kind: io::ErrorKind, message: &str, forbidden: &[&str]) {
    assert_eq!(error.kind(), kind);
    let rendered = error.to_string();
    assert_eq!(rendered, message);
    for value in forbidden {
        assert!(!rendered.contains(value), "error exposed {value:?}");
    }
}

#[test]
fn frozen_npi_token_and_locator_use_type_aware_framing() {
    let message = canonical_tin_token_message(b"npi", b"1000000491");
    assert_eq!(
        hex(&message),
        "6865616c7468706f7274612e7074672e74696e2e76310000036e7069000a31303030303030343931"
    );
    assert_ne!(message, canonical_tin_token_message(b"ein", b"1000000491"));

    let token = policy().token_for_npi(b"1000000491").unwrap();
    assert_eq!(
        hex(&token.tin_hmac_sha256),
        "8370f2246a6b7b08abb55f6fc11fd75015467c4270ddeef3f87396ed734e1f73"
    );
    assert_eq!(hex(&token.tin_id_128), "8370f2246a6b7b08abb55f6fc11fd750");
    assert_ne!(token, policy().token_for_ein(b"123456789"));
}

#[test]
fn direct_npi_tokenization_validates_every_input_byte() {
    for valid in [b"1000000491", b"2999999990"] {
        assert!(policy().token_for_npi(valid).is_ok());
    }

    let mut non_ascii = *b"1000000491";
    non_ascii[9] = 0xff;
    for (case, invalid) in [
        ("checksum", *b"1000000492"),
        ("structural-low", *b"0999999999"),
        ("structural-high", *b"3000000000"),
        ("nondigit", *b"100000049x"),
        ("non-ascii", non_ascii),
    ] {
        let byte_array = format!("{invalid:?}");
        let error = policy().token_for_npi(&invalid).unwrap_err();
        assert_generic_error(
            error,
            io::ErrorKind::InvalidInput,
            INVALID_NPI_TOKEN_MESSAGE,
            &[case, &String::from_utf8_lossy(&invalid), &byte_array],
        );
    }
}

#[test]
fn v2_matches_only_checksum_valid_low_and_high_npis() {
    for (tin_type, raw_value, normalized) in [
        ("npi", "1000000491", *b"1000000491"),
        ("NPI", "2999999990", *b"2999999990"),
        (" NpI ", " \t1000000491\r\n", *b"1000000491"),
    ] {
        let tin = json!({"type": tin_type, "value": raw_value});
        let classified = classify_provider_group_tin_v2(Some(&tin));
        assert_eq!(classified.state, TaxIdentityStateV2::MatchedNpi);
        assert_eq!(
            classified.normalized_identity,
            Some(NormalizedTaxIdentity::Npi(normalized))
        );
        let observation = policy().observe_v2(Some(&tin));
        assert_eq!(observation.state, TaxIdentityStateV2::MatchedNpi);
        assert!(observation.tin_hmac_sha256.is_some());
    }
}

#[test]
fn v2_npi_invalidity_classes_are_never_matchable_or_tokenized() {
    for (case, raw_value) in [
        ("checksum", "1000000492"),
        ("structural-low", "0999999999"),
        ("structural-high", "3000000000"),
        ("short", "123"),
        ("nondigit", "100000049x"),
        ("unicode", "１００００００４９１"),
        ("slash", "10/00/0491"),
        ("hyphen", "10000004-1"),
        ("space", "10000 00491"),
    ] {
        let tin = json!({"type": "npi", "value": raw_value});
        assert_eq!(
            classify_provider_group_tin_v2(Some(&tin)),
            classified(TaxIdentityStateV2::Malformed, None),
            "{case}"
        );
        assert_eq!(
            policy().observe_v2(Some(&tin)),
            observation(TaxIdentityStateV2::Malformed, None),
            "{case}"
        );
    }
}

#[test]
fn v2_is_additive_and_preserves_the_ein_only_v1_contract() {
    let npi = json!({"type": "npi", "value": "1000000491"});
    assert_eq!(
        classify_provider_group_tin(Some(&npi)).state,
        TaxIdentityState::UnsupportedType
    );
    assert_eq!(
        policy().observe(Some(&npi)),
        TaxIdentityObservation {
            state: TaxIdentityState::UnsupportedType,
            tin_hmac_sha256: None,
        }
    );

    let ein = json!({"type": " EIN ", "value": "12-3456789"});
    let v1 = policy().observe(Some(&ein));
    let v2 = policy().observe_v2(Some(&ein));
    assert_eq!(v1.state, TaxIdentityState::MatchedEin);
    assert_eq!(v2.state, TaxIdentityStateV2::MatchedEin);
    assert_eq!(v1.tin_hmac_sha256, v2.tin_hmac_sha256);
    assert_eq!(
        classify_provider_group_tin_v2(Some(&ein)).normalized_identity,
        Some(NormalizedTaxIdentity::Ein(*b"123456789"))
    );
    let rendered = format!("{:?}", classify_provider_group_tin_v2(Some(&ein)));
    assert!(rendered.contains("Ein(<redacted>)"));
    assert!(!rendered.contains("12-3456789"));
    assert!(!rendered.contains("[49, 50, 51, 52, 53, 54, 55, 56, 57]"));
}

#[test]
fn all_v2_unavailable_states_are_explicit_and_untokenized() {
    assert_eq!(
        classify_provider_group_tin_v2(None),
        classified(TaxIdentityStateV2::Missing, None)
    );
    for tin in [Value::Null, json!({}), json!({"type": " ", "value": null})] {
        assert_eq!(
            classify_provider_group_tin_v2(Some(&tin)),
            classified(TaxIdentityStateV2::Missing, None)
        );
        assert_eq!(
            policy().observe_v2(Some(&tin)),
            observation(TaxIdentityStateV2::Missing, None)
        );
    }

    for tin in [
        json!("npi"),
        json!({"type": "npi"}),
        json!({"type": 1, "value": "1000000491"}),
        json!({"type": "npi", "value": false}),
        json!({"type": "npi", "value": ""}),
        json!({"type": "ein", "value": "12 3456789"}),
    ] {
        assert_eq!(
            classify_provider_group_tin_v2(Some(&tin)),
            classified(TaxIdentityStateV2::Malformed, None)
        );
        assert_eq!(
            policy().observe_v2(Some(&tin)),
            observation(TaxIdentityStateV2::Malformed, None)
        );
    }

    for tin in [
        json!({"type": "ssn", "value": "1000000491"}),
        json!({"type": "other", "value": "opaque-value"}),
    ] {
        assert_eq!(
            classify_provider_group_tin_v2(Some(&tin)),
            classified(TaxIdentityStateV2::UnsupportedType, None)
        );
        assert_eq!(
            policy().observe_v2(Some(&tin)),
            observation(TaxIdentityStateV2::UnsupportedType, None)
        );
    }
}

#[test]
fn v2_observations_merge_by_availability_without_changing_identity() {
    let missing = observation(TaxIdentityStateV2::Missing, None);
    let malformed = observation(TaxIdentityStateV2::Malformed, None);
    let unsupported = observation(TaxIdentityStateV2::UnsupportedType, None);
    let matched = policy().observe_v2(Some(&json!({
        "type": "npi",
        "value": "1000000491"
    })));

    assert_eq!(missing.merge(malformed).unwrap(), malformed);
    assert_eq!(malformed.merge(unsupported).unwrap(), unsupported);
    assert_eq!(unsupported.merge(matched).unwrap(), matched);
    assert_eq!(matched.merge(missing).unwrap(), matched);
}

#[test]
fn v2_merge_rejects_every_invalid_public_observation_shape() {
    let nonzero_hmac = [7u8; 32];
    let valid = observation(TaxIdentityStateV2::Missing, None);
    let invalid_shapes = [
        observation(TaxIdentityStateV2::MatchedEin, None),
        observation(TaxIdentityStateV2::MatchedNpi, None),
        observation(TaxIdentityStateV2::MatchedEin, Some([0u8; 32])),
        observation(TaxIdentityStateV2::MatchedNpi, Some([0u8; 32])),
        observation(TaxIdentityStateV2::Missing, Some(nonzero_hmac)),
        observation(TaxIdentityStateV2::Malformed, Some(nonzero_hmac)),
        observation(TaxIdentityStateV2::UnsupportedType, Some(nonzero_hmac)),
    ];

    for invalid in invalid_shapes {
        for error in [
            invalid.merge(valid).unwrap_err(),
            valid.merge(invalid).unwrap_err(),
        ] {
            assert_generic_error(
                error,
                io::ErrorKind::InvalidData,
                INVALID_OBSERVATION_MESSAGE,
                &["123456789", "1000000491", "[7, 7, 7"],
            );
        }
    }
}

#[test]
fn v2_merge_rejects_different_matched_states_even_with_equal_hmac() {
    let shared_hmac = [8u8; 32];
    let ein = observation(TaxIdentityStateV2::MatchedEin, Some(shared_hmac));
    let npi = observation(TaxIdentityStateV2::MatchedNpi, Some(shared_hmac));

    for error in [ein.merge(npi).unwrap_err(), npi.merge(ein).unwrap_err()] {
        assert_generic_error(
            error,
            io::ErrorKind::InvalidData,
            CONFLICTING_OBSERVATION_MESSAGE,
            &["123456789", "1000000491", "[8, 8, 8"],
        );
    }
}

#[test]
fn v2_merge_rejects_different_full_hmacs_and_accepts_exact_duplicates() {
    for state in [
        TaxIdentityStateV2::MatchedEin,
        TaxIdentityStateV2::MatchedNpi,
    ] {
        let first = observation(state, Some([1u8; 32]));
        let second = observation(state, Some([2u8; 32]));
        let error = first.merge(second).unwrap_err();
        assert_generic_error(
            error,
            io::ErrorKind::InvalidData,
            CONFLICTING_OBSERVATION_MESSAGE,
            &["123456789", "1000000491", "[1, 1, 1", "[2, 2, 2"],
        );
        assert_eq!(first.merge(first).unwrap(), first);
    }
}

#[test]
fn natural_v2_conflict_and_debug_output_never_expose_raw_values() {
    let ein_value = "12-3456789";
    let npi_value = "1000000491";
    let ein = json!({"type": "ein", "value": ein_value});
    let npi = json!({"type": "npi", "value": npi_value});
    let error = policy()
        .observe_v2(Some(&ein))
        .merge(policy().observe_v2(Some(&npi)))
        .unwrap_err();
    assert_generic_error(
        error,
        io::ErrorKind::InvalidData,
        CONFLICTING_OBSERVATION_MESSAGE,
        &[ein_value, npi_value],
    );

    let rendered = format!("{:?}", classify_provider_group_tin_v2(Some(&npi)));
    assert!(rendered.contains("Npi(<redacted>)"));
    assert!(!rendered.contains(npi_value));
    assert!(!rendered.contains("[49, 48, 48, 48, 48, 48, 48, 52, 57, 49]"));
}

#[test]
fn business_name_is_not_v2_identity_or_token_input() {
    let without_name = json!({"type": "npi", "value": "1000000491"});
    let with_name = json!({
        "type": "npi",
        "value": "1000000491",
        "business_name": {"untrusted": ["synthetic", 7]}
    });
    assert_eq!(
        classify_provider_group_tin_v2(Some(&without_name)),
        classify_provider_group_tin_v2(Some(&with_name))
    );
    assert_eq!(
        policy().observe_v2(Some(&without_name)),
        policy().observe_v2(Some(&with_name))
    );
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
