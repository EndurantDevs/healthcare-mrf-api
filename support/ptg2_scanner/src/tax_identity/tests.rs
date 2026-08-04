use super::*;
use serde_json::json;
use std::fs;

fn policy() -> TinTokenPolicy {
    let mut secret = [0u8; 32];
    for (index, byte) in secret.iter_mut().enumerate() {
        *byte = index as u8;
    }
    TinTokenPolicy::from_secret("ptg-tin-hmac-sha256-v1:release-1".to_string(), secret).unwrap()
}

#[test]
fn frozen_wire_vector_matches_python_reference() {
    let message = canonical_tin_token_message(b"ein", b"123456789");
    assert_eq!(
        hex(&message),
        "6865616c7468706f7274612e7074672e74696e2e763100000365696e0009313233343536373839"
    );
    let token = policy().token_for_ein(b"123456789");
    assert_eq!(
        hex(&token.tin_hmac_sha256),
        "2b5a279904848d15ed9f42d5afd73341f77ee63e1376521f3f78c94d722993c0"
    );
    assert_eq!(hex(&token.tin_id_128), "2b5a279904848d15ed9f42d5afd73341");
}

#[test]
fn matched_ein_accepts_only_the_shared_raw_input_grammar() {
    for raw_value in ["123456789", "12-3456789", " \t12-3456789\r\n"] {
        let tin = json!({"type": " EIN ", "value": raw_value});
        assert_eq!(
            classify_provider_group_tin(Some(&tin)),
            classified(TaxIdentityState::MatchedEin, Some(*b"123456789"))
        );
    }
    for raw_value in [
        "01💥2345678",
        "12 3456789",
        "1-23456789",
        "12--3456789",
        "12345678",
        "1234567890",
        "12_3456789",
        "１２３４５６７８９",
    ] {
        let tin = json!({"type": "ein", "value": raw_value});
        assert_eq!(
            classify_provider_group_tin(Some(&tin)).state,
            TaxIdentityState::Malformed
        );
    }
}

#[test]
fn classified_tax_identity_debug_redacts_normalized_ein() {
    let raw_ein = "12-3456789";
    let tin = json!({"type": "ein", "value": raw_ein});
    let classified = classify_provider_group_tin(Some(&tin));
    let rendered = format!("{classified:?}");

    assert_eq!(
        classified,
        super::classified(TaxIdentityState::MatchedEin, Some(*b"123456789"))
    );
    assert!(rendered.contains("<redacted>"));
    assert!(!rendered.contains(raw_ein));
    assert!(!rendered.contains("123456789"));
    assert!(!rendered.contains("[49, 50, 51, 52, 53, 54, 55, 56, 57]"));
}

#[test]
fn all_unavailable_states_are_explicit() {
    assert_eq!(
        classify_provider_group_tin(None).state,
        TaxIdentityState::Missing
    );
    for tin in [Value::Null, json!({}), json!({"type": " ", "value": null})] {
        assert_eq!(
            classify_provider_group_tin(Some(&tin)).state,
            TaxIdentityState::Missing
        );
    }
    for tin in [
        json!("ein"),
        json!({"type": "ein"}),
        json!({"type": 1, "value": "123456789"}),
        json!({"type": "ein", "value": false}),
        json!({"type": "ein", "value": ""}),
    ] {
        assert_eq!(
            classify_provider_group_tin(Some(&tin)).state,
            TaxIdentityState::Malformed
        );
    }
    for tin in [
        json!({"type": "ssn", "value": "123456789"}),
        json!({"type": "other", "value": "opaque-value"}),
    ] {
        assert_eq!(
            classify_provider_group_tin(Some(&tin)).state,
            TaxIdentityState::UnsupportedType
        );
    }
}

#[test]
fn duplicate_observations_merge_deterministically() {
    let policy = policy();
    let missing = policy.observe(None);
    let malformed_tin = json!({"type": "ein", "value": "12 3456789"});
    let malformed = policy.observe(Some(&malformed_tin));
    let unsupported_tin = json!({"type": "ssn", "value": "123456789"});
    let unsupported = policy.observe(Some(&unsupported_tin));
    let matched_tin = json!({"type": "ein", "value": "12-3456789"});
    let matched = policy.observe(Some(&matched_tin));
    assert_eq!(
        missing.merge(malformed).unwrap().state,
        TaxIdentityState::Malformed
    );
    assert_eq!(
        malformed.merge(unsupported).unwrap().state,
        TaxIdentityState::UnsupportedType
    );
    assert_eq!(
        unsupported.merge(matched).unwrap().state,
        TaxIdentityState::MatchedEin
    );
    assert_eq!(matched.merge(missing).unwrap(), matched);
}

#[test]
fn conflicting_supported_tokens_fail_closed() {
    let policy = policy();
    let first_tin = json!({"type": "ein", "value": "12-3456789"});
    let second_tin = json!({"type": "ein", "value": "98-7654321"});
    let first = policy.observe(Some(&first_tin));
    let second = policy.observe(Some(&second_tin));
    assert!(first.merge(second).is_err());
}

#[test]
fn business_name_is_not_owner_identity_input() {
    let tin = json!({
        "type": "ein",
        "value": "12-3456789",
        "business_name": {"untrusted": true}
    });
    assert_eq!(
        classify_provider_group_tin(Some(&tin)).state,
        TaxIdentityState::MatchedEin
    );
}

#[test]
fn policy_ids_are_strict_and_bounded() {
    for policy_id in [
        "ptg-tin-hmac-sha256-v1:a",
        "ptg-tin-hmac-sha256-v1:release-1",
        "ptg-tin-hmac-sha256-v1:key_01.prod",
    ] {
        validate_token_policy_id(policy_id).unwrap();
    }
    for policy_id in [
        "",
        "other:a",
        "ptg-tin-hmac-sha256-v1:",
        "ptg-tin-hmac-sha256-v1:-bad",
        "ptg-tin-hmac-sha256-v1:UPPER",
        "ptg-tin-hmac-sha256-v1:bad/slash",
        "ptg-tin-hmac-sha256-v1:abcdefghijklmnopqrstuvwxyz1234567",
    ] {
        assert!(validate_token_policy_id(policy_id).is_err(), "{policy_id}");
    }
}

#[test]
fn invalid_policy_clears_and_rejects_supplied_secret() {
    let error = TinTokenPolicy::from_secret("bad-policy".to_string(), [0x5a; 32]).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(!error.to_string().contains("5a"));
}

#[test]
fn secret_file_contract_rejects_absent_short_and_long_inputs() {
    let temp = tempfile::tempdir().unwrap();
    let absent = temp.path().join("absent");
    assert!(read_tin_token_secret(absent.to_str().unwrap()).is_err());

    for (name, bytes) in [("short", vec![7u8; 31]), ("long", vec![7u8; 33])] {
        let path = temp.path().join(name);
        fs::write(&path, bytes).unwrap();
        let error = read_tin_token_secret(path.to_str().unwrap()).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(!error.to_string().contains(path.to_str().unwrap()));
    }
}

#[test]
fn environment_loader_requires_both_policy_and_exact_raw_secret() {
    let temp = tempfile::tempdir().unwrap();
    let secret_path = temp.path().join("secret");
    fs::write(&secret_path, [0x42; 32]).unwrap();
    env::remove_var(TIN_TOKEN_POLICY_ID_ENV);
    env::remove_var(TIN_TOKEN_SECRET_FILE_ENV);
    assert!(load_tin_token_policy_from_env().is_err());

    env::set_var(TIN_TOKEN_POLICY_ID_ENV, "ptg-tin-hmac-sha256-v1:release-1");
    assert!(load_tin_token_policy_from_env().is_err());
    env::set_var(TIN_TOKEN_SECRET_FILE_ENV, &secret_path);
    let loaded = load_tin_token_policy_from_env().unwrap();
    assert_eq!(loaded.policy_id(), "ptg-tin-hmac-sha256-v1:release-1");
    assert_eq!(
        loaded.token_for_ein(b"123456789").tin_id_128,
        TinTokenPolicy::from_secret("ptg-tin-hmac-sha256-v1:release-1".to_string(), [0x42; 32],)
            .unwrap()
            .token_for_ein(b"123456789")
            .tin_id_128
    );
    env::remove_var(TIN_TOKEN_POLICY_ID_ENV);
    env::remove_var(TIN_TOKEN_SECRET_FILE_ENV);
}

#[test]
fn debug_output_never_contains_secret_bytes() {
    let policy = policy();
    let rendered = format!("{policy:?}");
    assert!(rendered.contains("release-1"));
    assert!(!rendered.contains("[0, 1, 2"));
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
