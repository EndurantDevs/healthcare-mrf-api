use super::*;
use crate::tax_identity_sidecar_v1::{TaxIdentitySidecarV1Header, TaxIdentitySidecarV1Record};
use crate::tax_identity_sidecar_v2::{TaxIdentitySidecarV2Header, TaxIdentitySidecarV2Record};
use std::io::Cursor;

const POLICY_ID: &str = "ptg-tin-hmac-sha256-v1:pair-test";

fn v1_record(
    group: [u8; 16],
    state: TaxIdentityState,
    hmac_byte: u8,
) -> TaxIdentitySidecarV1Record {
    let (locator, hmac) = if state == TaxIdentityState::MatchedEin {
        ([hmac_byte; 16], [hmac_byte; 32])
    } else {
        ([0; 16], [0; 32])
    };
    TaxIdentitySidecarV1Record::new(group, state, locator, hmac).unwrap()
}

fn v2_record(
    group: [u8; 16],
    state: TaxIdentityStateV2,
    hmac_byte: u8,
) -> TaxIdentitySidecarV2Record {
    let (locator, hmac) = if matches!(
        state,
        TaxIdentityStateV2::MatchedEin | TaxIdentityStateV2::MatchedNpi
    ) {
        ([hmac_byte; 16], [hmac_byte; 32])
    } else {
        ([0; 16], [0; 32])
    };
    TaxIdentitySidecarV2Record::new(group, state, locator, hmac).unwrap()
}

fn v1_sidecar(policy_id: &str, records: &[TaxIdentitySidecarV1Record]) -> Vec<u8> {
    let mut encoded = TaxIdentitySidecarV1Header::new(policy_id.to_owned())
        .unwrap()
        .encode();
    for record in records {
        encoded.extend_from_slice(&record.encode());
    }
    encoded
}

fn v2_sidecar(policy_id: &str, records: &[TaxIdentitySidecarV2Record]) -> Vec<u8> {
    let mut encoded = TaxIdentitySidecarV2Header::new(policy_id.to_owned())
        .unwrap()
        .encode();
    for record in records {
        encoded.extend_from_slice(&record.encode());
    }
    encoded
}

fn validator(
    v1: &[TaxIdentitySidecarV1Record],
    v2: &[TaxIdentitySidecarV2Record],
    record_limit: u64,
) -> TaxIdentitySidecarPairValidator<Cursor<Vec<u8>>, Cursor<Vec<u8>>> {
    TaxIdentitySidecarPairValidator::new(
        Cursor::new(v1_sidecar(POLICY_ID, v1)),
        Cursor::new(v2_sidecar(POLICY_ID, v2)),
        record_limit,
    )
    .unwrap()
}

fn invalid_data<T>(result: io::Result<T>) -> io::Error {
    let error = result.err().expect("expected fail-closed pair validation");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    error
}

fn allowed_transition(v1: TaxIdentityState, v2: TaxIdentityStateV2) -> bool {
    matches!(
        (v1, v2),
        (TaxIdentityState::MatchedEin, TaxIdentityStateV2::MatchedEin)
            | (TaxIdentityState::Missing, TaxIdentityStateV2::Missing)
            | (TaxIdentityState::Malformed, TaxIdentityStateV2::Malformed)
            | (
                TaxIdentityState::UnsupportedType,
                TaxIdentityStateV2::MatchedNpi
                    | TaxIdentityStateV2::Malformed
                    | TaxIdentityStateV2::UnsupportedType
            )
    )
}

#[test]
fn frozen_allowed_rows_produce_checked_five_state_summary() {
    let v1 = [
        v1_record([1; 16], TaxIdentityState::MatchedEin, 0x31),
        v1_record([2; 16], TaxIdentityState::Missing, 0),
        v1_record([3; 16], TaxIdentityState::Malformed, 0),
        v1_record([4; 16], TaxIdentityState::UnsupportedType, 0),
        v1_record([5; 16], TaxIdentityState::UnsupportedType, 0),
        v1_record([6; 16], TaxIdentityState::UnsupportedType, 0),
    ];
    let v2 = [
        v2_record([1; 16], TaxIdentityStateV2::MatchedEin, 0x31),
        v2_record([2; 16], TaxIdentityStateV2::Missing, 0),
        v2_record([3; 16], TaxIdentityStateV2::Malformed, 0),
        v2_record([4; 16], TaxIdentityStateV2::MatchedNpi, 0x41),
        v2_record([5; 16], TaxIdentityStateV2::Malformed, 0),
        v2_record([6; 16], TaxIdentityStateV2::UnsupportedType, 0),
    ];
    let mut pair = validator(&v1, &v2, 6);
    assert_eq!(pair.policy_id(), POLICY_ID);
    assert_eq!(pair.records_validated(), 0);
    assert_eq!(pair.validated_summary(), None);

    let first = pair.next_record().unwrap().unwrap();
    assert_eq!(first.v1(), &v1[0]);
    assert_eq!(first.v2(), &v2[0]);
    let debug = format!("{first:?}");
    assert!(debug.contains("<opaque>"));
    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains("3131"));

    let summary = pair.validate_to_end().unwrap();
    assert_eq!(summary.row_count(), 6);
    assert_eq!(summary.matched_ein_count(), 1);
    assert_eq!(summary.matched_npi_count(), 1);
    assert_eq!(summary.missing_count(), 1);
    assert_eq!(summary.malformed_count(), 2);
    assert_eq!(summary.unsupported_type_count(), 1);
    assert_eq!(pair.records_validated(), 6);
    assert_eq!(pair.validated_summary(), Some(summary));
    assert!(pair.next_record().unwrap().is_none());
}

#[test]
fn empty_pair_is_complete_and_has_a_zero_summary() {
    let mut pair = validator(&[], &[], 0);
    let summary = pair.validate_to_end().unwrap();
    assert_eq!(summary, TaxIdentitySidecarPairSummary::default());
    assert_eq!(pair.validated_summary(), Some(summary));
}

#[test]
fn complete_transition_matrix_accepts_only_the_frozen_rules() {
    let v1_states = [
        TaxIdentityState::MatchedEin,
        TaxIdentityState::Missing,
        TaxIdentityState::Malformed,
        TaxIdentityState::UnsupportedType,
    ];
    let v2_states = [
        TaxIdentityStateV2::MatchedEin,
        TaxIdentityStateV2::Missing,
        TaxIdentityStateV2::Malformed,
        TaxIdentityStateV2::UnsupportedType,
        TaxIdentityStateV2::MatchedNpi,
    ];
    for v1_state in v1_states {
        for v2_state in v2_states {
            let v1 = [v1_record([1; 16], v1_state, 0x51)];
            let v2 = [v2_record([1; 16], v2_state, 0x51)];
            let mut pair = validator(&v1, &v2, 1);
            if allowed_transition(v1_state, v2_state) {
                assert_eq!(pair.validate_to_end().unwrap().row_count(), 1);
            } else {
                assert!(invalid_data(pair.next_record())
                    .to_string()
                    .contains("transition is invalid"));
                assert!(invalid_data(pair.next_record())
                    .to_string()
                    .contains("poisoned"));
            }
        }
    }
}

#[test]
fn matched_ein_requires_exact_full_hmac_and_locator_parity() {
    let locator = [0x61; 16];
    let v1_hmac = [0x61; 32];
    let mut v2_hmac = v1_hmac;
    v2_hmac[31] = 0x62;
    let v1 =
        [
            TaxIdentitySidecarV1Record::new(
                [1; 16],
                TaxIdentityState::MatchedEin,
                locator,
                v1_hmac,
            )
            .unwrap(),
        ];
    let v2 = [TaxIdentitySidecarV2Record::new(
        [1; 16],
        TaxIdentityStateV2::MatchedEin,
        locator,
        v2_hmac,
    )
    .unwrap()];
    let mut pair = validator(&v1, &v2, 1);
    assert!(invalid_data(pair.next_record())
        .to_string()
        .contains("transition is invalid"));
    assert!(invalid_data(pair.next_record())
        .to_string()
        .contains("poisoned"));
}

#[test]
fn constructor_requires_the_exact_same_validated_policy() {
    let v1 = Cursor::new(v1_sidecar(
        POLICY_ID,
        &[v1_record([1; 16], TaxIdentityState::Missing, 0)],
    ));
    let v2 = Cursor::new(v2_sidecar(
        "ptg-tin-hmac-sha256-v1:other",
        &[v2_record([1; 16], TaxIdentityStateV2::Missing, 0)],
    ));
    let error = TaxIdentitySidecarPairValidator::new(v1, v2, 1)
        .err()
        .unwrap();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("policies do not match"));
    assert!(!error.to_string().contains(POLICY_ID));
    assert!(!error.to_string().contains("other"));
}

#[test]
fn exact_group_id_is_required_at_every_ordinal() {
    let v1 = [v1_record([1; 16], TaxIdentityState::Missing, 0)];
    let v2 = [v2_record([2; 16], TaxIdentityStateV2::Missing, 0)];
    let mut pair = validator(&v1, &v2, 1);
    assert!(invalid_data(pair.next_record())
        .to_string()
        .contains("group ids do not match"));
    assert!(invalid_data(pair.next_record())
        .to_string()
        .contains("poisoned"));
}

#[test]
fn each_stream_must_be_independently_strictly_ordered() {
    let v1_duplicate = [
        v1_record([1; 16], TaxIdentityState::Missing, 0),
        v1_record([1; 16], TaxIdentityState::Missing, 0),
    ];
    let v2_ordered = [
        v2_record([1; 16], TaxIdentityStateV2::Missing, 0),
        v2_record([2; 16], TaxIdentityStateV2::Missing, 0),
    ];
    let mut v1_invalid = validator(&v1_duplicate, &v2_ordered, 2);
    assert!(v1_invalid.next_record().unwrap().is_some());
    assert!(invalid_data(v1_invalid.next_record())
        .to_string()
        .contains("strictly increasing"));
    assert!(invalid_data(v1_invalid.next_record())
        .to_string()
        .contains("poisoned"));

    let v1_ordered = [
        v1_record([1; 16], TaxIdentityState::Missing, 0),
        v1_record([2; 16], TaxIdentityState::Missing, 0),
    ];
    let v2_duplicate = [
        v2_record([1; 16], TaxIdentityStateV2::Missing, 0),
        v2_record([1; 16], TaxIdentityStateV2::Missing, 0),
    ];
    let mut v2_invalid = validator(&v1_ordered, &v2_duplicate, 2);
    assert!(v2_invalid.next_record().unwrap().is_some());
    assert!(invalid_data(v2_invalid.next_record())
        .to_string()
        .contains("strictly increasing"));
    assert!(invalid_data(v2_invalid.next_record())
        .to_string()
        .contains("poisoned"));
}

#[test]
fn missing_or_extra_rows_fail_in_both_directions() {
    let v1 = [v1_record([1; 16], TaxIdentityState::Missing, 0)];
    let v2 = [v2_record([1; 16], TaxIdentityStateV2::Missing, 0)];
    for (left, right) in [(v1.as_slice(), &[][..]), (&[][..], v2.as_slice())] {
        let mut pair = validator(left, right, 1);
        assert!(invalid_data(pair.next_record())
            .to_string()
            .contains("row counts do not match"));
        assert!(invalid_data(pair.next_record())
            .to_string()
            .contains("poisoned"));
    }
}

#[test]
fn record_limit_and_partial_rows_fail_closed_and_poison() {
    let v1 = [
        v1_record([1; 16], TaxIdentityState::Missing, 0),
        v1_record([2; 16], TaxIdentityState::Missing, 0),
    ];
    let v2 = [
        v2_record([1; 16], TaxIdentityStateV2::Missing, 0),
        v2_record([2; 16], TaxIdentityStateV2::Missing, 0),
    ];
    let mut limited = validator(&v1, &v2, 1);
    assert!(limited.next_record().unwrap().is_some());
    assert!(invalid_data(limited.next_record())
        .to_string()
        .contains("limit exceeded"));
    assert!(invalid_data(limited.next_record())
        .to_string()
        .contains("poisoned"));

    let v1_full = v1_sidecar(POLICY_ID, &v1[..1]);
    let v2_full = v2_sidecar(POLICY_ID, &v2[..1]);
    for partial_length in 1..65 {
        let mut pair = TaxIdentitySidecarPairValidator::new(
            Cursor::new(v1_full[..v1_full.len() - 65 + partial_length].to_vec()),
            Cursor::new(v2_full.clone()),
            1,
        )
        .unwrap();
        assert!(invalid_data(pair.next_record())
            .to_string()
            .contains("truncated"));
        assert!(invalid_data(pair.next_record())
            .to_string()
            .contains("poisoned"));

        let mut pair = TaxIdentitySidecarPairValidator::new(
            Cursor::new(v1_full.clone()),
            Cursor::new(v2_full[..v2_full.len() - 65 + partial_length].to_vec()),
            1,
        )
        .unwrap();
        assert!(invalid_data(pair.next_record())
            .to_string()
            .contains("truncated"));
        assert!(invalid_data(pair.next_record())
            .to_string()
            .contains("poisoned"));
    }
}

#[test]
fn invalid_record_shape_from_either_stream_is_permanent() {
    let v1 = [v1_record([1; 16], TaxIdentityState::Missing, 0)];
    let v2 = [v2_record([1; 16], TaxIdentityStateV2::Missing, 0)];
    let mut invalid_v1 = v1_sidecar(POLICY_ID, &v1);
    let v1_record_start = invalid_v1.len() - 65;
    invalid_v1[v1_record_start + 17] = 1;
    let mut pair = TaxIdentitySidecarPairValidator::new(
        Cursor::new(invalid_v1),
        Cursor::new(v2_sidecar(POLICY_ID, &v2)),
        1,
    )
    .unwrap();
    assert!(invalid_data(pair.next_record())
        .to_string()
        .contains("record is invalid"));
    assert!(invalid_data(pair.next_record())
        .to_string()
        .contains("poisoned"));

    let mut invalid_v2 = v2_sidecar(POLICY_ID, &v2);
    let v2_record_start = invalid_v2.len() - 65;
    invalid_v2[v2_record_start + 33] = 1;
    let mut pair = TaxIdentitySidecarPairValidator::new(
        Cursor::new(v1_sidecar(POLICY_ID, &v1)),
        Cursor::new(invalid_v2),
        1,
    )
    .unwrap();
    assert!(invalid_data(pair.next_record())
        .to_string()
        .contains("record is invalid"));
    assert!(invalid_data(pair.next_record())
        .to_string()
        .contains("poisoned"));
}

#[test]
fn checked_summary_rejects_overflow_and_internal_inconsistency() {
    assert!(invalid_data(checked_increment(u64::MAX))
        .to_string()
        .contains("count overflow"));

    let mut overflow = TaxIdentitySidecarPairSummary {
        row_count: u64::MAX,
        ..TaxIdentitySidecarPairSummary::default()
    };
    let before = overflow;
    assert!(invalid_data(overflow.observe(TaxIdentityStateV2::Missing))
        .to_string()
        .contains("count overflow"));
    assert_eq!(overflow, before);

    let total_overflow = TaxIdentitySidecarPairSummary {
        row_count: u64::MAX,
        matched_ein_count: u64::MAX,
        matched_npi_count: 1,
        ..TaxIdentitySidecarPairSummary::default()
    };
    assert!(invalid_data(total_overflow.validate_total())
        .to_string()
        .contains("count overflow"));

    let wrong_total = TaxIdentitySidecarPairSummary {
        row_count: 1,
        ..TaxIdentitySidecarPairSummary::default()
    };
    assert!(invalid_data(wrong_total.validate_total())
        .to_string()
        .contains("state total is invalid"));
}

#[test]
fn finalization_rechecks_reader_counts_and_state_total() {
    let v1 = [v1_record([1; 16], TaxIdentityState::Missing, 0)];
    let v2 = [v2_record([1; 16], TaxIdentityStateV2::Missing, 0)];

    let mut count_drift = validator(&v1, &v2, 1);
    assert!(count_drift.next_record().unwrap().is_some());
    count_drift.summary.row_count = 0;
    assert!(invalid_data(count_drift.next_record())
        .to_string()
        .contains("row counts do not match"));
    assert!(invalid_data(count_drift.next_record())
        .to_string()
        .contains("poisoned"));

    let mut state_drift = validator(&v1, &v2, 1);
    assert!(state_drift.next_record().unwrap().is_some());
    state_drift.summary.missing_count = 0;
    assert!(invalid_data(state_drift.next_record())
        .to_string()
        .contains("state total is invalid"));
    assert!(invalid_data(state_drift.next_record())
        .to_string()
        .contains("poisoned"));
}

#[test]
fn errors_and_debug_are_generic_and_redacted() {
    let v1 = [v1_record([0xab; 16], TaxIdentityState::MatchedEin, 0xcd)];
    let v2 = [v2_record([0xab; 16], TaxIdentityStateV2::MatchedEin, 0xce)];
    let mut pair = validator(&v1, &v2, 1);
    let message = invalid_data(pair.next_record()).to_string();
    assert!(!message.contains("abab"));
    assert!(!message.contains("cdcd"));
    assert!(!message.contains("cece"));
}
