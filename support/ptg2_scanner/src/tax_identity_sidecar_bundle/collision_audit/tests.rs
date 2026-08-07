use super::records::{CollisionAuditAccumulator, CollisionAuditRecord};
use super::*;
use crate::tax_identity::TaxIdentityStateV2;
use crate::tax_identity_sidecar_bundle::digests::encode_hex;
use crate::tax_identity_sidecar_bundle::TAX_IDENTITY_SIDECAR_COLLISION_AUDIT;
use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::PathBuf;

mod contract_tests;
mod file_tests;
mod fixture;
mod resource_tests;
use fixture::{token, BundleFixture};

fn record(byte: u8, type_tag: u8) -> CollisionAuditRecord {
    let mut encoded = [byte; TAX_IDENTITY_COLLISION_AUDIT_RECORD_BYTES];
    encoded[32] = type_tag;
    CollisionAuditRecord::decode(encoded).unwrap()
}

#[test]
fn same_identity_repetition_is_valid_and_counted() {
    let mut audit = CollisionAuditAccumulator::new(3);
    for value in [record(1, 1), record(1, 1), record(2, 5)] {
        audit.observe(value).unwrap();
    }
    let summary = audit.finish().unwrap();
    assert_eq!(summary.matched_row_count, 3);
    assert_eq!(summary.unique_identity_count, 2);
    assert_eq!(summary.repeated_identity_count, 1);
    assert_eq!(summary.repeated_occurrence_count, 1);
}

#[test]
fn collision_reasons_remain_distinct() {
    let mut first = [1; TAX_IDENTITY_COLLISION_AUDIT_RECORD_BYTES];
    first[32] = 1;
    let mut locator_collision = first;
    locator_collision[31] = 2;
    let mut cross_type = first;
    cross_type[32] = 5;

    let mut locator_audit = CollisionAuditAccumulator::new(2);
    locator_audit
        .observe(CollisionAuditRecord::decode(first).unwrap())
        .unwrap();
    let locator_error = locator_audit
        .observe(CollisionAuditRecord::decode(locator_collision).unwrap())
        .and_then(|()| locator_audit.finish().map(|_| ()))
        .unwrap_err();
    assert_eq!(locator_error.to_string(), LOCATOR_PREFIX_COLLISION);

    let mut type_audit = CollisionAuditAccumulator::new(2);
    type_audit
        .observe(CollisionAuditRecord::decode(first).unwrap())
        .unwrap();
    let type_error = type_audit
        .observe(CollisionAuditRecord::decode(cross_type).unwrap())
        .unwrap_err();
    assert_eq!(type_error.to_string(), FULL_HMAC_CROSS_TYPE_COLLISION);
}

#[test]
fn descending_records_are_rejected_without_shadowing_cross_type_collisions() {
    let mut descending = CollisionAuditAccumulator::new(2);
    descending.observe(record(2, 1)).unwrap();
    let error = descending.observe(record(1, 1)).unwrap_err();
    assert_eq!(error.to_string(), NONCANONICAL_AUDIT_ORDER);

    let mut cross_type = CollisionAuditAccumulator::new(2);
    cross_type.observe(record(3, 5)).unwrap();
    let error = cross_type.observe(record(3, 1)).unwrap_err();
    assert_eq!(error.to_string(), FULL_HMAC_CROSS_TYPE_COLLISION);
}

#[test]
fn bundle_audit_preserves_valid_repetition_and_v1_authority() {
    let repeated = token(0x31);
    let fixture = BundleFixture::new(vec![
        vec![
            (TaxIdentityStateV2::MatchedEin, Some(repeated)),
            (TaxIdentityStateV2::MatchedNpi, Some(token(0x72))),
            (TaxIdentityStateV2::Missing, None),
        ],
        vec![
            (TaxIdentityStateV2::MatchedEin, Some(repeated)),
            (TaxIdentityStateV2::MatchedNpi, Some(token(0x53))),
            (TaxIdentityStateV2::Malformed, None),
        ],
    ]);
    let scratch = fixture.scratch_root("valid");
    let result = audit_tax_identity_sidecar_bundle(
        &fixture.checkpoint,
        &fixture.descriptors,
        &config(scratch.clone(), 1_000_000, 2, 6),
    )
    .unwrap();
    let checkpoint = result.checkpoint();

    assert!(!checkpoint.publication_admissible());
    assert_eq!(checkpoint.projection_authority(), "v1_only");
    assert_eq!(checkpoint.matched_row_count(), 4);
    assert_eq!(checkpoint.matched_ein_count(), 2);
    assert_eq!(checkpoint.matched_npi_count(), 2);
    assert_eq!(checkpoint.unique_identity_count(), 3);
    assert_eq!(checkpoint.repeated_identity_count(), 1);
    assert_eq!(checkpoint.repeated_occurrence_count(), 1);
    assert_eq!(
        checkpoint.source_bundle_sha256(),
        fixture.checkpoint.bundle_sha256()
    );
    assert_eq!(
        fixture
            .checkpoint
            .cross_row_full_hmac_type_collision_check(),
        TAX_IDENTITY_SIDECAR_COLLISION_AUDIT
    );
    assert!(directory_is_empty(&scratch));
}

#[test]
fn bundle_audit_distinguishes_locator_and_cross_type_collisions() {
    let first = token(0x11);
    let mut same_locator = first;
    same_locator[31] = 0x12;
    let locator_fixture = BundleFixture::new(vec![vec![
        (TaxIdentityStateV2::MatchedEin, Some(first)),
        (TaxIdentityStateV2::MatchedEin, Some(same_locator)),
    ]]);
    let locator_error = audit_fixture(&locator_fixture, "locator").unwrap_err();
    assert_eq!(locator_error.to_string(), LOCATOR_PREFIX_COLLISION);

    let cross_type_fixture = BundleFixture::new(vec![vec![
        (TaxIdentityStateV2::MatchedEin, Some(first)),
        (TaxIdentityStateV2::MatchedNpi, Some(first)),
    ]]);
    let type_error = audit_fixture(&cross_type_fixture, "cross-type").unwrap_err();
    assert_eq!(type_error.to_string(), FULL_HMAC_CROSS_TYPE_COLLISION);
}

#[test]
fn empty_matched_set_is_a_deterministic_success() {
    let fixture = BundleFixture::new(vec![vec![
        (TaxIdentityStateV2::Missing, None),
        (TaxIdentityStateV2::Malformed, None),
        (TaxIdentityStateV2::UnsupportedType, None),
    ]]);
    let first = audit_fixture(&fixture, "empty-first").unwrap();
    let second = audit_fixture(&fixture, "empty-second").unwrap();

    assert_eq!(first.checkpoint(), second.checkpoint());
    assert_eq!(first.checkpoint().matched_row_count(), 0);
    assert_eq!(first.checkpoint().unique_identity_count(), 0);
    assert_eq!(first.stats().initial_run_count, 0);
    assert_eq!(first.stats().peak_scratch_bytes, 0);
}

#[test]
fn checkpoint_is_independent_of_chunking_fan_in_and_descriptor_order() {
    let rows = (1..=12)
        .map(|value| {
            let state = if value % 2 == 0 {
                TaxIdentityStateV2::MatchedNpi
            } else {
                TaxIdentityStateV2::MatchedEin
            };
            (state, Some(token(value)))
        })
        .collect::<Vec<_>>();
    let fixture = BundleFixture::new(vec![rows[..6].to_vec(), rows[6..].to_vec()]);
    let roomy_root = fixture.scratch_root("roomy");
    let roomy = audit_tax_identity_sidecar_bundle(
        &fixture.checkpoint,
        &fixture.descriptors,
        &config(roomy_root.clone(), 1_000_000, 3, 7),
    )
    .unwrap();
    let forced_root = fixture.scratch_root("forced");
    let mut reversed = fixture.descriptors.clone();
    reversed.reverse();
    let forced = audit_tax_identity_sidecar_bundle(
        &fixture.checkpoint,
        &reversed,
        &config(forced_root.clone(), 361_505, 2, 6),
    )
    .unwrap();

    assert_eq!(roomy.checkpoint(), forced.checkpoint());
    assert_eq!(roomy.stats().initial_run_count, 1);
    assert_eq!(forced.stats().initial_run_count, 12);
    assert_eq!(forced.stats().merge_operation_count, 10);
    assert!(directory_is_empty(&roomy_root));
    assert!(directory_is_empty(&forced_root));
}

#[test]
fn progress_is_phase_global_and_cancellation_cleans_scratch() {
    let rows = (1..=10)
        .map(|value| (TaxIdentityStateV2::MatchedEin, Some(token(value))))
        .collect::<Vec<_>>();
    let fixture = BundleFixture::new(vec![rows]);
    let scratch = fixture.scratch_root("progress");
    let mut events = Vec::new();
    let result = audit_tax_identity_sidecar_bundle_with_progress(
        &fixture.checkpoint,
        &fixture.descriptors,
        &config(scratch.clone(), 361_377, 2, 6),
        |event| {
            events.push(event);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(
        events.last().unwrap().phase,
        TaxIdentityCollisionAuditPhase::Complete
    );
    assert_eq!(result.stats().cancellation_poll_count, events.len() as u64);
    let mut by_phase: HashMap<TaxIdentityCollisionAuditPhase, Vec<(u64, u64)>> = HashMap::new();
    for event in events {
        by_phase
            .entry(event.phase)
            .or_default()
            .push((event.completed, event.total));
    }
    for values in by_phase.values() {
        assert!(values.windows(2).all(|pair| pair[0].0 <= pair[1].0));
        assert!(values.iter().all(|(completed, total)| completed <= total));
        assert!(values.iter().all(|value| value.1 == values[0].1));
    }

    let cancelled_root = fixture.scratch_root("cancelled");
    let error = audit_tax_identity_sidecar_bundle_with_progress(
        &fixture.checkpoint,
        &fixture.descriptors,
        &config(cancelled_root.clone(), 361_377, 2, 6),
        |event| {
            if event.phase == TaxIdentityCollisionAuditPhase::Scan {
                Err(io::Error::new(io::ErrorKind::Interrupted, "cancelled"))
            } else {
                Ok(())
            }
        },
    )
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Interrupted);
    assert!(directory_is_empty(&cancelled_root));
}

#[test]
fn descriptor_set_limits_and_diagnostics_fail_closed_without_paths_or_tokens() {
    let fixture = BundleFixture::new(vec![vec![(
        TaxIdentityStateV2::MatchedEin,
        Some(token(0x41)),
    )]]);
    let scratch = fixture.scratch_root("diagnostics");
    let baseline = config(scratch.clone(), 1_000_000, 2, 6);
    let mut limit_values = baseline.limits();
    limit_values.max_source_rows = 0;
    let limits = TaxIdentityCollisionAuditConfig::new(scratch.clone(), limit_values);
    let limit_error =
        audit_tax_identity_sidecar_bundle(&fixture.checkpoint, &fixture.descriptors, &limits)
            .unwrap_err();
    assert_eq!(
        limit_error.to_string(),
        "PTG tax identity collision audit row limit exceeded"
    );
    assert!(directory_is_empty(&scratch));

    let set_error = audit_tax_identity_sidecar_bundle(
        &fixture.checkpoint,
        &[],
        &config(scratch.clone(), 1_000_000, 2, 6),
    )
    .unwrap_err();
    assert_eq!(set_error.to_string(), artifacts::ARTIFACT_SET_MISMATCH);

    let mut missing = fixture.descriptors.clone();
    missing[0].path = fixture.temporary.path().join("sensitive-tax-value.sidecar");
    let artifact_error = audit_tax_identity_sidecar_bundle(
        &fixture.checkpoint,
        &missing,
        &config(scratch.clone(), 1_000_000, 2, 6),
    )
    .unwrap_err();
    let message = artifact_error.to_string();
    assert_eq!(message, artifacts::ARTIFACT_UNAVAILABLE);
    assert!(!message.contains("sensitive-tax-value"));
    assert!(!message.contains(&encode_hex(&token(0x41))));
    assert!(directory_is_empty(&scratch));
}

#[test]
fn audited_checkpoint_json_is_frozen_and_debug_is_redacted() {
    let fixture = BundleFixture::new(vec![vec![
        (TaxIdentityStateV2::MatchedEin, Some(token(0x21))),
        (TaxIdentityStateV2::MatchedEin, Some(token(0x21))),
    ]]);
    let result = audit_fixture(&fixture, "frozen").unwrap();
    let json = serde_json::to_string(result.checkpoint()).unwrap();
    assert_eq!(
        json,
        "{\"contract\":\"ptg2_tax_identity_sidecar_collision_audit_v1\",\"publication_admissible\":false,\"projection_authority\":\"v1_only\",\"source_bundle_sha256\":\"3ab130b5361d7bd78648045ba69c4fe0f61d3f49a09048e794e382fd665e6335\",\"record_contract\":\"full_hmac_sha256_then_v2_type_tag_33_bytes_v1\",\"occurrence_digest_contract\":\"sha256_ptg2_tax_collision_occurrences_v1_with_expected_count_prefix\",\"locator_collision_policy\":\"reject_different_full_hmac_same_128_bit_prefix_v1\",\"full_hmac_type_collision_policy\":\"reject_same_full_hmac_different_type_v1\",\"same_identity_repetition_policy\":\"allow_same_full_hmac_same_type_across_groups_v1\",\"multi_candidate_locator_support\":\"deferred_phase1\",\"locator_prefix_collision_check\":\"passed\",\"full_hmac_cross_type_collision_check\":\"passed\",\"matched_row_count\":2,\"matched_ein_count\":2,\"matched_npi_count\":0,\"unique_identity_count\":1,\"repeated_identity_count\":1,\"repeated_occurrence_count\":1,\"occurrence_multiset_sha256\":\"477938b966571d64f4beeee5457f2b3669cf5cf88b856bd4304d88200217d3d8\",\"audit_sha256\":\"475565ac64f839be993e6661efae4240468e7b714924d477ecd4e2c3839e1e2d\"}"
    );
    let debug = format!("{result:?}");
    assert!(!debug.contains(fixture.temporary.path().to_string_lossy().as_ref()));
    assert!(!debug.contains(result.checkpoint().source_bundle_sha256()));
    assert!(!debug.contains(result.checkpoint().occurrence_multiset_sha256()));
    assert!(!debug.contains(result.checkpoint().audit_sha256()));
}

fn audit_fixture(
    fixture: &BundleFixture,
    suffix: &str,
) -> io::Result<TaxIdentityCollisionAuditResult> {
    let scratch = fixture.scratch_root(suffix);
    let result = audit_tax_identity_sidecar_bundle(
        &fixture.checkpoint,
        &fixture.descriptors,
        &config(scratch.clone(), 1_000_000, 2, 6),
    );
    assert!(directory_is_empty(&scratch));
    result
}

fn config(
    scratch_root: PathBuf,
    max_memory_bytes: u64,
    merge_fan_in: usize,
    max_open_files: usize,
) -> TaxIdentityCollisionAuditConfig {
    TaxIdentityCollisionAuditConfig::new(
        scratch_root,
        TaxIdentityCollisionAuditLimits {
            max_artifacts: 64,
            max_source_rows: 10_000,
            max_matched_rows: 10_000,
            max_memory_bytes,
            max_scratch_bytes: 10_000 * TAX_IDENTITY_COLLISION_AUDIT_RECORD_BYTES as u64 * 2,
            minimum_free_scratch_bytes: 0,
            merge_fan_in,
            max_open_files,
        },
    )
}

fn directory_is_empty(path: &std::path::Path) -> bool {
    fs::read_dir(path).unwrap().next().is_none()
}
