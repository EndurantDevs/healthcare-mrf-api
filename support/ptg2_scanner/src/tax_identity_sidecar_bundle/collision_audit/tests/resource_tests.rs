use super::*;

const ONE_MATCH_MEMORY_BYTES: u64 = 361_377;
const ZERO_MATCH_MEMORY_BYTES: u64 = 65_664;

#[test]
fn ragged_multilevel_merge_matches_the_admitted_plan() {
    let rows = (1..=13)
        .map(|value| (TaxIdentityStateV2::MatchedEin, Some(token(value))))
        .collect();
    let fixture = BundleFixture::new(vec![rows]);
    let scratch = fixture.scratch_root("ragged-multilevel");

    let result = audit_tax_identity_sidecar_bundle(
        &fixture.checkpoint,
        &fixture.descriptors,
        &config(scratch.clone(), 361_410, 2, 6),
    )
    .unwrap();

    assert_eq!(result.stats().initial_run_count, 7);
    assert_eq!(result.stats().merge_operation_count, 5);
    assert_eq!(result.stats().maximum_merge_fan_in, 2);
    assert!(directory_is_empty(&scratch));
}

#[test]
fn matched_memory_and_scan_triggered_merge_fd_boundaries_are_exact() {
    let fixture = BundleFixture::new(vec![vec![
        (TaxIdentityStateV2::MatchedEin, Some(token(0x71))),
        (TaxIdentityStateV2::MatchedNpi, Some(token(0x72))),
    ]]);

    let accepted_root = fixture.scratch_root("resource-boundary-accepted");
    let accepted = audit_tax_identity_sidecar_bundle(
        &fixture.checkpoint,
        &fixture.descriptors,
        &config(accepted_root.clone(), ONE_MATCH_MEMORY_BYTES, 2, 6),
    )
    .unwrap();
    assert_eq!(accepted.stats().maximum_merge_fan_in, 2);
    assert_eq!(accepted.stats().merge_operation_count, 1);
    assert!(directory_is_empty(&accepted_root));

    for (suffix, memory, open_files, expected) in [
        (
            "memory-one-below",
            ONE_MATCH_MEMORY_BYTES - 1,
            6,
            "PTG tax identity collision audit memory limit exceeded",
        ),
        (
            "fd-one-below",
            ONE_MATCH_MEMORY_BYTES,
            5,
            "PTG tax identity collision audit limits are invalid",
        ),
    ] {
        let scratch = fixture.scratch_root(suffix);
        let error = audit_tax_identity_sidecar_bundle(
            &fixture.checkpoint,
            &fixture.descriptors,
            &config(scratch.clone(), memory, 2, open_files),
        )
        .unwrap_err();
        assert_eq!(error.to_string(), expected);
        assert!(directory_is_empty(&scratch));
    }
}

#[test]
fn zero_match_skips_scratch_and_unused_merge_resources() {
    let fixture = BundleFixture::new(vec![vec![
        (TaxIdentityStateV2::Missing, None),
        (TaxIdentityStateV2::Malformed, None),
    ]]);
    let absent_scratch = fixture.temporary.path().join("absent-scratch-root");
    let mut limits = config(absent_scratch.clone(), ZERO_MATCH_MEMORY_BYTES, 2, 2).limits();
    limits.max_scratch_bytes = 0;
    limits.minimum_free_scratch_bytes = u64::MAX;

    let result = audit_tax_identity_sidecar_bundle(
        &fixture.checkpoint,
        &fixture.descriptors,
        &TaxIdentityCollisionAuditConfig::new(absent_scratch.clone(), limits),
    )
    .unwrap();

    assert!(!absent_scratch.exists());
    assert_eq!(result.stats().initial_run_count, 0);
    assert_eq!(result.stats().merge_operation_count, 0);
    assert_eq!(result.stats().peak_scratch_bytes, 0);

    let mut low_limits = limits;
    low_limits.max_memory_bytes = ZERO_MATCH_MEMORY_BYTES - 1;
    let error = audit_tax_identity_sidecar_bundle(
        &fixture.checkpoint,
        &fixture.descriptors,
        &TaxIdentityCollisionAuditConfig::new(absent_scratch, low_limits),
    )
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        "PTG tax identity collision audit memory limit exceeded"
    );
}

#[test]
fn high_artifact_count_is_bounded_and_memory_accounted() {
    let fixture = BundleFixture::new(
        (0..65)
            .map(|_| vec![(TaxIdentityStateV2::Missing, None)])
            .collect(),
    );
    let scratch = fixture.scratch_root("many-artifacts");
    let mut limits = config(scratch.clone(), 1_000_000, 2, 2).limits();
    limits.max_artifacts = 64;
    let mut invalid_checkpoint = fixture.checkpoint.clone();
    invalid_checkpoint.contract = "invalid-before-full-validation".into();
    let mut events = Vec::new();
    let error = audit_tax_identity_sidecar_bundle_with_progress(
        &invalid_checkpoint,
        &fixture.descriptors,
        &TaxIdentityCollisionAuditConfig::new(scratch.clone(), limits),
        |event| {
            events.push(event);
            Ok(())
        },
    )
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        "PTG tax identity collision audit artifact limit exceeded"
    );
    assert_eq!(
        events,
        vec![TaxIdentityCollisionAuditProgress {
            phase: TaxIdentityCollisionAuditPhase::Admission,
            completed: 0,
            total: 1,
        }]
    );

    limits.max_artifacts = 128;
    limits.max_memory_bytes = 73_855;
    let error = audit_tax_identity_sidecar_bundle(
        &fixture.checkpoint,
        &fixture.descriptors,
        &TaxIdentityCollisionAuditConfig::new(scratch.clone(), limits),
    )
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        "PTG tax identity collision audit memory limit exceeded"
    );
    assert!(directory_is_empty(&scratch));
}

#[test]
fn scratch_count_and_configuration_overflow_paths_fail_closed() {
    let base_limits = TaxIdentityCollisionAuditLimits {
        max_artifacts: 1,
        max_source_rows: u64::MAX,
        max_matched_rows: u64::MAX,
        max_memory_bytes: u64::MAX,
        max_scratch_bytes: u64::MAX,
        minimum_free_scratch_bytes: 0,
        merge_fan_in: 2,
        max_open_files: 6,
    };
    let overflow = sort::CollisionSortPlan::admit(u64::MAX, u64::MAX, 1, base_limits)
        .err()
        .unwrap();
    assert_eq!(
        overflow.to_string(),
        "PTG tax identity collision audit count overflow"
    );

    for mutate in [
        |limits: &mut TaxIdentityCollisionAuditLimits| limits.merge_fan_in = 1,
        |limits: &mut TaxIdentityCollisionAuditLimits| limits.merge_fan_in = 65,
        |limits: &mut TaxIdentityCollisionAuditLimits| limits.max_artifacts = 4097,
    ] {
        let mut limits = base_limits;
        mutate(&mut limits);
        assert!(sort::CollisionSortPlan::admit(1, 1, 1, limits).is_err());
    }
}

#[test]
fn scratch_capacity_and_byte_ceilings_cleanup_private_state() {
    let fixture = BundleFixture::new(vec![vec![(
        TaxIdentityStateV2::MatchedEin,
        Some(token(0x72)),
    )]]);
    let scratch = fixture.scratch_root("scratch-ceilings");
    let baseline = config(scratch.clone(), ONE_MATCH_MEMORY_BYTES, 2, 6);

    let mut byte_limits = baseline.limits();
    byte_limits.max_scratch_bytes = 65;
    let byte_error = audit_tax_identity_sidecar_bundle(
        &fixture.checkpoint,
        &fixture.descriptors,
        &TaxIdentityCollisionAuditConfig::new(scratch.clone(), byte_limits),
    )
    .unwrap_err();
    assert_eq!(
        byte_error.to_string(),
        "PTG tax identity collision audit scratch byte limit exceeded"
    );

    let mut capacity_limits = baseline.limits();
    capacity_limits.minimum_free_scratch_bytes = u64::MAX;
    let capacity_error = audit_tax_identity_sidecar_bundle(
        &fixture.checkpoint,
        &fixture.descriptors,
        &TaxIdentityCollisionAuditConfig::new(scratch.clone(), capacity_limits),
    )
    .unwrap_err();
    assert_eq!(
        capacity_error.to_string(),
        "PTG tax identity collision audit scratch capacity is insufficient"
    );
    assert!(directory_is_empty(&scratch));
}

#[test]
fn every_progress_phase_can_cancel_without_leaking_scratch_or_a_checkpoint() {
    let rows = (1..=20)
        .map(|value| (TaxIdentityStateV2::MatchedEin, Some(token(value))))
        .collect::<Vec<_>>();
    let fixture = BundleFixture::new(vec![rows]);
    let phases = [
        TaxIdentityCollisionAuditPhase::Admission,
        TaxIdentityCollisionAuditPhase::Authenticate,
        TaxIdentityCollisionAuditPhase::Scan,
        TaxIdentityCollisionAuditPhase::Spill,
        TaxIdentityCollisionAuditPhase::Merge,
        TaxIdentityCollisionAuditPhase::Verify,
        TaxIdentityCollisionAuditPhase::Complete,
    ];

    for phase in phases {
        let scratch = fixture.scratch_root(&format!("cancel-{phase:?}"));
        let mut reached = false;
        let error = audit_tax_identity_sidecar_bundle_with_progress(
            &fixture.checkpoint,
            &fixture.descriptors,
            &config(scratch.clone(), ONE_MATCH_MEMORY_BYTES, 2, 6),
            |event| {
                if event.phase == phase {
                    reached = true;
                    return Err(io::Error::new(io::ErrorKind::Interrupted, "cancelled"));
                }
                Ok(())
            },
        )
        .unwrap_err();
        assert!(reached, "phase {phase:?} was not emitted");
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
        assert!(directory_is_empty(&scratch));
    }
}
