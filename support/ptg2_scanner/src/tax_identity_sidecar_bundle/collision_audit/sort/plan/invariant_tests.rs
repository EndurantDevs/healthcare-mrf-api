use super::*;

fn limits() -> TaxIdentityCollisionAuditLimits {
    TaxIdentityCollisionAuditLimits {
        max_artifacts: 64,
        max_source_rows: u64::MAX,
        max_matched_rows: u64::MAX,
        max_memory_bytes: u64::MAX,
        max_scratch_bytes: u64::MAX,
        minimum_free_scratch_bytes: 0,
        merge_fan_in: 2,
        max_open_files: 6,
    }
}

#[test]
fn ragged_merge_plan_accounts_for_residual_runs() {
    let plan = CollisionSortPlan::admit(
        13,
        13,
        1,
        TaxIdentityCollisionAuditLimits {
            max_memory_bytes: 361_410,
            ..limits()
        },
    )
    .unwrap();
    assert_eq!(plan.expected_records, 13);
    assert_eq!(plan.chunk_record_capacity, 2);
    assert_eq!(plan.expected_merge_records, 25);
    assert_eq!(planned_merge_records(0, 0, 2).unwrap(), 0);
    assert_eq!(planned_merge_records(13, 2, 2).unwrap(), 25);
}

#[test]
fn plan_rejects_invalid_counts_limits_and_resource_boundaries() {
    for (source, matched, artifacts, mutate, expected) in [
        (0, 0, 1, None, ROW_LIMIT_EXCEEDED),
        (1, 2, 1, None, ROW_LIMIT_EXCEEDED),
        (1, 1, 0, None, ARTIFACT_LIMIT_EXCEEDED),
        (1, 1, 1, Some((0, 6)), ARTIFACT_LIMIT_EXCEEDED),
        (1, 1, 1, Some((64, 5)), INVALID_LIMITS),
    ] {
        let mut value = limits();
        if let Some((max_artifacts, max_open_files)) = mutate {
            value.max_artifacts = max_artifacts;
            value.max_open_files = max_open_files;
        }
        assert_eq!(
            CollisionSortPlan::admit(source, matched, artifacts, value)
                .err()
                .unwrap()
                .to_string(),
            expected
        );
    }

    let mut value = limits();
    value.max_scratch_bytes = 65;
    assert_eq!(
        CollisionSortPlan::admit(1, 1, 1, value)
            .err()
            .unwrap()
            .to_string(),
        SCRATCH_LIMIT_EXCEEDED
    );
}

#[test]
fn planner_arithmetic_and_allocation_failures_are_bounded() {
    assert_eq!(
        checked_add(u64::MAX, 1).unwrap_err().to_string(),
        COUNT_OVERFLOW
    );
    assert_eq!(
        required_scratch_bytes(u64::MAX, limits())
            .unwrap_err()
            .to_string(),
        COUNT_OVERFLOW
    );
    assert_eq!(
        planned_merge_records(u64::MAX, usize::MAX, 2)
            .unwrap_err()
            .to_string(),
        COUNT_OVERFLOW
    );
    assert_eq!(
        append_residual_runs(&mut Vec::new(), u64::MAX, 1, 1)
            .unwrap_err()
            .to_string(),
        MEMORY_LIMIT_EXCEEDED
    );
    assert_eq!(
        runtime_owned_bytes(1, usize::MAX, limits())
            .unwrap_err()
            .to_string(),
        COUNT_OVERFLOW
    );
}
