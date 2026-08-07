use super::*;
use std::io::Write;

#[test]
fn scratch_budget_enforces_cap_overflow_and_release_accounting() {
    let mut budget = ScratchBudget::new(5);
    budget.reserve(5).unwrap();
    assert_eq!((budget.used(), budget.peak()), (5, 5));
    assert_eq!(
        budget.reserve(1).unwrap_err().to_string(),
        SCRATCH_LIMIT_EXCEEDED
    );
    budget.release(3).unwrap();
    assert_eq!(
        budget.release(3).unwrap_err().to_string(),
        SCRATCH_ACCOUNTING_INVALID
    );

    let mut overflow = ScratchBudget::new(u64::MAX);
    overflow.reserve(u64::MAX).unwrap();
    assert_eq!(
        overflow.reserve(1).unwrap_err().to_string(),
        SCRATCH_ACCOUNTING_INVALID
    );
}

#[test]
fn scratch_capacity_compares_actual_free_space_without_overflow() {
    let temporary = tempfile::tempdir().unwrap();
    let error = PrivateScratch::create(temporary.path(), 0, u64::MAX).unwrap_err();
    assert_eq!(error.to_string(), SCRATCH_CAPACITY_INSUFFICIENT);
    assert!(std::fs::read_dir(temporary.path())
        .unwrap()
        .next()
        .is_none());
}

#[test]
fn private_scratch_and_run_debug_are_redacted() {
    let temporary = tempfile::tempdir().unwrap();
    let scratch = PrivateScratch::create(temporary.path(), 3, 0).unwrap();
    assert!(format!("{scratch:?}").contains("<redacted>"));
    let (pending, mut file) = scratch.create_run(ScratchRunKind::Initial, 3).unwrap();
    file.write_all(b"abc").unwrap();
    let run = scratch
        .seal_run(pending, &mut file, &mut || Ok(()))
        .unwrap();
    let debug = format!("{run:?}");
    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains("tax-collision"));
    scratch.remove_run(&run, &mut || Ok(())).unwrap();
}

#[test]
fn scratch_names_and_pending_run_state_fail_closed() {
    for invalid in ["", ".", "..", "slash/name", "under_score", &"a".repeat(97)] {
        assert_eq!(
            validate_run_name(invalid).unwrap_err().to_string(),
            SCRATCH_UNAVAILABLE
        );
    }

    let temporary = tempfile::tempdir().unwrap();
    let scratch = PrivateScratch::create(temporary.path(), 1, 0).unwrap();
    let (pending, mut file) = scratch.create_run(ScratchRunKind::Merge, 1).unwrap();
    file.write_all(b"ab").unwrap();
    assert_eq!(
        scratch
            .seal_run(pending, &mut file, &mut || Ok(()))
            .unwrap_err()
            .to_string(),
        SCRATCH_UNAVAILABLE
    );
}
