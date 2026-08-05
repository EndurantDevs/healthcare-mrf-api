use super::*;
use crate::tax_identity_sidecar_bundle::collision_audit::TaxIdentityCollisionAuditLimits;
use std::io::{Cursor, Read};

fn record() -> CollisionAuditRecord {
    let mut encoded = [0x31; TAX_IDENTITY_COLLISION_AUDIT_RECORD_BYTES];
    encoded[32] = 1;
    CollisionAuditRecord::decode(encoded).unwrap()
}

fn limits(max_open_files: usize) -> TaxIdentityCollisionAuditLimits {
    TaxIdentityCollisionAuditLimits {
        max_artifacts: 1,
        max_source_rows: 10,
        max_matched_rows: 10,
        max_memory_bytes: 1_000_000,
        max_scratch_bytes: 1_000,
        minimum_free_scratch_bytes: 0,
        merge_fan_in: 2,
        max_open_files,
    }
}

struct InterruptedOnce<R> {
    interrupted: bool,
    inner: R,
}

impl<R: Read> Read for InterruptedOnce<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if !self.interrupted {
            self.interrupted = true;
            Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "synthetic interrupt",
            ))
        } else {
            self.inner.read(buffer)
        }
    }
}

struct FailedReader;

impl Read for FailedReader {
    fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::other("synthetic read failure"))
    }
}

#[test]
fn compact_record_reader_handles_eof_interrupt_and_error_boundaries() {
    assert!(read_record(&mut Cursor::new(Vec::<u8>::new()))
        .unwrap()
        .is_none());
    assert_eq!(
        read_record(&mut Cursor::new(vec![0x31; 5]))
            .unwrap_err()
            .to_string(),
        RUN_INVALID
    );
    assert_eq!(
        read_record(&mut FailedReader).unwrap_err().to_string(),
        RUN_INVALID
    );

    let encoded = record().encode().to_vec();
    let mut interrupted = InterruptedOnce {
        interrupted: false,
        inner: Cursor::new(encoded),
    };
    assert_eq!(read_record(&mut interrupted).unwrap(), Some(record()));
}

#[test]
fn sorter_rejects_missing_or_excess_records_and_absent_scratch() {
    let temporary = tempfile::tempdir().unwrap();
    let zero_plan = CollisionSortPlan::admit(1, 0, 1, limits(2)).unwrap();
    let mut zero_sorter = CollisionSorter::create(temporary.path(), zero_plan).unwrap();
    assert_eq!(
        zero_sorter.scratch_ref().unwrap_err().to_string(),
        RUN_INVALID
    );
    assert_eq!(
        zero_sorter
            .push(record(), &mut |_, _, _| Ok(()))
            .unwrap_err()
            .to_string(),
        RUN_INVALID
    );

    let one_plan = CollisionSortPlan::admit(1, 1, 1, limits(6)).unwrap();
    let one_sorter = CollisionSorter::create(temporary.path(), one_plan).unwrap();
    assert_eq!(
        one_sorter
            .finish(&mut |_, _, _| Ok(()))
            .err()
            .unwrap()
            .to_string(),
        RUN_INVALID
    );
}

#[test]
fn sort_arithmetic_and_progress_boundaries_fail_closed() {
    assert_eq!(
        checked_record_bytes(u64::MAX).unwrap_err().to_string(),
        COUNT_OVERFLOW
    );
    assert_eq!(
        checked_increment(u64::MAX).unwrap_err().to_string(),
        COUNT_OVERFLOW
    );
    assert_eq!(
        checked_add(u64::MAX, 1).unwrap_err().to_string(),
        COUNT_OVERFLOW
    );

    let mut events = Vec::new();
    poll_records(
        &mut |phase, completed, total| {
            events.push((phase, completed, total));
            Ok(())
        },
        TaxIdentityCollisionAuditPhase::Verify,
        1,
        2,
    )
    .unwrap();
    assert!(events.is_empty());
    poll_records(
        &mut |phase, completed, total| {
            events.push((phase, completed, total));
            Ok(())
        },
        TaxIdentityCollisionAuditPhase::Verify,
        2,
        2,
    )
    .unwrap();
    assert_eq!(events.len(), 1);
}
