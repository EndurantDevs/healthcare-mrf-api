use super::{to_io_error, AssignedV3Row, AssignedV3RowSource, V3_FINALIZER_ASSIGNED_BYTES};
use ptg2_scanner::v3_runs::{AssignedServingRunMerger, AuditCandidateSelector};
use std::io;
use std::path::PathBuf;

pub(super) struct AssignedFixedRecordStream {
    partition_paths: Vec<Vec<PathBuf>>,
    partition_index: usize,
    remove_consumed: bool,
    partition_merger: Option<AssignedServingRunMerger>,
    previous_record: Option<[u8; V3_FINALIZER_ASSIGNED_BYTES]>,
    audit_candidates: AuditCandidateSelector,
    distinct_record_count: u64,
    duplicate_record_count: u64,
}

impl AssignedFixedRecordStream {
    #[cfg(test)]
    pub(super) fn new_many(paths: Vec<PathBuf>, population_count: u64) -> io::Result<Self> {
        Self::new_partition_runs(
            paths.into_iter().map(|path| vec![path]).collect(),
            population_count,
        )
    }

    /// Consume only scratch runs owned by this finalizer invocation.
    pub(super) fn new_owned_partition_runs(
        partition_paths: Vec<Vec<PathBuf>>,
        population_count: u64,
    ) -> io::Result<Self> {
        let mut stream = Self::new_partition_runs(partition_paths, population_count)?;
        stream.remove_consumed = true;
        Ok(stream)
    }

    fn new_partition_runs(
        partition_paths: Vec<Vec<PathBuf>>,
        population_count: u64,
    ) -> io::Result<Self> {
        if partition_paths.iter().flatten().any(|path| !path.is_file()) {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "assigned partition input does not exist",
            ));
        }
        if partition_paths.iter().any(Vec::is_empty) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "assigned partition has no sorted runs",
            ));
        }
        Ok(Self {
            partition_paths,
            partition_index: 0,
            remove_consumed: false,
            partition_merger: None,
            previous_record: None,
            audit_candidates: AuditCandidateSelector::new(population_count),
            distinct_record_count: 0,
            duplicate_record_count: 0,
        })
    }

    pub(super) fn audit_candidates(
        &self,
    ) -> io::Result<&[ptg2_scanner::v3_runs::AuditCandidateRecord]> {
        self.audit_candidates.finish()
    }

    pub(super) fn distinct_record_count(&self) -> u64 {
        self.distinct_record_count
    }

    pub(super) fn duplicate_record_count(&self) -> u64 {
        self.duplicate_record_count
    }
}

impl AssignedV3RowSource for AssignedFixedRecordStream {
    fn next_row(&mut self) -> io::Result<Option<AssignedV3Row>> {
        let record = loop {
            if self.partition_merger.is_none() {
                let Some(paths) = self.partition_paths.get(self.partition_index) else {
                    return Ok(None);
                };
                self.partition_merger = Some(AssignedServingRunMerger::new(paths)?);
                self.partition_index += 1;
            }
            if let Some(record) = self.partition_merger.as_mut().unwrap().next_record()? {
                break record;
            }
            self.partition_merger = None;
            if self.remove_consumed {
                for path in &self.partition_paths[self.partition_index - 1] {
                    std::fs::remove_file(path)?;
                }
            }
        };
        if self
            .previous_record
            .is_some_and(|previous| record < previous)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "assigned partition files are not globally ordered",
            ));
        }
        if self.previous_record == Some(record) {
            self.duplicate_record_count = self.duplicate_record_count.saturating_add(1);
        } else {
            self.distinct_record_count = self.distinct_record_count.saturating_add(1);
        }
        self.previous_record = Some(record);
        let code_key = i32::from_be_bytes(record[0..4].try_into().map_err(to_io_error)?);
        let provider_set_key = i32::from_be_bytes(record[4..8].try_into().map_err(to_io_error)?);
        let price_key = u32::from_be_bytes(record[8..12].try_into().map_err(to_io_error)?);
        let source_key = u32::from_be_bytes(record[12..16].try_into().map_err(to_io_error)?);
        let provider_count = u32::from_be_bytes(record[16..20].try_into().map_err(to_io_error)?);
        self.audit_candidates.observe(
            u32::try_from(code_key).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "assigned code_key cannot be negative",
                )
            })?,
            u32::try_from(provider_set_key).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "assigned provider_set_key cannot be negative",
                )
            })?,
            price_key,
            source_key,
            provider_count,
        )?;
        Ok(Some(AssignedV3Row {
            code_key,
            provider_set_key,
            provider_count: u64::from(provider_count),
            price_key,
            source_key,
        }))
    }

    fn source_copy_format(&self) -> &'static str {
        "assigned_fixed_v1"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn write_run(path: &Path, codes: &[i32]) {
        let bytes = codes
            .iter()
            .flat_map(|code| [*code, 0, 0, 0, 1])
            .flat_map(i32::to_be_bytes)
            .collect::<Vec<_>>();
        std::fs::write(path, bytes).unwrap();
    }

    #[test]
    fn owned_runs_are_removed_after_partition_exhaustion() {
        for remove_consumed in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let first = directory.path().join("first.bin");
            let second = directory.path().join("second.bin");
            let last = directory.path().join("last.bin");
            write_run(&first, &[0, 2]);
            write_run(&second, &[1]);
            write_run(&last, &[3]);
            let partitions = vec![vec![first.clone(), second.clone()], vec![last.clone()]];
            let mut stream = if remove_consumed {
                AssignedFixedRecordStream::new_owned_partition_runs(partitions, 4)
            } else {
                AssignedFixedRecordStream::new_partition_runs(partitions, 4)
            }
            .unwrap();
            for code in 0..3 {
                assert_eq!(stream.next_row().unwrap().unwrap().code_key, code);
                assert!(first.exists() && second.exists() && last.exists());
            }
            assert_eq!(stream.next_row().unwrap().unwrap().code_key, 3);
            assert_eq!(first.exists(), !remove_consumed);
            assert_eq!(second.exists(), !remove_consumed);
            assert!(last.exists());
            assert_eq!(stream.next_row().unwrap(), None);
            assert_eq!(last.exists(), !remove_consumed);
            assert_eq!(stream.distinct_record_count(), 4);
            assert_eq!(stream.duplicate_record_count(), 0);
            assert!(!stream.audit_candidates().unwrap().is_empty());
            assert_eq!(stream.next_row().unwrap(), None);
        }
    }

    #[test]
    fn invalid_owned_partition_keeps_unconsumed_runs() {
        let directory = tempfile::tempdir().unwrap();
        let consumed = directory.path().join("consumed.bin");
        let invalid = directory.path().join("invalid.bin");
        let future = directory.path().join("future.bin");
        write_run(&consumed, &[0]);
        std::fs::write(&invalid, [0; V3_FINALIZER_ASSIGNED_BYTES - 1]).unwrap();
        write_run(&future, &[2]);
        let mut stream = AssignedFixedRecordStream::new_owned_partition_runs(
            vec![
                vec![consumed.clone()],
                vec![invalid.clone()],
                vec![future.clone()],
            ],
            3,
        )
        .unwrap();
        assert_eq!(stream.next_row().unwrap().unwrap().code_key, 0);
        assert!(consumed.exists());
        assert_eq!(
            stream.next_row().unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert!(!consumed.exists());
        assert!(invalid.exists() && future.exists());
    }

    #[test]
    fn owned_partition_cleanup_failure_stops_the_stream() {
        let directory = tempfile::tempdir().unwrap();
        let consumed = directory.path().join("consumed.bin");
        let future = directory.path().join("future.bin");
        write_run(&consumed, &[0]);
        write_run(&future, &[1]);
        let mut stream = AssignedFixedRecordStream::new_owned_partition_runs(
            vec![vec![consumed.clone()], vec![future.clone()]],
            2,
        )
        .unwrap();
        assert_eq!(stream.next_row().unwrap().unwrap().code_key, 0);
        std::fs::remove_file(&consumed).unwrap();
        std::fs::create_dir(&consumed).unwrap();
        assert!(stream.next_row().is_err());
        assert!(future.exists());
    }
}
