use super::contracts::TaxIdentityCollisionAuditPhase;
use super::records::{CollisionAuditAccumulator, CollisionAuditRecord, CollisionAuditSummary};
use super::scratch::{PrivateScratch, ScratchBudget, ScratchRun, ScratchRunKind};
use super::{invalid_data, TAX_IDENTITY_COLLISION_AUDIT_RECORD_BYTES};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

mod plan;
pub(super) use plan::CollisionSortPlan;
#[cfg(test)]
mod stream_guard_tests;

const IO_BUFFER_BYTES: usize = 64 * 1024;
const POLL_RECORD_INTERVAL: u64 = 4096;

const MEMORY_LIMIT_EXCEEDED: &str = "PTG tax identity collision audit memory limit exceeded";
const COUNT_OVERFLOW: &str = "PTG tax identity collision audit count overflow";
const RUN_INVALID: &str = "PTG tax identity collision audit sort run is invalid";

pub(super) type AuditProgressCallback<'a> =
    dyn FnMut(TaxIdentityCollisionAuditPhase, u64, u64) -> io::Result<()> + 'a;

pub(super) struct CollisionSortResult {
    pub(super) summary: CollisionAuditSummary,
    pub(super) initial_run_count: u64,
    pub(super) merge_operation_count: u64,
    pub(super) peak_scratch_bytes: u64,
    pub(super) maximum_merge_fan_in: usize,
}

pub(super) struct CollisionSorter {
    scratch: Option<PrivateScratch>,
    budget: ScratchBudget,
    records: Vec<CollisionAuditRecord>,
    levels: Vec<Vec<ScratchRun>>,
    expected_records: u64,
    observed_records: u64,
    spilled_records: u64,
    merged_records: u64,
    expected_merge_records: u64,
    chunk_record_capacity: usize,
    merge_fan_in: usize,
    initial_run_count: u64,
    merge_operation_count: u64,
    maximum_merge_fan_in: usize,
}

#[derive(Clone, Copy)]
struct MergeProgress {
    phase: TaxIdentityCollisionAuditPhase,
    base: u64,
    total: u64,
}

enum MergeSink<'a> {
    Writer(&'a mut BufWriter<std::fs::File>),
    Accumulator(&'a mut CollisionAuditAccumulator),
}

impl CollisionSorter {
    pub(super) fn create(root: &Path, plan: CollisionSortPlan) -> io::Result<Self> {
        let scratch = if plan.expected_records == 0 {
            None
        } else {
            Some(PrivateScratch::create(
                root,
                plan.required_scratch_bytes,
                plan.minimum_free_scratch_bytes,
            )?)
        };
        let mut records = Vec::new();
        if plan.chunk_record_capacity != 0 {
            records
                .try_reserve_exact(plan.chunk_record_capacity)
                .map_err(|_| invalid_data(MEMORY_LIMIT_EXCEEDED))?;
        }
        Ok(Self {
            scratch,
            budget: ScratchBudget::new(plan.max_scratch_bytes),
            records,
            levels: Vec::new(),
            expected_records: plan.expected_records,
            observed_records: 0,
            spilled_records: 0,
            merged_records: 0,
            expected_merge_records: plan.expected_merge_records,
            chunk_record_capacity: plan.chunk_record_capacity,
            merge_fan_in: plan.merge_fan_in,
            initial_run_count: 0,
            merge_operation_count: 0,
            maximum_merge_fan_in: 0,
        })
    }

    pub(super) fn push(
        &mut self,
        record: CollisionAuditRecord,
        progress: &mut AuditProgressCallback<'_>,
    ) -> io::Result<()> {
        self.observed_records = checked_increment(self.observed_records)?;
        if self.observed_records > self.expected_records {
            return Err(invalid_data(RUN_INVALID));
        }
        self.records.push(record);
        if self.records.len() == self.chunk_record_capacity {
            self.spill_chunk(progress)?;
        }
        Ok(())
    }

    pub(super) fn finish(
        mut self,
        progress: &mut AuditProgressCallback<'_>,
    ) -> io::Result<CollisionSortResult> {
        if !self.records.is_empty() {
            self.spill_chunk(progress)?;
        }
        if self.observed_records != self.expected_records {
            return Err(invalid_data(RUN_INVALID));
        }
        if self.spilled_records != self.expected_records {
            return Err(invalid_data(RUN_INVALID));
        }
        let levels = std::mem::take(&mut self.levels);
        let mut runs = levels.into_iter().flatten().collect::<Vec<_>>();
        while runs.len() > self.merge_fan_in {
            let group = runs.drain(..self.merge_fan_in).collect::<Vec<_>>();
            runs.push(self.merge_runs(group, progress)?);
        }
        if self.merged_records != self.expected_merge_records {
            return Err(invalid_data(RUN_INVALID));
        }
        let summary = self.verify_runs(&runs, progress)?;
        let verify_total = self.expected_records;
        for run in &runs {
            let mut poll = || {
                progress(
                    TaxIdentityCollisionAuditPhase::Verify,
                    verify_total,
                    verify_total,
                )
            };
            self.scratch_ref()?.remove_run(run, &mut poll)?;
            self.budget.release(run.byte_count())?;
        }
        if self.budget.used() != 0 {
            return Err(invalid_data(RUN_INVALID));
        }
        Ok(CollisionSortResult {
            summary,
            initial_run_count: self.initial_run_count,
            merge_operation_count: self.merge_operation_count,
            peak_scratch_bytes: self.budget.peak(),
            maximum_merge_fan_in: self.maximum_merge_fan_in,
        })
    }

    fn spill_chunk(&mut self, progress: &mut AuditProgressCallback<'_>) -> io::Result<()> {
        self.records.sort_unstable();
        let record_count =
            u64::try_from(self.records.len()).map_err(|_| invalid_data(COUNT_OVERFLOW))?;
        let byte_count = checked_record_bytes(record_count)?;
        self.budget.reserve(byte_count)?;
        let (pending_run, file) = self
            .scratch_ref()?
            .create_run(ScratchRunKind::Initial, byte_count)?;
        let mut writer = BufWriter::with_capacity(IO_BUFFER_BYTES, file);
        let spill_base = self.spilled_records;
        for (index, record) in self.records.iter().enumerate() {
            writer
                .write_all(&record.encode())
                .map_err(|_| invalid_data(RUN_INVALID))?;
            poll_records(
                progress,
                TaxIdentityCollisionAuditPhase::Spill,
                checked_add(spill_base, index as u64 + 1)?,
                self.expected_records,
            )?;
        }
        writer.flush().map_err(|_| invalid_data(RUN_INVALID))?;
        if writer
            .get_ref()
            .metadata()
            .map_err(|_| invalid_data(RUN_INVALID))?
            .len()
            != byte_count
        {
            return Err(invalid_data(RUN_INVALID));
        }
        let mut file = writer.into_inner().map_err(|_| invalid_data(RUN_INVALID))?;
        let spill_completed = checked_add(spill_base, record_count)?;
        let mut poll = || {
            progress(
                TaxIdentityCollisionAuditPhase::Spill,
                spill_completed,
                self.expected_records,
            )
        };
        let run = self
            .scratch_ref()?
            .seal_run(pending_run, &mut file, &mut poll)?;
        drop(file);
        self.spilled_records = checked_add(self.spilled_records, record_count)?;
        self.records.clear();
        self.initial_run_count = checked_increment(self.initial_run_count)?;
        self.push_run(0, run, progress)
    }

    fn push_run(
        &mut self,
        mut level: usize,
        mut run: ScratchRun,
        progress: &mut AuditProgressCallback<'_>,
    ) -> io::Result<()> {
        loop {
            if self.levels.len() == level {
                self.levels.push(Vec::with_capacity(self.merge_fan_in));
            }
            self.levels[level].push(run);
            if self.levels[level].len() < self.merge_fan_in {
                return Ok(());
            }
            let group = std::mem::replace(
                &mut self.levels[level],
                Vec::with_capacity(self.merge_fan_in),
            );
            run = self.merge_runs(group, progress)?;
            level = level
                .checked_add(1)
                .ok_or_else(|| invalid_data(COUNT_OVERFLOW))?;
        }
    }

    fn merge_runs(
        &mut self,
        runs: Vec<ScratchRun>,
        progress: &mut AuditProgressCallback<'_>,
    ) -> io::Result<ScratchRun> {
        if runs.len() < 2 || runs.len() > self.merge_fan_in {
            return Err(invalid_data(RUN_INVALID));
        }
        self.maximum_merge_fan_in = self.maximum_merge_fan_in.max(runs.len());
        let byte_count = sum_run_bytes(&runs)?;
        self.budget.reserve(byte_count)?;
        let (pending_output_run, output_file) = self
            .scratch_ref()?
            .create_run(ScratchRunKind::Merge, byte_count)?;
        let mut writer = BufWriter::with_capacity(IO_BUFFER_BYTES, output_file);
        let merge_base = self.merged_records;
        let merged_records = merge_sorted_runs(
            self.scratch_ref()?,
            &runs,
            MergeSink::Writer(&mut writer),
            MergeProgress {
                phase: TaxIdentityCollisionAuditPhase::Merge,
                base: merge_base,
                total: self.expected_merge_records,
            },
            progress,
        )?;
        if checked_record_bytes(merged_records)? != byte_count {
            return Err(invalid_data(RUN_INVALID));
        }
        writer.flush().map_err(|_| invalid_data(RUN_INVALID))?;
        if writer
            .get_ref()
            .metadata()
            .map_err(|_| invalid_data(RUN_INVALID))?
            .len()
            != byte_count
        {
            return Err(invalid_data(RUN_INVALID));
        }
        let mut output_file = writer.into_inner().map_err(|_| invalid_data(RUN_INVALID))?;
        let merge_completed = checked_add(merge_base, merged_records)?;
        let mut seal_poll = || {
            progress(
                TaxIdentityCollisionAuditPhase::Merge,
                merge_completed,
                self.expected_merge_records,
            )
        };
        let output_run =
            self.scratch_ref()?
                .seal_run(pending_output_run, &mut output_file, &mut seal_poll)?;
        drop(output_file);
        self.merged_records = checked_add(self.merged_records, merged_records)?;
        if self.merged_records > self.expected_merge_records {
            return Err(invalid_data(RUN_INVALID));
        }
        for run in &runs {
            let mut remove_poll = || {
                progress(
                    TaxIdentityCollisionAuditPhase::Merge,
                    merge_completed,
                    self.expected_merge_records,
                )
            };
            self.scratch_ref()?.remove_run(run, &mut remove_poll)?;
            self.budget.release(run.byte_count())?;
        }
        self.merge_operation_count = checked_increment(self.merge_operation_count)?;
        Ok(output_run)
    }

    fn verify_runs(
        &self,
        runs: &[ScratchRun],
        progress: &mut AuditProgressCallback<'_>,
    ) -> io::Result<CollisionAuditSummary> {
        let mut accumulator = CollisionAuditAccumulator::new(self.expected_records);
        if self.expected_records == 0 {
            progress(TaxIdentityCollisionAuditPhase::Verify, 0, 0)?;
            return accumulator.finish();
        }
        let record_count = merge_sorted_runs(
            self.scratch_ref()?,
            runs,
            MergeSink::Accumulator(&mut accumulator),
            MergeProgress {
                phase: TaxIdentityCollisionAuditPhase::Verify,
                base: 0,
                total: self.expected_records,
            },
            progress,
        )?;
        if record_count != self.expected_records {
            return Err(invalid_data(RUN_INVALID));
        }
        accumulator.finish()
    }

    fn scratch_ref(&self) -> io::Result<&PrivateScratch> {
        self.scratch
            .as_ref()
            .ok_or_else(|| invalid_data(RUN_INVALID))
    }
}

fn merge_sorted_runs(
    scratch: &PrivateScratch,
    runs: &[ScratchRun],
    mut sink: MergeSink<'_>,
    tracker: MergeProgress,
    progress: &mut AuditProgressCallback<'_>,
) -> io::Result<u64> {
    if runs.is_empty() {
        return Ok(0);
    }
    let mut readers = runs
        .iter()
        .map(|run| {
            let mut poll = || progress(tracker.phase, tracker.base, tracker.total);
            scratch
                .open_run(run, &mut poll)
                .map(|file| BufReader::with_capacity(IO_BUFFER_BYTES, file))
        })
        .collect::<io::Result<Vec<_>>>()?;
    let mut previous_by_run = vec![None; readers.len()];
    let mut heap = BinaryHeap::with_capacity(readers.len());
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(record) = read_record(reader)? {
            previous_by_run[index] = Some(record);
            heap.push(Reverse((record, index)));
        }
    }
    let expected_records = sum_run_bytes(runs)? / TAX_IDENTITY_COLLISION_AUDIT_RECORD_BYTES as u64;
    let mut output_records = 0u64;
    while let Some(Reverse((record, run_index))) = heap.pop() {
        match &mut sink {
            MergeSink::Writer(output) => output
                .write_all(&record.encode())
                .map_err(|_| invalid_data(RUN_INVALID))?,
            MergeSink::Accumulator(audit) => audit.observe(record)?,
        }
        output_records = checked_increment(output_records)?;
        poll_records(
            progress,
            tracker.phase,
            checked_add(tracker.base, output_records)?,
            tracker.total,
        )?;
        if let Some(next) = read_record(&mut readers[run_index])? {
            if previous_by_run[run_index].is_some_and(|previous| next < previous) {
                return Err(invalid_data(RUN_INVALID));
            }
            previous_by_run[run_index] = Some(next);
            heap.push(Reverse((next, run_index)));
        }
    }
    if output_records != expected_records {
        return Err(invalid_data(RUN_INVALID));
    }
    for (reader, run) in readers.iter_mut().zip(runs) {
        let completed = checked_add(tracker.base, output_records)?;
        let mut poll = || progress(tracker.phase, completed, tracker.total);
        scratch.reauthenticate_run(reader.get_mut(), run, &mut poll)?;
    }
    Ok(output_records)
}

fn read_record(reader: &mut impl Read) -> io::Result<Option<CollisionAuditRecord>> {
    let mut encoded = [0u8; TAX_IDENTITY_COLLISION_AUDIT_RECORD_BYTES];
    let mut offset = 0usize;
    while offset < encoded.len() {
        match reader.read(&mut encoded[offset..]) {
            Ok(0) if offset == 0 => return Ok(None),
            Ok(0) => return Err(invalid_data(RUN_INVALID)),
            Ok(count) => offset += count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => return Err(invalid_data(RUN_INVALID)),
        }
    }
    CollisionAuditRecord::decode(encoded).map(Some)
}

fn poll_records(
    progress: &mut AuditProgressCallback<'_>,
    phase: TaxIdentityCollisionAuditPhase,
    completed: u64,
    total: u64,
) -> io::Result<()> {
    if completed == total || completed.is_multiple_of(POLL_RECORD_INTERVAL) {
        progress(phase, completed, total)?;
    }
    Ok(())
}

fn checked_record_bytes(record_count: u64) -> io::Result<u64> {
    record_count
        .checked_mul(TAX_IDENTITY_COLLISION_AUDIT_RECORD_BYTES as u64)
        .ok_or_else(|| invalid_data(COUNT_OVERFLOW))
}

fn sum_run_bytes(runs: &[ScratchRun]) -> io::Result<u64> {
    runs.iter().try_fold(0u64, |total, run| {
        if run.byte_count() % TAX_IDENTITY_COLLISION_AUDIT_RECORD_BYTES as u64 != 0 {
            return Err(invalid_data(RUN_INVALID));
        }
        total
            .checked_add(run.byte_count())
            .ok_or_else(|| invalid_data(COUNT_OVERFLOW))
    })
}

fn checked_increment(value: u64) -> io::Result<u64> {
    value
        .checked_add(1)
        .ok_or_else(|| invalid_data(COUNT_OVERFLOW))
}

fn checked_add(left: u64, right: u64) -> io::Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| invalid_data(COUNT_OVERFLOW))
}
