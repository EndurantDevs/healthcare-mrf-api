use super::super::contracts::TaxIdentityCollisionAuditLimits;
use super::super::{invalid_data, TAX_IDENTITY_COLLISION_AUDIT_RECORD_BYTES};
use std::io;

const IO_BUFFER_BYTES: u64 = 64 * 1024;
const MAX_MERGE_FAN_IN: usize = 64;
const MAX_RUN_LEVELS: u64 = u64::BITS as u64;
const MAX_ARTIFACTS: usize = 4096;
const MAX_CHUNK_RECORDS: u64 = 4096;
// Covers a live run Vec slot, its cloned <=96-byte private name, and
// conservative scratch-tracker hash-bucket/allocation overhead.
const RUN_METADATA_UPPER_BOUND_BYTES: u64 = 512;
// Covers ordered descriptor refs plus path and physical-key hash buckets.
const ARTIFACT_TRACKING_UPPER_BOUND_BYTES: u64 = 128;
const HEAP_ENTRY_UPPER_BOUND_BYTES: u64 = 128;

#[cfg(test)]
mod invariant_tests;

const INVALID_LIMITS: &str = "PTG tax identity collision audit limits are invalid";
const ARTIFACT_LIMIT_EXCEEDED: &str = "PTG tax identity collision audit artifact limit exceeded";
const ROW_LIMIT_EXCEEDED: &str = "PTG tax identity collision audit row limit exceeded";
const MEMORY_LIMIT_EXCEEDED: &str = "PTG tax identity collision audit memory limit exceeded";
const SCRATCH_LIMIT_EXCEEDED: &str = "PTG tax identity collision audit scratch byte limit exceeded";
const COUNT_OVERFLOW: &str = "PTG tax identity collision audit count overflow";

pub(crate) struct CollisionSortPlan {
    pub(super) expected_records: u64,
    pub(super) expected_merge_records: u64,
    pub(super) required_scratch_bytes: u64,
    pub(super) chunk_record_capacity: usize,
    pub(super) merge_fan_in: usize,
    pub(super) max_scratch_bytes: u64,
    pub(super) minimum_free_scratch_bytes: u64,
    artifact_capacity: usize,
}

impl CollisionSortPlan {
    pub(crate) fn preflight_artifact_count(
        artifact_count: usize,
        limits: TaxIdentityCollisionAuditLimits,
    ) -> io::Result<()> {
        if limits.max_artifacts == 0
            || limits.max_artifacts > MAX_ARTIFACTS
            || artifact_count == 0
            || artifact_count > limits.max_artifacts
        {
            return Err(invalid_data(ARTIFACT_LIMIT_EXCEEDED));
        }
        Ok(())
    }

    pub(crate) fn admit(
        source_rows: u64,
        matched_rows: u64,
        artifact_count: usize,
        limits: TaxIdentityCollisionAuditLimits,
    ) -> io::Result<Self> {
        validate_counts(source_rows, matched_rows, artifact_count, limits)?;
        let required_scratch_bytes = required_scratch_bytes(matched_rows, limits)?;
        let runtime_owned_bytes = runtime_owned_bytes(matched_rows, artifact_count, limits)?;
        let record_bytes = TAX_IDENTITY_COLLISION_AUDIT_RECORD_BYTES as u64;
        let available_chunk_bytes = limits
            .max_memory_bytes
            .checked_sub(runtime_owned_bytes)
            .ok_or_else(|| invalid_data(MEMORY_LIMIT_EXCEEDED))?;
        let chunk_records = (available_chunk_bytes / record_bytes)
            .min(matched_rows.max(1))
            .min(MAX_CHUNK_RECORDS);
        if matched_rows != 0 && chunk_records == 0 {
            return Err(invalid_data(MEMORY_LIMIT_EXCEEDED));
        }
        let chunk_record_capacity = if matched_rows == 0 {
            0
        } else {
            usize::try_from(chunk_records).map_err(|_| invalid_data(MEMORY_LIMIT_EXCEEDED))?
        };
        Ok(Self {
            expected_records: matched_rows,
            expected_merge_records: planned_merge_records(
                matched_rows,
                chunk_record_capacity,
                limits.merge_fan_in,
            )?,
            required_scratch_bytes,
            chunk_record_capacity,
            merge_fan_in: limits.merge_fan_in,
            max_scratch_bytes: limits.max_scratch_bytes,
            minimum_free_scratch_bytes: limits.minimum_free_scratch_bytes,
            artifact_capacity: limits.max_artifacts,
        })
    }

    pub(crate) const fn artifact_capacity(&self) -> usize {
        self.artifact_capacity
    }
}

fn validate_counts(
    source_rows: u64,
    matched_rows: u64,
    artifact_count: usize,
    limits: TaxIdentityCollisionAuditLimits,
) -> io::Result<()> {
    CollisionSortPlan::preflight_artifact_count(artifact_count, limits)?;
    if source_rows == 0
        || source_rows > limits.max_source_rows
        || matched_rows > source_rows
        || matched_rows > limits.max_matched_rows
    {
        return Err(invalid_data(ROW_LIMIT_EXCEEDED));
    }
    if !(2..=MAX_MERGE_FAN_IN).contains(&limits.merge_fan_in) {
        return Err(invalid_data(INVALID_LIMITS));
    }
    let required_open_files = if matched_rows == 0 {
        2
    } else {
        limits
            .merge_fan_in
            .checked_add(4)
            .ok_or_else(|| invalid_data(INVALID_LIMITS))?
    };
    if limits.max_open_files < required_open_files {
        return Err(invalid_data(INVALID_LIMITS));
    }
    Ok(())
}

fn required_scratch_bytes(
    matched_rows: u64,
    limits: TaxIdentityCollisionAuditLimits,
) -> io::Result<u64> {
    let required = matched_rows
        .checked_mul(TAX_IDENTITY_COLLISION_AUDIT_RECORD_BYTES as u64)
        .and_then(|value| value.checked_mul(2))
        .ok_or_else(|| invalid_data(COUNT_OVERFLOW))?;
    if required > limits.max_scratch_bytes {
        return Err(invalid_data(SCRATCH_LIMIT_EXCEEDED));
    }
    Ok(required)
}

fn runtime_owned_bytes(
    matched_rows: u64,
    artifact_count: usize,
    limits: TaxIdentityCollisionAuditLimits,
) -> io::Result<u64> {
    let artifacts = u64::try_from(artifact_count)
        .map_err(|_| invalid_data(COUNT_OVERFLOW))?
        .checked_mul(ARTIFACT_TRACKING_UPPER_BOUND_BYTES)
        .ok_or_else(|| invalid_data(COUNT_OVERFLOW))?;
    if matched_rows == 0 {
        return IO_BUFFER_BYTES
            .checked_add(artifacts)
            .ok_or_else(|| invalid_data(COUNT_OVERFLOW));
    }
    let fan_in = limits.merge_fan_in as u64;
    // Input readers + output writer + outer source reader + scratch
    // reauthentication buffer overlap during a scan-triggered merge.
    let buffers = fan_in
        .checked_add(3)
        .and_then(|value| value.checked_mul(IO_BUFFER_BYTES))
        .ok_or_else(|| invalid_data(COUNT_OVERFLOW))?;
    let heap = fan_in
        .checked_mul(HEAP_ENTRY_UPPER_BOUND_BYTES)
        .ok_or_else(|| invalid_data(COUNT_OVERFLOW))?;
    let active_runs = fan_in
        .checked_sub(1)
        .and_then(|value| value.checked_mul(MAX_RUN_LEVELS))
        .and_then(|value| value.checked_add(1))
        .ok_or_else(|| invalid_data(COUNT_OVERFLOW))?;
    let run_metadata = active_runs
        .checked_mul(RUN_METADATA_UPPER_BOUND_BYTES)
        .ok_or_else(|| invalid_data(COUNT_OVERFLOW))?;
    buffers
        .checked_add(heap)
        .and_then(|value| value.checked_add(run_metadata))
        .and_then(|value| value.checked_add(artifacts))
        .ok_or_else(|| invalid_data(COUNT_OVERFLOW))
}

fn planned_merge_records(
    expected_records: u64,
    chunk_record_capacity: usize,
    merge_fan_in: usize,
) -> io::Result<u64> {
    if expected_records == 0 {
        return Ok(0);
    }
    let chunk = u64::try_from(chunk_record_capacity).map_err(|_| invalid_data(COUNT_OVERFLOW))?;
    let fan_in = u64::try_from(merge_fan_in).map_err(|_| invalid_data(COUNT_OVERFLOW))?;
    if chunk == 0 || fan_in < 2 {
        return Err(invalid_data(INVALID_LIMITS));
    }
    let mut run_count = expected_records
        .checked_add(chunk - 1)
        .ok_or_else(|| invalid_data(COUNT_OVERFLOW))?
        / chunk;
    let mut regular_size = chunk;
    let mut last_size = match expected_records % chunk {
        0 => chunk,
        remainder => remainder,
    };
    let mut residual_runs = Vec::new();
    let mut total = 0u64;
    while run_count >= fan_in {
        let merged_count = run_count / fan_in;
        let residual_count = run_count % fan_in;
        let processed_count = run_count - residual_count;
        let processed = if residual_count == 0 {
            checked_add(
                (processed_count - 1)
                    .checked_mul(regular_size)
                    .ok_or_else(|| invalid_data(COUNT_OVERFLOW))?,
                last_size,
            )?
        } else {
            processed_count
                .checked_mul(regular_size)
                .ok_or_else(|| invalid_data(COUNT_OVERFLOW))?
        };
        total = checked_add(total, processed)?;
        append_residual_runs(&mut residual_runs, residual_count, regular_size, last_size)?;
        let next_regular = regular_size
            .checked_mul(fan_in)
            .ok_or_else(|| invalid_data(COUNT_OVERFLOW))?;
        last_size = if residual_count == 0 {
            checked_add(
                regular_size
                    .checked_mul(fan_in - 1)
                    .ok_or_else(|| invalid_data(COUNT_OVERFLOW))?,
                last_size,
            )?
        } else {
            next_regular
        };
        run_count = merged_count;
        regular_size = next_regular;
    }
    append_residual_runs(&mut residual_runs, run_count, regular_size, last_size)?;
    while residual_runs.len() > merge_fan_in {
        let merged = residual_runs
            .drain(..merge_fan_in)
            .try_fold(0u64, checked_add)?;
        total = checked_add(total, merged)?;
        residual_runs.push(merged);
    }
    Ok(total)
}

fn append_residual_runs(
    output: &mut Vec<u64>,
    count: u64,
    regular_size: u64,
    last_size: u64,
) -> io::Result<()> {
    if count == 0 {
        return Ok(());
    }
    let regular_count = usize::try_from(count - 1).map_err(|_| invalid_data(COUNT_OVERFLOW))?;
    output
        .try_reserve(regular_count.saturating_add(1))
        .map_err(|_| invalid_data(MEMORY_LIMIT_EXCEEDED))?;
    output.extend(std::iter::repeat_n(regular_size, regular_count));
    output.push(last_size);
    Ok(())
}

fn checked_add(left: u64, right: u64) -> io::Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| invalid_data(COUNT_OVERFLOW))
}
