use super::{
    emit_v3_partition_progress, sha256_hex, try_zeroed_u64_vec, V3AssignedPartition,
    V3AssignmentContext, V3FinalizerInputs, V3FinalizerOptions, V3FinalizerPartitionInput,
    V3ScratchBytes, V3_FINALIZER_ASSIGNED_BYTES,
};
use ptg2_scanner::v3_runs::{partition_for_record, AssignedServingRunBuilder, ServingRunRecord};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::fs::File;
use std::io::{self, BufReader};
use std::path::PathBuf;
use std::time::Instant;

/// Validate all listed input roles before any worker can consume a serving file.
pub(super) fn preflight_owned_serving_inputs(
    options: &V3FinalizerOptions,
    inputs: &mut V3FinalizerInputs,
) -> io::Result<()> {
    let retained_paths = options
        .manifest_paths
        .iter()
        .chain(std::iter::once(&options.price_key_map_input))
        .chain(&options.price_membership_inputs)
        .chain(&options.price_atom_inputs)
        .chain(inputs.code_dictionaries.iter().map(|input| &input.path))
        .chain(inputs.provider_metadata.iter().map(|input| &input.path))
        .map(std::fs::canonicalize)
        .collect::<io::Result<HashSet<PathBuf>>>()?;
    let mut seen_paths = HashSet::new();
    for input in &mut inputs.partitions {
        if !std::fs::symlink_metadata(&input.path)?
            .file_type()
            .is_file()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "owned serving input must be a regular file, not a symlink",
            ));
        }
        let canonical_path = std::fs::canonicalize(&input.path)?;
        if retained_paths.contains(&canonical_path) || !seen_paths.insert(canonical_path.clone()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "owned serving input aliases another serving or retained input",
            ));
        }
        input.path = canonical_path;
    }
    Ok(())
}

pub(super) fn assign_v3_partition(
    partition: usize,
    inputs: &[V3FinalizerPartitionInput],
    context: &V3AssignmentContext<'_>,
) -> io::Result<V3AssignedPartition> {
    let started_at = Instant::now();
    let partition_directory = context.work_root.join(format!("partition-{partition:03}"));
    std::fs::create_dir_all(&partition_directory)?;
    let mut assigned_runs = AssignedServingRunBuilder::with_scratch_durability(
        &partition_directory,
        context.assigned_record_limit,
        context.scratch_durability,
    )?;
    let &(code_key_start, partition_code_count) = context
        .code_partition_ranges
        .get(partition)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing code partition"))?;
    if partition_code_count == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "serving-run partition has rows but no code dictionary range",
        ));
    }
    let mut code_rate_counts =
        try_zeroed_u64_vec(partition_code_count, "partition code rate counts")?;
    let mut provider_seen_words = try_zeroed_u64_vec(
        context.provider_key_count.saturating_add(63) / 64,
        "partition provider coverage bitmap",
    )?;
    let mut price_seen_words = try_zeroed_u64_vec(
        context.price_key_count.saturating_add(63) / 64,
        "partition price coverage bitmap",
    )?;
    let mut row_count = 0u64;
    let mut source_bytes_read = 0u64;
    for input in inputs {
        let mut reader = BufReader::new(File::open(&input.path)?);
        let mut input_digest = Sha256::new();
        let mut input_rows = 0u64;
        while let Some(record) = ServingRunRecord::read_from(&mut reader)? {
            input_digest.update(record.encode());
            let actual_partition = partition_for_record(&record, context.partition_count)?;
            if actual_partition != partition {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "serving run record belongs to partition {actual_partition}, expected {}",
                        partition
                    ),
                ));
            }
            let code = context.code_map.get(&record.code_id).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "assigned code identity is absent",
                )
            })?;
            let provider = context
                .provider_map
                .get(&record.provider_set_id)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "assigned provider identity is absent",
                    )
                })?;
            if provider.auxiliary != record.provider_count {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "assigned provider identity/count conflicts with immutable dense map",
                ));
            }
            let price = context.price_map.get(&record.price_set_id).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "assigned price identity is absent",
                )
            })?;
            let provider_key = provider.key as usize;
            if provider_key >= context.provider_key_count {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "assigned provider key is outside the authoritative map",
                ));
            }
            provider_seen_words[provider_key / 64] |= 1u64 << (provider_key % 64);
            let price_key = price.key as usize;
            if price_key >= context.price_key_count {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "assigned price key is outside the authoritative map",
                ));
            }
            price_seen_words[price_key / 64] |= 1u64 << (price_key % 64);
            let relative_code_key = code
                .key
                .checked_sub(code_key_start)
                .and_then(|value| usize::try_from(value).ok())
                .filter(|value| *value < code_rate_counts.len())
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "code identity is outside its leading-bit partition range",
                    )
                })?;
            let code_count = &mut code_rate_counts[relative_code_key];
            *code_count = code_count.checked_add(1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "code rate_count overflow")
            })?;
            let mut assigned_record = [0u8; V3_FINALIZER_ASSIGNED_BYTES];
            assigned_record[0..4].copy_from_slice(&code.key.to_be_bytes());
            assigned_record[4..8].copy_from_slice(&provider.key.to_be_bytes());
            assigned_record[8..12].copy_from_slice(&price.key.to_be_bytes());
            assigned_record[12..16].copy_from_slice(&input.source_key.to_be_bytes());
            assigned_record[16..20].copy_from_slice(&record.provider_count.to_be_bytes());
            assigned_runs.push(assigned_record)?;
            row_count = row_count.checked_add(1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "partition row_count overflow")
            })?;
            input_rows = input_rows.checked_add(1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "input row_count overflow")
            })?;
        }
        if input_rows != input.row_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "serving run row count changed during authenticated assignment for {}",
                    input.path.display()
                ),
            ));
        }
        let actual_digest: [u8; 32] = input_digest.finalize().into();
        if actual_digest != input.sha256 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "serving run content digest mismatch during assignment scan for {}: expected {}, got {}",
                    input.path.display(),
                    sha256_hex(&input.sha256),
                    sha256_hex(&actual_digest),
                ),
            ));
        }
        source_bytes_read = source_bytes_read.saturating_add(input.bytes);
    }
    let expected_rows = inputs.iter().map(|input| input.row_count).sum::<u64>();
    if row_count != expected_rows {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "partition assignment changed the source multiset",
        ));
    }
    {
        let mut combined = context
            .combined_provider_seen_words
            .lock()
            .map_err(|_| io::Error::other("combined provider coverage bitmap is poisoned"))?;
        if combined.len() != provider_seen_words.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "partition provider coverage bitmap length mismatch",
            ));
        }
        for (target, value) in combined.iter_mut().zip(provider_seen_words) {
            *target |= value;
        }
    }
    {
        let mut combined = context
            .combined_price_seen_words
            .lock()
            .map_err(|_| io::Error::other("combined price coverage bitmap is poisoned"))?;
        if combined.len() != price_seen_words.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "partition price coverage bitmap length mismatch",
            ));
        }
        for (target, value) in combined.iter_mut().zip(price_seen_words) {
            *target |= value;
        }
    }
    let assigned_run_set = assigned_runs.finish()?;
    let sync_stats = assigned_run_set.sync_stats;
    let mut sort_stats = assigned_run_set.stats;
    sort_stats.input_file_count = inputs.len() as u64;
    if sort_stats.input_records != row_count
        || sort_stats.unique_records != row_count
        || sort_stats.output_bytes != row_count.saturating_mul(V3_FINALIZER_ASSIGNED_BYTES as u64)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "partition-local assignment sort did not preserve every source occurrence",
        ));
    }
    if context.consume_serving_inputs {
        // Readers are closed and every assigned source occurrence is authenticated.
        for input in inputs {
            std::fs::remove_file(&input.path)?;
        }
    }
    let scratch = V3ScratchBytes {
        read: source_bytes_read.saturating_add(sort_stats.final_copy_bytes),
        written: sort_stats.spill_bytes,
    };
    let elapsed_seconds = started_at.elapsed().as_secs_f64();
    emit_v3_partition_progress(
        "assign_sort",
        partition,
        context.partition_count,
        row_count,
        elapsed_seconds,
        scratch,
    );
    Ok(V3AssignedPartition {
        partition,
        assigned_paths: assigned_run_set.paths,
        row_count,
        code_key_start,
        code_rate_counts,
        sort_stats,
        elapsed_seconds,
        scratch,
        sync_stats,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{V3FinalizerCodeDictionaryInput, V3FinalizerProviderMetadataInput};
    use ptg2_scanner::v3_dense::{DenseIdentityMap, DenseIdentityValue};
    use ptg2_scanner::v3_runs::{
        AssignedServingRunMerger, ScratchDurability, TaggedServingRunCodec,
    };
    use std::sync::Mutex;

    #[test]
    fn owned_preflight_rejects_aliases_before_consumption() {
        let temporary = tempfile::tempdir().unwrap();
        let serving = temporary.path().join("serving.ready");
        let retained = temporary.path().join("retained.copy");
        std::fs::write(&serving, b"serving").unwrap();
        std::fs::write(&retained, b"retained").unwrap();
        let partition = V3FinalizerPartitionInput {
            path: serving.clone(),
            partition: 0,
            source_key: 0,
            row_count: 1,
            bytes: 7,
            sha256: [0; 32],
        };
        let mut inputs = V3FinalizerInputs {
            partitions: vec![partition.clone()],
            code_dictionaries: vec![V3FinalizerCodeDictionaryInput {
                path: retained.clone(),
                row_count: 1,
                bytes: 8,
                sha256: [0; 32],
            }],
            provider_metadata: vec![V3FinalizerProviderMetadataInput {
                path: retained.clone(),
                source_key: 0,
                row_count: 1,
                bytes: 8,
                sha256: [0; 32],
            }],
            manifest_bytes: 0,
            partition_count: 1,
            source_count: 1,
            source_key_bits: 0,
            source_key_bytes: 0,
            tagged_record_bytes: 52,
            tagged_codec: TaggedServingRunCodec::new(1, 0).unwrap(),
        };
        let options = V3FinalizerOptions {
            output_directory: temporary.path().join("output"),
            manifest_paths: vec![retained.clone()],
            consume_serving_inputs: true,
            scratch_durability: ScratchDurability::Ephemeral,
            total_sort_memory_bytes: 1,
            workers: 1,
            identity_map_max_bytes: 1,
            price_key_map_input: retained.clone(),
            price_key_map_row_count: 1,
            price_membership_inputs: vec![retained.clone()],
            price_atom_inputs: vec![retained.clone()],
        };
        let alias_directory = temporary.path().join("alias");
        std::fs::create_dir(&alias_directory).unwrap();
        let mut alias = partition.clone();
        alias.path = alias_directory.join("..").join("serving.ready");
        inputs.partitions.push(alias);
        assert!(preflight_owned_serving_inputs(&options, &mut inputs).is_err());
        assert!(serving.is_file());
        inputs.partitions = vec![partition.clone()];
        for role in 0..6 {
            let mut conflicting = options.clone();
            let mut conflicting_inputs = inputs.clone();
            match role {
                0 => conflicting.manifest_paths = vec![serving.clone()],
                1 => conflicting.price_key_map_input = serving.clone(),
                2 => conflicting.price_membership_inputs = vec![serving.clone()],
                3 => conflicting.price_atom_inputs = vec![serving.clone()],
                4 => conflicting_inputs.code_dictionaries[0].path = serving.clone(),
                _ => conflicting_inputs.provider_metadata[0].path = serving.clone(),
            }
            assert!(preflight_owned_serving_inputs(&conflicting, &mut conflicting_inputs).is_err());
            assert!(serving.is_file());
        }
        #[cfg(unix)]
        {
            let symlink = temporary.path().join("symlink.ready");
            std::os::unix::fs::symlink(&serving, &symlink).unwrap();
            inputs.partitions[0].path = symlink;
            assert!(preflight_owned_serving_inputs(&options, &mut inputs).is_err());
        }
        let hardlink = temporary.path().join("hardlink.ready");
        std::fs::hard_link(&serving, &hardlink).unwrap();
        let mut occurrence = partition.clone();
        occurrence.path = hardlink.clone();
        inputs.partitions = vec![partition, occurrence];
        preflight_owned_serving_inputs(&options, &mut inputs).unwrap();
        assert_eq!(inputs.partitions.len(), 2);
        assert_ne!(inputs.partitions[0].path, inputs.partitions[1].path);
        assert!(serving.is_file() && hardlink.is_file() && retained.is_file());
        assert!(!options.output_directory.exists());
    }

    #[test]
    fn owned_assignment_preserves_occurrences_and_keeps_failed_inputs() {
        let temporary = tempfile::tempdir().unwrap();
        let record = ServingRunRecord {
            code_id: [0; 16],
            provider_set_id: [1; 16],
            price_set_id: [2; 16],
            provider_count: 2,
        };
        let serving = temporary.path().join("serving.ready");
        let hardlink = temporary.path().join("hardlink.ready");
        let witness = temporary.path().join("witness.bin");
        std::fs::write(&serving, record.encode()).unwrap();
        std::fs::hard_link(&serving, &hardlink).unwrap();
        std::fs::write(&witness, b"retained witness").unwrap();
        let inputs: Vec<_> = [serving.clone(), hardlink.clone()]
            .into_iter()
            .map(|path| V3FinalizerPartitionInput {
                path,
                partition: 0,
                source_key: 0,
                row_count: 1,
                bytes: record.encode().len() as u64,
                sha256: Sha256::digest(record.encode()).into(),
            })
            .collect();
        let mut code_map = DenseIdentityMap::with_capacity(1).unwrap();
        let mut provider_map = DenseIdentityMap::with_capacity(1).unwrap();
        let mut price_map = DenseIdentityMap::with_capacity(1).unwrap();
        code_map
            .insert(
                record.code_id,
                DenseIdentityValue {
                    key: 0,
                    auxiliary: 0,
                },
            )
            .unwrap();
        provider_map
            .insert(
                record.provider_set_id,
                DenseIdentityValue {
                    key: 0,
                    auxiliary: 2,
                },
            )
            .unwrap();
        price_map
            .insert(
                record.price_set_id,
                DenseIdentityValue {
                    key: 0,
                    auxiliary: 0,
                },
            )
            .unwrap();
        let combined_provider = Mutex::new(vec![0u64]);
        let combined_price = Mutex::new(vec![0u64]);
        let ranges = [(0, 1)];
        for owned in [false, true] {
            let work = temporary.path().join(format!("failed-{owned}"));
            let mut context = V3AssignmentContext {
                partition_count: 1,
                work_root: &work,
                code_map: &code_map,
                code_partition_ranges: &ranges,
                provider_map: &provider_map,
                provider_key_count: 1,
                price_key_count: 1,
                price_map: &price_map,
                combined_provider_seen_words: &combined_provider,
                combined_price_seen_words: &combined_price,
                assigned_record_limit: 1,
                consume_serving_inputs: owned,
                scratch_durability: ScratchDurability::Ephemeral,
            };
            let mut bad_inputs = inputs.clone();
            bad_inputs[1].sha256 = [0; 32];
            let error = assign_v3_partition(0, &bad_inputs, &context).unwrap_err();
            assert!(error
                .to_string()
                .contains("digest mismatch during assignment"));
            assert!(serving.is_file() && hardlink.is_file());
            let successful_work = temporary.path().join(format!("success-{owned}"));
            context.work_root = &successful_work;
            let assigned = assign_v3_partition(0, &inputs, &context).unwrap();
            assert_eq!(assigned.row_count, 2);
            assert_eq!(assigned.sort_stats.input_records, 2);
            assert_eq!(assigned.sort_stats.unique_records, 2);
            let mut merger = AssignedServingRunMerger::new(&assigned.assigned_paths).unwrap();
            let first = merger.next_record().unwrap().unwrap();
            assert_eq!(merger.next_record().unwrap(), Some(first));
            assert!(merger.next_record().unwrap().is_none());
            assert_eq!(serving.is_file(), !owned);
            assert_eq!(hardlink.is_file(), !owned);
            assert!(witness.is_file() && temporary.path().is_dir());
        }
    }

    #[test]
    fn owned_partition_failure_requires_fresh_input_retry() {
        let temporary = tempfile::tempdir().unwrap();
        let records = [
            ServingRunRecord {
                code_id: [0; 16],
                provider_set_id: [1; 16],
                price_set_id: [2; 16],
                provider_count: 2,
            },
            ServingRunRecord {
                code_id: [128; 16],
                provider_set_id: [1; 16],
                price_set_id: [2; 16],
                provider_count: 2,
            },
        ];
        let mut code_map = DenseIdentityMap::with_capacity(2).unwrap();
        for (partition, record) in records.iter().enumerate() {
            assert_eq!(partition_for_record(record, 2).unwrap(), partition);
            code_map
                .insert(
                    record.code_id,
                    DenseIdentityValue {
                        key: partition as u32,
                        auxiliary: 0,
                    },
                )
                .unwrap();
        }
        let mut provider_map = DenseIdentityMap::with_capacity(1).unwrap();
        provider_map
            .insert(
                records[0].provider_set_id,
                DenseIdentityValue {
                    key: 0,
                    auxiliary: 2,
                },
            )
            .unwrap();
        let mut price_map = DenseIdentityMap::with_capacity(1).unwrap();
        price_map
            .insert(
                records[0].price_set_id,
                DenseIdentityValue {
                    key: 0,
                    auxiliary: 0,
                },
            )
            .unwrap();
        let ranges = [(0, 1), (1, 1)];
        for fail_later in [true, false] {
            let attempt = temporary
                .path()
                .join(if fail_later { "failed" } else { "fresh" });
            std::fs::create_dir(&attempt).unwrap();
            let witness = attempt.join("witness.bin");
            std::fs::write(&witness, b"retained witness").unwrap();
            let inputs: Vec<_> = records
                .iter()
                .enumerate()
                .map(|(partition, record)| {
                    let path = attempt.join(format!("source-{partition}.ready"));
                    std::fs::write(&path, record.encode()).unwrap();
                    V3FinalizerPartitionInput {
                        path,
                        partition,
                        source_key: 0,
                        row_count: if fail_later && partition == 1 { 2 } else { 1 },
                        bytes: record.encode().len() as u64,
                        sha256: Sha256::digest(record.encode()).into(),
                    }
                })
                .collect();
            let work = attempt.join("assigned");
            let combined_provider = Mutex::new(vec![0u64]);
            let combined_price = Mutex::new(vec![0u64]);
            let context = V3AssignmentContext {
                partition_count: 2,
                work_root: &work,
                code_map: &code_map,
                code_partition_ranges: &ranges,
                provider_map: &provider_map,
                provider_key_count: 1,
                price_key_count: 1,
                price_map: &price_map,
                combined_provider_seen_words: &combined_provider,
                combined_price_seen_words: &combined_price,
                assigned_record_limit: 1,
                consume_serving_inputs: true,
                scratch_durability: ScratchDurability::Ephemeral,
            };
            for partition in 0..2 {
                let result = assign_v3_partition(
                    partition,
                    std::slice::from_ref(&inputs[partition]),
                    &context,
                );
                if fail_later && partition == 1 {
                    assert!(result
                        .unwrap_err()
                        .to_string()
                        .contains("row count changed during authenticated assignment"));
                    assert!(!inputs[0].path.exists());
                    assert!(inputs[1].path.is_file());
                    assert_eq!(std::fs::read(&inputs[1].path).unwrap(), records[1].encode());
                } else {
                    let assigned = result.unwrap();
                    assert_eq!(assigned.row_count, 1);
                    assert_eq!(assigned.sort_stats.input_records, 1);
                    assert_eq!(assigned.sort_stats.unique_records, 1);
                    let mut merger =
                        AssignedServingRunMerger::new(&assigned.assigned_paths).unwrap();
                    let mut expected = [0u8; V3_FINALIZER_ASSIGNED_BYTES];
                    expected[..4].copy_from_slice(&(partition as u32).to_be_bytes());
                    expected[16..20].copy_from_slice(&2u32.to_be_bytes());
                    assert_eq!(merger.next_record().unwrap(), Some(expected));
                    assert!(merger.next_record().unwrap().is_none());
                    assert!(!inputs[partition].path.exists());
                }
                assert_eq!(std::fs::read(&witness).unwrap(), b"retained witness");
                assert!(attempt.is_dir());
            }
            if !fail_later {
                assert!(inputs.iter().all(|input| !input.path.exists()));
            }
        }
    }
}
