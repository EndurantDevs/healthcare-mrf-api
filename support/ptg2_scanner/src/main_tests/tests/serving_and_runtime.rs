use super::*;

#[test]
fn v3_price_key_map_rejects_malformed_and_noncontiguous_rows() {
    let base = std::env::temp_dir().join(format!(
        "ptg2-direct-v3-finalizer-price-map-validation-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let id_a = prefixed_test_id(2, 1);
    let id_b = prefixed_test_id(2, 2);

    #[derive(Debug)]
    struct ValidatedPriceMap {
        mapped_a: Option<DenseIdentityValue>,
        mapped_b: Option<DenseIdentityValue>,
        summary: Value,
        output: Vec<u8>,
    }

    let validate = |label: &str,
                    rows: Vec<Vec<Option<Vec<u8>>>>,
                    expected_rows: u64|
     -> io::Result<ValidatedPriceMap> {
        let input = base.join(format!("{label}.copy"));
        std::fs::write(&input, pg_binary_copy_rows(&rows))?;
        let mut writer = CountingWriter::new(Vec::new());
        let (map, stage, summary) =
            load_v3_price_key_map_and_write_dictionary(&input, expected_rows, &mut writer)?;
        assert_eq!(stage.row_count, expected_rows);
        Ok(ValidatedPriceMap {
            mapped_a: map.get(&id_a),
            mapped_b: map.get(&id_b),
            summary,
            output: writer.inner,
        })
    };

    let valid = validate(
        "valid-key-order-with-nonmonotonic-ids",
        vec![
            vec![Some(id_b.to_vec()), pg_i64_field(0)],
            vec![Some(id_a.to_vec()), pg_i64_field(1)],
        ],
        2,
    )
    .unwrap();
    assert_eq!(valid.mapped_b.unwrap().key, 0);
    assert_eq!(valid.mapped_a.unwrap().key, 1);
    assert_eq!(valid.summary["price_set_count"], 2);

    let mut fixed_input = Vec::new();
    for (price_key, price_set_id) in [id_b, id_a].iter().enumerate() {
        fixed_input.extend_from_slice(&(price_key as u32).to_be_bytes());
        fixed_input.extend_from_slice(price_set_id);
    }
    let mut fixed_reader = Cursor::new(fixed_input);
    let mut fixed_writer = CountingWriter::new(Vec::new());
    let fixed_summary = write_serving_binary_v3_price_dictionary_copy_from_fixed_reader(
        &mut fixed_reader,
        &mut fixed_writer,
        ServingBinaryTargetCopyFormat::SharedBinary,
        V3_FINALIZER_HOT_BLOCK_BYTES,
    )
    .unwrap();
    assert_eq!(valid.output, fixed_writer.inner);
    assert_eq!(valid.summary["storage"], fixed_summary["storage"]);

    let malformed = validate(
        "malformed-int4",
        vec![vec![Some(id_a.to_vec()), pg_i32_field(0)]],
        1,
    )
    .unwrap_err();
    assert!(malformed.to_string().contains("must be int8"));

    let swapped = validate(
        "swapped-keys",
        vec![
            vec![Some(id_a.to_vec()), pg_i64_field(1)],
            vec![Some(id_b.to_vec()), pg_i64_field(0)],
        ],
        2,
    )
    .unwrap_err();
    assert!(swapped.to_string().contains("expected 0, got 1"));

    let gap = validate(
        "noncontiguous-keys",
        vec![
            vec![Some(id_a.to_vec()), pg_i64_field(0)],
            vec![Some(id_b.to_vec()), pg_i64_field(2)],
        ],
        2,
    )
    .unwrap_err();
    assert!(gap.to_string().contains("expected 1, got 2"));

    let duplicate_key = validate(
        "duplicate-keys",
        vec![
            vec![Some(id_a.to_vec()), pg_i64_field(0)],
            vec![Some(id_b.to_vec()), pg_i64_field(0)],
        ],
        2,
    )
    .unwrap_err();
    assert!(duplicate_key.to_string().contains("expected 1, got 0"));

    let duplicate_id = validate(
        "duplicate-id",
        vec![
            vec![Some(id_a.to_vec()), pg_i64_field(0)],
            vec![Some(id_a.to_vec()), pg_i64_field(1)],
        ],
        2,
    )
    .unwrap_err();
    assert!(duplicate_id
        .to_string()
        .contains("duplicate identity in immutable dense map"));

    let zero_id = validate(
        "zero-id",
        vec![vec![Some(vec![0; GLOBAL_ID_BYTES]), pg_i64_field(0)]],
        1,
    )
    .unwrap_err();
    assert!(zero_id.to_string().contains("zero global identity"));

    let too_many = validate(
        "more-than-declared",
        vec![
            vec![Some(id_a.to_vec()), pg_i64_field(0)],
            vec![Some(id_b.to_vec()), pg_i64_field(1)],
        ],
        1,
    )
    .unwrap_err();
    assert!(too_many
        .to_string()
        .contains("exceeds its declared row count 1"));

    let too_few = validate(
        "fewer-than-declared",
        vec![vec![Some(id_a.to_vec()), pg_i64_field(0)]],
        2,
    )
    .unwrap_err();
    assert!(too_few
        .to_string()
        .contains("row count mismatch: expected 2, got 1"));

    let trailing_path = base.join("trailing.copy");
    let mut trailing_copy = pg_binary_copy_rows(&[vec![Some(id_a.to_vec()), pg_i64_field(0)]]);
    trailing_copy.push(0);
    std::fs::write(&trailing_path, trailing_copy).unwrap();
    let trailing_error = load_v3_price_key_map_and_write_dictionary(
        &trailing_path,
        1,
        &mut CountingWriter::new(Vec::new()),
    )
    .err()
    .expect("trailing COPY bytes must fail");
    assert!(trailing_error
        .to_string()
        .contains("bytes after its COPY trailer"));
    let _ = std::fs::remove_dir_all(base);
}
#[test]
fn v3_price_key_map_requires_exact_source_id_coverage() {
    let _env_lock = scanner_env_lock().lock().unwrap();
    let base = std::env::temp_dir().join(format!(
        "ptg2-direct-v3-finalizer-price-map-coverage-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let id_a = prefixed_test_id(2, 1);
    let id_b = prefixed_test_id(2, 2);
    let id_c = prefixed_test_id(2, 3);
    let rows = [
        V3FinalizerTestRow {
            coverage_scope_id: [0x62; COVERAGE_SCOPE_ID_BYTES],
            code_system: Some("CPT"),
            code: Some("99213"),
            negotiation_arrangement: Some("FFS"),
            provider_id: prefixed_test_id(1, 1),
            price_id: id_a,
            provider_count: 1,
        },
        V3FinalizerTestRow {
            coverage_scope_id: [0x62; COVERAGE_SCOPE_ID_BYTES],
            code_system: Some("CPT"),
            code: Some("99213"),
            negotiation_arrangement: Some("FFS"),
            provider_id: prefixed_test_id(1, 1),
            price_id: id_b,
            provider_count: 1,
        },
    ];
    let manifest = write_v3_finalizer_test_manifest(&base, "coverage", &rows);
    let finalize = |label: &str, ids_in_key_order: &[[u8; GLOBAL_ID_BYTES]]| -> io::Result<Value> {
        let price_key_map_input =
            write_v3_finalizer_test_price_key_map(&base, label, ids_in_key_order);
        finalize_v3_runs(&V3FinalizerOptions {
            output_directory: base.join(format!("{label}-output")),
            manifest_paths: vec![manifest.clone()],
            scratch_durability: ScratchDurability::Durable,
            total_sort_memory_bytes: v3_finalizer_test_sort_memory_bytes(1, 2),
            workers: 1,
            identity_map_max_bytes: V3_FINALIZER_DEFAULT_IDENTITY_MAP_MAX_BYTES,
            price_key_map_input,
            price_key_map_row_count: ids_in_key_order.len() as u64,
            price_membership_inputs: Vec::new(),
            price_atom_inputs: Vec::new(),
        })
    };

    let exact = finalize("exact", &[id_b, id_a]).unwrap();
    assert_eq!(exact["dense_keys"]["price"]["count"], 2);
    assert_eq!(exact["price_key_map"]["source_ids_exact_match"], true);

    let missing = finalize("missing", &[id_a]).unwrap_err();
    assert!(missing
        .to_string()
        .contains("assigned price identity is absent"));
    assert!(!base.join("missing-output").exists());

    let extra = finalize("extra", &[id_b, id_a, id_c]).unwrap_err();
    assert!(extra
        .to_string()
        .contains("authoritative price-key map contains unused price key 2"));
    assert!(!base.join("extra-output").exists());
    let _ = std::fs::remove_dir_all(base);
}
#[test]
fn direct_v3_finalizer_provider_conflict_leaves_no_output() {
    let _env_lock = scanner_env_lock().lock().unwrap();
    let base = std::env::temp_dir().join(format!(
        "ptg2-direct-v3-finalizer-conflict-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let provider = prefixed_test_id(1, 1);
    let manifest_a = write_v3_finalizer_test_manifest_with_source(
        &base,
        "conflict-a",
        &[V3FinalizerTestRow {
            coverage_scope_id: [0x11; COVERAGE_SCOPE_ID_BYTES],
            code_system: Some("CPT"),
            code: Some("99213"),
            negotiation_arrangement: Some("FFS"),
            provider_id: provider,
            price_id: prefixed_test_id(2, 1),
            provider_count: 2,
        }],
        0,
        2,
    );
    let manifest_b = write_v3_finalizer_test_manifest_with_source(
        &base,
        "conflict-b",
        &[V3FinalizerTestRow {
            coverage_scope_id: [0x11; COVERAGE_SCOPE_ID_BYTES],
            code_system: Some("CPT"),
            code: Some("A100"),
            negotiation_arrangement: Some("FFS"),
            provider_id: provider,
            price_id: prefixed_test_id(2, 2),
            provider_count: 3,
        }],
        1,
        2,
    );
    let price_key_map_input = write_v3_finalizer_test_price_key_map(
        &base,
        "conflict",
        &[prefixed_test_id(2, 1), prefixed_test_id(2, 2)],
    );
    let output = base.join("output");
    let error = finalize_v3_runs(&V3FinalizerOptions {
        output_directory: output.clone(),
        manifest_paths: vec![manifest_a, manifest_b],
        scratch_durability: ScratchDurability::Durable,
        total_sort_memory_bytes: v3_finalizer_test_sort_memory_bytes(2, 1),
        workers: 2,
        identity_map_max_bytes: V3_FINALIZER_DEFAULT_IDENTITY_MAP_MAX_BYTES,
        price_key_map_input,
        price_key_map_row_count: 2,
        price_membership_inputs: Vec::new(),
        price_atom_inputs: Vec::new(),
    })
    .unwrap_err();
    assert!(error.to_string().contains("provider_count"));
    assert!(!output.exists());
    assert!(std::fs::read_dir(&base).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains("ptg2-finalizer")
    }));
    let _ = std::fs::remove_dir_all(base);
}
#[test]
fn serving_binary_v3_provider_shard_block_keys_are_collision_free_and_i64_safe() {
    assert_eq!(
        serving_binary_by_code_provider_shard_block_key(0, 0).unwrap(),
        0
    );
    assert_eq!(
        serving_binary_by_code_provider_shard_block_key(0, 1023).unwrap(),
        0
    );
    assert_eq!(
        serving_binary_by_code_provider_shard_block_key(0, 1024).unwrap(),
        0
    );
    assert_eq!(
        serving_binary_by_code_provider_shard_block_key(0, 8191).unwrap(),
        0
    );
    assert_eq!(
        serving_binary_by_code_provider_shard_block_key(0, 8192).unwrap(),
        1
    );
    assert_eq!(
        serving_binary_by_code_provider_shard_block_key(1, 0).unwrap(),
        1i64 << 31
    );
    assert!(
        serving_binary_by_code_provider_shard_block_key(0, i32::MAX).unwrap()
            < serving_binary_by_code_provider_shard_block_key(1, 0).unwrap()
    );
    let maximum_key = serving_binary_by_code_provider_shard_block_key(i32::MAX, i32::MAX).unwrap();
    assert_eq!(
        maximum_key,
        (i64::from(i32::MAX) << 31)
            | (i64::from(i32::MAX) / PTG2_SERVING_BINARY_BY_CODE_PROVIDER_SHARD_SPAN)
    );
    assert!(maximum_key < (1i64 << 62));
    assert!(serving_binary_by_code_provider_shard_block_key(-1, 0).is_err());
    assert!(serving_binary_by_code_provider_shard_block_key(0, -1).is_err());
}
#[test]
fn serving_binary_v3_fixed_assigned_stream_matches_pg_binary_exactly() {
    let directory = tempfile::tempdir().unwrap();
    let first_path = directory.path().join("assigned-000.bin");
    let second_path = directory.path().join("assigned-001.bin");
    let rows = [
        AssignedV3Row {
            code_key: 1,
            provider_set_key: 2,
            provider_count: 3,
            price_key: 0,
            source_key: 0,
        },
        AssignedV3Row {
            code_key: 1,
            provider_set_key: 2,
            provider_count: 3,
            price_key: 1,
            source_key: 1,
        },
        AssignedV3Row {
            code_key: 2,
            provider_set_key: 4,
            provider_count: 7,
            price_key: 2,
            source_key: 2,
        },
    ];
    write_assigned_fixed_records(&first_path, &rows[..2]);
    write_assigned_fixed_records(&second_path, &rows[2..]);
    let options = AssignedV3EncoderOptions {
        grouped_payload_bytes: 1024,
        hot_payload_bytes: 1024,
        provider_code_sort_chunk_bytes: 1024,
        provider_set_count: None,
        provider_code_count: None,
        provider_code_bitmap_max_bytes: 0,
        rate_schedule_observe: false,
        source: SourceEncoding {
            count: 3,
            key_bits: 2,
            tagged_codec: TaggedServingRunCodec::new(3, 1).unwrap(),
        },
    };

    let pg_rows = rows
        .iter()
        .map(|row| {
            vec![
                pg_i32_field(row.code_key),
                pg_i32_field(row.provider_set_key),
                pg_i64_field(row.provider_count as i64),
                pg_i64_field(i64::from(row.price_key)),
                pg_i64_field(i64::from(row.source_key)),
            ]
        })
        .collect::<Vec<_>>();
    let mut pg_reader = Cursor::new(pg_binary_copy_rows(&pg_rows));
    let mut pg_output = CountingWriter::new(Vec::new());
    let pg_summary =
        write_serving_binary_v3_assigned_by_code_copy_from_pg_binary_reader_with_provenance(
            &mut pg_reader,
            &mut pg_output,
            ServingBinaryTargetCopyFormat::SharedBinary,
            options,
            true,
        )
        .unwrap();

    let mut fixed_reader =
        AssignedFixedRecordStream::new_many(vec![first_path, second_path], rows.len() as u64)
            .unwrap();
    let mut fixed_output = CountingWriter::new(Vec::new());
    let fixed_summary = write_serving_binary_v3_assigned_rows_copy_with_provenance(
        &mut fixed_reader,
        &mut fixed_output,
        ServingBinaryTargetCopyFormat::SharedBinary,
        options,
    )
    .unwrap();

    assert_eq!(fixed_output.inner, pg_output.inner);
    assert_eq!(fixed_summary["row_count"], pg_summary["row_count"]);
    assert_eq!(
        fixed_summary["copy_record_count"],
        pg_summary["copy_record_count"]
    );
    assert_eq!(fixed_summary["source_copy_format"], "assigned_fixed_v1");
    assert_eq!(fixed_reader.distinct_record_count(), rows.len() as u64);
    assert_eq!(fixed_reader.duplicate_record_count(), 0);
    assert!(!fixed_reader.audit_candidates().unwrap().is_empty());
}
#[test]
fn serving_binary_v3_reports_exact_observe_only_rate_schedule_reuse() {
    let rows = [
        AssignedV3Row {
            code_key: 1,
            provider_set_key: 0,
            provider_count: 1,
            price_key: 2,
            source_key: 0,
        },
        AssignedV3Row {
            code_key: 1,
            provider_set_key: 1,
            provider_count: 1,
            price_key: 2,
            source_key: 0,
        },
        AssignedV3Row {
            code_key: 2,
            provider_set_key: 0,
            provider_count: 1,
            price_key: 3,
            source_key: 1,
        },
        AssignedV3Row {
            code_key: 2,
            provider_set_key: 1,
            provider_count: 1,
            price_key: 3,
            source_key: 1,
        },
    ];
    let pg_rows = rows
        .iter()
        .map(|row| {
            vec![
                pg_i32_field(row.code_key),
                pg_i32_field(row.provider_set_key),
                pg_i64_field(row.provider_count as i64),
                pg_i64_field(i64::from(row.price_key)),
                pg_i64_field(i64::from(row.source_key)),
            ]
        })
        .collect::<Vec<_>>();
    let mut reader = Cursor::new(pg_binary_copy_rows(&pg_rows));
    let mut writer = CountingWriter::new(Vec::new());
    let summary =
        write_serving_binary_v3_assigned_by_code_copy_from_pg_binary_reader_with_provenance(
            &mut reader,
            &mut writer,
            ServingBinaryTargetCopyFormat::SharedBinary,
            AssignedV3EncoderOptions {
                grouped_payload_bytes: 1024,
                hot_payload_bytes: 1024,
                provider_code_sort_chunk_bytes: 1024,
                provider_set_count: Some(2),
                provider_code_count: Some(3),
                provider_code_bitmap_max_bytes: 1024,
                rate_schedule_observe: true,
                source: SourceEncoding {
                    count: 2,
                    key_bits: 1,
                    tagged_codec: TaggedServingRunCodec::new(2, 1).unwrap(),
                },
            },
            true,
        )
        .unwrap();

    assert_eq!(summary["rate_schedule_observe"]["enabled"], true);
    assert_eq!(
        summary["rate_schedule_observe"]["representation_effect"],
        "observe_only_no_serving_change"
    );
    assert_eq!(summary["rate_schedule_observe"]["provider_set_count_s"], 2);
    assert_eq!(
        summary["rate_schedule_observe"]["distinct_schedule_count_k"],
        1
    );
    assert_eq!(
        summary["rate_schedule_observe"]["rate_occurrence_count_r"],
        4
    );
    assert_eq!(
        summary["rate_schedule_observe"]["unique_schedule_occurrence_count_u"],
        2
    );
    assert_eq!(
        summary["rate_schedule_observe"]["distinct_schedule_code_incidence_count_i"],
        2
    );
    assert_eq!(
        summary["rate_schedule_observe"]["weighted_reuse_r_over_u"],
        2.0
    );
    assert_eq!(
        summary["rate_schedule_observe"]["occurrence_external_sort"],
        false
    );
    assert_eq!(summary["rate_schedule_observe"]["scratch_bytes_written"], 0);
}
#[test]
fn assigned_fixed_stream_rejects_unordered_and_partial_records() {
    let directory = tempfile::tempdir().unwrap();
    let unordered_path = directory.path().join("unordered.bin");
    let later = AssignedV3Row {
        code_key: 2,
        provider_set_key: 0,
        provider_count: 1,
        price_key: 0,
        source_key: 0,
    };
    let earlier = AssignedV3Row {
        code_key: 1,
        ..later
    };
    write_assigned_fixed_records(&unordered_path, &[later, earlier]);
    let mut unordered = AssignedFixedRecordStream::new_many(vec![unordered_path], 2).unwrap();
    let error = unordered.next_row().unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("input is not sorted"));

    let partial_path = directory.path().join("partial.bin");
    std::fs::write(&partial_path, [0u8; V3_FINALIZER_ASSIGNED_BYTES - 1]).unwrap();
    let mut partial = AssignedFixedRecordStream::new_many(vec![partial_path], 1).unwrap();
    let error = partial.next_row().unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("not aligned to 20 bytes"));
}
#[test]
fn serving_binary_direct_group_append_matches_temporary_encoder_bytes() {
    let reference = |provider_set_key: i32,
                     previous_provider_set_key: i32,
                     occurrence_count: usize,
                     price_payload: &[u8],
                     source_payload: &[u8]| {
        let mut encoded = Vec::new();
        write_uvarint_to_vec(
            &mut encoded,
            (provider_set_key - previous_provider_set_key) as u64,
        );
        write_uvarint_to_vec(&mut encoded, occurrence_count as u64);
        encoded.extend_from_slice(price_payload);
        encoded.extend_from_slice(source_payload);
        encoded
    };
    let cases = [
        (0, 0, 1, vec![0], vec![]),
        (127, 0, 127, vec![0x7f, 0x80, 0x01], vec![0xaa; 16]),
        (128, 127, 128, vec![0x80, 0x01], vec![0x55; 17]),
        (128, 0, 1, vec![0], vec![]),
        (16_384, 128, 16_384, vec![0xff; 257], vec![0x11; 33]),
    ];
    for (provider_set_key, previous_provider_set_key, occurrence_count, prices, sources) in cases {
        let expected = reference(
            provider_set_key,
            previous_provider_set_key,
            occurrence_count,
            &prices,
            &sources,
        );
        let layout = serving_by_code_group_layout(
            provider_set_key,
            previous_provider_set_key,
            occurrence_count,
            &prices,
            &sources,
        )
        .unwrap();
        let mut actual = vec![0xde, 0xad];
        append_serving_by_code_group(&mut actual, layout, occurrence_count, &prices, &sources);
        assert_eq!(&actual[2..], expected);
        assert_eq!(layout.encoded_bytes, expected.len());
    }

    let continuing = serving_by_code_group_layout(128, 127, 1, &[0], &[]).unwrap();
    let fragment_reset = serving_by_code_group_layout(128, 0, 1, &[0], &[]).unwrap();
    assert_eq!(continuing.provider_delta, 1);
    assert_eq!(fragment_reset.provider_delta, 128);
    assert_eq!(fragment_reset.encoded_bytes, continuing.encoded_bytes + 1);
}
#[test]
fn serving_binary_dense_provider_projections_match_sparse_copy_exactly() {
    let _env_lock = scanner_env_lock().lock().unwrap();
    let _compression = TestEnvVar::set(PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_ENV, "zlib");
    let input = pg_binary_copy_rows(&[
        vec![
            pg_i32_field(0),
            pg_i32_field(0),
            pg_i32_field(2),
            pg_i64_field(0),
        ],
        vec![
            pg_i32_field(0),
            pg_i32_field(1),
            pg_i32_field(3),
            pg_i64_field(1),
        ],
        vec![
            pg_i32_field(1),
            pg_i32_field(0),
            pg_i32_field(2),
            pg_i64_field(1),
        ],
        vec![
            pg_i32_field(1),
            pg_i32_field(2),
            pg_i32_field(1),
            pg_i64_field(2),
        ],
        vec![
            pg_i32_field(2),
            pg_i32_field(2),
            pg_i32_field(1),
            pg_i64_field(3),
        ],
    ]);
    let encode = |provider_set_count| {
        let mut reader = Cursor::new(input.clone());
        let mut writer = CountingWriter::new(Vec::new());
        let summary =
            write_serving_binary_v3_assigned_by_code_copy_from_pg_binary_reader_with_provenance(
                &mut reader,
                &mut writer,
                ServingBinaryTargetCopyFormat::SharedBinary,
                AssignedV3EncoderOptions {
                    grouped_payload_bytes: 1024,
                    hot_payload_bytes: 1024,
                    provider_code_sort_chunk_bytes: 1024,
                    provider_set_count,
                    provider_code_count: None,
                    provider_code_bitmap_max_bytes: 0,
                    rate_schedule_observe: false,
                    source: SourceEncoding {
                        count: 1,
                        key_bits: 0,
                        tagged_codec: TaggedServingRunCodec::new(1, 0).unwrap(),
                    },
                },
                false,
            )
            .unwrap();
        let block_summary = writer.shared_block_summary(Path::new("blocks.copy"));
        (writer.inner, block_summary, summary)
    };
    let (sparse_copy, sparse_blocks, sparse_summary) = encode(None);
    let (dense_copy, dense_blocks, dense_summary) = encode(Some(3));
    assert_eq!(dense_copy, sparse_copy);
    assert_eq!(dense_blocks, sparse_blocks);
    assert_eq!(
        sparse_summary["provider_set_page"]["provider_projection"]["storage_kind"],
        "sparse_unbounded_v1",
    );
    assert_eq!(
        dense_summary["provider_set_page"]["provider_projection"]["storage_kind"],
        "dense_vec_v1",
    );
    assert_eq!(
        dense_summary["provider_set_page"]["provider_projection"]["entry_count"],
        3,
    );
}
#[test]
fn dense_provider_projection_bounds_and_charge_are_explicit() {
    let dense_entry_bytes = std::mem::size_of::<Option<ServingBinaryV3ProviderProjection>>()
        + PTG2_SERVING_BINARY_V3_PAGE_ROWS * std::mem::size_of::<(i32, u32, u32)>();
    assert!(dense_entry_bytes <= V3_FINALIZER_PROVIDER_PROJECTION_MAX_BYTES_PER_ENTRY);
    assert_eq!(
        v3_finalizer_provider_projection_max_bytes(68_587).unwrap(),
        68_587 * V3_FINALIZER_PROVIDER_PROJECTION_MAX_BYTES_PER_ENTRY,
    );
    assert!(v3_finalizer_provider_projection_max_bytes(usize::MAX).is_err());

    let mut dense = ServingBinaryV3ProviderProjections::new(Some(2));
    let error = dense.push(2, 1, 0, 0, 0).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    dense.push(0, 1, 0, 0, 0).unwrap();
    let error = dense.validate_complete().unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    let mut sparse = ServingBinaryV3ProviderProjections::new(None);
    sparse.push(100, 1, 0, 0, 0).unwrap();
    assert_eq!(sparse.len(), 1);
    sparse.validate_complete().unwrap();
}
#[test]
fn provider_code_bitmap_matches_external_sort_copy_exactly() {
    let _env_lock = scanner_env_lock().lock().unwrap();
    let _compression = TestEnvVar::set(PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_ENV, "zlib");
    let input = pg_binary_copy_rows(&[
        vec![
            pg_i32_field(0),
            pg_i32_field(2),
            pg_i32_field(7),
            pg_i64_field(0),
        ],
        vec![
            pg_i32_field(63),
            pg_i32_field(0),
            pg_i32_field(2),
            pg_i64_field(1),
        ],
        vec![
            pg_i32_field(63),
            pg_i32_field(0),
            pg_i32_field(2),
            pg_i64_field(2),
        ],
        vec![
            pg_i32_field(63),
            pg_i32_field(2),
            pg_i32_field(7),
            pg_i64_field(3),
        ],
        vec![
            pg_i32_field(64),
            pg_i32_field(1),
            pg_i32_field(5),
            pg_i64_field(4),
        ],
        vec![
            pg_i32_field(127),
            pg_i32_field(0),
            pg_i32_field(2),
            pg_i64_field(5),
        ],
        vec![
            pg_i32_field(127),
            pg_i32_field(1),
            pg_i32_field(5),
            pg_i64_field(6),
        ],
        vec![
            pg_i32_field(127),
            pg_i32_field(2),
            pg_i32_field(7),
            pg_i64_field(7),
        ],
    ]);
    let encode = |bitmap_maximum_bytes| {
        let mut reader = Cursor::new(input.clone());
        let mut writer = CountingWriter::new(Vec::new());
        let summary =
            write_serving_binary_v3_assigned_by_code_copy_from_pg_binary_reader_with_provenance(
                &mut reader,
                &mut writer,
                ServingBinaryTargetCopyFormat::SharedBinary,
                AssignedV3EncoderOptions {
                    grouped_payload_bytes: 1024,
                    hot_payload_bytes: 1024,
                    provider_code_sort_chunk_bytes: PROVIDER_CODE_PAIR_RECORD_BYTES,
                    provider_set_count: Some(3),
                    provider_code_count: Some(128),
                    provider_code_bitmap_max_bytes: bitmap_maximum_bytes,
                    rate_schedule_observe: false,
                    source: SourceEncoding {
                        count: 1,
                        key_bits: 0,
                        tagged_codec: TaggedServingRunCodec::new(1, 0).unwrap(),
                    },
                },
                false,
            )
            .unwrap();
        let block_summary = writer.shared_block_summary(Path::new("blocks.copy"));
        (writer.inner, block_summary, summary)
    };

    let (sorted_copy, sorted_blocks, sorted_summary) = encode(0);
    let (bitmap_copy, bitmap_blocks, bitmap_summary) = encode(48);
    assert_eq!(bitmap_copy, sorted_copy);
    assert_eq!(bitmap_blocks, sorted_blocks);
    assert_eq!(
        sorted_summary["provider_set_codes"]["index_mode"],
        "pair_spool_sort_v1",
    );
    assert_eq!(
        sorted_summary["provider_set_codes"]["bitmap_fallback_reason"],
        "memory_cap_exceeded",
    );
    assert_eq!(
        sorted_summary["provider_set_codes"]["bitmap_candidate_bytes"],
        48,
    );
    assert_eq!(
        sorted_summary["provider_set_codes"]["bitmap_charged_bytes"],
        0,
    );
    assert_eq!(sorted_summary["provider_set_codes"]["external_sort"], true);
    assert!(
        sorted_summary["provider_set_codes"]["sort_scratch_bytes_read"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert_eq!(
        bitmap_summary["provider_set_codes"]["index_mode"],
        "provider_major_bitmap_v1",
    );
    assert_eq!(
        bitmap_summary["provider_set_codes"]["bitmap_candidate_bytes"],
        48,
    );
    assert_eq!(
        bitmap_summary["provider_set_codes"]["bitmap_charged_bytes"],
        48,
    );
    assert_eq!(
        bitmap_summary["provider_set_codes"]["sort_scratch_bytes_read"],
        0,
    );
    assert_eq!(
        bitmap_summary["provider_set_codes"]["sort_scratch_bytes_written"],
        0,
    );
    assert_eq!(bitmap_summary["provider_set_codes"]["external_sort"], false);
    assert_eq!(
        bitmap_summary["provider_set_codes"]["spool_input_pair_count"],
        7,
    );
    assert_eq!(
        bitmap_summary["provider_set_codes"]["spool_unique_pair_count"],
        7,
    );
}
#[test]
fn provider_code_bitmap_layout_bounds_and_fallback_are_explicit() {
    let high_cardinality_layout = provider_code_bitmap_layout_bytes(68_587, 18_807).unwrap();
    assert_eq!(high_cardinality_layout, (294, 161_316_624),);
    assert!(high_cardinality_layout.1 <= V3_FINALIZER_PROVIDER_CODE_BITMAP_MAX_BYTES);
    assert!(provider_code_bitmap_layout_bytes(1, usize::MAX).is_err());
    assert!(provider_code_bitmap_layout_bytes(usize::MAX, 64).is_err());

    let candidate_bytes = provider_code_bitmap_layout_bytes(2, 65).unwrap().1;
    let capped =
        ServingBinaryV3ProviderCodeIndex::new(Some(2), Some(65), candidate_bytes - 1).unwrap();
    assert!(matches!(
        capped,
        ServingBinaryV3ProviderCodeIndex::Spool {
            bitmap_candidate_bytes: Some(bytes),
            bitmap_fallback_reason: "memory_cap_exceeded",
            ..
        } if bytes == candidate_bytes
    ));

    let mut bitmap =
        ServingBinaryV3ProviderCodeIndex::new(Some(2), Some(65), candidate_bytes).unwrap();
    bitmap.push(0, 64).unwrap();
    let error = bitmap.push(2, 0).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    let error = bitmap.push(0, 65).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    let error = bitmap.push(-1, 0).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
}
#[test]
fn serving_binary_v3_provider_shards_follow_code_and_shard_transitions_and_summary() {
    let input = pg_binary_copy_rows(&[
        vec![
            pg_i32_field(1),
            pg_i32_field(8191),
            pg_i32_field(2),
            pg_i64_field(0),
        ],
        vec![
            pg_i32_field(1),
            pg_i32_field(8192),
            pg_i32_field(3),
            pg_i64_field(1),
        ],
        vec![
            pg_i32_field(2),
            pg_i32_field(8192),
            pg_i32_field(3),
            pg_i64_field(2),
        ],
    ]);
    let mut reader = Cursor::new(input);
    let mut writer = CountingWriter::new(Vec::new());

    let summary = write_serving_binary_v3_assigned_by_code_copy_from_pg_binary_reader(
        &mut reader,
        &mut writer,
        ServingBinaryTargetCopyFormat::SharedBinary,
        1024,
    )
    .unwrap();
    let records = read_test_shared_binary_records(writer.inner);
    let shard_records = records
        .iter()
        .filter(|record| record.kind == PTG2_SERVING_BINARY_BY_CODE_PROVIDER_SHARD_KIND)
        .collect::<Vec<_>>();
    assert_eq!(shard_records.len(), 3);
    assert_eq!(
        shard_records
            .iter()
            .map(|record| (record.block_key, record.block_no, record.entry_count))
            .collect::<Vec<_>>(),
        vec![
            ((1i64 << 31), 0, 1),
            ((1i64 << 31) | 1, 0, 1),
            ((2i64 << 31) | 1, 0, 1),
        ]
    );
    assert_eq!(
        summary["artifact_kind"],
        PTG2_SERVING_BINARY_BY_CODE_PROVIDER_SHARD_KIND
    );
    assert_eq!(summary["provider_shard_span"], 8192);
    assert_eq!(summary["block_count"], 3);
    assert_eq!(summary["by_code_copy_record_count"], 3);
    assert_eq!(summary["by_code_provider_shard"]["block_count"], 3);
    assert_eq!(summary["by_code_provider_shard"]["copy_record_count"], 3);
    assert_eq!(
        summary["emitted_artifact_kinds"],
        json!([
            PTG2_SERVING_BINARY_BY_CODE_PROVIDER_SHARD_KIND,
            PTG2_SERVING_BINARY_BY_CODE_PRICE_PAGE_V4_KIND,
            PTG2_SERVING_BINARY_PROVIDER_COUNT_DICTIONARY_KIND,
            PTG2_SERVING_BINARY_PROVIDER_SET_CODES_V3_KIND,
            PTG2_SERVING_BINARY_PROVIDER_SET_PAGE_V3_KIND,
        ])
    );
    assert!(!records
        .iter()
        .any(|record| record.kind == "by_code_grouped_v2"));
    assert!(!serde_json::to_string(&summary)
        .unwrap()
        .contains("by_code_grouped_v2"));
}
#[test]
fn serving_binary_v3_provider_shard_fragments_reset_headers_and_provider_deltas() {
    let input = pg_binary_copy_rows(
        &[126, 127, 128]
            .into_iter()
            .map(|provider_set_key| {
                vec![
                    pg_i32_field(4),
                    pg_i32_field(provider_set_key),
                    pg_i32_field(1),
                    pg_i64_field(i64::from(provider_set_key - 126)),
                ]
            })
            .collect::<Vec<_>>(),
    );
    let mut reader = Cursor::new(input);
    let mut writer = CountingWriter::new(Vec::new());

    let summary = write_serving_binary_v3_assigned_by_code_copy_from_pg_binary_reader_with_limits(
        &mut reader,
        &mut writer,
        ServingBinaryTargetCopyFormat::SharedBinary,
        8,
        1024,
    )
    .unwrap();
    let records = read_test_shared_binary_records(writer.inner);
    let shard_records = records
        .iter()
        .filter(|record| record.kind == PTG2_SERVING_BINARY_BY_CODE_PROVIDER_SHARD_KIND)
        .collect::<Vec<_>>();
    assert_eq!(shard_records.len(), 3);
    assert_eq!(
        shard_records
            .iter()
            .map(|record| record.block_no)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    for (record, expected_provider_set_key) in shard_records.iter().zip([126, 127, 128]) {
        assert_eq!(record.block_key, 4i64 << 31);
        assert_eq!(record.entry_count, 1);
        assert_eq!(
            &record.payload[..3],
            &[PTG2_SERVING_BINARY_V3_GROUPED_FORMAT_VERSION, 1, 0]
        );
        assert_eq!(
            decode_test_by_code_provider_shard_fragment(record),
            vec![(
                expected_provider_set_key,
                vec![((expected_provider_set_key - 126) as u32, 0)]
            )]
        );
    }
    assert_eq!(summary["block_count"], 1);
    assert_eq!(summary["by_code_copy_record_count"], 3);
}
#[test]
fn serving_binary_v3_provider_shard_continuations_bound_duplicate_occurrences() {
    let input = pg_binary_copy_rows(
        &(0..8)
            .map(|_| {
                vec![
                    pg_i32_field(4),
                    pg_i32_field(5),
                    pg_i32_field(1),
                    pg_i64_field(0),
                ]
            })
            .collect::<Vec<_>>(),
    );
    let mut reader = Cursor::new(input);
    let mut writer = CountingWriter::new(Vec::new());

    let summary = write_serving_binary_v3_assigned_by_code_copy_from_pg_binary_reader_with_limits(
        &mut reader,
        &mut writer,
        ServingBinaryTargetCopyFormat::SharedBinary,
        8,
        1024,
    )
    .unwrap();
    let records = read_test_shared_binary_records(writer.inner);
    let shard_records = records
        .iter()
        .filter(|record| record.kind == PTG2_SERVING_BINARY_BY_CODE_PROVIDER_SHARD_KIND)
        .collect::<Vec<_>>();
    assert_eq!(shard_records.len(), 3);
    assert!(shard_records.iter().all(|record| record.payload.len() <= 8));
    assert_eq!(
        shard_records
            .iter()
            .flat_map(|record| decode_test_by_code_provider_shard_fragment(record))
            .flat_map(|(provider_set_key, occurrences)| {
                occurrences
                    .into_iter()
                    .map(move |occurrence| (provider_set_key, occurrence))
            })
            .collect::<Vec<_>>(),
        vec![(5, (0, 0)); 8]
    );
    assert_eq!(summary["group_count"], 1);
    assert_eq!(
        summary["by_code_provider_shard"]["occurrence_chunk_count"],
        3
    );
    assert_eq!(
        summary["by_code_provider_shard"]["maximum_occurrences_buffered"],
        3
    );
}
#[test]
fn serving_binary_v3_provider_shards_decode_exact_rows_with_source_provenance() {
    let source = SourceEncoding {
        count: 3,
        key_bits: 2,
        tagged_codec: TaggedServingRunCodec::new(3, 1).unwrap(),
    };
    let expected_rows = vec![
        (7, 8191, 2, 0),
        (7, 8191, 3, 2),
        (7, 8192, 5, 1),
        (8, 8192, 6, 2),
    ];
    let input = pg_binary_copy_rows(
        &expected_rows
            .iter()
            .map(|(code_key, provider_set_key, price_key, source_key)| {
                vec![
                    pg_i32_field(*code_key),
                    pg_i32_field(*provider_set_key),
                    pg_i32_field(4),
                    pg_i64_field(i64::from(*price_key)),
                    pg_i64_field(i64::from(*source_key)),
                ]
            })
            .collect::<Vec<_>>(),
    );
    let mut reader = Cursor::new(input);
    let mut writer = CountingWriter::new(Vec::new());

    let summary =
        write_serving_binary_v3_assigned_by_code_copy_from_pg_binary_reader_with_provenance(
            &mut reader,
            &mut writer,
            ServingBinaryTargetCopyFormat::SharedBinary,
            AssignedV3EncoderOptions {
                grouped_payload_bytes: 1024,
                hot_payload_bytes: 1024,
                provider_code_sort_chunk_bytes: 1024,
                provider_set_count: None,
                provider_code_count: None,
                provider_code_bitmap_max_bytes: 0,
                rate_schedule_observe: false,
                source,
            },
            true,
        )
        .unwrap();
    let records = read_test_shared_binary_records(writer.inner);
    let mut decoded_rows = Vec::new();
    for record in records
        .iter()
        .filter(|record| record.kind == PTG2_SERVING_BINARY_BY_CODE_PROVIDER_SHARD_KIND)
    {
        let code_key = i32::try_from(record.block_key >> 31).unwrap();
        let shard_no = record.block_key & ((1i64 << 31) - 1);
        for (provider_set_key, occurrences) in decode_test_by_code_provider_shard_fragment(record) {
            assert_eq!(
                i64::from(provider_set_key) / PTG2_SERVING_BINARY_BY_CODE_PROVIDER_SHARD_SPAN,
                shard_no
            );
            decoded_rows.extend(occurrences.into_iter().map(|(price_key, source_key)| {
                (code_key, provider_set_key, price_key, source_key)
            }));
        }
    }
    assert_eq!(decoded_rows, expected_rows);
    assert_eq!(summary["row_count"], 4);
    assert_eq!(summary["group_count"], 3);
    assert_eq!(summary["block_count"], 3);
    assert_eq!(summary["source_count"], 3);
    assert_eq!(summary["source_key_bits"], 2);
}
#[test]
fn serving_binary_v3_provider_codes_match_golden_and_dedupe_pairs() {
    let input = pg_binary_copy_rows(&[
        vec![pg_i32_field(4), pg_i32_field(1)],
        vec![pg_i32_field(4), pg_i32_field(1)],
        vec![pg_i32_field(4), pg_i32_field(2)],
        vec![pg_i32_field(1024), pg_i32_field(65_536)],
    ]);
    let mut reader = Cursor::new(input);
    let mut writer = CountingWriter::new(Vec::new());

    let summary = write_serving_binary_v3_provider_codes_copy_from_pg_binary_reader(
        &mut reader,
        &mut writer,
        ServingBinaryTargetCopyFormat::SharedBinary,
        64 * 1024,
    )
    .unwrap();
    let records = read_test_shared_binary_records(writer.inner);

    assert_eq!(records.len(), 2);
    assert!(records.iter().all(|record| {
        record.kind == PTG2_SERVING_BINARY_PROVIDER_SET_CODES_V3_KIND
            && record.block_no == 0
            && record.entry_count == 1
    }));
    assert_eq!(records[0].block_key, 0);
    assert_eq!(records[1].block_key, 1);
    assert_eq!(records[0].payload, [1, 4, 8, 1, 1, 0, 1, 2, 2, 1, 1]);
    assert_eq!(records[1].payload, [1, 0, 7, 1, 1, 1, 1, 1, 1, 0]);
    assert_eq!(
        decode_test_provider_block(&records[0].payload, 0),
        BTreeMap::from([(4, vec![1, 2])])
    );
    assert_eq!(
        decode_test_provider_block(&records[1].payload, 1),
        BTreeMap::from([(1024, vec![65_536])])
    );
    assert_eq!(summary["format"], PTG2_SERVING_BINARY_V3_FORMAT);
    assert_eq!(
        summary["artifact_kind"],
        PTG2_SERVING_BINARY_PROVIDER_SET_CODES_V3_KIND
    );
    assert_eq!(summary["row_count"], 4);
    assert_eq!(summary["pair_count"], 3);
    assert_eq!(summary["duplicate_pair_count"], 1);
    assert_eq!(summary["provider_set_count"], 2);
    assert_eq!(summary["block_span"], 1024);
    assert_eq!(summary["block_count"], 2);
    assert_eq!(summary["storage"]["entry_count"], 2);
    assert_eq!(
        summary["target_copy_format"],
        "postgres_binary_shared_blocks"
    );
}
#[test]
fn serving_binary_v3_provider_codes_reject_order_and_copy_corruption() {
    let unordered = pg_binary_copy_rows(&[
        vec![pg_i32_field(2), pg_i32_field(8)],
        vec![pg_i32_field(2), pg_i32_field(7)],
    ]);
    let mut reader = Cursor::new(unordered);
    let mut writer = CountingWriter::new(Vec::new());
    let error = write_serving_binary_v3_provider_codes_copy_from_pg_binary_reader(
        &mut reader,
        &mut writer,
        ServingBinaryTargetCopyFormat::SharedBinary,
        1024,
    )
    .unwrap_err();
    assert!(error.to_string().contains("must be ordered"));

    let mut missing_trailer = Vec::new();
    write_pg_binary_copy_header(&mut missing_trailer).unwrap();
    missing_trailer.extend_from_slice(&2i16.to_be_bytes());
    append_pg_binary_field(&mut missing_trailer, &1i32.to_be_bytes());
    append_pg_binary_field(&mut missing_trailer, &2i32.to_be_bytes());
    let mut reader = Cursor::new(missing_trailer);
    let mut writer = CountingWriter::new(Vec::new());
    let error = write_serving_binary_v3_provider_codes_copy_from_pg_binary_reader(
        &mut reader,
        &mut writer,
        ServingBinaryTargetCopyFormat::SharedBinary,
        1024,
    )
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    assert!(error.to_string().contains("missing its trailer"));

    let wrong_shape = pg_binary_copy_rows(&[vec![pg_i32_field(1)]]);
    let mut reader = Cursor::new(wrong_shape);
    let mut writer = CountingWriter::new(Vec::new());
    let error = write_serving_binary_v3_provider_codes_copy_from_pg_binary_reader(
        &mut reader,
        &mut writer,
        ServingBinaryTargetCopyFormat::SharedBinary,
        1024,
    )
    .unwrap_err();
    assert!(error.to_string().contains("must have 2 fields"));
}
#[test]
fn serving_binary_v3_page_projections_cap_and_report_dense_rows() {
    let assigned_rows = (0..100)
        .map(|price_key| {
            vec![
                pg_i32_field(7),
                pg_i32_field(3),
                pg_i32_field(10),
                pg_i64_field(price_key),
            ]
        })
        .collect::<Vec<_>>();
    let input = pg_binary_copy_rows(&assigned_rows);
    let mut reader = Cursor::new(input);
    let mut writer = CountingWriter::new(Vec::new());

    let summary = write_serving_binary_v3_assigned_by_code_copy_from_pg_binary_reader(
        &mut reader,
        &mut writer,
        ServingBinaryTargetCopyFormat::SharedBinary,
        serving_binary_block_bytes(),
    )
    .unwrap();
    let records = read_test_shared_binary_records(writer.inner);

    assert_eq!(summary["by_code_page"]["row_count"], 64);
    assert_eq!(summary["by_code_page"]["code_count"], 1);
    assert_eq!(summary["provider_set_page"]["row_count"], 64);
    assert_eq!(summary["provider_set_page"]["source_row_count"], 100);
    assert_eq!(summary["provider_set_page"]["provider_set_count"], 1);
    assert_eq!(summary["provider_set_page"]["block_span"], 1);
    assert_eq!(
        summary["provider_set_page"]["truncated_provider_set_count"],
        1
    );

    let forward_payload =
        logical_test_payload(&records, PTG2_SERVING_BINARY_BY_CODE_PRICE_PAGE_V4_KIND, 7);
    let mut cursor = 0usize;
    assert_eq!(
        forward_payload[cursor],
        PTG2_SERVING_BINARY_PAGE_FORMAT_VERSION
    );
    cursor += 1;
    assert_eq!(test_read_uvarint(&forward_payload, &mut cursor), 1);
    assert_eq!(forward_payload[cursor], 0);
    cursor += 1;
    assert_eq!(test_read_uvarint(&forward_payload, &mut cursor), 64);
    for expected_price_key in 0..64 {
        assert_eq!(test_read_uvarint(&forward_payload, &mut cursor), 3);
        assert_eq!(test_read_uvarint(&forward_payload, &mut cursor), 10);
        assert_eq!(
            test_read_uvarint(&forward_payload, &mut cursor),
            expected_price_key
        );
    }
    assert_eq!(cursor, forward_payload.len());

    let provider_payload =
        logical_test_payload(&records, PTG2_SERVING_BINARY_PROVIDER_SET_PAGE_V3_KIND, 3);
    cursor = 0;
    assert_eq!(
        provider_payload[cursor],
        PTG2_SERVING_BINARY_PAGE_FORMAT_VERSION
    );
    cursor += 1;
    assert_eq!(test_read_uvarint(&provider_payload, &mut cursor), 1);
    assert_eq!(provider_payload[cursor], 0);
    cursor += 1;
    assert_eq!(test_read_uvarint(&provider_payload, &mut cursor), 1);
    assert_eq!(test_read_uvarint(&provider_payload, &mut cursor), 0);
    assert_eq!(test_read_uvarint(&provider_payload, &mut cursor), 10);
    assert_eq!(test_read_uvarint(&provider_payload, &mut cursor), 100);
    assert_eq!(test_read_uvarint(&provider_payload, &mut cursor), 64);
    for expected_price_key in 0..64 {
        let expected_code_delta = if expected_price_key == 0 { 7 } else { 0 };
        assert_eq!(
            test_read_uvarint(&provider_payload, &mut cursor),
            expected_code_delta
        );
        assert_eq!(
            test_read_uvarint(&provider_payload, &mut cursor),
            expected_price_key
        );
    }
    assert_eq!(cursor, provider_payload.len());
}
#[test]
fn serving_binary_v3_forward_page_ranks_price_before_provider_count() {
    let mut assigned_rows = vec![vec![
        pg_i32_field(7),
        pg_i32_field(0),
        pg_i32_field(10_000),
        pg_i64_field(64),
    ]];
    assigned_rows.extend((0..64).map(|price_key| {
        vec![
            pg_i32_field(7),
            pg_i32_field(price_key + 1),
            pg_i32_field(1),
            pg_i64_field(i64::from(price_key)),
        ]
    }));
    let mut reader = Cursor::new(pg_binary_copy_rows(&assigned_rows));
    let mut writer = CountingWriter::new(Vec::new());

    let summary = write_serving_binary_v3_assigned_by_code_copy_from_pg_binary_reader(
        &mut reader,
        &mut writer,
        ServingBinaryTargetCopyFormat::SharedBinary,
        serving_binary_block_bytes(),
    )
    .unwrap();
    let records = read_test_shared_binary_records(writer.inner);
    let payload = logical_test_payload(&records, PTG2_SERVING_BINARY_BY_CODE_PRICE_PAGE_V4_KIND, 7);
    let mut cursor = 0usize;
    assert_eq!(payload[cursor], 4);
    cursor += 1;
    assert_eq!(test_read_uvarint(&payload, &mut cursor), 1);
    assert_eq!(payload[cursor], 0);
    cursor += 1;
    assert_eq!(test_read_uvarint(&payload, &mut cursor), 64);
    for expected_price_key in 0..64u64 {
        assert_eq!(
            test_read_uvarint(&payload, &mut cursor),
            expected_price_key + 1
        );
        assert_eq!(test_read_uvarint(&payload, &mut cursor), 1);
        assert_eq!(test_read_uvarint(&payload, &mut cursor), expected_price_key);
    }
    assert_eq!(cursor, payload.len());
    assert_eq!(
        summary["by_code_page"]["artifact_kind"],
        "by_code_price_page_v4"
    );
    assert_eq!(summary["by_code_page"]["payload_format_version"], 4);
    assert_eq!(
        summary["by_code_page"]["ranking"],
        json!([
            "price_key",
            "provider_set_key",
            "source_key",
            "provider_count"
        ])
    );
}
#[test]
fn serving_binary_v3_assigned_forward_externally_sorts_provider_codes() {
    let _lock = scanner_env_lock().lock().unwrap();
    let _sort_chunk = TestEnvVar::set(
        PTG2_SERVING_BINARY_V3_PROVIDER_CODE_SORT_CHUNK_BYTES_ENV,
        "16",
    );
    let input = pg_binary_copy_rows(&[
        vec![
            pg_i32_field(1),
            pg_i32_field(10),
            pg_i32_field(2),
            pg_i64_field(0),
        ],
        vec![
            pg_i32_field(1),
            pg_i32_field(12),
            pg_i32_field(3),
            pg_i64_field(1),
        ],
        vec![
            pg_i32_field(2),
            pg_i32_field(10),
            pg_i32_field(2),
            pg_i64_field(2),
        ],
        vec![
            pg_i32_field(2),
            pg_i32_field(12),
            pg_i32_field(3),
            pg_i64_field(3),
        ],
    ]);
    let mut reader = Cursor::new(input);
    let mut writer = CountingWriter::new(Vec::new());

    let summary = write_serving_binary_v3_assigned_by_code_copy_from_pg_binary_reader(
        &mut reader,
        &mut writer,
        ServingBinaryTargetCopyFormat::SharedBinary,
        1024,
    )
    .unwrap();
    let records = read_test_shared_binary_records(writer.inner);
    let provider_record = records
        .iter()
        .find(|record| record.kind == PTG2_SERVING_BINARY_PROVIDER_SET_CODES_V3_KIND)
        .unwrap();

    assert_eq!(
        decode_test_provider_block(&provider_record.payload, provider_record.block_key),
        BTreeMap::from([(10, vec![1, 2]), (12, vec![1, 2])])
    );
    assert_eq!(summary["provider_set_codes"]["external_sort"], true);
    assert_eq!(summary["provider_set_codes"]["sort_chunk_count"], 2);
    assert_eq!(summary["provider_set_codes"]["spool_bytes"], 32);
    assert_eq!(summary["provider_set_codes"]["spool_unique_pair_count"], 4);
}
#[test]
fn serving_binary_v3_provider_code_sort_stays_bounded_beyond_one_merge_fan_in() {
    let _lock = scanner_env_lock().lock().unwrap();
    let _sort_chunk = TestEnvVar::set(
        PTG2_SERVING_BINARY_V3_PROVIDER_CODE_SORT_CHUNK_BYTES_ENV,
        "8",
    );
    let rows = (1..=130)
        .map(|provider_set_key| {
            vec![
                pg_i32_field(1),
                pg_i32_field(provider_set_key),
                pg_i32_field(1),
                pg_i64_field(i64::from(provider_set_key - 1)),
            ]
        })
        .collect::<Vec<_>>();
    let mut reader = Cursor::new(pg_binary_copy_rows(&rows));
    let mut writer = CountingWriter::new(Vec::new());

    let summary = write_serving_binary_v3_assigned_by_code_copy_from_pg_binary_reader(
        &mut reader,
        &mut writer,
        ServingBinaryTargetCopyFormat::SharedBinary,
        1024,
    )
    .unwrap();
    let provider_codes = &summary["provider_set_codes"];

    assert_eq!(provider_codes["external_sort"], true);
    assert_eq!(provider_codes["spool_input_pair_count"], 130);
    assert_eq!(provider_codes["spool_unique_pair_count"], 130);
    assert_eq!(provider_codes["sort_chunk_count"], 130);
    assert_eq!(provider_codes["merge_pass_count"], 2);
    assert_eq!(provider_codes["maximum_merge_fan_in"], 64);
    assert_eq!(provider_codes["sort_scratch_bytes_read"], 4_144);
    assert_eq!(provider_codes["sort_scratch_bytes_written"], 4_144);
    assert!(
        provider_codes["block_spool_scratch_bytes_read"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert_eq!(
        provider_codes["scratch_bytes_read"].as_u64().unwrap(),
        provider_codes["sort_scratch_bytes_read"].as_u64().unwrap()
            + provider_codes["block_spool_scratch_bytes_read"]
                .as_u64()
                .unwrap()
    );
    assert_eq!(
        provider_codes["scratch_bytes_written"].as_u64().unwrap(),
        provider_codes["sort_scratch_bytes_written"]
            .as_u64()
            .unwrap()
            + provider_codes["block_spool_scratch_bytes_written"]
                .as_u64()
                .unwrap()
    );
}
#[test]
fn provider_code_merge_pass_count_includes_eager_fan_in_boundaries() {
    assert_eq!(provider_code_merge_pass_count(0), 0);
    assert_eq!(provider_code_merge_pass_count(1), 1);
    assert_eq!(provider_code_merge_pass_count(63), 1);
    assert_eq!(provider_code_merge_pass_count(64), 2);
    assert_eq!(provider_code_merge_pass_count(65), 2);
    assert_eq!(provider_code_merge_pass_count(4_032), 2);
    assert_eq!(provider_code_merge_pass_count(4_095), 3);
    assert_eq!(provider_code_merge_pass_count(4_096), 3);
}
#[test]
fn v3_finalizer_encoder_workspace_contract_is_exact_and_checked() {
    let block_bytes = 64 * 1024;
    let code_count = 17usize;
    let active_workers = 8usize;
    assert_eq!(
        v3_finalizer_encoder_workspace_max_bytes(block_bytes, code_count, active_workers,).unwrap(),
        block_bytes * V3_FINALIZER_ENCODER_BLOCK_BUFFER_MULTIPLIER
            + code_count * V3_FINALIZER_ENCODER_PROVIDER_CODE_BYTES_PER_CODE
            + V3_FINALIZER_ENCODER_FIXED_WORKSPACE_BYTES
            + V3_FINALIZER_SHARED_BLOCK_BATCH_MAX_RAW_BYTES
                * V3_FINALIZER_SHARED_BLOCK_BATCH_MEMORY_MULTIPLIER
            + active_workers * V3_FINALIZER_SHARED_BLOCK_COMPRESSION_WORKSPACE_BYTES_PER_WORKER
    );
    assert!(v3_finalizer_encoder_workspace_max_bytes(usize::MAX, 1, 1).is_err());
    assert!(v3_finalizer_encoder_workspace_max_bytes(1, usize::MAX, 1).is_err());
    assert!(v3_finalizer_encoder_workspace_max_bytes(1, 1, usize::MAX).is_err());
}
#[test]
fn serving_binary_v3_assigned_forward_rejects_order_and_count_changes() {
    let unordered = pg_binary_copy_rows(&[
        vec![
            pg_i32_field(2),
            pg_i32_field(1),
            pg_i32_field(3),
            pg_i32_field(0),
        ],
        vec![
            pg_i32_field(1),
            pg_i32_field(1),
            pg_i32_field(3),
            pg_i32_field(0),
        ],
    ]);
    let mut reader = Cursor::new(unordered);
    let mut writer = CountingWriter::new(Vec::new());
    let error = write_serving_binary_v3_assigned_by_code_copy_from_pg_binary_reader(
        &mut reader,
        &mut writer,
        ServingBinaryTargetCopyFormat::SharedBinary,
        1024,
    )
    .unwrap_err();
    assert!(error.to_string().contains("must be ordered"));

    let changed_count = pg_binary_copy_rows(&[
        vec![
            pg_i32_field(1),
            pg_i32_field(4),
            pg_i32_field(2),
            pg_i32_field(0),
        ],
        vec![
            pg_i32_field(2),
            pg_i32_field(4),
            pg_i32_field(3),
            pg_i32_field(1),
        ],
    ]);
    let mut reader = Cursor::new(changed_count);
    let mut writer = CountingWriter::new(Vec::new());
    let error = write_serving_binary_v3_assigned_by_code_copy_from_pg_binary_reader(
        &mut reader,
        &mut writer,
        ServingBinaryTargetCopyFormat::SharedBinary,
        1024,
    )
    .unwrap_err();
    assert!(error.to_string().contains("provider_count changed"));
}
#[test]
fn serving_binary_v3_price_dictionary_streams_dense_aligned_fragments() {
    let price_ids = [
        test_price_id(0xa1),
        test_price_id(0xa2),
        test_price_id(0xa3),
    ];
    let input = pg_binary_copy_rows(
        &price_ids
            .iter()
            .enumerate()
            .map(|(price_key, price_id)| {
                vec![pg_i64_field(price_key as i64), Some(price_id.to_vec())]
            })
            .collect::<Vec<_>>(),
    );
    let mut reader = Cursor::new(input);
    let mut writer = CountingWriter::new(Vec::new());

    let summary = write_serving_binary_v3_price_dictionary_copy_from_pg_binary_reader(
        &mut reader,
        &mut writer,
        ServingBinaryTargetCopyFormat::SharedBinary,
        32,
    )
    .unwrap();
    let pg_binary_output = writer.inner;
    let records = read_test_shared_binary_records(pg_binary_output.clone());
    let expected_payload = price_ids.into_iter().flatten().collect::<Vec<_>>();

    let mut fixed_input = Vec::new();
    for (price_key, price_id) in price_ids.iter().enumerate() {
        fixed_input.extend_from_slice(&(price_key as u32).to_be_bytes());
        fixed_input.extend_from_slice(price_id);
    }
    let mut fixed_reader = Cursor::new(fixed_input);
    let mut fixed_writer = CountingWriter::new(Vec::new());
    let fixed_summary = write_serving_binary_v3_price_dictionary_copy_from_fixed_reader(
        &mut fixed_reader,
        &mut fixed_writer,
        ServingBinaryTargetCopyFormat::SharedBinary,
        32,
    )
    .unwrap();

    assert_eq!(records.len(), 2);
    assert_eq!(fixed_writer.inner, pg_binary_output);
    assert_eq!(fixed_summary["price_set_count"], summary["price_set_count"]);
    assert_eq!(fixed_summary["storage"], summary["storage"]);
    assert!(records
        .iter()
        .all(|record| record.kind == PTG2_SERVING_BINARY_BY_CODE_DICTIONARY_KIND));
    assert_eq!(records[0].block_key, 0);
    assert_eq!(records[0].block_no, 0);
    assert_eq!(records[0].entry_count, 2);
    assert_eq!(records[0].payload.len(), 32);
    assert_eq!(records[1].block_no, 1);
    assert_eq!(records[1].entry_count, 1);
    assert_eq!(records[1].payload.len(), 16);
    assert_eq!(
        logical_test_payload(&records, PTG2_SERVING_BINARY_BY_CODE_DICTIONARY_KIND, 0,),
        expected_payload
    );
    assert_eq!(
        summary["encoder_kind"],
        PTG2_SERVING_BINARY_PRICE_DICTIONARY_V3_ENCODER_KIND
    );
    assert_eq!(
        summary["artifact_kind"],
        PTG2_SERVING_BINARY_BY_CODE_DICTIONARY_KIND
    );
    assert_eq!(summary["price_set_count"], 3);
    assert_eq!(summary["copy_record_count"], 2);
    assert_eq!(summary["storage"]["entry_count"], 3);
}
#[test]
fn serving_binary_v3_price_dictionary_rejects_dense_gaps_and_emits_empty_artifact() {
    let gap = pg_binary_copy_rows(&[
        vec![pg_i32_field(0), Some(test_price_id(0xa1).to_vec())],
        vec![pg_i32_field(2), Some(test_price_id(0xa2).to_vec())],
    ]);
    let mut reader = Cursor::new(gap);
    let mut writer = CountingWriter::new(Vec::new());
    let error = write_serving_binary_v3_price_dictionary_copy_from_pg_binary_reader(
        &mut reader,
        &mut writer,
        ServingBinaryTargetCopyFormat::SharedBinary,
        1024,
    )
    .unwrap_err();
    assert!(error.to_string().contains("expected 1, got 2"));

    let text_id = pg_binary_copy_rows(&[vec![
        pg_i32_field(0),
        Some(b"00000000000000000000000000000001".to_vec()),
    ]]);
    let mut reader = Cursor::new(text_id);
    let mut writer = CountingWriter::new(Vec::new());
    let error = write_serving_binary_v3_price_dictionary_copy_from_pg_binary_reader(
        &mut reader,
        &mut writer,
        ServingBinaryTargetCopyFormat::SharedBinary,
        1024,
    )
    .unwrap_err();
    assert!(error.to_string().contains("must contain 16 bytes"));

    let mut reader = Cursor::new(pg_binary_copy_rows(&[]));
    let mut writer = CountingWriter::new(Vec::new());
    let summary = write_serving_binary_v3_price_dictionary_copy_from_pg_binary_reader(
        &mut reader,
        &mut writer,
        ServingBinaryTargetCopyFormat::SharedBinary,
        1024,
    )
    .unwrap();
    let records = read_test_shared_binary_records(writer.inner);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].kind, PTG2_SERVING_BINARY_BY_CODE_DICTIONARY_KIND);
    assert_eq!(records[0].entry_count, 0);
    assert!(records[0].payload.is_empty());
    assert_eq!(summary["price_set_count"], 0);
    assert_eq!(summary["copy_record_count"], 1);
}
#[test]
fn serving_binary_v3_memberships_fragment_and_span_price_keys() {
    let input = pg_binary_copy_rows(&[
        vec![pg_i32_field(0), pg_i32_field(1)],
        vec![pg_i32_field(0), pg_i32_field(2)],
        vec![pg_i32_field(512), pg_i32_field(7)],
    ]);
    let mut reader = Cursor::new(input);
    let mut writer = CountingWriter::new(Vec::new());

    let summary = write_serving_binary_v3_memberships_copy_from_pg_binary_reader(
        &mut reader,
        &mut writer,
        ServingBinaryTargetCopyFormat::SharedBinary,
        8,
        24,
    )
    .unwrap();
    let records = read_test_shared_binary_records(writer.inner);
    let block_zero_records = records
        .iter()
        .filter(|record| record.block_key == 0)
        .collect::<Vec<_>>();

    assert!(block_zero_records.len() > 1);
    assert_eq!(block_zero_records[0].entry_count, 1);
    for (block_number, record) in block_zero_records.iter().enumerate() {
        assert_eq!(record.block_no, block_number as i32);
        if block_number > 0 {
            assert_eq!(record.entry_count, 0);
        }
    }
    assert!(records.iter().all(|record| record.payload.len() <= 8));
    assert_eq!(
        ptg2_serving_binary_v3::decode_price_memberships(&logical_test_payload(
            &records,
            PTG2_SERVING_BINARY_PRICE_SET_ATOM_MEMBERSHIPS_V3_KIND,
            0,
        ))
        .unwrap(),
        BTreeMap::from([(0, vec![1, 2])])
    );
    assert_eq!(
        ptg2_serving_binary_v3::decode_price_memberships(&logical_test_payload(
            &records,
            PTG2_SERVING_BINARY_PRICE_SET_ATOM_MEMBERSHIPS_V3_KIND,
            1,
        ))
        .unwrap(),
        BTreeMap::from([(512, vec![7])])
    );
    assert_eq!(summary["atom_key_bits"], 24);
    assert_eq!(summary["atom_key_bytes"], 3);
    assert_eq!(summary["block_span"], 512);
    assert_eq!(summary["block_count"], 2);
    assert_eq!(summary["price_set_count"], 2);
    assert_eq!(summary["atom_reference_count"], 3);
    assert!(summary["copy_record_count"].as_u64().unwrap() > 2);
}
#[test]
fn deferred_v3_atom_streams_emit_shared_block_staging_copy() {
    let target_format = ServingBinaryTargetCopyFormat::SharedBinary;
    let membership_input = pg_binary_copy_rows(&[vec![pg_i32_field(0), pg_i32_field(7)]]);
    let mut membership_reader = Cursor::new(membership_input);
    let mut membership_writer = CountingWriter::new(Vec::new());
    write_serving_binary_v3_memberships_copy_from_pg_binary_reader(
        &mut membership_reader,
        &mut membership_writer,
        target_format,
        V3_FINALIZER_HOT_BLOCK_BYTES,
        24,
    )
    .unwrap();
    let membership_records = read_test_shared_binary_records(membership_writer.inner);
    assert_eq!(membership_records.len(), 1);
    assert_eq!(
        membership_records[0].kind,
        PTG2_SERVING_BINARY_PRICE_SET_ATOM_MEMBERSHIPS_V3_KIND
    );

    let atom_input = pg_binary_copy_rows(&[pg_v3_price_atom_row(0, None, [None; 7])]);
    let mut atom_reader = Cursor::new(atom_input);
    let mut atom_writer = CountingWriter::new(Vec::new());
    write_serving_binary_v3_price_atoms_copy_from_pg_binary_reader(
        &mut atom_reader,
        &mut atom_writer,
        ServingBinaryTargetCopyFormat::SharedBinary,
        V3_FINALIZER_HOT_BLOCK_BYTES,
        24,
    )
    .unwrap();
    let atom_records = read_test_shared_binary_records(atom_writer.inner);
    assert_eq!(atom_records.len(), 1);
    assert_eq!(
        atom_records[0].kind,
        PTG2_SERVING_BINARY_PRICE_ATOMS_V3_KIND
    );
}
#[test]
fn serving_binary_v3_memberships_enforce_width_and_strict_pairs() {
    let wide_atom = pg_binary_copy_rows(&[vec![pg_i32_field(0), pg_i64_field(1 << 24)]]);
    let mut reader = Cursor::new(wide_atom.clone());
    let mut writer = CountingWriter::new(Vec::new());
    let error = write_serving_binary_v3_memberships_copy_from_pg_binary_reader(
        &mut reader,
        &mut writer,
        ServingBinaryTargetCopyFormat::SharedBinary,
        1024,
        24,
    )
    .unwrap_err();
    assert!(error.to_string().contains("does not fit in 24 bits"));

    let mut reader = Cursor::new(wide_atom);
    let mut writer = CountingWriter::new(Vec::new());
    let summary = write_serving_binary_v3_memberships_copy_from_pg_binary_reader(
        &mut reader,
        &mut writer,
        ServingBinaryTargetCopyFormat::SharedBinary,
        1024,
        32,
    )
    .unwrap();
    let records = read_test_shared_binary_records(writer.inner);
    assert_eq!(summary["atom_key_bytes"], 4);
    assert_eq!(
        ptg2_serving_binary_v3::decode_price_memberships(&logical_test_payload(
            &records,
            PTG2_SERVING_BINARY_PRICE_SET_ATOM_MEMBERSHIPS_V3_KIND,
            0,
        ))
        .unwrap(),
        BTreeMap::from([(0, vec![1 << 24])])
    );

    let duplicate = pg_binary_copy_rows(&[
        vec![pg_i32_field(0), pg_i32_field(1)],
        vec![pg_i32_field(0), pg_i32_field(1)],
    ]);
    let mut reader = Cursor::new(duplicate);
    let mut writer = CountingWriter::new(Vec::new());
    write_serving_binary_v3_memberships_copy_from_pg_binary_reader(
        &mut reader,
        &mut writer,
        ServingBinaryTargetCopyFormat::SharedBinary,
        1024,
        24,
    )
    .unwrap();
    let records = read_test_shared_binary_records(writer.inner);
    assert_eq!(
        ptg2_serving_binary_v3::decode_price_memberships(&logical_test_payload(
            &records,
            PTG2_SERVING_BINARY_PRICE_SET_ATOM_MEMBERSHIPS_V3_KIND,
            0,
        ))
        .unwrap(),
        BTreeMap::from([(0, vec![1, 1])])
    );
    assert!(serving_binary_v3_validate_atom_key(0, 16).is_err());
    assert!(serving_binary_v3_validate_atom_key(-1, 24).is_err());
    assert_eq!(serving_binary_v3_atom_key_bits("24").unwrap(), 24);
    assert_eq!(serving_binary_v3_atom_key_bits("32").unwrap(), 32);
    assert!(serving_binary_v3_atom_key_bits("3").is_err());
    assert!(serving_binary_i32_count(usize::MAX, "count").is_err());
    assert!(serving_binary_v3_block_key(i64::MAX, 1, "key").is_err());

    let _env_guard = scanner_env_lock().lock().unwrap();
    {
        let _atom_bits = TestEnvVar::set(PTG2_SERVING_BINARY_V3_ATOM_KEY_BITS_ENV, "32");
        assert_eq!(serving_binary_v3_configured_atom_key_bits(&[]).unwrap(), 32);
    }
    let _atom_bits = TestEnvVar::remove(PTG2_SERVING_BINARY_V3_ATOM_KEY_BITS_ENV);
    let _atom_count = TestEnvVar::set(PTG2_SERVING_BINARY_V3_ATOM_COUNT_ENV, "not-a-count");
    assert!(serving_binary_v3_configured_atom_key_bits(&[]).is_err());
}
#[test]
fn serving_binary_v3_price_atoms_decode_numeric_and_text_goldens() {
    let input = pg_binary_copy_rows(&[
        pg_v3_price_atom_row(
            0,
            Some(pg_binary_numeric(false, 0, 2, &[15, 2500])),
            [Some(0), None, Some(2), Some(3), Some(4), Some(5), None],
        ),
        pg_v3_price_atom_row(
            1,
            Some(b"20.50".to_vec()),
            [
                Some(1),
                Some(8),
                Some(13),
                None,
                Some(21),
                Some(34),
                Some(55),
            ],
        ),
    ]);
    let mut reader = Cursor::new(input);
    let mut writer = CountingWriter::new(Vec::new());

    let summary = write_serving_binary_v3_price_atoms_copy_from_pg_binary_reader(
        &mut reader,
        &mut writer,
        ServingBinaryTargetCopyFormat::SharedBinary,
        64 * 1024,
        24,
    )
    .unwrap();
    let records = read_test_shared_binary_records(writer.inner);
    let atoms = ptg2_serving_binary_v3::decode_price_atoms(&logical_test_payload(
        &records,
        PTG2_SERVING_BINARY_PRICE_ATOMS_V3_KIND,
        0,
    ))
    .unwrap();

    assert_eq!(atoms.len(), 2);
    assert_eq!(atoms[0].negotiated_rate.as_deref(), Some("15.25"));
    assert_eq!(atoms[1].negotiated_rate.as_deref(), Some("20.50"));
    assert_eq!(
        atoms[0].attribute_keys,
        vec![Some(0), None, Some(2), Some(3), Some(4), Some(5), None]
    );
    assert_eq!(
        summary["artifact_kind"],
        PTG2_SERVING_BINARY_PRICE_ATOMS_V3_KIND
    );
    assert_eq!(summary["atom_count"], 2);
    assert_eq!(summary["attribute_count"], 7);
    assert_eq!(summary["block_span"], 512);
    assert_eq!(
        pg_binary_numeric_text(&pg_binary_numeric(false, -1, 2, &[100])).unwrap(),
        "0.01"
    );
    assert_eq!(
        pg_binary_numeric_text(&pg_binary_numeric(true, 1, 0, &[1, 2])).unwrap(),
        "-10002"
    );
}
#[test]
fn serving_binary_v3_price_atoms_use_dense_512_atom_spans() {
    let rows = (0..=512)
        .map(|atom_key| pg_v3_price_atom_row(atom_key, None, [None; 7]))
        .collect::<Vec<_>>();
    let input = pg_binary_copy_rows(&rows);
    let mut reader = Cursor::new(input);
    let mut writer = CountingWriter::new(Vec::new());

    let summary = write_serving_binary_v3_price_atoms_copy_from_pg_binary_reader(
        &mut reader,
        &mut writer,
        ServingBinaryTargetCopyFormat::SharedBinary,
        64 * 1024,
        24,
    )
    .unwrap();
    let records = read_test_shared_binary_records(writer.inner);

    assert_eq!(summary["atom_count"], 513);
    assert_eq!(summary["block_count"], 2);
    assert_eq!(
        records
            .iter()
            .map(|record| record.block_key)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert_eq!(records[0].entry_count, 512);
    assert_eq!(records[1].entry_count, 1);
    assert_eq!(
        ptg2_serving_binary_v3::decode_price_atoms(&records[0].payload)
            .unwrap()
            .len(),
        512
    );
    assert_eq!(
        ptg2_serving_binary_v3::decode_price_atoms(&records[1].payload)
            .unwrap()
            .len(),
        1
    );
}
#[test]
fn serving_binary_v3_price_atoms_reject_gaps_null_keys_and_corrupt_numeric() {
    let dense_gap = pg_binary_copy_rows(&[pg_v3_price_atom_row(1, None, [None; 7])]);
    let mut reader = Cursor::new(dense_gap);
    let mut writer = CountingWriter::new(Vec::new());
    let error = write_serving_binary_v3_price_atoms_copy_from_pg_binary_reader(
        &mut reader,
        &mut writer,
        ServingBinaryTargetCopyFormat::SharedBinary,
        1024,
        24,
    )
    .unwrap_err();
    assert!(error.to_string().contains("expected 0, got 1"));

    let mut null_atom_row = pg_v3_price_atom_row(0, None, [None; 7]);
    null_atom_row[0] = None;
    let mut reader = Cursor::new(pg_binary_copy_rows(&[null_atom_row]));
    let mut writer = CountingWriter::new(Vec::new());
    let error = write_serving_binary_v3_price_atoms_copy_from_pg_binary_reader(
        &mut reader,
        &mut writer,
        ServingBinaryTargetCopyFormat::SharedBinary,
        1024,
        24,
    )
    .unwrap_err();
    assert!(error.to_string().contains("atom_key cannot be NULL"));

    let corrupt_numeric =
        pg_binary_copy_rows(&[pg_v3_price_atom_row(0, Some(vec![0; 7]), [None; 7])]);
    let mut reader = Cursor::new(corrupt_numeric);
    let mut writer = CountingWriter::new(Vec::new());
    let error = write_serving_binary_v3_price_atoms_copy_from_pg_binary_reader(
        &mut reader,
        &mut writer,
        ServingBinaryTargetCopyFormat::SharedBinary,
        1024,
        24,
    )
    .unwrap_err();
    assert!(error.to_string().contains("invalid byte count"));

    let negative_attribute = pg_binary_copy_rows(&[pg_v3_price_atom_row(
        0,
        None,
        [Some(-1), None, None, None, None, None, None],
    )]);
    let mut reader = Cursor::new(negative_attribute);
    let mut writer = CountingWriter::new(Vec::new());
    let error = write_serving_binary_v3_price_atoms_copy_from_pg_binary_reader(
        &mut reader,
        &mut writer,
        ServingBinaryTargetCopyFormat::SharedBinary,
        1024,
        24,
    )
    .unwrap_err();
    assert!(error.to_string().contains("cannot be negative"));
}
#[test]
fn manifest_sidecar_collector_sorts_and_merges_members() {
    let provider_set_id = GlobalId128([5; GLOBAL_ID_BYTES]);
    let mut collector = ManifestSidecarCollector::default();

    collector
        .record_provider_set(
            provider_set_id,
            &[20, 10, 20],
            &[91, 92],
            &[1003002106, 1003007311],
        )
        .unwrap();

    let entries = collector.provider_forward_entries().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].owner, provider_set_id);
    assert_eq!(entries[0].members.len(), 2);
    assert!(entries[0].members[0] < entries[0].members[1]);
    let inverted = collector.provider_inverted_entries().unwrap();
    assert_eq!(inverted.len(), 2);
    assert!(inverted
        .iter()
        .all(|entry| entry.members == vec![provider_set_id]));
    let provider_npi = collector.provider_npi_entries().unwrap();
    assert_eq!(provider_npi.len(), 1);
    assert_eq!(provider_npi[0].owner, provider_set_id);
    assert_eq!(provider_npi[0].members.len(), 2);
}
#[test]
fn manifest_sidecar_collector_can_spill_members_before_sidecar_write() {
    let provider_set_id = GlobalId128([5; GLOBAL_ID_BYTES]);
    let mut collector = ManifestSidecarCollector {
        spools: Some(ManifestSidecarSpools::all().unwrap()),
        ..ManifestSidecarCollector::default()
    };

    collector
        .record_provider_set(
            provider_set_id,
            &[20, 10, 20],
            &[91, 92],
            &[1003002106, 1003007311],
        )
        .unwrap();

    assert!(collector.provider_forward.is_empty());
    assert!(collector.provider_inverted.is_empty());
    assert!(collector.provider_npi.is_empty());
    let entries = collector.provider_forward_entries().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].owner, provider_set_id);
    assert_eq!(entries[0].members.len(), 2);
    let inverted = collector.provider_inverted_entries().unwrap();
    assert_eq!(inverted.len(), 2);
    assert!(inverted
        .iter()
        .all(|entry| entry.members == vec![provider_set_id]));
    assert_eq!(collector.provider_npi_entries().unwrap().len(), 1);
    assert!(collector.price_forward_entries().unwrap().is_empty());
}
#[test]
fn manifest_sidecar_spools_only_open_configured_artifacts() {
    let paths = CopyPathConfig {
        manifest_provider_forward_sidecar: Some("provider-forward.ptg2sc".to_string()),
        ..CopyPathConfig::default()
    };

    let spools = ManifestSidecarSpools::for_paths(&paths).unwrap();
    let import = ManifestSidecarCollector::for_import(&paths).unwrap();

    assert!(spools.provider_forward.is_some());
    assert!(spools.provider_inverted.is_none());
    assert!(spools.provider_npi.is_none());
    assert!(spools.price_forward.is_none());
    assert!(import.spools.is_some());
}
#[test]
fn configured_manifest_sidecars_write_independent_spools_in_parallel() {
    let base =
        std::env::temp_dir().join(format!("ptg2-parallel-sidecar-test-{}", std::process::id()));
    let forward_path = base.with_extension("forward.ptg2sc");
    let inverted_path = base.with_extension("inverted.ptg2sc");
    let paths = CopyPathConfig {
        manifest_provider_forward_sidecar: Some(forward_path.display().to_string()),
        manifest_provider_inverted_sidecar: Some(inverted_path.display().to_string()),
        ..CopyPathConfig::default()
    };
    let mut collector = ManifestSidecarCollector {
        spools: Some(ManifestSidecarSpools::for_paths(&paths).unwrap()),
        ..ManifestSidecarCollector::default()
    };
    collector
        .record_provider_set(
            GlobalId128([5; GLOBAL_ID_BYTES]),
            &[20, 10, 20],
            &[91, 92],
            &[1003002106, 1003007311],
        )
        .unwrap();

    let semantic_progress = Arc::new(ScannerSemanticProgress::default());
    let results = configured_spooled_manifest_sidecars(
        &paths,
        &mut collector,
        Some(Arc::clone(&semantic_progress)),
    )
    .unwrap()
    .unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(
        results[0].record_kind,
        "manifest_provider_forward_sidecar_file"
    );
    assert_eq!(results[0].entry_count, 1);
    assert_eq!(
        results[1].record_kind,
        "manifest_provider_inverted_sidecar_file"
    );
    assert_eq!(results[1].entry_count, 2);
    assert_eq!(&std::fs::read(&forward_path).unwrap()[..8], b"PTG2MNDS");
    assert_eq!(&std::fs::read(&inverted_path).unwrap()[..8], b"PTG2MNDS");
    let progress = semantic_progress.snapshot();
    assert_eq!(progress.scan_finalize_jobs_started, 2);
    assert_eq!(progress.scan_finalize_jobs_completed, 2);
    assert!(progress.scan_finalize_bytes_processed > 0);
    assert!(progress.scan_finalize_pairs_processed > 0);
    assert_eq!(progress.scan_finalize_chunks_sorted, 2);
    let _ = std::fs::remove_file(forward_path);
    let _ = std::fs::remove_file(inverted_path);
}
#[test]
fn configured_manifest_sidecars_recreate_attempt_output_directory() {
    let base = std::env::temp_dir().join(format!(
        "ptg2-sidecar-parent-test-{}-{:?}",
        std::process::id(),
        thread::current().id()
    ));
    let output_path = base.join("nested/provider-forward.ptg2sc");
    let paths = CopyPathConfig {
        manifest_provider_forward_sidecar: Some(output_path.display().to_string()),
        ..CopyPathConfig::default()
    };
    let mut collector = ManifestSidecarCollector {
        spools: Some(ManifestSidecarSpools::for_paths(&paths).unwrap()),
        ..ManifestSidecarCollector::default()
    };
    collector
        .record_provider_set(
            GlobalId128([5; GLOBAL_ID_BYTES]),
            &[20],
            &[91],
            &[1003002106],
        )
        .unwrap();

    let results = configured_spooled_manifest_sidecars(&paths, &mut collector, None)
        .unwrap()
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(&std::fs::read(&output_path).unwrap()[..8], b"PTG2MNDS");
    let _ = std::fs::remove_dir_all(base);
}
#[test]
fn external_manifest_sidecar_sort_recreates_attempt_output_directory() {
    let base = std::env::temp_dir().join(format!(
        "ptg2-external-sidecar-parent-test-{}-{:?}",
        std::process::id(),
        thread::current().id()
    ));
    let output_path = base.join("nested/provider-forward.ptg2sc");
    let mut spool = ManifestPairSpool::new("external_parent_recovery").unwrap();
    let owner = GlobalId128([5; GLOBAL_ID_BYTES]);
    spool
        .push(owner, GlobalId128([6; GLOBAL_ID_BYTES]))
        .unwrap();
    spool
        .push(owner, GlobalId128([7; GLOBAL_ID_BYTES]))
        .unwrap();

    spool
        .write_dense_sidecar_with_chunk_bytes(
            &output_path.display().to_string(),
            MANIFEST_PAIR_RECORD_BYTES,
        )
        .unwrap();

    assert_eq!(&std::fs::read(&output_path).unwrap()[..8], b"PTG2MNDS");
    let _ = std::fs::remove_dir_all(base);
}
#[test]
fn manifest_sidecar_spool_failure_names_source_and_output_paths() {
    let output_path = std::env::temp_dir().join(format!(
        "ptg2-missing-spool-output-{}-{:?}.ptg2sc",
        std::process::id(),
        thread::current().id()
    ));
    let spool = ManifestPairSpool::new("missing_spool_diagnostic").unwrap();
    let spool_path = spool.path.clone();
    std::fs::remove_file(&spool_path).unwrap();

    let error = match write_manifest_sidecar_job(
        ManifestSidecarWriteJob {
            order: 0,
            record_kind: "manifest_provider_forward_sidecar_file",
            path: output_path.display().to_string(),
            spool,
        },
        None,
    ) {
        Ok(_) => panic!("missing manifest spool unexpectedly finalized"),
        Err(error) => error,
    };
    let message = error.to_string();

    assert!(message.contains("manifest_provider_forward_sidecar_file"));
    assert!(message.contains(&spool_path.display().to_string()));
    assert!(message.contains(&output_path.display().to_string()));
    let _ = std::fs::remove_file(output_path);
}
#[test]
fn manifest_dense_sidecar_external_sort_matches_in_memory_format() {
    let output_prefix = std::env::temp_dir().join(format!(
        "ptg2-dense-sidecar-sort-test-{}",
        std::process::id()
    ));
    let in_memory_path = output_prefix.with_extension("memory.ptg2sc");
    let external_path = output_prefix.with_extension("external.ptg2sc");
    let owner_a = GlobalId128([1; GLOBAL_ID_BYTES]);
    let owner_b = GlobalId128([2; GLOBAL_ID_BYTES]);
    let member_a = GlobalId128([7; GLOBAL_ID_BYTES]);
    let member_b = GlobalId128([8; GLOBAL_ID_BYTES]);
    let member_c = GlobalId128([9; GLOBAL_ID_BYTES]);
    let pairs = [
        (owner_b, member_c),
        (owner_a, member_b),
        (owner_a, member_a),
        (owner_b, member_a),
        (owner_a, member_b),
        (owner_b, member_b),
    ];
    let mut in_memory_spool = ManifestPairSpool::new("dense_parity_memory").unwrap();
    let mut external_spool = ManifestPairSpool::new("dense_parity_external").unwrap();
    for (owner, member) in pairs {
        in_memory_spool.push(owner, member).unwrap();
        external_spool.push(owner, member).unwrap();
    }

    let in_memory_metrics = in_memory_spool
        .write_dense_sidecar_with_chunk_bytes(in_memory_path.to_str().unwrap(), usize::MAX)
        .unwrap();
    let semantic_progress = Arc::new(ScannerSemanticProgress::default());
    let external_metrics = external_spool
        .write_dense_sidecar_with_chunk_bytes_and_progress(
            external_path.to_str().unwrap(),
            MANIFEST_PAIR_RECORD_BYTES * 2,
            Some(Arc::clone(&semantic_progress)),
        )
        .unwrap();

    assert_eq!(external_metrics, in_memory_metrics);
    assert_eq!(external_metrics, (2, 5));
    assert_eq!(
        std::fs::read(&external_path).unwrap(),
        std::fs::read(&in_memory_path).unwrap()
    );
    let progress = semantic_progress.snapshot();
    assert_eq!(progress.scan_finalize_jobs_started, 1);
    assert_eq!(progress.scan_finalize_jobs_completed, 1);
    assert!(progress.scan_finalize_bytes_processed > 0);
    assert!(progress.scan_finalize_pairs_processed > pairs.len() as u64);
    assert_eq!(progress.scan_finalize_chunks_sorted, 3);
    assert_eq!(progress.scan_finalize_chunks_merged, 3);
    assert!(progress.scan_finalize_sort_comparisons > 0);
    let _ = std::fs::remove_file(in_memory_path);
    let _ = std::fs::remove_file(external_path);
}
#[test]
fn manifest_standard_sidecar_is_sorted_deduplicated_and_handles_empty_input() {
    let temporary = tempfile::tempdir().unwrap();
    let populated_path = temporary.path().join("populated.ptg2sc");
    let empty_path = temporary.path().join("empty.ptg2sc");
    let owner_a = GlobalId128([1; GLOBAL_ID_BYTES]);
    let owner_b = GlobalId128([2; GLOBAL_ID_BYTES]);
    let member_a = GlobalId128([7; GLOBAL_ID_BYTES]);
    let member_b = GlobalId128([8; GLOBAL_ID_BYTES]);
    let mut populated = ManifestPairSpool::new("standard_populated").unwrap();
    for pair in [
        (owner_b, member_b),
        (owner_a, member_b),
        (owner_a, member_a),
        (owner_a, member_b),
    ] {
        populated.push(pair.0, pair.1).unwrap();
    }
    assert_eq!(
        populated
            .write_standard_sidecar(populated_path.to_str().unwrap())
            .unwrap(),
        (2, 3)
    );
    let bytes = std::fs::read(&populated_path).unwrap();
    assert_eq!(&bytes[..8], b"PTG2MNSC");
    assert_eq!(u32::from_le_bytes(bytes[8..12].try_into().unwrap()), 1);
    assert_eq!(u64::from_le_bytes(bytes[12..20].try_into().unwrap()), 2);
    assert_eq!(bytes.len(), 20 + 2 * 28 + 3 * GLOBAL_ID_BYTES);

    let mut empty = ManifestPairSpool::new("standard_empty").unwrap();
    assert_eq!(
        empty
            .write_standard_sidecar(empty_path.to_str().unwrap())
            .unwrap(),
        (0, 0)
    );
    assert_eq!(std::fs::read(&empty_path).unwrap().len(), 20);
}
#[test]
fn manifest_copy_merge_dedupes_by_kind_key() {
    let base = std::env::temp_dir().join(format!("ptg2-merge-test-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&base);
    let input_a = base.join("a.copy");
    let input_b = base.join("b.copy");
    let output = base.join("out.copy");
    std::fs::write(&input_a, b"b\t2\nmanifest\t1\t2\t3\t4\t5\t6\t7\t\\N\n").unwrap();
    std::fs::write(&input_b, b"a\t1\nmanifest\t1\t2\t3\t4\t5\t6\t7\ttrace\n").unwrap();

    merge_manifest_copy_files(
        "manifest_serving",
        &output,
        &[
            input_a.to_string_lossy().to_string(),
            input_b.to_string_lossy().to_string(),
        ],
    )
    .unwrap();

    let merged = std::fs::read_to_string(&output).unwrap();
    assert_eq!(merged, "a\t1\nb\t2\nmanifest\t1\t2\t3\t4\t5\t6\t7\ttrace\n");
    let _ = std::fs::remove_dir_all(base);
}
#[test]
fn manifest_copy_merge_dedupes_lean_serving_by_full_row() {
    let base = std::env::temp_dir().join(format!("ptg2-lean-merge-test-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&base);
    let input_a = base.join("a.copy");
    let input_b = base.join("b.copy");
    let output = base.join("out.copy");
    let row_a = "plan\tCPT\t29888\tprovider-set\t2\tprice-set-a\n";
    let row_b = "plan\tCPT\t29888\tprovider-set\t2\tprice-set-b\n";
    std::fs::write(&input_a, format!("{row_b}{row_b}")).unwrap();
    std::fs::write(&input_b, row_a).unwrap();

    merge_manifest_copy_files(
        "manifest_lean_serving",
        &output,
        &[
            input_a.to_string_lossy().to_string(),
            input_b.to_string_lossy().to_string(),
        ],
    )
    .unwrap();

    let merged = std::fs::read_to_string(&output).unwrap();
    assert_eq!(merged, format!("{row_a}{row_b}"));
    let _ = std::fs::remove_dir_all(base);
}
#[test]
fn manifest_copy_merge_dedupes_price_set_atoms_by_full_pair() {
    let base = std::env::temp_dir().join(format!(
        "ptg2-price-set-atom-merge-test-{}",
        std::process::id()
    ));
    let _ = std::fs::create_dir_all(&base);
    let input_a = base.join("a.copy");
    let input_b = base.join("b.copy");
    let output = base.join("out.copy");
    let atom_a = "price-set-a\tatom-a\n";
    let atom_b = "price-set-a\tatom-b\n";
    std::fs::write(&input_a, format!("{atom_b}{atom_b}")).unwrap();
    std::fs::write(&input_b, atom_a).unwrap();

    merge_manifest_copy_files(
        "price_set_atom",
        &output,
        &[
            input_a.to_string_lossy().to_string(),
            input_b.to_string_lossy().to_string(),
        ],
    )
    .unwrap();

    let merged = std::fs::read_to_string(&output).unwrap();
    assert_eq!(merged, format!("{atom_a}{atom_b}"));
    let _ = std::fs::remove_dir_all(base);
}
#[test]
fn manifest_copy_merge_parallel_chunk_sort_matches_serial_output() {
    let base =
        std::env::temp_dir().join(format!("ptg2-merge-parallel-test-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&base);
    let input_a = base.join("a.copy");
    let input_b = base.join("b.copy");
    let serial_output = base.join("serial.copy");
    let parallel_output = base.join("parallel.copy");
    let payload_a = "a".repeat(700_000);
    let payload_b = "b".repeat(700_000);
    let payload_c = "c".repeat(700_000);
    std::fs::write(
        &input_a,
        format!(
            "g2\t200\t{payload_a}\n\
             g1\t100\t{payload_b}\n\
             g3\t300\t{payload_c}\n\
             g1\t100\t{payload_b}\n"
        ),
    )
    .unwrap();
    std::fs::write(
        &input_b,
        format!(
            "g4\t400\t{payload_a}\n\
             g2\t200\t{payload_a}\n\
             g5\t500\t{payload_c}\n"
        ),
    )
    .unwrap();

    std::env::remove_var("HLTHPRT_PTG2_MANIFEST_MERGE_SORT_WORKERS");
    std::env::remove_var("HLTHPRT_PTG2_MANIFEST_MERGE_CHUNK_BYTES");
    merge_manifest_copy_files(
        "provider_group_member",
        &serial_output,
        &[
            input_a.to_string_lossy().to_string(),
            input_b.to_string_lossy().to_string(),
        ],
    )
    .unwrap();

    std::env::set_var("HLTHPRT_PTG2_MANIFEST_MERGE_SORT_WORKERS", "2");
    std::env::set_var("HLTHPRT_PTG2_MANIFEST_MERGE_CHUNK_BYTES", "1");
    merge_manifest_copy_files(
        "provider_group_member",
        &parallel_output,
        &[
            input_a.to_string_lossy().to_string(),
            input_b.to_string_lossy().to_string(),
        ],
    )
    .unwrap();
    std::env::remove_var("HLTHPRT_PTG2_MANIFEST_MERGE_SORT_WORKERS");
    std::env::remove_var("HLTHPRT_PTG2_MANIFEST_MERGE_CHUNK_BYTES");

    let serial = std::fs::read_to_string(&serial_output).unwrap();
    let parallel = std::fs::read_to_string(&parallel_output).unwrap();
    assert_eq!(parallel, serial);
    assert_eq!(parallel.lines().count(), 5);
    let _ = std::fs::remove_dir_all(base);
}
#[test]
fn raw_rate_chunks_flush_when_byte_limit_is_reached() {
    let payload = br#"{
        "billing_code_type": "CPT",
        "billing_code": "99213",
        "negotiated_rates": [
            {"provider_references":[7],"negotiated_prices":[{"negotiated_type":"negotiated","negotiated_rate":100}]},
            {"provider_references":[7],"negotiated_prices":[{"negotiated_type":"negotiated","negotiated_rate":101}]},
            {"provider_references":[7],"negotiated_prices":[{"negotiated_type":"negotiated","negotiated_rate":102}]}
        ]
    }"#;
    let mut reader = JsonStreamReader::new(&payload[..]);
    let (tx, rx) = bounded::<WorkerJob>(10);
    let (_event_tx, event_rx) = bounded::<CopyFileEvent>(10);
    let mut writer = Vec::new();
    let mut producer_blocked_micros = 0u128;
    let mut stats = RawChunkStats::default();
    let mut copy_file_event_gate = CopyFileEventGate::passthrough();

    let mut enqueue_io = InNetworkEnqueueIo {
        tx: &tx,
        event_rx: &event_rx,
        writer: &mut writer,
        copy_file_event_gate: &mut copy_file_event_gate,
        cancelled: None,
        producer_blocked_micros: &mut producer_blocked_micros,
        raw_chunk_stats: &mut stats,
    };
    let rate_count = enqueue_in_network_struson(
        &mut reader,
        &mut enqueue_io,
        InNetworkEnqueueOptions {
            chunk_size: 100,
            raw_chunk_byte_limit: 1,
            parse_in_workers: true,
            object_ordinal: 0,
        },
    )
    .unwrap();

    drop(tx);
    let jobs: Vec<_> = rx.try_iter().collect();
    stats.merge_from(&RawChunkStats::default());
    assert_eq!(rate_count, 3);
    assert_eq!(stats.chunk_count, 3);
    assert_eq!(stats.max_rates, 1);
    assert!(stats.max_bytes > 0);
    assert!(jobs.iter().all(|job| matches!(
        job,
        WorkerJob::RawRates { raw_rates, .. } if raw_rates.len() == 1
    )));
}
#[test]
fn shared_graph_cli_manifest_parser_preserves_complete_artifact_metadata() {
    let artifact = |name: &str| {
        json!({
            "path": format!("/tmp/{name}.sidecar"),
            "metadata": {
                "record_format": "format-v1",
                "sha256": "a".repeat(64),
                "byte_count": 123,
                "owner_count": 4,
                "member_count": 7,
                "member_global_count": 9,
                "name": name,
                "source_shard_id": "source-1",
                "shard_id": "source-1",
            },
        })
    };
    let shard = shared_graph_shard_from_json(
        &json!({
            "shard_id": "source-1",
            "group_npi": artifact("provider_group_npi"),
            "npi_group": artifact("provider_npi_group"),
            "group_provider_set": artifact("provider_inverted"),
            "provider_set_group": artifact("provider_forward"),
        }),
        0,
    )
    .unwrap();

    assert_eq!(shard.shard_id, "source-1");
    assert_eq!(shard.group_npi.metadata.member_global_count, Some(9));
    assert_eq!(
        shard.group_provider_set.metadata.name.as_deref(),
        Some("provider_inverted")
    );
    assert_eq!(
        shard.provider_set_group.metadata.source_shard_id.as_deref(),
        Some("source-1")
    );
}
#[test]
fn v4_manifest_factor_collectors_cover_spooled_and_in_memory_directions() {
    let provider_set = GlobalId128([1; GLOBAL_ID_BYTES]);
    let atom = GlobalId128([9; GLOBAL_ID_BYTES]);
    let price_set = PriceSetLite {
        global_id: GlobalId128([8; GLOBAL_ID_BYTES]),
        atoms: Vec::new(),
        atom_ids: vec![atom],
    };

    let mut in_memory = ManifestSidecarCollector::default();
    in_memory
        .record_provider_set(provider_set, &[2, 1, 1], &[4, 3, 3], &[1_234_567_890, 0])
        .unwrap();
    in_memory.record_provider_component(3, &[2, 1, 1]).unwrap();
    in_memory.record_provider_component(4, &[2]).unwrap();
    in_memory.record_price_set(&price_set).unwrap();
    assert_eq!(in_memory.provider_forward_entries().unwrap().len(), 1);
    assert_eq!(in_memory.provider_inverted_entries().unwrap().len(), 2);
    assert_eq!(in_memory.provider_set_component_entries().unwrap().len(), 1);
    assert_eq!(
        in_memory.provider_component_group_entries().unwrap().len(),
        2
    );
    assert_eq!(in_memory.provider_npi_entries().unwrap().len(), 1);
    assert_eq!(in_memory.price_forward_entries().unwrap().len(), 1);
    assert!(in_memory
        .write_spooled_standard_sidecar("unknown", "unused", false)
        .unwrap()
        .is_none());

    let factor_paths = CopyPathConfig {
        manifest_provider_set_component_sidecar: Some("set-component".to_owned()),
        manifest_provider_component_group_sidecar: Some("component-group".to_owned()),
        ..CopyPathConfig::default()
    };
    let mut spooled = ManifestSidecarCollector {
        spools: Some(ManifestSidecarSpools::for_paths(&factor_paths).unwrap()),
        ..ManifestSidecarCollector::default()
    };
    spooled
        .record_provider_set(provider_set, &[2, 1], &[4, 3], &[1_234_567_890])
        .unwrap();
    spooled.record_provider_component(3, &[2, 1]).unwrap();
    assert!(spooled.provider_set_component_entries().unwrap().len() == 1);
    assert!(spooled.provider_component_group_entries().unwrap().len() == 1);
}
#[test]
fn direct_spooled_sidecars_and_dictionary_outputs_finalize_exactly() {
    let temporary = tempfile::tempdir().unwrap();
    let provider_set = GlobalId128([1; GLOBAL_ID_BYTES]);
    let price_set = PriceSetLite {
        global_id: GlobalId128([8; GLOBAL_ID_BYTES]),
        atoms: Vec::new(),
        atom_ids: vec![GlobalId128([9; GLOBAL_ID_BYTES])],
    };
    let mut collector = ManifestSidecarCollector {
        spools: Some(ManifestSidecarSpools::all().unwrap()),
        ..ManifestSidecarCollector::default()
    };
    collector
        .record_provider_set(provider_set, &[2, 1], &[4, 3], &[1_234_567_890])
        .unwrap();
    collector.record_provider_component(3, &[2, 1]).unwrap();
    collector.record_price_set(&price_set).unwrap();

    for (name, dense) in [
        ("provider_forward", false),
        ("provider_inverted", false),
        ("provider_set_component", false),
        ("provider_component_group", false),
        ("provider_npi", true),
        ("price_forward", false),
    ] {
        let path = temporary.path().join(format!("{name}.ptg2sc"));
        assert!(collector
            .write_spooled_standard_sidecar(name, path.to_str().unwrap(), dense)
            .unwrap()
            .is_some());
    }
    assert!(collector
        .write_spooled_standard_sidecar(
            "unknown",
            temporary.path().join("unknown").to_str().unwrap(),
            false,
        )
        .unwrap()
        .is_none());

    let path = |name: &str| temporary.path().join(name).display().to_string();
    let paths = CopyPathConfig {
        manifest_price_atom: Some(path("manifest-price-atom.copy")),
        manifest_price_set_atom: Some(path("manifest-price-set-atom.copy")),
        manifest_price_set_summary: Some(path("manifest-price-set-summary.copy")),
        manifest_provider_group_member: Some(path("manifest-provider-member.copy")),
        manifest_code_count: Some(path("manifest-code-count.copy")),
        manifest_provider_set_dictionary: Some(path("manifest-provider-set.copy")),
        procedure: Some(path("procedure.copy")),
        price_code_set: Some(path("price-code-set.copy")),
        price_atom: Some(path("price-atom.copy")),
        price_set_entry: Some(path("price-set-entry.copy")),
        provider_set: Some(path("provider-set.copy")),
        provider_set_component: Some(path("provider-set-component.copy")),
        provider_set_entry: Some(path("provider-set-entry.copy")),
        provider_entry_component: Some(path("provider-entry-component.copy")),
        provider_group_member: Some(path("provider-group-member.copy")),
        ..CopyPathConfig::default()
    };
    let sinks = DictionaryCopySinks::from_paths(&paths, 0).unwrap();
    let stdout = io::stdout();
    let mut events = BufWriter::new(stdout.lock());
    sinks.finish(&mut events).unwrap();

    let mut closed_sinks = DictionaryCopySinks::from_paths(&paths, 0).unwrap();
    closed_sinks.price_code_set.as_mut().unwrap().writer = None;
    closed_sinks.price_set_entry.as_mut().unwrap().writer = None;
    closed_sinks.provider_set.as_mut().unwrap().writer = None;
    closed_sinks.manifest_code_count.as_mut().unwrap().writer = None;
    closed_sinks.provider_set_entry.as_mut().unwrap().writer = None;

    assert_eq!(
        closed_sinks
            .write_price_code_set("price-code-set", &["12345".to_owned()])
            .unwrap_err()
            .kind(),
        io::ErrorKind::BrokenPipe,
    );
    let mut emitted_price_set_entries = HashSet::new();
    assert_eq!(
        closed_sinks
            .write_price_set_entries(
                GlobalId128([2; GLOBAL_ID_BYTES]),
                &[GlobalId128([3; GLOBAL_ID_BYTES])],
                &mut emitted_price_set_entries,
            )
            .unwrap_err()
            .kind(),
        io::ErrorKind::BrokenPipe,
    );
    assert_eq!(
        closed_sinks
            .write_provider_set("provider-set", 1, &[1])
            .unwrap_err()
            .kind(),
        io::ErrorKind::BrokenPipe,
    );
    assert_eq!(
        closed_sinks
            .write_manifest_code_count("plan", Some("CPT"), Some("12345"), 1)
            .unwrap_err()
            .kind(),
        io::ErrorKind::BrokenPipe,
    );
    let mut emitted_provider_set_entries = HashSet::new();
    assert_eq!(
        closed_sinks
            .write_provider_set_entries("provider-set", &[1], &mut emitted_provider_set_entries,)
            .unwrap_err()
            .kind(),
        io::ErrorKind::BrokenPipe,
    );
}
#[test]
fn v4_provider_membership_sidecars_preserve_exact_edges_and_reject_bad_rows() {
    let temporary = tempfile::tempdir().unwrap();
    let group_a = GlobalId128([5; GLOBAL_ID_BYTES]).to_hex();
    let group_b = GlobalId128([6; GLOBAL_ID_BYTES]).to_hex();
    let input_a = temporary.path().join("membership-a.copy");
    let input_b = temporary.path().join("membership-b.copy");
    std::fs::write(
        &input_a,
        format!(
            "{group_a}\t1234567890\r\n\
             {group_b}\t0\n\
             {group_a}\t1234567890\textra\n"
        ),
    )
    .unwrap();
    std::fs::write(&input_b, format!("{group_b}\t2222222222\n")).unwrap();
    let group_npi = temporary.path().join("group-npi.sidecar");
    let npi_group = temporary.path().join("npi-group.sidecar");
    let npi_scope = temporary.path().join("npi-scope.copy");
    write_provider_membership_sidecars(
        &group_npi,
        &npi_group,
        &npi_scope,
        &[
            temporary.path().join("missing.copy").display().to_string(),
            input_a.display().to_string(),
            input_b.display().to_string(),
        ],
    )
    .unwrap();

    assert_eq!(&std::fs::read(&group_npi).unwrap()[..8], b"PTG2MNDS");
    assert_eq!(&std::fs::read(&npi_group).unwrap()[..8], b"PTG2MNDS");
    let scope = std::fs::read(&npi_scope).unwrap();
    let copy_header = [b"PGCOPY\n\xff\r\n\0".as_slice(), &[0u8; 8]].concat();
    assert_eq!(&scope[..copy_header.len()], copy_header);
    assert_eq!(scope.len(), copy_header.len() + 2 * 14 + 2);
    assert_eq!(
        i64::from_be_bytes(scope[25..33].try_into().unwrap()),
        1_234_567_890,
    );
    assert_eq!(
        i64::from_be_bytes(scope[39..47].try_into().unwrap()),
        2_222_222_222,
    );
    assert_eq!(&scope[47..], &(-1i16).to_be_bytes());

    let tampered_npi_group = temporary.path().join("tampered-npi-group.sidecar");
    let tampered_scope = temporary.path().join("tampered-scope.copy");
    let mut tampered = std::fs::read(&npi_group).unwrap();
    tampered[20..28].copy_from_slice(&99u64.to_le_bytes());
    std::fs::write(&tampered_npi_group, tampered).unwrap();
    assert!(write_provider_npi_scope_from_dense_sidecar(
        &tampered_npi_group,
        &tampered_scope,
        2,
        2,
        2,
    )
    .is_err());
    assert!(!tampered_scope.exists());

    let existing_scope = temporary.path().join("existing-scope.copy");
    std::fs::write(&existing_scope, b"caller-owned").unwrap();
    assert!(
        write_provider_npi_scope_from_dense_sidecar(&npi_group, &existing_scope, 2, 2, 2,).is_err()
    );
    assert_eq!(std::fs::read(&existing_scope).unwrap(), b"caller-owned");

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;

        let target = temporary.path().join("scope-link-target.copy");
        let link = temporary.path().join("scope-link.copy");
        symlink(&target, &link).unwrap();
        assert!(write_provider_npi_scope_from_dense_sidecar(&npi_group, &link, 2, 2, 2,).is_err());
        assert!(std::fs::symlink_metadata(&link)
            .unwrap()
            .file_type()
            .is_symlink());
        assert!(!target.exists());
    }

    assert!(global_id_from_hex_bytes(b"short").is_err());
    assert!(global_id_from_hex_bytes(&[b'g'; GLOBAL_ID_BYTES * 2]).is_err());
    let mut invalid_low = [b'0'; GLOBAL_ID_BYTES * 2];
    invalid_low[1] = b'g';
    assert!(global_id_from_hex_bytes(&invalid_low).is_err());

    for (name, row) in [
        ("missing-group", b"\t1234567890\n".as_slice()),
        (
            "invalid-group",
            b"0000000000000000000000000000000g\t1234567890\n".as_slice(),
        ),
        (
            "invalid-npi",
            b"05050505050505050505050505050505\tnot-an-npi\n".as_slice(),
        ),
        (
            "short-npi",
            b"05050505050505050505050505050505\t123456789\n".as_slice(),
        ),
        (
            "plus-npi",
            b"05050505050505050505050505050505\t+1234567890\n".as_slice(),
        ),
        (
            "leading-zero-eleven",
            b"05050505050505050505050505050505\t01234567890\n".as_slice(),
        ),
        (
            "plus-zero",
            b"05050505050505050505050505050505\t+0\n".as_slice(),
        ),
        (
            "minus-zero",
            b"05050505050505050505050505050505\t-0\n".as_slice(),
        ),
        (
            "multiple-zero",
            b"05050505050505050505050505050505\t00\n".as_slice(),
        ),
        (
            "ten-zero",
            b"05050505050505050505050505050505\t0000000000\n".as_slice(),
        ),
        (
            "leading-zero-ten",
            b"05050505050505050505050505050505\t0123456789\n".as_slice(),
        ),
        (
            "invalid-utf8",
            b"05050505050505050505050505050505\t\xff\n".as_slice(),
        ),
    ] {
        let input = temporary.path().join(format!("{name}.copy"));
        std::fs::write(&input, row).unwrap();
        assert!(write_provider_membership_sidecars(
            &temporary.path().join(format!("{name}-group.sidecar")),
            &temporary.path().join(format!("{name}-npi.sidecar")),
            &temporary.path().join(format!("{name}-scope.copy")),
            &[input.display().to_string()],
        )
        .is_err());
    }
}
#[test]
fn provider_membership_npi_parser_accepts_only_exact_wire_values() {
    assert_eq!(parse_provider_membership_npi(b"0").unwrap(), None);
    assert_eq!(
        parse_provider_membership_npi(b"1234567890").unwrap(),
        Some(1_234_567_890),
    );
    assert_eq!(
        parse_provider_membership_npi(b"9999999999").unwrap(),
        Some(9_999_999_999),
    );
    for value in [
        b"".as_slice(),
        b"+0",
        b"-0",
        b"00",
        b"0000000000",
        b"0123456789",
        b"+1234567890",
        b"01234567890",
        b"123456789",
        b"123456789a",
        b"\xff234567890",
    ] {
        assert!(parse_provider_membership_npi(value).is_err(), "{value:?}",);
    }
}
#[test]
fn configured_manifest_sidecar_emission_covers_memory_and_spooled_outputs() {
    let base = std::env::temp_dir().join(format!(
        "ptg2-sidecar-emission-test-{}-{:?}",
        std::process::id(),
        thread::current().id()
    ));
    std::fs::create_dir_all(&base).unwrap();
    let paths_for = |directory: &Path| CopyPathConfig {
        manifest_provider_forward_sidecar: Some(
            directory
                .join("provider-forward.ptg2sc")
                .display()
                .to_string(),
        ),
        manifest_provider_inverted_sidecar: Some(
            directory
                .join("provider-inverted.ptg2sc")
                .display()
                .to_string(),
        ),
        manifest_provider_set_component_sidecar: Some(
            directory.join("set-component.ptg2sc").display().to_string(),
        ),
        manifest_provider_component_group_sidecar: Some(
            directory
                .join("component-group.ptg2sc")
                .display()
                .to_string(),
        ),
        manifest_provider_npi_sidecar: Some(
            directory.join("provider-npi.ptg2sc").display().to_string(),
        ),
        manifest_price_forward_sidecar: Some(
            directory.join("price-forward.ptg2sc").display().to_string(),
        ),
        ..CopyPathConfig::default()
    };
    let provider_set = GlobalId128([1; GLOBAL_ID_BYTES]);
    let price_set = PriceSetLite {
        global_id: GlobalId128([8; GLOBAL_ID_BYTES]),
        atoms: Vec::new(),
        atom_ids: vec![GlobalId128([9; GLOBAL_ID_BYTES])],
    };

    let memory_directory = base.join("memory");
    let memory_paths = paths_for(&memory_directory);
    let mut in_memory = ManifestSidecarCollector::default();
    in_memory
        .record_provider_set(provider_set, &[2, 1], &[4, 3], &[1_234_567_890])
        .unwrap();
    in_memory.record_provider_component(3, &[2, 1]).unwrap();
    in_memory.record_price_set(&price_set).unwrap();
    let mut memory_events = Vec::new();
    emit_configured_manifest_sidecars(
        &mut memory_events,
        &memory_paths,
        Some(&mut in_memory),
        None,
    )
    .unwrap();
    assert_eq!(
        String::from_utf8(memory_events).unwrap().lines().count(),
        12
    );

    let spool_directory = base.join("spooled");
    let spool_paths = paths_for(&spool_directory);
    let mut spooled = ManifestSidecarCollector {
        spools: Some(ManifestSidecarSpools::for_paths(&spool_paths).unwrap()),
        ..ManifestSidecarCollector::default()
    };
    spooled
        .record_provider_set(provider_set, &[2, 1], &[4, 3], &[1_234_567_890])
        .unwrap();
    spooled.record_provider_component(3, &[2, 1]).unwrap();
    spooled.record_price_set(&price_set).unwrap();
    let mut spool_events = Vec::new();
    emit_configured_manifest_sidecars(&mut spool_events, &spool_paths, Some(&mut spooled), None)
        .unwrap();
    assert_eq!(String::from_utf8(spool_events).unwrap().lines().count(), 12);

    let mut no_events = Vec::new();
    emit_configured_manifest_sidecars(&mut no_events, &CopyPathConfig::default(), None, None)
        .unwrap();
    assert!(no_events.is_empty());
    std::fs::remove_dir_all(base).unwrap();
}
#[test]
fn v4_identity_and_projection_helpers_preserve_exact_boundaries() {
    let first_definition = json!({
        "provider_group_id": 7,
        "network_name": [" network-b ", "network-a", "network-a"],
        "provider_groups": [{
            "tin": {"type": "ein", "value": "12-3456789"},
            "npi": [1234567890, 1234567891]
        }]
    });
    let second_definition = json!({
        "provider_group_id": 8,
        "network_name": ["network-c"],
        "provider_groups": [{
            "tin": {"type": "ein", "value": "98-7654321"},
            "npi": [1234567891, 1234567892]
        }]
    });
    let (first_key, first_entry) = provider_ref_definition(&first_definition).unwrap();
    let (second_key, second_entry) = provider_ref_definition(&second_definition).unwrap();
    assert_eq!(ProviderRefKey::from("7"), first_key);
    assert_eq!(ProviderRefKey::from("8".to_string()), second_key);

    let rebuilt = build_provider_entry(&first_definition).unwrap();
    assert_eq!(rebuilt, first_entry);
    let mut provider_map = HashMap::new();
    insert_provider_definition(&mut provider_map, first_key.clone(), first_entry.clone()).unwrap();
    insert_provider_definition(&mut provider_map, second_key.clone(), second_entry.clone())
        .unwrap();
    insert_provider_definition(&mut provider_map, first_key.clone(), first_entry.clone()).unwrap();
    validate_preloaded_provider_definition(&provider_map, &first_key, &first_entry).unwrap();
    assert!(
        validate_preloaded_provider_definition(&provider_map, &first_key, &second_entry).is_err()
    );
    assert!(validate_preloaded_provider_definition(
        &provider_map,
        &ProviderRefKey::from("missing"),
        &first_entry,
    )
    .is_err());

    let borrowed =
        provider_entry_view_from_ref_keys(&provider_map, std::slice::from_ref(&first_key))
            .unwrap()
            .unwrap();
    assert_eq!(borrowed.entry_hash(), first_entry.entry_hash);
    assert_eq!(borrowed.provider_count(), 2);
    assert_eq!(
        borrowed.provider_group_hashes(),
        first_entry.provider_group_hashes
    );
    assert_eq!(borrowed.npi(), first_entry.npi);
    assert_eq!(borrowed.network_names(), first_entry.network_names);

    let owned =
        provider_entry_view_from_ref_keys(&provider_map, &[first_key.clone(), second_key.clone()])
            .unwrap()
            .unwrap();
    assert_eq!(owned.provider_count(), 3);
    assert_eq!(owned.npi(), &[1234567890, 1234567891, 1234567892]);
    assert_eq!(
        provider_set_from_ref_keys(&provider_map, &[]).unwrap(),
        None
    );
    assert!(provider_set_from_ref_keys(&provider_map, &[ProviderRefKey::from("unknown")]).is_err());

    let combined = combine_provider_entries(first_entry.clone(), second_entry.clone());
    assert_eq!(combined.provider_count, 3);
    assert_eq!(combined.npi, vec![1234567890, 1234567891, 1234567892]);
    let first_group_payload = provider_group_payload_canonical_json(
        first_entry.provider_group_hashes[0],
        "ein",
        "123456789",
        &first_entry.npi,
        &[9, 8, 9],
        &[],
    );
    let second_group_payload = provider_group_payload_canonical_json(
        second_entry.provider_group_hashes[0],
        "ein",
        "987654321",
        &second_entry.npi,
        &[],
        &[],
    );
    assert_eq!(
        provider_set_checksum_from_group_payloads(vec![
            first_group_payload.clone(),
            second_group_payload.clone(),
        ]),
        provider_set_checksum_from_group_payloads(vec![second_group_payload, first_group_payload,])
    );
    assert_eq!(
        provider_set_scope_hash(&[1, 2], &["a".to_string(), "b".to_string()]),
        provider_set_scope_hash(&[1, 2], &["a".to_string(), "b".to_string()])
    );
    assert_ne!(npi_member_id(1234567890), npi_member_id(1234567891));

    let prices = vec![
        PriceLite {
            negotiated_type: Some("negotiated".to_string()),
            negotiated_rate: "12.50".to_string(),
            expiration_date: Some("2027-01-01".to_string()),
            service_code: vec!["11".to_string()],
            billing_class: Some("professional".to_string()),
            setting: Some("outpatient".to_string()),
            billing_code_modifier: vec!["26".to_string()],
            additional_information: Some("contract".to_string()),
        },
        PriceLite {
            negotiated_type: Some("fee schedule".to_string()),
            negotiated_rate: "9.75".to_string(),
            expiration_date: None,
            service_code: vec!["12".to_string()],
            billing_class: None,
            setting: None,
            billing_code_modifier: Vec::new(),
            additional_information: None,
        },
    ];
    let first_atom = price_atom_from_lite(&prices[0], 0);
    assert_eq!(first_atom.global_id, price_atom_global_id(&first_atom));
    assert_eq!(
        price_code_set_hash(&first_atom.service_code),
        price_code_set_hash(&["11".to_string()])
    );
    let price_set = price_lite_set(&prices).unwrap();
    assert_eq!(price_set.minimum_negotiated_rate(), "9.75");
    assert_eq!(price_set.global_id, price_set_global_id(&price_set));
    let rate = RateLite {
        provider_refs: vec![first_key],
        provider_groups: Vec::new(),
        provider_groups_raw: None,
        network_names: Vec::new(),
        prices: Vec::new(),
        prepared_price_set: Some(price_set.clone()),
    };
    assert_eq!(rate, rate.clone());
    assert_eq!(rate.price_count(), 2);
    assert!(!rate.has_inline_provider_groups());
    assert_eq!(rate_price_set(&rate).unwrap().into_owned(), price_set);
    let grouped: GroupedPriceSet = (
        price_set,
        HashSet::from([first_entry.entry_hash]),
        HashSet::from([first_entry.provider_group_hashes[0]]),
        HashSet::from([1234567890]),
        1,
        HashSet::from(["network-a".to_string()]),
        BTreeMap::from([(
            first_entry.entry_hash,
            first_entry.provider_group_hashes.clone(),
        )]),
    )
        .into();
    assert_eq!(grouped.provider_count, 1);

    let default_cache = ProviderSetScopeCache::default();
    assert!(!default_cache.v4_factor_mode());
    assert!(ProviderSetScopeCache::with_v4_factor_mode(true).v4_factor_mode());
}
#[test]
fn manifest_merge_ordering_and_empty_merge_cleanup_are_exact() {
    let pair_one = [1u8; MANIFEST_PAIR_RECORD_BYTES];
    let pair_two = [2u8; MANIFEST_PAIR_RECORD_BYTES];
    let first_pair = ManifestPairMergeItem {
        pair: pair_one,
        reader_index: 0,
    };
    let same_pair = ManifestPairMergeItem {
        pair: pair_one,
        reader_index: 0,
    };
    let later_pair = ManifestPairMergeItem {
        pair: pair_two,
        reader_index: 1,
    };
    assert!(first_pair == same_pair);
    assert!(first_pair != later_pair);
    assert_eq!(
        first_pair.partial_cmp(&later_pair),
        Some(first_pair.cmp(&later_pair))
    );

    let first_line = ManifestMergeItem {
        key: b"a".to_vec(),
        line: b"first\n".to_vec(),
        reader_index: 0,
    };
    let same_line = ManifestMergeItem {
        key: b"a".to_vec(),
        line: b"first\n".to_vec(),
        reader_index: 0,
    };
    let later_line = ManifestMergeItem {
        key: b"b".to_vec(),
        line: b"second\n".to_vec(),
        reader_index: 1,
    };
    assert!(first_line == same_line);
    assert!(first_line != later_line);
    assert_eq!(
        first_line.partial_cmp(&later_line),
        Some(first_line.cmp(&later_line))
    );

    let temporary = tempfile::tempdir().unwrap();
    let source = temporary.path().join("pairs.bin");
    let mut tracked_files = ManifestPairTemporaryFiles::default();
    let mut progress = ManifestFinalizeProgress::new(None);
    let merged = merge_sorted_pair_chunks(&source, &[], &mut tracked_files, &mut progress).unwrap();
    assert_eq!(merged.entry_count, 0);
    assert_eq!(merged.member_count, 0);
    assert!(merged.member_ids.is_empty());
    assert!(merged.path.is_file());
    let merged_path = merged.path.clone();
    drop(tracked_files);
    assert!(!merged_path.exists());

    assert_eq!(provider_code_merge_pass_count(0), 0);
    assert_eq!(provider_code_merge_pass_count(1), 1);
    assert!(
        provider_code_merge_pass_count(SERVING_BINARY_V3_PROVIDER_CODE_MERGE_FAN_IN as u64) > 1
    );
}
#[test]
fn scanner_failure_and_binary_value_helpers_fail_closed() {
    let diagnostic =
        primary_producer_failure_diagnostic(&io::Error::other("upstream contract failed")).unwrap();
    assert!(diagnostic.contains("producer_error"));
    for kind in [io::ErrorKind::Interrupted, io::ErrorKind::BrokenPipe] {
        assert!(primary_producer_failure_diagnostic(&io::Error::new(kind, "peer")).is_none());
    }
    assert_eq!(panic_payload_message(&"borrowed panic"), "borrowed panic");
    assert_eq!(
        panic_payload_message(&"owned panic".to_string()),
        "owned panic"
    );
    assert_eq!(panic_payload_message(&17usize), "non-string panic payload");
    log_worker_failure(7, "contract", "expected test diagnostic");

    assert_eq!(decimal_text_field(b"-12.50"), Some("-12.50"));
    assert_eq!(decimal_text_field(b"+7"), Some("+7"));
    for invalid in [b"".as_slice(), b"+", b"1.2.3", b"NaN", b"\xff"] {
        assert_eq!(decimal_text_field(invalid), None);
    }
    assert_eq!(pg_binary_u64(&7i32.to_be_bytes(), "value").unwrap(), 7);
    assert_eq!(pg_binary_u64(&9i64.to_be_bytes(), "value").unwrap(), 9);
    assert!(pg_binary_u64(&(-1i32).to_be_bytes(), "value").is_err());
    assert!(pg_binary_u64(&(-1i64).to_be_bytes(), "value").is_err());
    assert!(pg_binary_u64(&[0; 3], "value").is_err());
    assert_eq!(
        pg_binary_nonnegative_i64(&7i32.to_be_bytes(), "value").unwrap(),
        7
    );
    assert!(pg_binary_nonnegative_i64(&u64::MAX.to_be_bytes(), "value").is_err());
    assert!(pg_binary_nonnegative_i32(&(i64::from(i32::MAX) + 1).to_be_bytes(), "value",).is_err());
    assert_eq!(
        pg_binary_optional_nonnegative_i64(None, "value").unwrap(),
        None
    );
    assert_eq!(
        pg_binary_optional_nonnegative_i64(Some(&7i32.to_be_bytes()), "value").unwrap(),
        Some(7)
    );
    assert_eq!(pg_binary_negotiated_rate(b"12.50").unwrap(), "12.50");
    assert!(pg_binary_numeric_text(&[0; 7]).is_err());
}
#[test]
fn pg_binary_copy_header_accepts_extensions_and_rejects_invalid_boundaries() {
    let mut with_extension = b"PGCOPY\n\xff\r\n\0".to_vec();
    with_extension.extend_from_slice(&0i32.to_be_bytes());
    with_extension.extend_from_slice(&2i32.to_be_bytes());
    with_extension.extend_from_slice(b"ok");
    read_pg_binary_copy_header(&mut Cursor::new(with_extension)).unwrap();

    let invalid_signature = read_pg_binary_copy_header(&mut Cursor::new(b"not-pg-copy\n"));
    assert!(invalid_signature
        .unwrap_err()
        .to_string()
        .contains("invalid header"));

    let mut negative_extension = b"PGCOPY\n\xff\r\n\0".to_vec();
    negative_extension.extend_from_slice(&0i32.to_be_bytes());
    negative_extension.extend_from_slice(&(-1i32).to_be_bytes());
    assert!(
        read_pg_binary_copy_header(&mut Cursor::new(negative_extension))
            .unwrap_err()
            .to_string()
            .contains("negative extension length")
    );

    let mut truncated_extension = b"PGCOPY\n\xff\r\n\0".to_vec();
    truncated_extension.extend_from_slice(&0i32.to_be_bytes());
    truncated_extension.extend_from_slice(&2i32.to_be_bytes());
    truncated_extension.push(b'o');
    assert_eq!(
        read_pg_binary_copy_header(&mut Cursor::new(truncated_extension))
            .unwrap_err()
            .kind(),
        io::ErrorKind::UnexpectedEof
    );
}
#[test]
fn pg_binary_copy_rows_reject_truncated_and_invalid_boundaries() {
    let partial_field = read_exact_optional(&mut Cursor::new(vec![b'x']), &mut [0_u8; 2]);
    assert_eq!(
        partial_field.unwrap_err().kind(),
        io::ErrorKind::UnexpectedEof
    );

    let missing_trailer = read_pg_binary_copy_row(&mut Cursor::new(Vec::<u8>::new()), 1, "test");
    assert_eq!(
        missing_trailer.unwrap_err().kind(),
        io::ErrorKind::UnexpectedEof
    );

    let wrong_field_count =
        read_pg_binary_copy_row(&mut Cursor::new(2i16.to_be_bytes().to_vec()), 1, "test");
    assert!(wrong_field_count
        .unwrap_err()
        .to_string()
        .contains("must have 1 fields, got 2"));

    let mut invalid_length = 1i16.to_be_bytes().to_vec();
    invalid_length.extend_from_slice(&(-2i32).to_be_bytes());
    assert!(
        read_pg_binary_copy_row(&mut Cursor::new(invalid_length), 1, "test")
            .unwrap_err()
            .to_string()
            .contains("invalid length")
    );

    assert_eq!(
        read_pg_binary_copy_row(&mut Cursor::new((-1i16).to_be_bytes().to_vec()), 1, "test")
            .unwrap(),
        None
    );
}
#[test]
fn worker_copy_reader_orders_shards_and_refuses_absent_output() {
    let temporary = tempfile::tempdir().unwrap();
    let copy_path = temporary.path().join("provider-members.copy");
    std::fs::write(
        temporary.path().join("provider-members.copy.0001"),
        b"second\n",
    )
    .unwrap();
    std::fs::write(
        temporary.path().join("provider-members.copy.0000"),
        b"first\n",
    )
    .unwrap();

    assert_eq!(
        read_worker_copy_text(&copy_path).unwrap(),
        "first\nsecond\n"
    );
    let absent = temporary.path().join("absent.copy");
    let error = read_worker_copy_text(&absent).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::NotFound);
}
#[test]
fn benchmark_fixture_helpers_build_small_partition_complete_manifest() {
    let temporary = tempfile::tempdir().unwrap();
    let coverage_scope_id = [0x75; COVERAGE_SCOPE_ID_BYTES];
    let codes = v3_finalizer_benchmark_codes_by_partition(&coverage_scope_id, 2);
    assert_eq!(codes.len(), 2);
    assert_ne!(codes[0], codes[1]);

    let manifest_path =
        write_v3_finalizer_benchmark_manifest(temporary.path(), "small", 8, 2, 3, 2);
    let manifest: Value = serde_json::from_slice(&std::fs::read(manifest_path).unwrap()).unwrap();
    assert_eq!(manifest["source_count"], 1);
    assert_eq!(manifest["expected_serving_run_rows"], 8);
    assert_eq!(
        manifest["serving_run_partition_files"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
}
#[test]
fn witness_failure_code_distinguishes_payload_limits() {
    for message in [
        "source witness payload budget exceeded",
        "source witness intermediate budget exceeded",
        "source witness fail-closed limit",
        "source witness spool byte limit exceeded",
    ] {
        assert_eq!(
            scanner_failure_code(&io::Error::new(io::ErrorKind::InvalidData, message)),
            "witness_payload_limit",
        );
    }
    assert_eq!(
        scanner_failure_code(&io::Error::other("source witness parse failed")),
        "scanner_failure",
    );
    assert_eq!(
        scanner_failure_code(&io::Error::other("unrelated")),
        "scanner_failure",
    );
}
#[test]
fn shared_graph_manifest_admission_rejects_every_incomplete_shape() {
    assert!(shared_graph_required_object(&Value::Null, "value").is_err());

    let mut object = Map::new();
    assert!(shared_graph_required_string(&object, "text", "value").is_err());
    object.insert("text".to_owned(), json!(1));
    assert!(shared_graph_required_string(&object, "text", "value").is_err());
    object.insert("text".to_owned(), json!(""));
    assert!(shared_graph_required_string(&object, "text", "value").is_err());
    object.insert("text".to_owned(), json!("ok"));
    assert_eq!(
        shared_graph_required_string(&object, "text", "value").unwrap(),
        "ok"
    );

    assert!(shared_graph_required_u64(&object, "count", "value").is_err());
    object.insert("count".to_owned(), json!(-1));
    assert!(shared_graph_required_u64(&object, "count", "value").is_err());
    object.insert("count".to_owned(), json!(4));
    assert_eq!(
        shared_graph_required_u64(&object, "count", "value").unwrap(),
        4
    );

    assert_eq!(
        shared_graph_optional_u64(&object, "optional_count", "value").unwrap(),
        None
    );
    object.insert("optional_count".to_owned(), Value::Null);
    assert_eq!(
        shared_graph_optional_u64(&object, "optional_count", "value").unwrap(),
        None
    );
    object.insert("optional_count".to_owned(), json!("four"));
    assert!(shared_graph_optional_u64(&object, "optional_count", "value").is_err());
    object.insert("optional_count".to_owned(), json!(4));
    assert_eq!(
        shared_graph_optional_u64(&object, "optional_count", "value").unwrap(),
        Some(4)
    );

    assert_eq!(
        shared_graph_optional_string(&object, "optional_text", "value").unwrap(),
        None
    );
    object.insert("optional_text".to_owned(), Value::Null);
    assert_eq!(
        shared_graph_optional_string(&object, "optional_text", "value").unwrap(),
        None
    );
    object.insert("optional_text".to_owned(), json!(4));
    assert!(shared_graph_optional_string(&object, "optional_text", "value").is_err());
    object.insert("optional_text".to_owned(), json!("text"));
    assert_eq!(
        shared_graph_optional_string(&object, "optional_text", "value").unwrap(),
        Some("text".to_owned())
    );

    assert!(shared_graph_artifact_from_json(&Value::Null, "artifact").is_err());
    assert!(shared_graph_artifact_from_json(&json!({}), "artifact").is_err());
    assert!(shared_graph_artifact_from_json(&json!({"metadata": null}), "artifact").is_err());
    let artifact = json!({
        "path": "artifact.bin",
        "metadata": {
            "record_format": "test",
            "sha256": "0",
            "byte_count": 1,
            "owner_count": 1,
            "member_count": 1,
            "member_global_count": null,
            "name": "test",
            "source_shard_id": null,
            "shard_id": "shard"
        }
    });
    assert!(shared_graph_artifact_from_json(&artifact, "artifact").is_ok());
    assert!(shared_graph_shard_from_json(&Value::Null, 0).is_err());
    assert!(shared_graph_shard_from_json(&json!({}), 0).is_err());
    let shard = json!({
        "shard_id": "shard",
        "group_npi": artifact,
        "npi_group": artifact,
        "group_provider_set": artifact,
        "provider_set_group": artifact
    });
    assert!(shared_graph_shard_from_json(&shard, 0).is_ok());

    let invalid_utf8 = PathBuf::from(
        <std::ffi::OsString as std::os::unix::ffi::OsStringExt>::from_vec(vec![0xff]),
    );
    assert!(shared_graph_summary_path(&invalid_utf8, "test").is_err());

    let directory = tempfile::tempdir().unwrap();
    assert!(run_shared_graph_converter(&[]).is_err());
    assert!(run_shared_graph_converter(&[directory
        .path()
        .join("missing")
        .to_string_lossy()
        .into_owned()])
    .is_err());
    for (name, manifest) in [
        ("invalid", b"not-json".as_slice()),
        ("null", b"null".as_slice()),
        ("missing-key-map", br#"{}"#.as_slice()),
        (
            "missing-output",
            br#"{"provider_set_key_map_path":"map"}"#.as_slice(),
        ),
        (
            "missing-shards",
            br#"{"provider_set_key_map_path":"map","output_directory":"out"}"#.as_slice(),
        ),
        (
            "empty-shards",
            br#"{"provider_set_key_map_path":"map","output_directory":"out","shards":[]}"#
                .as_slice(),
        ),
    ] {
        let path = directory.path().join(format!("{name}.json"));
        std::fs::write(&path, manifest).unwrap();
        assert!(
            run_shared_graph_converter(&[path.to_string_lossy().into_owned()]).is_err(),
            "accepted invalid shared graph manifest {name}",
        );
    }
}
#[test]
fn copy_path_presence_and_v4_pairing_are_independently_admitted() {
    let setters: &[fn(&mut CopyPathConfig)] = &[
        |config| config.compact = Some("path".to_owned()),
        |config| config.manifest_serving = Some("path".to_owned()),
        |config| config.manifest_lean_serving = Some("path".to_owned()),
        |config| config.v3_serving_run_directory = Some("path".to_owned()),
        |config| config.manifest_provider_forward_sidecar = Some("path".to_owned()),
        |config| config.manifest_provider_inverted_sidecar = Some("path".to_owned()),
        |config| config.manifest_provider_set_component_sidecar = Some("path".to_owned()),
        |config| config.manifest_provider_component_group_sidecar = Some("path".to_owned()),
        |config| config.manifest_provider_group_tax_identity_sidecar = Some("path".to_owned()),
        |config| config.manifest_provider_group_tax_identity_v2_sidecar = Some("path".to_owned()),
        |config| config.manifest_provider_npi_sidecar = Some("path".to_owned()),
        |config| config.manifest_price_forward_sidecar = Some("path".to_owned()),
        |config| config.manifest_price_atom = Some("path".to_owned()),
        |config| config.manifest_price_set_atom = Some("path".to_owned()),
        |config| config.manifest_price_set_summary = Some("path".to_owned()),
        |config| config.manifest_provider_group_member = Some("path".to_owned()),
        |config| config.manifest_code_count = Some("path".to_owned()),
        |config| config.manifest_provider_set_dictionary = Some("path".to_owned()),
        |config| config.procedure = Some("path".to_owned()),
        |config| config.price_code_set = Some("path".to_owned()),
        |config| config.price_atom = Some("path".to_owned()),
        |config| config.price_set_entry = Some("path".to_owned()),
        |config| config.provider_set = Some("path".to_owned()),
        |config| config.provider_set_component = Some("path".to_owned()),
        |config| config.provider_set_entry = Some("path".to_owned()),
        |config| config.provider_entry_component = Some("path".to_owned()),
        |config| config.provider_group_member = Some("path".to_owned()),
    ];
    assert!(!CopyPathConfig::default().has_file_paths());
    for set_path in setters {
        let mut config = CopyPathConfig::default();
        set_path(&mut config);
        assert!(config.has_file_paths());
    }

    let _lock = scanner_env_lock().lock().unwrap();
    let _disabled = TestEnvVar::set(PROVIDER_GRAPH_V4_ENV, "false");
    let mismatched_pair = CopyPathConfig {
        manifest_provider_set_component_sidecar: Some("set".to_owned()),
        ..CopyPathConfig::default()
    };
    assert!(configured_v4_factor_mode(&mismatched_pair).is_err());

    let disabled_tax = CopyPathConfig {
        manifest_provider_group_tax_identity_sidecar: Some("tax".to_owned()),
        ..CopyPathConfig::default()
    };
    assert!(configured_v4_factor_mode(&disabled_tax).is_err());

    let _enabled = TestEnvVar::set(PROVIDER_GRAPH_V4_ENV, "true");
    let missing_tax = CopyPathConfig {
        manifest_provider_set_component_sidecar: Some("set".to_owned()),
        manifest_provider_component_group_sidecar: Some("component".to_owned()),
        ..CopyPathConfig::default()
    };
    assert!(configured_v4_factor_mode(&missing_tax).is_err());
}
#[test]
fn v2_tax_identity_paths_require_v4_v1_and_distinct_final_and_temporary_paths() {
    let _lock = scanner_env_lock().lock().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path_text = |name: &str| directory.path().join(name).display().to_string();
    let paths = |v1: Option<String>, v2: Option<String>| CopyPathConfig {
        manifest_provider_set_component_sidecar: Some(path_text("set")),
        manifest_provider_component_group_sidecar: Some(path_text("component")),
        manifest_provider_group_tax_identity_sidecar: v1,
        manifest_provider_group_tax_identity_v2_sidecar: v2,
        ..CopyPathConfig::default()
    };

    let _disabled = TestEnvVar::set(PROVIDER_GRAPH_V4_ENV, "false");
    assert!(
        configured_v4_factor_mode(&paths(Some(path_text("v1")), Some(path_text("v2")),)).is_err()
    );

    let _enabled = TestEnvVar::set(PROVIDER_GRAPH_V4_ENV, "true");
    assert!(configured_v4_factor_mode(&paths(None, Some(path_text("v2")))).is_err());
    assert!(
        configured_v4_factor_mode(&paths(Some(path_text("v1")), Some(path_text("v2")),)).unwrap()
    );

    for (v1, v2) in [
        (path_text("same"), path_text("same")),
        (path_text("cross.building"), path_text("cross")),
        (path_text("reverse"), path_text("reverse.building")),
        (
            path_text("unused/../lexical-alias"),
            path_text("lexical-alias"),
        ),
        (
            "/tmp/ptg2-root-aware-alias".to_string(),
            "/../tmp/ptg2-root-aware-alias".to_string(),
        ),
    ] {
        let error = configured_v4_factor_mode(&paths(Some(v1), Some(v2)))
            .err()
            .unwrap();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("must differ"));
    }

    let final_alias = paths(Some(path_text("v1")), Some(path_text("set")));
    assert!(configured_v4_factor_mode(&final_alias).is_err());
    let v1_only_final_alias = paths(Some(path_text("set")), None);
    assert!(configured_v4_factor_mode(&v1_only_final_alias).is_err());
    let mut temporary_alias = paths(Some(path_text("v1")), Some(path_text("v2-temporary-alias")));
    temporary_alias.manifest_provider_component_group_sidecar =
        Some(path_text("v2-temporary-alias.building"));
    assert!(configured_v4_factor_mode(&temporary_alias).is_err());
}
#[test]
fn tax_identity_runtime_paths_reject_inputs_workers_rotations_and_reserved_directories() {
    let directory = tempfile::tempdir().unwrap();
    let source_path = directory.path().join("source.json");
    let secret_path = directory.path().join("token-secret.bin");
    let price_base = directory.path().join("price-atom.copy");
    let provider_group_base = directory.path().join("provider-group-member.copy");
    let factor_sidecar = directory.path().join("provider-set-component.sidecar");
    let v1_path = directory.path().join("tax-v1.ptg2tax");
    let serving_directory = directory.path().join("serving");
    let witness_scratch = directory.path().join("witness-scratch");
    std::fs::write(&source_path, b"source-sentinel").unwrap();
    std::fs::write(&secret_path, b"secret-sentinel").unwrap();
    std::fs::create_dir(&serving_directory).unwrap();
    std::fs::create_dir(&witness_scratch).unwrap();

    let base_paths = CopyPathConfig {
        v3_serving_run_directory: Some(serving_directory.display().to_string()),
        manifest_provider_group_tax_identity_sidecar: Some(v1_path.display().to_string()),
        manifest_provider_set_component_sidecar: Some(factor_sidecar.display().to_string()),
        manifest_price_atom: Some(price_base.display().to_string()),
        manifest_provider_group_member: Some(provider_group_base.display().to_string()),
        ..CopyPathConfig::default()
    };
    let collision_cases = [
        (source_path.clone(), "raw_input"),
        (secret_path.clone(), "token_secret"),
        (
            PathBuf::from(format!("{}.worker0000", price_base.display())),
            "manifest_price_atom.worker0000",
        ),
        (
            PathBuf::from(format!(
                "{}.worker0000.part000000.ready",
                price_base.display()
            )),
            "manifest_price_atom.worker0000",
        ),
        (
            PathBuf::from(format!(
                "{}.provider_refs.worker0000",
                provider_group_base.display()
            )),
            "manifest_provider_group_member.provider_refs.worker0000",
        ),
        (
            factor_sidecar.clone(),
            "manifest_provider_set_component_sidecar.final",
        ),
        (
            serving_directory.join("tax-v2.ptg2tax"),
            "serving_run_directory",
        ),
        (
            witness_scratch.join("tax-v2.ptg2tax"),
            "source_witness_scratch",
        ),
    ];
    for (candidate, expected_conflict) in collision_cases {
        let mut paths = base_paths.clone();
        paths.manifest_provider_group_tax_identity_v2_sidecar =
            Some(candidate.display().to_string());
        let error = validate_runtime_tax_identity_output_paths(
            &paths,
            &source_path,
            &secret_path,
            Some(&witness_scratch),
            2,
            2,
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains(expected_conflict));
        assert!(!error
            .to_string()
            .contains(directory.path().to_str().unwrap()));
        assert_eq!(std::fs::read(&source_path).unwrap(), b"source-sentinel");
        assert_eq!(std::fs::read(&secret_path).unwrap(), b"secret-sentinel");
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;

        let source_hardlink = directory.path().join("source-hardlink");
        std::fs::hard_link(&source_path, &source_hardlink).unwrap();
        let mut hardlink_paths = base_paths.clone();
        hardlink_paths.manifest_provider_group_tax_identity_v2_sidecar =
            Some(source_hardlink.display().to_string());
        assert!(validate_runtime_tax_identity_output_paths(
            &hardlink_paths,
            &source_path,
            &secret_path,
            Some(&witness_scratch),
            1,
            1,
        )
        .is_err());

        let actual_parent = directory.path().join("canonical-parent");
        let alias_parent = directory.path().join("canonical-parent-alias");
        std::fs::create_dir(&actual_parent).unwrap();
        symlink(&actual_parent, &alias_parent).unwrap();
        let canonical_source = actual_parent.join("source.json");
        std::fs::write(&canonical_source, b"canonical-source").unwrap();
        let mut symlink_parent_paths = base_paths.clone();
        symlink_parent_paths.manifest_provider_group_tax_identity_v2_sidecar =
            Some(alias_parent.join("source.json").display().to_string());
        assert!(validate_runtime_tax_identity_output_paths(
            &symlink_parent_paths,
            &canonical_source,
            &secret_path,
            Some(&witness_scratch),
            1,
            1,
        )
        .is_err());
        assert_eq!(
            std::fs::read(&canonical_source).unwrap(),
            b"canonical-source"
        );

        let manifest_target = actual_parent.join("manifest.sidecar");
        let mut manifest_alias_paths = base_paths.clone();
        manifest_alias_paths.manifest_provider_set_component_sidecar =
            Some(manifest_target.display().to_string());
        manifest_alias_paths.manifest_provider_group_tax_identity_v2_sidecar =
            Some(alias_parent.join("manifest.sidecar").display().to_string());
        assert!(validate_runtime_tax_identity_output_paths(
            &manifest_alias_paths,
            &source_path,
            &secret_path,
            Some(&witness_scratch),
            1,
            1,
        )
        .is_err());

        for (reserved_name, reserved_root) in [
            ("serving_run_directory", &serving_directory),
            ("source_witness_scratch", &witness_scratch),
        ] {
            let outside_target = directory
                .path()
                .join(format!("{reserved_name}-outside-target"));
            std::fs::write(&outside_target, b"outside-target").unwrap();
            let reserved_alias = directory.path().join(format!("{reserved_name}-alias"));
            symlink(reserved_root, &reserved_alias).unwrap();
            let reserved_entry = reserved_root.join("outward-tax-link");
            symlink(&outside_target, &reserved_entry).unwrap();
            let mut reserved_symlink_paths = base_paths.clone();
            reserved_symlink_paths.manifest_provider_group_tax_identity_v2_sidecar = Some(
                reserved_alias
                    .join("outward-tax-link")
                    .display()
                    .to_string(),
            );
            let error = validate_runtime_tax_identity_output_paths(
                &reserved_symlink_paths,
                &source_path,
                &secret_path,
                Some(&witness_scratch),
                1,
                1,
            )
            .unwrap_err();
            assert!(error.to_string().contains(reserved_name));
            assert_eq!(std::fs::read(&outside_target).unwrap(), b"outside-target");
        }
    }

    let mut valid_paths = base_paths;
    valid_paths.manifest_provider_group_tax_identity_v2_sidecar = Some(
        directory
            .path()
            .join("tax-v2.ptg2tax")
            .display()
            .to_string(),
    );
    validate_runtime_tax_identity_output_paths(
        &valid_paths,
        &source_path,
        &secret_path,
        Some(&witness_scratch),
        2,
        2,
    )
    .unwrap();
}
#[test]
fn strict_copy_path_environment_reports_each_missing_root_coordinate() {
    let _lock = scanner_env_lock().lock().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let _strict = strict_scan_env(directory.path());
    {
        let _missing = TestEnvVar::remove("HLTHPRT_PTG2_SNAPSHOT_ARCH");
        assert!(CopyPathConfig::from_env().is_err());
    }
    {
        let _invalid = TestEnvVar::set("HLTHPRT_PTG2_SNAPSHOT_ARCH", "x86_64");
        assert!(CopyPathConfig::from_env().is_err());
    }
    {
        let _missing = TestEnvVar::remove("HLTHPRT_PTG2_V3_SERVING_RUN_DIR");
        assert!(CopyPathConfig::from_env().is_err());
    }
    {
        let _missing = TestEnvVar::remove(V3_COVERAGE_SCOPE_ID_ENV);
        assert!(CopyPathConfig::from_env().is_err());
    }
}
#[test]
fn bounded_worker_queue_delivery_covers_success_pressure_and_shutdown() {
    let empty_job = || WorkerJob::Rates {
        procedure: Map::new(),
        rates: Vec::new(),
    };
    let empty_batch = || RawRateChunk::with_capacity(0, 0);
    let (_event_tx, event_rx) = unbounded();
    let mut writer = Vec::new();
    let mut blocked_micros = 0;
    let mut stats = RawChunkStats::default();
    let mut copy_file_event_gate = CopyFileEventGate::passthrough();

    let (job_tx, job_rx) = bounded(1);
    {
        let mut send_test_job = |tx, cancelled, job| {
            let mut io_state = InNetworkEnqueueIo {
                tx,
                event_rx: &event_rx,
                writer: &mut writer,
                copy_file_event_gate: &mut copy_file_event_gate,
                cancelled,
                producer_blocked_micros: &mut blocked_micros,
                raw_chunk_stats: &mut stats,
            };
            send_worker_job(&mut io_state, job)
        };
        send_test_job(&job_tx, None, empty_job()).unwrap();
        assert!(matches!(job_rx.recv().unwrap(), WorkerJob::Rates { .. }));

        let cancelled = AtomicBool::new(true);
        assert_eq!(
            send_test_job(&job_tx, Some(&cancelled), empty_job())
                .unwrap_err()
                .kind(),
            io::ErrorKind::Interrupted
        );

        let (stopped_job_tx, stopped_job_rx) = bounded(1);
        drop(stopped_job_rx);
        assert_eq!(
            send_test_job(&stopped_job_tx, None, empty_job())
                .unwrap_err()
                .kind(),
            io::ErrorKind::BrokenPipe
        );
    }

    super::super::super::bounded_queue_pressure::assert_worker_job_queue_pressure(
        &mut blocked_micros,
        &mut stats,
    );

    let (batch_tx, batch_rx) = bounded(1);
    send_provider_ref_batch(
        &batch_tx,
        &event_rx,
        &mut writer,
        &mut blocked_micros,
        &mut stats,
        &mut copy_file_event_gate,
        empty_batch(),
    )
    .unwrap();
    assert!(batch_rx.recv().unwrap().is_empty());

    let (stopped_batch_tx, stopped_batch_rx) = bounded(1);
    drop(stopped_batch_rx);
    assert_eq!(
        send_provider_ref_batch(
            &stopped_batch_tx,
            &event_rx,
            &mut writer,
            &mut blocked_micros,
            &mut stats,
            &mut copy_file_event_gate,
            empty_batch(),
        )
        .unwrap_err()
        .kind(),
        io::ErrorKind::BrokenPipe
    );

    super::super::super::bounded_queue_pressure::assert_provider_reference_queue_pressure(
        &mut blocked_micros,
        &mut stats,
    );

    let mut sink = io::sink();
    let event = super::super::super::bounded_queue_pressure::empty_copy_file_event();
    emit_copy_file_event(&mut sink, &event).unwrap();
    let (sink_event_tx, sink_event_rx) = unbounded();
    sink_event_tx.send(event).unwrap();
    drain_copy_file_events(&sink_event_rx, &mut sink, &mut copy_file_event_gate).unwrap();

    let (sink_job_tx, sink_job_rx) = bounded(1);
    let mut sink_io_state = InNetworkEnqueueIo {
        tx: &sink_job_tx,
        event_rx: &sink_event_rx,
        writer: &mut sink,
        copy_file_event_gate: &mut copy_file_event_gate,
        cancelled: None,
        producer_blocked_micros: &mut blocked_micros,
        raw_chunk_stats: &mut stats,
    };
    send_worker_job(&mut sink_io_state, empty_job()).unwrap();
    assert!(matches!(
        sink_job_rx.recv().unwrap(),
        WorkerJob::Rates { .. }
    ));
}
