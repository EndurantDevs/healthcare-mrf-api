    #[test]
    fn rejects_invalid_builder_sequences_without_retaining_artifacts() {
        let directory = tempfile::tempdir().unwrap();
        assert!(PackedOutputBuilder::create(
            directory.path(),
            "version-1",
            Arc::new(AtomicU64::new(0)),
            0,
        )
        .is_err());
        assert!(PackedOutputBuilder::create(
            directory.path(),
            &"x".repeat(MAX_VERSION_ID_BYTES + 1),
            Arc::new(AtomicU64::new(0)),
            1024,
        )
        .is_err());
        let file_path = directory.path().join("not-a-directory");
        fs::write(&file_path, b"x").unwrap();
        assert!(PackedOutputBuilder::create(
            &file_path,
            "version-1",
            Arc::new(AtomicU64::new(0)),
            1024,
        )
        .is_err());

        let cases = (0..9)
            .map(|_| tempfile::tempdir().unwrap())
            .collect::<Vec<_>>();
        assert!(builder(cases[0].path()).finish().is_err());

        let mut output = builder(cases[1].path());
        output.service(0, &service()).unwrap();
        assert!(output.finish().is_err());

        let mut output = builder(cases[2].path());
        assert!(output.charge(0, 0, &charge("1")).is_err());
        assert!(output.payer(0, 0, &payer("1")).is_err());

        let mut output = builder(cases[3].path());
        output.service(0, &service()).unwrap();
        assert!(output.service(0, &service()).is_err());
        output.charge(0, 0, &charge("1")).unwrap();
        assert!(output.charge(0, 0, &charge("1")).is_err());
        assert!(output.payer(1, 0, &payer("1")).is_err());

        let mut output = builder(cases[4].path());
        output.service_count = u64::MAX;
        assert!(output.service(0, &service()).is_err());

        let mut output = builder(cases[5].path());
        output.service(0, &service()).unwrap();
        output.next_charge_key = u32::MAX;
        assert!(output.charge(0, 0, &charge("1")).is_err());

        let mut output = builder(cases[6].path());
        output.service(0, &service()).unwrap();
        output.charge(0, 0, &charge("1")).unwrap();
        output.next_fact_ordinal = u64::MAX;
        assert!(output.payer(0, 0, &payer("1")).is_err());

        let mut output = builder(cases[7].path());
        output.service(0, &service()).unwrap();
        output.charge(0, 0, &charge("1")).unwrap();
        output.current_charge.as_mut().unwrap().first_fact_ordinal = 1;
        assert!(output.finish_current_charge().is_err());

        let mut output = builder(cases[8].path());
        output.service(0, &service()).unwrap();
        output.charge(0, 0, &charge("1")).unwrap();
        output.next_fact_ordinal = u64::from(u32::MAX) + 1;
        assert!(output.finish_current_charge().is_err());
    }
    #[test]
    fn packed_sink_rejects_closed_and_out_of_range_records() {
        let directory = tempfile::tempdir().unwrap();
        let mut sink = PackedSink::create(
            directory.path(),
            "test",
            "version-1",
            Arc::new(AtomicU64::new(0)),
            1024 * 1024,
        )
        .unwrap();
        let mut invalid_records = Vec::new();
        let mut value = metadata();
        value.block_ordinal = u64::MAX;
        invalid_records.push(value);
        value = metadata();
        value.logical_first = u64::MAX;
        invalid_records.push(value);
        value = metadata();
        value.logical_count = u32::MAX;
        invalid_records.push(value);
        value = metadata();
        value.secondary_first = u64::MAX;
        invalid_records.push(value);
        value = metadata();
        value.secondary_count = u32::MAX;
        invalid_records.push(value);
        value = metadata();
        value.page_index = u32::MAX;
        invalid_records.push(value);
        value = metadata();
        value.page_count = u32::MAX;
        invalid_records.push(value);
        for invalid_record in invalid_records {
            assert!(sink.write_record(invalid_record, b"payload").is_err());
        }
        sink.rows = u64::MAX;
        assert!(sink.write_record(metadata(), b"payload").is_err());
        sink.rows = 0;
        assert_eq!(sink.finish().unwrap().rows, 0);
        assert!(sink.write_record(metadata(), b"payload").is_err());
        assert!(sink.writer_mut().is_err());
        assert!(sink.finish().is_err());

        for allowance in [0, 2, 15, 21, 33, 45, 53, 65, 73, 81, 89, 125, 161, 197] {
            let directory = tempfile::tempdir().unwrap();
            let mut sink = PackedSink::create(
                directory.path(),
                "test",
                "version-1",
                Arc::new(AtomicU64::new(0)),
                u64::MAX,
            )
            .unwrap();
            limit_sink_writes(&mut sink, allowance);
            let mut value = metadata();
            value.key_sha256 = Some([1; 32]);
            value.parent_sha256 = Some([2; 32]);
            assert!(sink.write_record(value, b"x").is_err());
        }

        let directory = tempfile::tempdir().unwrap();
        let mut sink = PackedSink::create(
            directory.path(),
            "test",
            "version-1",
            Arc::new(AtomicU64::new(0)),
            0,
        )
        .unwrap();
        assert!(sink.finish().is_err());
    }

    #[test]
    fn selector_preflight_rejects_corrupt_spools() {
        use crate::hospital_price_selector_block::HospitalPriceSelectorKey;

        let keys = vec![
            selector_code("12345"),
            HospitalPriceSelectorKey::PayerPlan {
                payer_name: "payer".to_owned(),
                plan_name: "plan".to_owned(),
            },
        ];
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("selector-spool");
        let write = |records: &[[u8; 13]]| {
            fs::write(
                &path,
                records
                    .iter()
                    .flat_map(|record| record.iter().copied())
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        };

        write(&[
            selector_record(1, 0, 0),
            selector_record(1, 0, 1),
            selector_record(2, 1, 0),
        ]);
        let preflight = count_selector_pages(&path, &keys, 2, 1).unwrap();
        assert_eq!(preflight.page_counts, [1, 1]);
        for records in [
            vec![selector_record(1, 0, 0), selector_record(1, 0, 0)],
            vec![selector_record(1, 9, 0)],
            vec![selector_record(2, 0, 0)],
            vec![selector_record(1, 0, 2)],
            vec![selector_record(2, 1, 1)],
            vec![selector_record(1, 0, 0)],
        ] {
            write(&records);
            assert!(count_selector_pages(&path, &keys, 2, 1).is_err());
        }
        fs::write(&path, [1, 0]).unwrap();
        assert!(count_selector_pages(&path, &keys, 2, 1).is_err());
        fs::write(&path, []).unwrap();
        assert!(count_selector_pages(&path, &keys, 2, 1).is_err());

        let mut counts = [0];
        assert!(add_selector_page_count(&mut counts, 1, &keys[0], 1).is_err());
        counts[0] = u32::MAX;
        assert!(add_selector_page_count(&mut counts, 0, &keys[0], 1).is_err());
        assert!(add_selector_page_count(&mut [0], 0, &keys[0], usize::MAX).is_err());

        let oversized_keys = vec![
            selector_code("x".repeat(
                crate::hospital_price_selector_block::HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_KEY_BYTES
                    + 1,
            )),
            keys[1].clone(),
        ];
        write(&[
            selector_record(1, 0, 0),
            selector_record(2, 1, 0),
        ]);
        assert!(count_selector_pages(&path, &oversized_keys, 1, 1).is_err());
        assert!(selector_ref_capacity(&selector_code("x".repeat(
            crate::hospital_price_selector_block::HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_KEY_BYTES + 1
        )))
        .is_err());
        assert!(service_block_size_error(
            crate::hospital_price_service_block::HOSPITAL_PRICE_SERVICE_BLOCK_RAW_SIZE_ERROR
        ));
        assert!(!service_block_size_error("different error"));
        assert!(fact_block_size_error(
            crate::hospital_price_block::HOSPITAL_PRICE_FACT_BLOCK_RAW_SIZE_ERROR
        ));
        assert!(!fact_block_size_error(
            "hospital price fact block decimal value is invalid"
        ));
    }

    #[test]
    fn selector_builder_rejects_closed_or_over_budget_scratch() {
        let directories = (0..4)
            .map(|_| tempfile::tempdir().unwrap())
            .collect::<Vec<_>>();
        let mut output = builder(directories[0].path());
        output.max_output_bytes = 257;
        assert!(output
            .write_selector_ref(selector_code("x"), 0)
            .is_err());

        let mut output = builder(directories[1].path());
        let key = selector_code("x");
        output.selector_key_ordinal(key.clone()).unwrap();
        output.max_output_bytes = 38;
        assert!(output.write_selector_ref(key, 0).is_err());

        let mut output = builder(directories[2].path());
        output.selector_spool_bytes = u64::MAX;
        assert!(output
            .write_selector_ref(selector_code("x"), 0)
            .is_err());

        let mut output = builder(directories[3].path());
        output.selector_spool = None;
        assert!(output
            .write_selector_ref(selector_code("x"), 0)
            .is_err());
        assert!(output.finish_selector_pages().is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        assert!(output.finish_selector_pages().is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        output
            .write_selector_ref(selector_code("x"), 0)
            .unwrap();
        fs::remove_file(&output.selector_spool_path).unwrap();
        assert!(output.finish_selector_pages().is_err());

        assert!(ensure_selector_key_capacity(MAX_SELECTOR_KEYS).is_err());
    }

    #[test]
    fn packed_helpers_cover_fallbacks_and_disabled_text_output() {
        assert!(charge_json_retained_bytes(1).is_ok());
        let mut deserializer = serde_json::Deserializer::from_str("\"code\"");
        assert_eq!(
            deserialize_json_code_text(&mut deserializer).unwrap(),
            "code"
        );
        let mut deserializer = serde_json::Deserializer::from_str("\"value\"");
        assert_eq!(
            deserialize_optional_json_retained_string(&mut deserializer).unwrap(),
            Some("value".to_owned())
        );
        let previous = JSON_RETAINED_BYTES.with(|budget| budget.replace(Some(0)));
        let mut deserializer = serde_json::Deserializer::from_str("\"over-budget\"");
        assert!(deserialize_optional_json_retained_string(&mut deserializer).is_err());
        JSON_RETAINED_BYTES.with(|budget| budget.set(previous));
        let values: FanoutVec<String> = serde_json::from_str("[\"one\"]").unwrap();
        assert_eq!(values.0, ["one"]);
        assert!(serde_json::from_str::<FanoutVec<String>>("1").is_err());
        assert!(find_header(&StringRecord::from(vec!["different"]), &["missing"]).is_err());
        assert_eq!(
            crate::hashing::hash_text_key("not-hex"),
            crate::hashing::xxh3_63(b"not-hex")
        );

        let mut row = payer("90");
        row.additional_payer_notes = None;
        assert!(validate_payer(row, Some("generic note"), true).is_ok());

        let directory = tempfile::tempdir().unwrap();
        let mut outputs = CopyOutputs::create(
            directory.path(),
            "version-1",
            16 * 1024 * 1024,
            HospitalMrfOutputMode::Packed,
        )
        .unwrap();
        assert!(outputs.write(CopyKind::Service, &[]).is_err());

        let legacy_directory = tempfile::tempdir().unwrap();
        let mut outputs = CopyOutputs::create(
            legacy_directory.path(),
            "version-1",
            16 * 1024 * 1024,
            HospitalMrfOutputMode::Legacy,
        )
        .unwrap();
        emit_service(&mut outputs, "version-1", 0, &service()).unwrap();
    }
    fn selector_code(
        code: impl Into<String>,
    ) -> crate::hospital_price_selector_block::HospitalPriceSelectorKey {
        selector_code_with_type("CPT", code)
    }

    fn selector_code_with_type(
        code_type: impl Into<String>,
        code: impl Into<String>,
    ) -> crate::hospital_price_selector_block::HospitalPriceSelectorKey {
        crate::hospital_price_selector_block::HospitalPriceSelectorKey::Code {
            code_type: code_type.into(),
            code: code.into(),
        }
    }

    #[test]
    fn selector_code_identity_includes_code_type() {
        let key = selector_code_with_type("HCPCS", "12345");
        assert_ne!(
            crate::hospital_price_selector_block::selector_key_sha256(
                &selector_code_with_type("CPT", "12345"),
            ),
            crate::hospital_price_selector_block::selector_key_sha256(&key),
        );
        assert_eq!(
            selector_key_memory_bytes(&key),
            (("HCPCS".len() + "12345".len()) * 2) as u64 + SELECTOR_KEY_MEMORY_OVERHEAD_BYTES,
        );
        assert_eq!(
            selector_ref_capacity(&key).unwrap(),
            (crate::hospital_price_selector_block::HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_RAW_BYTES
                - (4 + "HCPCS".len())
                - (4 + "12345".len())
                - 4)
                / 8,
        );
        let mut row = service();
        row.codes[0].code_type = "x".repeat(
            crate::hospital_price_selector_block::HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_KEY_BYTES + 1,
        );
        assert!(packed_service_code_indexes(&row).is_err());
    }

    type SelectorTestRows = ([u64; 2], Vec<Vec<Option<Vec<u8>>>>);

    fn write_selector_test_rows(
        directory: &Path,
        keys: Vec<crate::hospital_price_selector_block::HospitalPriceSelectorKey>,
        refs_by_key: Vec<Vec<u64>>,
        page_counts: &[u32],
    ) -> SelectorTestRows {
        assert_eq!(keys.len(), refs_by_key.len());
        assert_eq!(keys.len(), page_counts.len());
        let digests = keys
            .iter()
            .map(crate::hospital_price_selector_block::selector_key_sha256)
            .collect::<Vec<_>>();
        assert!(digests.windows(2).all(|pair| pair[0] < pair[1]));

        let mut output = builder(directory);
        for key in keys {
            output.selector_key_ordinal(key).unwrap();
        }
        let mut records = Vec::with_capacity(
            refs_by_key.iter().map(Vec::len).sum::<usize>() * SELECTOR_SPOOL_RECORD_BYTES,
        );
        for (ordinal, references) in refs_by_key.iter().enumerate() {
            for reference in references {
                records.extend_from_slice(&selector_record(1, ordinal as u32, *reference));
            }
        }
        fs::write(&output.selector_sorted_path, records).unwrap();
        output.write_selector_pages(page_counts).unwrap();
        let block_counts = output.selector_block_counts;
        let artifact = output.sinks[2].finish().unwrap();
        let rows = copy_rows(&directory.join("selector_page.copy"));
        assert_eq!(artifact.rows as usize, rows.len());
        (block_counts, rows)
    }

    #[test]
    fn selector_packing_keeps_multi_page_keys_between_packs() {
        use crate::hospital_price_selector_block::{
            HospitalPriceSelectorKey, HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_KEY_BYTES,
        };

        let code_key = |code: &str| HospitalPriceSelectorKey::Code {
            code_type: "CPT".to_owned(),
            code: code.to_owned(),
        };
        let keys = vec![
            code_key("adjacent-32"),
            code_key("adjacent-95"),
            HospitalPriceSelectorKey::Code {
                code_type: "x".repeat(HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_KEY_BYTES),
                code: "x".repeat(HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_KEY_BYTES),
            },
            code_key("adjacent-24"),
            code_key("adjacent-3"),
        ];
        let capacity = selector_ref_capacity(&keys[2]).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let (block_counts, rows) = write_selector_test_rows(
            directory.path(),
            keys,
            vec![
                vec![0],
                vec![1],
                (0..=capacity as u64).collect(),
                vec![0],
                vec![1],
            ],
            &[1, 1, 2, 1, 1],
        );

        assert_eq!(block_counts, [4, 0]);
        assert_eq!(
            rows.iter()
                .map(|row| (
                    field_i64(row, 2),
                    field_i64(row, 3),
                    field_i32(row, 4),
                    field_i32(row, 7),
                    field_i32(row, 8),
                ))
                .collect::<Vec<_>>(),
            [
                (0, 0, 2, 0, 1),
                (1, 2, 1, 0, 2),
                (2, 2, 1, 1, 2),
                (3, 3, 2, 0, 1),
            ]
        );
    }
