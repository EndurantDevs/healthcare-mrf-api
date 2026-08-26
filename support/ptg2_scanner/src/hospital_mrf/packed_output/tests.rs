#[cfg(test)]
mod packed_output_tests {
    use super::*;

    fn service() -> ServiceRow {
        ServiceRow {
            description: "multi code".to_owned(),
            codes: vec![
                CodeRow {
                    code_type: "CPT".to_owned(),
                    code: "12345".to_owned(),
                },
                CodeRow {
                    code_type: "HCPCS".to_owned(),
                    code: "A1234".to_owned(),
                },
            ],
            drug_unit: None,
            drug_type: None,
        }
    }

    fn charge(amount: &str) -> ChargeRow {
        ChargeRow {
            setting: "inpatient".to_owned(),
            billing_class: Some("facility".to_owned()),
            modifier_codes: Vec::new(),
            gross_charge: Some(amount.to_owned()),
            discounted_cash: Some("80.00".to_owned()),
            minimum: Some("70.00".to_owned()),
            maximum: Some("130.00".to_owned()),
            additional_generic_notes: None,
        }
    }

    fn payer(amount: &str) -> PayerChargeRow {
        PayerChargeRow {
            payer_name: "Shared Payer".to_owned(),
            plan_name: "Shared Plan".to_owned(),
            standard_charge_dollar: Some(amount.to_owned()),
            standard_charge_percentage: None,
            standard_charge_algorithm: None,
            median_amount: Some("95.00".to_owned()),
            percentile_10: None,
            percentile_90: None,
            allowed_count: Some("42".to_owned()),
            methodology: "fee schedule".to_owned(),
            additional_payer_notes: None,
        }
    }

    fn build(directory: &Path) -> PackedOutputSummary {
        let retained_bytes = Arc::new(AtomicU64::new(7));
        let mut output = PackedOutputBuilder::create(
            directory,
            "version-1",
            Arc::clone(&retained_bytes),
            16 * 1024 * 1024,
        )
        .unwrap();
        output.service(0, &service()).unwrap();
        output.charge(0, 0, &charge("100.00")).unwrap();
        output.payer(0, 0, &payer("90.00")).unwrap();
        output.charge(0, 1, &charge("200.00")).unwrap();
        output.payer(0, 1, &payer("180.00")).unwrap();
        let summary = output.finish().unwrap();
        assert_eq!(
            retained_bytes.load(Ordering::Relaxed),
            7 + summary
                .artifacts
                .iter()
                .map(|artifact| artifact.bytes)
                .sum::<u64>()
        );
        summary
    }

    fn builder(directory: &Path) -> PackedOutputBuilder {
        PackedOutputBuilder::create(
            directory,
            "version-1",
            Arc::new(AtomicU64::new(0)),
            16 * 1024 * 1024,
        )
        .unwrap()
    }

    fn metadata() -> PackedRecordMetadata {
        PackedRecordMetadata {
            block_kind: HOSPITAL_PRICE_SERVICE_BLOCK_KIND,
            block_ordinal: 0,
            logical_first: 0,
            logical_count: 1,
            secondary_first: 0,
            secondary_count: 1,
            page_index: 0,
            page_count: 1,
            key_sha256: None,
            parent_sha256: None,
        }
    }

    fn selector_record(kind: u8, ordinal: u32, reference: u64) -> [u8; 13] {
        let mut record = [0u8; 13];
        record[0] = kind;
        record[1..5].copy_from_slice(&ordinal.to_be_bytes());
        record[5..].copy_from_slice(&reference.to_be_bytes());
        record
    }

    fn packed_charge_row(charge_key: u32) -> crate::hospital_price_service_block::HospitalPriceChargeRow {
        crate::hospital_price_service_block::HospitalPriceChargeRow {
            charge_key,
            charge_ordinal: u64::from(charge_key),
            setting: "inpatient".to_owned(),
            billing_class: Some("facility".to_owned()),
            modifier_codes: Vec::new(),
            gross_charge: Some("100".to_owned()),
            discounted_cash: None,
            minimum: None,
            maximum: None,
            additional_generic_notes: None,
            first_fact_ordinal: 0,
            fact_count: 0,
        }
    }

    fn packed_service_row(
        first_charge_key: u32,
        charge_count: u32,
        description: String,
    ) -> crate::hospital_price_service_block::HospitalPriceServiceRow {
        crate::hospital_price_service_block::HospitalPriceServiceRow {
            service_ordinal: 0,
            description,
            drug_unit: None,
            drug_type: None,
            codes: vec![crate::hospital_price_service_block::HospitalPriceServiceCode {
                code_type: "CPT".to_owned(),
                code: "12345".to_owned(),
            }],
            charges: (first_charge_key..first_charge_key + charge_count)
                .map(packed_charge_row)
                .collect(),
        }
    }

    fn packed_fact(note_bytes: usize) -> crate::hospital_price_block::HospitalPriceFactRow {
        crate::hospital_price_block::HospitalPriceFactRow {
            charge_key: 0,
            payer_name: "payer".to_owned(),
            plan_name: "plan".to_owned(),
            negotiated_dollar: Some("90".to_owned()),
            negotiated_percentage: None,
            negotiated_algorithm: None,
            methodology: "fee schedule".to_owned(),
            median_amount: None,
            percentile_10: None,
            percentile_90: None,
            allowed_count: Some("1".to_owned()),
            additional_payer_notes: Some("x".repeat(note_bytes)),
            comparison_amount: Some("90".to_owned()),
        }
    }

    fn prepared_builder(directory: &Path) -> PackedOutputBuilder {
        let mut output = builder(directory);
        output.service(0, &service()).unwrap();
        output.charge(0, 0, &charge("100")).unwrap();
        output.payer(0, 0, &payer("90")).unwrap();
        output.finish_current_charge().unwrap();
        output.finish_current_service().unwrap();
        output.flush_service_rows().unwrap();
        output.flush_fact_rows().unwrap();
        output
    }

    fn limit_sink_writes(sink: &mut PackedSink, allowance: u64) {
        let writer = sink.writer.take().unwrap();
        let mut inner = writer.into_inner().ok().unwrap();
        inner.max_output_bytes = inner.aggregate_bytes.load(Ordering::Relaxed) + allowance;
        sink.writer = Some(BufWriter::with_capacity(1, inner));
    }

    fn fail_next_sink_write(sink: &mut PackedSink) {
        limit_sink_writes(sink, 0);
    }

    fn copy_rows(path: &Path) -> Vec<Vec<Option<Vec<u8>>>> {
        let bytes = fs::read(path).unwrap();
        assert!(bytes.len() >= 21);
        assert_eq!(&bytes[..11], PG_BINARY_COPY_SIGNATURE);
        assert_eq!(i32::from_be_bytes(bytes[11..15].try_into().unwrap()), 0);
        assert_eq!(i32::from_be_bytes(bytes[15..19].try_into().unwrap()), 0);
        let mut rows = Vec::new();
        let mut offset = 19usize;
        loop {
            let field_count = i16::from_be_bytes(bytes[offset..offset + 2].try_into().unwrap());
            offset += 2;
            if field_count == -1 {
                break;
            }
            assert_eq!(field_count, PG_BINARY_COPY_FIELD_COUNT);
            let mut row = Vec::with_capacity(field_count as usize);
            for _ in 0..field_count {
                let length = i32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap());
                offset += 4;
                if length == -1 {
                    row.push(None);
                    continue;
                }
                assert!(length >= 0);
                let end = offset + length as usize;
                row.push(Some(bytes[offset..end].to_vec()));
                offset = end;
            }
            rows.push(row);
        }
        assert_eq!(offset, bytes.len());
        rows
    }

    fn field_i16(row: &[Option<Vec<u8>>], index: usize) -> i16 {
        i16::from_be_bytes(row[index].as_deref().unwrap().try_into().unwrap())
    }

    fn field_i32(row: &[Option<Vec<u8>>], index: usize) -> i32 {
        i32::from_be_bytes(row[index].as_deref().unwrap().try_into().unwrap())
    }

    fn field_i64(row: &[Option<Vec<u8>>], index: usize) -> i64 {
        i64::from_be_bytes(row[index].as_deref().unwrap().try_into().unwrap())
    }

    fn payloads(path: &Path) -> Vec<Vec<u8>> {
        copy_rows(path)
            .into_iter()
            .map(|row| {
                let payload = row[12].clone().unwrap();
                assert_eq!(
                    row[11].as_deref(),
                    Some(Sha256::digest(&payload).as_slice())
                );
                payload
            })
            .collect()
    }

    #[test]
    fn streams_multi_code_multi_charge_and_shared_payer_deterministically() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        let first_summary = build(first.path());
        let second_summary = build(second.path());
        assert_eq!(
            first_summary
                .artifacts
                .iter()
                .map(|summary| (&summary.kind, summary.rows, &summary.sha256))
                .collect::<Vec<_>>(),
            second_summary
                .artifacts
                .iter()
                .map(|summary| (&summary.kind, summary.rows, &summary.sha256))
                .collect::<Vec<_>>()
        );

        let root = &first_summary.root;
        assert_eq!(
            (root.service_count, root.charge_count, root.fact_count),
            (1, 2, 2)
        );
        assert_eq!(
            (
                root.code_selector_key_count,
                root.payer_plan_selector_key_count
            ),
            (2, 1)
        );
        assert_eq!(
            (
                root.code_selector_ref_count,
                root.payer_plan_selector_ref_count
            ),
            (4, 2)
        );
        assert_eq!(
            (
                root.code_selector_page_count,
                root.payer_plan_selector_page_count
            ),
            (2, 1)
        );
        assert_eq!(
            (
                root.service_block_count,
                root.fact_block_count,
                root.code_selector_block_count,
                root.payer_plan_selector_block_count,
            ),
            (1, 1, 2, 1)
        );
        assert_eq!(
            (root.selector_spool_bytes, root.peak_scratch_bytes),
            (78, 234)
        );

        let service_rows = copy_rows(&first.path().join("service_block.copy"));
        assert_eq!(service_rows.len(), 1);
        assert_eq!(service_rows[0].len(), 13);
        assert_eq!(service_rows[0][0].as_deref(), Some(&b"version-1"[..]));
        assert_eq!(
            field_i16(&service_rows[0], 1),
            HOSPITAL_PRICE_SERVICE_BLOCK_KIND
        );
        assert_eq!(field_i64(&service_rows[0], 2), 0);
        assert!(service_rows[0][9].is_none());
        assert!(service_rows[0][10].is_none());
        let services = payloads(&first.path().join("service_block.copy"));
        let services =
            crate::hospital_price_service_block::decode_service_block(&services[0]).unwrap();
        assert_eq!(services.len(), 1);
        assert_eq!(services[0].codes.len(), 2);
        assert_eq!(services[0].charges.len(), 2);
        assert_eq!(services[0].charges[0].charge_key, 0);
        assert_eq!(services[0].charges[1].charge_key, 1);

        let fact_rows = copy_rows(&first.path().join("fact_block.copy"));
        assert_eq!(field_i16(&fact_rows[0], 1), HOSPITAL_PRICE_FACT_BLOCK_KIND);
        assert!(fact_rows[0][9].is_none());
        assert!(fact_rows[0][10].is_none());
        let facts = payloads(&first.path().join("fact_block.copy"));
        let facts = crate::hospital_price_block::decode_fact_block(
            &facts[0],
            None,
            None,
            0,
            crate::hospital_price_block::HOSPITAL_PRICE_FACT_BLOCK_MAX_ROWS,
        )
        .unwrap();
        assert_eq!(facts.len(), 2);
        assert_eq!(facts[0].comparison_amount.as_deref(), Some("90.00"));
        assert_eq!(facts[1].comparison_amount.as_deref(), Some("180.00"));

        let selector_rows = copy_rows(&first.path().join("selector_page.copy"));
        assert_eq!(selector_rows.len(), 3);
        assert!(selector_rows.iter().all(|row| row[9].is_some()));
        assert!(selector_rows
            .iter()
            .filter(|row| field_i16(row, 1) == HOSPITAL_PRICE_CODE_SELECTOR_BLOCK_KIND)
            .all(|row| row[10].is_none()));
        assert!(selector_rows
            .iter()
            .filter(|row| field_i16(row, 1) == HOSPITAL_PRICE_PAYER_PLAN_SELECTOR_BLOCK_KIND)
            .all(|row| row[10].is_some()));
        let selectors = payloads(&first.path().join("selector_page.copy"))
            .into_iter()
            .map(|payload| {
                crate::hospital_price_selector_block::decode_selector_page(&payload).unwrap()
            })
            .collect::<Vec<_>>();
        let code_pages = selectors
            .iter()
            .filter(|page| {
                page.kind
                    == crate::hospital_price_selector_block::HospitalPriceSelectorKind::CodeToCharge
            })
            .collect::<Vec<_>>();
        assert_eq!(code_pages.len(), 2);
        assert!(code_pages.iter().all(|page| page.entries[0].refs == [0, 1]));
        let payer_page = selectors
            .iter()
            .find(|page| {
                page.kind
                    == crate::hospital_price_selector_block::HospitalPriceSelectorKind::PayerPlanToFact
            })
            .unwrap();
        assert_eq!(payer_page.entries[0].refs, [0, 1]);
    }

    #[test]
    fn one_service_spans_blocks_without_inflating_the_service_count() {
        let directory = tempfile::tempdir().unwrap();
        let mut output = PackedOutputBuilder::create(
            directory.path(),
            "version-1",
            Arc::new(AtomicU64::new(0)),
            32 * 1024 * 1024,
        )
        .unwrap();
        output.service(0, &service()).unwrap();
        for ordinal in 0..513 {
            output.charge(0, ordinal, &charge("100.00")).unwrap();
            output.payer(0, ordinal, &payer("90.00")).unwrap();
        }
        let summary = output.finish().unwrap();
        assert_eq!(
            (
                summary.root.service_count,
                summary.root.charge_count,
                summary.root.fact_count,
                summary.root.service_block_count,
            ),
            (1, 513, 513, 2)
        );

        let rows = copy_rows(&directory.path().join("service_block.copy"));
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows.iter()
                .map(|row| {
                    (
                        field_i64(row, 3),
                        field_i32(row, 4),
                        field_i64(row, 5),
                        field_i32(row, 6),
                    )
                })
                .collect::<Vec<_>>(),
            vec![(0, 1, 0, 512), (0, 1, 512, 1)]
        );
        assert_eq!(
            payloads(&directory.path().join("service_block.copy"))
                .into_iter()
                .map(|payload| {
                    let services = crate::hospital_price_service_block::decode_service_block(
                        &payload,
                    )
                    .unwrap();
                    assert_eq!(services.len(), 1);
                    assert_eq!(services[0].service_ordinal, 0);
                    services[0].charges.len()
                })
                .collect::<Vec<_>>(),
            vec![512, 1]
        );
    }

    #[test]
    fn output_limit_failure_removes_every_partial_and_final_artifact() {
        let directory = tempfile::tempdir().unwrap();
        let retained_bytes = Arc::new(AtomicU64::new(0));
        let mut output =
            PackedOutputBuilder::create(directory.path(), "version-1", retained_bytes, 512)
                .unwrap();
        output.service(0, &service()).unwrap();
        output.charge(0, 0, &charge("100.00")).unwrap();
        output.payer(0, 0, &payer("90.00")).unwrap();
        assert!(output.finish().is_err());
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
    }

    #[test]
    fn oversized_service_codes_fail_before_the_builder_retains_a_copy() {
        let directory = tempfile::tempdir().unwrap();
        let mut output = PackedOutputBuilder::create(
            directory.path(),
            "version-1",
            Arc::new(AtomicU64::new(0)),
            32 * 1024 * 1024,
        )
        .unwrap();
        let mut row = service();
        row.codes = (0..5)
            .map(|index| CodeRow {
                code_type: "CPT".to_owned(),
                code: format!(
                    "{index}{}",
                    "x".repeat(
                        crate::hospital_price_selector_block::HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_KEY_BYTES
                            - 1
                    )
                ),
            })
            .collect();

        let error = output.service(0, &row).unwrap_err();

        assert!(error
            .to_string()
            .contains("service code data exceeds 4 MiB"));
        assert!(output.current_service.is_none());
        assert_eq!(output.service_count, 0);

        let mut row = service();
        row.codes[0].code = "x".repeat(
            crate::hospital_price_selector_block::HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_KEY_BYTES + 1,
        );
        assert!(packed_service_code_indexes(&row).is_err());
    }

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
            HospitalPriceSelectorKey::Code("12345".to_owned()),
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
            HospitalPriceSelectorKey::Code(
                "x".repeat(
                    crate::hospital_price_selector_block::HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_KEY_BYTES
                        + 1,
                ),
            ),
            keys[1].clone(),
        ];
        write(&[
            selector_record(1, 0, 0),
            selector_record(2, 1, 0),
        ]);
        assert!(count_selector_pages(&path, &oversized_keys, 1, 1).is_err());
        assert!(selector_ref_capacity(&HospitalPriceSelectorKey::Code(
            "x".repeat(
                crate::hospital_price_selector_block::HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_KEY_BYTES
                    + 1
            )
        ))
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
        use crate::hospital_price_selector_block::HospitalPriceSelectorKey;

        let directories = (0..4)
            .map(|_| tempfile::tempdir().unwrap())
            .collect::<Vec<_>>();
        let mut output = builder(directories[0].path());
        output.max_output_bytes = 257;
        assert!(output
            .write_selector_ref(HospitalPriceSelectorKey::Code("x".to_owned()), 0)
            .is_err());

        let mut output = builder(directories[1].path());
        let key = HospitalPriceSelectorKey::Code("x".to_owned());
        output.selector_key_ordinal(key.clone()).unwrap();
        output.max_output_bytes = 38;
        assert!(output.write_selector_ref(key, 0).is_err());

        let mut output = builder(directories[2].path());
        output.selector_spool_bytes = u64::MAX;
        assert!(output
            .write_selector_ref(HospitalPriceSelectorKey::Code("x".to_owned()), 0)
            .is_err());

        let mut output = builder(directories[3].path());
        output.selector_spool = None;
        assert!(output
            .write_selector_ref(HospitalPriceSelectorKey::Code("x".to_owned()), 0)
            .is_err());
        assert!(output.finish_selector_pages().is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        assert!(output.finish_selector_pages().is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        output
            .write_selector_ref(HospitalPriceSelectorKey::Code("x".to_owned()), 0)
            .unwrap();
        fs::remove_file(&output.selector_spool_path).unwrap();
        assert!(output.finish_selector_pages().is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        output.selector_keys = vec![HospitalPriceSelectorKey::Code(String::new()); MAX_SELECTOR_KEYS];
        assert!(output
            .selector_key_ordinal(HospitalPriceSelectorKey::Code("new".to_owned()))
            .is_err());
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

    #[test]
    fn validation_failures_are_explicit() {
        let mut row = service();
        row.codes.clear();
        assert!(validate_service(row, false).is_err());
        let mut row = service();
        row.codes[0].code_type = "NDC".to_owned();
        assert!(validate_service(row, false).is_err());
        let mut row = service();
        row.drug_unit = Some("mg".to_owned());
        assert!(validate_service(row, false).is_err());

        let mut row = charge("1");
        row.gross_charge = None;
        row.discounted_cash = None;
        assert!(validate_charge(row, &[], false).is_err());
        let mut row = charge("1");
        row.minimum = None;
        row.maximum = None;
        assert!(validate_charge(row, &[payer("1")], false).is_err());

        let mut row = payer("1");
        row.standard_charge_dollar = None;
        assert!(validate_payer(row, None, false).is_err());
        let mut row = payer("1");
        row.standard_charge_dollar = None;
        row.standard_charge_percentage = Some("10".to_owned());
        row.allowed_count = None;
        assert!(validate_payer(row, None, false).is_err());
        let mut row = payer("1");
        row.standard_charge_dollar = None;
        row.standard_charge_percentage = Some("10".to_owned());
        assert!(validate_payer(row, None, false).is_err());
        let mut row = payer("1");
        row.standard_charge_dollar = None;
        row.standard_charge_percentage = Some("10".to_owned());
        row.median_amount = Some("1".to_owned());
        assert!(validate_payer(row, None, false).is_err());
        let mut row = payer("1");
        row.standard_charge_dollar = None;
        row.standard_charge_percentage = Some("10".to_owned());
        row.median_amount = Some("1".to_owned());
        row.percentile_10 = Some("1".to_owned());
        assert!(validate_payer(row, None, false).is_err());
        let mut row = payer("1");
        row.methodology = "other".to_owned();
        assert!(validate_payer(row, None, false).is_err());
        let mut row = payer("1");
        row.allowed_count = Some("0".to_owned());
        assert!(validate_payer(row, None, false).is_err());
    }

    #[test]
    fn service_and_fact_block_splitting_is_bounded() {
        let rows = vec![
            packed_service_row(0, 3, "first".to_owned()),
            packed_service_row(3, 1, "second".to_owned()),
        ];
        let (left, right) = split_service_rows(rows.clone(), 1);
        assert_eq!(left[0].charges.len(), 1);
        assert_eq!(right.iter().map(|row| row.charges.len()).sum::<usize>(), 3);
        let (left, right) = split_service_rows(rows, 3);
        assert_eq!(left[0].charges.len(), 3);
        assert_eq!(right[0].charges.len(), 1);

        let directories = (0..5)
            .map(|_| tempfile::tempdir().unwrap())
            .collect::<Vec<_>>();
        let mut output = builder(directories[0].path());
        assert!(output
            .write_service_rows(vec![packed_service_row(1, 1, "gap".to_owned())])
            .is_err());

        let mut output = builder(directories[1].path());
        assert!(output
            .write_service_rows(vec![packed_service_row(0, 0, "empty".to_owned())])
            .is_err());

        let mut output = builder(directories[2].path());
        assert!(output
            .write_service_rows(vec![packed_service_row(
                0,
                2,
                "x".repeat(
                    crate::hospital_price_service_block::HOSPITAL_PRICE_SERVICE_BLOCK_MAX_RAW_BYTES
                )
            )])
            .is_err());

        let mut output = builder(directories[3].path());
        output
            .write_fact_rows(0, &[packed_fact(140 * 1024), packed_fact(140 * 1024)])
            .unwrap();
        assert_eq!(output.written_fact_count, 2);

        let mut output = builder(directories[4].path());
        assert!(output.write_fact_rows(1, &[packed_fact(0)]).is_err());
        let oversized = packed_fact(
            crate::hospital_price_block::HOSPITAL_PRICE_FACT_BLOCK_MAX_RAW_BYTES + 1,
        );
        assert!(output
            .write_fact_rows(0, &[oversized.clone(), oversized])
            .is_err());

        let split_directory = tempfile::tempdir().unwrap();
        let mut output = builder(split_directory.path());
        let mut second = packed_service_row(1, 1, "y".repeat(2_100_000));
        second.service_ordinal = 1;
        output
            .write_service_rows(vec![
                packed_service_row(0, 1, "x".repeat(2_100_000)),
                second,
            ])
            .unwrap();
        assert_eq!(output.written_charge_count, 2);

        let split_directory = tempfile::tempdir().unwrap();
        let mut output = builder(split_directory.path());
        let first = packed_fact(2_100_000);
        let mut second = packed_fact(0);
        second.additional_payer_notes = Some("y".repeat(2_100_000));
        output.write_fact_rows(0, &[first, second]).unwrap();
        assert_eq!(output.written_fact_count, 2);
    }

    #[test]
    fn selector_private_paths_fail_closed() {
        use crate::hospital_price_selector_block::HospitalPriceSelectorKey;

        let directories = (0..5)
            .map(|_| tempfile::tempdir().unwrap())
            .collect::<Vec<_>>();
        let mut output = builder(directories[0].path());
        let key = HospitalPriceSelectorKey::Code("12345".to_owned());
        output.selector_key_ordinal(key).unwrap();
        fs::write(
            &output.selector_sorted_path,
            selector_record(1, 0, 0),
        )
        .unwrap();
        assert!(output.write_selector_pages(&[2]).is_err());

        for (index, record) in [selector_record(1, 9, 0), selector_record(2, 0, 0)]
            .into_iter()
            .enumerate()
        {
            let mut output = builder(directories[index + 1].path());
            output
                .selector_key_ordinal(HospitalPriceSelectorKey::Code("12345".to_owned()))
                .unwrap();
            fs::write(&output.selector_sorted_path, record).unwrap();
            assert!(output.write_selector_pages(&[1]).is_err());
        }

        let mut output = builder(directories[3].path());
        let mut refs = Vec::new();
        output
            .write_selector_ref_chunks(0, &mut refs, &[], &mut [])
            .unwrap();
        refs.push(0);
        assert!(output
            .write_selector_ref_chunks(0, &mut refs, &[], &mut [])
            .is_err());

        let mut output = builder(directories[4].path());
        output
            .selector_key_ordinal(HospitalPriceSelectorKey::Code("12345".to_owned()))
            .unwrap();
        let mut refs = vec![0];
        assert!(output
            .write_selector_ref_chunks(0, &mut refs, &[], &mut [0])
            .is_err());
        assert!(output
            .write_selector_ref_chunks(0, &mut refs, &[1], &mut [])
            .is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        output
            .selector_key_ordinal(HospitalPriceSelectorKey::Code("first".to_owned()))
            .unwrap();
        output
            .selector_key_ordinal(HospitalPriceSelectorKey::Code("second".to_owned()))
            .unwrap();
        fs::write(
            &output.selector_sorted_path,
            [selector_record(1, 0, 0), selector_record(1, 1, 0)].concat(),
        )
        .unwrap();
        assert!(output.write_selector_pages(&[]).is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        output
            .selector_key_ordinal(HospitalPriceSelectorKey::Code("12345".to_owned()))
            .unwrap();
        fail_next_sink_write(&mut output.sinks[2]);
        assert!(output
            .write_selector_ref_chunks(0, &mut vec![0], &[1], &mut [0])
            .is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        let component = "x".repeat(
            crate::hospital_price_selector_block::HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_KEY_BYTES,
        );
        let key = HospitalPriceSelectorKey::PayerPlan {
            payer_name: component.clone(),
            plan_name: component,
        };
        let capacity = selector_ref_capacity(&key).unwrap();
        output.selector_key_ordinal(key).unwrap();
        let mut records = Vec::with_capacity(capacity * SELECTOR_SPOOL_RECORD_BYTES);
        for reference in 0..capacity as u64 {
            records.extend_from_slice(&selector_record(2, 0, reference));
        }
        fs::write(&output.selector_sorted_path, records).unwrap();
        output.write_selector_pages(&[1]).unwrap();

        struct InterruptedReader {
            interrupted: bool,
            cursor: std::io::Cursor<Vec<u8>>,
        }
        impl Read for InterruptedReader {
            fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
                if !self.interrupted {
                    self.interrupted = true;
                    return Err(io::Error::from(io::ErrorKind::Interrupted));
                }
                self.cursor.read(buffer)
            }
        }
        struct ErrorReader;
        impl Read for ErrorReader {
            fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
                Err(io::Error::other("read failed"))
            }
        }
        let mut reader = InterruptedReader {
            interrupted: false,
            cursor: std::io::Cursor::new(selector_record(1, 0, 0).to_vec()),
        };
        assert_eq!(
            read_selector_spool_record(&mut reader).unwrap(),
            Some((1, 0, 0))
        );
        assert!(read_selector_spool_record(&mut ErrorReader).is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        output
            .write_selector_ref(HospitalPriceSelectorKey::Code("12345".to_owned()), 0)
            .unwrap();
        fs::remove_dir(&output.selector_sort_directory).unwrap();
        assert!(output.finish_selector_pages().is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        output
            .write_selector_ref(HospitalPriceSelectorKey::Code("12345".to_owned()), 0)
            .unwrap();
        assert!(output.finish_selector_pages().is_err());
    }

    #[test]
    fn finish_rejects_every_counter_mismatch() {
        let directories = (0..6)
            .map(|_| tempfile::tempdir().unwrap())
            .collect::<Vec<_>>();

        let mut output = prepared_builder(directories[0].path());
        output.next_charge_key = 2;
        assert!(output.finish().is_err());

        let mut output = prepared_builder(directories[1].path());
        output.next_fact_ordinal = 2;
        assert!(output.finish().is_err());

        let mut output = prepared_builder(directories[2].path());
        output.next_fact_ordinal = 2;
        output.written_fact_count = 2;
        assert!(output.finish().is_err());

        let mut output = prepared_builder(directories[3].path());
        output.next_charge_key = 3;
        output.written_charge_count = 3;
        assert!(output.finish().is_err());

        let mut output = prepared_builder(directories[4].path());
        output.selector_block_counts = [1, 0];
        assert!(output.finish().is_err());

        let mut output = prepared_builder(directories[5].path());
        output.selector_spool_bytes = u64::MAX;
        assert!(output.finish().is_err());
    }

    #[test]
    fn scratch_drop_removes_owned_sorted_output() {
        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        fs::write(&output.selector_sorted_path, b"sorted").unwrap();
        output.selector_sorted_owned = true;
        let sorted_path = output.selector_sorted_path.clone();
        drop(output);
        assert!(!sorted_path.exists());
    }

    #[test]
    fn builder_creation_and_empty_segments_fail_cleanly() {
        for partial in [".fact_block.copy.partial", ".selector_page.copy.partial"] {
            let directory = tempfile::tempdir().unwrap();
            fs::write(directory.path().join(partial), b"occupied").unwrap();
            assert!(PackedOutputBuilder::create(
                directory.path(),
                "version-1",
                Arc::new(AtomicU64::new(0)),
                16 * 1024 * 1024,
            )
            .is_err());
        }
        let directory = tempfile::tempdir().unwrap();
        fs::write(
            directory.path().join(".selector_refs.partial"),
            b"occupied",
        )
        .unwrap();
        assert!(PackedOutputBuilder::create(
            directory.path(),
            "version-1",
            Arc::new(AtomicU64::new(0)),
            16 * 1024 * 1024,
        )
        .is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        output.finish_current_service_segment();
        output.service(0, &service()).unwrap();
        output.finish_current_service_segment();
        output.charge(0, 0, &charge("1")).unwrap();
        output.current_service = None;
        assert!(output.finish_current_charge().is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        fail_next_sink_write(&mut output.sinks[0]);
        assert!(output
            .write_service_rows(vec![packed_service_row(0, 1, "service".to_owned())])
            .is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        fail_next_sink_write(&mut output.sinks[1]);
        assert!(output.write_fact_rows(0, &[packed_fact(0)]).is_err());
    }

    #[test]
    fn remaining_io_edges_fail_closed() {
        use crate::hospital_price_selector_block::HospitalPriceSelectorKey;
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let locked = directory.path().join("locked");
        fs::create_dir(&locked).unwrap();
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o000)).unwrap();
        let locked_result = PackedSink::create(
            &locked,
            "test",
            "version-1",
            Arc::new(AtomicU64::new(0)),
            1024,
        );
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o700)).unwrap();
        assert!(locked_result.is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut sink = PackedSink::create(
            directory.path(),
            "test",
            "version-1",
            Arc::new(AtomicU64::new(0)),
            1024,
        )
        .unwrap();
        fail_next_sink_write(&mut sink);
        assert!(sink.finish().is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut sink = PackedSink::create(
            directory.path(),
            "test",
            "version-1",
            Arc::new(AtomicU64::new(0)),
            1024,
        )
        .unwrap();
        let writer = sink.writer.take().unwrap();
        let mut inner = writer.into_inner().ok().unwrap();
        inner.file = OpenOptions::new().write(true).open("/dev/null").unwrap();
        sink.writer = Some(BufWriter::with_capacity(1, inner));
        assert!(sink.finish().is_err());

        let directory = tempfile::tempdir().unwrap();
        assert!(PackedOutputBuilder::create(
            &directory.path().join("missing"),
            "version-1",
            Arc::new(AtomicU64::new(0)),
            1024,
        )
        .is_err());
        assert!(PackedOutputBuilder::create(
            directory.path(),
            " ",
            Arc::new(AtomicU64::new(0)),
            1024,
        )
        .is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        output.service(0, &service()).unwrap();
        output.charge(0, 0, &charge("1")).unwrap();
        output.current_service = None;
        assert!(output.service(1, &service()).is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        output.service(0, &service()).unwrap();
        assert!(output.service(1, &service()).is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        output.service(0, &service()).unwrap();
        output.charge(0, 0, &charge("1")).unwrap();
        output.current_charge.as_mut().unwrap().first_fact_ordinal = 1;
        assert!(output.charge(0, 1, &charge("1")).is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        output.service(0, &service()).unwrap();
        output.charge(0, 0, &charge("1")).unwrap();
        output.max_output_bytes = 38;
        assert!(output.payer(0, 0, &payer("1")).is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        output.service(0, &service()).unwrap();
        output.charge(0, 0, &charge("1")).unwrap();
        output.fact_rows = vec![
            packed_fact(0);
            crate::hospital_price_block::HOSPITAL_PRICE_FACT_BLOCK_MAX_ROWS - 1
        ];
        output.next_fact_ordinal = output.fact_rows.len() as u64;
        fail_next_sink_write(&mut output.sinks[1]);
        assert!(output.payer(0, 0, &payer("1")).is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        output.service(0, &service()).unwrap();
        output.charge(0, 0, &charge("1")).unwrap();
        output.service_charge_count =
            crate::hospital_price_service_block::HOSPITAL_PRICE_SERVICE_BLOCK_MAX_CHARGES - 1;
        fail_next_sink_write(&mut output.sinks[0]);
        assert!(output.charge(0, 1, &charge("1")).is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        assert!(output
            .write_fact_rows(1, &[packed_fact(140 * 1024), packed_fact(140 * 1024)])
            .is_err());

        for capacity in [0, 2, 6] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("read-only");
            fs::write(&path, b"x").unwrap();
            let mut output = builder(directory.path());
            output.selector_spool = Some(BufWriter::with_capacity(
                capacity,
                File::open(&path).unwrap(),
            ));
            assert!(output
                .write_selector_ref(HospitalPriceSelectorKey::Code("x".to_owned()), 0)
                .is_err());
        }

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("read-only");
        fs::write(&path, b"x").unwrap();
        let mut output = builder(directory.path());
        output.selector_spool = Some(BufWriter::with_capacity(16, File::open(&path).unwrap()));
        output
            .selector_spool
            .as_mut()
            .unwrap()
            .write_all(b"buffered")
            .unwrap();
        output.selector_spool_bytes = 8;
        assert!(output.finish_selector_pages().is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        output.selector_spool = Some(BufWriter::new(
            OpenOptions::new().write(true).open("/dev/null").unwrap(),
        ));
        output.selector_spool_bytes = 1;
        assert!(output.finish_selector_pages().is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        assert!(output.write_selector_pages(&[]).is_err());
        fs::write(&output.selector_sorted_path, [1, 0]).unwrap();
        assert!(output.write_selector_pages(&[]).is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        output
            .selector_key_ordinal(HospitalPriceSelectorKey::Code("x".to_owned()))
            .unwrap();
        fs::write(&output.selector_sorted_path, selector_record(1, 0, 0)).unwrap();
        assert!(output.write_selector_pages(&[]).is_err());

        let oversized = "x".repeat(
            crate::hospital_price_selector_block::HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_KEY_BYTES + 1,
        );
        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        assert!(output
            .selector_key_ordinal(HospitalPriceSelectorKey::Code(oversized.clone()))
            .is_err());
        assert!(selector_ref_capacity(&HospitalPriceSelectorKey::PayerPlan {
            payer_name: oversized.clone(),
            plan_name: "plan".to_owned(),
        })
        .is_err());
        assert!(selector_ref_capacity(&HospitalPriceSelectorKey::PayerPlan {
            payer_name: "payer".to_owned(),
            plan_name: oversized.clone(),
        })
        .is_err());
        output.selector_keys = vec![HospitalPriceSelectorKey::Code(oversized)];
        fs::write(&output.selector_sorted_path, selector_record(1, 0, 0)).unwrap();
        assert!(output.write_selector_pages(&[1]).is_err());

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("selector");
        assert!(count_selector_pages(&path, &[], 0, 0).is_err());
        let oversized = "x".repeat(
            crate::hospital_price_selector_block::HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_KEY_BYTES + 1,
        );
        fs::write(&path, selector_record(1, 0, 0)).unwrap();
        assert!(count_selector_pages(
            &path,
            &[HospitalPriceSelectorKey::Code(oversized)],
            1,
            0,
        )
        .is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = prepared_builder(directory.path());
        fail_next_sink_write(&mut output.sinks[2]);
        assert!(output.finish_selector_pages().is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = prepared_builder(directory.path());
        fs::write(output.selector_sort_directory.join("keep"), b"x").unwrap();
        assert!(output.finish_selector_pages().is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        output.service(0, &service()).unwrap();
        output.charge(0, 0, &charge("1")).unwrap();
        output.payer(0, 0, &payer("1")).unwrap();
        fail_next_sink_write(&mut output.sinks[0]);
        assert!(output.finish().is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = prepared_builder(directory.path());
        fail_next_sink_write(&mut output.sinks[1]);
        output.fact_rows.push(packed_fact(0));
        assert!(output.finish().is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = prepared_builder(directory.path());
        output
            .write_selector_ref(HospitalPriceSelectorKey::Code("12345".to_owned()), 99)
            .unwrap();
        assert!(output.finish().is_err());
    }
}
