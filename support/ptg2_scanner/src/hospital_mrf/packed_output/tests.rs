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
            estimated_amount: None,
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
            estimated_amount: None,
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

    pub(super) fn payloads(path: &Path) -> Vec<Vec<u8>> {
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
            (1, 1, 1, 1)
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
        assert_eq!(selector_rows.len(), 2);
        assert!(selector_rows.iter().all(|row| row[9].is_some()));
        assert!(selector_rows.iter().all(|row| row[10].is_some()));
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
        assert_eq!(code_pages.len(), 1);
        assert_eq!(code_pages[0].entries.len(), 2);
        assert!(code_pages[0]
            .entries
            .iter()
            .all(|entry| entry.refs == [0, 1]));
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

    include!("tests_middle.rs");
    include!("tests_selector_limits.rs");

    include!("tests_tail.rs");
    include!("tests_io.rs");
}
