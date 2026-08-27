    fn queue_selector_test_chunk(
        output: &mut PackedOutputBuilder,
        ordinal: u32,
        refs: &mut Vec<u64>,
        page_counts: &[u32],
        page_indexes: &mut [u32],
    ) -> io::Result<()> {
        let mut pack = SelectorPackState::default();
        output.queue_selector_ref_chunk(
            ordinal,
            refs,
            page_counts,
            page_indexes,
            &mut pack,
        )?;
        output.flush_selector_pack(&mut pack, page_indexes)
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
        let key = selector_code("12345");
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
                .selector_key_ordinal(selector_code("12345"))
                .unwrap();
            fs::write(&output.selector_sorted_path, record).unwrap();
            assert!(output.write_selector_pages(&[1]).is_err());
        }

        let mut output = builder(directories[3].path());
        let mut refs = Vec::new();
        queue_selector_test_chunk(&mut output, 0, &mut refs, &[], &mut []).unwrap();
        refs.push(0);
        assert!(queue_selector_test_chunk(&mut output, 0, &mut refs, &[], &mut []).is_err());

        let mut output = builder(directories[4].path());
        output
            .selector_key_ordinal(selector_code("12345"))
            .unwrap();
        let mut refs = vec![0];
        assert!(queue_selector_test_chunk(&mut output, 0, &mut refs, &[], &mut [0]).is_err());
        assert!(queue_selector_test_chunk(&mut output, 0, &mut refs, &[1], &mut []).is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        output
            .selector_key_ordinal(selector_code("first"))
            .unwrap();
        output
            .selector_key_ordinal(selector_code("second"))
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
            .selector_key_ordinal(selector_code("12345"))
            .unwrap();
        fail_next_sink_write(&mut output.sinks[2]);
        assert!(queue_selector_test_chunk(
            &mut output,
            0,
            &mut vec![0],
            &[1],
            &mut [0],
        )
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
            .write_selector_ref(selector_code("12345"), 0)
            .unwrap();
        fs::remove_dir(&output.selector_sort_directory).unwrap();
        assert!(output.finish_selector_pages().is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        output
            .write_selector_ref(selector_code("12345"), 0)
            .unwrap();
        assert!(output.finish_selector_pages().is_err());
    }

    #[test]
    fn selector_remap_rejects_corrupt_records_and_lengths() {
        for (record, expected_error) in [
            (selector_record(1, 9, 0).to_vec(), "key ordinal is invalid"),
            (selector_record(2, 0, 0).to_vec(), "kind does not match"),
            (vec![1], "partial record"),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let mut output = builder(directory.path());
            output
                .selector_key_ordinal(selector_code("12345"))
                .unwrap();
            drop(output.selector_spool.take());
            output.selector_spool_bytes = record.len() as u64;
            fs::write(&output.selector_spool_path, record).unwrap();
            assert!(output
                .reorder_selector_spool_by_digest()
                .unwrap_err()
                .to_string()
                .contains(expected_error));
        }
    }

    #[test]
    fn selector_pack_validation_rejects_corrupt_internal_state() {
        use crate::hospital_price_selector_block::{
            selector_key_sha256, HospitalPriceSelectorEntry, HospitalPriceSelectorKey,
        };

        let entry = |key, reference| HospitalPriceSelectorEntry {
            key,
            refs: vec![reference],
        };

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        let mut pack = SelectorPackState {
            first_ordinal: None,
            raw_bytes: 1,
            entries: vec![entry(selector_code("missing"), 0)],
        };
        assert!(output.flush_selector_pack(&mut pack, &mut [0]).is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        let mut pack = SelectorPackState {
            first_ordinal: Some(1),
            raw_bytes: 1,
            entries: vec![entry(selector_code("short"), 0)],
        };
        assert!(output.flush_selector_pack(&mut pack, &mut [0]).is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        assert!(output.write_selector_pack(0, 0, 1, &[]).is_err());

        let payer = HospitalPriceSelectorKey::PayerPlan {
            payer_name: "payer".to_owned(),
            plan_name: "plan".to_owned(),
        };
        assert!(output
            .write_selector_pack(
                0,
                0,
                1,
                &[entry(selector_code("mixed"), 0), entry(payer, 1)],
            )
            .is_err());

        let mut reversed = [
            entry(selector_code("lower"), 0),
            entry(selector_code("upper"), 1),
        ];
        reversed.sort_by_key(|item| selector_key_sha256(&item.key));
        reversed.reverse();
        assert!(output
            .write_selector_pack(0, 0, 1, &reversed)
            .is_err());

        let duplicate = entry(selector_code("duplicate"), 0);
        assert!(output
            .write_selector_pack(0, 0, 1, &[duplicate.clone(), duplicate])
            .is_err());

        let empty_refs = HospitalPriceSelectorEntry {
            key: selector_code("empty"),
            refs: Vec::new(),
        };
        assert!(output
            .write_selector_pack(0, 0, 1, &[empty_refs])
            .is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        output.selector_keys = vec![selector_code("overflow")];
        let mut refs = vec![0];
        let mut pack = SelectorPackState {
            first_ordinal: None,
            raw_bytes: usize::MAX,
            entries: Vec::new(),
        };
        assert!(output
            .queue_selector_ref_chunk(0, &mut refs, &[1], &mut [0], &mut pack)
            .is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        output
            .selector_key_ordinal(selector_code("final-flush"))
            .unwrap();
        fs::write(&output.selector_sorted_path, selector_record(1, 0, 0)).unwrap();
        fail_next_sink_write(&mut output.sinks[2]);
        assert!(output.write_selector_pages(&[1]).is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        output.selector_keys = vec![selector_code("multi-page")];
        fail_next_sink_write(&mut output.sinks[2]);
        assert!(output
            .queue_selector_ref_chunk(
                0,
                &mut vec![0],
                &[2],
                &mut [0],
                &mut SelectorPackState::default(),
            )
            .is_err());

    }
