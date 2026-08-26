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
        output
            .write_selector_ref_chunks(0, &mut refs, &[], &mut [])
            .unwrap();
        refs.push(0);
        assert!(output
            .write_selector_ref_chunks(0, &mut refs, &[], &mut [])
            .is_err());

        let mut output = builder(directories[4].path());
        output
            .selector_key_ordinal(selector_code("12345"))
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
