    #[test]
    fn repeated_csv_contract_provisions_preserve_nonblank_order() {
        let mut records = csv_fixture_records(&fixture_tall_csv());
        let first = csv_fixture_index(&records[0], "general_contract_provisions");
        records[1][first] = "First provision".to_owned();
        records[0][first + 1] = " GENERAL_CONTRACT_PROVISIONS ".to_owned();
        records[1][first + 1] = "  ".to_owned();
        records[0][first + 2] = "general_contract_provisions".to_owned();
        records[1][first + 2] = "Second provision".to_owned();

        let payload = csv_fixture_bytes(&records);
        let rows = run_fixture(InputFormat::TallCsv, &payload, false);
        assert_eq!(
            String::from_utf8(rows["contract_provision"].clone()).unwrap(),
            concat!(
                "fixture-version\t0\t\\N\t\\N\tFirst provision\n",
                "fixture-version\t1\t\\N\t\\N\tSecond provision\n",
            )
        );
        assert_import_error(
            InputFormat::TallCsv,
            &payload,
            1,
            "general_contract_provisions fanout exceeds configured limit 1",
        );
    }

    #[test]
    fn repeated_ordinary_general_csv_header_remains_invalid() {
        let mut records = csv_fixture_records(&fixture_tall_csv());
        let duplicate = csv_fixture_index(&records[0], "general_contract_provisions") + 1;
        records[0][duplicate] = " HOSPITAL_NAME ".to_owned();
        records[1][duplicate] = "Other Hospital".to_owned();

        assert_import_error(
            InputFormat::TallCsv,
            &csv_fixture_bytes(&records),
            DEFAULT_MAX_FANOUT_ROWS,
            "duplicate general CSV header hospital_name",
        );
    }

    #[test]
    fn wide_payer_placeholders_are_ignored_only_when_empty() {
        let mut records = csv_fixture_records(&fixture_wide_csv());
        for column in 0..records[2].len() {
            if records[2][column].contains("Payer, Inc.|Plan A") {
                records[2][column] = records[2][column]
                    .replace("Payer, Inc.|Plan A", "[PAYER_NAME]|[PLAN_NAME]");
                records[3][column].clear();
            }
        }
        let rows = run_fixture(
            InputFormat::WideCsv,
            &csv_fixture_bytes(&records),
            false,
        );
        assert!(rows["payer_charge"].is_empty());
        let blank_records = records.clone();

        let negotiated_dollar = csv_fixture_index(
            &records[2],
            "standard_charge|[PAYER_NAME]|[PLAN_NAME]|negotiated_dollar",
        );
        records[3][negotiated_dollar] = "1".to_owned();
        assert_import_error(
            InputFormat::WideCsv,
            &csv_fixture_bytes(&records),
            DEFAULT_MAX_FANOUT_ROWS,
            "wide CSV payer headers must replace payer and plan placeholders",
        );

        let mut modifier_records = blank_records;
        let mut modifier = vec![String::new(); modifier_records[2].len()];
        for (header, value) in [
            ("description", "Modifier percentage"),
            ("modifiers", "TC"),
            ("setting", "outpatient"),
            ("additional_generic_notes", "Generic modifier note"),
        ] {
            modifier[csv_fixture_index(&modifier_records[2], header)] = value.to_owned();
        }
        modifier_records.push(modifier.clone());
        let rows = run_fixture(
            InputFormat::WideCsv,
            &csv_fixture_bytes(&modifier_records),
            false,
        );
        assert!(!rows["modifier"].is_empty());
        assert!(rows["modifier_payer"].is_empty());

        let placeholder_percentage = csv_fixture_index(
            &modifier_records[2],
            "standard_charge|[PAYER_NAME]|[PLAN_NAME]|negotiated_percentage",
        );
        modifier[placeholder_percentage] = "50".to_owned();
        *modifier_records.last_mut().unwrap() = modifier;
        assert_import_error(
            InputFormat::WideCsv,
            &csv_fixture_bytes(&modifier_records),
            DEFAULT_MAX_FANOUT_ROWS,
            "wide CSV payer headers must replace payer and plan placeholders",
        );
    }
    #[test]
    fn csv_scans_a_bounded_metadata_preamble() {
        let payload = fixture_wide_csv();
        let expected = run_fixture(InputFormat::WideCsv, &payload, false);
        let mut records = csv_fixture_records(&payload);
        let mut note = vec![String::new(); records[0].len()];
        note[0] = "***** END NOTES".to_owned();
        records.insert(0, note.clone());
        assert_eq!(
            expected,
            run_fixture(InputFormat::WideCsv, &csv_fixture_bytes(&records), false)
        );

        for _ in 1..CSV_METADATA_HEADER_SCAN_MAX_RECORDS {
            records.insert(0, note.clone());
        }
        assert_import_error(
            InputFormat::WideCsv,
            &csv_fixture_bytes(&records),
            DEFAULT_MAX_FANOUT_ROWS,
            "metadata header exceeds its scan limit",
        );
    }
