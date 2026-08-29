    #[test]
    fn gross_cash_only_csv_rows_ignore_zero_count_payer_fields() {
        let tall = append_csv_row(
            &fixture_tall_csv(),
            &[
                ("description", "Cash-only tall service"),
                ("code | 1", "10001"),
                ("code | 1 | type", "CPT"),
                ("setting", "outpatient"),
                ("billing_class", "facility"),
                ("standard_charge | gross", "20"),
                ("count", "0"),
                ("standard_charge | methodology", "fee schedule"),
                ("additional_generic_notes", "No remittances during measurement period"),
            ],
        );
        let tall = append_csv_row(
            &tall,
            &[
                ("description", "Cash-only tall service without methodology"),
                ("code | 1", "10006"),
                ("code | 1 | type", "CPT"),
                ("setting", "outpatient"),
                ("billing_class", "facility"),
                ("standard_charge | gross", "21"),
                ("count", "0"),
                ("additional_generic_notes", "No remittances during measurement period"),
            ],
        );
        let tall_rows = run_fixture(InputFormat::TallCsv, &tall, false);
        let tall_charges = String::from_utf8(tall_rows["charge"].clone()).unwrap();
        let tall_charge_lines = tall_charges.lines().collect::<Vec<_>>();
        assert_eq!(tall_charge_lines.len(), 3);
        for line in &tall_charge_lines[1..] {
            assert_eq!(
                line.split('\t').nth(9),
                Some("No remittances during measurement period")
            );
        }
        assert_eq!(
            String::from_utf8(tall_rows["payer_charge"].clone())
                .unwrap()
                .lines()
                .count(),
            1
        );

        let wide = append_csv_row(
            &fixture_wide_csv(),
            &[
                ("description", "Cash-only wide service"),
                ("code|1", "10002"),
                ("code|1|type", "CPT"),
                ("setting", "outpatient"),
                ("billing_class", "facility"),
                ("standard_charge|gross", "30"),
                ("count|Payer, Inc.|Plan A", "0"),
                (
                    "additional_payer_notes|Payer, Inc.|Plan A",
                    "No remittances during measurement period",
                ),
            ],
        );
        let wide_rows = run_fixture(InputFormat::WideCsv, &wide, false);
        assert_eq!(String::from_utf8(wide_rows["charge"].clone()).unwrap().lines().count(), 2);
        assert_eq!(
            String::from_utf8(wide_rows["payer_charge"].clone())
                .unwrap()
                .lines()
                .count(),
            1
        );

        let invalid_tall = append_csv_row(
            &fixture_tall_csv(),
            &[
                ("description", "Missing tall note"),
                ("code | 1", "10003"),
                ("code | 1 | type", "CPT"),
                ("setting", "outpatient"),
                ("billing_class", "facility"),
                ("standard_charge | gross", "40"),
                ("count", "0"),
            ],
        );
        assert_import_error(
            InputFormat::TallCsv,
            &invalid_tall,
            DEFAULT_MAX_FANOUT_ROWS,
            "payer_name",
        );

        let invalid_tall_methodology = append_csv_row(
            &fixture_tall_csv(),
            &[
                ("description", "Invalid tall methodology"),
                ("code | 1", "10005"),
                ("code | 1 | type", "CPT"),
                ("setting", "outpatient"),
                ("billing_class", "facility"),
                ("standard_charge | gross", "40"),
                ("count", "0"),
                ("standard_charge | methodology", "unsupported"),
                ("additional_generic_notes", "No remittances during measurement period"),
            ],
        );
        assert_import_error(
            InputFormat::TallCsv,
            &invalid_tall_methodology,
            DEFAULT_MAX_FANOUT_ROWS,
            "invalid standard charge methodology",
        );

        for (methodology, notes, expected) in [
            (
                "fee schedule",
                "",
                "count 0 requires explanatory notes",
            ),
            (
                "unsupported",
                "No remittances during measurement period",
                "invalid standard charge methodology",
            ),
        ] {
            let invalid_wide = append_csv_row(
                &fixture_wide_csv(),
                &[
                    ("description", "Invalid cash-only wide service"),
                    ("code|1", "10004"),
                    ("code|1|type", "CPT"),
                    ("setting", "outpatient"),
                    ("billing_class", "facility"),
                    ("standard_charge|gross", "50"),
                    ("count|Payer, Inc.|Plan A", "0"),
                    (
                        "standard_charge|Payer, Inc.|Plan A|methodology",
                        methodology,
                    ),
                    ("additional_payer_notes|Payer, Inc.|Plan A", notes),
                ],
            );
            assert_import_error(
                InputFormat::WideCsv,
                &invalid_wide,
                DEFAULT_MAX_FANOUT_ROWS,
                expected,
            );
        }
    }

    #[test]
    fn wide_rows_omit_ancillary_only_payer_fields_with_or_without_notes() {
        let methodology_only = append_csv_row(
            &fixture_wide_csv(),
            &[
                ("description", "Methodology-only payer fields"),
                ("code|1", "10007"),
                ("code|1|type", "CPT"),
                ("setting", "outpatient"),
                ("billing_class", "facility"),
                ("standard_charge|gross", "50"),
                (
                    "standard_charge|Payer, Inc.|Plan A|methodology",
                    "fee schedule",
                ),
            ],
        );
        let methodology_rows = run_fixture(InputFormat::WideCsv, &methodology_only, false);
        assert_eq!(
            String::from_utf8(methodology_rows["payer_charge"].clone())
                .unwrap()
                .lines()
                .count(),
            1
        );

        let ancillary_without_notes = append_csv_row(
            &methodology_only,
            &[
                ("description", "Statistics-only payer fields"),
                ("code|1", "10008"),
                ("code|1|type", "CPT"),
                ("setting", "outpatient"),
                ("billing_class", "facility"),
                ("standard_charge|gross", "51"),
                ("median_amount|Payer, Inc.|Plan A", "45"),
                ("10th_percentile|Payer, Inc.|Plan A", "40"),
                ("90th_percentile|Payer, Inc.|Plan A", "49"),
                ("count|Payer, Inc.|Plan A", "1 through 10"),
            ],
        );
        let rows = run_fixture(InputFormat::WideCsv, &ancillary_without_notes, false);
        assert_eq!(
            String::from_utf8(rows["payer_charge"].clone())
                .unwrap()
                .lines()
                .count(),
            1
        );

        let ancillary_only = append_csv_row(
            &methodology_only,
            &[
                ("description", "Explained statistics-only payer fields"),
                ("code|1", "10008"),
                ("code|1|type", "CPT"),
                ("setting", "outpatient"),
                ("billing_class", "facility"),
                ("standard_charge|gross", "51"),
                ("median_amount|Payer, Inc.|Plan A", "45"),
                ("10th_percentile|Payer, Inc.|Plan A", "40"),
                ("90th_percentile|Payer, Inc.|Plan A", "49"),
                ("count|Payer, Inc.|Plan A", "1 through 10"),
                (
                    "additional_payer_notes|Payer, Inc.|Plan A",
                    "No negotiated charge reported",
                ),
            ],
        );

        let rows = run_fixture(InputFormat::WideCsv, &ancillary_only, false);
        assert_eq!(
            String::from_utf8(rows["charge"].clone())
                .unwrap()
                .lines()
                .count(),
            3
        );
        assert_eq!(
            String::from_utf8(rows["payer_charge"].clone())
                .unwrap()
                .lines()
                .count(),
            1
        );

        let (_directory, packed) = import_packed(
            InputFormat::WideCsv,
            &ancillary_only,
            TEST_MAX_OUTPUT_BYTES,
        );
        assert_eq!(packed.root.unwrap().fact_count, 1);
    }

    #[test]
    fn wide_ancillary_only_payer_fields_preserve_validation_boundaries() {
        let assert_wide_error = |code: &str, fields: &[(&str, &str)], expected: &str| {
            let mut values = vec![
                ("description", "Invalid ancillary-only payer fields"),
                ("code|1", code),
                ("code|1|type", "CPT"),
                ("setting", "outpatient"),
                ("billing_class", "facility"),
                ("standard_charge|gross", "50"),
            ];
            values.extend_from_slice(fields);
            assert_import_error(
                InputFormat::WideCsv,
                &append_csv_row(&fixture_wide_csv(), &values),
                DEFAULT_MAX_FANOUT_ROWS,
                expected,
            );
        };

        assert_wide_error(
            "10009",
            &[("median_amount|Payer, Inc.|Plan A", "not-a-number")],
            "median_amount must be an exact decimal number",
        );
        assert_wide_error(
            "10010",
            &[("count|Payer, Inc.|Plan A", "10")],
            "count values from 1 through 10 must use the literal 1 through 10",
        );
        assert_wide_error(
            "10011",
            &[
                (
                    "standard_charge|Payer, Inc.|Plan A|methodology",
                    "other",
                ),
            ],
            "methodology other requires explanatory notes",
        );
        assert_wide_error(
            "10012",
            &[
                (
                    "standard_charge|Payer, Inc.|Plan A|negotiated_percentage",
                    "25",
                ),
                (
                    "standard_charge|Payer, Inc.|Plan A|methodology",
                    "fee schedule",
                ),
            ],
            "percentage and algorithm charges require count",
        );

        let no_standard_charge = append_csv_row(
            &fixture_wide_csv(),
            &[
                ("description", "Ancillary fields without a standard charge"),
                ("code|1", "10013"),
                ("code|1|type", "CPT"),
                ("setting", "outpatient"),
                ("billing_class", "facility"),
                (
                    "standard_charge|Payer, Inc.|Plan A|methodology",
                    "fee schedule",
                ),
            ],
        );
        assert_import_error(
            InputFormat::WideCsv,
            &no_standard_charge,
            DEFAULT_MAX_FANOUT_ROWS,
            "standard charge requires gross, discounted cash, or payer information",
        );
    }
