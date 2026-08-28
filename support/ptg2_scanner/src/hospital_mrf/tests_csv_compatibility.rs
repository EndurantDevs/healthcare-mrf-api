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
                "payer information requires dollar, percentage, or algorithm charge",
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
