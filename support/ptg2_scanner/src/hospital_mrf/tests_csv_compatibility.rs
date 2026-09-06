    #[test]
    fn csv_skips_wholly_blank_records_around_structural_rows() {
        let payload = fixture_tall_csv();
        let expected = run_fixture(InputFormat::TallCsv, &payload, false);
        let mut records = csv_fixture_records(&payload);
        let width = records[0].len();
        records.insert(1, vec![String::new(); width]);
        let mut whitespace = vec![String::new(); width];
        whitespace[0] = " ".to_owned();
        records.insert(3, whitespace);
        records.insert(5, vec![String::new(); width]);

        assert_eq!(
            expected,
            run_fixture(InputFormat::TallCsv, &csv_fixture_bytes(&records), false)
        );
    }

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
    fn gross_cash_only_tall_row_ignores_methodology_without_payer_evidence() {
        let csv = append_csv_row(
            &fixture_tall_csv(),
            &[
                ("description", "Gross and cash only service"),
                ("code | 1", "10010"),
                ("code | 1 | type", "CPT"),
                ("setting", "outpatient"),
                ("billing_class", "facility"),
                ("standard_charge | gross", "1288"),
                ("standard_charge | discounted_cash", "966"),
                (
                    "standard_charge | methodology",
                    "percent of total billed charges",
                ),
            ],
        );

        let rows = run_fixture(InputFormat::TallCsv, &csv, false);
        assert_eq!(
            String::from_utf8(rows["charge"].clone())
                .unwrap()
                .lines()
                .count(),
            2
        );
        assert_eq!(
            String::from_utf8(rows["payer_charge"].clone())
                .unwrap()
                .lines()
                .count(),
            1
        );
    }

    #[test]
    fn wide_rows_omit_anonymous_ancillary_only_payer_groups() {
        let anonymous = String::from_utf8(fixture_wide_csv())
            .unwrap()
            .replace("Payer, Inc.|Plan A", "|Anonymous Plan")
            .replacen("9.125", "", 1)
            .into_bytes();
        let statistics_only = append_csv_row(
            &anonymous,
            &[
                ("description", "Statistics without payer identity"),
                ("code|1", "10009"),
                ("code|1|type", "CPT"),
                ("setting", "outpatient"),
                ("billing_class", "facility"),
                ("standard_charge|gross", "51"),
                ("median_amount||Anonymous Plan", "45"),
                ("10th_percentile||Anonymous Plan", "40"),
                ("90th_percentile||Anonymous Plan", "49"),
                ("count||Anonymous Plan", "1 through 10"),
                (
                    "standard_charge||Anonymous Plan|methodology",
                    "fee schedule",
                ),
                (
                    "additional_payer_notes||Anonymous Plan",
                    "No negotiated charge reported",
                ),
            ],
        );

        let rows = run_fixture(InputFormat::WideCsv, &statistics_only, false);
        assert_eq!(
            String::from_utf8(rows["charge"].clone())
                .unwrap()
                .lines()
                .count(),
            2
        );
        assert!(rows["payer_charge"].is_empty());
    }

    #[test]
    fn wide_anonymous_payer_evidence_fails_closed() {
        let negotiated_charge = String::from_utf8(fixture_wide_csv())
            .unwrap()
            .replace("Payer, Inc.|Plan A", "|Anonymous Plan")
            .into_bytes();
        assert_import_error(
            InputFormat::WideCsv,
            &negotiated_charge,
            DEFAULT_MAX_FANOUT_ROWS,
            "payer_name must be a non-empty string",
        );

        let anonymous_plan = String::from_utf8(fixture_wide_csv())
            .unwrap()
            .replace("Payer, Inc.|Plan A", "Anonymous Payer|")
            .replacen("9.125", "", 1)
            .into_bytes();
        let modifier_evidence = append_csv_row(
            &anonymous_plan,
            &[
                ("description", "Modifier evidence without payer identity"),
                ("modifiers", "25"),
                ("setting", "outpatient"),
                (
                    "additional_payer_notes|Anonymous Payer|",
                    "Payer-specific modifier note",
                ),
            ],
        );
        assert_import_error(
            InputFormat::WideCsv,
            &modifier_evidence,
            DEFAULT_MAX_FANOUT_ROWS,
            "modifier plan_name must be a non-empty string",
        );
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

    #[test]
    fn v2_label_accepts_complete_v3_csv_while_preserving_declared_version() {
        for format in [InputFormat::TallCsv, InputFormat::WideCsv] {
            let payload = match format {
                InputFormat::TallCsv => fixture_tall_csv(),
                InputFormat::WideCsv => fixture_wide_csv(),
                InputFormat::Json => unreachable!(),
            };
            let mut records = csv_fixture_records(&payload);
            let version = csv_fixture_index(&records[0], "version");
            records[1][version] = "2.0.0".to_owned();

            let (rows, summary) = run_fixture_with_summary(
                format,
                &csv_fixture_bytes(&records),
                false,
            );
            assert_eq!(summary.schema_version, "2.0.0");
            let mrf = String::from_utf8(rows["mrf"].clone()).unwrap();
            let mrf = mrf.trim_end().split('\t').collect::<Vec<_>>();
            assert_eq!(mrf[3], "2.0.0");
            assert_eq!(mrf[4], ATTESTATION_TEXT);
        }
    }

    fn omit_csv_drug_headers(records: &mut [Vec<String>], omitted: &[&str]) {
        let mut indexes = omitted
            .iter()
            .map(|name| csv_fixture_index(&records[2], name))
            .collect::<Vec<_>>();
        indexes.sort_unstable_by(|left, right| right.cmp(left));
        for record in records.iter_mut().skip(2) {
            for &index in &indexes {
                record.remove(index);
                // Keep metadata untouched and the fixture writer's record width stable.
                record.push(String::new());
            }
        }
    }

    #[test]
    fn v2_optional_drug_headers_preserve_non_drug_artifacts() {
        for format in [InputFormat::TallCsv, InputFormat::WideCsv] {
            for version in ["2.0.0", "2.2.1"] {
                let mut records = csv_fixture_records(&fixture_v2_csv(format, version));
                let code = if format == InputFormat::TallCsv {
                    "code | 1"
                } else {
                    "code|1"
                };
                let code_index = csv_fixture_index(&records[2], code);
                records[3][code_index] = "00070551".to_owned();
                let original = csv_fixture_bytes(&records);
                let expected = run_fixture(format, &original, false);
                omit_csv_drug_headers(
                    &mut records,
                    &["drug_unit_of_measurement", "drug_type_of_measurement"],
                );
                let missing = csv_fixture_bytes(&records);
                assert_eq!(expected, run_fixture(format, &missing, false));
                let (_original_directory, original_summary) =
                    import_packed(format, &original, TEST_MAX_OUTPUT_BYTES);
                let (_missing_directory, missing_summary) =
                    import_packed(format, &missing, TEST_MAX_OUTPUT_BYTES);
                assert_eq!(
                    original_summary.schema_version,
                    missing_summary.schema_version
                );
                assert_eq!(
                    serde_json::to_value(&original_summary.root).unwrap(),
                    serde_json::to_value(&missing_summary.root).unwrap(),
                );
                assert_eq!(
                    original_summary
                        .artifacts
                        .iter()
                        .map(|artifact| (
                            artifact.kind,
                            artifact.rows,
                            artifact.bytes,
                            &artifact.sha256
                        ))
                        .collect::<Vec<_>>(),
                    missing_summary
                        .artifacts
                        .iter()
                        .map(|artifact| (
                            artifact.kind,
                            artifact.rows,
                            artifact.bytes,
                            &artifact.sha256
                        ))
                        .collect::<Vec<_>>(),
                );
            }
        }
    }

    #[test]
    fn optional_drug_headers_keep_one_missing_duplicates_and_v3_strict() {
        for format in [InputFormat::TallCsv, InputFormat::WideCsv] {
            let original = csv_fixture_records(&fixture_v2_csv(format, "2.0.0"));
            for header in ["drug_unit_of_measurement", "drug_type_of_measurement"] {
                let mut missing = original.clone();
                omit_csv_drug_headers(&mut missing, &[header]);
                assert_import_error(
                    format,
                    &csv_fixture_bytes(&missing),
                    DEFAULT_MAX_FANOUT_ROWS,
                    &format!("missing CSV header {header}"),
                );
                let mut duplicate = original.clone();
                for record in &mut duplicate {
                    record.push(String::new());
                }
                *duplicate[2].last_mut().unwrap() = header.to_owned();
                assert_import_error(
                    format,
                    &csv_fixture_bytes(&duplicate),
                    DEFAULT_MAX_FANOUT_ROWS,
                    &format!("duplicate CSV header {header}"),
                );
            }
            for declared_version in ["3.0.0", "2.0.0"] {
                let payload = if format == InputFormat::TallCsv {
                    fixture_tall_csv()
                } else {
                    fixture_wide_csv()
                };
                let mut modern = csv_fixture_records(&payload);
                let version = csv_fixture_index(&modern[0], "version");
                modern[1][version] = declared_version.to_owned();
                omit_csv_drug_headers(
                    &mut modern,
                    &["drug_unit_of_measurement", "drug_type_of_measurement"],
                );
                assert_import_error(
                    format,
                    &csv_fixture_bytes(&modern),
                    DEFAULT_MAX_FANOUT_ROWS,
                    "missing CSV header drug_unit_of_measurement",
                );
            }
        }
    }

    #[test]
    fn optional_drug_headers_preserve_present_measurement_validation() {
        for format in [InputFormat::TallCsv, InputFormat::WideCsv] {
            let mut records = csv_fixture_records(&fixture_v2_csv(format, "2.0.0"));
            let code = if format == InputFormat::TallCsv {
                "code | 1"
            } else {
                "code|1"
            };
            let code_index = csv_fixture_index(&records[2], code);
            let unit = csv_fixture_index(&records[2], "drug_unit_of_measurement");
            let kind = csv_fixture_index(&records[2], "drug_type_of_measurement");
            records[3][code_index] = "00001234567".to_owned();
            records[3][code_index + 1] = "ndc".to_owned();
            records[3][unit] = "1.00".to_owned();
            records[3][kind] = "ml".to_owned();
            let valid = run_fixture(format, &csv_fixture_bytes(&records), false);
            assert!(String::from_utf8(valid["code"].clone())
                .unwrap()
                .contains("\tNDC\t00001234567\n"));
            assert!(String::from_utf8(valid["service"].clone())
                .unwrap()
                .contains("\t1\tML\n"));
            for (unit_value, type_value, code_type, error) in [
                ("0", "ML", "NDC", "drug unit must be greater than zero"),
                (
                    "NA",
                    "ML",
                    "NDC",
                    "drug unit must be an exact decimal number",
                ),
                ("1", "NA", "NDC", "invalid drug type"),
                (
                    "",
                    "ML",
                    "CPT",
                    "drug unit and drug type must be supplied together",
                ),
                (
                    "1",
                    "",
                    "CPT",
                    "drug unit and drug type must be supplied together",
                ),
                (
                    "",
                    "",
                    "NDC",
                    "NDC services require drug unit and drug type",
                ),
            ] {
                records[3][unit] = unit_value.to_owned();
                records[3][kind] = type_value.to_owned();
                records[3][code_index + 1] = code_type.to_owned();
                assert_import_error(
                    format,
                    &csv_fixture_bytes(&records),
                    DEFAULT_MAX_FANOUT_ROWS,
                    error,
                );
            }
        }
    }

    #[test]
    fn optional_drug_headers_reject_late_second_code_ndc_and_clear_outputs() {
        for format in [InputFormat::TallCsv, InputFormat::WideCsv] {
            let mut records = csv_fixture_records(&fixture_v2_csv(format, "2.0.0"));
            omit_csv_drug_headers(
                &mut records,
                &["drug_unit_of_measurement", "drug_type_of_measurement"],
            );
            let second_code = records[2].len();
            for record in &mut records {
                record.extend([String::new(), String::new()]);
            }
            records[2][second_code] = "code|2".to_owned();
            records[2][second_code + 1] = "code|2|type".to_owned();
            let mut late = records[3].clone();
            late[0] = "Later drug service".to_owned();
            late[second_code] = "00001234567".to_owned();
            late[second_code + 1] = "ndc".to_owned();
            records.push(late);
            let payload = csv_fixture_bytes(&records);
            let expected = "NDC services require drug unit and drug type";
            assert_import_error(format, &payload, DEFAULT_MAX_FANOUT_ROWS, expected);
            let directory = tempfile::tempdir().unwrap();
            let input = directory.path().join("source.csv");
            let output = directory.path().join("output");
            fs::write(&input, &payload).unwrap();
            fs::create_dir(&output).unwrap();
            let error = import_hospital_mrf_with_output_mode(
                format,
                VERSION_ID,
                &input,
                &output,
                HospitalMrfLimits::new(
                    DEFAULT_MAX_FANOUT_ROWS,
                    TEST_MAX_DECOMPRESSED_BYTES,
                    TEST_MAX_OUTPUT_BYTES,
                ),
                HospitalMrfOutputMode::Packed,
            )
            .unwrap_err();
            assert!(error.to_string().contains(expected));
            assert_eq!(fs::read_dir(&output).unwrap().count(), 0);
        }
    }
