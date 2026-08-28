    #[test]
    fn three_formats_and_gzip_emit_identical_copy_rows() {
        let json = fixture_json();
        let tall = fixture_tall_csv();
        let wide = fixture_wide_csv();
        let expected = run_fixture(InputFormat::Json, &json, false);
        assert_eq!(expected, run_fixture(InputFormat::TallCsv, &tall, false));
        assert_eq!(expected, run_fixture(InputFormat::WideCsv, &wide, false));
        assert_eq!(expected, run_fixture(InputFormat::Json, &json, true));
        assert_eq!(expected, run_fixture(InputFormat::TallCsv, &tall, true));
        assert_eq!(expected, run_fixture(InputFormat::WideCsv, &wide, true));
        assert!(tall
            .windows(b"\"MRI,\nbrain\"".len())
            .any(|window| window == b"\"MRI,\nbrain\""));
    }

    #[test]
    fn stored_and_deflated_zip_emit_identical_copy_rows() {
        let mut bom_json = b"\xEF\xBB\xBF".to_vec();
        bom_json.extend(fixture_json());
        for (format, payload) in [
            (InputFormat::Json, bom_json),
            (InputFormat::TallCsv, fixture_tall_csv()),
        ] {
            let expected = run_fixture(format, &payload, false);
            for method in [CompressionMethod::Stored, CompressionMethod::Deflated] {
                assert_eq!(run_zip_fixture(format, &payload, method), expected);
            }
        }
    }

    #[test]
    fn matching_appledouble_member_is_ignored() {
        let payload = fixture_json();
        for (method, reverse) in [
            (CompressionMethod::Stored, false),
            (CompressionMethod::Deflated, true),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let input_path = directory.path().join("input.zip");
            let mut entries = vec![
                ("prices.json", payload.as_slice()),
                ("__MACOSX/._prices.json", b"AppleDouble metadata".as_slice()),
            ];
            if reverse {
                entries.reverse();
            }
            fs::write(
                &input_path,
                zip_bytes(&entries, method),
            )
            .unwrap();
            let output_directory = directory.path().join("output");
            fs::create_dir(&output_directory).unwrap();

            let summary = import_hospital_mrf(
                InputFormat::Json,
                VERSION_ID,
                &input_path,
                &output_directory,
                TEST_MAX_OUTPUT_BYTES,
            )
            .unwrap();

            assert_eq!(summary.artifacts.len(), CopyKind::ALL.len());
        }
    }

    #[test]
    fn mixed_cp1252_text_matches_utf8_in_every_container() {
        let mut value: serde_json::Value = serde_json::from_slice(&fixture_json()).unwrap();
        value["standard_charge_information"][0]["description"] =
            json!("MRI – Women’s\u{00a0}care");
        let canonical = String::from_utf8(serde_json::to_vec(&value).unwrap()).unwrap();
        let mut mixed = Vec::with_capacity(canonical.len());
        for character in canonical.chars() {
            match character {
                '’' => mixed.push(0x92),
                '–' => mixed.push(0x96),
                '\u{00a0}' => mixed.push(0xa0),
                _ => {
                    let mut encoded = [0u8; 4];
                    mixed.extend_from_slice(character.encode_utf8(&mut encoded).as_bytes());
                }
            }
        }

        let expected = run_fixture(InputFormat::Json, canonical.as_bytes(), false);
        assert_eq!(run_fixture(InputFormat::Json, &mixed, false), expected);
        assert_eq!(run_fixture(InputFormat::Json, &mixed, true), expected);
        for method in [CompressionMethod::Stored, CompressionMethod::Deflated] {
            assert_eq!(run_zip_fixture(InputFormat::Json, &mixed, method), expected);
        }
    }

    #[test]
    fn aggregate_output_cap_accepts_exact_size_and_cleans_failed_outputs() {
        let payload = fixture_json();
        let expected = run_fixture(InputFormat::Json, &payload, false);
        let exact_bytes = expected
            .values()
            .map(|value| value.len() as u64)
            .sum::<u64>();
        let largest_artifact = expected
            .values()
            .map(|value| value.len() as u64)
            .max()
            .unwrap();
        assert!(exact_bytes - 1 > largest_artifact);

        let directory = tempfile::tempdir().unwrap();
        let input_path = directory.path().join("input.json");
        fs::write(&input_path, &payload).unwrap();
        let output_directory = directory.path().join("exact");
        fs::create_dir(&output_directory).unwrap();
        let summary = import_hospital_mrf(
            InputFormat::Json,
            VERSION_ID,
            &input_path,
            &output_directory,
            exact_bytes,
        )
        .unwrap();
        assert_eq!(summary.max_output_bytes, exact_bytes);
        assert_eq!(
            summary
                .artifacts
                .iter()
                .map(|artifact| artifact.bytes)
                .sum::<u64>(),
            exact_bytes
        );

        let failed_output_directory = directory.path().join("over-limit");
        fs::create_dir(&failed_output_directory).unwrap();
        let error = import_hospital_mrf(
            InputFormat::Json,
            VERSION_ID,
            &input_path,
            &failed_output_directory,
            exact_bytes - 1,
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("COPY output exceeds configured limit"));
        assert_eq!(fs::read_dir(failed_output_directory).unwrap().count(), 0);
    }

    #[test]
    fn aggregate_output_limit_is_reserved_before_writing() {
        let directory = tempfile::tempdir().unwrap();
        let aggregate_bytes = Arc::new(AtomicU64::new(0));
        let mut first = DigestWriter {
            file: File::create(directory.path().join("first.copy")).unwrap(),
            digest: Sha256::new(),
            bytes: 0,
            aggregate_bytes: Arc::clone(&aggregate_bytes),
            max_output_bytes: 5,
        };
        let mut second = DigestWriter {
            file: File::create(directory.path().join("second.copy")).unwrap(),
            digest: Sha256::new(),
            bytes: 0,
            aggregate_bytes: Arc::clone(&aggregate_bytes),
            max_output_bytes: 5,
        };

        first.write_all(b"abc").unwrap();
        assert!(second.write_all(b"def").is_err());
        assert_eq!(aggregate_bytes.load(Ordering::Relaxed), 3);
        assert_eq!(second.file.metadata().unwrap().len(), 0);
        second.write_all(b"de").unwrap();
        assert_eq!(aggregate_bytes.load(Ordering::Relaxed), 5);

        let mut closed = CopySink::create(
            directory.path(),
            CopyKind::Mrf,
            Arc::new(AtomicU64::new(0)),
            128,
        )
        .unwrap();
        closed.write_fields(&[Some("row")]).unwrap();
        closed.finish().unwrap();
        assert!(closed.write_fields(&[Some("second row")]).is_err());
        assert!(closed.finish().is_err());
    }

    #[test]
    fn cli_output_limit_must_be_a_positive_integer() {
        assert_eq!(parse_max_output_bytes("1").unwrap(), 1);
        for value in ["0", "-1", "not-a-number"] {
            let error = parse_max_output_bytes(value).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        }
        for (fanout, decompressed, output, expected) in [
            (0, 1, 1, "max fanout rows must be positive"),
            (1, 0, 1, "max decompressed bytes must be positive"),
            (1, 1, 0, "max output bytes must be positive"),
        ] {
            let error = import_hospital_mrf_with_limits(
                InputFormat::Json,
                VERSION_ID,
                Path::new("unused-input"),
                Path::new("unused-output"),
                fanout,
                decompressed,
                output,
            )
            .unwrap_err();
            assert!(error.to_string().contains(expected));
        }
    }

    #[test]
    fn tall_note_on_payer_row_is_canonicalized_as_payer_note() {
        let tall = fixture_tall_csv();
        let mut reader = ReaderBuilder::new()
            .has_headers(false)
            .from_reader(tall.as_slice());
        let records = reader.records().collect::<Result<Vec<_>, _>>().unwrap();
        let notes_index = records[2]
            .iter()
            .position(|header| header == "additional_generic_notes")
            .unwrap();
        let count_index = records[2]
            .iter()
            .position(|header| header == "count")
            .unwrap();
        let mut data_row = records[3].iter().map(str::to_owned).collect::<Vec<_>>();
        data_row[notes_index] = "Tall payer note".to_owned();
        data_row[count_index] = "1 THROUGH 10".to_owned();

        let mut writer = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(Vec::new());
        for record in &records[..3] {
            writer.write_record(record).unwrap();
        }
        writer.write_record(data_row).unwrap();
        let rows = run_fixture(InputFormat::TallCsv, &writer.into_inner().unwrap(), false);

        let charge = String::from_utf8(rows["charge"].clone()).unwrap();
        let charge_fields = charge.trim_end().split('\t').collect::<Vec<_>>();
        assert_eq!(charge_fields.len(), CHARGE_COPY_COLUMNS.len());
        assert_eq!(charge_fields[9], "\\N");

        let payer_charge = String::from_utf8(rows["payer_charge"].clone()).unwrap();
        let payer_fields = payer_charge.trim_end().split('\t').collect::<Vec<_>>();
        assert_eq!(payer_fields.len(), PAYER_CHARGE_COPY_COLUMNS.len());
        assert_eq!(payer_fields[12], "1 through 10");
        assert_eq!(payer_fields[14], "Tall payer note");
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
                ("additional_generic_notes", "No remittances during measurement period"),
            ],
        );
        let tall_rows = run_fixture(InputFormat::TallCsv, &tall, false);
        let tall_charges = String::from_utf8(tall_rows["charge"].clone()).unwrap();
        let tall_charge_lines = tall_charges.lines().collect::<Vec<_>>();
        assert_eq!(tall_charge_lines.len(), 2);
        assert_eq!(
            tall_charge_lines[1].split('\t').nth(9),
            Some("No remittances during measurement period")
        );
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

    #[test]
    fn csv_modifier_adjustments_and_notes_are_lossless() {
        let tall = append_csv_row(
            &fixture_tall_csv(),
            &[
                ("description", "Modifier dollar"),
                ("modifiers", "25"),
                ("setting", "OUTPATIENT"),
                ("payer_name", "Payer, Inc."),
                ("plan_name", "Plan A"),
                ("standard_charge | negotiated_dollar", "150.000"),
                ("additional_generic_notes", "Tall payer note"),
            ],
        );
        let tall = append_csv_row(
            &tall,
            &[
                ("description", "Generic modifier"),
                ("modifiers", "59"),
                ("setting", "BOTH"),
                ("additional_generic_notes", "Applies,\nwhen documented"),
            ],
        );
        let tall_rows = run_fixture(InputFormat::TallCsv, &tall, false);
        let modifiers = String::from_utf8(tall_rows["modifier"].clone()).unwrap();
        let modifier_lines = modifiers.lines().collect::<Vec<_>>();
        assert_eq!(modifier_lines.len(), 2);
        assert_eq!(modifier_lines[0].split('\t').next_back(), Some("\\N"));
        assert_eq!(modifier_lines[1].split('\t').next_back(), Some("\\N"));
        let tall_payers = String::from_utf8(tall_rows["modifier_payer"].clone()).unwrap();
        let tall_payer_fields = tall_payers
            .lines()
            .map(|line| line.split('\t').collect::<Vec<_>>())
            .collect::<Vec<_>>();
        assert_eq!(tall_payer_fields.len(), 2);
        assert_eq!(
            &tall_payer_fields[0][5..],
            &["Tall payer note", "150", "\\N", "\\N"]
        );
        assert_eq!(
            &tall_payer_fields[1][3..],
            &[
                "\\N",
                "\\N",
                "Applies,\\nwhen documented",
                "\\N",
                "\\N",
                "\\N"
            ]
        );

        let wide = append_csv_row(
            &fixture_wide_csv(),
            &[
                ("description", "Modifier percentage"),
                ("modifiers", "TC"),
                ("setting", "OUTPATIENT"),
                (
                    "standard_charge|Payer, Inc.|Plan A|negotiated_percentage",
                    "62.500",
                ),
            ],
        );
        let wide_rows = run_fixture(InputFormat::WideCsv, &wide, false);
        let wide_payers = String::from_utf8(wide_rows["modifier_payer"].clone()).unwrap();
        let wide_payer_fields = wide_payers.trim_end().split('\t').collect::<Vec<_>>();
        assert_eq!(wide_payer_fields.len(), MODIFIER_PAYER_COPY_COLUMNS.len());
        assert_eq!(&wide_payer_fields[5..], &["\\N", "\\N", "62.5", "\\N"]);

        let merged_wide = append_csv_row(
            &fixture_wide_csv(),
            &[
                ("description", "MRI,\nbrain"),
                ("code|1", "70551"),
                ("code|1|type", "CPT"),
                ("modifiers", "26|TC"),
                ("setting", "outpatient"),
                ("billing_class", "facility"),
                ("standard_charge|gross", "12.34"),
                ("standard_charge|discounted_cash", "10.5"),
                (
                    "standard_charge|Payer, Inc.|Plan A|negotiated_dollar",
                    "8.5",
                ),
                ("count|Payer, Inc.|Plan A", "1 THROUGH 10"),
                (
                    "standard_charge|Payer, Inc.|Plan A|methodology",
                    "fee schedule",
                ),
                ("standard_charge|min", "7"),
                ("standard_charge|max", "11"),
            ],
        );
        let merged_rows = run_fixture(InputFormat::WideCsv, &merged_wide, false);
        let merged_charge = String::from_utf8(merged_rows["charge"].clone()).unwrap();
        let merged_charge_fields = merged_charge.trim_end().split('\t').collect::<Vec<_>>();
        assert_eq!(merged_charge_fields[7], "7");
        assert_eq!(merged_charge_fields[8], "11");
        let merged_payers = String::from_utf8(merged_rows["payer_charge"].clone()).unwrap();
        let merged_payer_fields = merged_payers
            .lines()
            .map(|line| line.split('\t').collect::<Vec<_>>())
            .collect::<Vec<_>>();
        assert_eq!(merged_payer_fields.len(), 2);
        assert_eq!(merged_payer_fields[1][12], "1 through 10");

        let mut json_value: serde_json::Value = serde_json::from_slice(&fixture_json()).unwrap();
        json_value["standard_charge_information"][0]["standard_charges"][0]
            ["payers_information"][0]["count"] = json!("1 through 10");
        json_value["standard_charge_information"][0]["standard_charges"][0]
            ["payers_information"]
            .as_array_mut()
            .unwrap()
            .push(json!({
                "payer_name": "Payer, Inc.",
                "plan_name": "A Plan",
                "standard_charge_dollar": 9.5,
                "methodology": "fee schedule"
            }));
        json_value["modifier_information"] = json!([{
            "code": "25",
            "description": "Professional component",
            "setting": "outpatient",
            "modifier_payer_information": [{
                "payer_name": "Payer, Inc.",
                "plan_name": "Plan A",
                "description": "Contract note"
            }]
        }]);
        let json_rows = run_fixture(
            InputFormat::Json,
            &serde_json::to_vec(&json_value).unwrap(),
            false,
        );
        let payer_lines = String::from_utf8(json_rows["payer_charge"].clone()).unwrap();
        let payer_fields = payer_lines
            .lines()
            .map(|line| line.split('\t').collect::<Vec<_>>())
            .collect::<Vec<_>>();
        assert_eq!(payer_fields.len(), 2);
        assert_eq!(payer_fields[0][5], "A Plan");
        assert_eq!(payer_fields[1][5], "Plan A");
        assert_eq!(payer_fields[1][12], "1 through 10");
        assert_eq!(
            String::from_utf8(json_rows["modifier"].clone()).unwrap(),
            "fixture-version\t0\t25\tProfessional component\toutpatient\t\\N\n"
        );
        assert_eq!(
            String::from_utf8(json_rows["modifier_payer"].clone()).unwrap(),
            "fixture-version\t0\t0\tPayer, Inc.\tPlan A\tContract note\t\\N\t\\N\t\\N\n"
        );
    }

    #[test]
    fn tall_modifier_payer_identity_is_paired_optional() {
        let anonymous = append_csv_row(
            &fixture_tall_csv(),
            &[
                ("description", "Modifier adjustment"),
                ("modifiers", "25"),
                ("standard_charge | negotiated_dollar", "150"),
            ],
        );
        let rows = run_fixture(InputFormat::TallCsv, &anonymous, false);
        let payer = String::from_utf8(rows["modifier_payer"].clone()).unwrap();
        let fields = payer.trim_end().split('\t').collect::<Vec<_>>();
        assert_eq!(&fields[3..7], &["\\N", "\\N", "\\N", "150"]);

        let note_only = append_csv_row(
            &fixture_tall_csv(),
            &[
                ("description", "Modifier note"),
                ("modifiers", "59"),
                ("additional_generic_notes", "Explain adjustment"),
            ],
        );
        let rows = run_fixture(InputFormat::TallCsv, &note_only, false);
        let payer = String::from_utf8(rows["modifier_payer"].clone()).unwrap();
        let fields = payer.trim_end().split('\t').collect::<Vec<_>>();
        assert_eq!(&fields[3..7], &["\\N", "\\N", "Explain adjustment", "\\N"]);

        for (payer_name, plan_name, expected) in [
            ("Payer", "", "modifier payer evidence requires plan_name"),
            ("", "Plan", "modifier payer evidence requires payer_name"),
        ] {
            let unpaired = append_csv_row(
                &fixture_tall_csv(),
                &[
                    ("description", "Modifier adjustment"),
                    ("modifiers", "25"),
                    ("payer_name", payer_name),
                    ("plan_name", plan_name),
                    ("standard_charge | negotiated_dollar", "150"),
                ],
            );
            assert_import_error(
                InputFormat::TallCsv,
                &unpaired,
                DEFAULT_MAX_FANOUT_ROWS,
                expected,
            );
        }
    }

    #[test]
    fn exact_decimal_rules_reject_invalid_non_positive_values() {
        assert_eq!(positive_decimal("1.234e1", "amount").unwrap(), "12.34");
        assert_eq!(positive_decimal("00012.3400", "amount").unwrap(), "12.34");
        for invalid_value in ["12.3x", "NaN", "0", "0.000", "-1"] {
            assert!(positive_decimal(invalid_value, "amount").is_err());
        }

        let invalid_csv = String::from_utf8(fixture_tall_csv())
            .unwrap()
            .replacen("12.3400", "12.3x", 1);
        let directory = tempfile::tempdir().unwrap();
        let input_path = directory.path().join("invalid.csv");
        fs::write(&input_path, invalid_csv).unwrap();
        let output_directory = directory.path().join("output");
        fs::create_dir(&output_directory).unwrap();
        let error = import_hospital_mrf(
            InputFormat::TallCsv,
            VERSION_ID,
            &input_path,
            &output_directory,
            TEST_MAX_OUTPUT_BYTES,
        )
        .unwrap_err();
        assert!(error.to_string().contains("gross_charge"));
        assert_eq!(fs::read_dir(output_directory).unwrap().count(), 0);
    }
