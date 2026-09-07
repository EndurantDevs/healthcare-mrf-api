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
include!("tests_metadata_address.rs");

    fn csv_redundant_address_records(payload: &[u8]) -> Vec<Vec<String>> {
        let mut records = csv_fixture_records(payload);
        let spare = records[0].iter().position(String::is_empty).unwrap();
        let address = csv_fixture_index(&records[0], "hospital_address");
        records[0][spare] = "hospital_location".to_owned();
        records[1][spare] = records[1][address].clone();
        records
    }

    fn csv_redundant_name_records(payload: &[u8]) -> Vec<Vec<String>> {
        let mut records = csv_redundant_address_records(payload);
        let legacy = csv_fixture_index(&records[0], "hospital_location");
        let location = csv_fixture_index(&records[0], "location_name");
        records[1][legacy] = records[1][location].clone();
        records
    }

    #[test]
    fn csv_redundant_name_detects_v3_and_preserves_declared_metadata() {
        for (format, fixture) in [
            (InputFormat::TallCsv, fixture_tall_csv()),
            (InputFormat::WideCsv, fixture_wide_csv()),
        ] {
            for version in ["1", "1.0.0", "2", "2.0.0", "2.2.0", "2.2.1", "3.0.0", "3.0.1", "4.0.0"] {
                let mut records = csv_fixture_records(&fixture);
                for (field, value) in [
                    ("version", version),
                    (ATTESTATION_TEXT, if version == "2.0.0" { "false" } else { "true" }),
                    ("type_2_npi", "1234567890, 1111111111"),
                ] {
                    let index = csv_fixture_index(&records[0], field);
                    records[1][index] = value.to_owned();
                }
                let canonical = csv_fixture_bytes(&records);
                let redundant = csv_fixture_bytes(&csv_redundant_name_records(&canonical));
                let expected = run_fixture(format, &canonical, false);
                let (actual, summary) = run_fixture_with_summary(format, &redundant, false);
                assert_eq!(actual, expected);
                assert_eq!(summary.schema_version, version);
                let mrf = std::str::from_utf8(&actual["mrf"]).unwrap();
                let fields = mrf.trim_end().split('\t').collect::<Vec<_>>();
                assert_eq!(fields[3], version);
                assert_eq!(fields[4], ATTESTATION_TEXT);
                assert_eq!(fields[5], if version == "2.0.0" { "false" } else { "true" });
                assert!(std::str::from_utf8(&actual["npi"]).unwrap()
                    .contains("\t0\t1234567890, 1111111111\n"));
                if version == "2.0.0" {
                    assert_eq!(run_fixture(format, &redundant, true), expected);
                    assert_eq!(run_zip_fixture(format, &redundant, CompressionMethod::Deflated), expected);
                    let (_canonical_dir, canonical_summary) = import_packed(format, &canonical, TEST_MAX_OUTPUT_BYTES);
                    let (_redundant_dir, redundant_summary) = import_packed(format, &redundant, TEST_MAX_OUTPUT_BYTES);
                    assert_eq!(redundant_summary.schema_version, version);
                    assert_eq!(
                        canonical_summary.artifacts.iter().map(|a| (a.kind, a.rows, &a.sha256)).collect::<Vec<_>>(),
                        redundant_summary.artifacts.iter().map(|a| (a.kind, a.rows, &a.sha256)).collect::<Vec<_>>()
                    );
                }
            }
        }
    }

    #[test]
    fn csv_redundant_name_rejects_ambiguity_and_keeps_v3_validation() {
        for (format, fixture) in [
            (InputFormat::TallCsv, fixture_tall_csv()),
            (InputFormat::WideCsv, fixture_wide_csv()),
        ] {
            let mut fixture = csv_redundant_name_records(&fixture);
            let version = csv_fixture_index(&fixture[0], "version");
            fixture[1][version] = "2.0.0".to_owned();
            for (legacy, location, address) in [
                ("Other", "Main", "1 Main St"), (" Main", "Main", "1 Main St"),
                ("Main", "Main ", "1 Main St"), ("main", "Main", "1 Main St"),
                ("", "", "1 Main St"), (" ", " ", "1 Main St"),
                ("Main|Second", "Main|Second", "1 Main St"),
                ("Main", "Main", ""), ("Main", "Main", " "),
                ("Main", "Main", "1 Main St|2 North St"),
            ] {
                let mut records = fixture.clone();
                for (field, value) in [("hospital_location", legacy), ("location_name", location), ("hospital_address", address)] {
                    let index = csv_fixture_index(&records[0], field);
                    records[1][index] = value.to_owned();
                }
                assert_import_error(format, &csv_fixture_bytes(&records), DEFAULT_MAX_FANOUT_ROWS,
                    "headers mix V2 and V3 profiles");
            }
            for (field, value, error) in [
                ("type_2_npi", "", "type_2_npi"), ("attester_name", "", "attester_name"),
                (ATTESTATION_TEXT, "1", "attestation value must be true or false"),
                ("version", "5.0.0", "unsupported CMS CSV version"),
            ] {
                let mut records = fixture.clone();
                let index = csv_fixture_index(&records[0], field);
                records[1][index] = value.to_owned();
                assert_import_error(format, &csv_fixture_bytes(&records), DEFAULT_MAX_FANOUT_ROWS, error);
            }
            for field in ["hospital_location", AFFIRMATION_TEXT] {
                let mut records = fixture.clone();
                let spare = records[0].iter().position(String::is_empty).unwrap();
                records[0][spare] = field.to_owned();
                assert_import_error(format, &csv_fixture_bytes(&records), DEFAULT_MAX_FANOUT_ROWS,
                    if field == "hospital_location" { "duplicate general CSV header" } else { "headers mix V2 and V3 profiles" });
            }
            let mut missing_attestation = fixture.clone();
            let index = csv_fixture_index(&missing_attestation[0], ATTESTATION_TEXT);
            missing_attestation[0][index].clear();
            assert_import_error(format, &csv_fixture_bytes(&missing_attestation), DEFAULT_MAX_FANOUT_ROWS,
                "headers mix V2 and V3 profiles");
            for replacement in ["", "estimated_amount"] {
                let mut records = fixture.clone();
                let index = records[2].iter().position(|h| h.starts_with("median_amount")).unwrap();
                records[2][index] = if replacement.is_empty() { String::new() } else {
                    records[2][index].replacen("median_amount", replacement, 1)
                };
                assert_import_error(format, &csv_fixture_bytes(&records), DEFAULT_MAX_FANOUT_ROWS,
                    if replacement.is_empty() { "median_amount" } else { "mix V2 and V3 payer profiles" });
            }
            let mut excess_npis = fixture.clone();
            let index = csv_fixture_index(&excess_npis[0], "type_2_npi");
            excess_npis[1][index] = "1234567890|1111111111".to_owned();
            assert_import_error(format, &csv_fixture_bytes(&excess_npis), 1, "fanout exceeds configured limit");
        }
    }

    #[test]
    fn csv_redundant_address_preserves_copy_packed_and_container_semantics() {
        for (format, fixture) in [
            (InputFormat::TallCsv, fixture_tall_csv()),
            (InputFormat::WideCsv, fixture_wide_csv()),
        ] {
            for confirmation in ["true", "false"] {
                let mut records = csv_fixture_records(&fixture);
                let index = csv_fixture_index(&records[0], ATTESTATION_TEXT);
                records[1][index] = confirmation.to_owned();
                let canonical = csv_fixture_bytes(&records);
                let redundant = csv_fixture_bytes(&csv_redundant_address_records(&canonical));
                let expected = run_fixture(format, &canonical, false);
                assert_eq!(run_fixture(format, &redundant, false), expected);
                assert_eq!(run_fixture(format, &redundant, true), expected);
                assert_eq!(
                    run_zip_fixture(format, &redundant, CompressionMethod::Deflated),
                    expected
                );
                let (_canonical_dir, canonical_summary) =
                    import_packed(format, &canonical, TEST_MAX_OUTPUT_BYTES);
                let (_redundant_dir, redundant_summary) =
                    import_packed(format, &redundant, TEST_MAX_OUTPUT_BYTES);
                assert_eq!(redundant_summary.schema_version, "3.0.0");
                assert_eq!(
                    canonical_summary.artifacts.iter()
                        .map(|artifact| (artifact.kind, artifact.rows, &artifact.sha256))
                        .collect::<Vec<_>>(),
                    redundant_summary.artifacts.iter()
                        .map(|artifact| (artifact.kind, artifact.rows, &artifact.sha256))
                        .collect::<Vec<_>>()
                );
                let root = redundant_summary.root.unwrap();
                assert_eq!((root.service_count, root.charge_count, root.fact_count), (1, 1, 1));
            }
        }
    }

    #[test]
    fn csv_redundant_address_rejects_ambiguous_metadata() {
        let fixture = csv_redundant_address_records(&fixture_tall_csv());
        for (legacy, address, location, version) in [
            ("Other address", "1 Main St", "Main", "3.0.0"),
            (" 1 Main St", "1 Main St", "Main", "3.0.0"),
            ("1 Main St", "1 Main St ", "Main", "3.0.0"),
            ("", "", "Main", "3.0.0"),
            (" ", " ", "Main", "3.0.0"),
            ("1 Main St", "1 Main St", "", "3.0.0"),
            ("1 Main St", "1 Main St", " ", "3.0.0"),
            ("1 Main St|", "1 Main St|", "Main", "3.0.0"),
            ("1 Main St|2 Main St", "1 Main St|2 Main St", "Main", "3.0.0"),
            ("1 Main St", "1 Main St", "Main|", "3.0.0"),
            ("1 Main St", "1 Main St", "Main|Second", "3.0.0"),
            ("1 Main St", "1 Main St", "Main", "2.0.0"),
            ("1 Main St", "1 Main St", "Main", "3.0.1"),
            ("1 Main St", "1 Main St", "Main", "4.0.0"),
        ] {
            let mut records = fixture.clone();
            for (field, value) in [
                ("hospital_location", legacy), ("hospital_address", address),
                ("location_name", location), ("version", version),
            ] {
                let index = csv_fixture_index(&records[0], field);
                records[1][index] = value.to_owned();
            }
            assert_import_error(InputFormat::TallCsv, &csv_fixture_bytes(&records),
                DEFAULT_MAX_FANOUT_ROWS, "headers mix V2 and V3 profiles");
        }
        for field in ["hospital_address", "location_name"] {
            let mut records = fixture.clone();
            let index = csv_fixture_index(&records[0], field);
            records[0][index].clear();
            assert_import_error(InputFormat::TallCsv, &csv_fixture_bytes(&records),
                DEFAULT_MAX_FANOUT_ROWS, "headers mix V2 and V3 profiles");
        }
        for (header, expected) in [
            ("hospital_location", "duplicate general CSV header hospital_location"),
            (AFFIRMATION_TEXT, "headers mix V2 and V3 profiles at affirmation"),
        ] {
            let mut records = fixture.clone();
            let spare = records[0].iter().position(String::is_empty).unwrap();
            records[0][spare] = header.to_owned();
            records[1][spare] = "true".to_owned();
            assert_import_error(InputFormat::TallCsv, &csv_fixture_bytes(&records),
                DEFAULT_MAX_FANOUT_ROWS, expected);
        }
    }

    #[test]
    fn csv_redundant_address_keeps_v3_validation_and_limits() {
        let fixture = csv_redundant_address_records(&fixture_tall_csv());
        for (field, value, expected) in [
            ("type_2_npi", "", "type_2_npi"),
            ("attester_name", "", "attester_name"),
            (ATTESTATION_TEXT, "1", "attestation value must be true or false"),
            (ATTESTATION_TEXT, "", "attestation value must be true or false"),
        ] {
            let mut records = fixture.clone();
            let index = csv_fixture_index(&records[0], field);
            records[1][index] = value.to_owned();
            assert_import_error(InputFormat::TallCsv, &csv_fixture_bytes(&records),
                DEFAULT_MAX_FANOUT_ROWS, expected);
        }
        let mut missing_attestation = fixture.clone();
        let index = csv_fixture_index(&missing_attestation[0], ATTESTATION_TEXT);
        missing_attestation[0][index].clear();
        assert_import_error(InputFormat::TallCsv, &csv_fixture_bytes(&missing_attestation),
            DEFAULT_MAX_FANOUT_ROWS, "missing attestation header");
        let mut negative_rate = fixture.clone();
        let index = csv_fixture_index(&negative_rate[2], "standard_charge | negotiated_dollar");
        negative_rate[3][index] = "-1".to_owned();
        assert_import_error(InputFormat::TallCsv, &csv_fixture_bytes(&negative_rate),
            DEFAULT_MAX_FANOUT_ROWS, "must be greater than zero");
        let mut mixed_profile = fixture.clone();
        let index = csv_fixture_index(&mixed_profile[2], "median_amount");
        mixed_profile[2][index] = "estimated_amount".to_owned();
        assert_import_error(InputFormat::TallCsv, &csv_fixture_bytes(&mixed_profile),
            DEFAULT_MAX_FANOUT_ROWS, "mix V2 and V3 payer profiles");
        let mut excess_npis = fixture.clone();
        let index = csv_fixture_index(&excess_npis[0], "type_2_npi");
        excess_npis[1][index] = "1234567890|1111111111".to_owned();
        assert_import_error(InputFormat::TallCsv, &csv_fixture_bytes(&excess_npis), 1,
            "fanout exceeds configured limit");
        let payload = csv_fixture_bytes(&fixture);
        assert_import_error(InputFormat::TallCsv, &payload, 0, "max fanout rows must be positive");
        assert_payload_limit_error(InputFormat::TallCsv, &payload, 128,
            "CSV record exceeds configured limit");
    }

    fn unquote_fixture_hospital_name(payload: &[u8]) -> Vec<u8> {
        let payload = std::str::from_utf8(payload).unwrap();
        assert!(payload.contains("\"North, Hospital\""));
        payload
            .replacen("\"North, Hospital\"", "North, Hospital", 1)
            .into_bytes()
    }

    #[test]
    fn csv_name_alignment_preserves_quoted_semantics_and_literal_confirmation() {
        for format in [InputFormat::TallCsv, InputFormat::WideCsv] {
            let fixture = match format {
                InputFormat::TallCsv => fixture_tall_csv(),
                InputFormat::WideCsv => fixture_wide_csv(),
                InputFormat::Json => unreachable!(),
            };
            for (version, date, canonical_date) in [
                ("2.0.0", "2026-02-19", "2026-02-19"),
                ("3.0.0", "2026-02-19", "2026-02-19"),
                ("3.0.1", "2024-02-29", "2024-02-29"),
                ("4.0.0", "2/19/2026", "2026-02-19"),
            ] {
                let version_fixture = if version == "2.0.0" {
                    fixture_v2_csv(format, version)
                } else {
                    fixture.clone()
                };
                for confirmation in ["true", "false"] {
                    let mut records = csv_fixture_records(&version_fixture);
                    records[1][1] = date.to_owned();
                    records[1][2] = version.to_owned();
                    records[1][7] = confirmation.to_owned();
                    let quoted = csv_fixture_bytes(&records);
                    let unquoted = unquote_fixture_hospital_name(&quoted);
                    let expected = run_fixture(format, &quoted, false);
                    let actual = run_fixture(format, &unquoted, false);
                    assert_eq!(actual, expected);
                    let mrf = std::str::from_utf8(&actual["mrf"]).unwrap();
                    let fields = mrf.trim_end().split('\t').collect::<Vec<_>>();
                    assert_eq!(fields[1], "North, Hospital");
                    assert_eq!(fields[2], canonical_date);
                    assert_eq!(fields[3], version);
                    assert_eq!(
                        fields[4],
                        if version == "2.0.0" {
                            AFFIRMATION_TEXT
                        } else {
                            ATTESTATION_TEXT
                        }
                    );
                    assert_eq!(fields[5], confirmation);
                    assert_eq!(
                        fields[6],
                        if version == "2.0.0" { "\\N" } else { "Alex Attester" }
                    );

                    let (_quoted_dir, quoted_summary) =
                        import_packed(format, &quoted, TEST_MAX_OUTPUT_BYTES);
                    let (_unquoted_dir, unquoted_summary) =
                        import_packed(format, &unquoted, TEST_MAX_OUTPUT_BYTES);
                    assert_eq!(
                        quoted_summary.artifacts.iter()
                            .map(|artifact| (artifact.kind, artifact.rows, &artifact.sha256))
                            .collect::<Vec<_>>(),
                        unquoted_summary.artifacts.iter()
                            .map(|artifact| (artifact.kind, artifact.rows, &artifact.sha256))
                            .collect::<Vec<_>>()
                    );
                    let root = unquoted_summary.root.unwrap();
                    assert_eq!(
                        (root.service_count, root.charge_count, root.fact_count),
                        (1, 1, 1)
                    );
                }
            }
            let unquoted = unquote_fixture_hospital_name(&fixture);
            let expected = run_fixture(format, &fixture, false);
            assert_eq!(run_fixture(format, &unquoted, true), expected);
            assert_eq!(
                run_zip_fixture(format, &unquoted, CompressionMethod::Deflated),
                expected
            );
        }
    }

    #[test]
    fn csv_name_alignment_leaves_ambiguous_records_unchanged() {
        let (headers, mut values) = general_rows(11);
        values[0] = "North".to_owned();
        values.insert(1, " Hospital".to_owned());
        let mut cases = Vec::new();
        for (index, value) in [
            (0, ""),
            (1, " "),
            (1, "2026-01-01"),
            (2, "2026-02-30"),
            (2, "2025-02-29"),
            (2, "2/19/26"),
            (2, "2026-02/19"),
            (3, "5.0.0"),
            (3, ""),
        ] {
            let mut invalid_values = values.clone();
            invalid_values[index] = value.to_owned();
            cases.push((headers.clone(), invalid_values));
        }
        let mut extra = values.clone();
        extra.insert(1, " General".to_owned());
        cases.push((headers.clone(), extra));
        let mut missing = values.clone();
        missing.pop();
        cases.push((headers.clone(), missing));
        let mut reordered = headers.clone();
        reordered.swap(0, 1);
        cases.push((reordered, values));
        for (headers, values) in cases {
            let headers = StringRecord::from(headers);
            let values = StringRecord::from(values);
            assert!(align_csv_hospital_name(&headers, &values).is_none());
            assert!(parse_csv_metadata(&headers, &values, DEFAULT_MAX_FANOUT_ROWS).is_err());
        }
        let (headers, values) = general_rows(11);
        assert!(align_csv_hospital_name(
            &StringRecord::from(headers),
            &StringRecord::from(values)
        ).is_none());
    }

    #[test]
    fn csv_name_alignment_keeps_metadata_body_and_resource_validation() {
        let fixture = fixture_tall_csv();
        for (field, value, expected) in [
            ("type_2_npi", "", "type_2_npi"),
            (ATTESTATION_TEXT, "1", "attestation value must be true or false"),
            (ATTESTATION_TEXT, "", "attestation value must be true or false"),
            ("attester_name", "", "attester_name"),
        ] {
            let mut records = csv_fixture_records(&fixture);
            let index = csv_fixture_index(&records[0], field);
            records[1][index] = value.to_owned();
            // A true value elsewhere must never replace the actual confirmation.
            records[1][9] = "true".to_owned();
            assert_import_error(
                InputFormat::TallCsv,
                &unquote_fixture_hospital_name(&csv_fixture_bytes(&records)),
                DEFAULT_MAX_FANOUT_ROWS,
                expected,
            );
        }
        let mut records = csv_fixture_records(&fixture);
        let index = csv_fixture_index(&records[2], "standard_charge | negotiated_dollar");
        records[3][index] = "-1".to_owned();
        assert_import_error(
            InputFormat::TallCsv,
            &unquote_fixture_hospital_name(&csv_fixture_bytes(&records)),
            DEFAULT_MAX_FANOUT_ROWS,
            "must be greater than zero",
        );
        assert_payload_limit_error(
            InputFormat::TallCsv,
            &unquote_fixture_hospital_name(&fixture),
            128,
            "CSV record exceeds configured limit",
        );
    }
