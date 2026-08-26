    #[test]
    fn optional_fields_are_preserved_and_json_enums_are_case_sensitive() {
        assert_eq!(canonical_drug_type("gr", true).unwrap(), "GR");
        assert!(canonical_drug_type("gr", false).is_err());
        assert_eq!(allowed_count("1 THROUGH 10", true).unwrap(), "1 through 10");
        assert!(allowed_count("1 THROUGH 10", false).is_err());
        assert_eq!(allowed_count("0", false).unwrap(), "0");
        assert!(allowed_count("", true).is_err());
        assert!(allowed_count("", false).is_err());
        assert!(allowed_count("1", false).is_err());
        assert_eq!(allowed_count("11", false).unwrap(), "11");
        let drug_service = validate_service(
            ServiceRow {
                description: "Drug".to_owned(),
                codes: vec![CodeRow {
                    code_type: "NDC".to_owned(),
                    code: "0001".to_owned(),
                }],
                drug_unit: Some("1".to_owned()),
                drug_type: Some("gr".to_owned()),
            },
            true,
        )
        .unwrap();
        assert_eq!(drug_service.drug_type.as_deref(), Some("GR"));
        let other_payer = validate_payer(
            PayerChargeRow {
                payer_name: "Payer".to_owned(),
                plan_name: "Plan".to_owned(),
                standard_charge_dollar: Some("1".to_owned()),
                standard_charge_percentage: None,
                standard_charge_algorithm: None,
                median_amount: None,
                percentile_10: None,
                percentile_90: None,
                allowed_count: None,
                methodology: "other".to_owned(),
                additional_payer_notes: None,
            },
            Some("Generic note"),
            true,
        )
        .unwrap();
        assert_eq!(other_payer.methodology, "other");
        let rows = run_fixture(InputFormat::Json, &fixture_json(), false);
        assert!(String::from_utf8(rows["mrf"].clone())
            .unwrap()
            .contains("Policy,\\nline"));
        assert_eq!(
            String::from_utf8(rows["contract_provision"].clone()).unwrap(),
            "fixture-version\t0\t\\N\t\\N\tAggregate,\\nterms\n"
        );
        assert!(String::from_utf8(rows["charge"].clone())
            .unwrap()
            .ends_with("\tfacility\n"));

        let original: serde_json::Value = serde_json::from_slice(&fixture_json()).unwrap();
        assert!(serde_json::from_str::<FanoutVec<String>>("{}")
            .unwrap_err()
            .to_string()
            .contains("bounded hospital MRF array"));
        let mut non_array_codes = original.clone();
        non_array_codes["standard_charge_information"][0]["code_information"] = json!({});
        assert_import_error(
            InputFormat::Json,
            &serde_json::to_vec(&non_array_codes).unwrap(),
            DEFAULT_MAX_FANOUT_ROWS,
            "expected JSON value type Array",
        );
        for (pointer, invalid_value, expected) in [
            ("/version", "", "version must be a non-empty string"),
            ("/version", "2.0.0", "version must be 3.0.0"),
            (
                "/license_information/state",
                "",
                "license state must be a non-empty string",
            ),
            ("/license_information/state", "ca", "invalid license state"),
            (
                "/standard_charge_information/0/code_information/0/type",
                "cpt",
                "invalid code type",
            ),
            (
                "/standard_charge_information/0/standard_charges/0/setting",
                "OUTPATIENT",
                "setting must be",
            ),
            (
                "/standard_charge_information/0/standard_charges/0/billing_class",
                "FACILITY",
                "billing_class must be",
            ),
            (
                "/standard_charge_information/0/standard_charges/0/payers_information/0/methodology",
                "Fee Schedule",
                "invalid standard charge methodology",
            ),
        ] {
            let mut value = original.clone();
            *value.pointer_mut(pointer).unwrap() = json!(invalid_value);
            assert_import_error(
                InputFormat::Json,
                &serde_json::to_vec(&value).unwrap(),
                DEFAULT_MAX_FANOUT_ROWS,
                expected,
            );
        }

        let mut empty_locations = original.clone();
        empty_locations["location_name"] = json!([]);
        assert_import_error(
            InputFormat::Json,
            &serde_json::to_vec(&empty_locations).unwrap(),
            DEFAULT_MAX_FANOUT_ROWS,
            "location_name must contain at least one value",
        );

        let mut attestation_whitespace = original.clone();
        *attestation_whitespace
            .pointer_mut("/attestation/attestation")
            .unwrap() = json!(format!("{ATTESTATION_TEXT} "));
        assert_import_error(
            InputFormat::Json,
            &serde_json::to_vec(&attestation_whitespace).unwrap(),
            DEFAULT_MAX_FANOUT_ROWS,
            "attestation text does not match",
        );

        let mut generic_only_other = original.clone();
        generic_only_other["standard_charge_information"][0]["standard_charges"][0]
            ["additional_generic_notes"] = json!("Not payer-specific");
        generic_only_other["standard_charge_information"][0]["standard_charges"][0]
            ["payers_information"][0]["methodology"] = json!("other");
        assert_import_error(
            InputFormat::Json,
            &serde_json::to_vec(&generic_only_other).unwrap(),
            DEFAULT_MAX_FANOUT_ROWS,
            "methodology other requires explanatory notes",
        );

        let mut identified_provision = original;
        identified_provision["general_contract_provisions"][0]["payer_name"] = json!("Payer, Inc.");
        identified_provision["general_contract_provisions"][0]["plan_name"] = json!("Plan A");
        let identified_rows = run_fixture(
            InputFormat::Json,
            &serde_json::to_vec(&identified_provision).unwrap(),
            false,
        );
        assert_eq!(
            String::from_utf8(identified_rows["contract_provision"].clone()).unwrap(),
            "fixture-version\t0\tPayer, Inc.\tPlan A\tAggregate,\\nterms\n"
        );
    }
    #[test]
    fn nul_header_gap_and_fanout_abort_without_outputs() {
        let mut nul: serde_json::Value = serde_json::from_slice(&fixture_json()).unwrap();
        *nul.pointer_mut("/standard_charge_information/0/description")
            .unwrap() = json!("MRI\0brain");
        assert_import_error(
            InputFormat::Json,
            &serde_json::to_vec(&nul).unwrap(),
            DEFAULT_MAX_FANOUT_ROWS,
            "contains NUL",
        );
        let mut nul_modifier: serde_json::Value = serde_json::from_slice(&fixture_json()).unwrap();
        *nul_modifier
            .pointer_mut("/standard_charge_information/0/standard_charges/0/modifier_code/0")
            .unwrap() = json!("26\0");
        assert_import_error(
            InputFormat::Json,
            &serde_json::to_vec(&nul_modifier).unwrap(),
            DEFAULT_MAX_FANOUT_ROWS,
            "modifier code contains NUL",
        );
        let mut empty_modifier: serde_json::Value =
            serde_json::from_slice(&fixture_json()).unwrap();
        *empty_modifier
            .pointer_mut("/standard_charge_information/0/standard_charges/0/modifier_code")
            .unwrap() = json!([]);
        assert_import_error(
            InputFormat::Json,
            &serde_json::to_vec(&empty_modifier).unwrap(),
            DEFAULT_MAX_FANOUT_ROWS,
            "modifier_code must contain at least one value",
        );

        let duplicate = StringRecord::from(vec!["description", " DESCRIPTION "]);
        assert!(find_header(&duplicate, &["description"])
            .unwrap_err()
            .to_string()
            .contains("duplicate CSV header description"));
        let duplicate_optional = StringRecord::from(vec!["billing_class", " BILLING_CLASS "]);
        assert!(find_optional_header(&duplicate_optional, &["billing_class"])
            .unwrap_err()
            .to_string()
            .contains("duplicate CSV header billing_class"));

        let gap = String::from_utf8(fixture_tall_csv())
            .unwrap()
            .replace("code | 1 | type", "code | 2 | type")
            .replace("code | 1", "code | 2");
        assert_import_error(
            InputFormat::TallCsv,
            gap.as_bytes(),
            DEFAULT_MAX_FANOUT_ROWS,
            "ordinals must be exactly 1 through N",
        );
        let leading_zero = String::from_utf8(fixture_tall_csv())
            .unwrap()
            .replace("code | 1 | type", "code | 01 | type")
            .replace("code | 1", "code | 01");
        assert_import_error(
            InputFormat::TallCsv,
            leading_zero.as_bytes(),
            DEFAULT_MAX_FANOUT_ROWS,
            "canonical positive integers",
        );

        assert_import_error(
            InputFormat::Json,
            &fixture_json(),
            2,
            "fanout exceeds configured limit 2",
        );

        let mut oversized_header: serde_json::Value =
            serde_json::from_slice(&fixture_json()).unwrap();
        oversized_header["location_name"] = json!(["A", "B", "C"]);
        assert_import_error(
            InputFormat::Json,
            &serde_json::to_vec(&oversized_header).unwrap(),
            2,
            "fanout exceeds configured limit 2",
        );

        let (headers, mut values) = general_rows(11);
        values[3] = "A|B|C".to_owned();
        let error = parse_csv_metadata(
            &StringRecord::from(headers),
            &StringRecord::from(values),
            2,
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("location_name fanout exceeds configured limit 2"));

        let tall = String::from_utf8(fixture_tall_csv())
            .unwrap()
            .replace("26 | TC", "26")
            .into_bytes();
        let mut reader = ReaderBuilder::new()
            .has_headers(false)
            .from_reader(tall.as_slice());
        let records = reader.records().collect::<Result<Vec<_>, _>>().unwrap();
        let mut writer = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(Vec::new());
        for record in &records {
            writer.write_record(record).unwrap();
        }
        writer.write_record(records.last().unwrap()).unwrap();
        let repeated_payer = writer.into_inner().unwrap();
        assert_import_error(
            InputFormat::TallCsv,
            &repeated_payer,
            1,
            "payer fanout exceeds configured limit 1",
        );
    }

    #[test]
    fn wide_payers_group_case_insensitively_and_require_payer_notes() {
        let wide = fixture_wide_csv();
        let mut reader = ReaderBuilder::new()
            .has_headers(false)
            .from_reader(wide.as_slice());
        let records = reader.records().collect::<Result<Vec<_>, _>>().unwrap();
        let headers = records[2]
            .iter()
            .enumerate()
            .map(|(index, header)| {
                if index > 10 {
                    header.replace("Payer, Inc.|Plan A", "PAYER, INC.|plan a")
                } else {
                    header.to_owned()
                }
            })
            .collect::<Vec<_>>();
        let columns = parse_wide_columns(&StringRecord::from(headers.clone()), 1).unwrap();
        assert_eq!(columns.payers.len(), 1);
        assert_eq!(columns.payers[0].payer_name, "Payer, Inc.");
        assert_eq!(columns.payers[0].plan_name, "Plan A");

        let mut duplicate = headers.clone();
        duplicate.push(
            "STANDARD_CHARGE|payer, inc.|PLAN A|NEGOTIATED_DOLLAR".to_owned(),
        );
        let error = parse_wide_columns(&StringRecord::from(duplicate), 1).unwrap_err();
        assert!(error
            .to_string()
            .contains("duplicate wide CSV payer header"));

        let missing_notes = headers
            .into_iter()
            .filter(|header| !header.to_ascii_lowercase().starts_with("additional_payer_notes|"))
            .collect::<Vec<_>>();
        let error = parse_wide_columns(&StringRecord::from(missing_notes), 1).unwrap_err();
        assert!(error.to_string().contains("is missing additional_payer_notes"));

        let payer_row = |methodology, count, payer_notes| {
            let mut values = vec![
                    ("description", "Generic note is not payer-specific"),
                    ("code|1", "0001"),
                    ("code|1|type", "CPT"),
                    ("setting", "outpatient"),
                    ("billing_class", "facility"),
                    ("standard_charge|min", "1"),
                    ("standard_charge|max", "1"),
                    (
                        "standard_charge|Payer, Inc.|Plan A|negotiated_dollar",
                        "1",
                    ),
                    ("count|Payer, Inc.|Plan A", count),
                    (
                        "standard_charge|Payer, Inc.|Plan A|methodology",
                        methodology,
                    ),
                    ("additional_generic_notes", "Generic note"),
                ];
            if let Some(notes) = payer_notes {
                values.push((
                    "additional_payer_notes|Payer, Inc.|Plan A",
                    notes,
                ));
            }
            append_csv_row(&fixture_wide_csv(), &values)
        };
        assert_import_error(
            InputFormat::WideCsv,
            &payer_row("fee schedule", "0", None),
            DEFAULT_MAX_FANOUT_ROWS,
            "count 0 requires explanatory notes",
        );
        assert_import_error(
            InputFormat::WideCsv,
            &payer_row("other", "", None),
            DEFAULT_MAX_FANOUT_ROWS,
            "methodology other requires explanatory notes",
        );

        let payer_note = payer_row("fee schedule", "0", Some("Payer-specific note"));
        run_fixture(InputFormat::WideCsv, &payer_note, false);

        let note_only_payer = append_csv_row(
            &fixture_wide_csv(),
            &[
                ("description", "Service unavailable for payer"),
                ("code|1", "0002"),
                ("code|1|type", "CPT"),
                ("setting", "outpatient"),
                ("billing_class", "facility"),
                ("standard_charge|gross", "1"),
                (
                    "additional_payer_notes|Payer, Inc.|Plan A",
                    "service not payable",
                ),
            ],
        );
        run_fixture(InputFormat::WideCsv, &note_only_payer, false);
    }
