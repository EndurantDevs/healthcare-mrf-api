    fn service_description_fixture(format: InputFormat, descriptions: &[&str]) -> Vec<u8> {
        if format == InputFormat::Json {
            let mut payload: serde_json::Value = serde_json::from_slice(&fixture_json()).unwrap();
            let service = payload["standard_charge_information"][0].clone();
            payload["standard_charge_information"] = descriptions.iter().map(|description| {
                let mut service = service.clone();
                service["description"] = json!(description);
                service
            }).collect();
            return serde_json::to_vec(&payload).unwrap();
        }
        let fixture = match format {
            InputFormat::TallCsv => fixture_tall_csv(),
            InputFormat::WideCsv => fixture_wide_csv(),
            InputFormat::Json => unreachable!(),
        };
        let mut records = csv_fixture_records(&fixture);
        let description_column = csv_fixture_index(&records[2], "description");
        let service = records.pop().unwrap();
        for description in descriptions {
            let mut service = service.clone();
            service[description_column] = (*description).to_owned();
            records.push(service);
        }
        csv_fixture_bytes(&records)
    }

    #[test]
    fn service_description_preserves_present_whitespace() {
        for (description, copy_description) in [
            (" ", " "),
            (" \t\r\n", " \\t\\r\\n"),
            ("\u{a0}\u{2003}", "\u{a0}\u{2003}"),
        ] {
            let payload = service_description_fixture(InputFormat::Json, &[description]);
            let expected_copy = run_fixture(InputFormat::Json, &payload, false);
            assert_eq!(
                expected_copy["service"],
                format!("{VERSION_ID}\t0\t{copy_description}\t\\N\t\\N\n").as_bytes(),
            );
            let (_, expected_packed) = import_packed_json(&payload, TEST_MAX_OUTPUT_BYTES);
            for format in [InputFormat::Json, InputFormat::TallCsv, InputFormat::WideCsv] {
                let payload = service_description_fixture(format, &[description]);
                assert_eq!(run_fixture(format, &payload, false), expected_copy);
                let (directory, summary) = import_packed(format, &payload, TEST_MAX_OUTPUT_BYTES);
                assert_eq!(
                    summary.artifacts.iter().map(|a| (a.kind, a.rows, &a.sha256)).collect::<Vec<_>>(),
                    expected_packed.artifacts.iter().map(|a| (a.kind, a.rows, &a.sha256)).collect::<Vec<_>>()
                );
                let blocks = super::packed_output_tests::payloads(
                    &directory.path().join("output/service_block.copy"),
                );
                let services = crate::hospital_price_service_block::decode_service_block(&blocks[0]).unwrap();
                assert_eq!(services[0].description, description);
                assert_eq!(services[0].codes[0].code, "70551");
                assert_eq!(services[0].charges[0].gross_charge.as_deref(), Some("12.34"));
                assert_eq!(summary.root.unwrap().fact_count, 1);
            }
        }
    }

    #[test]
    fn service_description_keeps_nonblank_trimming() {
        for format in [InputFormat::Json, InputFormat::TallCsv, InputFormat::WideCsv] {
            let plain = service_description_fixture(format, &["MRI,\nbrain"]);
            let padded = service_description_fixture(format, &[" \tMRI,\nbrain\r\n "]);
            assert_eq!(run_fixture(format, &plain, false), run_fixture(format, &padded, false));
            let (_, plain) = import_packed(format, &plain, TEST_MAX_OUTPUT_BYTES);
            let (_, padded) = import_packed(format, &padded, TEST_MAX_OUTPUT_BYTES);
            assert_eq!(
                plain.artifacts.iter().map(|a| (a.kind, a.rows, &a.sha256)).collect::<Vec<_>>(),
                padded.artifacts.iter().map(|a| (a.kind, a.rows, &a.sha256)).collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn service_description_keeps_distinct_source_ordinals() {
        let descriptions = [" ", "  ", "MRI", " "];
        for format in [InputFormat::Json, InputFormat::TallCsv, InputFormat::WideCsv] {
            let payload = service_description_fixture(format, &descriptions);
            let rows = run_fixture(format, &payload, false);
            for kind in ["service", "code", "charge", "payer_charge"] {
                assert_eq!(rows[kind].split(|byte| *byte == b'\n').count() - 1, 4);
            }
            let (directory, summary) = import_packed(format, &payload, TEST_MAX_OUTPUT_BYTES);
            let root = summary.root.unwrap();
            assert_eq!((root.service_count, root.charge_count, root.fact_count), (4, 4, 4));
            assert_eq!((root.code_selector_key_count, root.code_selector_ref_count), (1, 4));
            assert_eq!((root.payer_plan_selector_key_count, root.payer_plan_selector_ref_count), (1, 4));
            let blocks = super::packed_output_tests::payloads(
                &directory.path().join("output/service_block.copy"),
            );
            let services = crate::hospital_price_service_block::decode_service_block(&blocks[0]).unwrap();
            assert_eq!(services.len(), descriptions.len());
            for (ordinal, service) in services.iter().enumerate() {
                assert_eq!(service.service_ordinal, ordinal as u64);
                assert_eq!(service.description, descriptions[ordinal]);
                assert_eq!(service.charges.len(), 1);
                let charge = &service.charges[0];
                assert_eq!((charge.charge_key, charge.charge_ordinal), (ordinal as u32, 0));
                assert_eq!((charge.first_fact_ordinal, charge.fact_count), (ordinal as u64, 1));
            }
            let blocks = super::packed_output_tests::payloads(
                &directory.path().join("output/fact_block.copy"),
            );
            let facts = crate::hospital_price_block::decode_fact_block(&blocks[0], None, None, 0, 4).unwrap();
            assert_eq!(facts.len(), 4);
            for (ordinal, fact) in facts.iter().enumerate() {
                assert_eq!(fact.charge_key, ordinal as u32);
                assert_eq!(fact.negotiated_dollar.as_deref(), Some("9.125"));
            }
        }
    }

    fn assert_description_error(format: InputFormat, payload: &[u8], expected: &str) {
        assert_import_error(format, payload, DEFAULT_MAX_FANOUT_ROWS, expected);
        let directory = tempfile::tempdir().unwrap();
        let input = directory.path().join("input");
        let output = directory.path().join("output");
        fs::write(&input, payload).unwrap();
        fs::create_dir(&output).unwrap();
        let error = import_hospital_mrf_with_output_mode(
            format, VERSION_ID, &input, &output,
            HospitalMrfLimits::new(DEFAULT_MAX_FANOUT_ROWS, TEST_MAX_DECOMPRESSED_BYTES, TEST_MAX_OUTPUT_BYTES),
            HospitalMrfOutputMode::Packed,
        ).unwrap_err();
        assert!(error.to_string().contains(expected), "expected {expected:?} in {error}");
        assert_eq!(fs::read_dir(output).unwrap().count(), 0);
    }

    #[test]
    fn service_description_rejects_absent_empty_and_nul() {
        for (description, expected) in [
            (json!(""), "description must be a non-empty string"),
            (json!(null), "String"),
            (json!(1), "String"),
            (json!(" \0 "), "contains NUL"),
        ] {
            let mut payload: serde_json::Value = serde_json::from_slice(&fixture_json()).unwrap();
            payload["standard_charge_information"][0]["description"] = description;
            assert_description_error(InputFormat::Json, &serde_json::to_vec(&payload).unwrap(), expected);
        }
        let mut payload: serde_json::Value = serde_json::from_slice(&fixture_json()).unwrap();
        payload["standard_charge_information"][0].as_object_mut().unwrap().remove("description");
        assert_description_error(InputFormat::Json, &serde_json::to_vec(&payload).unwrap(), "description");
        for format in [InputFormat::TallCsv, InputFormat::WideCsv] {
            for (description, expected) in [("", "description must be a non-empty string"), (" \0 ", "contains NUL")] {
                assert_description_error(format, &service_description_fixture(format, &[description]), expected);
            }
            let mut records = csv_fixture_records(&service_description_fixture(format, &[" "]));
            records[2][0] = "not_description".to_owned();
            assert_description_error(format, &csv_fixture_bytes(&records), "description");
        }
    }

    #[test]
    fn service_description_keeps_code_requirements() {
        for codes in [json!([]), json!([{"code": " ", "type": "CPT"}])] {
            let mut payload: serde_json::Value = serde_json::from_slice(
                &service_description_fixture(InputFormat::Json, &[" "]),
            ).unwrap();
            payload["standard_charge_information"][0]["code_information"] = codes;
            assert_description_error(InputFormat::Json, &serde_json::to_vec(&payload).unwrap(), "code");
        }
    }

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
                plan_name: Some("Plan".to_owned()),
                negotiated_rate_term: None,
                standard_charge_dollar: Some("1".to_owned()),
                standard_charge_percentage: None,
                standard_charge_algorithm: None,
                estimated_amount: None,
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
    fn contract_provision_aliases_emit_canonical_text() {
        for (alias, text) in [
            ("provision", "Singular contract terms"),
            ("description", "Described contract terms"),
        ] {
            let mut payload: serde_json::Value = serde_json::from_slice(&fixture_json()).unwrap();
            let provision = payload["general_contract_provisions"][0]
                .as_object_mut()
                .unwrap();
            provision.remove("provisions");
            provision.insert(alias.to_owned(), json!(text));

            let rows = run_fixture(
                InputFormat::Json,
                &serde_json::to_vec(&payload).unwrap(),
                false,
            );
            assert_eq!(
                String::from_utf8(rows["contract_provision"].clone()).unwrap(),
                format!("fixture-version\t0\t\\N\t\\N\t{text}\n")
            );
        }
    }

    #[test]
    fn csv_billing_class_aliases_do_not_relax_json_validation() {
        for alias in ["hospital", "facilty"] {
            assert_eq!(canonical_billing_class(alias, true).unwrap(), "facility");
            assert!(canonical_billing_class(alias, false).is_err());
        }
        assert!(canonical_billing_class("hospitalized", true).is_err());
    }

    #[test]
    fn singleton_financial_aid_policy_array_matches_scalar_copy_rows() {
        let scalar_rows = run_fixture(InputFormat::Json, &fixture_json(), false);
        let mut singleton: serde_json::Value =
            serde_json::from_slice(&fixture_json()).unwrap();
        singleton["financial_aid_policy"] = json!(["Policy,\nline"]);
        let singleton_rows = run_fixture(
            InputFormat::Json,
            &serde_json::to_vec(&singleton).unwrap(),
            false,
        );
        assert_eq!(singleton_rows, scalar_rows);
    }

    #[test]
    fn invalid_financial_aid_policy_arrays_are_rejected_without_outputs() {
        for policy in [json!([]), json!(["one", "two"]), json!([1]), json!([{}])] {
            let mut payload: serde_json::Value =
                serde_json::from_slice(&fixture_json()).unwrap();
            payload["financial_aid_policy"] = policy;
            assert_import_error(
                InputFormat::Json,
                &serde_json::to_vec(&payload).unwrap(),
                DEFAULT_MAX_FANOUT_ROWS,
                if payload["financial_aid_policy"].as_array().unwrap().len() != 1 {
                    "financial_aid_policy array must contain exactly one string"
                } else {
                    "expected JSON value type String"
                },
            );
        }
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
        let bracketed_wide = String::from_utf8(wide.clone())
            .unwrap()
            .replace("Payer, Inc.|Plan A", "[Aetna]|[MADV]");
        let bracketed_rows = run_fixture(
            InputFormat::WideCsv,
            bracketed_wide.as_bytes(),
            false,
        );
        assert!(String::from_utf8(bracketed_rows["payer_charge"].clone())
            .unwrap()
            .contains("\t[Aetna]\t[MADV]\t"));

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
        let columns =
            parse_wide_columns(&StringRecord::from(headers.clone()), CmsProfile::V3, true, 1).unwrap();
        assert_eq!(columns.payers.len(), 1);
        assert_eq!(columns.payers[0].payer_name, "Payer, Inc.");
        assert_eq!(columns.payers[0].plan_name, "Plan A");

        let mut duplicate = headers.clone();
        duplicate.push(
            "STANDARD_CHARGE|payer, inc.|PLAN A|NEGOTIATED_DOLLAR".to_owned(),
        );
        let duplicate_columns =
            parse_wide_columns(&StringRecord::from(duplicate), CmsProfile::V3, true, 1).unwrap();
        assert_eq!(duplicate_columns.payers.len(), 1);
        assert_eq!(duplicate_columns.payers[0].standard_charge_dollar, 10);
        assert_eq!(duplicate_columns.duplicate_columns, vec![(10, headers.len())]);

        let missing_notes = headers
            .into_iter()
            .filter(|header| !header.to_ascii_lowercase().starts_with("additional_payer_notes|"))
            .collect::<Vec<_>>();
        let error =
            parse_wide_columns(&StringRecord::from(missing_notes), CmsProfile::V3, true, 1).unwrap_err();
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
include!("tests_duplicate_wide.rs");
