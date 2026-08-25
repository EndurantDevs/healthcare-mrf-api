    #[test]
    fn optional_fields_are_preserved_and_json_enums_are_case_sensitive() {
        assert_eq!(canonical_drug_type("gr", true).unwrap(), "GR");
        assert!(canonical_drug_type("gr", false).is_err());
        assert_eq!(allowed_count("1 THROUGH 10", true).unwrap(), "1 through 10");
        assert!(allowed_count("1 THROUGH 10", false).is_err());
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
        for (pointer, invalid_value, expected) in [
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
    fn unsafe_zip_inputs_are_rejected_without_outputs() {
        let nested = zip_bytes(
            &[(
                "inner.zip",
                &zip_bytes(
                    &[("prices.json", &fixture_json())],
                    CompressionMethod::Stored,
                ),
            )],
            CompressionMethod::Deflated,
        );
        let cases = [
            ("empty", zip_bytes(&[], CompressionMethod::Stored)),
            (
                "empty-member",
                zip_bytes(&[("prices.json", b"")], CompressionMethod::Stored),
            ),
            (
                "multiple",
                zip_bytes(
                    &[
                        ("one.json", &fixture_json()),
                        ("two.json", &fixture_json()),
                    ],
                    CompressionMethod::Stored,
                ),
            ),
            ("nested", nested),
        ];
        for (_, bytes) in cases {
            assert_zip_import_error(&bytes, TEST_MAX_OUTPUT_BYTES, "ZIP");
        }

        let payload = fixture_json();
        let mut encrypted = zip_bytes(&[("prices.json", &payload)], CompressionMethod::Stored);
        let local_flags = u16::from_le_bytes(encrypted[6..8].try_into().unwrap()) | 1;
        encrypted[6..8].copy_from_slice(&local_flags.to_le_bytes());
        let central = encrypted
            .windows(4)
            .position(|window| window == b"PK\x01\x02")
            .unwrap();
        let central_flags =
            u16::from_le_bytes(encrypted[central + 8..central + 10].try_into().unwrap()) | 1;
        encrypted[central + 8..central + 10].copy_from_slice(&central_flags.to_le_bytes());
        assert_zip_import_error(&encrypted, TEST_MAX_OUTPUT_BYTES, "encrypted");

        let mut unsupported = zip_bytes(&[("prices.json", &payload)], CompressionMethod::Stored);
        unsupported[8..10].copy_from_slice(&12u16.to_le_bytes());
        let central = unsupported
            .windows(4)
            .position(|window| window == b"PK\x01\x02")
            .unwrap();
        unsupported[central + 10..central + 12].copy_from_slice(&12u16.to_le_bytes());
        assert_zip_import_error(&unsupported, TEST_MAX_OUTPUT_BYTES, "compression method");

        let invalid_utf8 = zip_bytes(
            &[("prices.json", b"{\"hospital_name\":\"\xff\"}")],
            CompressionMethod::Deflated,
        );
        assert_zip_import_error(&invalid_utf8, TEST_MAX_OUTPUT_BYTES, "UTF-8");
    }

    #[test]
    fn zip_expansion_and_crc_are_bounded_and_validated() {
        let mut payload = fixture_json();
        payload.extend(vec![b' '; 4096]);
        let max_bytes = payload.len() as u64 - 1;

        let archive = zip_bytes(&[("prices.json", &payload)], CompressionMethod::Deflated);
        assert_zip_import_error(&archive, max_bytes, "decompressed size exceeds");

        let mut understated = archive.clone();
        zip_field(&mut understated, b"PK\x03\x04", 22, max_bytes as u32);
        zip_field(&mut understated, b"PK\x01\x02", 24, max_bytes as u32);
        assert_zip_import_error(&understated, max_bytes, "decompressed data exceeds");

        let mut bad_crc = archive;
        zip_field(&mut bad_crc, b"PK\x03\x04", 14, 0);
        zip_field(&mut bad_crc, b"PK\x01\x02", 16, 0);
        assert_zip_import_error(&bad_crc, TEST_MAX_OUTPUT_BYTES, "checksum");
    }

    #[test]
    fn decompression_record_and_string_limits_abort_without_outputs() {
        let directory = tempfile::tempdir().unwrap();
        let input_path = directory.path().join("oversized.json.gz");
        let file = File::create(&input_path).unwrap();
        let mut encoder = GzEncoder::new(file, Compression::default());
        encoder.write_all(&fixture_json()).unwrap();
        encoder.finish().unwrap();
        let output_directory = directory.path().join("output");
        fs::create_dir(&output_directory).unwrap();
        let error = import_hospital_mrf_with_limits(
            InputFormat::Json,
            VERSION_ID,
            &input_path,
            &output_directory,
            DEFAULT_MAX_FANOUT_ROWS,
            128,
            TEST_MAX_OUTPUT_BYTES,
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("decompressed data exceeds configured limit"));
        assert_eq!(fs::read_dir(&output_directory).unwrap().count(), 0);

        let giant_header = format!("a\nb\n{}\n", "x".repeat(64));
        assert_payload_limit_error(
            InputFormat::TallCsv,
            giant_header.as_bytes(),
            16,
            "CSV record exceeds configured limit",
        );
        let giant_string = format!(r#"{{"oversized":"{}"}}"#, "x".repeat(64));
        assert_payload_limit_error(
            InputFormat::Json,
            giant_string.as_bytes(),
            16,
            "JSON string exceeds configured limit",
        );
    }

    #[test]
    fn public_copy_contract_uses_version_id() {
        assert_eq!(
            CopyKind::ALL.map(CopyKind::name),
            [
                "mrf",
                "location",
                "npi",
                "license",
                "contract_provision",
                "service",
                "code",
                "charge",
                "payer_charge",
                "modifier",
                "modifier_payer",
            ]
        );
        assert_eq!(
            MRF_COPY_COLUMNS,
            &[
                "version_id",
                "hospital_name",
                "last_updated_on",
                "version",
                "attestation_text",
                "confirm_attestation",
                "attester_name",
                "financial_aid_policy",
            ]
        );
        assert_eq!(
            CONTRACT_PROVISION_COPY_COLUMNS,
            &[
                "version_id",
                "provision_ordinal",
                "payer_name",
                "plan_name",
                "provisions",
            ]
        );
        assert_eq!(
            CHARGE_COPY_COLUMNS,
            &[
                "version_id",
                "service_ordinal",
                "charge_ordinal",
                "setting",
                "modifier_codes",
                "gross_charge",
                "discounted_cash",
                "minimum",
                "maximum",
                "additional_generic_notes",
                "billing_class",
            ]
        );
        assert_eq!(
            MODIFIER_COPY_COLUMNS,
            &[
                "version_id",
                "modifier_ordinal",
                "code",
                "description",
                "setting",
                "additional_generic_notes",
            ]
        );
        assert_eq!(
            MODIFIER_PAYER_COPY_COLUMNS,
            &[
                "version_id",
                "modifier_ordinal",
                "payer_ordinal",
                "payer_name",
                "plan_name",
                "description",
                "standard_charge_dollar",
                "standard_charge_percentage",
                "standard_charge_algorithm",
            ]
        );
        for columns in [
            MRF_COPY_COLUMNS,
            LOCATION_COPY_COLUMNS,
            NPI_COPY_COLUMNS,
            LICENSE_COPY_COLUMNS,
            CONTRACT_PROVISION_COPY_COLUMNS,
            SERVICE_COPY_COLUMNS,
            CODE_COPY_COLUMNS,
            CHARGE_COPY_COLUMNS,
            PAYER_CHARGE_COPY_COLUMNS,
            MODIFIER_COPY_COLUMNS,
            MODIFIER_PAYER_COPY_COLUMNS,
        ] {
            assert_eq!(columns.first(), Some(&"version_id"));
            assert!(!columns.contains(&"source_id"));
        }
    }
