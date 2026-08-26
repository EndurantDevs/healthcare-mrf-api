    #[test]
    fn unsafe_zip_inputs_are_rejected_without_outputs() {
        assert_zip_import_error(
            b"PK\x03\x04not-a-zip",
            TEST_MAX_OUTPUT_BYTES,
            "ZIP",
        );
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
    fn json_service_code_budget_fails_before_retaining_unbounded_fanout() {
        let mut payload: serde_json::Value = serde_json::from_slice(&fixture_json()).unwrap();
        payload["standard_charge_information"][0]["code_information"] = serde_json::Value::Array(
            (0..5)
                .map(|index| {
                    json!({
                        "code": format!("{index}{}", "x".repeat(1024 * 1024 - 1)),
                        "type": "CPT",
                    })
                })
                .collect(),
        );

        assert_payload_limit_error(
            InputFormat::Json,
            &serde_json::to_vec(&payload).unwrap(),
            MAX_INPUT_VALUE_BYTES,
            "service code data exceeds 4 MiB",
        );
    }

    #[test]
    fn json_retained_budget_rejects_metadata_and_nested_payer_heap() {
        let mut metadata: serde_json::Value = serde_json::from_slice(&fixture_json()).unwrap();
        metadata["location_name"] = json!(["x".repeat(JSON_RETAINED_BYTE_LIMIT)]);
        assert_payload_limit_error(
            InputFormat::Json,
            &serde_json::to_vec(&metadata).unwrap(),
            MAX_INPUT_VALUE_BYTES,
            "JSON retained data exceeds 64 MiB",
        );
        drop(metadata);

        let mut payer: serde_json::Value = serde_json::from_slice(&fixture_json()).unwrap();
        payer["standard_charge_information"][0]["standard_charges"][0]
            ["payers_information"][0]["additional_payer_notes"] =
            json!("x".repeat(JSON_RETAINED_BYTE_LIMIT));
        assert_payload_limit_error(
            InputFormat::Json,
            &serde_json::to_vec(&payer).unwrap(),
            MAX_INPUT_VALUE_BYTES,
            "JSON retained data exceeds 64 MiB",
        );
    }

    #[test]
    fn csv_record_limit_follows_quote_and_line_ending_boundaries() {
        let mut accepted = BoundedCsvRecordReader::new(
            Cursor::new(b"ab\"cd\rx\n\"a\"\"\nb\",x\r\nz"),
            10,
        );
        let mut byte = [0u8; 1];
        while accepted.read(&mut byte).unwrap() != 0 {}

        let mut oversized = BoundedCsvRecordReader::new(Cursor::new(b"short\r12345678901"), 10);
        assert!(oversized.read_to_end(&mut Vec::new()).unwrap_err().to_string().contains(
            "CSV record exceeds configured limit"
        ));
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
