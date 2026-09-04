fn estimated_only_v2_csv_without_methodology(format: InputFormat) -> Vec<u8> {
    let mut records = csv_fixture_records(&fixture_v2_csv(format, "2.0.0"));
    for header in ["negotiated_percentage", "methodology"] {
        let index = records[2]
            .iter()
            .position(|candidate| candidate.contains(header))
            .unwrap();
        records[3][index].clear();
    }
    csv_fixture_bytes(&records)
}

#[test]
fn v2_estimated_only_csv_payers_allow_blank_methodology() {
    for format in [InputFormat::TallCsv, InputFormat::WideCsv] {
        let payload = estimated_only_v2_csv_without_methodology(format);
        assert_estimated_only_payer(&run_fixture(format, &payload, false));

        let (directory, summary) = import_packed(format, &payload, TEST_MAX_OUTPUT_BYTES);
        assert_eq!(summary.schema_version, "2.0.0");
        let payloads = super::packed_output_tests::payloads(
            &directory.path().join("output/fact_block.copy"),
        );
        let facts = crate::hospital_price_block::decode_fact_block(
            &payloads[0],
            None,
            None,
            0,
            crate::hospital_price_block::HOSPITAL_PRICE_FACT_BLOCK_MAX_ROWS,
        )
        .unwrap();
        assert_eq!(facts[0].estimated_amount.as_deref(), Some("9.125"));
        assert!(facts[0].methodology.is_empty());
    }
}

#[test]
fn csv_missing_methodology_is_rejected_for_negotiated_charges() {
    for format in [InputFormat::TallCsv, InputFormat::WideCsv] {
        let mut v2 = csv_fixture_records(&fixture_v2_csv(format, "2.0.0"));
        let methodology = v2[2]
            .iter()
            .position(|header| header.contains("methodology"))
            .unwrap();
        v2[3][methodology].clear();
        assert_import_error(
            format,
            &csv_fixture_bytes(&v2),
            DEFAULT_MAX_FANOUT_ROWS,
            "invalid standard charge methodology",
        );

        let payload = match format {
            InputFormat::TallCsv => fixture_tall_csv(),
            InputFormat::WideCsv => fixture_wide_csv(),
            InputFormat::Json => unreachable!(),
        };
        let mut v3 = csv_fixture_records(&payload);
        let methodology = v3[2]
            .iter()
            .position(|header| header.contains("methodology"))
            .unwrap();
        v3[3][methodology].clear();
        assert_import_error(
            format,
            &csv_fixture_bytes(&v3),
            DEFAULT_MAX_FANOUT_ROWS,
            "invalid standard charge methodology",
        );
    }
}
