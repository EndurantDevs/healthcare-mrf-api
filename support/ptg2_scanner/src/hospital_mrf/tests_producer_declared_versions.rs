#[test]
fn producer_csv_v4_keeps_v3_validation() {
    for format in [InputFormat::TallCsv, InputFormat::WideCsv] {
        let payload = match format {
            InputFormat::TallCsv => fixture_tall_csv(),
            InputFormat::WideCsv => fixture_wide_csv(),
            InputFormat::Json => unreachable!(),
        };
        let mut records = csv_fixture_records(&payload);
        let version_index = csv_fixture_index(&records[0], "version");
        records[1][version_index] = "4.0.0".to_owned();

        let (rows, summary) =
            run_fixture_with_summary(format, &csv_fixture_bytes(&records), false);
        assert_eq!(summary.schema_version, "4.0.0");
        assert!(!rows["service"].is_empty());
        assert!(!rows["payer_charge"].is_empty());
        let mrf = String::from_utf8(rows["mrf"].clone()).unwrap();
        assert_eq!(mrf.trim_end().split('\t').collect::<Vec<_>>()[3], "4.0.0");
        for (field, error) in [("type_2_npi", "type_2_npi"), ("attester_name", "attester_name must be a non-empty string")] {
            let mut missing = records.clone();
            let index = csv_fixture_index(&missing[0], field);
            missing[1][index].clear();
            assert_import_error(format, &csv_fixture_bytes(&missing), DEFAULT_MAX_FANOUT_ROWS, error);
        }

        let mut v2_records = csv_fixture_records(&fixture_v2_csv(format, "2.0.0"));
        let version_index = csv_fixture_index(&v2_records[0], "version");
        v2_records[1][version_index] = "4.0.0".to_owned();
        let location = csv_fixture_index(&v2_records[0], "hospital_location");
        v2_records[0][location] = "location_name".to_owned();
        assert_import_error(
            format,
            &csv_fixture_bytes(&v2_records),
            DEFAULT_MAX_FANOUT_ROWS,
            "headers mix V2 and V3 profiles",
        );
    }

    let mut json_payload: serde_json::Value = serde_json::from_slice(&fixture_json()).unwrap();
    json_payload["version"] = json!("4.0.0");
    assert_import_error(
        InputFormat::Json,
        &serde_json::to_vec(&json_payload).unwrap(),
        DEFAULT_MAX_FANOUT_ROWS,
        "unsupported CMS JSON version",
    );
}

#[test]
fn producer_csv_v4_detects_v2_losslessly() {
    for format in [InputFormat::TallCsv, InputFormat::WideCsv] {
        let control = fixture_v2_csv(format, "2.0.0");
        let mut expected = run_fixture(format, &control, false);
        expected.insert("mrf".to_owned(), String::from_utf8(expected["mrf"].clone())
            .unwrap().replace("\t2.0.0\t", "\t4.0.0\t").into_bytes());
        let payload = fixture_v2_csv(format, "4.0.0");
        let (actual, summary) = run_fixture_with_summary(format, &payload, false);
        assert_eq!(actual, expected);
        assert_eq!(summary.schema_version, "4.0.0");
        assert!(actual["npi"].is_empty());
        assert!(String::from_utf8(actual["mrf"].clone()).unwrap().contains(AFFIRMATION_TEXT));
        assert_eq!(run_fixture(format, &payload, true), expected);
        assert_eq!(run_zip_fixture(format, &payload, CompressionMethod::Deflated), expected);
        let (_control_dir, control_summary) = import_packed(format, &control, TEST_MAX_OUTPUT_BYTES);
        let (_actual_dir, actual_summary) = import_packed(format, &payload, TEST_MAX_OUTPUT_BYTES);
        assert_eq!(actual_summary.schema_version, "4.0.0");
        assert_eq!(serde_json::to_value(actual_summary.root).unwrap(),
            serde_json::to_value(control_summary.root).unwrap());
        assert_eq!(actual_summary.artifacts.iter().filter(|artifact| artifact.kind != "mrf")
            .map(|artifact| (&artifact.kind, artifact.rows, &artifact.sha256)).collect::<Vec<_>>(),
            control_summary.artifacts.iter().filter(|artifact| artifact.kind != "mrf")
            .map(|artifact| (&artifact.kind, artifact.rows, &artifact.sha256)).collect::<Vec<_>>());
    }
}

fn producer_v4_multisite_records() -> Vec<Vec<String>> {
    // Retained two-site V2 header shape; all facility and price values are synthetic.
    let mut headers = vec!["hospital_name", "last_updated_on", "version", "hospital_location",
        "hospital_address", "license_number|NY", AFFIRMATION_TEXT];
    headers.resize(19, "");
    let mut metadata = vec!["Example Community Hospital", "4/25/2025", "4.0.0",
        "Example Community Hospital | Example Infusion Center",
        "1 Main Street, Example, NY 10001 | 2 Main Street, Example, NY 10001", "1000001|NY", "TRUE"];
    metadata.resize(19, "");
    [headers, metadata, vec!["code|1", "code|1|type", "description", "plan_name", "payer_name",
        "setting", "standard_charge|gross", "standard_charge|discounted_cash",
        "standard_charge|negotiated_dollar", "estimated_amount", "standard_charge|min",
        "standard_charge|max", "standard_charge|negotiated_algorithm", "standard_charge|negotiated_percentage",
        "drug_unit_of_measurement", "drug_type_of_measurement", "modifiers",
        "standard_charge | methodology", "additional_generic_notes"],
        vec!["G1001", "HCPCS", "Example service", "Plan A", "Payer A", "both", "2", "1.5",
        "1.25", "1.25", "1", "2", "fee schedule", "", "", "", "", "fee schedule", "Example note"]]
        .into_iter().map(|record| record.into_iter().map(str::to_owned).collect()).collect()
}

#[test]
fn producer_csv_v4_preserves_multisite_metadata() {
    let records = producer_v4_multisite_records();
    let actual = run_fixture(InputFormat::TallCsv, &csv_fixture_bytes(&records), false);
    let locations = String::from_utf8(actual["location"].clone()).unwrap();
    assert!(locations.contains("\t0\tExample Community Hospital\t1 Main Street, Example, NY 10001\n"));
    assert!(locations.contains("\t1\tExample Infusion Center\t2 Main Street, Example, NY 10001\n"));
    assert!(actual["npi"].is_empty());
    let mrf = String::from_utf8(actual["mrf"].clone()).unwrap();
    assert!(mrf.contains("\t2025-04-25\t4.0.0\t"));
    assert!(mrf.ends_with("\ttrue\t\\N\t\\N\n"));
    for (record, column, replacement, error) in [
        (0, 3, "location_name", "headers mix V2 and V3 profiles"),
        (0, 6, ATTESTATION_TEXT, "headers mix V2 and V3 profiles"),
        (0, 7, ATTESTATION_TEXT, "headers mix V2 and V3 profiles"),
        (1, 6, "1", "affirmation value must be true or false"),
        (1, 2, "9", "unsupported CMS CSV version"),
        (2, 9, "median_amount", "mix V2 and V3 payer profiles"),
        (3, 4, "", "payer_name must be a non-empty string"),
        (3, 6, "0", "gross_charge must be greater than zero"),
    ] {
        let mut invalid = records.clone();
        invalid[record][column] = replacement.to_owned();
        assert_import_error(InputFormat::TallCsv, &csv_fixture_bytes(&invalid), DEFAULT_MAX_FANOUT_ROWS, error);
    }
    assert_import_error(InputFormat::TallCsv, &csv_fixture_bytes(&records), 1, "fanout exceeds configured limit");
}

#[test]
fn producer_declared_csv_v3_0_1_requires_v3_shape_and_preserves_version() {
    for format in [InputFormat::TallCsv, InputFormat::WideCsv] {
        let payload = match format {
            InputFormat::TallCsv => fixture_tall_csv(),
            InputFormat::WideCsv => fixture_wide_csv(),
            InputFormat::Json => unreachable!(),
        };
        let mut records = csv_fixture_records(&payload);
        let version_index = csv_fixture_index(&records[0], "version");
        records[1][version_index] = "3.0.1".to_owned();

        let (rows, summary) =
            run_fixture_with_summary(format, &csv_fixture_bytes(&records), false);
        assert_eq!(summary.schema_version, "3.0.1");
        assert!(!rows["service"].is_empty());
        assert!(!rows["payer_charge"].is_empty());
        let mrf = String::from_utf8(rows["mrf"].clone()).unwrap();
        assert_eq!(mrf.trim_end().split('\t').collect::<Vec<_>>()[3], "3.0.1");

        let mut v2_records = csv_fixture_records(&fixture_v2_csv(format, "2.0.0"));
        let version_index = csv_fixture_index(&v2_records[0], "version");
        v2_records[1][version_index] = "3.0.1".to_owned();
        assert_import_error(
            format,
            &csv_fixture_bytes(&v2_records),
            DEFAULT_MAX_FANOUT_ROWS,
            "headers mix V2 and V3 profiles",
        );
    }

    let mut json_payload: serde_json::Value = serde_json::from_slice(&fixture_json()).unwrap();
    json_payload["version"] = json!("3.0.1");
    assert_import_error(
        InputFormat::Json,
        &serde_json::to_vec(&json_payload).unwrap(),
        DEFAULT_MAX_FANOUT_ROWS,
        "unsupported CMS JSON version",
    );
}
