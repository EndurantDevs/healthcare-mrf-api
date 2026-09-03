#[test]
fn producer_declared_csv_v4_requires_v3_shape_and_preserves_version() {
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

        let mut v2_records = csv_fixture_records(&fixture_v2_csv(format, "2.0.0"));
        let version_index = csv_fixture_index(&v2_records[0], "version");
        v2_records[1][version_index] = "4.0.0".to_owned();
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
