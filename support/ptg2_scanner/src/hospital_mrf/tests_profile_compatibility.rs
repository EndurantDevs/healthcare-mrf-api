fn csv_fixture_records(payload: &[u8]) -> Vec<Vec<String>> {
    ReaderBuilder::new()
        .has_headers(false)
        .from_reader(payload)
        .records()
        .map(|record| record.unwrap().iter().map(str::to_owned).collect())
        .collect()
}

fn csv_fixture_bytes(records: &[Vec<String>]) -> Vec<u8> {
    let mut writer = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(Vec::new());
    for record in records {
        writer.write_record(record).unwrap();
    }
    writer.into_inner().unwrap()
}

fn csv_fixture_index(headers: &[String], name: &str) -> usize {
    headers
        .iter()
        .position(|header| header == name)
        .expect("missing CSV fixture header")
}

fn fixture_v2_csv(format: InputFormat, version: &str) -> Vec<u8> {
    let payload = match format {
        InputFormat::TallCsv => fixture_tall_csv(),
        InputFormat::WideCsv => fixture_wide_csv(),
        InputFormat::Json => panic!("V2 CSV fixture requires a CSV format"),
    };
    let mut records = csv_fixture_records(&payload);
    let version_index = csv_fixture_index(&records[0], "version");
    let location_index = csv_fixture_index(&records[0], "location_name");
    let npi_index = csv_fixture_index(&records[0], "type_2_npi");
    let attestation_index = csv_fixture_index(&records[0], ATTESTATION_TEXT);
    let attester_index = csv_fixture_index(&records[0], "attester_name");
    records[1][version_index] = version.to_owned();
    records[0][location_index] = "hospital_location".to_owned();
    records[0][npi_index].clear();
    records[1][npi_index].clear();
    records[0][attestation_index] = AFFIRMATION_TEXT.to_owned();
    records[0][attester_index].clear();
    records[1][attester_index].clear();

    let (dollar, percentage, estimated, profile_only) = match format {
        InputFormat::TallCsv => (
            "standard_charge | negotiated_dollar",
            "standard_charge | negotiated_percentage",
            "median_amount",
            ["10th_percentile", "90th_percentile", "count"],
        ),
        InputFormat::WideCsv => (
            "standard_charge|Payer, Inc.|Plan A|negotiated_dollar",
            "standard_charge|Payer, Inc.|Plan A|negotiated_percentage",
            "median_amount|Payer, Inc.|Plan A",
            [
                "10th_percentile|Payer, Inc.|Plan A",
                "90th_percentile|Payer, Inc.|Plan A",
                "count|Payer, Inc.|Plan A",
            ],
        ),
        InputFormat::Json => unreachable!(),
    };
    let dollar_index = csv_fixture_index(&records[2], dollar);
    let percentage_index = csv_fixture_index(&records[2], percentage);
    let estimated_index = csv_fixture_index(&records[2], estimated);
    records[3][dollar_index].clear();
    records[3][percentage_index] = "80".to_owned();
    records[2][estimated_index] = match format {
        InputFormat::TallCsv => "estimated_amount".to_owned(),
        InputFormat::WideCsv => "estimated_amount|Payer, Inc.|Plan A".to_owned(),
        InputFormat::Json => unreachable!(),
    };
    records[3][estimated_index] = "9.125".to_owned();
    for header in profile_only {
        let index = csv_fixture_index(&records[2], header);
        records[2][index].clear();
        records[3][index].clear();
    }
    csv_fixture_bytes(&records)
}

#[test]
fn cms_json_v2_profiles_emit_legacy_metadata_and_estimated_amount() {
    for version in ["2.2.0", "2.2.1"] {
        let (rows, summary) =
            run_fixture_with_summary(InputFormat::Json, &fixture_v2_json(version), false);
        assert_eq!(summary.schema_version, version);
        let mrf = String::from_utf8(rows["mrf"].clone()).unwrap();
        let mrf = mrf.trim_end().split('\t').collect::<Vec<_>>();
        assert_eq!(mrf[3], version);
        assert_eq!(mrf[6], "\\N");
        assert!(rows["npi"].is_empty());
        assert!(String::from_utf8(rows["service"].clone())
            .unwrap()
            .contains("\t2.5\tML\n"));
        let payer = String::from_utf8(rows["payer_charge"].clone()).unwrap();
        let payer = payer.trim_end().split('\t').collect::<Vec<_>>();
        let estimated = PAYER_CHARGE_COPY_COLUMNS
            .iter()
            .position(|column| *column == "estimated_amount")
            .expect("estimated_amount output column");
        assert_eq!(payer[estimated], "9.125");
    }
}

#[test]
fn cms_json_profiles_reject_mixed_shapes_and_profile_specific_values() {
    let mut v3_shape: serde_json::Value = serde_json::from_slice(&fixture_json()).unwrap();
    v3_shape["version"] = json!("2.2.1");
    assert_import_error(
        InputFormat::Json,
        &serde_json::to_vec(&v3_shape).unwrap(),
        DEFAULT_MAX_FANOUT_ROWS,
        "mixes CMS JSON profiles",
    );

    let mut missing_estimated: serde_json::Value =
        serde_json::from_slice(&fixture_v2_json("2.2.1")).unwrap();
    missing_estimated["standard_charge_information"][0]["standard_charges"][0]
        ["payers_information"][0]
        .as_object_mut()
        .unwrap()
        .remove("estimated_amount");
    assert_import_error(
        InputFormat::Json,
        &serde_json::to_vec(&missing_estimated).unwrap(),
        DEFAULT_MAX_FANOUT_ROWS,
        "percentage and algorithm charges require estimated_amount",
    );

    let mut numeric_v2_drug: serde_json::Value =
        serde_json::from_slice(&fixture_v2_json("2.2.1")).unwrap();
    numeric_v2_drug["standard_charge_information"][0]["drug_information"]["unit"] = json!(2.5);
    assert_import_error(
        InputFormat::Json,
        &serde_json::to_vec(&numeric_v2_drug).unwrap(),
        DEFAULT_MAX_FANOUT_ROWS,
        "CMS JSON v2 drug unit must be a string",
    );

    let mut estimated_v3: serde_json::Value = serde_json::from_slice(&fixture_json()).unwrap();
    estimated_v3["standard_charge_information"][0]["standard_charges"][0]
        ["payers_information"][0]["estimated_amount"] = json!(9.125);
    assert_import_error(
        InputFormat::Json,
        &serde_json::to_vec(&estimated_v3).unwrap(),
        DEFAULT_MAX_FANOUT_ROWS,
        "mixes CMS JSON profiles",
    );

    let mut modifier_v2: serde_json::Value =
        serde_json::from_slice(&fixture_v2_json("2.2.1")).unwrap();
    modifier_v2["standard_charge_information"][0]["standard_charges"][0]["modifier_code"] =
        json!(["26"]);
    assert_import_error(
        InputFormat::Json,
        &serde_json::to_vec(&modifier_v2).unwrap(),
        DEFAULT_MAX_FANOUT_ROWS,
        "mixes CMS JSON profiles",
    );
}

#[test]
fn json_2_0_remains_unsupported() {
    let mut payload: serde_json::Value = serde_json::from_slice(&fixture_json()).unwrap();
    payload["version"] = json!("2.0.0");
    assert_import_error(
        InputFormat::Json,
        &serde_json::to_vec(&payload).unwrap(),
        DEFAULT_MAX_FANOUT_ROWS,
        "unsupported CMS JSON version",
    );
}

#[test]
fn cms_csv_v2_tall_and_wide_preserve_declared_version_and_estimated_amount() {
    for version in ["2.0.0", "2.2.0", "2.2.1"] {
        let (tall, tall_summary) = run_fixture_with_summary(
            InputFormat::TallCsv,
            &fixture_v2_csv(InputFormat::TallCsv, version),
            false,
        );
        let (wide, wide_summary) = run_fixture_with_summary(
            InputFormat::WideCsv,
            &fixture_v2_csv(InputFormat::WideCsv, version),
            false,
        );
        assert_eq!(tall, wide);
        assert_eq!(tall_summary.schema_version, version);
        assert_eq!(wide_summary.schema_version, version);
        assert!(tall["npi"].is_empty());
        let mrf = String::from_utf8(tall["mrf"].clone()).unwrap();
        let mrf = mrf.trim_end().split('\t').collect::<Vec<_>>();
        assert_eq!(mrf[3], version);
        assert_eq!(mrf[4], AFFIRMATION_TEXT);
        assert_eq!(mrf[6], "\\N");
        let payer = String::from_utf8(tall["payer_charge"].clone()).unwrap();
        let payer = payer.trim_end().split('\t').collect::<Vec<_>>();
        for (column, expected) in [
            ("estimated_amount", "9.125"),
            ("median_amount", "\\N"),
            ("percentile_10", "\\N"),
            ("percentile_90", "\\N"),
            ("allowed_count", "\\N"),
        ] {
            let index = PAYER_CHARGE_COPY_COLUMNS
                .iter()
                .position(|candidate| *candidate == column)
                .unwrap();
            assert_eq!(payer[index], expected);
        }
    }
}

#[test]
fn cms_csv_v2_preserves_optional_forward_metadata_without_changing_profile() {
    let v3_records = csv_fixture_records(&fixture_tall_csv());
    let npi_index = csv_fixture_index(&v3_records[0], "type_2_npi");
    let attester_index = csv_fixture_index(&v3_records[0], "attester_name");
    for format in [InputFormat::TallCsv, InputFormat::WideCsv] {
        let mut records = csv_fixture_records(&fixture_v2_csv(format, "2.0.0"));
        records[0][npi_index] = "type_2_npi".to_owned();
        records[1][npi_index] = "1407430291".to_owned();
        records[0][attester_index] = "attester_name".to_owned();
        records[1][attester_index] = "Ben Levin".to_owned();

        let (rows, summary) = run_fixture_with_summary(
            format,
            &csv_fixture_bytes(&records),
            false,
        );
        assert_eq!(summary.schema_version, "2.0.0");
        assert_eq!(
            String::from_utf8(rows["npi"].clone()).unwrap(),
            "fixture-version\t0\t1407430291\n"
        );
        let mrf = String::from_utf8(rows["mrf"].clone()).unwrap();
        let mrf = mrf.trim_end().split('\t').collect::<Vec<_>>();
        assert_eq!(mrf[3], "2.0.0");
        assert_eq!(mrf[4], AFFIRMATION_TEXT);
        assert_eq!(mrf[6], "Ben Levin");
        let payer = String::from_utf8(rows["payer_charge"].clone()).unwrap();
        let payer = payer.trim_end().split('\t').collect::<Vec<_>>();
        let estimated = PAYER_CHARGE_COPY_COLUMNS
            .iter()
            .position(|column| *column == "estimated_amount")
            .unwrap();
        assert_eq!(payer[estimated], "9.125");
    }
}

#[test]
fn cms_csv_profiles_reject_mixed_and_unsupported_headers() {
    for format in [InputFormat::TallCsv, InputFormat::WideCsv] {
        let mut records = csv_fixture_records(&fixture_v2_csv(format, "2.0.0"));
        let estimated = records[2]
            .iter()
            .position(|header| header.starts_with("estimated_amount"))
            .unwrap();
        records[2][estimated] = match format {
            InputFormat::TallCsv => "median_amount".to_owned(),
            InputFormat::WideCsv => "median_amount|Payer, Inc.|Plan A".to_owned(),
            InputFormat::Json => unreachable!(),
        };
        assert_import_error(
            format,
            &csv_fixture_bytes(&records),
            DEFAULT_MAX_FANOUT_ROWS,
            "mix V2 and V3 payer profiles",
        );

        let payload = match format {
            InputFormat::TallCsv => fixture_tall_csv(),
            InputFormat::WideCsv => fixture_wide_csv(),
            InputFormat::Json => unreachable!(),
        };
        let mut records = csv_fixture_records(&payload);
        let median = records[2]
            .iter()
            .position(|header| header.starts_with("median_amount"))
            .unwrap();
        records[2][median] = match format {
            InputFormat::TallCsv => "estimated_amount".to_owned(),
            InputFormat::WideCsv => "estimated_amount|Payer, Inc.|Plan A".to_owned(),
            InputFormat::Json => unreachable!(),
        };
        assert_import_error(
            format,
            &csv_fixture_bytes(&records),
            DEFAULT_MAX_FANOUT_ROWS,
            "mix V2 and V3 payer profiles",
        );
    }

    let mut mixed_metadata = csv_fixture_records(&fixture_v2_csv(InputFormat::TallCsv, "2.0.0"));
    let location = csv_fixture_index(&mixed_metadata[0], "hospital_location");
    mixed_metadata[0][location] = "location_name".to_owned();
    assert_import_error(
        InputFormat::TallCsv,
        &csv_fixture_bytes(&mixed_metadata),
        DEFAULT_MAX_FANOUT_ROWS,
        "headers mix V2 and V3 profiles",
    );

    let mut unsupported = csv_fixture_records(&fixture_tall_csv());
    let version = csv_fixture_index(&unsupported[0], "version");
    unsupported[1][version] = "2.1.0".to_owned();
    assert_import_error(
        InputFormat::TallCsv,
        &csv_fixture_bytes(&unsupported),
        DEFAULT_MAX_FANOUT_ROWS,
        "unsupported CMS CSV version",
    );

    let mut missing_estimated =
        csv_fixture_records(&fixture_v2_csv(InputFormat::TallCsv, "2.0.0"));
    let estimated = csv_fixture_index(&missing_estimated[2], "estimated_amount");
    missing_estimated[3][estimated].clear();
    assert_import_error(
        InputFormat::TallCsv,
        &csv_fixture_bytes(&missing_estimated),
        DEFAULT_MAX_FANOUT_ROWS,
        "percentage and algorithm charges require estimated_amount",
    );
}

#[test]
fn compressed_csv_v2_matches_plain_and_preserves_detected_version() {
    let payload = fixture_v2_csv(InputFormat::TallCsv, "2.0.0");
    let (plain, plain_summary) =
        run_fixture_with_summary(InputFormat::TallCsv, &payload, false);
    let (compressed, compressed_summary) =
        run_fixture_with_summary(InputFormat::TallCsv, &payload, true);
    assert_eq!(plain, compressed);
    assert_eq!(plain_summary.schema_version, "2.0.0");
    assert_eq!(compressed_summary.schema_version, "2.0.0");
}

fn assert_estimated_only_payer(rows: &BTreeMap<String, Vec<u8>>) {
    let payer = String::from_utf8(rows["payer_charge"].clone()).unwrap();
    let payer = payer.trim_end().split('\t').collect::<Vec<_>>();
    assert_eq!(payer.len(), PAYER_CHARGE_COPY_COLUMNS.len());
    for (column, expected) in [
        ("standard_charge_dollar", "\\N"),
        ("standard_charge_percentage", "\\N"),
        ("standard_charge_algorithm", "\\N"),
        ("estimated_amount", "9.125"),
    ] {
        let index = PAYER_CHARGE_COPY_COLUMNS
            .iter()
            .position(|candidate| *candidate == column)
            .unwrap();
        assert_eq!(payer[index], expected);
    }
}

#[test]
fn v2_estimated_only_payers_are_retained_in_json_tall_and_wide() {
    let mut json_payload: serde_json::Value =
        serde_json::from_slice(&fixture_v2_json("2.2.1")).unwrap();
    json_payload["standard_charge_information"][0]["standard_charges"][0]
        ["payers_information"][0]
        .as_object_mut()
        .unwrap()
        .remove("standard_charge_percentage");
    assert_estimated_only_payer(&run_fixture(
        InputFormat::Json,
        &serde_json::to_vec(&json_payload).unwrap(),
        false,
    ));

    for format in [InputFormat::TallCsv, InputFormat::WideCsv] {
        let mut records = csv_fixture_records(&fixture_v2_csv(format, "2.0.0"));
        let percentage = records[2]
            .iter()
            .position(|header| header.contains("negotiated_percentage"))
            .unwrap();
        records[3][percentage].clear();
        assert_estimated_only_payer(&run_fixture(
            format,
            &csv_fixture_bytes(&records),
            false,
        ));
    }

    let mut v3_payload: serde_json::Value = serde_json::from_slice(&fixture_json()).unwrap();
    let payer = v3_payload["standard_charge_information"][0]["standard_charges"][0]
        ["payers_information"][0]
        .as_object_mut()
        .unwrap();
    payer.remove("standard_charge_dollar");
    payer.insert("estimated_amount".to_owned(), json!(9.125));
    assert_import_error(
        InputFormat::Json,
        &serde_json::to_vec(&v3_payload).unwrap(),
        DEFAULT_MAX_FANOUT_ROWS,
        "mixes CMS JSON profiles",
    );
}

#[test]
fn v2_charge_free_payers_are_omitted_and_v3_rejects_them() {
    let mut v2_json: serde_json::Value =
        serde_json::from_slice(&fixture_v2_json("2.2.1")).unwrap();
    let charge = v2_json["standard_charge_information"][0]["standard_charges"][0]
        .as_object_mut()
        .unwrap();
    charge.insert("gross_charge".to_owned(), json!(12.34));
    let payer = charge["payers_information"][0].as_object_mut().unwrap();
    payer.remove("standard_charge_percentage");
    payer.remove("estimated_amount");
    assert!(run_fixture(
        InputFormat::Json,
        &serde_json::to_vec(&v2_json).unwrap(),
        false,
    )["payer_charge"]
        .is_empty());

    let mut v3_json: serde_json::Value = serde_json::from_slice(&fixture_json()).unwrap();
    v3_json["standard_charge_information"][0]["standard_charges"][0]
        ["payers_information"][0]
        .as_object_mut()
        .unwrap()
        .remove("standard_charge_dollar");
    assert_import_error(
        InputFormat::Json,
        &serde_json::to_vec(&v3_json).unwrap(),
        DEFAULT_MAX_FANOUT_ROWS,
        "payer information requires dollar, percentage, algorithm, or estimated charge",
    );

    for format in [InputFormat::TallCsv, InputFormat::WideCsv] {
        let mut v2_records = csv_fixture_records(&fixture_v2_csv(format, "2.0.0"));
        for charge_header in [
            "negotiated_dollar",
            "negotiated_percentage",
            "estimated_amount",
        ] {
            let index = v2_records[2]
                .iter()
                .position(|header| header.contains(charge_header))
                .unwrap();
            v2_records[3][index].clear();
        }
        assert!(run_fixture(format, &csv_fixture_bytes(&v2_records), false)
            ["payer_charge"]
            .is_empty());

    }

    let mut v3_tall = csv_fixture_records(&fixture_tall_csv());
    let dollar = v3_tall[2]
        .iter()
        .position(|header| header.contains("negotiated_dollar"))
        .unwrap();
    v3_tall[3][dollar].clear();
    assert_import_error(
        InputFormat::TallCsv,
        &csv_fixture_bytes(&v3_tall),
        DEFAULT_MAX_FANOUT_ROWS,
        "payer information requires dollar, percentage, algorithm, or estimated charge",
    );
}

#[test]
fn v3_wide_empty_payer_columns_are_ignored() {
    let mut records = csv_fixture_records(&fixture_wide_csv());
    let payer_columns = records[2]
        .iter()
        .enumerate()
        .filter_map(|(index, header)| header.contains("Payer, Inc.|Plan A").then_some(index))
        .collect::<Vec<_>>();
    for index in payer_columns {
        records[3][index].clear();
    }
    assert!(run_fixture(InputFormat::WideCsv, &csv_fixture_bytes(&records), false)
        ["payer_charge"]
        .is_empty());
}

#[test]
fn v3_only_code_types_are_rejected_by_declared_v2_profiles() {
    for code_type in ["CMG", "MS-LTC-DRG"] {
        let mut v2_json: serde_json::Value =
            serde_json::from_slice(&fixture_v2_json("2.2.1")).unwrap();
        v2_json["standard_charge_information"][0]["code_information"][0]["type"] =
            json!(code_type);
        assert_import_error(
            InputFormat::Json,
            &serde_json::to_vec(&v2_json).unwrap(),
            DEFAULT_MAX_FANOUT_ROWS,
            "mixes CMS JSON profiles",
        );

        let mut v3_json: serde_json::Value = serde_json::from_slice(&fixture_json()).unwrap();
        v3_json["standard_charge_information"][0]["code_information"][0]["type"] =
            json!(code_type);
        run_fixture(
            InputFormat::Json,
            &serde_json::to_vec(&v3_json).unwrap(),
            false,
        );

        for format in [InputFormat::TallCsv, InputFormat::WideCsv] {
            let mut v2_records = csv_fixture_records(&fixture_v2_csv(format, "2.0.0"));
            let code_type_column = v2_records[2]
                .iter()
                .position(|header| header.replace(' ', "") == "code|1|type")
                .unwrap();
            v2_records[3][code_type_column] = code_type.to_owned();
            assert_import_error(
                format,
                &csv_fixture_bytes(&v2_records),
                DEFAULT_MAX_FANOUT_ROWS,
                "V3-only code type",
            );

            let payload = match format {
                InputFormat::TallCsv => fixture_tall_csv(),
                InputFormat::WideCsv => fixture_wide_csv(),
                InputFormat::Json => unreachable!(),
            };
            let mut v3_records = csv_fixture_records(&payload);
            let code_type_column = v3_records[2]
                .iter()
                .position(|header| header.replace(' ', "") == "code|1|type")
                .unwrap();
            v3_records[3][code_type_column] = code_type.to_owned();
            run_fixture(format, &csv_fixture_bytes(&v3_records), false);
        }
    }
}

#[test]
fn v2_json_rejects_v3_only_modifier_setting_and_attester_name() {
    let mut modifier_payload: serde_json::Value =
        serde_json::from_slice(&fixture_v2_json("2.2.1")).unwrap();
    modifier_payload["modifier_information"] = json!([{
        "code": "25",
        "description": "Professional component",
        "setting": "both",
        "modifier_payer_information": [{
            "payer_name": "Payer, Inc.",
            "plan_name": "Plan A",
            "description": "Contract note"
        }]
    }]);
    assert_import_error(
        InputFormat::Json,
        &serde_json::to_vec(&modifier_payload).unwrap(),
        DEFAULT_MAX_FANOUT_ROWS,
        "modifier_information.setting",
    );

    let mut attester_payload: serde_json::Value =
        serde_json::from_slice(&fixture_v2_json("2.2.1")).unwrap();
    attester_payload["affirmation"]["attester_name"] = json!("Legacy Attester");
    assert_import_error(
        InputFormat::Json,
        &serde_json::to_vec(&attester_payload).unwrap(),
        DEFAULT_MAX_FANOUT_ROWS,
        "affirmation.attester_name",
    );

    let mut missing_v3_attester: serde_json::Value =
        serde_json::from_slice(&fixture_json()).unwrap();
    missing_v3_attester["attestation"]
        .as_object_mut()
        .unwrap()
        .remove("attester_name");
    assert_import_error(
        InputFormat::Json,
        &serde_json::to_vec(&missing_v3_attester).unwrap(),
        DEFAULT_MAX_FANOUT_ROWS,
        "missing attester_name",
    );
}
