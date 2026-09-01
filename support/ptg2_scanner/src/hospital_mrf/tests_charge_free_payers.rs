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
        if format == InputFormat::TallCsv {
            let methodology = v2_records[2]
                .iter()
                .position(|header| header.contains("methodology"))
                .unwrap();
            v2_records[3][methodology].clear();
        }
        assert!(run_fixture(format, &csv_fixture_bytes(&v2_records), false)
            ["payer_charge"]
            .is_empty());
    }

    let mut estimated_only_v2_tall =
        csv_fixture_records(&fixture_v2_csv(InputFormat::TallCsv, "2.0.0"));
    for header in ["negotiated_percentage", "methodology"] {
        let index = estimated_only_v2_tall[2]
            .iter()
            .position(|candidate| candidate.contains(header))
            .unwrap();
        estimated_only_v2_tall[3][index].clear();
    }
    assert_import_error(
        InputFormat::TallCsv,
        &csv_fixture_bytes(&estimated_only_v2_tall),
        DEFAULT_MAX_FANOUT_ROWS,
        "invalid standard charge methodology",
    );

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
