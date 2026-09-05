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
fn v3_tall_explicitly_uncontracted_payer_label_is_ignored() {
    let mut records = csv_fixture_records(&fixture_tall_csv());
    for header in [
        "plan_name",
        "standard_charge | negotiated_dollar",
        "standard_charge | methodology",
    ] {
        let index = csv_fixture_index(&records[2], header);
        records[3][index].clear();
    }
    let notes = csv_fixture_index(&records[2], "additional_generic_notes");
    records[3][notes] =
        "NOT CONTRACTED, ALL SERVICES ARE BUNDLED INTO A PER DIEM RATE".to_owned();

    let rows = run_fixture(
        InputFormat::TallCsv,
        &csv_fixture_bytes(&records),
        false,
    );
    assert!(rows["payer_charge"].is_empty());
    let charge = String::from_utf8(rows["charge"].clone()).unwrap();
    assert!(charge.contains("12.34\t10.5\t8.001\t9.999"));
    assert!(charge.contains("NOT CONTRACTED, ALL SERVICES ARE BUNDLED INTO A PER DIEM RATE"));

    records[3][notes] =
        "NOT CONTRACTED, ALL SERVICES ARE BUNDLED INTO A PER DIEM RATE".to_ascii_lowercase();
    assert_import_error(
        InputFormat::TallCsv,
        &csv_fixture_bytes(&records),
        DEFAULT_MAX_FANOUT_ROWS,
        "plan_name must be a non-empty string",
    );
}

#[test]
fn v2_tall_charge_free_payer_label_does_not_require_plan_name() {
    let mut records = csv_fixture_records(&fixture_v2_csv(InputFormat::TallCsv, "1"));
    for header in [
        "plan_name",
        "standard_charge | negotiated_percentage",
        "estimated_amount",
    ] {
        let index = csv_fixture_index(&records[2], header);
        records[3][index].clear();
    }

    assert!(run_fixture(
        InputFormat::TallCsv,
        &csv_fixture_bytes(&records),
        false,
    )["payer_charge"]
        .is_empty());

    let methodology = csv_fixture_index(&records[2], "standard_charge | methodology");
    records[3][methodology] = "unsupported".to_owned();
    assert_import_error(
        InputFormat::TallCsv,
        &csv_fixture_bytes(&records),
        DEFAULT_MAX_FANOUT_ROWS,
        "invalid standard charge methodology",
    );

    records[3][methodology] = "fee schedule".to_owned();
    let dollar = csv_fixture_index(&records[2], "standard_charge | negotiated_dollar");
    records[3][dollar] = "9.125".to_owned();
    assert_import_error(
        InputFormat::TallCsv,
        &csv_fixture_bytes(&records),
        DEFAULT_MAX_FANOUT_ROWS,
        "plan_name must be a non-empty string",
    );
}
