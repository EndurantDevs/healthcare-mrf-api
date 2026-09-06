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

fn service_only_tall_records() -> Vec<Vec<String>> {
    let mut records = csv_fixture_records(&fixture_tall_csv());
    for (header, value) in [
        ("payer_name", "All Payers / All Plans"),
        ("plan_name", ""),
        ("standard_charge | negotiated_dollar", ""),
        ("standard_charge | methodology", ""),
        ("additional_generic_notes", "Published service charge note"),
    ] {
        let index = csv_fixture_index(&records[2], header);
        records[3][index] = value.to_owned();
    }
    records
}

#[test]
fn v3_tall_service_only_preserves_charges() {
    for version in ["2.0.0", "3.0.0"] {
        let mut records = service_only_tall_records();
        historical_csv_metadata(&mut records, "version", version);
        let payer = csv_fixture_index(&records[2], "payer_name");
        records[3][payer].clear();
        let control = csv_fixture_bytes(&records);
        let baseline = run_fixture(InputFormat::TallCsv, &control, false);
        let (_control_directory, packed_control) =
            import_packed(InputFormat::TallCsv, &control, TEST_MAX_OUTPUT_BYTES);
        for label in ["All Payers / All Plans", "  All Payers / All Plans  "] {
            records[3][payer] = label.to_owned();
            let payload = csv_fixture_bytes(&records);
            let (rows, summary) = run_fixture_with_summary(InputFormat::TallCsv, &payload, false);
            assert_eq!(summary.schema_version, version);
            assert_eq!(rows, baseline);
            assert!(rows["payer_charge"].is_empty());
            assert!(String::from_utf8(rows["charge"].clone())
                .unwrap()
                .contains("Published service charge note"));
            let (_directory, packed) =
                import_packed(InputFormat::TallCsv, &payload, TEST_MAX_OUTPUT_BYTES);
            let root = packed.root.as_ref().unwrap();
            assert_eq!(
                (root.service_count, root.charge_count, root.fact_count),
                (1, 1, 0)
            );
            assert_eq!(packed.schema_version, version);
            assert_eq!(
                packed
                    .artifacts
                    .iter()
                    .map(|a| (a.kind, &a.sha256))
                    .collect::<Vec<_>>(),
                packed_control
                    .artifacts
                    .iter()
                    .map(|a| (a.kind, &a.sha256))
                    .collect::<Vec<_>>()
            );
        }
    }
}

#[test]
fn v3_tall_service_only_rejects_values() {
    let records = service_only_tall_records();
    for field in [
        "standard_charge | negotiated_dollar",
        "standard_charge | negotiated_percentage",
        "standard_charge | negotiated_algorithm",
        "median_amount",
        "10th_percentile",
        "90th_percentile",
        "count",
        "standard_charge | methodology",
    ] {
        for value in ["0", "1"] {
            let mut invalid = records.clone();
            let index = csv_fixture_index(&invalid[2], field);
            invalid[3][index] = value.to_owned();
            let error = if value == "0"
                && !matches!(
                    field,
                    "standard_charge | negotiated_algorithm"
                        | "standard_charge | methodology"
                        | "count"
                ) {
                "must be greater than zero"
            } else if field == "count" && value == "1" {
                "count values from 1 through 10 must use the literal 1 through 10"
            } else {
                "plan_name must be a non-empty string"
            };
            assert_historical_csv_error(InputFormat::TallCsv, &invalid, error);
        }
    }
    for value in ["", "0", "1"] {
        let mut invalid = records.clone();
        for record in &mut invalid {
            record.push(String::new());
        }
        *invalid[2].last_mut().unwrap() = "estimated_amount".to_owned();
        *invalid[3].last_mut().unwrap() = value.to_owned();
        assert_historical_csv_error(
            InputFormat::TallCsv,
            &invalid,
            "headers mix V2 and V3 payer profiles",
        );
    }
}

#[test]
fn v3_tall_service_only_requires_exact_identity() {
    let records = service_only_tall_records();
    for label in ["All Payers", "all payers / all plans", "Payer, Inc."] {
        let mut invalid = records.clone();
        let index = csv_fixture_index(&invalid[2], "payer_name");
        invalid[3][index] = label.to_owned();
        assert_historical_csv_error(
            InputFormat::TallCsv,
            &invalid,
            "plan_name must be a non-empty string",
        );
    }
    let mut invalid = records.clone();
    let plan = csv_fixture_index(&invalid[2], "plan_name");
    invalid[3][plan] = "All Plans".to_owned();
    assert_historical_csv_error(
        InputFormat::TallCsv,
        &invalid,
        "invalid standard charge methodology",
    );
    let mut invalid = records;
    for field in [
        "standard_charge | gross",
        "standard_charge | discounted_cash",
    ] {
        let index = csv_fixture_index(&invalid[2], field);
        invalid[3][index].clear();
    }
    assert_historical_csv_error(
        InputFormat::TallCsv,
        &invalid,
        "standard charge requires gross, discounted cash, or payer information",
    );
}

#[test]
fn service_only_label_does_not_relax_json() {
    let mut payload: serde_json::Value = serde_json::from_slice(&fixture_json()).unwrap();
    let payer = payload["standard_charge_information"][0]["standard_charges"][0]
        ["payers_information"][0]
        .as_object_mut()
        .unwrap();
    payer.insert("payer_name".to_owned(), json!("All Payers / All Plans"));
    payer.insert("plan_name".to_owned(), json!(""));
    payer.insert("methodology".to_owned(), json!(""));
    payer.remove("standard_charge_dollar");
    assert_import_error(
        InputFormat::Json,
        &serde_json::to_vec(&payload).unwrap(),
        DEFAULT_MAX_FANOUT_ROWS,
        "plan_name must be a non-empty string",
    );
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

    let methodology = csv_fixture_index(&records[2], "standard_charge | methodology");
    records[3][methodology] = "PER DIEM".to_owned();
    let rows = run_fixture(
        InputFormat::TallCsv,
        &csv_fixture_bytes(&records),
        false,
    );
    assert!(rows["payer_charge"].is_empty());
    let charge = String::from_utf8(rows["charge"].clone()).unwrap();
    assert!(charge.contains("12.34\t10.5\t8.001\t9.999"));
    assert!(charge.contains("NOT CONTRACTED, ALL SERVICES ARE BUNDLED INTO A PER DIEM RATE"));

    let dollar = csv_fixture_index(&records[2], "standard_charge | negotiated_dollar");
    records[3][dollar] = "9.125".to_owned();
    assert_import_error(
        InputFormat::TallCsv,
        &csv_fixture_bytes(&records),
        DEFAULT_MAX_FANOUT_ROWS,
        "plan_name must be a non-empty string",
    );
    records[3][dollar].clear();

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

#[test]
fn v2_tall_gross_charge_marker_retains_service_prices() {
    let mut records = csv_fixture_records(&fixture_v2_csv(InputFormat::TallCsv, "2.0.0"));
    for header in [
        "payer_name",
        "plan_name",
        "standard_charge | negotiated_percentage",
        "estimated_amount",
    ] {
        let index = csv_fixture_index(&records[2], header);
        records[3][index].clear();
    }
    let methodology = csv_fixture_index(&records[2], "standard_charge | methodology");
    records[3][methodology] = "gross charge".to_owned();
    let notes = csv_fixture_index(&records[2], "additional_generic_notes");
    records[3][notes] = "IP/OP DISCOUNT BASED ON SELF PAY CONTRACT(S).".to_owned();
    for blank_identity in ["", " "] {
        for header in ["payer_name", "plan_name"] {
            let index = csv_fixture_index(&records[2], header);
            records[3][index] = blank_identity.to_owned();
        }
        let rows = run_fixture(InputFormat::TallCsv, &csv_fixture_bytes(&records), false);
        assert!(rows["payer_charge"].is_empty());
        let charge = String::from_utf8(rows["charge"].clone()).unwrap();
        assert!(charge.contains("12.34\t10.5\t8.001\t9.999"));
        assert!(charge.contains("IP/OP DISCOUNT BASED ON SELF PAY CONTRACT(S)."));
    }

    for (header, value, error) in [
        ("payer_name", "Payer", "invalid standard charge methodology"),
        ("plan_name", "Plan", "invalid standard charge methodology"),
        (
            "standard_charge | negotiated_dollar",
            "8",
            "payer_name must be a non-empty string",
        ),
        (
            "standard_charge | negotiated_percentage",
            "80",
            "payer_name must be a non-empty string",
        ),
        (
            "estimated_amount",
            "8",
            "payer_name must be a non-empty string",
        ),
    ] {
        let mut invalid = records.clone();
        let index = csv_fixture_index(&invalid[2], header);
        invalid[3][index] = value.to_owned();
        assert_import_error(
            InputFormat::TallCsv,
            &csv_fixture_bytes(&invalid),
            DEFAULT_MAX_FANOUT_ROWS,
            error,
        );
    }

    let mut v3_records = csv_fixture_records(&fixture_tall_csv());
    v3_records[3] = records[3].clone();
    assert_import_error(
        InputFormat::TallCsv,
        &csv_fixture_bytes(&v3_records),
        DEFAULT_MAX_FANOUT_ROWS,
        "invalid standard charge methodology",
    );
}
