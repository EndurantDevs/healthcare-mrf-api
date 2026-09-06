fn duplicate_wide_fixture_columns(
    records: &mut [Vec<String>],
    columns: &[usize],
) -> Vec<(usize, usize)> {
    let mut pairs = Vec::new();
    for &first in columns {
        let duplicate = records[2].len();
        for (row, record) in records.iter_mut().enumerate() {
            let value = match row {
                0 | 1 => String::new(),
                2 => record[first].replace("Payer, Inc.|Plan A", "PAYER, INC.|plan a "),
                _ => record[first].clone(),
            };
            record.push(value);
        }
        pairs.push((first, duplicate));
    }
    pairs
}

#[test]
fn identical_wide_payer_columns_preserve_v2_v3_artifacts_and_order() {
    for payload in [
        fixture_wide_csv(),
        fixture_v2_csv(InputFormat::WideCsv, "2.2.0"),
    ] {
        let mut records = csv_fixture_records(&payload);
        let columns = records[2]
            .iter()
            .enumerate()
            .filter_map(|(column, header)| header.contains("Payer, Inc.|Plan A").then_some(column))
            .collect::<Vec<_>>();
        duplicate_wide_fixture_columns(&mut records, &columns);
        duplicate_wide_fixture_columns(&mut records, &columns);
        let duplicated = csv_fixture_bytes(&records);
        assert_eq!(
            run_fixture(InputFormat::WideCsv, &payload, false),
            run_fixture(InputFormat::WideCsv, &duplicated, false),
        );
        let (control, expected) =
            import_packed(InputFormat::WideCsv, &payload, TEST_MAX_OUTPUT_BYTES);
        for _ in 0..2 {
            let (candidate, actual) =
                import_packed(InputFormat::WideCsv, &duplicated, TEST_MAX_OUTPUT_BYTES);
            assert_eq!(
                serde_json::to_value(&expected.root).unwrap(),
                serde_json::to_value(&actual.root).unwrap()
            );
            assert_eq!(actual.root.as_ref().unwrap().fact_count, 1);
            for artifact in &expected.artifacts {
                assert_eq!(
                    fs::read(
                        control
                            .path()
                            .join("output")
                            .join(format!("{}.copy", artifact.kind))
                    )
                    .unwrap(),
                    fs::read(
                        candidate
                            .path()
                            .join("output")
                            .join(format!("{}.copy", artifact.kind))
                    )
                    .unwrap(),
                    "{}",
                    artifact.kind,
                );
            }
        }
    }
}

#[test]
fn wide_duplicate_fields_require_raw_equality_not_parsed_equivalence() {
    for payload in [
        fixture_wide_csv(),
        fixture_v2_csv(InputFormat::WideCsv, "2.2.0"),
    ] {
        let mut records = csv_fixture_records(&payload);
        let columns = records[2]
            .iter()
            .enumerate()
            .filter_map(|(column, header)| header.contains("Payer, Inc.|Plan A").then_some(column))
            .collect::<Vec<_>>();
        let pairs = duplicate_wide_fixture_columns(&mut records, &columns);
        for &(first, duplicate) in &pairs {
            let mut changed = records.clone();
            changed[3][duplicate].push(' ');
            assert_import_error(
                InputFormat::WideCsv,
                &csv_fixture_bytes(&changed),
                DEFAULT_MAX_FANOUT_ROWS,
                "must have identical present values",
            );
            for (left, right) in [
                ("", "1"),
                ("1", ""),
                ("1", "1.0"),
                ("0", "0.0"),
                ("NA", ""),
                ("null", ""),
            ] {
                changed[3][first] = left.to_owned();
                changed[3][duplicate] = right.to_owned();
                assert_import_error(
                    InputFormat::WideCsv,
                    &csv_fixture_bytes(&changed),
                    DEFAULT_MAX_FANOUT_ROWS,
                    "must have identical present values",
                );
            }
        }
        let third = duplicate_wide_fixture_columns(&mut records, &columns)[0].1;
        records[3][third].push(' ');
        assert_import_error(
            InputFormat::WideCsv,
            &csv_fixture_bytes(&records),
            DEFAULT_MAX_FANOUT_ROWS,
            "must have identical present values",
        );
    }
}

#[test]
fn wide_duplicate_columns_do_not_hide_missing_or_blank_records() {
    let mut records = csv_fixture_records(&fixture_wide_csv());
    let first = csv_fixture_index(
        &records[2],
        "standard_charge|Payer, Inc.|Plan A|negotiated_dollar",
    );
    let duplicate = duplicate_wide_fixture_columns(&mut records, &[first])[0].1;
    let expected = run_fixture(InputFormat::WideCsv, &csv_fixture_bytes(&records), false);
    records.push(vec![String::new(); records[2].len()]);
    assert_eq!(
        expected,
        run_fixture(InputFormat::WideCsv, &csv_fixture_bytes(&records), false)
    );
    records[4][duplicate] = " ".to_owned();
    assert_import_error(
        InputFormat::WideCsv,
        &csv_fixture_bytes(&records),
        DEFAULT_MAX_FANOUT_ROWS,
        "must have identical present values",
    );
    for width in [first, duplicate] {
        records[4] = vec![String::new(); width];
        let mut writer = csv::WriterBuilder::new()
            .has_headers(false)
            .flexible(true)
            .from_writer(Vec::new());
        for record in &records {
            writer.write_record(record).unwrap();
        }
        assert_import_error(
            InputFormat::WideCsv,
            &writer.into_inner().unwrap(),
            DEFAULT_MAX_FANOUT_ROWS,
            "must have identical present values",
        );
    }
    records.truncate(3);
    for blank in [false, true] {
        if blank {
            records.push(vec![String::new(); records[2].len()]);
        }
        assert_import_error(
            InputFormat::WideCsv,
            &csv_fixture_bytes(&records),
            DEFAULT_MAX_FANOUT_ROWS,
            "contains no standard charge rows",
        );
    }
}

#[test]
fn wide_duplicate_columns_keep_modifier_and_rate_term_semantics() {
    let payload = append_csv_row(
        &fixture_wide_csv(),
        &[
            ("description", "Modifier adjustment"),
            ("modifiers", "TC"),
            ("setting", "outpatient"),
            (
                "standard_charge|Payer, Inc.|Plan A|negotiated_percentage",
                "62.5",
            ),
        ],
    );
    let mut records = csv_fixture_records(&payload);
    let percentage = csv_fixture_index(
        &records[2],
        "standard_charge|Payer, Inc.|Plan A|negotiated_percentage",
    );
    let duplicate = duplicate_wide_fixture_columns(&mut records, &[percentage])[0].1;
    assert_eq!(
        run_fixture(InputFormat::WideCsv, &payload, false),
        run_fixture(InputFormat::WideCsv, &csv_fixture_bytes(&records), false)
    );
    records[4][duplicate] = "62.50".to_owned();
    assert_import_error(
        InputFormat::WideCsv,
        &csv_fixture_bytes(&records),
        DEFAULT_MAX_FANOUT_ROWS,
        "must have identical present values",
    );

    records = csv_fixture_records(&fixture_wide_csv());
    for header in &mut records[2] {
        if header.contains("Payer, Inc.|Plan A") {
            *header = header.replace("Payer, Inc.|Plan A", "Payer, Inc.|Plan A|[TERM A]");
        }
    }
    let control = csv_fixture_bytes(&records);
    let columns = records[2]
        .iter()
        .enumerate()
        .filter_map(|(column, header)| header.contains("Payer, Inc.|Plan A").then_some(column))
        .collect::<Vec<_>>();
    for (_, duplicate) in duplicate_wide_fixture_columns(&mut records, &columns) {
        records[2][duplicate] = records[2][duplicate].replace("[TERM A]", "[a]");
    }
    let (_, expected) = import_packed(InputFormat::WideCsv, &control, TEST_MAX_OUTPUT_BYTES);
    let (_, actual) = import_packed(
        InputFormat::WideCsv,
        &csv_fixture_bytes(&records),
        TEST_MAX_OUTPUT_BYTES,
    );
    assert_eq!(
        expected
            .artifacts
            .iter()
            .map(|artifact| (&artifact.kind, &artifact.sha256))
            .collect::<Vec<_>>(),
        actual
            .artifacts
            .iter()
            .map(|artifact| (&artifact.kind, &artifact.sha256))
            .collect::<Vec<_>>()
    );
}

#[test]
fn wide_duplicate_columns_keep_other_validation_and_input_limits() {
    let mut records = csv_fixture_records(&fixture_wide_csv());
    let dollar = csv_fixture_index(
        &records[2],
        "standard_charge|Payer, Inc.|Plan A|negotiated_dollar",
    );
    records[3][dollar] = "NaN".to_owned();
    duplicate_wide_fixture_columns(&mut records, &[dollar]);
    assert_import_error(
        InputFormat::WideCsv,
        &csv_fixture_bytes(&records),
        DEFAULT_MAX_FANOUT_ROWS,
        "standard_charge_dollar",
    );
    for field in ["description", "code|1", "drug_unit_of_measurement"] {
        records = csv_fixture_records(&fixture_wide_csv());
        let column = csv_fixture_index(&records[2], field);
        duplicate_wide_fixture_columns(&mut records, &[column]);
        assert_import_error(
            InputFormat::WideCsv,
            &csv_fixture_bytes(&records),
            DEFAULT_MAX_FANOUT_ROWS,
            "duplicate",
        );
    }
    records = csv_fixture_records(&fixture_wide_csv());
    duplicate_wide_fixture_columns(&mut records, &[dollar]);
    let payload = csv_fixture_bytes(&records);
    assert_payload_limit_error(
        InputFormat::WideCsv,
        &payload,
        1,
        "CSV record exceeds configured limit",
    );
    let payer_columns = records[2]
        .iter()
        .enumerate()
        .filter_map(|(column, header)| header.contains("Payer, Inc.|Plan A").then_some(column))
        .collect::<Vec<_>>();
    for (_, duplicate) in duplicate_wide_fixture_columns(&mut records, &payer_columns) {
        records[2][duplicate] =
            records[2][duplicate].replace("PAYER, INC.|plan a ", "Second Payer|Second Plan");
    }
    assert_import_error(
        InputFormat::WideCsv,
        &csv_fixture_bytes(&records),
        1,
        "payer fanout exceeds configured limit 1",
    );
}

#[test]
fn late_wide_duplicate_conflict_removes_packed_output() {
    let mut records = csv_fixture_records(&fixture_wide_csv());
    let dollar = csv_fixture_index(
        &records[2],
        "standard_charge|Payer, Inc.|Plan A|negotiated_dollar",
    );
    let duplicate = duplicate_wide_fixture_columns(&mut records, &[dollar])[0].1;
    for index in 0..crate::hospital_price_block::HOSPITAL_PRICE_FACT_BLOCK_MAX_ROWS + 2 {
        let mut row = records[3].clone();
        row[0] = format!("Service {index}");
        records.push(row);
    }
    records.last_mut().unwrap()[duplicate] = "9.1250".to_owned();
    let directory = tempfile::tempdir().unwrap();
    let input = directory.path().join("input.csv");
    let output = directory.path().join("output");
    fs::write(&input, csv_fixture_bytes(&records)).unwrap();
    fs::create_dir(&output).unwrap();
    let error = import_hospital_mrf_with_output_mode(
        InputFormat::WideCsv,
        VERSION_ID,
        &input,
        &output,
        HospitalMrfLimits::new(
            DEFAULT_MAX_FANOUT_ROWS,
            TEST_MAX_DECOMPRESSED_BYTES,
            TEST_MAX_OUTPUT_BYTES,
        ),
        HospitalMrfOutputMode::Packed,
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("must have identical present values"),
        "{error}"
    );
    assert_eq!(fs::read_dir(output).unwrap().count(), 0);
}
