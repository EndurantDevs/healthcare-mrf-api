fn historical_csv_column(records: &[Vec<String>], field: &str) -> usize {
    records[2]
        .iter()
        .position(|header| header.split('|').any(|part| part.trim() == field))
        .expect("missing historical fixture column")
}

fn historical_csv_metadata(records: &mut [Vec<String>], field: &str, value: &str) {
    let index = csv_fixture_index(&records[0], field);
    records[1][index] = value.to_owned();
}

fn historical_csv_records(format: InputFormat, rate_field: &str) -> Vec<Vec<String>> {
    let mut records = csv_fixture_records(&fixture_v2_csv(format, "2.0.0"));
    historical_csv_metadata(&mut records, "last_updated_on", "2024-12-31");
    for field in ["negotiated_percentage", "estimated_amount"] {
        let index = historical_csv_column(&records, field);
        records[3][index].clear();
    }
    let index = historical_csv_column(&records, rate_field);
    records[3][index] = match rate_field {
        "negotiated_percentage" => "80",
        "negotiated_algorithm" => "80% of Medicare fee schedule",
        _ => panic!("historical fixture requires a derived rate"),
    }
    .to_owned();
    let notes = historical_csv_column(&records, "additional_generic_notes");
    records[3][notes] = "Source rate terms".to_owned();
    if format == InputFormat::WideCsv {
        // Tall payer-row notes become payer notes, not service notes.
        records[3][notes].clear();
        let notes = historical_csv_column(&records, "additional_payer_notes");
        records[3][notes] = "Source rate terms".to_owned();
    }
    records
}

fn assert_historical_csv_error(format: InputFormat, records: &[Vec<String>], message: &str) {
    assert_import_error(
        format,
        &csv_fixture_bytes(records),
        DEFAULT_MAX_FANOUT_ROWS,
        message,
    );
}

fn historical_packed_fact(directory: &Path) -> crate::hospital_price_block::HospitalPriceFactRow {
    let blocks = super::packed_output_tests::payloads(&directory.join("output/fact_block.copy"));
    assert_eq!(blocks.len(), 1);
    let facts = crate::hospital_price_block::decode_fact_block(
        &blocks[0],
        None,
        None,
        0,
        crate::hospital_price_block::HOSPITAL_PRICE_FACT_BLOCK_MAX_ROWS,
    )
    .unwrap();
    assert_eq!(facts.len(), 1);
    facts.into_iter().next().unwrap()
}

fn assert_historical_selectors(directory: &Path) {
    use crate::hospital_price_selector_block::{decode_selector_page, HospitalPriceSelectorKey};
    let blocks = super::packed_output_tests::payloads(&directory.join("output/selector_page.copy"));
    let entries = blocks
        .iter()
        .flat_map(|block| decode_selector_page(block).unwrap().entries)
        .map(|entry| (entry.key, entry.refs))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        entries,
        BTreeMap::from([
            (
                HospitalPriceSelectorKey::Code {
                    code_type: "CPT".to_owned(),
                    code: "70551".to_owned(),
                },
                vec![0]
            ),
            (
                HospitalPriceSelectorKey::PayerPlan {
                    payer_name: "Payer, Inc.".to_owned(),
                    plan_name: "Plan A".to_owned(),
                },
                vec![0]
            ),
        ])
    );
}

#[test]
fn historical_csv_packed_rates_are_deterministic_without_invented_estimates() {
    for rate_field in ["negotiated_percentage", "negotiated_algorithm"] {
        let mut expected_artifacts = None;
        for format in [
            InputFormat::TallCsv,
            InputFormat::WideCsv,
            InputFormat::TallCsv,
        ] {
            for has_estimate_header in [true, false] {
                let mut records = historical_csv_records(format, rate_field);
                if !has_estimate_header {
                    let index = historical_csv_column(&records, "estimated_amount");
                    for record in &mut records {
                        record.remove(index);
                    }
                }
                let (directory, summary) =
                    import_packed(format, &csv_fixture_bytes(&records), TEST_MAX_OUTPUT_BYTES);
                let artifacts = summary
                    .artifacts
                    .iter()
                    .map(|artifact| (artifact.kind, artifact.rows, artifact.sha256.clone()))
                    .collect::<Vec<_>>();
                assert_eq!(
                    expected_artifacts
                        .get_or_insert(artifacts.clone())
                        .as_slice(),
                    artifacts.as_slice()
                );
                let root = summary.root.as_ref().unwrap();
                assert_eq!(
                    (root.service_count, root.charge_count, root.fact_count),
                    (1, 1, 1)
                );
                let fact = historical_packed_fact(directory.path());
                assert_eq!(fact.charge_key, 0);
                assert_eq!(fact.payer_name, "Payer, Inc.");
                assert_eq!(fact.plan_name, "Plan A");
                assert_eq!(fact.negotiated_dollar, None);
                assert_eq!(fact.estimated_amount, None);
                assert_eq!(
                    fact.negotiated_percentage.as_deref(),
                    (rate_field == "negotiated_percentage").then_some("80")
                );
                assert_eq!(
                    fact.negotiated_algorithm.as_deref(),
                    (rate_field == "negotiated_algorithm")
                        .then_some("80% of Medicare fee schedule")
                );
                assert_eq!(
                    fact.additional_payer_notes.as_deref(),
                    Some("Source rate terms")
                );
                assert_eq!(fact.comparison_amount.as_deref(), Some("12.34"));
                assert_historical_selectors(directory.path());
            }
        }
    }
}

#[test]
fn historical_csv_keeps_null_comparisons_and_supplied_estimates_distinct() {
    for format in [InputFormat::TallCsv, InputFormat::WideCsv] {
        for estimate in [None, Some("9.125")] {
            let mut records = historical_csv_records(format, "negotiated_percentage");
            for field in ["gross", "discounted_cash", "min", "max"] {
                let index = historical_csv_column(&records, field);
                records[3][index].clear();
            }
            let index = historical_csv_column(&records, "estimated_amount");
            records[3][index] = estimate.unwrap_or_default().to_owned();
            let (directory, _) =
                import_packed(format, &csv_fixture_bytes(&records), TEST_MAX_OUTPUT_BYTES);
            let fact = historical_packed_fact(directory.path());
            assert_eq!(fact.negotiated_dollar, None);
            assert_eq!(fact.estimated_amount.as_deref(), estimate);
            assert_eq!(fact.comparison_amount.as_deref(), estimate);
            assert_eq!(fact.negotiated_percentage.as_deref(), Some("80"));
            assert_historical_selectors(directory.path());
        }
    }
}

#[test]
fn historical_csv_qualifies_only_valid_2024_dates() {
    for format in [InputFormat::TallCsv, InputFormat::WideCsv] {
        for (date, canonical) in [
            ("2024-01-01", "2024-01-01"),
            ("1/24/2024", "2024-01-24"),
            ("2024-02-29", "2024-02-29"),
            ("12/31/2024", "2024-12-31"),
            ("    2024-12-23", "2024-12-23"),
        ] {
            let mut records = historical_csv_records(format, "negotiated_percentage");
            historical_csv_metadata(&mut records, "last_updated_on", date);
            let rows = run_fixture(format, &csv_fixture_bytes(&records), false);
            assert!(String::from_utf8(rows["mrf"].clone())
                .unwrap()
                .contains(&format!("\t{canonical}\t")));
        }
        for date in ["2023-12-31", "2025-01-01", "2026-04-01"] {
            let mut records = historical_csv_records(format, "negotiated_percentage");
            historical_csv_metadata(&mut records, "last_updated_on", date);
            assert_historical_csv_error(format, &records, "require estimated_amount");
        }
    }
}

#[test]
fn historical_csv_rejects_malformed_or_conflicting_dates() {
    for date in [
        "",
        "unknown",
        "2024-02-30",
        "2024-13-01",
        "12-31/2024",
        "12/31-2024",
        "+1/2/2024",
        "2024-+1-02",
        "001/2/2024",
        "2024-01-002",
        "1/2/+2024",
    ] {
        let mut records = historical_csv_records(InputFormat::TallCsv, "negotiated_percentage");
        historical_csv_metadata(&mut records, "last_updated_on", date);
        assert_historical_csv_error(InputFormat::TallCsv, &records, "last_updated_on");
    }
    for duplicate in ["2024-12-31", "2025-01-01"] {
        let mut records = historical_csv_records(InputFormat::TallCsv, "negotiated_percentage");
        let index = records[0]
            .iter()
            .position(|header| header.is_empty())
            .unwrap();
        records[0][index] = "last_updated_on".to_owned();
        records[1][index] = duplicate.to_owned();
        assert_historical_csv_error(InputFormat::TallCsv, &records, "duplicate");
    }
}

#[test]
fn historical_csv_does_not_qualify_other_declared_versions() {
    for format in [InputFormat::TallCsv, InputFormat::WideCsv] {
        for version in ["1", "1.0.0", "2", "2.2.0", "2.2.1"] {
            let mut records = historical_csv_records(format, "negotiated_percentage");
            historical_csv_metadata(&mut records, "version", version);
            assert_historical_csv_error(format, &records, "require estimated_amount");
        }
        let mut records = historical_csv_records(format, "negotiated_percentage");
        historical_csv_metadata(&mut records, "version", "unknown");
        assert_historical_csv_error(format, &records, "unsupported CMS CSV version");
    }
}

#[test]
fn historical_csv_retains_identity_and_present_value_validation() {
    for format in [InputFormat::TallCsv, InputFormat::WideCsv] {
        for (field, identity) in [("payer_name", "Payer, Inc."), ("plan_name", "Plan A")] {
            let mut records = historical_csv_records(format, "negotiated_percentage");
            if format == InputFormat::TallCsv {
                let index = historical_csv_column(&records, field);
                records[3][index] = "  ".to_owned();
            } else {
                for header in &mut records[2] {
                    *header = header.replace(identity, "  ");
                }
            }
            assert_historical_csv_error(format, &records, field);
        }
        for (field, value, error) in [
            ("estimated_amount", "NaN", "estimated_amount"),
            ("estimated_amount", "=80*12.34", "estimated_amount"),
            ("estimated_amount", "-1", "estimated_amount"),
            ("estimated_amount", "0", "estimated_amount"),
            ("negotiated_percentage", "NaN", "standard_charge_percentage"),
            ("min", "NaN", "minimum"),
            ("max", "NaN", "maximum"),
        ] {
            let mut records = historical_csv_records(format, "negotiated_percentage");
            let index = historical_csv_column(&records, field);
            records[3][index] = value.to_owned();
            assert_historical_csv_error(format, &records, error);
        }
        let mut records = historical_csv_records(format, "negotiated_percentage");
        let index = historical_csv_column(&records, "negotiated_percentage");
        records[3][index].clear();
        let (_, summary) =
            import_packed(format, &csv_fixture_bytes(&records), TEST_MAX_OUTPUT_BYTES);
        assert_eq!(summary.root.unwrap().fact_count, 0);
    }
}

#[test]
fn historical_csv_limits_remove_partial_artifacts() {
    for format in [InputFormat::TallCsv, InputFormat::WideCsv] {
        let payload = csv_fixture_bytes(&historical_csv_records(format, "negotiated_percentage"));
        for (input_limit, output_limit, message) in [
            (1, TEST_MAX_OUTPUT_BYTES, "decompressed data exceeds"),
            (
                TEST_MAX_DECOMPRESSED_BYTES,
                1,
                "output exceeds configured limit",
            ),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let output = directory.path().join("output");
            fs::create_dir(&output).unwrap();
            let error = parse_hospital_payload_with_limits(
                format,
                Cursor::new(&payload),
                VERSION_ID,
                &output,
                HospitalMrfLimits::new(DEFAULT_MAX_FANOUT_ROWS, input_limit, output_limit),
            )
            .unwrap_err();
            assert!(error.to_string().contains(message), "{error}");
            assert_eq!(fs::read_dir(output).unwrap().count(), 0);
        }
    }
}

#[test]
fn historical_csv_does_not_relax_structurally_v3_or_json_inputs() {
    let mut mixed = historical_csv_records(InputFormat::TallCsv, "negotiated_percentage");
    let index = mixed[2]
        .iter()
        .position(|header| header.is_empty())
        .unwrap();
    mixed[2][index] = "count".to_owned();
    mixed[3][index] = "0".to_owned();
    assert_historical_csv_error(InputFormat::TallCsv, &mixed, "headers mix V2 and V3");

    let mut records = csv_fixture_records(&fixture_tall_csv());
    historical_csv_metadata(&mut records, "version", "2.0.0");
    historical_csv_metadata(&mut records, "last_updated_on", "2024-12-31");
    let dollar = historical_csv_column(&records, "negotiated_dollar");
    let percentage = historical_csv_column(&records, "negotiated_percentage");
    records[3][dollar].clear();
    records[3][percentage] = "80".to_owned();
    assert_historical_csv_error(InputFormat::TallCsv, &records, "require count");
    let count = historical_csv_column(&records, "count");
    let notes = historical_csv_column(&records, "additional_generic_notes");
    records[3][count] = "0".to_owned();
    records[3][notes] = "No allowed claims".to_owned();
    run_fixture(InputFormat::TallCsv, &csv_fixture_bytes(&records), false);

    for version in ["2.2.0", "2.2.1"] {
        let mut payload: serde_json::Value =
            serde_json::from_slice(&fixture_v2_json(version)).unwrap();
        payload["last_updated_on"] = json!("2024-12-31");
        payload["standard_charge_information"][0]["standard_charges"][0]["payers_information"][0]
            .as_object_mut()
            .unwrap()
            .remove("estimated_amount");
        assert_import_error(
            InputFormat::Json,
            &serde_json::to_vec(&payload).unwrap(),
            DEFAULT_MAX_FANOUT_ROWS,
            "require estimated_amount",
        );
    }
}

fn historical_csv_group_records(format: InputFormat) -> Vec<Vec<String>> {
    let mut records = historical_csv_records(format, "negotiated_percentage");
    for field in ["gross", "discounted_cash"] {
        let index = historical_csv_column(&records, field);
        records[3][index].clear();
    }
    let mut empty = records[3].clone();
    let fields = if format == InputFormat::TallCsv {
        [
            "negotiated_percentage",
            "payer_name",
            "plan_name",
            "additional_generic_notes",
        ]
    } else {
        [
            "negotiated_percentage",
            "additional_payer_notes",
            "additional_generic_notes",
            "estimated_amount",
        ]
    };
    for field in fields {
        empty[historical_csv_column(&records, field)].clear();
    }
    records.push(empty);
    records
}

#[test]
fn historical_tall_empty_siblings_preserve_packed_rates_and_order() {
    let mut baseline = historical_csv_group_records(InputFormat::TallCsv);
    let empty = baseline.pop().unwrap();
    let mut dollar_row = baseline[3].clone();
    for (field, value) in [
        ("plan_name", "Plan B"),
        ("negotiated_percentage", ""),
        ("negotiated_dollar", "9.125"),
        ("additional_generic_notes", "Dollar source terms"),
    ] {
        dollar_row[historical_csv_column(&baseline, field)] = value.to_owned();
    }
    baseline.push(dollar_row);
    let mut expected_artifacts = None;
    for (position, empty_payer_name) in [
        (None, ""),
        (Some(3), ""),
        (Some(4), "Uncontracted payer"),
        (Some(5), ""),
        (Some(3), "Uncontracted payer"),
    ] {
        let mut records = baseline.clone();
        if let Some(position) = position {
            records.insert(position, empty.clone());
            let payer_name = historical_csv_column(&records, "payer_name");
            records[position][payer_name] = empty_payer_name.to_owned();
        }
        let (directory, summary) = import_packed(
            InputFormat::TallCsv,
            &csv_fixture_bytes(&records),
            TEST_MAX_OUTPUT_BYTES,
        );
        let artifacts = summary
            .artifacts
            .iter()
            .map(|artifact| (artifact.kind, artifact.rows, artifact.sha256.clone()))
            .collect::<Vec<_>>();
        assert_eq!(
            expected_artifacts.get_or_insert(artifacts.clone()),
            &artifacts
        );
        let root = summary.root.unwrap();
        assert_eq!(
            (root.service_count, root.charge_count, root.fact_count),
            (1, 1, 2)
        );
        let blocks =
            super::packed_output_tests::payloads(&directory.path().join("output/fact_block.copy"));
        assert_eq!(blocks.len(), 1);
        let facts =
            crate::hospital_price_block::decode_fact_block(&blocks[0], None, None, 0, 512).unwrap();
        assert_eq!(facts.len(), 2);
        for (fact, plan, dollar, percentage, notes) in [
            (&facts[0], "Plan A", None, Some("80"), "Source rate terms"),
            (
                &facts[1],
                "Plan B",
                Some("9.125"),
                None,
                "Dollar source terms",
            ),
        ] {
            assert_eq!(fact.charge_key, 0);
            assert_eq!(fact.payer_name, "Payer, Inc.");
            assert_eq!(fact.plan_name, plan);
            assert_eq!(fact.negotiated_dollar.as_deref(), dollar);
            assert_eq!(fact.negotiated_percentage.as_deref(), percentage);
            assert_eq!(fact.estimated_amount, None);
            assert_eq!(fact.comparison_amount.as_deref(), dollar);
            assert_eq!(fact.additional_payer_notes.as_deref(), Some(notes));
        }
    }
}

#[test]
fn historical_tall_unpriced_groups_cannot_borrow_across_boundaries() {
    let baseline = historical_csv_group_records(InputFormat::TallCsv);
    for empty_count in [1, 2] {
        let mut records = baseline[..3].to_vec();
        records.extend(std::iter::repeat_n(baseline[4].clone(), empty_count));
        assert_historical_csv_error(InputFormat::TallCsv, &records, "standard charge requires");
    }
    for (field, value) in [
        ("min", "7"),
        ("max", "11"),
        ("setting", "inpatient"),
        ("billing_class", "professional"),
        ("modifiers", "59"),
        ("description", "Different service"),
        ("additional_generic_notes", "Distinct generic note"),
    ] {
        for empty_first in [false, true] {
            let mut records = baseline.clone();
            let index = historical_csv_column(&records, field);
            records[4][index] = value.to_owned();
            if empty_first {
                records.swap(3, 4);
            }
            assert_historical_csv_error(InputFormat::TallCsv, &records, "standard charge requires");
        }
    }
}

#[test]
fn historical_tall_grouping_keeps_present_value_validation() {
    for (field, value, error) in [
        ("min", "0", "minimum"),
        ("max", "-1", "maximum"),
        ("gross", "NaN", "gross_charge"),
        ("discounted_cash", "0", "discounted_cash"),
        ("estimated_amount", "0", "estimated_amount"),
        ("negotiated_percentage", "-1", "standard_charge_percentage"),
        ("negotiated_dollar", "9.125", "payer_name"),
    ] {
        let mut records = historical_csv_group_records(InputFormat::TallCsv);
        let index = historical_csv_column(&records, field);
        records[4][index] = value.to_owned();
        assert_historical_csv_error(InputFormat::TallCsv, &records, error);
    }
}

#[test]
fn historical_grouping_does_not_relax_modern_tall_or_wide_rows() {
    for (format, version, date) in [
        (InputFormat::TallCsv, "2.0.0", "2025-01-01"),
        (InputFormat::TallCsv, "2.2.0", "2024-12-31"),
        (InputFormat::WideCsv, "2.0.0", "2024-12-31"),
    ] {
        let mut records = historical_csv_group_records(format);
        historical_csv_metadata(&mut records, "version", version);
        historical_csv_metadata(&mut records, "last_updated_on", date);
        // The priced row meets even the modern estimate rule; only its empty sibling is invalid.
        let estimate = historical_csv_column(&records, "estimated_amount");
        records[3][estimate] = "9.125".to_owned();
        assert_historical_csv_error(format, &records, "standard charge requires");
    }
}

#[test]
fn historical_grouping_preserves_existing_gross_only_charge_boundaries() {
    let mut records = historical_csv_group_records(InputFormat::TallCsv);
    let gross = historical_csv_column(&records, "gross");
    records[3][gross] = "12.34".to_owned();
    records[4][gross] = "12.34".to_owned();
    let (_, summary) = import_packed(
        InputFormat::TallCsv,
        &csv_fixture_bytes(&records),
        TEST_MAX_OUTPUT_BYTES,
    );
    let root = summary.root.unwrap();
    assert_eq!(
        (root.service_count, root.charge_count, root.fact_count),
        (1, 2, 1)
    );
}
