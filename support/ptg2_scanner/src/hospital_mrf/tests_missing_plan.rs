fn missing_plan_csv(explicit_blank: bool) -> Vec<u8> {
    let mut records = csv_fixture_records(&fixture_wide_csv());
    let columns = (10..19).collect::<Vec<_>>();
    for column in columns {
        for (ordinal, record) in records.iter_mut().enumerate() {
            let value = if ordinal == 2 {
                record[column].replace("|Plan A", if explicit_blank { "|" } else { "" })
            } else if ordinal == 3 && column == 10 {
                "8.5".to_owned()
            } else { record[column].clone() };
            record.push(value);
        }
    }
    csv_fixture_bytes(&records)
}

#[test]
fn missing_plan_csv_keeps_all_facts() {
    // Both observed wide shapes retain the same payer with and without a named plan.
    let mut previous = None;
    for explicit_blank in [false, true] {
        let payload = missing_plan_csv(explicit_blank);
        let (directory, summary) = import_packed(InputFormat::WideCsv, &payload, TEST_MAX_OUTPUT_BYTES);
        let root = summary.root.as_ref().unwrap();
        assert_eq!((root.service_count, root.charge_count, root.fact_count), (1, 1, 2));
        assert_eq!((root.payer_plan_selector_key_count, root.payer_plan_selector_ref_count), (2, 2));
        let blocks = super::packed_output_tests::payloads(&directory.path().join("output/fact_block.copy"));
        let facts = crate::hospital_price_block::decode_fact_block(&blocks[0], None, None, 0, 10).unwrap();
        assert_eq!(facts.iter().map(|fact| fact.plan_name.as_deref()).collect::<Vec<_>>(),
            vec![None, Some("Plan A")]);
        assert_eq!(facts.iter().map(|fact| fact.negotiated_dollar.as_deref()).collect::<Vec<_>>(),
            vec![Some("8.5"), Some("9.125")]);
        if let Some(previous) = previous { assert_eq!(facts, previous); }
        previous = Some(facts);
        assert_import_error(InputFormat::WideCsv, &payload, DEFAULT_MAX_FANOUT_ROWS,
            "missing plan_name requires packed hospital MRF output");
    }
}

#[test]
fn missing_plan_retains_other_validation() {
    for (header_suffix, value, error) in [
        ("negotiated_dollar", "0", "must be greater than zero"),
        ("negotiated_dollar", "-1", "must be greater than zero"),
        ("methodology", "invented", "methodology"),
    ] {
        let mut records = csv_fixture_records(&missing_plan_csv(false));
        let header = format!("standard_charge|Payer, Inc.|{header_suffix}");
        let column = csv_fixture_index(&records[2], &header);
        records[3][column] = value.to_owned();
        assert_import_error(InputFormat::WideCsv, &csv_fixture_bytes(&records),
            DEFAULT_MAX_FANOUT_ROWS, error);
    }
    let mut records = csv_fixture_records(&missing_plan_csv(false));
    for header in &mut records[2] { *header = header.replace("Payer, Inc.", ""); }
    assert_import_error(InputFormat::WideCsv, &csv_fixture_bytes(&records),
        DEFAULT_MAX_FANOUT_ROWS, "payer_name");
}
