#[test]
fn packed_wide_rate_terms_are_distinct_and_lossless() {
    let mut records = csv_fixture_records(&fixture_wide_csv());
    for header in &mut records[2] {
        if !header.contains("Payer, Inc.|Plan A") {
            continue;
        }
        if header.starts_with("standard_charge|") {
            let (prefix, field) = header.rsplit_once('|').unwrap();
            let term = if field == "negotiated_dollar" {
                "[TERM JAN 2026-MAY 2026]"
            } else {
                "[JAN 2026-MAY 2026]"
            };
            *header = format!("{prefix}|{term}|{field}");
        } else {
            header.push_str("|[JAN 2026-MAY 2026]");
        }
    }

    let second_term_columns = records[2]
        .iter()
        .enumerate()
        .filter(|(_, header)| header.contains("Payer, Inc.|Plan A"))
        .map(|(column, header)| {
            (
                header
                    .replace("[TERM JAN 2026-MAY 2026]", "[TERM JUN 2026-DEC 2026]")
                    .replace("[JAN 2026-MAY 2026]", "[JUN 2026-DEC 2026]"),
                records[3][column].clone(),
            )
        })
        .collect::<Vec<_>>();
    for (header, value) in second_term_columns {
        records[0].push(String::new());
        records[1].push(String::new());
        records[2].push(header);
        records[3].push(value);
    }

    let payload = csv_fixture_bytes(&records);
    let (directory, summary) = import_packed(
        InputFormat::WideCsv,
        &payload,
        TEST_MAX_OUTPUT_BYTES,
    );
    assert_eq!(summary.root.unwrap().fact_count, 2);
    let payloads = super::packed_output_tests::payloads(
        &directory.path().join("output/fact_block.copy"),
    );
    let facts = crate::hospital_price_block::decode_fact_block(
        &payloads[0],
        Some("Payer, Inc."),
        Some("Plan A"),
        0,
        crate::hospital_price_block::HOSPITAL_PRICE_FACT_BLOCK_MAX_ROWS,
    )
    .unwrap();
    assert_eq!(
        facts
            .iter()
            .map(|fact| fact.negotiated_rate_term.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("JAN 2026-MAY 2026"), Some("JUN 2026-DEC 2026")],
    );
    assert_import_error(
        InputFormat::WideCsv,
        &payload,
        DEFAULT_MAX_FANOUT_ROWS,
        "negotiated rate terms require packed hospital MRF output",
    );
}

#[test]
fn malformed_wide_rate_term_headers_fail_closed() {
    let records = csv_fixture_records(&fixture_wide_csv());
    for rate_term in ["", "[]", "[TERM ]", "[negotiated_rate_term]"] {
        let headers = records[2]
            .iter()
            .map(|header| {
                if header == "standard_charge|Payer, Inc.|Plan A|negotiated_dollar" {
                    format!(
                        "standard_charge|Payer, Inc.|Plan A|{rate_term}|negotiated_dollar"
                    )
                } else {
                    header.to_owned()
                }
            })
            .collect::<Vec<_>>();
        assert!(parse_wide_columns(
            &StringRecord::from(headers),
            CmsProfile::V3,
            true,
            DEFAULT_MAX_FANOUT_ROWS,
        )
        .unwrap_err()
        .to_string()
        .contains("negotiated_rate_term"));
    }

    for malformed in [
        "standard_charge|Payer, Inc.|Plan A|TERM|EXTRA|negotiated_dollar",
        "standard_charge|Payer, Inc.|Plan A|TERM|negotiated_dollar|extra",
        "standard_charge|Payer, Inc.|Plan A|TERM|negotiated_dollar|",
    ] {
        let headers = records[2]
            .iter()
            .map(|header| {
                if header == "standard_charge|Payer, Inc.|Plan A|negotiated_dollar" {
                    malformed.to_owned()
                } else {
                    header.to_owned()
                }
            })
            .collect::<Vec<_>>();
        assert!(parse_wide_columns(
            &StringRecord::from(headers),
            CmsProfile::V3,
            true,
            DEFAULT_MAX_FANOUT_ROWS,
        )
        .unwrap_err()
        .to_string()
        .contains("unsupported wide CSV payer header shape"));
    }
}

#[test]
fn packed_wide_modifier_rate_term_is_lossless() {
    let mut records = csv_fixture_records(&fixture_wide_csv());
    for column in 0..records[2].len() {
        let header = &mut records[2][column];
        if !header.contains("Payer, Inc.|Plan A") {
            continue;
        }
        if header.starts_with("standard_charge|") {
            let (prefix, field) = header.rsplit_once('|').unwrap();
            *header = format!("{prefix}|[TERM JAN 2026-MAY 2026]|{field}");
        } else {
            header.push_str("|[JAN 2026-MAY 2026]");
        }
        records[3][column].clear();
    }
    let mut modifier_row = vec![String::new(); records[2].len()];
    for (header, value) in [
        ("description", "Modifier percentage"),
        ("modifiers", "TC"),
        ("setting", "OUTPATIENT"),
        (
            "standard_charge|Payer, Inc.|Plan A|[TERM JAN 2026-MAY 2026]|negotiated_percentage",
            "62.500",
        ),
    ] {
        let column = records[2]
            .iter()
            .position(|candidate| candidate == header)
            .unwrap();
        modifier_row[column] = value.to_owned();
    }
    records.push(modifier_row);

    let (directory, _) = import_packed(
        InputFormat::WideCsv,
        &csv_fixture_bytes(&records),
        TEST_MAX_OUTPUT_BYTES,
    );
    let modifier_payers = fs::read_to_string(directory.path().join("output/modifier_payer.copy"))
        .unwrap();
    let fields = modifier_payers.trim_end().split('\t').collect::<Vec<_>>();
    assert_eq!(fields.len(), 10);
    assert_eq!(fields[5], "JAN 2026-MAY 2026");
    assert_eq!(fields[8], "62.5");
    assert_import_error(
        InputFormat::WideCsv,
        &csv_fixture_bytes(&records),
        DEFAULT_MAX_FANOUT_ROWS,
        "negotiated rate terms require packed hospital MRF output",
    );
}
