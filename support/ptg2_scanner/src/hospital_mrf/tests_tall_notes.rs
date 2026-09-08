fn tall_explicit_note_records(payload: &[u8], generic: &str, payer: &str) -> Vec<Vec<String>> {
    let mut records = csv_fixture_records(payload);
    let generic_column = csv_fixture_index(&records[2], "additional_generic_notes");
    records[3][generic_column] = generic.to_owned();
    for record in &mut records {
        record.push(String::new());
    }
    *records[2].last_mut().unwrap() = " additional_PAYER_notes ".to_owned();
    *records[3].last_mut().unwrap() = payer.to_owned();
    records
}

#[test]
fn tall_explicit_notes_preserve_both_columns_and_packed_output() {
    let records = tall_explicit_note_records(&fixture_tall_csv(), "Generic note", "Payer note");
    let payload = csv_fixture_bytes(&records);
    let mut expected: serde_json::Value = serde_json::from_slice(&fixture_json()).unwrap();
    let charge = &mut expected["standard_charge_information"][0]["standard_charges"][0];
    charge["additional_generic_notes"] = json!("Generic note");
    charge["payers_information"][0]["additional_payer_notes"] = json!("Payer note");
    let expected = serde_json::to_vec(&expected).unwrap();
    assert_eq!(run_fixture(InputFormat::TallCsv, &payload, false),
        run_fixture(InputFormat::Json, &expected, false));
    let (_actual_dir, actual) = import_packed(InputFormat::TallCsv, &payload, TEST_MAX_OUTPUT_BYTES);
    let (_expected_dir, expected) = import_packed_json(&expected, TEST_MAX_OUTPUT_BYTES);
    assert_eq!(serde_json::to_value(actual.root).unwrap(), serde_json::to_value(expected.root).unwrap());
    assert_eq!(actual.artifacts.iter().map(|a| (a.kind, a.rows, &a.sha256)).collect::<Vec<_>>(),
        expected.artifacts.iter().map(|a| (a.kind, a.rows, &a.sha256)).collect::<Vec<_>>());
}

#[test]
fn tall_explicit_blank_notes_do_not_fall_back_to_generic_notes() {
    for fixture in [fixture_tall_csv(), fixture_v2_csv(InputFormat::TallCsv, "2.0.0")] {
        for note in ["", " \t ", "Payer note"] {
            let records = tall_explicit_note_records(&fixture, "Generic note", note);
            let rows = run_fixture(InputFormat::TallCsv, &csv_fixture_bytes(&records), false);
            let charge = std::str::from_utf8(&rows["charge"]).unwrap();
            let payer = std::str::from_utf8(&rows["payer_charge"]).unwrap();
            assert_eq!(charge.trim_end().split('\t').nth(9), Some("Generic note"));
            assert_eq!(payer.trim_end().split('\t').nth(15),
                Some(if note.trim().is_empty() { "\\N" } else { note }));
        }
    }
}

#[test]
fn tall_explicit_header_absence_preserves_legacy_output() {
    let records = tall_explicit_note_records(&fixture_tall_csv(), "", "Legacy note");
    let expected = run_fixture(InputFormat::TallCsv, &csv_fixture_bytes(&records), false);
    let mut legacy = records;
    for record in &mut legacy {
        record.pop();
    }
    let generic = csv_fixture_index(&legacy[2], "additional_generic_notes");
    legacy[3][generic] = "Legacy note".to_owned();
    assert_eq!(run_fixture(InputFormat::TallCsv, &csv_fixture_bytes(&legacy), false), expected);
    let headers = StringRecord::from(tall_explicit_note_records(&fixture_tall_csv(), "", "")[2].clone());
    let columns = parse_tall_columns(&headers, CmsProfile::V3, false, DEFAULT_MAX_FANOUT_ROWS).unwrap();
    let row = StringRecord::from(legacy[3].clone());
    assert!(parse_tall_payer(&row, &columns, Some("Legacy note")).unwrap().unwrap()
        .additional_payer_notes.is_none());
}

#[test]
fn tall_explicit_generic_notes_prevent_cross_note_charge_merging() {
    let mut records = tall_explicit_note_records(&fixture_tall_csv(), "First note", "Payer note");
    let generic = csv_fixture_index(&records[2], "additional_generic_notes");
    let mut second = records[3].clone();
    second[generic] = "Second note".to_owned();
    records.push(second);
    let rows = run_fixture(InputFormat::TallCsv, &csv_fixture_bytes(&records), false);
    let charges = std::str::from_utf8(&rows["charge"]).unwrap();
    assert_eq!(charges.lines().map(|line| line.split('\t').nth(9).unwrap()).collect::<Vec<_>>(),
        ["First note", "Second note"]);
    assert_eq!(std::str::from_utf8(&rows["payer_charge"]).unwrap().lines().count(), 2);
}

#[test]
fn tall_explicit_modifier_notes_remain_separate() {
    for (payer, plan, adjustment, generic, note) in [
        ("Payer A", "Plan A", "150", "Generic modifier note", "Payer note"),
        ("Payer A", "Plan A", "150", "Generic modifier note", ""),
        ("Payer A", "Plan A", "150", "", "Payer note"),
        ("", "", "", "Generic modifier note", "Anonymous adjustment note"),
        ("", "", "", "Generic modifier note", ""),
    ] {
        let mut records = tall_explicit_note_records(&fixture_tall_csv(), "", "");
        let mut modifier = vec![String::new(); records[2].len()];
        for (field, value) in [("description", "Modifier"), ("modifiers", "25"),
            ("payer_name", payer), ("plan_name", plan),
            ("standard_charge | negotiated_dollar", adjustment),
            ("additional_generic_notes", generic)] {
            modifier[csv_fixture_index(&records[2], field)] = value.to_owned();
        }
        *modifier.last_mut().unwrap() = note.to_owned();
        records.push(modifier);
        let rows = run_fixture(InputFormat::TallCsv, &csv_fixture_bytes(&records), false);
        assert_eq!(std::str::from_utf8(&rows["modifier"]).unwrap().trim_end().split('\t').nth(5),
            Some(if generic.is_empty() { "\\N" } else { generic }));
        let actual = std::str::from_utf8(&rows["modifier_payer"]).unwrap();
        if adjustment.is_empty() && note.is_empty() {
            assert!(actual.is_empty());
        } else {
            assert_eq!(actual.trim_end().split('\t').nth(6),
                Some(if note.is_empty() { "\\N" } else { note }));
        }
    }
}

#[test]
fn tall_explicit_modifier_identity_stays_paired() {
    let records = tall_explicit_note_records(&fixture_tall_csv(), "Generic", "Specific");
    let headers = StringRecord::from(records[2].clone());
    let columns = parse_tall_columns(&headers, CmsProfile::V3, false, DEFAULT_MAX_FANOUT_ROWS).unwrap();
    for (field, error) in [("payer_name", "requires payer_name"), ("plan_name", "requires plan_name")] {
        let mut row = records[3].clone();
        row[csv_fixture_index(&records[2], field)].clear();
        assert!(parse_tall_modifier_payer(&StringRecord::from(row), &columns)
            .unwrap_err().to_string().contains(error));
    }
}

#[test]
fn tall_explicit_notes_do_not_weaken_validation_or_discard_unbound_notes() {
    for (field, value, error) in [
        ("payer_name", "", "payer_name must be a non-empty string"),
        ("plan_name", "", "plan_name must be a non-empty string"),
        ("standard_charge | negotiated_dollar", "", "payer information requires dollar"),
        ("additional_generic_notes", "bad\0note", "contains NUL"),
        (" additional_PAYER_notes ", "bad\0note", "contains NUL"),
    ] {
        let mut records = tall_explicit_note_records(&fixture_tall_csv(), "Generic", "Payer");
        let column = csv_fixture_index(&records[2], field);
        records[3][column] = value.to_owned();
        assert_import_error(InputFormat::TallCsv, &csv_fixture_bytes(&records), DEFAULT_MAX_FANOUT_ROWS, error);
    }
    let mut records = tall_explicit_note_records(&fixture_tall_csv(), "", "");
    for record in &mut records {
        record.push(String::new());
    }
    *records[2].last_mut().unwrap() = "ADDITIONAL_PAYER_NOTES".to_owned();
    assert_import_error(InputFormat::TallCsv, &csv_fixture_bytes(&records), DEFAULT_MAX_FANOUT_ROWS,
        "duplicate CSV header additional_payer_notes");
    let records = tall_explicit_note_records(&csv_fixture_bytes(&service_only_tall_records()), "Generic", "Unbound note");
    assert_import_error(InputFormat::TallCsv, &csv_fixture_bytes(&records), DEFAULT_MAX_FANOUT_ROWS,
        "explicit additional_payer_notes requires a material payer charge");
}
