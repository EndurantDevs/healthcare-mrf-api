// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

use ptg2_scanner::registry_target_ledger::{
    encode_registry_target_ledger_artifact, parse_registry_target_ledger, COMPONENT, HEADERS,
    MAX_COPY_BYTES, MAX_FIELD_BYTES, MAX_INPUT_BYTES, MAX_OUTPUT_BYTES, MAX_ROWS, MAX_TARGETS,
    PARSER_VERSION,
};
use sha2::{Digest, Sha256};

const SNAPSHOT_ID: &str = "34567890-3456-7890-8345-34567890abcd";

fn row(fc: &str) -> [String; 7] {
    let mut cells = std::array::from_fn(|_| String::new());
    cells[6] = fc.to_owned();
    cells
}

fn csv_input(rows: &[[String; 7]]) -> Vec<u8> {
    let mut writer = csv::Writer::from_writer(Vec::new());
    writer.write_record(HEADERS).unwrap();
    for cells in rows {
        writer.write_record(cells).unwrap();
    }
    writer.into_inner().unwrap()
}

fn rejected(input: &[u8], code: &str, ordinal: Option<usize>) {
    let error = parse_registry_target_ledger(input).unwrap_err();
    assert_eq!(error.code, code);
    assert_eq!(error.source_row_ordinal, ordinal);
    assert_eq!(error.to_string(), format!("Target ledger rejected: {code}"));
    let value = serde_json::to_value(error).unwrap();
    assert_eq!(value.as_object().unwrap().len(), 2);
}

#[test]
fn quoted_cells_and_record_ordinals_retain_source_evidence() {
    let mut first = row("123");
    first[0] = " alias, with comma ".into();
    first[1] = "Company \"Example\"\nsecond line\r\nthird line".into();
    let second = row("124");
    let input = csv_input(&[first.clone(), second.clone()]);
    let ledger = parse_registry_target_ledger(&input).unwrap();
    assert_eq!(ledger.source_sha256, sha256(&input));
    assert_eq!(ledger.row_count, 2);
    assert_eq!(ledger.observations[0].raw_cells, first);
    assert_eq!(ledger.observations[1].raw_cells, second);
    assert_eq!(ledger.observations[1].source_row_ordinal, 2);
    assert_eq!(ledger.observations[0].target_keys, vec!["fc:123"]);
    assert_eq!(parse_registry_target_ledger(&input).unwrap(), ledger);
    assert!(ledger
        .targets
        .windows(2)
        .all(|pair| pair[0].target_key < pair[1].target_key));
}

#[test]
fn relations_deduplicate_without_merging_source_identifiers() {
    let mut first = row("10");
    first[0] = "Original spelling".into();
    let mut duplicate = row("10");
    duplicate[0] = "Different spelling".into();
    let ledger =
        parse_registry_target_ledger(&csv_input(&[first.clone(), duplicate.clone(), row("11")]))
            .unwrap();
    assert_eq!(ledger.targets.len(), 2);
    assert_eq!(ledger.observations[0].raw_cells, first);
    assert_eq!(ledger.observations[1].raw_cells, duplicate);
    let shared = ledger
        .targets
        .iter()
        .find(|target| target.target_key == "fc:10")
        .unwrap();
    assert_eq!(shared.source_row_ordinals, vec![1, 2]);
    assert_eq!(shared.fc_network_id.as_deref(), Some("10"));
    let document = serde_json::to_value(&ledger).unwrap();
    assert_eq!(document["targets"][0].as_object().unwrap().len(), 3);
    assert!(document["targets"][0].get("network_id").is_none());
}

#[test]
fn all_missing_identifier_cases_remain_explicit_and_distinct() {
    let ledger = parse_registry_target_ledger(&csv_input(&[
        row("10"),
        row("10"),
        row(""),
        row(""),
        row(""),
    ]))
    .unwrap();
    assert_eq!(ledger.targets.len(), 4);
    assert_eq!(ledger.observations[0].target_keys, vec!["fc:10"]);
    for ordinal in [3, 4, 5] {
        let expected = format!("csv:{}:row:{ordinal}", ledger.source_sha256);
        assert_eq!(
            ledger.observations[ordinal - 1].target_keys,
            vec![expected.clone()]
        );
        assert_eq!(
            ledger
                .targets
                .iter()
                .find(|target| target.target_key == expected)
                .unwrap()
                .fc_network_id,
            None
        );
    }
    assert_eq!(
        ledger
            .targets
            .iter()
            .find(|target| target.target_key == "fc:10")
            .unwrap()
            .source_row_ordinals,
        vec![1, 2]
    );
}

#[test]
fn fc_source_identifier_is_not_narrowed_to_a_network_integer() {
    let fc = "9".repeat(128);
    let ledger = parse_registry_target_ledger(&csv_input(&[row(&fc)])).unwrap();
    assert_eq!(
        ledger.targets[0].fc_network_id.as_deref(),
        Some(fc.as_str())
    );
    for invalid in ["0", "01", "+1", "-1", "1.0", "1e2", " 1", "1 ", "١", "a"] {
        rejected(&csv_input(&[row(invalid)]), "invalid_fc_id", Some(1));
    }
    rejected(
        &csv_input(&[row(&"9".repeat(129))]),
        "invalid_fc_id",
        Some(1),
    );
}

#[test]
fn headers_are_exact_ordered_and_utf8() {
    for mutation in [0, 1, 2, 3] {
        let mut headers: Vec<&str> = HEADERS.to_vec();
        match mutation {
            0 => headers.swap(0, 1),
            1 => headers[0] = "company_alias",
            2 => {
                headers.pop();
            }
            _ => headers.push("EXTRA"),
        }
        let input = format!("{}\n,,,,,,1\n", headers.join(","));
        rejected(input.as_bytes(), "invalid_headers", None);
    }
    let input = csv_input(&[row("1")]);
    let mut bom = b"\xef\xbb\xbf".to_vec();
    bom.extend_from_slice(&input);
    rejected(&bom, "invalid_headers", None);
    let mut invalid_utf8 = input;
    invalid_utf8.push(0xff);
    rejected(&invalid_utf8, "invalid_utf8", None);
    rejected(b"", "invalid_headers", None);
    rejected(
        format!("{}\n", HEADERS.join(",")).as_bytes(),
        "empty_ledger",
        None,
    );
}

#[test]
fn permissive_csv_quote_and_blank_record_gaps_are_rejected() {
    for malformed in [
        "\"unfinished,,,,,,1",
        "\"value\"tail,,,,,,1\n",
        "un\"quoted,,,,,,1\n",
        "\n",
    ] {
        let input = format!("{}\n,,,,,,1\n{malformed}", HEADERS.join(","));
        let decoded = csv::ReaderBuilder::new()
            .flexible(true)
            .from_reader(input.as_bytes())
            .records()
            .collect::<Result<Vec<_>, _>>();
        assert!(
            decoded.is_ok(),
            "the strict scan addresses an actual decoder gap"
        );
        rejected(input.as_bytes(), "invalid_csv", Some(2));
    }
    let input = format!("{}\n\"two\nlines\",,,,,,1\n,,,,,1\n", HEADERS.join(","));
    rejected(input.as_bytes(), "invalid_csv", Some(2));
    let input = format!("{}\n,,,,,,1\n,,,,,,2,extra\n", HEADERS.join(","));
    rejected(input.as_bytes(), "invalid_csv", Some(2));
}

#[test]
fn crlf_and_final_record_without_terminator_are_supported() {
    let input = format!("{}\r\n,,,,,,1\r\n,,,,,,2", HEADERS.join(","));
    let ledger = parse_registry_target_ledger(input.as_bytes()).unwrap();
    assert_eq!(ledger.row_count, 2);
    assert_eq!(ledger.observations[1].source_row_ordinal, 2);
}

#[test]
fn utf8_byte_bounds_are_exact_for_all_requested_columns() {
    for field in 0..6 {
        let mut cells = row("1");
        cells[field] = "é".repeat(MAX_FIELD_BYTES / 2);
        assert!(parse_registry_target_ledger(&csv_input(&[cells.clone()])).is_ok());
        cells[field].push('x');
        rejected(&csv_input(&[cells]), "field_limit", Some(1));
    }
}

#[test]
fn row_and_target_limits_are_independent_and_atomic() {
    let cells = row("1");
    let input = csv_input(&vec![cells.clone(); MAX_ROWS]);
    let ledger = parse_registry_target_ledger(&input).unwrap();
    assert_eq!(ledger.row_count, MAX_ROWS);
    assert_eq!(ledger.targets.len(), 1);
    assert_eq!(
        ledger.targets[0].source_row_ordinals.last(),
        Some(&MAX_ROWS)
    );
    rejected(
        &csv_input(&vec![cells; MAX_ROWS + 1]),
        "row_limit",
        Some(MAX_ROWS + 1),
    );
    let rows = (1..=MAX_TARGETS)
        .map(|id| row(&id.to_string()))
        .collect::<Vec<_>>();
    assert_eq!(
        parse_registry_target_ledger(&csv_input(&rows))
            .unwrap()
            .targets
            .len(),
        MAX_TARGETS
    );
    let mut extra = rows;
    extra.push(row("5001"));
    rejected(&csv_input(&extra), "target_limit", Some(MAX_TARGETS + 1));
}

#[test]
fn missing_requested_ids_count_as_distinct_targets() {
    let rows = vec![row(""); MAX_TARGETS];
    assert_eq!(
        parse_registry_target_ledger(&csv_input(&rows))
            .unwrap()
            .targets
            .len(),
        MAX_TARGETS
    );
    let mut extra = rows;
    extra.push(row(""));
    rejected(&csv_input(&extra), "target_limit", Some(MAX_TARGETS + 1));
}

#[test]
fn input_byte_limit_accepts_exact_boundary_and_rejects_excess() {
    let mut input = format!("{}\n", HEADERS.join(",")).into_bytes();
    let row_bytes = MAX_FIELD_BYTES + 8;
    let full_rows = (MAX_INPUT_BYTES - input.len() - 8) / row_bytes;
    for _ in 0..full_rows {
        input.extend(std::iter::repeat_n(b'x', MAX_FIELD_BYTES));
        input.extend_from_slice(b",,,,,,1\n");
    }
    let remaining = MAX_INPUT_BYTES - input.len();
    input.extend(std::iter::repeat_n(b'x', remaining - 8));
    input.extend_from_slice(b",,,,,,1\n");
    assert_eq!(input.len(), MAX_INPUT_BYTES);
    assert!(parse_registry_target_ledger(&input).is_ok());
    input.push(b'\n');
    rejected(&input, "input_limit", None);
}

#[test]
fn escaped_serialized_output_is_bounded_without_partial_ledger() {
    let mut cells = row("1");
    cells[0] = "\0".repeat(MAX_FIELD_BYTES);
    let input = csv_input(&vec![cells; 700]);
    assert!(input.len() < MAX_INPUT_BYTES);
    rejected(&input, "output_limit", None);
}

#[test]
fn serialized_output_accepts_exact_boundary_and_rejects_one_extra_byte() {
    let mut cells = row("1");
    cells[0] = "\0".repeat(MAX_FIELD_BYTES);
    let mut rows = vec![cells; 500];
    let initial = parse_registry_target_ledger(&csv_input(&rows)).unwrap();
    let remaining = MAX_OUTPUT_BYTES - serde_json::to_vec(&initial).unwrap().len();
    let mut controls = remaining / 6;
    for cells in &mut rows {
        for cell in &mut cells[1..6] {
            let count = controls.min(MAX_FIELD_BYTES);
            *cell = "\0".repeat(count);
            controls -= count;
        }
    }
    assert_eq!(controls, 0);
    let padding = rows
        .iter_mut()
        .flat_map(|cells| cells[1..6].iter_mut())
        .find(|cell| cell.len() + remaining % 6 < MAX_FIELD_BYTES)
        .unwrap();
    padding.push_str(&"x".repeat(remaining % 6));
    let input = csv_input(&rows);
    assert!(input.len() < MAX_INPUT_BYTES);
    let exact = parse_registry_target_ledger(&input).unwrap();
    assert_eq!(serde_json::to_vec(&exact).unwrap().len(), MAX_OUTPUT_BYTES);
    rows.iter_mut()
        .flat_map(|cells| cells[1..6].iter_mut())
        .find(|cell| cell.len() < MAX_FIELD_BYTES)
        .unwrap()
        .push('x');
    rejected(&csv_input(&rows), "output_limit", None);
}

#[test]
fn whole_source_hash_binds_missing_identifier_targets() {
    let input = csv_input(&[row("")]);
    let first = parse_registry_target_ledger(&input).unwrap();
    let mut changed = input;
    changed.pop();
    let second = parse_registry_target_ledger(&changed).unwrap();
    assert_ne!(first.source_sha256, second.source_sha256);
    assert_ne!(first.targets[0].target_key, second.targets[0].target_key);
    assert_eq!(
        first.observations[0].raw_cells,
        second.observations[0].raw_cells
    );
}

fn observation_fields(copy: &[u8]) -> Vec<&[u8]> {
    assert_eq!(&copy[..19], b"PGCOPY\n\xff\r\n\0\0\0\0\0\0\0\0\0");
    assert_eq!(i16::from_be_bytes(copy[19..21].try_into().unwrap()), 6);
    let mut cursor = 21;
    let fields = (0..6)
        .map(|_| {
            let length = i32::from_be_bytes(copy[cursor..cursor + 4].try_into().unwrap());
            assert!(length >= 0);
            cursor += 4;
            let field = &copy[cursor..cursor + length as usize];
            cursor += length as usize;
            field
        })
        .collect();
    assert_eq!(&copy[cursor..], &(-1i16).to_be_bytes());
    fields
}

fn sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[test]
fn artifact_retains_7338_logical_rows_in_one_physical_record() {
    let rows = (0..7_338)
        .map(|index| {
            let mut cells = row(&(index % 37 + 1).to_string());
            for (field, cell) in cells[..6].iter_mut().enumerate() {
                *cell = format!("évidence {index}, field {field}: \"quoted\"\nnext line");
            }
            cells
        })
        .collect::<Vec<_>>();
    let input = csv_input(&rows);
    let parsed = parse_registry_target_ledger(&input).unwrap();
    let (copy, descriptor_bytes) =
        encode_registry_target_ledger_artifact(&input, SNAPSHOT_ID).unwrap();
    assert_eq!(
        encode_registry_target_ledger_artifact(&input, SNAPSHOT_ID).unwrap(),
        (copy.clone(), descriptor_bytes.clone())
    );
    let fields = observation_fields(&copy);
    assert_eq!(fields[0], &hex_snapshot());
    assert_eq!(fields[1], b"ledger:v2");
    assert_eq!(fields[2], &1i32.to_be_bytes());
    assert_eq!(fields[3], b"accepted");
    assert_eq!(fields[4][0], 1);
    assert_eq!(fields[5], b"\x01[]");
    let artifact_bytes = &fields[4][1..];
    assert!(artifact_bytes.len() <= MAX_OUTPUT_BYTES && copy.len() <= MAX_COPY_BYTES);
    let document: serde_json::Value = serde_json::from_slice(artifact_bytes).unwrap();
    assert_eq!(document.as_object().unwrap().len(), 4);
    assert_eq!(document["component"], COMPONENT);
    assert_eq!(document["revision"], 1);
    assert_eq!(document["parser_version"], PARSER_VERSION);
    assert_eq!(document["ledger"], serde_json::to_value(&parsed).unwrap());
    assert_eq!(serde_json::to_vec(&document).unwrap(), artifact_bytes);
    assert!(std::str::from_utf8(artifact_bytes)
        .unwrap()
        .contains("évidence"));
    for (observation, cells) in parsed.observations.iter().zip(&rows) {
        assert_eq!(&observation.raw_cells, cells);
    }
    assert_eq!(
        parsed.observations.last().unwrap().source_row_ordinal,
        7_338
    );
    let descriptor: serde_json::Value = serde_json::from_slice(&descriptor_bytes).unwrap();
    assert_eq!(descriptor.as_object().unwrap().len(), 9);
    assert_eq!(descriptor["component"], COMPONENT);
    assert_eq!(descriptor["revision"], 1);
    assert_eq!(descriptor["parser_version"], PARSER_VERSION);
    assert_eq!(descriptor["snapshot_id"], SNAPSHOT_ID);
    assert_eq!(descriptor["source_sha256"], sha256(&input));
    assert_eq!(descriptor["artifact_sha256"], sha256(artifact_bytes));
    assert_eq!(descriptor["source_rows"], 7_338);
    assert_eq!(descriptor["target_count"], 37);
    assert_eq!(descriptor["physical_records"], 1);
    assert_eq!(serde_json::to_vec(&descriptor).unwrap(), descriptor_bytes);
}

fn hex_snapshot() -> [u8; 16] {
    [
        0x34, 0x56, 0x78, 0x90, 0x34, 0x56, 0x78, 0x90, 0x83, 0x45, 0x34, 0x56, 0x78, 0x90, 0xab,
        0xcd,
    ]
}

#[test]
fn artifact_rejects_noncanonical_snapshot_and_malformed_final_record() {
    let input = csv_input(&[row("1")]);
    for snapshot in [
        "00000000-0000-0000-0000-000000000000",
        "34567890-3456-7890-8345-34567890ABCD",
        "3456789034567890834534567890abcd",
        "34567890-3456-7890-8345-34567890abcd ",
        "urn:uuid:34567890-3456-7890-8345-34567890abcd",
        "",
    ] {
        let error = encode_registry_target_ledger_artifact(&input, snapshot).unwrap_err();
        assert_eq!(error.code, "invalid_snapshot_id");
        assert_eq!(error.source_row_ordinal, None);
    }
    let mut input = csv_input(&vec![row("1"); 7_338]);
    input.extend_from_slice(b"\"unterminated,,,,,,1");
    let error = encode_registry_target_ledger_artifact(&input, SNAPSHOT_ID).unwrap_err();
    assert_eq!(error.code, "invalid_csv");
    assert_eq!(error.source_row_ordinal, Some(7_339));
}

#[test]
fn artifact_rejects_jsonb_nul_without_changing_parser_contract() {
    let mut cells = row("1");
    cells[0] = "raw\0cell".into();
    let input = csv_input(&[cells.clone()]);
    assert_eq!(
        parse_registry_target_ledger(&input).unwrap().observations[0].raw_cells,
        cells
    );
    let error = encode_registry_target_ledger_artifact(&input, SNAPSHOT_ID).unwrap_err();
    assert_eq!(error.code, "invalid_jsonb");
    assert_eq!(error.source_row_ordinal, Some(1));
}

#[test]
fn artifact_document_byte_limit_includes_envelope_and_exact_copy_framing() {
    let mut cells = row("1");
    cells[0] = "\u{1}".repeat(MAX_FIELD_BYTES);
    let mut rows = vec![cells; 500];
    let (copy, _) = encode_registry_target_ledger_artifact(&csv_input(&rows), SNAPSHOT_ID).unwrap();
    let remaining = MAX_OUTPUT_BYTES - (observation_fields(&copy)[4].len() - 1);
    let mut controls = remaining / 6;
    for cells in &mut rows {
        for cell in &mut cells[1..6] {
            let count = controls.min(MAX_FIELD_BYTES);
            *cell = "\u{1}".repeat(count);
            controls -= count;
        }
    }
    assert_eq!(controls, 0);
    rows.iter_mut()
        .flat_map(|cells| cells[1..6].iter_mut())
        .find(|cell| cell.len() + remaining % 6 < MAX_FIELD_BYTES)
        .unwrap()
        .push_str(&"x".repeat(remaining % 6));
    let input = csv_input(&rows);
    assert!(input.len() <= MAX_INPUT_BYTES);
    let (copy, _) = encode_registry_target_ledger_artifact(&input, SNAPSHOT_ID).unwrap();
    assert_eq!(observation_fields(&copy)[4].len() - 1, MAX_OUTPUT_BYTES);
    assert_eq!(copy.len(), MAX_OUTPUT_BYTES + 88);
    rows.iter_mut()
        .flat_map(|cells| cells[1..6].iter_mut())
        .find(|cell| cell.len() < MAX_FIELD_BYTES)
        .unwrap()
        .push('x');
    let input = csv_input(&rows);
    assert!(parse_registry_target_ledger(&input).is_ok());
    assert_eq!(
        encode_registry_target_ledger_artifact(&input, SNAPSHOT_ID)
            .unwrap_err()
            .code,
        "output_limit"
    );
}
