use ptg2_scanner::cms_mlr_registry::{
    encode_cms_mlr_observations, parse_cms_mlr_edition, MlrEdition, RowStatus, COPY_COLUMNS,
    HEADERS, MAX_COPY_BYTES, MAX_FIELD_BYTES, MAX_INPUT_BYTES, MAX_ROWS,
};
use serde_json::Value;
use sha2::{Digest, Sha256};

fn filing(submission: &str) -> Vec<String> {
    let mut fields = vec![String::new(); HEADERS.len()];
    for (field, value) in [
        ("mr_submission_template_id", submission),
        ("business_state", "CA"),
        ("group_affiliation", "Synthetic Group"),
        ("hios_issuer_id", "00123"),
        ("company_name", "Synthetic Company"),
        ("naic_group_code", "00707"),
        ("naic_company_code", "00456"),
        ("federal_ein", "01-2345678"),
        ("created_date", "2025-09-12"),
        ("dba_marketing_name", "Synthetic Brand"),
    ] {
        fields[HEADERS.iter().position(|header| *header == field).unwrap()] = value.to_owned();
    }
    fields
}

fn set(row: &mut [String], name: &str, value: &str) {
    row[HEADERS.iter().position(|header| *header == name).unwrap()] = value.to_owned();
}

fn csv(headers: &[&str], rows: &[Vec<String>]) -> Vec<u8> {
    let mut writer = csv::Writer::from_writer(Vec::new());
    writer.write_record(headers).unwrap();
    for row in rows {
        writer.write_record(row).unwrap();
    }
    writer.into_inner().unwrap()
}

fn edition(bytes: &[u8]) -> MlrEdition {
    MlrEdition {
        snapshot_id: "synthetic-edition".to_owned(),
        reporting_year: 2024,
        input_sha256: Sha256::digest(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
    }
}

fn copy_edition(bytes: &[u8]) -> MlrEdition {
    let mut metadata = edition(bytes);
    metadata.snapshot_id = "01234567-89AB-CDEF-8123-456789ABCDEF".to_owned();
    metadata
}

// Independent reader checks framing, field lengths and complete trailer consumption.
fn copy_fields(copy_bytes: &[u8]) -> Vec<Vec<&[u8]>> {
    assert_eq!(&copy_bytes[..11], b"PGCOPY\n\xff\r\n\0");
    assert_eq!(&copy_bytes[11..19], &[0; 8]);
    let mut offset = 19;
    let mut observations = Vec::new();
    loop {
        let field_count = i16::from_be_bytes(copy_bytes[offset..offset + 2].try_into().unwrap());
        offset += 2;
        if field_count == -1 {
            break;
        }
        assert_eq!(field_count, 6);
        let mut fields = Vec::new();
        for _ in 0..field_count {
            let length = i32::from_be_bytes(copy_bytes[offset..offset + 4].try_into().unwrap());
            offset += 4;
            assert!(length >= 0);
            let end = offset + length as usize;
            fields.push(&copy_bytes[offset..end]);
            offset = end;
        }
        observations.push(fields);
    }
    assert_eq!(offset, copy_bytes.len());
    observations
}

fn jsonb_field(encoded: &[u8]) -> Value {
    assert_eq!(encoded[0], 1, "PostgreSQL JSONB binary version must be one");
    serde_json::from_slice(&encoded[1..]).unwrap()
}

#[test]
fn raw_fields_and_all_filings_remain_edition_local_assertions() {
    let mut state = filing("1");
    set(&mut state, "business_state", "ca");
    set(&mut state, "naic_company_code", "456");
    let mut total = filing("2");
    set(&mut total, "business_state", "Grand Total");
    set(&mut total, "hios_issuer_id", "");
    set(&mut total, "naic_group_code", "");
    let input = csv(&HEADERS, &[state, total]);
    let batch = parse_cms_mlr_edition(&input, &edition(&input)).unwrap();
    assert_eq!(batch.observations.len(), 2);
    let state = &batch.observations[0];
    assert_eq!(state.state.as_deref(), Some("CA"));
    assert_eq!(state.normalized_ein.as_deref(), Some("012345678"));
    assert_eq!(state.normalized_naic_company.as_deref(), Some("00456"));
    assert_eq!(state.normalized_naic_group.as_deref(), Some("707"));
    assert_eq!(state.hios.as_deref(), Some("00123"));
    assert_eq!(state.group_kind, Some("naic_group"));
    assert_eq!(state.company_key.as_deref(), Some("cms_mlr:ein:012345678"));
    assert_eq!(state.raw_fields.len(), 17);
    assert_eq!(state.raw_fields["federal_ein"], "01-2345678");
    assert_eq!(state.raw_fields["naic_company_code"], "456");
    assert_eq!(state.raw_fields["company_pk"], "");
    assert_eq!(state.raw_fields["created_date"], "2025-09-12");
    assert_eq!(state.status, RowStatus::Accepted);
    assert_eq!(state.source_row_number, 2);
    assert_eq!(batch.observations[1].row_kind, "grand_total");
    assert!(batch.observations[1].hios.is_none());
    assert!(batch.observations[1].state.is_none());
    assert_eq!(batch.observations[1].status, RowStatus::Unresolved);
    assert_eq!(batch.counts.grand_total_rows, 1);
    assert_eq!(batch.counts.source_company_anchors, 1);
    assert_eq!(
        batch.counts.accepted_rows + batch.counts.unresolved_rows + batch.counts.rejected_rows,
        2
    );
}

#[test]
fn syntax_gaps_retain_rows_without_synthesizing_identifiers_or_groups() {
    let mut missing = filing("1");
    for field in ["federal_ein", "hios_issuer_id", "naic_company_code"] {
        set(&mut missing, field, "");
    }
    set(&mut missing, "naic_group_code", "00000");
    let mut invalid = filing("2");
    set(&mut invalid, "federal_ein", "01\t2345678");
    set(&mut invalid, "hios_issuer_id", "123");
    set(&mut invalid, "naic_company_code", "１２３４５");
    set(&mut invalid, "business_state", "ZZ");
    let input = csv(&HEADERS, &[missing, invalid]);
    let batch = parse_cms_mlr_edition(&input, &edition(&input)).unwrap();
    assert_eq!(batch.observations[0].status, RowStatus::Unresolved);
    assert!(batch.observations[0].company_key.is_none());
    assert!(batch.observations[0].normalized_naic_group.is_none());
    assert!(batch.observations[0].group_kind.is_none());
    assert_eq!(batch.observations[1].status, RowStatus::Rejected);
    assert_eq!(
        batch.observations[1].raw_fields["federal_ein"],
        "01\t2345678"
    );
    assert!(batch.observations[1].normalized_ein.is_none());
    assert_eq!(batch.counts.unresolved_rows, 1);
    assert_eq!(batch.counts.rejected_rows, 1);
}

#[test]
fn names_identifiers_issuer_conflicts_and_duplicates_have_no_winner() {
    let first = filing("1");
    let mut second = filing("2");
    set(&mut second, "company_name", "Other Synthetic Company");
    set(&mut second, "naic_company_code", "00457");
    set(&mut second, "group_affiliation", "Other Synthetic Group");
    set(&mut second, "business_state", "FL");
    let mut third = filing("3");
    set(&mut third, "federal_ein", "02-3456789");
    set(&mut third, "hios_issuer_id", "00999");
    let mut duplicate = filing("1");
    set(&mut duplicate, "naic_group_code", "708");
    let input = csv(&HEADERS, &[first, second, third, duplicate]);
    let batch = parse_cms_mlr_edition(&input, &edition(&input)).unwrap();
    for code in [
        "conflicting_company_names_for_ein",
        "conflicting_naic_codes_for_ein",
        "conflicting_eins_for_naic_code",
        "conflicting_issuer_identity",
        "conflicting_group_codes_for_company",
        "ambiguous_group_label",
        "duplicate_submission_id",
    ] {
        assert!(
            batch.conflicts.iter().any(|conflict| conflict.code == code),
            "{code}"
        );
    }
    assert_eq!(batch.counts.input_rows, 4);
    assert_eq!(batch.counts.accepted_rows, 0);
    assert_eq!(batch.observations[0].status, RowStatus::Rejected);
    assert_eq!(batch.observations[3].status, RowStatus::Rejected);
    assert_eq!(
        batch.observations[1].normalized_naic_company.as_deref(),
        Some("00457")
    );
    assert_eq!(
        batch.observations[2].normalized_ein.as_deref(),
        Some("023456789")
    );
}

#[test]
fn duplicate_missing_extra_headers_and_malformed_rows_reject_entire_edition() {
    let mut duplicate = HEADERS;
    duplicate[16] = duplicate[0];
    for headers in [&duplicate[..], &HEADERS[..16]] {
        let input = csv(headers, &[]);
        assert_eq!(
            parse_cms_mlr_edition(&input, &edition(&input))
                .unwrap_err()
                .code,
            "invalid_headers"
        );
    }
    let mut input = csv(&HEADERS, &[filing("1")]);
    input.extend_from_slice(b"too,few,columns\n");
    assert_eq!(
        parse_cms_mlr_edition(&input, &edition(&input))
            .unwrap_err()
            .code,
        "invalid_csv"
    );
    let input = csv(&HEADERS, &[]);
    assert_eq!(
        parse_cms_mlr_edition(&input, &edition(&input))
            .unwrap_err()
            .code,
        "empty_edition"
    );
    let mut input = csv(&HEADERS, &[filing("1")]);
    input.push(0xff);
    assert_eq!(
        parse_cms_mlr_edition(&input, &edition(&input))
            .unwrap_err()
            .code,
        "invalid_csv"
    );
}

#[test]
fn bounds_digest_and_source_year_fail_closed() {
    let input = csv(&HEADERS, &[filing("1")]);
    let mut invalid = edition(&input);
    invalid.input_sha256 = "a".repeat(64);
    assert_eq!(
        parse_cms_mlr_edition(&input, &invalid).unwrap_err().code,
        "digest_mismatch"
    );
    invalid = edition(&input);
    invalid.reporting_year = 2009;
    assert_eq!(
        parse_cms_mlr_edition(&input, &invalid).unwrap_err().code,
        "invalid_edition"
    );
    assert_eq!(
        parse_cms_mlr_edition(&vec![b' '; MAX_INPUT_BYTES + 1], &invalid)
            .unwrap_err()
            .code,
        "input_limit"
    );
    let mut oversized = filing("1");
    set(
        &mut oversized,
        "company_address",
        &"x".repeat(MAX_FIELD_BYTES + 1),
    );
    let input = csv(&HEADERS, &[oversized]);
    assert_eq!(
        parse_cms_mlr_edition(&input, &edition(&input))
            .unwrap_err()
            .code,
        "field_limit"
    );
    let rows: Vec<_> = (0..=MAX_ROWS)
        .map(|index| filing(&index.to_string()))
        .collect();
    let input = csv(&HEADERS, &rows);
    assert_eq!(
        parse_cms_mlr_edition(&input, &edition(&input))
            .unwrap_err()
            .code,
        "row_limit"
    );
}

#[test]
fn header_order_bom_csv_quotes_and_raw_labels_are_preserved() {
    let mut headers = HEADERS;
    headers.reverse();
    let mut row = filing("1");
    set(&mut row, "company_name", "Synthetic, Company\nSecond Line");
    row.reverse();
    let mut input = b"\xef\xbb\xbf".to_vec();
    input.extend_from_slice(&csv(&headers, &[row]));
    let batch = parse_cms_mlr_edition(&input, &edition(&input)).unwrap();
    assert_eq!(
        batch.observations[0].raw_fields["company_name"],
        "Synthetic, Company\nSecond Line"
    );
    assert_eq!(batch.observations[0].submission_id, "1");
}

#[test]
fn binary_copy_preserves_all_six_columns_and_complete_assertions() {
    let input = csv(&HEADERS, &[filing("1")]);
    let metadata = copy_edition(&input);
    let encoded = encode_cms_mlr_observations(&input, &metadata).unwrap();
    assert_eq!(encoded.row_count, 1);
    assert_eq!(
        COPY_COLUMNS,
        [
            "snapshot_id",
            "source_record_key",
            "source_row_number",
            "status",
            "observation_json",
            "issues_json"
        ]
    );
    let fields = copy_fields(&encoded.copy_bytes);
    assert_eq!(fields.len(), encoded.row_count);
    assert_eq!(
        fields[0][0],
        [
            0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x81, 0x23, 0x45, 0x67, 0x89, 0xab,
            0xcd, 0xef
        ]
    );
    assert_eq!(fields[0][1], b"row:2");
    assert_eq!(fields[0][2], 2i32.to_be_bytes());
    assert_eq!(fields[0][3], b"accepted");
    let assertion_json = jsonb_field(fields[0][4]);
    assert_eq!(
        assertion_json,
        serde_json::to_value(&encoded.batch.observations[0]).unwrap()
    );
    assert_eq!(
        assertion_json["raw_fields"].as_object().unwrap().len(),
        HEADERS.len()
    );
    assert_eq!(jsonb_field(fields[0][5]), assertion_json["issues"]);
    assert!(assertion_json.is_object());
    assert!(jsonb_field(fields[0][5]).is_array());
    let parsed = parse_cms_mlr_edition(&input, &metadata).unwrap();
    assert_eq!(
        serde_json::to_value(&encoded.batch).unwrap(),
        serde_json::to_value(parsed).unwrap()
    );
}

#[test]
fn binary_copy_keeps_missing_and_invalid_identifiers_with_status_and_issues() {
    let mut missing = filing("1");
    for field in [
        "federal_ein",
        "hios_issuer_id",
        "naic_company_code",
        "naic_group_code",
    ] {
        set(&mut missing, field, "");
    }
    let mut invalid = filing("2");
    set(&mut invalid, "hios_issuer_id", "not-an-issuer");
    let input = csv(&HEADERS, &[missing, invalid]);
    let encoded = encode_cms_mlr_observations(&input, &copy_edition(&input)).unwrap();
    let fields = copy_fields(&encoded.copy_bytes);
    assert_eq!(encoded.row_count, 2);
    assert_eq!(fields[0][3], b"unresolved");
    assert_eq!(fields[1][3], b"rejected");
    assert_eq!(encoded.batch.counts.unresolved_rows, 1);
    assert_eq!(encoded.batch.counts.rejected_rows, 1);
    for (index, assertion) in encoded.batch.observations.iter().enumerate() {
        assert_eq!(
            jsonb_field(fields[index][4]),
            serde_json::to_value(assertion).unwrap()
        );
        assert_eq!(
            jsonb_field(fields[index][5]),
            serde_json::to_value(&assertion.issues).unwrap()
        );
    }
    assert!(jsonb_field(fields[0][4])["normalized_ein"].is_null());
    assert_eq!(
        jsonb_field(fields[1][4])["raw_fields"]["hios_issuer_id"],
        "not-an-issuer"
    );
}

#[test]
fn binary_copy_keys_and_conflict_references_follow_physical_csv_lines() {
    let mut multiline = filing("1");
    set(
        &mut multiline,
        "company_address",
        "First line\nSecond line\nThird line",
    );
    let mut duplicate = filing("1");
    set(&mut duplicate, "company_address", "Other office");
    let input = csv(&HEADERS, &[multiline, duplicate]);
    let encoded = encode_cms_mlr_observations(&input, &copy_edition(&input)).unwrap();
    let fields = copy_fields(&encoded.copy_bytes);
    assert_eq!(encoded.batch.observations[0].source_row_number, 2);
    assert_eq!(encoded.batch.observations[1].source_row_number, 5);
    assert_eq!(fields[0][1], b"row:2");
    assert_eq!(fields[1][1], b"row:5");
    assert_eq!(fields[1][2], 5i32.to_be_bytes());
    let conflict = encoded
        .batch
        .conflicts
        .iter()
        .find(|conflict| conflict.code == "duplicate_submission_id")
        .unwrap();
    assert_eq!(conflict.source_rows, [2, 5]);
    assert_eq!(
        jsonb_field(fields[0][4])["raw_fields"]["company_address"],
        "First line\nSecond line\nThird line"
    );
}

#[test]
fn binary_copy_physical_lines_handle_crlf_cr_and_skipped_blank_lines() {
    let mut multiline = filing("1");
    set(&mut multiline, "company_address", "First line\nSecond line");
    let lf_input = csv(&HEADERS, &[multiline, filing("2")]);
    for ending in ["\r\n", "\r", "\n"] {
        let mut input = String::from_utf8(lf_input.clone())
            .unwrap()
            .replace('\n', ending);
        input.insert_str(0, ending);
        let encoded =
            encode_cms_mlr_observations(input.as_bytes(), &copy_edition(input.as_bytes())).unwrap();
        let fields = copy_fields(&encoded.copy_bytes);
        assert_eq!(encoded.batch.observations[0].source_row_number, 3);
        assert_eq!(encoded.batch.observations[1].source_row_number, 5);
        assert_eq!(fields[0][1], b"row:3");
        assert_eq!(fields[1][1], b"row:5");
    }
}

#[test]
fn binary_copy_rejects_invalid_snapshots_without_echoing_identifiers() {
    let input = csv(&HEADERS, &[filing("1")]);
    for snapshot in [
        "synthetic-edition",
        "00000000-0000-0000-0000-000000000000",
        "0123456789abcdef8123456789abcdef",
        "{01234567-89ab-cdef-8123-456789abcdef}",
        "01234567-89ab-cdef-8123-456789abcdeg",
    ] {
        let mut metadata = copy_edition(&input);
        metadata.snapshot_id = snapshot.to_owned();
        let error = encode_cms_mlr_observations(&input, &metadata).unwrap_err();
        assert_eq!(error.code, "invalid_snapshot_id");
        assert!(error.source_row_number.is_none());
        assert!(!error.to_string().contains(snapshot));
        assert!(!serde_json::to_string(&error).unwrap().contains(snapshot));
    }
    let mut lowercase = copy_edition(&input);
    lowercase.snapshot_id.make_ascii_lowercase();
    let lower = encode_cms_mlr_observations(&input, &lowercase).unwrap();
    let upper = encode_cms_mlr_observations(&input, &copy_edition(&input)).unwrap();
    assert_eq!(
        copy_fields(&lower.copy_bytes)[0][0],
        copy_fields(&upper.copy_bytes)[0][0]
    );
}

#[test]
fn binary_copy_digest_malformed_rows_and_bounds_discard_the_entire_batch() {
    let input = csv(&HEADERS, &[filing("1")]);
    let mut wrong_digest = copy_edition(&input);
    wrong_digest.input_sha256 = "a".repeat(64);
    assert_eq!(
        encode_cms_mlr_observations(&input, &wrong_digest)
            .unwrap_err()
            .code,
        "digest_mismatch"
    );
    let mut malformed = input.clone();
    malformed.extend_from_slice(b"too,few,columns\n");
    assert_eq!(
        encode_cms_mlr_observations(&malformed, &copy_edition(&malformed))
            .unwrap_err()
            .code,
        "invalid_csv"
    );
    let oversized_input = vec![b' '; MAX_INPUT_BYTES + 1];
    assert_eq!(
        encode_cms_mlr_observations(&oversized_input, &copy_edition(&oversized_input))
            .unwrap_err()
            .code,
        "input_limit"
    );
    let mut oversized_field = filing("1");
    set(
        &mut oversized_field,
        "company_address",
        &"x".repeat(MAX_FIELD_BYTES + 1),
    );
    let input = csv(&HEADERS, &[filing("0"), oversized_field]);
    assert_eq!(
        encode_cms_mlr_observations(&input, &copy_edition(&input))
            .unwrap_err()
            .code,
        "field_limit"
    );
    let filings: Vec<_> = (0..=MAX_ROWS)
        .map(|index| filing(&index.to_string()))
        .collect();
    let input = csv(&HEADERS, &filings);
    assert_eq!(
        encode_cms_mlr_observations(&input, &copy_edition(&input))
            .unwrap_err()
            .code,
        "row_limit"
    );
}

#[test]
fn binary_copy_has_a_whole_batch_output_bound_even_when_json_expands() {
    let filings: Vec<_> = (0..800)
        .map(|index| {
            let mut fields = filing(&index.to_string());
            set(
                &mut fields,
                "company_address",
                &"\u{1}".repeat(MAX_FIELD_BYTES),
            );
            fields
        })
        .collect();
    let smaller_input = csv(&HEADERS, &filings[..100]);
    let encoded =
        encode_cms_mlr_observations(&smaller_input, &copy_edition(&smaller_input)).unwrap();
    assert_eq!(encoded.row_count, 100);
    assert!(encoded.copy_bytes.len() < MAX_COPY_BYTES);
    let input = csv(&HEADERS, &filings);
    assert!(input.len() < MAX_INPUT_BYTES);
    let error = encode_cms_mlr_observations(&input, &copy_edition(&input)).unwrap_err();
    assert_eq!(error.code, "output_limit");
    assert!(error.source_row_number.unwrap() > 100);
}

#[test]
fn binary_copy_rejects_jsonb_incompatible_text_without_partial_output() {
    let mut invalid = filing("2");
    set(&mut invalid, "company_address", "Synthetic\0address");
    let input = csv(&HEADERS, &[filing("1"), invalid]);
    let metadata = copy_edition(&input);
    let parsed = parse_cms_mlr_edition(&input, &metadata).unwrap();
    assert_eq!(
        parsed.observations[1].raw_fields["company_address"],
        "Synthetic\0address"
    );
    let error = encode_cms_mlr_observations(&input, &metadata).unwrap_err();
    assert_eq!(error.code, "invalid_jsonb");
    assert_eq!(error.source_row_number, Some(3));
    assert!(!serde_json::to_string(&error).unwrap().contains("Synthetic"));
}
