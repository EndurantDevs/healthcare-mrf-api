// Licensed under the HealthPorta Non-Commercial License (see LICENSE).
//! Synthetic component checks; callers retain archive custody and publication authority.

use ptg2_scanner::{cms_mlr_registry, naic_company_registry};

use cms_mlr_registry::RowStatus;
use naic_company_registry::{
    encode_naic_company_registry_observations, parse_naic_company_registry_member, NaicEdition,
    NaicMember, COPY_COLUMNS, MAX_COPY_BYTES, MAX_FIELD_BYTES, MAX_INPUT_BYTES, MAX_ROWS,
    PARSER_VERSION,
};
use serde_json::Value;
use sha2::{Digest, Sha256};

fn digest(input: &[u8]) -> String {
    Sha256::digest(input)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn row(member: NaicMember, company: &str, group: &str) -> Vec<Vec<u8>> {
    member
        .headers()
        .iter()
        .map(|header| match *header {
            "COMPANY NAME" => b"Synthetic Company".to_vec(),
            "GROUP NAME" => b"Synthetic Group".to_vec(),
            "COMPANY CODE" => company.as_bytes().to_vec(),
            "GROUP CODE" => group.as_bytes().to_vec(),
            "FEIN NUMBER" => b"01-2345678".to_vec(),
            "COMPANY STATUS" => b"1".to_vec(),
            "STATE OF DOMICILE" => b"CA".to_vec(),
            _ => Vec::new(),
        })
        .collect()
}

fn set(member: NaicMember, row: &mut [Vec<u8>], header: &str, value: &[u8]) {
    row[member
        .headers()
        .iter()
        .position(|candidate| *candidate == header)
        .unwrap()] = value.to_vec();
}

fn csv(member: NaicMember, rows: &[Vec<Vec<u8>>]) -> Vec<u8> {
    let mut writer = csv::WriterBuilder::new()
        .terminator(csv::Terminator::CRLF)
        .from_writer(Vec::new());
    writer.write_record(member.headers()).unwrap();
    for row in rows {
        writer.write_record(row).unwrap();
    }
    writer.into_inner().unwrap()
}

fn edition(member: NaicMember, input: &[u8]) -> NaicEdition {
    NaicEdition {
        snapshot_id: "01234567-89ab-cdef-8123-456789abcdef".to_owned(),
        edition_id: "2026-synthetic-edition".to_owned(),
        reporting_year: 2026,
        artifact_sha256: "a".repeat(64),
        member,
        member_sha256: digest(input),
        layout: member.layout().to_owned(),
        layout_pdf_sha256: "b".repeat(64),
        header_sha256: digest(&serde_json::to_vec(member.headers()).unwrap()),
        parser_version: PARSER_VERSION.to_owned(),
    }
}

fn parse(member: NaicMember, rows: &[Vec<Vec<u8>>]) -> naic_company_registry::NaicRegistryBatch {
    let input = csv(member, rows);
    parse_naic_company_registry_member(&input, &edition(member, &input)).unwrap()
}

#[test]
fn inspected_member_layouts_keep_distinct_company_and_group_namespaces() {
    for member in [NaicMember::Health, NaicMember::GroupCompanies] {
        let batch = parse(member, &[row(member, "707", "00707")]);
        let observation = &batch.observations[0];
        assert_eq!(
            observation.normalized_naic_company.as_deref(),
            Some("00707")
        );
        assert_eq!(observation.normalized_naic_group.as_deref(), Some("707"));
        assert_eq!(observation.normalized_ein.as_deref(), Some("012345678"));
        assert_eq!(observation.raw_fields["COMPANY CODE"], "707");
        assert_eq!(observation.raw_fields["GROUP CODE"], "00707");
        assert_eq!(
            observation.source_record_key,
            format!("{}:row:2", member.name())
        );
        assert_eq!(observation.source_evidence.member_name, member.name());
        assert_eq!(
            observation.source_evidence.raw_fields_hex.len(),
            member.headers().len()
        );
        assert_eq!(observation.group_kind, Some("naic_group"));
        assert_eq!(observation.relationship_kind, Some("reported_affiliation"));
        assert!(observation.valid_from.is_none() && observation.valid_to.is_none());
        assert_eq!(observation.status, RowStatus::Unresolved);
        assert!(observation
            .issues
            .iter()
            .any(|issue| issue.code == "unknown_effective_date"));
        assert_eq!(
            batch.counts.input_rows,
            batch.counts.accepted_rows + batch.counts.unresolved_rows + batch.counts.rejected_rows
        );
    }
    let batch = parse(NaicMember::Groups, &[row(NaicMember::Groups, "", "707")]);
    assert_eq!(batch.observations[0].row_kind, "group");
    assert!(batch.observations[0].normalized_naic_company.is_none());
    assert!(batch.observations[0].relationship_kind.is_none());
    assert_eq!(batch.counts.accepted_rows, 1);
}

#[test]
fn uninterpreted_non_ascii_and_control_bytes_survive_without_a_charset() {
    let member = NaicMember::Health;
    let mut submitted = row(member, "00456", "707");
    set(member, &mut submitted, "ADDRESS", b"Street \xa0\xd3\0");
    set(member, &mut submitted, "CITY", b"Place \xd3");
    let input = csv(member, &[submitted]);
    let batch = parse_naic_company_registry_member(&input, &edition(member, &input)).unwrap();
    let observation = &batch.observations[0];
    assert!(!observation.raw_fields.contains_key("ADDRESS"));
    assert_eq!(
        observation.source_evidence.raw_fields_hex["ADDRESS"],
        "53747265657420a0d300"
    );
    assert_eq!(
        observation.source_evidence.raw_fields_hex["CITY"],
        "506c61636520d3"
    );
    assert_eq!(observation.source_evidence.member_sha256, digest(&input));
    let source = &input[observation.source_evidence.source_byte_start
        ..observation.source_evidence.source_byte_end];
    assert_eq!(
        observation.source_evidence.raw_record_sha256,
        digest(source)
    );
    assert!(source.windows(3).any(|bytes| bytes == b"\xa0\xd3\0"));
    let encoded =
        encode_naic_company_registry_observations(&input, &edition(member, &input)).unwrap();
    assert_eq!(encoded.row_count, 1);
}

#[test]
fn projected_non_ascii_or_control_fields_are_rejected_with_raw_evidence() {
    for field in [
        "COMPANY CODE",
        "GROUP CODE",
        "FEIN NUMBER",
        "COMPANY NAME",
        "STATE OF DOMICILE",
        "COMPANY STATUS",
    ] {
        let mut submitted = row(NaicMember::Health, "00456", "707");
        set(NaicMember::Health, &mut submitted, field, b"bad\xd3\0");
        let batch = parse(NaicMember::Health, &[submitted]);
        assert_eq!(batch.counts.rejected_rows, 1);
        assert!(batch.observations[0]
            .issues
            .iter()
            .any(|issue| issue.field == field && issue.code == "invalid_ascii_projection"));
        assert_eq!(
            batch.observations[0].source_evidence.raw_fields_hex[field],
            "626164d300"
        );
    }
}

#[test]
fn missing_and_invalid_identifiers_remain_exactly_accounted_for() {
    let member = NaicMember::GroupCompanies;
    let mut missing = row(member, "", "");
    set(member, &mut missing, "FEIN NUMBER", b"");
    let invalid = row(member, "00000", "-7");
    let mut valid = row(member, "00456", "707");
    set(member, &mut valid, "FEIN NUMBER", b"012345678");
    let batch = parse(member, &[missing, invalid, valid]);
    assert_eq!(
        (
            batch.counts.input_rows,
            batch.counts.accepted_rows,
            batch.counts.unresolved_rows,
            batch.counts.rejected_rows
        ),
        (3, 0, 2, 1)
    );
    assert!(batch.observations[0].normalized_naic_company.is_none());
    assert!(batch.observations[1].normalized_naic_group.is_none());
    assert_eq!(
        batch.observations[2].normalized_ein.as_deref(),
        Some("012345678")
    );
    for invalid in [
        b"1-23456789".as_slice(),
        b"00-0000000",
        b"1e3",
        b"01 2345678",
    ] {
        let mut submitted = row(member, "456", "707");
        set(member, &mut submitted, "FEIN NUMBER", invalid);
        assert_eq!(parse(member, &[submitted]).counts.rejected_rows, 1);
    }
}

#[test]
fn agreeing_rows_and_identical_labels_never_merge_distinct_identifiers() {
    let member = NaicMember::Groups;
    let first = row(member, "", "707");
    let second = row(member, "", "808");
    let batch = parse(member, &[first.clone(), second, first]);
    assert_eq!(batch.counts.input_rows, 3);
    assert_eq!(batch.counts.distinct_naic_group_codes, 2);
    assert_eq!(batch.counts.accepted_rows, 3);
    assert!(batch.conflicts.is_empty());
    assert_eq!(
        batch
            .observations
            .iter()
            .map(|row| row.source_record_key.as_str())
            .collect::<Vec<_>>(),
        vec!["GPAL.csv:row:2", "GPAL.csv:row:3", "GPAL.csv:row:4"]
    );
}

#[test]
fn exact_company_identifier_conflicts_preserve_all_rows_without_choosing_a_group() {
    let member = NaicMember::GroupCompanies;
    let rows = [row(member, "456", "707"), row(member, "00456", "808")];
    let batch = parse(member, &rows);
    assert_eq!(batch.observations.len(), 2);
    assert_eq!(batch.counts.unresolved_rows, 2);
    assert_eq!(batch.conflicts.len(), 1);
    assert_eq!(batch.conflicts[0].field, "GROUP CODE");
    assert_eq!(batch.conflicts[0].values, vec!["707", "808"]);
    assert_eq!(batch.conflicts[0].source_rows, vec![2, 3]);
    assert_eq!(
        batch.observations[0].normalized_naic_group.as_deref(),
        Some("707")
    );
    assert_eq!(
        batch.observations[1].normalized_naic_group.as_deref(),
        Some("808")
    );
    let batch = parse(
        member,
        &[row(member, "456", "707"), row(member, "457", "707")],
    );
    assert_eq!(batch.observations.len(), 2);
    assert_eq!(batch.conflicts.len(), 1);
    assert_eq!(batch.conflicts[0].field, "COMPANY CODE");
    assert_eq!(batch.conflicts[0].values, vec!["00456", "00457"]);
    assert_eq!(batch.conflicts[0].source_rows, vec![2, 3]);
    let mut other_ein = row(member, "00456", "707");
    set(member, &mut other_ein, "FEIN NUMBER", b"02-3456789");
    let batch = parse(member, &[row(member, "456", "707"), other_ein]);
    assert_eq!(batch.observations.len(), 2);
    assert_eq!(batch.conflicts.len(), 1);
    assert_eq!(batch.conflicts[0].field, "FEIN NUMBER");
    assert_eq!(batch.conflicts[0].values, vec!["012345678", "023456789"]);
}

#[test]
fn member_qualified_provenance_and_declared_status_never_infer_dates_or_archive() {
    for status in ["0", "1", "4", "6"] {
        let mut submitted = row(NaicMember::Health, "456", "707");
        set(
            NaicMember::Health,
            &mut submitted,
            "COMPANY STATUS",
            status.as_bytes(),
        );
        let batch = parse(NaicMember::Health, &[submitted]);
        assert_eq!(batch.observations[0].raw_fields["COMPANY STATUS"], status);
        let document = serde_json::to_value(&batch.observations[0]).unwrap();
        assert!(document["valid_from"].is_null() && document["valid_to"].is_null());
        for forbidden in [
            "company_id",
            "group_id",
            "hios",
            "archived",
            "approved",
            "verified_ownership",
        ] {
            assert!(document.get(forbidden).is_none());
        }
    }
}

#[test]
fn exact_inspected_header_spaces_and_order_are_required() {
    let member = NaicMember::Health;
    let original = csv(member, &[row(member, "456", "707")]);
    for replacement in ["CRIN", "crin ", "ZIP", "UNKNOWN"] {
        let header = if replacement == "ZIP" {
            "ZIP "
        } else {
            "CRIN "
        };
        let changed = String::from_utf8(original.clone())
            .unwrap()
            .replacen(header, replacement, 1)
            .into_bytes();
        assert_eq!(
            parse_naic_company_registry_member(&changed, &edition(member, &changed))
                .unwrap_err()
                .code,
            "invalid_headers"
        );
    }
    let changed = String::from_utf8(original.clone())
        .unwrap()
        .replacen(
            "COMPANY NAME,STATE OF DOMICILE",
            "STATE OF DOMICILE,COMPANY NAME",
            1,
        )
        .into_bytes();
    assert_eq!(
        parse_naic_company_registry_member(&changed, &edition(member, &changed))
            .unwrap_err()
            .code,
        "invalid_headers"
    );
    let changed = [b"\xef\xbb\xbf".as_slice(), original.as_slice()].concat();
    assert_eq!(
        parse_naic_company_registry_member(&changed, &edition(member, &changed))
            .unwrap_err()
            .code,
        "invalid_headers"
    );
}

#[test]
fn framing_rejects_blank_rows_unclosed_quotes_trailing_quote_text_and_wrong_width() {
    let member = NaicMember::Groups;
    let header = b"GROUP NAME,GROUP CODE\r\n";
    for suffix in [
        b"\r\n".as_slice(),
        b"\"unclosed,707\n",
        b"\"closed\"junk,707\n",
        b"bad\"quote,707\n",
        b"one,707,extra\n",
        b"only-one\n",
    ] {
        let input = [header.as_slice(), suffix].concat();
        assert_eq!(
            parse_naic_company_registry_member(&input, &edition(member, &input))
                .unwrap_err()
                .code,
            "invalid_csv"
        );
    }
}

#[test]
fn embedded_newlines_and_escaped_quotes_keep_exact_physical_spans() {
    let member = NaicMember::Health;
    let mut first = row(member, "456", "707");
    set(member, &mut first, "ADDRESS", b"First\r\nSecond \"quoted\"");
    let input = csv(member, &[first, row(member, "457", "808")]);
    let batch = parse_naic_company_registry_member(&input, &edition(member, &input)).unwrap();
    assert_eq!(
        batch
            .observations
            .iter()
            .map(|row| row.source_row_number)
            .collect::<Vec<_>>(),
        vec![2, 4]
    );
    assert_eq!(
        batch.observations[0].source_evidence.raw_fields_hex["ADDRESS"],
        "46697273740d0a5365636f6e64202271756f74656422"
    );
    let second = &batch.observations[1].source_evidence;
    assert_eq!(
        second.source_byte_start,
        batch.observations[0].source_evidence.source_byte_end + 2
    );
    assert_eq!(
        second.raw_record_sha256,
        digest(&input[second.source_byte_start..second.source_byte_end])
    );
}

#[test]
fn line_endings_and_unterminated_final_record_keep_physical_coordinates() {
    let member = NaicMember::Groups;
    for terminator in ["\n", "\r\n", "\r"] {
        for final_terminator in ["", terminator] {
            let input = format!(
                "GROUP NAME,GROUP CODE{terminator}Synthetic One,707{terminator}Synthetic Two,808{final_terminator}"
            ).into_bytes();
            let batch =
                parse_naic_company_registry_member(&input, &edition(member, &input)).unwrap();
            assert_eq!(batch.counts.input_rows, 2);
            assert_eq!(batch.observations[0].source_row_number, 2);
            assert_eq!(batch.observations[1].source_row_number, 3);
            let last = &batch.observations[1].source_evidence;
            assert_eq!(
                &input[last.source_byte_start..last.source_byte_end],
                b"Synthetic Two,808"
            );
            assert_eq!(last.source_byte_end + final_terminator.len(), input.len());
        }
    }
}

#[test]
fn wrong_member_bytes_or_interpretation_metadata_refuse_atomically() {
    let member = NaicMember::Groups;
    let input = csv(member, &[row(member, "", "707")]);
    let metadata = edition(member, &input);
    let mut wrong = metadata.clone();
    wrong.member_sha256 = "c".repeat(64);
    assert_eq!(
        parse_naic_company_registry_member(&input, &wrong)
            .unwrap_err()
            .code,
        "digest_mismatch"
    );
    let mut wrong = metadata.clone();
    wrong.header_sha256 = "c".repeat(64);
    assert_eq!(
        parse_naic_company_registry_member(&input, &wrong)
            .unwrap_err()
            .code,
        "header_digest_mismatch"
    );
    let mut documents = Vec::new();
    for field in [
        "artifact_sha256",
        "member_sha256",
        "layout_pdf_sha256",
        "header_sha256",
    ] {
        let mut document = serde_json::to_value(&metadata).unwrap();
        document[field] = Value::String("A".repeat(64));
        documents.push(document);
    }
    for (field, value) in [
        ("snapshot_id", "00000000-0000-0000-0000-000000000000"),
        ("snapshot_id", "01234567-89AB-CDEF-8123-456789ABCDEF"),
        ("layout", "unreviewed-layout"),
        ("parser_version", "unknown"),
        ("edition_id", " padded "),
    ] {
        let mut document = serde_json::to_value(&metadata).unwrap();
        document[field] = Value::String(value.to_owned());
        documents.push(document);
    }
    for document in documents {
        let wrong: NaicEdition = serde_json::from_value(document).unwrap();
        assert_eq!(
            encode_naic_company_registry_observations(&input, &wrong)
                .unwrap_err()
                .code,
            "invalid_edition"
        );
    }
    let mut wrong = metadata.clone();
    wrong.reporting_year = 2027;
    assert_eq!(
        parse_naic_company_registry_member(&input, &wrong)
            .unwrap_err()
            .code,
        "invalid_edition"
    );
    let mut wrong = metadata.clone();
    wrong.member = NaicMember::Health;
    assert_eq!(
        parse_naic_company_registry_member(&input, &wrong)
            .unwrap_err()
            .code,
        "invalid_edition"
    );
    let mut document = serde_json::to_value(metadata).unwrap();
    document["encoding"] = Value::String("guessed".to_owned());
    assert!(serde_json::from_value::<NaicEdition>(document).is_err());
}

#[test]
fn complete_copy_frame_preserves_member_key_row_json_and_private_hashes() {
    let member = NaicMember::Groups;
    let input = csv(member, &[row(member, "", "707"), row(member, "", "808")]);
    let metadata = edition(member, &input);
    let encoded = encode_naic_company_registry_observations(&input, &metadata).unwrap();
    assert_eq!(encoded.row_count, 2);
    assert_eq!(encoded.batch.counts.input_rows, 2);
    assert_eq!(
        &encoded.copy_bytes[..19],
        b"PGCOPY\n\xff\r\n\0\0\0\0\0\0\0\0\0"
    );
    let mut offset = 19;
    for expected_line in [2, 3] {
        assert_eq!(
            i16::from_be_bytes(encoded.copy_bytes[offset..offset + 2].try_into().unwrap()) as usize,
            COPY_COLUMNS.len()
        );
        offset += 2;
        let mut fields = Vec::new();
        for _ in COPY_COLUMNS {
            let length =
                i32::from_be_bytes(encoded.copy_bytes[offset..offset + 4].try_into().unwrap())
                    as usize;
            offset += 4;
            fields.push(&encoded.copy_bytes[offset..offset + length]);
            offset += length;
        }
        assert_eq!(
            fields[0],
            &[
                0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x81, 0x23, 0x45, 0x67, 0x89, 0xab,
                0xcd, 0xef
            ]
        );
        assert_eq!(
            fields[1],
            format!("GPAL.csv:row:{expected_line}").as_bytes()
        );
        assert_eq!(
            i32::from_be_bytes(fields[2].try_into().unwrap()),
            expected_line
        );
        assert_eq!(fields[3], b"accepted");
        assert_eq!(fields[4][0], 1);
        assert_eq!(fields[5][0], 1);
        let observation: Value = serde_json::from_slice(&fields[4][1..]).unwrap();
        assert_eq!(
            observation["source_evidence"]["artifact_sha256"],
            metadata.artifact_sha256
        );
        assert_eq!(
            observation["source_evidence"]["layout_pdf_sha256"],
            metadata.layout_pdf_sha256
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&fields[5][1..]).unwrap(),
            serde_json::json!([])
        );
    }
    assert_eq!(&encoded.copy_bytes[offset..], &(-1i16).to_be_bytes());
}

#[test]
fn repeat_input_is_byte_deterministic_and_member_caps_remain_local() {
    let member = NaicMember::Groups;
    let rows = vec![row(member, "", "707"); MAX_ROWS];
    let input = csv(member, &rows);
    let first =
        encode_naic_company_registry_observations(&input, &edition(member, &input)).unwrap();
    let second =
        encode_naic_company_registry_observations(&input, &edition(member, &input)).unwrap();
    assert_eq!(first.copy_bytes, second.copy_bytes);
    assert_eq!(first.row_count, MAX_ROWS);
    assert!(first.copy_bytes.len() <= MAX_COPY_BYTES);
    let too_many = csv(member, &vec![row(member, "", "707"); MAX_ROWS + 1]);
    assert_eq!(
        parse_naic_company_registry_member(&too_many, &edition(member, &too_many))
            .unwrap_err()
            .code,
        "row_limit"
    );
    let too_large = vec![b'x'; MAX_INPUT_BYTES + 1];
    assert_eq!(
        parse_naic_company_registry_member(&too_large, &edition(member, &too_large))
            .unwrap_err()
            .code,
        "input_limit"
    );
    let mut oversized = row(member, "", "707");
    oversized[0] = vec![b'x'; MAX_FIELD_BYTES + 1];
    let oversized = csv(member, &[oversized]);
    assert_eq!(
        parse_naic_company_registry_member(&oversized, &edition(member, &oversized))
            .unwrap_err()
            .code,
        "field_limit"
    );
}

#[test]
fn bounded_member_refuses_copy_expansion_without_returning_a_partial_frame() {
    let member = NaicMember::Health;
    let mut submitted = row(member, "456", "707");
    for header in [
        "RJIN FLAG",
        "CRIN ",
        "RJIN",
        "ADDRESS",
        "CITY",
        "STATE",
        "ZIP ",
        "PHONE",
    ] {
        set(member, &mut submitted, header, &vec![b'x'; MAX_FIELD_BYTES]);
    }
    let input = csv(member, &vec![submitted; 255]);
    assert!(input.len() <= MAX_INPUT_BYTES);
    assert_eq!(
        encode_naic_company_registry_observations(&input, &edition(member, &input))
            .unwrap_err()
            .code,
        "output_limit"
    );
}

#[test]
fn rejected_company_status_is_retained_in_complete_copy_output() {
    let member = NaicMember::Health;
    let mut submitted = row(member, "00456", "707");
    set(member, &mut submitted, "COMPANY STATUS", b"9");
    let input = csv(member, &[submitted]);
    let encoded =
        encode_naic_company_registry_observations(&input, &edition(member, &input)).unwrap();
    assert_eq!(encoded.row_count, 1);
    assert_eq!(encoded.batch.counts.rejected_rows, 1);
    assert!(encoded.batch.observations[0]
        .issues
        .iter()
        .any(|issue| issue.field == "COMPANY STATUS" && issue.code == "invalid_company_status"));
    assert!(encoded
        .copy_bytes
        .windows(8)
        .any(|bytes| bytes == b"rejected"));
    assert_eq!(
        &encoded.copy_bytes[encoded.copy_bytes.len() - 2..],
        &(-1i16).to_be_bytes()
    );
}

#[test]
fn empty_and_unterminated_headers_refuse_with_sanitized_errors() {
    for input in [b"".as_slice(), b"GROUP NAME"] {
        let error = parse_naic_company_registry_member(input, &edition(NaicMember::Groups, input))
            .unwrap_err();
        assert_eq!(error.code, "invalid_headers");
        assert_eq!(error.source_row_number, Some(1));
        assert_eq!(
            error.to_string(),
            "Company listing rejected: invalid_headers"
        );
    }
}
