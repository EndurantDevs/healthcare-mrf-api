// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

use ptg2_scanner::cms_mlr_registry::RowStatus;
use ptg2_scanner::cms_planfinder_registry::{
    encode_cms_planfinder_observations, parse_cms_planfinder_batch, PlanFinderEdition,
    COPY_COLUMNS, HEADERS, LAYOUT, MAX_COPY_BYTES, MAX_FIELD_BYTES, MAX_INPUT_BYTES, MAX_ROWS,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

fn row(position: usize) -> Value {
    let mut values = vec![Value::Null; HEADERS.len()];
    let mut raw_values = values.clone();
    let mut cell_types = values.clone();
    let mut styles = values.clone();
    for (index, raw, normalized, kind, style) in [
        (0, "1234", "01234", "n", 0),
        (1, "Example Legal Company", "Example Legal Company", "s", 0),
        (2, "Example Brand", "Example Brand", "s", 0),
        (3, "CA", "CA", "s", 0),
        (4, "YES", "YES", "s", 0),
        (8, "12345678", "012345678", "n", 0),
        (9, "NO", "NO", "s", 0),
        (10, "46000.5", "46000.5", "n", 1),
        (12, "37.0", "37", "n", 0),
        (17, "1234", "01234", "n", 0),
    ] {
        raw_values[index] = json!(raw);
        values[index] = json!(normalized);
        cell_types[index] = json!(kind);
        styles[index] = json!(style);
    }
    json!({"source_row":position,"values":values,"raw_values":raw_values,
        "cell_types":cell_types,"style_ids":styles})
}

fn set_text(row: &mut Value, index: usize, value: Option<&str>) {
    row["raw_values"][index] = json!(value);
    row["values"][index] = json!(value);
    row["cell_types"][index] = if value.is_some() {
        json!("s")
    } else {
        Value::Null
    };
    row["style_ids"][index] = if value.is_some() {
        json!(0)
    } else {
        Value::Null
    };
}

fn document(rows: Vec<Value>) -> Value {
    json!({"component":"cms_planfinder_workbook_input","revision":1,"layout":LAYOUT,
        "artifact_sha256":"a".repeat(64),"workbook_sha256":"b".repeat(64),
        "sheet":"ISSUER_1","headers":HEADERS,"rows":rows})
}

fn input(rows: Vec<Value>) -> Vec<u8> {
    serde_json::to_vec(&document(rows)).unwrap()
}

fn edition(_bytes: &[u8]) -> PlanFinderEdition {
    PlanFinderEdition {
        snapshot_id: "01234567-89ab-cdef-8123-456789abcdef".to_owned(),
        reporting_year: 2026,
        input_sha256: "b".repeat(64),
    }
}

// Independent parser checks the native binary protocol and complete trailer.
fn copy_fields(copy: &[u8]) -> Vec<Vec<&[u8]>> {
    assert_eq!(&copy[..19], b"PGCOPY\n\xff\r\n\0\0\0\0\0\0\0\0\0");
    let mut offset = 19;
    let mut rows = Vec::new();
    loop {
        let count = i16::from_be_bytes(copy[offset..offset + 2].try_into().unwrap());
        offset += 2;
        if count == -1 {
            break;
        }
        assert_eq!(count, 6);
        let mut fields = Vec::new();
        for _ in 0..count {
            let length = i32::from_be_bytes(copy[offset..offset + 4].try_into().unwrap());
            offset += 4;
            assert!(length >= 0);
            fields.push(&copy[offset..offset + length as usize]);
            offset += length as usize;
        }
        rows.push(fields);
    }
    assert_eq!(offset, copy.len());
    rows
}

fn jsonb(bytes: &[u8]) -> Value {
    assert_eq!(bytes[0], 1);
    serde_json::from_slice(&bytes[1..]).unwrap()
}

#[test]
fn exact_source_fields_typed_evidence_and_leading_zeros_survive() {
    let source_row = row(2);
    let bytes = input(vec![source_row.clone()]);
    let metadata = edition(&bytes);
    let batch = parse_cms_planfinder_batch(&bytes, &metadata).unwrap();
    let observation = &batch.observations[0];
    assert_eq!(observation.hios.as_deref(), Some("01234"));
    assert_eq!(observation.normalized_ein.as_deref(), Some("012345678"));
    assert_eq!(observation.state.as_deref(), Some("CA"));
    assert_eq!(
        observation.company_key.as_deref(),
        Some("cms_planfinder:ein:012345678")
    );
    assert_eq!(observation.raw_fields.len(), 19);
    assert_eq!(
        observation.raw_fields["federal_ein"].as_deref(),
        Some("12345678")
    );
    assert_eq!(
        observation.raw_fields["issr_lgl_name"].as_deref(),
        Some("Example Legal Company")
    );
    assert!(!observation.raw_fields.contains_key("company_name"));
    assert!(!observation
        .raw_fields
        .contains_key("mr_submission_template_id"));
    assert!(!observation.raw_fields.contains_key("business_state"));
    let evidence = serde_json::to_value(&observation.source_evidence).unwrap();
    assert_eq!(evidence["source_row"], source_row["source_row"]);
    for field in ["values", "raw_values", "cell_types", "style_ids"] {
        assert_eq!(evidence[field], source_row[field]);
    }
    assert_eq!(evidence.as_object().unwrap().len(), 13);
    assert_eq!(evidence["component"], "cms_planfinder_workbook_input");
    assert_eq!(evidence["revision"], 1);
    assert_eq!(observation.source_evidence.headers, HEADERS);
    assert_eq!(observation.source_evidence.sheet, "ISSUER_1");
    assert_eq!(
        observation.source_evidence.batch_sha256,
        Sha256::digest(&bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );
    assert_ne!(
        observation.source_evidence.batch_sha256,
        metadata.input_sha256
    );
    assert_eq!(observation.source_evidence.artifact_sha256, "a".repeat(64));
    assert_eq!(observation.source_evidence.workbook_sha256, "b".repeat(64));
    assert_eq!(observation.submission_id, "row:2");
    assert_eq!(observation.status, RowStatus::Accepted);
    assert!(observation.normalized_naic_company.is_none());
    assert!(observation.normalized_naic_group.is_none());
    assert!(observation.group_kind.is_none());
    assert_eq!(batch.counts.accepted_rows, 1);
}

#[test]
fn missing_company_ein_and_jurisdiction_are_unresolved_with_valid_issuer() {
    let mut missing = row(2);
    for index in [1, 3, 8] {
        set_text(&mut missing, index, None);
    }
    let bytes = input(vec![missing]);
    let batch = parse_cms_planfinder_batch(&bytes, &edition(&bytes)).unwrap();
    let observation = &batch.observations[0];
    assert_eq!(observation.status, RowStatus::Unresolved);
    assert_eq!(observation.hios.as_deref(), Some("01234"));
    assert!(observation.company_key.is_none() && observation.state.is_none());
    assert_eq!(observation.issues.len(), 3);
    assert!(observation.issues.iter().all(|issue| !issue.rejecting));
    assert_eq!(batch.counts.input_rows, 1);
    assert_eq!(batch.counts.unresolved_rows, 1);
    assert_eq!(batch.counts.source_company_anchors, 0);
}

#[test]
fn bad_identifier_and_state_semantics_preserve_complete_row_accounting() {
    let mut rejected = row(3);
    set_text(&mut rejected, 0, Some("１２３４５"));
    set_text(&mut rejected, 3, Some("ZZ"));
    set_text(&mut rejected, 8, Some("00-0000000"));
    let mut missing = row(4);
    set_text(&mut missing, 8, None);
    let bytes = input(vec![row(2), rejected, missing]);
    let batch = parse_cms_planfinder_batch(&bytes, &edition(&bytes)).unwrap();
    assert_eq!(batch.counts.accepted_rows, 1);
    assert_eq!(batch.counts.rejected_rows, 1);
    assert_eq!(batch.counts.unresolved_rows, 1);
    assert_eq!(batch.observations[1].source_row_number, 3);
    assert_eq!(
        batch.observations[1].raw_fields["state"].as_deref(),
        Some("ZZ")
    );
    assert!(batch.observations[1].hios.is_none());
    assert!(batch.observations[1].normalized_ein.is_none());
    assert_eq!(
        batch.counts.input_rows,
        batch.counts.accepted_rows + batch.counts.unresolved_rows + batch.counts.rejected_rows
    );
}

#[test]
fn no_name_merge_group_inheritance_or_flag_date_company_id_authority() {
    let mut second = row(3);
    set_text(&mut second, 8, Some("023456789"));
    set_text(&mut second, 12, Some("37"));
    set_text(&mut second, 9, Some("YES"));
    let bytes = input(vec![row(2), second]);
    let batch = parse_cms_planfinder_batch(&bytes, &edition(&bytes)).unwrap();
    assert_eq!(batch.counts.source_company_anchors, 2);
    assert_eq!(batch.counts.accepted_rows, 2);
    assert_ne!(
        batch.observations[0].company_key,
        batch.observations[1].company_key
    );
    for observation in &batch.observations {
        assert!(observation.group_kind.is_none());
        assert!(observation.normalized_naic_group.is_none());
        assert_eq!(
            observation.raw_fields["datecreated"].as_deref(),
            Some("46000.5")
        );
        assert!(observation.raw_fields.contains_key("databasecompanyid"));
        let serialized = serde_json::to_value(observation).unwrap();
        assert!(serialized.get("current_owner").is_none());
        assert!(serialized.get("effective_from").is_none());
    }
}

#[test]
fn same_ein_differing_labels_are_independent_source_assertions() {
    let mut second = row(3);
    set_text(&mut second, 1, Some("Other Example Company"));
    set_text(&mut second, 3, Some("TX"));
    let bytes = input(vec![row(2), second]);
    let batch = parse_cms_planfinder_batch(&bytes, &edition(&bytes)).unwrap();
    assert_eq!(batch.observations.len(), 2);
    assert_eq!(batch.counts.source_company_anchors, 1);
    assert_ne!(
        batch.observations[0].raw_fields["issr_lgl_name"],
        batch.observations[1].raw_fields["issr_lgl_name"]
    );
}

#[test]
fn numeric_presentation_is_exact_and_fractions_are_not_rounded() {
    for (raw, normalized, accepted) in [
        ("1.234E3", "01234", true),
        ("1234.0", "01234", true),
        (
            "1234.000000000000000000001",
            "1234.000000000000000000001",
            false,
        ),
        ("1234.5", "1234.5", false),
        ("-12", "-12", false),
        ("123456", "123456", false),
    ] {
        let mut candidate = row(2);
        candidate["raw_values"][0] = json!(raw);
        candidate["values"][0] = json!(normalized);
        let bytes = input(vec![candidate]);
        let batch = parse_cms_planfinder_batch(&bytes, &edition(&bytes)).unwrap();
        assert_eq!(
            batch.observations[0].status == RowStatus::Accepted,
            accepted
        );
    }
}

#[test]
fn ordinary_text_ein_formatting_and_business_state_preserve_raw_evidence() {
    let mut candidate = row(2);
    set_text(&mut candidate, 0, Some("00123"));
    set_text(&mut candidate, 8, Some("01-2345678"));
    set_text(&mut candidate, 3, Some("ca"));
    set_text(&mut candidate, 16, Some("NY"));
    let bytes = input(vec![candidate]);
    let batch = parse_cms_planfinder_batch(&bytes, &edition(&bytes)).unwrap();
    assert_eq!(batch.observations[0].hios.as_deref(), Some("00123"));
    assert_eq!(batch.observations[0].state.as_deref(), Some("CA"));
    assert_eq!(
        batch.observations[0].raw_fields["org_state"].as_deref(),
        Some("NY")
    );
    assert_eq!(
        batch.observations[0].raw_fields["federal_ein"].as_deref(),
        Some("01-2345678")
    );
}

#[test]
fn exact_six_column_copy_matches_observation_and_issue_metadata() {
    let mut missing = row(5002);
    set_text(&mut missing, 8, None);
    let bytes = input(vec![row(5001), missing]);
    let metadata = edition(&bytes);
    let encoded = encode_cms_planfinder_observations(&bytes, &metadata).unwrap();
    let fields = copy_fields(&encoded.copy_bytes);
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
    assert_eq!(fields.len(), 2);
    assert_eq!(encoded.row_count, 2);
    assert_eq!(
        fields[0][0],
        &[1, 35, 69, 103, 137, 171, 205, 239, 129, 35, 69, 103, 137, 171, 205, 239]
    );
    assert_eq!(fields[0][1], b"row:5001");
    assert_eq!(i32::from_be_bytes(fields[0][2].try_into().unwrap()), 5001);
    assert_eq!(fields[0][3], b"accepted");
    assert_eq!(fields[1][3], b"unresolved");
    assert_eq!(
        jsonb(fields[0][4]),
        serde_json::to_value(&encoded.batch.observations[0]).unwrap()
    );
    assert_eq!(
        jsonb(fields[1][5]),
        serde_json::to_value(&encoded.batch.observations[1].issues).unwrap()
    );
    assert_eq!(
        encoded.copy_bytes,
        encode_cms_planfinder_observations(&bytes, &metadata)
            .unwrap()
            .copy_bytes
    );
}

#[test]
fn strict_envelope_header_layout_digest_and_row_provenance() {
    let original = document(vec![row(2)]);
    for (key, value) in [
        ("sheet", json!("PRODUCT_1")),
        ("revision", json!(2)),
        ("layout", json!("unknown-layout")),
        ("component", json!("other")),
        ("artifact_sha256", json!("A".repeat(64))),
        ("workbook_sha256", json!("short")),
    ] {
        let mut invalid = original.clone();
        invalid[key] = value;
        let bytes = serde_json::to_vec(&invalid).unwrap();
        assert_eq!(
            parse_cms_planfinder_batch(&bytes, &edition(&bytes))
                .unwrap_err()
                .code,
            "invalid_layout"
        );
    }
    for invalid in [
        {
            let mut doc = original.clone();
            doc["unknown"] = json!(true);
            doc
        },
        {
            let mut doc = original.clone();
            doc["headers"][1] = json!("company_name");
            doc
        },
        {
            let mut doc = original.clone();
            doc["rows"][0]["unknown"] = json!(true);
            doc
        },
        {
            let mut doc = original.clone();
            doc["rows"][0]["cell_types"][0] = json!("b");
            doc
        },
        {
            let mut doc = original.clone();
            doc["rows"][0]["source_row"] = json!(true);
            doc
        },
    ] {
        let bytes = serde_json::to_vec(&invalid).unwrap();
        assert!(parse_cms_planfinder_batch(&bytes, &edition(&bytes)).is_err());
    }
    let bytes = input(vec![row(2), row(4)]);
    assert_eq!(
        parse_cms_planfinder_batch(&bytes, &edition(&bytes))
            .unwrap_err()
            .code,
        "invalid_source_row"
    );
    let bytes = input(vec![row(1)]);
    assert_eq!(
        parse_cms_planfinder_batch(&bytes, &edition(&bytes))
            .unwrap_err()
            .code,
        "invalid_source_row"
    );
}

#[test]
fn modified_raw_values_types_styles_or_arrays_fail_without_partial_copy() {
    for (field, index, value) in [
        ("values", 0, json!("09999")),
        ("raw_values", 0, json!("9999")),
        ("style_ids", 0, json!(3)),
        ("cell_types", 0, Value::Null),
        ("values", 13, json!("invented missing field")),
    ] {
        let mut invalid = row(2);
        invalid[field][index] = value;
        let bytes = input(vec![row(1_000), {
            invalid["source_row"] = json!(1_001);
            invalid
        }]);
        assert_eq!(
            encode_cms_planfinder_observations(&bytes, &edition(&bytes))
                .unwrap_err()
                .code,
            "invalid_cell"
        );
    }
    let mut invalid = row(2);
    invalid["values"].as_array_mut().unwrap().pop();
    let bytes = input(vec![invalid]);
    assert_eq!(
        encode_cms_planfinder_observations(&bytes, &edition(&bytes))
            .unwrap_err()
            .code,
        "invalid_json"
    );
}

#[test]
fn input_row_field_metadata_limits_and_postgres_text_fail_closed() {
    let bytes = input(vec![row(2)]);
    let mut metadata = edition(&bytes);
    metadata.input_sha256 = "c".repeat(64);
    assert_eq!(
        parse_cms_planfinder_batch(&bytes, &metadata)
            .unwrap_err()
            .code,
        "digest_mismatch"
    );
    for bad_snapshot in ["not-uuid", "00000000-0000-0000-0000-000000000000"] {
        metadata = edition(&bytes);
        metadata.snapshot_id = bad_snapshot.to_owned();
        assert_eq!(
            encode_cms_planfinder_observations(&bytes, &metadata)
                .unwrap_err()
                .code,
            "invalid_edition"
        );
    }
    metadata = edition(&bytes);
    metadata.reporting_year = 2009;
    assert_eq!(
        parse_cms_planfinder_batch(&bytes, &metadata)
            .unwrap_err()
            .code,
        "invalid_edition"
    );
    assert_eq!(
        parse_cms_planfinder_batch(&vec![b' '; MAX_INPUT_BYTES + 1], &metadata)
            .unwrap_err()
            .code,
        "input_limit"
    );
    let bytes = input(Vec::new());
    assert_eq!(
        parse_cms_planfinder_batch(&bytes, &edition(&bytes))
            .unwrap_err()
            .code,
        "row_limit"
    );
    for bad_text in ["x".repeat(MAX_FIELD_BYTES + 1), "name\0suffix".to_owned()] {
        let mut candidate = row(2);
        set_text(&mut candidate, 1, Some(&bad_text));
        let bytes = input(vec![candidate]);
        assert_eq!(
            encode_cms_planfinder_observations(&bytes, &edition(&bytes))
                .unwrap_err()
                .code,
            "invalid_field"
        );
    }
}

#[test]
fn complete_maximum_batch_and_one_extra_row_have_exact_accounting() {
    let bytes = input((2..MAX_ROWS + 2).map(row).collect());
    assert!(bytes.len() < MAX_INPUT_BYTES);
    let result = encode_cms_planfinder_observations(&bytes, &edition(&bytes)).unwrap();
    assert_eq!(result.row_count, MAX_ROWS);
    assert_eq!(copy_fields(&result.copy_bytes).len(), MAX_ROWS);
    assert!(result.copy_bytes.len() <= MAX_COPY_BYTES);
    let bytes = input((2..MAX_ROWS + 3).map(row).collect());
    assert_eq!(
        encode_cms_planfinder_observations(&bytes, &edition(&bytes))
            .unwrap_err()
            .code,
        "row_limit"
    );
}

#[test]
fn error_messages_never_include_source_payload() {
    let bytes = b"{\"private-example-payload\":42}";
    let error = parse_cms_planfinder_batch(bytes, &edition(bytes)).unwrap_err();
    assert!(!error.to_string().contains("private-example-payload"));
    assert!(!serde_json::to_string(&error)
        .unwrap()
        .contains("private-example-payload"));
}
