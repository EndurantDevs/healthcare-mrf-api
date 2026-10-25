// Licensed under the HealthPorta Non-Commercial License (see LICENSE).
use ptg2_scanner::registry_business_target_ledger::{
    encode_business_target_ledger_artifact as encode, parse_business_target_ledger as parse,
    HEADERS,
};
use ptg2_scanner::registry_required_target_review::{
    encode_registry_required_target_review_artifact as review,
    validate_registry_required_target_ledger_artifact as validate,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

const SNAPSHOT: &str = "34567890-3456-7890-8345-34567890abcd";
fn input(rows: &[[&str; 6]]) -> Vec<u8> {
    let mut writer = csv::Writer::from_writer(Vec::new());
    writer.write_record(HEADERS).unwrap();
    for row in rows {
        writer.write_record(row).unwrap();
    }
    writer.into_inner().unwrap()
}
fn sha(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
fn document(copy: &[u8]) -> Value {
    let mut offset = 21;
    for i in 0..6 {
        let n = i32::from_be_bytes(copy[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        if i == 4 {
            return serde_json::from_slice(&copy[offset + 1..offset + n]).unwrap();
        }
        offset += n;
    }
    panic!("missing artifact")
}
#[test]
fn business_tuple_retains_rows_and_employer_context_without_merging_names() {
    let rows = [
        [
            "Employer one",
            "Employer one legal",
            "Plan A",
            " PPO ",
            " Carrier A ",
            " Network A ",
        ],
        [
            "Employer two",
            "Other employer",
            "Different plan",
            "PPO",
            "Carrier A",
            "Network A",
        ],
        [
            "Employer one",
            "Employer one legal",
            "Plan A",
            "Dental",
            "Carrier A",
            "Network A",
        ],
        [
            "Employer one",
            "Employer one legal",
            "Plan A",
            "PPO",
            "Carrier B",
            "Network A",
        ],
        [
            "Employer one",
            "Employer one legal",
            "Plan A",
            "PPO",
            "carrier a",
            "Network A",
        ],
    ];
    let ledger = parse(&input(&rows)).unwrap();
    assert_eq!(ledger.row_count, 5);
    assert_eq!(ledger.targets.len(), 4);
    assert_eq!(
        ledger.observations[0].target_keys,
        ledger.observations[1].target_keys
    );
    assert_eq!(ledger.observations[0].raw_cells[0], "Employer one");
    assert!(ledger
        .targets
        .iter()
        .any(|t| t.source_row_ordinals == [1, 2]));
    let mut reversed = rows;
    reversed.reverse();
    assert_eq!(
        ledger
            .targets
            .iter()
            .map(|t| &t.target_key)
            .collect::<Vec<_>>(),
        parse(&input(&reversed))
            .unwrap()
            .targets
            .iter()
            .map(|t| &t.target_key)
            .collect::<Vec<_>>()
    );
}
#[test]
fn blank_networks_deduplicate_and_remain_unresolved_review_targets() {
    let bytes = input(&[
        ["E", "Employer", "A", "Vision", "Carrier", " "],
        ["Other", "Other employer", "B", "Vision", "Carrier", ""],
    ]);
    let ledger = parse(&bytes).unwrap();
    assert_eq!(ledger.targets.len(), 1);
    assert_eq!(ledger.targets[0].source_row_ordinals, [1, 2]);
    let (copy, descriptor) = encode(&bytes, SNAPSHOT).unwrap();
    let doc = document(&copy);
    let d: Value = serde_json::from_slice(&descriptor).unwrap();
    assert_eq!(d["parser_version"], "registry-target-ledger-v3");
    let retained = json!({"snapshot_id":SNAPSHOT,"artifact_sha256":sha(&serde_json::to_vec(&doc).unwrap()),"document":doc});
    let verified: Value =
        serde_json::from_slice(&validate(&serde_json::to_vec(&retained).unwrap()).unwrap())
            .unwrap();
    assert_eq!(verified["target_count"], 1);
    let unresolved = json!({"target_key":ledger.targets[0].target_key,"resolution_status":"unresolved","network_id":null,"source_binding":null,"evidence_reference":"Review needed","evidence_sha256":"a".repeat(64),"reason":"No network label supplied"});
    let request = json!({"ledger_snapshot_id":SNAPSHOT,"ledger_artifact_sha256":retained["artifact_sha256"],"decisions":[unresolved]});
    assert!(review(
        &serde_json::to_vec(&request).unwrap(),
        &serde_json::to_vec(&retained["document"]).unwrap(),
        "45678901-4567-8901-8456-45678901abcd"
    )
    .is_ok());
    let mut resolved = request;
    resolved["decisions"][0]["resolution_status"] = json!("resolved");
    resolved["decisions"][0]["network_id"] = json!(42);
    resolved["decisions"][0]["source_binding"] = json!({"binding_id":"01234567-89ab-cdef-8123-456789abcdef","source_system":"ptg","source_id":"source-one","dataset_schema":"source_snapshot","dataset_id":"dataset-one","producer_id":"producer-one","edition_id":"edition-one","source_key":"opaque-source-key","source_scope_json":{"cohort_id":"cohort-one","snapshot_id":"snapshot-one","company_key":"company-one"}});
    assert!(review(
        &serde_json::to_vec(&resolved).unwrap(),
        &serde_json::to_vec(&retained["document"]).unwrap(),
        "45678901-4567-8901-8456-45678901abcd"
    )
    .is_err());
}
#[test]
fn retained_business_context_and_version_cannot_be_relabelled() {
    let bytes = input(&[["E", "Employer", "A", "PPO", "Carrier", "Network"]]);
    let (copy, _) = encode(&bytes, SNAPSHOT).unwrap();
    let original = document(&copy);
    for (pointer, value) in [
        (
            "/ledger/targets/0/business_context/plan_type",
            json!("Dental"),
        ),
        (
            "/ledger/observations/0/raw_cells/4",
            json!("Different carrier"),
        ),
        ("/parser_version", json!("registry-target-ledger-v2")),
        ("/ledger/targets/0/source_row_ordinals", json!([1, 1])),
    ] {
        let mut altered = original.clone();
        *altered.pointer_mut(pointer).unwrap() = value;
        let retained = json!({"snapshot_id":SNAPSHOT,"artifact_sha256":sha(&serde_json::to_vec(&altered).unwrap()),"document":altered});
        assert!(
            validate(&serde_json::to_vec(&retained).unwrap()).is_err(),
            "{pointer}"
        );
    }
}
#[test]
fn business_headers_are_six_columns_and_tuple_encoding_is_unambiguous() {
    assert!(parse(b"COMPANY_ALIAS,COMPANY_NAME,PLAN_NAME,PLAN_TYPE,PLAN_CARRIER,FIND_CARE_NETWORK_NAME,EXTRA\nE,E,P,PPO,C,N,opaque\n").is_err());
    let ledger = parse(&input(&[
        ["E", "E", "P", "x", "a|b", "c"],
        ["E", "E", "P", "x", "a", "b|c"],
    ]))
    .unwrap();
    assert_eq!(ledger.targets.len(), 2);
}

#[test]
fn resolved_business_reviews_bind_exact_integer_network_and_complete_artifacts() {
    use ptg2_scanner::registry_required_target_review::validate_registry_required_target_review_artifacts;
    let bytes = input(&[
        ["E", "Employer", "A", "PPO", "Carrier A", "Network"],
        ["E", "Employer", "B", "Dental", "Carrier B", "Network"],
    ]);
    let (copy, _) = encode(&bytes, SNAPSHOT).unwrap();
    let ledger = document(&copy);
    let binding = json!({"binding_id":"01234567-89ab-cdef-8123-456789abcdef","source_system":"ptg","source_id":"source-one","dataset_schema":"source_snapshot","dataset_id":"dataset-one","producer_id":"producer-one","edition_id":"edition-one","source_key":"opaque-source-key","source_scope_json":{"cohort_id":"cohort-one","snapshot_id":"snapshot-one","company_key":"company-one"}});
    let decisions=ledger["ledger"]["targets"].as_array().unwrap().iter().map(|target|json!({"target_key":target["target_key"],"resolution_status":"resolved","network_id":42,"source_binding":binding,"evidence_reference":"Explicit review","evidence_sha256":"a".repeat(64),"reason":"Reviewed source coordinates"})).collect::<Vec<_>>();
    let request = json!({"ledger_snapshot_id":SNAPSHOT,"ledger_artifact_sha256":sha(&serde_json::to_vec(&ledger).unwrap()),"decisions":decisions});
    let review_id = "45678901-4567-8901-8456-45678901abcd";
    let (review_copy, descriptor) = review(
        &serde_json::to_vec(&request).unwrap(),
        &serde_json::to_vec(&ledger).unwrap(),
        review_id,
    )
    .unwrap();
    let descriptor: Value = serde_json::from_slice(&descriptor).unwrap();
    assert_eq!(descriptor["resolved_count"], 2);
    let retained = json!({"reviews":[{"snapshot_id":review_id,"artifact_sha256":descriptor["artifact_sha256"],"document":document(&review_copy)}],"ledgers":[{"snapshot_id":SNAPSHOT,"artifact_sha256":request["ledger_artifact_sha256"],"document":ledger}]});
    assert!(validate_registry_required_target_review_artifacts(
        &serde_json::to_vec(&retained).unwrap()
    )
    .is_ok());
    let mut drift = retained;
    drift["ledgers"][0]["snapshot_id"] = json!("56789012-5678-9012-8567-56789012abcd");
    assert!(validate_registry_required_target_review_artifacts(
        &serde_json::to_vec(&drift).unwrap()
    )
    .is_err());
}

#[test]
fn business_input_refuses_malformed_headers_encoding_and_empty_ledgers() {
    for (bytes, code) in [
        (vec![b' '; ptg2_scanner::registry_target_ledger::MAX_INPUT_BYTES + 1], "input_limit"),
        (vec![0xff], "invalid_utf8"),
        ([b"\xef\xbb\xbf".as_slice(), input(&[["E", "Company", "Plan", "PPO", "Carrier", "Network"]]).as_slice()].concat(), "invalid_headers"),
        (input(&[]), "empty_ledger"),
        (b"COMPANY_NAME,COMPANY_ALIAS,PLAN_NAME,PLAN_TYPE,PLAN_CARRIER,FIND_CARE_NETWORK_NAME\nE,C,P,PPO,C,N\n".to_vec(), "invalid_headers"),
    ] {
        let error = parse(&bytes).unwrap_err();
        assert_eq!(error.code, code);
        assert_eq!(error.source_row_ordinal, None);
        assert_eq!(encode(&bytes, SNAPSHOT).unwrap_err(), error);
    }
}

#[test]
fn business_late_invalid_row_preserves_byte_limits_and_atomic_refusal() {
    use ptg2_scanner::registry_target_ledger::MAX_FIELD_BYTES;
    let exact = "é".repeat(MAX_FIELD_BYTES / 2);
    for column in 0..6 {
        let mut row = ["E", "Company", "Plan", "PPO", "Carrier", "Network"];
        row[column] = &exact;
        assert_eq!(
            parse(&input(&[row])).unwrap().observations[0].raw_cells[column],
            exact
        );
        for (value, code) in [
            (format!("{exact}x"), "field_limit"),
            ("before\0after".to_owned(), "invalid_jsonb"),
        ] {
            let mut invalid_row = row;
            invalid_row[column] = &value;
            let bytes = input(&[
                ["E", "Company", "Plan", "PPO", "Carrier", "Network"],
                invalid_row,
            ]);
            let error = parse(&bytes).unwrap_err();
            assert_eq!(error.code, code);
            assert_eq!(error.source_row_ordinal, Some(2));
            assert_eq!(encode(&bytes, SNAPSHOT).unwrap_err(), error);
        }
    }
}

#[test]
fn business_target_limit_counts_distinct_contexts_not_source_rows() {
    use ptg2_scanner::registry_target_ledger::MAX_TARGETS;
    let labels: Vec<_> = (0..=MAX_TARGETS).map(|i| format!("Network {i}")).collect();
    let rows: Vec<_> = labels
        .iter()
        .map(|label| ["E", "Company", "Plan", "PPO", "Carrier", label.as_str()])
        .collect();
    let valid = input(&rows[..MAX_TARGETS]);
    assert_eq!(parse(&valid).unwrap().targets.len(), MAX_TARGETS);
    let invalid = input(&rows);
    let error = parse(&invalid).unwrap_err();
    assert_eq!(error.code, "target_limit");
    assert_eq!(error.source_row_ordinal, Some(MAX_TARGETS + 1));
    assert_eq!(encode(&invalid, SNAPSHOT).unwrap_err(), error);
}

#[test]
fn business_output_limit_rejects_escaped_expansion_before_copy() {
    use ptg2_scanner::registry_target_ledger::{MAX_FIELD_BYTES, MAX_INPUT_BYTES};
    let escaped = "\u{0001}".repeat(MAX_FIELD_BYTES);
    let rows = vec![[escaped.as_str(); 6]; 120];
    assert!(parse(&input(&rows[..100])).is_ok());
    let bytes = input(&rows);
    assert!(bytes.len() < MAX_INPUT_BYTES);
    let error = parse(&bytes).unwrap_err();
    assert_eq!(error.code, "output_limit");
    assert_eq!(error.source_row_ordinal, None);
    assert_eq!(encode(&bytes, SNAPSHOT).unwrap_err(), error);
}

#[test]
fn rehashed_business_ledgers_require_exact_six_cell_provenance() {
    let (copy, _) = encode(
        &input(&[["E", "Company", "Plan", "PPO", "Carrier", "Network"]]),
        SNAPSHOT,
    )
    .unwrap();
    let original = document(&copy);
    for count in [5, 7] {
        let mut altered = original.clone();
        altered["ledger"]["observations"][0]["raw_cells"]
            .as_array_mut()
            .unwrap()
            .resize(count, json!("extra"));
        let retained = json!({"snapshot_id": SNAPSHOT, "artifact_sha256": sha(&serde_json::to_vec(&altered).unwrap()), "document": altered});
        assert_eq!(
            validate(&serde_json::to_vec(&retained).unwrap())
                .unwrap_err()
                .code,
            "registry_target_review_ledger_invalid"
        );
    }
}
