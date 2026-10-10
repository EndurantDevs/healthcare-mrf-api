use ptg2_scanner::canonical_network_values::{
    validate_network_catalog_batch, CatalogValueError, IdentityStatus, SourceAliasType,
    ValidatedNetworkValue, MAX_ALIASES, MAX_INPUT_BYTES, MAX_LABEL_BYTES, MAX_ROWS,
};
use serde_json::{json, Value};

fn catalog_row() -> Value {
    json!({
        "network_id": 17,
        "display_name": "Example Preferred",
        "aliases": ["Example PPO"],
        "source_aliases": [],
        "directory_available": true,
        "priceable": false
    })
}

fn source_alias(alias_type: &str, alias_value: &str) -> Value {
    json!({
        "source_system": "example-directory",
        "source_id": "source-network-one",
        "alias_type": alias_type,
        "alias_value": alias_value,
        "scope_key": "medical/2026",
        "evidence_id": "source-observation-one"
    })
}

fn validate(observations: &[Value]) -> Result<Vec<ValidatedNetworkValue>, CatalogValueError> {
    validate_network_catalog_batch(&serde_json::to_vec(observations).unwrap())
}

#[test]
fn same_labels_never_merge_distinct_ids_or_repeated_source_observations() {
    let first = catalog_row();
    let mut second = first.clone();
    second["network_id"] = json!(18);
    let mut repeated = first.clone();
    repeated["source_aliases"] = json!([source_alias("ptg_label", "Example Preferred")]);
    repeated["source_aliases"][0]["scope_key"] = json!("medical/2027");
    let output = validate(&[first, second, repeated]).unwrap();
    assert_eq!(output.len(), 3);
    assert_eq!(
        output
            .iter()
            .map(|entry| entry.network_id)
            .collect::<Vec<_>>(),
        vec![Some(17), Some(18), Some(17)]
    );
    assert!(output
        .iter()
        .all(|entry| entry.display_name == "Example Preferred"));
    assert!(output
        .iter()
        .all(|entry| entry.status == IdentityStatus::Resolved));
    assert_eq!(output[2].source_aliases[0].scope_key, "medical/2027");
}

#[test]
fn typed_legacy_and_checksum_aliases_preserve_raw_values_without_assigning_ids() {
    let uuid_spelling = "01234567-89AB-CDEF-8123-456789ABCDEF";
    let mut observation = catalog_row();
    observation["network_id"] = json!(42);
    observation["source_aliases"] = json!([
        source_alias("legacy_fhir_uuid", uuid_spelling),
        source_alias("aca_checksum", "00042"),
        source_alias("aca_checksum", "-2147483648"),
        source_alias("aca_checksum", "0")
    ]);
    let resolved = validate(&[observation.clone()]).unwrap();
    assert_eq!(resolved[0].network_id, Some(42));
    let aliases = &resolved[0].source_aliases;
    assert_eq!(aliases[0].alias_type, SourceAliasType::LegacyFhirUuid);
    assert_eq!(aliases[0].alias_value, uuid_spelling);
    assert_eq!(
        aliases[0].canonical_alias_value,
        uuid_spelling.to_ascii_lowercase()
    );
    assert_eq!(aliases[1].alias_type, SourceAliasType::AcaChecksum);
    assert_eq!(aliases[1].alias_value, "00042");
    assert_eq!(aliases[1].canonical_alias_value, "42");
    assert_eq!(aliases[2].canonical_alias_value, "-2147483648");
    assert_eq!(aliases[3].canonical_alias_value, "0");
    observation["network_id"] = Value::Null;
    let unresolved = validate(&[observation]).unwrap();
    assert_eq!(unresolved[0].network_id, None);
    assert_eq!(unresolved[0].status, IdentityStatus::Unresolved);
    assert_eq!(unresolved[0].source_aliases, resolved[0].source_aliases);
}

#[test]
fn canonical_ids_require_positive_int4_and_never_accept_external_identifiers() {
    let mut observation = catalog_row();
    observation["network_id"] = json!(i32::MAX);
    assert_eq!(
        validate(&[observation.clone()]).unwrap()[0].network_id,
        Some(i32::MAX)
    );
    for invalid_id in [
        json!(0),
        json!(-1),
        json!(2_147_483_648u64),
        json!(17.0),
        json!("17"),
        json!(true),
        json!("01234567-89ab-cdef-8123-456789abcdef"),
        json!({"network_id": 17}),
    ] {
        observation["network_id"] = invalid_id;
        let error = validate(&[catalog_row(), observation.clone()]).unwrap_err();
        assert_eq!(error.code, "invalid_network_id");
        assert_eq!(error.row_index, Some(1));
        assert_eq!(error.field, Some("network_id"));
    }
}

#[test]
fn availability_flags_remain_independent_for_resolved_and_unresolved_records() {
    for network_id in [json!(17), Value::Null] {
        for (directory_available, priceable) in
            [(true, false), (false, true), (false, false), (true, true)]
        {
            let mut observation = catalog_row();
            observation["network_id"] = network_id.clone();
            observation["directory_available"] = json!(directory_available);
            observation["priceable"] = json!(priceable);
            let output = validate(&[observation]).unwrap();
            assert_eq!(output[0].directory_available, directory_available);
            assert_eq!(output[0].priceable, priceable);
        }
    }
}

#[test]
fn labels_trim_for_display_while_source_aliases_and_original_spelling_survive() {
    let mut observation = catalog_row();
    observation["display_name"] = json!(" \tExample Réséau\u{2003}");
    observation["aliases"] = json!([" Example PPO ", "\t示例網絡\n", " Example PPO "]);
    observation["source_aliases"] = json!([
        source_alias("ptg_label", " Example Réséau "),
        source_alias("ribbon_network_id", "000042/external")
    ]);
    let output = validate(&[observation]).unwrap();
    let entry = &output[0];
    assert_eq!(entry.display_name, "Example Réséau");
    assert_eq!(entry.raw_display_name, " \tExample Réséau\u{2003}");
    assert_eq!(
        entry.aliases,
        vec!["Example PPO", "示例網絡", "Example PPO"]
    );
    assert_eq!(
        entry.raw_aliases,
        vec![" Example PPO ", "\t示例網絡\n", " Example PPO "]
    );
    assert_eq!(entry.source_aliases[0].alias_value, " Example Réséau ");
    assert_eq!(
        entry.source_aliases[0].canonical_alias_value,
        " Example Réséau "
    );
    assert_eq!(
        entry.source_aliases[1].canonical_alias_value,
        "000042/external"
    );
    assert_eq!(entry.source_aliases[0].source_id, "source-network-one");
    assert_eq!(
        entry.source_aliases[0].evidence_id,
        "source-observation-one"
    );
}

#[test]
fn bytes_rows_aliases_and_utf8_fields_have_inclusive_limits() {
    let mut observation = catalog_row();
    observation["display_name"] = json!("é".repeat(MAX_LABEL_BYTES / 2));
    observation["aliases"] = json!(vec!["x"; MAX_ALIASES]);
    observation["source_aliases"] = json!(vec![source_alias("ptg_label", "Example"); MAX_ALIASES]);
    assert!(validate(&[observation.clone()]).is_ok());
    observation["display_name"] = json!("é".repeat(MAX_LABEL_BYTES / 2 + 1));
    assert_eq!(
        validate(&[observation.clone()]).unwrap_err().field,
        Some("display_name")
    );
    observation["display_name"] = json!("Example");
    observation["aliases"] = json!(vec!["x"; MAX_ALIASES + 1]);
    assert_eq!(
        validate(&[observation.clone()]).unwrap_err().field,
        Some("aliases")
    );
    observation["aliases"] = json!([]);
    observation["source_aliases"] =
        json!(vec![source_alias("ptg_label", "Example"); MAX_ALIASES + 1]);
    assert_eq!(
        validate(&[observation]).unwrap_err().field,
        Some("source_aliases")
    );
    assert_eq!(
        validate(&vec![catalog_row(); MAX_ROWS]).unwrap().len(),
        MAX_ROWS
    );
    let row_error = validate(&vec![catalog_row(); MAX_ROWS + 1]).unwrap_err();
    assert_eq!(row_error.code, "row_limit");
    assert_eq!(row_error.row_index, Some(MAX_ROWS));
    let mut exact_limit = b"[]".to_vec();
    exact_limit.resize(MAX_INPUT_BYTES, b' ');
    assert!(validate_network_catalog_batch(&exact_limit)
        .unwrap()
        .is_empty());
    exact_limit.push(b' ');
    assert_eq!(
        validate_network_catalog_batch(&exact_limit)
            .unwrap_err()
            .code,
        "input_limit"
    );
}

#[test]
fn source_alias_types_identifiers_scopes_and_evidence_are_validated() {
    for (alias_type, invalid_spellings) in [
        (
            "legacy_fhir_uuid",
            vec![
                "00000000-0000-0000-0000-000000000000",
                "0123456789abcdef8123456789abcdef",
                "{01234567-89ab-cdef-8123-456789abcdef}",
                "01234567-89ab-cdef-8123-456789abcdeg",
            ],
        ),
        (
            "aca_checksum",
            vec![
                "2147483648",
                "-2147483649",
                "+42",
                " 42",
                "42.0",
                "--42",
                "-",
            ],
        ),
    ] {
        for spelling in invalid_spellings {
            let mut observation = catalog_row();
            observation["source_aliases"] = json!([source_alias(alias_type, spelling)]);
            let error = validate(&[observation]).unwrap_err();
            assert_eq!(error.field, Some("source_aliases.alias_value"));
        }
    }
    for (field, maximum) in [
        ("source_system", 64),
        ("source_id", 128),
        ("alias_value", 512),
        ("scope_key", 512),
        ("evidence_id", 512),
    ] {
        let mut alias = source_alias("ptg_label", "Example");
        alias[field] = json!("x".repeat(maximum));
        let mut observation = catalog_row();
        observation["source_aliases"] = json!([alias.clone()]);
        assert!(validate(&[observation.clone()]).is_ok());
        alias[field] = json!("x".repeat(maximum + 1));
        observation["source_aliases"] = json!([alias.clone()]);
        assert_eq!(
            validate(&[observation.clone()]).unwrap_err().code,
            "field_limit"
        );
        alias[field] = json!(" \t");
        observation["source_aliases"] = json!([alias]);
        assert_eq!(validate(&[observation]).unwrap_err().code, "blank_field");
    }
}

#[test]
fn malformed_duplicate_missing_unknown_fields_fail_without_payload_disclosure() {
    for field in [
        "network_id",
        "display_name",
        "aliases",
        "source_aliases",
        "directory_available",
        "priceable",
    ] {
        let mut observation = catalog_row();
        observation.as_object_mut().unwrap().remove(field);
        assert_eq!(validate(&[observation]).unwrap_err().code, "invalid_row");
    }
    let mut unknown = catalog_row();
    unknown["unexpected"] = json!("private-source-value");
    let error = validate(&[unknown]).unwrap_err();
    assert_eq!(
        serde_json::to_value(&error).unwrap(),
        json!({"code":"invalid_row","row_index":0,"field":null})
    );
    assert!(!error.to_string().contains("private-source-value"));
    let mut nested_unknown = catalog_row();
    nested_unknown["source_aliases"] = json!([source_alias("ptg_label", "Example")]);
    nested_unknown["source_aliases"][0]["unexpected"] = json!(true);
    assert_eq!(validate(&[nested_unknown]).unwrap_err().code, "invalid_row");
    let serialized = serde_json::to_string(&catalog_row()).unwrap();
    let duplicate = format!("[{}]", serialized.replacen("{", "{\"network_id\":19,", 1));
    assert_eq!(
        validate_network_catalog_batch(duplicate.as_bytes())
            .unwrap_err()
            .code,
        "invalid_row"
    );
    for invalid in [
        b"{}".as_slice(),
        b"null",
        b"[",
        b"[] true",
        b"[\"private-source-value\"]",
        b"[\xff]",
    ] {
        assert!(validate_network_catalog_batch(invalid).is_err());
    }
    for blank in ["", "\t \n", "\u{2003}", "network\0label"] {
        let mut observation = catalog_row();
        observation["display_name"] = json!(blank);
        assert!(validate(&[catalog_row(), observation]).is_err());
    }
}
