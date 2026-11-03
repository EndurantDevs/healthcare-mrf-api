use ptg2_scanner::network_source_binding_values::encode_network_source_binding_batch as encode;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

fn digest(value: Value) -> String {
    Sha256::digest(serde_json::to_vec(&value).unwrap())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn row(tier: &str, network: i32) -> Value {
    let logical = digest(
        json!({"contract":"healthporta-uhc-logical-scope-v1","entry_id":"uhc-provider-files",
        "family":"ifp","market":"individual-exchange","product":"IEX","jurisdiction":"AZ"}),
    );
    let key = json!({"logical_scope_id":logical,"family":"ifp","market":"individual-exchange",
        "plan_id_type":"hios","plan_id":"12345AZ001","plan_year":2026});
    let mut plan = key.clone();
    plan["contract"] = json!("healthporta-uhc-plan-key-v1");
    let plan_id = digest(plan);
    let mut network_key = key.clone();
    network_key["contract"] = json!("healthporta-uhc-network-key-v1");
    network_key["network_tier"] = json!(tier);
    let network_key = digest(network_key);
    let mut scope = key;
    scope["product"] = json!("IEX");
    scope["jurisdiction"] = json!("AZ");
    scope["plan_key_id"] = json!(plan_id);
    json!({"binding_id":format!("01234567-89ab-cdef-8123-{network:012}"),
        "source_system":"fhir","source_id":"pdfhir_2754e999dd691175821ec26e","dataset_schema":"synthetic_source",
        "dataset_id":"edition-one","producer_id":"registry-source-fhir-witness-v1",
        "edition_id":"01234567-89ab-cdef-8123-456789abcde0","source_key":format!("UHCPlanTier/{network_key}"),
        "source_scope_json":{"kind":"uhc_plan_tier_v1","plan_scope":scope,"network_tier":tier,
            "network_key_id":network_key,"plan_resource_id":format!("uhcplan-{}",&plan_id[..48]),"capture_sha256":"a".repeat(64)},
        "network_id":network,"evidence_id":"synthetic-evidence","evidence_sha256":"b".repeat(64),
        "operation":"bind","expected_revision":0,"expected_network_id":null})
}

#[test]
fn complete_plan_tier_scope_preserves_native_copy_and_changes_identity_by_tier() {
    let first = row("preferred", 42);
    let second = row("standard", 43);
    let result = encode(&serde_json::to_vec(&[first.clone(), second.clone()]).unwrap()).unwrap();
    assert_eq!(result.row_count, 2);
    assert_eq!(result.rows[0].source_scope_json, first["source_scope_json"]);
    assert_eq!(
        result.rows[1].source_scope_json,
        second["source_scope_json"]
    );
    assert_ne!(result.rows[0].binding_key, result.rows[1].binding_key);
    assert_eq!(
        &result.copy_bytes[result.copy_bytes.len() - 2..],
        &[255, 255]
    );
}

#[test]
fn invalid_tier_digest_or_coordinate_refuses_whole_batch() {
    for field in [
        "kind",
        "network_key_id",
        "network_tier",
        "plan_resource_id",
        "capture_sha256",
    ] {
        let mut damaged = row("standard", 43);
        damaged["source_scope_json"][field] = json!("invalid");
        let error =
            encode(&serde_json::to_vec(&[row("preferred", 42), damaged]).unwrap()).unwrap_err();
        assert_eq!(error.code, "invalid_scope");
        assert_eq!(error.row_index, Some(1));
    }
    for field in [
        "family",
        "market",
        "product",
        "plan_id_type",
        "plan_id",
        "plan_key_id",
        "logical_scope_id",
    ] {
        let mut damaged = row("preferred", 42);
        damaged["source_scope_json"]["plan_scope"][field] = json!("invalid");
        assert!(encode(&serde_json::to_vec(&[damaged]).unwrap()).is_err());
    }
    for field in ["source_id", "producer_id", "edition_id", "source_key"] {
        let mut damaged = row("preferred", 42);
        damaged[field] = json!("invalid");
        assert!(encode(&serde_json::to_vec(&[damaged]).unwrap()).is_err());
    }
}

#[test]
fn exact_fields_and_distinct_targets_are_required() {
    let mut damaged = row("preferred", 42);
    damaged["source_scope_json"]["plan_scope"]
        .as_object_mut()
        .unwrap()
        .remove("jurisdiction");
    assert!(encode(&serde_json::to_vec(&[damaged]).unwrap()).is_err());
    let mut damaged = row("preferred", 42);
    damaged["source_scope_json"]["organization_id"] = json!("invented");
    assert!(encode(&serde_json::to_vec(&[damaged]).unwrap()).is_err());
    let mut second = row("standard", 42);
    second["binding_id"] = json!("01234567-89ab-cdef-8123-456789abcde1");
    assert_eq!(
        encode(&serde_json::to_vec(&[row("preferred", 42), second]).unwrap())
            .unwrap_err()
            .code,
        "duplicate_product_scope"
    );
    let mut rebound = row("preferred", 42);
    rebound["operation"] = json!("rebind");
    rebound["expected_revision"] = json!(1);
    rebound["expected_network_id"] = json!(43);
    assert!(encode(&serde_json::to_vec(&[rebound]).unwrap()).is_err());
}
