use ptg2_scanner::network_source_binding_values::encode_network_source_binding_batch;
use serde_json::{json, Value};

fn product(network: i32, identity: &str) -> Value {
    json!({
        "binding_id": identity, "source_system": "fhir", "source_id": "synthetic-source",
        "dataset_schema": "retained_source", "dataset_id": "dataset-one",
        "producer_id": "registry-retained-normalized-fhir-v1",
        "edition_id": "11111111-1111-4111-8111-111111111111", "source_key": "InsurancePlan/plan-one",
        "source_scope_json": {"insurance_plan_id": "plan-one", "capture_sha256": "a".repeat(64),
                              "product_network_id": network},
        "network_id": network, "evidence_id": "synthetic-product-review", "evidence_sha256": "b".repeat(64),
        "operation": "bind", "expected_revision": 0, "expected_network_id": null
    })
}

fn reject(rows: &[Value], code: &str) {
    let error =
        encode_network_source_binding_batch(&serde_json::to_vec(rows).unwrap()).unwrap_err();
    assert_eq!(error.code, code);
}

#[test]
fn products_sharing_one_plan_have_distinct_canonical_keys() {
    let first = product(101, "01234567-89ab-cdef-8123-456789abcde0");
    let second = product(102, "01234567-89ab-cdef-8123-456789abcde1");
    let encoded =
        encode_network_source_binding_batch(&serde_json::to_vec(&[first.clone(), second]).unwrap())
            .unwrap();
    assert_eq!(encoded.row_count, 2);
    assert_ne!(encoded.rows[0].binding_key, encoded.rows[1].binding_key);
    let mut reordered = first;
    reordered["source_scope_json"] = serde_json::from_str(&format!(
        r#"{{"product_network_id":101,"insurance_plan_id":"plan-one","capture_sha256":"{}"}}"#,
        "a".repeat(64)
    ))
    .unwrap();
    let canonical =
        encode_network_source_binding_batch(&serde_json::to_vec(&[reordered]).unwrap()).unwrap();
    assert_eq!(encoded.rows[0].binding_key, canonical.rows[0].binding_key);
}

#[test]
fn product_scopes_are_closed_retained_plan_selectors() {
    let first = product(101, "01234567-89ab-cdef-8123-456789abcde0");
    for (pointer, value) in [
        ("/producer_id", json!("other-producer")),
        ("/source_id", json!("cms-npd")),
        ("/source_key", json!("plan-one")),
        ("/source_key", json!("Organization/plan-one")),
        ("/source_key", json!("InsurancePlan/other")),
        ("/source_scope_json/insurance_plan_id", json!("plan_one")),
        ("/source_scope_json/insurance_plan_id", json!("plan/one")),
        (
            "/source_scope_json/insurance_plan_id",
            json!("a".repeat(65)),
        ),
        ("/source_scope_json/capture_sha256", json!("A".repeat(64))),
        ("/source_scope_json/product_network_id", json!(102)),
        ("/source_scope_json/product_network_id", json!(0)),
        ("/source_scope_json/product_network_id", json!(true)),
        ("/source_scope_json/product_network_id", json!(101.0)),
        (
            "/source_scope_json/product_network_id",
            json!(2_147_483_648_i64),
        ),
    ] {
        let mut changed = first.clone();
        *changed.pointer_mut(pointer).unwrap() = value;
        reject(&[changed], "invalid_scope");
    }
    let mut mixed = first.clone();
    mixed["source_scope_json"]["organization_id"] = json!("representative-org");
    reject(&[mixed], "invalid_scope");
    let mut rebound = first;
    rebound["operation"] = json!("rebind");
    rebound["expected_revision"] = json!(1);
    rebound["expected_network_id"] = json!(102);
    reject(&[rebound], "invalid_scope");
}

#[test]
fn product_selector_and_ordinary_org_cannot_widen_one_target() {
    let first = product(101, "01234567-89ab-cdef-8123-456789abcde0");
    let mut second = product(101, "01234567-89ab-cdef-8123-456789abcde1");
    second["source_key"] = json!("InsurancePlan/plan-two");
    second["source_scope_json"]["insurance_plan_id"] = json!("plan-two");
    reject(&[first.clone(), second.clone()], "duplicate_product_scope");
    second["source_key"] = json!("organization-one");
    second["source_scope_json"] =
        json!({"organization_id": "organization-one", "capture_sha256": "a".repeat(64)});
    reject(&[first.clone(), second.clone()], "duplicate_product_scope");
    reject(&[second.clone(), first.clone()], "duplicate_product_scope");
    second["network_id"] = json!(102);
    assert!(encode_network_source_binding_batch(
        &serde_json::to_vec(&[first.clone(), second.clone()]).unwrap()
    )
    .is_ok());
    second["network_id"] = json!(101);
    second["operation"] = json!("close");
    second["expected_revision"] = json!(1);
    second["expected_network_id"] = json!(101);
    assert!(
        encode_network_source_binding_batch(&serde_json::to_vec(&[second, first]).unwrap()).is_ok()
    );
}

#[test]
fn product_duplicate_json_keys_remain_invalid() {
    let raw =
        serde_json::to_string(&[product(101, "01234567-89ab-cdef-8123-456789abcde0")]).unwrap();
    let duplicate = raw.replace(
        r#""product_network_id":101"#,
        r#""product_network_id":101,"product_network_id":101"#,
    );
    assert_eq!(
        encode_network_source_binding_batch(duplicate.as_bytes())
            .unwrap_err()
            .code,
        "invalid_scope"
    );
}
