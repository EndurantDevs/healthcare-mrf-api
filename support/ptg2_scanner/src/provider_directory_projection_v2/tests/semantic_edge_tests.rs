// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

use super::support::{decode_copy_rows, spool_for_resources, try_spool_for_resources};
use serde_json::{json, Value};

fn one_row(resource: Value) -> super::support::DecodedCopyRow {
    decode_copy_rows(&spool_for_resources(&[resource]).copy_bytes).remove(0)
}

fn profile(row: &super::support::DecodedCopyRow) -> serde_json::Map<String, Value> {
    row.jsonb(17)
        .unwrap_or_else(|| json!({}))
        .as_object()
        .unwrap()
        .clone()
}

#[test]
fn specialties_only_come_from_the_python_supported_paths() {
    for resource in [
        json!({"resourceType":"Endpoint","id":"e","status":"active","payloadType":[{"coding":[{"code":"207Q00000X"}]}]}),
        json!({"resourceType":"InsurancePlan","id":"p","status":"active","type":[{"coding":[{"code":"207Q00000X"}]}]}),
        json!({"resourceType":"Organization","id":"o","active":true,"type":[{"coding":[{"code":"207Q00000X"}]}]}),
        json!({"resourceType":"Location","id":"l","status":"active","type":[{"coding":[{"code":"207Q00000X"}]}]}),
        json!({"resourceType":"HealthcareService","id":"h","active":true,"category":[{"coding":[{"code":"207Q00000X"}]}]}),
        json!({"resourceType":"Practitioner","id":"r","active":true,"qualification":[{"code":{"coding":[{"system":"urn:license","code":"MD"}]}}]}),
    ] {
        assert!(!profile(&one_row(resource)).contains_key("specialties"));
    }

    for (resource_type, field) in [
        ("HealthcareService", "specialty"),
        ("OrganizationAffiliation", "specialty"),
        ("PractitionerRole", "specialty"),
    ] {
        let resource = json!({
            "resourceType": resource_type,
            "id": format!("{resource_type}-specialty"),
            "active": true,
            field: [{"coding":[{"system":"urn:test","code":"207Q00000X"}]}],
        });
        assert_eq!(
            profile(&one_row(resource))["specialties"][0]["coding"][0]["code"],
            "207Q00000X"
        );
    }
}

#[test]
fn npi_priority_fallback_and_range_match_the_python_oracle() {
    let priority = one_row(json!({
        "resourceType":"Practitioner","id":"p","active":true,
        "identifier":[
            {"system":"urn:vendor:npi","value":"1999999999"},
            {"system":"http://hl7.org/fhir/sid/us-npi","value":"1234567893"}
        ]
    }));
    assert_eq!(priority.i64(7), Some(1_234_567_893));

    let fuzzy = one_row(json!({
        "resourceType":"HealthcareService","id":"h","active":true,
        "identifier":[{"type":{"text":"National Provider NPI"},"value":"1-987-654-321"}]
    }));
    assert_eq!(fuzzy.i64(7), Some(1_987_654_321));

    for (resource_type, resource_id, expected) in [
        ("Practitioner", "1000000000", Some(1_000_000_000)),
        ("Organization", "2999999999", Some(2_999_999_999)),
        ("HealthcareService", "1234567893", None),
        ("PractitionerRole", "1234567893", None),
        ("Practitioner", "0123456789", None),
        ("Organization", "9999999999", None),
    ] {
        let row = one_row(json!({
            "resourceType":resource_type,"id":resource_id,"active":true
        }));
        assert_eq!(row.i64(7), expected);
    }
}

#[test]
fn reference_forms_extensions_and_backbones_are_normalized_and_deduplicated() {
    let forms = [
        "network-bare",
        "Organization/network-relative",
        "https://directory.example/fhir/Organization/network-absolute",
        "Organization/network-versioned/_history/7",
        "Organization/network-query?active=true",
        "Organization/network-fragment#contained",
    ];
    let form_row = one_row(json!({
        "resourceType":"InsurancePlan","id":"forms","status":"active",
        "network": forms.map(|reference| json!({"reference":reference}))
    }));
    assert_eq!(form_row.i32(11), Some(6));

    let extension_row = one_row(json!({
        "resourceType":"InsurancePlan","id":"extensions","status":"active",
        "network":[{"reference":"Organization/network-one"}],
        "plan":[{"network":[{"reference":"Organization/network-two"}]}],
        "coverage":[{"network":[{"reference":"Organization/network-one"}]}],
        "extension":[
            {"url":"http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/network-reference","valueReference":{"reference":"Organization/network-one"}},
            {"url":"https://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/network-reference|1.2","valueReference":{"reference":"Organization/network-three"}}
        ]
    }));
    assert_eq!(extension_row.i32(11), Some(3));
}

#[test]
fn status_enums_and_boolean_activity_are_exact() {
    for (resource_type, status, expected) in [
        ("Location", "active", true),
        ("Location", "suspended", false),
        ("Location", "inactive", false),
        ("InsurancePlan", "active", true),
        ("InsurancePlan", "draft", false),
        ("InsurancePlan", "retired", false),
        ("InsurancePlan", "unknown", false),
        ("Endpoint", "active", true),
        ("Endpoint", "entered-in-error", false),
        ("Endpoint", "test", false),
    ] {
        let row = one_row(json!({
            "resourceType":resource_type,
            "id":format!("{resource_type}-{status}"),
            "status":status
        }));
        assert_eq!(row.boolean(13), Some(expected));
    }
    for resource_type in [
        "HealthcareService",
        "Organization",
        "OrganizationAffiliation",
        "Practitioner",
        "PractitionerRole",
    ] {
        for active in [true, false] {
            let row = one_row(json!({
                "resourceType":resource_type,"id":format!("{resource_type}-{active}"),"active":active
            }));
            assert_eq!(row.boolean(13), Some(active));
        }
    }
}

#[test]
fn malformed_present_cardinality_and_structures_fail_closed() {
    for invalid_fields in [
        json!({"telecom":{"value":"2025550100"}}),
        json!({"identifier":{"system":"urn:npi","value":"1234567893"}}),
        json!({"alias":"scalar-alias"}),
        json!({"qualification":{"code":{"text":"MD"}}}),
        json!({"period":"2026"}),
        json!({"meta":["not","an","object"]}),
    ] {
        let mut resource = json!({
            "resourceType":"Practitioner","id":"malformed","active":true
        });
        resource
            .as_object_mut()
            .unwrap()
            .extend(invalid_fields.as_object().unwrap().clone());
        assert!(try_spool_for_resources(&[resource]).is_err());
    }
}

#[test]
fn competing_versions_keep_deterministic_source_rank() {
    let resources = [
        json!({"resourceType":"Organization","id":"same","active":true,"name":"A"}),
        json!({"resourceType":"Organization","id":"same","active":true,"name":"B"}),
    ];
    let rows = decode_copy_rows(&spool_for_resources(&resources).copy_bytes);
    assert_eq!(rows.len(), 2);
    assert_ne!(rows[0].text(6), rows[1].text(6));
    assert!(rows[0].text(6) < rows[1].text(6));
}
