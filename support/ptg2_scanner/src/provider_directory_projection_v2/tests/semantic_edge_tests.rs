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

#[test]
fn optional_profile_shapes_preserve_periods_aliases_contacts_and_observation_time() {
    let row = one_row(json!({
        "resourceType":"Practitioner",
        "id":"profile-optionals",
        "active":true,
        "period":{"start":" 2026-01-01 ","end":"2026-12-31"},
        "meta":{"lastUpdated":" 2026-07-22T10:00:00Z "},
        "name":{
            "use":"official",
            "text":" Dr Jane Example ",
            "family":"Example",
            "given":["Jane"],
            "prefix":["Dr"],
            "suffix":["MD"],
            "period":{"start":"2020-01-01","end":"2029-12-31"}
        },
        "alias":[" J Example "],
        "telecom":[{
            "system":"phone",
            "use":"work",
            "value":"2025550100",
            "rank":2,
            "period":{"start":"2025-01-01","end":"2027-01-01"}
        }],
        "address":[{
            "use":"work",
            "type":"physical",
            "text":"1 Main St, Austin TX",
            "line":["1 Main St","Suite 2"],
            "city":"Austin",
            "district":"Travis",
            "state":"TX",
            "postalCode":"78701",
            "country":"US",
            "period":{"start":"2024-01-01","end":"2028-01-01"}
        }]
    }));
    let evidence = profile(&row);
    assert_eq!(evidence["names"].as_array().unwrap().len(), 2);
    assert_eq!(evidence["names"][0]["text"], "Dr Jane Example");
    assert_eq!(evidence["names"][1]["text"], "J Example");
    assert_eq!(evidence["contacts"][0]["rank"], 2);
    assert_eq!(evidence["contacts"][0]["period"]["end"], "2027-01-01");
    assert_eq!(evidence["addresses"][0]["district"], "Travis");
    assert_eq!(row.text(14), Some("2026-01-01"));
    assert_eq!(row.text(15), Some("2026-12-31"));
    assert_eq!(row.text(16), Some("2026-07-22T10:00:00Z"));
}

#[test]
fn explicit_nulls_and_absent_activity_remain_optional() {
    for resource in [
        json!({
            "resourceType":"Location","id":"null-location","status":"active",
            "name":null,"period":null,"meta":null,"address":null,"position":null
        }),
        json!({
            "resourceType":"Organization","id":"absent-active",
            "name":null,"period":null,"meta":null,"address":null,"identifier":[]
        }),
        json!({
            "resourceType":"HealthcareService","id":"null-specialty",
            "specialty":null,"telecom":[],"address":null,"extension":null
        }),
    ] {
        let row = one_row(resource);
        assert!(row.text(14).is_none());
        assert!(row.text(15).is_none());
        assert!(row.text(16).is_none());
    }
    assert_eq!(
        one_row(json!({"resourceType":"Location","id":"missing-status"})).boolean(13),
        None
    );
    assert_eq!(
        one_row(json!({"resourceType":"Organization","id":"missing-active"})).boolean(13),
        None
    );
}

#[test]
fn exact_geographic_bounds_and_out_of_range_identifier_candidates_are_stable() {
    let bounded = one_row(json!({
        "resourceType":"Location","id":"bounded-position","status":"active",
        "address":{"city":"Boundary"},
        "position":{"latitude":90,"longitude":180}
    }));
    assert_eq!(
        profile(&bounded)["addresses"][0]["geocode_evidence"]["latitude_microdegrees"],
        90_000_000
    );
    assert_eq!(
        profile(&bounded)["addresses"][0]["geocode_evidence"]["longitude_microdegrees"],
        180_000_000
    );

    let invalid_npi = one_row(json!({
        "resourceType":"Practitioner","id":"invalid-npi-candidate","active":true,
        "identifier":[{
            "system":"http://hl7.org/fhir/sid/us-npi","value":"9999999999"
        }]
    }));
    assert_eq!(invalid_npi.i64(7), None);
}

#[test]
fn codeable_concepts_accept_object_form_and_full_coding_shape() {
    let row = one_row(json!({
        "resourceType":"HealthcareService",
        "id":"concept-object",
        "active":true,
        "specialty":{
            "system":"urn:direct",
            "version":"1",
            "code":"207Q00000X",
            "display":"Family Medicine",
            "text":"Primary care",
            "coding":[{
                "system":"urn:nucc",
                "version":"2",
                "code":"207Q00000X",
                "display":"Family Medicine",
                "userSelected":true
            }]
        }
    }));
    let specialty = &profile(&row)["specialties"][0];
    assert_eq!(specialty["code"], "207Q00000X");
    assert_eq!(specialty["coding"][0]["userSelected"], true);
}

#[test]
fn malformed_profile_value_shapes_fail_closed_at_the_exact_semantic_boundary() {
    let invalid_resources = [
        json!({"resourceType":"Practitioner","id":"bad-name-scalar","active":true,"name":7}),
        json!({"resourceType":"Practitioner","id":"bad-name-empty","active":true,"name":{}}),
        json!({"resourceType":"Practitioner","id":"bad-name-parts","active":true,"name":{"family":"Example","given":[7]}}),
        json!({"resourceType":"Practitioner","id":"bad-telecom-entry","active":true,"telecom":["phone"]}),
        json!({"resourceType":"Practitioner","id":"bad-telecom-value","active":true,"telecom":[{"system":"phone"}]}),
        json!({"resourceType":"Practitioner","id":"bad-telecom-rank","active":true,"telecom":[{"value":"2025550100","rank":0}]}),
        json!({"resourceType":"Practitioner","id":"bad-telecom-period","active":true,"telecom":[{"value":"2025550100","period":{}}]}),
        json!({"resourceType":"Organization","id":"bad-address-scalar","active":true,"address":"Austin"}),
        json!({"resourceType":"Organization","id":"bad-address-empty","active":true,"address":{}}),
        json!({"resourceType":"Organization","id":"bad-address-line","active":true,"address":{"city":"Austin","line":[7]}}),
        json!({"resourceType":"Organization","id":"bad-address-period","active":true,"address":{"city":"Austin","period":{}}}),
        json!({"resourceType":"HealthcareService","id":"bad-concept-scalar","active":true,"specialty":"family"}),
        json!({"resourceType":"HealthcareService","id":"bad-concept-empty","active":true,"specialty":{}}),
        json!({"resourceType":"HealthcareService","id":"bad-coding-entry","active":true,"specialty":{"coding":["family"]}}),
        json!({"resourceType":"HealthcareService","id":"bad-coding-empty","active":true,"specialty":{"coding":[{"system":"urn:test"}]}}),
        json!({"resourceType":"HealthcareService","id":"bad-user-selected","active":true,"specialty":{"coding":[{"code":"207Q00000X","userSelected":"yes"}]}}),
        json!({"resourceType":"Location","id":"bad-position-scalar","status":"active","position":"here"}),
        json!({"resourceType":"Location","id":"bad-position-latitude","status":"active","position":{"longitude":1}}),
        json!({"resourceType":"Location","id":"bad-position-longitude","status":"active","position":{"latitude":1,"longitude":"west"}}),
        json!({"resourceType":"Location","id":"bad-position-bound","status":"active","position":{"latitude":90.1,"longitude":1}}),
    ];
    for resource in invalid_resources {
        assert!(
            try_spool_for_resources(std::slice::from_ref(&resource)).is_err(),
            "malformed profile resource was accepted: {resource}"
        );
    }
}

#[test]
fn malformed_identity_activity_metadata_and_npi_shapes_fail_closed() {
    for resource in [
        json!(42),
        json!({"id":"missing-type"}),
        json!({"resourceType":"Patient","id":"unsupported"}),
        json!({"resourceType":"Organization","id":"bad/id","active":true}),
        json!({"resourceType":"Organization","id":"bad-active","active":"yes"}),
        json!({"resourceType":"Location","id":"bad-status","status":"unknown"}),
        json!({"resourceType":"Endpoint","id":"bad-status-type","status":7}),
        json!({"resourceType":"Organization","id":"bad-meta","active":true,"meta":7}),
        json!({"resourceType":"Practitioner","id":"bad-identifier-entry","active":true,"identifier":[7]}),
        json!({"resourceType":"Practitioner","id":"bad-identifier-type","active":true,"identifier":[{"value":"1234567893","type":7}]}),
        json!({"resourceType":"Practitioner","id":"bad-identifier-coding","active":true,"identifier":[{"value":"1234567893","type":{"coding":[7]}}]}),
    ] {
        assert!(
            try_spool_for_resources(std::slice::from_ref(&resource)).is_err(),
            "malformed semantic resource was accepted: {resource}"
        );
    }

    let no_value = one_row(json!({
        "resourceType":"Practitioner","id":"identifier-no-value","active":true,
        "identifier":[{"system":"http://hl7.org/fhir/sid/us-npi"}]
    }));
    assert_eq!(no_value.i64(7), None);
    let null_type = one_row(json!({
        "resourceType":"Practitioner","id":"identifier-null-type","active":true,
        "identifier":[{"system":"urn:vendor","value":"1234567893","type":null}]
    }));
    assert_eq!(null_type.i64(7), None);
    let coding_descriptor = one_row(json!({
        "resourceType":"Practitioner","id":"identifier-coding","active":true,
        "identifier":[{
            "system":"urn:vendor","value":"1234567893",
            "type":{"text":null,"coding":[{"system":"urn:test","code":7,"display":"National Provider NPI"}]}
        }]
    }));
    assert_eq!(coding_descriptor.i64(7), Some(1_234_567_893));
}

#[test]
fn reference_identifiers_type_ids_and_nested_extensions_are_normalized() {
    let row = one_row(json!({
        "resourceType":"InsurancePlan",
        "id":"reference-shapes",
        "status":"active",
        "ownedBy":{"identifier":{"system":"urn:org","value":"owner-1"},"display":"Owner"},
        "administeredBy":{"type":"Organization","id":"admin-1"},
        "network":[{"reference":"Organization/network-1","identifier":null}],
        "extension":{
            "url":"urn:container",
            "extension":[{
                "url":"https://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/plannet-participatingNetwork-extension/|2.0",
                "valueReference":{"reference":"https://directory.example/fhir/Organization/network-2"}
            }]
        }
    }));
    let references = profile(&row)["references"].as_array().unwrap().clone();
    assert!(references
        .iter()
        .any(|value| value["identifier"]["value"] == "owner-1"));
    assert!(references.iter().any(|value| value["id"] == "admin-1"));
    assert_eq!(row.i32(11), Some(2));
}

#[test]
fn malformed_reference_and_extension_shapes_fail_closed() {
    for resource in [
        json!({"resourceType":"Organization","id":"bad-reference-scalar","active":true,"partOf":"org"}),
        json!({"resourceType":"PractitionerRole","id":"bad-reference-entry","active":true,"location":[7]}),
        json!({"resourceType":"Organization","id":"bad-reference-empty","active":true,"partOf":{}}),
        json!({"resourceType":"Organization","id":"bad-reference-identifier","active":true,"partOf":{"identifier":"org"}}),
        json!({"resourceType":"Organization","id":"bad-reference-identifier-value","active":true,"partOf":{"identifier":{"system":"urn:org"}}}),
        json!({"resourceType":"InsurancePlan","id":"bad-extension-entry","status":"active","extension":[7]}),
        json!({"resourceType":"InsurancePlan","id":"bad-extension-nested","status":"active","extension":{"url":"urn:outer","extension":7}}),
        json!({"resourceType":"InsurancePlan","id":"bad-extension-value","status":"active","extension":[{"url":"http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/network-reference"}]}),
        json!({"resourceType":"InsurancePlan","id":"bad-extension-reference","status":"active","extension":[{"url":"http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/network-reference","valueReference":7}]}),
        json!({"resourceType":"InsurancePlan","id":"bad-plan-cardinality","status":"active","plan":{}}),
        json!({"resourceType":"InsurancePlan","id":"bad-plan-entry","status":"active","plan":[7]}),
        json!({"resourceType":"InsurancePlan","id":"wrong-network-target","status":"active","network":{"reference":"Practitioner/not-a-network"}}),
    ] {
        assert!(
            try_spool_for_resources(std::slice::from_ref(&resource)).is_err(),
            "malformed reference resource was accepted: {resource}"
        );
    }
}
