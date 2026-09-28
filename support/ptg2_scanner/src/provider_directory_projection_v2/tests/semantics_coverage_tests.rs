// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

use super::*;
use serde_json::json;

fn object(value: Value) -> Map<String, Value> {
    value.as_object().expect("JSON object").clone()
}

fn expanded(addresses: Vec<Value>, references: Vec<Value>) -> Value {
    json!({
        "names": [],
        "specialties": [],
        "contacts": [],
        "addresses": addresses,
        "references": references
    })
}

#[test]
fn explicit_npi_policy_rejects_resource_id_fallback_and_invalid_identifiers() {
    let without_identifier = object(json!({}));
    assert_eq!(
        summary_npi(
            &without_identifier,
            "Organization",
            "1003000126",
            NpiIdentityPolicy::Legacy
        )
        .unwrap(),
        Some(1_003_000_126),
    );
    assert_eq!(
        summary_npi(
            &without_identifier,
            "Organization",
            "1003000126",
            NpiIdentityPolicy::ExplicitOnly
        )
        .unwrap(),
        None,
    );
    let identifiers = object(json!({"identifier": [
        {"system": "http://hl7.org/fhir/sid/us-npi", "value": "1234567890"},
        {"system": "http://hl7.org/fhir/sid/us-npi", "value": "1234567893"}
    ]}));
    assert_eq!(
        summary_npi(
            &identifiers,
            "Organization",
            "1003000126",
            NpiIdentityPolicy::ExplicitOnly
        )
        .unwrap(),
        Some(1_234_567_893),
    );
    let type_text = object(json!({"identifier": [{
        "type": {"text": "National Provider Identifier"},
        "value": "1234567893"
    }]}));
    assert_eq!(
        summary_npi(
            &type_text,
            "Organization",
            "1003000126",
            NpiIdentityPolicy::ExplicitOnly
        )
        .unwrap(),
        Some(1_234_567_893),
    );
    let legacy_suffix = object(json!({"identifier": [{
        "system": "http://hl7.org/fhir/sid/us-npi",
        "value": "1234567893\u{2163}"
    }]}));
    assert_eq!(
        summary_npi(
            &legacy_suffix,
            "Organization",
            "1003000126",
            NpiIdentityPolicy::Legacy
        )
        .unwrap(),
        Some(1_234_567_893),
    );
}

#[test]
fn explicit_npi_policy_rejects_unicode_numeric_suffix_for_all_npi_resources() {
    for resource_type in [
        "HealthcareService",
        "Organization",
        "Practitioner",
        "PractitionerRole",
    ] {
        for (value, expected_npi) in [
            ("123-456-7893", Some(1_234_567_893)),
            ("1234567893\u{0661}", None),
            ("1234567893\u{00b2}", None),
            ("1234567893\u{2163}", None),
        ] {
            let resource = object(json!({"identifier": [{
                "system": "http://hl7.org/fhir/sid/us-npi",
                "value": value
            }]}));
            assert_eq!(
                summary_npi(
                    &resource,
                    resource_type,
                    "1003000126",
                    NpiIdentityPolicy::ExplicitOnly
                )
                .unwrap(),
                expected_npi,
                "{resource_type} with {value:?}",
            );
        }
    }
}

#[test]
fn internal_semantic_validation_boundaries_fail_closed() {
    let context = ProjectionCopyContext {
        recipe_id: "a".repeat(64),
        partition_id: "b".repeat(64),
        partition_ordinal: 1,
        npi_identity_policy: NpiIdentityPolicy::Legacy,
    };
    assert!(project_resource(
        0,
        json!({"resourceType": "Organization", "id": "org", "meta": []}),
        &context
    )
    .is_err());
    assert!(project_resource(
        0,
        json!({
            "resourceType": "Organization",
            "id": "org",
            "meta": {"lastUpdated": null}
        }),
        &context
    )
    .is_err());

    assert_eq!(
        profile_addresses(
            &object(json!({"address": [{"city": "One"}, {"city": "Two"}]})),
            "Organization"
        )
        .unwrap()
        .len(),
        2
    );
    assert!(profile_addresses(&object(json!({"address": false})), "Organization").is_err());
    assert!(profile_addresses(
        &object(json!({"address": {"city": "Town"}, "position": {"latitude": 1}})),
        "Location"
    )
    .is_err());

    assert!(practitioner_taxonomies(&object(json!({"qualification": [false]}))).is_err());
    assert_eq!(
        practitioner_taxonomies(&object(json!({
            "qualification": [{
                "code": {
                    "text": "Taxonomy",
                    "coding": [
                        {"system": "other", "code": "short"},
                        {"system": "http://nucc.org", "code": "207Q00000X"}
                    ]
                }
            }]
        })))
        .unwrap(),
        vec![json!({
            "text": "Taxonomy",
            "coding": [{"system": "http://nucc.org", "code": "207Q00000X"}]
        })]
    );
    assert!(!is_taxonomy_coding(&Value::Null));
    assert!(is_taxonomy_coding(&json!({"code": "207Q00000X"})));

    let geocode = |value: Value| json!({"geocode_evidence": value});
    assert!(typed_evidence(
        "Organization",
        &expanded(
            vec![geocode(json!({
                "latitude_microdegrees": 1,
                "longitude_microdegrees": 2
            }))],
            vec![]
        )
    )
    .is_err());
    assert!(typed_evidence("Location", &expanded(vec![geocode(json!(false))], vec![])).is_err());
    assert!(typed_evidence(
        "Location",
        &expanded(vec![geocode(json!({"longitude_microdegrees": 2}))], vec![])
    )
    .is_err());
    assert!(typed_evidence(
        "Location",
        &expanded(vec![geocode(json!({"latitude_microdegrees": 1}))], vec![])
    )
    .is_err());

    let relationship = |value: Value| json!({"relationship_evidence": value});
    for invalid in [
        relationship(json!(false)),
        relationship(json!({
            "target_resource_type": "Organization",
            "target_resource_id": "org"
        })),
        relationship(json!({"kind": "network", "target_resource_id": "org"})),
        relationship(json!({"kind": "network", "target_resource_type": "Organization"})),
    ] {
        assert!(typed_evidence("InsurancePlan", &expanded(vec![], vec![invalid])).is_err());
    }
    let valid_relationship = relationship(json!({
        "kind": "network",
        "target_resource_type": "Organization",
        "target_resource_id": "org"
    }));
    assert!(typed_evidence(
        "InsurancePlan",
        &expanded(vec![], vec![valid_relationship.clone(), valid_relationship])
    )
    .is_err());

    assert_eq!(
        summary_npi(
            &object(json!({"identifier": [{"system": "npi"}]})),
            "Practitioner",
            "not-an-npi",
            NpiIdentityPolicy::Legacy,
        )
        .unwrap(),
        None
    );
    assert!(summary_npi(
        &object(json!({"identifier": [false]})),
        "Practitioner",
        "not-an-npi",
        NpiIdentityPolicy::Legacy,
    )
    .is_err());
    assert_eq!(
        identifier_type_text(&object(json!({"type": null}))).unwrap(),
        ""
    );
    assert!(identifier_type_text(&object(json!({"type": false}))).is_err());
    assert!(identifier_type_text(&object(json!({
        "type": {"coding": [false]}
    })))
    .is_err());
}
