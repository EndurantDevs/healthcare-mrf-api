// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

//! Network roles, the explicit-NPI flag and the direct references kept for relationships.

use super::resources::{single_row, strings, NPI, TS, U1, U2, U3};
use super::{boolean, jsonb, text, texts, u, uuid, uuids};
use serde_json::{json, Value};

fn organization_types(types: Value) -> Vec<Option<Vec<String>>> {
    let line = json!({"resourceType": "Organization", "id": "Organization-net-1", "type": types});
    let row = single_row("Organization", &line.to_string());
    vec![
        text(&row[7]).map(|code| vec![code.to_owned()]),
        text(&row[8]).map(|text| vec![text.to_owned()]),
        texts(&row[9]),
    ]
}

#[test]
fn cms_npd_typed_copy_organization_other_type_codes() {
    let types = json!([
        {"coding": [{"code": "prov", "display": "Provider"}, {"code": "ntwk"}, {"code": "prov"}], "text": "Provider"},
        {"coding": [{"code": "ntwk", "display": "Network"}], "text": "Network"},
        {"coding": [{"code": "team"}], "text": "Care team"},
        {"text": "ein"}
    ]);
    assert_eq!(
        organization_types(types),
        [
            strings(&["prov"]),
            None,
            strings(&["ntwk", "team", "Care team", "ein"])
        ]
    );
    let text_only = json!([{"text": "ntwk"}, {"text": "ntwk"}]);
    assert_eq!(
        organization_types(text_only),
        [None, strings(&["ntwk"]), None]
    );
    assert_eq!(
        organization_types(json!([{"coding": [{"code": "ntwk"}]}])),
        [strings(&["ntwk"]), None, None]
    );
}

fn npi_explicit(identifiers: Value) -> Option<bool> {
    let line = json!({"resourceType": "Practitioner", "id": "Practitioner-1234567893", "identifier": identifiers});
    boolean(&single_row("Practitioner", &line.to_string())[9])
}

#[test]
fn cms_npd_typed_copy_practitioner_npi_explicit() {
    let us_npi = "http://hl7.org/fhir/sid/us-npi";
    let own = json!({"system": NPI, "value": "1234567893"});
    assert_eq!(npi_explicit(json!([own])), Some(true));
    assert_eq!(
        npi_explicit(json!([{"system": us_npi, "value": "1234567893"}])),
        Some(true)
    );
    assert_eq!(npi_explicit(json!([])), Some(false));
    assert_eq!(
        npi_explicit(json!([{"system": "urn:other", "value": "1234567893"}])),
        Some(false)
    );
    assert_eq!(
        npi_explicit(json!([own, {"system": us_npi, "value": "1245319599"}])),
        Some(false)
    );
    // A second NPI_SYSTEM identifier stays explicit here; it lands in old_npis, which the reader checks.
    assert_eq!(
        npi_explicit(json!([own, {"system": NPI, "value": "1245319599"}])),
        Some(true)
    );
    let line = json!({"resourceType": "Practitioner", "id": "Practitioner-1234567890",
        "identifier": [{"system": NPI, "value": "1234567890"}]});
    assert_eq!(
        boolean(&single_row("Practitioner", &line.to_string())[9]),
        Some(false)
    );
}

#[test]
fn cms_npd_typed_copy_location_part_of() {
    let line = json!({"resourceType": "Location", "id": format!("Location-{U1}"),
        "partOf": {"reference": format!("Location/Location-{U2}")}});
    assert_eq!(
        uuid(&single_row("Location", &line.to_string())[15]),
        Some(u(U2))
    );
}

#[test]
fn cms_npd_typed_copy_practitioner_role_plans_and_networks() {
    let line = json!({"resourceType": "PractitionerRole", "id": format!("PractitionerRole-{U1}"),
        "insurancePlan": [{"reference": format!("InsurancePlan/InsurancePlan-{U2}")}],
        "network": [{"reference": "Organization/Organization-net-1"}, {"reference": format!("Organization/Organization-{U3}")}]});
    let row = single_row("PractitionerRole", &line.to_string());
    assert_eq!(uuids(&row[13]), Some(vec![u(U2)]));
    assert_eq!(texts(&row[14]), strings(&["net-1", U3]));
}

#[test]
fn cms_npd_typed_copy_insurance_plan_references() {
    let line = json!({"resourceType": "InsurancePlan", "id": format!("InsurancePlan-{U1}"), "meta": {"lastUpdated": TS},
        "period": {"start": "2026-01-01T00:00:00Z"},
        "administeredBy": {"reference": "Organization/Organization-1234567893"},
        "coverageArea": [{"reference": format!("Location/Location-{U2}")}],
        "plan": [
            {"network": [{"reference": "Organization/Organization-net-1"}], "coverageArea": [{"reference": format!("Location/Location-{U3}")}]},
            {"period": {"start": "2026-03-01", "end": "2026-06-30T00:00:00Z"},
             "network": [{"reference": "Organization/Organization-net-2"}, {"reference": "Organization/Organization-net-3"}]}
        ],
        "coverage": [{"period": {}, "network": [{"reference": "Organization/Organization-net-4"}]}]});
    let row = single_row("InsurancePlan", &line.to_string());
    assert_eq!(text(&row[9]), Some("1234567893"));
    assert_eq!(uuids(&row[10]), Some(vec![u(U2)]));
    let reference = |field, parent, ord, target: &str, start: Option<&str>, end: Option<&str>| json!({"field": field, "parent": parent, "ord": ord, "target": target, "period_start": start, "period_end": end});
    let plan_period = Some("2026-01-01");
    assert_eq!(
        jsonb(&row[11]),
        Some(json!([
            reference("plan.network", 0, 0, "net-1", plan_period, None),
            reference(
                "plan.network",
                1,
                0,
                "net-2",
                Some("2026-03-01"),
                Some("2026-06-30")
            ),
            reference(
                "plan.network",
                1,
                1,
                "net-3",
                Some("2026-03-01"),
                Some("2026-06-30")
            ),
            reference("plan.coverageArea", 0, 0, U3, plan_period, None),
            reference("coverage.network", 0, 0, "net-4", None, None),
        ]))
    );
}

#[test]
fn cms_npd_typed_copy_rejects_bad_nested_references() {
    let cases = [
        json!({"plan": [{"coverageArea": [{"reference": "Location/Location-not-a-uuid"}]}]}),
        json!({"coverage": [{"network": [{"reference": "Location/Location-x"}]}]}),
        json!({"plan": [{"period": {"start": "2026-02-30"}, "network": [{"reference": "Organization/Organization-n"}]}]}),
        json!({"administeredBy": {"reference": "Practitioner/Practitioner-1234567893"}}),
    ];
    for mut case in cases {
        case["resourceType"] = json!("InsurancePlan");
        case["id"] = json!(format!("InsurancePlan-{U1}"));
        let (encoder, _) = crate::cms_npd_typed_copy::encoder_for("InsurancePlan").unwrap();
        let mut output = crate::cms_npd_typed_copy::Output::default();
        assert!(
            encoder(case.to_string().as_bytes(), &mut output).is_err(),
            "{case}"
        );
    }
}

#[test]
fn cms_npd_typed_copy_rejects_invalid_directory_references_and_dates() {
    let org = json!({"reference": "Organization/Organization-example"});
    let wrong_org = json!({"reference": "Location/Location-example"});
    let bad_date = json!({"lastUpdated": "tomorrow"});
    let cases = [
        (
            "Endpoint",
            json!({"connectionType": "direct"}),
            "invalid record",
        ),
        (
            "HealthcareService",
            json!({"location": "remote"}),
            "invalid record",
        ),
        ("InsurancePlan", json!({"plan": "active"}), "invalid record"),
        (
            "OrganizationAffiliation",
            json!({"network": "network-1"}),
            "invalid record",
        ),
        (
            "Location",
            json!({"partOf": org}),
            "invalid Location reference",
        ),
        (
            "Location",
            json!({"managingOrganization": wrong_org}),
            "invalid Organization reference",
        ),
        (
            "Endpoint",
            json!({"managingOrganization": wrong_org}),
            "invalid Organization reference",
        ),
        (
            "HealthcareService",
            json!({"providedBy": wrong_org}),
            "invalid Organization reference",
        ),
        (
            "HealthcareService",
            json!({"extension": [{"url": "https://example.test/base-ext-network-reference", "valueReference": wrong_org}]}),
            "invalid Organization reference",
        ),
        (
            "HealthcareService",
            json!({"location": [org]}),
            "invalid Location reference",
        ),
        (
            "InsurancePlan",
            json!({"network": [wrong_org]}),
            "invalid Organization reference",
        ),
        (
            "InsurancePlan",
            json!({"ownedBy": wrong_org}),
            "invalid Organization reference",
        ),
        (
            "InsurancePlan",
            json!({"coverageArea": [org]}),
            "invalid Location reference",
        ),
        (
            "InsurancePlan",
            json!({"period": {"end": "2026-02-30"}}),
            "invalid date",
        ),
        (
            "InsurancePlan",
            json!({"plan": [{"period": {"end": "2026-02-30"}, "network": [org]}]}),
            "invalid date",
        ),
        (
            "InsurancePlan",
            json!({"plan": [{"coverageArea": [org]}]}),
            "invalid Location reference",
        ),
        (
            "OrganizationAffiliation",
            json!({"organization": wrong_org}),
            "invalid Organization reference",
        ),
        (
            "OrganizationAffiliation",
            json!({"participatingOrganization": wrong_org}),
            "invalid Organization reference",
        ),
        (
            "OrganizationAffiliation",
            json!({"network": [wrong_org]}),
            "invalid Organization reference",
        ),
        ("Endpoint", json!({"meta": bad_date}), "invalid timestamp"),
        (
            "InsurancePlan",
            json!({"meta": bad_date}),
            "invalid timestamp",
        ),
        (
            "HealthcareService",
            json!({"meta": bad_date}),
            "invalid timestamp",
        ),
        (
            "OrganizationAffiliation",
            json!({"meta": bad_date}),
            "invalid timestamp",
        ),
    ];
    for (resource, mut record, reason) in cases {
        record["resourceType"] = json!(resource);
        record["id"] = json!(format!("{resource}-{U1}"));
        let error = super::encode_line(resource, &record.to_string())
            .err()
            .unwrap_or_else(|| panic!("accepted invalid {resource} record: {record}"));
        assert!(error.contains(reason), "{error} does not contain {reason}");
    }
}

#[test]
fn cms_npd_typed_copy_rejects_text_that_postgres_cannot_store() {
    let invalid = "text\0with a NUL";
    let cases = [
        ("Location", json!({"name": invalid})),
        ("Location", json!({"description": invalid})),
        ("Location", json!({"status": invalid})),
        ("Location", json!({"address": {"city": invalid}})),
        (
            "Location",
            json!({"telecom": [{"system": "phone", "value": invalid}]}),
        ),
        ("Endpoint", json!({"connectionType": {"code": invalid}})),
        ("Endpoint", json!({"address": invalid})),
        ("Endpoint", json!({"name": invalid})),
        ("Endpoint", json!({"status": invalid})),
        ("InsurancePlan", json!({"name": invalid})),
        ("InsurancePlan", json!({"status": invalid})),
        ("InsurancePlan", json!({"alias": [invalid]})),
        ("InsurancePlan", json!({"type": [{"text": invalid}]})),
        (
            "OrganizationAffiliation",
            json!({"code": [{"coding": [{"code": invalid}]}]}),
        ),
    ];
    for (resource, mut record) in cases {
        record["resourceType"] = json!(resource);
        record["id"] = json!(format!("{resource}-{U1}"));
        let error = super::encode_line(resource, &record.to_string())
            .err()
            .unwrap_or_else(|| panic!("accepted invalid {resource} text: {record}"));
        assert!(error.contains("NUL character"), "unexpected error: {error}");
    }
}
