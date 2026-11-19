// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

//! One synthetic record per resource type, decoded back column by column.

use super::{
    boolean, float8, int4, int8, int8s, jsonb, read_rows, text, texts, u, uuid, uuids, Field,
};
use crate::cms_npd_typed_copy::{encoder_for, Output};
use serde_json::json;

pub(super) const TS: &str = "2026-09-24T15:38:16.229996Z";
const TS_MICROS: Option<i64> = Some(843_579_496_229_996);
pub(super) const NPI: &str = "http://terminology.hl7.org/NamingSystem/npi";
const EXT: &str = "http://hl7.org/fhir/us/ndh/StructureDefinition/";
pub(super) const U1: &str = "11111111-1111-4111-8111-111111111111";
pub(super) const U2: &str = "22222222-2222-4222-8222-222222222222";
pub(super) const U3: &str = "33333333-3333-4333-8333-333333333333";

fn encode(resource_type: &str, line: &str) -> Result<Output, String> {
    let (encoder, _) = encoder_for(resource_type).unwrap();
    let mut output = Output::default();
    encoder(line.as_bytes(), &mut output).map(|()| output)
}

pub(super) fn single_row(resource_type: &str, line: &str) -> Vec<Field> {
    let output = encode(resource_type, line).unwrap();
    assert_eq!(output.rows, 1);
    let mut rows = read_rows(&output.main);
    assert_eq!(rows.len(), 1);
    rows.remove(0)
}

pub(super) fn strings(items: &[&str]) -> Option<Vec<String>> {
    Some(items.iter().map(|item| item.to_string()).collect())
}

fn assert_address(row: &[Field], ord: i32, expected: [Option<&str>; 8]) {
    assert_eq!(int4(&row[1]), Some(ord));
    let actual: Vec<Option<&str>> = row[2..].iter().map(text).collect();
    assert_eq!(actual, expected);
}

#[test]
fn cms_npd_typed_copy_organization_row_and_addresses() {
    let line = json!({
        "resourceType": "Organization", "id": "Organization-1234567890", "meta": {"lastUpdated": TS},
        "extension": [{"url": format!("{EXT}base-ext-verification-status"), "valueCodeableConcept": {"coding": [{"code": "complete"}]}}],
        "identifier": [
            {"system": NPI, "value": "1234567890", "period": {"start": "2009-12-10T00:00:00Z"}},
            {"system": NPI, "value": "1098765432"},
            {"system": "https://npd.cms.gov/fhir/sid/us-pseudo-ein", "value": U1},
            {"system": "urn:example", "value": "ignored"}
        ],
        "active": true,
        "type": [{"coding": [{"code": "prov", "display": "Healthcare Provider"}], "text": "Healthcare Provider"}],
        "name": "Example Clinic \u{e9}",
        "partOf": {"reference": "Organization/Organization-State-ZZ"},
        "endpoint": [{"reference": format!("Endpoint/Endpoint-{U2}")}],
        "telecom": [
            {"system": "phone", "value": "5550100"}, {"system": "fax", "value": "5550101"},
            {"system": "url", "value": "https://example.test"}, {"system": "email", "value": "a@example.test"}
        ],
        "address": [
            {"use": "work", "type": "physical", "line": ["1 Main St", "Suite 2", "Floor 3"], "city": "Exampleville", "state": "ZZ", "postalCode": "00001", "country": "US"},
            {"line": ["PO Box 9"]}
        ],
        "unknownField": {"nested": [1, 2.5, null]}
    })
    .to_string();
    let output = encode("Organization", &line).unwrap();
    let row = &read_rows(&output.main)[0];
    assert_eq!(row.len(), 17);
    assert_eq!(text(&row[0]), Some("1234567890"));
    assert_eq!(int8(&row[1]), Some(1_234_567_890));
    assert_eq!(int8s(&row[2]), Some(vec![1_098_765_432]));
    assert_eq!(uuids(&row[3]), Some(vec![u(U1)]));
    assert_eq!(int4(&row[4]), Some(3631));
    assert_eq!(text(&row[5]), Some("Example Clinic \u{e9}"));
    assert_eq!(boolean(&row[6]), Some(true));
    assert_eq!(text(&row[7]), Some("prov"));
    assert_eq!(text(&row[8]), None);
    assert_eq!(texts(&row[9]), None);
    assert_eq!(text(&row[10]), Some("State-ZZ"));
    assert_eq!(text(&row[11]), Some("complete"));
    assert_eq!(uuids(&row[12]), Some(vec![u(U2)]));
    assert_eq!(texts(&row[13]), strings(&["5550100"]));
    assert_eq!(texts(&row[14]), strings(&["5550101"]));
    assert_eq!(texts(&row[15]), strings(&["https://example.test"]));
    assert_eq!(int8(&row[16]), TS_MICROS);

    assert_eq!(output.address_rows, 2);
    let addresses = read_rows(&output.address);
    assert_eq!(text(&addresses[0][0]), Some("1234567890"));
    let first = [
        "work",
        "physical",
        "1 Main St",
        "Suite 2, Floor 3",
        "Exampleville",
        "ZZ",
        "00001",
        "US",
    ];
    assert_address(&addresses[0], 0, first.map(Some));
    assert_address(
        &addresses[1],
        1,
        [None, None, Some("PO Box 9"), None, None, None, None, None],
    );
}

#[test]
fn cms_npd_typed_copy_organization_nulls_and_type_text() {
    let line = json!({
        "resourceType": "Organization", "id": "Organization-1234567890-part-1", "meta": {"lastUpdated": TS},
        "type": [{"coding": [{"code": "pay", "display": "Payer"}], "text": "ein"}],
        "telecom": null
    })
    .to_string();
    let output = encode("Organization", &line).unwrap();
    let row = &read_rows(&output.main)[0];
    assert_eq!(text(&row[0]), Some("1234567890-part-1"));
    assert_eq!(text(&row[8]), Some("ein"));
    for (index, field) in row.iter().enumerate() {
        if ![0, 7, 8, 16].contains(&index) {
            assert_eq!(field, &None, "column {index}");
        }
    }
    assert_eq!((output.address_rows, output.address.len()), (0, 0));
}

#[test]
fn cms_npd_typed_copy_practitioner_row_and_addresses() {
    let tax = "http://hl7.org/fhir/us/ndh/ValueSet/HealthcareIndividualTaxonomyVS";
    let line = json!({
        "resourceType": "Practitioner", "id": "Practitioner-1234567890", "meta": {"lastUpdated": TS},
        "extension": [
            {"url": format!("{EXT}base-ext-cms-identity-verified"), "valueBoolean": true},
            {"url": format!("{EXT}base-ext-cms_medicare_enrollment"), "valueBoolean": false},
            {"url": format!("{EXT}base-ext-hhs-in-exclusion-list"), "valueBoolean": false}
        ],
        "identifier": [
            {"system": NPI, "use": "official", "value": "1234567890", "period": {"start": "2009-12-10T00:00:00Z", "end": "2030-01-01T00:00:00Z"}},
            {"system": NPI, "use": "old", "value": "1111111111"}
        ],
        "active": true, "gender": "female",
        "name": [
            {"use": "official", "family": "Doe", "given": ["Jane", "Q"], "prefix": ["Dr."], "suffix": ["MD", "PhD"]},
            {"use": "old", "family": "Roe", "given": ["Jane"], "period": {"start": "1987-06-15T00:00:00Z"}}
        ],
        "qualification": [
            {"code": {"coding": [{"system": tax, "code": "207Q00000X"}], "text": "Family Medicine"}},
            {"code": {"coding": [{"system": tax, "code": "207Q00000X"}]}, "identifier": [{"value": "LIC-1"}], "issuer": {"reference": "Organization/Organization-State-ZZ"}},
            {"code": {"coding": [{"system": "urn:example:credential", "code": "MD"}]}},
            {"code": {"text": "Uncoded credential"}, "identifier": [{"value": "LIC-2"}]}
        ],
        "telecom": [{"system": "phone", "value": "5550100"}],
        "address": [{"use": "work", "type": "physical", "line": ["1 Main St"], "city": "Exampleville", "state": "ZZ", "postalCode": "00001", "country": "US"}]
    })
    .to_string();
    let output = encode("Practitioner", &line).unwrap();
    let row = &read_rows(&output.main)[0];
    assert_eq!(row.len(), 24);
    assert_eq!(int8(&row[0]), Some(1_234_567_890));
    assert_eq!(boolean(&row[1]), Some(true));
    assert_eq!(text(&row[2]), Some("female"));
    assert_eq!(text(&row[3]), Some("Doe"));
    assert_eq!(texts(&row[4]), strings(&["Jane", "Q"]));
    assert_eq!(text(&row[5]), Some("Dr."));
    assert_eq!(text(&row[6]), Some("MD PhD"));
    let other = json!([{"use": "old", "family": "Roe", "given": ["Jane"], "prefix": null, "suffix": null, "since": "1987-06-15"}]);
    assert_eq!(jsonb(&row[7]), Some(other));
    assert_eq!(int8s(&row[8]), Some(vec![1_111_111_111]));
    assert_eq!(boolean(&row[9]), Some(false)); // 1234567890 fails the NPI checksum
    assert_eq!((int4(&row[10]), int4(&row[11])), (Some(3631), Some(10_958)));
    let flags: Vec<_> = row[12..16].iter().map(boolean).collect();
    assert_eq!(flags, [Some(true), Some(false), Some(false), None]);
    assert_eq!(texts(&row[16]), strings(&["207Q00000X"]));
    assert_eq!(texts(&row[17]), strings(&["MD", "Uncoded credential"]));
    assert_eq!(texts(&row[18]), strings(&["ZZ", ""]));
    assert_eq!(texts(&row[19]), strings(&["LIC-1", "LIC-2"]));
    assert_eq!(
        texts(&row[20]),
        strings(&["207Q00000X", "Uncoded credential"])
    );
    assert_eq!(texts(&row[21]), strings(&["5550100"]));
    assert_eq!(texts(&row[22]), None);
    assert_eq!(int8(&row[23]), TS_MICROS);

    assert_eq!(output.address_rows, 1);
    let address = &read_rows(&output.address)[0];
    assert_eq!(int8(&address[0]), Some(1_234_567_890));
    let mut expected = [
        "work",
        "physical",
        "1 Main St",
        "",
        "Exampleville",
        "ZZ",
        "00001",
        "US",
    ]
    .map(Some);
    expected[3] = None;
    assert_address(address, 0, expected);
}

#[test]
fn cms_npd_typed_copy_practitioner_role_row() {
    let line = json!({
        "resourceType": "PractitionerRole", "id": format!("PractitionerRole-{U1}"), "meta": {"lastUpdated": TS},
        "active": false,
        "practitioner": {"reference": "Practitioner/Practitioner-1234567890"},
        "organization": {"reference": "Organization/Organization-1098765432"},
        "location": [{"reference": format!("Location/Location-{U2}")}],
        "endpoint": [{"reference": format!("Endpoint/Endpoint-{U3}")}],
        "extension": [
            {"url": format!("{EXT}base-ext-newpatients"), "extension": [{"url": "acceptingPatients", "valueCodeableConcept": {"coding": [{"code": "newpt"}]}}]},
            {"url": format!("{EXT}base-ext-network-reference"), "valueReference": {"reference": format!("Organization/Organization-{U3}")}}
        ],
        "specialty": [{"coding": [{"code": "207Q00000X"}]}, {"text": "Uncoded"}],
        "period": {"start": "2024-02-29T00:00:00Z"},
        "telecom": [{"system": "fax", "value": "5550101"}]
    })
    .to_string();
    let row = single_row("PractitionerRole", &line);
    assert_eq!(row.len(), 16);
    assert_eq!(uuid(&row[0]), Some(u(U1)));
    assert_eq!(int8(&row[1]), Some(1_234_567_890));
    assert_eq!(text(&row[2]), Some("1098765432"));
    assert_eq!(boolean(&row[3]), Some(false));
    assert_eq!(uuids(&row[4]), Some(vec![u(U2)]));
    assert_eq!(uuids(&row[5]), Some(vec![u(U3)]));
    assert_eq!(text(&row[6]), Some(U3));
    assert_eq!(text(&row[7]), Some("newpt"));
    assert_eq!(texts(&row[8]), strings(&["207Q00000X", "Uncoded"]));
    assert_eq!((int4(&row[9]), int4(&row[10])), (Some(8825), None));
    assert_eq!(texts(&row[11]), None);
    assert_eq!(texts(&row[12]), strings(&["5550101"]));
    assert_eq!((uuids(&row[13]), texts(&row[14])), (None, None));
    assert_eq!(int8(&row[15]), TS_MICROS);
}

#[test]
fn cms_npd_typed_copy_location_row() {
    let line = json!({
        "resourceType": "Location", "id": format!("Location-{U1}"), "meta": {"lastUpdated": TS},
        "status": "active", "name": "1 Main St", "description": "1 Main St",
        "telecom": [{"system": "fax", "value": "5550101"}, {"system": "phone", "value": "5550100"}],
        "address": {"line": ["1 Main St", "Unit 4"], "city": "Exampleville", "state": "ZZ", "postalCode": "00001", "country": "US", "type": "physical"},
        "position": {"longitude": -98.765_432, "latitude": 12.345_678},
        "managingOrganization": {"reference": "Organization/Organization-1234567890"}
    })
    .to_string();
    let row = single_row("Location", &line);
    assert_eq!(row.len(), 17);
    assert_eq!(uuid(&row[0]), Some(u(U1)));
    let middle: Vec<Option<&str>> = row[1..10].iter().map(text).collect();
    let place = ["1 Main St", "Unit 4", "Exampleville", "ZZ", "00001", "US"].map(Some);
    assert_eq!(middle[..3], [Some("1 Main St"), None, Some("active")]);
    assert_eq!(middle[3..], place);
    assert_eq!(
        (float8(&row[10]), float8(&row[11])),
        (Some(-98.765_432), Some(12.345_678))
    );
    assert_eq!(text(&row[12]), Some("1234567890"));
    assert_eq!(
        (texts(&row[13]), texts(&row[14])),
        (strings(&["5550100"]), strings(&["5550101"]))
    );
    assert_eq!(uuid(&row[15]), None);
    assert_eq!(int8(&row[16]), TS_MICROS);
}

#[test]
fn cms_npd_typed_copy_endpoint_row() {
    let line = json!({
        "resourceType": "Endpoint", "id": format!("Endpoint-{U1}"), "meta": {"lastUpdated": TS},
        "extension": [{"url": format!("{EXT}base-ext-verification-status"), "valueCodeableConcept": {"coding": [{"code": "complete"}]}}],
        "status": "active", "connectionType": {"code": "direct-project", "display": "Direct Project"},
        "name": "Direct Messaging Address", "address": "someone@direct.example.test",
        "managingOrganization": {"reference": format!("Organization/Organization-{U2}")}
    })
    .to_string();
    let row = single_row("Endpoint", &line);
    assert_eq!(uuid(&row[0]), Some(u(U1)));
    let middle: Vec<Option<&str>> = row[1..7].iter().map(text).collect();
    let expected = [
        "direct-project",
        "someone@direct.example.test",
        "Direct Messaging Address",
        "active",
        "complete",
        U2,
    ];
    assert_eq!(middle, expected.map(Some));
    assert_eq!(int8(&row[7]), TS_MICROS);
}

#[test]
fn cms_npd_typed_copy_healthcare_service_row() {
    let line = json!({
        "resourceType": "HealthcareService", "id": format!("HealthcareService-{U1}"), "meta": {"lastUpdated": TS},
        "active": true, "providedBy": {"reference": "Organization/Organization-1234567890"},
        "location": [{"reference": format!("Location/Location-{U2}")}, {"reference": format!("Location/Location-{U3}")}],
        "extension": [{"url": format!("{EXT}base-ext-network-reference"), "valueReference": {"reference": format!("Organization/Organization-{U3}")}}]
    })
    .to_string();
    let row = single_row("HealthcareService", &line);
    assert_eq!(row.len(), 6);
    assert_eq!((uuid(&row[0]), boolean(&row[1])), (Some(u(U1)), Some(true)));
    assert_eq!(
        (text(&row[2]), text(&row[3])),
        (Some(U3), Some("1234567890"))
    );
    assert_eq!(uuids(&row[4]), Some(vec![u(U2), u(U3)]));
    assert_eq!(int8(&row[5]), TS_MICROS);
}

#[test]
fn cms_npd_typed_copy_insurance_plan_row() {
    let line = json!({
        "resourceType": "InsurancePlan", "id": format!("InsurancePlan-{U1}"), "meta": {"lastUpdated": TS},
        "status": "active", "name": "Example Plan (PPO)", "type": [{"text": "Example PPO Plan"}],
        "alias": ["X0000-001-000", "X0000"],
        "period": {"start": "2026-01-01T00:00:00Z", "end": "2026-12-31T00:00:00Z"},
        "ownedBy": {"reference": format!("Organization/Organization-{U2}")},
        "network": [{"reference": format!("Organization/Organization-{U3}")}, {"reference": "Organization/Organization-HSI00000001"}]
    })
    .to_string();
    let row = single_row("InsurancePlan", &line);
    assert_eq!(row.len(), 13);
    assert_eq!(uuid(&row[0]), Some(u(U1)));
    assert_eq!(
        (text(&row[1]), text(&row[2])),
        (Some("Example Plan (PPO)"), Some("active"))
    );
    assert_eq!(texts(&row[3]), strings(&["X0000-001-000", "X0000"]));
    assert_eq!(text(&row[4]), Some(U2));
    assert_eq!(texts(&row[5]), strings(&[U3, "HSI00000001"]));
    assert_eq!(text(&row[6]), Some("Example PPO Plan"));
    assert_eq!((int4(&row[7]), int4(&row[8])), (Some(9497), Some(9861)));
    assert_eq!(row[9..12], [None, None, None]);
    assert_eq!(int8(&row[12]), TS_MICROS);
}

#[test]
fn cms_npd_typed_copy_organization_affiliation_row() {
    let line = json!({
        "resourceType": "OrganizationAffiliation", "id": format!("OrganizationAffiliation-{U1}"), "meta": {"lastUpdated": TS},
        "active": true,
        "organization": {"reference": format!("Organization/Organization-{U2}")},
        "participatingOrganization": {"reference": "Organization/Organization-1234567890"},
        "code": [{"coding": [{"code": "bt", "display": "Member Of"}], "text": "Member Of"}]
    })
    .to_string();
    let row = single_row("OrganizationAffiliation", &line);
    assert_eq!(row.len(), 7);
    assert_eq!((uuid(&row[0]), boolean(&row[1])), (Some(u(U1)), Some(true)));
    assert_eq!(
        (text(&row[2]), text(&row[3]), text(&row[4])),
        (Some("bt"), Some(U2), Some("1234567890"))
    );
    assert_eq!(texts(&row[5]), None);
    assert_eq!(int8(&row[6]), TS_MICROS);
}

#[test]
fn cms_npd_typed_copy_missing_last_updated_is_null() {
    let line = json!({"resourceType": "Location", "id": format!("Location-{U1}")}).to_string();
    let row = single_row("Location", &line);
    assert_eq!(uuid(&row[0]), Some(u(U1)));
    assert_eq!(int8(row.last().unwrap()), None);
}
