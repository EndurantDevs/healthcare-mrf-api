// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

use super::support::{decode_copy_rows, rich_resources, rich_spool, PARTITION_ID, RECIPE_ID};
use serde_json::Value;

const EXPECTED_ROW_SHA256: &str =
    "ec3af3226a11a5d8726fa9632d2cab33f22b12289c5eb46e736165553dcb61ef";

const EXPECTED_IDENTITIES: [(&str, &str, &str); 8] = [
    (
        "Endpoint",
        "endpoint-1",
        "4cee9b58505b01d9b5bfe5c6258bf8fa01c8cdab66c02b65e462ad27e78bfed1",
    ),
    (
        "HealthcareService",
        "service-1",
        "ba039eaa0a581bdba7a70e36710c5404eaff95c6527dc8fe25fa7aace4a22a30",
    ),
    (
        "InsurancePlan",
        "plan-1",
        "45202be33aeea1f3afbb731a49934cc3d6703693281a6289ddbe81bd8ac99e24",
    ),
    (
        "Location",
        "location-1",
        "c3983b61742125326aebcafedaa904863b822108b1341565a311621422535679",
    ),
    (
        "Organization",
        "org-1",
        "cb561806fb8e89edc33b7222a7fe77d24d83a77699628e96357b8ed5d3275d44",
    ),
    (
        "OrganizationAffiliation",
        "aff-1",
        "0202d73195b82f28c78fae09f5c44ed68a18f303925308ef2c49f94131c180ae",
    ),
    (
        "Practitioner",
        "practitioner-1",
        "1a5131e2c7edc1e5d6c4239d98ba2c511a98c5a88793a169006656be433f9f49",
    ),
    (
        "PractitionerRole",
        "role-1",
        "e74af2400b7c7369e025ce7cb39c3df2c9fee102e27badfcc3409e01172da437",
    ),
];

#[test]
fn all_eight_families_match_the_python_golden_projection() {
    let resources = rich_resources();
    let spool = rich_spool();
    let rows = decode_copy_rows(&spool.copy_bytes);
    assert_eq!(rows.len(), 8);
    assert_eq!(spool.summary.canonical_row_sha256, EXPECTED_ROW_SHA256);

    for (ordinal, (row, (resource_type, resource_id, payload_hash))) in
        rows.iter().zip(EXPECTED_IDENTITIES).enumerate()
    {
        assert_eq!(row.field_count(), 18);
        assert_eq!(row.text(0), Some(RECIPE_ID));
        assert_eq!(row.text(1), Some(resource_type));
        assert_eq!(row.text(2), Some(resource_id));
        assert_eq!(row.text(3), Some(PARTITION_ID));
        assert_eq!(row.text(4), Some(payload_hash));
        let payload = row.jsonb(5).unwrap();
        assert_eq!(payload["resourceType"], resource_type);
        assert_eq!(payload["id"], resource_id);
        assert!(row
            .text(6)
            .unwrap()
            .starts_with(&format!("{:020}:{payload_hash}:", 7)));
        assert_eq!(row.text(6).unwrap().len(), 20 + 1 + 64 + 1 + 20);
        assert!(row.text(14).is_none());
        assert!(row.text(15).is_none());
        assert!(row.text(16).is_none());
        assert_eq!(payload, resources[input_ordinal_for_sorted_row(ordinal)]);
    }
}

#[test]
fn profile_evidence_and_summary_scalars_match_python_golden_rows() {
    let rows = decode_copy_rows(&rich_spool().copy_bytes);
    let expected = expected_profile_evidence();
    let expected_scalars = [
        (None, 0, false, false, 0, 0, true),
        (None, 1, false, false, 0, 0, true),
        (None, 0, false, false, 2, 0, true),
        (None, 1, true, true, 0, 0, true),
        (Some(1_234_567_893), 1, false, false, 0, 0, true),
        (None, 0, false, false, 0, 4, true),
        (Some(1_987_654_321), 0, false, false, 0, 0, false),
        (None, 0, false, false, 0, 0, true),
    ];
    for ((row, evidence), scalars) in rows.iter().zip(expected).zip(expected_scalars) {
        assert_eq!(row.i64(7), scalars.0);
        assert_eq!(row.i32(8), Some(scalars.1));
        assert_eq!(row.boolean(9), Some(scalars.2));
        assert_eq!(row.boolean(10), Some(scalars.3));
        assert_eq!(row.i32(11), Some(scalars.4));
        assert_eq!(row.i32(12), Some(scalars.5));
        assert_eq!(row.boolean(13), Some(scalars.6));
        assert_eq!(row.jsonb(17), Some(evidence));
    }
}

fn input_ordinal_for_sorted_row(sorted_ordinal: usize) -> usize {
    sorted_ordinal
}

fn expected_profile_evidence() -> Vec<Value> {
    serde_json::from_str(
        r#"[
{"contacts":[{"system":"phone","value":"2025550100"}],"references":[{"reference":"Organization/org-1"}]},
{"addresses":[{"city":"Prague","country":"CZ"}],"names":[{"text":"Cardiology Service"}],"references":[{"reference":"Organization/org-1"}],"specialties":[{"coding":[{"code":"207RC0000X","system":"urn:test"}]}]},
{"names":[{"text":"Plan One"}],"references":[{"reference":"Organization/network-1"},{"relationship_evidence":{"kind":"network","target_resource_id":"network-1","target_resource_type":"Organization"}},{"relationship_evidence":{"kind":"network","target_resource_id":"network-2","target_resource_type":"Organization"}}]},
{"addresses":[{"city":"Iowa City","country":"US","geocode_evidence":{"latitude_microdegrees":41600000,"longitude_microdegrees":-93600002},"line":["1 Main St"],"postalCode":"52240","state":"IA"}],"names":[{"text":"Klinika Žluťoučký 😀"}],"references":[{"reference":"Location/root"}]},
{"addresses":[{"city":"Austin","country":"US","state":"TX"}],"names":[{"text":"Org One"}]},
{"references":[{"reference":"Organization/org-1"},{"reference":"Organization/org-2"},{"reference":"Organization/network-1"},{"reference":"Location/location-1"},{"relationship_evidence":{"kind":"organization","target_resource_id":"org-1","target_resource_type":"Organization"}},{"relationship_evidence":{"kind":"participating_organization","target_resource_id":"org-2","target_resource_type":"Organization"}},{"relationship_evidence":{"kind":"network","target_resource_id":"network-1","target_resource_type":"Organization"}},{"relationship_evidence":{"kind":"location","target_resource_id":"location-1","target_resource_type":"Location"}}],"specialties":[{"coding":[{"code":"207Q00000X","system":"urn:test"}]}]},
{"contacts":[{"rank":1,"system":"phone","value":"2025550199"}],"names":[{"family":"Müller","given":["Zoë"]}],"specialties":[{"coding":[{"code":"207Q00000X","system":"urn:vendor"}],"text":"Family Medicine"}]},
{"references":[{"reference":"Practitioner/practitioner-1"},{"reference":"Organization/org-1"},{"reference":"Location/location-1"},{"reference":"InsurancePlan/plan-1"}],"specialties":[{"coding":[{"code":"207Q00000X","system":"urn:test"}]}]}
]"#,
    )
    .unwrap()
}
