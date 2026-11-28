# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Only detail-confirmed dental network participation enters the directory."""

import pytest

from process.guardian_directory_normalization import merge_guardian_resources, normalize_guardian_detail
from process.provider_directory_fhir import parse_fhir_resource
from process.registry_fhir_catalog_extraction import extract_fhir_catalog_batch


def detail(**overrides):
    return {
        "cardType": "P",
        "practiceId": "office-01",
        "denMastId": "doctor-03",
        "ddsPracticeName": "Example Dental Office",
        "ddsFirstName": "Alex",
        "ddsLastName": "Example",
        "address1": "17 Example Lane",
        "suite": "Unit 2",
        "city": "Example City",
        "state": "NY",
        "zip": "10001",
        "phone": "212-555-0104",
        "npi": "1234567893",
        "networks": "Example Dental Network~Example Local Network~Example Dental Network",
        "tiers": "Example Benefit Tier",
        "planType": "PPO",
        **overrides,
    }


def family(resources, resource_type):
    return [item for item in resources if item["resourceType"] == resource_type]


def test_clinician_memberships_flow_through_shared_parser_and_catalog():
    resources = normalize_guardian_detail(detail())
    (site,) = family(resources, "Location")
    (person,) = family(resources, "Practitioner")
    assert parse_fhir_resource("example", site)[1]["second_line"] == "Unit 2"
    assert parse_fhir_resource("example", person)[1]["npi"] == 1234567893
    roles = family(resources, "PractitionerRole")
    networks = family(resources, "Organization")
    assert {network["name"] for network in networks} == {"Example Dental Network", "Example Local Network"}
    assert len(roles) == 2
    for role in roles:
        parsed = parse_fhir_resource("example", role)[1]
        assert parsed["location_refs"] == [f"Location/{site['id']}"]
        assert parsed["practitioner_ref"] == f"Practitioner/{person['id']}"
        assert len(parsed["network_refs"]) == 1
    catalog = extract_fhir_catalog_batch(
        [
            {
                "resource_type": resource["resourceType"],
                "resource_id": resource["id"],
                "payload_json": resource,
                "payload_hash": "a" * 64,
            }
            for resource in (*networks, *roles)
        ],
        profile="raw_fhir",
    )
    assert all(org.declared_network for org in catalog.organizations)
    assert {edge.network_resource_id for edge in catalog.edges} == {network["id"] for network in networks}


def test_selection_does_not_create_membership_and_overlap_is_idempotent():
    first = normalize_guardian_detail(detail())
    second = normalize_guardian_detail(detail(planType="ELITE", networks="Example Local Network", tiers="Other Tier"))
    assert all(r in first for r in second)
    unlinked = normalize_guardian_detail(detail(networks=""))
    assert family(unlinked, "Organization") == []
    (role,) = family(unlinked, "PractitionerRole")
    assert not parse_fhir_resource("example", role)[1]["network_refs"]


@pytest.mark.parametrize("npi", [None, "", "0000000000", "1234567890"])
def test_missing_npi_keeps_publisher_identity(npi):
    resources = normalize_guardian_detail(detail(npi=npi, denMastId=123))
    (person,) = family(resources, "Practitioner")
    assert person["identifier"] == [{"system": "urn:guardian:directory:dentist-id", "value": "123"}]
    assert parse_fhir_resource("example", person)[1]["npi"] is None


def test_facility_and_nested_clinician_memberships():
    nested_map = {
        "denMastId": "doctor-03",
        "ddsFirstName": "Alex",
        "ddsLastName": "Example",
        "specialties": "Endodontist",
    }
    resources = normalize_guardian_detail(detail(cardType="F", denMastId=0, npi="", providers=[nested_map]))
    (site,) = family(resources, "Location")
    (person,) = family(resources, "Practitioner")
    (practitioner_role,) = family(resources, "PractitionerRole")
    parsed = parse_fhir_resource("example", practitioner_role)[1]
    assert parsed["network_refs"] == []
    assert parsed["location_refs"] == [f"Location/{site['id']}"]
    assert parsed["specialty_codes"][0]["text"] == "Endodontist"
    facility_orgs = [org for org in family(resources, "Organization") if "type" not in org]
    assert len(facility_orgs) == 1
    affiliations = family(resources, "OrganizationAffiliation")
    assert len(affiliations) == 2
    for affiliation in affiliations:
        parsed = parse_fhir_resource("example", affiliation)[1]
        assert parsed["participating_organization_ref"] == f"Organization/{facility_orgs[0]['id']}"
        assert parsed["location_refs"] == [f"Location/{site['id']}"]
        assert len(parsed["network_refs"]) == 1
    (richer,) = family(
        normalize_guardian_detail(detail(ddsMiddleName="T", ddsDegree="DDS", gender="Female")), "Practitioner"
    )
    assert richer["gender"] == "female"
    assert merge_guardian_resources(richer, person) == merge_guardian_resources(person, richer) == richer
    (conflicting,) = family(normalize_guardian_detail(detail(npi="1999999992")), "Practitioner")
    with pytest.raises(ValueError, match="guardian_resource_payload_conflict"):
        merge_guardian_resources(richer, conflicting)
    with pytest.raises(ValueError, match="guardian_resource_payload_conflict"):
        merge_guardian_resources(site, {**site, "address": {"city": "Different Site"}})


@pytest.mark.parametrize(
    "change,reason",
    [
        ({"cardType": "other"}, "shape_invalid"),
        ({"practiceId": -2}, "identity_invalid"),
        ({"denMastId": 0, "npi": ""}, "identity_invalid"),
        ({"cardType": "F", "ddsPracticeName": ""}, "facility_name_missing"),
        ({"address1": "", "suite": "", "address2": ""}, "address_invalid"),
        ({"networks": 3}, "text_invalid"),
        ({"providers": [{}]}, "nested_provider_shape_invalid"),
        ({"latitude": 95, "longitude": -63}, "position_invalid"),
    ],
)
def test_malformed_or_ambiguous_rows_are_rejected(change, reason):
    with pytest.raises(ValueError, match=reason):
        normalize_guardian_detail(detail(**change))


def test_verified_npi_can_identify_a_clinician_without_a_source_id():
    (person,) = family(normalize_guardian_detail(detail(denMastId=None)), "Practitioner")
    assert person["id"] == "guardian-npi-1234567893"
    assert parse_fhir_resource("example", person)[1]["npi"] == 1234567893


def test_unattributed_facility_npi_stays_in_source_record():
    source = detail(cardType="F", denMastId=0, npi="1234567893")
    resources = normalize_guardian_detail(source)
    assert source["npi"] == "1234567893"
    assert family(resources, "Practitioner") == []
    for org in family(resources, "Organization"):
        assert parse_fhir_resource("example", org)[1]["npi"] is None


def test_source_missing_label_does_not_declare_a_network():
    resources = normalize_guardian_detail(detail(networks="Example One~No Information Available"))
    assert [item["name"] for item in family(resources, "Organization")] == ["Example One"]


def test_shared_location_retains_contact_variants():
    (site,) = family(normalize_guardian_detail(detail()), "Location")
    (variant,) = family(
        normalize_guardian_detail(detail(ddsPracticeName="Another Example Office", phone="212-555-0108")), "Location"
    )
    assert variant["id"] == site["id"]
    merged = merge_guardian_resources(variant, site)
    assert merge_guardian_resources(site, variant) == merge_guardian_resources(merged, variant) == merged
    assert {merged["name"], *merged["alias"]} == {variant["name"], site["name"]}
    assert {contact["value"] for contact in merged["telecom"]} == {"212-555-0104", "212-555-0108"}


def test_scoped_membership_preserves_overlapping_specialties():
    roles = family(
        normalize_guardian_detail(detail(networks="Example Network", specialties="General Dentist")), "PractitionerRole"
    )
    others = family(
        normalize_guardian_detail(detail(networks="Example Network", specialties="Endodontist")), "PractitionerRole"
    )
    merged = merge_guardian_resources(roles[0], others[0])
    assert merged["id"] == roles[0]["id"] == others[0]["id"]
    assert merged["specialty"] == [{"text": "Endodontist"}, {"text": "General Dentist"}]
