# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""VSP office joins retain the source's network memberships and their limits."""

import pytest

from process.provider_directory_fhir import parse_fhir_resource
from process.registry_fhir_catalog_extraction import extract_fhir_catalog_batch
from process.vsp_directory_normalization import (
    VSPDirectoryNormalizationError,
    normalize_vsp_office,
    vsp_network_organizations,
)


def office(*summaries):
    return {
        "pid": 52,
        "name": "Example Office",
        "phone": "212-555-0100",
        "addr": {
            "street1": "12 Example Road",
            "street2": "Suite 3",
            "city": "Example City",
            "stateCode": "NY",
            "zipCode": "10001",
            "countryCode": "US",
            "latitude": 40.75,
            "longitude": -73.99,
        },
        "providerSummaries": list(summaries),
    }


def profile(doctor_id=2, *codes):
    return {
        "doctorPublicId": doctor_id,
        "networkIds": list(codes),
        "providerNetworks": [{"networkId": code} for code in codes],
    }


def _catalog_batch(resources):
    catalog_rows = [
        {
            "resource_type": resource["resourceType"],
            "resource_id": resource["id"],
            "payload_json": resource,
            "payload_hash": "a" * 64,
        }
        for resource in resources
    ]
    return extract_fhir_catalog_batch(catalog_rows, profile="raw_fhir")


def test_parsed_memberships_use_declared_networks_without_a_product():
    capture = office(
        {"pid": 2, "npi": "1234567893", "first": "Alex", "last": "Example"}, {"pid": 1, "npi": "1999999992"}
    )
    capture["addr"]["countyName"] = "Example County"
    profiles = [profile(2, "4000", "1000", "9999"), profile(1, "2000")]
    resources = normalize_vsp_office(capture, profiles=profiles)
    assert resources.other_network_ids == ("9999",)
    assert resources.outside_profile_count == resources.outside_summary_count == 0
    assert normalize_vsp_office(capture, profiles=list(reversed(profiles))) == resources
    assert [resource["resourceType"] for resource in resources] == [
        "Location",
        "Practitioner",
        "PractitionerRole",
        "Practitioner",
        "PractitionerRole",
    ]
    site, _, first_role, doctor, last_role = resources
    assert site["address"]["district"] == "Example County"
    site_row = parse_fhir_resource("example", site)[1]
    assert (site_row["first_line"], site_row["second_line"], site_row["state_code"], site_row["zip5"]) == (
        "12 Example Road",
        "Suite 3",
        "NY",
        "10001",
    )
    assert site["position"] == {"latitude": 40.75, "longitude": -73.99}
    assert (site_row["latitude"], site_row["longitude"]) == ("40.75", "-73.99")
    practitioner = parse_fhir_resource("example", doctor)[1]
    assert (practitioner["npi"], practitioner["family_name"], practitioner["given_names"]) == (
        1234567893,
        "Example",
        ["Alex"],
    )
    membership = parse_fhir_resource("example", last_role)[1]
    assert membership["practitioner_ref"] == "Practitioner/vsp-npi-1234567893"
    assert membership["location_refs"] == ["Location/vsp-office-52"]
    assert membership["network_refs"] == ["Organization/vsp-network-1000", "Organization/vsp-network-4000"]
    assert (
        membership["specialty_codes"] == [] and membership["period_start"] is None and membership["period_end"] is None
    )
    networks = vsp_network_organizations()
    assert [(org["identifier"][0], org["name"]) for org in networks] == [
        ({"system": "urn:vsp:network-id", "value": code}, name)
        for code, name in [("1000", "VSP Signature"), ("2000", "VSP Choice"), ("4000", "VSP Advantage")]
    ]
    batch = _catalog_batch((*networks, first_role, last_role))
    assert all(org.declared_network for org in batch.organizations) and len(batch.organizations) == 3
    assert [(edge.owner_resource_id, edge.network_resource_id, edge.plan_resource_id) for edge in batch.edges] == [
        (first_role["id"], "vsp-network-2000", None),
        (last_role["id"], "vsp-network-1000", None),
        (last_role["id"], "vsp-network-4000", None),
    ]


def test_outside_profiles_keep_unknown_codes_without_membership():
    resources = normalize_vsp_office(office({"pid": 3}), profiles=[profile(3, "other"), profile(4, "Choice")])
    assert resources == []
    assert resources.outside_profile_count == 2 and resources.outside_summary_count == 1
    assert resources.other_network_ids == ("Choice", "other")


def test_printable_padding_is_normalized_without_changing_source_profiles():
    profile_by_field = {
        **profile(2, " 2000 ", " OTHER "),
        "providerNetworks": [{"networkId": "2000"}, {"networkId": "OTHER"}],
    }
    resources = normalize_vsp_office(office({"pid": 2, "npi": "1234567893"}), profiles=[profile_by_field])
    assert resources[-1]["extension"][0]["valueReference"]["reference"] == "Organization/vsp-network-2000"
    assert resources.other_network_ids == ("OTHER",)
    assert profile_by_field["networkIds"] == [" 2000 ", " OTHER "]


@pytest.mark.parametrize("code", ["", "   ", "\t2000", "2000\n", "OTHER\x00", 2000])
def test_network_padding_does_not_accept_empty_control_or_nontext_codes(code):
    with pytest.raises(VSPDirectoryNormalizationError, match="^vsp_directory_invalid_network_ids$"):
        normalize_vsp_office(office({"pid": 2}), profiles=[profile(2, code)])


def test_padding_does_not_hide_conflicting_network_fields():
    profile_by_field = {**profile(2, " OTHER "), "providerNetworks": [{"networkId": "DIFFERENT"}]}
    with pytest.raises(VSPDirectoryNormalizationError, match="^vsp_directory_conflicting_network_ids$"):
        normalize_vsp_office(office({"pid": 2}), profiles=[profile_by_field])


@pytest.mark.parametrize("location_type,codes,expected", [("Affiliate", ["2000"], "2000"), ("VSP", ["OTHER"], None)])
def test_search_evidence_does_not_override_canonical_or_nonaffiliate_profiles(location_type, codes, expected):
    office_by_field = {**office({"pid": 2, "npi": "1234567893"}), "locationType": location_type}
    resources = normalize_vsp_office(
        office_by_field, profiles=[profile(2, *codes)], search_memberships={"Signature": {"2"}}
    )
    assert [
        row["valueReference"]["reference"]
        for resource in resources
        if resource["resourceType"] == "PractitionerRole"
        for row in resource["extension"]
    ] == ([] if expected is None else ["Organization/vsp-network-" + expected])


@pytest.mark.parametrize(
    ("summaries", "profiles", "reason"),
    [
        ([], [profile(2, "2000")], "unresolved_doctor"),
        ([{"pid": 2, "npi": "1234567893"}], [], "unresolved_doctor"),
        ([{"pid": 2, "npi": 1234567893}], [profile(2, "2000")], "invalid_npi"),
        ([{"pid": 2, "npi": "1234567893"}], [{**profile(2, "2000"), "npi": "1999999992"}], "conflicting_npi"),
        (
            [{"pid": 2, "npi": "1234567893"}, {"pid": 3, "npi": "1234567893"}],
            [profile(2, "2000"), profile(3, "other")],
            "conflicting_npi",
        ),
        ([{"pid": 2}, {"pid": 2}], [profile(2, "2000")], "duplicate_doctor_id"),
        ([{"pid": 2}], [profile(2, "2000"), profile(2, "4000")], "duplicate_doctor_id"),
        ([{"pid": 2}], [{"doctorPublicId": 2, "networkIds": [2000]}], "invalid_network_ids"),
        ([{"pid": 2}], [{"doctorPublicId": 2}], "invalid_network_ids"),
        ([{"pid": 2}], [{**profile(2, "2000"), "providerNetworks": ["2000"]}], "invalid_network_ids"),
        (
            [{"pid": 2}],
            [{**profile(2, "2000"), "providerNetworks": [{"networkId": "1000"}]}],
            "conflicting_network_ids",
        ),
        ([{"pid": True}], [profile(1, "2000")], "invalid_public_id"),
    ],
)
def test_unproven_membership_refuses_the_whole_office(summaries, profiles, reason):
    with pytest.raises(VSPDirectoryNormalizationError, match=f"^vsp_directory_{reason}$"):
        normalize_vsp_office(office(*summaries), profiles=profiles)


@pytest.mark.parametrize("npi", [None, "", "1234567890", "not-an-npi"])
def test_source_doctor_identity_preserves_membership_without_an_invalid_npi(npi):
    summary_by_field = {"pid": 2, "npi": npi, "first": "Alex", "last": "Example"}
    resources = normalize_vsp_office(office(summary_by_field), profiles=[profile(2, "2000")])
    _, practitioner, role = resources
    assert practitioner["id"] == "vsp-doctor-2"
    assert practitioner["identifier"] == [{"system": "urn:vsp:doctor-id", "value": "2"}]
    assert parse_fhir_resource("example", practitioner)[1]["npi"] is None
    membership = parse_fhir_resource("example", role)[1]
    assert membership["practitioner_ref"] == "Practitioner/vsp-doctor-2"
    assert membership["network_refs"] == ["Organization/vsp-network-2000"]
    assert summary_by_field["npi"] == npi


@pytest.mark.parametrize("coordinate", [float("nan"), float("inf"), -90.1, True, "40.75", None])
def test_invalid_position_is_not_published(coordinate):
    capture = office({"pid": 2, "npi": "1234567893"})
    capture["addr"]["latitude"] = coordinate
    with pytest.raises(VSPDirectoryNormalizationError, match="^vsp_directory_invalid_position$"):
        normalize_vsp_office(capture, profiles=[profile(2, "2000")])


@pytest.mark.parametrize(
    "replacement,reason",
    [
        ({"providerSummaries": None}, "invalid_doctor_list"),
        ({"addr": None}, "invalid_address"),
        ({"addr": {"city": "Example City"}}, "invalid_address"),
        ({"phone": 1234}, "invalid_text"),
    ],
)
def test_malformed_office_cannot_publish_a_role(replacement, reason):
    office_by_field = {**office({"pid": 2, "npi": "1234567893"}), **replacement}
    with pytest.raises(VSPDirectoryNormalizationError, match=f"^vsp_directory_{reason}$"):
        normalize_vsp_office(office_by_field, profiles=[profile(2, "2000")])
