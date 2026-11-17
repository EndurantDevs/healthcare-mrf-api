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


@pytest.mark.parametrize("summaries", [[], [{"pid": 2, "npi": None}]])
def test_profile_npi_is_retained_when_summary_has_no_npi(summaries):
    profile_by_field = {**profile(2, "2000"), "npi": "1234567893", "firstName": "Example", "lastName": "Clinician"}
    resources = normalize_vsp_office(office(*summaries), profiles=[profile_by_field])
    practitioner = next(resource for resource in resources if resource["resourceType"] == "Practitioner")
    assert practitioner["id"] == "vsp-npi-1234567893"
    assert practitioner["identifier"] == [
        {"system": "http://hl7.org/fhir/sid/us-npi", "value": "1234567893"},
        {"system": "urn:vsp:doctor-id", "value": "2"},
    ]
    role = next(resource for resource in resources if resource["resourceType"] == "PractitionerRole")
    assert role["practitioner"]["reference"] == "Practitioner/vsp-npi-1234567893"


@pytest.mark.parametrize("summary_backed_duplicate", [False, True])
def test_profile_npi_duplicates_are_checked_across_both_identity_sources(summary_backed_duplicate):
    summaries = [{"pid": 3, "npi": "1234567893"}] if summary_backed_duplicate else []
    other = profile(3, "2000") if summary_backed_duplicate else {**profile(3, "2000"), "npi": "1234567893"}
    details = [{**profile(2, "2000"), "npi": "1234567893"}, other]
    with pytest.raises(VSPDirectoryNormalizationError, match="^vsp_directory_conflicting_npi$"):
        normalize_vsp_office(office(*summaries), profiles=details)


def test_canonical_profile_without_summary_preserves_source_identity_and_name():
    profile_dict = {**profile(2, "2000"), "firstName": "Alex", "lastName": "Example"}
    resources = normalize_vsp_office(office(), profiles=[profile_dict])
    assert resources.outside_profile_count == resources.outside_summary_count == 0
    site, doctor, role = resources
    assert doctor["id"] == "vsp-doctor-2"
    assert doctor["identifier"] == [{"system": "urn:vsp:doctor-id", "value": "2"}]
    assert doctor["name"] == [{"family": "Example", "given": ["Alex"]}]
    assert parse_fhir_resource("example", doctor)[1]["npi"] is None
    assert role["practitioner"]["reference"] == "Practitioner/vsp-doctor-2"
    assert role["location"][0]["reference"] == "Location/" + site["id"]
    assert role["extension"][0]["valueReference"]["reference"] == "Organization/vsp-network-2000"


def test_summary_only_doctor_requires_direct_scope_and_preserves_source_identity():
    summary_by_field = {"pid": 2, "npi": "1234567893", "first": "Alex", "last": "Example"}
    resources = normalize_vsp_office(office(summary_by_field), profiles=[], search_memberships={"Signature": {"2"}})
    site, doctor, role = resources
    assert doctor["id"] == "vsp-npi-1234567893" and doctor["name"] == [{"family": "Example", "given": ["Alex"]}]
    assert parse_fhir_resource("example", doctor)[1]["npi"] == 1234567893
    assert role["location"] == [{"reference": "Location/" + site["id"]}]
    assert role["extension"] == [
        {
            "url": "http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/network-reference",
            "valueReference": {"reference": "Organization/vsp-network-1000"},
        }
    ]
    assert resources.outside_profile_count == resources.outside_summary_count == 0


@pytest.mark.parametrize(
    ("npi", "scope"),
    [
        (None, {"2"}),
        ("bad", {"2"}),
        (" 1234567893 ", {"2"}),
        (1234567893, {"2"}),
        ("1234567893", set()),
        ("1234567893", {"3"}),
    ],
)
def test_summary_only_doctor_without_identity_or_direct_scope_is_rejected(npi, scope):
    with pytest.raises(VSPDirectoryNormalizationError, match="^vsp_directory_unresolved_doctor$"):
        normalize_vsp_office(office({"pid": 2, "npi": npi}), profiles=[], search_memberships={"Signature": scope})


def retail_office_and_profile():
    site_dict = {
        **office(),
        "name": "Example Optical Store",
        "locationType": "Affiliate",
        "serviceTypeCd": "DL",
        "parentCompanyCd": "EXAMPLE",
        "droStoreNumber": "1",
    }
    profile_dict = {
        **profile(9, "VWRK ", "VWRKQ"),
        "firstName": "Example",
        "lastName": "Optical Store",
        "doctorDegreeCode": "NA",
        "serviceTypeCode": "DL",
    }
    return site_dict, profile_dict


def company_office_fixture(office_id=52, service="EC"):
    site, display = retail_office_and_profile()
    site.update(
        pid=office_id,
        name="Pearle Vision " + ("42" if service == "EC" else "Q1"),
        locationType="VSP",
        serviceTypeCd=service,
        parentCompanyCd="PERL",
        droStoreNumber="Q1",
    )
    display.update(
        profile(9, "1000", "2000", "4000"), firstName="PEARLE", lastName="VISION 42", serviceTypeCode=service
    )
    summary_dict = {"pid": 9, "degree": "NA", "first": "Pearle", "last": "Vision 42", "npi": "1999999992"}
    site["providerSummaries"] = [summary_dict] if service in {"EC", "FS"} else []
    return site, display, summary_dict


def multi_company_office_fixture():
    site, display, summary = company_office_fixture(service="FS")
    site["name"] = "Independent Optical"
    site["providerSummaries"].extend(
        [
            {**summary, "pid": 10, "npi": "1000000004"},
            {"pid": 2, "npi": "1234567893", "first": "Alex", "last": "Example"},
        ]
    )
    profiles = [
        display,
        {**display, "doctorPublicId": 10},
        {**profile(2, "2000"), "serviceTypeCode": "FS", "doctorDegreeCode": "OD"},
    ]
    return site, profiles


def test_summary_only_companies_preserve_distinct_organization_npis():
    site, _profiles = multi_company_office_fixture()
    resources = normalize_vsp_office(
        site, profiles=[], retail_networks={"Signature"}, search_memberships={"Signature": {"2", "9", "10"}}
    )
    by_id = {resource["id"]: resource for resource in resources}
    assert by_id["vsp-site-company-52-9"]["identifier"][1]["value"] == "1999999992"
    assert by_id["vsp-site-company-52-10"]["identifier"][1]["value"] == "1000000004"
    assert len([resource for resource in resources if resource["resourceType"] == "Practitioner"]) == 1


def test_summary_only_company_rejects_a_conflicting_retained_npi():
    site, _profiles = multi_company_office_fixture()
    retained_summary_by_field = {**site["providerSummaries"][0], "npi": "1000000004"}
    with pytest.raises(VSPDirectoryNormalizationError, match="^vsp_directory_unresolved_doctor$"):
        normalize_vsp_office(
            site,
            profiles=[],
            retail_networks={"Signature"},
            search_memberships={"Signature": {"2", "9", "10"}},
            company_summaries_by_doctor={9: retained_summary_by_field},
        )


def test_distinct_company_npis_share_a_site_without_inheriting_network_membership():
    site, profiles = multi_company_office_fixture()
    resources = normalize_vsp_office(site, profiles=profiles, retail_networks={"Signature"})
    assert len(resources) == 9
    by_id = {resource["id"]: resource for resource in resources}
    assert by_id["vsp-site-org-52"]["name"] == "Independent Optical"
    assert by_id["vsp-site-org-52"]["identifier"] == [{"system": "urn:vsp:office-id", "value": "52"}]
    assert by_id["vsp-site-network-52"]["network"] == [{"reference": "Organization/vsp-network-1000"}]
    for did, npi in ((9, "1999999992"), (10, "1000000004")):
        company = by_id[f"vsp-site-company-52-{did}"]
        assert company["identifier"] == [
            {"system": "urn:vsp:doctor-id", "value": str(did)},
            {"system": "http://hl7.org/fhir/sid/us-npi", "value": npi},
        ]
        assert company["name"] == "PEARLE VISION 42"
        assert parse_fhir_resource("example", company)[1]["npi"] == int(npi)
        affiliation = parse_fhir_resource("example", by_id[f"vsp-site-company-link-52-{did}"])[1]
        assert affiliation["organization_ref"] == "Organization/vsp-site-org-52"
        assert affiliation["participating_organization_ref"] == f"Organization/vsp-site-company-52-{did}"
        assert affiliation["location_refs"] == ["Location/vsp-office-52"]
        assert affiliation["network_refs"] == []
    assert [resource["id"] for resource in resources if resource["resourceType"] == "Practitioner"] == [
        "vsp-npi-1234567893"
    ]
    assert by_id["vsp-role-2-52-1234567893"]["extension"][0]["valueReference"] == {
        "reference": "Organization/vsp-network-2000"
    }


def test_distinct_companies_still_reject_a_conflicting_npi_for_one_profile():
    site, profiles = multi_company_office_fixture()
    conflicting_summary_dict = {**site["providerSummaries"][0], "npi": "1000000004"}
    with pytest.raises(VSPDirectoryNormalizationError, match="^vsp_directory_conflicting_npi$"):
        normalize_vsp_office(
            site,
            profiles=profiles,
            retail_networks={"Signature"},
            company_summaries_by_doctor={9: conflicting_summary_dict},
        )


@pytest.mark.parametrize("service", ["EC", "DL", "FS"])
def test_company_npi_is_an_organization_identifier_with_only_site_membership(service):
    site, display, summary = company_office_fixture(service=service)
    clinician_dict = {**profile(2, "2000"), "serviceTypeCode": service, "doctorDegreeCode": "OD"}
    site["providerSummaries"].append({"pid": 2, "npi": "1234567893", "first": "Alex", "last": "Example"})
    resources = normalize_vsp_office(
        site,
        profiles=[display, clinician_dict],
        retail_networks={"Signature"},
        company_summaries_by_doctor={9: summary},
    )
    location, organization, affiliation, person, role = resources
    assert organization["id"] == "vsp-site-org-52" and organization["name"] == site["name"]
    assert organization["identifier"] == [
        {"system": "urn:vsp:office-id", "value": "52"},
        {"system": "http://hl7.org/fhir/sid/us-npi", "value": "1999999992"},
    ]
    assert parse_fhir_resource("example", organization)[1]["npi"] == 1999999992
    assert affiliation["network"] == [{"reference": "Organization/vsp-network-1000"}]
    assert location["managingOrganization"] == affiliation["participatingOrganization"]
    assert person["id"] == "vsp-npi-1234567893" and role["id"] == "vsp-role-2-52-1234567893"
    assert role["extension"][0]["valueReference"] == {"reference": "Organization/vsp-network-2000"}


@pytest.mark.parametrize("damage", ["clinical", "name", "npi_type", "npi_checksum", "npi_conflict"])
def test_company_identity_rejects_ambiguous_or_clinical_evidence(damage):
    site, display, summary = company_office_fixture()
    incoming_summary_dict = dict(summary)
    if damage == "clinical":
        incoming_summary_dict["degree"] = "OD"
    if damage == "name":
        incoming_summary_dict["last"] = "Vision 43"
    if damage == "npi_type":
        incoming_summary_dict["npi"] = 1999999992
    if damage == "npi_checksum":
        incoming_summary_dict["npi"] = "1999999991"
    if damage == "npi_conflict":
        incoming_summary_dict["npi"] = "1234567893"
    with pytest.raises(VSPDirectoryNormalizationError, match="vsp_directory_(unresolved_doctor|conflicting_npi)"):
        normalize_vsp_office(
            site,
            profiles=[display],
            retail_networks={"Signature"},
            company_summaries_by_doctor={9: incoming_summary_dict},
        )


@pytest.mark.parametrize(("suffix", "location_type"), [("", "Affiliate"), (" INC", "Affiliate"), (" 1", "VSP")])
def test_retail_display_preserves_site_networks_without_a_practitioner(suffix, location_type):
    site, display = retail_office_and_profile()
    site["name"] += suffix
    site["locationType"] = location_type
    resources = normalize_vsp_office(site, profiles=[display], retail_networks={"Signature", "Choice"})
    assert [x["resourceType"] for x in resources] == ["Location", "Organization", "OrganizationAffiliation"]
    location, organization, affiliation = resources
    assert resources.outside_profile_count == 1 and resources.outside_summary_count == 0
    assert organization["id"] == "vsp-site-org-52"
    assert organization["identifier"] == [{"system": "urn:vsp:office-id", "value": "52"}]
    assert location["managingOrganization"] == affiliation["participatingOrganization"]
    assert affiliation["location"] == [{"reference": "Location/vsp-office-52"}]
    assert affiliation["network"] == [
        {"reference": "Organization/vsp-network-1000"},
        {"reference": "Organization/vsp-network-2000"},
    ]
    parsed = parse_fhir_resource("example", affiliation)[1]
    assert parsed["participating_organization_ref"] == "Organization/vsp-site-org-52"
    assert parsed["location_refs"] == ["Location/vsp-office-52"]
    assert parsed["network_refs"] == [entry["reference"] for entry in affiliation["network"]]
    assert display["networkIds"] == ["VWRK ", "VWRKQ"] and site["providerSummaries"] == []


@pytest.mark.parametrize(
    ("service", "office_name", "profile_name"),
    [
        ("FS", "Example Optical LLC", ("EXAMPLE", "OPTICAL")),
        ("FS", "Current Store", ("Earlier", "Company Name")),
        ("EC", "Independent Optical", ("Optical", "Example")),
        ("DL", "Example Optical #6", ("Example", "Optical 31")),
        ("DL", "Current Brand", ("Earlier", "Brand")),
    ],
)
def test_source_typed_company_keeps_bound_office_and_explicit_site_networks(service, office_name, profile_name):
    site, display = retail_office_and_profile()
    site.update(name=office_name, serviceTypeCd=service, droStoreNumber="STORE006")
    display.update(
        firstName=profile_name[0],
        lastName=profile_name[1],
        serviceTypeCode=service,
        networkIds=["1000", "2000", "4000"],
        providerNetworks=[{"networkId": code} for code in ("1000", "2000", "4000")],
    )
    resources = normalize_vsp_office(site, profiles=[display], retail_networks={"Signature"})
    assert [resource["resourceType"] for resource in resources] == [
        "Location",
        "Organization",
        "OrganizationAffiliation",
    ]
    assert resources[0]["id"] == "vsp-office-52" and resources[0]["name"] == office_name
    assert resources[1]["id"] == "vsp-site-org-52"
    assert resources[1]["identifier"] == [{"system": "urn:vsp:office-id", "value": "52"}]
    assert resources[-1]["network"] == [{"reference": "Organization/vsp-network-1000"}]
    with pytest.raises(VSPDirectoryNormalizationError, match="^vsp_directory_unresolved_doctor$"):
        normalize_vsp_office(site, profiles=[display], retail_networks=set())


@pytest.mark.parametrize("npi", [None, "", "invalid", " 1999999992 ", 1999999992])
def test_source_company_summary_retains_missing_npi_and_refuses_invalid_values(npi):
    site, display, summary = company_office_fixture(service="FS")
    site.update(name="Example Vision LLC", parentCompanyCd="EXAMPLE")
    display.update(firstName="EXAMPLE", lastName="VISION")
    summary.update(first="Example", last="Vision", npi=npi)
    if npi is not None:
        with pytest.raises(VSPDirectoryNormalizationError, match="^vsp_directory_unresolved_doctor$"):
            normalize_vsp_office(site, profiles=[display], retail_networks={"Signature"})
        return
    resources = normalize_vsp_office(site, profiles=[display], retail_networks={"Signature"})
    assert len(resources) == 3
    assert resources[1]["identifier"] == [{"system": "urn:vsp:office-id", "value": "52"}]
    resources = normalize_vsp_office(
        site,
        profiles=[display],
        retail_networks={"Signature"},
        company_summaries_by_doctor={9: {**summary, "npi": "1999999992"}},
    )
    assert resources[1]["identifier"][-1] == {"system": "http://hl7.org/fhir/sid/us-npi", "value": "1999999992"}


@pytest.mark.parametrize("identity_evidence", ["npi", "summary"])
@pytest.mark.parametrize("display_suffix", ["", " 9001"])
def test_company_display_does_not_override_clinician_identity(identity_evidence, display_suffix):
    site, display = retail_office_and_profile()
    site.update(name="Pearle Vision Example Plaza", parentCompanyCd="PERL")
    display.update(firstName="Pearle", lastName="Vision" + display_suffix)
    if identity_evidence == "npi":
        display["npi"] = "1234567893"
    else:
        site["providerSummaries"] = [{"pid": 9, "npi": "1234567893"}]
    with pytest.raises(VSPDirectoryNormalizationError, match="^vsp_directory_unresolved_doctor$"):
        normalize_vsp_office(site, profiles=[display], retail_networks={"Signature"})


@pytest.mark.parametrize(
    "damage", ["degree", "service", "npi", "summary", "display", "store", "parent", "name", "network"]
)
def test_retail_mapping_does_not_accept_clinician_or_unproven_evidence(damage):
    site, display = retail_office_and_profile()
    networks = {"Signature"}
    if damage == "degree":
        display["doctorDegreeCode"] = "OD"
    if damage == "service":
        display["serviceTypeCode"] = "FS"
    if damage == "npi":
        display["npi"] = "1234567893"
    if damage == "summary":
        site["providerSummaries"] = [{"pid": 9, "npi": "1234567893"}]
    if damage == "display":
        display.update(firstName="", lastName="")
    if damage == "store":
        site["droStoreNumber"] = ""
    if damage == "parent":
        site["parentCompanyCd"] = ""
    if damage == "name":
        site["name"] = ""
    if damage == "network":
        networks = {"Unproven"}
    with pytest.raises(VSPDirectoryNormalizationError, match="^vsp_directory_unresolved_doctor$"):
        normalize_vsp_office(site, profiles=[display], retail_networks=networks)


def test_mixed_store_and_clinician_share_one_location_with_separate_networks():
    site, display = retail_office_and_profile()
    site["providerSummaries"] = [{"pid": 2, "npi": "1234567893"}]
    clinician_dict = {
        **profile(2, "2000"),
        "firstName": "Alex",
        "lastName": "Example",
        "serviceTypeCode": "DL",
        "doctorDegreeCode": "OD",
    }
    resources = normalize_vsp_office(site, profiles=[display, clinician_dict], retail_networks={"Signature"})
    assert [row["resourceType"] for row in resources] == [
        "Location",
        "Organization",
        "OrganizationAffiliation",
        "Practitioner",
        "PractitionerRole",
    ]
    location, organization, affiliation, doctor, role = resources
    assert doctor["id"] == "vsp-npi-1234567893"
    assert role["location"] == affiliation["location"] == [{"reference": "Location/" + location["id"]}]
    assert affiliation["network"] == [{"reference": "Organization/vsp-network-1000"}]
    assert role["extension"][0]["valueReference"] == {"reference": "Organization/vsp-network-2000"}
    assert location["managingOrganization"] == {"reference": "Organization/" + organization["id"]}
    assert resources.outside_profile_count == 1 and resources.outside_summary_count == 0
