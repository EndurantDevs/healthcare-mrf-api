# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Deterministic source counts preserve explicit network and office evidence."""

from tests.cms_npd_admission_postgres_support import fhir
from tests.provider_directory_rooted_synthetic_support import rooted_source_resources


def test_eight_families_keep_duplicate_root_and_outside_cohort_distinct():
    resources = rooted_source_resources()
    assert set(resources) == {
        "Practitioner",
        "PractitionerRole",
        "Organization",
        "OrganizationAffiliation",
        "Location",
        "InsurancePlan",
        "HealthcareService",
        "Endpoint",
    }
    npis = [fhir._resource_npi(row, source_id="synthetic-source") for row in resources["Practitioner"]]
    assert npis == [1234567893, 1234567893, 1000000491]
    assert len(resources["OrganizationAffiliation"]) == 795
    assert len(resources["PractitionerRole"]) == 23
    assert {row["id"] for row in resources["Location"]} == {"site-2", "site-3"}
    assert len({row["id"] for rows in resources.values() for row in rows}) == sum(map(len, resources.values()))


def test_shared_parser_preserves_two_networks_and_only_the_asserted_office():
    resources = rooted_source_resources()
    normalized_rows = [
        parser(row, {"source_id": "synthetic-source"})[1]
        for kind, parser in (
            ("OrganizationAffiliation", fhir._parse_organization_affiliation_resource),
            ("PractitionerRole", fhir._parse_practitioner_role_resource),
        )
        for row in resources[kind]
    ]
    assert {tuple(row["network_refs"]) for row in normalized_rows} == {
        ("Organization/network-new",),
        ("Organization/network-old",),
    }
    assert {tuple(row["location_refs"]) for row in normalized_rows} == {("Location/site-2",)}
    assert sum(row["network_refs"] == ["Organization/network-new"] for row in normalized_rows) == 410
    assert sum(row["network_refs"] == ["Organization/network-old"] for row in normalized_rows) == 408
