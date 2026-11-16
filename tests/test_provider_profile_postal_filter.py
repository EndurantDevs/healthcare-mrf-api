# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source thresholds suppress uncounted postal facts without changing qualified offices."""

import json
from copy import deepcopy
from unittest.mock import AsyncMock

import pytest

from api import provider_batch
from api.endpoint import npi
from api.provider_profile import _without_uncounted_postal_addresses
from tests import test_npi_location_paging as paging
from tests.provider_profile_snapshot_test_support import (
    stub_provider_profile_snapshot as stub_provider_profile_snapshot,
)

NPI = 1234567890
POSTAL_SUMMARY_FIELDS = ("ffs_practice_cities", "ffs_practice_states", "ffs_practice_zip_codes")


def _enrollment_summary():
    row = [None] * 27
    row[0], row[1], row[2], row[3], row[5] = NPI, 2025, "active", True, True
    row[12], row[16] = "IL", ["example-enrollment"]
    row[20], row[21], row[22] = ["60001"], ["Example City"], ["IL"]
    return npi._provider_enrichment_summary_from_row(row)


def _provider_payload(source_count):
    provider = paging._detail_payload()
    for address in provider["address_list"]:
        address["source_count"] = source_count
    provider["endpoint"] = {"address": "https://example.invalid/fhir"}
    provider["provider_organization_name"] = "Example Practice"
    return provider


def _assert_enrollment_summary(actual, original, minimum):
    expected_by_field = dict(original)
    if minimum:
        expected_by_field.update(dict.fromkeys(POSTAL_SUMMARY_FIELDS, []))
    assert actual == npi._public_provider_enrichment_summary(expected_by_field)


def test_standalone_enrollment_summary_keeps_nonpostal_facts_and_input():
    summary = _enrollment_summary()
    before = deepcopy(summary)

    filtered = _without_uncounted_postal_addresses(summary)

    assert filtered == {**summary, **dict.fromkeys(POSTAL_SUMMARY_FIELDS, [])}
    assert summary == before


@pytest.mark.parametrize("street_field", ["address_line_1", "first_line"])
def test_uncounted_postal_aliases_are_removed_outside_qualified_office_trees(street_field):
    office_by_field = {
        street_field: "100 Example Avenue",
        "second_line": "Suite 100",
        "city_name": "Example City",
        "state_name": "Illinois",
        "state_code": "IL",
        "postal_code": "60001",
        "country_code": "US",
        "formatted_address": "100 Example Avenue, Example City, IL 60001",
        "lat": 41.0,
        "long": -87.0,
        "source_count": 3,
    }
    payload_by_field = {
        "provider_enrichment": {"enrollments": {"ffs_public": [{**office_by_field, "enrollment_id": "example"}]}},
        "address_list": [office_by_field],
        "address_groups": [{"members": [office_by_field]}],
        "endpoint": {"address": "https://example.invalid/fhir"},
        "license": {"state": "IL", "status": "active"},
    }
    before = deepcopy(payload_by_field)

    filtered = _without_uncounted_postal_addresses(payload_by_field)

    assert filtered["provider_enrichment"]["enrollments"]["ffs_public"] == [
        {"source_count": 3, "enrollment_id": "example"}
    ]
    for field in ("address_list", "address_groups", "endpoint", "license"):
        assert filtered[field] == payload_by_field[field]
    filtered["address_list"][0]["city_name"] = "Changed"
    assert payload_by_field == before


def test_qualified_office_preserves_its_postal_fields_but_filters_nested_source_contacts():
    contact_by_field = {
        "system": "phone",
        "value": "5550100",
        "address": {"line": ["200 Example Avenue"], "city": "Other City", "state": "IL"},
    }
    role = npi._provider_directory_role_detail(
        {
            "source_id": "example",
            "resource_id": "role-1",
            "role_endpoints": [
                {
                    "source_id": "example",
                    "resource_id": "endpoint-1",
                    "address": "https://example.invalid/fhir",
                    "contact": [contact_by_field],
                }
            ],
        }
    )
    office_by_field = {
        **_provider_payload(3)["address_list"][0],
        "provider_directory_sources": [{"practitioner_roles": [role]}],
    }
    payload_by_field = {"address_list": [office_by_field], "address_groups": [{"members": [office_by_field]}]}
    before = deepcopy(payload_by_field)

    filtered = _without_uncounted_postal_addresses(payload_by_field)

    for address in (filtered["address_list"][0], filtered["address_groups"][0]["members"][0]):
        assert address["first_line"] == office_by_field["first_line"]
        assert address["city_name"] == office_by_field["city_name"]
        endpoint = address["provider_directory_sources"][0]["practitioner_roles"][0]["endpoints"][0]
        assert endpoint["address"] == "https://example.invalid/fhir"
        assert endpoint["contact"] == [{"system": "phone", "value": "5550100"}]
    assert payload_by_field == before


@pytest.mark.asyncio
@pytest.mark.parametrize(("minimum", "source_count"), [(None, 2), (0, 2), (3, 2), (3, 3)])
async def test_detail_suppresses_uncounted_enrollment_locations_after_office_filtering(
    monkeypatch, minimum, source_count
):
    provider = _provider_payload(source_count)
    monkeypatch.setattr(paging, "_detail_payload", lambda: deepcopy(provider))
    paging._install_route_mocks(monkeypatch)
    summary = _enrollment_summary()
    monkeypatch.setattr(
        npi,
        "_fetch_provider_enrichment_summary_detail",
        AsyncMock(return_value={"summary": npi._public_provider_enrichment_summary(summary), "ffs_visibility": {}}),
    )
    request = paging._detail_request(limit="5")
    if minimum is not None:
        request.args["min_address_sources"] = str(minimum)

    response = await npi.read_npi_detail(request, str(NPI))
    provider_result_by_field = json.loads(response.body)

    assert response.status == 200 and provider_result_by_field["npi"] == NPI
    _assert_enrollment_summary(provider_result_by_field["provider_enrichment"]["summary"], summary, minimum)
    assert provider_result_by_field["endpoint"] == provider["endpoint"]
    assert provider_result_by_field["provider_organization_name"] == "Example Practice"
    if minimum and source_count < minimum:
        assert provider_result_by_field["address_list"] == []
        assert provider_result_by_field["address_pagination"]["total"] == 0
    else:
        assert provider_result_by_field["address_list"]
        assert all(address["city_name"] == "Example City" for address in provider_result_by_field["address_list"])
        assert provider_result_by_field["address_list"][0]["first_line"]


@pytest.mark.asyncio
@pytest.mark.parametrize(("minimum", "source_count"), [(None, 2), (0, 2), (3, 2), (3, 3)])
async def test_batch_suppresses_uncounted_enrollment_locations_after_office_filtering(
    monkeypatch, minimum, source_count
):
    provider = _provider_payload(source_count)
    summary = _enrollment_summary()
    monkeypatch.setattr(npi, "_build_npi_identity_details_map", AsyncMock(return_value={NPI: provider}))
    monkeypatch.setattr(
        npi, "_fetch_npi_location_candidates_map", AsyncMock(return_value={NPI: provider["address_list"]})
    )
    monkeypatch.setattr(npi, "_fetch_provider_directory_address_overlay_map", AsyncMock(return_value={}))
    monkeypatch.setattr(npi, "_apply_location_statuses", AsyncMock())
    monkeypatch.setattr(npi, "_fetch_other_names_map", AsyncMock(return_value={}))
    monkeypatch.setattr(npi, "_fetch_provider_enrichment_summary_map", AsyncMock(return_value={NPI: summary}))
    monkeypatch.setattr(npi, "_project_provider_network_metadata", AsyncMock(side_effect=lambda rows, **_: rows))
    params = npi._normalize_npi_batch_request(
        {"npis": [NPI], **({"min_address_sources": minimum} if minimum is not None else {})}
    )

    payload_by_field = await provider_batch.build_native_batch_payload(params)
    provider_result_by_field = payload_by_field["items"][0]["provider"]

    assert payload_by_field["found"] == 1 and payload_by_field["not_found"] == 0
    _assert_enrollment_summary(provider_result_by_field["provider_enrichment"]["summary"], summary, minimum)
    assert provider_result_by_field["endpoint"] == provider["endpoint"]
    if minimum and source_count < minimum:
        assert provider_result_by_field["address_list"] == []
        assert provider_result_by_field["address_pagination"]["total"] == 0
    else:
        assert len(provider_result_by_field["address_list"]) == 2
        assert all(address["city_name"] == "Example City" for address in provider_result_by_field["address_list"])
        assert provider_result_by_field["address_list"][0]["first_line"]
