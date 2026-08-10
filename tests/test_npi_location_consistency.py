# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Cross-page provider-location identity and projection regressions."""

from copy import deepcopy
import json
from unittest.mock import AsyncMock

import pytest

from api.endpoint import npi as npi_module
from tests.test_npi_location_paging import (
    ADDRESS_A,
    ADDRESS_B,
    SITE_A,
    _address,
    _detail_request,
    _get_page,
)


def test_directory_city_alias_merges_into_only_concrete_site():
    concrete_address = _address(
        ADDRESS_A,
        "100 Example Avenue",
        source_id="nppes",
        premise_key=SITE_A,
    )
    directory_alias_map = {
        "npi": 1234567890,
        "address_key": ADDRESS_A,
        "type": "practice",
        "first_line": "100 Example Avenue",
        "city_name": "Example Township",
        "state_name": "IL",
        "postal_code": "60001",
        "address_sources": ["provider_directory_fhir"],
        "source_record_ids": ["synthetic:directory:location"],
        "location_status": "active",
    }

    deduped = npi_module._dedupe_addresses_by_key(
        [concrete_address, directory_alias_map]
    )

    assert len(deduped) == 1
    assert deduped[0]["premise_key"] == SITE_A
    assert deduped[0]["address_sources"] == [
        "nppes",
        "provider_directory_fhir",
    ]
    assert deduped[0]["location_status"] == "active"


def test_hydrated_unsited_location_stays_distinct_on_display_mismatch():
    concrete_address = _address(
        ADDRESS_A,
        "100 Example Avenue",
        source_id="nppes",
        premise_key=SITE_A,
    )
    hydrated_unsited_address = _address(
        ADDRESS_A,
        "900 Other Avenue",
        source_id="provider_directory_fhir",
    )

    deduped = npi_module._dedupe_addresses_by_key(
        [concrete_address, hydrated_unsited_address]
    )

    assert len(deduped) == 2


def test_distinct_concrete_sites_keep_scoped_evidence():
    second_site = "10000000-0000-0000-0000-000000000002"
    first_address = _address(
        ADDRESS_A,
        "100 Example Avenue",
        source_id="synthetic_source_a",
        premise_key=SITE_A,
    )
    second_address = _address(
        ADDRESS_A,
        "100 Example Avenue",
        source_id="synthetic_source_b",
        premise_key=second_site,
    )

    deduped = npi_module._dedupe_addresses_by_key(
        [first_address, second_address]
    )

    source_by_site = {
        address["premise_key"]: address["address_sources"]
        for address in deduped
    }
    assert source_by_site == {
        SITE_A: ["synthetic_source_a"],
        second_site: ["synthetic_source_b"],
    }


def test_hydration_keeps_primary_identity_metadata_correlated():
    selected_location_map = _address(
        ADDRESS_A,
        "100 Example Avenue",
        source_id="synthetic_candidate",
        premise_key=SITE_A,
    )
    selected_location_map.update(
        {
            "location_key": "location-z",
            "_base_row_identities": ["location:location-z", "location:location-a"],
        }
    )
    hydrated_location_maps = [
        {
            **deepcopy(selected_location_map),
            "location_key": "location-a",
            "_base_row_identities": ["location:location-a"],
            "entity_id": "entity-a",
            "entity_name": "Entity A",
            "inference_method": "method-a",
            "address_sources": ["synthetic_source_a"],
        },
        {
            **deepcopy(selected_location_map),
            "location_key": "location-z",
            "_base_row_identities": ["location:location-z"],
            "entity_id": "entity-z",
            "entity_name": "Entity Z",
            "inference_method": "method-z",
            "address_sources": ["synthetic_source_z"],
        },
    ]

    merged_location_map = npi_module._merge_hydrated_location_candidates(
        [selected_location_map],
        hydrated_location_maps,
    )[0]

    assert merged_location_map["location_key"] == "location-z"
    assert merged_location_map["entity_id"] == "entity-z"
    assert merged_location_map["entity_name"] == "Entity Z"
    assert merged_location_map["inference_method"] == "method-z"
    assert merged_location_map["address_sources"] == [
        "synthetic_candidate",
        "synthetic_source_a",
        "synthetic_source_z",
    ]


async def _load_consistent_location_views(monkeypatch):
    """Load exhaustive and bounded responses from identical fixtures."""
    all_response_map, _all_calls = await _get_page(
        monkeypatch,
        limit="all",
        include_evidence=True,
        degraded_hydration=True,
    )
    first_page_map, _first_calls = await _get_page(
        monkeypatch,
        limit="2",
        include_evidence=True,
        degraded_hydration=True,
    )
    second_page_map, _second_calls = await _get_page(
        monkeypatch,
        limit="2",
        offset=2,
        include_evidence=True,
        degraded_hydration=True,
    )
    return all_response_map, first_page_map, second_page_map


def _location_identity_set(location_maps):
    """Return the public composite identities present on one response page."""
    return {
        (location_map["address_key"], location_map.get("address_site_key"))
        for location_map in location_maps
    }


def _assert_hydrated_projection(location_maps):
    """Assert candidate scalars and hydrated-only evidence both survive."""
    assert all(location_map.get("state_code") == "IL" for location_map in location_maps)
    assert any(
        location_map.get("aca_plan_array") == ["synthetic-plan"]
        for location_map in location_maps
    )
    hydrated_location_map = next(
        location_map
        for location_map in location_maps
        if location_map["address_key"].endswith("0002")
    )
    assert hydrated_location_map["archive_identity_version"] == 2
    assert hydrated_location_map["base_address_version"] == 3
    assert hydrated_location_map["entity_id"] == "synthetic-entity"
    assert hydrated_location_map["inference_method"] == "synthetic_method"
    assert hydrated_location_map["row_origin"] == "synthetic_origin"


@pytest.mark.asyncio
async def test_all_matches_bounded_order_and_candidate_projection(monkeypatch):
    """Require exhaustive detail to equal concatenated bounded pages."""
    all_response_map, first_page_map, second_page_map = (
        await _load_consistent_location_views(monkeypatch)
    )
    bounded_location_maps = (
        first_page_map["address_list"] + second_page_map["address_list"]
    )

    assert all_response_map["address_list"] == bounded_location_maps
    assert _location_identity_set(first_page_map["address_list"]).isdisjoint(
        _location_identity_set(second_page_map["address_list"])
    )
    _assert_hydrated_projection(all_response_map["address_list"])
    assert first_page_map["address_pagination"] == {
        "limit": 2,
        "offset": 0,
        "returned": 2,
        "total": 3,
        "has_more": True,
    }
    assert second_page_map["address_pagination"] == {
        "limit": 2,
        "offset": 2,
        "returned": 1,
        "total": 3,
        "has_more": False,
    }


def _install_direct_inferred_route_mocks(monkeypatch):
    """Install exact-detail fixtures spanning direct and inferred identities."""
    direct_address_map = _address(
        ADDRESS_A,
        "100 Example Avenue",
        source_id="nppes",
    )
    inferred_address_map = _address(
        ADDRESS_B,
        "200 Example Avenue",
        source_id="provider_directory_fhir",
    )
    inferred_address_map["npi"] = None
    inferred_address_map["inferred_npi"] = 1234567890
    base_address_maps = [direct_address_map, inferred_address_map]
    address_rows_mock = AsyncMock(return_value=deepcopy(base_address_maps))
    location_candidates_mock = AsyncMock(
        return_value=[
            {
                **deepcopy(address_map),
                "_base_row_identities": [
                    npi_module._base_address_row_identity(address_map)
                ],
            }
            for address_map in base_address_maps
        ]
    )
    route_replacement_by_name = {
        "_build_npi_details": AsyncMock(
            return_value={
                "npi": 1234567890,
                "taxonomy_list": [],
                "taxonomy_group_list": [],
                "do_business_as": [],
                "address_list": [deepcopy(direct_address_map)],
            }
        ),
        "_fetch_npi_location_candidates": location_candidates_mock,
        "_fetch_npi_address_rows": address_rows_mock,
        "_fetch_provider_directory_address_overlay": AsyncMock(return_value=[]),
        "_fetch_other_names": AsyncMock(return_value=[]),
        "_fetch_provider_enrichment_summary_detail": AsyncMock(
            return_value={"summary": None, "ffs_visibility": {}}
        ),
    }
    for function_name, replacement in route_replacement_by_name.items():
        monkeypatch.setattr(npi_module, function_name, replacement)
    monkeypatch.setattr(npi_module, "_NPI_DETAIL_RESPONSE_CACHE_TTL_SECONDS", 0.0)
    return location_candidates_mock, address_rows_mock


@pytest.mark.asyncio
async def test_get_npi_all_includes_direct_and_inferred_base_rows(monkeypatch):
    """Require exhaustive detail to hydrate direct and inferred base rows."""
    location_candidates_mock, address_rows_mock = (
        _install_direct_inferred_route_mocks(monkeypatch)
    )

    operation_response = await npi_module.get_npi(
        _detail_request(limit="all"),
        "1234567890",
    )
    response_map = json.loads(operation_response.body)

    assert [
        location_map["address_key"]
        for location_map in response_map["address_list"]
    ] == [ADDRESS_A, ADDRESS_B]
    assert response_map["address_pagination"] == {
        "limit": None,
        "offset": 0,
        "returned": 2,
        "total": 2,
        "has_more": False,
    }
    location_candidates_mock.assert_awaited_once()
    address_rows_mock.assert_awaited_once()
