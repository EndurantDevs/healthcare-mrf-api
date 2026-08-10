# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from copy import deepcopy
from unittest.mock import AsyncMock

from api.endpoint import npi as npi_module
from tests.test_npi_location_paging import ADDRESS_A, SITE_A, _address


def install_detail_runtime_mocks(monkeypatch, replacement_by_name):
    """Install shared location-detail dependencies plus test-specific replacements."""
    replacement_by_name = {
        "_fetch_other_names": AsyncMock(return_value=[]),
        "_fetch_provider_enrichment_summary_detail": AsyncMock(
            return_value={"summary": None, "ffs_visibility": {}}
        ),
        **replacement_by_name,
    }
    for function_name, replacement in replacement_by_name.items():
        monkeypatch.setattr(npi_module, function_name, replacement)
    monkeypatch.setattr(npi_module, "_NPI_DETAIL_RESPONSE_CACHE_TTL_SECONDS", 0.0)


def install_location_only_detail_mocks(
    monkeypatch,
    profile_fetch,
    *,
    overlay_locations=None,
):
    """Install a profile-independent provider backed by one unified location."""
    location_map = _address(
        "00000000-0000-0000-0000-000000000002",
        "200 Example Avenue",
        source_id="nppes",
        address_type="primary",
    )
    location_map["_base_row_identities"] = [
        npi_module._base_address_row_identity(location_map)
    ]
    install_detail_runtime_mocks(
        monkeypatch,
        {
            "_build_npi_details": AsyncMock(return_value={}),
            "_fetch_npi_location_candidates": AsyncMock(
                return_value=[deepcopy(location_map)]
            ),
            "_fetch_npi_address_rows": AsyncMock(
                return_value=[deepcopy(location_map)]
            ),
            "_fetch_provider_directory_address_overlay": AsyncMock(
                return_value=deepcopy(overlay_locations or [])
            ),
            "_fetch_provider_directory_profile_map": profile_fetch,
        },
    )


def unified_location_mapping():
    """Build one real-shape unified location for candidate hydration tests."""
    return {
        "npi": None,
        "inferred_npi": 1234567890,
        "checksum": 77,
        "type": "practice",
        "location_key": "synthetic-location-1",
        "address_key": ADDRESS_A,
        "premise_key": SITE_A,
        "first_line": "100 Example Avenue",
        "second_line": "Suite 100",
        "city_name": "Example City",
        "state_name": "IL",
        "state_code": "IL",
        "postal_code": "60001",
        "country_code": "US",
        "telephone_number": "2025550101",
        "address_precision": "street",
        "address_sources": ["synthetic_directory"],
        "source_record_ids": [
            "provider_directory_fhir:practitioner_role:synthetic:role-1:loc-1"
        ],
        "source_count": 1,
        "independent_source_count": 1,
        "multi_source_confirmed": False,
        "aca_plan_array": ["synthetic-plan"],
        "aca_network_array": ["synthetic-network"],
        "lat": 41.0,
        "long": -87.0,
    }


def large_base_locations(count=1000):
    """Build a large deterministic location set for bounded hydration tests."""
    return [
        _address(
            f"00000000-0000-0000-0000-{index:012x}",
            f"{index} Example Avenue",
            source_id="nppes",
        )
        for index in range(1, count + 1)
    ]


def install_large_detail_mocks(monkeypatch, base_locations):
    """Install a large candidate set and record bounded hydration calls."""
    build_calls: list[dict[str, object]] = []
    address_row_calls: list[dict[str, object]] = []

    async def fake_build(_npi, **kwargs):
        build_calls.append(kwargs)
        selected_ids = set(kwargs.get("address_row_identities") or [])
        selected_locations = [
            deepcopy(location_map)
            for location_map in base_locations
            if npi_module._base_address_row_identity(location_map) in selected_ids
        ]
        return {
            "npi": 1234567890,
            "taxonomy_list": [],
            "taxonomy_group_list": [],
            "do_business_as": [],
            "address_list": selected_locations,
        }

    async def fake_candidates(_npi, **_kwargs):
        candidate_locations = deepcopy(base_locations)
        for candidate_location in candidate_locations:
            candidate_location["_base_row_identities"] = [
                npi_module._base_address_row_identity(candidate_location)
            ]
        return candidate_locations

    async def fake_address_rows(_npi, **kwargs):
        address_row_calls.append(kwargs)
        selected_ids = set(kwargs.get("address_row_identities") or [])
        return [
            deepcopy(location_map)
            for location_map in base_locations
            if npi_module._base_address_row_identity(location_map) in selected_ids
        ]

    install_detail_runtime_mocks(
        monkeypatch,
        {
            "_build_npi_details": fake_build,
            "_fetch_npi_location_candidates": fake_candidates,
            "_fetch_provider_directory_address_overlay": AsyncMock(return_value=[]),
            "_fetch_npi_address_rows": fake_address_rows,
        },
    )
    return build_calls, address_row_calls


def duplicate_site_locations():
    """Build two source rows for one address with conflicting scalar contacts."""
    first_location = _address(
        ADDRESS_A,
        "100 Example Avenue",
        source_id="synthetic-a",
    )
    second_location = _address(
        ADDRESS_A,
        "100 Example Avenue",
        source_id="synthetic-b",
    )
    first_location["telephone_number"] = "2025550101"
    second_location["telephone_number"] = "2025550199"
    for location_map, location_key in (
        (first_location, "location-a"),
        (second_location, "location-b"),
    ):
        location_map.update(
            premise_key=SITE_A,
            checksum=77,
            type="practice",
            location_key=location_key,
        )
        location_map["_base_row_identities"] = [
            npi_module._base_address_row_identity(location_map)
        ]
    return first_location, second_location


def install_duplicate_detail_mocks(monkeypatch):
    """Install deterministic reverse-order hydration for one duplicate site."""
    first_location, second_location = duplicate_site_locations()

    async def fake_build(_npi, **kwargs):
        selected_ids = set(kwargs.get("address_row_identities") or [])
        selected_locations = [
            deepcopy(location_map)
            for location_map in (first_location, second_location)
            if npi_module._base_address_row_identity(location_map) in selected_ids
        ]
        return {
            "npi": 1234567890,
            "taxonomy_list": [],
            "taxonomy_group_list": [],
            "do_business_as": [],
            "address_list": selected_locations,
        }

    async def fake_address_rows(_npi, **kwargs):
        selected_ids = set(kwargs.get("address_row_identities") or [])
        return [
            deepcopy(location_map)
            for location_map in (second_location, first_location)
            if npi_module._base_address_row_identity(location_map) in selected_ids
        ]

    install_detail_runtime_mocks(
        monkeypatch,
        {
            "_build_npi_details": fake_build,
            "_fetch_npi_location_candidates": AsyncMock(
                return_value=deepcopy([first_location, second_location])
            ),
            "_fetch_provider_directory_address_overlay": AsyncMock(return_value=[]),
            "_fetch_npi_address_rows": fake_address_rows,
        },
    )
