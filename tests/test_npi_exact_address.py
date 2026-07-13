# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import types
from copy import deepcopy
from unittest.mock import AsyncMock

import pytest

from api.endpoint import npi as npi_module


def test_npi_detail_cache_key_tracks_exact_address_key():
    common_options_by_name = {
        "npi": 1518379601,
        "view": "full",
        "include_chain": False,
        "extra_info": False,
        "sync_geocode": False,
        "lookup_stored_geocode": False,
    }

    unfiltered_key = npi_module._npi_detail_cache_key(**common_options_by_name)
    exact_address_key = npi_module._npi_detail_cache_key(
        **common_options_by_name,
        address_key="00000000-0000-0000-0000-000000000002",
    )

    assert unfiltered_key != exact_address_key


def test_overlay_query_types_optional_address_key_as_uuid():
    """Give PostgreSQL a stable type for nullable exact-address parameters."""
    sql = npi_module._provider_directory_overlay_query_sql(
        {"lat", "long", "address_key"}
    )

    assert sql.count("CAST(:address_key AS uuid)") == 2
    assert ":address_key IS NULL" not in sql


def _address_key_detail_payloads(address_key):
    """Return deterministic base and overlay fixtures for exact-address detail."""
    return (
        {
            "npi": 1518379601,
            "taxonomy_list": [],
            "taxonomy_group_list": [],
            "do_business_as": [],
            "address_list": [
                {
                    "address_key": "00000000-0000-0000-0000-000000000001",
                    "address_precision": "street",
                    "type": "primary",
                    "lat": 41.0,
                    "long": -87.0,
                },
                {
                    "address_key": address_key,
                    "address_precision": "street",
                    "type": "practice",
                    "lat": 42.0,
                    "long": -88.0,
                },
            ],
        },
        [
            {
                "address_key": address_key,
                "address_precision": "street",
                "type": "practice",
                "lat": 42.0,
                "long": -88.0,
                "address_sources": ["provider_directory_fhir"],
                "source_record_ids": ["provider_directory_fhir:role:source:role-1"],
                "provider_directory_sources": [
                    {
                        "source_ids": ["pdfhir_example"],
                        "practitioner_roles": [
                            {
                                "resource_type": "PractitionerRole",
                                "resource_id": "role-1",
                            }
                        ],
                    }
                ],
            },
            {
                "address_key": "00000000-0000-0000-0000-000000000003",
                "address_precision": "street",
                "type": "practice",
                "lat": 43.0,
                "long": -89.0,
            },
        ],
    )


async def _get_default_and_exact_address_lists(address_key):
    """Fetch unfiltered and exact-address test payloads."""
    app = types.SimpleNamespace(config={"NPI_API_UPDATE_GEOCODE": False})
    common_arg_map = {
        "include_evidence": "true",
        "include_profile": "false",
        "sync_geocode": "false",
        "lookup_stored_geocode": "false",
    }
    default_response = await npi_module.get_npi(
        types.SimpleNamespace(args=common_arg_map, app=app),
        "1518379601",
    )
    exact_response = await npi_module.get_npi(
        types.SimpleNamespace(
            args={**common_arg_map, "address_key": address_key},
            app=app,
        ),
        "1518379601",
    )
    return (
        json.loads(default_response.body)["address_list"],
        json.loads(exact_response.body)["address_list"],
    )


def _install_exact_address_mocks(monkeypatch, address_key):
    """Install deterministic NPI detail dependencies and return call spies."""
    detail_payload_map, overlay_payload_list = _address_key_detail_payloads(address_key)
    build_details = AsyncMock(
        side_effect=[deepcopy(detail_payload_map), deepcopy(detail_payload_map)]
    )
    fetch_overlay = AsyncMock(
        side_effect=[deepcopy(overlay_payload_list), deepcopy(overlay_payload_list)]
    )
    monkeypatch.setattr(npi_module, "_build_npi_details", build_details)
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_directory_address_overlay",
        fetch_overlay,
    )
    monkeypatch.setattr(
        npi_module,
        "_attach_provider_directory_source_details",
        AsyncMock(),
    )
    monkeypatch.setattr(npi_module, "_fetch_other_names", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_enrichment_detail",
        AsyncMock(return_value={"summary": None, "enrollments": {}, "ffs_visibility": {}}),
    )
    monkeypatch.setattr(npi_module, "_NPI_DETAIL_RESPONSE_CACHE_TTL_SECONDS", 0.0)
    return build_details, fetch_overlay


@pytest.mark.asyncio
async def test_get_npi_address_key_returns_exact_address_with_typed_evidence(
    monkeypatch,
):
    """Keep only the requested address while preserving typed FHIR evidence."""
    address_key = "00000000-0000-0000-0000-000000000002"
    build_details, fetch_overlay = _install_exact_address_mocks(
        monkeypatch,
        address_key,
    )
    default_addresses, exact_addresses = await _get_default_and_exact_address_lists(
        address_key
    )

    assert [address["address_key"] for address in default_addresses] == [
        "00000000-0000-0000-0000-000000000001",
        address_key,
        "00000000-0000-0000-0000-000000000003",
    ]
    assert [address["address_key"] for address in exact_addresses] == [address_key]
    assert exact_addresses[0]["provider_directory_sources"][0]["practitioner_roles"] == [
        {"resource_type": "PractitionerRole", "resource_id": "role-1"}
    ]
    assert [call.kwargs["address_key"] for call in build_details.await_args_list] == [
        None,
        address_key,
    ]
    assert [call.kwargs["address_key"] for call in fetch_overlay.await_args_list] == [
        None,
        address_key,
    ]
