# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from copy import deepcopy
import json
import types
from unittest.mock import AsyncMock

import pytest

from api.endpoint import npi as npi_module


ADDRESS_A = "00000000-0000-0000-0000-000000000001"
ADDRESS_B = "00000000-0000-0000-0000-000000000002"
ADDRESS_C = "00000000-0000-0000-0000-000000000003"
SITE_A = "10000000-0000-0000-0000-000000000001"


def _address(
    address_key: str,
    first_line: str,
    *,
    source_id: str,
    status: str = "unknown",
    premise_key: str | None = None,
    address_type: str = "practice",
) -> dict[str, object]:
    address_map: dict[str, object] = {
        "npi": 1234567890,
        "checksum": int(address_key.replace("-", "")[-12:], 16),
        "address_key": address_key,
        "type": address_type,
        "first_line": first_line,
        "second_line": "Suite 100",
        "city_name": "Example City",
        "state_name": "IL",
        "state_code": "IL",
        "postal_code": "60001",
        "country_code": "US",
        "address_precision": "street",
        "address_sources": [source_id],
        "source_count": 1,
        "independent_source_count": 1,
        "multi_source_confirmed": False,
        "location_status": status,
        "lat": 41.0,
        "long": -87.0,
    }
    if premise_key is not None:
        address_map["premise_key"] = premise_key
    return address_map


def _detail_payload() -> dict[str, object]:
    return {
        "npi": 1234567890,
        "taxonomy_list": [],
        "taxonomy_group_list": [],
        "do_business_as": [],
        "address_list": [
            _address(
                ADDRESS_A,
                "100 Example Avenue",
                source_id="nppes",
                premise_key=SITE_A,
            ),
            _address(
                ADDRESS_B,
                "200 Example Avenue",
                source_id="nppes",
                address_type="primary",
            ),
        ],
    }


def _overlay_payload() -> list[dict[str, object]]:
    return [
        _address(
            ADDRESS_A,
            "100 Example Avenue",
            source_id="provider_directory_fhir",
            status="active",
        ),
        _address(
            ADDRESS_C,
            "300 Example Avenue",
            source_id="provider_directory_fhir",
            status="active",
        ),
    ]


def _install_route_mocks(monkeypatch):
    build_calls: list[dict[str, object]] = []
    address_row_calls: list[dict[str, object]] = []

    async def fake_build(_npi, **kwargs):
        build_calls.append(kwargs)
        detail = deepcopy(_detail_payload())
        if kwargs.get("address_limit") == 0:
            detail["address_list"] = []
        selected_identities = kwargs.get("address_row_identities")
        if selected_identities is not None:
            detail["address_list"] = [
                address
                for address in detail["address_list"]
                if npi_module._base_address_row_identity(address)
                in selected_identities
            ]
        return detail

    async def fake_candidates(_npi, **_kwargs):
        candidates = deepcopy(_detail_payload()["address_list"])
        for candidate in candidates:
            candidate["_base_row_identities"] = [
                npi_module._base_address_row_identity(candidate)
            ]
        return candidates

    async def fake_overlay(_npi, **_kwargs):
        return deepcopy(_overlay_payload())

    async def fake_address_rows(_npi, **kwargs):
        address_row_calls.append(kwargs)
        address_maps = deepcopy(_detail_payload()["address_list"])
        selected_identities = kwargs.get("address_row_identities")
        if selected_identities is not None:
            address_maps = [
                address
                for address in address_maps
                if npi_module._base_address_row_identity(address)
                in selected_identities
            ]
        return address_maps

    route_replacement_by_name = {
        "_build_npi_details": fake_build,
        "_fetch_provider_directory_address_overlay": fake_overlay,
        "_fetch_npi_location_candidates": fake_candidates,
        "_fetch_npi_address_rows": fake_address_rows,
    }
    for function_name, replacement in route_replacement_by_name.items():
        monkeypatch.setattr(npi_module, function_name, replacement)
    monkeypatch.setattr(npi_module, "_fetch_other_names", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_enrichment_summary_detail",
        AsyncMock(return_value={"summary": None, "ffs_visibility": {}}),
    )
    monkeypatch.setattr(npi_module, "_NPI_DETAIL_RESPONSE_CACHE_TTL_SECONDS", 0.0)
    return {"build": build_calls, "address_rows": address_row_calls}


def _detail_request(
    *,
    limit: str | None,
    offset: int = 0,
    include_total: bool = True,
    include_profile: bool = False,
    include_evidence: bool = False,
):
    request_arg_map = {
        "view": "summary",
        "include_profile": str(include_profile).lower(),
        "include_evidence": str(include_evidence).lower(),
        "sync_geocode": "false",
        "lookup_stored_geocode": "false",
        "address_offset": str(offset),
        "include_address_total": str(include_total).lower(),
    }
    if limit is not None:
        request_arg_map["address_limit"] = limit
    return types.SimpleNamespace(
        args=request_arg_map,
        app=types.SimpleNamespace(config={"NPI_API_UPDATE_GEOCODE": False}),
    )


async def _get_page(monkeypatch, *, limit: str, offset: int = 0, include_total: bool = True):
    route_calls = _install_route_mocks(monkeypatch)
    request = _detail_request(limit=limit, offset=offset, include_total=include_total)
    operation_response = await npi_module.get_npi(request, "1234567890")
    return json.loads(operation_response.body), route_calls


def test_dedupe_merges_bare_overlay_into_one_concrete_site():
    rows = [
        _address(ADDRESS_A, "100 Example Avenue", source_id="nppes", premise_key=SITE_A),
        _address(
            ADDRESS_A,
            "100 Example Avenue",
            source_id="provider_directory_fhir",
            status="active",
        ),
    ]

    deduped = npi_module._dedupe_addresses_by_key(rows)

    assert len(deduped) == 1
    assert deduped[0]["premise_key"] == SITE_A
    assert deduped[0]["address_sources"] == ["nppes", "provider_directory_fhir"]
    assert deduped[0]["location_status"] == "active"
    assert deduped[0]["multi_source_confirmed"] is True


@pytest.mark.parametrize(
    ("base_changes", "overlay_changes"),
    [
        ({"state_name": "Illinois"}, {"state_name": "IL"}),
        (
            {"first_line": "100 Example Street", "postal_code": "60001-1234"},
            {"first_line": "100 Example St", "postal_code": "60001"},
        ),
    ],
)
def test_dedupe_merges_unsited_alias_into_only_concrete_site(
    base_changes,
    overlay_changes,
):
    base = _address(
        ADDRESS_A,
        "100 Example Avenue",
        source_id="nppes",
        premise_key=SITE_A,
    )
    overlay = _address(
        ADDRESS_A,
        "100 Example Avenue",
        source_id="provider_directory_fhir",
    )
    base.update(base_changes)
    overlay.update(overlay_changes)

    deduped = npi_module._dedupe_addresses_by_key([base, overlay])

    assert len(deduped) == 1
    assert deduped[0]["premise_key"] == SITE_A
    assert deduped[0]["address_sources"] == [
        "nppes",
        "provider_directory_fhir",
    ]


def test_dedupe_keeps_bare_overlay_when_site_match_is_ambiguous():
    rows = [
        _address(ADDRESS_A, "100 Example Avenue", source_id="nppes", premise_key=SITE_A),
        _address(
            ADDRESS_A,
            "100 Example Avenue",
            source_id="nppes",
            premise_key="10000000-0000-0000-0000-000000000002",
        ),
        _address(ADDRESS_A, "100 Example Avenue", source_id="provider_directory_fhir"),
    ]

    deduped = npi_module._dedupe_addresses_by_key(rows)

    assert len(deduped) == 3
    assert sum("premise_key" not in row for row in deduped) == 1


def test_merge_preserves_endpoint_aware_confirmation():
    primary = _address(ADDRESS_A, "100 Example Avenue", source_id="provider_directory_fhir")
    duplicate = _address(ADDRESS_A, "100 Example Avenue", source_id="provider_directory_fhir")
    duplicate["independent_source_count"] = 2
    duplicate["multi_source_confirmed"] = True

    npi_module._merge_duplicate_address(primary, duplicate)

    assert primary["independent_source_count"] == 2
    assert primary["multi_source_confirmed"] is True


@pytest.mark.parametrize(
    ("source_ids", "status_by_id", "expected"),
    [
        (["active", "missing"], {"active": "active"}, "active"),
        (["inactive", "missing"], {"inactive": "inactive"}, "unknown"),
        (
            ["inactive-a", "inactive-b"],
            {"inactive-a": "inactive", "inactive-b": "inactive"},
            "inactive",
        ),
    ],
)
def test_location_status_preserves_unknown_source_evidence(
    source_ids,
    status_by_id,
    expected,
):
    assert (
        npi_module._location_status_from_source_records(
            source_ids,
            status_by_id,
        )
        == expected
    )


@pytest.mark.asyncio
async def test_location_status_distinguishes_independent_and_fhir_only_evidence(
    monkeypatch,
):
    status_lookup = AsyncMock(return_value={"synthetic-role": "inactive"})
    monkeypatch.setattr(
        npi_module,
        "_fetch_location_status_by_record_id",
        status_lookup,
    )
    location_maps = [
        {
            "address_sources": ["nppes", "provider_directory_fhir"],
            "source_record_ids": ["synthetic-role"],
        },
        {
            "address_sources": ["provider_directory_fhir"],
            "source_record_ids": ["synthetic-role"],
        },
    ]

    await npi_module._apply_location_statuses(location_maps)

    assert [location["location_status"] for location in location_maps] == [
        "unknown",
        "inactive",
    ]


@pytest.mark.asyncio
async def test_get_npi_pages_after_overlay_merge_and_dedupe(monkeypatch):
    first_page, first_route_calls = await _get_page(monkeypatch, limit="2")
    second_page, second_route_calls = await _get_page(monkeypatch, limit="2", offset=2)

    assert [row["address_key"] for row in first_page["address_list"]] == [
        ADDRESS_A,
        ADDRESS_C,
    ]
    assert [row["address_key"] for row in second_page["address_list"]] == [ADDRESS_B]
    assert first_page["address_pagination"] == {
        "limit": 2,
        "offset": 0,
        "returned": 2,
        "total": 3,
        "has_more": True,
    }
    assert second_page["address_pagination"] == {
        "limit": 2,
        "offset": 2,
        "returned": 1,
        "total": 3,
        "has_more": False,
    }
    for route_calls in (first_route_calls, second_route_calls):
        build_calls = route_calls["build"]
        address_row_calls = route_calls["address_rows"]
        assert build_calls[0]["address_limit"] == 0
        assert len(address_row_calls) == 1
        assert len(address_row_calls[0]["address_row_identities"]) <= 2


@pytest.mark.asyncio
async def test_get_npi_limit_one_never_leaks_overlay_rows(monkeypatch):
    payload, _build_calls = await _get_page(monkeypatch, limit="1")

    assert len(payload["address_list"]) == 1
    assert payload["address_pagination"]["returned"] == 1
    assert payload["address_pagination"]["has_more"] is True


@pytest.mark.asyncio
async def test_get_npi_all_returns_complete_combined_location_set(monkeypatch):
    payload, _build_calls = await _get_page(monkeypatch, limit="all")

    assert [row["address_key"] for row in payload["address_list"]] == [
        ADDRESS_A,
        ADDRESS_C,
        ADDRESS_B,
    ]
    assert payload["address_pagination"] == {
        "limit": None,
        "offset": 0,
        "returned": 3,
        "total": 3,
        "has_more": False,
    }


@pytest.mark.asyncio
async def test_get_npi_all_includes_direct_and_inferred_base_rows(monkeypatch):
    direct_address = _address(
        ADDRESS_A,
        "100 Example Avenue",
        source_id="nppes",
    )
    inferred_address = _address(
        ADDRESS_B,
        "200 Example Avenue",
        source_id="provider_directory_fhir",
    )
    inferred_address["npi"] = None
    inferred_address["inferred_npi"] = 1234567890
    address_rows = AsyncMock(
        return_value=[deepcopy(direct_address), deepcopy(inferred_address)]
    )
    monkeypatch.setattr(
        npi_module,
        "_build_npi_details",
        AsyncMock(
            return_value={
                "npi": 1234567890,
                "taxonomy_list": [],
                "taxonomy_group_list": [],
                "do_business_as": [],
                "address_list": [deepcopy(direct_address)],
            }
        ),
    )
    monkeypatch.setattr(npi_module, "_fetch_npi_address_rows", address_rows)
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_directory_address_overlay",
        AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(npi_module, "_fetch_other_names", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_enrichment_summary_detail",
        AsyncMock(return_value={"summary": None, "ffs_visibility": {}}),
    )
    monkeypatch.setattr(npi_module, "_NPI_DETAIL_RESPONSE_CACHE_TTL_SECONDS", 0.0)
    operation_response = await npi_module.get_npi(
        _detail_request(limit="all"),
        "1234567890",
    )
    response_map = json.loads(operation_response.body)
    assert [location["address_key"] for location in response_map["address_list"]] == [
        ADDRESS_A,
        ADDRESS_B,
    ]
    assert response_map["address_pagination"] == {
        "limit": None,
        "offset": 0,
        "returned": 2,
        "total": 2,
        "has_more": False,
    }
    address_rows.assert_awaited_once()
