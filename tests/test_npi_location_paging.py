# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import types
from copy import deepcopy
from types import MappingProxyType
from unittest.mock import AsyncMock
from urllib.parse import urlencode

import pytest
from sanic import Request
from sanic.compat import Header
from sanic.exceptions import InvalidUsage

from api.endpoint import npi as npi_module
from api.network_address_scope import network_address_read_scope
from process.network_serving_read import PinnedNetworkServingManifest
from tests.provider_profile_snapshot_test_support import (
    stub_provider_profile_snapshot as stub_provider_profile_snapshot,
)

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
        "formatted_address": (f"{first_line}, Suite 100, Example City, IL 60001"),
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


def _prepare_hydrated_address_maps(
    *,
    degraded_hydration: bool,
) -> list[dict[str, object]]:
    """Build full-row address fixtures with stable base identities."""
    address_maps = deepcopy(_detail_payload()["address_list"])
    if degraded_hydration:
        address_maps[0]["state_code"] = None
        address_maps[1].update(
            {
                "location_status": "active",
                "independent_source_count": 3,
                "multi_source_confirmed": True,
                "aca_plan_array": ["synthetic-plan"],
                "archive_identity_version": 2,
                "base_address_version": 3,
                "confidence_score": 0.9,
                "entity_id": "synthetic-entity",
                "entity_name": "Synthetic Entity",
                "entity_type": "provider",
                "freshness_score": 0.8,
                "inference_confidence": 0.7,
                "inference_method": "synthetic_method",
                "location_confidence_id": "synthetic-confidence",
                "row_origin": "synthetic_origin",
            }
        )
    for address_map in address_maps:
        address_map["_base_row_identities"] = [npi_module._base_address_row_identity(address_map)]
    return address_maps


def _build_detail_mock(build_calls):
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
                if npi_module._base_address_row_identity(address) in selected_identities
            ]
        return detail

    return fake_build


def _location_candidate_mock(candidate_calls):
    async def fake_candidates(_npi, **kwargs):
        candidate_calls.append(kwargs)
        candidates = deepcopy(_detail_payload()["address_list"])
        for candidate in candidates:
            candidate["_base_row_identities"] = [npi_module._base_address_row_identity(candidate)]
        return candidates

    return fake_candidates


def _address_row_mock(address_row_calls, *, degraded_hydration):
    async def fake_address_rows(_npi, **kwargs):
        address_row_calls.append(kwargs)
        address_maps = _prepare_hydrated_address_maps(
            degraded_hydration=degraded_hydration,
        )
        selected_identities = kwargs.get("address_row_identities")
        if selected_identities is not None:
            address_maps = [
                address
                for address in address_maps
                if npi_module._base_address_row_identity(address) in selected_identities
            ]
        return address_maps

    return fake_address_rows


async def _fake_overlay(_npi, **_kwargs):
    return deepcopy(_overlay_payload())


def _install_route_mocks(monkeypatch, *, degraded_hydration: bool = False):
    """Install deterministic exact-detail candidate and hydration fixtures."""
    build_calls: list[dict[str, object]] = []
    address_row_calls: list[dict[str, object]] = []
    candidate_calls: list[dict[str, object]] = []
    route_replacement_by_name = {
        "_build_npi_details": _build_detail_mock(build_calls),
        "_fetch_provider_directory_address_overlay": _fake_overlay,
        "_fetch_npi_location_candidates": _location_candidate_mock(candidate_calls),
        "_fetch_npi_address_rows": _address_row_mock(
            address_row_calls,
            degraded_hydration=degraded_hydration,
        ),
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
    return {
        "build": build_calls,
        "candidates": candidate_calls,
        "address_rows": address_row_calls,
    }


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


async def _get_page(
    monkeypatch,
    *,
    limit: str,
    offset: int = 0,
    include_total: bool = True,
    include_evidence: bool = False,
    degraded_hydration: bool = False,
):
    route_calls = _install_route_mocks(
        monkeypatch,
        degraded_hydration=degraded_hydration,
    )
    request = _detail_request(
        limit=limit,
        offset=offset,
        include_total=include_total,
        include_evidence=include_evidence,
    )
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


def test_dedupe_merges_bare_overlay_and_conflicting_sites_by_exact_key(caplog):
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

    assert len(deduped) == 1
    assert deduped[0]["premise_key"] == SITE_A
    assert deduped[0]["address_sources"] == ["nppes", "provider_directory_fhir"]
    assert "maps to 2 conflicting non-null site keys" in caplog.text


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

    assert [address["address_key"] for address in first_page["address_list"]] == [
        ADDRESS_A,
        ADDRESS_C,
    ]
    assert [address["address_key"] for address in second_page["address_list"]] == [ADDRESS_B]
    assert first_page["address_list"][0]["formatted_address"] == (
        "100 Example Avenue, Suite 100, Example City, IL 60001"
    )
    assert second_page["address_list"][0]["formatted_address"] == (
        "200 Example Avenue, Suite 100, Example City, IL 60001"
    )
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
    payload, route_calls = await _get_page(monkeypatch, limit="all")

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
    assert len(route_calls["candidates"]) == 1
    assert len(route_calls["address_rows"]) == 1
    assert len(route_calls["address_rows"][0]["address_row_identities"]) == 2


def _real_detail_request(arguments):
    return Request(
        ("/api/v1/npi/id/1234567890?" + urlencode(arguments)).encode(),
        Header({}),
        "1.1",
        "GET",
        None,
        types.SimpleNamespace(config={"NPI_API_UPDATE_GEOCODE": False}),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "selection",
    [
        {"address_limit": 1, "address_offset": 1},
        {"address_grouping": "premise", "address_limit": 1, "address_offset": 1},
        {"address_key": ADDRESS_B, "address_limit": 1},
    ],
)
async def test_internal_detail_matches_route_and_uses_explicit_args_without_request_mutation(monkeypatch, selection):
    _install_route_mocks(monkeypatch)
    detail_args_by_name = {
        "view": "summary",
        "include_profile": False,
        "sync_geocode": False,
        "lookup_stored_geocode": False,
        **selection,
    }
    ordinary_request = _real_detail_request(detail_args_by_name)
    expected = await npi_module.get_npi(ordinary_request, "1234567890")
    request = _real_detail_request(
        {"address_grouping": "invalid", "address_limit": "all", "force_address_update": "true", "debug": "true"}
    )
    original_args = deepcopy(dict(request.args))
    original_app = request.app
    request.ctx.caller_marker = object()
    original_marker = request.ctx.caller_marker
    explicit_args = MappingProxyType(detail_args_by_name)
    actual = await npi_module.read_npi_detail(request, "1234567890", native_args=explicit_args)
    assert actual.status == expected.status == 200
    assert actual.body == expected.body
    assert actual.content_type == expected.content_type == "application/json"
    assert dict(request.args) == original_args
    assert request.app is original_app and request.ctx.caller_marker is original_marker
    assert dict(explicit_args) == detail_args_by_name
    document = json.loads(actual.body)
    if selection.get("address_grouping") == "premise":
        assert document["address_groups"][0]["members"][0]["address_key"] == ADDRESS_C
    elif "address_key" in selection:
        assert [address["address_key"] for address in document["address_list"]] == [ADDRESS_B]
    else:
        assert [address["address_key"] for address in document["address_list"]] == [ADDRESS_C]


@pytest.mark.asyncio
async def test_internal_detail_empty_args_ignore_browser_values(monkeypatch):
    route_calls = _install_route_mocks(monkeypatch)
    from api import provider_profile_network_read

    profile_read = AsyncMock(return_value=None)
    monkeypatch.setattr(provider_profile_network_read, "read_detail_directory_profile", profile_read)
    request = _real_detail_request({"address_grouping": "invalid", "address_limit": "1", "debug": "true"})
    original_args = deepcopy(dict(request.args))
    operation_response = await npi_module.read_npi_detail(request, "1234567890", native_args=MappingProxyType({}))
    payload = json.loads(operation_response.body)
    assert payload["address_pagination"]["limit"] == npi_module.NPI_DETAIL_ADDRESS_DEFAULT_LIMIT
    assert [address["address_key"] for address in payload["address_list"]] == [ADDRESS_A, ADDRESS_C, ADDRESS_B]
    assert "include_evidence" not in route_calls["build"][0]
    profile_read.assert_awaited_once_with(None, 1234567890, include_evidence=False)
    assert dict(request.args) == original_args


@pytest.mark.asyncio
@pytest.mark.parametrize("write_option", ["force_address_update", "sync_geocode"])
async def test_internal_detail_scoped_write_options_refuse_before_loading(monkeypatch, write_option):
    build = AsyncMock()
    writer = AsyncMock()
    monkeypatch.setattr(npi_module, "_build_npi_details", build)
    monkeypatch.setattr(npi_module, "run_as_writer", writer)
    candidate_id = "11111111-1111-4111-8111-111111111111"
    manifest = PinnedNetworkServingManifest(
        9, candidate_id, "network_candidate_" + candidate_id.replace("-", ""), 1, {}, 4, "a" * 64, 123
    )
    request = _real_detail_request({write_option: "false"})
    with network_address_read_scope(manifest, (7,)):
        with pytest.raises(InvalidUsage, match="cannot update address data"):
            await npi_module.read_npi_detail(request, "1234567890", native_args={write_option: True})
    build.assert_not_awaited()
    writer.assert_not_awaited()
    assert request.args[write_option] == ["false"]
