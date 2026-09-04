# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact premise-grouping contracts for provider detail addresses."""

from copy import deepcopy
import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
import sanic.exceptions
import yaml

from api.endpoint import npi as npi_module
from tests.test_npi_location_paging import (
    ADDRESS_A,
    ADDRESS_B,
    ADDRESS_C,
    SITE_A,
    _address,
    _detail_request,
    _install_route_mocks,
    _overlay_payload,
)


SITE_B = "10000000-0000-0000-0000-000000000002"


def _premise_request(*, limit: str = "5", offset: int = 0):
    request = _detail_request(limit=limit, offset=offset)
    request.args["address_grouping"] = "premise"
    return request


def test_exact_premise_grouping_preserves_units_and_fails_closed():
    suite_one = _address(
        ADDRESS_A,
        "100 Example Avenue",
        source_id="synthetic-a",
        premise_key=SITE_A,
    )
    suite_two = _address(
        ADDRESS_B,
        "100 Example Avenue",
        source_id="synthetic-b",
        premise_key=SITE_A,
    )
    suite_two["second_line"] = "Suite 200"
    missing_site = _address(
        ADDRESS_C,
        "200 Example Avenue",
        source_id="synthetic-c",
    )
    unkeyed_address_by_field = {"type": "mail", "first_line": "PO Box 7"}

    groups = npi_module._group_provider_locations_by_premise(
        [suite_one, suite_two, missing_site, unkeyed_address_by_field]
    )

    assert groups[0]["group_key"] == SITE_A
    assert groups[0]["grouping_basis"] == "address_site_key"
    assert [member["second_line"] for member in groups[0]["members"]] == [
        "Suite 100",
        "Suite 200",
    ]
    assert groups[1]["group_key"] == ADDRESS_C
    assert groups[1]["grouping_basis"] == "address_key_fallback"
    assert groups[1]["address_site_key_status"] == "missing"
    assert groups[2]["group_key"] is None
    assert groups[2]["grouping_basis"] == "singleton"


def test_conflicting_non_null_sites_use_address_key_fallback():
    first = _address(
        ADDRESS_A,
        "100 Example Avenue",
        source_id="synthetic-a",
        premise_key=SITE_A,
    )
    second = _address(
        ADDRESS_A,
        "100 Example Avenue",
        source_id="synthetic-b",
        premise_key=SITE_B,
    )

    member = npi_module._dedupe_addresses_by_key([first, second])[0]
    groups = npi_module._group_provider_locations_by_premise([member])

    assert member["_address_site_keys"] == [SITE_A, SITE_B]
    assert groups[0]["group_key"] == ADDRESS_A
    assert groups[0]["address_site_key"] is None
    assert groups[0]["address_site_key_status"] == "conflicting"


@pytest.mark.asyncio
async def test_premise_route_pages_groups_and_preserves_member_rows(monkeypatch):
    route_calls = _install_route_mocks(monkeypatch)
    overlay_rows = deepcopy(_overlay_payload())
    overlay_rows[1]["premise_key"] = SITE_A
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_directory_address_overlay",
        AsyncMock(return_value=overlay_rows),
    )

    operation_response = await npi_module.get_npi(
        _premise_request(limit="1"),
        "1234567890",
    )
    response_map = json.loads(operation_response.body)

    assert "address_list" not in response_map
    assert "address_pagination" not in response_map
    assert response_map["address_grouping"] == "premise"
    assert response_map["address_group_pagination"] == {
        "limit": 1,
        "offset": 0,
        "returned": 1,
        "total": 2,
        "has_more": True,
        "next_offset": 1,
    }
    premise_group = response_map["address_groups"][0]
    assert premise_group["group_key"] == SITE_A
    assert premise_group["grouping_basis"] == "address_site_key"
    assert [member["address_key"] for member in premise_group["members"]] == [
        ADDRESS_A,
        ADDRESS_C,
    ]
    assert all(member.get("formatted_address") for member in premise_group["members"])
    assert premise_group["member_pagination"] == {
        "limit": 5,
        "offset": 0,
        "returned": 2,
        "total": 2,
        "has_more": False,
        "next_offset": None,
    }
    assert len(route_calls["address_rows"][0]["address_row_identities"]) == 1


@pytest.mark.asyncio
async def test_premise_member_preview_is_bounded_to_five(monkeypatch):
    _install_route_mocks(monkeypatch)
    member_rows = []
    for index in range(6):
        member = _address(
            f"00000000-0000-0000-0000-{index + 10:012d}",
            f"{index + 1} Example Avenue",
            source_id="provider_directory_fhir",
            premise_key=SITE_A,
        )
        member_rows.append(member)
    monkeypatch.setattr(
        npi_module,
        "_fetch_npi_location_candidates",
        AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_directory_address_overlay",
        AsyncMock(return_value=member_rows),
    )

    operation_response = await npi_module.get_npi(
        _premise_request(),
        "1234567890",
    )
    response_map = json.loads(operation_response.body)
    group = response_map["address_groups"][0]

    assert len(group["members"]) == 5
    assert group["member_pagination"] == {
        "limit": 5,
        "offset": 0,
        "returned": 5,
        "total": 6,
        "has_more": True,
        "next_offset": 5,
    }


@pytest.mark.asyncio
async def test_flat_site_continuation_returns_members_after_group_preview(monkeypatch):
    _install_route_mocks(monkeypatch)
    member_rows = [
        _address(
            f"00000000-0000-0000-0000-{index + 10:012d}",
            f"{index + 1} Example Avenue",
            source_id="provider_directory_fhir",
            premise_key=SITE_A,
        )
        for index in range(6)
    ]
    monkeypatch.setattr(
        npi_module,
        "_fetch_npi_location_candidates",
        AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_directory_address_overlay",
        AsyncMock(return_value=member_rows),
    )
    request = _detail_request(limit="5", offset=5)
    request.args["address_site_key"] = SITE_A

    operation_response = await npi_module.get_npi(request, "1234567890")
    response_map = json.loads(operation_response.body)

    assert [member["address_key"] for member in response_map["address_list"]] == [
        member_rows[5]["address_key"]
    ]
    assert response_map["address_pagination"] == {
        "limit": 5,
        "offset": 5,
        "returned": 1,
        "total": 6,
        "has_more": False,
    }
    assert "address_groups" not in response_map


@pytest.mark.asyncio
async def test_premise_group_pagination_is_truthful_at_and_beyond_end(monkeypatch):
    _install_route_mocks(monkeypatch)

    final_response = await npi_module.get_npi(
        _premise_request(limit="1", offset=2),
        "1234567890",
    )
    beyond_response = await npi_module.get_npi(
        _premise_request(limit="1", offset=3),
        "1234567890",
    )
    final_page = json.loads(final_response.body)
    beyond_page = json.loads(beyond_response.body)

    assert final_page["address_group_pagination"] == {
        "limit": 1,
        "offset": 2,
        "returned": 1,
        "total": 3,
        "has_more": False,
        "next_offset": None,
    }
    assert beyond_page["address_groups"] == []
    assert beyond_page["address_group_pagination"] == {
        "limit": 1,
        "offset": 3,
        "returned": 0,
        "total": 3,
        "has_more": False,
        "next_offset": None,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("parameter_map", "message"),
    [
        ({"address_grouping": "fuzzy"}, "address_grouping"),
        ({"address_grouping": "premise", "address_limit": "all"}, "between 1 and 5"),
        ({"address_grouping": "premise", "address_limit": "0"}, "between 1 and 5"),
        ({"address_grouping": "premise", "address_limit": "6"}, "between 1 and 5"),
        ({"address_grouping": "premise", "address_key": ADDRESS_A}, "not supported"),
        ({"address_grouping": "premise", "address_site_key": SITE_A}, "not supported"),
    ],
)
async def test_premise_route_rejects_ambiguous_or_unbounded_inputs(
    parameter_map,
    message,
):
    request = _detail_request(limit=None)
    request.args.update(parameter_map)

    with pytest.raises(sanic.exceptions.InvalidUsage, match=message):
        await npi_module.get_npi(request, "1234567890")


@pytest.mark.asyncio
async def test_flat_site_filter_selects_members_without_changing_shape(monkeypatch):
    route_calls = _install_route_mocks(monkeypatch)
    request = _detail_request(limit="5")
    request.args["address_site_key"] = SITE_A

    operation_response = await npi_module.get_npi(request, "1234567890")
    response_map = json.loads(operation_response.body)

    assert [member["address_key"] for member in response_map["address_list"]] == [
        ADDRESS_A
    ]
    assert "address_groups" not in response_map
    assert route_calls["candidates"][0]["address_site_key"] == SITE_A


def test_overlay_query_uses_only_materialized_premise_key():
    current_sql = npi_module._provider_directory_overlay_query_sql(
        {"lat", "long", "premise_key"}
    )
    compatibility_sql = npi_module._provider_directory_overlay_query_sql(
        {"lat", "long"}
    )

    assert "overlay.premise_key = CAST(:address_site_key AS uuid)" in current_sql
    assert "premise_key," in current_sql
    assert "NULL::uuid AS premise_key" in compatibility_sql
    assert "CAST(:address_site_key AS uuid) IS NULL" in compatibility_sql
    assert "address_archive_v2" not in current_sql


def test_backend_openapi_documents_premise_grouping_and_continuation():
    document = yaml.safe_load(Path("doc/openapi.yaml").read_text(encoding="utf-8"))
    operation = document["paths"]["/npi/id/{npi}"]["get"]
    parameter_by_name = {
        parameter["name"]: parameter for parameter in operation["parameters"]
    }
    grouping_parameter = parameter_by_name["address_grouping"]
    site_parameter = parameter_by_name["address_site_key"]

    assert grouping_parameter["schema"]["enum"] == ["flat", "premise"]
    assert grouping_parameter["schema"]["default"] == "flat"
    assert site_parameter["schema"]["format"] == "uuid"

    schemas = document["components"]["schemas"]
    group_schema = schemas["NpiAddressGroup"]
    assert group_schema["properties"]["members"]["maxItems"] == 5
    pagination = group_schema["properties"]["member_pagination"]
    assert "flat mode" in pagination["description"]
    assert "next_offset" in pagination["description"]
    assert schemas["NpiAddressGroupPagination"]["properties"]["limit"]["maximum"] == 5


def test_detail_cache_key_separates_flat_site_and_premise_windows():
    cache_key_option_map = {
        "npi": 1234567890,
        "view": "summary",
        "include_chain": False,
        "extra_info": False,
        "sync_geocode": False,
        "lookup_stored_geocode": False,
    }

    flat_key = npi_module._npi_detail_cache_key(
        npi_module._NpiDetailCacheIdentity(**cache_key_option_map)
    )
    site_key = npi_module._npi_detail_cache_key(
        npi_module._NpiDetailCacheIdentity(
            **cache_key_option_map,
            address_site_key=SITE_A,
        )
    )
    premise_key = npi_module._npi_detail_cache_key(
        npi_module._NpiDetailCacheIdentity(
            **cache_key_option_map,
            address_grouping="premise",
        )
    )

    assert len({flat_key, site_key, premise_key}) == 3
