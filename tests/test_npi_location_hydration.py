# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from datetime import UTC, date, datetime
import json
import types
from unittest.mock import AsyncMock

import pytest

from api.endpoint import npi as npi_module
from tests.npi_location_hydration_support import (
    install_duplicate_detail_mocks,
    install_large_detail_mocks,
    install_location_only_detail_mocks,
    large_base_locations,
    unified_location_mapping,
)
from tests.test_npi_location_paging import (
    ADDRESS_A,
    ADDRESS_B,
    ADDRESS_C,
    SITE_A,
    _address,
    _detail_request,
    _overlay_payload,
)


class _ResultRows:
    """Expose deterministic result rows through the SQLAlchemy-style API."""

    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


@pytest.mark.asyncio
@pytest.mark.parametrize("include_profile", (False, True))
async def test_location_backed_provider_exists_without_profile_payload(
    monkeypatch,
    include_profile,
):
    profile_fetch = (
        AsyncMock(side_effect=RuntimeError("synthetic profile outage"))
        if include_profile
        else AsyncMock(return_value={})
    )
    install_location_only_detail_mocks(monkeypatch, profile_fetch)
    operation_response = await npi_module.get_npi(
        _detail_request(
            limit="5",
            include_profile=include_profile,
            include_evidence=True,
        ),
        "1234567890",
    )
    response_map = json.loads(operation_response.body)

    assert response_map["npi"] == 1234567890
    assert response_map["address_list"][0]["address_key"] == ADDRESS_B
    assert "provider_directory_profile" not in response_map
    if not include_profile:
        profile_fetch.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("raw_limit", "expected_limit"),
    [(None, npi_module.NPI_DETAIL_ADDRESS_DEFAULT_LIMIT), ("all", None)],
)
async def test_profile_only_provider_continues_through_location_pipeline(
    monkeypatch,
    raw_limit,
    expected_limit,
):
    profile_fetch = AsyncMock(
        return_value={
            1234567890: {
                "profile": {"generation_id": "synthetic-generation"},
            }
        }
    )
    install_location_only_detail_mocks(
        monkeypatch,
        profile_fetch,
        overlay_locations=_overlay_payload(),
    )
    operation_response = await npi_module.get_npi(
        _detail_request(limit=raw_limit, include_profile=True),
        "1234567890",
    )
    response_map = json.loads(operation_response.body)

    assert [location["address_key"] for location in response_map["address_list"]] == [
        ADDRESS_A,
        ADDRESS_C,
        ADDRESS_B,
    ]
    assert response_map["address_pagination"] == {
        "limit": expected_limit,
        "offset": 0,
        "returned": 3,
        "total": 3,
        "has_more": False,
    }
    assert response_map["provider_directory_profile"]["generation_id"] == (
        "synthetic-generation"
    )


def test_overlay_query_derives_status_without_graph_metadata_parsing():
    sql = npi_module._provider_directory_overlay_query_sql({"lat", "long"})

    assert "AS location_status" in sql
    assert "PractitionerRole" in sql
    assert "publication_metadata_json" not in sql
    assert "dataset_network_plan_complete" not in sql
    assert "dataset_affiliation_organization_complete" not in sql
    assert "pg_input_is_valid" in sql
    assert "BOOL_AND(COALESCE" in sql


@pytest.mark.asyncio
async def test_unified_candidate_and_address_only_hydrator_share_location_identity(
    monkeypatch,
):
    location_mapping = unified_location_mapping()

    class RowsResult:
        def all(self):
            return [types.SimpleNamespace(_mapping=location_mapping)]

    monkeypatch.setattr(
        npi_module,
        "_address_serving_model",
        AsyncMock(return_value=npi_module.EntityAddressUnified),
    )
    monkeypatch.setattr(
        npi_module,
        "_table_columns",
        AsyncMock(return_value=set(location_mapping)),
    )
    execute_stmt = AsyncMock(return_value=RowsResult())
    monkeypatch.setattr(npi_module, "_execute_stmt", execute_stmt)

    candidates = await npi_module._fetch_npi_location_candidates(1234567890)
    identity = candidates[0]["_base_row_identities"][0]
    hydrated_locations = await npi_module._fetch_npi_address_rows(
        1234567890,
        include_evidence=True,
        address_row_identities=[identity],
    )

    assert identity == "location:synthetic-location-1"
    assert hydrated_locations[0]["_base_row_identities"] == [identity]
    assert hydrated_locations[0]["address_sources"] == ["synthetic_directory"]
    assert hydrated_locations[0]["source_record_ids"] == location_mapping[
        "source_record_ids"
    ]
    assert hydrated_locations[0]["aca_plan_array"] == ["synthetic-plan"]
    assert hydrated_locations[0]["address_site_key"] == SITE_A
    assert execute_stmt.await_count == 2


@pytest.mark.asyncio
async def test_bounded_detail_hydrates_only_selected_base_rows(monkeypatch):
    build_calls, address_row_calls = install_large_detail_mocks(
        monkeypatch,
        large_base_locations(),
    )
    operation_response = await npi_module.get_npi(
        _detail_request(limit="5"),
        "1234567890",
    )
    response_map = json.loads(operation_response.body)

    assert response_map["address_pagination"]["total"] == 1000
    assert response_map["address_pagination"]["returned"] == 5
    assert build_calls[0]["address_limit"] == 0
    assert len(build_calls) == 1
    assert len(address_row_calls) == 1
    assert len(address_row_calls[0]["address_row_identities"]) == 5


@pytest.mark.asyncio
async def test_bounded_hydration_uses_unique_unified_location_identity(monkeypatch):
    install_duplicate_detail_mocks(monkeypatch)
    operation_response = await npi_module.get_npi(
        _detail_request(limit="1"),
        "1234567890",
    )
    response_map = json.loads(operation_response.body)

    assert response_map["address_pagination"]["returned"] == 1
    assert len(response_map["address_list"]) == 1
    assert response_map["address_list"][0]["telephone_number"] == "2025550101"
    assert response_map["address_list"][0]["address_sources"] == [
        "synthetic-a",
        "synthetic-b",
    ]


def test_unkeyed_candidate_order_is_stable_across_input_order():
    first = _address(ADDRESS_A, "100 Example Avenue", source_id="nppes")
    second = _address(ADDRESS_B, "200 Example Avenue", source_id="nppes")
    first.pop("address_key")
    second.pop("address_key")
    first["_base_row_identities"] = ["legacy:1234567890:primary:1"]
    second["_base_row_identities"] = ["legacy:1234567890:primary:2"]

    forward = npi_module._rank_provider_locations([first, second])
    reverse = npi_module._rank_provider_locations([second, first])

    assert [row["first_line"] for row in forward] == [
        row["first_line"] for row in reverse
    ]


def test_address_datetime_types_and_naive_freshness_are_normalized():
    aware_value = datetime(2026, 8, 10, 12, 30, tzinfo=UTC)
    date_value = date(2026, 8, 10)

    assert npi_module._parse_address_datetime(aware_value) is aware_value
    assert npi_module._parse_address_datetime(date_value) == datetime(
        2026, 8, 10, tzinfo=UTC
    )
    assert npi_module._address_freshness_timestamp(
        {"updated_at": "2026-08-10T12:30:00"}
    ) == aware_value.timestamp()
    assert npi_module._address_freshness_timestamp(
        {"updated_at": aware_value}
    ) == aware_value.timestamp()


@pytest.mark.asyncio
async def test_location_status_lookup_normalizes_rows_and_empty_input(monkeypatch):
    query_result = _ResultRows(
        [
            types.SimpleNamespace(
                _mapping={
                    "source_record_id": "synthetic-role-a",
                    "location_status": "ACTIVE",
                }
            ),
            {"source_record_id": " ", "location_status": "inactive"},
            {"source_record_id": "synthetic-role-b", "location_status": None},
        ]
    )
    monkeypatch.setattr(
        npi_module,
        "_is_table_available",
        AsyncMock(return_value=True),
    )
    execute_stmt = AsyncMock(return_value=query_result)
    monkeypatch.setattr(npi_module, "_execute_stmt", execute_stmt)

    assert await npi_module._fetch_location_status_by_record_id(["", None]) == {}
    statuses = await npi_module._fetch_location_status_by_record_id(
        [" synthetic-role-b ", "synthetic-role-a", "synthetic-role-a"]
    )

    assert statuses == {
        "synthetic-role-a": "active",
        "synthetic-role-b": "unknown",
    }
    assert execute_stmt.await_args.kwargs["params"] == {
        "source_record_ids": ["synthetic-role-a", "synthetic-role-b"]
    }
    status_sql = str(execute_stmt.await_args.args[0])
    assert "PractitionerRole" in status_sql
    assert "matched_overlays AS MATERIALIZED" in status_sql
    assert "FROM matched_overlays AS overlay" in status_sql


@pytest.mark.asyncio
async def test_location_status_lookup_degrades_on_runtime_failure(monkeypatch):
    monkeypatch.setattr(
        npi_module,
        "_is_table_available",
        AsyncMock(side_effect=RuntimeError("synthetic status lookup failure")),
    )

    status_map = await npi_module._fetch_location_status_by_record_id(
        ["synthetic-role-a"]
    )

    assert status_map == {}


@pytest.mark.asyncio
async def test_legacy_location_queries_use_fallback_columns_and_address_key(
    monkeypatch,
):
    legacy_mapping = {
        column.key: None for column in npi_module.NPIAddress.__table__.columns
    }
    legacy_mapping.update(
        npi=1234567890,
        type="primary",
        first_line="100 Example Avenue",
        city_name="Example City",
        state_name="IL",
        postal_code="60001",
    )
    monkeypatch.setattr(
        npi_module,
        "_address_serving_model",
        AsyncMock(return_value=npi_module.NPIAddress),
    )
    monkeypatch.setattr(npi_module, "_table_columns", AsyncMock(return_value=set()))
    monkeypatch.setattr(
        npi_module,
        "_execute_stmt",
        AsyncMock(return_value=_ResultRows([legacy_mapping])),
    )

    candidates = await npi_module._fetch_npi_location_candidates(
        1234567890,
        address_key=ADDRESS_A,
    )
    hydrated = await npi_module._fetch_npi_address_rows(
        1234567890,
        address_key=ADDRESS_A,
    )

    assert candidates[0]["npi"] == 1234567890
    assert "_base_row_identities" not in candidates[0]
    assert hydrated[0]["first_line"] == "100 Example Avenue"
    assert "_base_row_identities" not in hydrated[0]


def test_legacy_identity_filter_and_hydrator_reject_unselected_row():
    address_table = npi_module.NPIAddress.__table__
    identities, identity_filter = npi_module._address_identity_filter(
        npi_module.NPIAddress,
        address_table,
        ["legacy:1234567890:primary:77", "legacy:invalid"],
    )
    selected_columns = [
        address_table.c.npi,
        address_table.c.type,
        address_table.c.checksum,
    ]
    query_result = _ResultRows(
        [{"npi": 1234567890, "type": "primary", "checksum": 77}]
    )
    hydrated = npi_module._hydrate_address_query_rows(
        query_result,
        selected_columns,
        1234567890,
        {"legacy:1234567890:primary:88"},
    )

    assert identities == {
        "legacy:1234567890:primary:77",
        "legacy:invalid",
    }
    assert identity_filter is not None
    assert hydrated == []


def test_inactive_duplicate_locations_preserve_inactive_status():
    base_location = _address(
        ADDRESS_A,
        "100 Example Avenue",
        source_id="synthetic-a",
        status="inactive",
    )
    duplicate_location = _address(
        ADDRESS_A,
        "100 Example Avenue",
        source_id="synthetic-b",
        status="inactive",
    )

    npi_module._merge_duplicate_address(base_location, duplicate_location)

    assert base_location["location_status"] == "inactive"


def test_identity_only_address_dedupes_deterministically():
    identity_only_location_map = {
        "type": "primary",
        "first_line": "100 Example Avenue",
        "_base_row_identities": ["legacy:1234567890:primary:77"],
    }
    assert npi_module._dedupe_addresses_by_key([identity_only_location_map]) == [
        identity_only_location_map
    ]
