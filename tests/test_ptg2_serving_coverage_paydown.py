# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from tests.ptg2_serving_coverage_paydown_support import (
    FakeResult,
    FakeSession,
    strict_v3_tables,
)


def _location_query(*, knn_order_sql=None):
    return serving._MembershipLocationQuery(
        address_table="mrf.entity_address_unified",
        npi_scope_table="mrf.ptg2_v3_npi_scope",
        filter_sql="npi_scope.snapshot_key = :shared_snapshot_key",
        parameter_map={"limit": 2},
        distance_sql="NULL::double precision",
        knn_order_sql=knn_order_sql,
    )


def test_membership_filter_rejects_empty_scope_and_invalid_values():
    assert (
        serving._membership_filter_sql(
            {},
            candidate_npis=(),
            uses_unified_addresses=False,
            address_zip5_sql="LEFT(addr.postal_code, 5)",
            parameter_map={},
        )
        is None
    )
    assert (
        serving._membership_filter_sql(
            {"radius_miles": "5"},
            candidate_npis=None,
            uses_unified_addresses=False,
            address_zip5_sql="LEFT(addr.postal_code, 5)",
            parameter_map={},
        )
        is None
    )
    assert (
        serving._membership_filter_sql(
            {"npi": "not-an-npi"},
            candidate_npis=None,
            uses_unified_addresses=False,
            address_zip5_sql="LEFT(addr.postal_code, 5)",
            parameter_map={},
        )
        is None
    )


def test_membership_filter_supports_literal_address_and_text_location_filters():
    parameter_map = {}

    filter_sql, distance_sql = serving._membership_filter_sql(
        {
            "state": " il ",
            "city": " chicago ",
            "zip": "60601-1234",
            "npi": "1234567890",
        },
        candidate_npis=None,
        uses_unified_addresses=False,
        address_zip5_sql="LEFT(addr.postal_code, 5)",
        parameter_map=parameter_map,
        literal_service_address_types=True,
        include_taxonomy_filters=False,
    )

    assert "addr.type IN ('primary', 'secondary', 'practice', 'site')" in filter_sql
    assert "state_value" in filter_sql
    assert "city_value" in filter_sql
    assert "LEFT(addr.postal_code, 5) = :zip5" in filter_sql
    assert "addr.npi = :provider_npi" in filter_sql
    assert distance_sql == "NULL::double precision"
    assert parameter_map == {
        "state_value": "IL",
        "city_value": "CHICAGO",
        "zip5": "60601",
        "provider_npi": 1234567890,
    }


def test_membership_filter_appends_geo_clauses_without_zip(monkeypatch):
    monkeypatch.setattr(
        serving,
        "_membership_taxonomy_filters",
        lambda _args, _parameters: ["taxonomy_matches"],
    )
    monkeypatch.setattr(
        serving,
        "_membership_geo_sql",
        lambda *_args, **_kwargs: ("distance_expression", ["geo_matches"]),
    )
    parameter_map = {}

    filter_sql, distance_sql = serving._membership_filter_sql(
        {},
        candidate_npis=(1234567890,),
        uses_unified_addresses=True,
        address_zip5_sql="addr.zip5",
        parameter_map=parameter_map,
    )

    assert "taxonomy_matches" in filter_sql
    assert "geo_matches" in filter_sql
    assert parameter_map["candidate_npis"] == [1234567890]
    assert distance_sql == "distance_expression"


@pytest.mark.asyncio
async def test_membership_location_rows_short_circuits_empty_and_unavailable_queries(
    monkeypatch,
):
    query_builder = AsyncMock(return_value=None)
    monkeypatch.setattr(serving, "_membership_location_query", query_builder)

    assert (
        await serving._membership_location_rows(
            object(),
            strict_v3_tables(),
            {},
            candidate_npis=(),
            limit=2,
        )
        == []
    )
    query_builder.assert_not_awaited()

    assert (
        await serving._membership_location_rows(
            object(),
            strict_v3_tables(),
            {},
            candidate_npis=None,
            limit=2,
        )
        is None
    )
    query_builder.assert_awaited_once()


@pytest.mark.asyncio
async def test_membership_location_rows_executes_standard_query(monkeypatch):
    monkeypatch.setattr(
        serving,
        "_membership_location_query",
        AsyncMock(return_value=_location_query()),
    )
    session = FakeSession([FakeResult([{"npi": 1234567890}])])

    location_rows = await serving._membership_location_rows(
        session,
        strict_v3_tables(),
        {},
        candidate_npis=None,
        limit=2,
        offset=3,
    )

    assert location_rows == [{"npi": 1234567890}]
    assert "raw_probe_limit" not in session.calls[0][0][1]


@pytest.mark.asyncio
async def test_membership_location_rows_bounds_knn_and_restores_planner(monkeypatch):
    query = _location_query(knn_order_sql="addr.location <-> :request_location")
    monkeypatch.setattr(
        serving,
        "_membership_location_query",
        AsyncMock(return_value=query),
    )
    enable = AsyncMock(return_value=("auto", "2"))
    restore = AsyncMock()
    monkeypatch.setattr(serving, "_enable_serial_knn_planning", enable)
    monkeypatch.setattr(serving, "_restore_knn_planning", restore)
    session = FakeSession([FakeResult([{"npi": 1234567890}])])

    location_rows = await serving._membership_location_rows(
        session,
        strict_v3_tables(),
        {},
        candidate_npis=None,
        limit=2,
    )

    assert location_rows == [{"npi": 1234567890}]
    assert query.parameter_map["raw_probe_limit"] == 67
    enable.assert_awaited_once_with(session)
    restore.assert_awaited_once_with(session, ("auto", "2"))


@pytest.mark.asyncio
async def test_membership_location_rows_preserves_knn_query_failure(monkeypatch):
    monkeypatch.setattr(
        serving,
        "_membership_location_query",
        AsyncMock(return_value=_location_query(knn_order_sql="knn_order")),
    )
    monkeypatch.setattr(
        serving,
        "_enable_serial_knn_planning",
        AsyncMock(return_value=("auto", "2")),
    )
    restore = AsyncMock()
    monkeypatch.setattr(serving, "_restore_knn_planning", restore)
    session = FakeSession([RuntimeError("query failed")])

    with pytest.raises(RuntimeError, match="query failed"):
        await serving._membership_location_rows(
            session,
            strict_v3_tables(),
            {},
            candidate_npis=None,
            limit=1,
        )

    restore.assert_not_awaited()
