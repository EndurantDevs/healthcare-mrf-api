# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Canonical general geo query composition and unsupported relationship refusal."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from sanic.exceptions import InvalidUsage

from api import provider_list_sql
from api.endpoint import npi as npi_module
from api.network_address_scope import network_address_read_scope
from process.network_serving_read import PinnedNetworkServingManifest
from tests.network_read_execution_support import with_read_identity_receipts
from tests.npi_location_hydration_support import unified_location_mapping
from tests.test_npi_facility_providers_api import FakeSession, _facility_query_results


def _guarded_query_session(query):
    """Buffer semantic SQL rows while retaining the original transaction guard."""
    session = with_read_identity_receipts(SimpleNamespace())

    async def execute(statement, parameters):
        rows = await query(statement, **parameters)
        return SimpleNamespace(all=lambda: rows)

    session.execute = execute
    return session


@pytest.mark.asyncio
@pytest.mark.parametrize("entity_type_code", ("1", "2"))
@pytest.mark.parametrize("empty_result", (False, True))
async def test_general_geo_page_and_count_bind_retained_offices(monkeypatch, entity_type_code, empty_result):
    """Pin page and count SQL before spatial and entity predicates."""
    candidate_id = uuid4()
    manifest = PinnedNetworkServingManifest(
        12, str(candidate_id), "network_candidate_" + candidate_id.hex, 1, {}, 0, "c" * 64, 1
    )
    statements = []
    location_by_field = {
        **unified_location_mapping(),
        "npi": 1000000004,
        "inferred_npi": None,
        "entity_type_code": entity_type_code,
        "location_key": "a" * 64,
        "canonical_network_ids": [42],
        "cursor_distance_meters": 100.0,
        "distance": 0.06,
    }

    async def query(statement, **parameters):
        sql = str(statement)
        statements.append((sql, parameters))
        assert f'FROM ONLY "{manifest.schema_name}".entity_address_unified' in sql
        assert "WHERE canonical_network_ids && CAST(:_canonical_network_ids AS INTEGER[])" in sql
        assert parameters["_canonical_network_ids"] == [42]
        assert parameters["entity_type_code"] == int(entity_type_code)
        assert "d.entity_type_code = :entity_type_code" in sql
        assert "provider_directory_address_overlay" not in sql
        if "COUNT(DISTINCT (a.npi, a.address_key))" in sql:
            assert "LIMIT" not in sql
            return [(0 if empty_result else 1,)]
        assert sql.index("WHERE canonical_network_ids") < sql.index("LIMIT :limit")
        assert "a.location_key ASC" in sql
        return [] if empty_result or len(statements) > 1 else [SimpleNamespace(_mapping=location_by_field)]

    session = _guarded_query_session(query)
    proxy = Mock(wraps=provider_list_sql.NetworkReadConnectionProxy)
    monkeypatch.setattr(provider_list_sql, "NetworkReadConnectionProxy", proxy)
    monkeypatch.setattr(npi_module, "db", SimpleNamespace(acquire=Mock(side_effect=AssertionError("unpinned query"))))
    monkeypatch.setattr(npi_module, "_plan_release_npi_scope", AsyncMock(return_value=(None, {})))
    request = SimpleNamespace(
        args={"lat": "41", "long": "-87", "entity_type_code": entity_type_code, "include_total": "1", "limit": "1"},
        ctx=SimpleNamespace(sa_session=session),
        app=SimpleNamespace(),
    )
    with network_address_read_scope(manifest, (42,)):
        response_by_field = json.loads((await npi_module.get_near_npi(request)).body)
    assert len(statements) == (2 if empty_result else 3)
    assert proxy.called and all(call.args[1] is session for call in proxy.call_args_list)
    assert response_by_field["total_count"] == (0 if empty_result else 1)
    assert response_by_field["has_more"] is False and response_by_field["next_cursor"] is None
    assert response_by_field["result_identity"] == ["npi", "address_key"]
    if empty_result:
        assert response_by_field["items"] == []
    else:
        assert [
            (provider_by_field["npi"], provider_by_field["address_key"])
            for provider_by_field in response_by_field["items"]
        ] == [(1000000004, location_by_field["address_key"])]
        assert "canonical_network_ids" not in response_by_field["items"][0]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "query_string",
    (
        "network_ids=42",
        "network_ids=",
        "network_ids=42&network_generation=12",
        "network_generation=12",
        "network_ids=42&network%5Fids=42",
        "network_ids=42&plan_network_checksum=42",
    ),
)
async def test_facility_relationships_refuse_canonical_selectors_before_reads(monkeypatch, query_string):
    """Enrollment rows cannot establish a selected provider's exact network office."""
    availability = AsyncMock(side_effect=AssertionError("unfiltered relationship lookup"))
    monkeypatch.setattr(npi_module, "_is_table_available", availability)
    session = FakeSession([])
    request = SimpleNamespace(
        query_string="ccn=123456&" + query_string,
        args={"ccn": "123456"},
        ctx=SimpleNamespace(sa_session=session),
    )
    with pytest.raises(InvalidUsage):
        await npi_module.get_facility_connected_providers(request)
    availability.assert_not_awaited()
    assert session.calls == []


@pytest.mark.asyncio
async def test_facility_relationships_preserve_legacy_request(monkeypatch):
    """An ordinary enrollment locator keeps its existing result and query shape."""
    session = FakeSession(_facility_query_results())
    monkeypatch.setattr(npi_module, "_is_table_available", AsyncMock(return_value=True))
    request = SimpleNamespace(
        query_string="ccn=123456&limit=25&offset=0",
        args={"ccn": "123456", "limit": "25", "offset": "0"},
        ctx=SimpleNamespace(sa_session=session),
    )
    response_by_field = json.loads((await npi_module.get_facility_connected_providers(request)).body)
    assert response_by_field["total_providers"] == 2
    assert len(response_by_field["providers"]) == 2 and len(session.calls) == 4


@pytest.mark.asyncio
@pytest.mark.parametrize("count_rows,expected", [([], 0), ([(7,)], 7)])
async def test_canonical_geo_count_handles_empty_and_positional_results(monkeypatch, count_rows, expected):
    from tests.test_network_read_execution import _manifest

    statements = []

    async def query(statement, **parameters):
        statements.append(str(statement))
        assert parameters["_canonical_network_ids"] == [42]
        assert "COUNT(DISTINCT (a.npi, a.address_key))" in str(statement)
        assert "LIMIT" not in str(statement)
        return count_rows

    session = _guarded_query_session(query)
    monkeypatch.setattr(npi_module, "db", SimpleNamespace(acquire=Mock(side_effect=AssertionError("unpinned read"))))
    monkeypatch.setattr(npi_module, "_plan_release_npi_scope", AsyncMock(return_value=(None, {})))
    metadata = AsyncMock(side_effect=AssertionError("count cannot hydrate provider data"))
    monkeypatch.setattr(npi_module, "_project_provider_network_metadata", metadata)
    request = SimpleNamespace(
        args={"lat": "41", "long": "-87", "count_only": "1"},
        ctx=SimpleNamespace(sa_session=session),
        app=SimpleNamespace(),
    )
    with network_address_read_scope(_manifest(), (42,)):
        assert json.loads((await npi_module.get_near_npi(request)).body) == {"rows": expected}
    assert len(statements) == 1
    metadata.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "malformed",
    [SimpleNamespace(_mapping={"address_key": "a" * 64}), SimpleNamespace(_mapping={"npi": 1000000004}), (1,)],
)
async def test_canonical_geo_never_invents_provider_or_office_identity(monkeypatch, malformed):
    from tests.test_network_read_execution import _manifest

    async def query(statement, **parameters):
        assert parameters["_canonical_network_ids"] == [42]
        if "COUNT(DISTINCT" in str(statement):
            return [(0,)]
        return [malformed]

    session = _guarded_query_session(query)
    monkeypatch.setattr(npi_module, "db", SimpleNamespace(acquire=Mock(side_effect=AssertionError("unpinned read"))))
    monkeypatch.setattr(npi_module, "_plan_release_npi_scope", AsyncMock(return_value=(None, {})))
    request = SimpleNamespace(
        args={"lat": "41", "long": "-87", "limit": "1", "include_total": "1"},
        ctx=SimpleNamespace(sa_session=session),
        app=SimpleNamespace(),
    )
    with network_address_read_scope(_manifest(), (42,)):
        document = json.loads((await npi_module.get_near_npi(request)).body)
    assert document["items"] == [] and document["has_more"] is False


@pytest.mark.asyncio
async def test_geo_cursor_from_another_pinned_network_generation_refuses_before_read(monkeypatch):
    from tests.test_network_read_execution import _manifest

    original = _manifest()
    successor = _manifest()
    object.__setattr__(successor, "generation_id", 2)
    object.__setattr__(successor, "manifest_sha256", "b" * 64)
    arguments_by_name = {"lat": "41", "long": "-87", "limit": "1"}
    with network_address_read_scope(original, (42,)):
        cursor = npi_module._encode_nearby_cursor(
            npi_module._nearby_cursor_scope(arguments_by_name),
            100,
            1000000004,
            "00000000-0000-0000-0000-000000000001",
        )
    query = AsyncMock(side_effect=AssertionError("stale cursor reached storage"))
    monkeypatch.setattr(npi_module, "_execute_stmt", query)
    monkeypatch.setattr(npi_module, "_plan_release_npi_scope", AsyncMock(return_value=(None, {})))
    request = SimpleNamespace(args={**arguments_by_name, "cursor": cursor}, app=SimpleNamespace())
    with network_address_read_scope(successor, (42,)), pytest.raises(InvalidUsage, match="cursor"):
        await npi_module.get_near_npi(request)
    query.assert_not_awaited()
