# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compose configured provider reads with one server-pinned address scope."""

import json
from dataclasses import replace
from types import SimpleNamespace

import pytest
from sanic.request.parameters import RequestParameters
from sqlalchemy import text

from api import provider_geo_sql, provider_list_sql
from api.endpoint import npi
from api.network_address_scope import network_address_read_scope, scoped_address_relation_sql
from process.network_serving_read import PinnedNetworkServingManifest
from tests import test_custom_import_provider_geo_sql as geo_fixture
from tests import test_custom_import_provider_list as list_fixture

_CANDIDATE = "00000000-0000-0000-0000-000000000001"
_MANIFEST = PinnedNetworkServingManifest(
    7,
    _CANDIDATE,
    "network_candidate_" + _CANDIDATE.replace("-", ""),
    1,
    {"unified_address": "synthetic-version"},
    0,
    "a" * 64,
    17,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("direction", [None, "asc"])
@pytest.mark.parametrize("empty", [False, True])
async def test_configured_page_keeps_filtered_total_for_empty_page(monkeypatch, direction, empty):
    class Connection(list_fixture._TransactionConnection):
        async def all(self, statement, **parameters):
            if empty and "page_npis AS" in str(statement):
                self.calls.append((str(statement), dict(parameters), statement))
                return [SimpleNamespace(_mapping={"_provider_total": 501})]
            return await super().all(statement, **parameters)

    connection = Connection()
    session = list_fixture._Session(connection)
    address_selector = npi._address_serving_table_sql
    list_fixture._configure_imported_page_dependencies(monkeypatch)
    monkeypatch.setattr(npi, "_address_serving_table_sql", address_selector)

    async def empty_enrichment(npis, **kwargs):
        assert npis == ([] if empty else [1000000002])
        assert kwargs["session"] is session
        return {}

    monkeypatch.setattr(npi, "_fetch_provider_enrichment_summary_map", empty_enrichment)
    context = list_fixture._import_context(direction=direction, require_match=True)
    request = SimpleNamespace(args={}, ctx=SimpleNamespace(sa_session=session))
    args = RequestParameters({"include_total": ["true"], "limit": ["1"], "start": ["0"], "phone": ["2125550100"]})
    with network_address_read_scope(_MANIFEST, (42,)):
        reply = await npi.list_providers(request, native_args=args, import_context=context)
        relation = scoped_address_relation_sql("mrf.entity_address_unified")
    body = json.loads(reply.body)
    assert body["total"] == 501
    assert [provider["npi"] for provider in body["rows"]] == ([] if empty else [1000000002])
    page_sql, parameters, _ = connection.calls[0]
    assert relation in page_sql
    assert page_sql.index("canonical_network_ids &&") < page_sql.index("LIMIT :limit")
    assert "provider_totals LEFT JOIN sub_s ON TRUE" in page_sql
    assert parameters["_canonical_network_ids"] == [42]
    assert set(context.compiled.values) <= set(parameters)
    assert not any("SELECT COUNT(DISTINCT" in call[0] for call in connection.calls)


@pytest.mark.parametrize("direction", ["asc", "desc"])
def test_configured_geo_keeps_typed_membership_scope_before_total_rank_and_cursor(direction):
    context = geo_fixture._context(direction=direction, require_match=True)
    query = geo_fixture._imported_geo_query(anchor=("1000000003", geo_fixture._THIRD_ADDRESS_KEY))
    with network_address_read_scope(_MANIFEST, (42,)):
        query = replace(
            query,
            nearby=replace(query.nearby, address_table_sql=scoped_address_relation_sql("mrf.entity_address_unified")),
        )
        statements = provider_geo_sql.build_imported_geo_statements(context, query)
    assert statements.parameters["_canonical_network_ids"] == [42]
    assert set(context.compiled.values) <= set(statements.parameters)
    page = str(statements.page)
    assert page.index("canonical_network_ids &&") < page.index("geo_totals AS") < page.index("page_geo AS")
    assert "a.location_key ASC" in page
    assert "a.location_key AS _geo_address_location_key" in page
    assert "a.location_key = page_geo._geo_address_location_key" in page
    assert "_geo_address_checksum" not in page and "_geo_address_type" not in page
    assert "EXISTS (SELECT 1 FROM custom_import_provider_relation" in page
    assert "AS _geo_total" in page and "AS _geo_anchor_count" in page


@pytest.mark.asyncio
async def test_native_scoped_list_uses_the_existing_request_session(monkeypatch):
    class Result:
        def all(self):
            return [(1,)]

    calls = []

    async def execute(statement, parameters):
        calls.append((statement, parameters))
        return Result()

    session = SimpleNamespace(execute=execute)
    database = SimpleNamespace(acquire=lambda: (_ for _ in ()).throw(AssertionError("unscoped pool acquired")))
    with network_address_read_scope(_MANIFEST, (42,)):
        async with provider_list_sql._provider_list_connection(database, None, session) as connection:
            assert await connection.all(text("SELECT 1"), **provider_list_sql._provider_list_parameters({}, None)) == [
                (1,)
            ]
    assert len(calls) == 1 and calls[0][1] == {"_canonical_network_ids": [42]}


def test_filtered_unified_relation_retains_exact_geo_types_and_full_attribution(monkeypatch):
    monkeypatch.setattr(npi, "_should_include_geo_service_locations", lambda: True)
    provider_by_field = {
        "npi": 1111111111,
        "entity_type_code": 1,
        "type": "practice",
        "address_sources": ["synthetic"],
        "ptg_plan_array": ["synthetic-plan"],
        "source_count": 2,
        "canonical_network_ids": [42],
    }
    with network_address_read_scope(_MANIFEST, (42,)):
        relation = scoped_address_relation_sql("mrf.entity_address_unified")
        assert npi._nearby_geo_type_clause(relation) == npi._nearby_geo_type_clause("mrf.entity_address_unified")
        assert npi._exact_geo_precision_clause(relation) == npi._exact_geo_precision_clause(
            "mrf.entity_address_unified"
        )
        provider = npi._new_provider_from_search_mapping(provider_by_field, 1111111111, relation)
        near_provider_by_field = {}
        npi._populate_near_provider_mapping(
            near_provider_by_field, provider_by_field, 1111111111, "synthetic-address", relation
        )
    for projection_by_field in (provider, near_provider_by_field):
        assert projection_by_field["address_sources"] == ["synthetic"]
        assert projection_by_field["ptg_plan_array"] == ["synthetic-plan"] and projection_by_field["source_count"] == 2
        assert "canonical_network_ids" not in projection_by_field


@pytest.mark.asyncio
@pytest.mark.parametrize("imported", [False, True])
async def test_scoped_geo_handler_binds_one_pinned_source_before_count_and_page(monkeypatch, imported):
    session = SimpleNamespace(calls=[])
    cursor_calls = []

    class Proxy(geo_fixture._RecordingGeoProxy):
        async def all(self, statement, **parameters):
            sql = str(statement)
            if imported:
                return await super().all(statement, **parameters)
            self._session.calls.append((sql, dict(parameters)))
            if "COUNT" in sql.upper():
                return [SimpleNamespace(_mapping={"total_count": 3})]
            return [
                geo_fixture._geo_row(1000000004, geo_fixture._FOURTH_ADDRESS_KEY, 10.0),
                geo_fixture._geo_row(1000000003, geo_fixture._THIRD_ADDRESS_KEY, 20.0),
                geo_fixture._geo_row(1000000002, geo_fixture._SECOND_ADDRESS_KEY, 30.0),
            ]

    async def prepare_cursor(**kwargs):
        cursor_calls.append(kwargs)
        return None

    monkeypatch.setattr(provider_list_sql, "ConnectionProxy", Proxy)
    monkeypatch.setattr(npi, "_plan_release_npi_scope", geo_fixture._empty_plan_scope)
    monkeypatch.setattr(npi.db, "acquire", lambda: (_ for _ in ()).throw(AssertionError("unscoped pool acquired")))
    args = RequestParameters({"lat": ["0"], "long": ["0"], "include_total": ["true"], "limit": ["2"]})
    with network_address_read_scope(_MANIFEST, (42,)):
        reply = await npi.get_near_npi(
            SimpleNamespace(args={} if imported else args, ctx=SimpleNamespace(sa_session=session)),
            native_args=args if imported else None,
            import_context=geo_fixture._context() if imported else None,
            prepare_cursor=prepare_cursor if imported else None,
        )
    body = json.loads(reply.body)
    assert body["total_count"] == 3 and body["has_more"] is True
    assert [provider["npi"] for provider in body["items"]] == [1000000004, 1000000003]
    assert len(session.calls) == (1 if imported else 2)
    for sql, parameters in session.calls:
        assert "canonical_network_ids &&" in sql
        assert parameters["_canonical_network_ids"] == [42]
    if imported:
        assert cursor_calls[0]["query_parameters"]["_canonical_network_ids"] == [42]
        assert body["_custom_import_next_anchor"] == ["1000000003", geo_fixture._THIRD_ADDRESS_KEY]


@pytest.mark.asyncio
async def test_scoped_native_query_refuses_missing_session_before_pool_acquisition():
    database = SimpleNamespace(acquire=lambda: (_ for _ in ()).throw(AssertionError("unscoped pool acquired")))
    with network_address_read_scope(_MANIFEST, (42,)):
        with pytest.raises(RuntimeError, match="requires a request session"):
            async with provider_list_sql._provider_list_connection(database, None, None):
                pytest.fail("missing request transaction accepted")


def test_scoped_geo_refuses_a_conflicting_native_network_binding():
    with network_address_read_scope(_MANIFEST, (42,)):
        with pytest.raises(ValueError, match="network_address_reserved_parameter"):
            provider_geo_sql.build_imported_geo_statements(
                geo_fixture._context(),
                geo_fixture._imported_geo_query(native_parameters={"_canonical_network_ids": [43]}),
            )
