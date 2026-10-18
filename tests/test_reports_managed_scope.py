# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pharmacy reports preserve exact offices and existing managed policy custody."""

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic import response
from sanic.exceptions import InvalidUsage
from sanic.request.parameters import RequestParameters

from api import provider_collection_network_read as collection
from api.endpoint import reports
from api.network_address_scope import network_address_read_scope, scoped_address_parameters
from api.provider_collection_network_contract import parse_managed_collection_query
from process.network_serving_read import PinnedNetworkServingManifest

MANIFEST = PinnedNetworkServingManifest(
    9,
    "00000000-0000-0000-0000-000000000001",
    "network_candidate_00000000000000000000000000000001",
    1,
    {},
    8,
    "a" * 64,
    100,
)
CASES = [
    ("pharmacy-markets", {}),
    ("pharmacy-market", {"market_id": "state:NY"}),
    ("pharmacy-rankings", {}),
    ("pharmacy-chain-summary", {"name_like": ["%"]}),
    ("pharmacy-state-stats", {}),
    ("pharmacy-context", {"npi": "1111111111"}),
]


@pytest.mark.parametrize("kind,native", CASES)
def test_closed_report_kinds_reuse_collection_selector_and_retained_generation(kind, native):
    query = parse_managed_collection_query(
        {
            "kind": kind,
            "native_query": native,
            "selector": {"namespace": "canonical", "values": [42]},
            "network_generation": "9",
        }
    )
    assert query.kind == kind and query.selector == (42,) and query.network_generation == 9
    for injected in ({"network_ids": "42"}, {"_network_approved_revision": "9"}, {"sql": "SELECT 1"}):
        with pytest.raises(ValueError):
            parse_managed_collection_query({"kind": kind, "native_query": {**native, **injected}})


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["selected", "default"])
async def test_managed_reports_pin_relation_and_live_staffing_without_global_fallback(monkeypatch, mode):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "legacy")
    availability = AsyncMock(side_effect=AssertionError("managed addresses/staffing cannot probe global fallback"))
    monkeypatch.setattr(reports, "_is_table_available", availability)
    options = {"excluded_network_ids": (88,)} if mode == "default" else {}
    with network_address_read_scope(
        MANIFEST, None if mode == "default" else (42,), access_scope_sha256="b" * 64, **options
    ):
        relation = await reports._resolve_pharmacy_address_table_sql(object())
        assert MANIFEST.schema_name in relation and "WHERE" in relation
        assert reports._is_pharmacy_address_unified(relation)
        assert reports._pharmacy_address_zip5_expr("a", relation) == "a.zip5"
        sql, params = reports._build_chain_summary_sql(
            names=["%"], has_staffing_helper=False, include_states=True, address_table_sql=relation
        )
        assert sql.count(relation) == 2 and "practice','site" in sql
        assert "plans_network_array" not in sql and "canonical_network_ids" in sql
        assert sql.index(relation) < sql.index("summary AS")
        state_sql = reports._build_pharmacy_state_stats_sql(has_staffing_helper=False, address_table_sql=relation)
        assert "aca_binding" in state_sql and "source_system'='aca'" in state_sql
        assert "aca_pharmacies" in state_sql and "plans_network_array" not in state_sql
        assert "ACA-bound canonical" in str(reports._pharmacy_state_stats_methodology(staffing_helper_available=False))
        parameters = scoped_address_parameters(params)
        assert parameters.get("_canonical_network_ids", parameters.get("_network_excluded_ids")) == (
            [42] if mode == "selected" else [88]
        )
        count_sql, data_sql, _ = reports._build_market_sql(
            scope="state",
            sort="access_score",
            order="desc",
            include_staffing=True,
            has_partd=False,
            has_license=False,
            has_other_id=False,
            market_id_filter=None,
            state=None,
            city=None,
            county=None,
            zip_code=None,
            chain=None,
            address_table_sql=relation,
        )
        assert relation in count_sql and relation in data_sql and "{address_table_sql}" not in data_sql
        assert data_sql.index(relation) < data_sql.index("LIMIT :limit")
    availability.assert_not_awaited()


@pytest.mark.asyncio
async def test_protected_collection_dispatch_keeps_native_report_args_and_same_scope(monkeypatch):
    # Exercise the real existing dispatcher; the surrounding reader is covered below.
    query = parse_managed_collection_query({"kind": "pharmacy-context", "native_query": {"npi": "1111111111"}})
    args, batch = collection._validated_inputs(query)
    assert batch is None
    handler = AsyncMock(return_value=response.json({"pharmacy": {"npi": 1111111111}}))
    monkeypatch.setattr(reports, "get_pharmacy_market_context", handler)
    request = SimpleNamespace(args={"npi": "2222222222"}, ctx=SimpleNamespace())
    with network_address_read_scope(MANIFEST, (42,)):
        document = await collection._collection_data(request, query, object(), args, None)
    assert document == {"pharmacy": {"npi": 1111111111}}
    assert handler.await_args.args == (request, "1111111111")
    assert handler.await_args.kwargs["native_args"].get("npi") == "1111111111"


@pytest.mark.asyncio
@pytest.mark.parametrize("kind,native", CASES)
async def test_report_collection_uses_real_existing_policy_snapshot_enrollment(monkeypatch, kind, native):
    from api.endpoint import npi as provider
    from api.network_address_scope import current_network_address_scope

    session = object()
    events = []

    @asynccontextmanager
    async def snapshot(database, schema, *, include_detail):
        assert database is provider.db and include_detail is True
        events.append("snapshot")
        yield session
        events.append("closed")

    @asynccontextmanager
    async def scope(actual, **options):
        assert actual is session and options["excluded_network_ids"] == (88,)
        assert options["network_generation"] == 9
        with network_address_read_scope(MANIFEST, None, excluded_network_ids=(88,), access_scope_sha256="b" * 64):
            yield MANIFEST

    async def dispatch(request, actual_kind, args):
        assert request.ctx.sa_session is session and current_network_address_scope().manifest is MANIFEST
        assert actual_kind == kind
        return response.json({"report": kind})

    monkeypatch.setattr(collection, "provider_profile_read_snapshot", snapshot)
    monkeypatch.setattr(collection, "managed_provider_network_read_scope", scope)
    monkeypatch.setattr(reports, "read_managed_pharmacy_report", dispatch)
    request = SimpleNamespace(ctx=SimpleNamespace(sa_session="prior"))
    query = parse_managed_collection_query({"kind": kind, "native_query": native, "network_generation": "9"})
    serving_document = await collection.read_managed_provider_collection(
        request, query, excluded_network_ids=(88,), access_scope_sha256="b" * 64
    )
    assert serving_document["generation"] == "9" and serving_document["data"] == {"report": kind}
    assert request.ctx.sa_session == "prior" and events == ["snapshot", "closed"]


@pytest.mark.asyncio
async def test_raw_get_selector_and_unbound_native_reports_fail_closed():
    request = SimpleNamespace(args={}, query_string="network_ids=42", ctx=SimpleNamespace())
    with pytest.raises(InvalidUsage):
        await reports.list_pharmacy_markets(request)
    with pytest.raises(InvalidUsage):
        await reports.read_managed_pharmacy_report(request, "pharmacy-state-stats", RequestParameters({}))


from tests.test_registry_provider_collection_routes import collection_context as collection_context


@pytest.mark.asyncio
@pytest.mark.parametrize("kind,native", CASES)
@pytest.mark.parametrize("selected", [False, True])
async def test_report_http_collection_preserves_policy_and_manifest_headers(collection_context, kind, native, selected):
    from tests.test_network_catalog_routes import v2_envelope
    from tests.test_registry_provider_collection_routes import _post

    query_by_name = {"kind": kind, "native_query": native, "network_generation": "9"}
    if selected:
        query_by_name["selector"] = {"namespace": "canonical", "values": [42]}
    document = v2_envelope(query_by_name)
    reply = await _post(collection_context, document)
    assert reply.status == 200
    assert reply.headers["X-Network-Generation"] == "9"
    assert reply.headers["X-Network-Access-Scope"] == document["access_scope_sha256"]
    assert reply.headers["Cache-Control"] == "private, no-store"
    query_dto = collection_context[1].await_args.args[1]
    assert query_dto.kind == kind and query_dto.network_generation == 9
    assert query_dto.selector == ((42,) if selected else None)
    collection_context[1].reset_mock()
    document["access_scope_sha256"] = "0" * 64
    assert (await _post(collection_context, document)).status == 400
    collection_context[1].assert_not_awaited()
    assert (await _post(collection_context, document, headers={"Authorization": "Bearer wrong"})).status == 403
    collection_context[1].assert_not_awaited()
