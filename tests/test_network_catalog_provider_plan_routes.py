# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protected provider-plan transport with policy admission before database work."""

import asyncio
import json
from uuid import UUID

import pytest

from api.endpoint import network_catalog as endpoint
from process.registry_network_catalog_read import RegistryNetworkCatalogError
from tests.test_network_catalog_routes import _OWNED_READ_EVENTS, envelope, send, v2_envelope
from tests.test_network_catalog_routes import batch_boundary as batch_boundary
from tests.test_network_catalog_routes import context as context

_PATH = "/api/v1/registry/serving/providers/plans"


@pytest.fixture
def plans_context(context, monkeypatch):
    context.plan_calls = []
    context.plan_page = {
        "generation": "9",
        "approved_custom_revision": "4",
        "npi_data": [],
        "plan_data": [],
        "issuer_data": [],
    }

    async def read(connection, query, *, excluded_network_ids):
        assert connection is context.driver and connection.in_transaction
        context.plan_calls.append((query, excluded_network_ids))
        return context.plan_page

    monkeypatch.setattr(endpoint, "read_registry_provider_plans", read, raising=False)
    return context


@pytest.mark.asyncio
@pytest.mark.parametrize("headers", [None, {"X-HealthPorta-Control-Token": "synthetic-catalog-control-token"}])
async def test_provider_plans_full_policy_and_one_snapshot(plans_context, headers):
    context = plans_context
    identity = str(UUID(int=1))
    document = v2_envelope(
        {"npi": "0000000001", "year": 2000, "network_generation": "9"},
        policy_revision="12",
        excluded_network_ids=[84],
        excluded_company_ids=[identity],
        excluded_group_ids=[identity],
    )
    result = await send(context, document, path=_PATH, headers=headers)
    assert result.status == 200
    assert result.json == {"client_id": "sample-client", "policy_revision": "12"} | context.plan_page
    assert result.headers["X-Network-Generation"] == "9"
    assert result.headers["X-Network-Access-Scope"] == document["access_scope_sha256"]
    query, exclusions = context.plan_calls[0]
    assert (query.npi, query.year, query.generation_id, exclusions) == ("0000000001", 2000, 9, (84,))
    assert len(context.plan_calls) == 1 and context.driver.calls == []
    assert context.session.events == _OWNED_READ_EVENTS
    assert len(context.session.snapshot_queries) == 3


@pytest.mark.asyncio
async def test_provider_plans_default_policy_preserves_empty_success(plans_context):
    result = await send(plans_context, v2_envelope({"npi": "1000000491"}), path=_PATH)
    assert result.status == 200 and result.json["npi_data"] == []
    query, exclusions = plans_context.plan_calls[0]
    assert (query.year, query.generation_id, exclusions) == (None, None, ())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "query",
    [
        {},
        {"npi": 1000000491},
        {"npi": "１００００００４９１"},
        {"npi": "100000049"},
        {"npi": "1000000491", "year": None},
        {"npi": "1000000491", "year": True},
        {"npi": "1000000491", "year": "2026"},
        {"npi": "1000000491", "year": 1999},
        {"npi": "1000000491", "year": 2101},
        {"npi": "1000000491", "network_generation": None},
        {"npi": "1000000491", "network_generation": 9},
        {"npi": "1000000491", "network_generation": "09"},
        {"npi": "1000000491", "network_generation": "0"},
        {"npi": "1000000491", "limit": 1},
        {"npi": "1000000491", "source": {}},
    ],
)
async def test_provider_plans_closed_query_refuses_before_transaction(plans_context, query):
    result = await send(plans_context, v2_envelope(query), path=_PATH)
    assert result.status == 400 and plans_context.session.events == [] and plans_context.plan_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["v1", "query-string", "digest", "duplicate"])
async def test_provider_plans_policy_and_wire_refusals(plans_context, kind):
    document = v2_envelope({"npi": "1000000491"})
    path, raw = _PATH, None
    if kind == "v1":
        document = envelope({"npi": "1000000491"})
    elif kind == "query-string":
        path += "?npi=1000000491"
    elif kind == "digest":
        document["access_scope_sha256"] = "0" * 64
    else:
        raw = json.dumps(document).replace('"query": {', '"query": {"npi":"0000000001",').encode()
    result = await send(plans_context, document, path=path, raw=raw)
    assert result.status == 400 and plans_context.session.events == []


@pytest.mark.asyncio
async def test_provider_plans_authentication_precedes_body(plans_context):
    result = await send(plans_context, path=_PATH, headers={}, raw=b"\xff")
    assert result.status == 403 and plans_context.session.events == []


@pytest.mark.asyncio
@pytest.mark.parametrize("kind,status", [("detail_denied", 404), ("source_unavailable", 503), ("response_bounds", 503)])
async def test_provider_plans_reader_refusal_closes_snapshot_without_retry(plans_context, monkeypatch, kind, status):
    async def refuse(connection, query, *, excluded_network_ids):
        plans_context.plan_calls.append(query)
        raise RegistryNetworkCatalogError("registry_network_catalog_" + kind)

    monkeypatch.setattr(endpoint, "read_registry_provider_plans", refuse, raising=False)
    document = v2_envelope({"npi": "1000000491"})
    result = await send(plans_context, document, path=_PATH)
    assert result.status == status and len(plans_context.plan_calls) == 1
    assert result.headers["X-Network-Access-Scope"] == document["access_scope_sha256"]
    assert "X-Network-Generation" not in result.headers and not plans_context.driver.in_transaction
    assert plans_context.session.events[-1] == ("exit", RegistryNetworkCatalogError)


@pytest.mark.asyncio
async def test_provider_plans_cancellation_propagates_and_releases_snapshot(plans_context, monkeypatch):
    async def cancel(connection, query, *, excluded_network_ids):
        raise asyncio.CancelledError

    monkeypatch.setattr(endpoint, "read_registry_provider_plans", cancel, raising=False)
    request = type(
        "Request",
        (),
        {
            "headers": {"Authorization": "Bearer synthetic-catalog-control-token"},
            "query_string": "",
            "body": json.dumps(v2_envelope({"npi": "1000000491"})).encode(),
            "ctx": type("Context", (), {"sa_session": plans_context.session})(),
        },
    )()
    with pytest.raises(asyncio.CancelledError):
        await endpoint._catalog_response(request, "provider-plans")
    assert plans_context.session.events[-1] == ("exit", asyncio.CancelledError)
    assert not plans_context.driver.in_transaction


@pytest.mark.asyncio
@pytest.mark.parametrize("trailing", ["", "/"])
async def test_provider_plans_framework_method_error_is_private(plans_context, trailing):
    _, result = await plans_context.app.asgi_client.get(_PATH + trailing)
    assert result.status == 405 and result.headers["Cache-Control"] == "private, no-store"
    assert plans_context.session.events == []


@pytest.mark.asyncio
async def test_provider_plans_complete_response_bound(plans_context):
    plans_context.plan_page["npi_data"] = [{"value": "x" * endpoint.MAX_RESPONSE_BYTES}]
    result = await send(plans_context, v2_envelope({"npi": "1000000491"}), path=_PATH)
    assert result.status == 503 and "X-Network-Generation" not in result.headers
