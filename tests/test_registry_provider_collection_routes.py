# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Collection transport validates authority before entering the Reader scope."""

import json
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sanic import Blueprint, Sanic
from sanic.exceptions import Forbidden, InvalidUsage, NotFound, SanicException, ServiceUnavailable

from api import provider_collection_network_read as collection
from api import provider_detail_network_read as detail
from api.endpoint import network_catalog as endpoint
from api.network_address_scope import current_network_address_scope
from api.provider_collection_network_read import ProviderCollectionServiceRefusal
from process.registry_network_catalog_read import RegistryNetworkCatalogError
from tests.test_network_catalog_routes import batch_boundary as batch_boundary
from tests.test_network_catalog_routes import context as catalog_context
from tests.test_network_catalog_routes import envelope, v2_envelope
from tests.test_network_catalog_routes import send as catalog_post
from tests.test_provider_collection_network_read import _service_refusal
from tests.test_provider_collection_network_read import boundary as boundary

_PATH = "/api/v1/registry/serving/providers/collection"
_AUTH = {"Authorization": "Bearer synthetic-collection-control"}


@pytest.fixture
def collection_context(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-collection-control")
    app = Sanic("managed_collection_routes_" + uuid4().hex)
    app.blueprint(Blueprint.group([endpoint.blueprint], version_prefix="/api/v"))
    endpoint.register_network_catalog_private_responses(app)
    reader = AsyncMock(
        return_value={
            "generation": "9",
            "approved_custom_revision": "4",
            "manifest_sha256": "b" * 64,
            "data": {"rows": [], "total": 0},
        }
    )
    monkeypatch.setattr(endpoint, "read_managed_provider_collection", reader)
    return app, reader


def collection_query_document(kind="list"):
    query_by_field = {"kind": kind, "native_query": {"include_total": "false"}}
    if kind == "by-service":
        query_by_field["native_query"] = {"code": "99213"}
    if kind == "batch":
        query_by_field["native_query"] = {}
        query_by_field["native_batch"] = {"npis": ["1000000491"]}
    return query_by_field


async def _post(context, document=None, *, raw=None, headers=None, suffix="", path=_PATH):
    _, reply = await context[0].asgi_client.post(
        path + suffix,
        data=raw if raw is not None else json.dumps(document).encode(),
        headers=_AUTH if headers is None else headers,
    )
    assert reply.headers["Cache-Control"] == "private, no-store"
    return reply


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["list", "geo", "batch", "by-service"])
async def test_collection_full_policy_and_native_kind_reach_reader(collection_context, kind):
    document = v2_envelope(collection_query_document(kind))
    reply = await _post(collection_context, document)
    assert reply.status == 200
    assert set(reply.json) == {
        "client_id",
        "policy_revision",
        "generation",
        "approved_custom_revision",
        "manifest_sha256",
        "data",
    }
    assert reply.headers["X-Network-Generation"] == "9"
    assert reply.headers["X-Network-Access-Scope"] == document["access_scope_sha256"]
    args, options = collection_context[1].await_args
    assert args[0].path == _PATH and args[1].kind == kind and args[1].selector is None
    if kind == "by-service":
        assert dict(args[1].native_query)["code"] == "99213"
    elif kind != "batch":
        assert dict(args[1].native_query)["include_total"] == "false"
    assert options["excluded_network_ids"] == ()
    assert options["access_scope_sha256"] == document["access_scope_sha256"]


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["v1", "digest", "hidden_selector", "unknown", "batch_missing"])
@pytest.mark.parametrize("kind", ["list", "by-service"])
async def test_collection_bad_authority_or_query_never_opens_reader(collection_context, damage, kind):
    document = v2_envelope(collection_query_document(kind))
    if damage == "v1":
        document = envelope(collection_query_document(kind))
    elif damage == "digest":
        document["access_scope_sha256"] = "0" * 64
    elif damage == "hidden_selector":
        document["query"]["native_query"]["network_ids"] = "42"
    elif damage == "unknown":
        document["query"]["ignored"] = True
    else:
        document["query"]["kind"] = "batch"
    assert (await _post(collection_context, document)).status == 400
    collection_context[1].assert_not_awaited()


@pytest.mark.asyncio
async def test_collection_auth_duplicate_and_query_bounds_precede_reader(collection_context):
    assert (await _post(collection_context, raw=b"\xff", headers={"Authorization": "Bearer wrong"})).status == 403
    raw = json.dumps(v2_envelope(collection_query_document())).replace(
        '"kind": "list"', '"kind": "list", "kind": "geo"'
    )
    assert (await _post(collection_context, raw=raw.encode())).status == 400
    assert (await _post(collection_context, raw=b" " * (1024 * 1024 + 1))).status == 400
    assert (
        await _post(collection_context, v2_envelope(collection_query_document()), suffix="?ignored=1")
    ).status == 400
    collection_context[1].assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [InvalidUsage(), Forbidden(), NotFound(), ServiceUnavailable()])
async def test_collection_preserves_safe_native_status(collection_context, failure):
    collection_context[1].side_effect = failure
    document = v2_envelope(collection_query_document())
    reply = await _post(collection_context, document)
    assert reply.status == failure.status_code and set(reply.json) == {"error"}
    assert reply.headers["X-Network-Access-Scope"] == document["access_scope_sha256"]
    assert "X-Network-Generation" not in reply.headers


@pytest.mark.asyncio
async def test_collection_bounds_envelope_and_framework_refusals(collection_context, monkeypatch):
    monkeypatch.setattr(endpoint, "MAX_DETAIL_BYTES", 32)
    reply = await _post(collection_context, v2_envelope(collection_query_document()))
    assert reply.status == 503 and "X-Network-Generation" not in reply.headers
    _, reply = await collection_context[0].asgi_client.get(_PATH)
    assert reply.status == 405 and reply.headers["Cache-Control"] == "private, no-store"


@pytest.mark.asyncio
async def test_service_refusal_transport_keeps_only_closed_document_and_private_scope(collection_context):
    failure = ProviderCollectionServiceRefusal(json.dumps(_service_refusal()).encode())
    collection_context[1].side_effect = failure
    document = v2_envelope({"kind": "by-service", "native_query": {"code": "99213"}})
    reply = await _post(collection_context, document)
    assert reply.status == 422 and reply.json == _service_refusal()
    assert reply.headers["X-Network-Access-Scope"] == document["access_scope_sha256"]
    assert "X-Network-Generation" not in reply.headers
    collection_context[1].assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["list", "geo", "batch"])
async def test_service_refusal_cannot_escape_another_collection_kind(collection_context, kind):
    collection_context[1].side_effect = ProviderCollectionServiceRefusal(json.dumps(_service_refusal()).encode())
    reply = await _post(collection_context, v2_envelope(collection_query_document(kind)))
    assert reply.status == 503 and reply.json == {"error": {"code": "network_catalog_unavailable"}}
    assert "X-Network-Generation" not in reply.headers


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["profile", "detail", "list", "geo", "batch", "by-service"])
@pytest.mark.parametrize("failure_kind", ["source_changed", "native"])
async def test_full_policy_provider_restart_is_closed(collection_context, monkeypatch, kind, failure_kind):
    failure = (
        RegistryNetworkCatalogError("registry_network_catalog_source_changed")
        if failure_kind == "source_changed"
        else SanicException("discarded diagnostic", status_code=409)
    )
    reader = AsyncMock(side_effect=failure)
    route_kind = kind if kind in {"profile", "detail"} else "collection"
    monkeypatch.setattr(endpoint, "read_managed_provider_" + route_kind, reader)
    query_by_field = (
        {"npi": "1000000491"}
        if route_kind != "collection"
        else {"kind": "by-service", "native_query": {"code": "99213"}}
        if kind == "by-service"
        else collection_query_document(kind)
    )
    document = v2_envelope(query_by_field)
    reply = await _post(collection_context, document, path="/api/v1/registry/serving/providers/" + route_kind)
    assert reply.status == 409 and reply.json == {"error": {"code": "network_catalog_source_changed"}}
    assert reply.headers["X-Network-Access-Scope"] == document["access_scope_sha256"]
    assert "X-Network-Generation" not in reply.headers and "discarded" not in reply.text
    reader.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["profile", "detail"])
async def test_service_refusal_cannot_escape_other_provider_routes(collection_context, monkeypatch, kind):
    reader = AsyncMock(side_effect=ProviderCollectionServiceRefusal(json.dumps(_service_refusal()).encode()))
    monkeypatch.setattr(endpoint, "read_managed_provider_" + kind, reader)
    reply = await _post(
        collection_context, v2_envelope({"npi": "1000000491"}), path="/api/v1/registry/serving/providers/" + kind
    )
    assert reply.status == 503 and reply.json == {"error": {"code": "network_catalog_unavailable"}}
    assert "X-Network-Generation" not in reply.headers


@pytest.mark.asyncio
@pytest.mark.parametrize("full_policy", [False, True])
@pytest.mark.parametrize("failure_kind", ["source_changed", "native"])
@pytest.mark.parametrize(
    "suffix,query_by_field",
    [("", {}), ("/detail", {"network_id": 84}), ("/legacy", {"namespace": "checksum_network", "values": ["42"]})],
)
async def test_catalog_and_legacy_restart_behavior_is_unchanged(
    catalog_context, monkeypatch, full_policy, failure_kind, suffix, query_by_field
):
    failure = (
        RegistryNetworkCatalogError("registry_network_catalog_source_changed")
        if failure_kind == "source_changed"
        else SanicException("discarded diagnostic", status_code=409)
    )
    reader = AsyncMock(side_effect=failure)
    monkeypatch.setattr(endpoint, "_read_catalog_page", reader)
    document = (v2_envelope if full_policy else envelope)(query_by_field)
    reply = await catalog_post(catalog_context, document, path="/api/v1/registry/serving/catalog" + suffix)
    expected = 409 if suffix == "/legacy" and full_policy and failure_kind == "source_changed" else 503
    assert reply.status == expected and reply.json == {
        "error": {"code": "network_catalog_source_changed" if expected == 409 else "network_catalog_unavailable"}
    }
    assert "X-Network-Generation" not in reply.headers
    reader.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("has_valid_refusal", [False, True])
async def test_native_service_refusal_crosses_actual_collection_transport(
    collection_context, boundary, monkeypatch, has_valid_refusal
):
    from sanic import response

    from api import custom_import_provider_service_http as service

    observed_requests = []

    async def refused(request, *, native_args):
        observed_requests.append(request)
        assert request.ctx.sa_session is boundary.session and boundary.state.active
        assert current_network_address_scope().manifest is boundary.manifest
        assert native_args.get("code") == "99213"
        document_by_field = _service_refusal()
        if not has_valid_refusal:
            document_by_field["code"] = "unknown_diagnostic"
        return response.json(document_by_field, status=422)

    monkeypatch.setattr(service, "_service_page", refused)
    monkeypatch.setattr(endpoint, "read_managed_provider_collection", collection.read_managed_provider_collection)
    document = v2_envelope({"kind": "by-service", "native_query": {"code": "99213"}})
    reply = await _post(collection_context, document)
    assert reply.status == (422 if has_valid_refusal else 503)
    assert reply.json == (
        _service_refusal() if has_valid_refusal else {"error": {"code": "network_catalog_unavailable"}}
    )
    assert reply.headers["X-Network-Access-Scope"] == document["access_scope_sha256"]
    assert "X-Network-Generation" not in reply.headers
    assert len(observed_requests) == 1 and not hasattr(observed_requests[0].ctx, "sa_session")
    detail._pin.assert_awaited_once()
    assert len(boundary.state.snapshots) == 1 and not boundary.state.active and not boundary.state.catalog_active
    assert current_network_address_scope() is None
