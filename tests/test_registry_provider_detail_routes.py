# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protected detail validates authority before opening a managed snapshot."""

import json
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sanic import Blueprint, Sanic
from sanic.exceptions import NotFound, ServiceUnavailable

from api.endpoint import network_catalog as endpoint
from tests.test_network_catalog_routes import envelope, v2_envelope

_PATH = "/api/v1/registry/serving/providers/detail"
_AUTH = {"Authorization": "Bearer synthetic-detail-control"}


@pytest.fixture
def detail_context(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-detail-control")
    app = Sanic("managed_detail_routes_" + uuid4().hex)
    app.blueprint(Blueprint.group([endpoint.blueprint], version_prefix="/api/v"))
    endpoint.register_network_catalog_private_responses(app)
    reader = AsyncMock(
        return_value={
            "generation": "9",
            "approved_custom_revision": "4",
            "manifest_sha256": "b" * 64,
            "data": {"npi": {"npi": 1000000491, "address_list": []}},
        }
    )
    monkeypatch.setattr(endpoint, "read_managed_provider_detail", reader)
    return app, reader


async def _post(context, document=None, *, raw=None, headers=None, path=_PATH):
    app, _ = context
    _, reply = await app.asgi_client.post(
        path,
        data=raw if raw is not None else json.dumps(document).encode(),
        headers=_AUTH if headers is None else headers,
    )
    assert reply.headers["Cache-Control"] == "private, no-store"
    return reply


@pytest.mark.asyncio
async def test_detail_binds_real_request_closed_query_and_full_current_policy(detail_context):
    document = v2_envelope({"npi": "1000000491", "selector": {"namespace": "canonical", "values": [42]}})
    reply = await _post(detail_context, document)
    assert reply.status == 200
    assert set(reply.json) == {
        "client_id",
        "policy_revision",
        "generation",
        "approved_custom_revision",
        "manifest_sha256",
        "data",
    }
    assert reply.headers["X-Network-Access-Scope"] == document["access_scope_sha256"]
    assert reply.headers["X-Network-Generation"] == "9"
    _, reader = detail_context
    args, options = reader.await_args
    assert args[0].path == _PATH and args[1].npi == "1000000491" and args[1].selector == (42,)
    assert options["excluded_network_ids"] == ()
    assert options["access_scope_sha256"] == document["access_scope_sha256"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "document",
    [
        envelope({"npi": "1000000491"}),
        v2_envelope({"npi": "1000000491", "network_ids": [42]}),
        v2_envelope({"npi": "1000000491"}) | {"access_scope_sha256": "0" * 64},
    ],
)
async def test_detail_refuses_invalid_authority_or_query_before_reader(detail_context, document):
    reply = await _post(detail_context, document)
    assert reply.status == 400 and "X-Network-Generation" not in reply.headers
    detail_context[1].assert_not_awaited()


@pytest.mark.asyncio
async def test_detail_authentication_precedes_json_decoding(detail_context):
    reply = await _post(detail_context, raw=b"\xff", headers={"Authorization": "Bearer wrong"})
    assert reply.status == 403
    detail_context[1].assert_not_awaited()


@pytest.mark.asyncio
async def test_detail_duplicate_json_keys_refuse_before_reader(detail_context):
    raw = (
        json.dumps(v2_envelope({"npi": "1000000491"}))
        .replace('"npi": "1000000491"', '"npi": "1000000491", "npi": "1000000004"')
        .encode()
    )
    assert (await _post(detail_context, raw=raw)).status == 400
    detail_context[1].assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure,status", [(NotFound(), 404), (ServiceUnavailable(), 503)])
async def test_detail_sanitizes_native_failures_and_preserves_policy_header(detail_context, failure, status):
    detail_context[1].side_effect = failure
    document = v2_envelope({"npi": "1000000491"})
    reply = await _post(detail_context, document)
    assert reply.status == status and set(reply.json) == {"error"}
    assert reply.headers["X-Network-Access-Scope"] == document["access_scope_sha256"]
    assert "X-Network-Generation" not in reply.headers


@pytest.mark.asyncio
async def test_detail_framework_refusals_remain_private(detail_context):
    app, reader = detail_context
    _, reply = await app.asgi_client.get(_PATH)
    assert reply.status == 405 and reply.headers["Cache-Control"] == "private, no-store"
    _, reply = await app.asgi_client.post(_PATH + "/", data=b"{}", headers=_AUTH)
    assert reply.headers["Cache-Control"] == "private, no-store"
    reader.assert_not_awaited()
