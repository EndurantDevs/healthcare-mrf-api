# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed batch-authority transport and authenticated original-review dispatch."""

import json
import time
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from api import control_registry_ptg_scope as routes
from process import registry_ptg_scope_engine as engine
from tests.test_registry_ptg_source_reviews import sample, service


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [0, 1, 20])
async def test_batch_client_uses_one_fixed_bounded_request_with_live_deadline(monkeypatch, count):
    approval_record, _, envelope = sample()
    commands = []
    for index in range(count):
        command = deepcopy(approval_record["approval_json"]["command"])
        command |= {"scope_id": str(UUID(int=index + 1)), "idempotency_key": "original-" + str(index)}
        commands.append(command)
    request_by_field = envelope | {"commands": commands}
    receipt_by_field = {
        "authorized": True,
        "actor": envelope["actor"],
        "request_sha256": engine._digest(request_by_field),
        "policy_revision": 2,
    }
    calls = []

    class Reply:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self, size):
            assert size == 4097
            return json.dumps(receipt_by_field).encode()

    class Opener:
        def open(self, request, *, timeout):
            calls.append(request)
            assert 0 < timeout <= 5
            assert (
                request.full_url
                == "https://authority.example/internal/v1/registry/ptg-source-scopes/authorize-source-reviews"
            )
            assert request.get_header("Authorization") == "Bearer synthetic-only"
            assert request.method == "POST" and request.data == engine._canonical(request_by_field).encode()
            return Reply()

    def opener(*handlers):
        assert handlers[0].proxies == {} and type(handlers[1]) is engine._NoRedirects
        return Opener()

    monkeypatch.setattr(engine, "build_opener", opener)
    authority = engine.RegistryPTGScopeAuthorityClient("https://authority.example", "synthetic-only")
    assert await authority.authorize_source_reviews(request_by_field, deadline=time.monotonic() + 5) == receipt_by_field
    assert len(calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["extra", "count", "ownership", "commands", "session", "limit", "order", "client"])
async def test_batch_client_refuses_invalid_page_before_http(monkeypatch, mutation):
    approval_record, _, envelope = sample()
    envelope["commands"] = [approval_record["approval_json"]["command"]]
    match mutation:
        case "extra":
            envelope["authority"] = True
        case "count":
            envelope["commands"] *= 21
        case "ownership":
            envelope["commands"][0]["ownership"]["content_version"] = "other-version"
        case "commands":
            envelope["commands"] = {}
        case "limit":
            envelope["selection"]["limit"] = 1
            envelope["commands"] *= 2
        case "order":
            envelope["commands"] *= 2
        case "client":
            envelope["selection"]["client_id"] = envelope["ownership"]["client_id"] = "system"
        case _:
            del envelope["session_token_sha256"]
    request = AsyncMock()
    monkeypatch.setattr(engine.RegistryPTGScopeAuthorityClient, "_request", request)
    authority = engine.RegistryPTGScopeAuthorityClient("https://authority.example", "synthetic-only")
    with pytest.raises((ValueError, PermissionError)):
        await authority.authorize_source_reviews(envelope)
    request.assert_not_called()


@pytest.mark.parametrize("batch", [False, True])
def test_transport_byte_bound_refuses_before_opening_http(monkeypatch, batch):
    monkeypatch.setattr(engine, "build_opener", lambda *_args: pytest.fail("oversized HTTP request"))
    authority = engine.RegistryPTGScopeAuthorityClient("https://authority.example", "synthetic-only")
    with pytest.raises(ValueError):
        authority._request({"value": "é" * (131072 if batch else 65536)}, source_reviews=batch)


@pytest.mark.asyncio
async def test_batch_client_refuses_changed_receipt_and_expired_deadline(monkeypatch):
    _, _, envelope = sample()
    envelope["commands"] = []
    requests = []

    def request(_self, body, **kwargs):
        requests.append((body, kwargs))
        return {"authorized": True, "actor": envelope["actor"], "request_sha256": "0" * 64, "policy_revision": 1}

    monkeypatch.setattr(engine.RegistryPTGScopeAuthorityClient, "_request", request)
    authority = engine.RegistryPTGScopeAuthorityClient("https://authority.example", "synthetic-only")
    with pytest.raises(PermissionError):
        await authority.authorize_source_reviews(envelope)
    with pytest.raises(TimeoutError):
        await authority.authorize_source_reviews(envelope, deadline=0)
    assert len(requests) == 1


@pytest.mark.asyncio
async def test_control_auth_precedes_review_page_body(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-control")
    monkeypatch.setattr(routes, "_body", lambda *_args: pytest.fail("unauthenticated body parse"))
    reply = await routes.read_registry_ptg_source_reviews(SimpleNamespace(headers={}))
    assert reply.status == 401 and reply.headers["Cache-Control"] == "private, no-store"


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "uninstalled", "extra", "query"])
async def test_control_dispatch_validates_then_uses_only_installed_scope_service(monkeypatch, failure):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-control")
    _, _, envelope = sample()
    installed = service(AsyncMock())
    app = SimpleNamespace(ctx=SimpleNamespace())
    if failure != "uninstalled":
        routes.install_registry_ptg_scope_engine(app, installed)
    if failure == "extra":
        envelope["commands"] = []
    method = AsyncMock(return_value={"source": {}, "items": [], "next_cursor": None})
    monkeypatch.setattr(engine.RegistryPTGScopeEngineService, "source_reviews", method)
    request = SimpleNamespace(
        app=app,
        headers={"Authorization": "Bearer synthetic-control"},
        body=json.dumps(envelope).encode(),
        args={},
        query_string="unexpected=1" if failure == "query" else "",
    )
    reply = await routes.read_registry_ptg_source_reviews(request)
    assert reply.status == (503 if failure == "uninstalled" else 400 if failure else 200)
    if failure:
        method.assert_not_awaited()
    else:
        method.assert_awaited_once()
        assert method.await_args.args == (envelope,) and type(method.await_args.kwargs["deadline"]) is float
        assert reply.headers["Cache-Control"] == "private, no-store"
