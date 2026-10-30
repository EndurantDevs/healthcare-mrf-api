# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Refresh enqueue has one static route and fails closed before unconfigured DB work."""

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from sanic import Blueprint, Sanic
from sanic.exceptions import Forbidden

from api.endpoint import registry_publication as routes
from process.registry_membership_refresh_runtime import RegistryMembershipRefreshInputsService
from tests.test_registry_membership_refresh_contract import envelope

pytestmark = pytest.mark.asyncio
TOKEN = uuid4().hex


def incoming(*, document=None, raw=None, token=TOKEN, query_string=""):
    return SimpleNamespace(
        headers={"Authorization": "Bearer " + token},
        body=raw if raw is not None else json.dumps(document or envelope()).encode(),
        args={},
        query_string=query_string,
        app=SimpleNamespace(ctx=SimpleNamespace()),
    )


async def test_transport_authentication_precedes_body_and_missing_host_profile(monkeypatch):
    monkeypatch.setenv("HLTHPRT_REGISTRY_CONTROL_API_TOKEN", TOKEN)
    with pytest.raises(Forbidden):
        await routes.queue_membership_refresh(incoming(token="wrong", raw=b"invalid"))
    response = await routes.queue_membership_refresh(incoming())
    assert response.status == 503 and response.headers["Cache-Control"] == "private, no-store"


@pytest.mark.parametrize("fault", ["duplicate", "bytes", "query", "session", "human", "operation"])
async def test_closed_service_body_never_enters_ordinary_human_publication(monkeypatch, fault):
    monkeypatch.setenv("HLTHPRT_REGISTRY_CONTROL_API_TOKEN", TOKEN)
    document = envelope()
    if fault == "session":
        document["session_token_sha256"] = "a" * 64
    if fault == "human":
        document["actor"] = {"kind": "platform_admin", "user_id": str(uuid4()), "client_id": "system"}
    if fault == "operation":
        document["command"]["operation"] = "publish_initial_inventory"
    incoming_request = incoming(document=document)
    if fault == "duplicate":
        incoming_request.body = b'{"actor":{},"actor":{},"command":{}}'
    if fault == "bytes":
        incoming_request.body = b" " * (1024 * 1024 + 1)
    if fault == "query":
        incoming_request.query_string = "unexpected=1"
    assert (await routes.queue_membership_refresh(incoming_request)).status == 400


async def test_actual_publication_blueprint_uses_exact_static_refresh_path(monkeypatch):
    monkeypatch.setenv("HLTHPRT_REGISTRY_CONTROL_API_TOKEN", TOKEN)
    app = Sanic("membership_refresh_queue_" + uuid4().hex)
    app.blueprint(Blueprint.group([routes.blueprint], version_prefix="/api/v"))
    _, response = await app.asgi_client.post(
        "/api/v1/registry/publication/refresh-memberships",
        headers={"Authorization": "Bearer " + TOKEN},
        json=envelope(),
    )
    assert response.status == 503 and response.json == {"error": {"code": "registry_unavailable"}}


async def test_desired_inputs_require_transport_authentication_and_host_service(monkeypatch):
    monkeypatch.setenv("HLTHPRT_REGISTRY_CONTROL_API_TOKEN", TOKEN)
    with pytest.raises(Forbidden):
        await routes.membership_refresh_inputs(incoming(token="wrong", raw=b""))
    unavailable = await routes.membership_refresh_inputs(incoming(raw=b""))
    assert unavailable.status == 503
    assert unavailable.headers["Cache-Control"] == "private, no-store"
    assert (await routes.membership_refresh_inputs(incoming())).status == 400
    assert (await routes.membership_refresh_inputs(incoming(raw=b"", query_string="unexpected=1"))).status == 400


async def test_desired_inputs_static_get_route_is_registered(monkeypatch):
    monkeypatch.setenv("HLTHPRT_REGISTRY_CONTROL_API_TOKEN", TOKEN)
    app = Sanic("membership_refresh_inputs_" + uuid4().hex)
    app.blueprint(Blueprint.group([routes.blueprint], version_prefix="/api/v"))
    _, reply = await app.asgi_client.get(
        "/api/v1/registry/publication/membership-refresh/inputs",
        headers={"Authorization": "Bearer " + TOKEN},
    )
    assert reply.status == 503 and reply.json == {"error": {"code": "registry_unavailable"}}
    assert reply.headers["Cache-Control"] == "private, no-store"


@pytest.mark.parametrize(
    "failure,status",
    [
        (None, 200),
        (routes.RegistryRecordConflict("stale"), 409),
        (PermissionError("denied"), 403),
        (ValueError("invalid"), 400),
        (routes.RegistryMembershipRefreshUnavailable("unavailable"), 503),
        (routes.SQLAlchemyError("closed"), 503),
    ],
)
async def test_refresh_queue_uses_one_repeatable_read_transaction_and_preserves_error_status(
    monkeypatch,
    failure,
    status,
):
    monkeypatch.setenv("HLTHPRT_REGISTRY_CONTROL_API_TOKEN", TOKEN)
    events = []

    @asynccontextmanager
    async def begin():
        events.append("begin")
        try:
            yield
        except BaseException:
            events.append("rollback")
            raise
        else:
            events.append("commit")

    session = SimpleNamespace(begin=begin, execute=AsyncMock())
    driver = object()
    monkeypatch.setattr(routes, "_driver", AsyncMock(return_value=driver))
    receipt_by_field = {"request_id": str(uuid4()), "state": "queued"}
    enqueue = AsyncMock(side_effect=failure, return_value=receipt_by_field)
    monkeypatch.setattr(routes, "enqueue_registry_membership_refresh", enqueue)

    def unexpected_request(_request):
        pytest.fail("HTTP adapter called authority outside the enqueue service")

    async with routes.RegistryMembershipRefreshAuthorityClient(
        "https://authority.example", uuid4().hex, enabled=True, transport=httpx.MockTransport(unexpected_request)
    ) as authorize:
        profile = routes.RegistryMembershipRefreshAddressProfile("mrf", ("network_reader",))
        request = incoming()
        request.ctx = SimpleNamespace(sa_session=session)
        request.app.ctx.registry_membership_refresh_authority = authorize
        request.app.ctx.registry_membership_refresh_address_profile = profile
        reply = await routes.queue_membership_refresh(request)
        assert reply.status == status
        assert reply.headers["Cache-Control"] == "private, no-store"
        assert events == ["begin", "commit" if failure is None else "rollback"]
        assert str(session.execute.await_args.args[0]) == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"
        enqueue.assert_awaited_once_with(driver, envelope(), authorize=authorize, address_profile=profile)
        if failure is None:
            assert json.loads(reply.body) == receipt_by_field


@pytest.mark.parametrize("service_fault", ["wrong-type", "permission", "conflict", "unavailable", "invalid-json"])
async def test_inputs_service_refusals_remain_private_and_never_become_partial_documents(monkeypatch, service_fault):
    monkeypatch.setenv("HLTHPRT_REGISTRY_CONTROL_API_TOKEN", TOKEN)
    request = incoming(raw=b"")
    failures_by_fault = {
        "permission": PermissionError("denied"),
        "conflict": routes.RegistryRecordConflict("stale"),
        "unavailable": RuntimeError("unavailable"),
    }

    async def observe(_service):
        if service_fault in failures_by_fault:
            raise failures_by_fault[service_fault]
        return {"unsupported": object()}

    monkeypatch.setattr(RegistryMembershipRefreshInputsService, "__call__", observe)
    request.app.ctx.registry_membership_refresh_inputs = (
        object() if service_fault == "wrong-type" else RegistryMembershipRefreshInputsService(object(), object())
    )
    reply = await routes.membership_refresh_inputs(request)
    assert reply.status == {"permission": 403, "conflict": 409}.get(service_fault, 503)
    assert reply.headers["Cache-Control"] == "private, no-store"
    assert "accepted_sources" not in json.loads(reply.body)


async def test_inputs_observation_returns_complete_json_without_writer_session(monkeypatch):
    monkeypatch.setenv("HLTHPRT_REGISTRY_CONTROL_API_TOKEN", TOKEN)
    observed_by_field = {
        "accepted_sources": [],
        "approved_revision": 5,
        "address_generation": "a" * 64,
        "address_artifact_sha256": "b" * 64,
        "ready_address_generation": None,
        "expected_serving_generation": 9,
        "expected_serving_manifest_sha256": "c" * 64,
        "enrolled_scopes": [],
        "published_inputs_digest": None,
        "refresh_request": None,
    }

    async def observe(_service):
        return observed_by_field

    monkeypatch.setattr(RegistryMembershipRefreshInputsService, "__call__", observe)
    request = incoming(raw=b"")
    request.app.ctx.registry_membership_refresh_inputs = RegistryMembershipRefreshInputsService(object(), object())
    reply = await routes.membership_refresh_inputs(request)
    assert reply.status == 200 and json.loads(reply.body) == observed_by_field
    assert reply.content_type == "application/json"
    assert reply.headers["Cache-Control"] == "private, no-store"
