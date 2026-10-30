# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The ordinary management bearer never supplies native refresh service capabilities."""

import json
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sanic import Blueprint, Sanic
from sanic.exceptions import Forbidden

from api.endpoint import registry_management as management
from process.registry_refresh_anchor_contract import (
    RegistryRefreshAnchorMismatch,
    RegistryRefreshAnchorStale,
    RegistryRefreshAnchorUnavailable,
)
from process.registry_refresh_anchor_service import RegistryRefreshAnchorService
from tests.test_registry_refresh_anchor_contract import query

pytestmark = pytest.mark.asyncio
TOKEN = uuid4().hex


def incoming(document=None, *, service=None, token=TOKEN, raw=None, query_string=""):
    return SimpleNamespace(
        headers={"Authorization": "Bearer " + token},
        body=raw if raw is not None else json.dumps(query() if document is None else document).encode(),
        args={},
        query_string=query_string,
        app=SimpleNamespace(ctx=SimpleNamespace(registry_refresh_anchor_service=service)),
    )


async def test_authentication_precedes_body_and_any_native_context(monkeypatch):
    monkeypatch.setenv("HLTHPRT_REGISTRY_CONTROL_API_TOKEN", TOKEN)
    with pytest.raises(Forbidden):
        await management.approved_refresh_anchors(incoming(token="wrong", raw=b"invalid-json"))
    response = await management.approved_refresh_anchors(incoming())
    assert response.status == 503
    fake = SimpleNamespace(verify=lambda _document: pytest.fail("untrusted service accepted"))
    assert (await management.approved_refresh_anchors(incoming(service=fake))).status == 503


@pytest.mark.parametrize("fault", ["duplicate", "body", "query", "unknown", "empty"])
async def test_closed_input_refuses_before_unconfigured_service_lookup(monkeypatch, fault):
    monkeypatch.setenv("HLTHPRT_REGISTRY_CONTROL_API_TOKEN", TOKEN)
    requests_by_fault = {
        "duplicate": incoming(raw=b'{"approved_revision":5,"approved_revision":5,"enrolled_scopes":[]}'),
        "body": incoming(raw=b" " * (1024 * 1024 + 1)),
        "query": incoming(query_string="unexpected=1"),
        "unknown": incoming({**query(), "reader_sessions": {}}),
        "empty": incoming(raw=b""),
    }
    response = await management.approved_refresh_anchors(requests_by_fault[fault])
    assert response.status == 400 and response.headers["Cache-Control"] == "private, no-store"


@pytest.mark.parametrize(
    "error,status",
    [(RegistryRefreshAnchorStale, 409), (RegistryRefreshAnchorMismatch, 403), (RegistryRefreshAnchorUnavailable, 503)],
)
async def test_native_refusal_statuses_are_distinct(monkeypatch, error, status):
    monkeypatch.setenv("HLTHPRT_REGISTRY_CONTROL_API_TOKEN", TOKEN)
    # Only the exact installed service type enters verify; the double replaces execution, not credentials.
    service = object.__new__(RegistryRefreshAnchorService)

    async def refuse(_service, _document):
        raise error("native refusal")

    monkeypatch.setattr(RegistryRefreshAnchorService, "verify", refuse)
    response = await management.approved_refresh_anchors(incoming(service=service))
    assert response.status == status and set(json.loads(response.body)) == {"error"}


async def test_actual_existing_blueprint_keeps_management_route_prefix(monkeypatch):
    monkeypatch.setenv("HLTHPRT_REGISTRY_CONTROL_API_TOKEN", TOKEN)
    app = Sanic("refresh_anchor_route_" + uuid4().hex)
    app.blueprint(Blueprint.group([management.blueprint], version_prefix="/api/v"))
    _, response = await app.asgi_client.post(
        "/api/v1/registry/manage/approved-refresh-anchors", headers={"Authorization": "Bearer " + TOKEN}, json=query()
    )
    assert response.status == 503 and response.json == {"error": {"code": "registry_unavailable"}}
