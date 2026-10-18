# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The optional evidence selector preserves ordinary GET and authentication."""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sanic import Sanic

from api import control, plan_pricing_no_work_http


def _application():
    app = Sanic("pricing_no_work_" + uuid4().hex)
    app.add_route(control.control_get_import, "/control/v1/imports/<run_id>", methods={"GET"})
    return app


@pytest.mark.asyncio
async def test_real_http_auth_and_selector_before_evidence(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-token")
    invoked_run_ids = []

    @asynccontextmanager
    async def evidence(run_id):
        invoked_run_ids.append(run_id)
        yield {"contract": "managed-pricing-no-work-v1"}

    monkeypatch.setattr(plan_pricing_no_work_http, "managed_pricing_no_work", evidence)
    app = _application()
    path = "/control/v1/imports/exact?evidence=managed-pricing-no-work-v1"
    _, denied = await app.asgi_client.get(path)
    assert denied.status == 403 and not invoked_run_ids
    headers_by_name = {"Authorization": "Bearer synthetic-token"}
    for suffix in ("unsupported", "managed-pricing-no-work-v1&evidence=managed-pricing-no-work-v1"):
        _, refused = await app.asgi_client.get("/control/v1/imports/exact?evidence=" + suffix, headers=headers_by_name)
        assert refused.status == 400 and not invoked_run_ids
    _, accepted = await app.asgi_client.get(path, headers=headers_by_name)
    assert accepted.status == 200 and invoked_run_ids == ["exact"]


@pytest.mark.asyncio
async def test_ordinary_get_keeps_original_service(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-token")
    service = AsyncMock(return_value={"run_id": "ordinary"})
    monkeypatch.setattr(control, "get_import_run", service)
    app = _application()
    _, returned = await app.asgi_client.get(
        "/control/v1/imports/ordinary", headers={"Authorization": "Bearer synthetic-token"}
    )
    assert returned.status == 200 and returned.json == {"run_id": "ordinary"}
    service.assert_awaited_once_with("ordinary")


@pytest.mark.asyncio
@pytest.mark.parametrize("failure,status", ((ValueError("fixed"), 409), (RuntimeError("private detail"), 503)))
async def test_evidence_failure_is_sanitized_and_scope_closes(monkeypatch, failure, status):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-token")
    events = []

    @asynccontextmanager
    async def evidence(_run_id):
        try:
            events.append("opened")
            raise failure
            yield {}
        finally:
            events.append("closed")

    monkeypatch.setattr(plan_pricing_no_work_http, "managed_pricing_no_work", evidence)
    _, returned = await _application().asgi_client.get(
        "/control/v1/imports/exact?evidence=managed-pricing-no-work-v1",
        headers={"Authorization": "Bearer synthetic-token"},
    )
    assert returned.status == status and "private detail" not in returned.text
    assert events == ["opened", "closed"]
