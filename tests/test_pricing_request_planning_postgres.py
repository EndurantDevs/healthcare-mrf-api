# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real HTTP Reader ownership and transaction-local planning cleanup."""

import asyncio
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import text

from api.endpoint import pricing
from api.plan_release_read_boundary import (
    PLAN_RELEASE_DIGEST_SELECTOR,
    PLAN_RELEASE_READ_BOUNDARY_HEADER,
    PLAN_RELEASE_REVISION_SELECTOR,
    require_internal_plan_release_read,
)
from api.request_lifecycle import RequestLifecycleSanic
from api.signed_request_reader import is_owned_reader
from db.connection import db
from tests.test_plan_release_read_admission import DIGEST, RELEASE, REVISION
from tests.test_plan_release_read_admission_postgres import native_admission as native_admission


async def _perform_http_request(app, ending, entered):
    """Exercise normal completion or cancel the actual in-flight ASGI request."""
    query_by_name = {
        "plan_release_id": RELEASE,
        PLAN_RELEASE_REVISION_SELECTOR: REVISION,
        PLAN_RELEASE_DIGEST_SELECTOR: DIGEST,
        "code_system": "CPT",
        "code": "99213",
        "npi": "1234567890",
        "include_allowed_amounts": "false",
    }
    headers_by_name = {PLAN_RELEASE_READ_BOUNDARY_HEADER: "internal"}
    if ending.endswith("cancel"):
        task = asyncio.create_task(
            app.asgi_client.get(
                "/api/v1/pricing/providers/search-by-procedure", params=query_by_name, headers=headers_by_name
            )
        )
        try:
            await asyncio.wait_for(entered.wait(), timeout=10)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    else:
        _, reply = await app.asgi_client.get(
            "/api/v1/pricing/providers/search-by-procedure", params=query_by_name, headers=headers_by_name
        )
        assert reply.status == (200 if ending == "success" else 500)


def _pricing_http_app(monkeypatch, ending, tuning_enabled, entered, jit_values, requests):
    """Register the real route with controlled search and encoding outcomes."""

    async def search(session, *_args, **_kwargs):
        assert is_owned_reader(session)
        jit_values.append(await session.scalar(text("SHOW jit")))
        assert jit_values[-1] == ("off" if tuning_enabled else "on")
        assert await session.scalar(text("SHOW plan_cache_mode")) == ("force_custom_plan" if tuning_enabled else "auto")
        if ending == "error":
            raise RuntimeError("synthetic failure")
        if ending == "cancel":
            entered.set()
            await asyncio.Event().wait()
        return {"items": [], "query": {"source": "ptg2"}}

    monkeypatch.setattr(pricing, "search_current_ptg2_index", search)
    app = RequestLifecycleSanic("pricing-planning-" + uuid4().hex)
    db.init_app(app)
    app.register_middleware(require_internal_plan_release_read, "request")

    @app.get("/api/v1/pricing/providers/search-by-procedure")
    async def serve(request):
        requests.append(request)
        assert request.ctx._sa_reader_scope is not None
        assert is_owned_reader(request.ctx.sa_session)
        reply = await pricing.list_providers_by_procedure(request)
        assert reply.body
        jit_values.append(await request.ctx.sa_session.scalar(text("SHOW jit")))
        assert await request.ctx.sa_session.scalar(text("SHOW plan_cache_mode")) == (
            "force_custom_plan" if tuning_enabled else "auto"
        )
        if ending == "encoding_error":
            raise RuntimeError("synthetic encoding failure")
        if ending == "encoding_cancel":
            entered.set()
            await asyncio.Event().wait()
        return reply

    return app


@pytest.mark.parametrize("ending", ["success", "error", "cancel", "encoding_error", "encoding_cancel"])
@pytest.mark.parametrize("tuning_enabled", [True, False])
@pytest.mark.parametrize("middleware_enabled", [True, False])
async def test_http_planning_setting_lives_through_encoding_and_cleans_up(
    native_admission, monkeypatch, ending, tuning_enabled, middleware_enabled
):
    """Prove setting lifetime and cleanup against a genuine owned Reader."""
    owner, _schema, reader = native_admission
    monkeypatch.setenv("HLTHPRT_API_READER_ENABLED", str(middleware_enabled).lower())
    if not tuning_enabled:
        monkeypatch.setattr(pricing, "is_owned_reader", lambda _session: False)
    monkeypatch.setattr(pricing, "resolve_plan_release_serving", AsyncMock(return_value=None))
    entered = asyncio.Event()
    jit_values = []
    requests = []

    app = _pricing_http_app(monkeypatch, ending, tuning_enabled, entered, jit_values, requests)
    await _perform_http_request(app, ending, entered)
    expected = "off" if tuning_enabled else "on"
    assert jit_values == (
        [expected, expected] if ending in {"success", "encoding_error", "encoding_cancel"} else [expected]
    )
    assert len(requests) == 1
    assert requests[0].ctx.sa_session is None
    assert requests[0].ctx._sa_reader_scope is None
    assert requests[0].ctx._sa_session_token is None
    assert not db.has_reader_session()
    assert (
        await owner.fetchval(
            "SELECT count(*) FROM pg_stat_activity WHERE usename=$1 AND xact_start IS NOT NULL", reader
        )
        == 0
    )
    async with db.reader_session(independent=True) as session:
        assert await session.scalar(text("SHOW jit")) == "on"
        assert await session.scalar(text("SHOW plan_cache_mode")) == "auto"
