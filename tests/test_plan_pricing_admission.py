# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reserved release pricing runs stay refused; no deployment configures a managed control schema."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from sanic import Sanic

from api import control, control_imports
from db.models import db
from process.plan_pricing_control_transaction import pricing_worker_claim_transaction
from tests.test_plan_pricing_idempotency import _durable_import_request

UNCONFIGURED = "pricing control schema is unavailable or invalid"
RESERVED = "plan-pricing-projection:" + "a" * 64


def _database(monkeypatch, persisted_run):
    connection = AsyncMock()
    connection.fetchrow.return_value = persisted_run
    raw_connection = SimpleNamespace(driver_connection=connection)
    session = SimpleNamespace(
        raw_connection=raw_connection,
        execute=AsyncMock(),
        connection=AsyncMock(return_value=SimpleNamespace(get_raw_connection=AsyncMock(return_value=raw_connection))),
    )
    events = []

    @asynccontextmanager
    async def transaction():
        events.append("begin")
        yield session
        events.append("commit")

    monkeypatch.setattr(db, "transaction", transaction)
    monkeypatch.setattr(db, "acquire", transaction)
    monkeypatch.setattr(db, "connect", AsyncMock())
    monkeypatch.setattr(db, "execute", AsyncMock(side_effect=AssertionError("unguarded database write")))
    return events


@pytest.fixture(
    params=(
        (RESERVED, None),
        ("plan-pricing-projection-retry:" + "b" * 64, "parent-run"),
        ("ordinary", "parent-run"),
    ),
    ids=("reserved", "retry", "retry-only"),
)
def reserved_run(monkeypatch, request):
    idempotency_key, retry_of_run_id = request.param
    run_by_field = {
        **_durable_import_request("plan-pricing-projection", "reserved-run"),
        "idempotency_key": idempotency_key,
        "retry_of_run_id": retry_of_run_id,
    }
    _database(monkeypatch, run_by_field)
    return run_by_field


@pytest.mark.asyncio
async def test_reserved_run_is_refused_before_insert(reserved_run):
    with pytest.raises(ValueError, match=UNCONFIGURED):
        await control_imports._admit_import_row(
            "plan-pricing-projection", reserved_run, is_ptg_source_file_admission=False
        )
    db.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_reserved_request_is_refused_before_active_run_lookup(reserved_run, monkeypatch):
    active = AsyncMock(return_value={"run_id": "active", "importer": "plan-pricing-projection"})
    monkeypatch.setattr(control_imports, "find_importer_run_by_idempotency_key", AsyncMock(return_value=None))
    monkeypatch.setattr(control_imports, "find_earliest_active_run_by_importer", active)
    with pytest.raises(ValueError, match=UNCONFIGURED):
        await control_imports.create_import_run(reserved_run)
    active.assert_not_awaited()
    db.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_reserved_replay_is_refused(reserved_run, monkeypatch):
    replayed_by_field = {
        **reserved_run,
        "run_id": "replayed-run",
        "engine": control_imports.ENGINE_NAME,
        "node_id": control_imports.os.getenv("HLTHPRT_IMPORT_NODE_ID"),
        "status": "succeeded",
    }
    active = AsyncMock(side_effect=AssertionError("replay must not reach active lookup"))
    lookup = AsyncMock(return_value=replayed_by_field)
    monkeypatch.setattr(control_imports, "find_importer_run_by_idempotency_key", lookup)
    monkeypatch.setattr(control_imports, "find_earliest_active_run_by_importer", active)
    with pytest.raises(ValueError, match=UNCONFIGURED):
        await control_imports.create_import_run(reserved_run)
    db.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_reserved_run_is_refused_before_worker_launch(reserved_run, monkeypatch):
    from api import control_workers

    for name in (
        "acquire_ptg_admission_lock",
        "acquire_control_run_worker_action_lock",
        "require_not_wave_owned_run",
        "require_no_capacity_owning_wave",
        "_admit_worker_ensure",
    ):
        monkeypatch.setattr(control_workers, name, AsyncMock(return_value=None))
    launch = Mock(side_effect=AssertionError("reserved pricing worker launched"))
    monkeypatch.setattr(control_workers, "ensure_worker", launch)
    with pytest.raises(ValueError, match=UNCONFIGURED):
        await control_workers.guarded_ensure_worker(reserved_run)
    launch.assert_not_called()


@pytest.mark.asyncio
async def test_reserved_run_is_refused_before_initial_target(reserved_run, monkeypatch):
    from process import control_lifecycle

    target = Mock(side_effect=AssertionError("unguarded target import"))
    monkeypatch.setattr(control_lifecycle, "import_module", target)
    monkeypatch.setattr(control_lifecycle, "bind_status_event_loop", Mock())
    monkeypatch.setattr(control_lifecycle, "set_live_progress_context", Mock(return_value=None))
    with pytest.raises(ValueError, match=UNCONFIGURED):
        await control_lifecycle.control_single_job_start({}, task=reserved_run)
    control_lifecycle.db.execute.assert_not_awaited()
    target.assert_not_called()


@pytest.mark.asyncio
async def test_ordinary_run_inserts_without_a_control_transaction(monkeypatch):
    run_by_field = _durable_import_request("plan-pricing-projection", "ordinary-run")
    events = _database(monkeypatch, run_by_field)
    monkeypatch.setattr(db, "execute", AsyncMock())
    assert (
        await control_imports._admit_import_row(
            "plan-pricing-projection", run_by_field, is_ptg_source_file_admission=False
        )
        is None
    )
    db.execute.assert_awaited_once()
    assert events == []


@pytest.mark.asyncio
@pytest.mark.parametrize("persisted", (None, "ordinary"), ids=("other-importer", "ordinary"))
async def test_ordinary_claim_stays_inside_one_owned_transaction(monkeypatch, persisted):
    run_by_field = _durable_import_request("plan-pricing-projection", "ordinary-run") if persisted else None
    events = _database(monkeypatch, run_by_field)
    async with pricing_worker_claim_transaction({"run_id": "ordinary-run"}):
        events.append("claim")
    assert events == ["begin", "claim", "commit"]
    async with pricing_worker_claim_transaction({}):
        events.append("unbound")
    assert events[-1] == "unbound"


def _application():
    app = Sanic("pricing_evidence_" + uuid4().hex)
    app.add_route(control.control_get_import, "/control/v1/imports/<run_id>", methods={"GET"})
    return app


@pytest.mark.asyncio
@pytest.mark.parametrize("persisted", (None, "ordinary"), ids=("missing", "ordinary"))
async def test_evidence_selector_is_authenticated_and_never_qualified(monkeypatch, persisted):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-token")
    _database(monkeypatch, _durable_import_request("plan-pricing-projection", "exact") if persisted else None)
    service = AsyncMock(side_effect=AssertionError("evidence selector reached the ordinary GET"))
    monkeypatch.setattr(control, "get_import_run", service)
    app = _application()
    path = "/control/v1/imports/exact?evidence=managed-pricing-no-work-v1"
    _, denied = await app.asgi_client.get(path)
    assert denied.status == 403
    headers_by_name = {"Authorization": "Bearer synthetic-token"}
    for suffix in ("unsupported", "managed-pricing-no-work-v1&evidence=managed-pricing-no-work-v1"):
        _, refused = await app.asgi_client.get("/control/v1/imports/exact?evidence=" + suffix, headers=headers_by_name)
        assert refused.status == 400
    _, unqualified = await app.asgi_client.get(path, headers=headers_by_name)
    assert unqualified.status == 409
    service.assert_not_awaited()


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
