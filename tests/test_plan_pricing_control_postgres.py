# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native enqueue acknowledgements cannot replace worker or terminal state."""

from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from sqlalchemy import select, update

from api import control_imports
from db.models import ImportRun, db
from process import control_lifecycle
from process.plan_pricing_control import DELIVERY_FIELD, preserve_delivery
from tests.test_plan_pricing_idempotency import _durable_import_request
from tests.test_plan_pricing_idempotency_postgres import (
    _drop_import_run_table,
    _fake_enqueue,
    _reset_import_run_schema,
)

pytestmark = pytest.mark.asyncio(loop_scope="module")


@pytest_asyncio.fixture(loop_scope="module")
async def pricing_control_db(monkeypatch):
    await _reset_import_run_schema()
    monkeypatch.setattr(control_imports, "read_live_progress", lambda _run_id: None)
    monkeypatch.setattr(control_imports, "_write_run_live_progress", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(control_imports, "enqueue_status_event", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(control_lifecycle, "write_live_progress", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(control_lifecycle, "_should_update_control_run_db", AsyncMock(return_value=True))
    try:
        yield
    finally:
        await _drop_import_run_table()


async def _raw(run_id):
    result = await db.execute(select(ImportRun.__table__).where(ImportRun.run_id == run_id))
    return dict(result.mappings().one())


async def _worker_update(run_id, status):
    pair_by_field = {"attempt_id": run_id + ":" + "a" * 32, "attempt_started_at": "2026-10-15T13:00:00.000000+00:00"}
    assert await control_lifecycle.mark_control_run(
        run_id, status="running", phase_detail="worker running", progress_message="running", **pair_by_field
    )
    if status != "running":
        assert await control_lifecycle.mark_control_run(
            run_id,
            status=status,
            phase_detail="worker " + status,
            progress_message=status,
            metrics={"projection_receipt": {"exact": True}},
            error={"code": "synthetic-worker-failure"} if status == "failed" else None,
            **pair_by_field,
        )


@pytest.mark.parametrize("worker_status", ("running", "succeeded", "failed"))
async def test_native_late_ack_preserves_full_newer_worker_state(pricing_control_db, monkeypatch, worker_status):
    request = _durable_import_request("plan-pricing-projection", "late_ack_run")
    captured_by_field = {}

    async def enqueue(run):
        pending = await _raw(run["run_id"])
        assert pending["metrics"][DELIVERY_FIELD] == {"run_id": run["run_id"], "state": "pending"}
        await _worker_update(run["run_id"], worker_status)
        captured_by_field.update(await _raw(run["run_id"]))
        return await _fake_enqueue(run)

    monkeypatch.setattr(control_imports, "_enqueue_import_start", enqueue)
    response, created = await control_imports.create_import_run(request)
    stored = await _raw(request["run_id"])
    assert created and response["status"] == worker_status
    for field in ("status", "phase_detail", "progress", "error", "started_at", "finished_at", "heartbeat_at"):
        assert stored[field] == captured_by_field[field]
    assert stored["metrics"][DELIVERY_FIELD] == {"run_id": request["run_id"], "state": "settled", "outcome": "queued"}
    if worker_status != "running":
        assert stored["metrics"]["projection_receipt"] == {"exact": True}


async def test_native_lost_enqueue_reply_preserves_terminal_and_uncertainty(pricing_control_db, monkeypatch):
    request = _durable_import_request("plan-pricing-projection", "lost_reply_run")

    async def enqueue(run):
        await _worker_update(run["run_id"], "succeeded")
        return control_imports._failed_enqueue_result(
            importer=run["importer"],
            params=run["params"],
            adapter=control_imports._SINGLE_JOB_ADAPTERS[run["importer"]],
            error=TimeoutError("lost reply"),
        )

    monkeypatch.setattr(control_imports, "_enqueue_import_start", enqueue)
    response, _created = await control_imports.create_import_run(request)
    stored = await _raw(request["run_id"])
    assert response["status"] == stored["status"] == "succeeded"
    assert stored["metrics"][DELIVERY_FIELD]["state"] == "unknown"
    assert stored["metrics"]["projection_receipt"] == {"exact": True}
    assert stored["error"] is None


async def test_native_stale_worker_metrics_cannot_erase_settled_delivery(pricing_control_db, monkeypatch):
    request = _durable_import_request("plan-pricing-projection", "settled_run")
    monkeypatch.setattr(control_imports, "_enqueue_import_start", _fake_enqueue)
    await control_imports.create_import_run(request)
    settled = (await _raw(request["run_id"]))["metrics"][DELIVERY_FIELD]
    await _worker_update(request["run_id"], "succeeded")
    await db.execute(
        update(ImportRun)
        .where(ImportRun.run_id == request["run_id"])
        .values(metrics=preserve_delivery(ImportRun, {DELIVERY_FIELD: {"state": "pending"}, "new": True}))
    )
    metrics = (await _raw(request["run_id"]))["metrics"]
    assert metrics[DELIVERY_FIELD] == settled
    assert metrics["new"] is True


async def test_native_conflicting_key_never_changes_original_request(pricing_control_db, monkeypatch):
    request = _durable_import_request("plan-pricing-projection", "original_run")
    enqueue = AsyncMock(side_effect=_fake_enqueue)
    monkeypatch.setattr(control_imports, "_enqueue_import_start", enqueue)
    await control_imports.create_import_run(request)
    with pytest.raises(ValueError, match="different request"):
        await control_imports.create_import_run({**request, "params": {"different": True}, "run_id": "other_run"})
    assert enqueue.await_count == 1
    assert (await _raw(request["run_id"]))["params"] == request["params"]


async def test_native_cancel_keeps_current_producer_settlement(pricing_control_db, monkeypatch):
    request = _durable_import_request("plan-pricing-projection", "cancel_race_run")
    monkeypatch.setattr(control_imports, "_enqueue_import_start", _fake_enqueue)
    await control_imports.create_import_run(request)
    current_by_field = await _raw(request["run_id"])
    settled = current_by_field["metrics"][DELIVERY_FIELD]
    stale_metrics_by_field = {DELIVERY_FIELD: {"run_id": request["run_id"], "state": "pending"}, "cancel_signal": {}}
    await control_imports._persist_cancel_request(
        request["run_id"],
        current_run=current_by_field,
        requested_at=control_imports.utc_now(),
        cancel_state_by_name=control_imports._cancel_state_by_name(
            canceled_before_start=True,
            has_terminalized_active_worker=False,
            current_progress=current_by_field["progress"],
        ),
        run_metrics_by_name=stale_metrics_by_field,
    )
    stored = await _raw(request["run_id"])
    assert stored["status"] == "canceled"
    assert stored["metrics"][DELIVERY_FIELD] == settled
