# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Taxonomy API actions retain their real Source fence and durable outcome."""

from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from api import control_imports as imports
from api import control_workers as workers
from api import nucc_source_control as source
from process.ptg_parts.ptg_source_attempt_actions import _validate_worker_selection


@pytest.fixture(autouse=True)
def closed_handoff_boundary(monkeypatch):
    """API unit cases isolate the separately tested original Source closed-stage reconciliation."""
    from process import nucc_protected_source

    bridge = AsyncMock(return_value=None)
    monkeypatch.setattr(nucc_protected_source, "reconcile_nucc_source_handoff", bridge)
    return bridge


RUN_ID = "synthetic_taxonomy_run"
ATTEMPT = RUN_ID + ":" + "a" * 32
STARTED = "2026-01-02T00:00:00Z"


def native_run(status="queued", *, handoff=False):
    return {
        "run_id": RUN_ID,
        "node_id": "synthetic_node",
        "engine": "healthcare-mrf-api",
        "importer": "nucc",
        "status": status,
        "phase_detail": "created",
        "params": {"source_url": "https://example.invalid/taxonomy.csv"},
        "progress": {} if status == "queued" else {"attempt_id": ATTEMPT, "attempt_started_at": STARTED},
        "metrics": {"nucc_handoff": {"synthetic": True}} if handoff else {},
        "finished_at": None,
        "error": None,
    }


def action_context(monkeypatch, outcome, events):
    @asynccontextmanager
    async def action(run_id):
        assert run_id in (RUN_ID, "")
        events.append("enter")
        try:
            yield outcome
        finally:
            events.append("exit")

    monkeypatch.setattr(source, "source_action", action)
    current = outcome[0] if outcome is not None else native_run()

    @asynccontextmanager
    async def owner_action(run_id):
        async with action(run_id) as protected:
            yield (*protected, True) if protected is not None else (current, None, False)

    monkeypatch.setattr(workers, "_nucc_action_transaction", owner_action)
    monkeypatch.setattr(workers, "_publish_nucc_run", Mock(side_effect=lambda row: row))
    monkeypatch.setattr(workers, "_nucc_remaining_seconds", lambda: 30)
    monkeypatch.setattr(imports, "get_import_run", AsyncMock(return_value=current))


@pytest.mark.parametrize("protected", [False, True])
@pytest.mark.parametrize("receipt", [None, {"synthetic": "committed"}])
async def test_source_action_orders_native_guard_and_keeps_original_session(monkeypatch, protected, receipt):
    session = object()
    actor = object()
    events = []

    @asynccontextmanager
    async def transaction():
        events.append("begin")
        yield session
        events.append("commit")

    def operation(name, result):
        async def call(actual, *args):
            assert actual is session
            events.append(name)
            return result

        return call

    monkeypatch.setattr(source, "db", SimpleNamespace(transaction=transaction, _transaction_binding=lambda: None))
    monkeypatch.setattr(source, "_is_protected_nucc_claim_parent", operation("native-kind", protected))
    monkeypatch.setattr(source.control, "reconcile_source_publication", operation("reconcile", receipt))
    monkeypatch.setattr(source, "capture_nucc_parent", operation("capture", {}))
    monkeypatch.setattr(source.control, "_source_transaction", operation("actor", actor))
    monkeypatch.setattr(source.control, "_locked_source_run", operation("run", native_run()))
    monkeypatch.setattr(source.control, "_recheck_source_transaction", operation("recheck", None))
    async with source.source_action(RUN_ID) as outcome:
        if protected:
            assert outcome == (native_run(), receipt)
        else:
            assert outcome is None
        events.append("action")
    assert events == (
        ["begin", "native-kind", "reconcile", "capture", "actor", "run", "action", "recheck", "commit"]
        if protected
        else ["begin", "native-kind", "commit", "action"]
    )


@pytest.mark.parametrize("step", ["native-kind", "reconcile", "capture", "actor", "run", "recheck"])
async def test_source_action_propagates_failures_without_legacy_fallback(monkeypatch, step):
    events = []

    @asynccontextmanager
    async def transaction():
        events.append("begin")
        try:
            yield object()
        except RuntimeError:
            events.append("rollback")
            raise
        else:
            events.append("commit")

    failure = RuntimeError("synthetic Source refusal")

    def operation(name, result):
        async def call(*args):
            events.append(name)
            if name == step:
                raise failure
            return result

        return call

    monkeypatch.setattr(source, "db", SimpleNamespace(transaction=transaction, _transaction_binding=lambda: None))
    monkeypatch.setattr(source, "_is_protected_nucc_claim_parent", operation("native-kind", True))
    monkeypatch.setattr(source.control, "reconcile_source_publication", operation("reconcile", None))
    monkeypatch.setattr(source, "capture_nucc_parent", operation("capture", {}))
    monkeypatch.setattr(source.control, "_source_transaction", operation("actor", {}))
    monkeypatch.setattr(source.control, "_locked_source_run", operation("run", native_run()))
    monkeypatch.setattr(source.control, "_recheck_source_transaction", operation("recheck", None))
    with pytest.raises(RuntimeError) as caught:
        async with source.source_action(RUN_ID):
            events.append("action")
    assert caught.value is failure
    assert events[-1] == "rollback" and "commit" not in events
    assert ("action" in events) is (step == "recheck")


@pytest.mark.parametrize("field", ["run_id", "engine", "importer"])
async def test_source_action_refuses_changed_native_run_identity(monkeypatch, field):
    current = native_run()
    current[field] = "synthetic_other"

    @asynccontextmanager
    async def transaction():
        yield object()

    monkeypatch.setattr(source, "db", SimpleNamespace(transaction=transaction, _transaction_binding=lambda: None))
    monkeypatch.setattr(source, "_is_protected_nucc_claim_parent", AsyncMock(return_value=True))
    monkeypatch.setattr(source.control, "reconcile_source_publication", AsyncMock(return_value=None))
    monkeypatch.setattr(source, "capture_nucc_parent", AsyncMock())
    monkeypatch.setattr(source.control, "_source_transaction", AsyncMock())
    monkeypatch.setattr(source.control, "_locked_source_run", AsyncMock(return_value=current))
    with pytest.raises(RuntimeError, match="identity differs"):
        async with source.source_action(RUN_ID):
            pytest.fail("changed run must never authorize an action")


@pytest.mark.parametrize(
    "progress_by_field,valid",
    [
        ({"attempt_id": ATTEMPT, "attempt_started_at": STARTED}, True),
        ({}, False),
        ({"attempt_id": ATTEMPT}, False),
        ({"attempt_id": "other:" + "a" * 32, "attempt_started_at": STARTED}, False),
        ({"attempt_id": ATTEMPT, "attempt_started_at": "2026-01-02T00:00:00"}, False),
        ({"attempt_id": ATTEMPT, "attempt_started_at": "not-a-time"}, False),
        ({"attempt_id": ATTEMPT + " ", "attempt_started_at": STARTED}, False),
    ],
)
def test_source_attempt_requires_complete_original_pair(progress_by_field, valid):
    run = native_run("running")
    run["progress"] = progress_by_field
    if valid:
        source.require_source_attempt(run)
        source.require_source_attempt(native_run())
    else:
        with pytest.raises(RuntimeError, match="attempt"):
            source.require_source_attempt(run)


@pytest.mark.parametrize("handoff", [False, True])
async def test_guarded_ensure_reconciles_before_admission_and_launch(monkeypatch, handoff):
    events = []
    action_context(monkeypatch, (native_run("running", handoff=handoff), None), events)

    def bind(_payload, actual):
        assert actual["run_id"] == RUN_ID and actual["importer"] == "nucc"
        events.append("bind")

    monkeypatch.setattr(workers, "_bind_admitted_job", bind)
    monkeypatch.setattr(workers, "ensure_worker", lambda payload: events.append("launch") or {"status": "started"})
    if handoff:
        assert (await workers.guarded_ensure_worker({"run_id": RUN_ID, "importer": "nucc"}))["status"] == "failed"
        assert events == ["enter", "exit"]
    else:
        assert await workers.guarded_ensure_worker({"run_id": RUN_ID, "importer": "nucc"}) == {"status": "started"}
        assert events == ["enter", "bind", "launch", "exit"]


async def test_guarded_ensure_committed_receipt_prevents_all_worker_side_effects(monkeypatch):
    events = []
    action_context(monkeypatch, (native_run("succeeded"), {"synthetic": "committed"}), events)
    admit, launch = AsyncMock(), Mock()
    monkeypatch.setattr(workers, "_admit_worker_ensure", admit)
    monkeypatch.setattr(workers, "ensure_worker", launch)
    result = await workers.guarded_ensure_worker({"run_id": RUN_ID, "queue": "arq:NUCC"})
    assert result["status"] == "failed" and "not launchable" in result["message"]
    admit.assert_not_awaited()
    launch.assert_not_called()


@pytest.mark.parametrize(
    "payload",
    [
        {"run_id": RUN_ID, "importer": "nucc", "queue": "arq:Geo"},
        {"run_id": RUN_ID, "importer": "geo", "queue": "arq:NUCC"},
    ],
)
async def test_guarded_ensure_checks_native_importer_against_selected_worker(monkeypatch, payload):
    events = []
    current = native_run()
    action_context(monkeypatch, (current, None), events)

    async def admit(**kwargs):
        _validate_worker_selection(current, kwargs["worker_selection"])
        return current

    monkeypatch.setattr(workers, "admit_existing_outer_run_action", admit)
    launch = Mock()
    monkeypatch.setattr(workers, "ensure_worker", launch)
    assert (await workers.guarded_ensure_worker(payload))["status"] == "failed"
    launch.assert_not_called()


@pytest.mark.parametrize("run_id", [RUN_ID, ""])
async def test_plain_ensure_preserves_original_launch_behavior(monkeypatch, run_id):
    events = []
    action_context(monkeypatch, None, events)
    monkeypatch.setattr(workers, "_admit_worker_ensure", AsyncMock(return_value=None))
    monkeypatch.setattr(workers, "ensure_worker", lambda payload: {"status": "started"})
    outcome = await workers.guarded_ensure_worker({"run_id": run_id, "importer": "nucc"})
    assert outcome == {"status": "started"} if run_id else outcome["status"] == "failed"


@pytest.mark.parametrize("receipt,handoff", [(None, False), ({"synthetic": "committed"}, False), (None, True)])
async def test_enqueue_uses_native_run_and_refuses_pending_publication(monkeypatch, receipt, handoff):
    events = []
    current = native_run("succeeded" if receipt else "queued", handoff=handoff)
    action_context(monkeypatch, (current, receipt), events)
    enqueue = AsyncMock(return_value={"status": "queued"})
    monkeypatch.setattr(imports, "_enqueue_selected_import", enqueue)
    requested_by_field = {"run_id": RUN_ID, "importer": "nucc", "params": {"untrusted": True}}
    if handoff:
        with pytest.raises(RuntimeError, match="publication is pending"):
            await imports._enqueue_import_start(requested_by_field)
        enqueue.assert_not_awaited()
    elif receipt:
        assert await imports._enqueue_import_start(requested_by_field) is current
        enqueue.assert_not_awaited()
    else:
        assert await imports._enqueue_import_start(requested_by_field) == {"status": "queued"}
        actual = enqueue.await_args.args[0]
        assert actual["params"] == current["params"] and actual["params"] != requested_by_field["params"]
        assert actual["family"] == imports._importer_family("nucc")


@pytest.mark.parametrize("advanced", ["created", "started", "handoff", "committed"])
async def test_enqueue_ack_preserves_newer_source_state_and_reads_after_commit(monkeypatch, advanced):
    events = []
    current = native_run("succeeded" if advanced == "committed" else "queued")
    if advanced == "started":
        current["progress"] = {"attempt_id": ATTEMPT, "attempt_started_at": STARTED}
    elif advanced == "handoff":
        current = native_run("finalizing", handoff=True)
    receipt = {"synthetic": "committed"} if advanced == "committed" else None
    action_context(monkeypatch, (current, receipt), events)

    async def write(*args):
        assert events == ["enter"]
        events.append("write")
        return {}

    async def read(run_id):
        assert events[-1] == "exit"
        events.append("read")
        return current

    monkeypatch.setattr(imports, "_write_enqueue_result", write)
    monkeypatch.setattr(imports, "get_import_run", read)
    assert await imports._persist_enqueue_result(RUN_ID, "nucc", {"status": "failed"}) is current
    assert ("write" in events) is (advanced == "created")


@pytest.mark.parametrize("status", ["running", "queued"])
async def test_enqueue_preserves_an_existing_complete_worker_attempt(monkeypatch, status):
    current = native_run(status)
    current["progress"] = {"attempt_id": ATTEMPT, "attempt_started_at": STARTED}
    action_context(monkeypatch, (current, None), [])
    enqueue = AsyncMock()
    monkeypatch.setattr(imports, "_enqueue_selected_import", enqueue)
    assert await imports._enqueue_import_start(current) is current
    enqueue.assert_not_awaited()


@pytest.mark.parametrize("receipt,handoff", [(None, False), ({"synthetic": "committed"}, False), (None, True)])
async def test_cancel_reconciles_before_signals_and_preserves_handoff(monkeypatch, receipt, handoff):
    events = []
    current = native_run("succeeded" if receipt else "finalizing" if handoff else "running", handoff=handoff)
    action_context(monkeypatch, (current, receipt), events)
    status = AsyncMock(return_value=1)
    monkeypatch.setattr(imports, "db", SimpleNamespace(status=status))
    worker, signal = AsyncMock(return_value={"status": "canceling"}), AsyncMock(return_value={"signaled": True})
    monkeypatch.setattr(imports, "_request_worker_cancel", worker)
    monkeypatch.setattr(imports, "_cancel_signal_for_run", signal)
    updated = current if receipt else {**current, "status": "canceling"}
    monkeypatch.setattr(imports, "get_import_run", AsyncMock(return_value=updated))
    original = deepcopy(current)
    result = await imports._request_nucc_cancel(RUN_ID)
    assert current == original and result is updated
    assert status.await_count == int(receipt is None)
    if receipt or handoff:
        worker.assert_not_awaited()
        signal.assert_not_awaited()
    else:
        worker.assert_awaited_once_with(current, cancel_signal={"signaled": True}, publish=False)
        signal.assert_awaited_once()
    if not receipt:
        statement = str(status.await_args.args[0])
        assert all(field in statement for field in ("progress", "metrics", "node_id", "engine"))
        assert "metrics=" not in statement
    assert events == ["enter", "exit"]


async def test_cancel_refuses_changed_attempt_after_native_guard(monkeypatch):
    events = []
    current = native_run("running")
    current["progress"].pop("attempt_started_at")
    action_context(monkeypatch, (current, None), events)
    worker = AsyncMock()
    monkeypatch.setattr(imports, "_request_worker_cancel", worker)
    with pytest.raises(RuntimeError, match="attempt differs"):
        await imports._request_nucc_cancel(RUN_ID)
    worker.assert_not_awaited()


async def test_cancel_plain_run_preserves_existing_worker_path(monkeypatch):
    events = []
    current = native_run()
    action_context(monkeypatch, None, events)
    worker, signal = AsyncMock(return_value={"status": "canceled"}), AsyncMock(return_value={"signaled": True})
    monkeypatch.setattr(imports, "db", SimpleNamespace(status=AsyncMock(return_value=1)))
    monkeypatch.setattr(imports, "_request_worker_cancel", worker)
    monkeypatch.setattr(imports, "_cancel_signal_for_run", signal)
    updated_by_field = {**current, "status": "canceled"}
    monkeypatch.setattr(imports, "get_import_run", AsyncMock(return_value=updated_by_field))
    assert await imports._request_nucc_cancel(RUN_ID) is updated_by_field
    worker.assert_awaited_once_with(current, cancel_signal={"signaled": True}, publish=False)
    assert events == ["enter", "exit"]


@pytest.mark.parametrize("handoff_status", ["running", "queued"])
async def test_cancel_rejects_handoff_outside_native_finalization(monkeypatch, handoff_status):
    events = []
    current = native_run(handoff_status, handoff=True)
    action_context(monkeypatch, (current, None), events)
    worker, execute = AsyncMock(), AsyncMock()
    monkeypatch.setattr(imports, "_request_worker_cancel", worker)
    monkeypatch.setattr(imports, "db", SimpleNamespace(execute=execute))
    with pytest.raises(RuntimeError, match="handoff state differs"):
        await imports._request_nucc_cancel(RUN_ID)
    worker.assert_not_awaited()
    execute.assert_not_awaited()


async def test_cancel_native_compare_failure_does_not_signal_or_publish(monkeypatch):
    events = []
    current = native_run("finalizing", handoff=True)
    action_context(monkeypatch, (current, None), events)
    monkeypatch.setattr(imports, "db", SimpleNamespace(status=AsyncMock(return_value=0)))
    worker, read, publish = AsyncMock(), AsyncMock(), Mock()
    monkeypatch.setattr(imports, "_request_worker_cancel", worker)
    monkeypatch.setattr(imports, "get_import_run", read)
    monkeypatch.setattr(workers, "_publish_nucc_run", publish)
    with pytest.raises(RuntimeError, match="cancellation attempt changed"):
        await imports._request_nucc_cancel(RUN_ID)
    worker.assert_not_awaited()
    read.assert_not_awaited()
    publish.assert_not_called()


async def test_cancel_routes_from_actual_selected_run_without_handoff_marker(monkeypatch):
    current = native_run("running")
    monkeypatch.setattr(imports, "is_protected_places_publication_enabled", lambda: False)
    monkeypatch.setattr(imports, "get_import_run", AsyncMock(return_value=current))
    protected, worker = AsyncMock(return_value={"status": "succeeded"}), AsyncMock()
    monkeypatch.setattr(imports, "_request_nucc_cancel", protected)
    monkeypatch.setattr(imports, "_request_worker_cancel", worker)
    assert await imports.request_cancel(RUN_ID) == {"status": "succeeded"}
    protected.assert_awaited_once_with(RUN_ID)
    worker.assert_not_awaited()


@pytest.mark.parametrize("receipt", [None, {"synthetic": "committed"}])
async def test_action_recovers_closed_handoff_only_after_owner_outcome_absence(
    monkeypatch, closed_handoff_boundary, receipt
):
    events = []
    current, recovered = native_run("running"), native_run("finalizing", handoff=True)

    @asynccontextmanager
    async def transaction():
        yield "original-session"

    async def outcome(*_args):
        events.append("owner-outcome")
        return receipt

    async def bridge(session, run_id):
        assert session == "original-session" and run_id == RUN_ID
        events.append("closed-stage")
        return recovered["metrics"]["nucc_handoff"]

    async def capture(_session):
        events.append("parent")

    closed_handoff_boundary.side_effect = bridge
    monkeypatch.setattr(source, "db", SimpleNamespace(transaction=transaction, _transaction_binding=lambda: None))
    monkeypatch.setattr(source, "_is_protected_nucc_claim_parent", AsyncMock(return_value=True))
    monkeypatch.setattr(source.control, "reconcile_source_publication", outcome)
    monkeypatch.setattr(source, "capture_nucc_parent", capture)
    monkeypatch.setattr(source.control, "_source_transaction", AsyncMock(return_value="actor"))
    monkeypatch.setattr(
        source.control,
        "_locked_source_run",
        AsyncMock(side_effect=[current, recovered] if receipt is None else [current]),
    )
    monkeypatch.setattr(source.control, "_recheck_source_transaction", AsyncMock())
    async with source.source_action(RUN_ID) as outcome:
        assert outcome[0] is (recovered if receipt is None else current)
        events.append("action")
    assert events == (
        ["owner-outcome", "parent", "closed-stage", "action"]
        if receipt is None
        else ["owner-outcome", "parent", "action"]
    )
    assert closed_handoff_boundary.await_count == int(receipt is None)


async def test_source_action_reuses_the_original_writer_without_a_nested_transaction(
    monkeypatch, closed_handoff_boundary
):
    session, actor = object(), object()
    transaction = Mock(side_effect=AssertionError("Source actions must not nest the existing writer transaction"))
    monkeypatch.setattr(
        source,
        "db",
        SimpleNamespace(transaction=transaction, _transaction_binding=lambda: SimpleNamespace(session=session)),
    )
    monkeypatch.setattr(source, "_is_protected_nucc_claim_parent", AsyncMock(return_value=True))
    monkeypatch.setattr(source.control, "reconcile_source_publication", AsyncMock(return_value=None))
    monkeypatch.setattr(source, "capture_nucc_parent", AsyncMock())
    monkeypatch.setattr(source.control, "_source_transaction", AsyncMock(return_value=actor))
    monkeypatch.setattr(source.control, "_locked_source_run", AsyncMock(return_value=native_run()))
    monkeypatch.setattr(source.control, "_recheck_source_transaction", AsyncMock())
    async with source.source_action(RUN_ID) as outcome:
        assert outcome == (native_run(), None)
    transaction.assert_not_called()
    closed_handoff_boundary.assert_awaited_once_with(session, RUN_ID)
    source.control._recheck_source_transaction.assert_awaited_once_with(session, actor)
