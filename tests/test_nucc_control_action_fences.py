# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import threading
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.dialects import postgresql

from api import control_imports, control_workers, nucc_source_control
from process import nucc_protected_control, nucc_protected_source
from process.nucc_protected_source import reconcile_nucc_source_handoff as real_reconcile_nucc_source_handoff


def _source_action_fixture(monkeypatch, state, transaction):
    """Model the separately tested native observations on one explicit unit session."""
    events, session, actor = state.events, state.session, object()
    read_run = control_imports.get_import_run

    async def source_actor(actual):
        assert actual is session and state.active
        return actor

    async def locked_run(actual, run_id):
        assert actual is session and state.active and run_id == state.current["run_id"]
        events.append("read")
        return state.current

    async def recheck(actual, original):
        assert actual is session and original is actor and state.active

    async def committed_run(run_id):
        assert run_id == state.current["run_id"] and not state.active
        return await read_run(run_id)

    def binding():
        return SimpleNamespace(session=session) if state.active else None

    monkeypatch.setattr(control_imports.db, "_transaction_binding", binding)
    monkeypatch.setattr(
        nucc_source_control, "db", SimpleNamespace(transaction=transaction, _transaction_binding=lambda: None)
    )
    monkeypatch.setattr(nucc_protected_control, "_source_transaction", source_actor)
    monkeypatch.setattr(nucc_protected_control, "_locked_source_run", locked_run)
    monkeypatch.setattr(nucc_protected_control, "_recheck_source_transaction", recheck)
    monkeypatch.setattr(control_imports, "get_import_run", committed_run)


@pytest.fixture
def action_state(monkeypatch):
    """Install a Source transaction double with observable commit and action order."""
    state = _new_action_state()
    events, session = state.events, state.session

    @asynccontextmanager
    async def transaction():
        events.append("begin")
        state.active = True
        try:
            yield session
        except BaseException:
            events.append("rollback")
            raise
        else:
            events.append("commit")
        finally:
            state.active = False

    async def classify(actual):
        assert actual is session
        events.append("classify")
        return state.protected

    async def reconcile(actual, run_id):
        assert actual is session and run_id == "run_nucc"
        events.append("reconcile")
        if isinstance(state.receipt, Exception):
            raise state.receipt
        return state.receipt

    async def capture(actual):
        assert actual is session
        events.append("parent")

    async def execute(statement):
        events.append("read")
        return SimpleNamespace(
            first=lambda: SimpleNamespace(_mapping=state.current), rowcount=0, scalar_one_or_none=lambda: state.current
        )

    async def status(statement):
        assert statement.compile().params["status"] == "canceling"
        events.append("cancel-cas")
        return state.cas

    monkeypatch.setattr(control_imports.db, "transaction", transaction)
    monkeypatch.setattr(control_imports.db, "execute", execute)
    monkeypatch.setattr(control_imports.db, "status", status)

    _source_action_fixture(monkeypatch, state, transaction)

    monkeypatch.setattr(nucc_source_control, "_is_protected_nucc_claim_parent", classify)
    monkeypatch.setattr(nucc_protected_control, "reconcile_source_publication", reconcile)
    monkeypatch.setattr(nucc_protected_source, "reconcile_nucc_source_handoff", AsyncMock(return_value=None))
    monkeypatch.setattr(nucc_source_control, "capture_nucc_parent", capture)
    monkeypatch.setattr(control_imports, "normalize_run", lambda row: dict(row))
    monkeypatch.setattr(control_imports, "enqueue_status_event", lambda row: events.append("project"))
    monkeypatch.setattr(control_imports, "_write_run_live_progress", lambda *a, **k: None)
    return state


async def _action(kind, state):
    if kind == "enqueue":
        return await control_imports._enqueue_nucc_run(state.current, {})
    if kind == "cancel":
        return await control_imports._request_nucc_cancel("run_nucc")
    return await control_workers.guarded_ensure_worker({"run_id": "run_nucc", "importer": "nucc"})


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["enqueue", "cancel", "ensure"])
async def test_publication_receipt_wins_without_external_action(monkeypatch, action_state, kind):
    state = action_state
    state.receipt = {"authenticated": "receipt"}
    state.current.update(status="succeeded", phase_detail="published")
    external = AsyncMock(side_effect=AssertionError("external action after publication"))
    monkeypatch.setattr(control_imports, "_enqueue_import_start", external)
    monkeypatch.setattr(control_imports, "_enqueue_selected_import", external)
    monkeypatch.setattr(control_imports, "_cancel_signal_for_run", external)
    monkeypatch.setattr(control_workers, "ensure_worker", lambda *_: pytest.fail("launched after publication"))
    result = await _action(kind, state)
    assert result["status"] == ("failed" if kind == "ensure" else "succeeded")
    assert state.events == ["begin", "classify", "reconcile", "parent", "read", "commit", "read", "project"]
    external.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["enqueue", "cancel", "ensure"])
async def test_reconciliation_error_is_never_absence(monkeypatch, action_state, kind):
    action_state.receipt = RuntimeError("custody unavailable")
    monkeypatch.setattr(control_imports, "_enqueue_import_start", AsyncMock(side_effect=AssertionError("enqueue")))
    monkeypatch.setattr(control_imports, "_cancel_signal_for_run", AsyncMock(side_effect=AssertionError("signal")))
    monkeypatch.setattr(control_workers, "ensure_worker", lambda *_: pytest.fail("launch"))
    with pytest.raises(RuntimeError, match="custody unavailable"):
        await _action(kind, action_state)
    assert action_state.events == ["begin", "classify", "reconcile", "rollback"]


@pytest.mark.asyncio
async def test_cancel_cas_precedes_signal_and_committed_projection(monkeypatch, action_state):
    async def signal(*args, **kwargs):
        assert action_state.events[-1] == "cancel-cas"
        action_state.events.append("signal")
        return {"cancel_flag": True}

    async def persist(current, *, cancel_signal, publish):
        assert cancel_signal == {"cancel_flag": True} and publish is False
        action_state.events.append("persist")
        action_state.current.update(status="canceling")
        return {**current, "status": "canceling"}

    monkeypatch.setattr(control_imports, "_cancel_signal_for_run", signal)
    monkeypatch.setattr(control_imports, "_request_worker_cancel", persist)
    assert (await _action("cancel", action_state))["status"] == "canceling"
    assert action_state.events[-6:] == ["cancel-cas", "signal", "persist", "commit", "read", "project"]


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["canceling", "canceled"])
async def test_repeated_cancel_keeps_real_handoff_recovery_before_the_first_cancellation(
    monkeypatch, action_state, status
):
    state = action_state
    state.current.update(
        status="running",
        progress={"attempt_id": "run_nucc:" + "a" * 32, "attempt_started_at": "2026-01-02T00:00:00Z"},
    )
    bridge = AsyncMock(wraps=real_reconcile_nucc_source_handoff)
    monkeypatch.setattr(nucc_protected_source, "reconcile_nucc_source_handoff", bridge)
    signal = AsyncMock(return_value={"cancel_flag": True})

    async def persist(current, **_options):
        state.current.update(status=status)
        return state.current

    monkeypatch.setattr(control_imports, "_cancel_signal_for_run", signal)
    monkeypatch.setattr(control_imports, "_request_worker_cancel", persist)
    assert (await _action("cancel", state))["status"] == status
    assert (await _action("cancel", state))["status"] == status
    bridge.assert_awaited_once_with(state.session, "run_nucc")
    assert state.events.count("reconcile") == 3
    assert signal.await_count == (2 if status == "canceling" else 1)
    assert "rollback" not in state.events


@pytest.mark.asyncio
async def test_cancel_cas_failure_does_not_signal(monkeypatch, action_state):
    action_state.cas = 0
    signal = AsyncMock(side_effect=AssertionError("signal after stale_by_field CAS"))
    monkeypatch.setattr(control_imports, "_cancel_signal_for_run", signal)
    with pytest.raises(RuntimeError, match="attempt changed"):
        await _action("cancel", action_state)
    signal.assert_not_awaited()
    assert action_state.events[-1] == "rollback"


@pytest.mark.asyncio
async def test_ensure_cancellation_drains_thread_before_releasing_fence(monkeypatch, action_state):
    entered = threading.Event()
    release = threading.Event()

    def launch(_payload):
        action_state.events.append("launch")
        entered.set()
        assert release.wait(2), "test did not release launch"
        action_state.events.append("settled")
        return {"status": "started"}

    monkeypatch.setattr(control_workers, "ensure_worker", launch)
    task = asyncio.create_task(_action("ensure", action_state))
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        task.cancel()
        await asyncio.sleep(0)
        assert "rollback" not in action_state.events and "commit" not in action_state.events
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert action_state.events[-2:] == ["settled", "rollback"]


@pytest.mark.asyncio
async def test_enqueue_cancellation_drains_started_redis_before_fence_exit(monkeypatch, action_state):
    entered = asyncio.Event()
    release = asyncio.Event()

    async def enqueue(values):
        assert values["run_id"] == "run_nucc"
        action_state.events.append("enqueue")
        entered.set()
        await release.wait()
        action_state.events.append("settled")
        return {"status": "queued"}

    monkeypatch.setattr(control_imports, "_enqueue_selected_import", enqueue)
    task = asyncio.create_task(_action("enqueue", action_state))
    await asyncio.wait_for(entered.wait(), 2)
    task.cancel()
    await asyncio.sleep(0)
    assert "rollback" not in action_state.events
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert action_state.events[-2:] == ["settled", "rollback"]


@pytest.mark.asyncio
async def test_late_enqueue_ack_preserves_newer_attempt(monkeypatch, action_state):
    statements = []
    winner_by_field = {**action_state.current, "status": "running", "progress": {"attempt_id": "new"}}

    async def execute(statement):
        statements.append(statement)
        action_state.current = winner_by_field
        return SimpleNamespace(rowcount=0, scalar_one_or_none=lambda: winner_by_field)

    monkeypatch.setattr(control_imports.db, "execute", execute)
    stale_by_field = {**action_state.current, "heartbeat_at": None, "error": None}
    result = await control_imports._persist_enqueue_result("run_nucc", "nucc", stale_by_field)
    assert result == winner_by_field
    sql = str(statements[0].compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": False}))
    params = statements[0].compile().params
    assert params["status_1"] == "queued" and params["phase_detail_1"] == "created"
    assert all(
        key in params.values()
        for key in ("attempt_id", "attempt_started_at", "nucc_handoff", "nucc_native_publication", "job_id")
    )
    assert "finished_at IS NULL" in sql


@pytest.mark.asyncio
async def test_ordinary_nucc_parent_preserves_legacy_without_receipt_probe(monkeypatch, action_state):
    action_state.protected = False
    action_state.current["status"] = "succeeded"
    assert (await _action("cancel", action_state))["status"] == "succeeded"
    assert action_state.events == ["begin", "classify", "commit", "begin", "read", "commit", "read", "project"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {"importer": "nucc"},
        {"importer": "nucc", "queue": "arq:NPI", "run_id": "run_nucc"},
        {"importer": "npi", "queue": "arq:NUCC", "run_id": "run_nucc"},
    ],
)
async def test_nucc_selector_refusal_precedes_database(action_state, payload):
    result = await control_workers.guarded_ensure_worker(payload)
    assert result["status"] == "failed"
    assert action_state.events == []


@pytest.mark.asyncio
async def test_changed_enqueue_params_never_enter_redis(monkeypatch, action_state):
    stale_by_field = {**action_state.current, "params": {"test_mode": True}}
    enqueue = AsyncMock(side_effect=AssertionError("stale_by_field enqueue"))
    monkeypatch.setattr(control_imports, "_enqueue_selected_import", enqueue)
    with pytest.raises(RuntimeError, match="parameters changed"):
        await control_imports._enqueue_nucc_run(stale_by_field, {"test_mode": True})
    enqueue.assert_not_awaited()
    assert action_state.events[-1] == "rollback"


@pytest.mark.asyncio
async def test_protected_enqueue_uses_run_bound_job_identity(monkeypatch, action_state):
    calls = []

    class Queue:
        async def enqueue_job(self, function, task, **options):
            calls.append((function, task, options))
            return SimpleNamespace(job_id=options["_job_id"])

    monkeypatch.setattr(control_imports, "create_pool", AsyncMock(return_value=Queue()))
    result = await control_imports._enqueue_import_start(action_state.current)
    assert result["metrics"]["job_id"] == "nucc_start_run_nucc"
    assert calls[0][2] == {"_queue_name": "arq:NUCC", "_job_id": "nucc_start_run_nucc"}
    assert calls[0][1]["run_id"] == "run_nucc"


@pytest.mark.asyncio
async def test_nucc_get_does_not_terminalize_before_reconciliation(monkeypatch, action_state):
    terminalize = AsyncMock(side_effect=AssertionError("premature terminalization"))
    monkeypatch.setattr(control_imports, "_sync_terminal_worker_failure", terminalize)
    assert (await control_imports.get_import_run("run_nucc"))["status"] == "queued"
    terminalize.assert_not_awaited()


def test_missing_enqueue_ack_retains_exact_cleanup_identity(action_state):
    current_by_field = {**action_state.current, "metrics": {}}
    identity = control_imports._arq_cleanup_identity(current_by_field)
    assert identity[0:2] == ("run_nucc", "nucc")
    assert identity[3:] == ("arq:NUCC", "nucc_start_run_nucc")
    assert control_imports._safe_enqueued_job_id("nucc", {}, None, requested_job_id=identity[-1]) == identity[-1]
    with pytest.raises(RuntimeError, match="acknowledgement identity changed"):
        control_imports._safe_enqueued_job_id(
            "nucc", {}, SimpleNamespace(job_id="foreign"), requested_job_id=identity[-1]
        )


def _new_action_state():
    current_by_field = {
        "run_id": "run_nucc",
        "importer": "nucc",
        "node_id": "synthetic-node",
        "engine": "healthcare-mrf-api",
        "status": "queued",
        "phase_detail": "created",
        "progress": {},
        "metrics": {},
        "params": {},
        "finished_at": None,
    }
    return SimpleNamespace(
        events=[],
        active=False,
        session=SimpleNamespace(scalar=AsyncMock(return_value={"idle_ms": 0, "transaction_remaining_ms": None})),
        current=current_by_field,
        receipt=None,
        protected=True,
        cas=1,
    )


def test_nucc_deadline_closes_external_admission():
    assert control_workers._nucc_subprocess_options() == {}
    token = control_workers._NUCC_ACTION_DEADLINE.set(0)
    try:
        with pytest.raises(RuntimeError, match="deadline expired"):
            control_workers._kubernetes_request("GET", "/never")
        with pytest.raises(RuntimeError, match="deadline expired"):
            control_workers._nucc_subprocess_options()
    finally:
        control_workers._NUCC_ACTION_DEADLINE.reset(token)


@pytest.mark.parametrize(
    "limits, expected",
    [
        ({"idle_ms": 0, "transaction_remaining_ms": None}, 45),
        ({"idle_ms": 10000, "transaction_remaining_ms": None}, 8),
        ({"idle_ms": 60000, "transaction_remaining_ms": 5000}, 3),
    ],
)
def test_nucc_budget_respects_database_limits(limits, expected):
    assert control_workers._bounded_nucc_action_budget(limits) == expected


@pytest.mark.parametrize(
    "limits",
    [
        {"idle_ms": 1000, "transaction_remaining_ms": None},
        {"idle_ms": 0, "transaction_remaining_ms": -1},
        {"idle_ms": True, "transaction_remaining_ms": None},
        {},
    ],
)
def test_nucc_budget_refuses_exhausted_fence(limits):
    with pytest.raises(RuntimeError, match="NUCC transaction timeout"):
        control_workers._bounded_nucc_action_budget(limits)
