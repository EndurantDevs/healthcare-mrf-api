# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import datetime as dt
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from db import connection as db_connection
from process import control_lifecycle
from process.control_cancel import ImportCancelledError
from process.control_lifecycle import control_single_job_start


class NestedProgressHarness:
    def __init__(self):
        self.progress_by_write = []

    async def persist_update(self, statement):
        values_by_field = {
            getattr(key, "key", str(key)): getattr(value, "value", value) for key, value in statement._values.items()
        }
        self.progress_by_write.append(values_by_field["progress"])
        return 1

    async def target(self, _ctx, task_by_field):
        await control_lifecycle.mark_control_run(
            task_by_field["run_id"],
            status="running",
            phase_detail="target work",
            progress_message="working",
        )
        return {"rows": 1}


@pytest.mark.asyncio
@pytest.mark.parametrize("target_module", ["process.places_zcta", "process.entity_address_unified", "process.nucc"])
@pytest.mark.parametrize("late_error", [None, ImportCancelledError, RuntimeError])
async def test_committed_native_handoff_is_not_overwritten_as_terminal(monkeypatch, target_module, late_error):
    committed_by_field = {"native_handoff": "synthetic"}

    async def target(ctx, _task):
        ctx["context"].update(control_run_handoff_committed=True, _control_committed_result=committed_by_field)
        if late_error:
            raise late_error("synthetic late failure")
        return committed_by_field

    marks = AsyncMock(return_value=True)
    monkeypatch.setenv("HLTHPRT_IMPORT_LIVE_PROGRESS_HEARTBEAT_SECONDS", "0")
    monkeypatch.setattr(control_lifecycle, "mark_control_run", marks)
    monkeypatch.setattr(control_lifecycle, "_flush_terminal_status_events", AsyncMock())
    monkeypatch.setattr(control_lifecycle, "import_module", lambda _name: SimpleNamespace(process_data=target))
    outcome = await control_single_job_start(
        {}, {"run_id": "synthetic_handoff", "target_module": target_module, "target_function": "process_data"}
    )
    assert outcome == {"status": "finalizing", "run_id": "synthetic_handoff", "result": committed_by_field}
    assert [call.kwargs["status"] for call in marks.await_args_list] == ["running"]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError, ImportCancelledError, asyncio.CancelledError])
async def test_uncertain_nucc_commit_does_not_overwrite_persisted_custody(monkeypatch, failure):
    async def target(ctx, _task):
        ctx["context"]["nucc_native_commit_unknown"] = True
        raise failure("synthetic readback lost")

    marks = AsyncMock(return_value=True)
    monkeypatch.setenv("HLTHPRT_IMPORT_LIVE_PROGRESS_HEARTBEAT_SECONDS", "0")
    monkeypatch.setattr(control_lifecycle, "mark_control_run", marks)
    monkeypatch.setattr(control_lifecycle, "import_module", lambda _name: SimpleNamespace(process_data=target))
    with pytest.raises(failure):
        await control_single_job_start(
            {}, {"run_id": "synthetic_unknown", "target_module": "process.nucc", "target_function": "process_data"}
        )
    assert [call.kwargs["status"] for call in marks.await_args_list] == ["running"]


def test_nucc_attempt_custody_is_not_copied_from_shared_worker_context():
    context_by_field = {
        "context": {
            "nucc_native_stage": "old",
            "nucc_native_predecessor": "old",
            "nucc_native_commit_unknown": True,
            "start": "original",
        }
    }
    isolated_by_field = control_lifecycle._isolated_control_job_context(context_by_field, "new")
    assert isolated_by_field["context"] == {"start": "original", "control_run_id": "new"}
    assert context_by_field["context"]["nucc_native_stage"] == "old"


@pytest.mark.asyncio
async def test_control_single_job_start_marks_cancelled(monkeypatch):
    marks = []

    async def is_control_run_marked(run_id, **kwargs):
        marks.append((run_id, kwargs))
        return True

    async def fake_target(_ctx, _task):
        raise ImportCancelledError("cancelled")

    class FakeModule:
        process_data = staticmethod(fake_target)

    monkeypatch.setattr(
        control_lifecycle,
        "mark_control_run",
        is_control_run_marked,
    )
    monkeypatch.setattr(control_lifecycle, "import_module", lambda _name: FakeModule)

    control_outcome = await control_single_job_start(
        {},
        {"run_id": "run_1", "target_module": "fake.module", "target_function": "process_data"},
    )

    assert control_outcome["status"] == "canceled"
    assert [item[1]["status"] for item in marks] == ["running", "canceled"]


@pytest.mark.asyncio
async def test_control_single_job_start_marks_cancelled_task_failed(monkeypatch):
    marks = []

    async def is_control_run_marked(run_id, **kwargs):
        marks.append((run_id, kwargs))
        return True

    async def fake_target(_ctx, _task):
        raise asyncio.CancelledError()

    class FakeModule:
        process_data = staticmethod(fake_target)

    monkeypatch.setattr(
        control_lifecycle,
        "mark_control_run",
        is_control_run_marked,
    )
    monkeypatch.setattr(control_lifecycle, "import_module", lambda _name: FakeModule)

    control_outcome = await control_single_job_start(
        {},
        {"run_id": "run_1", "target_module": "fake.module", "target_function": "process_data"},
    )

    assert control_outcome["status"] == "failed"
    assert [item[1]["status"] for item in marks] == ["running", "failed"]
    assert marks[-1][1]["error"]["code"] == "import_interrupted"


@pytest.mark.asyncio
async def test_control_target_progress_inherits_wrapper_attempt(monkeypatch):
    """Nested progress preserves ownership through the terminal transition."""

    harness = NestedProgressHarness()
    monkeypatch.setenv(
        "HLTHPRT_IMPORT_LIVE_PROGRESS_HEARTBEAT_SECONDS",
        "0",
    )
    monkeypatch.setattr(
        control_lifecycle,
        "_execute_control_run_update",
        harness.persist_update,
    )
    monkeypatch.setattr(
        control_lifecycle,
        "write_live_progress",
        lambda **_payload: True,
    )
    monkeypatch.setattr(
        control_lifecycle,
        "import_module",
        lambda _name: SimpleNamespace(process_data=harness.target),
    )

    control_outcome = await control_single_job_start(
        {},
        {
            "run_id": "run_nested_progress",
            "target_module": "fake.module",
            "target_function": "process_data",
        },
    )

    assert control_outcome["status"] == "succeeded"
    assert len(harness.progress_by_write) == 3
    attempt_pairs = {
        (
            progress["attempt_id"],
            progress["attempt_started_at"],
        )
        for progress in harness.progress_by_write
    }
    assert len(attempt_pairs) == 1


def _audit_only_terminal_progress() -> dict[str, object]:
    return {
        "unit": "audit_requests",
        "done": 26,
        "total": 26,
        "pct": 100,
        "message": "retained passing attestation without promotion",
        "phase": "candidate audit-only complete",
    }


@pytest.mark.asyncio
async def test_audit_only_terminal_progress_survives_control_wrapper(
    monkeypatch,
):
    """Retain the audit-only terminal phase through the outer wrapper."""

    marks = []
    terminal_progress_by_field = _audit_only_terminal_progress()

    async def is_run_marked(run_id, **kwargs):
        marks.append((run_id, kwargs))
        return True

    async def main():
        return {
            "candidate_audit_mode": "audit_only",
            "activation_status": "deferred",
            "count": 26,
            "terminal_progress": terminal_progress_by_field,
        }

    monkeypatch.setenv(
        "HLTHPRT_IMPORT_LIVE_PROGRESS_HEARTBEAT_SECONDS",
        "0",
    )
    monkeypatch.setattr(
        control_lifecycle,
        "mark_control_run",
        is_run_marked,
    )
    monkeypatch.setattr(
        control_lifecycle,
        "_flush_terminal_status_events",
        AsyncMock(),
    )
    monkeypatch.setattr(
        control_lifecycle,
        "import_module",
        lambda _name: SimpleNamespace(main=main),
    )

    outcome = await control_single_job_start(
        {},
        {
            "run_id": "run_audit_only",
            "target_module": "fake.audit",
            "target_function": "main",
            "call_style": "kwargs",
        },
    )

    assert outcome["status"] == "succeeded"
    assert marks[-1][1]["phase_detail"] == "candidate audit-only complete"
    assert marks[-1][1]["progress_message"] == ("retained passing attestation without promotion")
    assert marks[-1][1]["progress"] == terminal_progress_by_field
    assert marks[-1][1]["metrics"]["count"] == 26


@pytest.mark.asyncio
async def test_control_run_update_uses_base_database_then_restores_override(monkeypatch):
    calls = []

    class FakeDb:
        _database_override = "healthporta_test"

        def _transaction_binding(self):
            return None

        def _reader_binding(self):
            return None

        async def connect(self):
            calls.append(("connect", self._database_override))

        async def execute(self, stmt):
            calls.append(("execute", stmt, self._database_override))

    fake_db = FakeDb()
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", "healthporta")
    monkeypatch.setattr(control_lifecycle, "db", fake_db)

    await control_lifecycle._execute_control_run_update("UPDATE")

    assert fake_db._database_override == "healthporta_test"
    assert calls == [
        ("connect", "healthporta"),
        ("execute", "UPDATE", "healthporta"),
        ("connect", "healthporta_test"),
    ]


@pytest.mark.asyncio
async def test_mark_control_run_throttles_repeated_running_db_update(monkeypatch):
    db_updates = []
    live_events = []
    status_events = []

    async def fake_update(stmt):
        db_updates.append(stmt)
        return 1

    monkeypatch.setenv("HLTHPRT_CONTROL_RUN_DB_UPDATE_THROTTLE_SECONDS", "60")
    monkeypatch.setattr(
        control_lifecycle,
        "_claim_control_run_db_update_slot",
        lambda _key, _seconds: False,
    )
    monkeypatch.setattr(control_lifecycle, "_execute_control_run_update", fake_update)

    def is_live_progress_captured(**payload):
        live_events.append(payload)
        status_events.append(payload["status_event_payload"])
        return True

    monkeypatch.setattr(
        control_lifecycle,
        "write_live_progress",
        is_live_progress_captured,
    )

    await control_lifecycle.mark_control_run(
        "run_1",
        status="running",
        phase_detail="mrf provider jobs running",
        progress_message="processed provider file",
        metrics={"last_provider_records": 123},
    )

    assert db_updates == []
    assert live_events[-1]["run_id"] == "run_1"
    assert live_events[-1]["status"] == "running"
    assert status_events[-1]["phase_detail"] == "mrf provider jobs running"


def test_control_run_heartbeat_update_values_prefers_live_progress():
    now = dt.datetime(2026, 6, 21, 12, 0, 0)
    values = control_lifecycle._control_run_heartbeat_update_values(
        "process_data",
        {
            "phase": "compact-serving scanner",
            "unit": "compressed_bytes",
            "done": 1048576,
            "total": 2097152,
            "pct": 50,
            "message": "compact-serving scanner 50.00%",
            "updated_at": "2026-06-21T12:00:00Z",
        },
        now,
    )

    assert values["status"] == "running"
    assert values["phase_detail"] == "compact-serving scanner"
    assert values["heartbeat_at"] == now
    assert values["finished_at"] is None
    assert values["progress"] == {
        "unit": "compressed_bytes",
        "done": 1048576,
        "total": 2097152,
        "pct": 50,
        "message": "compact-serving scanner 50.00%",
        "phase": "compact-serving scanner",
        "updated_at": "2026-06-21T12:00:00Z",
    }


def test_control_run_heartbeat_update_values_preserves_live_progress_detail():
    now = dt.datetime(2026, 6, 29, 14, 0, 0)
    detail_by_field = {
        "active_source_groups": [
            {
                "sample_source_id": "source_a",
                "sample_org_name": "Cigna",
                "current_resource": "PractitionerRole",
            }
        ]
    }

    values = control_lifecycle._control_run_heartbeat_update_values(
        "process_data",
        {
            "phase": "provider-directory importing resources",
            "unit": "steps",
            "done": 8,
            "total": 25,
            "pct": 32,
            "message": "imported resources for 8/25 source group(s)",
            "detail": detail_by_field,
            "updated_at": "2026-06-29T14:00:00Z",
        },
        now,
    )

    assert values["progress"]["detail"] == detail_by_field


@pytest.mark.asyncio
async def test_mark_control_run_always_persists_terminal_update(monkeypatch):
    db_updates = []

    async def fake_update(stmt):
        db_updates.append(stmt)
        return 1

    def fail_slot(_key, _seconds):
        raise AssertionError("terminal updates must not consult the running throttle")

    monkeypatch.setenv("HLTHPRT_CONTROL_RUN_DB_UPDATE_THROTTLE_SECONDS", "60")
    monkeypatch.setattr(control_lifecycle, "_claim_control_run_db_update_slot", fail_slot)
    monkeypatch.setattr(control_lifecycle, "_execute_control_run_update", fake_update)
    monkeypatch.setattr(
        control_lifecycle,
        "write_live_progress",
        lambda **_payload: True,
    )

    await control_lifecycle.mark_control_run(
        "run_1",
        status="succeeded",
        phase_detail="mrf import published",
        progress_message="succeeded",
    )

    assert len(db_updates) == 1


@pytest.mark.asyncio
async def test_mark_control_run_can_bind_preterminal_owner(monkeypatch):
    db_updates = []

    async def fake_update(stmt):
        db_updates.append(stmt)
        return 1

    monkeypatch.setattr(control_lifecycle, "_execute_control_run_update", fake_update)
    monkeypatch.setattr(
        control_lifecycle,
        "write_live_progress",
        lambda **_payload: True,
    )

    await control_lifecycle.mark_control_run(
        "run_hospital",
        status="failed",
        phase_detail="target rejected",
        progress_message="failed",
        error={"code": "control_target_rejected"},
        expected_state=("hospital-prices", "queued"),
    )

    statement = db_updates[0]
    compiled = statement.compile()
    assert "import_run.importer" in str(statement.whereclause)
    assert "hospital-prices" in compiled.params.values()
    assert "queued" in compiled.params.values()


@pytest.mark.asyncio
async def test_mark_control_run_fails_closed_without_exactly_one_row(
    monkeypatch,
):
    live_writes = []

    async def ambiguous_update(_stmt):
        return 2

    monkeypatch.setattr(
        control_lifecycle,
        "_execute_control_run_update",
        ambiguous_update,
    )
    monkeypatch.setattr(
        control_lifecycle,
        "write_live_progress",
        lambda **payload: live_writes.append(payload),
    )

    accepted = await control_lifecycle.mark_control_run(
        "run_ambiguous",
        status="succeeded",
        phase_detail="succeeded",
        progress_message="succeeded",
        attempt_id="run_ambiguous:attempt",
        attempt_started_at="2026-07-23T12:00:00.000000+00:00",
    )

    assert accepted is False
    assert live_writes == []


@pytest.mark.asyncio
async def test_mark_control_run_can_preserve_existing_finished_at(monkeypatch):
    db_updates = []
    live_events = []
    status_events = []

    async def fake_update(stmt):
        db_updates.append(stmt)
        return 1

    monkeypatch.setattr(control_lifecycle, "_execute_control_run_update", fake_update)

    def is_live_progress_captured(**payload):
        live_events.append(payload)
        status_events.append(payload["status_event_payload"])
        return True

    monkeypatch.setattr(
        control_lifecycle,
        "write_live_progress",
        is_live_progress_captured,
    )

    await control_lifecycle.mark_control_run(
        "run_1",
        status="succeeded",
        phase_detail="entity-address-unified post-publish indexes warmed",
        progress_message="post-publish indexes warmed",
        preserve_finished_at=True,
    )

    values_by_field = {getattr(key, "key", str(key)): field_value for key, field_value in db_updates[0]._values.items()}
    assert "finished_at" not in values_by_field
    assert live_events[-1]["finished_at"] is None
    assert status_events[-1]["finished_at"] is None


class _BoundControlDatabase(db_connection.Database):
    def __setattr__(self, name, value):
        if getattr(self, "watch_custody", False) and name in {"_database_override", "engine", "session_factory"}:
            self.custody_writes.append((name, value))
        super().__setattr__(name, value)


class _BoundControlSession:
    def __init__(self, result):
        self.bind = SimpleNamespace(url=SimpleNamespace(database="synthetic_control"))
        self.active = True
        self.nested = False
        self.result = result
        self.executed = []
        self.after_execute = None
        self.commit = AsyncMock(side_effect=AssertionError("owner alone commits"))
        self.rollback = AsyncMock(side_effect=AssertionError("owner alone rolls back"))
        self.close = AsyncMock(side_effect=AssertionError("owner alone closes"))

    def in_transaction(self):
        return self.active

    def in_nested_transaction(self):
        return self.nested

    async def execute(self, statement, params):
        self.executed.append((statement, params))
        if self.after_execute is not None:
            self.after_execute()
        if isinstance(self.result, BaseException):
            raise self.result
        return self.result


@contextmanager
def _bound_control_update(monkeypatch, result, *, bound=True, borrowed=True):
    database = _BoundControlDatabase()
    database._database_override = "synthetic_override"
    database.engine = object()
    database.session_factory = object()
    database.connect = AsyncMock(side_effect=AssertionError("borrowed update cannot reconnect"))
    database.disconnect = AsyncMock(side_effect=AssertionError("borrowed update cannot replace pool"))
    database.custody_writes = []
    database.watch_custody = True
    session = _BoundControlSession(result)
    binding = db_connection._TransactionBinding(id(database), session, asyncio.current_task(), borrowed=borrowed)
    transaction_token = db_connection._TRANSACTION.set((binding,) if bound else ())
    reader_token = db_connection._READER.set(None)
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", "synthetic_control")
    monkeypatch.setenv("HLTHPRT_CONTROL_RUN_DB_UPDATE_THROTTLE_SECONDS", "30")
    monkeypatch.setattr(control_lifecycle, "db", database)
    try:
        yield SimpleNamespace(db=database, session=session, binding=binding)
    finally:
        db_connection._READER.reset(reader_token)
        db_connection._TRANSACTION.reset(transaction_token)
        database.connect.assert_not_awaited()
        database.disconnect.assert_not_awaited()
        session.commit.assert_not_awaited()
        session.rollback.assert_not_awaited()
        session.close.assert_not_awaited()
        assert database._database_override == "synthetic_override"
        assert database.custody_writes == []


@pytest.mark.asyncio
@pytest.mark.parametrize("rows", [None, [], ["row"], ["row1", "row2"]])
@pytest.mark.parametrize("borrowed", [False, True])
async def test_bound_control_update_preserves_exact_returned_count_and_statement(monkeypatch, rows, borrowed):
    result = None if rows is None else SimpleNamespace(all=Mock(return_value=rows))
    statement = (
        control_lifecycle.update(control_lifecycle.ImportRun)
        .where(control_lifecycle.ImportRun.run_id == "synthetic_run")
        .returning(control_lifecycle.ImportRun.run_id)
    )
    with _bound_control_update(monkeypatch, result, borrowed=borrowed) as scope:
        changed = await control_lifecycle._execute_control_run_update(statement)
        assert changed == (0 if rows is None else len(rows))
        assert scope.session.executed == [(statement, {})]
        assert scope.db._transaction_binding() is scope.binding
        assert scope.session.active and not scope.session.nested


@pytest.mark.asyncio
async def test_bound_control_update_retains_result_all_failure_behavior(monkeypatch):
    result = SimpleNamespace(all=Mock(side_effect=ValueError("unavailable rows")))
    with _bound_control_update(monkeypatch, result) as scope:
        assert await control_lifecycle._execute_control_run_update("UPDATE synthetic") == 0
        assert len(scope.session.executed) == 1
        result.all.assert_called_once_with()


def _make_control_scope_unverified(monkeypatch, scope, invalid):
    """Apply the pre-execution scope defect selected by the test."""
    if invalid.endswith("database"):
        scope.session.bind.url.database = {
            "missing_database": None,
            "empty_database": "",
            "different_database": "synthetic_other",
        }[invalid]
        return
    if invalid == "ended":
        scope.session.active = False
        return
    if invalid == "nested":
        scope.session.nested = True
        return
    if invalid == "reader":
        db_connection._READER.set(scope.binding)
        return
    if invalid == "unknown_binding":
        db_connection._TRANSACTION.set((object(),))
        return
    monkeypatch.setattr(scope.db, "_session_database_name", Mock(side_effect=RuntimeError("identity unavailable")))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid",
    [
        "missing_database",
        "empty_database",
        "different_database",
        "ended",
        "nested",
        "reader",
        "unknown_binding",
        "identity_unavailable",
    ],
)
async def test_bound_control_update_rejects_unverified_scope_before_sql(monkeypatch, invalid):
    with _bound_control_update(monkeypatch, None) as scope:
        _make_control_scope_unverified(monkeypatch, scope, invalid)
        with pytest.raises((RuntimeError, AttributeError)):
            await control_lifecycle._execute_control_run_update("UPDATE synthetic")
        assert scope.session.executed == []


@pytest.mark.asyncio
async def test_unbound_reader_control_update_rejects_before_reconnect(monkeypatch):
    with _bound_control_update(monkeypatch, None, bound=False) as scope:
        db_connection._READER.set(scope.binding)
        with pytest.raises(RuntimeError, match="control_run_bound_transaction_invalid"):
            await control_lifecycle._execute_control_run_update("UPDATE synthetic")
        assert scope.session.executed == []


@pytest.mark.asyncio
async def test_bound_control_update_rejects_inherited_child_task_before_sql(monkeypatch):
    with _bound_control_update(monkeypatch, None) as scope:
        with pytest.raises(RuntimeError, match="child asyncio task"):
            await asyncio.create_task(control_lifecycle._execute_control_run_update("UPDATE synthetic"))
        assert scope.session.executed == []


def _lose_bound_control_scope(scope, lost):
    """Remove the selected authority after execution and before returned rows are read."""
    if lost == "binding":
        db_connection._TRANSACTION.set(())
        return
    if lost == "replacement_binding":
        db_connection._TRANSACTION.set(
            (db_connection._TransactionBinding(id(scope.db), scope.session, asyncio.current_task(), borrowed=True),)
        )
        return
    if lost == "reader":
        db_connection._READER.set(scope.binding)
        return
    if lost == "ended":
        scope.session.active = False
        return
    if lost == "nested":
        scope.session.nested = True
        return
    scope.session.bind.url.database = "synthetic_other" if lost == "database" else None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "lost", ["binding", "replacement_binding", "reader", "ended", "nested", "database", "missing_database"]
)
async def test_bound_control_update_rejects_scope_loss_after_execute_before_row_read(monkeypatch, lost):
    returned_rows = SimpleNamespace(all=Mock(return_value=["row"]))
    with _bound_control_update(monkeypatch, returned_rows) as scope:

        def lose_scope():
            _lose_bound_control_scope(scope, lost)

        scope.session.after_execute = lose_scope
        with pytest.raises(RuntimeError, match="control_run_bound_transaction_invalid"):
            await control_lifecycle._execute_control_run_update("UPDATE synthetic")
        assert len(scope.session.executed) == 1
        returned_rows.all.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [RuntimeError("executor failure"), asyncio.CancelledError()])
async def test_bound_control_update_propagates_executor_failure_without_lifecycle_work(monkeypatch, error):
    with _bound_control_update(monkeypatch, error) as scope:
        with pytest.raises(type(error)) as caught:
            await control_lifecycle._execute_control_run_update("UPDATE synthetic")
        assert caught.value is error
        assert len(scope.session.executed) == 1


@pytest.mark.asyncio
async def test_bound_control_run_retains_attempt_and_expected_state_fences(monkeypatch):
    monkeypatch.setattr(control_lifecycle, "write_live_progress", lambda **_payload: True)
    result = SimpleNamespace(all=Mock(return_value=["synthetic_run"]))
    with _bound_control_update(monkeypatch, result) as scope:
        accepted = await control_lifecycle.mark_control_run(
            "synthetic_run",
            status="failed",
            phase_detail="target rejected",
            progress_message="failed",
            attempt_id="synthetic_attempt",
            attempt_started_at="2026-01-01T00:00:00+00:00",
            expected_state=("synthetic_importer", "queued"),
        )
        assert accepted is True
        statement, params = scope.session.executed[0]
        assert params == {} and len(scope.session.executed) == 1
        where = str(statement.whereclause)
        assert "import_run.run_id" in where and "import_run.importer" in where and "import_run.status" in where
        assert "attempt_id" in str(statement.compile().params.values())
        values = statement.compile().params.values()
        assert "synthetic_importer" in values and "queued" in values and "synthetic_attempt" in values
        assert list(statement._returning) == [control_lifecycle.ImportRun.run_id]


@pytest.mark.asyncio
@pytest.mark.parametrize("returned_rows", [[], ["synthetic_run"]])
@pytest.mark.parametrize("borrowed", [False, True])
async def test_bound_running_progress_persists_without_optional_redis_throttle(monkeypatch, returned_rows, borrowed):
    """A verified owner receives the actual fenced write result even without an attempt."""
    live_events = []
    slot = Mock(side_effect=AssertionError("owned progress cannot use the optional throttle"))
    monkeypatch.setattr(control_lifecycle, "_claim_control_run_db_update_slot", slot)
    monkeypatch.setattr(control_lifecycle, "read_live_progress", lambda _run: None)
    monkeypatch.setattr(control_lifecycle, "write_live_progress", lambda **payload: live_events.append(payload))
    result = SimpleNamespace(all=Mock(return_value=returned_rows))
    with _bound_control_update(monkeypatch, result, borrowed=borrowed) as scope:
        marked = await control_lifecycle.mark_control_run(
            "synthetic_run", status="running", phase_detail="evidence window", progress_message="window complete"
        )
        assert marked is bool(returned_rows)
        assert len(scope.session.executed) == 1
        statement, parameters = scope.session.executed[0]
        assert parameters == {}
        compiled = statement.compile()
        assert compiled.params["progress_1"] == "attempt_started_at"
        assert "RETURNING" in str(compiled)
        assert len(live_events) == len(returned_rows)
        slot.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["ended", "nested", "reader", "different_database"])
async def test_bound_running_progress_rejects_invalid_owner_before_optional_throttle(monkeypatch, invalid):
    """A denied slot must not conceal loss of the admitted control transaction."""
    slot = Mock(return_value=False)
    monkeypatch.setattr(control_lifecycle, "_claim_control_run_db_update_slot", slot)
    monkeypatch.setattr(control_lifecycle, "read_live_progress", lambda _run: None)
    live = Mock()
    monkeypatch.setattr(control_lifecycle, "write_live_progress", live)
    with _bound_control_update(monkeypatch, None) as scope:
        _make_control_scope_unverified(monkeypatch, scope, invalid)
        with pytest.raises(RuntimeError, match="control_run_bound_transaction_invalid"):
            await control_lifecycle.mark_control_run(
                "synthetic_run", status="running", phase_detail="evidence window", progress_message="window complete"
            )
        assert scope.session.executed == []
        slot.assert_not_called()
        live.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError("synthetic failure"), asyncio.CancelledError()])
async def test_bound_running_progress_does_not_report_owner_failure_as_success(monkeypatch, failure):
    """Cancellation and SQL failure propagate without a live success or owner cleanup."""
    monkeypatch.setattr(control_lifecycle, "read_live_progress", lambda _run: None)
    live = Mock()
    monkeypatch.setattr(control_lifecycle, "write_live_progress", live)
    with _bound_control_update(monkeypatch, failure) as scope:
        with pytest.raises(type(failure)):
            await control_lifecycle.mark_control_run(
                "synthetic_run", status="running", phase_detail="evidence window", progress_message="window complete"
            )
        assert len(scope.session.executed) == 1
        live.assert_not_called()


@pytest.mark.asyncio
async def test_inherited_child_running_progress_cannot_bypass_owner_custody_with_throttle(monkeypatch):
    """A child task cannot reuse its parent's transaction or hide behind a denied slot."""
    slot = Mock(return_value=False)
    monkeypatch.setattr(control_lifecycle, "_claim_control_run_db_update_slot", slot)
    monkeypatch.setattr(control_lifecycle, "write_live_progress", Mock())
    with _bound_control_update(monkeypatch, None) as scope:
        with pytest.raises(RuntimeError, match="child asyncio task"):
            await asyncio.create_task(
                control_lifecycle.mark_control_run(
                    "synthetic_run",
                    status="running",
                    phase_detail="evidence window",
                    progress_message="window complete",
                )
            )
        assert scope.session.executed == []
        slot.assert_not_called()


@pytest.mark.asyncio
async def test_reader_only_running_progress_cannot_be_reported_as_throttled_success(monkeypatch):
    """A reader never gains progress-write authority through the optional cache."""
    slot = Mock(return_value=False)
    monkeypatch.setattr(control_lifecycle, "_claim_control_run_db_update_slot", slot)
    with _bound_control_update(monkeypatch, None, bound=False) as scope:
        db_connection._READER.set(scope.binding)
        with pytest.raises(RuntimeError, match="control_run_bound_transaction_invalid"):
            await control_lifecycle.mark_control_run(
                "synthetic_run", status="running", phase_detail="evidence window", progress_message="window complete"
            )
        assert scope.session.executed == []
        slot.assert_not_called()
