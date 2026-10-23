# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import datetime as dt
from contextlib import asynccontextmanager, contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from db import connection as db_connection
from process import control_lifecycle
from process.control_cancel import ImportCancelledError
from process.control_lifecycle import control_single_job_start


def _cms_owner_control_task():
    """Use the actual fixed native owner adapter's request shape."""
    return {
        "run_id": "cms_owner",
        "importer": "provider-directory-fhir",
        "target_module": "process.provider_directory_fhir",
        "target_function": "process_data",
        "run_shutdown": True,
        "task": {"selected": ["original"]},
    }


@pytest.mark.asyncio
async def test_void_target_preserves_recovery_metrics(monkeypatch):
    recovery_by_field = {"generation": "6b75740a-7c8a-556a-8e3c-21a9718c88f1", "phase": "published"}
    writes = []

    async def is_marked(_run_id, **updates_by_name):
        writes.append(updates_by_name)
        return True

    async def target(context, _task):
        context["context"]["rows"] = 2

    async def shutdown(context):
        context["context"]["serving_recovery"] = recovery_by_field
        await is_marked("run_example", status="succeeded", metrics={"rows": 2, "serving_recovery": recovery_by_field})

    monkeypatch.setenv("HLTHPRT_IMPORT_LIVE_PROGRESS_HEARTBEAT_SECONDS", "0")
    monkeypatch.setattr(control_lifecycle, "mark_control_run", is_marked)
    monkeypatch.setattr(control_lifecycle, "_flush_terminal_status_events", AsyncMock())
    monkeypatch.setattr(
        control_lifecycle, "import_module", lambda _name: SimpleNamespace(process_data=target, shutdown=shutdown)
    )
    outcome_by_field = await control_single_job_start(
        {"context": {}},
        {
            "run_id": "run_example",
            "importer": "entity-address-unified",
            "target_module": "process.entity_address_unified",
            "target_function": "process_data",
            "call_style": "ctx_task",
            "run_shutdown": True,
            "task": {},
        },
    )
    assert outcome_by_field["status"] == "succeeded" and outcome_by_field["result"] is None
    assert [write["status"] for write in writes] == ["running", "succeeded", "succeeded"]
    assert writes[-1]["attempt_id"] == writes[0]["attempt_id"]
    assert writes[-1]["metrics"] == {"rows": 2, "serving_recovery": recovery_by_field}


@pytest.mark.asyncio
@pytest.mark.parametrize("claim", [True, False, RuntimeError("claim unavailable")])
async def test_cms_owner_binding_follows_only_a_successful_native_claim(monkeypatch, claim):
    """A skipped or failed native claim exposes no CMS bootstrap or importer work."""
    from process.provider_directory_cms_source_runtime import CMSRegistrySourceRuntime

    events = []
    claimed_contexts = []
    target_callback = AsyncMock(return_value={"rows": 1})
    authenticator = AsyncMock()

    async def mark(_run_id, **values):
        events.append(values["status"])
        if values["status"] == "running":
            if isinstance(claim, Exception):
                raise claim
            return claim
        return True

    async def bind(**arguments):
        events.append("bind")
        attempt = arguments["source_attempt"]
        context = arguments["context"]
        assert context["context"]["_control_attempt_id"] == attempt.attempt_id
        assert context["context"]["_control_attempt_started_at"] == attempt.attempt_started_at
        assert arguments["task"] == {"run_id": "cms_owner", "selected": ["original"]}
        claimed_contexts.append(context)
        return authenticator

    binder = AsyncMock(side_effect=bind)
    runtime = CMSRegistrySourceRuntime(AsyncMock(side_effect=ValueError("unbound")), binder)
    original_context_by_field = {"job_id": "provider_directory_start_cms_owner", "cms_registry_source_runtime": runtime}

    async def invoke(context, task):
        events.append("target_callback")
        assert context is claimed_contexts[0]
        assert context["cms_registry_source_runtime"] is not runtime
        return await target_callback(context, task)

    monkeypatch.setenv("HLTHPRT_WORKER_ONCE_TARGET_JOB_ID", original_context_by_field["job_id"])
    monkeypatch.setenv("HLTHPRT_IMPORT_LIVE_PROGRESS_HEARTBEAT_SECONDS", "0")
    monkeypatch.setattr(control_lifecycle, "mark_control_run", mark)
    monkeypatch.setattr(control_lifecycle, "_flush_terminal_status_events", AsyncMock())
    importer_module = SimpleNamespace(process_data=invoke, shutdown=AsyncMock())
    monkeypatch.setattr(control_lifecycle, "import_module", lambda _name: importer_module)
    operation = control_single_job_start(original_context_by_field, _cms_owner_control_task())
    if isinstance(claim, Exception):
        with pytest.raises(RuntimeError, match="claim unavailable"):
            await operation
    else:
        outcome = await operation
        assert outcome["status"] == ("succeeded" if claim else "skipped")
    if claim is True:
        assert events == ["running", "bind", "target_callback", "succeeded"]
        binder.assert_awaited_once()
        target_callback.assert_awaited_once()
    else:
        assert events == ["running"]
        binder.assert_not_awaited()
        target_callback.assert_not_awaited()
    assert original_context_by_field["cms_registry_source_runtime"] is runtime
    assert "context" not in original_context_by_field


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
@pytest.mark.parametrize(
    "target_module",
    [
        "process.places_zcta",
        "process.entity_address_unified",
        "process.nucc",
        *sorted(control_lifecycle._SOURCE_PROFILE_IMPORTERS_BY_MODULE),
    ],
)
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
@pytest.mark.parametrize(
    "target_module", ["process.nucc", *sorted(control_lifecycle._SOURCE_PROFILE_IMPORTERS_BY_MODULE)]
)
async def test_uncertain_native_commit_does_not_overwrite_persisted_custody(monkeypatch, failure, target_module):
    async def target(ctx, _task):
        marker = "nucc_native_commit_unknown" if target_module == "process.nucc" else "source_profile_commit_unknown"
        ctx["context"][marker] = True
        raise failure("synthetic readback lost")

    marks = AsyncMock(return_value=True)
    monkeypatch.setenv("HLTHPRT_IMPORT_LIVE_PROGRESS_HEARTBEAT_SECONDS", "0")
    monkeypatch.setattr(control_lifecycle, "mark_control_run", marks)
    monkeypatch.setattr(control_lifecycle, "import_module", lambda _name: SimpleNamespace(process_data=target))
    with pytest.raises(failure):
        await control_single_job_start(
            {}, {"run_id": "synthetic_unknown", "target_module": target_module, "target_function": "process_data"}
        )
    assert [call.kwargs["status"] for call in marks.await_args_list] == ["running"]


def test_nucc_attempt_custody_is_not_copied_from_shared_worker_context():
    context_by_field = {
        "context": {
            "nucc_native_stage": "old",
            "nucc_native_predecessor": "old",
            "nucc_native_commit_unknown": True,
            "source_profile_commit_unknown": True,
            "start": "original",
        }
    }
    isolated_by_field = control_lifecycle._isolated_control_job_context(context_by_field, "new")
    assert isolated_by_field["context"] == {"start": "original", "control_run_id": "new"}
    assert context_by_field["context"]["nucc_native_stage"] == "old"


@pytest.mark.asyncio
async def test_kwargs_importer_receives_only_the_wrappers_trusted_context():
    received_contexts = []

    async def target(*, _control_context, source_keys):
        received_contexts.append((_control_context, source_keys))

    trusted_by_field = {"context": {"control_run_id": "actual"}}
    await control_lifecycle._invoke_control_target(
        SimpleNamespace(process_data=target),
        target_function="process_data",
        call_style="kwargs",
        control_context=trusted_by_field,
        target_task_by_field={"_control_context": {"forged": True}, "source_keys": ["one"]},
    )
    assert received_contexts == [(trusted_by_field, ["one"])]


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
    monkeypatch.setattr(control_lifecycle, "read_live_progress", lambda _run: None)
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
    monkeypatch.setattr(control_lifecycle, "read_live_progress", lambda _run: None)
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


@contextmanager
def _nucc_claim_scope(monkeypatch):
    """Reuse the existing control-session fixture for one short claim transaction."""
    from process import nucc_protected_control, nucc_protected_source, nucc_snapshot_partition

    events = []
    durable_run_by_field = _acknowledged_nucc_run()
    sql_result = SimpleNamespace(
        all=Mock(return_value=["nucc_run"]),
        mappings=Mock(return_value=SimpleNamespace(one_or_none=Mock(return_value=durable_run_by_field))),
    )
    with _bound_control_update(monkeypatch, sql_result, bound=False, borrowed=False) as scope:

        @asynccontextmanager
        async def transaction():
            events.append("begin")
            binding = db_connection._TransactionBinding(
                id(scope.db), scope.session, asyncio.current_task(), borrowed=False
            )
            token = db_connection._TRANSACTION.set((binding,))
            try:
                yield scope.session
            except BaseException:
                events.append("rollback")
                raise
            else:
                events.append("commit")
            finally:
                db_connection._TRANSACTION.reset(token)

        async def probe(_statement):
            events.append("probe")
            return scope.parent_kind

        scope.parent_kind = "p"
        scope.session.scalar = AsyncMock(side_effect=probe)
        scope.session.after_execute = lambda: events.append("claim")
        reconcile = AsyncMock(side_effect=lambda *_args: events.append("reconcile"))
        capture = AsyncMock(side_effect=lambda *_args: events.append("capture"))
        monkeypatch.setattr(scope.db, "transaction", transaction)
        monkeypatch.setattr(nucc_protected_control, "reconcile_source_publication", reconcile)
        monkeypatch.setattr(nucc_snapshot_partition, "capture_nucc_parent", capture)
        closed = AsyncMock(return_value=None)
        monkeypatch.setattr(nucc_protected_source, "reconcile_nucc_source_handoff", closed)
        yield SimpleNamespace(
            scope=scope,
            events=events,
            reconcile=reconcile,
            capture=capture,
            closed=closed,
            durable_run=durable_run_by_field,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("published", [False, True])
async def test_nucc_short_claim_reconciles_before_parent_and_same_session_cas(monkeypatch, published):
    """A committed receipt returns its exact durable row, while absence retains the claim CAS."""
    with _nucc_claim_scope(monkeypatch) as case:
        receipt = case.durable_run["metrics"]["nucc_native_publication"] if published else None
        if published:
            case.scope.session.after_execute = lambda: case.events.append("read-committed")
        case.reconcile.side_effect = lambda *_args: case.events.append("reconcile") or receipt
        statement = control_lifecycle.update(control_lifecycle.ImportRun).returning(control_lifecycle.ImportRun.run_id)
        changed = await control_lifecycle._execute_nucc_claim_update(statement, "nucc_run")
        assert changed == (case.durable_run if published else 1)
        assert case.events == [
            "begin",
            "probe",
            "reconcile",
            "capture",
            "read-committed" if published else "claim",
            "commit",
        ]
        case.reconcile.assert_awaited_once_with(case.scope.session, "nucc_run")
        case.capture.assert_awaited_once_with(case.scope.session)
        if published:
            assert len(case.scope.session.executed) == 1
            assert case.scope.session.executed[0][0].is_select
        else:
            assert case.scope.session.executed == [(statement, {})]
        assert case.scope.db._transaction_binding() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", [None, "r"])
async def test_nucc_plain_or_absent_parent_preserves_legacy_claim(monkeypatch, kind):
    """Legacy v1/v2 storage never enters protected receipt or partition helpers."""
    with _nucc_claim_scope(monkeypatch) as case:
        case.scope.parent_kind = kind
        assert await control_lifecycle._execute_nucc_claim_update("UPDATE synthetic", "nucc_run") == 1
        assert case.events == ["begin", "probe", "claim", "commit"]
        case.reconcile.assert_not_awaited()
        case.capture.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_at", ["reconcile", "capture", "claim"])
@pytest.mark.parametrize("error", [RuntimeError("native refusal"), asyncio.CancelledError()])
async def test_nucc_short_claim_failure_never_falls_back_or_projects(monkeypatch, failure_at, error):
    """Custody, parent, SQL and cancellation failures leave no successful claim."""
    with _nucc_claim_scope(monkeypatch) as case:
        if failure_at == "claim":
            case.scope.session.result = error
        else:
            getattr(case, failure_at).side_effect = error
        with pytest.raises(type(error)) as raised:
            await control_lifecycle._execute_nucc_claim_update("UPDATE synthetic", "nucc_run")
        assert raised.value is error
        assert case.events[-1] == "rollback" and "commit" not in case.events
        assert case.scope.db._transaction_binding() is None
        if failure_at != "claim":
            assert case.scope.session.executed == []


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["v", "f", "m", "", "P"])
async def test_nucc_other_parent_kinds_refuse_without_legacy_fallback(monkeypatch, kind):
    with _nucc_claim_scope(monkeypatch) as case:
        case.scope.parent_kind = kind
        with pytest.raises(RuntimeError, match="parent kind differs"):
            await control_lifecycle._execute_nucc_claim_update("UPDATE synthetic", "nucc_run")
        assert case.events == ["begin", "probe", "rollback"]
        assert case.scope.session.executed == []
        case.reconcile.assert_not_awaited()
        case.capture.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", [None, "r", "p"])
async def test_nucc_preexisting_binding_preserves_legacy_and_refuses_native(monkeypatch, kind):
    with _bound_control_update(monkeypatch, SimpleNamespace(all=Mock(return_value=["nucc_run"]))) as scope:
        scope.session.scalar = AsyncMock(return_value=kind)
        operation = control_lifecycle._execute_control_run_update("UPDATE synthetic", nucc_claim_run_id="nucc_run")
        if kind == "p":
            with pytest.raises(RuntimeError, match="own short transaction"):
                await operation
            assert scope.session.executed == []
        else:
            assert await operation == 1
            assert [(str(statement), params) for statement, params in scope.session.executed] == [
                ("UPDATE synthetic", {})
            ]
        sql = str(scope.session.scalar.await_args.args[0])
        assert "n.nspname='mrf'" in sql and "c.relname='nucc_taxonomy'" in sql
        assert "LOCK TABLE" not in sql and "pg_get_expr" not in sql


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,importer,expected",
    [("running", "nucc", {"nucc_claim_run_id": "nucc_run"}), ("failed", "nucc", {}), ("running", "facility", {})],
)
async def test_nucc_claim_dispatch_keeps_importer_attempt_and_handoff_guards(monkeypatch, status, importer, expected):
    monkeypatch.setattr(control_lifecycle, "read_live_progress", lambda _run_id: None)
    execute = AsyncMock(return_value=1)
    monkeypatch.setattr(control_lifecycle, "_execute_control_run_update", execute)
    monkeypatch.setattr(control_lifecycle, "write_live_progress", lambda **_fields: True)
    assert (
        await control_lifecycle.mark_control_run(
            "nucc_run",
            status=status,
            phase_detail="claim",
            progress_message="claim",
            attempt_id="nucc_attempt",
            attempt_started_at="2026-10-17T00:00:00+00:00",
            pricing_claim_task={"importer": importer},
        )
        is True
    )
    assert execute.await_args.kwargs == expected
    statement = execute.await_args.args[0]
    params = statement.compile().params
    assert "nucc_handoff" in params.values() and "nucc_attempt" in params.values()
    assert list(statement._returning) == [control_lifecycle.ImportRun.run_id]
    if status == "running":
        assert importer in params.values() and any(
            isinstance(bound_value, list) and "canceling" in bound_value for bound_value in params.values()
        )


@pytest.mark.asyncio
async def test_nucc_zero_claim_settles_wrapper_before_target_or_heartbeat(monkeypatch):
    """A genuine zero-row claim refusal remains skipped without importer work."""
    execute = AsyncMock(return_value=0)
    target_import = Mock(side_effect=AssertionError("no importer work after settled claim"))
    heartbeat = AsyncMock(side_effect=AssertionError("no heartbeat after settled claim"))
    projection = Mock()
    monkeypatch.setattr(control_lifecycle, "_execute_control_run_update", execute)
    monkeypatch.setattr(control_lifecycle, "import_module", target_import)
    monkeypatch.setattr(control_lifecycle, "_live_progress_heartbeat", heartbeat)
    monkeypatch.setattr(control_lifecycle, "write_live_progress", projection)
    result = await control_lifecycle.control_single_job_start(
        {},
        {
            "run_id": "nucc_run",
            "importer": "nucc",
            "target_module": "process.nucc",
            "target_function": "process_data",
        },
    )
    assert result == {"status": "skipped", "run_id": "nucc_run", "reason": "newer_attempt_active"}
    assert execute.await_args.kwargs == {"nucc_claim_run_id": "nucc_run"}
    target_import.assert_not_called()
    heartbeat.assert_not_awaited()
    projection.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "marker,attempt_id,started_at",
    [
        (None, "attempt", "2026-10-17T00:00:00+00:00"),
        ({"importer": "nucc"}, None, None),
        ({"importer": "nucc"}, "attempt", None),
        ({"importer": "nucc"}, None, "2026-10-17T00:00:00+00:00"),
    ],
)
async def test_nucc_progress_without_complete_initial_claim_retains_old_path(
    monkeypatch, marker, attempt_id, started_at
):
    """Running status and a payload importer alone never select the native claim seam."""
    execute = AsyncMock(return_value=1)
    monkeypatch.setattr(control_lifecycle, "_execute_control_run_update", execute)
    monkeypatch.setattr(control_lifecycle, "_should_update_control_run_db", AsyncMock(return_value=True))
    monkeypatch.setattr(control_lifecycle, "write_live_progress", lambda **_fields: True)
    assert (
        await control_lifecycle.mark_control_run(
            "nucc_progress",
            status="running",
            phase_detail="progress",
            progress_message="working",
            attempt_id=attempt_id,
            attempt_started_at=started_at,
            pricing_claim_task=marker,
        )
        is True
    )
    assert execute.await_args.kwargs == {}


@pytest.mark.asyncio
async def test_nucc_heartbeat_does_not_select_action_fence(monkeypatch):
    execute = AsyncMock(return_value=1)
    monkeypatch.setattr(control_lifecycle, "_execute_control_run_update", execute)
    monkeypatch.setattr(control_lifecycle, "read_live_progress", lambda _run_id: None)
    assert (
        await control_lifecycle._is_control_run_heartbeat_persisted(
            "nucc_progress",
            "process_data",
            attempt_id="attempt",
            attempt_started_at="2026-10-17T00:00:00+00:00",
        )
        is True
    )
    assert execute.await_args.kwargs == {}


def _acknowledged_nucc_run():
    attempt_id = "nucc_run:" + "a" * 32
    receipt_by_field = {"node_id": "synthetic-node", "attempt_id": attempt_id, "row_count": 17}
    return {
        "run_id": "nucc_run",
        "engine": "healthcare-mrf-api",
        "node_id": "synthetic-node",
        "importer": "nucc",
        "status": "succeeded",
        "phase_detail": "nucc published",
        "error": None,
        "snapshot_id": "native-snapshot",
        "progress": {
            "attempt_id": attempt_id,
            "attempt_started_at": "2026-10-16T00:00:00+00:00",
            "phase": "nucc published",
            "message": "succeeded",
            "unit": "rows",
            "pct": 100,
            "done": 17,
            "total": 17,
        },
        "metrics": {"nucc_native_publication": receipt_by_field, "retained": "original"},
        "heartbeat_at": dt.datetime(2026, 10, 16, 0, 1, 0),
        "finished_at": dt.datetime(2026, 10, 16, 0, 0, 59),
    }


@contextmanager
def _published_nucc_claim(monkeypatch):
    with _nucc_claim_scope(monkeypatch) as case:
        receipt_by_field = dict(case.durable_run["metrics"]["nucc_native_publication"])
        case.reconcile.side_effect = lambda *_args: case.events.append("reconcile") or receipt_by_field
        case.scope.session.after_execute = lambda: case.events.append("read-committed")

        async def execute(statement, **keywords):
            assert keywords == {"nucc_claim_run_id": "nucc_run"}
            return await control_lifecycle._execute_nucc_claim_update(statement, "nucc_run")

        monkeypatch.setattr(control_lifecycle, "_execute_control_run_update", execute)
        monkeypatch.setattr(control_lifecycle, "import_module", Mock(side_effect=AssertionError("target launched")))
        monkeypatch.setattr(
            control_lifecycle, "_live_progress_heartbeat", AsyncMock(side_effect=AssertionError("heartbeat launched"))
        )
        yield case


def _nucc_claim_task():
    return {
        "run_id": "nucc_run",
        "importer": "nucc",
        "target_module": "process.nucc",
        "target_function": "process_data",
    }


@pytest.mark.asyncio
async def test_nucc_receipt_returns_and_projects_committed_outcome(monkeypatch):
    with _published_nucc_claim(monkeypatch) as case:
        projected_events = []

        def project(**values):
            assert case.scope.db._transaction_binding() is None
            assert case.events[-1] == "commit"
            projected_events.append(values)
            case.events.append("project")

        monkeypatch.setattr(control_lifecycle, "write_live_progress", project)
        monkeypatch.setattr(control_lifecycle, "_flush_terminal_status_events", AsyncMock())
        result = await control_lifecycle.control_single_job_start({}, _nucc_claim_task())
        assert result == {"status": "succeeded", "run_id": "nucc_run", "result": case.durable_run["metrics"]}
        assert case.events == ["begin", "probe", "reconcile", "capture", "read-committed", "commit", "project"]
        event = projected_events[0]["status_event_payload"]
        assert event["progress"] == case.durable_run["progress"]
        assert event["metrics"] == case.durable_run["metrics"]
        assert event["phase_detail"] == "nucc published" and event["snapshot_id"] == "native-snapshot"
        assert event["heartbeat_at"] == "2026-10-16T00:01:00+00:00"
        assert event["finished_at"] == "2026-10-16T00:00:59+00:00"
        assert len(case.scope.session.executed) == 1 and case.scope.session.executed[0][0].is_select


@pytest.mark.asyncio
async def test_nucc_receipt_projection_drains_cancellation(monkeypatch):
    with _published_nucc_claim(monkeypatch) as case:
        entered, release = asyncio.Event(), asyncio.Event()
        monkeypatch.setattr(control_lifecycle, "write_live_progress", lambda **values: case.events.append("project"))

        async def flush(_run_id):
            entered.set()
            await release.wait()
            case.events.append("flushed")

        monkeypatch.setattr(control_lifecycle, "_flush_terminal_status_events", flush)
        operation = asyncio.create_task(control_lifecycle.control_single_job_start({}, _nucc_claim_task()))
        try:
            await asyncio.wait_for(entered.wait(), 2)
            assert case.events[-2:] == ["commit", "project"]
            operation.cancel()
            await asyncio.sleep(0)
            assert not operation.done()
        finally:
            release.set()
            outcome_by_field = await operation
        assert outcome_by_field["status"] == "succeeded"
        assert case.events[-2:] == ["project", "flushed"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("status", "failed"),
        ("node_id", "foreign"),
        ("finished_at", None),
        ("heartbeat_at", None),
        ("progress", {"attempt_id": "foreign", "attempt_started_at": "2026-10-16T00:00:00+00:00"}),
        ("metrics", {"nucc_native_publication": {"foreign": True}}),
    ],
)
async def test_nucc_changed_committed_row_never_projects(monkeypatch, field, value):
    with _published_nucc_claim(monkeypatch) as case:
        case.durable_run[field] = value
        projection = Mock(side_effect=AssertionError("invalid outcome projected"))
        monkeypatch.setattr(control_lifecycle, "write_live_progress", projection)
        with pytest.raises((RuntimeError, ValueError)):
            await control_lifecycle.control_single_job_start({}, _nucc_claim_task())
        assert case.events[-1] == "rollback"
        projection.assert_not_called()


@pytest.mark.asyncio
async def test_nucc_unknown_claim_commit_never_projects_or_relaunches(monkeypatch):
    with _published_nucc_claim(monkeypatch) as case:
        original = case.scope.db.transaction

        @asynccontextmanager
        async def uncertain_commit():
            async with original() as session:
                yield session
            raise RuntimeError("commit acknowledgement unavailable")

        monkeypatch.setattr(case.scope.db, "transaction", uncertain_commit)
        projection = Mock(side_effect=AssertionError("unsettled commit projected"))
        monkeypatch.setattr(control_lifecycle, "write_live_progress", projection)
        with pytest.raises(RuntimeError, match="commit acknowledgement unavailable"):
            await control_lifecycle.control_single_job_start({}, _nucc_claim_task())
        assert case.events[-1] == "commit"
        projection.assert_not_called()


@pytest.mark.asyncio
async def test_nucc_foreign_model_schema_never_projects(monkeypatch):
    with _published_nucc_claim(monkeypatch) as case:
        monkeypatch.setattr(control_lifecycle.ImportRun.__table__, "schema", "synthetic_foreign")
        projection = Mock(side_effect=AssertionError("foreign schema projected"))
        monkeypatch.setattr(control_lifecycle, "write_live_progress", projection)
        with pytest.raises(RuntimeError, match="schema differs"):
            await control_lifecycle.control_single_job_start({}, _nucc_claim_task())
        assert case.events[-1] == "rollback" and case.scope.session.executed == []
        projection.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("handoff", [None, {"contract": "synthetic closed handoff"}])
async def test_nucc_claim_recovers_closed_original_attempt_before_a_new_claim(monkeypatch, handoff):
    with _nucc_claim_scope(monkeypatch) as case:
        case.closed.side_effect = lambda *_args: case.events.append("closed-handoff") or handoff
        changed = await control_lifecycle._execute_nucc_claim_update("UPDATE synthetic", "nucc_run")
        assert changed == (0 if handoff else 1)
        assert case.events == (
            ["begin", "probe", "reconcile", "closed-handoff", "commit"]
            if handoff
            else ["begin", "probe", "reconcile", "closed-handoff", "capture", "claim", "commit"]
        )
        case.closed.assert_awaited_once_with(case.scope.session, "nucc_run")
        if handoff:
            case.capture.assert_not_awaited()
            assert case.scope.session.executed == []


@pytest.mark.asyncio
@pytest.mark.parametrize("late_error", [None, RuntimeError, ImportCancelledError, asyncio.CancelledError])
async def test_plain_nucc_terminal_commit_survives_late_worker_errors(monkeypatch, late_error):
    result_by_field = {
        "rows": 2,
        "terminal_progress": {
            "unit": "rows",
            "done": 2,
            "total": 2,
            "pct": 100,
            "message": "succeeded",
            "phase": "nucc published",
        },
    }

    async def target(ctx, _task):
        ctx["context"].update(
            control_run_terminal_committed=True,
            preserve_control_run_finished_at=True,
            _control_committed_result=result_by_field,
            _control_committed_heartbeat_at="native-heartbeat",
            _control_committed_finished_at="native-finished",
        )
        if late_error:
            raise late_error("synthetic after commit")
        return {"incorrect_later_result": True}

    marks, projection = AsyncMock(return_value=True), AsyncMock()
    monkeypatch.setenv("HLTHPRT_IMPORT_LIVE_PROGRESS_HEARTBEAT_SECONDS", "0")
    monkeypatch.setattr(control_lifecycle, "mark_control_run", marks)
    monkeypatch.setattr(control_lifecycle, "_mark_and_flush_terminal_control_run", projection)
    monkeypatch.setattr(control_lifecycle, "_flush_terminal_status_events", AsyncMock())
    monkeypatch.setattr(control_lifecycle, "import_module", lambda _name: SimpleNamespace(process_data=target))
    actual = await control_single_job_start(
        {},
        {
            "run_id": "synthetic_plain",
            "importer": "nucc",
            "target_module": "process.nucc",
            "target_function": "process_data",
        },
    )
    assert actual == {"status": "succeeded", "run_id": "synthetic_plain", "result": result_by_field}
    assert [call.kwargs["status"] for call in marks.await_args_list] == ["running"]
    projected = projection.await_args.kwargs
    assert projected["database_state_committed"] is True
    assert projected["database_heartbeat_at"] == "native-heartbeat"
    assert projected["database_finished_at"] == "native-finished" and projected["phase_detail"] == "nucc published"
