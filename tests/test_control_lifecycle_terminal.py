# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import datetime as dt
from types import SimpleNamespace

import pytest

from process import control_lifecycle
from process.control_cancel import ImportCancelledError
from process.control_lifecycle import control_single_job_start


class NestedProgressHarness:
    def __init__(self):
        self.progress_by_write = []

    async def persist_update(self, statement):
        values_by_field = {
            getattr(key, "key", str(key)): getattr(value, "value", value)
            for key, value in statement._values.items()
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


@pytest.mark.asyncio
async def test_control_run_update_uses_base_database_then_restores_override(monkeypatch):
    calls = []

    class FakeDb:
        _database_override = "healthporta_test"

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

    values_by_field = {
        getattr(key, "key", str(key)): field_value
        for key, field_value in db_updates[0]._values.items()
    }
    assert "finished_at" not in values_by_field
    assert live_events[-1]["finished_at"] is None
    assert status_events[-1]["finished_at"] is None
