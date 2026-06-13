# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio

import pytest

from process.control_cancel import ImportCancelledError, raise_if_cancelled, run_id_from_task
from process import control_lifecycle
from process.control_lifecycle import control_single_job_start


def test_run_id_from_task_normalizes_missing_values():
    assert run_id_from_task(None) is None
    assert run_id_from_task({}) is None
    assert run_id_from_task({"run_id": " run_1 "}) == "run_1"


@pytest.mark.asyncio
async def test_raise_if_cancelled_noops_without_run_id():
    await raise_if_cancelled({"redis": object()}, {})


@pytest.mark.asyncio
async def test_raise_if_cancelled_raises_for_redis_flag():
    class FakeRedis:
        async def get(self, key):
            assert key == "cancel:run_1"
            return b"1"

    with pytest.raises(ImportCancelledError):
        await raise_if_cancelled({"redis": FakeRedis()}, {"run_id": "run_1"})


@pytest.mark.asyncio
async def test_raise_if_cancelled_allows_missing_flag():
    class FakeRedis:
        async def get(self, _key):
            return None

    await raise_if_cancelled({"redis": FakeRedis()}, {"run_id": "run_1"})


@pytest.mark.asyncio
async def test_control_single_job_start_marks_success(monkeypatch):
    marks = []
    calls = []

    async def fake_mark(run_id, **kwargs):
        marks.append((run_id, kwargs))

    async def fake_target(ctx, task):
        calls.append((ctx, task))
        return {"ok": True}

    class FakeModule:
        process_data = staticmethod(fake_target)

    monkeypatch.setattr(control_lifecycle, "mark_control_run", fake_mark)
    monkeypatch.setattr(control_lifecycle, "import_module", lambda name: FakeModule if name == "fake.module" else None)

    result = await control_single_job_start(
        {"redis": object()},
        {
            "run_id": "run_1",
            "target_module": "fake.module",
            "target_function": "process_data",
            "task": {"test_mode": True},
        },
    )

    assert result["status"] == "succeeded"
    assert calls[0][1] == {"test_mode": True, "run_id": "run_1"}
    assert [item[1]["status"] for item in marks] == ["running", "succeeded"]


@pytest.mark.asyncio
async def test_control_single_job_start_ignores_arq_metadata_kwargs(monkeypatch):
    marks = []
    calls = []

    async def fake_mark(run_id, **kwargs):
        marks.append((run_id, kwargs))

    async def fake_target(ctx, task):
        calls.append((ctx, task))
        return {"ok": True}

    class FakeModule:
        process_data = staticmethod(fake_target)

    monkeypatch.setattr(control_lifecycle, "mark_control_run", fake_mark)
    monkeypatch.setattr(control_lifecycle, "import_module", lambda name: FakeModule if name == "fake.module" else None)

    result = await control_single_job_start(
        {"redis": object()},
        {
            "run_id": "run_1",
            "target_module": "fake.module",
            "target_function": "process_data",
            "task": {"test_mode": True},
        },
        _max_tries=1,
    )

    assert result["status"] == "succeeded"
    assert calls[0][1] == {"test_mode": True, "run_id": "run_1"}
    assert [item[1]["status"] for item in marks] == ["running", "succeeded"]


@pytest.mark.asyncio
async def test_control_single_job_start_marks_cancelled(monkeypatch):
    marks = []

    async def fake_mark(run_id, **kwargs):
        marks.append((run_id, kwargs))

    async def fake_target(_ctx, _task):
        raise ImportCancelledError("cancelled")

    class FakeModule:
        process_data = staticmethod(fake_target)

    monkeypatch.setattr(control_lifecycle, "mark_control_run", fake_mark)
    monkeypatch.setattr(control_lifecycle, "import_module", lambda _name: FakeModule)

    result = await control_single_job_start(
        {},
        {"run_id": "run_1", "target_module": "fake.module", "target_function": "process_data"},
    )

    assert result["status"] == "canceled"
    assert [item[1]["status"] for item in marks] == ["running", "canceled"]


@pytest.mark.asyncio
async def test_control_single_job_start_marks_cancelled_task_failed(monkeypatch):
    marks = []

    async def fake_mark(run_id, **kwargs):
        marks.append((run_id, kwargs))

    async def fake_target(_ctx, _task):
        raise asyncio.CancelledError()

    class FakeModule:
        process_data = staticmethod(fake_target)

    monkeypatch.setattr(control_lifecycle, "mark_control_run", fake_mark)
    monkeypatch.setattr(control_lifecycle, "import_module", lambda _name: FakeModule)

    result = await control_single_job_start(
        {},
        {"run_id": "run_1", "target_module": "fake.module", "target_function": "process_data"},
    )

    assert result["status"] == "failed"
    assert [item[1]["status"] for item in marks] == ["running", "failed"]
    assert marks[-1][1]["error"]["code"] == "import_interrupted"


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
