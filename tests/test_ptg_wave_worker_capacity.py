"""PTG-only capacity fencing around generic worker launch."""

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest

from api import control_workers


@pytest.mark.asyncio
async def test_generic_ptg_launch_holds_capacity_lock_through_external_start(monkeypatch):
    events: list[str] = []
    connection = object()

    @asynccontextmanager
    async def acquire():
        events.append("transaction-enter")
        yield connection
        events.append("transaction-exit")

    async def capacity_lock(actual):
        assert actual is connection
        events.append("capacity-lock")

    async def worker_action_lock(actual, run_id):
        assert actual is connection and run_id == "run-unit"
        events.append("worker-action-lock")

    async def not_wave_owned(actual, run_id):
        assert actual is connection and run_id == "run-unit"
        events.append("ownership-check")

    async def no_wave(actual):
        assert actual is connection
        events.append("wave-check")

    async def admit(*_args, **_kwargs):
        events.append("source-admission")
        return None

    async def launch(function, payload):
        assert function is control_workers.ensure_worker
        assert payload["run_id"] == "run-unit"
        events.append("worker-launch")
        return {"status": "started", "items": []}

    monkeypatch.setattr(control_workers.db, "acquire", acquire)
    monkeypatch.setattr(control_workers, "acquire_ptg_admission_lock", capacity_lock)
    monkeypatch.setattr(
        control_workers, "acquire_control_run_worker_action_lock", worker_action_lock
    )
    monkeypatch.setattr(control_workers, "require_not_wave_owned_run", not_wave_owned)
    monkeypatch.setattr(control_workers, "require_no_capacity_owning_wave", no_wave)
    monkeypatch.setattr(control_workers, "_admit_worker_ensure", admit)
    monkeypatch.setattr(control_workers.asyncio, "to_thread", launch)

    launch_result = await control_workers._guarded_ptg_family_ensure(
        {"run_id": "run-unit", "importer": "ptg"},
        run_id="run-unit",
        importer="ptg",
        selected_specs=[control_workers._BY_QUEUE["arq:PTGSmall"]],
    )

    assert launch_result["status"] == "started"
    assert events == [
        "transaction-enter", "capacity-lock", "worker-action-lock",
        "ownership-check", "wave-check", "source-admission", "worker-launch",
        "transaction-exit",
    ]


@pytest.mark.asyncio
async def test_generic_ptg_launch_does_not_start_when_wave_owns_capacity(monkeypatch):
    @asynccontextmanager
    async def acquire():
        yield object()

    async def no_op(*_args, **_kwargs):
        return None

    async def blocked(*_args, **_kwargs):
        raise control_workers.PTGWaveCapacityConflict("PTG wave capacity is reserved")

    async def unexpected(*_args, **_kwargs):  # pragma: no cover - safety assertion
        raise AssertionError("worker launch must remain fenced")

    monkeypatch.setattr(control_workers.db, "acquire", acquire)
    monkeypatch.setattr(control_workers, "acquire_ptg_admission_lock", no_op)
    monkeypatch.setattr(
        control_workers,
        "acquire_control_run_worker_action_lock",
        no_op,
    )
    monkeypatch.setattr(control_workers, "require_not_wave_owned_run", no_op)
    monkeypatch.setattr(control_workers, "require_no_capacity_owning_wave", blocked)
    monkeypatch.setattr(control_workers, "_admit_worker_ensure", unexpected)

    launch_result_by_field = await control_workers._guarded_ptg_family_ensure(
        {"run_id": "run-unit", "importer": "ptg"},
        run_id="run-unit",
        importer="ptg",
        selected_specs=[control_workers._BY_QUEUE["arq:PTGSmall"]],
    )

    assert launch_result_by_field["status"] == "failed"
    assert launch_result_by_field["message"] == "PTG wave capacity is reserved"


@pytest.mark.asyncio
async def test_ptg_family_requires_run_identity_but_fhir_remains_unfenced(monkeypatch):
    ptg = await control_workers.guarded_ensure_worker(
        {"importer": "ptg", "queue": "arq:PTGSmall"}
    )
    assert ptg["status"] == "failed"
    assert ptg["message"] == "PTG-family worker launch requires run_id"

    async def admit(*_args, **_kwargs):
        return None

    async def launch(_function, _payload):
        return {"status": "already_running", "items": []}

    async def unexpected(*_args, **_kwargs):  # pragma: no cover - safety assertion
        raise AssertionError("FHIR must not enter the PTG capacity fence")

    monkeypatch.setattr(control_workers, "_admit_worker_ensure", admit)
    monkeypatch.setattr(control_workers.asyncio, "to_thread", launch)
    monkeypatch.setattr(control_workers, "_guarded_ptg_family_ensure", unexpected)

    fhir = await control_workers.guarded_ensure_worker({
        "run_id": "fhir-run-unit",
        "importer": "provider-directory-fhir",
    })
    assert fhir["status"] == "already_running"
