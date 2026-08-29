# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Scope-aware concurrent admission proof for hospital-price imports."""

from __future__ import annotations

import asyncio
import importlib
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import AsyncMock

import pytest

control_imports = importlib.import_module("api.control_imports")


class _SharedExclusiveLock:
    def __init__(self) -> None:
        self.condition = asyncio.Condition()
        self.shared_holders = 0
        self.has_exclusive_holder = False

    async def acquire(self, *, shared: bool) -> None:
        async with self.condition:
            await self.condition.wait_for(
                lambda: not self.has_exclusive_holder
                and (shared or self.shared_holders == 0)
            )
            if shared:
                self.shared_holders += 1
            else:
                self.has_exclusive_holder = True

    async def release(self, *, shared: bool) -> None:
        async with self.condition:
            if shared:
                self.shared_holders -= 1
            else:
                self.has_exclusive_holder = False
            self.condition.notify_all()


class _HospitalAdmissionConnection:
    def __init__(self, database: "_HospitalAdmissionDb") -> None:
        self.database = database
        self.held_locks: list[tuple[str, bool]] = []

    async def scalar(self, statement: Any, **params: Any) -> None:
        lock_key = str(params["lock_key"])
        is_shared = "pg_advisory_xact_lock_shared" in str(statement)
        self.database.events.append(("lock_wait", lock_key, is_shared))
        await self.database.lock_by_key.setdefault(
            lock_key, _SharedExclusiveLock()
        ).acquire(shared=is_shared)
        self.held_locks.append((lock_key, is_shared))
        self.database.events.append(("lock_acquired", lock_key, is_shared))

    async def status(self, statement: Any) -> int:
        run_map = dict(statement.compile().params)
        self.database.runs.append(run_map)
        self.database.events.append(("insert", run_map["run_id"]))
        self.database.insert_started.set()
        if self.database.pause_insert:
            await self.database.allow_insert_exit.wait()
        return 1


class _HospitalAdmissionDb:
    def __init__(self, runs: tuple[dict[str, Any], ...] = ()) -> None:
        self.runs = list(runs)
        self.events: list[tuple[Any, ...]] = []
        self.lock_by_key: dict[str, _SharedExclusiveLock] = {}
        self.comparison_barrier: asyncio.Barrier | None = None
        self.pause_insert = False
        self.insert_started = asyncio.Event()
        self.allow_insert_exit = asyncio.Event()

    @asynccontextmanager
    async def acquire(self):
        connection = _HospitalAdmissionConnection(self)
        try:
            yield connection
        finally:
            for lock_key, is_shared in reversed(connection.held_locks):
                await self.lock_by_key[lock_key].release(shared=is_shared)
                self.events.append(("lock_released", lock_key, is_shared))

    async def execute(self, statement: Any) -> None:
        self.events.append(("execute", statement))


def _hospital_run(
    run_id: str, params: dict[str, Any], *, idempotency_key: str | None = None
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "importer": "hospital-prices",
        "status": "queued",
        "params": params,
        "idempotency_key": idempotency_key,
    }


def _hospital_request(
    run_id: str,
    hospital_ids: list[str] | None = None,
    *,
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    params = (
        {"hospital_ids": hospital_ids}
        if hospital_ids is not None
        else {"all_hospitals": True}
    )
    return {
        "run_id": run_id,
        "importer": "hospital-prices",
        "idempotency_key": idempotency_key,
        "params": params,
    }


def _install_hospital_admission(monkeypatch, database: _HospitalAdmissionDb) -> None:
    async def active_idempotency(_connection, importer, idempotency_key):
        run_snapshots = list(database.runs)
        await asyncio.sleep(0)
        return next(
            (
                run
                for run in run_snapshots
                if run.get("importer") == importer
                and run.get("idempotency_key") == idempotency_key
                and run.get("status") in control_imports.ACTIVE_STATUSES
            ),
            None,
        )

    async def active_hospital_runs(_connection, importer):
        assert importer == "hospital-prices"
        if database.comparison_barrier is not None:
            await database.comparison_barrier.wait()
        return [
            run
            for run in database.runs
            if run.get("importer") == importer
            and run.get("status") in control_imports.ACTIVE_STATUSES
        ]

    async def reject_outside_transaction(*_args, **_kwargs):
        raise AssertionError("hospital admission must compare inside its transaction")

    async def enqueue(run):
        return {
            "status": "queued",
            "phase_detail": "enqueued",
            "heartbeat_at": run["heartbeat_at"],
            "progress": run["progress"],
            "metrics": {},
            "error": None,
        }

    monkeypatch.setattr(control_imports, "db", database)
    monkeypatch.setattr(control_imports, "importer_names", lambda: {"hospital-prices"})
    monkeypatch.setattr(
        control_imports,
        "_validate_hospital_price_params",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(control_imports, "_active_idempotency_run", active_idempotency)
    monkeypatch.setattr(control_imports, "_active_importer_runs", active_hospital_runs)
    monkeypatch.setattr(
        control_imports, "_idempotent_import_run", reject_outside_transaction
    )
    monkeypatch.setattr(
        control_imports,
        "find_earliest_active_run_by_importer",
        reject_outside_transaction,
    )
    monkeypatch.setattr(control_imports, "_enqueue_import_start", enqueue)


@pytest.mark.parametrize(
    ("requested", "active", "blocked"),
    [
        ({"all_hospitals": True}, {"hospital_id": "hospital-a"}, True),
        ({"hospital_id": "hospital-a"}, {"all_hospitals": True}, True),
        ({"hospital_ids": ["hospital-a", "hospital-b"]}, {"hospital_id": "hospital-b"}, True),
        ({"hospital_ids": ["hospital-a", "hospital-b"]}, {"hospital_id": "hospital-c"}, False),
    ],
)
def test_hospital_scope_overlap_matrix(requested, active, blocked):
    active_run = _hospital_run("run-active", active)
    assert (
        control_imports._hospital_price_blocking_run(requested, [active_run])
        is active_run
    ) is blocked


def test_hospital_exact_replay_requires_the_same_canonical_scope():
    assert control_imports._is_exact_hospital_price_replay(
        {"params": {"all_hospitals": True}},
        _hospital_run("run-all", {"all_hospitals": True}),
    )
    assert not control_imports._is_exact_hospital_price_replay(
        {"params": {"all_hospitals": True}},
        _hospital_run("run-selected", {"hospital_id": "hospital-a"}),
    )


def test_reviewed_aliases_share_admission_and_replay_scope(monkeypatch):
    def expanded_registry(params):
        requested = params.get("hospital_id")
        if requested in {"hospital-canonical", "hospital-alias"}:
            return (
                {"hospital_id": "hospital-canonical"},
                {"hospital_id": "hospital-alias"},
            )
        raise ValueError("unknown hospital")

    monkeypatch.setattr(
        control_imports, "selected_hospital_hpt_registry", expanded_registry
    )
    monkeypatch.setattr(
        control_imports, "hospital_price_artifact_store", lambda: object()
    )
    monkeypatch.setattr(control_imports, "locator_groups", lambda _hospitals: ())
    monkeypatch.setattr(
        control_imports, "configured_resource_limits", lambda *_args: None
    )
    canonical_params = control_imports._validate_hospital_price_admission(
        {"hospital_id": "hospital-canonical"}
    )
    alias_params = control_imports._validate_hospital_price_admission(
        {"hospital_id": "hospital-alias"}
    )
    active = _hospital_run("run-active", alias_params)

    assert control_imports._hospital_price_blocking_run(
        canonical_params, [active]
    ) is active
    assert control_imports._is_exact_hospital_price_replay(
        {"params": canonical_params}, active
    )


@pytest.mark.asyncio
async def test_hospital_admission_replays_only_the_exact_idempotent_scope(monkeypatch):
    active_run = _hospital_run(
        "run-active", {"hospital_ids": ["hospital-a", "hospital-b"]},
        idempotency_key="same-request",
    )
    database = _HospitalAdmissionDb((active_run,))
    _install_hospital_admission(monkeypatch, database)
    replayed, created = await control_imports.create_import_run(
        _hospital_request("run-replay", ["hospital-b", "hospital-a"],
                          idempotency_key="same-request")
    )
    assert created is False and replayed["run_id"] == "run-active"
    with pytest.raises(ValueError, match="idempotency key belongs to a different"):
        await control_imports.create_import_run(
            _hospital_request("run-different", ["hospital-c"],
                              idempotency_key="same-request")
        )


@pytest.mark.asyncio
async def test_hospital_admission_ignores_other_importer_families(monkeypatch):
    other_run_map = {
        "run_id": "run-npi", "importer": "npi", "status": "running",
        "params": {}, "idempotency_key": "npi-request",
    }
    database = _HospitalAdmissionDb((other_run_map,))
    _install_hospital_admission(monkeypatch, database)
    admitted, created = await control_imports.create_import_run(
        _hospital_request(
            "run-hospital",
            ["hospital-a"],
            idempotency_key="hospital-request",
        )
    )
    assert created is True and admitted["run_id"] == "run-hospital"


@pytest.mark.asyncio
async def test_same_hospital_admission_race_has_one_winner(monkeypatch):
    database = _HospitalAdmissionDb()
    _install_hospital_admission(monkeypatch, database)
    admissions = await asyncio.wait_for(
        asyncio.gather(*(
            control_imports.create_import_run(
                _hospital_request(f"run-{suffix}", ["hospital-a"],
                                  idempotency_key="same-request")
            )
            for suffix in ("a", "b")
        )), timeout=1,
    )
    winner = next(run for run, created in admissions if created)
    assert [created for _run, created in admissions].count(True) == 1
    assert {run["run_id"] for run, _created in admissions} == {winner["run_id"]}
    assert len(database.runs) == 1


@pytest.mark.asyncio
async def test_disjoint_hospital_admissions_compare_and_insert_concurrently(monkeypatch):
    database = _HospitalAdmissionDb()
    database.comparison_barrier = asyncio.Barrier(2)
    _install_hospital_admission(monkeypatch, database)
    admissions = await asyncio.wait_for(
        asyncio.gather(*(
            control_imports.create_import_run(
                _hospital_request(f"run-{suffix}", [f"hospital-{suffix}"],
                                  idempotency_key=f"request-{suffix}")
            )
            for suffix in ("a", "b")
        )), timeout=1,
    )
    assert all(created for _run, created in admissions)
    assert {run["run_id"] for run, _created in admissions} == {"run-a", "run-b"}


@pytest.mark.asyncio
async def test_all_hospital_admission_waits_for_selected_transaction(monkeypatch):
    database = _HospitalAdmissionDb()
    database.pause_insert = True
    _install_hospital_admission(monkeypatch, database)
    selected = asyncio.create_task(control_imports.create_import_run(
        _hospital_request("run-selected", ["hospital-a"])
    ))
    await asyncio.wait_for(database.insert_started.wait(), timeout=1)
    all_hospitals = asyncio.create_task(control_imports.create_import_run(
        _hospital_request("run-all")
    ))
    await asyncio.sleep(0)
    assert not all_hospitals.done()
    database.allow_insert_exit.set()
    selected_result, all_result = await asyncio.wait_for(
        asyncio.gather(selected, all_hospitals), timeout=1
    )
    assert selected_result[1] is True
    assert all_result[0]["run_id"] == selected_result[0]["run_id"]
    assert all_result[1] is False
