# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real-PostgreSQL helpers for durable plan-pricing replay tests."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError

from api import control_imports
from db.models import ImportRun, db


class _ConcurrentPrecheckBarrier:
    def __init__(self, durable_lookup):
        self.durable_lookup = durable_lookup
        self.arrival_count = 0
        self.both_arrived = asyncio.Event()

    async def find(self, importer: str, idempotency_key: str):
        self.arrival_count += 1
        if self.arrival_count <= 2:
            if self.arrival_count == 2:
                self.both_arrived.set()
            await asyncio.wait_for(self.both_arrived.wait(), timeout=5)
            return None
        return await self.durable_lookup(importer, idempotency_key)


class _MissOnceLookup:
    def __init__(self, durable_lookup):
        self.durable_lookup = durable_lookup
        self.call_count = 0

    async def find(self, importer: str, idempotency_key: str):
        self.call_count += 1
        if self.call_count == 1:
            return None
        return await self.durable_lookup(importer, idempotency_key)


class _TerminalAdmissionRace:
    def __init__(self, original_admit, durable_lookup, index_name):
        self.original_admit = original_admit
        self.durable_lookup = durable_lookup
        self.index_name = index_name
        self.admission_count = 0
        self.conflict_count = 0
        self.winner_terminal = asyncio.Event()

    async def admit(self, importer, row_by_field, **options):
        self.admission_count += 1
        ordinal = self.admission_count
        if ordinal == 2:
            await asyncio.wait_for(self.winner_terminal.wait(), timeout=5)
            winner = await self.durable_lookup(importer, row_by_field["idempotency_key"])
            assert winner["status"] == "succeeded"
        try:
            return await self.original_admit(importer, row_by_field, **options)
        except IntegrityError as error:
            assert ordinal == 2
            assert self.index_name in str(error.orig)
            self.conflict_count += 1
            raise

    async def create(self, request_by_field):
        run_by_field, created = await control_imports.create_import_run(request_by_field)
        if created and not self.winner_terminal.is_set():
            await db.execute(
                update(ImportRun).where(ImportRun.run_id == run_by_field["run_id"]).values(status="succeeded")
            )
            self.winner_terminal.set()
        return run_by_field, created


def _replay_run_request(run_id: str, importer: str) -> dict:
    request_by_field = {
        "run_id": run_id,
        "importer": importer,
        "idempotency_key": "pricing-exact-release-replay",
        "params": {
            "plan_release_id": "hprelease_" + "2" * 26,
            "serving_revision_id": "hpserve_" + "3" * 26,
        },
    }
    if importer == "registry-source-check":
        request_by_field["params"] = {
            "source_system": "cms",
            "source_id": "commercial-mlr",
            "edition_policy": "latest-verified",
            "reporting_year": None,
        }
    elif importer == "plan-pricing-prewarm":
        request_by_field["params"]["projection_id"] = "a" * 64
    else:
        assert importer == "plan-pricing-em-attribution"
        request_by_field["params"].update(owner_role="attribution_owner", runtime_roles=["attribution_reader"])
    return request_by_field


async def _concurrent_admissions(monkeypatch, fake_enqueue, importer):
    durable_lookup = control_imports.find_importer_run_by_idempotency_key
    precheck_barrier = _ConcurrentPrecheckBarrier(durable_lookup)
    enqueued_run_ids = []

    async def record_enqueue(run_by_field):
        enqueued_run_ids.append(run_by_field["run_id"])
        return await fake_enqueue(run_by_field)

    monkeypatch.setattr(
        control_imports,
        "find_importer_run_by_idempotency_key",
        precheck_barrier.find,
    )
    monkeypatch.setattr(
        control_imports,
        "find_earliest_active_run_by_importer",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        control_imports,
        "_enqueue_import_start",
        record_enqueue,
    )
    create = control_imports.create_import_run
    race = None
    if importer in {"plan-pricing-em-attribution", "registry-source-check"}:
        index_name = (
            "import_run_registry_source_check_idempotency_idx"
            if importer == "registry-source-check"
            else "import_run_plan_pricing_idempotency_idx"
        )
        race = _TerminalAdmissionRace(control_imports._admit_import_row, durable_lookup, index_name)
        monkeypatch.setattr(control_imports, "_admit_import_row", race.admit)
        create = race.create
    admission_results = await asyncio.gather(
        create(_replay_run_request("run_pricing_race_one", importer)),
        create(_replay_run_request("run_pricing_race_two", importer)),
    )
    if race is not None:
        assert (race.admission_count, race.conflict_count) == (2, 1)
        monkeypatch.setattr(control_imports, "_admit_import_row", race.original_admit)
    return durable_lookup, enqueued_run_ids, admission_results


async def _assert_terminal_replay(
    monkeypatch,
    durable_lookup,
    winner_run_id: str,
    importer: str,
) -> None:
    await db.execute(
        update(ImportRun)
        .where(ImportRun.run_id == winner_run_id)
        .values(status="succeeded", phase_detail="pricing succeeded")
    )
    winner = await durable_lookup(importer, "pricing-exact-release-replay")
    assert winner["run_id"] == winner_run_id and winner["status"] == "succeeded"
    terminal_lookup = _MissOnceLookup(durable_lookup)
    monkeypatch.setattr(
        control_imports,
        "find_importer_run_by_idempotency_key",
        terminal_lookup.find,
    )
    terminal_run, created = await control_imports.create_import_run(
        _replay_run_request("run_pricing_terminal_replay", importer)
    )
    assert created is False
    assert terminal_run["run_id"] == winner_run_id
    assert terminal_run["status"] == "succeeded"
    assert terminal_lookup.call_count == 2
    stored_rows = (await db.execute(select(ImportRun).where(ImportRun.importer == importer))).scalars().all()
    assert [stored_row.run_id for stored_row in stored_rows] == [winner_run_id]


async def assert_plan_pricing_replay_durable(
    monkeypatch,
    *,
    reset_schema,
    drop_schema,
    fake_enqueue,
    importer="plan-pricing-prewarm",
) -> None:
    """Prove concurrent admission plus forced terminal index replay."""

    await reset_schema()
    try:
        durable_lookup, enqueued_run_ids, results = await _concurrent_admissions(monkeypatch, fake_enqueue, importer)
        assert sorted(created for _, created in results) == [False, True]
        returned_run_ids = {run_by_field["run_id"] for run_by_field, _created in results}
        assert returned_run_ids == set(enqueued_run_ids)
        await _assert_terminal_replay(
            monkeypatch,
            durable_lookup,
            enqueued_run_ids[0],
            importer,
        )
        assert len(enqueued_run_ids) == 1
    finally:
        await drop_schema()
