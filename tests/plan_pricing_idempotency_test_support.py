# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real-PostgreSQL helpers for durable plan-pricing replay tests."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

from sqlalchemy import select, update

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


def _prewarm_run_request(run_id: str) -> dict:
    return {
        "run_id": run_id,
        "importer": "plan-pricing-prewarm",
        "idempotency_key": "prewarm-exact-release-replay",
        "params": {
            "plan_release_id": "hprelease_" + "2" * 26,
            "serving_revision_id": "hpserve_" + "3" * 26,
            "projection_id": "a" * 64,
        },
    }


async def _concurrent_admissions(monkeypatch, fake_enqueue):
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
    admission_results = await asyncio.gather(
        control_imports.create_import_run(
            _prewarm_run_request("run_prewarm_race_one")
        ),
        control_imports.create_import_run(
            _prewarm_run_request("run_prewarm_race_two")
        ),
    )
    return durable_lookup, enqueued_run_ids, admission_results


async def _assert_terminal_replay(
    monkeypatch,
    durable_lookup,
    winner_run_id: str,
) -> None:
    await db.execute(
        update(ImportRun)
        .where(ImportRun.run_id == winner_run_id)
        .values(status="succeeded", phase_detail="prewarm succeeded")
    )
    terminal_lookup = _MissOnceLookup(durable_lookup)
    monkeypatch.setattr(
        control_imports,
        "find_importer_run_by_idempotency_key",
        terminal_lookup.find,
    )
    terminal_run, created = await control_imports.create_import_run(
        _prewarm_run_request("run_prewarm_terminal_replay")
    )
    assert created is False
    assert terminal_run["run_id"] == winner_run_id
    assert terminal_run["status"] == "succeeded"
    stored_rows = (
        await db.execute(
            select(ImportRun).where(
                ImportRun.importer == "plan-pricing-prewarm"
            )
        )
    ).scalars().all()
    assert [stored_row.run_id for stored_row in stored_rows] == [
        winner_run_id
    ]


async def assert_plan_pricing_replay_durable(
    monkeypatch,
    *,
    reset_schema,
    drop_schema,
    fake_enqueue,
) -> None:
    """Prove concurrent admission plus forced terminal index replay."""

    await reset_schema()
    try:
        durable_lookup, enqueued_run_ids, results = (
            await _concurrent_admissions(monkeypatch, fake_enqueue)
        )
        assert sorted(created for _, created in results) == [False, True]
        returned_run_ids = {
            run_by_field["run_id"] for run_by_field, _created in results
        }
        assert returned_run_ids == set(enqueued_run_ids)
        await _assert_terminal_replay(
            monkeypatch,
            durable_lookup,
            enqueued_run_ids[0],
        )
    finally:
        await drop_schema()
