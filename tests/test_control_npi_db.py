# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL concurrency proofs specific to controlled NPI imports."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from api import control_imports
from db.models import ImportRun, db
from process.control_lifecycle import mark_control_run
from tests.test_control_imports_db import (
    _drop_import_run_schema,
    _fake_enqueue,
    _reset_import_run_schema,
)


pytestmark = [
    pytest.mark.asyncio(loop_scope="module"),
    pytest.mark.filterwarnings(
        "ignore:coroutine 'Connection._cancel' was never awaited:RuntimeWarning"
    ),
]


class _OuterPrecheckBarrier:
    def __init__(self):
        self.arrival_count = 0
        self.both_arrived = asyncio.Event()

    async def miss(self, importer: str):
        assert importer == "npi"
        self.arrival_count += 1
        if self.arrival_count == 2:
            self.both_arrived.set()
        await self.both_arrived.wait()
        return None


async def _insert_running_npi_attempt(
    attempt_id: str,
    attempt_started_at: str,
) -> None:
    """Insert the exact running attempt used by the cancellation CAS proof."""

    await db.execute(
        control_imports.insert(ImportRun).values(
            run_id="run_npi_cancel",
            engine=control_imports.ENGINE_NAME,
            importer="npi",
            family="provider",
            status="running",
            phase_detail="process_data running",
            params={},
            created_at=control_imports.utc_now(),
            heartbeat_at=control_imports.utc_now(),
            progress={
                "unit": "run",
                "total": 1,
                "done": 0,
                "pct": 25,
                "message": "running",
                "attempt_id": attempt_id,
                "attempt_started_at": attempt_started_at,
            },
            metrics={},
        )
    )


async def test_concurrent_npi_admission_uses_postgres_advisory_fence(monkeypatch):
    """Prove the PostgreSQL advisory fence admits exactly one active NPI run."""

    await _reset_import_run_schema()
    try:
        precheck_barrier = _OuterPrecheckBarrier()
        enqueued_run_ids: list[str] = []

        async def record_enqueue(run_by_name: dict) -> dict:
            enqueued_run_ids.append(run_by_name["run_id"])
            return await _fake_enqueue(run_by_name)

        monkeypatch.setattr(
            control_imports,
            "find_earliest_active_run_by_importer",
            precheck_barrier.miss,
        )
        monkeypatch.setattr(control_imports, "_enqueue_import_start", record_enqueue)
        admission_results = await asyncio.gather(
            control_imports.create_import_run(
                {"run_id": "run_npi_race_one", "importer": "npi"}
            ),
            control_imports.create_import_run(
                {"run_id": "run_npi_race_two", "importer": "npi"}
            ),
        )

        assert sorted(is_created for _, is_created in admission_results) == [False, True]
        returned_run_ids = {
            run_by_name["run_id"] for run_by_name, _is_created in admission_results
        }
        assert returned_run_ids == set(enqueued_run_ids)
        active_npi_rows = (
            await db.execute(
                select(ImportRun).where(
                    ImportRun.importer == "npi",
                    ImportRun.status.in_(control_imports.ACTIVE_STATUSES),
                )
            )
        ).scalars().all()
        assert [run_row.run_id for run_row in active_npi_rows] == enqueued_run_ids
        assert len(enqueued_run_ids) == 1
    finally:
        await _drop_import_run_schema()


async def test_npi_cancel_preserves_attempt_for_worker_terminal_cas(monkeypatch):
    """Preserve attempt ownership from cancel request through terminal worker CAS."""

    await _reset_import_run_schema()
    try:
        attempt_id = "run_npi_cancel:" + "a" * 32
        attempt_started_at = "2026-08-09T03:04:05.000000+00:00"
        await _insert_running_npi_attempt(attempt_id, attempt_started_at)
        monkeypatch.setattr(
            control_imports,
            "_cancel_signal_for_run",
            AsyncMock(
                return_value={
                    "redis": True,
                    "kubernetes": {"enabled": False, "items": []},
                }
            ),
        )

        canceling_run_by_name = await control_imports.request_cancel(
            "run_npi_cancel"
        )
        assert canceling_run_by_name["status"] == "canceling"
        assert canceling_run_by_name["progress"]["attempt_id"] == attempt_id
        assert canceling_run_by_name["progress"][
            "attempt_started_at"
        ] == attempt_started_at

        await mark_control_run(
            "run_npi_cancel",
            status="canceled",
            phase_detail="process_data canceled",
            progress_message="canceled",
            attempt_id=attempt_id,
            attempt_started_at=attempt_started_at,
        )
        stored_run = (
            await db.execute(
                select(ImportRun).where(ImportRun.run_id == "run_npi_cancel")
            )
        ).scalar_one()
        assert stored_run.status == "canceled"
        assert stored_run.finished_at is not None
    finally:
        await _drop_import_run_schema()
