# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Transaction-bound PTG plan-month write regressions."""

from __future__ import annotations

import importlib
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.sql.dml import Insert

from db.connection import InsertAdapter
from tests.ptg_v4_import_orchestration_support import (
    FirstResult,
    RecordingSession,
    transaction_factory,
)

process_ptg = importlib.import_module("process.ptg")


@pytest.mark.asyncio
async def test_plan_month_write_locks_distinct_snapshot_ids_in_sorted_order(
    monkeypatch,
):
    """Fence each distinct snapshot before the plan-month batch is executed."""

    session = RecordingSession([FirstResult(None)])
    snapshot_lock = AsyncMock()
    monkeypatch.setattr(process_ptg.db, "transaction", transaction_factory(session))
    monkeypatch.setattr(
        process_ptg.db,
        "_execution_session",
        transaction_factory(session),
    )
    monkeypatch.setattr(process_ptg, "lock_writable_snapshot", snapshot_lock)

    await process_ptg._push_fenced_ptg2_plan_months(
        [
            {"snapshot_id": "snapshot-z", "plan_id": "z"},
            {"snapshot_id": "", "plan_id": "ignored"},
            {"snapshot_id": "snapshot-a", "plan_id": "a"},
            {"snapshot_id": "snapshot-z", "plan_id": "duplicate"},
        ]
    )

    assert [call.kwargs["snapshot_id"] for call in snapshot_lock.await_args_list] == [
        "snapshot-a",
        "snapshot-z",
    ]
    assert len(session.executions) == 1
    assert isinstance(session.executions[0][0], Insert)
    assert not isinstance(session.executions[0][0], InsertAdapter)


@pytest.mark.asyncio
async def test_empty_plan_month_batch_executes_without_snapshot_locks(monkeypatch):
    """Allow the empty batch while avoiding a meaningless snapshot lock."""

    session = RecordingSession([FirstResult(None)])
    snapshot_lock = AsyncMock()
    monkeypatch.setattr(process_ptg.db, "transaction", transaction_factory(session))
    monkeypatch.setattr(
        process_ptg.db,
        "_execution_session",
        transaction_factory(session),
    )
    monkeypatch.setattr(process_ptg, "lock_writable_snapshot", snapshot_lock)

    await process_ptg._push_fenced_ptg2_plan_months([])

    snapshot_lock.assert_not_awaited()
    assert len(session.executions) == 1
    assert isinstance(session.executions[0][0], Insert)
    assert not isinstance(session.executions[0][0], InsertAdapter)


@pytest.mark.asyncio
async def test_plan_month_writes_route_through_the_fenced_adapter_path(monkeypatch):
    """Keep plan-month writes on the transaction-safe adapter boundary."""

    entries = [{"snapshot_id": "snapshot-one", "plan_id": "plan-one"}]
    push_fenced = AsyncMock()
    monkeypatch.setattr(
        process_ptg,
        "_push_fenced_ptg2_plan_months",
        push_fenced,
    )

    result = await process_ptg._push_ptg2_objects(
        entries,
        process_ptg.PTG2PlanMonth,
    )

    assert result is None
    push_fenced.assert_awaited_once_with(entries)
