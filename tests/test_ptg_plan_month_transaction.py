# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Transaction-bound PTG plan-month write regressions."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import date, datetime
import importlib
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.dialects.postgresql import asyncpg
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
async def test_empty_plan_month_batch_skips_sql_and_snapshot_locks(monkeypatch):
    """An empty batch must not attempt a default-values insert."""

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
    assert session.executions == []


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


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_second_batch", [False, True])
async def test_large_plan_month_write_bounds_binds_in_one_fenced_transaction(
    monkeypatch, fail_second_batch,
):
    """Oversized input keeps every row and rolls back on a later batch failure."""

    entries = _large_plan_month_entries()
    assert len(entries) * len(entries[0]) > 32_767
    events = []
    pending_ids = []
    committed_ids = []

    class Session:
        """Reject oversized statements and record transaction publication."""

        @asynccontextmanager
        async def begin(self):
            events.append("begin")
            try:
                yield
            except RuntimeError:
                pending_ids.clear()
                events.append("rollback")
                raise
            else:
                committed_ids.extend(pending_ids)
                events.append("commit")

        async def execute(self, statement, _parameters):
            compiled = statement.compile(dialect=asyncpg.dialect())
            assert len(compiled.positiontup) <= 30_000
            assert "ON CONFLICT (plan_month_id) DO UPDATE" in str(compiled)
            assert events[:3] == ["begin", "snapshot-0", "snapshot-1"]
            assert events[-1] != "commit"
            if fail_second_batch and pending_ids:
                raise RuntimeError("later batch failed")
            pending_ids.extend(
                value for key, value in compiled.params.items()
                if key.startswith("plan_month_id_m")
            )
            events.append("insert")
            return FirstResult(None)

    session = Session()

    async def lock_snapshot(lock_session, _db, **kwargs):
        assert lock_session is session
        events.append(kwargs["snapshot_id"])

    monkeypatch.setattr(process_ptg.db, "session", transaction_factory(session))
    monkeypatch.setattr(process_ptg, "lock_writable_snapshot", lock_snapshot)

    if fail_second_batch:
        with pytest.raises(RuntimeError, match="later batch failed"):
            await process_ptg._push_fenced_ptg2_plan_months(entries)
        assert committed_ids == []
        assert events[-1] == "rollback"
    else:
        await process_ptg._push_fenced_ptg2_plan_months(entries)
        assert committed_ids == [entry["plan_month_id"] for entry in entries]
        assert events == ["begin", "snapshot-0", "snapshot-1", "insert", "insert", "commit"]


def _large_plan_month_entries():
    """Return a synthetic plan scope exceeding one asyncpg statement."""

    return [
        {
            "plan_month_id": f"month-{index}",
            "snapshot_id": f"snapshot-{index % 2}",
            "plan_hash": f"plan-{index}",
            "import_month": date(2026, 1, 1),
            "created_at": datetime(2026, 1, 1),
        }
        for index in range(7_000)
    ]
