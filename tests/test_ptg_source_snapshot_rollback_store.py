# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime

import pytest

from process.ptg_parts import source_snapshot_rollback_store as rollback_store
from process.ptg_parts.source_snapshot_rollback_types import RollbackDecision


class _ChangedRowResult:
    def one_or_none(self):
        return {"changed": True}


class _RecordingSession:
    def __init__(self):
        self.executed_statements = []

    async def execute(self, statement, params):
        self.executed_statements.append((str(statement), params))
        return _ChangedRowResult()


def _plan_pointer_entry() -> dict:
    return {
        "plan_source_key": "plan-source-key",
        "plan_id": "plan-1",
        "plan_market_type": "group",
        "import_month": datetime.date(2026, 7, 1),
        "source_key": "source_a",
        "snapshot_id": "snapshot_a",
        "previous_snapshot_id": "snapshot_b",
        "updated_at": datetime.datetime.min,
    }


@pytest.mark.asyncio
async def test_apply_rollback_mutates_only_pointer_tables():
    session = _RecordingSession()
    allowed_import_month = datetime.date(2026, 6, 1)
    updated_at = datetime.datetime(2026, 7, 2, 1, 0)
    decision = RollbackDecision(
        is_already_rolled_back=False,
        plan_pointer_entries=(_plan_pointer_entry(),),
        should_reverse_global_pointer=True,
        allowed_action="reverse",
        allowed_snapshot_id="allowed-snapshot-a",
        allowed_previous_snapshot_id="snapshot_b",
        allowed_import_month=allowed_import_month,
    )

    await rollback_store.apply_rollback(
        session,
        schema_name="mrf",
        source_key="source_a",
        snapshot_id="snapshot_a",
        expected_current_snapshot_id="snapshot_b",
        target_import_month=datetime.date(2026, 7, 1),
        updated_at=updated_at,
        decision=decision,
    )

    statements = [statement for statement, _params in session.executed_statements]
    joined_statements = "\n".join(statements)
    assert 'UPDATE "mrf".ptg2_current_source_snapshot' in joined_statements
    assert 'DELETE FROM "mrf".ptg2_current_plan_source' in joined_statements
    assert 'INSERT INTO "mrf".ptg2_current_plan_source' in joined_statements
    assert 'UPDATE "mrf".ptg2_current_snapshot' in joined_statements
    assert 'UPDATE "mrf".ptg2_snapshot' not in joined_statements
    allowed_params = session.executed_statements[-1][1]
    assert allowed_params["target_snapshot_id"] == "allowed-snapshot-a"
    assert allowed_params["allowed_import_month"] == allowed_import_month


@pytest.mark.asyncio
async def test_apply_rollback_deletes_null_predecessor_allowed_pointer():
    session = _RecordingSession()
    decision = RollbackDecision(
        is_already_rolled_back=False,
        plan_pointer_entries=(_plan_pointer_entry(),),
        should_reverse_global_pointer=False,
        allowed_action="delete",
    )

    await rollback_store.apply_rollback(
        session,
        schema_name="mrf",
        source_key="source_a",
        snapshot_id="snapshot_a",
        expected_current_snapshot_id="snapshot_b",
        target_import_month=datetime.date(2026, 7, 1),
        updated_at=datetime.datetime(2026, 7, 2, 1, 0),
        decision=decision,
    )

    final_statement, final_params = session.executed_statements[-1]
    assert 'DELETE FROM "mrf".ptg2_current_source_snapshot' in final_statement
    assert "previous_snapshot_id IS NULL" in final_statement
    assert final_params["expected_current_snapshot_id"] == "snapshot_b"
