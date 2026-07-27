# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import source_snapshot_predecessor_retirement_store as store
from process.ptg_parts.source_snapshot_predecessor_retirement import (
    normalized_predecessor_retirement_request,
)
from process.ptg_parts.source_snapshot_predecessor_retirement_types import (
    PTG2PredecessorRetirementConflict,
    PredecessorRetirementDecision,
)


class _ReturningResult:
    def __init__(self, row):
        self._row = row

    def one_or_none(self):
        return self._row


class _RecordingSession:
    def __init__(self):
        self.statements: list[tuple[str, dict]] = []

    async def execute(self, statement, params):
        sql = str(statement)
        self.statements.append((sql, dict(params)))
        if "RETURNING source_key" in sql:
            return _ReturningResult({"source_key": "synthetic-source"})
        if "RETURNING plan_source_key" in sql:
            return _ReturningResult({"plan_source_key": "synthetic-plan-key"})
        if "RETURNING slot" in sql:
            return _ReturningResult({"slot": "current"})
        if "RETURNING owner_id" in sql:
            return _ReturningResult({"owner_id": "rollback-owner"})
        if "RETURNING idempotency_key" in sql:
            return _ReturningResult(
                {
                    "idempotency_key": "retire-synthetic-001",
                    "retired_at": datetime.datetime(
                        2026,
                        7,
                        27,
                        tzinfo=datetime.UTC,
                    ),
                }
            )
        raise AssertionError(sql)


def _request():
    return normalized_predecessor_retirement_request(
        source_key="synthetic-source",
        current_snapshot_id="snapshot-current",
        predecessor_snapshot_id="snapshot-previous",
        rollback_pin_mode="owned",
        rollback_owner_id="rollback-owner",
        actor="operator@example.invalid",
        reason="retention window complete",
        idempotency_key="retire-synthetic-001",
    )


def _absent_pin_request():
    return normalized_predecessor_retirement_request(
        source_key="synthetic-source",
        current_snapshot_id="snapshot-current",
        predecessor_snapshot_id="snapshot-previous",
        rollback_pin_mode="absent",
        rollback_owner_id=None,
        actor="operator@example.invalid",
        reason="retention window complete",
        idempotency_key="retire-synthetic-absent-001",
    )


@pytest.mark.asyncio
async def test_apply_retirement_uses_exact_cas_and_never_mutates_snapshot_rows():
    session = _RecordingSession()

    await store.apply_predecessor_retirement(
        session,
        schema_name="mrf",
        request=_request(),
        decision=PredecessorRetirementDecision(
            source_pointer_count=1,
            plan_pointer_count=1,
            global_pointer_count=1,
            deleted_rollback_pin_count=1,
        ),
    )

    joined = "\n".join(statement for statement, _params in session.statements)
    assert 'UPDATE "mrf".ptg2_current_source_snapshot' in joined
    assert 'UPDATE "mrf".ptg2_current_plan_source' in joined
    assert 'UPDATE "mrf".ptg2_current_snapshot' in joined
    assert 'DELETE FROM "mrf".ptg2_snapshot_pin' in joined
    assert "previous_snapshot_id = NULL" in joined
    assert "snapshot_id = :current_snapshot_id" in joined
    assert "previous_snapshot_id = :predecessor_snapshot_id" in joined
    assert 'UPDATE "mrf".ptg2_snapshot' not in joined
    assert 'DELETE FROM "mrf".ptg2_snapshot\n' not in joined


@pytest.mark.asyncio
async def test_absent_pin_mode_performs_no_pin_delete():
    session = _RecordingSession()

    await store.apply_predecessor_retirement(
        session,
        schema_name="mrf",
        request=_absent_pin_request(),
        decision=PredecessorRetirementDecision(1, 1, 0, 0),
    )

    joined = "\n".join(statement for statement, _params in session.statements)
    assert "ptg2_current_source_snapshot" in joined
    assert "ptg2_current_plan_source" in joined
    assert "DELETE FROM" not in joined


@pytest.mark.asyncio
async def test_insert_audit_records_counts_without_snapshot_foreign_key_writes():
    session = _RecordingSession()
    retired_at = datetime.datetime(2026, 7, 27, tzinfo=datetime.UTC)

    audit_record = await store.insert_retirement_audit(
        session,
        schema_name="mrf",
        request=_request(),
        decision=PredecessorRetirementDecision(
            source_pointer_count=1,
            plan_pointer_count=2,
            global_pointer_count=0,
            deleted_rollback_pin_count=1,
        ),
        retired_at=retired_at,
    )

    statement, params = session.statements[-1]
    assert 'INSERT INTO "mrf".ptg2_predecessor_retirement_audit' in statement
    assert params["cleared_plan_pointer_count"] == 2
    assert params["deleted_rollback_pin_count"] == 1
    assert audit_record["idempotency_key"] == "retire-synthetic-001"


@pytest.mark.asyncio
async def test_apply_skips_absent_global_pointer(monkeypatch):
    clear_source = AsyncMock()
    clear_plans = AsyncMock()
    clear_global = AsyncMock()
    delete_pin = AsyncMock()
    monkeypatch.setattr(store, "_clear_source_predecessor", clear_source)
    monkeypatch.setattr(store, "_clear_plan_predecessors", clear_plans)
    monkeypatch.setattr(store, "_clear_global_predecessor", clear_global)
    monkeypatch.setattr(store, "_delete_exact_rollback_pin", delete_pin)

    await store.apply_predecessor_retirement(
        object(),
        schema_name="mrf",
        request=_request(),
        decision=PredecessorRetirementDecision(1, 2, 0, 1),
    )

    clear_source.assert_awaited_once()
    clear_plans.assert_awaited_once()
    clear_global.assert_not_awaited()
    delete_pin.assert_awaited_once()


@pytest.mark.asyncio
async def test_compare_and_swap_helpers_reject_changed_rows():
    no_row_session = AsyncMock()
    no_row_session.execute.return_value = _ReturningResult(None)
    with pytest.raises(PTG2PredecessorRetirementConflict, match="changed"):
        await store._require_one_changed(
            no_row_session,
            "UPDATE exact row",
            {},
            conflict_message="row changed",
        )
    with pytest.raises(PTG2PredecessorRetirementConflict, match="changed"):
        await store._require_changed_count(
            no_row_session,
            "UPDATE exact rows",
            {},
            expected_count=2,
            conflict_message="rows changed",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "postcheck_by_field",
    [
        {"pin_references": 1},
        {"preserved_lineage": 0, "preserved_current_pointer": 1},
    ],
)
async def test_postcheck_rejects_live_references_and_lineage_drift(
    monkeypatch,
    postcheck_by_field,
):
    complete_postcheck_by_field = {
        "global_references": 0,
        "source_references": 0,
        "plan_references": 0,
        "pin_references": 0,
        "release_references": 0,
        "control_release_references": 0,
        "control_pin_references": 0,
        "preserved_lineage": 1,
        "preserved_current_pointer": 1,
        **postcheck_by_field,
    }
    monkeypatch.setattr(
        store,
        "_one",
        AsyncMock(return_value=complete_postcheck_by_field),
    )

    with pytest.raises(PTG2PredecessorRetirementConflict):
        await store.postcheck_predecessor_retirement(
            object(),
            schema_name="mrf",
            control_schema_name="hp_" + "imp" + "ort_control",
            request=_request(),
        )


@pytest.mark.asyncio
async def test_audit_timestamp_and_insert_are_fail_closed(monkeypatch):
    load_one = AsyncMock(return_value={"retired_at": "not-a-timestamp"})
    monkeypatch.setattr(store, "_one", load_one)
    with pytest.raises(RuntimeError, match="retirement time"):
        await store.database_utc_timestamp(object())

    load_one.return_value = {}
    with pytest.raises(
        PTG2PredecessorRetirementConflict,
        match="did not persist",
    ):
        await store.insert_retirement_audit(
            object(),
            schema_name="mrf",
            request=_request(),
            decision=PredecessorRetirementDecision(1, 1, 0, 1),
            retired_at=datetime.datetime(
                2026,
                7,
                27,
                tzinfo=datetime.UTC,
            ),
        )
