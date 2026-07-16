# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import datetime
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import source_snapshot_control


class _RecordingTransaction:
    def __init__(self):
        self.statements = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def execute(self, statement, params):
        self.statements.append((str(statement), params))


def _published_snapshot(snapshot_id, serving_index):
    serving_index = {
        "arch_version": "postgres_binary_v3",
        "storage_generation": "shared_blocks_v3",
        **serving_index,
    }
    return {
        "snapshot_id": snapshot_id,
        "status": "published",
        "import_month": datetime.date(2026, 7, 1),
        "manifest": {"serving_index": serving_index},
    }


def test_manual_promotion_rejects_published_snapshot_rollback(monkeypatch):
    publish = AsyncMock(side_effect=ValueError("snapshot is not a validated candidate"))
    monkeypatch.setattr(
        source_snapshot_control,
        "activate_ptg2_source_candidate",
        publish,
    )

    with pytest.raises(ValueError, match="not a validated candidate"):
        asyncio.run(
            source_snapshot_control.promote_ptg2_source_snapshot(
                source_key="source_a",
                snapshot_id="snap_a",
                expected_current_snapshot_id="snap_b",
            )
        )
    publish.assert_awaited_once()


@pytest.mark.parametrize("snapshot_status", ["pending", "running", "building"])
def test_manual_removal_rejects_in_flight_snapshot(monkeypatch, snapshot_status):
    monkeypatch.setattr(
        source_snapshot_control,
        "_snapshot_row",
        AsyncMock(
            return_value={
                "snapshot_id": "snap_retry",
                "status": snapshot_status,
                "manifest": {"serving_index": {
                    "source_key": "source_a",
                    "arch_version": "postgres_binary_v3",
                    "storage_generation": "shared_blocks_v3",
                }},
            }
        ),
    )
    monkeypatch.setattr(
        source_snapshot_control,
        "_current_references",
        AsyncMock(return_value={"global_slots": [], "source_keys": [], "plan_source_keys": []}),
    )
    monkeypatch.setattr(
        source_snapshot_control,
        "_artifact_manifest_ids",
        AsyncMock(return_value=[]),
    )

    removal_plan = asyncio.run(
        source_snapshot_control.build_ptg2_source_snapshot_remove_plan(
            snapshot_id="snap_retry",
            source_key="source_a",
        )
    )

    assert removal_plan["removable"] is False
    assert removal_plan["reason"] == f"snapshot is in-flight (status: {snapshot_status})"


def test_manual_removal_allows_unreferenced_validated_candidate(monkeypatch):
    monkeypatch.setattr(
        source_snapshot_control,
        "_snapshot_row",
        AsyncMock(
            return_value={
                "snapshot_id": "snap_validated",
                "status": "validated",
                "manifest": {"serving_index": {
                    "source_key": "source_a",
                    "arch_version": "postgres_binary_v3",
                    "storage_generation": "shared_blocks_v3",
                }},
            }
        ),
    )
    monkeypatch.setattr(
        source_snapshot_control,
        "_current_references",
        AsyncMock(return_value={
            "global_slots": [],
            "source_keys": [],
            "plan_source_keys": [],
            "previous_global_slots": [],
            "previous_source_keys": [],
            "previous_plan_source_keys": [],
        }),
    )
    monkeypatch.setattr(
        source_snapshot_control,
        "_artifact_manifest_ids",
        AsyncMock(return_value=[]),
    )

    removal_plan = asyncio.run(
        source_snapshot_control.build_ptg2_source_snapshot_remove_plan(
            snapshot_id="snap_validated",
            source_key="source_a",
        )
    )

    assert removal_plan["removable"] is True
    assert removal_plan["reason"] is None


def test_manual_removal_rejects_previous_pointer_reference(monkeypatch):
    monkeypatch.setattr(
        source_snapshot_control,
        "_snapshot_row",
        AsyncMock(return_value=_published_snapshot("snap_a", {"source_key": "source_a"})),
    )
    monkeypatch.setattr(
        source_snapshot_control,
        "_current_references",
        AsyncMock(
            return_value={
                "global_slots": [],
                "source_keys": [],
                "plan_source_keys": [],
                "previous_global_slots": [],
                "previous_source_keys": ["source_a"],
                "previous_plan_source_keys": ["plan_source_a"],
            }
        ),
    )
    monkeypatch.setattr(
        source_snapshot_control,
        "_artifact_manifest_ids",
        AsyncMock(return_value=[]),
    )

    removal_plan = asyncio.run(
        source_snapshot_control.build_ptg2_source_snapshot_remove_plan(
            snapshot_id="snap_a",
            source_key="source_a",
        )
    )

    assert removal_plan["removable"] is False
    assert "previous source pointer" in removal_plan["reason"]
    assert "previous plan pointer" in removal_plan["reason"]


@pytest.mark.parametrize("snapshot_status", ["pending", "running", "building", "validated"])
def test_manual_retire_rejects_in_flight_snapshot(monkeypatch, snapshot_status):
    transaction = _RecordingTransaction()
    monkeypatch.setattr(
        source_snapshot_control,
        "_snapshot_row",
        AsyncMock(
            return_value={
                "snapshot_id": "snap_retry",
                "status": snapshot_status,
                "manifest": {"serving_index": {
                    "source_key": "source_a",
                    "arch_version": "postgres_binary_v3",
                    "storage_generation": "shared_blocks_v3",
                }},
            }
        ),
    )
    monkeypatch.setattr(source_snapshot_control.db, "transaction", lambda: transaction)

    with pytest.raises(ValueError, match=f"snapshot is in-flight \\(status: {snapshot_status}\\)"):
        asyncio.run(
            source_snapshot_control.retire_ptg2_source_snapshot(
                snapshot_id="snap_retry",
                source_key="source_a",
            )
        )

    assert len(transaction.statements) == 1
    assert "pg_advisory_xact_lock" in transaction.statements[0][0]


def test_manual_retire_rejects_legacy_manifest_before_pointer_mutation(monkeypatch):
    transaction = _RecordingTransaction()
    monkeypatch.setattr(
        source_snapshot_control,
        "_snapshot_row",
        AsyncMock(return_value={
            "snapshot_id": "snap_legacy",
            "status": "published",
            "manifest": {"serving_index": {"source_key": "source_a"}},
        }),
    )
    monkeypatch.setattr(source_snapshot_control.db, "transaction", lambda: transaction)

    with pytest.raises(ValueError, match="postgres_binary_v3/shared_blocks_v3"):
        asyncio.run(
            source_snapshot_control.retire_ptg2_source_snapshot(
                snapshot_id="snap_legacy",
                source_key="source_a",
            )
        )

    assert len(transaction.statements) == 1


def test_current_references_include_previous_pointer_columns(monkeypatch):
    async def pointer_rows(statement, **params):
        assert params == {"snapshot_id": "snap_a"}
        is_previous_reference = "previous_snapshot_id" in statement
        if not is_previous_reference:
            return []
        if "ptg2_current_snapshot" in statement:
            return [{"slot": "current"}]
        if "ptg2_current_source_snapshot" in statement:
            return [{"source_key": "source_a"}]
        if "ptg2_current_plan_source" in statement:
            return [{"plan_source_key": "plan_source_a"}]
        raise AssertionError(statement)

    monkeypatch.setattr(source_snapshot_control.db, "all", pointer_rows)

    references = asyncio.run(
        source_snapshot_control._current_references("mrf", "snap_a")
    )

    assert references["previous_global_slots"] == ["current"]
    assert references["previous_source_keys"] == ["source_a"]
    assert references["previous_plan_source_keys"] == ["plan_source_a"]
