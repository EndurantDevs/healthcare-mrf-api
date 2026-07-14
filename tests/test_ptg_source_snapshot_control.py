# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import datetime
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import source_pointers, source_snapshot_control


class _RecordingTransaction:
    def __init__(self, executed):
        self.executed = executed
        self.active = False

    async def __aenter__(self):
        self.active = True
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.active = False
        return False

    async def execute(self, statement, params):
        self.executed.append((str(statement), params))


def test_promote_ptg2_source_snapshot_repoints_source_and_plan_pointers(monkeypatch):
    publish_calls = []
    clear_calls = []

    async def fake_publish(**kwargs):
        publish_calls.append(kwargs)
        return {
            "status": "promoted",
            "source_key": kwargs["source_key"],
            "snapshot_id": kwargs["snapshot_id"],
            "previous_snapshot_id": kwargs["expected_current_snapshot_id"],
            "plan_source_count": 1,
            "global_pointer": "reconciled",
        }

    monkeypatch.setattr(
        source_snapshot_control,
        "activate_ptg2_source_candidate",
        fake_publish,
    )
    monkeypatch.setattr(
        source_snapshot_control,
        "_clear_ptg2_snapshot_cache",
        lambda: clear_calls.append(True),
    )

    result = asyncio.run(
        source_snapshot_control.promote_ptg2_source_snapshot(
            source_key="source_a",
            snapshot_id="snap_new",
            expected_current_snapshot_id="snap_old",
        )
    )

    assert result == {
        "status": "promoted",
        "source_key": "source_a",
        "snapshot_id": "snap_new",
        "previous_snapshot_id": "snap_old",
        "plan_source_count": 1,
        "global_pointer": "reconciled",
    }
    assert len(publish_calls) == 1
    publish = publish_calls[0]
    assert publish["source_key"] == "source_a"
    assert publish["expected_current_snapshot_id"] == "snap_old"
    assert clear_calls == [True]


def test_promote_ptg2_source_snapshot_rejects_candidate_predecessor_mismatch(monkeypatch):
    publish = AsyncMock(side_effect=ValueError("candidate predecessor disagrees"))
    monkeypatch.setattr(
        source_snapshot_control,
        "activate_ptg2_source_candidate",
        publish,
    )

    with pytest.raises(ValueError, match="predecessor disagrees"):
        asyncio.run(
            source_snapshot_control.promote_ptg2_source_snapshot(
                source_key="source_a",
                snapshot_id="snap_new",
                expected_current_snapshot_id="snap_old",
            )
        )
    publish.assert_awaited_once()


def test_source_plan_rows_use_only_public_snapshot_scope(monkeypatch):
    captured_sql_calls = []

    async def fake_database_all(statement, **params):
        captured_sql_calls.append((str(statement), params))
        return [
            {"plan_id": "386004849", "plan_market_type": "group"},
            {"plan_id": "611480273", "plan_market_type": "Group"},
        ]

    monkeypatch.setattr(source_pointers.db, "all", fake_database_all)

    plan_source_rows = asyncio.run(
        source_pointers._source_plan_rows(
            snapshot_id="ptg2:202607:abc",
            source_key="ptg_source",
            import_month=datetime.date(2026, 7, 1),
            previous_snapshot_id=None,
            updated_at=datetime.datetime(2026, 7, 5, 1, 0, 0),
        )
    )

    assert [plan_source_row["plan_id"] for plan_source_row in plan_source_rows] == ["386004849", "611480273"]
    assert {plan_source_row["plan_market_type"] for plan_source_row in plan_source_rows} == {"group"}
    assert all(plan_source_row["source_key"] == "ptg_source" for plan_source_row in plan_source_rows)
    assert len(captured_sql_calls) == 1
    scope_sql = captured_sql_calls[0][0]
    assert "ptg2_v3_snapshot_scope" in scope_sql


def test_promote_ptg2_source_snapshot_refuses_stale_expected_pointer(monkeypatch):
    publish = AsyncMock(
        side_effect=source_pointers.PTG2SourcePointerConflict("stale pointer")
    )
    monkeypatch.setattr(
        source_snapshot_control,
        "activate_ptg2_source_candidate",
        publish,
    )

    with pytest.raises(source_snapshot_control.SourceSnapshotConflict):
        asyncio.run(
            source_snapshot_control.promote_ptg2_source_snapshot(
                source_key="source_a",
                snapshot_id="snap_new",
                expected_current_snapshot_id="snap_other",
                )
            )
    publish.assert_awaited_once()


def test_build_ptg2_source_snapshot_remove_plan_refuses_current_references(monkeypatch):
    monkeypatch.setattr(
        source_snapshot_control,
        "_snapshot_row",
        AsyncMock(
            return_value={
                "snapshot_id": "snap_live",
                "status": "published",
                "import_month": "2026-06-01",
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
        AsyncMock(return_value={"global_slots": [], "source_keys": ["source_a"], "plan_source_keys": ["ps_1"]}),
    )
    monkeypatch.setattr(source_snapshot_control, "_artifact_manifest_ids", AsyncMock(return_value=["artifact_1"]))

    plan = asyncio.run(
        source_snapshot_control.build_ptg2_source_snapshot_remove_plan(
            snapshot_id="snap_live",
            source_key="source_a",
        )
    )

    assert plan["removable"] is False
    assert "current source pointer" in plan["reason"]
    assert "current plan pointer" in plan["reason"]
    assert plan["tables"] == []
    assert plan["artifact_manifest_ids"] == ["artifact_1"]


def test_build_ptg2_source_snapshot_remove_plan_rejects_legacy_manifest(monkeypatch):
    monkeypatch.setattr(
        source_snapshot_control,
        "_snapshot_row",
        AsyncMock(
            return_value={
                "snapshot_id": "snap_old",
                "status": "published",
                "import_month": "2026-07-01",
                "manifest": {
                    "serving_index": {
                        "storage": "manifest_snapshot",
                        "source_key": "source_a",
                        "table": "mrf.ptg2_serving_abcd1234",
                        "price_atom_table": "mrf.ptg2_price_atom_abcd1234",
                        "provider_group_member_table": "mrf.ptg2_provider_group_member_abcd1234",
                        "provider_group_location_table": "mrf.ptg2_provider_group_location_abcd1234",
                        "provider_set_component_table": "mrf.ptg2_provider_set_component_abcd1234",
                        "code_count_table": "mrf.ptg2_code_count_abcd1234",
                        "ignored_table": "mrf.provider_directory_canonical_resource",
                    }
                },
            }
        ),
    )
    plan = asyncio.run(
        source_snapshot_control.build_ptg2_source_snapshot_remove_plan(
            snapshot_id="snap_old",
            source_key="source_a",
        )
    )

    assert plan["removable"] is False
    assert plan["reason"] == "only postgres_binary_v3/shared_blocks_v3 snapshots can be removed"
    assert plan["tables"] == []


def test_remove_ptg2_source_snapshot_deletes_only_v3_metadata(monkeypatch):
    """Verify remove ptg2 source snapshot deletes only v3 metadata."""
    status_calls = []
    transaction_statements = []
    transaction = _RecordingTransaction(transaction_statements)

    async def fake_status(statement, **params):
        assert transaction.active
        status_calls.append((statement, params))
        if "ptg2_v3_snapshot_" in statement:
            return 0
        return 1

    async def fake_remove_plan(**_kwargs):
        assert transaction.active
        return {
            "snapshot_id": "snap_old",
            "source_key": "source_a",
            "exists": True,
            "removable": True,
            "tables": [],
            "artifact_manifest_ids": ["artifact_1"],
            "current_references": {},
        }

    monkeypatch.setattr(
        source_snapshot_control,
        "build_ptg2_source_snapshot_remove_plan",
        fake_remove_plan,
    )
    monkeypatch.setattr(source_snapshot_control.db, "status", fake_status)
    monkeypatch.setattr(source_snapshot_control.db, "transaction", lambda: transaction)

    cleanup_summary = asyncio.run(
        source_snapshot_control.remove_ptg2_source_snapshot(snapshot_id="snap_old", source_key="source_a")
    )

    assert cleanup_summary["executed"] is True
    assert cleanup_summary["deleted_tables"] == 0
    assert cleanup_summary["deleted_v3_snapshot_scopes"] == 0
    assert cleanup_summary["deleted_v3_snapshot_bindings"] == 0
    assert cleanup_summary["deleted_artifact_chunks"] == 1
    assert cleanup_summary["deleted_artifact_manifests"] == 1
    assert cleanup_summary["deleted_snapshots"] == 1
    assert cleanup_summary["released_shared_layouts"] == 0
    assert transaction_statements == [
        (
            "SELECT pg_advisory_xact_lock(hashtext(:publish_lock_key))",
            {"publish_lock_key": source_pointers.PTG2_SOURCE_POINTER_GC_LOCK_KEY},
        )
    ]
    assert any("ptg2_artifact_blob_chunk" in call[0] for call in status_calls)
    assert any("ptg2_artifact_manifest" in call[0] and call[1]["snapshot_id"] == "snap_old" for call in status_calls)
    assert any("ptg2_snapshot" in call[0] and call[1]["snapshot_id"] == "snap_old" for call in status_calls)
    scope_delete_index = next(
        index for index, call in enumerate(status_calls) if "ptg2_v3_snapshot_scope" in call[0]
    )
    binding_delete_index = next(
        index for index, call in enumerate(status_calls) if "ptg2_v3_snapshot_binding" in call[0]
    )
    snapshot_delete_index = next(
        index
        for index, call in enumerate(status_calls)
        if 'DELETE FROM "mrf".ptg2_snapshot WHERE' in call[0]
    )
    assert scope_delete_index < binding_delete_index < snapshot_delete_index


def test_retire_ptg2_source_snapshot_deletes_current_source_and_plan_pointers(monkeypatch):
    """Verify retire ptg2 source snapshot deletes current source and plan pointers."""
    status_calls = []
    reference_calls = []
    transaction_statements = []
    transaction = _RecordingTransaction(transaction_statements)

    async def fake_status(statement, **params):
        assert transaction.active
        status_calls.append((statement, params))
        return 1

    async def fake_current_references(_schema, snapshot_id):
        assert transaction.active
        reference_calls.append(snapshot_id)
        if len(reference_calls) == 1:
            return {"global_slots": [], "source_keys": ["source_a"], "plan_source_keys": ["ps_1"]}
        return {"global_slots": [], "source_keys": [], "plan_source_keys": []}

    clear_calls = []
    async def fake_snapshot_row(*_args):
        assert transaction.active
        return {
            "snapshot_id": "snap_live",
            "status": "published",
            "manifest": {"serving_index": {
                "source_key": "source_a",
                "arch_version": "postgres_binary_v3",
                "storage_generation": "shared_blocks_v3",
            }},
        }

    monkeypatch.setattr(source_snapshot_control, "_snapshot_row", fake_snapshot_row)
    monkeypatch.setattr(source_snapshot_control, "_current_references", fake_current_references)
    monkeypatch.setattr(source_snapshot_control.db, "status", fake_status)
    monkeypatch.setattr(source_snapshot_control.db, "transaction", lambda: transaction)
    monkeypatch.setattr(source_snapshot_control, "_clear_ptg2_snapshot_cache", lambda: clear_calls.append(True))

    retire_summary = asyncio.run(
        source_snapshot_control.retire_ptg2_source_snapshot(
            snapshot_id="snap_live",
            source_key="source_a",
        )
    )

    assert retire_summary["retired"] is True
    assert retire_summary["deleted_plan_pointers"] == 1
    assert retire_summary["deleted_source_pointers"] == 1
    assert retire_summary["previous_current_references"]["source_keys"] == ["source_a"]
    assert retire_summary["current_references"]["source_keys"] == []
    assert transaction_statements == [
        (
            "SELECT pg_advisory_xact_lock(hashtext(:publish_lock_key))",
            {"publish_lock_key": source_pointers.PTG2_SOURCE_POINTER_GC_LOCK_KEY},
        )
    ]
    assert len(status_calls) == 2
    assert all(call[1] == {"snapshot_id": "snap_live", "source_key": "source_a"} for call in status_calls)
    assert any("ptg2_current_plan_source" in call[0] for call in status_calls)
    assert any("ptg2_current_source_snapshot" in call[0] for call in status_calls)
    assert clear_calls == [True]
