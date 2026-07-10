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
    executed = []
    transaction = _RecordingTransaction(executed)

    async def fake_snapshot_row(*_args):
        assert transaction.active
        return {
            "snapshot_id": "snap_new",
            "status": "published",
            "import_month": datetime.date(2026, 6, 1),
            "manifest": {"serving_index": {"source_key": "source_a"}},
        }

    async def fake_current_source_snapshot(*_args):
        assert transaction.active
        return "snap_old"

    async def fake_source_plan_rows(**kwargs):
        assert transaction.active
        assert kwargs["updated_at"].tzinfo is None
        return [
            {
                "plan_source_key": "ps_1",
                "plan_id": "P1",
                "plan_market_type": "group",
                "import_month": kwargs["import_month"],
                "source_key": kwargs["source_key"],
                "snapshot_id": kwargs["snapshot_id"],
                "previous_snapshot_id": kwargs["previous_snapshot_id"],
                "updated_at": kwargs["updated_at"],
            }
        ]

    clear_calls = []
    monkeypatch.setattr(source_snapshot_control, "_snapshot_row", fake_snapshot_row)
    monkeypatch.setattr(source_snapshot_control, "_current_source_snapshot", fake_current_source_snapshot)
    monkeypatch.setattr(source_snapshot_control, "_source_plan_rows", fake_source_plan_rows)
    monkeypatch.setattr(source_snapshot_control.db, "transaction", lambda: transaction)
    monkeypatch.setattr(source_snapshot_control, "_clear_ptg2_snapshot_cache", lambda: clear_calls.append(True))

    result = asyncio.run(
        source_snapshot_control.promote_ptg2_source_snapshot(
            source_key="source_a",
            snapshot_id="snap_new",
            expected_current_snapshot_id="snap_old",
        )
    )

    assert result == {
        "source_key": "source_a",
        "snapshot_id": "snap_new",
        "previous_snapshot_id": "snap_old",
        "plan_source_count": 1,
    }
    assert "pg_advisory_xact_lock" in executed[0][0]
    assert executed[0][1] == {"publish_lock_key": source_pointers.PTG2_SOURCE_POINTER_GC_LOCK_KEY}
    assert any("ptg2_current_source_snapshot" in statement for statement, _ in executed)
    assert any("DELETE FROM \"mrf\".ptg2_current_plan_source" in statement for statement, _ in executed)
    assert any("ptg2_current_plan_source" in statement and "ON CONFLICT" in statement for statement, _ in executed)
    assert clear_calls == [True]


def test_source_plan_rows_falls_back_to_import_catalog(monkeypatch):
    captured_sql_calls = []

    async def fake_database_all(statement, **params):
        captured_sql_calls.append((str(statement), params))
        if "hp_import_control" in str(statement):
            return [
                {"plan_id": "386004849", "plan_market_type": "group"},
                {"plan_id": "611480273", "plan_market_type": "Group"},
            ]
        return []

    async def is_catalog_table_present(schema, table_name):
        return (schema, table_name) in {
            ("hp_import_control", "source_file_import"),
            ("hp_import_control", "discovered_plan_file"),
            ("hp_import_control", "discovered_plan"),
        }

    monkeypatch.setattr(source_pointers.db, "all", fake_database_all)
    monkeypatch.setattr(source_pointers, "_table_exists", is_catalog_table_present)

    plan_source_rows = asyncio.run(
        source_pointers._source_plan_rows(
            snapshot_id="ptg2:202607:abc",
            source_key="ptg_source",
            import_month=datetime.date(2026, 7, 1),
            previous_snapshot_id=None,
            updated_at=datetime.datetime(2026, 7, 5, 1, 0, 0),
            serving_index={"serving_table_layout": "lean_provider_key_v1"},
        )
    )

    assert [plan_source_row["plan_id"] for plan_source_row in plan_source_rows] == ["386004849", "611480273"]
    assert {plan_source_row["plan_market_type"] for plan_source_row in plan_source_rows} == {"group"}
    assert all(plan_source_row["source_key"] == "ptg_source" for plan_source_row in plan_source_rows)
    assert any("hp_import_control" in statement for statement, _params in captured_sql_calls)


def test_promote_ptg2_source_snapshot_refuses_stale_expected_pointer(monkeypatch):
    executed_statements = []
    transaction = _RecordingTransaction(executed_statements)
    monkeypatch.setattr(
        source_snapshot_control,
        "_snapshot_row",
        AsyncMock(
            return_value={
                "snapshot_id": "snap_new",
                "status": "published",
                "import_month": "2026-06-01",
                "manifest": {"serving_index": {"source_key": "source_a"}},
            }
        ),
    )
    monkeypatch.setattr(source_snapshot_control, "_current_source_snapshot", AsyncMock(return_value="snap_old"))
    monkeypatch.setattr(source_snapshot_control.db, "transaction", lambda: transaction)

    with pytest.raises(source_snapshot_control.SourceSnapshotConflict):
        asyncio.run(
            source_snapshot_control.promote_ptg2_source_snapshot(
                source_key="source_a",
                snapshot_id="snap_new",
                expected_current_snapshot_id="snap_other",
            )
        )
    assert len(executed_statements) == 1
    assert "pg_advisory_xact_lock" in executed_statements[0][0]


def test_build_ptg2_source_snapshot_remove_plan_refuses_current_references(monkeypatch):
    monkeypatch.setattr(
        source_snapshot_control,
        "_snapshot_row",
        AsyncMock(
            return_value={
                "snapshot_id": "snap_live",
                "status": "published",
                "import_month": "2026-06-01",
                "manifest": {"serving_index": {"source_key": "source_a", "tables": {"rates": "ptg2_rates_live"}}},
            }
        ),
    )
    monkeypatch.setattr(
        source_snapshot_control,
        "_current_references",
        AsyncMock(return_value={"global_slots": [], "source_keys": ["source_a"], "plan_source_keys": ["ps_1"]}),
    )
    monkeypatch.setattr(source_snapshot_control, "_snapshot_manifest_table_names", lambda _manifest: ["ptg2_rates_live"])
    monkeypatch.setattr(source_snapshot_control, "_all_snapshot_manifest_rows", AsyncMock(return_value=[]))
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
    assert plan["tables"] == ["ptg2_rates_live"]
    assert plan["artifact_manifest_ids"] == ["artifact_1"]


def test_build_ptg2_source_snapshot_remove_plan_includes_manifest_snapshot_tables(monkeypatch):
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
    monkeypatch.setattr(
        source_snapshot_control,
        "_current_references",
        AsyncMock(return_value={"global_slots": [], "source_keys": [], "plan_source_keys": []}),
    )
    monkeypatch.setattr(source_snapshot_control, "_all_snapshot_manifest_rows", AsyncMock(return_value=[]))
    monkeypatch.setattr(source_snapshot_control, "_artifact_manifest_ids", AsyncMock(return_value=[]))

    plan = asyncio.run(
        source_snapshot_control.build_ptg2_source_snapshot_remove_plan(
            snapshot_id="snap_old",
            source_key="source_a",
        )
    )

    assert plan["removable"] is True
    assert plan["tables"] == [
        "ptg2_serving_abcd1234",
        "ptg2_price_atom_abcd1234",
        "ptg2_provider_set_component_abcd1234",
        "ptg2_provider_group_member_abcd1234",
        "ptg2_provider_group_location_abcd1234",
        "ptg2_code_count_abcd1234",
    ]


def test_remove_ptg2_source_snapshot_delegates_table_drop_and_metadata_delete(monkeypatch):
    dropped_tables = []
    status_calls = []
    transaction_statements = []
    transaction = _RecordingTransaction(transaction_statements)

    async def fake_status(statement, **params):
        assert transaction.active
        status_calls.append((statement, params))
        return 1

    async def fake_remove_plan(**_kwargs):
        assert transaction.active
        return {
            "snapshot_id": "snap_old",
            "source_key": "source_a",
            "exists": True,
            "removable": True,
            "tables": ["ptg2_rates_old", "ptg2_providers_old"],
            "artifact_manifest_ids": ["artifact_1"],
            "current_references": {},
        }

    monkeypatch.setattr(
        source_snapshot_control,
        "build_ptg2_source_snapshot_remove_plan",
        fake_remove_plan,
    )
    monkeypatch.setattr(source_snapshot_control, "_drop_ptg2_snapshot_table_names", AsyncMock(side_effect=dropped_tables.append))
    monkeypatch.setattr(source_snapshot_control.db, "status", fake_status)
    monkeypatch.setattr(source_snapshot_control.db, "transaction", lambda: transaction)

    cleanup_summary = asyncio.run(
        source_snapshot_control.remove_ptg2_source_snapshot(snapshot_id="snap_old", source_key="source_a")
    )

    assert dropped_tables == [["ptg2_rates_old", "ptg2_providers_old"]]
    assert cleanup_summary["executed"] is True
    assert cleanup_summary["deleted_tables"] == 2
    assert cleanup_summary["deleted_artifact_chunks"] == 1
    assert cleanup_summary["deleted_artifact_manifests"] == 1
    assert cleanup_summary["deleted_snapshots"] == 1
    assert transaction_statements == [
        (
            "SELECT pg_advisory_xact_lock(hashtext(:publish_lock_key))",
            {"publish_lock_key": source_pointers.PTG2_SOURCE_POINTER_GC_LOCK_KEY},
        )
    ]
    assert any("ptg2_artifact_blob_chunk" in call[0] for call in status_calls)
    assert any("ptg2_artifact_manifest" in call[0] and call[1]["snapshot_id"] == "snap_old" for call in status_calls)
    assert any("ptg2_snapshot" in call[0] and call[1]["snapshot_id"] == "snap_old" for call in status_calls)


def _cross_source_shared_table_snapshots():
    """Return target and peer manifests that share one serving table."""
    target_snapshot_dict = {
        "snapshot_id": "source-a-old",
        "status": "published",
        "import_month": "2026-07-01",
        "manifest": {
            "serving_index": {
                "source_key": "source_a",
                "table": "mrf.ptg2_serving_shared",
                "price_atom_table": "mrf.ptg2_price_atom_source_a_old",
            }
        },
    }
    snapshot_manifest_rows = [
        target_snapshot_dict,
        {
            "snapshot_id": "source-b-current",
            "manifest": {
                "serving_index": {
                    "source_key": "source_b",
                    "table": "mrf.ptg2_serving_shared",
                }
            },
        },
    ]
    return target_snapshot_dict, snapshot_manifest_rows


def test_remove_ptg2_source_snapshot_preserves_cross_source_shared_table(monkeypatch):
    """Targeted removal drops only tables exclusively owned by its snapshot."""
    transaction_statements = []
    transaction = _RecordingTransaction(transaction_statements)
    drop_table_names_mock = AsyncMock()
    target_snapshot_dict, snapshot_manifest_rows = _cross_source_shared_table_snapshots()

    async def fake_status(_statement, **_params):
        assert transaction.active
        return 1

    async def fake_snapshot_row(*_args):
        assert transaction.active
        return target_snapshot_dict

    async def fake_all_snapshot_manifest_rows(*_args, **_kwargs):
        assert transaction.active
        return snapshot_manifest_rows

    monkeypatch.setattr(source_snapshot_control, "_snapshot_row", fake_snapshot_row)
    monkeypatch.setattr(
        source_snapshot_control,
        "_current_references",
        AsyncMock(return_value={"global_slots": [], "source_keys": [], "plan_source_keys": []}),
    )
    monkeypatch.setattr(source_snapshot_control, "_artifact_manifest_ids", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        source_snapshot_control,
        "_all_snapshot_manifest_rows",
        fake_all_snapshot_manifest_rows,
    )
    monkeypatch.setattr(source_snapshot_control, "_drop_ptg2_snapshot_table_names", drop_table_names_mock)
    monkeypatch.setattr(source_snapshot_control.db, "status", fake_status)
    monkeypatch.setattr(source_snapshot_control.db, "transaction", lambda: transaction)

    removal_summary = asyncio.run(
        source_snapshot_control.remove_ptg2_source_snapshot(
            snapshot_id="source-a-old",
            source_key="source_a",
        )
    )

    drop_table_names_mock.assert_awaited_once_with(["ptg2_price_atom_source_a_old"])
    assert removal_summary["tables"] == ["ptg2_price_atom_source_a_old"]
    assert removal_summary["deleted_tables"] == 1
    assert transaction_statements == [
        (
            "SELECT pg_advisory_xact_lock(hashtext(:publish_lock_key))",
            {"publish_lock_key": source_pointers.PTG2_SOURCE_POINTER_GC_LOCK_KEY},
        )
    ]


def test_retire_ptg2_source_snapshot_deletes_current_source_and_plan_pointers(monkeypatch):
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
            "manifest": {"serving_index": {"source_key": "source_a"}},
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
