# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import datetime
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import source_snapshot_control


def test_promote_ptg2_source_snapshot_repoints_source_and_plan_pointers(monkeypatch):
    executed = []

    class FakeSession:
        async def execute(self, statement, params):
            executed.append((str(statement), params))

    class FakeTransaction:
        async def __aenter__(self):
            return FakeSession()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    async def fake_source_plan_rows(**kwargs):
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
    monkeypatch.setattr(
        source_snapshot_control,
        "_snapshot_row",
        AsyncMock(
            return_value={
                "snapshot_id": "snap_new",
                "status": "published",
                "import_month": datetime.date(2026, 6, 1),
                "manifest": {"serving_index": {"source_key": "source_a"}},
            }
        ),
    )
    monkeypatch.setattr(source_snapshot_control, "_current_source_snapshot", AsyncMock(return_value="snap_old"))
    monkeypatch.setattr(source_snapshot_control, "_source_plan_rows", fake_source_plan_rows)
    monkeypatch.setattr(source_snapshot_control.db, "transaction", lambda: FakeTransaction())
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
    assert any("ptg2_current_source_snapshot" in statement for statement, _ in executed)
    assert any("DELETE FROM \"mrf\".ptg2_current_plan_source" in statement for statement, _ in executed)
    assert any("ptg2_current_plan_source" in statement and "ON CONFLICT" in statement for statement, _ in executed)
    assert clear_calls == [True]


def test_promote_ptg2_source_snapshot_refuses_stale_expected_pointer(monkeypatch):
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

    with pytest.raises(source_snapshot_control.SourceSnapshotConflict):
        asyncio.run(
            source_snapshot_control.promote_ptg2_source_snapshot(
                source_key="source_a",
                snapshot_id="snap_new",
                expected_current_snapshot_id="snap_other",
            )
        )


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

    async def fake_status(statement, **params):
        status_calls.append((statement, params))
        return 1

    monkeypatch.setattr(
        source_snapshot_control,
        "build_ptg2_source_snapshot_remove_plan",
        AsyncMock(
            return_value={
                "snapshot_id": "snap_old",
                "source_key": "source_a",
                "exists": True,
                "removable": True,
                "tables": ["ptg2_rates_old", "ptg2_providers_old"],
                "artifact_manifest_ids": ["artifact_1"],
                "current_references": {},
            }
        ),
    )
    monkeypatch.setattr(source_snapshot_control, "_drop_ptg2_snapshot_table_names", AsyncMock(side_effect=dropped_tables.append))
    monkeypatch.setattr(source_snapshot_control.db, "status", fake_status)

    result = asyncio.run(source_snapshot_control.remove_ptg2_source_snapshot(snapshot_id="snap_old", source_key="source_a"))

    assert dropped_tables == [["ptg2_rates_old", "ptg2_providers_old"]]
    assert result["executed"] is True
    assert result["deleted_tables"] == 2
    assert result["deleted_artifact_manifests"] == 1
    assert result["deleted_snapshots"] == 1
    assert len(status_calls) == 2
    assert all(call[1]["snapshot_id"] == "snap_old" for call in status_calls)
