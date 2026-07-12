# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import datetime
import struct
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import snapshot_cleanup, source_snapshot_control
from process.ptg_parts.ptg2_manifest_artifacts import (
    PTG2_MANIFEST_MEMBERSHIP_FORMAT,
    PTG2_MANIFEST_MEMBERSHIP_INDEX_FENCE_FORMAT,
    PTG2_MANIFEST_MEMBERSHIP_INDEX_FENCE_STRIDE,
    PTG2_MANIFEST_MEMBERSHIP_MAGIC,
    PTG2_PROVIDER_MEMBERSHIP_GRAPH_ARTIFACT_NAMES,
    PTG2_PROVIDER_MEMBERSHIP_GRAPH_VERSION,
)


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
    return {
        "snapshot_id": snapshot_id,
        "status": "published",
        "import_month": datetime.date(2026, 7, 1),
        "manifest": {"serving_index": serving_index},
    }


async def _source_plan_pointer_rows(**pointer_fields):
    return [
        {
            "plan_source_key": "plan_source_a",
            "plan_id": "plan_a",
            "plan_market_type": "group",
            "import_month": pointer_fields["import_month"],
            "source_key": pointer_fields["source_key"],
            "snapshot_id": pointer_fields["snapshot_id"],
            "previous_snapshot_id": pointer_fields["previous_snapshot_id"],
            "updated_at": pointer_fields["updated_at"],
        }
    ]


def _empty_v3_graph_entry(artifact_name: str) -> dict:
    artifact_id = f"graph-{artifact_name.replace('_', '-')}"
    return {
        "name": artifact_name,
        "source_shard_id": "source-shard-a",
        "storage_uri": f"db://ptg2_artifact/{artifact_id}",
        "chunk_bytes": 1024 * 1024,
        "membership_version": 1,
        "membership_header_hex": struct.pack("<8sIQ", PTG2_MANIFEST_MEMBERSHIP_MAGIC, 1, 0).hex(),
        "record_format": PTG2_MANIFEST_MEMBERSHIP_FORMAT,
        "entry_count": 0,
        "owner_count": 0,
        "member_count": 0,
        "byte_count": 20,
        "owner_index_fence_format": PTG2_MANIFEST_MEMBERSHIP_INDEX_FENCE_FORMAT,
        "owner_index_fence_stride": PTG2_MANIFEST_MEMBERSHIP_INDEX_FENCE_STRIDE,
        "owner_index_fence_owners": [],
    }


def test_manual_rollback_preserves_displaced_snapshot(monkeypatch):
    transaction = _RecordingTransaction()
    monkeypatch.setattr(
        source_snapshot_control,
        "_snapshot_row",
        AsyncMock(return_value=_published_snapshot("snap_a", {"source_key": "source_a"})),
    )
    monkeypatch.setattr(
        source_snapshot_control,
        "_current_source_snapshot_state",
        AsyncMock(return_value=("snap_b", None)),
    )
    monkeypatch.setattr(
        source_snapshot_control,
        "_source_plan_rows",
        _source_plan_pointer_rows,
    )
    monkeypatch.setattr(source_snapshot_control.db, "transaction", lambda: transaction)
    monkeypatch.setattr(source_snapshot_control, "_clear_ptg2_snapshot_cache", lambda: None)

    promotion = asyncio.run(
        source_snapshot_control.promote_ptg2_source_snapshot(
            source_key="source_a",
            snapshot_id="snap_a",
            expected_current_snapshot_id="snap_b",
        )
    )

    source_pointer = next(
        params
        for statement, params in transaction.statements
        if "INSERT INTO \"mrf\".ptg2_current_source_snapshot" in statement
    )
    plan_pointer = next(
        params
        for statement, params in transaction.statements
        if "INSERT INTO \"mrf\".ptg2_current_plan_source" in statement
    )
    assert promotion["previous_snapshot_id"] == "snap_b"
    expected_lineage = ("snap_a", "snap_b")
    assert (source_pointer["snapshot_id"], source_pointer["previous_snapshot_id"]) == expected_lineage
    assert (plan_pointer["snapshot_id"], plan_pointer["previous_snapshot_id"]) == expected_lineage


def test_manual_promotion_rejects_cleaned_table(monkeypatch):
    transaction = _RecordingTransaction()
    current_snapshot = AsyncMock(return_value=("snap_b", None))
    monkeypatch.setattr(
        source_snapshot_control,
        "_snapshot_row",
        AsyncMock(
            return_value=_published_snapshot(
                "snap_a",
                {
                    "source_key": "source_a",
                    "materialized_tables": {"serving": "mrf.ptg2_serving_snap_a"},
                },
            )
        ),
    )
    monkeypatch.setattr(source_snapshot_control, "_current_source_snapshot_state", current_snapshot)
    monkeypatch.setattr(source_snapshot_control.db, "transaction", lambda: transaction)
    monkeypatch.setattr(snapshot_cleanup, "_table_exists", AsyncMock(return_value=False))

    with pytest.raises(ValueError, match="snapshot serving tables are missing: ptg2_serving_snap_a"):
        asyncio.run(
            source_snapshot_control.promote_ptg2_source_snapshot(
                source_key="source_a",
                snapshot_id="snap_a",
            )
        )

    current_snapshot.assert_not_awaited()
    assert len(transaction.statements) == 1
    assert "pg_advisory_xact_lock" in transaction.statements[0][0]


def test_manual_promotion_rejects_cleaned_artifact(monkeypatch):
    transaction = _RecordingTransaction()
    artifact_query = AsyncMock(return_value=[])
    monkeypatch.setattr(
        source_snapshot_control,
        "_snapshot_row",
        AsyncMock(
            return_value=_published_snapshot(
                "snap_a",
                {
                    "source_key": "source_a",
                    "artifacts": {
                        "provider_forward": {
                            "storage_uri": "db://ptg2_artifact/provider-forward-a",
                        }
                    },
                },
            )
        ),
    )
    monkeypatch.setattr(source_snapshot_control.db, "transaction", lambda: transaction)
    monkeypatch.setattr(snapshot_cleanup, "_table_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(snapshot_cleanup.db, "all", artifact_query)

    with pytest.raises(ValueError, match="snapshot serving artifacts are missing: provider-forward-a"):
        asyncio.run(
            source_snapshot_control.promote_ptg2_source_snapshot(
                source_key="source_a",
                snapshot_id="snap_a",
            )
        )

    artifact_sql = artifact_query.await_args.args[0]
    assert "ptg2_artifact_manifest" in artifact_sql
    assert "ptg2_artifact_blob_chunk" in artifact_sql


def test_postgres_v3_promotion_ignores_local_manifest(monkeypatch):
    """Promote only a complete PostgreSQL graph while ignoring stale local paths."""

    transaction = _RecordingTransaction()
    graph_entries = [
        _empty_v3_graph_entry(artifact_name)
        for artifact_name in sorted(PTG2_PROVIDER_MEMBERSHIP_GRAPH_ARTIFACT_NAMES)
    ]
    graph_artifact_ids = [
        entry["storage_uri"].rsplit("/", 1)[-1]
        for entry in graph_entries
    ]
    artifact_query = AsyncMock(
        return_value=[{"artifact_id": artifact_id} for artifact_id in graph_artifact_ids]
    )
    serving_index = {
        "source_key": "source_a",
        "arch_version": "postgres_binary_v3",
        "provider_membership_graph": {
            "artifact_version": PTG2_PROVIDER_MEMBERSHIP_GRAPH_VERSION,
            "artifact_names": sorted(PTG2_PROVIDER_MEMBERSHIP_GRAPH_ARTIFACT_NAMES),
            "storage": "postgresql_chunks_v1",
        },
        "materialized_tables": {"serving_binary": "mrf.ptg2_serving_binary_snap_a"},
        "artifacts": {
            "manifest_uri": "file:///removed-pod-build/snapshot.manifest.json",
            "build_report": {"path": "/removed-pod-build/publish-report.json"},
            "sidecars": graph_entries,
        },
    }
    monkeypatch.setattr(
        source_snapshot_control,
        "_snapshot_row",
        AsyncMock(return_value=_published_snapshot("snap_a", serving_index)),
    )
    monkeypatch.setattr(
        source_snapshot_control,
        "_current_source_snapshot_state",
        AsyncMock(return_value=("snap_b", None)),
    )
    monkeypatch.setattr(
        source_snapshot_control,
        "_source_plan_rows",
        _source_plan_pointer_rows,
    )
    monkeypatch.setattr(source_snapshot_control.db, "transaction", lambda: transaction)
    monkeypatch.setattr(source_snapshot_control, "_clear_ptg2_snapshot_cache", lambda: None)
    monkeypatch.setattr(snapshot_cleanup, "_table_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(snapshot_cleanup.db, "all", artifact_query)

    promotion = asyncio.run(
        source_snapshot_control.promote_ptg2_source_snapshot(
            source_key="source_a",
            snapshot_id="snap_a",
            expected_current_snapshot_id="snap_b",
        )
    )

    assert promotion["snapshot_id"] == "snap_a"
    assert artifact_query.await_args.kwargs["artifact_ids"] == sorted(graph_artifact_ids)


def test_v3_graph_contract_rejects_duplicate_declared_names():
    canonical_names = sorted(PTG2_PROVIDER_MEMBERSHIP_GRAPH_ARTIFACT_NAMES)
    serving_index = {
        "arch_version": "postgres_binary_v3",
        "provider_membership_graph": {
            "artifact_version": PTG2_PROVIDER_MEMBERSHIP_GRAPH_VERSION,
            "artifact_names": [*canonical_names, canonical_names[0]],
            "storage": "postgresql_chunks_v1",
        },
        "artifacts": {
            "sidecars": [_empty_v3_graph_entry(artifact_name) for artifact_name in canonical_names],
        },
    }

    assert snapshot_cleanup.v3_graph_contract_errors(serving_index) == [
        PTG2_PROVIDER_MEMBERSHIP_GRAPH_VERSION
    ]


def test_v3_graph_contract_rejects_obsolete_direct_provider_membership():
    canonical_names = sorted(PTG2_PROVIDER_MEMBERSHIP_GRAPH_ARTIFACT_NAMES)
    serving_index = {
        "arch_version": "postgres_binary_v3",
        "provider_membership_graph": {
            "artifact_version": PTG2_PROVIDER_MEMBERSHIP_GRAPH_VERSION,
            "artifact_names": canonical_names,
            "storage": "postgresql_chunks_v1",
        },
        "artifacts": {
            "sidecars": [
                *[_empty_v3_graph_entry(artifact_name) for artifact_name in canonical_names],
                {"name": "provider_npi", "storage_uri": "db://ptg2_artifact/obsolete-provider-npi"},
            ],
        },
    }

    assert snapshot_cleanup.v3_graph_contract_errors(serving_index) == ["obsolete:provider_npi"]


@pytest.mark.parametrize("snapshot_status", ["pending", "running", "building", "validated"])
def test_manual_removal_rejects_in_flight_snapshot(monkeypatch, snapshot_status):
    monkeypatch.setattr(
        source_snapshot_control,
        "_snapshot_row",
        AsyncMock(
            return_value={
                "snapshot_id": "snap_retry",
                "status": snapshot_status,
                "manifest": {"serving_index": {"source_key": "source_a"}},
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
        "_all_snapshot_manifest_rows",
        AsyncMock(return_value=[]),
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
        "_all_snapshot_manifest_rows",
        AsyncMock(return_value=[]),
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
                "manifest": {"serving_index": {"source_key": "source_a"}},
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
