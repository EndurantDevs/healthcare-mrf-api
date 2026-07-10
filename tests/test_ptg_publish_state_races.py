import asyncio
import datetime
import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import source_pointers


process_ptg = importlib.import_module("process.ptg")


SERVING_INDEX = {
    "storage": "manifest_snapshot",
    "table": "mrf.ptg2_manifest_serving_candidate",
    "price_atom_table": "mrf.ptg2_price_atom_candidate",
    "rate_count": 7,
    "serving_rates": 7,
}


class _NoCurrentSnapshotQuery:
    def where(self, *_args, **_kwargs):
        return self

    async def first(self):
        return None


class _Result:
    def __init__(self, first_entry):
        self.first_entry = first_entry

    def first(self):
        return self.first_entry


class _Transaction:
    def __init__(self, session):
        self.session = session
        self.entered = 0

    async def __aenter__(self):
        self.entered += 1
        return self.session

    async def __aexit__(self, exc_type, exc, tb):
        return False


def _install_successful_import_path(monkeypatch, *, push, publish_pointer):
    """Install a deterministic source-publish path and return its cleanup spies."""
    async def downloaded_jobs(jobs, **_kwargs):
        for job in jobs:
            yield process_ptg.PTG2DownloadedJob(
                job=job,
                raw_artifact=SimpleNamespace(raw_sha256=str(job["url"])),
                logical_artifact=SimpleNamespace(logical_path="/tmp/rates.json.gz"),
            )

    async def process_file(*_args, **_kwargs):
        return process_ptg.PTG2FileProcessResult(
            "in_network",
            "https://example.test/rates.json.gz",
            True,
            summary={"serving_rates": 7},
        )

    final_table_cleanup = AsyncMock()
    artifact_cleanup = AsyncMock()
    monkeypatch.setenv(process_ptg.PTG2_MANIFEST_PRECOPY_MERGE_ENV, "false")
    monkeypatch.setattr(process_ptg, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_ptg, "ensure_ptg2_tables", AsyncMock())
    monkeypatch.setattr(process_ptg, "_current_source_snapshot_id", AsyncMock(return_value="snap_previous"))
    monkeypatch.setattr(process_ptg.db, "status", AsyncMock())
    monkeypatch.setattr(process_ptg.db, "select", lambda *_args: _NoCurrentSnapshotQuery())
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", push)
    monkeypatch.setattr(process_ptg, "_prepare_ptg_tables", AsyncMock(return_value={"ImportLog": "log"}))
    monkeypatch.setattr(
        process_ptg, "_create_ptg2_manifest_serving_stage_table", AsyncMock(return_value="manifest_stage")
    )
    monkeypatch.setattr(process_ptg, "_iter_downloaded_ptg_jobs", downloaded_jobs)
    monkeypatch.setattr(process_ptg, "_process_in_network_file", process_file)
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())
    monkeypatch.setattr(
        process_ptg,
        "_publish_ptg2_manifest_serving_snapshot",
        AsyncMock(return_value=dict(SERVING_INDEX)),
    )
    monkeypatch.setattr(
        process_ptg,
        "_current_source_snapshot_id",
        AsyncMock(return_value="snap_previous"),
    )
    monkeypatch.setattr(process_ptg, "_publish_ptg2_source_pointers", publish_pointer)
    monkeypatch.setattr(process_ptg, "_drop_ptg2_snapshot_tables_for_manifest", final_table_cleanup)
    monkeypatch.setattr(process_ptg, "delete_ptg2_artifacts_for_snapshot", artifact_cleanup)
    monkeypatch.setattr(process_ptg, "_drop_ptg2_snapshot_table_names", AsyncMock())
    monkeypatch.setattr(process_ptg, "_cleanup_old_ptg2_source_tables", AsyncMock())
    monkeypatch.setattr(
        process_ptg,
        "_enqueue_ptg2_auto_address_refresh_after_import",
        AsyncMock(return_value={"status": "skipped"}),
    )
    return final_table_cleanup, artifact_cleanup


def _run_source_import():
    return asyncio.run(
        process_ptg.main(
            in_network_url="https://example.test/rates.json.gz",
            import_month="2026-07",
            import_id="publish_race_candidate",
            source_key="source_a",
        )
    )


def test_deterministic_rerun_returns_already_published_without_table_work(monkeypatch):
    calls = []
    create_stage = AsyncMock()
    cleanup = AsyncMock()
    publish_pointers = AsyncMock(
        return_value={"status": "promoted", "global_pointer": "reconciled"}
    )

    async def push(object_entries, cls, **_kwargs):
        calls.append((cls, object_entries[0]))
        assert cls is process_ptg.PTG2Snapshot
        return {
            **object_entries[0],
            "status": process_ptg.PTG2_STATUS_PUBLISHED,
            "previous_snapshot_id": "snap_previous",
            "manifest": {
                "serving_index": dict(SERVING_INDEX),
                "serving_rates": 7,
            },
        }

    monkeypatch.setattr(process_ptg, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_ptg, "ensure_ptg2_tables", AsyncMock())
    monkeypatch.setattr(process_ptg, "_current_source_snapshot_id", AsyncMock(return_value="snap_previous"))
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", push)
    monkeypatch.setattr(process_ptg, "_publish_ptg2_source_pointers", publish_pointers)
    monkeypatch.setattr(process_ptg, "_create_ptg2_manifest_serving_stage_table", create_stage)
    monkeypatch.setattr(process_ptg, "_drop_ptg2_snapshot_tables_for_manifest", cleanup)

    rerun_result = _run_source_import()

    assert rerun_result["status"] == "succeeded"
    assert rerun_result["publish_status"] == "already_published"
    assert rerun_result["already_published"] is True
    assert rerun_result["serving_rates"] == 7
    assert rerun_result["pointer_reconciliation"]["global_pointer"] == "reconciled"
    assert len(calls) == 1
    assert calls[0][1]["status"] == process_ptg.PTG2_STATUS_BUILDING
    create_stage.assert_not_awaited()
    cleanup.assert_not_awaited()
    assert publish_pointers.await_args.kwargs["previous_snapshot_id"] == "snap_previous"
    assert publish_pointers.await_args.kwargs["snapshot_attributes"]["status"] == "published"


def test_building_snapshot_collision_fails_closed(monkeypatch):
    calls = []
    create_stage = AsyncMock()
    cleanup = AsyncMock()

    async def push(object_entries, cls, **_kwargs):
        calls.append((cls, object_entries[0]))
        assert cls is process_ptg.PTG2Snapshot
        return {
            **object_entries[0],
            "status": process_ptg.PTG2_STATUS_BUILDING,
            "snapshot_claim_status": "existing",
        }

    monkeypatch.setattr(process_ptg, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_ptg, "ensure_ptg2_tables", AsyncMock())
    monkeypatch.setattr(process_ptg, "_current_source_snapshot_id", AsyncMock(return_value="snap_previous"))
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", push)
    monkeypatch.setattr(process_ptg, "_create_ptg2_manifest_serving_stage_table", create_stage)
    monkeypatch.setattr(process_ptg, "_drop_ptg2_snapshot_tables_for_manifest", cleanup)

    with pytest.raises(
        process_ptg.PTG2SnapshotInProgressConflict,
        match="already being built",
    ):
        _run_source_import()

    assert len(calls) == 1
    create_stage.assert_not_awaited()
    cleanup.assert_not_awaited()


def test_source_pointer_read_failure_leaves_no_building_claim(monkeypatch):
    push = AsyncMock()
    monkeypatch.setattr(process_ptg, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_ptg, "ensure_ptg2_tables", AsyncMock())
    monkeypatch.setattr(
        process_ptg,
        "_current_source_snapshot_id",
        AsyncMock(side_effect=RuntimeError("database unavailable")),
    )
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", push)

    with pytest.raises(RuntimeError, match="database unavailable"):
        _run_source_import()

    push.assert_not_awaited()


def test_building_snapshot_claim_only_reclaims_failed_candidate(monkeypatch):
    conflict_options_by_name = {}

    class ExcludedValues:
        def __getattr__(self, name):
            return f"excluded.{name}"

    class InsertStatement:
        excluded = ExcludedValues()

        def values(self, _row):
            return self

        def on_conflict_do_update(self, **kwargs):
            conflict_options_by_name.update(kwargs)
            return self

        def returning(self, *_columns):
            return self

        async def first(self):
            return None

    class ExistingSnapshotQuery:
        def where(self, *_args):
            return self

        async def first(self):
            return {
                "snapshot_id": "snap_building",
                "status": process_ptg.PTG2_STATUS_BUILDING,
                "manifest": {},
            }

    monkeypatch.setattr(process_ptg.db, "insert", lambda *_args: InsertStatement())
    monkeypatch.setattr(process_ptg.db, "select", lambda *_args: ExistingSnapshotQuery())

    snapshot_state = asyncio.run(
        process_ptg._push_ptg2_snapshot_preserving_publication(
            {
                "snapshot_id": "snap_building",
                "status": process_ptg.PTG2_STATUS_BUILDING,
            }
        )
    )

    assert conflict_options_by_name["where"].right.value == process_ptg.PTG2_STATUS_FAILED
    assert snapshot_state["snapshot_claim_status"] == "existing"


def test_source_pointer_compare_and_swap_rejects_stale_candidate(monkeypatch):
    executed_statements = []

    class ConflictSession:
        async def execute(self, statement, params=None):
            executed_statements.append((str(statement), params or {}))
            return _Result(None)

    transaction = _Transaction(ConflictSession())
    monkeypatch.setattr(source_pointers, "_source_plan_rows", AsyncMock(return_value=[]))
    monkeypatch.setattr(source_pointers.db, "transaction", lambda: transaction)

    with pytest.raises(source_pointers.PTG2SourcePointerConflict, match="changed after import planning"):
        asyncio.run(
            source_pointers._publish_ptg2_source_pointers(
                source_key="source_a",
                snapshot_id="snap_stale",
                previous_snapshot_id="snap_observed",
                import_month=datetime.date(2026, 7, 1),
                updated_at=datetime.datetime(2026, 7, 10),
                serving_index=dict(SERVING_INDEX),
            )
        )

    assert transaction.entered == 1
    assert len(executed_statements) == 1
    assert "IS NOT DISTINCT FROM :previous_snapshot_id" in executed_statements[0][0]
    assert "DELETE FROM" not in executed_statements[0][0]


def test_pre_pointer_failure_drops_final_tables_and_marks_candidate_failed(monkeypatch):
    pushed_entries = []

    async def push(object_entries, cls, **_kwargs):
        pushed_entries.extend((cls, entry) for entry in object_entries)
        if (
            cls is process_ptg.PTG2Snapshot
            and object_entries[0]["status"] == process_ptg.PTG2_STATUS_BUILDING
        ):
            return {
                **object_entries[0],
                "snapshot_claim_status": "acquired",
                "observed_source_snapshot_id": "snap_previous",
            }
        return None

    async def reject_pointer(**_kwargs):
        raise source_pointers.PTG2SourcePointerConflict("stale candidate")

    cleanup, artifact_cleanup = _install_successful_import_path(
        monkeypatch,
        push=push,
        publish_pointer=reject_pointer,
    )

    with pytest.raises(source_pointers.PTG2SourcePointerConflict, match="stale candidate"):
        _run_source_import()

    cleanup.assert_awaited_once_with(SERVING_INDEX)
    artifact_cleanup.assert_awaited_once()
    snapshot_entries = [
        entry for cls, entry in pushed_entries if cls is process_ptg.PTG2Snapshot
    ]
    assert [entry["status"] for entry in snapshot_entries] == [
        process_ptg.PTG2_STATUS_BUILDING,
        process_ptg.PTG2_STATUS_FAILED,
    ]
    assert snapshot_entries[-1]["manifest"]["serving_index"] == SERVING_INDEX


def test_mid_publisher_failure_enumerates_and_cleans_possible_final_tables(monkeypatch):
    pushed_entries = []

    async def push(object_entries, cls, **_kwargs):
        pushed_entries.extend((cls, entry) for entry in object_entries)
        if (
            cls is process_ptg.PTG2Snapshot
            and object_entries[0]["status"] == process_ptg.PTG2_STATUS_BUILDING
        ):
            return {**object_entries[0], "snapshot_claim_status": "acquired"}
        return None

    cleanup, artifact_cleanup = _install_successful_import_path(
        monkeypatch,
        push=push,
        publish_pointer=AsyncMock(),
    )
    monkeypatch.setattr(
        process_ptg,
        "_publish_ptg2_manifest_serving_snapshot",
        AsyncMock(side_effect=RuntimeError("publisher failed after rename")),
    )

    with pytest.raises(RuntimeError, match="publisher failed after rename"):
        _run_source_import()

    candidate_index = cleanup.await_args.args[0]
    artifact_cleanup.assert_awaited_once()
    assert candidate_index["type"] == "ptg2_serving_candidate"
    assert set(process_ptg._snapshot_manifest_table_names(candidate_index)) == {
        table_name.split(".", 1)[-1]
        for table_name in candidate_index["materialized_tables"].values()
    }
    snapshot_entries = [
        entry for cls, entry in pushed_entries if cls is process_ptg.PTG2Snapshot
    ]
    assert snapshot_entries[-1]["status"] == process_ptg.PTG2_STATUS_FAILED
    assert snapshot_entries[-1]["manifest"]["serving_index"] == candidate_index


def test_pointer_recheck_failure_preserves_tables_but_marks_candidate_failed(monkeypatch):
    pushed_entries = []

    async def push(object_entries, cls, **_kwargs):
        pushed_entries.extend((cls, entry) for entry in object_entries)
        if (
            cls is process_ptg.PTG2Snapshot
            and object_entries[0]["status"] == process_ptg.PTG2_STATUS_BUILDING
        ):
            return {
                **object_entries[0],
                "snapshot_claim_status": "acquired",
                "observed_source_snapshot_id": "snap_previous",
            }
        return None

    async def reject_pointer(**_kwargs):
        raise source_pointers.PTG2SourcePointerConflict("stale candidate")

    cleanup, artifact_cleanup = _install_successful_import_path(
        monkeypatch,
        push=push,
        publish_pointer=reject_pointer,
    )
    monkeypatch.setattr(
        process_ptg,
        "_current_source_snapshot_id",
        AsyncMock(side_effect=["snap_previous", RuntimeError("database unavailable")]),
    )

    with pytest.raises(source_pointers.PTG2SourcePointerConflict, match="stale candidate"):
        _run_source_import()

    cleanup.assert_not_awaited()
    artifact_cleanup.assert_not_awaited()
    snapshot_entries = [
        entry for cls, entry in pushed_entries if cls is process_ptg.PTG2Snapshot
    ]
    assert [entry["status"] for entry in snapshot_entries] == [
        process_ptg.PTG2_STATUS_BUILDING,
        process_ptg.PTG2_STATUS_FAILED,
    ]


def test_post_pointer_failure_preserves_published_snapshot_and_tables(monkeypatch):
    pushed_entries = []
    promoted_snapshots = []

    async def push(object_entries, cls, **_kwargs):
        pushed_entries.extend((cls, entry) for entry in object_entries)
        if (
            cls is process_ptg.PTG2Snapshot
            and object_entries[0]["status"] == process_ptg.PTG2_STATUS_BUILDING
        ):
            return {
                **object_entries[0],
                "snapshot_claim_status": "acquired",
                "observed_source_snapshot_id": "snap_previous",
            }
        return None

    async def publish_pointer(**kwargs):
        promoted_snapshots.append(kwargs["snapshot_attributes"])
        return {"status": "promoted"}

    cleanup, artifact_cleanup = _install_successful_import_path(
        monkeypatch,
        push=push,
        publish_pointer=publish_pointer,
    )
    monkeypatch.setattr(
        process_ptg,
        "_cleanup_old_ptg2_source_tables",
        AsyncMock(side_effect=RuntimeError("post-publish cleanup failed")),
    )

    with pytest.raises(RuntimeError, match="post-publish cleanup failed"):
        _run_source_import()

    cleanup.assert_not_awaited()
    artifact_cleanup.assert_not_awaited()
    assert promoted_snapshots[0]["status"] == process_ptg.PTG2_STATUS_PUBLISHED
    snapshot_entries = [
        entry for cls, entry in pushed_entries if cls is process_ptg.PTG2Snapshot
    ]
    assert [entry["status"] for entry in snapshot_entries] == [
        process_ptg.PTG2_STATUS_BUILDING,
        process_ptg.PTG2_STATUS_PUBLISHED,
    ]
    import_entries = [
        entry for cls, entry in pushed_entries if cls is process_ptg.PTG2ImportRun
    ]
    assert import_entries[-1]["status"] == process_ptg.PTG2_STATUS_FAILED
