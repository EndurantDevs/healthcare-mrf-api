# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed edge coverage for PTG snapshot cleanup control paths."""

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_source_snapshot_gc as snapshot_gc
from process.ptg_parts import snapshot_cleanup
from process.ptg_parts import source_snapshot_control
from process.ptg_parts import source_snapshot_control_results


def test_strict_layout_validators_report_every_corrupt_resource_dimension() -> None:
    """A corrupt persisted layout must report every unsafe identity dimension."""

    assert snapshot_cleanup._audit_sample_context(
        {"sample_count": object(), "format_version": 2, "maximum_rows": 2560}
    ) == (-1, "", False)
    assert set(snapshot_cleanup._validate_resource_identity({})) == {
        "layout_state",
        "layout_generation",
        "mapping_digest",
        "support_digest",
        "snapshot_blocks",
        "snapshot_scope",
        "coverage_scope_binding",
        "logical_plan_scope",
    }
    assert snapshot_cleanup._validate_code_scope(
        {
            "code_count": 0,
            "code_scope_count": 1,
            "matching_code_scope_count": 0,
        }
    ) == (0, ["coverage_scope_code"])
    assert snapshot_cleanup._validate_layout_manifest(
        {"layout_manifest": "{"},
        {},
    ) == ["layout_manifest"]


def test_dense_resource_validation_reports_each_missing_serving_relation() -> None:
    """Positive manifest counts require every corresponding dense relation."""

    errors = snapshot_cleanup._validate_dense_resources(
        {},
        {
            "serving_rates": 1,
            "provider_graph": {
                "owner_count": 1,
                "provider_group_count": 1,
                "npi_count": 1,
            },
        },
        0,
    )
    assert set(errors) == {
        "dense:graph_owner",
        "dense:provider_group",
        "dense:provider_set",
        "dense:price_attr",
        "dense:npi_scope",
        "dense:code",
    }


def test_snapshot_artifact_policy_keeps_database_ids_and_filters_local_paths() -> None:
    """PostgreSQL layouts retain DB identities without trusting local paths."""

    assert snapshot_cleanup._required_snapshot_table_names(
        {"table": "mrf.ptg2_serving_rate_compact_retained"}
    ) == ["ptg2_serving_rate_compact_retained"]
    database_ids, local_paths = snapshot_cleanup._snapshot_artifact_references(
        {
            "artifact_uri": "db://ptg2_artifact/direct",
            "storage_uri": "relative.bin",
            "artifacts": {
                "primary": {"path": "primary.bin"},
                "sidecars": [
                    {"storage_uri": "db://ptg2_artifact/sidecar"},
                    {"path": "sidecar.bin"},
                ],
            },
        }
    )
    assert database_ids == {"direct", "sidecar"}
    assert local_paths == {"relative.bin", "primary.bin", "sidecar.bin"}
    shared_ids, shared_paths = snapshot_cleanup._snapshot_artifact_references(
        {
            "arch_version": "postgres_binary_v3",
            "storage_generation": "shared_blocks_v3",
            "artifact_uri": "db://ptg2_artifact/shared",
            "storage_uri": "ignored.bin",
        }
    )
    assert shared_ids == {"shared"}
    assert shared_paths == set()


def test_snapshot_ownership_and_lineage_helpers_fail_closed_on_sparse_rows() -> None:
    """Sparse manifests cannot make shared tables or absent lineage removable."""

    assert snapshot_cleanup._source_snapshot_keep_ids(
        [{"snapshot_id": "current", "previous_snapshot_id": None}],
        {"current"},
    ) == {"current"}
    assert snapshot_cleanup._snapshot_serving_index_dict(
        SimpleNamespace(_mapping={"manifest": "{"})
    ) == {}
    assert snapshot_cleanup._snapshot_serving_index_dict(
        {"manifest": ["not", "a", "mapping"]}
    ) == {}
    assert snapshot_cleanup._exclusively_owned_snapshot_table_names(
        "candidate",
        ["ptg2_serving_rate_compact_candidate"],
        [],
    ) == ["ptg2_serving_rate_compact_candidate"]


@pytest.mark.parametrize(
    (
        "layout_keys",
        "release",
        "expected_layout_cleanup",
        "expected_physical_cleanup",
    ),
    [
        ((), None, "not_applicable", "not_applicable"),
        ((11,), None, "retained_shared", "deferred"),
        (
            (11,),
            SimpleNamespace(
                logical_layout_count=1,
                candidate_hash_count=2,
                stored_bytes=4096,
            ),
            "released",
            "pending_sweep",
        ),
        (
            (11,),
            SimpleNamespace(
                logical_layout_count=1,
                candidate_hash_count=0,
                stored_bytes=0,
            ),
            "released",
            "not_applicable",
        ),
    ],
)
def test_snapshot_removal_separates_layout_and_block_cleanup_states(
    layout_keys,
    release,
    expected_layout_cleanup,
    expected_physical_cleanup,
) -> None:
    """A released layout is not reported as physically swept."""

    result = source_snapshot_control_results.executed_snapshot_remove_plan(
        plan={"snapshot_id": "snapshot-a"},
        deletion_counts={"deleted_snapshots": 1},
        layout_keys=layout_keys,
        shared_layout_release=release,
    )

    assert result["layout_cleanup"] == expected_layout_cleanup
    assert result["physical_cleanup"] == expected_physical_cleanup


@pytest.mark.asyncio
async def test_snapshot_table_drop_guards_require_complete_attempt_identity(
    monkeypatch,
) -> None:
    """Attempt-stage cleanup requires both identifiers before any deletion."""

    await snapshot_cleanup._drop_ptg2_snapshot_table_names([])
    with pytest.raises(ValueError, match="requires snapshot and run identifiers"):
        await snapshot_cleanup._drop_ptg2_snapshot_table_names(
            ["ptg2_serving_rate_compact_candidate"],
            executor=AsyncMock(),
            snapshot_id="candidate",
        )
    drop = AsyncMock()
    await snapshot_cleanup._drop_ptg2_snapshot_table_names(
        ["ptg2_serving_rate_compact_candidate"],
        executor=SimpleNamespace(status=drop),
    )
    drop.assert_awaited_once()
    strict_drop = AsyncMock()
    monkeypatch.setattr(
        snapshot_cleanup,
        "_drop_ptg2_snapshot_table_names",
        strict_drop,
    )
    await snapshot_cleanup._drop_ptg2_snapshot_tables_for_manifest(
        {
            "arch_version": "postgres_binary_v3",
            "storage_generation": "shared_blocks_v3",
        }
    )
    strict_drop.assert_awaited_once_with([])


class _TransactionSession:
    def __init__(self) -> None:
        self.execute = AsyncMock(
            side_effect=[
                SimpleNamespace(all=lambda: [{"value": 1}]),
                SimpleNamespace(rowcount=3),
            ]
        )


@pytest.mark.asyncio
async def test_source_control_executor_and_input_guards_preserve_fail_closed_paths(
    monkeypatch,
) -> None:
    """Control helpers normalize SQL results and reject missing identities."""

    session = _TransactionSession()
    executor = source_snapshot_control._TransactionExecutor(session)
    assert await executor.all("SELECT 1") == [{"value": 1}]
    assert await executor.status(object()) == 3
    assert source_snapshot_control._row_mapping(None) == {}
    with pytest.raises(ValueError, match="source_key and snapshot_id"):
        await source_snapshot_control.promote_ptg2_source_snapshot(
            source_key="",
            snapshot_id="candidate",
        )
    with pytest.raises(ValueError, match="snapshot_id is required"):
        await source_snapshot_control.build_source_snapshot_remove_plan(
            snapshot_id=" "
        )
    monkeypatch.setattr(
        source_snapshot_control,
        "_snapshot_row",
        AsyncMock(return_value={}),
    )
    missing = await source_snapshot_control.build_source_snapshot_remove_plan(
        snapshot_id="missing",
        source_key="source-a",
    )
    assert missing["exists"] is False


@asynccontextmanager
async def _transaction():
    yield object()


@pytest.mark.asyncio
async def test_source_removal_rechecks_plan_under_lock_before_deleting(
    monkeypatch,
) -> None:
    """Removal rejects stale and absent plans while holding the pointer lock."""

    monkeypatch.setattr(source_snapshot_control.db, "transaction", _transaction)
    monkeypatch.setattr(
        source_snapshot_control,
        "_lock_source_pointer_gc",
        AsyncMock(return_value=None),
    )
    with pytest.raises(ValueError, match="snapshot_id is required"):
        await source_snapshot_control.remove_ptg2_source_snapshot(snapshot_id="")
    plan = AsyncMock(
        side_effect=[
            {"removable": False, "reason": "still referenced"},
            {"removable": True, "exists": False},
        ]
    )
    monkeypatch.setattr(
        source_snapshot_control,
        "build_source_snapshot_remove_plan",
        plan,
    )
    with pytest.raises(ValueError, match="still referenced"):
        await source_snapshot_control.remove_ptg2_source_snapshot(
            snapshot_id="candidate"
        )
    removal_result = await source_snapshot_control.remove_ptg2_source_snapshot(
        snapshot_id="missing"
    )
    assert removal_result["executed"] is True
    assert removal_result["deleted_snapshots"] == 0


@pytest.mark.asyncio
async def test_source_retirement_rejects_previous_pointer_references(
    monkeypatch,
) -> None:
    """Previous-pointer reachability blocks retirement before pointer mutation."""

    with pytest.raises(ValueError, match="snapshot_id is required"):
        await source_snapshot_control.retire_ptg2_source_snapshot(snapshot_id="")
    monkeypatch.setattr(source_snapshot_control.db, "transaction", _transaction)
    monkeypatch.setattr(
        source_snapshot_control,
        "_lock_source_pointer_gc",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        source_snapshot_control,
        "_snapshot_row",
        AsyncMock(return_value={"snapshot_id": "candidate"}),
    )
    monkeypatch.setattr(
        source_snapshot_control,
        "retirement_manifest_source_key",
        lambda _snapshot, _source_key: "source-a",
    )
    monkeypatch.setattr(
        source_snapshot_control,
        "validate_retirement_shared_layout",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        source_snapshot_control,
        "_current_references",
        AsyncMock(return_value={"previous_source_keys": ["source-a"]}),
    )
    with pytest.raises(ValueError, match="previous snapshot pointer"):
        await source_snapshot_control.retire_ptg2_source_snapshot(
            snapshot_id="candidate",
            source_key="source-a",
        )


@pytest.mark.asyncio
async def test_source_state_maps_absent_and_partial_pointer_rows(monkeypatch) -> None:
    """Pointer state preserves the distinction between absent and partial rows."""

    monkeypatch.setattr(
        source_snapshot_control.db,
        "first",
        AsyncMock(side_effect=[None, (None, "previous")]),
    )
    assert await source_snapshot_control._current_source_snapshot_state(
        "mrf",
        "source-a",
    ) == (None, None)
    assert await source_snapshot_control._current_source_snapshot_state(
        "mrf",
        "source-a",
    ) == (None, "previous")


def test_source_gc_parsers_and_lineage_reject_malformed_metadata() -> None:
    """Malformed JSON and invalid age settings cannot broaden GC selection."""

    assert snapshot_gc._row_mapping(None) == {}
    assert (
        snapshot_gc._stale_build_seconds(object())
        == snapshot_gc._STALE_BUILD_SECONDS_DEFAULT
    )
    assert snapshot_gc._manifest_dict("{") == {}
    assert snapshot_gc._manifest_dict("[]") == {}
    assert snapshot_gc._protected_snapshot_lineage_ids(
        [
            {"snapshot_id": "current", "previous_snapshot_id": "previous"},
            {"snapshot_id": "previous", "previous_snapshot_id": None},
        ],
        ("current",),
        4,
    ) == {"current", "previous"}
    assert snapshot_gc.PTG2SourceSnapshotGCPlan(
        current_snapshot_ids=(),
        candidate_snapshot_ids=(),
        tables=(),
    ).has_actions is False


def _gc_candidate(
    snapshot_id: str,
    *table_names: str,
) -> snapshot_gc._PTG2SnapshotGCCandidate:
    return snapshot_gc._PTG2SnapshotGCCandidate(
        snapshot_id=snapshot_id,
        source_key="source-a",
        table_names=tuple(table_names),
        is_shared=False,
        reason="terminal",
    )


@pytest.mark.asyncio
async def test_source_gc_context_sizes_only_exclusive_candidate_tables() -> None:
    """GC sizes tables only when the selected candidate owns the final reference."""

    executor = SimpleNamespace(
        all=AsyncMock(return_value=[{"table_name": "exclusive", "bytes": 9}])
    )
    context = await snapshot_gc._build_snapshot_gc_context(
        schema_name="mrf",
        executor=executor,
        current_snapshot_ids=(),
        candidates=[
            _gc_candidate("first", "shared", "missing", "retained"),
            _gc_candidate("second", "shared", "exclusive"),
        ],
        retained_table_refs={"retained"},
        snapshot_limit=2,
    )
    assert context.size_by_table == {"exclusive": 9}
    assert snapshot_gc._selected_gc_tables(context, 1) == ()
    selected = snapshot_gc._selected_gc_tables(context, 2)
    assert [(table.snapshot_id, table.table_name) for table in selected] == [
        ("second", "exclusive")
    ]


@pytest.mark.asyncio
async def test_source_gc_bounding_and_execution_honor_zero_limits() -> None:
    """Zero bounds select no oversized table and empty plans perform no metadata DML."""

    context = snapshot_gc._SnapshotGCPlanContext(
        schema_name="mrf",
        executor=AsyncMock(),
        current_snapshot_ids=(),
        candidates=(_gc_candidate("candidate", "large"),),
        retained_table_refs=frozenset(),
        last_candidate_index_by_table={"large": 0},
        size_by_table={"large": 100},
    )
    plan = await snapshot_gc._select_bounded_gc_plan(
        context,
        snapshot_limit=1,
        table_limit=None,
        byte_limit=0,
    )
    assert plan.candidate_snapshot_ids == ()
    oversized = snapshot_gc.PTG2SourceSnapshotGCPlan(
        current_snapshot_ids=(),
        candidate_snapshot_ids=("a", "b"),
        tables=(
            snapshot_gc.PTG2SnapshotGCTable("a", "source-a", "table-a", 1),
        ),
    )
    with pytest.raises(RuntimeError, match="snapshot count"):
        snapshot_gc.validate_ptg2_source_snapshot_gc_plan(
            oversized, max_snapshots=1, max_tables=2, max_bytes=2
        )
    with pytest.raises(RuntimeError, match="table count"):
        snapshot_gc.validate_ptg2_source_snapshot_gc_plan(
            oversized, max_snapshots=2, max_tables=0, max_bytes=2
        )


@pytest.mark.asyncio
async def test_source_gc_empty_candidate_plan_drops_only_selected_tables() -> None:
    """Table-only plans stop before candidate metadata and layout release."""

    connection = SimpleNamespace(status=AsyncMock(return_value=1))
    plan = snapshot_gc.PTG2SourceSnapshotGCPlan(
        current_snapshot_ids=(),
        candidate_snapshot_ids=(),
        tables=(
            snapshot_gc.PTG2SnapshotGCTable(
                "candidate",
                "source-a",
                "ptg2_serving_rate_compact_candidate",
                1,
            ),
        ),
    )
    await snapshot_gc._execute_snapshot_gc_actions(connection, "mrf", plan)
    connection.status.assert_awaited_once()
