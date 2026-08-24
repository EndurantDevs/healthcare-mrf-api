# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

from db.connection import Database
from process.ptg_parts import ptg2_shared_gc as shared_gc
from process.ptg_parts import ptg2_source_snapshot_gc as source_snapshot_gc
from process.ptg_parts import snapshot_cleanup

from tests.ptg2_shared_gc_test_support import (
    _SharedGCExecutor,
    _SourceGCProjectionExecutor,
    _hash,
    _patch_v4_abandonment_pipeline,
)

def test_shared_schema_requires_all_migration_owned_lifecycle_tables():
    assert "ptg2_v3_snapshot_scope" in shared_gc._SHARED_TABLE_NAMES
    assert "ptg2_v3_snapshot_source" in shared_gc._SHARED_TABLE_NAMES
    assert (
        "ptg2_v3_candidate_audit_attestation"
        in shared_gc.PTG2_V3_MIGRATION_OWNED_TABLE_NAMES
    )
    assert (
        shared_gc._SHARED_TABLE_NAMES
        == shared_gc.PTG2_V3_MIGRATION_OWNED_TABLE_NAMES
    )
    assert "ptg2_v3_snapshot_scope" in snapshot_cleanup._STRICT_V3_SHARED_TABLE_NAMES
    assert "ptg2_v3_audit_occurrence" in shared_gc._SHARED_TABLE_NAMES
    assert "ptg2_v3_audit_occurrence" in shared_gc.PTG2_V3_DENSE_LAYOUT_TABLES
    assert (
        "ptg2_v3_audit_occurrence"
        in snapshot_cleanup._STRICT_V3_SHARED_TABLE_NAMES
    )
    assert shared_gc.PTG2_PROVIDER_TAX_IDENTITY_TABLE_NAMES == (
        "ptg2_provider_tax_identity_legacy_layout",
        "ptg2_provider_tax_identity_manifest",
        "ptg2_provider_tax_identity",
        "ptg2_provider_group_tax_identity",
        "ptg2_provider_tax_identity_source_manifest",
        "ptg2_provider_tax_identity_source_binding",
        "ptg2_provider_group_tax_identity_source",
    )
    assert shared_gc.PTG2_PROVIDER_TAX_IDENTITY_TABLE_NAMES == (
        shared_gc.PTG2_PROVIDER_TAX_IDENTITY_BASE_TABLE_NAMES
        + shared_gc.PTG2_PROVIDER_TAX_IDENTITY_SOURCE_TABLE_NAMES
    )


def test_cleanup_recognizes_current_and_legacy_shared_generations_only():
    for generation in ("shared_blocks_v1", "shared_blocks_v3", "shared_blocks_v4"):
        assert shared_gc.is_shared_blocks_cleanup_manifest(
            {"storage_generation": generation}
        )
    assert not shared_gc.is_shared_blocks_cleanup_manifest(
        {"storage_generation": "shared_blocks_v0"}
    )


def test_shared_gc_helper_defaults_cover_empty_inputs(monkeypatch):
    assert shared_gc._row_mapping(None) == {}
    assert not shared_gc.is_shared_blocks_cleanup_manifest(None)
    monkeypatch.delenv(
        shared_gc.PTG2_V4_ABANDONMENT_STATEMENT_TIMEOUT_SECONDS_ENV,
        raising=False,
    )
    assert (
        shared_gc._v4_abandonment_statement_timeout_seconds()
        == shared_gc.PTG2_V4_ABANDONMENT_STATEMENT_TIMEOUT_SECONDS_DEFAULT
    )
    assert shared_gc._v4_abandonment_statement_timeout_seconds(0) == 0.001
    monkeypatch.setenv(
        shared_gc.PTG2_V4_ABANDONMENT_STATEMENT_TIMEOUT_SECONDS_ENV,
        "0",
    )
    assert shared_gc._v4_abandonment_statement_timeout_seconds() == 0.001


@pytest.mark.asyncio
async def test_owned_v4_abandonment_acquires_connection_without_executor(
    monkeypatch,
):
    """The convenience entry point commits each bounded cleanup step."""

    connection = object()
    expected = shared_gc.PTG2SharedLayoutGCStats(logical_layout_count=1)
    acquired_connections: list[object] = []

    @asynccontextmanager
    async def acquire():
        acquired_connections.append(connection)
        yield connection

    inventory = shared_gc._OwnedV4AbandonmentInventory(
        block_hashes=(),
        stored_bytes=0,
        abandonment_token="abandon-token",
    )
    monkeypatch.setattr(shared_gc.db, "acquire", acquire)
    pipeline_mock_by_name = _patch_v4_abandonment_pipeline(
        monkeypatch,
        inventory=inventory,
        final_stats=expected,
    )

    observed = await shared_gc.abandon_owned_v4_layout(
        schema_name="mrf",
        snapshot_key=17,
        build_token="build-token",
        grace_seconds=23,
    )

    assert observed is expected
    assert len(acquired_connections) == len(shared_gc.PTG2_V3_DENSE_LAYOUT_TABLES) + 2
    pipeline_mock_by_name["shared_tables"].assert_awaited_once_with(
        connection,
        "mrf",
        require_shared=True,
    )
    pipeline_mock_by_name["map_tables"].assert_awaited_once_with(connection, "mrf")
    pipeline_mock_by_name["inventory"].assert_awaited_once()
    context = pipeline_mock_by_name["inventory"].await_args.kwargs["context"]
    assert context.schema_name == "mrf"
    assert context.snapshot_key == 17
    assert context.build_token == "build-token"
    assert context.batch_rows == shared_gc.PTG2_V4_ABANDONMENT_BATCH_ROWS_DEFAULT
    assert pipeline_mock_by_name["delete_dense"].await_count == len(
        shared_gc.PTG2_V3_DENSE_LAYOUT_TABLES
    )
    pipeline_mock_by_name["finalize"].assert_awaited_once_with(
        connection,
        context=context,
        inventory=inventory,
    )


@pytest.mark.asyncio
async def test_owned_v4_abandonment_reports_bounded_work(monkeypatch):
    """Progress reports only committed candidate, dense, and layout work."""

    inventory = shared_gc._OwnedV4AbandonmentInventory(
        block_hashes=(_hash(1), _hash(2), _hash(3)),
        stored_bytes=60,
        abandonment_token="abandon-token",
    )
    progress: list[tuple[str, int]] = []
    dense_deletes = iter((2, 1, *(0 for _ in shared_gc.PTG2_V3_DENSE_LAYOUT_TABLES)))
    expected = shared_gc.PTG2SharedLayoutGCStats(
        logical_layout_count=1,
        candidate_hash_count=3,
        stored_bytes=60,
    )
    pipeline_mock_by_name = _patch_v4_abandonment_pipeline(
        monkeypatch,
        inventory=inventory,
        final_stats=expected,
        dense_delete_effect=lambda _connection, **_kwargs: next(
            dense_deletes
        ),
    )

    observed = await shared_gc.abandon_owned_v4_layout(
        schema_name="mrf",
        snapshot_key=17,
        build_token="build-token",
        executor=object(),
        progress_callback=lambda metric, amount: progress.append(
            (metric, amount)
        ),
        options=shared_gc.PTG2V4AbandonmentOptions(
            batch_rows=2,
            timeout_seconds=5,
            monotonic=lambda: 100.0,
        ),
    )

    assert observed == expected
    assert pipeline_mock_by_name["queue_batch"].await_count == 2
    assert progress == [
        ("candidate_hashes", 2),
        ("candidate_hashes", 1),
        ("dense_rows", 2),
        ("dense_rows", 1),
        *(
            ("dense_tables", 1)
            for _ in shared_gc.PTG2_V3_DENSE_LAYOUT_TABLES
        ),
        ("layouts", 1),
    ]


@pytest.mark.asyncio
async def test_owned_v4_abandonment_cancellation_stops_after_committed_batch(
    monkeypatch,
):
    """Cancellation after progress leaves only the completed batch committed."""

    connection = object()
    committed_connections: list[object] = []

    @asynccontextmanager
    async def acquire():
        yield connection
        committed_connections.append(connection)

    inventory = shared_gc._OwnedV4AbandonmentInventory(
        block_hashes=(_hash(1), _hash(2), _hash(3)),
        stored_bytes=60,
        abandonment_token="abandon-token",
    )
    monkeypatch.setattr(shared_gc.db, "acquire", acquire)
    pipeline_mock_by_name = _patch_v4_abandonment_pipeline(
        monkeypatch,
        inventory=inventory,
        final_stats=shared_gc.PTG2SharedLayoutGCStats(),
    )

    def cancel_after_first_batch(metric: str, _amount: int) -> None:
        assert metric == "candidate_hashes"
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await shared_gc.abandon_owned_v4_layout(
            schema_name="mrf",
            snapshot_key=17,
            build_token="build-token",
            progress_callback=cancel_after_first_batch,
            options=shared_gc.PTG2V4AbandonmentOptions(batch_rows=2),
        )

    assert len(committed_connections) == 2
    assert pipeline_mock_by_name["queue_batch"].await_count == 1
    pipeline_mock_by_name["delete_dense"].assert_not_awaited()
    pipeline_mock_by_name["finalize"].assert_not_awaited()


@pytest.mark.asyncio
async def test_owned_v4_abandonment_fails_closed_at_time_budget(monkeypatch):
    load_inventory = AsyncMock()
    monkeypatch.setattr(
        shared_gc,
        "_owned_v4_inventory",
        load_inventory,
    )
    observed_times = iter((10.0, 12.0))

    with pytest.raises(
        shared_gc.PTG2SharedLayoutAbandonmentDeferred,
        match="time budget",
    ):
        await shared_gc.abandon_owned_v4_layout(
            schema_name="mrf",
            snapshot_key=17,
            build_token="build-token",
            executor=object(),
            options=shared_gc.PTG2V4AbandonmentOptions(
                timeout_seconds=1,
                monotonic=lambda: next(observed_times),
            ),
        )

    load_inventory.assert_not_awaited()


@pytest.mark.asyncio
async def test_shared_gc_cli_dry_run_and_execute(monkeypatch, capsys):
    plan = shared_gc.PTG2SharedGCPlan(
        layouts=shared_gc.PTG2SharedLayoutGCStats(1, 2, 3),
        sweep=shared_gc.PTG2SharedBlockSweepPlan((b"a" * 32,), 4),
    )
    build = AsyncMock(return_value=plan)
    release = AsyncMock(return_value=plan.layouts)
    sweep = AsyncMock(return_value=plan.sweep)
    monkeypatch.setattr(shared_gc, "build_ptg2_shared_gc_plan", build)
    monkeypatch.setattr(
        shared_gc,
        "release_unbound_ptg2_shared_layouts",
        release,
    )
    monkeypatch.setattr(shared_gc, "sweep_ptg2_shared_blocks", sweep)

    await shared_gc._amain(
        ("--schema", "testing", "--max-layouts", "1", "--max-rows", "2")
    )
    assert "cleanup_executed=false" in capsys.readouterr().out
    build.assert_awaited_once_with(
        schema_name="testing",
        max_layouts=1,
        max_rows=2,
        max_bytes=None,
    )
    release.assert_not_awaited()

    await shared_gc._amain(
        (
            "--schema",
            "testing",
            "--execute",
            "--max-layouts",
            "3",
            "--max-rows",
            "4",
            "--max-bytes",
            "5",
        )
    )
    output = capsys.readouterr().out
    assert "selected_hash=" in output
    assert "cleanup_executed=true" in output
    release.assert_awaited_once_with(schema_name="testing", max_layouts=3)
    sweep.assert_awaited_once_with(
        schema_name="testing",
        max_rows=4,
        max_bytes=5,
    )

    assert shared_gc._non_negative_int("0") == 0
    with pytest.raises(Exception, match="non-negative"):
        shared_gc._non_negative_int("-1")
