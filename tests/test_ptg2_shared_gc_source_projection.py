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

@pytest.mark.asyncio
async def test_source_snapshot_gc_does_not_project_unrelated_shared_layout_bytes():
    """Verify source snapshot gc does not project unrelated shared layout bytes."""
    plan = await source_snapshot_gc.build_ptg2_source_snapshot_gc_plan(
        executor=_SourceGCProjectionExecutor()
    )

    assert plan.shared_snapshot_ids == ("shared-old",)
    assert plan.shared_layout_count == 1
    assert plan.shared_candidate_hash_count == 1
    assert plan.shared_stored_bytes == 25
    assert plan.total_bytes == 25
    source_snapshot_gc.validate_ptg2_source_snapshot_gc_plan(
        plan,
        max_snapshots=10,
        max_tables=10,
        max_bytes=25,
    )
    with pytest.raises(RuntimeError, match="candidate bytes 25"):
        source_snapshot_gc.validate_ptg2_source_snapshot_gc_plan(
            plan,
            max_snapshots=10,
            max_tables=10,
            max_bytes=24,
        )


@pytest.mark.asyncio
async def test_source_snapshot_gc_releases_unbound_layout_in_same_transaction(monkeypatch):
    events: list[str] = []
    connection_state_map = {"connection": None}

    class _Connection:
        async def all(self, statement, **_params):
            assert "SELECT DISTINCT snapshot_key" in statement
            return [{"snapshot_key": 10}]

        async def status(self, statement, **_params):
            if 'DELETE FROM "mrf".ptg2_v3_snapshot_binding' in statement:
                events.append("binding-delete")
            if 'DELETE FROM "mrf".ptg2_snapshot' in statement:
                events.append("logical-delete")
            return 1

    class _DB:
        @asynccontextmanager
        async def acquire(self):
            connection_state_map["connection"] = _Connection()
            yield connection_state_map["connection"]

    plan = source_snapshot_gc.PTG2SourceSnapshotGCPlan(
        current_snapshot_ids=(),
        candidate_snapshot_ids=("shared-old",),
        tables=(),
        shared_snapshot_ids=("shared-old",),
        shared_layout_count=0,
        shared_candidate_hash_count=0,
        shared_stored_bytes=0,
    )

    monkeypatch.setattr(source_snapshot_gc, "db", _DB())
    monkeypatch.setattr(
        source_snapshot_gc,
        "build_ptg2_source_snapshot_gc_plan",
        AsyncMock(return_value=plan),
    )
    release = AsyncMock(return_value=shared_gc.PTG2SharedLayoutGCStats())
    monkeypatch.setattr(
        source_snapshot_gc,
        "release_unbound_ptg2_shared_layouts",
        release,
    )
    gc_result = await source_snapshot_gc.execute_ptg2_source_snapshot_gc_plan(max_bytes=100)

    assert gc_result is plan
    assert events == ["binding-delete", "logical-delete"]
    release.assert_awaited_once_with(
        schema_name="mrf",
        executor=connection_state_map["connection"],
        require_shared=True,
        layout_keys=(10,),
    )


@pytest.mark.asyncio
async def test_source_snapshot_gc_skips_layout_release_without_deleted_binding(monkeypatch):
    class _Connection:
        async def all(self, statement, **_params):
            assert "SELECT DISTINCT snapshot_key" in statement
            return [{"snapshot_key": 10}]

        async def status(self, statement, **_params):
            if 'DELETE FROM "mrf".ptg2_v3_snapshot_binding' in statement:
                return 0
            return 1

    class _DB:
        @asynccontextmanager
        async def acquire(self):
            yield _Connection()

    plan = source_snapshot_gc.PTG2SourceSnapshotGCPlan(
        current_snapshot_ids=(),
        candidate_snapshot_ids=("shared-old",),
        tables=(),
        shared_snapshot_ids=("shared-old",),
    )
    monkeypatch.setattr(source_snapshot_gc, "db", _DB())
    monkeypatch.setattr(
        source_snapshot_gc,
        "build_ptg2_source_snapshot_gc_plan",
        AsyncMock(return_value=plan),
    )
    release = AsyncMock()
    monkeypatch.setattr(
        source_snapshot_gc,
        "release_unbound_ptg2_shared_layouts",
        release,
    )

    gc_result = await source_snapshot_gc.execute_ptg2_source_snapshot_gc_plan(
        max_bytes=100
    )

    assert gc_result is plan
    release.assert_not_awaited()
