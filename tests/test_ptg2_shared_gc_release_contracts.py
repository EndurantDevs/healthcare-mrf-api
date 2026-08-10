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
async def test_migration_preflight_requires_candidate_attestation_table():
    executor = _SharedGCExecutor()
    executor.present_tables.remove("ptg2_v3_candidate_audit_attestation")

    with pytest.raises(
        RuntimeError,
        match="ptg2_v3_candidate_audit_attestation.*alembic upgrade head",
    ):
        await shared_gc.require_migration_owned_tables(executor, "mrf")
    with pytest.raises(
        RuntimeError,
        match="complete shared schema.*ptg2_v3_candidate_audit_attestation",
    ):
        await shared_gc.build_shared_layout_release_plan(
            executor=executor,
            require_shared=True,
        )


@pytest.mark.asyncio
async def test_cleanup_rejects_partial_provider_tax_identity_schema():
    executor = _SharedGCExecutor()
    legacy_layout, manifest, *remaining_base_tables = (
        shared_gc.PTG2_PROVIDER_TAX_IDENTITY_BASE_TABLE_NAMES
    )
    executor.present_tables.add(legacy_layout)
    executor.present_tables.add(manifest)

    with pytest.raises(
        RuntimeError,
        match="complete additive schema.*missing tables",
    ):
        await shared_gc.build_shared_layout_release_plan(
            executor=executor,
            require_shared=True,
        )

    executor.present_tables.update(remaining_base_tables)
    plan = await shared_gc.build_shared_layout_release_plan(
        executor=executor,
        require_shared=True,
    )

    assert plan.tables_available is True

    executor.present_tables.add(
        shared_gc.PTG2_PROVIDER_TAX_IDENTITY_SOURCE_TABLE_NAMES[0]
    )
    with pytest.raises(
        RuntimeError,
        match="complete additive schema.*missing tables",
    ):
        await shared_gc.build_shared_layout_release_plan(
            executor=executor,
            require_shared=True,
        )

    executor.present_tables.update(
        shared_gc.PTG2_PROVIDER_TAX_IDENTITY_SOURCE_TABLE_NAMES
    )
    plan = await shared_gc.build_shared_layout_release_plan(
        executor=executor,
        require_shared=True,
    )

    assert plan.tables_available is True


@pytest.mark.asyncio
async def test_cleanup_rejects_source_tax_identity_schema_without_base():
    executor = _SharedGCExecutor()
    executor.present_tables.update(
        shared_gc.PTG2_PROVIDER_TAX_IDENTITY_SOURCE_TABLE_NAMES
    )

    with pytest.raises(
        RuntimeError,
        match="complete additive schema.*missing tables",
    ):
        await shared_gc.build_shared_layout_release_plan(
            executor=executor,
            require_shared=True,
        )


@pytest.mark.asyncio
async def test_candidate_projection_excludes_unrelated_unbound_layouts():
    executor = _SharedGCExecutor()
    selected_hash = _hash(30)
    unrelated_hash = _hash(31)
    executor.add_layout(10)
    executor.add_layout(20)
    executor.add_block(selected_hash, 25)
    executor.add_block(unrelated_hash, 1_000)
    executor.map_block(10, selected_hash)
    executor.map_block(20, unrelated_hash)
    executor.bindings["selected-snapshot"] = 10

    plan = await shared_gc.build_shared_layout_release_plan(
        executor=executor,
        removing_snapshot_ids=("selected-snapshot",),
        all_eligible_layouts=True,
        require_shared=True,
    )

    assert plan == shared_gc.PTG2SharedLayoutGCStats(1, 1, 25)
    statement, params = next(
        call
        for call in executor.calls
        if "WITH eligible_layouts AS MATERIALIZED" in call[0]
    )
    assert "candidate_binding.snapshot_id" in statement
    assert params["removing_snapshot_ids"] == ["selected-snapshot"]


@pytest.mark.asyncio
async def test_candidate_release_is_limited_to_projected_layout_keys():
    executor = _SharedGCExecutor()
    selected_hash = _hash(32)
    unrelated_hash = _hash(33)
    executor.add_layout(10)
    executor.add_layout(20)
    executor.add_block(selected_hash, 25)
    executor.add_block(unrelated_hash, 1_000)
    executor.map_block(10, selected_hash)
    executor.map_block(20, unrelated_hash)

    released = await shared_gc.release_unbound_ptg2_shared_layouts(
        executor=executor,
        layout_keys=(10,),
        require_shared=True,
    )

    assert released == shared_gc.PTG2SharedLayoutGCStats(1, 1, 25)
    assert set(executor.layouts) == {20}
    assert (20, unrelated_hash) in executor.mappings


@pytest.mark.asyncio
async def test_two_bindings_retain_layout_until_last_logical_snapshot_is_removed():
    executor = _SharedGCExecutor()
    block_hash = _hash(1)
    executor.add_layout(10)
    executor.add_block(block_hash, 50)
    executor.map_block(10, block_hash)
    executor.bindings = {"snap-a": 10, "snap-b": 10}

    executor.bindings.pop("snap-a")
    retained = await shared_gc.release_unbound_ptg2_shared_layouts(
        executor=executor, grace_seconds=60
    )
    assert retained.logical_layout_count == 0
    assert 10 in executor.layouts

    executor.bindings.pop("snap-b")
    released = await shared_gc.release_unbound_ptg2_shared_layouts(
        executor=executor, grace_seconds=60
    )
    assert released == shared_gc.PTG2SharedLayoutGCStats(1, 1, 50)
    assert 10 not in executor.layouts
    assert executor.candidates[block_hash] == executor.now + timedelta(seconds=60)


@pytest.mark.asyncio
async def test_shared_hash_survives_while_any_layout_still_maps_it():
    executor = _SharedGCExecutor()
    block_hash = _hash(2)
    executor.add_layout(10)
    executor.add_layout(20)
    executor.add_block(block_hash, 75)
    executor.map_block(10, block_hash)
    executor.map_block(20, block_hash)
    executor.bindings["live"] = 20

    await shared_gc.release_unbound_ptg2_shared_layouts(
        executor=executor, grace_seconds=0
    )
    swept = await shared_gc.sweep_ptg2_shared_blocks(executor=executor)

    assert swept.selected_hashes == ()
    assert block_hash in executor.blocks
    assert (20, block_hash) in executor.mappings


@pytest.mark.asyncio
async def test_stale_building_layout_is_released_but_fresh_build_is_retained():
    executor = _SharedGCExecutor()
    stale_hash = _hash(3)
    fresh_hash = _hash(4)
    executor.add_layout(10, state="building", age_seconds=21_601)
    executor.add_layout(20, state="building", age_seconds=21_599)
    executor.add_block(stale_hash, 20)
    executor.add_block(fresh_hash, 30)
    executor.map_block(10, stale_hash)
    executor.map_block(20, fresh_hash)

    result = await shared_gc.release_unbound_ptg2_shared_layouts(
        executor=executor,
        building_max_age_seconds=21_600,
    )

    assert result == shared_gc.PTG2SharedLayoutGCStats(1, 1, 20)
    assert set(executor.layouts) == {20}
    assert stale_hash in executor.candidates
    assert fresh_hash not in executor.candidates


@pytest.mark.asyncio
async def test_active_layout_lease_prevents_seal_bind_and_retry_gc_races():
    executor = _SharedGCExecutor()
    sealed_hash = _hash(14)
    building_hash = _hash(15)
    executor.add_layout(10, state="sealed", lease_seconds=3_600)
    executor.add_layout(
        20,
        state="building",
        age_seconds=21_601,
        lease_seconds=3_600,
    )
    executor.add_block(sealed_hash, 20)
    executor.add_block(building_hash, 30)
    executor.map_block(10, sealed_hash)
    executor.map_block(20, building_hash)

    protected = await shared_gc.release_unbound_ptg2_shared_layouts(
        executor=executor,
        building_max_age_seconds=21_600,
    )
    assert protected.logical_layout_count == 0
    assert set(executor.layouts) == {10, 20}

    executor.now += timedelta(seconds=3_600)
    released = await shared_gc.release_unbound_ptg2_shared_layouts(
        executor=executor,
        building_max_age_seconds=21_600,
    )
    assert released.logical_layout_count == 2
    assert not executor.layouts


@pytest.mark.asyncio
async def test_grace_period_must_elapse_before_payload_sweep():
    executor = _SharedGCExecutor()
    block_hash = _hash(5)
    executor.add_layout(10)
    executor.add_block(block_hash, 20)
    executor.map_block(10, block_hash)

    await shared_gc.release_unbound_ptg2_shared_layouts(
        executor=executor, grace_seconds=60
    )
    before_grace = await shared_gc.sweep_ptg2_shared_blocks(executor=executor)
    executor.now += timedelta(seconds=60)
    after_grace = await shared_gc.sweep_ptg2_shared_blocks(executor=executor)

    assert before_grace.selected_hashes == ()
    assert after_grace.selected_hashes == (block_hash,)
    assert block_hash not in executor.blocks
    assert block_hash not in executor.candidates


@pytest.mark.asyncio
async def test_sweep_respects_aggregate_byte_cap_and_reports_exact_hashes():
    executor = _SharedGCExecutor()
    size_by_block_hash = {_hash(6): 60, _hash(7): 50, _hash(8): 40}
    for block_hash, stored_bytes in size_by_block_hash.items():
        executor.add_block(block_hash, stored_bytes)
        executor.candidates[block_hash] = executor.now - timedelta(seconds=1)

    plan = await shared_gc.build_shared_block_sweep_plan(
        executor=executor,
        max_bytes=100,
        max_rows=10,
    )
    swept = await shared_gc.sweep_ptg2_shared_blocks(
        executor=executor,
        max_bytes=100,
        max_rows=10,
    )

    assert plan.selected_hashes == (_hash(6), _hash(8))
    assert plan.stored_bytes == 100
    assert swept == plan
    assert set(executor.blocks) == {_hash(7)}


@pytest.mark.asyncio
async def test_candidate_rereference_is_rechecked_at_delete_time():
    executor = _SharedGCExecutor()
    block_hash = _hash(9)
    executor.add_layout(99)
    executor.bindings["live"] = 99
    executor.add_block(block_hash, 20)
    executor.candidates[block_hash] = executor.now - timedelta(seconds=1)
    executor.rereference_on_delete = (99, block_hash)

    result = await shared_gc.sweep_ptg2_shared_blocks(executor=executor)

    assert result.selected_hashes == ()
    assert block_hash in executor.blocks
    assert block_hash in executor.candidates
    assert (99, block_hash) in executor.mappings


@pytest.mark.asyncio
async def test_release_upsert_never_shortens_existing_later_eligibility():
    executor = _SharedGCExecutor()
    block_hash = _hash(10)
    executor.add_layout(10)
    executor.add_block(block_hash, 20)
    executor.map_block(10, block_hash)
    later = executor.now + timedelta(hours=2)
    executor.candidates[block_hash] = later

    await shared_gc.release_unbound_ptg2_shared_layouts(
        executor=executor, grace_seconds=60
    )

    assert executor.candidates[block_hash] == later


@pytest.mark.asyncio
async def test_layout_release_rechecks_binding_after_lock_selection():
    executor = _SharedGCExecutor()
    block_hash = _hash(13)
    executor.add_layout(10)
    executor.add_block(block_hash, 20)
    executor.map_block(10, block_hash)
    executor.binding_on_release = ("new-binding", 10)

    result = await shared_gc.release_unbound_ptg2_shared_layouts(executor=executor)

    assert result.logical_layout_count == 0
    assert executor.bindings == {"new-binding": 10}
    assert 10 in executor.layouts
    assert block_hash not in executor.candidates


@pytest.mark.asyncio
async def test_dry_run_keeps_sweep_immutable():
    executor = _SharedGCExecutor()
    queued_hash = _hash(11)
    sweep_hash = _hash(12)
    executor.add_layout(10)
    executor.add_block(queued_hash, 70)
    executor.map_block(10, queued_hash)
    executor.add_block(sweep_hash, 30)
    executor.candidates[sweep_hash] = executor.now - timedelta(seconds=1)

    plan = await shared_gc.build_ptg2_shared_gc_plan(
        executor=executor,
        max_bytes=100,
        max_rows=10,
    )

    assert plan.logical_layout_count == 1
    assert plan.candidate_hash_count == 1
    assert plan.stored_bytes == 70
    assert plan.selected_hashes == (sweep_hash,)
    assert plan.sweep.stored_bytes == 30
    assert 10 in executor.layouts
    assert executor.mappings == {(10, queued_hash)}
    assert set(executor.blocks) == {queued_hash, sweep_hash}
    assert not any("payload" in sql for sql, _params in executor.calls)


@pytest.mark.asyncio
async def test_missing_shared_schema_noops_only_without_manifest_or_binding():
    executor = _SharedGCExecutor()
    executor.present_tables = {"ptg2_snapshot"}

    result = await shared_gc.release_unbound_ptg2_shared_layouts(executor=executor)
    assert result.tables_available is False

    executor.manifest_involved = True
    with pytest.raises(RuntimeError, match="complete shared schema"):
        await shared_gc.release_unbound_ptg2_shared_layouts(executor=executor)

    executor.manifest_involved = False
    executor.present_tables.add("ptg2_v3_snapshot_scope")
    executor.scopes.add("orphaned-logical-snapshot")
    with pytest.raises(RuntimeError, match="complete shared schema"):
        await shared_gc.release_unbound_ptg2_shared_layouts(executor=executor)


@pytest.mark.asyncio
async def test_normal_snapshot_cleanup_releases_shared_layouts(monkeypatch):
    class _CleanupExecutor:
        async def all(self, _statement, **_params):
            return [
                {
                    "snapshot_id": "shared-current",
                    "status": "published",
                    "manifest": {
                        "serving_index": {
                            "source_key": "source-a",
                            "storage": "manifest_snapshot",
                            "arch_version": "postgres_binary_v3",
                            "storage_generation": "shared_blocks_v3",
                        }
                    },
                }
            ]

        async def status(self, _statement, **_params):
            return 0

    executor = _CleanupExecutor()
    release = AsyncMock(return_value=shared_gc.PTG2SharedLayoutGCStats())
    monkeypatch.setattr(snapshot_cleanup, "release_unbound_ptg2_shared_layouts", release)

    await snapshot_cleanup._cleanup_source_tables(
        executor,
        source_key="source-a",
        keep_snapshot_ids={"shared-current"},
    )

    release.assert_awaited_once_with(
        schema_name="mrf",
        executor=executor,
        require_shared=True,
    )
