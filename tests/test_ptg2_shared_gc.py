# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

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


def _hash(value: int) -> bytes:
    return bytes([value]) * 32


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


def test_cleanup_recognizes_current_and_legacy_shared_generations_only():
    for generation in ("shared_blocks_v1", "shared_blocks_v3"):
        assert shared_gc.is_shared_blocks_cleanup_manifest(
            {"storage_generation": generation}
        )
    assert not shared_gc.is_shared_blocks_cleanup_manifest(
        {"storage_generation": "shared_blocks_v0"}
    )


class _SharedGCExecutor:
    def __init__(self) -> None:
        self.now = datetime(2026, 7, 12, 12, tzinfo=timezone.utc)
        self.layouts: dict[int, dict[str, object]] = {}
        self.bindings: dict[str, int] = {}
        self.scopes: set[str] = set()
        self.blocks: dict[bytes, int] = {}
        self.mappings: set[tuple[int, bytes]] = set()
        self.candidates: dict[bytes, datetime] = {}
        self.present_tables = {"ptg2_snapshot", *shared_gc._SHARED_TABLE_NAMES}
        self.manifest_involved = False
        self.binding_on_release: tuple[str, int] | None = None
        self.rereference_on_delete: tuple[int, bytes] | None = None
        self.calls: list[tuple[str, dict[str, object]]] = []

    def add_layout(
        self,
        snapshot_key: int,
        *,
        state: str = "sealed",
        age_seconds: int = 0,
        lease_seconds: int | None = None,
    ) -> None:
        timestamp = self.now - timedelta(seconds=age_seconds)
        self.layouts[snapshot_key] = {
            "state": state,
            "generation": shared_gc.PTG2_V3_SHARED_GENERATION,
            "created_at": timestamp,
            "heartbeat_at": timestamp,
            "lease_until": (
                self.now + timedelta(seconds=lease_seconds)
                if lease_seconds is not None
                else None
            ),
        }

    def add_block(self, block_hash: bytes, stored_bytes: int) -> None:
        self.blocks[block_hash] = stored_bytes

    def map_block(self, snapshot_key: int, block_hash: bytes) -> None:
        self.mappings.add((snapshot_key, block_hash))

    def _eligible_layouts(
        self,
        *,
        removing_snapshot_ids: set[str],
        building_max_age_seconds: int,
        limit: int | None,
    ) -> list[int]:
        eligible: list[int] = []
        for snapshot_key, layout in self.layouts.items():
            selected_bindings = [
                snapshot_id
                for snapshot_id, bound_key in self.bindings.items()
                if bound_key == snapshot_key and snapshot_id in removing_snapshot_ids
            ]
            if removing_snapshot_ids and not selected_bindings:
                continue
            outside_bindings = [
                snapshot_id
                for snapshot_id, bound_key in self.bindings.items()
                if bound_key == snapshot_key and snapshot_id not in removing_snapshot_ids
            ]
            if outside_bindings:
                continue
            state = str(layout["state"])
            created_at = layout["created_at"]
            heartbeat_at = layout["heartbeat_at"]
            lease_until = layout["lease_until"]
            assert isinstance(created_at, datetime)
            assert isinstance(heartbeat_at, datetime)
            if isinstance(lease_until, datetime) and lease_until > self.now:
                continue
            if state == "sealed" or (
                state == "building"
                and heartbeat_at
                < self.now - timedelta(seconds=building_max_age_seconds)
            ):
                eligible.append(snapshot_key)
        eligible.sort(key=lambda key: (self.layouts[key]["created_at"], key))
        return eligible if limit is None else eligible[: int(limit)]

    def _layout_stats(self, layout_keys: list[int]) -> dict[str, int]:
        mapped_hashes = {
            block_hash
            for snapshot_key, block_hash in self.mappings
            if snapshot_key in layout_keys
        }
        return {
            "logical_layout_count": len(layout_keys),
            "candidate_hash_count": len(mapped_hashes),
            "stored_bytes": sum(self.blocks[block_hash] for block_hash in mapped_hashes),
        }

    async def all(self, statement: str, **params):
        """Emulate the executor SQL used by shared-layout GC tests."""

        self.calls.append((statement, dict(params)))
        if "FROM information_schema.tables" in statement:
            return [{"table_name": table_name} for table_name in sorted(self.present_tables)]
        if "FROM \"mrf\".ptg2_snapshot" in statement and "AS involved" in statement:
            return [{"involved": self.manifest_involved}]
        if "FROM \"mrf\".ptg2_v3_snapshot_binding" in statement and "AS involved" in statement:
            return [{"involved": bool(self.bindings)}]
        if "FROM \"mrf\".ptg2_v3_snapshot_scope" in statement and "AS involved" in statement:
            return [{"involved": bool(self.scopes)}]
        if "WITH eligible_layouts AS MATERIALIZED" in statement:
            layout_keys = self._eligible_layouts(
                removing_snapshot_ids=set(params["removing_snapshot_ids"]),
                building_max_age_seconds=int(params["building_max_age_seconds"]),
                limit=params["layout_limit"],
            )
            return [self._layout_stats(layout_keys)]
        if (
            "SELECT layout.snapshot_key" in statement
            and "FOR UPDATE OF layout SKIP LOCKED" in statement
        ):
            assert "FOR UPDATE OF layout SKIP LOCKED" in statement
            layout_keys = self._eligible_layouts(
                removing_snapshot_ids=set(),
                building_max_age_seconds=int(params["building_max_age_seconds"]),
                limit=int(params["layout_limit"]),
            )
            if params.get("restrict_layout_keys"):
                allowed_layout_keys = {
                    int(value) for value in params.get("layout_keys", ())
                }
                layout_keys = [
                    snapshot_key
                    for snapshot_key in layout_keys
                    if snapshot_key in allowed_layout_keys
                ]
            return [{"snapshot_key": snapshot_key} for snapshot_key in layout_keys]
        if "WITH layout_batch AS MATERIALIZED" in statement:
            assert "GREATEST(" in statement
            for table_name in shared_gc.PTG2_V3_DENSE_LAYOUT_TABLES:
                assert f'DELETE FROM "mrf"."{table_name}" AS payload' in statement
            if self.binding_on_release is not None:
                snapshot_id, snapshot_key = self.binding_on_release
                self.bindings[snapshot_id] = snapshot_key
                self.binding_on_release = None
            eligible_keys = set(
                self._eligible_layouts(
                    removing_snapshot_ids=set(),
                    building_max_age_seconds=int(params["building_max_age_seconds"]),
                    limit=None,
                )
            )
            layout_keys = [
                int(snapshot_key)
                for snapshot_key in params["layout_keys"]
                if int(snapshot_key) in eligible_keys
            ]
            stats = self._layout_stats(layout_keys)
            mapped_hashes = {
                block_hash
                for snapshot_key, block_hash in self.mappings
                if snapshot_key in layout_keys
            }
            eligible_at = self.now + timedelta(seconds=int(params["grace_seconds"]))
            for block_hash in mapped_hashes:
                self.candidates[block_hash] = max(
                    self.candidates.get(block_hash, eligible_at),
                    eligible_at,
                )
            for snapshot_key in layout_keys:
                self.layouts.pop(snapshot_key)
            self.mappings = {
                mapping for mapping in self.mappings if mapping[0] not in layout_keys
            }
            return [stats]
        if "SELECT candidate.block_hash" in statement:
            assert "payload" not in statement
            rows = []
            mapped_hashes = {block_hash for _snapshot_key, block_hash in self.mappings}
            for block_hash, eligible_at in sorted(
                self.candidates.items(), key=lambda item: (item[1], item[0])
            ):
                stored_bytes = self.blocks.get(block_hash)
                if (
                    stored_bytes is not None
                    and eligible_at <= self.now
                    and block_hash not in mapped_hashes
                    and stored_bytes <= int(params["max_bytes"])
                ):
                    rows.append(
                        {
                            "block_hash": block_hash,
                            "stored_byte_count": stored_bytes,
                            "eligible_at": eligible_at,
                        }
                    )
            return rows[: int(params["max_rows"])]
        if "DELETE FROM \"mrf\".ptg2_v3_block AS block" in statement:
            assert "NOT EXISTS" in statement
            if self.rereference_on_delete is not None:
                self.mappings.add(self.rereference_on_delete)
                self.rereference_on_delete = None
            mapped_hashes = {block_hash for _snapshot_key, block_hash in self.mappings}
            deleted = []
            for block_hash in params["block_hashes"]:
                if block_hash in mapped_hashes or block_hash not in self.blocks:
                    continue
                stored_bytes = self.blocks.pop(block_hash)
                self.candidates.pop(block_hash, None)
                deleted.append(
                    {"block_hash": block_hash, "stored_byte_count": stored_bytes}
                )
            return deleted
        raise AssertionError(statement)

    async def status(self, statement: str, **params):
        self.calls.append((statement, dict(params)))
        if "WITH orphaned AS" in statement:
            orphaned = [
                block_hash
                for block_hash in sorted(self.candidates)
                if block_hash not in self.blocks
            ][: int(params["max_rows"])]
            for block_hash in orphaned:
                self.candidates.pop(block_hash, None)
            return len(orphaned)
        raise AssertionError(statement)


@pytest.mark.asyncio
async def test_migration_preflight_requires_candidate_attestation_table():
    executor = _SharedGCExecutor()
    executor.present_tables.remove("ptg2_v3_candidate_audit_attestation")

    with pytest.raises(
        RuntimeError,
        match="ptg2_v3_candidate_audit_attestation.*alembic upgrade head",
    ):
        await shared_gc.require_ptg2_v3_migration_owned_tables(executor, "mrf")
    with pytest.raises(
        RuntimeError,
        match="complete shared schema.*ptg2_v3_candidate_audit_attestation",
    ):
        await shared_gc.build_ptg2_shared_layout_release_plan(
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

    plan = await shared_gc.build_ptg2_shared_layout_release_plan(
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
    sizes = {_hash(6): 60, _hash(7): 50, _hash(8): 40}
    for block_hash, stored_bytes in sizes.items():
        executor.add_block(block_hash, stored_bytes)
        executor.candidates[block_hash] = executor.now - timedelta(seconds=1)

    plan = await shared_gc.build_ptg2_shared_block_sweep_plan(
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


@pytest.mark.asyncio
async def test_source_snapshot_gc_does_not_project_unrelated_shared_layout_bytes():
    """Verify source snapshot gc does not project unrelated shared layout bytes."""
    class _SourceGCExecutor:
        async def all(self, statement, **_params):
            if "FROM information_schema.tables" in statement:
                return [
                    {"table_name": table_name}
                    for table_name in shared_gc.PTG2_V3_MIGRATION_OWNED_TABLE_NAMES
                ]
            if "SELECT DISTINCT snapshot_id" in statement:
                return []
            if 'FROM "mrf".ptg2_snapshot' in statement:
                return [
                    {
                        "snapshot_id": "shared-old",
                        "status": "published",
                        "source_key": "source-a",
                        "serving_index": {
                            "storage": "manifest_snapshot",
                            "arch_version": "postgres_binary_v3",
                            "storage_generation": "shared_blocks_v3",
                            "source_key": "source-a",
                        },
                    }
                ]
            if "WITH eligible_layouts AS MATERIALIZED" in statement:
                assert "candidate_binding.snapshot_id" in statement
                assert _params["removing_snapshot_ids"] == ["shared-old"]
                # The selected layout is 25 bytes. A separate 1,000-byte
                # unbound layout is deliberately outside this projection.
                return [
                    {
                        "logical_layout_count": 1,
                        "candidate_hash_count": 1,
                        "stored_bytes": 25,
                    }
                ]
            raise AssertionError(statement)

    plan = await source_snapshot_gc.build_ptg2_source_snapshot_gc_plan(
        executor=_SourceGCExecutor()
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
    state = {"connection": None}

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
            state["connection"] = _Connection()
            yield state["connection"]

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
        "ensure_ptg2_artifact_blob_table",
        AsyncMock(return_value=None),
    )
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
    result = await source_snapshot_gc.execute_ptg2_source_snapshot_gc_plan(max_bytes=100)

    assert result is plan
    assert events == ["binding-delete", "logical-delete"]
    release.assert_awaited_once_with(
        schema_name="mrf",
        executor=state["connection"],
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
        "ensure_ptg2_artifact_blob_table",
        AsyncMock(return_value=None),
    )
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

    result = await source_snapshot_gc.execute_ptg2_source_snapshot_gc_plan(
        max_bytes=100
    )

    assert result is plan
    release.assert_not_awaited()


@pytest.mark.asyncio
async def test_real_postgres_candidate_scoped_release_and_sweep_sql():
    """Exercise candidate-scoped layout release and block sweep in PostgreSQL."""

    if os.getenv("HLTHPRT_PTG2_SHARED_GC_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_SHARED_GC_POSTGRES_TEST=1 for the isolated PostgreSQL test")

    database = Database()
    schema_name = f"ptg2_shared_gc_test_{uuid.uuid4().hex}"
    schema = f'"{schema_name}"'
    block_hash = _hash(20)
    unrelated_hash = _hash(21)
    await database.connect()
    try:
        async with database.acquire() as connection:
            await connection.status(f"CREATE SCHEMA {schema}")
            await connection.status(
                f"CREATE TABLE {schema}.ptg2_snapshot "
                "(snapshot_id varchar(96) PRIMARY KEY, manifest jsonb)"
            )
            await connection.status(
                f"""
                CREATE TABLE {schema}.ptg2_v3_snapshot_layout (
                    snapshot_key bigint PRIMARY KEY,
                    generation varchar(32) NOT NULL,
                    state varchar(16) NOT NULL,
                    created_at timestamptz NOT NULL,
                    heartbeat_at timestamptz NOT NULL,
                    lease_until timestamptz
                )
                """
            )
            await connection.status(
                f"""
                CREATE TABLE {schema}.ptg2_v3_snapshot_binding (
                    snapshot_id varchar(96) PRIMARY KEY
                        REFERENCES {schema}.ptg2_snapshot(snapshot_id) ON DELETE CASCADE,
                    snapshot_key bigint NOT NULL
                        REFERENCES {schema}.ptg2_v3_snapshot_layout(snapshot_key) ON DELETE RESTRICT
                )
                """
            )
            await connection.status(
                f"""
                CREATE TABLE {schema}.ptg2_v3_block (
                    block_hash bytea PRIMARY KEY,
                    stored_byte_count bigint NOT NULL
                )
                """
            )
            await connection.status(
                f"""
                CREATE TABLE {schema}.ptg2_v3_snapshot_block (
                    snapshot_key bigint NOT NULL
                        REFERENCES {schema}.ptg2_v3_snapshot_layout(snapshot_key) ON DELETE CASCADE,
                    block_hash bytea NOT NULL REFERENCES {schema}.ptg2_v3_block(block_hash),
                    PRIMARY KEY (snapshot_key, block_hash)
                )
                """
            )
            await connection.status(
                f"""
                CREATE TABLE {schema}.ptg2_v3_gc_candidate (
                    block_hash bytea PRIMARY KEY
                        REFERENCES {schema}.ptg2_v3_block(block_hash) ON DELETE CASCADE,
                    eligible_at timestamptz NOT NULL,
                    queued_at timestamptz NOT NULL
                )
                """
            )
            for table_name in set(shared_gc._SHARED_TABLE_NAMES) - {
                "ptg2_v3_snapshot_layout",
                "ptg2_v3_snapshot_binding",
                "ptg2_v3_block",
                "ptg2_v3_snapshot_block",
                "ptg2_v3_gc_candidate",
            }:
                await connection.status(
                    f"CREATE TABLE {schema}.\"{table_name}\" (snapshot_key bigint)"
                )
            await connection.status(
                f"""
                INSERT INTO {schema}.ptg2_v3_snapshot_layout
                    (snapshot_key, generation, state, created_at, heartbeat_at, lease_until)
                VALUES (
                    10, :generation, 'sealed', transaction_timestamp(),
                    transaction_timestamp(), NULL
                ), (
                    20, :generation, 'sealed', transaction_timestamp(),
                    transaction_timestamp(), NULL
                )
                """,
                generation=shared_gc.PTG2_V3_SHARED_GENERATION,
            )
            for table_name in shared_gc.PTG2_V3_DENSE_LAYOUT_TABLES:
                await connection.status(
                    f"INSERT INTO {schema}.\"{table_name}\" (snapshot_key) VALUES (10)"
                )
            await connection.status(
                f"""
                INSERT INTO {schema}.ptg2_snapshot (snapshot_id, manifest)
                VALUES ('selected-snapshot', '{{}}'::jsonb)
                """
            )
            await connection.status(
                f"""
                INSERT INTO {schema}.ptg2_v3_snapshot_binding
                    (snapshot_id, snapshot_key)
                VALUES ('selected-snapshot', 10)
                """
            )
            await connection.status(
                f"""
                INSERT INTO {schema}.ptg2_v3_block
                    (block_hash, stored_byte_count)
                VALUES (:block_hash, 25), (:unrelated_hash, 1000)
                """,
                block_hash=block_hash,
                unrelated_hash=unrelated_hash,
            )
            await connection.status(
                f"""
                INSERT INTO {schema}.ptg2_v3_snapshot_block
                    (snapshot_key, block_hash)
                VALUES (10, :block_hash), (20, :unrelated_hash)
                """,
                block_hash=block_hash,
                unrelated_hash=unrelated_hash,
            )

        async with database.acquire() as connection:
            dry_run = await shared_gc.build_ptg2_shared_layout_release_plan(
                schema_name=schema_name,
                executor=connection,
                removing_snapshot_ids=("selected-snapshot",),
                all_eligible_layouts=True,
                require_shared=True,
            )
        assert dry_run == shared_gc.PTG2SharedLayoutGCStats(1, 1, 25)

        async with database.acquire() as connection:
            await connection.status(
                f"""
                DELETE FROM {schema}.ptg2_v3_snapshot_binding
                 WHERE snapshot_id = 'selected-snapshot'
                """
            )
            released = await shared_gc.release_unbound_ptg2_shared_layouts(
                schema_name=schema_name,
                executor=connection,
                grace_seconds=0,
                require_shared=True,
                layout_keys=(10,),
            )
        assert released == shared_gc.PTG2SharedLayoutGCStats(1, 1, 25)

        async with database.acquire() as connection:
            swept = await shared_gc.sweep_ptg2_shared_blocks(
                schema_name=schema_name,
                executor=connection,
                max_bytes=25,
                require_shared=True,
            )
        assert swept == shared_gc.PTG2SharedBlockSweepPlan((block_hash,), 25)

        async with database.acquire() as connection:
            assert await connection.scalar(
                f"SELECT COUNT(*) FROM {schema}.ptg2_v3_snapshot_layout"
            ) == 1
            assert await connection.scalar(
                f"SELECT COUNT(*) FROM {schema}.ptg2_v3_block"
            ) == 1
            assert await connection.scalar(
                f"SELECT COUNT(*) FROM {schema}.ptg2_v3_gc_candidate"
            ) == 0
    finally:
        try:
            async with database.acquire() as connection:
                await connection.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        finally:
            await database.disconnect()
