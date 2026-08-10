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


def _hash(value: int) -> bytes:
    return bytes([value]) * 32


def _patch_v4_abandonment_pipeline(
    monkeypatch,
    *,
    inventory,
    final_stats,
    dense_delete_effect=None,
):
    """Replace the bounded pipeline and return its observable async mocks."""

    pipeline_mock_by_name = {
        "shared_tables": AsyncMock(return_value=True),
        "map_tables": AsyncMock(return_value=True),
        "inventory": AsyncMock(return_value=inventory),
        "queue_batch": AsyncMock(
            side_effect=lambda _connection, **kwargs: len(
                kwargs["block_hashes"]
            )
        ),
        "delete_dense": (
            AsyncMock(side_effect=dense_delete_effect)
            if dense_delete_effect is not None
            else AsyncMock(return_value=0)
        ),
        "finalize": AsyncMock(return_value=final_stats),
    }
    monkeypatch.setattr(
        shared_gc,
        "_has_shared_tables",
        pipeline_mock_by_name["shared_tables"],
    )
    monkeypatch.setattr(
        shared_gc,
        "_has_v4_map_tables",
        pipeline_mock_by_name["map_tables"],
    )
    monkeypatch.setattr(
        shared_gc,
        "_owned_v4_inventory",
        pipeline_mock_by_name["inventory"],
    )
    monkeypatch.setattr(
        shared_gc,
        "_queue_owned_v4_candidate_batch",
        pipeline_mock_by_name["queue_batch"],
    )
    monkeypatch.setattr(
        shared_gc,
        "_delete_owned_v4_dense_batch",
        pipeline_mock_by_name["delete_dense"],
    )
    monkeypatch.setattr(
        shared_gc,
        "_finalize_owned_v4_abandonment",
        pipeline_mock_by_name["finalize"],
    )
    return pipeline_mock_by_name

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
        eligible_layout_keys: list[int] = []
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
                eligible_layout_keys.append(snapshot_key)
        eligible_layout_keys.sort(
            key=lambda key: (self.layouts[key]["created_at"], key)
        )
        return (
            eligible_layout_keys
            if limit is None
            else eligible_layout_keys[: int(limit)]
        )

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

    def _metadata_rows(self, statement: str, _params: dict[str, object]):
        if "FROM information_schema.tables" in statement:
            return [{"table_name": table_name} for table_name in sorted(self.present_tables)]
        if "FROM \"mrf\".ptg2_snapshot" in statement and "AS involved" in statement:
            return [{"involved": self.manifest_involved}]
        if "FROM \"mrf\".ptg2_v3_snapshot_binding" in statement and "AS involved" in statement:
            return [{"involved": bool(self.bindings)}]
        if "FROM \"mrf\".ptg2_v3_snapshot_scope" in statement and "AS involved" in statement:
            return [{"involved": bool(self.scopes)}]
        return None

    def _eligible_rows(self, statement: str, params: dict[str, object]):
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
                    int(layout_key) for layout_key in params.get("layout_keys", ())
                }
                layout_keys = [
                    snapshot_key
                    for snapshot_key in layout_keys
                    if snapshot_key in allowed_layout_keys
                ]
            return [{"snapshot_key": snapshot_key} for snapshot_key in layout_keys]
        return None

    def _release_rows(self, statement: str, params: dict[str, object]):
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
        return None

    def _candidate_rows(self, statement: str, params: dict[str, object]):
        if "SELECT candidate.block_hash" in statement:
            assert "payload" not in statement
            eligible_block_rows = []
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
                    eligible_block_rows.append(
                        {
                            "block_hash": block_hash,
                            "stored_byte_count": stored_bytes,
                            "eligible_at": eligible_at,
                        }
                    )
            return eligible_block_rows[: int(params["max_rows"])]
        return None

    def _deleted_block_rows(self, statement: str, params: dict[str, object]):
        if "DELETE FROM \"mrf\".ptg2_v3_block AS block" in statement:
            assert "NOT EXISTS" in statement
            if self.rereference_on_delete is not None:
                self.mappings.add(self.rereference_on_delete)
                self.rereference_on_delete = None
            mapped_hashes = {block_hash for _snapshot_key, block_hash in self.mappings}
            deleted_blocks = []
            for block_hash in params["block_hashes"]:
                if block_hash in mapped_hashes or block_hash not in self.blocks:
                    continue
                stored_bytes = self.blocks.pop(block_hash)
                self.candidates.pop(block_hash, None)
                deleted_blocks.append(
                    {"block_hash": block_hash, "stored_byte_count": stored_bytes}
                )
            return deleted_blocks
        return None

    async def all(self, statement: str, **params):
        """Emulate the executor SQL used by shared-layout GC tests."""

        self.calls.append((statement, dict(params)))
        for handler in (
            self._metadata_rows,
            self._eligible_rows,
            self._release_rows,
            self._candidate_rows,
            self._deleted_block_rows,
        ):
            result = handler(statement, params)
            if result is not None:
                return result
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

class _SourceGCProjectionExecutor:
    async def all(self, statement, **params):
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
            assert params["removing_snapshot_ids"] == ["shared-old"]
            return [{
                "logical_layout_count": 1,
                "candidate_hash_count": 1,
                "stored_bytes": 25,
            }]
        raise AssertionError(statement)
