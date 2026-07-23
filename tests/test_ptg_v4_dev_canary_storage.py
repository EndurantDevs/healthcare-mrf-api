# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Sequence

import pytest

from process.ptg_parts.ptg2_shared_blocks import SharedBlock
from process.ptg_parts.ptg2_v4_snapshot_maps import (
    PTG2_V4_MAP_BLOCK_KIND,
    encode_v4_snapshot_map_pack,
)
from scripts.ptg_v4_dev_canary_cas import collect_cas_evidence


class _AsyncRecords:
    def __init__(self, records: Sequence[dict[str, Any]]) -> None:
        self._iterator = iter(records)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._iterator)
        except StopIteration as exc:
            raise StopAsyncIteration from exc


class _FakeCasConnection:
    def __init__(
        self,
        *,
        map_record_by_field: dict[str, Any],
        block_record_by_hash: dict[bytes, dict[str, Any]],
    ) -> None:
        self.map_record_by_field = map_record_by_field
        self.block_record_by_hash = block_record_by_hash

    def cursor(self, statement: str):
        if "ptg2_v3_snapshot_block" in statement:
            return _AsyncRecords([])
        if "ptg2_v4_snapshot_map_pack" in statement:
            return _AsyncRecords([self.map_record_by_field])
        raise AssertionError("unexpected cursor query")

    async def fetchval(self, statement: str):
        assert "SUM(pg_column_size(block))" in statement
        return sum(
            int(block_record["row_bytes"])
            for block_record in self.block_record_by_hash.values()
        )

    async def fetch(self, statement: str, block_hashes: list[bytes]):
        assert "block_hash = ANY" in statement
        return [
            self.block_record_by_hash[block_hash]
            for block_hash in block_hashes
            if block_hash in self.block_record_by_hash
        ]


def _target_block() -> SharedBlock:
    """Build the CAS target that has no direct snapshot-block reference."""

    target_block = SharedBlock(
        object_kind="v4_pattern_groups_members_v1",
        block_key=4,
        fragment_no=0,
        entry_count=3,
        codec="none",
        raw_byte_count=12,
        payload=b"\x01\x00\x00\x00\x02\x00\x00\x00\x03\x00\x00\x00",
    )
    return target_block


def _map_fixture(target_block: SharedBlock) -> tuple[SharedBlock, dict[str, Any]]:
    """Build one authenticated coordinate-map block and its database row."""

    target_reference = target_block.reference()
    map_payload = encode_v4_snapshot_map_pack(
        target_reference.object_kind,
        [target_reference],
    )
    map_block = SharedBlock(
        object_kind=PTG2_V4_MAP_BLOCK_KIND,
        block_key=0,
        fragment_no=0,
        entry_count=1,
        codec="none",
        raw_byte_count=len(map_payload),
        payload=map_payload,
    )
    return map_block, {
        "snapshot_key": 17,
        "object_kind": target_reference.object_kind,
        "pack_no": 0,
        "first_block_key": 4,
        "first_fragment_no": 0,
        "last_block_key": 4,
        "last_fragment_no": 0,
        "coordinate_count": 1,
        "entry_count": 3,
        "map_block_hash": map_block.block_hash,
        "map_format_version": map_block.format_version,
        "map_object_kind": PTG2_V4_MAP_BLOCK_KIND,
        "map_codec": "none",
        "map_entry_count": 1,
        "map_raw_byte_count": len(map_payload),
        "map_stored_byte_count": len(map_payload),
        "map_payload": map_payload,
    }


def _cas_only_fixture() -> tuple[_FakeCasConnection, datetime]:
    """Build a snapshot whose V4 target is reachable only through its map."""

    imported_at = datetime(2026, 7, 23, tzinfo=timezone.utc)
    target_block = _target_block()
    map_block, map_record_by_field = _map_fixture(target_block)
    block_record_by_hash = {
        map_block.block_hash: {
            "block_hash": map_block.block_hash,
            "object_kind": map_block.object_kind,
            "entry_count": map_block.entry_count,
            "created_at": imported_at,
            "row_bytes": 180,
        },
        target_block.block_hash: {
            "block_hash": target_block.block_hash,
            "object_kind": target_block.object_kind,
            "entry_count": target_block.entry_count,
            "created_at": imported_at,
            "row_bytes": 120,
        },
    }
    return (
        _FakeCasConnection(
            map_record_by_field=map_record_by_field,
            block_record_by_hash=block_record_by_hash,
        ),
        imported_at,
    )


@pytest.mark.asyncio
async def test_storage_counts_v4_blocks_reachable_only_through_map_payload() -> None:
    connection, imported_at = _cas_only_fixture()

    evidence_by_field = await collect_cas_evidence(
        connection,
        schema_name="mrf",
        snapshot_key=17,
        import_started_at=imported_at - timedelta(seconds=1),
        import_finished_at=imported_at + timedelta(seconds=1),
    )

    assert evidence_by_field["bytes"]["distinct_referenced_block_count"] == 2
    assert evidence_by_field["bytes"]["allocated_all_snapshot_row_bytes"] == 300
    assert evidence_by_field["object_kind_counts"] == {
        PTG2_V4_MAP_BLOCK_KIND: 1,
        "v4_pattern_groups_members_v1": 1,
    }
    assert evidence_by_field["map_cas_block_count"] == 1
    assert (
        evidence_by_field["reference_source"]
        == "direct_rows_plus_authenticated_v4_map_payloads"
    )
    assert (
        evidence_by_field["reference_population"]
        == "published_sealed_layout_keys"
    )


@pytest.mark.asyncio
async def test_storage_rejects_map_payload_whose_cas_hash_is_tampered() -> None:
    connection, imported_at = _cas_only_fixture()
    connection.map_record_by_field["map_block_hash"] = b"x" * 32

    with pytest.raises(RuntimeError, match="CAS hash does not match"):
        await collect_cas_evidence(
            connection,
            schema_name="mrf",
            snapshot_key=17,
            import_started_at=imported_at - timedelta(seconds=1),
            import_finished_at=imported_at + timedelta(seconds=1),
        )
