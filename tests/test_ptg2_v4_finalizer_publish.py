from __future__ import annotations

import asyncio
import hashlib
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts.ptg2_v4_finalizer_map_sidecars import (
    PackedMapArtifact,
    PackedMapNativeReceipt,
    PackedMapSidecars,
)
from process.ptg_parts.ptg2_v4_finalizer_maps import (
    PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS,
)
from process.ptg_parts import ptg2_v4_finalizer_publish as publish


def _artifact(path, name: str, rows: int) -> PackedMapArtifact:
    payload = f"{name}-payload".encode()
    artifact_path = path / name
    artifact_path.write_bytes(payload)
    return PackedMapArtifact(
        path=artifact_path,
        row_count=rows,
        byte_count=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
    )


def _sidecars(tmp_path) -> tuple[PackedMapSidecars, PackedMapSidecars]:
    lanes = []
    for lane_no, object_kinds in enumerate(
        (
            PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS[:3],
            PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS[3:],
        )
    ):
        directory = tmp_path / f"lane-{lane_no}"
        directory.mkdir()
        lanes.append(
            PackedMapSidecars(
                directory=directory,
                target_blocks=_artifact(directory, "target-blocks", 4),
                map_blocks=_artifact(directory, "map-blocks", 3),
                map_packs=_artifact(directory, "map-packs", 3),
                object_kinds=tuple(sorted(object_kinds)),
                map_pack_count=3,
                coordinate_count=6,
                target_block_count=4,
                entry_count=9,
                logical_byte_count=12,
                stored_byte_count=10,
                stored_map_byte_count=20,
                kind_digests=tuple(
                    (object_kind, hashlib.sha256(object_kind.encode()).digest())
                    for object_kind in sorted(object_kinds)
                ),
                source_copy_bytes=100,
                target_stored_byte_count=8,
            )
        )
    return tuple(lanes)


def _receipt(
    tmp_path,
    sidecars: tuple[PackedMapSidecars, ...] | None = None,
) -> PackedMapNativeReceipt:
    return PackedMapNativeReceipt(
        directory=tmp_path,
        sidecars=sidecars or _sidecars(tmp_path),
        canonical_mapping_digest=b"c" * 32,
        canonical_byte_count=120,
        target_identity_digest=b"t" * 32,
        elapsed_seconds=0.5,
    )


def test_combines_exact_disjoint_lanes_and_emits_explicit_manifest(tmp_path) -> None:
    receipt = _receipt(tmp_path)
    sidecars = receipt.sidecars
    publication = publish._combined_publication(receipt)

    assert publication.mapping_count == 12
    assert publication.unique_block_count == publication.target_block_count == 8
    assert publication.map_pack_count == 6
    assert publication.object_kinds == PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS
    assert len(publication.map_digest) == 32
    assert publication.manifest() == {
        "contract": "packed_finalizer_map_v2",
        "map_format": "packed_coordinate_hash_v1",
        "map_digest": publication.map_digest.hex(),
        "object_kinds": list(PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS),
        "object_kind_count": 6,
        "map_pack_count": 6,
        "coordinate_count": 12,
        "entry_count": 18,
        "logical_byte_count": 24,
        "stored_map_byte_count": 40,
        "target_block_count": 8,
        "canonical_mapping_digest": (b"c" * 32).hex(),
        "canonical_byte_count": 120,
        "target_identity_digest": (b"t" * 32).hex(),
    }

    overlapping = replace(
        sidecars[1],
        object_kinds=sidecars[0].object_kinds,
        kind_digests=sidecars[0].kind_digests,
    )
    with pytest.raises(ValueError, match="overlap"):
        publish._combined_publication(
            replace(receipt, sidecars=(sidecars[0], overlapping))
        )


@pytest.mark.asyncio
async def test_binary_copy_authenticates_every_consumed_artifact_byte(
    tmp_path,
    monkeypatch,
) -> None:
    artifact = _artifact(tmp_path, "artifact.copy", 1)
    copied = bytearray()

    class Driver:
        async def copy_to_table(self, _table, *, source, **_options):
            while chunk := source.read(3):
                copied.extend(chunk)

    class Acquire:
        async def __aenter__(self):
            return SimpleNamespace(
                raw_connection=SimpleNamespace(driver_connection=Driver())
            )

        async def __aexit__(self, *_args):
            return None

    monkeypatch.setattr(publish.db, "acquire", lambda: Acquire())
    await publish._copy_artifact(
        artifact,
        schema_name="mrf",
        stage_table="stage",
        columns=("payload",),
    )
    assert bytes(copied) == artifact.path.read_bytes()

    with pytest.raises(RuntimeError, match="changed during publication"):
        await publish._copy_artifact(
            replace(artifact, sha256="0" * 64),
            schema_name="mrf",
            stage_table="stage",
            columns=("payload",),
        )


@pytest.mark.asyncio
async def test_orchestrates_atomic_publish_and_drops_only_pack_stage(
    tmp_path,
    monkeypatch,
) -> None:
    receipt = _receipt(tmp_path)
    sidecars = receipt.sidecars
    stage = AsyncMock()
    atomic = AsyncMock()
    status = AsyncMock()
    monkeypatch.setattr(publish, "_stage_sidecars", stage)
    monkeypatch.setattr(publish, "_publish_atomic_map", atomic)
    monkeypatch.setattr(publish.db, "status", status)

    publication = await publish.publish_v4_finalizer_maps(
        receipt,
        schema_name="mrf",
        stage_table="target_stage",
        snapshot_key=7,
        build_token="build-token",
    )

    assert publication.mapping_count == 12
    atomic.assert_awaited_once()
    assert atomic.await_args.kwargs["snapshot_key"] == 7
    dropped = status.await_args.args[0]
    assert "DROP TABLE IF EXISTS" in dropped
    assert dropped == (
        'DROP TABLE IF EXISTS "mrf".'
        f'"{publish._pack_stage_name("target_stage")}";'
    )
    assert all(artifact.path.exists() for lane in sidecars for artifact in (
        lane.target_blocks,
        lane.map_blocks,
        lane.map_packs,
    ))


@pytest.mark.asyncio
async def test_atomic_failure_cleans_pack_stage(
    tmp_path,
    monkeypatch,
) -> None:
    receipt = _receipt(tmp_path)
    atomic = AsyncMock(side_effect=RuntimeError("CAS failed"))
    status = AsyncMock()
    monkeypatch.setattr(publish, "_stage_sidecars", AsyncMock())
    monkeypatch.setattr(publish, "_publish_atomic_map", atomic)
    monkeypatch.setattr(publish.db, "status", status)

    with pytest.raises(RuntimeError, match="CAS failed"):
        await publish.publish_v4_finalizer_maps(
            receipt,
            schema_name="mrf",
            stage_table="target_stage",
            snapshot_key=7,
            build_token="build-token",
        )

    atomic.assert_awaited_once()
    assert "DROP TABLE IF EXISTS" in status.await_args.args[0]


@pytest.mark.asyncio
async def test_repeated_cancel_drains_pack_stage_cleanup(
    tmp_path,
    monkeypatch,
) -> None:
    receipt = _receipt(tmp_path)
    staging_started = asyncio.Event()
    cleanup_started = asyncio.Event()
    cleanup_release = asyncio.Event()
    cleanup_finished = asyncio.Event()

    async def stage(*_args, **_kwargs):
        staging_started.set()
        await asyncio.Event().wait()

    async def status(_statement):
        cleanup_started.set()
        await cleanup_release.wait()
        cleanup_finished.set()

    monkeypatch.setattr(publish, "_stage_sidecars", stage)
    monkeypatch.setattr(publish.db, "status", status)
    task = asyncio.create_task(
        publish.publish_v4_finalizer_maps(
            receipt,
            schema_name="muk",
            stage_table="target_stage",
            snapshot_key=7,
            build_token="build-token",
        )
    )

    await asyncio.wait_for(staging_started.wait(), timeout=1)
    task.cancel()
    await asyncio.wait_for(cleanup_started.wait(), timeout=1)
    task.cancel()
    cleanup_release.set()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=1)
    assert cleanup_finished.is_set()
