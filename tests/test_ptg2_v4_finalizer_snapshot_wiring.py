from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_shared_snapshot_publish as snapshot_publish
from process.ptg_parts import ptg2_v4_finalizer_publish as finalizer_publish
from process.ptg_parts import ptg2_v4_finalizer_native as finalizer_native
from process.ptg_parts import ptg2_v4_snapshot_maps as snapshot_maps
from process.ptg_parts.ptg2_v4_finalizer_maps import (
    PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS,
)

class _NativeReceipt:
    def __init__(self) -> None:
        self.cleanup_calls = 0

    def cleanup(self) -> None:
        self.cleanup_calls += 1

    def manifest(self):
        return {"contract": "native_unique_shared_block_copy_v2"}


def _stage_request(tmp_path, *, packed: bool):
    stage_table = (
        snapshot_publish._finalizer_block_stage_name(17, "build-token")
        if packed
        else "task_finalizer_stage"
    )
    return snapshot_publish._FinalizerBlockStageRequest(
        schema_name="mrf",
        stage_table=stage_table,
        snapshot_key=17,
        build_token="build-token",
        expected_generation=(
            snapshot_maps.PTG2_V4_SHARED_GENERATION
            if packed
            else "shared_blocks_v3"
        ),
        finalizer_summary={},
        serving_summary={
            "artifact_record_counts": {
                object_kind: 1
                for object_kind in PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS
                if object_kind != "by_code_price_dictionary"
            }
        },
        price_summary={
            "artifact_record_counts": {"by_code_price_dictionary": 1}
        },
        work_directory=tmp_path,
        packed=packed,
        progress_callback=None,
    )


def _install_stage_mocks(monkeypatch: pytest.MonkeyPatch):
    copy_calls: list[dict[str, object]] = []

    async def copy_finalizer_block(*_args, **kwargs):
        copy_calls.append(dict(kwargs))
        return SimpleNamespace()

    create_stage = AsyncMock()
    drop_stage = AsyncMock()

    @asynccontextmanager
    async def stage_guard(**_kwargs):
        yield

    monkeypatch.setattr(snapshot_publish, "create_shared_block_stage", create_stage)
    monkeypatch.setattr(snapshot_publish, "_copy_finalizer_block", copy_finalizer_block)
    monkeypatch.setattr(snapshot_publish, "_finalizer_block_stage_guard", stage_guard)
    monkeypatch.setattr(snapshot_publish.db, "status", drop_stage)
    return copy_calls, create_stage, drop_stage


@pytest.mark.asyncio
async def test_v4_finalizer_stage_uses_native_receipt_and_cleans_it(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    copy_calls, create_stage, drop_stage = _install_stage_mocks(monkeypatch)
    receipt = _NativeReceipt()
    monkeypatch.setattr(
        finalizer_native,
        "pack_v4_finalizer_copies",
        AsyncMock(return_value=receipt),
    )
    publication = SimpleNamespace(mapping_count=6)
    publish = AsyncMock(return_value=publication)
    monkeypatch.setattr(finalizer_publish, "publish_v4_finalizer_maps", publish)

    stage_publication = await snapshot_publish._publish_finalizer_block_stage(
        _stage_request(tmp_path, packed=True)
    )

    stage_table = snapshot_publish._finalizer_block_stage_name(17, "build-token")
    assert stage_publication.publication is publication
    assert copy_calls == []
    publish.assert_awaited_once_with(
        receipt,
        schema_name="mrf",
        stage_table=stage_table,
        snapshot_key=17,
        build_token="build-token",
    )
    assert receipt.cleanup_calls == 1
    create_stage.assert_awaited_once_with(
        schema_name="mrf", stage_table=stage_table
    )
    assert f'DROP TABLE IF EXISTS "mrf"."{stage_table}"' in (
        drop_stage.await_args.args[0]
    )


@pytest.mark.asyncio
async def test_v4_finalizer_stage_cleans_native_receipt_on_publish_failure(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _copy_calls, _create_stage, drop_stage = _install_stage_mocks(monkeypatch)
    receipt = _NativeReceipt()
    monkeypatch.setattr(
        finalizer_native,
        "pack_v4_finalizer_copies",
        AsyncMock(return_value=receipt),
    )
    monkeypatch.setattr(
        finalizer_publish,
        "publish_v4_finalizer_maps",
        AsyncMock(side_effect=RuntimeError("publication failed")),
    )

    with pytest.raises(RuntimeError, match="publication failed"):
        await snapshot_publish._publish_finalizer_block_stage(
            _stage_request(tmp_path, packed=True)
        )

    assert receipt.cleanup_calls == 1
    drop_stage.assert_awaited_once()


@pytest.mark.asyncio
async def test_v3_finalizer_stage_keeps_relational_publisher(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    copy_calls, _create_stage, _drop_stage = _install_stage_mocks(monkeypatch)
    relational_publication = SimpleNamespace(mapping_count=6)
    publish = AsyncMock(return_value=relational_publication)
    monkeypatch.setattr(snapshot_publish, "publish_shared_block_stage", publish)

    result = await snapshot_publish._publish_finalizer_block_stage(
        _stage_request(tmp_path, packed=False)
    )

    assert result.publication is relational_publication
    assert len(copy_calls) == 2
    assert all("packed_map" not in call for call in copy_calls)
    publish.assert_awaited_once_with(
        schema_name="mrf",
        stage_table="task_finalizer_stage",
        snapshot_key=17,
        build_token="build-token",
        expected_generation="shared_blocks_v3",
    )


class _ReservationResult:
    def first(self):
        return SimpleNamespace(_mapping={"snapshot_key": 17})


class _ReservationSession:
    def __init__(self) -> None:
        self.sql = ""
        self.params: dict[str, object] = {}

    async def execute(self, statement, params):
        self.sql = str(statement)
        self.params = dict(params)
        return _ReservationResult()


@pytest.mark.asyncio
async def test_reservation_load_selects_finalizer_root_and_mixed_storage_fence() -> None:
    session = _ReservationSession()

    loaded = await snapshot_maps._load_v4_layout_reservation(
        session,
        schema='"mrf"',
        fingerprint=b"f" * 32,
    )

    assert loaded == {"snapshot_key": 17}
    assert "LEFT JOIN \"mrf\".ptg2_v4_finalizer_map_root" in session.sql
    assert "finalizer_relational_mapping_present" in session.sql
    assert session.params["finalizer_object_kinds"] == (
        PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS
    )


@pytest.mark.asyncio
async def test_seal_reuse_load_selects_finalizer_root_and_mixed_storage_fence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _ReservationSession()
    digest_lock = AsyncMock()
    monkeypatch.setattr(snapshot_maps, "acquire_layout_digest_lock", digest_lock)

    loaded = await snapshot_maps._load_reusable_v4_layout(
        session,
        schema='"mrf"',
        snapshot_key=19,
        mapping_digest=b"m" * 32,
        support_digest=b"s" * 32,
    )

    assert loaded == {"snapshot_key": 17}
    assert "LEFT JOIN \"mrf\".ptg2_v4_finalizer_map_root" in session.sql
    assert "finalizer_relational_mapping_present" in session.sql
    assert session.params["finalizer_object_kinds"] == (
        PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS
    )
    digest_lock.assert_awaited_once_with(
        session,
        digest=b"m" * 32,
        purpose="V4 mapping digest",
    )


def test_seal_reuse_authenticates_finalizer_root_after_graph_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest_by_field = {"serving_index": {}}
    map_summary = SimpleNamespace(object_kind_count=6)
    metadata_summary = object()
    finalizer_validation_calls = []
    monkeypatch.setattr(
        snapshot_maps,
        "_sealed_root_summaries",
        lambda _reusable: (manifest_by_field, map_summary, metadata_summary),
    )
    monkeypatch.setattr(snapshot_maps, "_require_matching_summaries", lambda *_a, **_k: None)
    monkeypatch.setattr(snapshot_maps, "_validate_v4_manifest_root", lambda *_a, **_k: None)
    monkeypatch.setattr(
        snapshot_maps,
        "_validate_reused_finalizer_root",
        lambda reusable, manifest: finalizer_validation_calls.append(
            (reusable, manifest)
        ),
    )
    reusable_by_field = {
        "root_state": "complete",
        "root_format_version": snapshot_maps.PTG2_V4_MAP_FORMAT_VERSION,
        "map_format": snapshot_maps.PTG2_V4_MAP_FORMAT,
        "representation": "pattern_v1",
        "projection_id_scope": snapshot_maps.PTG2_V4_PROJECTION_ID_SCOPE,
        "object_kind_count": 6,
    }

    snapshot_maps._validate_reusable_v4_layout(
        reusable_by_field,
        representation="pattern_v1",
        observed_summary=map_summary,
        observed_metadata=metadata_summary,
    )

    assert finalizer_validation_calls == [
        (reusable_by_field, manifest_by_field)
    ]


@pytest.mark.asyncio
async def test_seal_reuse_validates_before_refresh_or_canonical_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _reject_reusable_layout(*_args, **_kwargs):
        raise RuntimeError("invalid root")

    monkeypatch.setattr(
        snapshot_maps,
        "_load_reusable_v4_layout",
        AsyncMock(return_value={"snapshot_key": 17}),
    )
    monkeypatch.setattr(
        snapshot_maps,
        "_validate_reusable_v4_layout",
        _reject_reusable_layout,
    )
    refresh = AsyncMock()
    publish_fingerprint = AsyncMock()
    defer_cleanup = AsyncMock()
    monkeypatch.setattr(snapshot_maps, "_refresh_reusable_v4_layout", refresh)
    monkeypatch.setattr(snapshot_maps, "publish_layout_fingerprint", publish_fingerprint)
    monkeypatch.setattr(snapshot_maps, "_defer_duplicate_v4_cleanup", defer_cleanup)
    state = SimpleNamespace(
        schema='"mrf"',
        schema_name="mrf",
        snapshot_key=19,
        summary=SimpleNamespace(map_digest=b"m" * 32),
        support_digest=b"s" * 32,
        representation="pattern_v1",
        metadata=object(),
    )

    with pytest.raises(RuntimeError, match="invalid root"):
        await snapshot_maps._reuse_v4_layout_if_available(object(), state)

    refresh.assert_not_awaited()
    publish_fingerprint.assert_not_awaited()
    defer_cleanup.assert_not_awaited()


def test_finalizer_block_stage_names_bind_exact_attempt_identity():
    """Separate stale tokens while retaining retry-reclaimable stage names."""

    owner_stage = snapshot_publish._finalizer_block_stage_name(17, "owner-token")
    assert owner_stage == snapshot_publish._finalizer_block_stage_name(
        17, "owner-token"
    )
    assert owner_stage != snapshot_publish._finalizer_block_stage_name(
        17, "stale-token"
    )
    assert owner_stage != snapshot_publish._finalizer_block_stage_name(
        18, "owner-token"
    )
