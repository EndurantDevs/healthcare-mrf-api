"""Reviewer-oriented coverage for packed-map persistence boundaries."""

from __future__ import annotations

from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from tests.ptg2_v4_coverage_support import (
    PTG2_V3_SHARED_FORMAT_VERSION,
    _Result,
    _ScriptedSession,
    _metadata,
    _owner_row,
    _reference,
    _relation_row,
    _summary,
    snapshot_manifest_fixture,
    snapshot_maps,
    synthetic_adaptive_layout_decision,
)


class _Rows(_Result):
    def all(self):
        return self.rows


class _ScalarSession:
    def __init__(self, *values):
        self.values = list(values)

    async def scalar(self, _statement, _parameters):
        return self.values.pop(0)


def test_progress_context_and_manifest_fail_closed_boundaries() -> None:
    callback = lambda _phase, _count: None
    with snapshot_maps.observe_v4_seal_progress(callback):
        assert snapshot_maps._V4_SEAL_PROGRESS_CALLBACK.get() is callback
    assert snapshot_maps._V4_SEAL_PROGRESS_CALLBACK.get() is None

    with pytest.raises(ValueError, match="object_kind"):
        snapshot_maps.encode_v4_snapshot_map_pack("x" * 65, (_reference("x", 0, 0),))
    normalized, serving_index = snapshot_maps._manifest_copy_with_index({})
    assert normalized == {} and serving_index == {}

    metadata = _metadata()
    with pytest.raises(ValueError, match="resource admission"):
        snapshot_maps._validated_graph_resources(
            replace(metadata, provider_graph_resources={})
        )
    resources_by_field = dict(metadata.provider_graph_resources)
    resources_by_field["compressed_acquisition_bytes"] = 0
    with pytest.raises(ValueError, match="resource admission"):
        snapshot_maps._validated_graph_resources(
            replace(metadata, provider_graph_resources=resources_by_field)
        )
    with pytest.raises(ValueError, match="differs"):
        snapshot_maps._validated_adaptive_layout(
            synthetic_adaptive_layout_decision("direct_v1"),
            representation="pattern_v1",
        )
    with pytest.raises(ValueError, match="compiler graph"):
        snapshot_maps._serving_binary_v4_map(
            {},
            representation="pattern_v1",
            summary=_summary(),
            metadata=metadata,
        )


def test_manifest_root_and_pack_identity_rejections() -> None:
    summary = _summary()
    manifest, metadata = snapshot_manifest_fixture(summary)
    with pytest.raises(RuntimeError, match="no serving_index"):
        snapshot_maps._validate_v4_manifest_root(
            {},
            representation="pattern_v1",
            summary=summary,
            metadata=metadata,
        )
    manifest["serving_index"]["serving_binary"]["provider_graph_v4"][
        "contract"
    ] = "drifted"
    with pytest.raises(RuntimeError, match="provider graph"):
        snapshot_maps._validate_v4_manifest_root(
            manifest,
            representation="pattern_v1",
            summary=summary,
            metadata=metadata,
        )

    reference = _reference("v4_relation_members_v1", 0, 0)
    pack = snapshot_maps._make_map_pack(
        object_kind=reference.object_kind,
        pack_no=0,
        references=(reference,),
    )
    pack_row_by_field = {
        **snapshot_maps._map_pack_row(pack, 1),
        "map_format_version": PTG2_V3_SHARED_FORMAT_VERSION,
        "map_object_kind": snapshot_maps.PTG2_V4_MAP_BLOCK_KIND,
        "map_codec": "none",
        "map_entry_count": 2,
        "map_raw_byte_count": len(pack.map_block.payload),
        "map_stored_byte_count": len(pack.map_block.payload),
        "map_payload": pack.map_block.payload,
    }
    with pytest.raises(RuntimeError, match="entry count"):
        snapshot_maps._decode_persisted_map_payload(
            pack_row_by_field,
            object_kind=reference.object_kind,
        )


@pytest.mark.asyncio
async def test_persisted_summary_reports_progress(monkeypatch) -> None:
    reference = _reference("v4_relation_members_v1", 0, 0)
    pack = snapshot_maps._make_map_pack(
        object_kind=reference.object_kind,
        pack_no=0,
        references=(reference,),
    )
    pack_row_by_field = {
        **snapshot_maps._map_pack_row(pack, 1),
        "map_format_version": PTG2_V3_SHARED_FORMAT_VERSION,
        "map_object_kind": snapshot_maps.PTG2_V4_MAP_BLOCK_KIND,
        "map_codec": "none",
        "map_entry_count": 1,
        "map_raw_byte_count": len(pack.map_block.payload),
        "map_stored_byte_count": len(pack.map_block.payload),
        "map_payload": pack.map_block.payload,
    }
    batches = [[pack_row_by_field], []]

    async def load_rows(*_args, **_kwargs):
        return batches.pop(0)

    monkeypatch.setattr(snapshot_maps, "_load_persisted_map_rows", load_rows)
    monkeypatch.setattr(
        snapshot_maps,
        "_load_target_metadata",
        AsyncMock(
            return_value={
                reference.block_hash: (
                    reference.object_kind,
                    reference.entry_count,
                    reference.raw_byte_count,
                )
            }
        ),
    )
    progress = []
    await snapshot_maps.summarize_persisted_v4_snapshot_maps(
        object(),
        schema_name="mrf",
        snapshot_key=1,
        progress_callback=lambda phase, count: progress.append((phase, count)),
    )
    assert progress == [("seal_map_packs", 1), ("seal_map_coordinates", 1)]


@pytest.mark.asyncio
async def test_target_metadata_rejects_incompatible_cas() -> None:
    session = _ScriptedSession(
        _Result(
            rows=(
                {
                    "block_hash": b"h" * 32,
                    "format_version": 99,
                    "object_kind": "kind",
                    "codec": "none",
                    "entry_count": 1,
                    "raw_byte_count": 1,
                    "stored_byte_count": 1,
                },
            )
        )
    )
    with pytest.raises(RuntimeError, match="incompatible format"):
        await snapshot_maps._load_target_metadata(
            session,
            schema='"mrf"',
            target_hashes={b"h" * 32},
        )


@pytest.mark.asyncio
async def test_locator_pages_reject_missing_and_ambiguous_coordinates() -> None:
    reference = _reference("locator", 4, 0)
    map_payload = snapshot_maps.encode_v4_snapshot_map_pack("locator", (reference,))
    for rows, message in (
        ((), "missing"),
        (
            (
                {"object_kind": "locator", "payload": map_payload},
                {"object_kind": "locator", "payload": map_payload},
            ),
            "ambiguous",
        ),
    ):
        with pytest.raises(RuntimeError, match=message):
            await snapshot_maps._load_locator_coordinates(
                _ScriptedSession(_Result(rows=rows)),
                schema='"mrf"',
                snapshot_key=1,
                requested_by_kind={"locator": {4}},
            )


@pytest.mark.asyncio
async def test_dense_metadata_range_reports_progress_and_rejects_holes() -> None:
    progress = []
    assert (
        await snapshot_maps._load_dense_metadata_count(
            _ScalarSession(1, 2),
            schema='"mrf"',
            snapshot_key=1,
            table_name="ptg2_v4_npi",
            key_name="npi_key",
            progress_callback=lambda phase, count: progress.append((phase, count)),
        )
        == 2
    )
    assert progress == [("seal_metadata_rows", 2), ("seal_metadata_batches", 1)]
    with pytest.raises(RuntimeError, match="not dense"):
        await snapshot_maps._load_dense_metadata_count(
            _ScalarSession(0, 0),
            schema='"mrf"',
            snapshot_key=1,
            table_name="ptg2_v4_npi",
            key_name="npi_key",
            progress_callback=None,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("row", "map_kinds", "message"),
    (
        (
            {**_relation_row(), "owner_base": 2**63, "owner_count": 1},
            set(),
            "bigint",
        ),
        (_relation_row(), set(), "missing map kind"),
        (
            {**_relation_row(), "logical_member_count": 1, "vector_member_count": 2},
            {
                _relation_row()["locator_object_kind"],
                _relation_row()["member_object_kind"],
            },
            "logical count",
        ),
    ),
)
async def test_relation_metadata_rejects_invalid_ranges(
    row, map_kinds, message
) -> None:
    with pytest.raises(RuntimeError, match=message):
        await snapshot_maps._load_relation_metadata(
            _ScriptedSession(_Result(rows=(row,))),
            schema='"mrf"',
            snapshot_key=1,
            map_object_kinds=map_kinds,
        )


@pytest.mark.asyncio
async def test_heavy_owner_loader_handles_missing_and_duplicate_fragments() -> None:
    missing = await snapshot_maps._load_heavy_owners(
        _ScriptedSession(
            _Result(
                rows=(
                    {
                        **_owner_row("r", 7),
                        "map_block_hash": None,
                        "payload": None,
                    },
                )
            )
        ),
        schema='"mrf"',
        snapshot_key=1,
    )
    assert missing[("r", 7)]["fragments"] == set()

    object_kind = _owner_row("r", 7)["object_kind"]
    reference = _reference(object_kind, 7, 0)
    map_payload = snapshot_maps.encode_v4_snapshot_map_pack(object_kind, (reference,))
    repeated_owner_by_field = {
        **_owner_row("r", 7),
        "map_block_hash": b"h" * 32,
        "payload": map_payload,
    }
    with pytest.raises(RuntimeError, match="duplicated"):
        await snapshot_maps._load_heavy_owners(
            _ScriptedSession(
                _Result(rows=(repeated_owner_by_field, repeated_owner_by_field))
            ),
            schema='"mrf"',
            snapshot_key=1,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("rows", "message"),
    (
        ((), "missing or duplicated"),
        (({"unexpected": 1},), "columns changed"),
        (
            (
                {
                    "compressed_acquisition_bytes": 0,
                    "input_factor_bytes": 0,
                    "factor_edge_count": 0,
                    "empty_npi_tin_only_normalization_count": 0,
                },
            ),
            "invalid",
        ),
    ),
)
async def test_graph_resource_rows_fail_closed(rows, message) -> None:
    with pytest.raises(RuntimeError, match=message):
        await snapshot_maps._load_provider_graph_resources(
            _ScriptedSession(_Rows(rows=rows)),
            schema='"mrf"',
            snapshot_key=1,
        )


@pytest.mark.asyncio
async def test_relation_and_owner_publication_reject_bad_batches_and_conflicts(
    monkeypatch,
) -> None:
    with pytest.raises(ValueError, match="batch size"):
        await snapshot_maps.publish_v4_relation_manifests(
            object(),
            schema_name="mrf",
            snapshot_key=1,
            build_token="token",
            entries=(),
            batch_rows=0,
        )
    with pytest.raises(ValueError, match="batch size"):
        await snapshot_maps.publish_v4_heavy_owners(
            object(),
            schema_name="mrf",
            snapshot_key=1,
            build_token="token",
            entries=(),
            batch_rows=0,
        )
    monkeypatch.setattr(
        snapshot_maps,
        "lock_v4_shared_layout_for_map_write",
        AsyncMock(),
    )
    monkeypatch.setattr(snapshot_maps, "_insert_relation_rows", AsyncMock())
    monkeypatch.setattr(
        snapshot_maps, "_load_relation_rows", AsyncMock(return_value=[])
    )
    with pytest.raises(RuntimeError, match="relation manifest conflicts"):
        await snapshot_maps.publish_v4_relation_manifests(
            object(),
            schema_name="mrf",
            snapshot_key=1,
            build_token="token",
            entries=(_relation_row(),),
        )
    monkeypatch.setattr(snapshot_maps, "_insert_heavy_owner_rows", AsyncMock())
    monkeypatch.setattr(
        snapshot_maps,
        "_load_heavy_owner_rows",
        AsyncMock(return_value=[]),
    )
    with pytest.raises(RuntimeError, match="heavy-owner manifest conflicts"):
        await snapshot_maps.publish_v4_heavy_owners(
            object(),
            schema_name="mrf",
            snapshot_key=1,
            build_token="token",
            entries=(_owner_row(),),
        )


@pytest.mark.asyncio
async def test_seal_database_compare_and_swap_failures(monkeypatch) -> None:
    with pytest.raises(TypeError, match="unexpected keyword"):
        snapshot_maps._v4_seal_options({"unknown": True})
    with pytest.raises(RuntimeError, match="compatible map root"):
        await snapshot_maps._lock_v4_build_owner(
            _ScriptedSession(_Result()),
            schema='"mrf"',
            snapshot_key=1,
            build_token="token",
        )
    with pytest.raises(RuntimeError, match="could not be completed"):
        await snapshot_maps._complete_v4_map_root(
            _ScriptedSession(_Result(scalar=None)),
            schema='"mrf"',
            snapshot_key=1,
            representation="direct_v1",
            summary=_summary(),
            metadata=_metadata(),
        )
    cleanup_pending = AsyncMock()
    monkeypatch.setattr(
        snapshot_maps,
        "mark_layout_build_candidate_cleanup_pending",
        cleanup_pending,
    )
    session = object()
    await snapshot_maps._defer_duplicate_v4_cleanup(
        session,
        schema_name="mrf",
        snapshot_key=1,
        canonical_snapshot_key=2,
    )
    cleanup_pending.assert_awaited_once_with(
        session,
        schema_name="mrf",
        snapshot_key=1,
        canonical_snapshot_key=2,
    )
    with pytest.raises(RuntimeError, match="expected building generation"):
        await snapshot_maps._seal_new_v4_layout(
            _ScriptedSession(_Result(scalar=None)),
            schema='"mrf"',
            snapshot_key=1,
            build_token="token",
            mapping_digest=b"m" * 32,
            support_digest=b"s" * 32,
            sealed_manifest={},
            logical_byte_count=1,
        )


@pytest.mark.asyncio
async def test_pattern_seal_requires_pattern_metadata(monkeypatch) -> None:
    monkeypatch.setattr(
        snapshot_maps,
        "_lock_v4_build_owner",
        AsyncMock(return_value="pattern_v1"),
    )
    monkeypatch.setattr(
        snapshot_maps,
        "_summarize_v4_seal_state",
        AsyncMock(return_value=(_summary(), _metadata(pattern_count=0))),
    )
    request = snapshot_maps._V4SealRequest(
        schema_name="mrf",
        schema='"mrf"',
        snapshot_key=1,
        build_token="token",
        expected_summary=_summary(),
        support_digest=b"s" * 32,
        layout_manifest={},
        summary_batch_rows=1,
        progress_callback=None,
    )
    with pytest.raises(RuntimeError, match="no pattern metadata"):
        await snapshot_maps._prepare_v4_seal_state(
            _ScriptedSession(_Result(rows=((None, None, None),))),
            request,
        )
