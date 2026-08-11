from __future__ import annotations

from unittest.mock import AsyncMock

from tests.ptg2_v4_coverage_support import (
    PTG2_V3_SHARED_FORMAT_VERSION,
    _Result,
    _ScriptedSession,
    _metadata,
    _reference,
    pytest,
    snapshot_maps,
    synthetic_adaptive_layout_decision,
)


async def _publish_snapshot_map(monkeypatch):
    reference = _reference("v4_relation_members_v1", 0, 0, entry_count=2)
    published_packs = []

    async def no_op(*_args, **_kwargs):
        return None

    async def capture_pack(_session, *, pack, **_kwargs):
        published_packs.append(pack)

    monkeypatch.setattr(
        snapshot_maps,
        "lock_v4_shared_layout_for_map_write",
        no_op,
    )
    monkeypatch.setattr(
        snapshot_maps,
        "_initialize_v4_snapshot_map_root",
        no_op,
    )
    monkeypatch.setattr(snapshot_maps, "_publish_v4_map_pack", capture_pack)
    expected_summary = await snapshot_maps.publish_v4_snapshot_maps(
        object(),
        schema_name="mrf",
        snapshot_key=17,
        build_token="token",
        representation="pattern_v1",
        references=(reference,),
    )
    assert len(published_packs) == 1
    return reference, expected_summary, published_packs[0], no_op


async def _assert_persisted_map_summary(
    monkeypatch,
    reference,
    expected_summary,
    pack,
) -> None:
    persisted_rows = [
        [
            {
                **snapshot_maps._map_pack_row(pack, 17),
                "map_format_version": PTG2_V3_SHARED_FORMAT_VERSION,
                "map_object_kind": snapshot_maps.PTG2_V4_MAP_BLOCK_KIND,
                "map_codec": "none",
                "map_entry_count": 1,
                "map_raw_byte_count": len(pack.map_block.payload),
                "map_stored_byte_count": len(pack.map_block.payload),
                "map_payload": pack.map_block.payload,
            }
        ],
        [],
    ]

    async def fake_load_rows(*_args, **_kwargs):
        return persisted_rows.pop(0)

    async def fake_target_metadata(*_args, **_kwargs):
        return {
            reference.block_hash: (
                reference.object_kind,
                reference.entry_count,
                reference.raw_byte_count,
            )
        }

    cancelled_hash_sets: list[set[bytes]] = []

    async def fake_cancel(_session, *, block_hashes, **_kwargs):
        cancelled_hash_sets.append(set(block_hashes))

    monkeypatch.setattr(snapshot_maps, "_load_persisted_map_rows", fake_load_rows)
    monkeypatch.setattr(snapshot_maps, "_load_target_metadata", fake_target_metadata)
    monkeypatch.setattr(snapshot_maps, "_cancel_map_gc_candidates", fake_cancel)
    observed_summary = await snapshot_maps.summarize_persisted_v4_snapshot_maps(
        object(),
        schema_name="mrf",
        snapshot_key=17,
        batch_rows=1,
        cancel_gc_candidates=True,
    )
    assert observed_summary == expected_summary
    assert reference.block_hash in cancelled_hash_sets[0]
    with pytest.raises(ValueError, match="batch size"):
        await snapshot_maps.summarize_persisted_v4_snapshot_maps(
            object(),
            schema_name="mrf",
            snapshot_key=17,
            batch_rows=0,
        )


def _patch_persisted_metadata_loaders(
    monkeypatch,
    expected_metadata,
    no_op,
    taxonomy_calls,
) -> None:
    async def fake_aggregate(*_args, **_kwargs):
        return {
            "npi_count": 1,
            "npi_min": 0,
            "npi_max": 0,
            "component_count": 1,
            "component_min": 0,
            "component_max": 0,
            "pattern_count": 1,
            "pattern_min": 0,
            "pattern_max": 0,
            "relation_count": 0,
            "heavy_owner_count": 0,
        }

    async def fake_empty_mapping(*_args, **_kwargs):
        return {}

    async def fake_diagnostics(*_args, **_kwargs):
        return expected_metadata.provider_graph_diagnostics

    async def fake_resources(*_args, **_kwargs):
        return expected_metadata.provider_graph_resources

    async def fake_taxonomy_candidates(*_args, **kwargs):
        taxonomy_calls.append(dict(kwargs))
        return expected_metadata.inferred_taxonomy_candidates

    monkeypatch.setattr(snapshot_maps, "_load_metadata_aggregate", fake_aggregate)
    monkeypatch.setattr(snapshot_maps, "_load_relation_metadata", fake_empty_mapping)
    monkeypatch.setattr(
        snapshot_maps,
        "_load_entry_counts_by_kind",
        fake_empty_mapping,
    )
    monkeypatch.setattr(snapshot_maps, "_load_heavy_owners", fake_empty_mapping)
    monkeypatch.setattr(snapshot_maps, "_validate_zero_heavy_locators", no_op)
    monkeypatch.setattr(
        snapshot_maps,
        "_load_provider_graph_diagnostics",
        fake_diagnostics,
    )
    monkeypatch.setattr(
        snapshot_maps,
        "_load_provider_graph_resources",
        fake_resources,
    )
    monkeypatch.setattr(
        snapshot_maps,
        "summarize_v4_inferred_taxonomy_candidates",
        fake_taxonomy_candidates,
    )


async def _assert_persisted_metadata_summary(
    monkeypatch,
    expected_summary,
    no_op,
) -> None:
    """Assert persisted graph metadata is summarized without identity drift."""
    expected_metadata = _metadata(relation_count=0)
    taxonomy_calls = []
    _patch_persisted_metadata_loaders(
        monkeypatch,
        expected_metadata,
        no_op,
        taxonomy_calls,
    )
    observed_metadata = await snapshot_maps.summarize_persisted_v4_snapshot_metadata(
        object(),
        schema_name="mrf",
        snapshot_key=17,
        map_summary=expected_summary,
    )
    assert observed_metadata == expected_metadata
    assert taxonomy_calls == [
        {
            "schema_name": "mrf",
            "snapshot_key": 17,
            "npi_count": 1,
            "pattern_count": 1,
            "rules": snapshot_maps.INFERRED_PROVIDER_TAXONOMY_RULES,
        }
    ]


async def _assert_new_layout_seal(monkeypatch, expected_summary):
    """Verify a fresh V4 layout seals with exact coverage metadata."""
    owner_by_field = {
        "snapshot_key": 17,
        "root_state": "building",
        "root_format_version": snapshot_maps.PTG2_V4_MAP_FORMAT_VERSION,
        "map_format": snapshot_maps.PTG2_V4_MAP_FORMAT,
        "representation": "pattern_v1",
        "projection_id_scope": snapshot_maps.PTG2_V4_PROJECTION_ID_SCOPE,
    }
    _install_new_layout_seal_mocks(monkeypatch, expected_summary)
    new_session = _ScriptedSession(
        _Result(rows=(owner_by_field,)),
        _Result(rows=((None, None, None),)),
        _Result(scalar=17),
        _Result(),
        _Result(scalar=17),
    )
    sealed = await snapshot_maps.seal_v4_shared_layout(
        new_session,
        schema_name="mrf",
        snapshot_key=17,
        build_token="token",
        expected_summary=expected_summary,
        support_digest=b"u" * 32,
        layout_manifest={
            "serving_index": {
                "provider_graph": {
                    "representation": "pattern_v1",
                    "adaptive_layout": synthetic_adaptive_layout_decision(),
                }
            }
        },
    )
    assert (sealed.snapshot_key, sealed.reused) == (17, False)
    return owner_by_field


def _install_new_layout_seal_mocks(monkeypatch, expected_summary) -> None:
    """Install exact V4 summary and short-seal dependencies."""

    async def fake_summarize_maps(*_args, **_kwargs):
        return expected_summary

    async def fake_summarize_metadata(*_args, **_kwargs):
        return _metadata()

    monkeypatch.setattr(
        snapshot_maps,
        "summarize_persisted_v4_snapshot_maps",
        fake_summarize_maps,
    )
    monkeypatch.setattr(
        snapshot_maps,
        "summarize_persisted_v4_snapshot_metadata",
        fake_summarize_metadata,
    )
    monkeypatch.setattr(
        snapshot_maps,
        "acquire_ptg2_source_lifecycle_lock",
        AsyncMock(return_value="layout_token"),
    )
    monkeypatch.setattr(
        snapshot_maps,
        "acquire_layout_digest_lock",
        AsyncMock(),
    )
    monkeypatch.setattr(
        snapshot_maps,
        "publish_layout_fingerprint",
        AsyncMock(
            side_effect=lambda *_args, **kwargs: kwargs[
                "canonical_snapshot_key"
            ]
        ),
    )
    monkeypatch.setattr(
        snapshot_maps,
        "delete_layout_build_candidate",
        AsyncMock(),
    )


def _reusable_layout_row(expected_summary):
    sealed_manifest = snapshot_maps._manifest_with_v4_root(
        {
            "serving_index": {
                "provider_graph": {
                    "representation": "pattern_v1",
                    "adaptive_layout": synthetic_adaptive_layout_decision(),
                }
            }
        },
        representation="pattern_v1",
        summary=expected_summary,
        metadata=_metadata(),
    )
    reusable_layout_by_field = {
        "snapshot_key": 18,
        "layout_manifest": sealed_manifest,
        "root_state": "complete",
        "root_format_version": snapshot_maps.PTG2_V4_MAP_FORMAT_VERSION,
        "map_format": snapshot_maps.PTG2_V4_MAP_FORMAT,
        "representation": "pattern_v1",
        "projection_id_scope": snapshot_maps.PTG2_V4_PROJECTION_ID_SCOPE,
        "map_digest": expected_summary.map_digest,
        "object_kind_count": expected_summary.object_kind_count,
        "map_pack_count": expected_summary.map_pack_count,
        "coordinate_count": expected_summary.coordinate_count,
        "entry_count": expected_summary.entry_count,
        "logical_byte_count": expected_summary.logical_byte_count,
        "stored_map_byte_count": expected_summary.stored_map_byte_count,
        "npi_count": 1,
        "component_count": 1,
        "pattern_count": 1,
        "relation_count": 1,
        "heavy_owner_count": 0,
    }
    return reusable_layout_by_field


async def _assert_reused_layout_seal(
    monkeypatch,
    expected_summary,
    owner_by_field,
) -> None:
    reusable_layout_by_field = _reusable_layout_row(expected_summary)
    deferred_cleanup_pairs: list[tuple[int, int]] = []

    async def fake_cleanup(
        _session,
        *,
        snapshot_key: int,
        canonical_snapshot_key: int,
        **_kwargs,
    ):
        deferred_cleanup_pairs.append((snapshot_key, canonical_snapshot_key))

    monkeypatch.setattr(
        snapshot_maps,
        "mark_layout_build_candidate_cleanup_pending",
        fake_cleanup,
    )
    reused_session = _ScriptedSession(
        _Result(rows=(owner_by_field,)),
        _Result(rows=((None, None, None),)),
        _Result(scalar=17),
        _Result(rows=(reusable_layout_by_field,)),
        _Result(),
        _Result(scalar=17),
    )
    reused = await snapshot_maps.seal_v4_shared_layout(
        reused_session,
        schema_name="mrf",
        snapshot_key=17,
        build_token="token",
        expected_summary=expected_summary,
        support_digest=b"u" * 32,
        layout_manifest={
            "serving_index": {
                "provider_graph": {
                    "representation": "pattern_v1",
                    "adaptive_layout": synthetic_adaptive_layout_decision(),
                }
            }
        },
    )
    assert (reused.snapshot_key, reused.reused) == (18, True)
    assert deferred_cleanup_pairs == [(17, 18)]
    with pytest.raises(ValueError, match="support digest"):
        await snapshot_maps.seal_v4_shared_layout(
            object(),
            schema_name="mrf",
            snapshot_key=17,
            build_token="token",
            expected_summary=expected_summary,
            support_digest=b"short",
            layout_manifest={},
        )


@pytest.mark.asyncio
async def test_snapshot_summary_publication_and_seal_paths(monkeypatch) -> None:
    """Publish, validate, and seal snapshot summaries through failure branches."""
    reference, summary, pack, no_op = await _publish_snapshot_map(monkeypatch)
    await _assert_persisted_map_summary(monkeypatch, reference, summary, pack)
    await _assert_persisted_metadata_summary(monkeypatch, summary, no_op)
    owner_by_field = await _assert_new_layout_seal(monkeypatch, summary)
    await _assert_reused_layout_seal(monkeypatch, summary, owner_by_field)
