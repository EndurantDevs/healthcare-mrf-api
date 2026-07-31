"""Small branch-margin contracts for packed-map validation."""

from __future__ import annotations

import pytest

from tests.ptg2_v4_coverage_support import (
    _Result,
    _ScriptedSession,
    _metadata,
    _owner_row,
    _reference,
    _summary,
    snapshot_manifest_fixture,
    snapshot_maps,
)


class _Rows(_Result):
    def all(self):
        return self.rows


def test_summary_and_dense_row_identity_rejections() -> None:
    reference = _reference("kind", 0, 0)
    pack = snapshot_maps._make_map_pack(
        object_kind="kind",
        pack_no=0,
        references=(reference,),
    )
    accumulator = snapshot_maps._V4SnapshotMapSummaryAccumulator()
    accumulator.add_pack(pack)
    with pytest.raises(ValueError, match="strictly ordered"):
        accumulator.add_pack(pack)

    with pytest.raises(ValueError, match="component keys"):
        snapshot_maps._normalized_component_row((1, b"c" * 16), 0)
    with pytest.raises(ValueError, match="pattern keys"):
        snapshot_maps._normalized_pattern_row((1, b"p" * 32, 0), 0)


@pytest.mark.asyncio
async def test_metadata_publishers_reject_nonpositive_batches() -> None:
    with pytest.raises(ValueError, match="component batch"):
        await snapshot_maps.publish_v4_provider_components(
            object(),
            schema_name="mrf",
            snapshot_key=1,
            build_token="token",
            entries=(),
            batch_rows=0,
        )
    with pytest.raises(ValueError, match="pattern batch"):
        await snapshot_maps.publish_v4_patterns(
            object(),
            schema_name="mrf",
            snapshot_key=1,
            build_token="token",
            entries=(),
            batch_rows=0,
        )


@pytest.mark.asyncio
async def test_locator_and_heavy_owner_skip_unrequested_coordinates() -> None:
    locator = _reference("locator", 5, 0)
    locator_payload = snapshot_maps.encode_v4_snapshot_map_pack(
        "locator",
        (locator,),
    )
    with pytest.raises(RuntimeError, match="missing"):
        await snapshot_maps._load_locator_coordinates(
            _ScriptedSession(
                _Result(rows=({"object_kind": "locator", "payload": locator_payload},))
            ),
            schema='"mrf"',
            snapshot_key=1,
            requested_by_kind={"locator": {4}},
        )

    object_kind = _owner_row("r", 7)["object_kind"]
    unrelated = _reference(object_kind, 8, 0)
    owners = await snapshot_maps._load_heavy_owners(
        _ScriptedSession(
            _Result(
                rows=(
                    {
                        **_owner_row("r", 7),
                        "map_block_hash": b"h" * 32,
                        "payload": snapshot_maps.encode_v4_snapshot_map_pack(
                            object_kind,
                            (unrelated,),
                        ),
                    },
                )
            )
        ),
        schema='"mrf"',
        snapshot_key=1,
    )
    assert owners[("r", 7)]["fragments"] == set()


@pytest.mark.asyncio
async def test_graph_diagnostic_and_prefix_guards() -> None:
    with pytest.raises(RuntimeError, match="missing or duplicated"):
        await snapshot_maps._load_graph_diagnostic_fields(
            _ScriptedSession(_Rows()),
            schema='"mrf"',
            snapshot_key=1,
        )
    with pytest.raises(RuntimeError, match="columns changed"):
        await snapshot_maps._load_graph_diagnostic_fields(
            _ScriptedSession(_Rows(rows=({"unexpected": 1},))),
            schema='"mrf"',
            snapshot_key=1,
        )
    diagnostic_by_field = {
        "override_owner_count": 1,
        "override_member_count": 2,
        "npi_prefix_target": 3,
    }
    with pytest.raises(RuntimeError, match="prefix diagnostics"):
        await snapshot_maps._validate_prefix_aggregate(
            _ScriptedSession(
                _Result(rows=({"owner_count": 0, "member_count": 0, "valid": True},))
            ),
            schema='"mrf"',
            snapshot_key=1,
            diagnostic=diagnostic_by_field,
        )
    with pytest.raises(RuntimeError, match="canary-owner"):
        snapshot_maps._validate_canary_prefixes(
            {
                "worst_provider_set_key": 7,
                "worst_member_count": 2,
                "worst_member_digest": b"d" * 32,
                "worst_uses_override": True,
                "worst_online_provider_set_key": None,
            },
            {},
        )


@pytest.mark.asyncio
async def test_map_pack_and_reusable_root_conflicts() -> None:
    with pytest.raises(RuntimeError, match="conflicts with stored"):
        await snapshot_maps._verify_map_pack_row(
            _ScriptedSession(_Result()),
            schema='"mrf"',
            row_by_field={
                "snapshot_key": 1,
                "object_kind": "kind",
                "pack_no": 0,
                "coordinate_count": 1,
            },
        )

    summary = _summary()
    manifest, metadata = snapshot_manifest_fixture(summary)
    reusable_by_field = {
        "layout_manifest": manifest,
        "root_state": "building",
        "root_format_version": snapshot_maps.PTG2_V4_MAP_FORMAT_VERSION,
        "map_format": snapshot_maps.PTG2_V4_MAP_FORMAT,
        "representation": "pattern_v1",
        "projection_id_scope": snapshot_maps.PTG2_V4_PROJECTION_ID_SCOPE,
        "map_digest": summary.map_digest,
        "object_kinds": list(summary.object_kinds),
        "object_kind_count": summary.object_kind_count,
        "map_pack_count": summary.map_pack_count,
        "coordinate_count": summary.coordinate_count,
        "entry_count": summary.entry_count,
        "logical_byte_count": summary.logical_byte_count,
        "stored_map_byte_count": summary.stored_map_byte_count,
        "npi_count": metadata.npi_count,
        "component_count": metadata.component_count,
        "pattern_count": metadata.pattern_count,
        "relation_count": metadata.relation_count,
        "heavy_owner_count": metadata.heavy_owner_count,
    }
    with pytest.raises(RuntimeError, match="root is incompatible"):
        snapshot_maps._validate_reusable_v4_layout(
            reusable_by_field,
            representation="pattern_v1",
            observed_summary=summary,
            observed_metadata=metadata,
        )
