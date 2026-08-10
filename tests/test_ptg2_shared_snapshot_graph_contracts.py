# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import datetime
import hashlib
import json
import struct
from collections import defaultdict
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from tests.live_progress_atomic_redis import AtomicLiveProgressRedis

from process import live_progress
from process.ptg_parts import ptg2_shared_snapshot_publish as shared_snapshot_publish
from process.ptg_parts import ptg2_shared_publish as shared_publish
from process.ptg_parts import live_progress as ptg_live_progress
from process.ptg_parts import ptg2_v4_graph_compiler as graph_compiler
from process.ptg_parts.ptg2_shared_blocks import SharedMappingDigestSummary
from process.ptg_parts.ptg2_shared_price import PreparedSharedPriceKeyMap
from process.ptg_parts.ptg2_shared_snapshot_publish import (
    _run_independent_publication_lanes,
    _validate_authoritative_mapping_summary,
)
def _v4_graph_summary():
    """Return one complete pattern-selected publication summary."""

    encoding_options_by_name = {
        name: option_value
        for name, option_value in graph_compiler._effective_compiler_options(
            None
        ).items()
        if name in graph_compiler.PTG2_V4_GRAPH_ENCODING_OPTION_NAMES
    }
    return {
        **encoding_options_by_name,
        "format": "ptg2_provider_graph_v4",
        "selected_layout": "pattern",
        "selected_encoded_bytes": 231,
        "direct_layout_complete_prefix_eligible": True,
        "pattern_layout_sparse_prefix_eligible": True,
        "pattern_layout_serving_degree_eligible": True,
        "direct_complete_prefix_projection_encoded_bytes": 10,
        "pattern_sparse_prefix_owner_count": 0,
        "pattern_sparse_prefix_member_count": 0,
        "pattern_sparse_prefix_raw_bytes": 0,
        "pattern_sparse_prefix_projection_encoded_bytes": 10,
        "direct_graph_encoded_bytes": 100,
        "direct_mapping_persistence_encoded_bytes": 132,
        "direct_inferred_taxonomy_encoded_bytes": 0,
        "direct_inferred_taxonomy_eligible": True,
        "direct_inferred_taxonomy_rejection_reason": None,
        "direct_inferred_taxonomy_rejection_rule_digest": None,
        "direct_inferred_taxonomy_rejection_observed_count": None,
        "direct_inferred_taxonomy_rejection_cap": None,
        "direct_map_payload_encoded_bytes": 132,
        "direct_map_coordinate_count": 1,
        "direct_map_pack_count": 1,
        "direct_map_object_kind_count": 1,
        "direct_complete_encoded_bytes": 232,
        "pattern_graph_encoded_bytes": 99,
        "pattern_mapping_persistence_encoded_bytes": 132,
        "pattern_inferred_taxonomy_encoded_bytes": 0,
        "pattern_inferred_taxonomy_eligible": True,
        "pattern_inferred_taxonomy_rejection_reason": None,
        "pattern_inferred_taxonomy_rejection_rule_digest": None,
        "pattern_inferred_taxonomy_rejection_observed_count": None,
        "pattern_inferred_taxonomy_rejection_cap": None,
        "pattern_map_payload_encoded_bytes": 132,
        "pattern_map_coordinate_count": 1,
        "pattern_map_pack_count": 1,
        "pattern_map_object_kind_count": 1,
        "pattern_complete_encoded_bytes": 231,
        "npi_prefix_target": 200,
        "max_npi_prefix_override_owners": 250_000,
        "max_npi_prefix_override_bytes": 64 * 1024 * 1024,
        "max_set_patterns_per_set": 4096,
        "max_set_components_per_fallback_set": 4096,
        "resource_admission": {
            "max_estimated_model_bytes": 8 * 1024 * 1024 * 1024,
            "max_factor_edges": 1_000_000,
        },
        "observe": {"unsafe_pattern_component_set_count": 0},
    }


def _v4_graph_publication_fixture(tmp_path):
    """Return authenticated graph, CAS, and map publication evidence."""

    artifact = SimpleNamespace(
        name="graph_blocks",
        byte_count=12,
        sha256="a" * 64,
        row_count=1,
    )
    compilation = SimpleNamespace(
        output_artifacts=(artifact,),
        block_copy_path=tmp_path / "graph.copy",
        reference_manifest_path=tmp_path / "references.jsonl",
        selected_layout="pattern",
        summary=_v4_graph_summary(),
        relation_summaries=(),
        heavy_bitmaps=(),
        observe={"group_count": 3, "npi_count": 2},
        resource_admission={
            "input_factor_bytes": 512,
            "factor_edge_count": 9,
        },
        block_count=1,
        provider_set_audit_npi_copy_path=tmp_path / "audit.copy",
    )
    cas_publication = SimpleNamespace(
        staged_row_count=1,
        unique_block_count=1,
        logical_byte_count=12,
        stored_byte_count=12,
    )
    map_summary = SimpleNamespace(
        map_digest=b"m" * 32,
        object_kinds=("v4_set_patterns_members_v1",),
        object_kind_count=1,
        map_pack_count=1,
        coordinate_count=1,
        stored_map_byte_count=132,
    )
    return compilation, cas_publication, map_summary


def _patch_v4_graph_publication(
    monkeypatch,
    cas_publication,
    map_summary,
):
    taxonomy_publication = SimpleNamespace(
        packed_byte_count=12,
        pattern_member_bytes=40,
        manifest={},
    )
    tax_identity_publication = SimpleNamespace(
        artifact_byte_count=24,
        manifest={
            "contract": "ptg2_provider_group_tax_identity_v1",
            "provider_group_count": 3,
            "tax_identity_count": 1,
            "content_digest": (b"t" * 32).hex(),
        },
    )
    tax_identity_source_publication = SimpleNamespace(
        artifact_byte_count=16,
        as_dict=lambda: {
            "contract": "ptg2_provider_group_tax_identity_source_v1",
            "content_digest": "s" * 64,
        },
    )
    publish_maps_mock = AsyncMock(
        return_value=(
            cas_publication,
            map_summary,
            taxonomy_publication,
            tax_identity_publication,
            tax_identity_source_publication,
        )
    )
    replacements_by_name = {
        "create_shared_block_stage": AsyncMock(),
        "copy_shared_block_binary_file": AsyncMock(),
        "_publish_v4_dictionaries_and_maps": publish_maps_mock,
    }
    for name, replacement in replacements_by_name.items():
        monkeypatch.setattr(shared_snapshot_publish, name, replacement)
    monkeypatch.setattr(
        shared_snapshot_publish.db,
        "status",
        AsyncMock(),
    )
    return publish_maps_mock


@pytest.mark.asyncio
async def test_v4_graph_publish_threads_compressed_acquisition_resources(
    monkeypatch,
    tmp_path,
):
    """Seal acquisition bytes with graph diagnostics, not the CAS stage."""

    compilation, cas_publication, map_summary = _v4_graph_publication_fixture(tmp_path)
    publish_maps_mock = _patch_v4_graph_publication(
        monkeypatch,
        cas_publication,
        map_summary,
    )

    publication = await shared_snapshot_publish._publish_v4_graph(
        compilation,
        publication_context=shared_snapshot_publish._V4GraphCoordinates(
            schema_name="mrf",
            logical_snapshot_id="synthetic-snapshot",
            snapshot_key=17,
            build_token="token",
        ),
        compressed_acquisition_bytes=4_096,
        empty_npi_tin_only_normalization_count=2,
    )

    assert publication.logical_byte_count == 104
    assert publication.stored_byte_count == 236
    assert publication.provider_tax_identity["tax_identity_count"] == 1
    assert (
        publication.provider_tax_identity_source["contract"]
        == "ptg2_provider_group_tax_identity_source_v1"
    )
    publication_context = publish_maps_mock.await_args.kwargs[
        "publication_context"
    ]
    assert publication_context.block_stage.startswith(
        "ptg2_v3_block_stage_"
    )
    assert publication_context.logical_snapshot_id == "synthetic-snapshot"
    assert not hasattr(shared_snapshot_publish, "publish_v4_cas_block_stage")
    assert publish_maps_mock.await_args.kwargs["compressed_acquisition_bytes"] == 4_096
    assert (
        publish_maps_mock.await_args.kwargs["empty_npi_tin_only_normalization_count"]
        == 2
    )


def test_v4_graph_publish_rejects_packed_map_plan_drift(
    tmp_path,
):
    """Reject estimator drift inside the CAS/map publication transaction."""

    compilation, cas_publication, map_summary = _v4_graph_publication_fixture(tmp_path)
    drifted_map_summary = SimpleNamespace(
        **{
            **vars(map_summary),
            "stored_map_byte_count": map_summary.stored_map_byte_count + 1,
        }
    )
    with pytest.raises(
        RuntimeError,
        match="packed-map plan differs from publication",
    ):
        shared_snapshot_publish._require_v4_atomic_map_publication(
            compilation,
            cas_publication,
            drifted_map_summary,
        )


@pytest.mark.parametrize(
    ("cas_count", "map_count"),
    ((2, 1), (1, 2)),
    ids=("extra-cas-stage-row", "extra-map-coordinate"),
)
def test_v4_atomic_publication_rejects_coordinate_count_drift(
    cas_count,
    map_count,
):
    """CAS and map coordinates must both equal the compiler block count."""

    with pytest.raises(RuntimeError, match="coordinate counts changed"):
        shared_snapshot_publish._require_v4_atomic_coordinate_counts(
            1,
            SimpleNamespace(staged_row_count=cas_count),
            SimpleNamespace(coordinate_count=map_count),
        )


@pytest.mark.asyncio
async def test_v4_graph_publish_queues_blocks_after_stage_failure(
    monkeypatch,
    tmp_path,
):
    """A partial CAS copy stays recoverable while its stage is removed."""

    compilation, _, _ = _v4_graph_publication_fixture(tmp_path)
    queue_failed = AsyncMock()
    status = AsyncMock()
    monkeypatch.setattr(
        shared_snapshot_publish,
        "create_shared_block_stage",
        AsyncMock(),
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "copy_shared_block_binary_file",
        AsyncMock(side_effect=RuntimeError("copy failed")),
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "_queue_failed_v4_graph_blocks",
        queue_failed,
    )
    monkeypatch.setattr(shared_snapshot_publish.db, "status", status)

    with pytest.raises(RuntimeError, match="copy failed"):
        await shared_snapshot_publish._publish_v4_graph(
            compilation,
            publication_context=shared_snapshot_publish._V4GraphCoordinates(
                schema_name="mrf",
                logical_snapshot_id="synthetic-snapshot",
                snapshot_key=17,
                build_token="token",
            ),
            compressed_acquisition_bytes=1,
            empty_npi_tin_only_normalization_count=0,
        )

    queue_failed.assert_awaited_once_with(
        schema_name="mrf",
        reference_manifest_path=compilation.reference_manifest_path,
    )
    assert "DROP TABLE IF EXISTS" in status.await_args.args[0]
