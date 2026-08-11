from __future__ import annotations

import hashlib
import json
import struct
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterable

import pytest

from api import ptg2_v4_graph as graph
from api import ptg2_v4_intersection as intersection
from api.ptg2_shared_blocks import PTG2SharedBlockError
from process.ptg_parts import ptg2_v4_audit as audit
from process.ptg_parts import ptg2_v4_graph_compiler as compiler
from process.ptg_parts import ptg2_v4_snapshot_maps as snapshot_maps
from process.ptg_parts.ptg2_shared_audit import AuditCandidate, _ReadBudget
from process.ptg_parts.ptg2_shared_blocks import (
    PTG2_V3_SHARED_FORMAT_VERSION,
    SharedBlock,
    SharedBlockReference,
    SharedLayoutBuildOwnership,
    shared_block_hash,
)


class _Result:
    def __init__(
        self,
        rows: Iterable[Any] = (),
        *,
        scalar: Any = None,
    ) -> None:
        self.rows = list(rows)
        self.scalar_value = scalar

    def __iter__(self):
        return iter(self.rows)

    def first(self):
        return self.rows[0] if self.rows else None

    def one(self):
        if len(self.rows) != 1:
            raise AssertionError(f"expected one row, observed {len(self.rows)}")
        return self.rows[0]

    def scalar(self):
        return self.scalar_value


class _ScriptedSession:
    def __init__(self, *responses: _Result) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, Any]] = []

    async def execute(self, statement, parameters=None):
        self.calls.append((str(statement), parameters))
        if not self.responses:
            raise AssertionError(f"unexpected SQL: {statement}")
        return self.responses.pop(0)

    async def scalar(self, statement, parameters=None):
        return (await self.execute(statement, parameters)).scalar()


def _reference(
    object_kind: str,
    block_key: int,
    fragment_no: int,
    *,
    payload: bytes | None = None,
    entry_count: int = 1,
) -> SharedBlockReference:
    raw_payload = payload if payload is not None else bytes((block_key + 1,)) * 4
    return SharedBlockReference(
        object_kind=object_kind,
        block_key=block_key,
        fragment_no=fragment_no,
        entry_count=entry_count,
        block_hash=shared_block_hash(
            format_version=PTG2_V3_SHARED_FORMAT_VERSION,
            object_kind=object_kind,
            codec="none",
            payload=raw_payload,
        ),
        raw_byte_count=len(raw_payload),
    )


def _summary(
    *,
    digest: bytes = b"d" * 32,
    object_kinds: tuple[str, ...] = ("v4_relation_members_v1",),
) -> snapshot_maps.V4SnapshotMapSummary:
    return snapshot_maps.V4SnapshotMapSummary(
        map_digest=digest,
        object_kinds=object_kinds,
        map_pack_count=1,
        coordinate_count=1,
        entry_count=2,
        logical_byte_count=8,
        stored_map_byte_count=136,
    )


def _metadata(
    *,
    npi_count: int = 1,
    component_count: int = 1,
    pattern_count: int = 1,
    relation_count: int = 1,
    heavy_owner_count: int = 0,
) -> snapshot_maps.V4SnapshotMetadataSummary:
    return snapshot_maps.V4SnapshotMetadataSummary(
        npi_count=npi_count,
        component_count=component_count,
        pattern_count=pattern_count,
        relation_count=relation_count,
        heavy_owner_count=heavy_owner_count,
        provider_graph_resources={
            "compressed_acquisition_bytes": 1024,
            "input_factor_bytes": 512,
            "factor_edge_count": 9,
            "empty_npi_tin_only_normalization_count": 0,
        },
    )


def _adaptive_direct_evidence(direct_bytes: int) -> dict[str, Any]:
    return {
        "eligible": True,
        "complete_prefix_eligible": True,
        "complete_prefix_projection_encoded_bytes": 10,
        "graph_encoded_bytes": direct_bytes - 200,
        "mapping_persistence_encoded_bytes": 200,
        "inferred_taxonomy_encoded_bytes": 0,
        "inferred_taxonomy_eligible": True,
        "inferred_taxonomy_rejection_reason": None,
        "inferred_taxonomy_rejection_rule_digest": None,
        "inferred_taxonomy_rejection_observed_count": None,
        "inferred_taxonomy_rejection_cap": None,
        "map_payload_encoded_bytes": 132,
        "map_coordinate_count": 1,
        "map_pack_count": 1,
        "map_object_kind_count": 1,
        "complete_persistent_encoded_bytes": direct_bytes,
    }


def _adaptive_pattern_evidence(pattern_bytes: int) -> dict[str, Any]:
    return {
        "eligible": True,
        "component_fallback_eligible": True,
        "unsafe_component_set_count": 0,
        "sparse_prefix_eligible": True,
        "sparse_prefix_owner_count": 0,
        "sparse_prefix_member_count": 0,
        "sparse_prefix_raw_bytes": 0,
        "sparse_prefix_projection_encoded_bytes": 10,
        "graph_encoded_bytes": pattern_bytes - 200,
        "mapping_persistence_encoded_bytes": 200,
        "inferred_taxonomy_encoded_bytes": 0,
        "inferred_taxonomy_eligible": True,
        "inferred_taxonomy_rejection_reason": None,
        "inferred_taxonomy_rejection_rule_digest": None,
        "inferred_taxonomy_rejection_observed_count": None,
        "inferred_taxonomy_rejection_cap": None,
        "map_payload_encoded_bytes": 132,
        "map_coordinate_count": 1,
        "map_pack_count": 1,
        "map_object_kind_count": 1,
        "complete_persistent_encoded_bytes": pattern_bytes,
    }


def synthetic_adaptive_layout_decision(
    representation: str = "pattern_v1",
) -> dict[str, Any]:
    """Build source-neutral sealed evidence for one shape-selected layout."""

    direct_bytes = 301 if representation == "pattern_v1" else 300
    pattern_bytes = 299 if representation == "pattern_v1" else 301
    layout_evidence_by_field = {
        "contract": compiler.PTG2_V4_ADAPTIVE_LAYOUT_DECISION_CONTRACT,
        "cost_contract": compiler.PTG2_V4_ADAPTIVE_LAYOUT_COST_CONTRACT,
        "selection_policy": compiler.PTG2_V4_ADAPTIVE_LAYOUT_SELECTION_POLICY,
        "compiler_options": compiler._effective_compiler_options(None),
        "selected_representation": representation,
        "selected_encoded_bytes": (
            pattern_bytes if representation == "pattern_v1" else direct_bytes
        ),
        "direct": _adaptive_direct_evidence(direct_bytes),
        "pattern": _adaptive_pattern_evidence(pattern_bytes),
    }
    return {
        **layout_evidence_by_field,
        "decision_digest": compiler._adaptive_layout_evidence_digest(
            layout_evidence_by_field
        ),
    }


def write_empty_taxonomy_artifact(
    output_directory: Path,
    artifact_by_name: dict[str, dict[str, Any]],
) -> None:
    """Add an authenticated zero-row taxonomy COPY artifact to a fixture."""

    template = artifact_by_name["provider_groups"]
    taxonomy_filename = compiler._OUTPUT_FILE_BY_NAME["inferred_taxonomy_candidates"][0]
    taxonomy_path = output_directory / taxonomy_filename
    taxonomy_path.write_bytes(Path(template["path"]).read_bytes())
    artifact_by_name["inferred_taxonomy_candidates"] = {
        "name": "inferred_taxonomy_candidates",
        "path": str(taxonomy_path),
        "byte_count": template["byte_count"],
        "sha256": template["sha256"],
        "row_count": 0,
    }


def taxonomy_summary_fields() -> dict[str, Any]:
    """Return an eligible zero-row taxonomy decision for both layouts."""

    rejection_field_by_name = {
        "inferred_taxonomy_rejection_reason": None,
        "inferred_taxonomy_rejection_rule_digest": None,
        "inferred_taxonomy_rejection_observed_count": None,
        "inferred_taxonomy_rejection_cap": None,
    }
    return {
        "direct_inferred_taxonomy_encoded_bytes": 0,
        "pattern_inferred_taxonomy_encoded_bytes": 0,
        "direct_inferred_taxonomy_eligible": True,
        "pattern_inferred_taxonomy_eligible": True,
        **{f"direct_{name}": value for name, value in rejection_field_by_name.items()},
        **{f"pattern_{name}": value for name, value in rejection_field_by_name.items()},
    }


def snapshot_manifest_fixture(
    summary: snapshot_maps.V4SnapshotMapSummary,
) -> tuple[dict[str, Any], snapshot_maps.V4SnapshotMetadataSummary]:
    """Build and authenticate one valid pattern-layout snapshot manifest."""

    metadata = _metadata()
    manifest = snapshot_maps._manifest_with_v4_root(
        {
            "serving_index": {
                "provider_graph": {
                    "adaptive_layout": synthetic_adaptive_layout_decision()
                }
            }
        },
        representation="pattern_v1",
        summary=summary,
        metadata=metadata,
    )
    snapshot_maps._validate_v4_manifest_root(
        manifest,
        representation="pattern_v1",
        summary=summary,
        metadata=metadata,
    )
    assert manifest["serving_index"]["snapshot_map"]["map_digest"] == (
        summary.map_digest.hex()
    )
    return manifest, metadata


def assert_snapshot_manifest_builder_rejections(
    summary: snapshot_maps.V4SnapshotMapSummary,
    metadata: snapshot_maps.V4SnapshotMetadataSummary,
) -> None:
    """Exercise conflicting snapshot-manifest builder inputs."""

    graph_index = {
        "provider_graph": {"adaptive_layout": synthetic_adaptive_layout_decision()}
    }
    builder_arguments_by_name = {
        "representation": "pattern_v1",
        "summary": summary,
        "metadata": metadata,
    }
    with pytest.raises(ValueError, match="serving_index"):
        snapshot_maps._manifest_copy_with_index({"serving_index": "bad"})
    with pytest.raises(ValueError, match="storage generation"):
        snapshot_maps._manifest_copy_with_index(
            {"serving_index": {"storage_generation": "foreign"}}
        )
    with pytest.raises(ValueError, match="incompatible type"):
        snapshot_maps._apply_v4_index_markers({"type": "foreign"})
    with pytest.raises(ValueError, match="serving_binary"):
        snapshot_maps._serving_binary_v4_map(
            {"serving_binary": "bad", **graph_index},
            **builder_arguments_by_name,
        )
    with pytest.raises(ValueError, match="must remain"):
        snapshot_maps._serving_binary_v4_map(
            {"serving_binary": {"format": "gzip"}, **graph_index},
            **builder_arguments_by_name,
        )
    with pytest.raises(ValueError, match="conflicting provider_graph"):
        snapshot_maps._serving_binary_v4_map(
            {
                "serving_binary": {"provider_graph_v4": {"bad": True}},
                **graph_index,
            },
            **builder_arguments_by_name,
        )
    with pytest.raises(ValueError, match="conflicting snapshot-map"):
        snapshot_maps._manifest_with_v4_root(
            {"serving_index": {"snapshot_map": {"bad": True}, **graph_index}},
            **builder_arguments_by_name,
        )


def _relation_row(relation: str = "group_patterns") -> dict[str, Any]:
    return {
        "relation": relation,
        "member_object_kind": f"v4_{relation}_members_v1",
        "locator_object_kind": f"v4_{relation}_locators_v1",
        "owner_base": 0,
        "owner_count": 2,
        "logical_member_count": 3,
        "vector_member_count": 3,
        "member_width": 4,
        "member_page_bytes": 16,
        "locator_page_bytes": 24,
        "locator_owner_span": 2,
    }


def _owner_row(
    relation: str = "group_patterns",
    owner_key: int = 1,
) -> dict[str, Any]:
    return {
        "relation": relation,
        "owner_key": owner_key,
        "object_kind": f"v4_{relation}_heavy_bitmap_v1",
        "member_count": 2,
        "member_base": 10,
        "member_span": 8,
        "fragment_count": 1,
    }


def configure_publication_spies(monkeypatch):
    """Install deterministic spies for bounded dictionary publication."""
    lock_calls: list[tuple[int, str]] = []
    batches: list[tuple[str, tuple[int, ...]]] = []

    async def fake_lock(
        _session,
        *,
        snapshot_key: int,
        build_token: str,
        **_kwargs,
    ):
        lock_calls.append((snapshot_key, build_token))

    async def fake_npi_batch(_session, *, npi_rows, **_kwargs):
        batches.append(("npi", tuple(row["npi_key"] for row in npi_rows)))

    async def fake_component_batch(_session, *, component_rows, **_kwargs):
        component_keys = tuple(
            row["component_key"] for row in component_rows
        )
        batches.append(("component", component_keys))

    async def fake_pattern_batch(_session, *, pattern_rows, **_kwargs):
        pattern_keys = tuple(row["pattern_key"] for row in pattern_rows)
        batches.append(("pattern", pattern_keys))

    async def fake_dense(_session, *, expected_count: int, **_kwargs):
        return expected_count

    monkeypatch.setattr(
        snapshot_maps,
        "lock_v4_shared_layout_for_map_write",
        fake_lock,
    )
    monkeypatch.setattr(
        snapshot_maps,
        "_publish_v4_npi_batch",
        fake_npi_batch,
    )
    monkeypatch.setattr(
        snapshot_maps,
        "_publish_v4_component_batch",
        fake_component_batch,
    )
    monkeypatch.setattr(
        snapshot_maps,
        "_publish_v4_pattern_batch",
        fake_pattern_batch,
    )
    monkeypatch.setattr(snapshot_maps, "_verify_dense_table_keys", fake_dense)
    return fake_lock, lock_calls, batches


__all__ = [
    "Any",
    "AuditCandidate",
    "Iterable",
    "PTG2SharedBlockError",
    "PTG2_V3_SHARED_FORMAT_VERSION",
    "Path",
    "SharedBlock",
    "SharedBlockReference",
    "SharedLayoutBuildOwnership",
    "SimpleNamespace",
    "_ReadBudget",
    "_Result",
    "_ScriptedSession",
    "_metadata",
    "_owner_row",
    "_reference",
    "_relation_row",
    "_summary",
    "assert_snapshot_manifest_builder_rejections",
    "configure_publication_spies",
    "snapshot_manifest_fixture",
    "synthetic_adaptive_layout_decision",
    "asynccontextmanager",
    "audit",
    "compiler",
    "graph",
    "hashlib",
    "intersection",
    "json",
    "pytest",
    "shared_block_hash",
    "snapshot_maps",
    "struct",
    "taxonomy_summary_fields",
    "write_empty_taxonomy_artifact",
]
