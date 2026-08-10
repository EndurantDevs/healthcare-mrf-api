# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import replace
import hashlib
import importlib.util
import os
from pathlib import Path
import struct
from types import SimpleNamespace
from typing import Any, Iterable
from unittest.mock import AsyncMock

import pytest

from api.ptg2_code_filters import InferredProviderTaxonomyRule
from api.ptg2_shared_blocks import PTG2SharedBlockError
from api import ptg2_v4_graph as v4_graph
from process.ptg_parts import ptg2_v4_snapshot_maps as snapshot_maps
from process.ptg_parts import ptg2_v4_taxonomy_candidates as candidates
from process.ptg_parts.ptg2_manifest_artifacts import (
    PTG2ManifestArtifactError,
)
from tests.ptg2_v4_coverage_support import (
    _metadata,
    _summary,
    synthetic_adaptive_layout_decision,
)

from tests.ptg2_v4_taxonomy_candidate_test_support import (
    _PreparedCompilerInputSession,
    _PublicationSession,
    _Result,
    _ScriptedSession,
    _assert_candidate_load_rejected,
    _assert_direct_publication_contract,
    _compiler_rules,
    _load_candidate_projection,
    _noop_map_write_lock,
    _observe_projection_row,
    _projection_row,
    _publish_candidate_projection,
    _reader_row,
    _rules,
    _tampered_pattern_projection,
)

def _sealed_taxonomy_reservation_fixture() -> tuple[
    dict[str, Any],
    dict[str, Any],
]:
    projection = candidates._candidate_projection_manifest(
        (_projection_row(_rules()[0]),)
    )
    summary = _summary()
    metadata = replace(
        _metadata(),
        inferred_taxonomy_candidates=projection,
    )
    layout_manifest = snapshot_maps._manifest_with_v4_root(
        {
            "serving_index": {
                "provider_graph": {
                    "representation": "pattern_v1",
                    "adaptive_layout": synthetic_adaptive_layout_decision(),
                }
            }
        },
        representation="pattern_v1",
        summary=summary,
        metadata=metadata,
    )
    existing_root_map = {
        "snapshot_key": 41,
        "layout_manifest": layout_manifest,
        "layout_mapping_digest": summary.map_digest,
        "root_state": "complete",
        "root_format_version": snapshot_maps.PTG2_V4_MAP_FORMAT_VERSION,
        "map_format": snapshot_maps.PTG2_V4_MAP_FORMAT,
        "representation": "pattern_v1",
        "projection_id_scope": snapshot_maps.PTG2_V4_PROJECTION_ID_SCOPE,
        "map_digest": summary.map_digest,
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
    return layout_manifest, existing_root_map


def test_seal_and_reuse_validation_reject_projection_manifest_drift() -> None:
    """Sealed reuse rejects a taxonomy projection changed after publication."""

    layout_manifest, existing_root_map = _sealed_taxonomy_reservation_fixture()
    assert (
        snapshot_maps._validate_sealed_reservation(existing_root_map) == layout_manifest
    )

    tampered = deepcopy(existing_root_map)
    projection_manifest = tampered["layout_manifest"]["serving_index"][
        "serving_binary"
    ]["provider_graph_v4"]["inferred_taxonomy_candidates"]
    projection_manifest["rules"][0]["member_count"] += 1
    with pytest.raises(PTG2ManifestArtifactError, match="projection rule"):
        snapshot_maps._validate_sealed_reservation(tampered)


def test_sealed_reservation_without_snapshot_map_has_no_object_kinds() -> None:
    """Legacy manifest gaps reconstruct an empty immutable map-kind tuple."""

    _, existing_root_map = _sealed_taxonomy_reservation_fixture()
    absent_snapshot_map = deepcopy(existing_root_map)
    absent_snapshot_map["layout_manifest"]["serving_index"].pop("snapshot_map")
    _, absent_map_summary, _ = snapshot_maps._sealed_root_summaries(absent_snapshot_map)
    assert absent_map_summary.object_kinds == ()


def test_sealed_reservation_rejects_incomplete_root() -> None:
    """A building root cannot be reused as a sealed V4 reservation."""

    _, existing_root_map = _sealed_taxonomy_reservation_fixture()
    incomplete_root = deepcopy(existing_root_map)
    incomplete_root["root_state"] = "building"
    with pytest.raises(RuntimeError, match="reuse root is inconsistent"):
        snapshot_maps._validate_sealed_reservation(incomplete_root)


def test_migration_installs_guard_and_cascading_snapshot_ownership() -> None:
    migration_path = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "20260724120000_ptg2_v4_taxonomy_candidates.py"
    )
    spec = importlib.util.spec_from_file_location(
        "test_ptg2_v4_taxonomy_candidate_migration",
        migration_path,
    )
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)

    class _Recorder:
        def __init__(self) -> None:
            self.sql: list[str] = []

        def execute(self, statement) -> None:
            self.sql.append(str(statement))

    recorder = _Recorder()
    migration.op = recorder
    migration._schema = lambda: "mrf"
    migration.upgrade()
    upgrade_sql = " ".join(" ".join(statement.split()) for statement in recorder.sql)
    assert "ptg2_v4_inferred_taxonomy_candidate_root_fkey" in upgrade_sql
    assert "ON DELETE CASCADE" in upgrade_sql
    assert "guard_ptg2_v4_snapshot_metadata" in upgrade_sql
    assert "max_online_filtered_reverse_code_sets" not in upgrade_sql
    assert "representation varchar(16) NOT NULL" in upgrade_sql
    assert "pattern_member_payload bytea NOT NULL" in upgrade_sql
    assert "pattern_member_digest bytea NOT NULL" in upgrade_sql
    assert "'direct_v1', 'pattern_v1', 'observe_v1'" in upgrade_sql
    assert "representation = 'observe_v1'" in upgrade_sql
    assert "member_count = 37001" in upgrade_sql
    assert "pattern_member_bytes = 0" in upgrade_sql
    assert "pattern_member_count >= pattern_count" in upgrade_sql
    assert "pattern_member_bytes = 24" in upgrade_sql

    migration.downgrade()
    assert "DROP TABLE IF EXISTS" in recorder.sql[-1]
