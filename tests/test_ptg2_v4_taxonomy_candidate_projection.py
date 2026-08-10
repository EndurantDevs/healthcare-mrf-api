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

def test_pattern_posting_codec_is_compact_strict_and_deterministic() -> None:
    packed_pattern_payload = candidates.pack_inferred_taxonomy_pattern_npi_keys(
        {9: (2, 5), 2: (0, 2)}
    )
    assert len(packed_pattern_payload) == 24 + 2 * 8 + 4 * 4
    assert candidates.unpack_inferred_taxonomy_pattern_npi_keys(
        packed_pattern_payload,
        pattern_count=2,
        pattern_member_count=4,
    ) == {2: (0, 2), 9: (2, 5)}
    assert packed_pattern_payload == candidates.pack_inferred_taxonomy_pattern_npi_keys(
        {2: (0, 2), 9: (2, 5)}
    )

    with pytest.raises(ValueError, match="strict uint32 order"):
        candidates.pack_inferred_taxonomy_pattern_npi_keys({2: (2, 2)})
    with pytest.raises(PTG2ManifestArtifactError, match="trailing data"):
        candidates.unpack_inferred_taxonomy_pattern_npi_keys(
            packed_pattern_payload + b"\x00",
            pattern_count=2,
            pattern_member_count=4,
        )
    with pytest.raises(PTG2ManifestArtifactError, match="truncated"):
        candidates.unpack_inferred_taxonomy_pattern_npi_keys(
            packed_pattern_payload[:-1],
            pattern_count=2,
            pattern_member_count=4,
        )
    non_strict = bytearray(
        candidates.pack_inferred_taxonomy_pattern_npi_keys({2: (0, 2)})
    )
    non_strict[36:40] = (0).to_bytes(4, "little")
    with pytest.raises(PTG2ManifestArtifactError, match="NPI keys"):
        candidates.unpack_inferred_taxonomy_pattern_npi_keys(
            bytes(non_strict),
            pattern_count=1,
            pattern_member_count=2,
        )


@pytest.mark.parametrize(
    ("representation", "pattern_count"),
    (
        ("direct_v1", True),
        ("direct_v1", 1),
        ("pattern_v1", 0),
        ("unknown", 0),
    ),
)
def test_publication_rejects_inconsistent_root_identity(
    representation: str,
    pattern_count: int,
) -> None:
    with pytest.raises(ValueError, match="root identity"):
        candidates._normalized_root_identity(representation, pattern_count)


@pytest.mark.asyncio
async def test_pattern_projection_short_circuits_empty_and_rejects_gaps(
    monkeypatch,
) -> None:
    """Avoid graph work for empty candidates and reject incomplete graph replies."""

    assert (
        await candidates._candidate_pattern_postings_for_rule(
            object(),
            schema_name="mrf",
            snapshot_key=41,
            build_token="build-token",
            candidate_npi_keys=(),
            root_pattern_count=3,
        )
        == {}
    )
    monkeypatch.setattr(
        v4_graph,
        "lookup_building_v4_relation_members",
        AsyncMock(return_value={}),
    )
    with pytest.raises(RuntimeError, match="incomplete"):
        await candidates._candidate_pattern_postings_for_rule(
            object(),
            schema_name="mrf",
            snapshot_key=41,
            build_token="build-token",
            candidate_npi_keys=(1,),
            root_pattern_count=3,
        )


@pytest.mark.parametrize(
    ("graph_message", "error_type", "message"),
    (
        (
            "different graph failure",
            PTG2SharedBlockError,
            "different graph failure",
        ),
        (
            "PTG V4 graph selection exceeds max_members",
            candidates._PatternProjectionCapExceeded,
            "pattern projection exceeds the online cap",
        ),
    ),
)
@pytest.mark.asyncio
async def test_pattern_projection_preserves_graph_failure_semantics(
    monkeypatch,
    graph_message: str,
    error_type: type[Exception],
    message: str,
) -> None:
    monkeypatch.setattr(
        v4_graph,
        "lookup_building_v4_relation_members",
        AsyncMock(side_effect=PTG2SharedBlockError(graph_message)),
    )

    with pytest.raises(error_type, match=message):
        await candidates._candidate_pattern_postings_for_rule(
            object(),
            schema_name="mrf",
            snapshot_key=41,
            build_token="build-token",
            candidate_npi_keys=(1,),
            root_pattern_count=3,
        )


@pytest.mark.asyncio
async def test_pattern_projection_rejects_boolean_pattern_keys(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        v4_graph,
        "lookup_building_v4_relation_members",
        AsyncMock(return_value={1: (True,)}),
    )

    with pytest.raises(RuntimeError, match="pattern key is invalid"):
        await candidates._candidate_pattern_postings_for_rule(
            object(),
            schema_name="mrf",
            snapshot_key=41,
            build_token="build-token",
            candidate_npi_keys=(1,),
            root_pattern_count=3,
        )


def test_v2_row_shaper_rejects_missing_candidate_and_pattern_bound() -> None:
    missing_candidate = _projection_row(
        _rules()[0],
        npi_keys_by_pattern={9: (0,)},
    )
    with pytest.raises(RuntimeError, match="projection is incomplete"):
        candidates.shape_v4_inferred_taxonomy_projection_manifest(
            (missing_candidate,),
            npi_count=3,
            pattern_count=10,
        )

    out_of_range_pattern = _projection_row(
        _rules()[0],
        npi_keys_by_pattern={9: (0, 2)},
    )
    with pytest.raises(RuntimeError, match="exceeds its pattern root"):
        candidates.shape_v4_inferred_taxonomy_projection_manifest(
            (out_of_range_pattern,),
            npi_count=3,
            pattern_count=9,
        )


@pytest.mark.parametrize(
    "cap_field",
    (
        "max_online_inferred_taxonomy_candidates",
        "max_online_candidate_pattern_projection_members",
    ),
)
def test_manifest_validator_enforces_per_rule_caps(cap_field: str) -> None:
    manifest = candidates.shape_v4_inferred_taxonomy_projection_manifest(
        (
            _projection_row(
                _rules()[0],
                npi_keys_by_pattern={9: (0, 2)},
            ),
        ),
        npi_count=3,
        pattern_count=10,
    )
    manifest[cap_field] = 1

    with pytest.raises(PTG2ManifestArtifactError, match="projection rule"):
        candidates.validate_v4_inferred_taxonomy_projection_manifest(manifest)


def test_observe_rule_is_explicit_fallback_and_status_is_authenticated(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        candidates,
        "PTG2_V4_MAX_ONLINE_INFERRED_TAXONOMY_CANDIDATES",
        1,
    )
    observe_row = _observe_projection_row(_rules()[0], (0, 2))
    manifest = candidates.shape_v4_inferred_taxonomy_projection_manifest(
        (observe_row,),
        npi_count=3,
        pattern_count=0,
    )

    assert (
        candidates.resolve_inferred_taxonomy_projection_rule_manifest(
            manifest,
            observe_row["rule_digest"],
        )
        is None
    )
    with pytest.raises(PTG2ManifestArtifactError, match="observe-only"):
        candidates.inferred_taxonomy_projection_rule_manifest(
            manifest,
            observe_row["rule_digest"],
        )

    tampered = deepcopy(manifest)
    tampered["observe_only_rules"][0]["reason"] = "not-the-sealed-reason"
    with pytest.raises(PTG2ManifestArtifactError, match="observe rule"):
        candidates.validate_v4_inferred_taxonomy_projection_manifest(tampered)

    missing = deepcopy(manifest)
    missing["observe_only_rules"] = []
    missing["observe_only_rule_count"] = 0
    with pytest.raises(PTG2ManifestArtifactError):
        candidates.resolve_inferred_taxonomy_projection_rule_manifest(
            missing,
            observe_row["rule_digest"],
        )


def test_projection_rule_resolution_scans_and_rejects_unknown_digest() -> None:
    projection_rows = tuple(
        sorted(
            (_projection_row(rule) for rule in _rules()),
            key=lambda projection_row: projection_row["rule_digest"],
        )
    )
    manifest = candidates.shape_v4_inferred_taxonomy_projection_manifest(
        projection_rows,
        npi_count=3,
        pattern_count=0,
    )

    resolved = candidates.resolve_inferred_taxonomy_projection_rule_manifest(
        manifest,
        projection_rows[1]["rule_digest"],
    )
    assert resolved is not None
    assert resolved.rule_digest == projection_rows[1]["rule_digest"]
    with pytest.raises(PTG2ManifestArtifactError, match="not in the sealed"):
        candidates.resolve_inferred_taxonomy_projection_rule_manifest(
            manifest,
            b"x" * 32,
        )


def test_compatibility_manifest_accepts_an_empty_direct_projection() -> None:
    empty_projection = _projection_row(_rules()[0], npi_keys=())

    manifest = candidates._candidate_projection_manifest((empty_projection,))

    assert manifest["rules"][0]["member_count"] == 0
    assert manifest["pattern_count"] == 0
