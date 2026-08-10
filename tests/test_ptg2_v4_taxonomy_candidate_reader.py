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

@pytest.mark.asyncio
async def test_persisted_summary_enforces_global_pattern_bound() -> None:
    projection_row = _projection_row(
        _rules()[0],
        npi_keys_by_pattern={9: (0, 2)},
    )
    session = _ScriptedSession(_Result((projection_row,)))

    with pytest.raises(RuntimeError, match="exceeds its pattern root"):
        await candidates.summarize_v4_inferred_taxonomy_candidates(
            session,
            schema_name="mrf",
            snapshot_key=41,
            npi_count=3,
            pattern_count=9,
            rules=(_rules()[0],),
        )


@pytest.mark.asyncio
async def test_reader_uses_one_bounded_authenticated_round_trip() -> None:
    projection_row = _projection_row(
        _rules()[0],
        npi_keys_by_pattern={9: (0, 2)},
    )
    manifest = candidates._candidate_projection_manifest((projection_row,))
    session = _ScriptedSession(_Result((_reader_row(projection_row),)))

    loaded = await candidates.load_v4_inferred_taxonomy_candidates(
        session,
        snapshot_key=41,
        rule_digest=projection_row["rule_digest"],
        schema_name="mrf",
        projection_manifest=manifest,
    )

    assert loaded.npi_keys == (0, 2)
    assert loaded.representation == "pattern_v1"
    assert loaded.npi_keys_by_pattern == {9: (0, 2)}
    assert len(session.calls) == 1
    assert "candidate.member_keys" in session.calls[0][0]
    assert "candidate.pattern_member_payload" in session.calls[0][0]


@pytest.mark.asyncio
async def test_reader_rejects_manifest_payload_and_cap_tamper() -> None:
    """Reject sealed manifest, payload, root, and cap drift before serving."""

    projection_row = _projection_row(_rules()[0])
    manifest = candidates._candidate_projection_manifest((projection_row,))
    tampered_manifest = deepcopy(manifest)
    tampered_manifest["projection_digest"] = "0" * 64
    no_query_session = _ScriptedSession()
    await _assert_candidate_load_rejected(
        no_query_session, projection_row, tampered_manifest, "digest changed"
    )
    assert no_query_session.calls == []

    tampered_payload = bytearray(projection_row["member_keys"])
    tampered_payload[-1] ^= 1
    payload_session = _ScriptedSession(
        _Result(
            (
                _reader_row(
                    projection_row,
                    member_keys=bytes(tampered_payload),
                ),
            )
        )
    )
    await _assert_candidate_load_rejected(
        payload_session, projection_row, manifest, "digest changed"
    )

    pattern_row, pattern_manifest, pattern_session = _tampered_pattern_projection()
    await _assert_candidate_load_rejected(
        pattern_session,
        pattern_row,
        pattern_manifest,
        "pattern digest changed",
    )

    pattern_bound_session = _ScriptedSession(
        _Result((_reader_row(pattern_row, root_pattern_count=9),))
    )
    await _assert_candidate_load_rejected(
        pattern_bound_session,
        pattern_row,
        pattern_manifest,
        "violates its root",
    )

    capped_manifest = deepcopy(manifest)
    capped_manifest["max_online_inferred_taxonomy_candidates"] = 1
    capped_session = _ScriptedSession()
    await _assert_candidate_load_rejected(
        capped_session, projection_row, capped_manifest, "projection rule"
    )
    assert capped_session.calls == []


@pytest.mark.parametrize(
    ("metadata_updates", "message"),
    (
        (None, "vector is unavailable"),
        ({"root_state": "building"}, "snapshot is not complete"),
        ({"catalog_contract": "incompatible"}, "contract is incompatible"),
        ({"member_bytes": 7}, "metadata is inconsistent"),
        ({"catalog_digest": b"z" * 32}, "metadata changed from its seal"),
    ),
)
@pytest.mark.asyncio
async def test_reader_rejects_unsealed_metadata_states(
    metadata_updates: dict[str, Any] | None,
    message: str,
) -> None:
    projection_row = _projection_row(_rules()[0])
    manifest = candidates._candidate_projection_manifest((projection_row,))
    metadata_rows = ()
    if metadata_updates is not None:
        metadata_by_field = _reader_row(projection_row)
        metadata_by_field.update(metadata_updates)
        metadata_rows = (metadata_by_field,)

    await _assert_candidate_load_rejected(
        _ScriptedSession(_Result(metadata_rows)),
        projection_row,
        manifest,
        message,
    )
