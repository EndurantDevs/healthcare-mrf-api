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

def test_manifest_validator_rejects_invalid_top_level_shapes() -> None:
    """Reject malformed containers, counters, and rule entries before sealing."""

    manifest = candidates.shape_v4_inferred_taxonomy_projection_manifest(
        (_projection_row(_rules()[0]),),
        npi_count=3,
        pattern_count=0,
    )
    with pytest.raises(PTG2ManifestArtifactError, match="manifest is invalid"):
        candidates.validate_v4_inferred_taxonomy_projection_manifest([])

    missing_field = deepcopy(manifest)
    missing_field.pop("contract")
    boolean_count = deepcopy(manifest)
    boolean_count["rule_count"] = True
    invalid_rule_container = deepcopy(manifest)
    invalid_rule_container["rules"] = {}
    invalid_rule_entry = deepcopy(manifest)
    invalid_rule_entry["rules"] = [{}]
    invalid_observe_entry = deepcopy(manifest)
    invalid_observe_entry["observe_only_rules"] = [{}]
    malformed_manifests = (
        missing_field,
        boolean_count,
        invalid_rule_container,
        invalid_rule_entry,
        invalid_observe_entry,
    )
    for malformed_manifest in malformed_manifests:
        with pytest.raises(PTG2ManifestArtifactError):
            candidates.validate_v4_inferred_taxonomy_projection_manifest(
                malformed_manifest
            )

@pytest.mark.parametrize("npi_count", (True, -1))
@pytest.mark.asyncio
async def test_publication_rejects_invalid_npi_dictionary_bounds(npi_count) -> None:
    """Reject boolean and out-of-range NPI dictionary bounds before storage work."""

    with pytest.raises(ValueError, match="NPI count"):
        await candidates.publish_v4_inferred_taxonomy_candidates(
            object(),
            schema_name="mrf",
            snapshot_key=41,
            build_token="build-token",
            rules=(_rules()[0],),
            npi_count=npi_count,
            representation="direct_v1",
            pattern_count=0,
        )


@pytest.mark.parametrize(
    ("catalog_row", "message"),
    (
        (
            {
                "npi_key": 3,
                "npi": 1_234_567_890,
                "matched_taxonomy_codes": ["AAA"],
            },
            "NPI key",
        ),
        (
            {
                "npi_key": 0,
                "npi": 999,
                "matched_taxonomy_codes": ["AAA"],
            },
            "NPI is invalid",
        ),
        (
            {
                "npi_key": 0,
                "npi": 1_234_567_890,
                "matched_taxonomy_codes": [],
            },
            "catalog evidence",
        ),
    ),
)
@pytest.mark.asyncio
async def test_publication_rejects_invalid_catalog_evidence(
    monkeypatch,
    catalog_row,
    message,
) -> None:
    """Reject catalog rows that escape the sealed NPI and taxonomy identities."""

    async def no_op_lock(*_args, **_kwargs):
        return None

    monkeypatch.setattr(
        snapshot_maps,
        "lock_v4_shared_layout_for_map_write",
        no_op_lock,
    )
    session = _PublicationSession(catalog_rows_by_codes={("AAA",): (catalog_row,)})
    with pytest.raises(RuntimeError, match=message):
        await candidates.publish_v4_inferred_taxonomy_candidates(
            session,
            schema_name="mrf",
            snapshot_key=41,
            build_token="build-token",
            rules=(_rules()[0],),
            npi_count=3,
            representation="direct_v1",
            pattern_count=0,
        )


@pytest.mark.asyncio
async def test_publication_is_individual_only_packed_and_rule_stable(
    monkeypatch,
) -> None:
    """Publish stable individual candidates and authenticate their rule set."""

    monkeypatch.setattr(
        snapshot_maps,
        "lock_v4_shared_layout_for_map_write",
        _noop_map_write_lock,
    )
    session = _PublicationSession()
    publication = await _publish_candidate_projection(
        session,
        representation="direct_v1",
        pattern_count=0,
    )

    _assert_direct_publication_contract(publication, session)

    first_rule = _rules()[0]
    renamed_rule = replace(first_rule, display_terms=("renamed",))
    changed_rule = replace(first_rule, ranges=((11, 19),))
    assert candidates.inferred_provider_taxonomy_rule_digest(
        first_rule
    ) == candidates.inferred_provider_taxonomy_rule_digest(renamed_rule)
    assert candidates.inferred_provider_taxonomy_rule_digest(
        first_rule
    ) != candidates.inferred_provider_taxonomy_rule_digest(changed_rule)
    assert candidates.inferred_provider_taxonomy_rule_set_digest(
        _rules()
    ) != candidates.inferred_provider_taxonomy_rule_set_digest(
        (changed_rule, _rules()[1])
    )


@pytest.mark.asyncio
async def test_publication_accepts_rule_scoped_pattern_projection(
    monkeypatch,
) -> None:
    """Publish exact rule-scoped pattern postings under the sealed root."""

    monkeypatch.setattr(
        snapshot_maps,
        "lock_v4_shared_layout_for_map_write",
        _noop_map_write_lock,
    )
    graph_calls: list[dict[str, Any]] = []

    async def lookup_building_patterns(_session, **kwargs):
        graph_calls.append(dict(kwargs))
        return {
            npi_key: ((2, 9) if npi_key == 0 else (2,))
            for npi_key in kwargs["owner_keys"]
        }

    monkeypatch.setattr(
        v4_graph,
        "lookup_building_v4_relation_members",
        lookup_building_patterns,
    )
    first_rule_digest = candidates.inferred_provider_taxonomy_rule_digest(_rules()[0])
    session = _PublicationSession()
    publication = await _publish_candidate_projection(
        session,
        representation="pattern_v1",
        pattern_count=10,
    )

    row_by_rule_digest = {
        stored_row["rule_digest"]: stored_row for stored_row in session.stored_rows
    }
    factored = row_by_rule_digest[first_rule_digest]
    assert factored["representation"] == "pattern_v1"
    assert factored["pattern_count"] == 2
    assert factored["pattern_member_count"] == 3
    assert candidates.unpack_inferred_taxonomy_pattern_npi_keys(
        factored["pattern_member_payload"],
        pattern_count=2,
        pattern_member_count=3,
    ) == {2: (0, 2), 9: (0,)}
    assert publication.pattern_count == 3
    assert publication.pattern_member_count == 4
    assert {stored_row["representation"] for stored_row in session.stored_rows} == {
        "pattern_v1"
    }
    assert len(graph_calls) == 2
    assert all(call["relation"] == "npi_patterns" for call in graph_calls)
    assert all(call["build_token"] == "build-token" for call in graph_calls)
    assert all(call["snapshot_key"] == 41 for call in graph_calls)
    assert all(call["max_members"] == 131_072 for call in graph_calls)


@pytest.mark.asyncio
async def test_publication_bounds_rule_catalog_before_materialization(
    monkeypatch,
) -> None:
    async def no_op_lock(*_args, **_kwargs):
        return None

    monkeypatch.setattr(
        snapshot_maps,
        "lock_v4_shared_layout_for_map_write",
        no_op_lock,
    )
    monkeypatch.setattr(
        candidates,
        "PTG2_V4_MAX_ONLINE_INFERRED_TAXONOMY_CANDIDATES",
        1,
    )
    session = _PublicationSession()
    publication = await candidates.publish_v4_inferred_taxonomy_candidates(
        session,
        schema_name="mrf",
        snapshot_key=41,
        build_token="build-token",
        rules=_rules(),
        npi_count=3,
        representation="direct_v1",
        pattern_count=0,
    )

    assert all(call["candidate_limit"] == 2 for call in session.catalog_calls)
    assert all(len(call["taxonomy_codes"]) == 1 for call in session.catalog_calls)
    assert publication.rule_count == 1
    assert publication.observe_only_rule_count == 1
    assert {stored_row["representation"] for stored_row in session.stored_rows} == {
        "direct_v1",
        "observe_v1",
    }
    assert publication.manifest["observe_only_rules"][0]["status"] == ("observe_only")
    assert publication.manifest["observe_only_rules"][0]["reason"] == (
        "candidate_cap_exceeded"
    )
    assert (
        publication.manifest["observe_only_rules"][0]["observed_count_lower_bound"] == 2
    )
    summarized = await candidates.summarize_v4_inferred_taxonomy_candidates(
        session,
        schema_name="mrf",
        snapshot_key=41,
        npi_count=3,
        pattern_count=0,
        rules=_rules(),
    )
    assert summarized == publication.manifest


@pytest.mark.asyncio
async def test_publication_can_seal_an_all_observe_rule_set(monkeypatch) -> None:
    """Seal a rule set whose every member vector exceeds the online cap."""

    monkeypatch.setattr(
        snapshot_maps,
        "lock_v4_shared_layout_for_map_write",
        _noop_map_write_lock,
    )
    monkeypatch.setattr(
        candidates,
        "PTG2_V4_MAX_ONLINE_INFERRED_TAXONOMY_CANDIDATES",
        1,
    )
    session = _PublicationSession(
        catalog_rows_by_codes={
            ("AAA",): (
                {
                    "npi_key": 0,
                    "npi": 1_234_567_890,
                    "matched_taxonomy_codes": ["AAA"],
                },
                {
                    "npi_key": 1,
                    "npi": 1_234_567_891,
                    "matched_taxonomy_codes": ["AAA"],
                },
            ),
            ("BBB",): (
                {
                    "npi_key": 1,
                    "npi": 1_234_567_891,
                    "matched_taxonomy_codes": ["BBB"],
                },
                {
                    "npi_key": 2,
                    "npi": 1_234_567_892,
                    "matched_taxonomy_codes": ["BBB"],
                },
            ),
        }
    )
    publication = await _publish_candidate_projection(
        session,
        representation="pattern_v1",
        pattern_count=10,
    )

    assert publication.rule_count == 0
    assert publication.observe_only_rule_count == 2
    assert publication.manifest["rules"] == []
    assert len(publication.manifest["observe_only_rules"]) == 2
    assert {stored_row["representation"] for stored_row in session.stored_rows} == {
        "observe_v1"
    }
    assert (
        candidates.validate_v4_inferred_taxonomy_projection_manifest(
            publication.manifest
        )
        == publication.manifest
    )


@pytest.mark.asyncio
async def test_pattern_publication_rejects_root_and_projection_drift(
    monkeypatch,
) -> None:
    """Reject pattern evidence that changes its root or candidate coverage."""

    monkeypatch.setattr(
        snapshot_maps,
        "lock_v4_shared_layout_for_map_write",
        _noop_map_write_lock,
    )

    async def out_of_root_patterns(_session, **kwargs):
        return {npi_key: (2,) for npi_key in kwargs["owner_keys"]}

    monkeypatch.setattr(
        v4_graph,
        "lookup_building_v4_relation_members",
        out_of_root_patterns,
    )
    with pytest.raises(RuntimeError, match="outside its root"):
        await _publish_candidate_projection(
            _PublicationSession(),
            representation="pattern_v1",
            pattern_count=2,
        )

    async def excessive_patterns(_session, **kwargs):
        return {npi_key: (0,) for npi_key in kwargs["owner_keys"]}

    monkeypatch.setattr(
        v4_graph,
        "lookup_building_v4_relation_members",
        excessive_patterns,
    )
    monkeypatch.setattr(
        candidates,
        "PTG2_V4_MAX_ONLINE_CANDIDATE_PATTERN_PROJECTION_MEMBERS",
        1,
    )
    publication = await _publish_candidate_projection(
        _PublicationSession(),
        representation="pattern_v1",
        pattern_count=2,
    )
    assert publication.rule_count == 1
    assert publication.observe_only_rule_count == 1
    assert {
        rule_manifest["reason"]
        for rule_manifest in publication.manifest["observe_only_rules"]
    } == {"pattern_projection_cap_exceeded"}
    assert {
        rule_manifest["observed_count_lower_bound"]
        for rule_manifest in publication.manifest["observe_only_rules"]
    } == {2}


@pytest.mark.asyncio
async def test_pattern_publication_empty_evidence_is_explicit(
    monkeypatch,
) -> None:
    async def no_op_lock(*_args, **_kwargs):
        return None

    monkeypatch.setattr(
        snapshot_maps,
        "lock_v4_shared_layout_for_map_write",
        no_op_lock,
    )
    pattern_lookup = AsyncMock(
        side_effect=lambda _session, **kwargs: {
            npi_key: () for npi_key in kwargs["owner_keys"]
        }
    )

    monkeypatch.setattr(
        v4_graph,
        "lookup_building_v4_relation_members",
        pattern_lookup,
    )
    with pytest.raises(RuntimeError, match="pattern evidence is incomplete"):
        await candidates.publish_v4_inferred_taxonomy_candidates(
            _PublicationSession(),
            schema_name="mrf",
            snapshot_key=41,
            build_token="build-token",
            rules=_rules(),
            npi_count=3,
            representation="pattern_v1",
            pattern_count=10,
        )
    assert pattern_lookup.await_count == 1

    empty_session = _PublicationSession(
        catalog_rows_by_codes={("AAA",): (), ("BBB",): ()}
    )
    publication = await candidates.publish_v4_inferred_taxonomy_candidates(
        empty_session,
        schema_name="mrf",
        snapshot_key=41,
        build_token="build-token",
        rules=_rules(),
        npi_count=3,
        representation="pattern_v1",
        pattern_count=10,
    )
    assert pattern_lookup.await_count == 1
    assert publication.member_count == 0
    assert publication.pattern_member_count == 0
    assert {
        stored_row["representation"] for stored_row in empty_session.stored_rows
    } == {"direct_v1"}
