# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
import importlib.util
from pathlib import Path
from typing import Any, Iterable
from unittest.mock import AsyncMock

import pytest

from api.ptg2_code_filters import InferredProviderTaxonomyRule
from api import ptg2_v4_graph as v4_graph
from process.ptg_parts import ptg2_v4_snapshot_maps as snapshot_maps
from process.ptg_parts import ptg2_v4_taxonomy_candidates as candidates
from process.ptg_parts.ptg2_manifest_artifacts import (
    PTG2ManifestArtifactError,
)
from tests.ptg2_v4_coverage_support import _metadata, _summary


class _Result:
    def __init__(self, rows: Iterable[Any] = ()) -> None:
        self.rows = tuple(rows)

    def __iter__(self):
        return iter(self.rows)


class _ScriptedSession:
    def __init__(self, *results: _Result) -> None:
        self.results = list(results)
        self.calls: list[tuple[str, Any]] = []

    async def execute(self, statement, parameters=None):
        self.calls.append((str(statement), parameters))
        if not self.results:
            raise AssertionError(f"unexpected SQL: {statement}")
        return self.results.pop(0)


class _PublicationSession:
    def __init__(
        self,
        *,
        catalog_rows_by_codes: dict[
            tuple[str, ...], tuple[dict[str, Any], ...]
        ]
        | None = None,
    ) -> None:
        self.catalog_sql = ""
        self.catalog_calls: list[dict[str, Any]] = []
        self.stored_rows: list[dict[str, Any]] = []
        self.catalog_rows_by_codes = catalog_rows_by_codes

    async def execute(self, statement, parameters=None):
        sql = str(statement)
        if "ARRAY_AGG" in sql:
            self.catalog_sql = sql
            normalized_parameters = dict(parameters)
            self.catalog_calls.append(normalized_parameters)
            taxonomy_codes = tuple(normalized_parameters["taxonomy_codes"])
            default_rows = {
                ("AAA",): (
                    {
                        "npi_key": 0,
                        "npi": 1_234_567_890,
                        "matched_taxonomy_codes": ["AAA"],
                    },
                    {
                        "npi_key": 2,
                        "npi": 1_234_567_892,
                        "matched_taxonomy_codes": ["AAA"],
                    },
                ),
                ("BBB",): (
                    {
                        "npi_key": 2,
                        "npi": 1_234_567_892,
                        "matched_taxonomy_codes": ["BBB"],
                    },
                ),
            }
            rows_by_codes = self.catalog_rows_by_codes or default_rows
            return _Result(
                rows_by_codes.get(taxonomy_codes, ())
            )
        if "INSERT INTO" in sql:
            self.stored_rows = [dict(row) for row in parameters]
            return _Result()
        if "SELECT rule_digest" in sql:
            return _Result(self.stored_rows)
        raise AssertionError(f"unexpected SQL: {sql}")


def _rules() -> tuple[InferredProviderTaxonomyRule, ...]:
    return (
        InferredProviderTaxonomyRule(
            ranges=((10, 19),),
            taxonomy_codes=("AAA",),
            display_terms=("first display",),
        ),
        InferredProviderTaxonomyRule(
            ranges=((20, 29),),
            taxonomy_codes=("BBB",),
            display_terms=("second display",),
        ),
    )


def _projection_row(
    rule: InferredProviderTaxonomyRule,
    npi_keys: tuple[int, ...] = (0, 2),
    npi_keys_by_pattern: dict[int, tuple[int, ...]] | None = None,
) -> dict[str, Any]:
    rule_digest = candidates.inferred_provider_taxonomy_rule_digest(rule)
    payload = candidates.pack_inferred_taxonomy_npi_keys(npi_keys)
    pattern_members = npi_keys_by_pattern or {}
    pattern_payload = candidates.pack_inferred_taxonomy_pattern_npi_keys(
        pattern_members
    )
    representation = (
        candidates.PTG2_V4_INFERRED_TAXONOMY_PATTERN_REPRESENTATION
        if pattern_payload
        else candidates.PTG2_V4_INFERRED_TAXONOMY_DIRECT_REPRESENTATION
    )
    pattern_member_count = sum(
        len(pattern_npi_keys)
        for pattern_npi_keys in pattern_members.values()
    )
    return {
        "rule_digest": rule_digest,
        "catalog_contract": (
            candidates.PTG2_V4_INFERRED_TAXONOMY_CATALOG_CONTRACT
        ),
        "catalog_digest": b"c" * 32,
        "vector_format": candidates.PTG2_V4_INFERRED_TAXONOMY_VECTOR_FORMAT,
        "member_count": len(npi_keys),
        "member_digest": candidates.inferred_taxonomy_member_digest(
            rule_digest,
            member_count=len(npi_keys),
            payload=payload,
        ),
        "member_keys": payload,
        "representation": representation,
        "pattern_count": len(pattern_members),
        "pattern_member_count": pattern_member_count,
        "pattern_member_bytes": len(pattern_payload),
        "pattern_member_digest": (
            candidates.inferred_taxonomy_pattern_member_digest(
                rule_digest,
                representation=representation,
                pattern_count=len(pattern_members),
                pattern_member_count=pattern_member_count,
                payload=pattern_payload,
            )
        ),
        "pattern_member_payload": pattern_payload,
    }


def _observe_projection_row(
    rule: InferredProviderTaxonomyRule,
    npi_keys: tuple[int, ...],
) -> dict[str, Any]:
    rule_digest = candidates.inferred_provider_taxonomy_rule_digest(rule)
    payload = candidates.pack_inferred_taxonomy_npi_keys(npi_keys)
    representation = (
        candidates.PTG2_V4_INFERRED_TAXONOMY_OBSERVE_REPRESENTATION
    )
    return {
        "rule_digest": rule_digest,
        "catalog_contract": (
            candidates.PTG2_V4_INFERRED_TAXONOMY_CATALOG_CONTRACT
        ),
        "catalog_digest": b"o" * 32,
        "vector_format": candidates.PTG2_V4_INFERRED_TAXONOMY_VECTOR_FORMAT,
        "member_count": len(npi_keys),
        "member_digest": candidates.inferred_taxonomy_member_digest(
            rule_digest,
            member_count=len(npi_keys),
            payload=payload,
        ),
        "member_keys": payload,
        "representation": representation,
        "observe_reason": (
            candidates.PTG2_V4_INFERRED_TAXONOMY_CANDIDATE_CAP_REASON
        ),
        "observe_count_lower_bound": len(npi_keys),
        "pattern_count": 0,
        "pattern_member_count": 0,
        "pattern_member_bytes": 0,
        "pattern_member_digest": (
            candidates.inferred_taxonomy_pattern_member_digest(
                rule_digest,
                representation=representation,
                pattern_count=0,
                pattern_member_count=0,
                payload=b"",
            )
        ),
        "pattern_member_payload": b"",
    }


def _reader_row(
    row: dict[str, Any],
    *,
    member_keys: bytes | None = None,
    pattern_member_payload: bytes | None = None,
    root_pattern_count: int | None = None,
) -> dict[str, Any]:
    candidate_payload = row["member_keys"] if member_keys is None else member_keys
    pattern_payload = (
        row["pattern_member_payload"]
        if pattern_member_payload is None
        else pattern_member_payload
    )
    if root_pattern_count is None:
        root_pattern_count = (
            max(row.get("pattern_count", 0), 10)
            if row.get("representation") == "pattern_v1"
            else 0
        )
    return {
        "catalog_contract": row["catalog_contract"],
        "catalog_digest": row["catalog_digest"],
        "vector_format": row["vector_format"],
        "member_count": row["member_count"],
        "member_digest": row["member_digest"],
        "member_keys": candidate_payload,
        "member_bytes": len(candidate_payload),
        "representation": row["representation"],
        "pattern_count": row["pattern_count"],
        "pattern_member_count": row["pattern_member_count"],
        "pattern_member_bytes": row["pattern_member_bytes"],
        "pattern_member_digest": row["pattern_member_digest"],
        "pattern_member_payload": pattern_payload,
        "pattern_payload_bytes": len(pattern_payload),
        "root_state": "complete",
        "npi_count": 3,
        "root_pattern_count": root_pattern_count,
    }


@pytest.mark.asyncio
async def test_publication_is_individual_only_packed_and_rule_stable(
    monkeypatch,
) -> None:
    async def no_op_lock(*_args, **_kwargs):
        return None

    monkeypatch.setattr(
        snapshot_maps,
        "lock_v4_shared_layout_for_map_write",
        no_op_lock,
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

    assert "COALESCE(entity.entity_type_code, 0) = 1" in session.catalog_sql
    assert "LIMIT :candidate_limit" in session.catalog_sql
    assert {
        call["taxonomy_codes"] for call in session.catalog_calls
    } == {("AAA",), ("BBB",)}
    assert {
        call["candidate_limit"] for call in session.catalog_calls
    } == {37_001}
    assert publication.rule_count == 2
    assert publication.member_count == 3
    assert publication.packed_byte_count == 12
    assert candidates.validate_v4_inferred_taxonomy_projection_manifest(
        publication.manifest
    ) == publication.manifest
    assert publication.manifest[
        "max_online_filtered_reverse_code_sets"
    ] == 6_600
    assert publication.manifest[
        "max_online_filtered_reverse_code_occurrences"
    ] == 6_700
    assert publication.manifest[
        "max_online_inferred_taxonomy_candidates"
    ] == 37_000
    assert publication.manifest[
        "max_online_candidate_pattern_projection_members"
    ] == 131_072
    assert publication.manifest[
        "max_online_inferred_taxonomy_retained_memberships"
    ] == 65_536
    assert publication.manifest[
        "max_online_inferred_taxonomy_graph_pages"
    ] == 256
    assert publication.manifest[
        "max_online_inferred_taxonomy_graph_bytes"
    ] == 4_194_304
    assert publication.manifest[
        "max_online_inferred_taxonomy_graph_batches"
    ] == 32
    assert publication.manifest["pattern_count"] == 0
    assert publication.manifest["pattern_member_count"] == 0
    assert publication.manifest["pattern_member_bytes"] == 0
    assert {
        row["representation"] for row in session.stored_rows
    } == {"direct_v1"}

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
    async def no_op_lock(*_args, **_kwargs):
        return None

    monkeypatch.setattr(
        snapshot_maps,
        "lock_v4_shared_layout_for_map_write",
        no_op_lock,
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
    first_rule_digest = candidates.inferred_provider_taxonomy_rule_digest(
        _rules()[0]
    )
    session = _PublicationSession()
    publication = await candidates.publish_v4_inferred_taxonomy_candidates(
        session,
        schema_name="mrf",
        snapshot_key=41,
        build_token="build-token",
        rules=_rules(),
        npi_count=3,
        representation="pattern_v1",
        pattern_count=10,
    )

    row_by_rule_digest = {
        row["rule_digest"]: row for row in session.stored_rows
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
    assert {
        row["representation"] for row in session.stored_rows
    } == {"pattern_v1"}
    assert len(graph_calls) == 2
    assert all(call["relation"] == "npi_patterns" for call in graph_calls)
    assert all(call["build_token"] == "build-token" for call in graph_calls)
    assert all(call["snapshot_key"] == 41 for call in graph_calls)
    assert all(
        call["max_members"] == 131_072 for call in graph_calls
    )


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
    assert all(
        len(call["taxonomy_codes"]) == 1 for call in session.catalog_calls
    )
    assert publication.rule_count == 1
    assert publication.observe_only_rule_count == 1
    assert {
        row["representation"] for row in session.stored_rows
    } == {"direct_v1", "observe_v1"}
    assert publication.manifest["observe_only_rules"][0]["status"] == (
        "observe_only"
    )
    assert publication.manifest["observe_only_rules"][0]["reason"] == (
        "candidate_cap_exceeded"
    )
    assert publication.manifest["observe_only_rules"][0][
        "observed_count_lower_bound"
    ] == 2
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
    publication = await candidates.publish_v4_inferred_taxonomy_candidates(
        session,
        schema_name="mrf",
        snapshot_key=41,
        build_token="build-token",
        rules=_rules(),
        npi_count=3,
        representation="pattern_v1",
        pattern_count=10,
    )

    assert publication.rule_count == 0
    assert publication.observe_only_rule_count == 2
    assert publication.manifest["rules"] == []
    assert len(publication.manifest["observe_only_rules"]) == 2
    assert {
        row["representation"] for row in session.stored_rows
    } == {"observe_v1"}
    assert candidates.validate_v4_inferred_taxonomy_projection_manifest(
        publication.manifest
    ) == publication.manifest


@pytest.mark.asyncio
async def test_pattern_publication_rejects_root_and_projection_drift(
    monkeypatch,
) -> None:
    async def no_op_lock(*_args, **_kwargs):
        return None

    monkeypatch.setattr(
        snapshot_maps,
        "lock_v4_shared_layout_for_map_write",
        no_op_lock,
    )

    async def out_of_root_patterns(_session, **kwargs):
        return {npi_key: (2,) for npi_key in kwargs["owner_keys"]}

    monkeypatch.setattr(
        v4_graph,
        "lookup_building_v4_relation_members",
        out_of_root_patterns,
    )
    with pytest.raises(RuntimeError, match="outside its root"):
        await candidates.publish_v4_inferred_taxonomy_candidates(
            _PublicationSession(),
            schema_name="mrf",
            snapshot_key=41,
            build_token="build-token",
            rules=_rules(),
            npi_count=3,
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
    publication = await candidates.publish_v4_inferred_taxonomy_candidates(
        _PublicationSession(),
        schema_name="mrf",
        snapshot_key=41,
        build_token="build-token",
        rules=_rules(),
        npi_count=3,
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
        row["representation"] for row in empty_session.stored_rows
    } == {"direct_v1"}


def test_pattern_posting_codec_is_compact_strict_and_deterministic() -> None:
    payload = candidates.pack_inferred_taxonomy_pattern_npi_keys(
        {9: (2, 5), 2: (0, 2)}
    )
    assert len(payload) == 24 + 2 * 8 + 4 * 4
    assert candidates.unpack_inferred_taxonomy_pattern_npi_keys(
        payload,
        pattern_count=2,
        pattern_member_count=4,
    ) == {2: (0, 2), 9: (2, 5)}
    assert payload == candidates.pack_inferred_taxonomy_pattern_npi_keys(
        {2: (0, 2), 9: (2, 5)}
    )

    with pytest.raises(ValueError, match="strict uint32 order"):
        candidates.pack_inferred_taxonomy_pattern_npi_keys({2: (2, 2)})
    with pytest.raises(PTG2ManifestArtifactError, match="trailing data"):
        candidates.unpack_inferred_taxonomy_pattern_npi_keys(
            payload + b"\x00",
            pattern_count=2,
            pattern_member_count=4,
        )
    with pytest.raises(PTG2ManifestArtifactError, match="truncated"):
        candidates.unpack_inferred_taxonomy_pattern_npi_keys(
            payload[:-1],
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
    (("direct_v1", 1), ("pattern_v1", 0), ("unknown", 0)),
)
def test_publication_rejects_inconsistent_root_identity(
    representation: str,
    pattern_count: int,
) -> None:
    with pytest.raises(ValueError, match="root identity"):
        candidates._normalized_root_identity(representation, pattern_count)


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
        candidates.validate_v4_inferred_taxonomy_projection_manifest(
            manifest
        )


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

    assert candidates.resolve_inferred_taxonomy_projection_rule_manifest(
        manifest,
        observe_row["rule_digest"],
    ) is None
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


@pytest.mark.asyncio
async def test_persisted_summary_enforces_global_pattern_bound() -> None:
    row = _projection_row(
        _rules()[0],
        npi_keys_by_pattern={9: (0, 2)},
    )
    session = _ScriptedSession(_Result((row,)))

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
    row = _projection_row(
        _rules()[0],
        npi_keys_by_pattern={9: (0, 2)},
    )
    manifest = candidates._candidate_projection_manifest((row,))
    session = _ScriptedSession(_Result((_reader_row(row),)))

    loaded = await candidates.load_v4_inferred_taxonomy_candidates(
        session,
        snapshot_key=41,
        rule_digest=row["rule_digest"],
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
    row = _projection_row(_rules()[0])
    manifest = candidates._candidate_projection_manifest((row,))
    tampered_manifest = deepcopy(manifest)
    tampered_manifest["projection_digest"] = "0" * 64
    no_query_session = _ScriptedSession()
    with pytest.raises(PTG2ManifestArtifactError, match="digest changed"):
        await candidates.load_v4_inferred_taxonomy_candidates(
            no_query_session,
            snapshot_key=41,
            rule_digest=row["rule_digest"],
            schema_name="mrf",
            projection_manifest=tampered_manifest,
        )
    assert no_query_session.calls == []

    tampered_payload = bytearray(row["member_keys"])
    tampered_payload[-1] ^= 1
    payload_session = _ScriptedSession(
        _Result((_reader_row(row, member_keys=bytes(tampered_payload)),))
    )
    with pytest.raises(PTG2ManifestArtifactError, match="digest changed"):
        await candidates.load_v4_inferred_taxonomy_candidates(
            payload_session,
            snapshot_key=41,
            rule_digest=row["rule_digest"],
            schema_name="mrf",
            projection_manifest=manifest,
        )

    pattern_row = _projection_row(
        _rules()[0],
        npi_keys_by_pattern={9: (0, 2)},
    )
    pattern_manifest = candidates._candidate_projection_manifest(
        (pattern_row,)
    )
    tampered_pattern_payload = bytearray(
        pattern_row["pattern_member_payload"]
    )
    tampered_pattern_payload[-1] ^= 1
    pattern_session = _ScriptedSession(
        _Result(
            (
                _reader_row(
                    pattern_row,
                    pattern_member_payload=bytes(tampered_pattern_payload),
                ),
            )
        )
    )
    with pytest.raises(
        PTG2ManifestArtifactError,
        match="pattern digest changed",
    ):
        await candidates.load_v4_inferred_taxonomy_candidates(
            pattern_session,
            snapshot_key=41,
            rule_digest=pattern_row["rule_digest"],
            schema_name="mrf",
            projection_manifest=pattern_manifest,
        )

    pattern_bound_session = _ScriptedSession(
        _Result((_reader_row(pattern_row, root_pattern_count=9),))
    )
    with pytest.raises(
        PTG2ManifestArtifactError,
        match="violates its root",
    ):
        await candidates.load_v4_inferred_taxonomy_candidates(
            pattern_bound_session,
            snapshot_key=41,
            rule_digest=pattern_row["rule_digest"],
            schema_name="mrf",
            projection_manifest=pattern_manifest,
        )

    capped_manifest = deepcopy(manifest)
    capped_manifest["max_online_inferred_taxonomy_candidates"] = 1
    capped_session = _ScriptedSession()
    with pytest.raises(PTG2ManifestArtifactError, match="projection rule"):
        await candidates.load_v4_inferred_taxonomy_candidates(
            capped_session,
            snapshot_key=41,
            rule_digest=row["rule_digest"],
            schema_name="mrf",
            projection_manifest=capped_manifest,
        )
    assert capped_session.calls == []


def test_seal_and_reuse_validation_reject_projection_manifest_drift() -> None:
    projection = candidates._candidate_projection_manifest(
        (_projection_row(_rules()[0]),)
    )
    summary = _summary()
    metadata = replace(
        _metadata(),
        inferred_taxonomy_candidates=projection,
    )
    layout_manifest = snapshot_maps._manifest_with_v4_root(
        {},
        representation="pattern_v1",
        summary=summary,
        metadata=metadata,
    )
    existing = {
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
    assert snapshot_maps._validate_sealed_reservation(existing) == layout_manifest

    tampered = deepcopy(existing)
    projection_manifest = tampered["layout_manifest"]["serving_index"][
        "serving_binary"
    ]["provider_graph_v4"]["inferred_taxonomy_candidates"]
    projection_manifest["rules"][0]["member_count"] += 1
    with pytest.raises(PTG2ManifestArtifactError, match="projection rule"):
        snapshot_maps._validate_sealed_reservation(tampered)


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
    upgrade_sql = " ".join(
        " ".join(statement.split()) for statement in recorder.sql
    )
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
