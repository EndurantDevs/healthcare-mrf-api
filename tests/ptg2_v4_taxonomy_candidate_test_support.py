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
        catalog_rows_by_codes: (
            dict[tuple[str, ...], tuple[dict[str, Any], ...]] | None
        ) = None,
    ) -> None:
        self.catalog_sql = ""
        self.catalog_calls: list[dict[str, Any]] = []
        self.stored_rows: list[dict[str, Any]] = []
        self.catalog_rows_by_codes = catalog_rows_by_codes

    def _execute_catalog_query(self, sql, parameters):
        self.catalog_sql = sql
        parameters_by_name = dict(parameters)
        self.catalog_calls.append(parameters_by_name)
        taxonomy_codes = tuple(parameters_by_name["taxonomy_codes"])
        default_rows_by_codes = {
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
        rows_by_codes = self.catalog_rows_by_codes or default_rows_by_codes
        return _Result(rows_by_codes.get(taxonomy_codes, ()))

    async def execute(self, statement, parameters=None):
        sql = str(statement)
        if "ARRAY_AGG" in sql:
            return self._execute_catalog_query(sql, parameters)
        if "INSERT INTO" in sql:
            self.stored_rows = [dict(stored_row) for stored_row in parameters]
            return _Result()
        if "SELECT rule_digest" in sql:
            return _Result(self.stored_rows)
        raise AssertionError(f"unexpected SQL: {sql}")


def _compiler_rules() -> tuple[InferredProviderTaxonomyRule, ...]:
    return tuple(
        InferredProviderTaxonomyRule(
            ranges=((index * 10, index * 10 + 9),),
            taxonomy_codes=(f"T{index:02d}",),
            display_terms=(f"compiler rule {index}",),
        )
        for index in range(10)
    )


class _PreparedCompilerInputSession:
    def __init__(self, *, common_count: int, observe_count: int) -> None:
        self.common_count = common_count
        self.observe_count = observe_count
        self.calls: list[tuple[str, Any]] = []

    async def execute(self, statement, parameters=None):
        sql = str(statement)
        self.calls.append((sql, parameters))
        if "current_setting('transaction_isolation')" in sql:
            return _Result(({"isolation": "repeatable read", "read_only": "on"},))
        code = tuple(parameters["taxonomy_codes"])[0]
        rule_index = int(code[1:])
        row_count = self.common_count if rule_index < 5 else self.observe_count
        return _Result(
            {
                "npi_key": npi_key,
                "npi": 1_000_000_000 + npi_key,
                "matched_taxonomy_codes": [code],
            }
            for npi_key in range(row_count)
        )

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


async def _noop_map_write_lock(*_args, **_kwargs) -> None:
    return None


async def _publish_candidate_projection(
    session: _PublicationSession,
    *,
    representation: str,
    pattern_count: int,
):
    return await candidates.publish_v4_inferred_taxonomy_candidates(
        session,
        schema_name="mrf",
        snapshot_key=41,
        build_token="build-token",
        rules=_rules(),
        npi_count=3,
        representation=representation,
        pattern_count=pattern_count,
    )


async def _load_candidate_projection(session, projection_row, manifest):
    return await candidates.load_v4_inferred_taxonomy_candidates(
        session,
        snapshot_key=41,
        rule_digest=projection_row["rule_digest"],
        schema_name="mrf",
        projection_manifest=manifest,
    )


async def _assert_candidate_load_rejected(
    session,
    projection_row,
    manifest,
    message: str,
) -> None:
    with pytest.raises(PTG2ManifestArtifactError, match=message):
        await _load_candidate_projection(session, projection_row, manifest)


def _tampered_pattern_projection():
    pattern_row = _projection_row(
        _rules()[0],
        npi_keys_by_pattern={9: (0, 2)},
    )
    pattern_manifest = candidates._candidate_projection_manifest((pattern_row,))
    tampered_pattern_payload = bytearray(pattern_row["pattern_member_payload"])
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
    return pattern_row, pattern_manifest, pattern_session


def _assert_direct_publication_contract(publication, session) -> None:
    assert "COALESCE(entity.entity_type_code, 0) = 1" in session.catalog_sql
    assert "LIMIT :candidate_limit" in session.catalog_sql
    assert {call["taxonomy_codes"] for call in session.catalog_calls} == {
        ("AAA",),
        ("BBB",),
    }
    assert {call["candidate_limit"] for call in session.catalog_calls} == {37_001}
    assert publication.rule_count == 2
    assert publication.member_count == 3
    assert publication.packed_byte_count == 12
    assert (
        candidates.validate_v4_inferred_taxonomy_projection_manifest(
            publication.manifest
        )
        == publication.manifest
    )
    expected_caps_by_name = {
        "max_online_filtered_reverse_code_sets": 6_600,
        "max_online_filtered_reverse_code_occurrences": 6_700,
        "max_online_inferred_taxonomy_candidates": 37_000,
        "max_online_candidate_pattern_projection_members": 131_072,
        "max_online_inferred_taxonomy_retained_memberships": 65_536,
        "max_online_inferred_taxonomy_graph_pages": 256,
        "max_online_inferred_taxonomy_graph_bytes": 4_194_304,
        "max_online_inferred_taxonomy_graph_batches": 32,
        "pattern_count": 0,
        "pattern_member_count": 0,
        "pattern_member_bytes": 0,
    }
    assert {
        name: publication.manifest[name] for name in expected_caps_by_name
    } == expected_caps_by_name
    assert {
        stored_projection["representation"] for stored_projection in session.stored_rows
    } == {"direct_v1"}


def _projection_row(
    rule: InferredProviderTaxonomyRule,
    npi_keys: tuple[int, ...] = (0, 2),
    npi_keys_by_pattern: dict[int, tuple[int, ...]] | None = None,
) -> dict[str, Any]:
    rule_digest = candidates.inferred_provider_taxonomy_rule_digest(rule)
    member_payload = candidates.pack_inferred_taxonomy_npi_keys(npi_keys)
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
        len(pattern_npi_keys) for pattern_npi_keys in pattern_members.values()
    )
    return {
        "rule_digest": rule_digest,
        "catalog_contract": (candidates.PTG2_V4_INFERRED_TAXONOMY_CATALOG_CONTRACT),
        "catalog_digest": b"c" * 32,
        "vector_format": candidates.PTG2_V4_INFERRED_TAXONOMY_VECTOR_FORMAT,
        "member_count": len(npi_keys),
        "member_digest": candidates.inferred_taxonomy_member_digest(
            rule_digest,
            member_count=len(npi_keys),
            payload=member_payload,
        ),
        "member_keys": member_payload,
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
                packed_pattern_payload=pattern_payload,
            )
        ),
        "pattern_member_payload": pattern_payload,
    }


def _observe_projection_row(
    rule: InferredProviderTaxonomyRule,
    npi_keys: tuple[int, ...],
) -> dict[str, Any]:
    rule_digest = candidates.inferred_provider_taxonomy_rule_digest(rule)
    member_payload = candidates.pack_inferred_taxonomy_npi_keys(npi_keys)
    representation = candidates.PTG2_V4_INFERRED_TAXONOMY_OBSERVE_REPRESENTATION
    return {
        "rule_digest": rule_digest,
        "catalog_contract": (candidates.PTG2_V4_INFERRED_TAXONOMY_CATALOG_CONTRACT),
        "catalog_digest": b"o" * 32,
        "vector_format": candidates.PTG2_V4_INFERRED_TAXONOMY_VECTOR_FORMAT,
        "member_count": len(npi_keys),
        "member_digest": candidates.inferred_taxonomy_member_digest(
            rule_digest,
            member_count=len(npi_keys),
            payload=member_payload,
        ),
        "member_keys": member_payload,
        "representation": representation,
        "observe_reason": (candidates.PTG2_V4_INFERRED_TAXONOMY_CANDIDATE_CAP_REASON),
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
                packed_pattern_payload=b"",
            )
        ),
        "pattern_member_payload": b"",
    }

def _reader_row(
    projection_row: dict[str, Any],
    *,
    member_keys: bytes | None = None,
    pattern_member_payload: bytes | None = None,
    root_pattern_count: int | None = None,
) -> dict[str, Any]:
    candidate_payload = (
        projection_row["member_keys"] if member_keys is None else member_keys
    )
    pattern_payload = (
        projection_row["pattern_member_payload"]
        if pattern_member_payload is None
        else pattern_member_payload
    )
    if root_pattern_count is None:
        root_pattern_count = (
            max(projection_row.get("pattern_count", 0), 10)
            if projection_row.get("representation") == "pattern_v1"
            else 0
        )
    return {
        "catalog_contract": projection_row["catalog_contract"],
        "catalog_digest": projection_row["catalog_digest"],
        "vector_format": projection_row["vector_format"],
        "member_count": projection_row["member_count"],
        "member_digest": projection_row["member_digest"],
        "member_keys": candidate_payload,
        "member_bytes": len(candidate_payload),
        "representation": projection_row["representation"],
        "pattern_count": projection_row["pattern_count"],
        "pattern_member_count": projection_row["pattern_member_count"],
        "pattern_member_bytes": projection_row["pattern_member_bytes"],
        "pattern_member_digest": projection_row["pattern_member_digest"],
        "pattern_member_payload": pattern_payload,
        "pattern_payload_bytes": len(pattern_payload),
        "root_state": "complete",
        "npi_count": 3,
        "root_pattern_count": root_pattern_count,
    }
