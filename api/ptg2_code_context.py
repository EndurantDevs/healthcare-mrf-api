# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Code crosswalk traversal for PTG2 serving searches."""

from __future__ import annotations

import os
from typing import Any

from sqlalchemy import text

from api.code_systems import canonical_catalog_code
from api.ptg2_code_filters import (
    INTERNAL_PROCEDURE_CODE_SYSTEM,
    PROCEDURE_CODE_SYSTEMS,
    PTG2_CODE_EXPANSION_HOPS,
    _is_signed_int_text,
    _normalize_code,
    _normalize_code_system,
    _ptg2_code_context,
    _ptg2_equivalent_external_pairs,
)
from api.ptg2_serving_utils import _row_mapping

PTG2_SCHEMA = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")


async def _query_ptg2_code_crosswalk_edges(session, pairs: set[tuple[str, str]]) -> list[dict[str, Any]]:
    if not pairs:
        return []
    clauses = []
    query_params_by_name: dict[str, Any] = {}
    for idx, (system, code) in enumerate(sorted(pairs)):
        query_params_by_name[f"system_{idx}"] = system
        query_params_by_name[f"code_{idx}"] = code
        clauses.append(
            f"""
            (
                UPPER(from_system) = :system_{idx}
            AND UPPER(from_code) = :code_{idx}
            )
            """
        )
        clauses.append(
            f"""
            (
                UPPER(to_system) = :system_{idx}
            AND UPPER(to_code) = :code_{idx}
            )
            """
        )
    try:
        query_result = await session.execute(
            text(
                f"""
                SELECT from_system, from_code, to_system, to_code, match_type, confidence, source
                  FROM {PTG2_SCHEMA}.code_crosswalk
                 WHERE {" OR ".join(clauses)}
                """
            ),
            query_params_by_name,
        )
    except Exception:
        return []
    return [_row_mapping(edge_row) for edge_row in query_result]


async def _resolve_ptg2_code_search_context(
    session,
    *,
    code: Any,
    code_system: Any,
) -> dict[str, Any] | None:
    """Resolve a requested procedure code through bounded crosswalk traversal."""

    requested_system = _normalize_code_system(code_system)
    requested_code = _normalize_code(code)
    if requested_system:
        requested_code = canonical_catalog_code(requested_system, requested_code)
    if not requested_code:
        return None
    if requested_system not in PROCEDURE_CODE_SYSTEMS:
        return None
    if requested_system == INTERNAL_PROCEDURE_CODE_SYSTEM and not _is_signed_int_text(requested_code):
        return _ptg2_code_context(
            input_system=requested_system,
            input_code=requested_code,
            resolved_pairs={(requested_system, requested_code)},
            internal_codes=set(),
        )

    resolved_pairs: set[tuple[str, str]] = {(requested_system, requested_code)}
    internal_codes: set[int] = set()
    matched_via_edges: list[dict[str, Any]] = []
    seen_edges: set[tuple[str, str, str, str]] = set()
    if requested_system == INTERNAL_PROCEDURE_CODE_SYSTEM:
        internal_codes.add(int(requested_code))
    else:
        resolved_pairs.update(_ptg2_equivalent_external_pairs(requested_system, requested_code))

    frontier_pairs = set(resolved_pairs)
    for _ in range(PTG2_CODE_EXPANSION_HOPS):
        edges = await _query_ptg2_code_crosswalk_edges(session, frontier_pairs)
        next_frontier_pairs: set[tuple[str, str]] = set()
        for edge in edges:
            from_pair = (
                _normalize_code_system(edge.get("from_system")) or "",
                _normalize_code(edge.get("from_code")),
            )
            to_pair = (
                _normalize_code_system(edge.get("to_system")) or "",
                _normalize_code(edge.get("to_code")),
            )
            if from_pair[0] not in PROCEDURE_CODE_SYSTEMS or to_pair[0] not in PROCEDURE_CODE_SYSTEMS:
                continue
            edge_key = (*from_pair, *to_pair)
            if edge_key not in seen_edges:
                seen_edges.add(edge_key)
                matched_via_edges.append(
                    {
                        "from_system": from_pair[0],
                        "from_code": from_pair[1],
                        "to_system": to_pair[0],
                        "to_code": to_pair[1],
                        "match_type": edge.get("match_type"),
                        "confidence": edge.get("confidence"),
                        "source": edge.get("source"),
                    }
                )
            for pair in (from_pair, to_pair):
                candidate_pairs = {pair}
                candidate_pairs.update(_ptg2_equivalent_external_pairs(pair[0], pair[1]))
                for candidate_pair in candidate_pairs:
                    if candidate_pair[0] == INTERNAL_PROCEDURE_CODE_SYSTEM and _is_signed_int_text(candidate_pair[1]):
                        internal_codes.add(int(candidate_pair[1]))
                    if candidate_pair not in resolved_pairs:
                        resolved_pairs.add(candidate_pair)
                        next_frontier_pairs.add(candidate_pair)
        if not next_frontier_pairs:
            break
        frontier_pairs = next_frontier_pairs

    return _ptg2_code_context(
        input_system=requested_system,
        input_code=requested_code,
        resolved_pairs=resolved_pairs,
        internal_codes=internal_codes,
        matched_via=matched_via_edges,
    )
