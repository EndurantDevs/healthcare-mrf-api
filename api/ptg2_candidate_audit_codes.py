# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resolve all requested audit codes through one exact candidate query."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Sequence

from sqlalchemy import text

from api.code_systems import (
    catalog_code_lookup_values,
    catalog_code_system_lookup_values,
)
from api.ptg2_candidate_audit import PTG2CandidateAuditAccess
from api.ptg2_candidate_audit_capacity import CandidateAuditDecodedRetentionBudget
from api.ptg2_serving import (
    _canonical_code_metadata_row,
    _required_shared_snapshot_key,
    _shared_v3_code_scope_sql,
    _shared_v3_code_table,
)
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts.ptg2_candidate_audit_batch_contract import AuditBatchChallenge
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError


_CODE_INDEX_BYTES = 512
_CODE_INDEX_RECORD_BYTES = 2048
_CODE_INDEX_PAIR_MEMBERSHIP_BYTES = 256


@dataclass(frozen=True)
class _RequestedCodeAlias:
    canonical_system: str
    canonical_code: str
    reported_system: str
    reported_code: str


@dataclass(frozen=True)
class CandidateCodeIndex:
    """Plan-scoped code metadata indexed for witnesses and sealed coordinates."""

    by_pair: dict[tuple[str, str], tuple[dict[str, Any], ...]]
    by_key: dict[int, dict[str, Any]]


def _requested_code_aliases(
    challenges: Sequence[AuditBatchChallenge],
) -> tuple[_RequestedCodeAlias, ...]:
    requested_pairs = sorted(
        {(challenge.code_system, challenge.code) for challenge in challenges}
    )
    return tuple(
        _RequestedCodeAlias(
            canonical_system=canonical_system,
            canonical_code=canonical_code,
            reported_system=reported_system,
            reported_code=reported_code,
        )
        for canonical_system, canonical_code in requested_pairs
        for reported_system in catalog_code_system_lookup_values(canonical_system)
        for reported_code in catalog_code_lookup_values(
            canonical_system,
            canonical_code,
        )
    )


def _code_query_sql(scope_join_sql: str, code_filters: Sequence[str]) -> str:
    return f"""
        WITH requested_code AS (
            SELECT *
              FROM unnest(
                  CAST(:canonical_systems AS text[]),
                  CAST(:canonical_codes AS text[]),
                  CAST(:reported_systems AS text[]),
                  CAST(:reported_codes AS text[])
              ) AS requested(
                  canonical_system, canonical_code,
                  reported_system, reported_code
              )
        )
        SELECT DISTINCT requested.canonical_system,
               requested.canonical_code, code_metadata.code_key,
               code_metadata.reported_code_system,
               code_metadata.reported_code,
               code_metadata.negotiation_arrangement,
               code_metadata.billing_code_type_version,
               code_metadata.source_name,
               code_metadata.source_description
          FROM {_shared_v3_code_table()} code_metadata
          LEFT JOIN requested_code requested
            ON code_metadata.reported_code_system = requested.reported_system
           AND code_metadata.reported_code = requested.reported_code
          {scope_join_sql}
         WHERE {" AND ".join(code_filters)}
           AND (
               requested.canonical_system IS NOT NULL
               OR code_metadata.code_key = ANY(
                   CAST(:persisted_code_keys AS integer[])
               )
           )
         ORDER BY requested.canonical_system, requested.canonical_code,
                  code_metadata.code_key
    """


def _index_candidate_code_record(
    raw_code_record: Any,
    records_by_pair_and_key: dict[
        tuple[str, str], dict[int, dict[str, Any]]
    ],
    records_by_key: dict[int, dict[str, Any]],
    retention_budget: CandidateAuditDecodedRetentionBudget | None = None,
) -> None:
    code_record = _canonical_code_metadata_row(raw_code_record)
    canonical_system = code_record.pop("canonical_system", None)
    canonical_code = code_record.pop("canonical_code", None)
    code_key = int(code_record.get("code_key"))
    existing_by_key = records_by_key.get(code_key)
    if existing_by_key is None:
        if retention_budget is not None:
            retention_budget.claim(
                _CODE_INDEX_RECORD_BYTES,
                category="a candidate code metadata record",
            )
        records_by_key[code_key] = code_record
        existing_by_key = code_record
    if existing_by_key != code_record:
        raise PTG2ManifestArtifactError(
            "PTG2 candidate code aliases resolve inconsistently"
        )
    if canonical_system is None or canonical_code is None:
        return
    code_pair = (str(canonical_system), str(canonical_code))
    pair_records_by_key = records_by_pair_and_key.get(code_pair)
    if pair_records_by_key is None or code_key not in pair_records_by_key:
        if retention_budget is not None:
            retention_budget.claim(
                _CODE_INDEX_PAIR_MEMBERSHIP_BYTES,
                category="a candidate code alias membership",
            )
        if pair_records_by_key is None:
            pair_records_by_key = {}
            records_by_pair_and_key[code_pair] = pair_records_by_key
        pair_records_by_key[code_key] = code_record


def _indexed_candidate_code_records(
    challenges: Sequence[AuditBatchChallenge],
    persisted_code_keys: Iterable[int],
    code_query: Iterable[Any],
    retention_budget: CandidateAuditDecodedRetentionBudget | None = None,
) -> CandidateCodeIndex:
    """Index candidate metadata only after each retained record is claimed."""

    records_by_pair_and_key: dict[
        tuple[str, str],
        dict[int, dict[str, Any]],
    ] = {}
    records_by_key: dict[int, dict[str, Any]] = {}
    if retention_budget is not None:
        retention_budget.claim(
            _CODE_INDEX_BYTES,
            category="the candidate code index",
        )
    for raw_code_record in code_query:
        _index_candidate_code_record(
            raw_code_record,
            records_by_pair_and_key,
            records_by_key,
            retention_budget,
        )
    requested_pairs = {
        (challenge.code_system, challenge.code) for challenge in challenges
    }
    if set(records_by_pair_and_key) != requested_pairs:
        raise PTG2ManifestArtifactError(
            "PTG2 candidate audit code is missing from the sealed layout"
        )
    required_code_keys = {int(code_key) for code_key in persisted_code_keys}
    if not required_code_keys.issubset(records_by_key):
        raise PTG2ManifestArtifactError(
            "PTG2 candidate persisted audit code is missing from the sealed layout"
        )
    return CandidateCodeIndex(
        by_pair={
            code_pair: tuple(
                pair_records_by_key[code_key]
                for code_key in sorted(pair_records_by_key)
            )
            for code_pair, pair_records_by_key in records_by_pair_and_key.items()
        },
        by_key=records_by_key,
    )


async def candidate_code_records_by_pair(
    session: Any,
    serving_tables: PTG2ServingTables,
    access: PTG2CandidateAuditAccess,
    challenges: Sequence[AuditBatchChallenge],
    *,
    persisted_code_keys: Iterable[int] = (),
    retention_budget: CandidateAuditDecodedRetentionBudget | None = None,
) -> CandidateCodeIndex:
    """Return every exact plan-scoped code record in one database query."""

    aliases = _requested_code_aliases(challenges)
    normalized_persisted_code_keys = tuple(
        sorted({int(code_key) for code_key in persisted_code_keys})
    )
    scope_join_sql, code_filters, params_by_name, _order_sql = (
        _shared_v3_code_scope_sql(
            serving_tables,
            requested_plan=access.plan_id,
            plan_market_type=access.plan_market_type,
        )
    )
    code_filters.append("code_metadata.snapshot_key = :shared_snapshot_key")
    params_by_name.update(
        {
            "shared_snapshot_key": _required_shared_snapshot_key(serving_tables),
            "canonical_systems": [alias.canonical_system for alias in aliases],
            "canonical_codes": [alias.canonical_code for alias in aliases],
            "reported_systems": [alias.reported_system for alias in aliases],
            "reported_codes": [alias.reported_code for alias in aliases],
            "persisted_code_keys": normalized_persisted_code_keys,
        }
    )
    code_query = await session.execute(
        text(_code_query_sql(scope_join_sql, code_filters)),
        params_by_name,
    )
    return _indexed_candidate_code_records(
        challenges,
        normalized_persisted_code_keys,
        code_query,
        retention_budget,
    )


__all__ = ["CandidateCodeIndex", "candidate_code_records_by_pair"]
