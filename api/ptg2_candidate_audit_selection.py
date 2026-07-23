# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Preserve exact candidate coordinates before price hydration."""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence

from api.ptg2_candidate_audit_capacity import (
    CandidateAuditDecodedRetentionBudget,
)
from api.ptg2_candidate_audit_codes import CandidateCodeIndex
from api.ptg2_candidate_audit_dimensions import (
    candidate_provider_set_keys as _candidate_provider_set_keys,
    candidate_requested_code_keys as _candidate_requested_code_keys,
    provider_code_request_upper_bound as _provider_code_request_upper_bound,
)
from api.ptg2_candidate_audit_integrity import PersistedAuditOccurrence
from api.ptg2_candidate_audit_maps import (
    INTEGER_MAP_BUCKET_BYTES as _INTEGER_MAP_BUCKET_BYTES,
    INTEGER_MAP_BYTES as _INTEGER_MAP_BYTES,
    freeze_integer_set_map as _freeze_integer_set_map,
)
from api.ptg2_candidate_audit_occurrences import (
    OccurrenceKey,
    required_candidate_occurrence_keys,
)
from api.ptg2_candidate_audit_provider_requests import (
    requested_code_keys_by_provider_set as _bounded_provider_code_requests,
)
from api.ptg2_candidate_audit_provider_code_sets import (
    ProviderCodeBounds as _ProviderCodeBounds,
    ensure_complete_provider_code_map as _ensure_complete_provider_code_map,
    freeze_nonempty_provider_code_sets as _freeze_nonempty_provider_code_sets,
    load_bounded_provider_code_sets as _load_bounded_provider_code_sets,
)
from api.ptg2_db_sidecars import (
    _ValidatedProviderCodeRequests,
    _freeze_provider_code_requests,
    _lookup_prepared_code_map_from_db,
    lookup_shared_provider_code_intersections_from_db,
)
from process.ptg_parts.ptg2_candidate_audit_batch_contract import AuditBatchChallenge
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError


PTG2_AUDIT_BATCH_MAX_PROVIDER_CODE_SCOPE = 1_000_000
PTG2_AUDIT_BATCH_MAX_REQUESTED_CODE_KEYS = 100_000
_INTEGER_SET_BYTES = 256
_INTEGER_SET_MEMBERSHIP_BYTES = 112
_COMPOSITE_SET_MEMBERSHIP_BYTES = 160


def _add_provider_match(
    provider_sets_by_npi_code: dict[tuple[int, int], set[int]],
    npi_code: tuple[int, int],
    provider_set_key: int,
    retained_membership_count: int,
    retention_budget: CandidateAuditDecodedRetentionBudget | None = None,
) -> int:
    """Add one unique exact membership without exceeding the audit budget."""

    provider_set_keys = provider_sets_by_npi_code.get(npi_code)
    if provider_set_keys is None:
        if retention_budget is not None:
            retention_budget.claim(
                _INTEGER_MAP_BUCKET_BYTES,
                category="a candidate NPI/code provider bucket",
            )
        provider_set_keys = set()
        provider_sets_by_npi_code[npi_code] = provider_set_keys
    if provider_set_key in provider_set_keys:
        return retained_membership_count
    if retained_membership_count >= PTG2_AUDIT_BATCH_MAX_PROVIDER_CODE_SCOPE:
        raise PTG2ManifestArtifactError(
            "PTG2 candidate exact provider-code matches exceed their bounded limit"
        )
    if retention_budget is not None:
        retention_budget.claim(
            _INTEGER_SET_MEMBERSHIP_BYTES,
            category="a candidate NPI/code provider membership",
        )
    provider_set_keys.add(provider_set_key)
    return retained_membership_count + 1


def _add_challenge_provider_matches(
    challenges: Sequence[AuditBatchChallenge],
    code_index: CandidateCodeIndex,
    provider_set_keys_by_npi: Mapping[int, tuple[int, ...]],
    provider_code_sets: Mapping[int, frozenset[int]],
    provider_sets_by_npi_code: dict[tuple[int, int], set[int]],
    seen_npi_codes: set[tuple[int, int]],
    retained_membership_count: int,
    retention_budget: CandidateAuditDecodedRetentionBudget | None = None,
) -> int:
    for challenge in challenges:
        provider_set_keys = provider_set_keys_by_npi.get(challenge.npi, ())
        for code_record in code_index.by_pair[(challenge.code_system, challenge.code)]:
            code_key = int(code_record["code_key"])
            npi_code = (challenge.npi, code_key)
            if npi_code in seen_npi_codes:
                continue
            if retention_budget is not None:
                retention_budget.claim(
                    _COMPOSITE_SET_MEMBERSHIP_BYTES,
                    category="a seen candidate NPI/code key",
                )
            seen_npi_codes.add(npi_code)
            for provider_set_key in provider_set_keys:
                if code_key not in provider_code_sets.get(
                    provider_set_key,
                    (),
                ):
                    continue
                retained_membership_count = _add_provider_match(
                    provider_sets_by_npi_code,
                    npi_code,
                    provider_set_key,
                    retained_membership_count,
                    retention_budget,
                )
    return retained_membership_count


def candidate_provider_scope_by_npi_code(
    challenges: Sequence[AuditBatchChallenge],
    code_index: CandidateCodeIndex,
    provider_set_keys_by_npi: Mapping[int, tuple[int, ...]],
    provider_code_sets: Mapping[int, frozenset[int]],
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence] = (),
    *,
    retention_budget: CandidateAuditDecodedRetentionBudget | None = None,
) -> dict[tuple[int, int], tuple[int, ...]]:
    """Intersect graph providers with codes into one reusable exact scope."""

    provider_sets_by_npi_code: dict[tuple[int, int], set[int]] = {}
    seen_npi_codes: set[tuple[int, int]] = set()
    if retention_budget is not None:
        retention_budget.claim(
            _INTEGER_MAP_BYTES + _INTEGER_SET_BYTES,
            category="the candidate NPI/code provider indexes",
        )
    retained_membership_count = _add_challenge_provider_matches(
        challenges,
        code_index,
        provider_set_keys_by_npi,
        provider_code_sets,
        provider_sets_by_npi_code,
        seen_npi_codes,
        0,
        retention_budget,
    )
    for occurrence in persisted_audit_occurrences:
        if occurrence.code_key not in provider_code_sets.get(
            occurrence.provider_set_key,
            (),
        ):
            raise PTG2ManifestArtifactError(
                "PTG2 candidate persisted audit provider-code membership is missing"
            )
        retained_membership_count = _add_provider_match(
            provider_sets_by_npi_code,
            (occurrence.npi, occurrence.code_key),
            occurrence.provider_set_key,
            retained_membership_count,
            retention_budget,
        )
    frozen_scope = _freeze_integer_set_map(
        provider_sets_by_npi_code,
        retention_budget,
        category="candidate NPI/code provider",
    )
    if retention_budget is not None:
        retention_budget.release(
            _INTEGER_SET_BYTES
            + len(seen_npi_codes) * _COMPOSITE_SET_MEMBERSHIP_BYTES
        )
    return frozen_scope


def provider_filters_by_code_key(
    provider_sets_by_npi_code: Mapping[tuple[int, int], tuple[int, ...]],
    retention_budget: CandidateAuditDecodedRetentionBudget | None = None,
) -> dict[int, tuple[int, ...]]:
    """Union the exact provider scope for one filtered forward read."""

    provider_sets_by_code_key: dict[int, set[int]] = {}
    if retention_budget is not None:
        retention_budget.claim(
            _INTEGER_MAP_BYTES,
            category="the candidate forward provider filter map",
        )
    for (_npi, code_key), provider_set_keys in provider_sets_by_npi_code.items():
        code_provider_keys = provider_sets_by_code_key.get(code_key)
        if code_provider_keys is None:
            if retention_budget is not None:
                retention_budget.claim(
                    _INTEGER_MAP_BUCKET_BYTES,
                    category="a candidate forward provider filter bucket",
                )
            code_provider_keys = set()
            provider_sets_by_code_key[code_key] = code_provider_keys
        for provider_set_key in provider_set_keys:
            if provider_set_key in code_provider_keys:
                continue
            if retention_budget is not None:
                retention_budget.claim(
                    _INTEGER_SET_MEMBERSHIP_BYTES,
                    category="a candidate forward provider filter",
                )
            code_provider_keys.add(provider_set_key)
    return _freeze_integer_set_map(
        provider_sets_by_code_key,
        retention_budget,
        category="candidate forward provider filter",
    )


async def load_candidate_provider_code_sets(
    session: Any,
    shared_snapshot_key: int,
    provider_set_keys: Iterable[int],
    requested_code_keys: Iterable[int],
    *,
    schema_name: str,
    retention_budget: CandidateAuditDecodedRetentionBudget | None = None,
) -> dict[int, frozenset[int]]:
    """Retain only requested memberships from each sealed provider code set."""

    return await _load_bounded_provider_code_sets(
        lookup_shared_provider_code_intersections_from_db,
        session,
        shared_snapshot_key,
        provider_set_keys,
        requested_code_keys,
        bounds=_ProviderCodeBounds(
            maximum_provider_keys=PTG2_AUDIT_BATCH_MAX_PROVIDER_CODE_SCOPE,
            maximum_code_keys=PTG2_AUDIT_BATCH_MAX_REQUESTED_CODE_KEYS,
            schema_name=schema_name,
        ),
        retention_budget=retention_budget,
    )


async def _load_provider_codes_by_dimension(
    session: Any,
    shared_snapshot_key: int,
    challenges: Sequence[AuditBatchChallenge],
    code_index: CandidateCodeIndex,
    provider_set_keys_by_npi: Mapping[int, tuple[int, ...]],
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence],
    *,
    schema_name: str,
    retention_budget: CandidateAuditDecodedRetentionBudget | None,
) -> dict[int, frozenset[int]]:
    """Intersect dense scopes by bounded dimensions instead of pair products."""

    return await load_candidate_provider_code_sets(
        session,
        shared_snapshot_key,
        _candidate_provider_set_keys(
            provider_set_keys_by_npi,
            persisted_audit_occurrences,
        ),
        _candidate_requested_code_keys(
            challenges,
            code_index,
            persisted_audit_occurrences,
        ),
        schema_name=schema_name,
        retention_budget=retention_budget,
    )


async def _load_provider_codes_for_scope(
    session: Any,
    shared_snapshot_key: int,
    challenges: Sequence[AuditBatchChallenge],
    code_index: CandidateCodeIndex,
    provider_set_keys_by_npi: Mapping[int, tuple[int, ...]],
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence],
    *,
    schema_name: str,
    retention_budget: CandidateAuditDecodedRetentionBudget | None,
) -> dict[int, frozenset[int]]:
    """Choose exact pairs or a dimension-first intersection before retention."""

    pair_upper_bound = _provider_code_request_upper_bound(
        challenges,
        code_index,
        provider_set_keys_by_npi,
        persisted_audit_occurrences,
        maximum_memberships=PTG2_AUDIT_BATCH_MAX_PROVIDER_CODE_SCOPE,
    )
    if pair_upper_bound > PTG2_AUDIT_BATCH_MAX_PROVIDER_CODE_SCOPE:
        return await _load_provider_codes_by_dimension(
            session,
            shared_snapshot_key,
            challenges,
            code_index,
            provider_set_keys_by_npi,
            persisted_audit_occurrences,
            schema_name=schema_name,
            retention_budget=retention_budget,
        )
    provider_code_requests = _requested_code_keys_by_provider_set(
        challenges,
        code_index,
        provider_set_keys_by_npi,
        persisted_audit_occurrences,
        retention_budget,
    )
    return await _load_candidate_provider_code_sets_prepared(
        session,
        shared_snapshot_key,
        provider_code_requests,
        schema_name=schema_name,
        retention_budget=retention_budget,
    )


async def _load_candidate_provider_code_sets_prepared(
    session: Any,
    shared_snapshot_key: int,
    requests: _ValidatedProviderCodeRequests,
    *,
    schema_name: str,
    retention_budget: CandidateAuditDecodedRetentionBudget | None = None,
) -> dict[int, frozenset[int]]:
    """Retain exact memberships without rebuilding the validated request map."""

    if not requests.provider_set_keys:
        return {}
    if not requests.membership_count:
        raise PTG2ManifestArtifactError(
            "PTG2 candidate provider-code scope has no requested codes"
        )
    code_keys_by_provider_set = (
        await _lookup_prepared_code_map_from_db(
            session,
            shared_snapshot_key,
            requests,
            max_retained_memberships=(
                PTG2_AUDIT_BATCH_MAX_PROVIDER_CODE_SCOPE
            ),
            schema_name=schema_name,
            decoded_retention_budget=retention_budget,
        )
    )
    _ensure_complete_provider_code_map(
        code_keys_by_provider_set,
        requests.provider_set_keys,
        retention_budget,
    )
    return _freeze_nonempty_provider_code_sets(
        code_keys_by_provider_set,
        retention_budget,
    )


def _requested_code_keys_by_provider_set(
    challenges: Sequence[AuditBatchChallenge],
    code_index: CandidateCodeIndex,
    provider_set_keys_by_npi: Mapping[int, tuple[int, ...]],
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence],
    retention_budget: CandidateAuditDecodedRetentionBudget | None = None,
) -> _ValidatedProviderCodeRequests:
    """Bind requested codes only to providers reachable from their exact NPI."""

    return _bounded_provider_code_requests(
        challenges,
        code_index,
        provider_set_keys_by_npi,
        persisted_audit_occurrences,
        retention_budget,
        maximum_memberships=PTG2_AUDIT_BATCH_MAX_PROVIDER_CODE_SCOPE,
        maximum_code_keys=PTG2_AUDIT_BATCH_MAX_REQUESTED_CODE_KEYS,
    )


async def load_candidate_provider_indexes(
    session: Any,
    shared_snapshot_key: int,
    challenges: Sequence[AuditBatchChallenge],
    code_index: CandidateCodeIndex,
    provider_set_keys_by_npi: Mapping[int, tuple[int, ...]],
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence],
    *,
    schema_name: str,
    retention_budget: CandidateAuditDecodedRetentionBudget | None = None,
) -> tuple[
    dict[tuple[int, int], tuple[int, ...]],
    dict[int, tuple[int, ...]],
]:
    """Load and build the exact provider scope once for downstream readers."""

    provider_code_sets = await _load_provider_codes_for_scope(
        session,
        shared_snapshot_key,
        challenges,
        code_index,
        provider_set_keys_by_npi,
        persisted_audit_occurrences,
        schema_name=schema_name,
        retention_budget=retention_budget,
    )
    provider_sets_by_npi_code = candidate_provider_scope_by_npi_code(
        challenges,
        code_index,
        provider_set_keys_by_npi,
        provider_code_sets,
        persisted_audit_occurrences,
        retention_budget=retention_budget,
    )
    return (
        provider_sets_by_npi_code,
        provider_filters_by_code_key(
            provider_sets_by_npi_code,
            retention_budget,
        ),
    )


__all__ = [
    "OccurrenceKey",
    "candidate_provider_scope_by_npi_code",
    "load_candidate_provider_code_sets",
    "load_candidate_provider_indexes",
    "provider_filters_by_code_key",
    "required_candidate_occurrence_keys",
]
