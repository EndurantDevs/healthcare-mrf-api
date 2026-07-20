# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Preserve exact candidate coordinates before price hydration."""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence

from api.ptg2_candidate_audit_codes import CandidateCodeIndex
from api.ptg2_candidate_audit_integrity import PersistedAuditOccurrence
from api.ptg2_db_sidecars import (
    _ValidatedProviderCodeRequests,
    _freeze_provider_code_requests,
    _lookup_prepared_code_map_from_db,
    lookup_shared_provider_code_intersections_from_db,
)
from process.ptg_parts.ptg2_candidate_audit_batch_contract import AuditBatchChallenge
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError


OccurrenceKey = tuple[int, int, int]
PTG2_AUDIT_BATCH_MAX_PROVIDER_CODE_SCOPE = 1_000_000
PTG2_AUDIT_BATCH_MAX_REQUESTED_CODE_KEYS = 100_000


def _bounded_integer_keys(
    source_keys: Iterable[int],
    maximum_count: int,
    error_message: str,
) -> tuple[int, ...]:
    """Normalize unique integer keys while stopping at the first excess key."""

    normalized_keys: set[int] = set()
    for source_key in source_keys:
        normalized_keys.add(int(source_key))
        if len(normalized_keys) > maximum_count:
            raise PTG2ManifestArtifactError(error_message)
    return tuple(sorted(normalized_keys))


def _add_provider_match(
    provider_sets_by_npi_code: dict[tuple[int, int], set[int]],
    npi_code: tuple[int, int],
    provider_set_key: int,
    retained_membership_count: int,
) -> int:
    """Add one unique exact membership without exceeding the audit budget."""

    provider_set_keys = provider_sets_by_npi_code.setdefault(npi_code, set())
    if provider_set_key in provider_set_keys:
        return retained_membership_count
    if retained_membership_count >= PTG2_AUDIT_BATCH_MAX_PROVIDER_CODE_SCOPE:
        raise PTG2ManifestArtifactError(
            "PTG2 candidate exact provider-code matches exceed their bounded limit"
        )
    provider_set_keys.add(provider_set_key)
    return retained_membership_count + 1


def _add_provider_code_request(
    code_keys_by_provider_set: dict[int, set[int]],
    requested_code_keys: set[int],
    provider_set_key: int,
    code_key: int,
    requested_membership_count: int,
) -> int:
    """Add one exact request pair while preserving both hard bounds."""

    provider_code_keys = code_keys_by_provider_set.setdefault(
        int(provider_set_key), set()
    )
    normalized_code_key = int(code_key)
    if normalized_code_key in provider_code_keys:
        return requested_membership_count
    if (
        normalized_code_key not in requested_code_keys
        and len(requested_code_keys)
        >= PTG2_AUDIT_BATCH_MAX_REQUESTED_CODE_KEYS
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 candidate requested code scope exceeds its bounded limit"
        )
    if (
        requested_membership_count
        >= PTG2_AUDIT_BATCH_MAX_PROVIDER_CODE_SCOPE
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 candidate provider-code scope exceeds its bounded limit"
        )
    provider_code_keys.add(normalized_code_key)
    requested_code_keys.add(normalized_code_key)
    return requested_membership_count + 1


def candidate_provider_scope_by_npi_code(
    challenges: Sequence[AuditBatchChallenge],
    code_index: CandidateCodeIndex,
    provider_set_keys_by_npi: Mapping[int, tuple[int, ...]],
    provider_code_sets: Mapping[int, frozenset[int]],
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence] = (),
) -> dict[tuple[int, int], tuple[int, ...]]:
    """Intersect graph providers with codes into one reusable exact scope."""

    provider_sets_by_npi_code: dict[tuple[int, int], set[int]] = {}
    seen_npi_codes: set[tuple[int, int]] = set()
    retained_membership_count = 0
    for challenge in challenges:
        provider_set_keys = provider_set_keys_by_npi.get(challenge.npi, ())
        for code_record in code_index.by_pair[(challenge.code_system, challenge.code)]:
            code_key = int(code_record["code_key"])
            npi_code = (challenge.npi, code_key)
            if npi_code in seen_npi_codes:
                continue
            seen_npi_codes.add(npi_code)
            for provider_set_key in provider_set_keys:
                if code_key not in provider_code_sets[provider_set_key]:
                    continue
                retained_membership_count = _add_provider_match(
                    provider_sets_by_npi_code,
                    npi_code,
                    provider_set_key,
                    retained_membership_count,
                )
    for occurrence in persisted_audit_occurrences:
        if occurrence.code_key not in provider_code_sets[occurrence.provider_set_key]:
            raise PTG2ManifestArtifactError(
                "PTG2 candidate persisted audit provider-code membership is missing"
            )
        retained_membership_count = _add_provider_match(
            provider_sets_by_npi_code,
            (occurrence.npi, occurrence.code_key),
            occurrence.provider_set_key,
            retained_membership_count,
        )
    return {
        npi_code: tuple(sorted(provider_set_keys))
        for npi_code, provider_set_keys in provider_sets_by_npi_code.items()
        if provider_set_keys
    }


def provider_filters_by_code_key(
    provider_sets_by_npi_code: Mapping[tuple[int, int], tuple[int, ...]],
) -> dict[int, tuple[int, ...]]:
    """Union the exact provider scope for one filtered forward read."""

    provider_sets_by_code_key: dict[int, set[int]] = {}
    for (_npi, code_key), provider_set_keys in provider_sets_by_npi_code.items():
        provider_sets_by_code_key.setdefault(code_key, set()).update(
            provider_set_keys
        )
    return {
        code_key: tuple(sorted(provider_set_keys))
        for code_key, provider_set_keys in provider_sets_by_code_key.items()
    }


async def load_candidate_provider_code_sets(
    session: Any,
    shared_snapshot_key: int,
    provider_set_keys: Iterable[int],
    requested_code_keys: Iterable[int],
    *,
    schema_name: str,
) -> dict[int, frozenset[int]]:
    """Retain only requested memberships from each sealed provider code set."""

    requested_keys = _bounded_integer_keys(
        provider_set_keys,
        PTG2_AUDIT_BATCH_MAX_PROVIDER_CODE_SCOPE,
        "PTG2 candidate provider-code scope exceeds its bounded limit",
    )
    if not requested_keys:
        return {}
    normalized_code_keys = _bounded_integer_keys(
        requested_code_keys,
        PTG2_AUDIT_BATCH_MAX_REQUESTED_CODE_KEYS,
        "PTG2 candidate requested code scope exceeds its bounded limit",
    )
    if not normalized_code_keys:
        raise PTG2ManifestArtifactError(
            "PTG2 candidate provider-code scope has no requested codes"
        )
    code_keys_by_provider_set = (
        await lookup_shared_provider_code_intersections_from_db(
            session,
            shared_snapshot_key,
            requested_keys,
            normalized_code_keys,
            max_retained_memberships=(
                PTG2_AUDIT_BATCH_MAX_PROVIDER_CODE_SCOPE
            ),
            schema_name=schema_name,
        )
    )
    if set(code_keys_by_provider_set) != set(requested_keys):
        raise PTG2ManifestArtifactError(
            "PTG2 candidate provider-code artifact is missing a referenced provider set"
        )
    return {
        provider_set_key: frozenset(code_keys)
        for provider_set_key, code_keys in code_keys_by_provider_set.items()
    }


async def _load_candidate_provider_code_sets_prepared(
    session: Any,
    shared_snapshot_key: int,
    requests: _ValidatedProviderCodeRequests,
    *,
    schema_name: str,
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
            max_retained_memberships=PTG2_AUDIT_BATCH_MAX_PROVIDER_CODE_SCOPE,
            schema_name=schema_name,
        )
    )
    if set(code_keys_by_provider_set) != set(requests.provider_set_keys):
        raise PTG2ManifestArtifactError(
            "PTG2 candidate provider-code artifact is missing a referenced provider set"
        )
    return {
        provider_set_key: frozenset(code_keys)
        for provider_set_key, code_keys in code_keys_by_provider_set.items()
    }


def _requested_code_keys_by_provider_set(
    challenges: Sequence[AuditBatchChallenge],
    code_index: CandidateCodeIndex,
    provider_set_keys_by_npi: Mapping[int, tuple[int, ...]],
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence],
) -> _ValidatedProviderCodeRequests:
    """Bind requested codes only to providers reachable from their exact NPI."""

    code_keys_by_provider_set: dict[int, set[int]] = {}
    requested_code_keys: set[int] = set()
    seen_npi_code_keys: set[tuple[int, int]] = set()
    requested_membership_count = 0

    for challenge in challenges:
        provider_set_keys = provider_set_keys_by_npi.get(challenge.npi, ())
        for code_record in code_index.by_pair[
            (challenge.code_system, challenge.code)
        ]:
            code_key = int(code_record["code_key"])
            npi_code_key = (challenge.npi, code_key)
            if npi_code_key in seen_npi_code_keys:
                continue
            seen_npi_code_keys.add(npi_code_key)
            for provider_set_key in provider_set_keys:
                requested_membership_count = _add_provider_code_request(
                    code_keys_by_provider_set,
                    requested_code_keys,
                    provider_set_key,
                    code_key,
                    requested_membership_count,
                )
    for occurrence in persisted_audit_occurrences:
        requested_membership_count = _add_provider_code_request(
            code_keys_by_provider_set,
            requested_code_keys,
            occurrence.provider_set_key,
            occurrence.code_key,
            requested_membership_count,
        )
    return _freeze_provider_code_requests(
        code_keys_by_provider_set,
        membership_count=requested_membership_count,
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
) -> tuple[
    dict[tuple[int, int], tuple[int, ...]],
    dict[int, tuple[int, ...]],
]:
    """Load and build the exact provider scope once for downstream readers."""

    provider_code_requests = _requested_code_keys_by_provider_set(
        challenges,
        code_index,
        provider_set_keys_by_npi,
        persisted_audit_occurrences,
    )
    provider_code_sets = await _load_candidate_provider_code_sets_prepared(
        session,
        shared_snapshot_key,
        provider_code_requests,
        schema_name=schema_name,
    )
    provider_sets_by_npi_code = candidate_provider_scope_by_npi_code(
        challenges,
        code_index,
        provider_set_keys_by_npi,
        provider_code_sets,
        persisted_audit_occurrences,
    )
    return (
        provider_sets_by_npi_code,
        provider_filters_by_code_key(provider_sets_by_npi_code),
    )


def required_candidate_occurrence_keys(
    challenges: Sequence[AuditBatchChallenge],
    code_records_by_pair: Mapping[
        tuple[str, str], Sequence[Mapping[str, Any]]
    ],
    provider_sets_by_npi_code: Mapping[tuple[int, int], tuple[int, ...]],
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence] = (),
) -> frozenset[OccurrenceKey]:
    """Return exact code/provider/source triples without a Cartesian product."""

    requested_coordinates = {
        (
            challenge.code_system,
            challenge.code,
            challenge.npi,
            challenge.source_artifact_key,
        )
        for challenge in challenges
    }
    required_keys = {
        (
            int(code_record["code_key"]),
            int(provider_set_key),
            int(source_artifact_key),
        )
        for code_system, code, npi, source_artifact_key in requested_coordinates
        for code_record in code_records_by_pair[(code_system, code)]
        for provider_set_key in provider_sets_by_npi_code.get(
            (npi, int(code_record["code_key"])),
            (),
        )
    }
    required_keys.update(
        (
            occurrence.code_key,
            occurrence.provider_set_key,
            occurrence.source_artifact_key,
        )
        for occurrence in persisted_audit_occurrences
    )
    return frozenset(required_keys)


__all__ = [
    "OccurrenceKey",
    "candidate_provider_scope_by_npi_code",
    "load_candidate_provider_code_sets",
    "load_candidate_provider_indexes",
    "provider_filters_by_code_key",
    "required_candidate_occurrence_keys",
]
