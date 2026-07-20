# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Preserve exact candidate coordinates before price hydration."""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence

from api.ptg2_candidate_audit_codes import CandidateCodeIndex
from api.ptg2_candidate_audit_integrity import PersistedAuditOccurrence
from api.ptg2_db_sidecars import lookup_shared_provider_code_keys_from_db
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchChallenge,
)
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError


OccurrenceKey = tuple[int, int, int]


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
    for challenge in challenges:
        provider_set_keys = provider_set_keys_by_npi.get(challenge.npi, ())
        for code_record in code_index.by_pair[(challenge.code_system, challenge.code)]:
            code_key = int(code_record["code_key"])
            npi_code = (challenge.npi, code_key)
            if npi_code in seen_npi_codes:
                continue
            seen_npi_codes.add(npi_code)
            provider_sets_by_npi_code.setdefault(npi_code, set()).update(
                provider_set_key
                for provider_set_key in provider_set_keys
                if code_key in provider_code_sets[provider_set_key]
            )
    for occurrence in persisted_audit_occurrences:
        if occurrence.code_key not in provider_code_sets[occurrence.provider_set_key]:
            raise PTG2ManifestArtifactError(
                "PTG2 candidate persisted audit provider-code membership is missing"
            )
        provider_sets_by_npi_code.setdefault(
            (occurrence.npi, occurrence.code_key), set()
        ).add(occurrence.provider_set_key)
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
    *,
    schema_name: str,
) -> dict[int, frozenset[int]]:
    """Load each requested provider's sealed code membership exactly once."""

    requested_keys = tuple(sorted(set(provider_set_keys)))
    if not requested_keys:
        return {}
    code_keys_by_provider_set = await lookup_shared_provider_code_keys_from_db(
        session,
        shared_snapshot_key,
        requested_keys,
        schema_name=schema_name,
    )
    if set(code_keys_by_provider_set) != set(requested_keys) or any(
        not code_keys for code_keys in code_keys_by_provider_set.values()
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 candidate provider-code artifact is missing a referenced provider set"
        )
    return {
        provider_set_key: frozenset(code_keys)
        for provider_set_key, code_keys in code_keys_by_provider_set.items()
    }


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

    challenge_npis = {challenge.npi for challenge in challenges}
    provider_code_keys = {
        provider_set_key
        for npi in challenge_npis
        for provider_set_key in provider_set_keys_by_npi.get(npi, ())
    } | {occurrence.provider_set_key for occurrence in persisted_audit_occurrences}
    provider_code_sets = await load_candidate_provider_code_sets(
        session,
        shared_snapshot_key,
        provider_code_keys,
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
