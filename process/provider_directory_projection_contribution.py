# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Non-authoritative candidate reducers, not byte attestations or publish gates."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
import hashlib
from typing import Any

from process.provider_directory_projection_inline_profile import (
    INLINE_PROFILE_EVIDENCE_CONTRACT_ID,
)
from process.provider_directory_projection_semantic_evidence import (
    PROFILE_CONTRIBUTION_ARRAY_FIELDS,
    NPI_ACCUMULATOR_MODULUS,
    SEMANTIC_CONTRIBUTION_COLUMNS,
    SEMANTIC_CONTRIBUTION_CONTRACT_ID,
    SemanticStableListDigest,
    canonical_semantic_resource_row,
    normalized_semantic_pair_stream,
    normalized_profile_contribution,
    normalized_semantic_contribution,
    normalized_semantic_winner,
    npi_occurrence_factor,
    semantic_identity,
    streamed_npi_proof,
    validate_semantic_pair,
)
from process.provider_directory_projection_types import (
    ProjectionSemanticOutcomeProof,
    ProviderDirectoryProjectionError,
    SEMANTIC_OUTCOME_FIELDS,
    required_hash,
    required_text,
    stable_hash,
)


SEMANTIC_OUTCOME_PROOF_CONTRACT_ID = "healthporta.provider-directory.semantic-outcome-proof.v1"
SEMANTIC_STREAMING_REDUCER_CONTRACT_ID = "healthporta.provider-directory.semantic-streaming-reducer.v1"
MAX_REFERENCE_REDUCER_ROWS = 10_000
_MAX_INT64 = 2**63 - 1

def _aggregate_count(raw_count: Any, field_name: str) -> int:
    if type(raw_count) is not int or raw_count < 0 or raw_count > _MAX_INT64:
        error_code = f"provider_directory_projection_{field_name}_invalid"
        raise ProviderDirectoryProjectionError(error_code)
    return raw_count


def _checked_add(current: int, increment: int, field_name: str) -> int:
    aggregate = current + increment
    if aggregate > _MAX_INT64:
        error_code = f"provider_directory_projection_{field_name}_invalid"
        raise ProviderDirectoryProjectionError(error_code)
    return aggregate


def _semantic_outcome_proof_payload(
    *,
    canonical_row_sha256: str,
    profile_contribution_sha256: str,
    distinct_npi_sha256: str,
    resource_count: int,
    resource_count_by_type: Mapping[str, int],
    outcome_count_by_name: Mapping[str, int],
) -> dict[str, Any]:
    return {
        "contract_id": SEMANTIC_OUTCOME_PROOF_CONTRACT_ID,
        "streaming_reducer_contract_id": SEMANTIC_STREAMING_REDUCER_CONTRACT_ID,
        "semantic_contribution_contract_id": SEMANTIC_CONTRIBUTION_CONTRACT_ID,
        "inline_profile_evidence_contract_id": INLINE_PROFILE_EVIDENCE_CONTRACT_ID,
        "canonical_row_sha256": canonical_row_sha256,
        "profile_contribution_sha256": profile_contribution_sha256,
        "distinct_npi_sha256": distinct_npi_sha256,
        "resource_count": resource_count,
        "resource_counts": dict(sorted(resource_count_by_type.items())),
        "outcome_counts": {
            field_name: outcome_count_by_name[field_name]
            for field_name in SEMANTIC_OUTCOME_FIELDS
        },
        "complete": True,
    }


def _validate_outcome_consistency(
    resource_count_by_type: Mapping[str, int],
    outcome_count_by_name: Mapping[str, int],
) -> None:
    if (
        outcome_count_by_name["individual_practitioners"]
        != resource_count_by_type.get("Practitioner", 0)
        or outcome_count_by_name["organization_resources"]
        != resource_count_by_type.get("Organization", 0)
        or outcome_count_by_name["practitioner_role_resources"]
        != resource_count_by_type.get("PractitionerRole", 0)
        or outcome_count_by_name["addressed_locations"]
        > resource_count_by_type.get("Location", 0)
        or outcome_count_by_name["geocoded_locations"]
        > outcome_count_by_name["addressed_locations"]
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_semantic_outcomes_inconsistent"
        )


def _normalized_index(
    candidates: Iterable[Mapping[str, Any]],
    normalize_candidate: Callable[[Mapping[str, Any]], dict[str, Any]],
    duplicate_error: str,
) -> dict[tuple[str, str], dict[str, Any]]:
    normalized_by_identity: dict[tuple[str, str], dict[str, Any]] = {}
    for candidate_index, raw_candidate in enumerate(candidates):
        if candidate_index >= MAX_REFERENCE_REDUCER_ROWS:
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_reference_reducer_row_limit_exceeded"
            )
        normalized_candidate = normalize_candidate(raw_candidate)
        candidate_identity = semantic_identity(normalized_candidate)
        if candidate_identity in normalized_by_identity:
            raise ProviderDirectoryProjectionError(duplicate_error)
        normalized_by_identity[candidate_identity] = normalized_candidate
    return normalized_by_identity


def _ordered_semantic_pairs(
    resources: Iterable[Mapping[str, Any]],
    profile_contributions: Iterable[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    resource_by_identity = _normalized_index(
        resources,
        normalized_semantic_winner,
        "provider_directory_projection_semantic_winner_duplicate",
    )
    contribution_by_identity = _normalized_index(
        profile_contributions,
        normalized_profile_contribution,
        "provider_directory_projection_profile_contribution_duplicate",
    )
    if not resource_by_identity or set(resource_by_identity) != set(
        contribution_by_identity
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_semantic_pair_set_mismatch"
        )
    ordered_identities = sorted(resource_by_identity)
    ordered_resources = [
        resource_by_identity[identity] for identity in ordered_identities
    ]
    ordered_contributions = [
        contribution_by_identity[identity] for identity in ordered_identities
    ]
    for resource, contribution in zip(
        ordered_resources,
        ordered_contributions,
        strict=True,
    ):
        validate_semantic_pair(resource, contribution)
    return ordered_resources, ordered_contributions


def _record_resource_counts(
    resource: Mapping[str, Any],
    contribution: Mapping[str, Any],
    resource_count_by_type: dict[str, int],
    outcome_count_by_name: dict[str, int],
) -> None:
    resource_type = resource["resource_type"]
    resource_count_by_type[resource_type] = _checked_add(
        resource_count_by_type.get(resource_type, 0),
        1,
        "semantic_resource_count",
    )
    increment_by_outcome = {
        "address_records": resource["summary_address_count"],
        "addressed_locations": int(resource["summary_addressed_location"]),
        "geocoded_locations": int(resource["summary_geocoded_location"]),
        "network_plan_links": resource["summary_network_link_count"],
        "organization_affiliation_links": resource[
            "summary_affiliation_link_count"
        ],
        "specialty_records": len(contribution["specialties"]),
        "contact_records": len(contribution["contacts"]),
        "reference_links": len(contribution["references"]),
    }
    for field_name, increment in increment_by_outcome.items():
        outcome_count_by_name[field_name] = _checked_add(
            outcome_count_by_name[field_name],
            increment,
            f"semantic_{field_name}",
        )


def _sealed_semantic_outcome_proof(
    *,
    canonical_row_sha256: str,
    profile_contribution_sha256: str,
    distinct_npi_sha256: str,
    resource_count: int,
    resource_count_by_type: Mapping[str, int],
    outcome_count_by_name: Mapping[str, int],
) -> ProjectionSemanticOutcomeProof:
    proof_map = _semantic_outcome_proof_payload(
        canonical_row_sha256=canonical_row_sha256,
        profile_contribution_sha256=profile_contribution_sha256,
        distinct_npi_sha256=distinct_npi_sha256,
        resource_count=resource_count,
        resource_count_by_type=resource_count_by_type,
        outcome_count_by_name=outcome_count_by_name,
    )
    proof_hash = stable_hash(
        proof_map,
        domain="provider-directory-projection-semantic-outcome-proof-v1",
    )
    return ProjectionSemanticOutcomeProof(
        canonical_row_sha256=canonical_row_sha256,
        profile_contribution_sha256=profile_contribution_sha256,
        distinct_npi_sha256=distinct_npi_sha256,
        resource_count=resource_count,
        resource_counts=dict(sorted(resource_count_by_type.items())),
        outcome_counts=dict(outcome_count_by_name),
        proof_sha256=proof_hash,
        proof=proof_map,
    )


def _record_resource_npi(
    resource: Mapping[str, Any],
    occurrence_total: int,
    occurrence_accumulator: int,
) -> tuple[int, int]:
    summary_npi = resource["summary_npi"]
    if summary_npi is None:
        return occurrence_total, occurrence_accumulator
    occurrence_total = _checked_add(
        occurrence_total,
        1,
        "npi_occurrence_count",
    )
    occurrence_accumulator = (
        occurrence_accumulator * npi_occurrence_factor(summary_npi)
    ) % NPI_ACCUMULATOR_MODULUS
    return occurrence_total, occurrence_accumulator


def _stream_semantic_pairs(
    semantic_pairs: Iterable[tuple[Mapping[str, Any], Mapping[str, Any]]],
) -> tuple[Any, SemanticStableListDigest, dict[str, int], dict[str, int], int, int, int]:
    row_digest = hashlib.sha256()
    profile_digest = SemanticStableListDigest("provider-directory-projection-profile-contribution-set-v1")
    resource_count_by_type: dict[str, int] = {}
    outcome_count_by_name = {field_name: 0 for field_name in SEMANTIC_OUTCOME_FIELDS}
    previous_identity: tuple[str, str] | None = None
    resource_count = 0
    resource_npi_occurrence_total = 0
    resource_npi_accumulator = 1
    for resource, contribution in semantic_pairs:
        current_identity = semantic_identity(resource)
        if previous_identity is not None and current_identity <= previous_identity:
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_semantic_pairs_not_strictly_sorted"
            )
        validate_semantic_pair(resource, contribution)
        if resource_count:
            row_digest.update(b"\n")
        row_digest.update(canonical_semantic_resource_row(resource))
        profile_digest.append(contribution)
        _record_resource_counts(
            resource,
            contribution,
            resource_count_by_type,
            outcome_count_by_name,
        )
        resource_count = _checked_add(
            resource_count,
            1,
            "semantic_resource_count",
        )
        resource_npi_occurrence_total, resource_npi_accumulator = (
            _record_resource_npi(
                resource,
                resource_npi_occurrence_total,
                resource_npi_accumulator,
            )
        )
        previous_identity = current_identity
    if not resource_count:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_semantic_pair_set_mismatch"
        )
    return (
        row_digest, profile_digest, resource_count_by_type, outcome_count_by_name,
        resource_count, resource_npi_occurrence_total, resource_npi_accumulator,
    )


def _reduce_normalized_semantic_outcomes(
    semantic_pairs: Iterable[tuple[Mapping[str, Any], Mapping[str, Any]]],
    npi_occurrences: Iterable[tuple[int, int]],
) -> ProjectionSemanticOutcomeProof:
    """Reduce already normalized pairs with constant row-relative memory."""

    (
        row_digest, profile_digest, resource_count_by_type, outcome_count_by_name,
        resource_count, resource_npi_occurrence_total, resource_npi_accumulator,
    ) = _stream_semantic_pairs(semantic_pairs)
    (
        grouped_npi_occurrence_total,
        distinct_npi_count,
        distinct_npi_sha256,
        grouped_npi_accumulator,
    ) = streamed_npi_proof(npi_occurrences)
    if (
        grouped_npi_occurrence_total != resource_npi_occurrence_total
        or grouped_npi_accumulator != resource_npi_accumulator
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_npi_occurrence_stream_mismatch"
        )
    outcome_count_by_name["distinct_npis"] = distinct_npi_count
    outcome_count_by_name["individual_practitioners"] = (
        resource_count_by_type.get("Practitioner", 0)
    )
    outcome_count_by_name["organization_resources"] = (
        resource_count_by_type.get("Organization", 0)
    )
    outcome_count_by_name["practitioner_role_resources"] = (
        resource_count_by_type.get("PractitionerRole", 0)
    )
    _validate_outcome_consistency(
        resource_count_by_type,
        outcome_count_by_name,
    )
    return _sealed_semantic_outcome_proof(
        canonical_row_sha256=row_digest.hexdigest(),
        profile_contribution_sha256=profile_digest.hexdigest(),
        distinct_npi_sha256=distinct_npi_sha256,
        resource_count=resource_count,
        resource_count_by_type=resource_count_by_type,
        outcome_count_by_name=outcome_count_by_name,
    )


def reduce_streaming_semantic_outcomes(
    semantic_pairs: Iterable[tuple[Mapping[str, Any], Mapping[str, Any]]],
    npi_occurrences: Iterable[tuple[int, int]],
) -> ProjectionSemanticOutcomeProof:
    """Build a candidate aggregate without retaining winner sets.

    Callers must supply a strict winner/Profile join ordered by
    ``(resource_type, resource_id)`` and a strict ``GROUP BY summary_npi``
    occurrence stream from one snapshot.  This checks shape, not byte origin.
    """

    return _reduce_normalized_semantic_outcomes(
        normalized_semantic_pair_stream(semantic_pairs),
        npi_occurrences,
    )


def reduce_semantic_outcomes(
    resources: Iterable[Mapping[str, Any]],
    profile_contributions: Iterable[Mapping[str, Any]],
) -> ProjectionSemanticOutcomeProof:
    """Build a bounded in-memory candidate for tests and small fixtures only."""

    ordered_resources, ordered_contributions = _ordered_semantic_pairs(
        resources,
        profile_contributions,
    )
    npi_occurrence_by_value: dict[int, int] = {}
    for resource in ordered_resources:
        summary_npi = resource["summary_npi"]
        if summary_npi is not None:
            npi_occurrence_by_value[summary_npi] = (
                npi_occurrence_by_value.get(summary_npi, 0) + 1
            )
    return _reduce_normalized_semantic_outcomes(
        zip(ordered_resources, ordered_contributions, strict=True),
        sorted(npi_occurrence_by_value.items()),
    )


def _normalized_proof_counts(
    raw_proof: ProjectionSemanticOutcomeProof,
) -> tuple[int, dict[str, int], dict[str, int]]:
    resource_count = _aggregate_count(
        raw_proof.resource_count,
        "semantic_resource_count",
    )
    if resource_count < 1 or not isinstance(raw_proof.resource_counts, Mapping):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_semantic_resource_count_mismatch"
        )
    resource_count_by_type = {
        required_text(resource_type, "resource_type", limit=64): _aggregate_count(
            count,
            "semantic_resource_count",
        )
        for resource_type, count in raw_proof.resource_counts.items()
    }
    if (
        not resource_count_by_type
        or any(count < 1 for count in resource_count_by_type.values())
        or sum(resource_count_by_type.values()) != resource_count
        or not isinstance(raw_proof.outcome_counts, Mapping)
        or set(raw_proof.outcome_counts) != set(SEMANTIC_OUTCOME_FIELDS)
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_semantic_resource_count_mismatch"
        )
    outcome_count_by_name = {
        field_name: _aggregate_count(
            raw_proof.outcome_counts[field_name],
            f"semantic_{field_name}",
        )
        for field_name in SEMANTIC_OUTCOME_FIELDS
    }
    _validate_outcome_consistency(resource_count_by_type, outcome_count_by_name)
    return resource_count, resource_count_by_type, outcome_count_by_name


def _validated_proof_hashes(
    raw_proof: ProjectionSemanticOutcomeProof,
) -> tuple[str, str, str]:
    hash_fields = (
        raw_proof.canonical_row_sha256,
        raw_proof.profile_contribution_sha256,
        raw_proof.distinct_npi_sha256,
        raw_proof.proof_sha256,
    )
    if not all(isinstance(candidate_hash, str) for candidate_hash in hash_fields):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_semantic_outcome_proof_invalid"
        )
    return (
        required_hash(
            raw_proof.canonical_row_sha256,
            "semantic_canonical_row_sha256",
        ),
        required_hash(
            raw_proof.profile_contribution_sha256,
            "profile_contribution_sha256",
        ),
        required_hash(raw_proof.distinct_npi_sha256, "distinct_npi_sha256"),
    )


def validated_semantic_outcome_proof(
    raw_proof: Any,
) -> ProjectionSemanticOutcomeProof:
    """Validate candidate self-consistency, not retained-byte provenance."""

    if not isinstance(raw_proof, ProjectionSemanticOutcomeProof) or not isinstance(
        raw_proof.proof,
        Mapping,
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_semantic_outcome_proof_invalid"
        )
    resource_count, resource_count_by_type, outcome_count_by_name = (
        _normalized_proof_counts(raw_proof)
    )
    row_hash, profile_hash, npi_hash = _validated_proof_hashes(raw_proof)
    proof_map = _semantic_outcome_proof_payload(
        canonical_row_sha256=row_hash,
        profile_contribution_sha256=profile_hash,
        distinct_npi_sha256=npi_hash,
        resource_count=resource_count,
        resource_count_by_type=resource_count_by_type,
        outcome_count_by_name=outcome_count_by_name,
    )
    proof_hash = stable_hash(
        proof_map,
        domain="provider-directory-projection-semantic-outcome-proof-v1",
    )
    if dict(raw_proof.proof) != proof_map or raw_proof.proof_sha256 != proof_hash:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_semantic_outcome_proof_mismatch"
        )
    return ProjectionSemanticOutcomeProof(
        canonical_row_sha256=row_hash,
        profile_contribution_sha256=profile_hash,
        distinct_npi_sha256=npi_hash,
        resource_count=resource_count,
        resource_counts=dict(sorted(resource_count_by_type.items())),
        outcome_counts=outcome_count_by_name,
        proof_sha256=proof_hash,
        proof=proof_map,
    )


__all__ = [
    "MAX_REFERENCE_REDUCER_ROWS", "PROFILE_CONTRIBUTION_ARRAY_FIELDS",
    "SEMANTIC_CONTRIBUTION_COLUMNS", "SEMANTIC_CONTRIBUTION_CONTRACT_ID",
    "SEMANTIC_OUTCOME_PROOF_CONTRACT_ID", "SEMANTIC_STREAMING_REDUCER_CONTRACT_ID",
    "normalized_semantic_contribution", "reduce_semantic_outcomes",
    "reduce_streaming_semantic_outcomes", "validated_semantic_outcome_proof",
]
