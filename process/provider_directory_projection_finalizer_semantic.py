# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Constant-memory semantic proof for a locked projection stage."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping

from process.provider_directory_projection_contribution import (
    _checked_add,
    _record_resource_counts,
    _record_resource_npi,
    _sealed_semantic_outcome_proof,
    _validate_outcome_consistency,
)
from process.provider_directory_projection_db import table_ref
from process.provider_directory_projection_finalizer_proof import _cursor
from process.provider_directory_projection_json import (
    canonical_exact_json,
    exactly_decoded_object,
)
from process.provider_directory_projection_semantic_evidence import (
    PROFILE_CONTRIBUTION_ARRAY_FIELDS,
    NPI_ACCUMULATOR_MODULUS,
    SemanticStableListDigest,
    canonical_semantic_resource_row,
    normalized_profile_contribution,
    normalized_semantic_winner,
    npi_occurrence_factor,
    semantic_identity,
    validate_semantic_pair,
)
from process.provider_directory_projection_types import (
    SEMANTIC_OUTCOME_FIELDS,
    ProjectionLease,
    ProjectionStage,
    ProviderDirectoryProjectionError,
    stable_json,
)


_STAGE_FIELDS = (
    "resource_type",
    "resource_id",
    "proof_partition_id",
    "payload_hash",
    "source_rank",
    "summary_npi",
    "summary_address_count",
    "summary_addressed_location",
    "summary_geocoded_location",
    "summary_network_link_count",
    "summary_affiliation_link_count",
    "active",
    "effective_start",
    "effective_end",
    "observed_at",
)


def _profile_contribution(resource_map: Mapping[str, Any]) -> dict[str, Any]:
    contribution_map = {
        field: resource_map[field]
        for field in (
            "resource_type",
            "resource_id",
            "proof_partition_id",
            "payload_hash",
            "source_rank",
            "active",
            "effective_start",
            "effective_end",
            "observed_at",
        )
    }
    contribution_map["direct_npi"] = resource_map["summary_npi"]
    contribution_map.update(
        {
            column_name: resource_map["profile_evidence"][evidence_name]
            for evidence_name, column_name in PROFILE_CONTRIBUTION_ARRAY_FIELDS
        }
    )
    return normalized_profile_contribution(contribution_map)


def _semantic_pair(raw_row_map: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    payload_text = str(raw_row_map.pop("payload_json_text"))
    payload = canonical_exact_json(exactly_decoded_object(payload_text.encode()))
    if hashlib.sha256(payload).hexdigest() != raw_row_map["payload_hash"]:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_finalizer_payload_hash_mismatch"
        )
    raw_evidence = raw_row_map["profile_evidence_json"]
    raw_row_map["profile_evidence_json"] = (
        json.loads(raw_evidence) if raw_evidence is not None else None
    )
    resource_map = normalized_semantic_winner(raw_row_map)
    contribution_map = _profile_contribution(resource_map)
    validate_semantic_pair(resource_map, contribution_map)
    return resource_map, contribution_map


async def _stream_rows(connection: Any, stage: ProjectionStage, lease: ProjectionLease):
    row_digest, dataset_digest = hashlib.sha256(), hashlib.sha256()
    profile_digest = SemanticStableListDigest(
        "provider-directory-projection-profile-contribution-set-v1"
    )
    resource_count_by_type: dict[str, int] = {}
    outcome_count_by_name = {field: 0 for field in SEMANTIC_OUTCOME_FIELDS}
    previous_identity: tuple[str, str] | None = None
    resource_count = npi_total = 0
    npi_accumulator = 1
    query = f"""
        SELECT {', '.join(_STAGE_FIELDS)}, payload_json::text AS payload_json_text,
               profile_evidence_json::text AS profile_evidence_json
          FROM {table_ref(stage.schema, stage.relation)}
         WHERE physical_projection_id = $1
         ORDER BY resource_type COLLATE "C", resource_id COLLATE "C";
    """
    async for raw_row_map in _cursor(connection, query, lease.recipe.recipe_id):
        resource_map, contribution_map = _semantic_pair(dict(raw_row_map))
        identity = semantic_identity(resource_map)
        if previous_identity is not None and identity <= previous_identity:
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_semantic_pairs_not_strictly_sorted"
            )
        if resource_count:
            row_digest.update(b"\n")
            dataset_digest.update(b"\n")
        row_digest.update(canonical_semantic_resource_row(resource_map))
        dataset_digest.update(stable_json((*identity, resource_map["payload_hash"])).encode())
        profile_digest.append(contribution_map)
        _record_resource_counts(
            resource_map, contribution_map, resource_count_by_type, outcome_count_by_name
        )
        resource_count = _checked_add(resource_count, 1, "semantic_resource_count")
        npi_total, npi_accumulator = _record_resource_npi(
            resource_map, npi_total, npi_accumulator
        )
        previous_identity = identity
    if not resource_count:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_semantic_pair_set_mismatch"
        )
    return (
        row_digest,
        dataset_digest,
        profile_digest,
        resource_count_by_type,
        outcome_count_by_name,
        resource_count,
        npi_total,
        npi_accumulator,
    )


async def _stream_npis(connection: Any, stage: ProjectionStage, lease: ProjectionLease):
    npi_digest = SemanticStableListDigest(
        "provider-directory-projection-distinct-npi-set-v1"
    )
    grouped_total = distinct_npis = 0
    grouped_accumulator = 1
    previous_npi: int | None = None
    query = f"""
        SELECT summary_npi, count(*)::bigint AS occurrence_count
          FROM {table_ref(stage.schema, stage.relation)}
         WHERE physical_projection_id = $1 AND summary_npi IS NOT NULL
         GROUP BY summary_npi ORDER BY summary_npi;
    """
    async for npi_group_map in _cursor(connection, query, lease.recipe.recipe_id):
        npi = npi_group_map["summary_npi"]
        occurrence_count = npi_group_map["occurrence_count"]
        if (
            type(npi) is not int
            or type(occurrence_count) is not int
            or not 1_000_000_000 <= npi <= 2_999_999_999
            or occurrence_count < 1
            or (previous_npi is not None and npi <= previous_npi)
        ):
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_npi_occurrence_invalid"
            )
        grouped_total = _checked_add(
            grouped_total, occurrence_count, "npi_occurrence_count"
        )
        grouped_accumulator = (
            grouped_accumulator
            * pow(npi_occurrence_factor(npi), occurrence_count, NPI_ACCUMULATOR_MODULUS)
        ) % NPI_ACCUMULATOR_MODULUS
        npi_digest.append(npi)
        distinct_npis += 1
        previous_npi = npi
    return grouped_total, grouped_accumulator, distinct_npis, npi_digest.hexdigest()


async def semantic_proof(
    connection: Any,
    stage: ProjectionStage,
    lease: ProjectionLease,
) -> tuple[Any, str, dict[str, int]]:
    """Build exact semantic and dataset proofs from the immutable stage."""

    (
        row_digest,
        dataset_digest,
        profile_digest,
        resource_count_by_type,
        outcome_count_by_name,
        resource_count,
        npi_total,
        npi_accumulator,
    ) = await _stream_rows(connection, stage, lease)
    grouped_total, grouped_accumulator, distinct_npis, distinct_npi_hash = (
        await _stream_npis(connection, stage, lease)
    )
    if grouped_total != npi_total or grouped_accumulator != npi_accumulator:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_npi_occurrence_stream_mismatch"
        )
    outcome_count_by_name.update(
        distinct_npis=distinct_npis,
        individual_practitioners=resource_count_by_type.get("Practitioner", 0),
        organization_resources=resource_count_by_type.get("Organization", 0),
        practitioner_role_resources=resource_count_by_type.get("PractitionerRole", 0),
    )
    _validate_outcome_consistency(resource_count_by_type, outcome_count_by_name)
    outcome_proof = _sealed_semantic_outcome_proof(
        canonical_row_sha256=row_digest.hexdigest(),
        profile_contribution_sha256=profile_digest.hexdigest(),
        distinct_npi_sha256=distinct_npi_hash,
        resource_count=resource_count,
        resource_count_by_type=resource_count_by_type,
        outcome_count_by_name=outcome_count_by_name,
    )
    return outcome_proof, dataset_digest.hexdigest(), resource_count_by_type
