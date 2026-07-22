# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Internal candidate-envelope checks, never native publication attestation."""

from __future__ import annotations

import pytest

from process.provider_directory_projection_contract import (
    PROJECTION_REDUCER_PROOF_CONTRACT_ID,
    projection_completeness_manifest,
    projection_proof_shard,
    projection_reducer_proof,
    projection_recipe_identity,
    reduced_physical_projection_proof,
)
from process.provider_directory_projection_contribution import (
    SEMANTIC_CONTRIBUTION_CONTRACT_ID,
    SEMANTIC_OUTCOME_PROOF_CONTRACT_ID,
    SEMANTIC_STREAMING_REDUCER_CONTRACT_ID,
    reduce_semantic_outcomes,
)
from process.provider_directory_projection_semantic_evidence import (
    SEMANTIC_TYPED_EVIDENCE_CONTRACT_ID,
)
from process.provider_directory_projection_inline_profile import (
    INLINE_PROFILE_EVIDENCE_CONTRACT_ID,
)
from process.provider_directory_projection_summary import (
    SEMANTIC_SOURCE_SUMMARY_CONTRACT_ID,
)
from process.provider_directory_projection_types import (
    PROJECTION_MIXED_RESOURCE_TYPE,
    ProviderDirectoryProjectionError,
    stable_hash,
)
from tests.provider_directory_projection_semantic_support import (
    digest,
    semantic_rows_and_contributions,
)


def _recipe():
    selected_resources = (
        "InsurancePlan",
        "Location",
        "Organization",
        "OrganizationAffiliation",
        "Practitioner",
        "PractitionerRole",
    )
    completeness = projection_completeness_manifest(
        endpoint_campaign_hash=digest("campaign"),
        partition_strategy_contract_id="test-partitions.v1",
        selected_resources=selected_resources,
        required_resources=("Organization",),
        terminal_partitions=(
            {
                "resource_type": resource_type,
                "partition_key_hash": digest(f"partition:{resource_type}"),
                "terminal": True,
                "block_count": 1,
                "row_count": 1,
                "byte_count": 100,
            }
            for resource_type in selected_resources
        ),
        complete=True,
    )
    return projection_recipe_identity(
        decoder_contract_id="fhir-r4-plan-net-decoder.v1",
        acquisition_adapter_id="fhir-rest-r4.v1",
        input_set_sha256=digest("input-set"),
        source_ids=("source-a",),
        transform_contract_id="provider-directory-normalization-v3",
        scope_contract_id="healthporta.provider-directory.global-scope.v1",
        transform_context={
            "as_of_date": "2026-07-22",
            "time_rule_contract_id": "fhir-time-rules.v1",
        },
        selected_resources=selected_resources,
        completeness_manifest=completeness,
        required_resources=("Organization",),
    )


def _candidate_inputs():
    resources, contributions = semantic_rows_and_contributions()
    recipe = _recipe()
    outcome_proof = reduce_semantic_outcomes(resources, contributions)
    shard = projection_proof_shard(
        resources,
        recipe=recipe,
        attempt=1,
        partition_ordinal=0,
        resource_type=PROJECTION_MIXED_RESOURCE_TYPE,
        input_sha256=digest("raw-input"),
    )
    resource_count_by_type = {
        resource_type: 1 for resource_type in recipe.selected_resources
    }
    reducer_proof_map = projection_reducer_proof(
        outcome_proof,
        resource_count_by_type,
    )
    return (
        recipe,
        shard,
        outcome_proof,
        resource_count_by_type,
        reducer_proof_map,
    )


def test_candidate_projection_binds_derived_source_summary():
    (
        recipe,
        shard,
        outcome_proof,
        resource_counts,
        reducer_proof,
    ) = _candidate_inputs()

    candidate_projection = reduced_physical_projection_proof(
        recipe,
        [shard],
        dataset_hash=digest("dataset"),
        canonical_row_sha256=outcome_proof.canonical_row_sha256,
        resource_counts=resource_counts,
        reducer_proof=reducer_proof,
        outcome_proof=outcome_proof,
    )

    source_summary = candidate_projection.proof["source_summary"]
    semantic_summary = source_summary["semantic_summary"]
    assert (
        candidate_projection.canonical_row_sha256
        == outcome_proof.canonical_row_sha256
    )
    assert source_summary["resource_counts"] == resource_counts
    assert semantic_summary["outcome_counts"] == outcome_proof.outcome_counts
    assert (
        semantic_summary["semantic_outcome_proof_sha256"]
        == outcome_proof.proof_sha256
    )


@pytest.mark.parametrize(
    "mutation, expected_error",
    (
        ("row_hash", "semantic_outcome_reducer_mismatch"),
        ("resource_count", "semantic_outcome_reducer_mismatch"),
        ("reducer_outcome_hash", "reducer_proof_invalid"),
        ("reducer_row_hash", "reducer_proof_invalid"),
    ),
)
def test_candidate_projection_rejects_cross_wired_reducer_or_semantic_input(
    mutation,
    expected_error,
):
    (
        recipe,
        shard,
        outcome_proof,
        resource_counts,
        reducer_proof,
    ) = _candidate_inputs()
    canonical_row_sha256 = outcome_proof.canonical_row_sha256
    if mutation == "row_hash":
        canonical_row_sha256 = digest("wrong-row-hash")
    elif mutation == "resource_count":
        resource_counts["InsurancePlan"] = 0
    elif mutation == "reducer_outcome_hash":
        reducer_proof["semantic_outcome_proof_sha256"] = digest("wrong-proof")
    else:
        reducer_proof["canonical_row_sha256"] = digest("wrong-row-hash")

    with pytest.raises(ProviderDirectoryProjectionError, match=expected_error):
        reduced_physical_projection_proof(
            recipe,
            [shard],
            dataset_hash=digest("dataset"),
            canonical_row_sha256=canonical_row_sha256,
            resource_counts=resource_counts,
            reducer_proof=reducer_proof,
            outcome_proof=outcome_proof,
        )


def test_recipe_resource_profile_hash_binds_every_semantic_contract():
    recipe = _recipe()
    expected_hash = stable_hash(
        {
            "selected": list(recipe.selected_resources),
            "required": list(recipe.required_resources),
            "inline_profile_evidence_contract_id": (
                INLINE_PROFILE_EVIDENCE_CONTRACT_ID
            ),
            "semantic_contribution_contract_id": (
                SEMANTIC_CONTRIBUTION_CONTRACT_ID
            ),
            "semantic_outcome_proof_contract_id": (
                SEMANTIC_OUTCOME_PROOF_CONTRACT_ID
            ),
            "semantic_streaming_reducer_contract_id": (
                SEMANTIC_STREAMING_REDUCER_CONTRACT_ID
            ),
            "semantic_typed_evidence_contract_id": (
                SEMANTIC_TYPED_EVIDENCE_CONTRACT_ID
            ),
            "semantic_source_summary_contract_id": (
                SEMANTIC_SOURCE_SUMMARY_CONTRACT_ID
            ),
            "reducer_proof_contract_id": PROJECTION_REDUCER_PROOF_CONTRACT_ID,
        },
        domain="provider-directory-projection-resource-profile-v1",
    )

    assert recipe.resource_profile_hash == expected_hash
