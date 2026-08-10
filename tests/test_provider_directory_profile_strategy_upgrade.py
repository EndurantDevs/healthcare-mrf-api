# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Prove canonical legacy-to-v6 Provider Directory Profile deltas."""

from __future__ import annotations

import dataclasses
import importlib
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_profile as profile_artifact
from process.provider_directory_rooted_graph_source_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
)
from process.uhc_flex_practitioner_contract import (
    UHC_FLEX_PRACTITIONER_SOURCE_ID,
)


importer = importlib.import_module("process.provider_directory_fhir")


LEGACY_V4_STRATEGY = "source-fact-role32-org32-membership32-npi5m-v4"
LEGACY_V5_STRATEGY = "source-fact-role32-org32-member32-dataset-pract-auth-npi5m-v5"


def _legacy_source_context_digest(source_id: str) -> str:
    return importer._source_context_digest(
        [
            {
                "source_id": source_id,
                "endpoint_id": "endpoint-a",
                "canonical_api_base": "https://directory.example.test/R4",
                "org_name": "Synthetic directory",
                "plan_name": None,
            }
        ]
    )


def _desired_identity():
    source_vector = (
        ("source-a", "dataset-a"),
        ("source-b", "dataset-b"),
    )
    source_context_vector = importer._provider_directory_profile_source_context_vector(
        (
            importer._ProviderDirectoryProfileSourceContext(
                source_id="source-a",
                endpoint_id="endpoint-a",
                canonical_api_base="https://directory.example.test/R4",
                org_name="Synthetic directory",
                plan_name=None,
                authority_id="shared-authority",
            ),
            importer._ProviderDirectoryProfileSourceContext(
                source_id="source-b",
                endpoint_id="endpoint-b",
                canonical_api_base="https://enrichment.example.test/R4",
                org_name="Synthetic enrichment",
                plan_name=None,
                authority_id="shared-authority",
            ),
        )
    )
    return importer._ProviderDirectoryProfileDesiredIdentity(
        source_ids=[source_id for source_id, _dataset_id in source_vector],
        dataset_ids=[dataset_id for _source_id, dataset_id in source_vector],
        source_vector=source_vector,
        source_vector_hash=(
            importer._provider_directory_profile_source_vector_hash(source_vector)
        ),
        source_context_vector=source_context_vector,
        source_context_vector_hash=(
            importer._provider_directory_profile_source_context_vector_hash(
                source_context_vector
            )
        ),
    )


def _legacy_serving_state():
    source_vector = (
        ("source-a", "dataset-a"),
        ("source-z", "dataset-z"),
    )
    source_context_vector = (
        ("source-a", _legacy_source_context_digest("source-a")),
        ("source-z", _legacy_source_context_digest("source-z")),
    )
    return importer._ProviderDirectoryProfileServingState(
        status="published",
        operation="publish",
        control_generation=7,
        generation_id="pdprofile_" + "1" * 32,
        selection_proof_id="2" * 64,
        authority_revision=7,
        profile_schema_version=1,
        profile_strategy_version=LEGACY_V4_STRATEGY,
        source_vector=source_vector,
        source_vector_hash=(
            importer._provider_directory_profile_source_vector_hash(source_vector)
        ),
        source_context_vector=source_context_vector,
        source_context_vector_hash=(
            importer._provider_directory_profile_source_context_vector_hash(
                source_context_vector
            )
        ),
        executable_plan_hash="3" * 64,
        evidence_target_oid=11,
        profile_target_oid=12,
        evidence_rows=10,
        profile_rows=5,
        profile_as_of="2026-08-09",
        published_at="2026-08-09T00:00:00+00:00",
    )


def test_v4_contexts_refresh_every_v6_source() -> None:
    serving_state = _legacy_serving_state()
    desired = _desired_identity()

    assert (
        serving_state.source_context_vector[0][1]
        != dict(desired.source_context_vector)["source-a"]
    )
    assert importer._provider_directory_profile_delta_sources(
        serving_state,
        desired.source_vector,
        desired.source_context_vector,
    ) == (("source-a", "source-b"), ("source-z",))


def _canonical_dataset_context(source_id: str):
    endpoint_id = dict(profile_artifact.configured_dataset_scoped_profile_endpoints())[
        source_id
    ]
    return importer._provider_directory_profile_source_context_vector(
        (
            importer._ProviderDirectoryProfileSourceContext(
                source_id=source_id,
                endpoint_id=endpoint_id,
                canonical_api_base="https://synthetic.invalid/R4",
                org_name="Synthetic dataset variant",
                plan_name=None,
                authority_id=(
                    profile_artifact.profile_reviewed_source_authority_id(source_id)
                ),
            ),
        )
    )


def _v5_dataset_serving_state(source_id: str, dataset_id: str):
    source_vector = ((source_id, dataset_id),)
    source_context_vector = _canonical_dataset_context(source_id)
    return dataclasses.replace(
        _legacy_serving_state(),
        profile_strategy_version=LEGACY_V5_STRATEGY,
        source_vector=source_vector,
        source_vector_hash=(
            importer._provider_directory_profile_source_vector_hash(source_vector)
        ),
        source_context_vector=source_context_vector,
        source_context_vector_hash=(
            importer._provider_directory_profile_source_context_vector_hash(
                source_context_vector
            )
        ),
    )


def test_v5_to_v6_variants_refresh_and_remove_old_generation() -> None:
    legacy_dataset = "pdufpd_" + "1" * 48
    rooted_dataset = "pdrgpd_" + "2" * 48
    legacy_context = _canonical_dataset_context(UHC_FLEX_PRACTITIONER_SOURCE_ID)
    rooted_context = _canonical_dataset_context(
        PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID
    )

    legacy_serving = _v5_dataset_serving_state(
        UHC_FLEX_PRACTITIONER_SOURCE_ID,
        legacy_dataset,
    )
    assert importer._provider_directory_profile_delta_sources(
        legacy_serving,
        ((UHC_FLEX_PRACTITIONER_SOURCE_ID, legacy_dataset),),
        legacy_context,
    ) == ((UHC_FLEX_PRACTITIONER_SOURCE_ID,), ())
    assert importer._provider_directory_profile_delta_sources(
        legacy_serving,
        ((PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID, rooted_dataset),),
        rooted_context,
    ) == (
        (PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,),
        (UHC_FLEX_PRACTITIONER_SOURCE_ID,),
    )

    rooted_serving = _v5_dataset_serving_state(
        PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
        rooted_dataset,
    )
    assert importer._provider_directory_profile_delta_sources(
        rooted_serving,
        ((UHC_FLEX_PRACTITIONER_SOURCE_ID, legacy_dataset),),
        legacy_context,
    ) == (
        (UHC_FLEX_PRACTITIONER_SOURCE_ID,),
        (PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,),
    )


@pytest.mark.asyncio
async def test_v4_serving_generation_uses_v6_source_delta(monkeypatch) -> None:
    serving_state = _legacy_serving_state()
    desired = _desired_identity()
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_delta_serving_state",
        AsyncMock(return_value=serving_state),
    )
    execution_token = importer._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.set(
        object()
    )
    try:
        materialization = await importer._profile_materialization_identity(
            "mrf",
            desired,
            has_existing_artifacts=True,
            allow_serving_generation_adoption=False,
        )
    finally:
        importer._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.reset(execution_token)

    assert materialization.materialization_mode == "source_delta"
    assert materialization.source_ids == ["source-a", "source-b"]
    assert materialization.dataset_ids == ["dataset-a", "dataset-b"]
    assert materialization.removed_source_ids == ("source-z",)
