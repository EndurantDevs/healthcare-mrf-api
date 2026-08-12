# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib
from unittest.mock import AsyncMock

import pytest


importer = importlib.import_module("process.provider_directory_fhir")


def _promotion_dataset() -> importer.ProviderDirectoryArtifactDataset:
    return importer.ProviderDirectoryArtifactDataset(
        source_id="source_a",
        endpoint_id="candidate_endpoint",
        serving_endpoint_id="serving_endpoint_old",
        dataset_id="dataset_v2_exact",
        evidence_run_id="root_v2_exact",
        recorded_expected_resources=(),
        status=importer.ENDPOINT_DATASET_VALIDATED,
        is_current=False,
        previous_dataset_id="dataset_current",
        expected_incumbent_dataset_id="dataset_current",
        promote_on_cutover=True,
    )


def _current_dataset() -> importer.ProviderDirectoryArtifactDataset:
    return importer.ProviderDirectoryArtifactDataset(
        source_id="source_a",
        endpoint_id="candidate_endpoint",
        serving_endpoint_id="candidate_endpoint",
        dataset_id="dataset_current",
        evidence_run_id="root_current",
        recorded_expected_resources=(),
        status=importer.ENDPOINT_DATASET_PUBLISHED,
        is_current=True,
    )


def _source_row(endpoint_id: str) -> dict[str, object]:
    return {
        "source_id": "source_a",
        "endpoint_id": endpoint_id,
        "source_record_json": {
            "source_id": "source_a",
            "endpoint_id": endpoint_id,
            "metadata_json": {},
        },
    }


def _dataset_row(
    dataset_id: str,
    root_run_id: str,
    status: str,
    is_current: bool,
    previous_dataset_id: str | None = None,
) -> dict[str, object]:
    return {
        "dataset_id": dataset_id,
        "endpoint_id": "candidate_endpoint",
        "acquisition_root_run_id": root_run_id,
        "previous_dataset_id": previous_dataset_id,
        "dataset_hash": None,
        "status": status,
        "is_current": is_current,
        "resource_count": 0,
        "validated_at": None,
        "published_at": None,
        "superseded_at": None,
        "publication_metadata_json": {},
    }


def _executor(*query_results):
    executor = AsyncMock()
    executor.all.side_effect = list(query_results)
    executor.status.return_value = 1
    return executor


def _assert_endpoint_advisories(executor, expected_endpoint_ids) -> None:
    """Require endpoint advisory locks in deterministic sorted order."""

    advisory_calls = [
        call
        for call in executor.status.await_args_list
        if "pg_advisory_xact_lock" in call.args[0]
    ]
    assert [call.kwargs["endpoint_id"] for call in advisory_calls] == (
        expected_endpoint_ids
    )


def test_candidate_eligibility_projection_is_compact_and_fail_closed():
    projection_sql = importer._artifact_candidate_eligibility_ctes(
        "dataset_table"
    )
    for field_name in (
        "requires_twin_root_verification jsonb",
        "source_ids jsonb",
        "completion_proof_v1 jsonb",
        f"{importer.SERVER_ISSUED_SUBSET_REPLAY_EVIDENCE_KEY} jsonb",
        f"{importer.SERVER_ISSUED_SUBSET_REPLAY_EVIDENCE_SHA256_KEY} text",
        f"{importer.SERVER_ISSUED_SUBSET_COVERAGE_KEY} jsonb",
        "selected_resources jsonb",
        "expected_resources jsonb",
        f"{importer.TWIN_ROOT_VERIFICATION_METADATA_KEY} jsonb",
        f"{importer.REVIEWED_ROOT_POLICY_METADATA_KEY} jsonb",
    ):
        assert field_name in projection_sql
    assert (
        f"{importer.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY} jsonb"
        not in projection_sql
    )
    assert (
        f"? '{importer.REVIEWED_ROOT_POLICY_METADATA_KEY}'"
        in projection_sql
    )
    assert (
        f"? '{importer.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY}'"
        in projection_sql
    )
    assert "candidate.completion_proof_required_version IS NOT NULL" in (
        projection_sql
    )
    assert "full_metadata_jsonb IS NULL" in projection_sql
    assert "jsonb_typeof(candidate.full_metadata_jsonb) = 'object'" in (
        projection_sql
    )
    for identity_field in (
        importer.RESOURCE_HASH_CONTRACT_METADATA_KEY,
        importer.PROVIDER_DIRECTORY_PROOF_RESOURCE_SCOPE_METADATA_KEY,
        importer.SEMANTIC_PROJECTION_AS_OF_METADATA_KEY,
    ):
        assert f"? '{identity_field}'" in projection_sql
        assert f"-> '{identity_field}'" in projection_sql


def test_twin_artifact_sql_binds_optional_semantic_hash_identity():
    matched_sql = " ".join(
        importer._artifact_matched_proof_sql(
            "candidate_metadata",
            "candidate_verification",
            "candidate_proof",
        ).split()
    )
    twin_equality_sql = " ".join(
        importer._artifact_twin_proof_equality_sql(
            "candidate_proof",
            "baseline_proof",
        ).split()
    )
    baseline_sql = " ".join(
        importer._artifact_baseline_proof_sql(
            "dataset_table",
            "candidate_metadata",
            "candidate_verification",
        ).split()
    )
    for identity_field in (
        importer.PROVIDER_DIRECTORY_PROOF_RESOURCE_SCOPE_METADATA_KEY,
        importer.SEMANTIC_PROJECTION_AS_OF_METADATA_KEY,
    ):
        assert (
            f"(candidate_proof -> '{identity_field}') "
            "IS NOT DISTINCT FROM "
            f"(candidate_metadata -> '{identity_field}')"
        ) in matched_sql
        assert (
            f"(candidate_proof -> '{identity_field}') "
            "IS NOT DISTINCT FROM "
            f"(baseline_proof -> '{identity_field}')"
        ) in twin_equality_sql
        assert identity_field in baseline_sql
        assert "IS NOT DISTINCT FROM" in baseline_sql
    assert importer.RESOURCE_HASH_CONTRACT_METADATA_KEY in baseline_sql
    assert importer.LEGACY_RESOURCE_HASH_CONTRACT in baseline_sql
    assert importer.TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT in baseline_sql
    assert importer.SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT in baseline_sql


def test_reviewed_candidate_sql_has_closed_single_root_policy_branch():
    eligibility_sql = importer._artifact_subset_candidate_eligibility_sql(
        "dataset_table",
        "source_table",
        "dataset.publication_metadata_json::jsonb",
        "dataset.publication_metadata_json::jsonb -> 'twin_root_verification_v1'",
        "dataset.publication_metadata_json::jsonb -> 'twin_root_verification_v1' -> 'proof'",
    )

    assert importer.PROVIDER_DIRECTORY_ROOT_POLICY_VERIFIED in eligibility_sql
    assert importer.REVIEWED_ROOT_POLICY_METADATA_KEY in eligibility_sql
    assert importer.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY in eligibility_sql
    assert "'required_root_count', 1" in eligibility_sql
    assert "'required_root_count', 2" in eligibility_sql
    assert "twin_state' = 'not_required'" in eligibility_sql


@pytest.mark.asyncio
async def test_locked_fence_ignores_ineligible_old_generation_candidate():
    """Lock only the exact reviewed candidate after endpoint serialization."""

    candidate = _promotion_dataset()
    fence = importer.ProviderDirectoryArtifactDatasetFence(
        (candidate,),
        should_select_validated_candidates=True,
    )
    executor = _executor(
        [
            {"endpoint_id": "candidate_endpoint"},
            {"endpoint_id": "serving_endpoint_old"},
        ],
        [_source_row("serving_endpoint_old")],
        [
            _dataset_row(
                "dataset_current",
                "root_current",
                importer.ENDPOINT_DATASET_PUBLISHED,
                True,
            ),
            _dataset_row(
                "dataset_v1_old",
                "root_v1_old",
                importer.ENDPOINT_DATASET_VALIDATED,
                False,
                "dataset_current",
            ),
            _dataset_row(
                "dataset_v2_exact",
                "root_v2_exact",
                importer.ENDPOINT_DATASET_VALIDATED,
                False,
                "dataset_current",
            ),
        ],
        [
            {
                "endpoint_id": "candidate_endpoint",
                "dataset_id": "dataset_v2_exact",
            }
        ],
    )

    await importer._lock_and_verify_artifact_dataset_fence(fence, executor)

    _assert_endpoint_advisories(
        executor,
        ["candidate_endpoint", "serving_endpoint_old"],
    )
    eligibility_sql = executor.all.await_args_list[-1].args[0]
    assert "candidate_source_ids AS MATERIALIZED" in eligibility_sql
    assert "eligible_candidate_ids AS MATERIALIZED" in eligibility_sql
    assert "publication_metadata_summary_json" in eligibility_sql
    assert executor.all.await_args_list[-1].kwargs["source_ids"] == [
        "source_a"
    ]


@pytest.mark.asyncio
async def test_locked_fence_rejects_cross_endpoint_candidate_drift():
    """Reject a new candidate for any source alias held by the fence."""

    candidate = _promotion_dataset()
    fence = importer.ProviderDirectoryArtifactDatasetFence(
        (candidate,),
        should_select_validated_candidates=True,
    )
    executor = _executor(
        [
            {"endpoint_id": "candidate_endpoint"},
            {"endpoint_id": "serving_endpoint_old"},
        ],
        [_source_row("serving_endpoint_old")],
        [
            _dataset_row(
                "dataset_current",
                "root_current",
                importer.ENDPOINT_DATASET_PUBLISHED,
                True,
            ),
            _dataset_row(
                "dataset_v2_exact",
                "root_v2_exact",
                importer.ENDPOINT_DATASET_VALIDATED,
                False,
                "dataset_current",
            ),
        ],
        [
            {
                "endpoint_id": "candidate_endpoint",
                "dataset_id": "dataset_v2_exact",
            },
            {
                "endpoint_id": "other_endpoint",
                "dataset_id": "dataset_other",
            },
        ],
    )

    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="endpoint_dataset_candidate_changed",
    ):
        await importer._lock_and_verify_artifact_dataset_fence(
            fence,
            executor,
        )

    assert executor.all.await_args_list[-1].kwargs["source_ids"] == [
        "source_a"
    ]


@pytest.mark.asyncio
async def test_locked_current_fence_rejects_newly_eligible_candidate():
    current = _current_dataset()
    fence = importer.ProviderDirectoryArtifactDatasetFence(
        (current,),
        should_select_validated_candidates=True,
    )
    executor = _executor(
        [{"endpoint_id": "candidate_endpoint"}],
        [_source_row("candidate_endpoint")],
        [
            _dataset_row(
                "dataset_current",
                "root_current",
                importer.ENDPOINT_DATASET_PUBLISHED,
                True,
            ),
            _dataset_row(
                "dataset_new_exact",
                "root_new_exact",
                importer.ENDPOINT_DATASET_VALIDATED,
                False,
                "dataset_current",
            ),
        ],
        [
            {
                "endpoint_id": "candidate_endpoint",
                "dataset_id": "dataset_new_exact",
            }
        ],
    )

    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="endpoint_dataset_candidate_changed",
    ):
        await importer._lock_and_verify_artifact_dataset_fence(
            fence,
            executor,
        )
