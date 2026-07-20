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


@pytest.mark.asyncio
async def test_locked_fence_ignores_ineligible_old_generation_candidate():
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

    eligibility_sql = executor.all.await_args_list[-1].args[0]
    assert "dataset_options" in eligibility_sql
    assert executor.all.await_args_list[-1].kwargs["endpoint_ids"] == [
        "candidate_endpoint"
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
