# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import dataclasses
import importlib
from unittest.mock import AsyncMock

import pytest


importer = importlib.import_module("process.provider_directory_fhir")


def _candidate(
    dataset_id: str,
    root_run_id: str,
    verification_role: str,
) -> importer.EndpointDatasetCandidate:
    resources = ("Organization", "Practitioner")
    return importer.EndpointDatasetCandidate(
        endpoint_id="endpoint_1",
        dataset_id=dataset_id,
        acquisition_root_run_id=root_run_id,
        source_ids=("source_a", "source_b"),
        selected_resources=resources,
        expected_resources=resources,
        import_run_id=root_run_id,
        previous_dataset_id="dataset_current",
        requires_twin_root_verification=True,
        verification_campaign_id="reviewed-candidates-v1",
        verification_source_scope_hash="scope-v1",
        verification_role=verification_role,
        verification_baseline_dataset_id=(
            "dataset_baseline"
            if verification_role
            == importer.TWIN_ROOT_VERIFICATION_CANDIDATE_ROLE
            else None
        ),
    )


def _diagnostics() -> dict[str, dict[str, object]]:
    return {
        resource_type: {
            "complete": True,
            "bounded": False,
            "error": None,
            "next_url_remaining": False,
        }
        for resource_type in ("Organization", "Practitioner")
    }


def _content_proof(
    *,
    dataset_hash: str = "a" * 64,
) -> importer.EndpointDatasetContentProof:
    return importer.EndpointDatasetContentProof(
        dataset_hash=dataset_hash,
        resource_count=2,
        resource_hashes={
            "Organization": "b" * 64,
            "Practitioner": "c" * 64,
        },
        resource_counts={"Organization": 1, "Practitioner": 1},
    )


def _baseline_map(
    candidate: importer.EndpointDatasetCandidate,
) -> dict[str, object]:
    content_proof = _content_proof()
    proof = importer._twin_root_content_proof(candidate, content_proof)
    proof["acquisition_root_run_id"] = "root_baseline"
    return {
        "dataset_id": "dataset_baseline",
        "endpoint_id": candidate.endpoint_id,
        "acquisition_root_run_id": "root_baseline",
        "status": importer.ENDPOINT_DATASET_VERIFICATION_BASELINE,
        "dataset_hash": content_proof.dataset_hash,
        "resource_count": content_proof.resource_count,
        "verification_baseline_count": 1,
        "publication_metadata_json": {
            "source_ids": list(candidate.source_ids),
            "selected_resources": list(candidate.selected_resources),
            "expected_resources": list(candidate.expected_resources),
            "requires_twin_root_verification": True,
            importer.TWIN_ROOT_VERIFICATION_CAMPAIGN_KEY: (
                candidate.verification_campaign_id
            ),
            importer.TWIN_ROOT_VERIFICATION_SOURCE_SCOPE_KEY: (
                candidate.verification_source_scope_hash
            ),
            importer.TWIN_ROOT_VERIFICATION_ROLE_KEY: (
                importer.TWIN_ROOT_BASELINE_CANDIDATE_ROLE
            ),
            importer.TWIN_ROOT_VERIFICATION_BASELINE_DATASET_KEY: None,
            importer.TWIN_ROOT_VERIFICATION_METADATA_KEY: {
                "role": "baseline",
                "admission_role": importer.TWIN_ROOT_BASELINE_CANDIDATE_ROLE,
                "result": "baseline_recorded",
                "proof": proof,
            },
        },
    }


def _terminal_dataset_map(
    candidate: importer.EndpointDatasetCandidate,
    status: str,
) -> dict[str, object]:
    baseline = None
    content_proof = _content_proof()
    if status == importer.ENDPOINT_DATASET_VERIFICATION_MISMATCH:
        baseline = _baseline_map(candidate)
        content_proof = _content_proof(dataset_hash="f" * 64)
    decided_status, verification_metadata, _mismatches = (
        importer._twin_root_verification_decision(
            candidate,
            content_proof,
            baseline,
        )
    )
    assert decided_status == status
    metadata = importer._endpoint_dataset_publication_metadata(
        candidate,
        _diagnostics(),
        dataset_hash=content_proof.dataset_hash,
        resource_count=content_proof.resource_count,
        **verification_metadata,
    )
    return {
        "dataset_id": candidate.dataset_id,
        "endpoint_id": candidate.endpoint_id,
        "acquisition_root_run_id": candidate.acquisition_root_run_id,
        "previous_dataset_id": candidate.previous_dataset_id,
        "status": status,
        "is_current": False,
        "dataset_hash": content_proof.dataset_hash,
        "resource_count": content_proof.resource_count,
        "publication_metadata_json": metadata,
    }


def _checkpoint_candidate(
    verification_role: str,
) -> importer.EndpointDatasetCandidate:
    return dataclasses.replace(
        _candidate("dataset_terminal", "root_terminal", verification_role),
        checkpoint_context=importer.PaginationCheckpointContext(
            canonical_api_base="https://example.test/fhir",
            source_scope_hash="scope-v1",
            source_ids=("source_a", "source_b"),
            owner_run_id="root_terminal",
            acquisition_root_run_id="root_terminal",
        ),
    )


async def _interrupt_terminal_cleanup(monkeypatch, candidate, status):
    finalize = AsyncMock(
        return_value={"dataset_id": candidate.dataset_id, "status": status}
    )
    clear = AsyncMock(side_effect=RuntimeError("cleanup_interrupted"))
    monkeypatch.setattr(
        importer,
        "_finalize_endpoint_dataset_candidate",
        finalize,
    )
    monkeypatch.setattr(
        importer,
        "_clear_finalized_endpoint_dataset_pagination_checkpoints",
        clear,
    )
    with pytest.raises(RuntimeError, match="cleanup_interrupted"):
        await importer._finalize_candidate_and_clear_checkpoints(
            candidate,
            _diagnostics(),
        )
    return clear


def _terminal_replay_candidate(candidate, terminal_row):
    selection = importer._verification_terminal_endpoint_dataset_selection(
        terminal_row,
        candidate.dataset_id,
        candidate.endpoint_id,
        candidate.acquisition_root_run_id,
        requires_twin_root_verification=True,
        verification_campaign_id=candidate.verification_campaign_id,
        verification_source_scope_hash=(
            candidate.verification_source_scope_hash
        ),
    )
    assert selection is not None
    return dataclasses.replace(
        candidate,
        verification_terminal_status=selection.verification_terminal_status,
        verification_terminal_metadata=selection.verification_terminal_metadata,
    )


async def _replay_terminal_candidate(candidate, clear, status):
    clear.side_effect = None
    if status == importer.ENDPOINT_DATASET_VERIFICATION_MISMATCH:
        with pytest.raises(RuntimeError, match="verification_mismatch"):
            await importer._replay_finalized_candidate_and_clear_checkpoints(
                candidate,
                {},
            )
    else:
        await importer._replay_finalized_candidate_and_clear_checkpoints(
            candidate,
            {},
        )
    assert clear.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,verification_role",
    [
        (
            importer.ENDPOINT_DATASET_VERIFICATION_BASELINE,
            importer.TWIN_ROOT_BASELINE_CANDIDATE_ROLE,
        ),
        (
            importer.ENDPOINT_DATASET_VERIFICATION_MISMATCH,
            importer.TWIN_ROOT_VERIFICATION_CANDIDATE_ROLE,
        ),
    ],
)
async def test_terminal_commit_before_cleanup_is_replayable(
    monkeypatch,
    status,
    verification_role,
):
    candidate = _checkpoint_candidate(verification_role)
    terminal_row = _terminal_dataset_map(candidate, status)
    clear = await _interrupt_terminal_cleanup(
        monkeypatch,
        candidate,
        status,
    )
    replay_candidate = _terminal_replay_candidate(candidate, terminal_row)
    await _replay_terminal_candidate(replay_candidate, clear, status)


def _matched_dataset_map(candidate, status, is_current):
    content_proof = _content_proof()
    decided_status, verification_metadata, mismatches = (
        importer._twin_root_verification_decision(
            candidate,
            content_proof,
            _baseline_map(candidate),
        )
    )
    assert decided_status == importer.ENDPOINT_DATASET_VALIDATED
    assert mismatches == []
    metadata = importer._endpoint_dataset_publication_metadata(
        candidate,
        _diagnostics(),
        dataset_hash=content_proof.dataset_hash,
        resource_count=content_proof.resource_count,
        **verification_metadata,
    )
    return {
        "dataset_id": candidate.dataset_id,
        "endpoint_id": candidate.endpoint_id,
        "acquisition_root_run_id": candidate.acquisition_root_run_id,
        "previous_dataset_id": candidate.previous_dataset_id,
        "status": status,
        "is_current": is_current,
        "dataset_hash": content_proof.dataset_hash,
        "resource_count": content_proof.resource_count,
        "validated_at": object(),
        "publication_metadata_json": metadata,
    }, metadata


def _select_finalized_root(candidate, dataset_map, selector, campaign_id):
    return selector(
        dataset_map,
        candidate.dataset_id,
        candidate.endpoint_id,
        candidate.acquisition_root_run_id,
        requires_twin_root_verification=False,
        verification_campaign_id=campaign_id,
        verification_source_scope_hash=(
            candidate.verification_source_scope_hash
        ),
    )


@pytest.mark.parametrize(
    "status,is_current",
    [
        (importer.ENDPOINT_DATASET_VALIDATED, False),
        (importer.ENDPOINT_DATASET_PUBLISHED, True),
    ],
)
def test_matched_root_replays_after_same_campaign_promotion(
    status,
    is_current,
):
    candidate = _candidate(
        "dataset_matched",
        "root_matched",
        importer.TWIN_ROOT_VERIFICATION_CANDIDATE_ROLE,
    )
    dataset_map, metadata = _matched_dataset_map(candidate, status, is_current)
    selector = (
        importer._validated_endpoint_dataset_selection
        if status == importer.ENDPOINT_DATASET_VALIDATED
        else importer._published_endpoint_dataset_selection
    )
    selection = _select_finalized_root(
        candidate,
        dataset_map,
        selector,
        candidate.verification_campaign_id,
    )
    assert selection is not None
    assert selection.requires_twin_root_verification is True
    replay_candidate = dataclasses.replace(
        candidate,
        already_validated=(status == importer.ENDPOINT_DATASET_VALIDATED),
        validated_metadata=(
            metadata if status == importer.ENDPOINT_DATASET_VALIDATED else None
        ),
        already_published=(status == importer.ENDPOINT_DATASET_PUBLISHED),
        published_metadata=(
            metadata if status == importer.ENDPOINT_DATASET_PUBLISHED else None
        ),
    )
    importer._assert_finalized_endpoint_dataset_replay(replay_candidate)
    with pytest.raises(RuntimeError, match="generation_mismatch"):
        _select_finalized_root(
            candidate,
            dataset_map,
            selector,
            "reviewed-candidates-v2",
        )
