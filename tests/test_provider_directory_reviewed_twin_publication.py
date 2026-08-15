# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Explicit and historical reviewed twin-root publication tests."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import provider_directory_source_catalog_outcomes as catalog_outcomes
from process.provider_directory_validated_publication_contract import (
    AUTOMATIC_VALIDATED_PUBLICATION_POLICY,
    ValidatedPublicationCandidate,
)
from tests.test_provider_directory_generic_admission_publication import (
    ADMISSION_SHA256,
    _locked_incumbent_row_map,
    importer,
)
from tests.test_provider_directory_reviewed_publication import (
    _reviewed_candidate_map,
    _reviewed_catalog_fixture,
    _reviewed_locked_candidate_row,
    _reviewed_publication_fence,
)
from tests.test_provider_directory_validated_publication_contract import (
    CANDIDATE_ID,
    ENDPOINT_ID,
    SOURCE_ID,
)


def _legacy_reviewed_candidate_map(*, first_publication=True):
    return {
        **_reviewed_candidate_map(
            root_count=2,
            first_publication=first_publication,
        ),
        "automatic_publication_policy": AUTOMATIC_VALIDATED_PUBLICATION_POLICY,
    }


def _legacy_reviewed_publication_fence():
    fence = _reviewed_publication_fence(
        root_count=2,
        first_publication=True,
    )
    return replace(
        fence,
        datasets=(
            replace(
                fence.datasets[0],
                reviewed_root_policy=None,
                verification_source_status=(
                    importer.PROVIDER_DIRECTORY_SUBSET_TWIN_ROOT_VERIFIED
                ),
            ),
        ),
        validated_publication_candidate=ValidatedPublicationCandidate.from_payload(
            _legacy_reviewed_candidate_map()
        ),
    )


@pytest.mark.parametrize("first_publication", (True, False))
def test_reviewed_twin_policy_uses_only_activation_proof(first_publication):
    candidate_map = _reviewed_candidate_map(
        root_count=2,
        first_publication=first_publication,
    )

    assert (
        ValidatedPublicationCandidate.from_payload(candidate_map).to_payload()
        == candidate_map
    )
    assert "content_proof_admission_sha256" not in candidate_map
    with pytest.raises(ValueError, match="schema_invalid"):
        ValidatedPublicationCandidate.from_payload(
            {**candidate_map, "content_proof_admission_sha256": ADMISSION_SHA256}
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("first_publication", (True, False))
async def test_catalog_exposes_reviewed_twin_first_and_replacement(
    monkeypatch,
    first_publication,
):
    catalog_map, _, _, is_activation_valid = _reviewed_catalog_fixture(
        monkeypatch,
        root_count=2,
        first_publication=first_publication,
    )

    enriched_catalog = (
        await catalog_outcomes.enrich_provider_directory_source_catalog(
            catalog_map
        )
    )
    assert enriched_catalog["items"][0]["validated_publication_candidate"] == (
        _reviewed_candidate_map(
            root_count=2,
            first_publication=first_publication,
        )
    )
    assert is_activation_valid.await_args.kwargs["source_id"] == SOURCE_ID


@pytest.mark.asyncio
async def test_manual_lane_projects_only_activated_legacy_reviewed_twin(
    monkeypatch,
):
    catalog_map, canonical_dataset, _, is_activation_valid = (
        _reviewed_catalog_fixture(
            monkeypatch,
            root_count=2,
            first_publication=True,
        )
    )
    canonical_dataset.reviewed_root_policy = None
    canonical_dataset.verification_source_status = (
        importer.PROVIDER_DIRECTORY_SUBSET_TWIN_ROOT_VERIFIED
    )

    enriched_catalog = (
        await catalog_outcomes.enrich_provider_directory_source_catalog(
            catalog_map
        )
    )
    assert enriched_catalog["items"][0]["validated_publication_candidate"] == (
        _legacy_reviewed_candidate_map()
    )
    assert is_activation_valid.await_args.kwargs["source_id"] == SOURCE_ID

    catalog_map["items"][0].update(classification="acquisition", runnable=True)
    ordinary_catalog = (
        await catalog_outcomes.enrich_provider_directory_source_catalog(
            catalog_map
        )
    )
    assert "validated_publication_candidate" not in ordinary_catalog["items"][0]

    catalog_map["items"][0].update(
        classification="manual_acquisition",
        runnable=False,
    )
    canonical_dataset.verification_source_status = (
        importer.PROVIDER_DIRECTORY_TWIN_ROOT_VERIFIED
    )
    canonical_dataset.completion_proof_required_version = None
    canonical_dataset.completion_proof_sha256 = None
    exhaustive_catalog = (
        await catalog_outcomes.enrich_provider_directory_source_catalog(
            catalog_map
        )
    )
    assert "validated_publication_candidate" not in (
        exhaustive_catalog["items"][0]
    )


@pytest.mark.asyncio
async def test_locked_legacy_reviewed_activation_is_rechecked():
    executor = SimpleNamespace(scalar=AsyncMock(return_value=False))

    with pytest.raises(importer.ProviderDirectoryArtifactBuildStale):
        await importer._assert_reviewed_activation_ready(
            _legacy_reviewed_publication_fence(),
            executor,
        )
    executor.scalar.assert_awaited_once()


@pytest.mark.parametrize("first_publication", (True, False))
def test_locked_reviewed_twin_first_and_replacement(first_publication):
    fence = _reviewed_publication_fence(
        root_count=2,
        first_publication=first_publication,
    )
    candidate_row_map = _reviewed_locked_candidate_row(
        first_publication=first_publication,
        content_proof_admission_sha256=None,
        generic_admission_sealed=False,
        artifact_selection_receipt_present=False,
    )
    locked_dataset_rows = [candidate_row_map]
    if not first_publication:
        locked_dataset_rows.append(_locked_incumbent_row_map())

    importer._assert_locked_artifact_fence_datasets(
        fence,
        locked_dataset_rows,
        {ENDPOINT_ID: [CANDIDATE_ID]},
    )


def test_locked_legacy_reviewed_twin_first_publication():
    importer._assert_locked_artifact_fence_datasets(
        _legacy_reviewed_publication_fence(),
        [
            _reviewed_locked_candidate_row(
                content_proof_admission_sha256=None,
                generic_admission_sealed=False,
                artifact_selection_receipt_present=False,
            )
        ],
        {ENDPOINT_ID: [CANDIDATE_ID]},
    )
