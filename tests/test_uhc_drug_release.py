# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Unit contracts for receipt-only UHC admitted-candidate publication."""

from __future__ import annotations

from contextlib import asynccontextmanager
import datetime as dt
from unittest.mock import ANY
from unittest.mock import AsyncMock

import pytest

import process.formulary_fhir.uhc_drug_release as release
from process.formulary_fhir.repository_shared import DatasetRef
from process.formulary_fhir.repository_shared import PublicationResult
from process.formulary_fhir.uhc_drug_receipt import UHCDrugPublicationInputs
from tests.uhc_drug_receipt_test_support import ADMITTED_AT
from tests.uhc_drug_receipt_test_support import admission_receipt
from tests.uhc_drug_receipt_test_support import admitted_twin
from tests.uhc_drug_receipt_test_support import source_binding


PUBLISHED_AT = ADMITTED_AT + dt.timedelta(minutes=1)


def _publication_inputs(*, selected_file_count: int = 48):
    twin_result, artifacts = admitted_twin(
        selected_file_count=selected_file_count
    )
    receipt = admission_receipt(twin_result)
    admission = receipt.admission
    candidate = DatasetRef(
        admission.source_id,
        admission.candidate_dataset_id,
        admission.candidate_run_id,
        admission.predecessor_dataset_id,
        admission.cutoff_at,
        admission.acquisition_contract_hash,
        "requested",
        "verified",
    )
    return UHCDrugPublicationInputs(
        receipt,
        source_binding(),
        artifacts,
        candidate,
    )


class _Repository:
    def __init__(self, publication: PublicationResult) -> None:
        self.publication = publication
        self.published_candidates: list[DatasetRef] = []
        self.seed_calls = 0

    async def publish_dataset(self, *, dataset):
        self.published_candidates.append(dataset)
        return self.publication

    async def publish_verified_seed(self, *, dataset):
        self.seed_calls += 1
        raise AssertionError(f"seed bypass called for {dataset.dataset_id}")


def _install_release_boundaries(monkeypatch, publication_inputs, lease_events):
    @asynccontextmanager
    async def source_lease(*_args, **_kwargs):
        lease_events.append("enter")
        yield
        lease_events.append("exit")

    monkeypatch.setattr(release.manual_lock, "manual_source_lease", source_lease)
    reconstruction = AsyncMock(return_value=publication_inputs)
    monkeypatch.setattr(
        release,
        "reconstruct_uhc_drug_publication_inputs",
        reconstruction,
    )
    source_fence = AsyncMock()
    monkeypatch.setattr(release, "require_source_unchanged", source_fence)
    return reconstruction, source_fence


@pytest.mark.asyncio
async def test_publication_reconstructs_receipt_under_fresh_lease(monkeypatch):
    """Only a durable receipt can select the admitted requested root."""

    publication_inputs = _publication_inputs()
    receipt = publication_inputs.receipt
    publication = PublicationResult(
        receipt.source_id,
        receipt.candidate_dataset_id,
        1,
        PUBLISHED_AT,
    )
    repository = _Repository(publication)
    lease_events: list[str] = []
    reconstruction, source_fence = _install_release_boundaries(
        monkeypatch,
        publication_inputs,
        lease_events,
    )

    observed = await release.publish_admitted_uhc_drug_candidate(
        receipt_id=receipt.receipt_id,
        database=object(),
        repository=repository,
    )

    assert observed is publication
    assert lease_events == ["enter", "exit"]
    reconstruction.assert_awaited_once_with(
        receipt_id=receipt.receipt_id,
        database=ANY,
    )
    source_fence.assert_awaited_once()
    assert repository.published_candidates == [publication_inputs.candidate]
    assert repository.seed_calls == 0


@pytest.mark.asyncio
async def test_publication_rejects_invalid_receipt_before_lease(monkeypatch):
    """Malformed caller input never reaches storage or pointer mutation."""

    lease = AsyncMock()
    monkeypatch.setattr(release.manual_lock, "manual_source_lease", lease)

    with pytest.raises(ValueError, match="receipt id is invalid"):
        await release.publish_admitted_uhc_drug_candidate(
            receipt_id="not-a-receipt",
            database=object(),
            repository=object(),
        )
    lease.assert_not_called()


@pytest.mark.asyncio
async def test_publication_blocks_reconstruction_or_artifact_failure(monkeypatch):
    """Missing receipt, CAS corruption, or contract drift makes zero writes."""

    publication_inputs = _publication_inputs()
    receipt = publication_inputs.receipt
    repository = _Repository(
        PublicationResult(
            receipt.source_id,
            receipt.candidate_dataset_id,
            1,
            PUBLISHED_AT,
        )
    )
    lease_events: list[str] = []
    _reconstruction, _source_fence = _install_release_boundaries(
        monkeypatch,
        publication_inputs,
        lease_events,
    )
    monkeypatch.setattr(
        release,
        "reconstruct_uhc_drug_publication_inputs",
        AsyncMock(side_effect=RuntimeError("retained blob digest mismatch")),
    )

    with pytest.raises(RuntimeError, match="retained blob digest mismatch"):
        await release.publish_admitted_uhc_drug_candidate(
            receipt_id=receipt.receipt_id,
            database=object(),
            repository=repository,
        )
    assert repository.published_candidates == []


@pytest.mark.asyncio
async def test_publication_rejects_repository_result_drift(monkeypatch):
    """The repository must echo the exact receipt-owned candidate."""

    publication_inputs = _publication_inputs()
    receipt = publication_inputs.receipt
    repository = _Repository(
        PublicationResult(
            receipt.source_id,
            "ffd_" + "9" * 48,
            1,
            PUBLISHED_AT,
        )
    )
    _install_release_boundaries(monkeypatch, publication_inputs, [])

    with pytest.raises(RuntimeError, match="result is inconsistent"):
        await release.publish_admitted_uhc_drug_candidate(
            receipt_id=receipt.receipt_id,
            database=object(),
            repository=repository,
        )


@pytest.mark.asyncio
async def test_publication_replay_returns_same_generation(monkeypatch):
    """An exact post-commit retry reuses the stored publication result."""

    publication_inputs = _publication_inputs()
    receipt = publication_inputs.receipt
    publication = PublicationResult(
        receipt.source_id,
        receipt.candidate_dataset_id,
        1,
        PUBLISHED_AT,
    )
    repository = _Repository(publication)
    lease_events: list[str] = []
    _install_release_boundaries(monkeypatch, publication_inputs, lease_events)

    first = await release.publish_admitted_uhc_drug_candidate(
        receipt_id=receipt.receipt_id,
        database=object(),
        repository=repository,
    )
    second = await release.publish_admitted_uhc_drug_candidate(
        receipt_id=receipt.receipt_id,
        database=object(),
        repository=repository,
    )

    assert first == second == publication
    assert publication.generation == 1
    assert lease_events == ["enter", "exit", "enter", "exit"]
    assert repository.published_candidates == [
        publication_inputs.candidate,
        publication_inputs.candidate,
    ]


@pytest.mark.asyncio
async def test_partial_receipt_publishes_only_its_selected_candidate(monkeypatch):
    """Receipt-only publication treats a reproduced partial root like any admission."""

    publication_inputs = _publication_inputs(selected_file_count=1)
    receipt = publication_inputs.receipt
    publication = PublicationResult(
        receipt.source_id,
        receipt.candidate_dataset_id,
        1,
        PUBLISHED_AT,
    )
    repository = _Repository(publication)
    _install_release_boundaries(monkeypatch, publication_inputs, [])

    observed = await release.publish_admitted_uhc_drug_candidate(
        receipt_id=receipt.receipt_id,
        database=object(),
        repository=repository,
    )

    assert receipt.expected_file_count == 48
    assert receipt.excluded_file_count == 47
    assert receipt.is_coverage_complete is False
    assert observed == publication
    assert repository.published_candidates == [publication_inputs.candidate]
