# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Failure-path coverage for recomputed formulary twin evidence."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock, Mock

import pytest

from process.formulary_fhir import repository_admission_proof
from process.formulary_fhir.repository_admission_proof import dataset_evidence_hash
from process.formulary_fhir.repository_admission_proof import require_admissible_pair
from process.formulary_fhir.repository_admission_types import AlternativeProof
from process.formulary_fhir.repository_admission_types import TwinAdmissionError
from tests.test_formulary_fhir_repository_admission_proof import _dataset
from tests.test_formulary_fhir_repository_admission_proof import _evidence
from tests.test_formulary_fhir_repository_admission_proof import _verification
from tests.test_formulary_fhir_repository_admission_proof import BASELINE_VERIFIED
from tests.test_formulary_fhir_repository_admission_proof import CANDIDATE_VERIFIED


@pytest.mark.asyncio
async def test_lifecycle_read_requires_exact_healthy_verification_timestamp():
    dataset = _dataset("a")
    exact_by_field = {
        "source_id": dataset.source_id,
        "dataset_id": dataset.dataset_id,
        "verified_at": BASELINE_VERIFIED,
        "failed_at": None,
        "error_json": None,
    }
    database = SimpleNamespace(first=AsyncMock(return_value=exact_by_field))

    observed = await repository_admission_proof._lifecycle_row(
        database,
        dataset.source_id,
        dataset.dataset_id,
    )

    assert observed["verified_at"] == BASELINE_VERIFIED
    assert ":dataset_id" in database.first.await_args.args[0]

    for invalid_row in (
        None,
        {**exact_by_field, "source_id": "source-b"},
        {**exact_by_field, "failed_at": BASELINE_VERIFIED},
        {**exact_by_field, "error_json": {"private": True}},
        {**exact_by_field, "verified_at": None},
    ):
        database.first.return_value = invalid_row
        with pytest.raises(TwinAdmissionError, match="content evidence"):
            await repository_admission_proof._lifecycle_row(
                database,
                dataset.source_id,
                dataset.dataset_id,
            )


@pytest.mark.asyncio
async def test_dataset_evidence_recomputes_every_proof(monkeypatch):
    dataset = _dataset("a")
    verification = _verification(dataset.dataset_id)
    lifecycle_by_field = {
        "source_id": dataset.source_id,
        "dataset_id": dataset.dataset_id,
        "verified_at": BASELINE_VERIFIED,
        "failed_at": None,
        "error_json": None,
    }
    dataset_by_field = {
        "list_count": verification.list_count,
        "alias_count": verification.alias_count,
        "medication_count": verification.medication_membership_count,
        "coverage_hash": verification.coverage_hash,
        "membership_hash": verification.membership_hash,
    }
    monkeypatch.setattr(
        repository_admission_proof,
        "_lifecycle_row",
        AsyncMock(return_value=lifecycle_by_field),
    )
    monkeypatch.setattr(
        repository_admission_proof,
        "_recompute_dataset_verification",
        AsyncMock(return_value=verification),
    )
    full_checkpoints = AsyncMock()
    monkeypatch.setattr(
        repository_admission_proof,
        "require_full_checkpoints",
        full_checkpoints,
    )
    alternative_proof = AlternativeProof(1, "f" * 64)
    monkeypatch.setattr(
        repository_admission_proof,
        "recompute_alternative_proof",
        AsyncMock(return_value=alternative_proof),
    )

    evidence = await repository_admission_proof._dataset_evidence(
        object(),
        dataset,
        dataset_by_field,
    )

    assert evidence.verification == verification
    assert evidence.alternative == alternative_proof
    full_checkpoints.assert_awaited_once_with(
        ANY,
        dataset,
        verification.alias_count,
    )


@pytest.mark.asyncio
async def test_dataset_evidence_rejects_stored_drift_and_unknown_failure(monkeypatch):
    dataset = _dataset("a")
    verification = _verification(dataset.dataset_id)
    monkeypatch.setattr(
        repository_admission_proof,
        "_lifecycle_row",
        AsyncMock(return_value={"verified_at": BASELINE_VERIFIED}),
    )
    monkeypatch.setattr(
        repository_admission_proof,
        "_recompute_dataset_verification",
        AsyncMock(return_value=verification),
    )
    with pytest.raises(TwinAdmissionError, match="content evidence"):
        await repository_admission_proof._dataset_evidence(
            object(),
            dataset,
            {"list_count": 0},
        )

    repository_admission_proof._recompute_dataset_verification.side_effect = (
        RuntimeError("private row detail")
    )
    with pytest.raises(TwinAdmissionError) as evidence_error:
        await repository_admission_proof._dataset_evidence(
            object(),
            dataset,
            {},
        )
    assert "private row" not in str(evidence_error.value)


@pytest.mark.asyncio
async def test_pair_lock_sanitizes_dataset_lock_failure(monkeypatch):
    monkeypatch.setattr(
        repository_admission_proof,
        "lock_dataset",
        AsyncMock(side_effect=RuntimeError("private dataset id")),
    )

    with pytest.raises(TwinAdmissionError) as evidence_error:
        await repository_admission_proof.lock_pair_evidence(
            object(),
            _dataset("a"),
            _dataset("b"),
            candidate_statuses={"verified"},
        )

    assert evidence_error.value.code == "evidence"
    assert "private dataset" not in str(evidence_error.value)

    bounded_error = TwinAdmissionError("evidence")
    repository_admission_proof.lock_dataset.side_effect = bounded_error
    with pytest.raises(TwinAdmissionError) as bounded_result:
        await repository_admission_proof.lock_pair_evidence(
            object(),
            _dataset("a"),
            _dataset("b"),
            candidate_statuses={"verified"},
        )
    assert bounded_result.value is bounded_error


@pytest.mark.asyncio
async def test_bounded_evidence_errors_are_preserved_without_rewrapping(monkeypatch):
    bounded_error = TwinAdmissionError("evidence")
    monkeypatch.setattr(
        repository_admission_proof,
        "_alternative_record",
        Mock(side_effect=bounded_error),
    )
    database = SimpleNamespace(all=AsyncMock(return_value=[{"private": True}]))
    with pytest.raises(TwinAdmissionError) as alternative_error:
        await repository_admission_proof.recompute_alternative_proof(
            database,
            "source-a",
            _dataset("a").dataset_id,
        )
    assert alternative_error.value is bounded_error

    monkeypatch.setattr(
        repository_admission_proof,
        "json_text",
        Mock(side_effect=bounded_error),
    )
    with pytest.raises(TwinAdmissionError) as hash_error:
        dataset_evidence_hash(
            _dataset("a"),
            "9" * 64,
            _evidence(_dataset("a"), BASELINE_VERIFIED),
        )
    assert hash_error.value is bounded_error


def test_admissible_pair_rejects_non_evidence_objects():
    with pytest.raises(TwinAdmissionError) as evidence_error:
        require_admissible_pair(
            object(),
            _evidence(_dataset("b"), CANDIDATE_VERIFIED),
        )

    assert evidence_error.value.code == "evidence"


def test_admission_result_validates_non_null_predecessor():
    from tests.test_formulary_fhir_repository_admission import _admission_row
    from process.formulary_fhir.repository_admission_types import result_from_row

    predecessor = "ffd_" + "p" * 48
    result = result_from_row(_admission_row(predecessor_dataset_id=predecessor))

    assert result.predecessor_dataset_id == predecessor
