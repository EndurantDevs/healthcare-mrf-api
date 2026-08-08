# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Failure and serialization contracts for reviewed publication."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import json

import pytest

import process.formulary_fhir.reviewed_publication as publication_module
from process.formulary_fhir.manual_lock import ManualSourceLockError
from process.formulary_fhir.repository_admission import TwinAdmissionError
from process.formulary_fhir.repository_shared import PublicationResult
from process.formulary_fhir.reviewed_operation import ReviewedOperationError
from process.formulary_fhir.reviewed_source import ReviewedSourceError
from tests.test_formulary_fhir_reviewed_publication import _candidate
from tests.test_formulary_fhir_reviewed_publication import _set_gates
from tests.test_formulary_fhir_reviewed_publication import CUTOFF
from tests.test_formulary_fhir_reviewed_publication import PUBLISHED_AT


@pytest.mark.parametrize(
    "upstream_error,expected_code",
    [
        (ReviewedSourceError("busy"), "busy"),
        (TwinAdmissionError("missing"), "missing"),
        (TwinAdmissionError("evidence"), "publication"),
    ],
)
@pytest.mark.asyncio
async def test_publication_maps_domain_errors(
    monkeypatch,
    upstream_error,
    expected_code,
):
    _set_gates(monkeypatch, None, "true")

    @asynccontextmanager
    async def source_lease(*_args, **_kwargs):
        yield

    async def fail_transaction(_database, _identities):
        raise upstream_error

    monkeypatch.setattr(
        publication_module.manual_lock,
        "manual_source_lease",
        source_lease,
    )
    monkeypatch.setattr(
        publication_module,
        "_publish_transaction",
        fail_transaction,
    )
    with pytest.raises(ReviewedOperationError) as caught:
        await publication_module.publish_reviewed_candidate(cutoff=CUTOFF)
    assert caught.value.code == expected_code


@pytest.mark.asyncio
async def test_publication_maps_lock_and_private_failures(monkeypatch):
    _set_gates(monkeypatch, None, "true")

    @asynccontextmanager
    async def unavailable_lease(*_args, **_kwargs):
        raise ManualSourceLockError("busy")
        yield

    monkeypatch.setattr(
        publication_module.manual_lock,
        "manual_source_lease",
        unavailable_lease,
    )
    with pytest.raises(ReviewedOperationError) as caught:
        await publication_module.publish_reviewed_candidate(cutoff=CUTOFF)
    assert caught.value.code == "busy"

    @asynccontextmanager
    async def source_lease(*_args, **_kwargs):
        yield

    async def fail_transaction(_database, _identities):
        raise RuntimeError("https://private.invalid/fhir?token=secret")

    monkeypatch.setattr(
        publication_module.manual_lock,
        "manual_source_lease",
        source_lease,
    )
    monkeypatch.setattr(
        publication_module,
        "_publish_transaction",
        fail_transaction,
    )
    with pytest.raises(ReviewedOperationError) as caught:
        await publication_module.publish_reviewed_candidate(cutoff=CUTOFF)
    assert caught.value.code == "publication"
    assert "private" not in str(caught.value)


@pytest.mark.asyncio
async def test_publication_preserves_timeout_and_cancellation(monkeypatch):
    _set_gates(monkeypatch, None, "true")
    lease_exits: list[str] = []

    @asynccontextmanager
    async def source_lease(*_args, **_kwargs):
        try:
            yield
        finally:
            lease_exits.append("exit")

    async def blocked_transaction(_database, _identities):
        await asyncio.Event().wait()

    monkeypatch.setattr(
        publication_module.manual_lock,
        "manual_source_lease",
        source_lease,
    )
    monkeypatch.setattr(
        publication_module,
        "_publish_transaction",
        blocked_transaction,
    )
    monkeypatch.setattr(publication_module, "PUBLICATION_TIMEOUT_SECONDS", 0.01)
    with pytest.raises(TimeoutError):
        await publication_module.publish_reviewed_candidate(cutoff=CUTOFF)
    assert lease_exits == ["exit"]

    monkeypatch.setattr(publication_module, "PUBLICATION_TIMEOUT_SECONDS", 60)
    publication_task = asyncio.create_task(
        publication_module.publish_reviewed_candidate(cutoff=CUTOFF)
    )
    await asyncio.sleep(0)
    publication_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await publication_task
    assert lease_exits == ["exit", "exit"]


@pytest.mark.asyncio
async def test_publication_preserves_reviewed_operation_error(monkeypatch):
    _set_gates(monkeypatch, None, "true")
    domain_error = ReviewedOperationError("evidence")

    @asynccontextmanager
    async def source_lease(*_args, **_kwargs):
        yield

    async def fail_transaction(_database, _identities):
        raise domain_error

    monkeypatch.setattr(
        publication_module.manual_lock,
        "manual_source_lease",
        source_lease,
    )
    monkeypatch.setattr(
        publication_module,
        "_publish_transaction",
        fail_transaction,
    )
    with pytest.raises(ReviewedOperationError) as caught:
        await publication_module.publish_reviewed_candidate(cutoff=CUTOFF)
    assert caught.value is domain_error


def _exact_publication_result():
    admission = _candidate()[0]
    repository_result = PublicationResult(
        admission.source_id,
        admission.candidate_dataset_id,
        7,
        PUBLISHED_AT,
    )
    publication_result = publication_module._publication_result(
        admission,
        repository_result,
    )
    return admission, publication_result


def test_publication_json_is_exact_and_rejects_other_types():
    """Serialize every bounded field and reject non-result objects."""

    admission, publication_result = _exact_publication_result()
    rendered_by_field = json.loads(
        publication_module.publication_result_json(publication_result)
    )

    assert rendered_by_field == {
        "status": "published",
        "candidate_dataset_id": admission.candidate_dataset_id,
        "predecessor_dataset_id": None,
        "cutoff": "2026-08-07T12:34:56Z",
        "generation": 7,
        "published_at": "2026-08-07T12:38:56Z",
        "source_configuration_hash": "a" * 64,
        "acquisition_contract_hash": "b" * 64,
        "list_count": 2,
        "alias_count": 3,
        "medication_count": 5,
        "coverage_hash": "c" * 64,
        "membership_hash": "d" * 64,
        "alternative_count": 1,
        "alternative_hash": "e" * 64,
        "admitted_at": "2026-08-07T12:37:56Z",
    }
    with pytest.raises(ReviewedOperationError) as caught:
        publication_module.publication_result_json(object())
    assert caught.value.code == "evidence"


def test_publication_result_requires_exact_repository_evidence():
    admission, _publication_result = _exact_publication_result()
    invalid_publications = (
        object(),
        PublicationResult(
            "source-neutral",
            admission.candidate_dataset_id,
            7,
            PUBLISHED_AT,
        ),
        PublicationResult(
            admission.source_id,
            "ffd_" + ("0" * 48),
            7,
            PUBLISHED_AT,
        ),
        PublicationResult(
            admission.source_id,
            admission.candidate_dataset_id,
            0,
            PUBLISHED_AT,
        ),
    )
    for invalid_publication in invalid_publications:
        with pytest.raises(ReviewedOperationError) as caught:
            publication_module._publication_result(
                admission,
                invalid_publication,
            )
        assert caught.value.code == "evidence"


@pytest.mark.parametrize(
    "evidence_name,field_name,invalid_value",
    [
        ("verification", "list_count", 0),
        ("alternative", "count", -1),
    ],
)
def test_publication_result_rejects_tampered_admission_evidence(
    evidence_name,
    field_name,
    invalid_value,
):
    admission = _candidate()[0]
    evidence = getattr(admission, evidence_name)
    object.__setattr__(evidence, field_name, invalid_value)
    repository_result = PublicationResult(
        admission.source_id,
        admission.candidate_dataset_id,
        7,
        PUBLISHED_AT,
    )

    with pytest.raises(ReviewedOperationError) as caught:
        publication_module._publication_result(admission, repository_result)

    assert caught.value.code == "evidence"


@pytest.mark.parametrize(
    "field_name,invalid_value",
    [("list_count", 0), ("alternative_count", -1)],
)
def test_publication_json_revalidates_tampered_result(
    field_name,
    invalid_value,
):
    _admission, publication_result = _exact_publication_result()
    object.__setattr__(publication_result, field_name, invalid_value)

    with pytest.raises(ReviewedOperationError) as caught:
        publication_module.publication_result_json(publication_result)

    assert caught.value.code == "evidence"
