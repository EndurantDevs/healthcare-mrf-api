# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Durable root-consumption tests for formulary twin attempts."""

from __future__ import annotations

import datetime as dt
from contextlib import AbstractAsyncContextManager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.formulary_fhir import repository_admission
from process.formulary_fhir import repository_admission_attempt
from process.formulary_fhir.repository_admission import admit_verified_twins
from process.formulary_fhir.repository_admission_attempt import persist_twin_attempt
from process.formulary_fhir.repository_admission_attempt import (
    require_exact_twin_attempt,
)
from process.formulary_fhir.repository_admission_proof import DatasetEvidence
from process.formulary_fhir.repository_admission_types import AlternativeProof
from process.formulary_fhir.repository_admission_types import TwinAdmissionError
from process.formulary_fhir.repository_admission_types import TwinAttemptResult
from process.formulary_fhir.repository_admission_types import attempt_from_row
from process.formulary_fhir.repository_admission_types import result_from_row
from process.formulary_fhir.repository_shared import DatasetRef
from process.formulary_fhir.repository_shared import DatasetVerification
from process.formulary_fhir.source import EnabledSourceBinding
from process.formulary_fhir.types import enabled_source_config


CUTOFF = dt.datetime(2026, 8, 8, 1, tzinfo=dt.UTC)
BASELINE_VERIFIED = CUTOFF + dt.timedelta(minutes=1)
CANDIDATE_VERIFIED = CUTOFF + dt.timedelta(minutes=2)
ATTEMPTED = CUTOFF + dt.timedelta(minutes=3)
RUNTIME_CONFIG = {
    "timeout_seconds": 30,
    "max_attempts": 2,
    "page_size": 50,
    "max_pages": 100,
    "max_total_resources": 5_000,
    "max_response_bytes": 1_048_576,
}


def _dataset(identity: str, intent: str) -> DatasetRef:
    return DatasetRef(
        "source-a",
        "ffd_" + identity * 48,
        "run-" + identity,
        None,
        CUTOFF,
        "c" * 64,
        intent,
        "verified",
    )


def _baseline() -> DatasetRef:
    return _dataset("a", "none")


def _candidate(identity: str = "b") -> DatasetRef:
    return _dataset(identity, "requested")


def _binding() -> EnabledSourceBinding:
    return EnabledSourceBinding(
        "source-a",
        enabled_source_config(
            canonical_base="https://synthetic.invalid/fhir",
            enabled=True,
            runtime_config_json=RUNTIME_CONFIG,
        ),
        "9" * 64,
    )


def _attempt_row(
    *,
    baseline_hash: str = "1" * 64,
    candidate_hash: str = "1" * 64,
    **overrides,
) -> dict[str, object]:
    values_by_field: dict[str, object] = {
        "source_id": "source-a",
        "baseline_dataset_id": _baseline().dataset_id,
        "baseline_run_id": _baseline().run_id,
        "candidate_dataset_id": _candidate().dataset_id,
        "candidate_run_id": _candidate().run_id,
        "cutoff_at": CUTOFF,
        "source_configuration_hash": "9" * 64,
        "acquisition_contract_hash": "c" * 64,
        "baseline_evidence_hash": baseline_hash,
        "candidate_evidence_hash": candidate_hash,
        "matched": baseline_hash == candidate_hash,
        "attempted_at": ATTEMPTED,
    }
    values_by_field.update(overrides)
    return values_by_field


def _admission_result():
    return result_from_row(
        {
            "source_id": "source-a",
            "baseline_dataset_id": _baseline().dataset_id,
            "baseline_run_id": _baseline().run_id,
            "candidate_dataset_id": _candidate().dataset_id,
            "candidate_run_id": _candidate().run_id,
            "predecessor_dataset_id": None,
            "cutoff_at": CUTOFF,
            "source_configuration_hash": "9" * 64,
            "acquisition_contract_hash": "c" * 64,
            "list_count": 1,
            "alias_count": 1,
            "medication_count": 2,
            "coverage_hash": "d" * 64,
            "membership_hash": "e" * 64,
            "alternative_count": 1,
            "alternative_hash": "f" * 64,
            "baseline_verified_at": BASELINE_VERIFIED,
            "candidate_verified_at": CANDIDATE_VERIFIED,
            "admitted_at": ATTEMPTED,
        }
    )


def _evidence(dataset: DatasetRef, verified_at: dt.datetime) -> DatasetEvidence:
    return DatasetEvidence(
        {"dataset_id": dataset.dataset_id, "status": "verified"},
        DatasetVerification(
            "source-a",
            dataset.dataset_id,
            1,
            1,
            2,
            "d" * 64,
            "e" * 64,
        ),
        AlternativeProof(1, "f" * 64),
        verified_at,
    )


class _Transaction(AbstractAsyncContextManager):
    def __init__(self, exits: list[type[BaseException] | None]) -> None:
        self.exits = exits

    async def __aenter__(self):
        return self

    async def __aexit__(self, error_type, _error, _traceback):
        self.exits.append(error_type)
        return False


def test_attempt_contract_validates_match_state_and_redacts_hashes():
    attempt = attempt_from_row(_attempt_row())

    assert attempt.matched is True
    assert attempt.source_configuration_hash not in repr(attempt)
    assert attempt.baseline_evidence_hash not in repr(attempt)

    with pytest.raises(ValueError, match="match state"):
        attempt_from_row(_attempt_row(matched=False))
    with pytest.raises(ValueError, match="identities"):
        attempt_from_row(
            _attempt_row(candidate_dataset_id=_baseline().dataset_id)
        )
    with pytest.raises(ValueError, match="evidence hash"):
        attempt_from_row(_attempt_row(baseline_hash="invalid"))


@pytest.mark.asyncio
async def test_attempt_lookup_checks_both_roots_in_both_roles():
    database = SimpleNamespace(all=AsyncMock(return_value=[_attempt_row()]))

    attempts = await repository_admission_attempt._root_attempts(
        database,
        _baseline().dataset_id,
        _candidate().dataset_id,
    )

    assert attempts == (attempt_from_row(_attempt_row()),)
    statement = database.all.await_args.args[0]
    assert statement.count("baseline_dataset_id") >= 2
    assert statement.count("candidate_dataset_id") >= 2
    assert ":baseline_dataset_id" in statement
    assert _baseline().dataset_id not in statement


@pytest.mark.asyncio
async def test_attempt_insert_then_exact_readback_is_idempotent():
    database = SimpleNamespace(
        all=AsyncMock(side_effect=[[], [_attempt_row()]]),
        status=AsyncMock(return_value=1),
    )

    inserted = await persist_twin_attempt(
        database,
        _baseline(),
        _candidate(),
        "9" * 64,
        "1" * 64,
        "1" * 64,
    )

    assert inserted == attempt_from_row(_attempt_row())
    statement = database.status.await_args.args[0]
    parameters = database.status.await_args.kwargs
    assert statement.startswith('INSERT INTO "mrf"."fhir_formulary_twin_attempt"')
    assert "ON CONFLICT DO NOTHING" in statement
    assert "UPDATE" not in statement and "DELETE" not in statement
    assert parameters["matched"] is True

    replay_database = SimpleNamespace(
        all=AsyncMock(return_value=[_attempt_row()]),
        status=AsyncMock(),
    )
    replayed = await persist_twin_attempt(
        replay_database,
        _baseline(),
        _candidate(),
        "9" * 64,
        "1" * 64,
        "1" * 64,
    )
    assert replayed == inserted
    replay_database.status.assert_not_awaited()


@pytest.mark.asyncio
async def test_mismatch_attempt_is_persisted_with_exact_false_state():
    mismatch_row = _attempt_row(candidate_hash="2" * 64)
    database = SimpleNamespace(
        all=AsyncMock(side_effect=[[], [mismatch_row]]),
        status=AsyncMock(return_value=1),
    )

    attempt = await persist_twin_attempt(
        database,
        _baseline(),
        _candidate(),
        "9" * 64,
        "1" * 64,
        "2" * 64,
    )

    assert attempt.matched is False
    assert database.status.await_args.kwargs["matched"] is False


@pytest.mark.asyncio
async def test_attempt_rejects_same_role_and_cross_role_root_reuse():
    reused_candidate = _candidate("c")
    same_role = _attempt_row(
        candidate_dataset_id=reused_candidate.dataset_id,
        candidate_run_id=reused_candidate.run_id,
    )
    same_role_database = SimpleNamespace(
        all=AsyncMock(return_value=[same_role]),
        status=AsyncMock(),
    )
    with pytest.raises(TwinAdmissionError) as same_role_error:
        await persist_twin_attempt(
            same_role_database,
            _baseline(),
            _candidate(),
            "9" * 64,
            "1" * 64,
            "1" * 64,
        )
    assert same_role_error.value.code == "attempt"
    same_role_database.status.assert_not_awaited()

    cross_role = _attempt_row(
        baseline_dataset_id=_candidate().dataset_id,
        baseline_run_id=_candidate().run_id,
        candidate_dataset_id=reused_candidate.dataset_id,
        candidate_run_id=reused_candidate.run_id,
    )
    cross_role_database = SimpleNamespace(
        all=AsyncMock(return_value=[cross_role]),
        status=AsyncMock(),
    )
    with pytest.raises(TwinAdmissionError) as cross_role_error:
        await persist_twin_attempt(
            cross_role_database,
            _baseline(),
            _candidate(),
            "9" * 64,
            "1" * 64,
            "1" * 64,
        )
    assert cross_role_error.value.code == "attempt"
    cross_role_database.status.assert_not_awaited()


@pytest.mark.asyncio
async def test_exact_attempt_requirement_rejects_missing_and_mismatch():
    missing_database = SimpleNamespace(all=AsyncMock(return_value=[]))
    with pytest.raises(TwinAdmissionError, match="attempt evidence"):
        await require_exact_twin_attempt(
            missing_database,
            _baseline(),
            _candidate(),
            "9" * 64,
            "1" * 64,
            "1" * 64,
        )

    mismatch_database = SimpleNamespace(
        all=AsyncMock(return_value=[_attempt_row(candidate_hash="2" * 64)])
    )
    with pytest.raises(TwinAdmissionError, match="attempt evidence"):
        await require_exact_twin_attempt(
            mismatch_database,
            _baseline(),
            _candidate(),
            "9" * 64,
            "1" * 64,
            "2" * 64,
        )


@pytest.mark.asyncio
async def test_mismatch_commits_attempt_before_public_error(monkeypatch):
    transaction_exits: list[type[BaseException] | None] = []
    database = SimpleNamespace(
        transaction=lambda: _Transaction(transaction_exits),
    )
    monkeypatch.setattr(repository_admission, "lock_source", AsyncMock())
    monkeypatch.setattr(
        repository_admission,
        "_current_configuration_hash",
        AsyncMock(return_value="9" * 64),
    )
    monkeypatch.setattr(
        repository_admission,
        "lock_pair_evidence",
        AsyncMock(
            return_value=(
                _evidence(_baseline(), BASELINE_VERIFIED),
                _evidence(_candidate(), CANDIDATE_VERIFIED),
            )
        ),
    )
    monkeypatch.setattr(
        repository_admission,
        "_locked_predecessor",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        repository_admission,
        "dataset_evidence_hash",
        Mock(side_effect=["1" * 64, "2" * 64]),
    )
    mismatch_attempt = TwinAttemptResult(
        **{
            **_attempt_row(candidate_hash="2" * 64),
        }
    )
    monkeypatch.setattr(
        repository_admission,
        "persist_twin_attempt",
        AsyncMock(return_value=mismatch_attempt),
    )
    admission_insert = AsyncMock()
    monkeypatch.setattr(
        repository_admission,
        "_persist_exact_admission",
        admission_insert,
    )

    with pytest.raises(TwinAdmissionError) as mismatch_error:
        await admit_verified_twins(
            database=database,
            binding=_binding(),
            baseline=_baseline(),
            candidate=_candidate(),
        )

    assert mismatch_error.value.code == "mismatch"
    assert transaction_exits == [None]
    admission_insert.assert_not_awaited()


@pytest.mark.asyncio
async def test_matched_attempt_and_admission_share_one_transaction(monkeypatch):
    transaction_exits: list[type[BaseException] | None] = []
    database = SimpleNamespace(
        transaction=lambda: _Transaction(transaction_exits),
    )
    monkeypatch.setattr(repository_admission, "lock_source", AsyncMock())
    monkeypatch.setattr(
        repository_admission,
        "_current_configuration_hash",
        AsyncMock(return_value="9" * 64),
    )
    monkeypatch.setattr(
        repository_admission,
        "lock_pair_evidence",
        AsyncMock(
            return_value=(
                _evidence(_baseline(), BASELINE_VERIFIED),
                _evidence(_candidate(), CANDIDATE_VERIFIED),
            )
        ),
    )
    monkeypatch.setattr(
        repository_admission,
        "_locked_predecessor",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        repository_admission,
        "dataset_evidence_hash",
        Mock(side_effect=["1" * 64, "1" * 64]),
    )
    monkeypatch.setattr(
        repository_admission,
        "persist_twin_attempt",
        AsyncMock(return_value=attempt_from_row(_attempt_row())),
    )
    monkeypatch.setattr(
        repository_admission,
        "_persist_exact_admission",
        AsyncMock(return_value=_admission_result()),
    )

    admission = await admit_verified_twins(
        database=database,
        binding=_binding(),
        baseline=_baseline(),
        candidate=_candidate(),
    )

    assert admission == _admission_result()
    assert transaction_exits == [None]
