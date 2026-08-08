# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Unit and SQL contracts for immutable formulary twin admission."""

from __future__ import annotations

import datetime as dt
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.formulary_fhir import repository_admission
from process.formulary_fhir.repository_admission import admit_verified_twins
from process.formulary_fhir.repository_admission import TwinAdmissionError
from process.formulary_fhir.repository_admission import verify_twin_admission_for_publication
from process.formulary_fhir.repository_admission_proof import DatasetEvidence
from process.formulary_fhir.repository_admission_types import AlternativeProof
from process.formulary_fhir.repository_admission_types import result_from_row
from process.formulary_fhir.repository_shared import DatasetRef
from process.formulary_fhir.repository_shared import DatasetVerification
from process.formulary_fhir.source import EnabledSourceBinding
from process.formulary_fhir.types import enabled_source_config


CUTOFF = dt.datetime(2026, 8, 8, 1, tzinfo=dt.UTC)
BASELINE_VERIFIED = CUTOFF + dt.timedelta(minutes=1)
CANDIDATE_VERIFIED = CUTOFF + dt.timedelta(minutes=2)
ADMITTED = CUTOFF + dt.timedelta(minutes=3)
RUNTIME_CONFIG = {
    "timeout_seconds": 30,
    "max_attempts": 2,
    "page_size": 50,
    "max_pages": 100,
    "max_total_resources": 5_000,
    "max_response_bytes": 1_048_576,
}


def _dataset(identity: str, *, intent: str, status: str = "verified") -> DatasetRef:
    return DatasetRef(
        "source-a",
        "ffd_" + identity * 48,
        "run-" + identity,
        None,
        CUTOFF,
        "c" * 64,
        intent,
        status,
    )


def _baseline() -> DatasetRef:
    return _dataset("a", intent="none")


def _candidate(*, status: str = "verified") -> DatasetRef:
    return _dataset("b", intent="requested", status=status)


def _binding(configuration_hash: str = "9" * 64) -> EnabledSourceBinding:
    config = enabled_source_config(
        canonical_base="https://synthetic.invalid/fhir",
        enabled=True,
        runtime_config_json=RUNTIME_CONFIG,
    )
    return EnabledSourceBinding("source-a", config, configuration_hash)


def _verification(dataset_id: str) -> DatasetVerification:
    return DatasetVerification(
        "source-a",
        dataset_id,
        1,
        1,
        2,
        "d" * 64,
        "e" * 64,
    )


def _evidence(dataset: DatasetRef, verified_at: dt.datetime) -> DatasetEvidence:
    return DatasetEvidence(
        {"dataset_id": dataset.dataset_id, "status": dataset.status},
        _verification(dataset.dataset_id),
        AlternativeProof(1, "f" * 64),
        verified_at,
    )


def _admission_row(**overrides) -> dict[str, object]:
    values_by_field: dict[str, object] = {
        "source_id": "source-a",
        "baseline_dataset_id": _baseline().dataset_id,
        "baseline_run_id": _baseline().run_id,
        "candidate_dataset_id": _candidate().dataset_id,
        "candidate_run_id": _candidate().run_id,
        "predecessor_dataset_id": None,
        "cutoff_at": CUTOFF,
        "source_configuration_hash": _binding().configuration_hash,
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
        "admitted_at": ADMITTED,
    }
    values_by_field.update(overrides)
    return values_by_field


def _admission_result():
    return result_from_row(_admission_row())


def test_admission_contracts_validate_and_redact_hashes():
    result = _admission_result()

    assert result.verification.dataset_id == _candidate().dataset_id
    assert result.alternative.count == 1
    assert result.source_configuration_hash not in repr(result)
    assert result.acquisition_contract_hash not in repr(result)

    with pytest.raises(ValueError, match="proof count"):
        AlternativeProof(True, "f" * 64)
    with pytest.raises(ValueError, match="proof hash"):
        AlternativeProof(1, "invalid")
    with pytest.raises(ValueError, match="timestamps"):
        result_from_row(
            _admission_row(baseline_verified_at=CANDIDATE_VERIFIED)
            | {"candidate_verified_at": BASELINE_VERIFIED}
        )
    with pytest.raises(ValueError, match="verification"):
        result_from_row(_admission_row(list_count=0))


@pytest.mark.parametrize(
    "baseline,candidate",
    [
        (_baseline(), replace(_candidate(), source_id="source-b")),
        (_baseline(), replace(_candidate(), dataset_id=_baseline().dataset_id)),
        (_baseline(), replace(_candidate(), run_id=_baseline().run_id)),
        (_baseline(), replace(_candidate(), cutoff_at=CUTOFF + dt.timedelta(seconds=1))),
        (_baseline(), replace(_candidate(), acquisition_contract_hash="0" * 64)),
        (replace(_baseline(), intent="requested"), _candidate()),
        (_baseline(), replace(_candidate(), intent="none")),
        (replace(_baseline(), status="building"), _candidate()),
    ],
)
def test_pair_request_rejects_role_identity_and_contract_drift(baseline, candidate):
    with pytest.raises(TwinAdmissionError) as admission_error:
        repository_admission._validate_pair(
            "source-a",
            baseline,
            candidate,
            candidate_statuses={"verified"},
        )

    assert admission_error.value.code == "invalid_request"


@pytest.mark.asyncio
async def test_admission_read_uses_bound_identity_and_sanitizes_invalid_rows():
    database = SimpleNamespace(first=AsyncMock(return_value=_admission_row()))

    admission_result = await repository_admission._read_admission(
        database,
        "source-a",
        _candidate().dataset_id,
    )

    assert admission_result == _admission_result()
    statement = database.first.await_args.args[0]
    assert "fhir_formulary_twin_admission" in statement
    assert ":candidate_dataset_id" in statement
    assert _candidate().dataset_id not in statement
    assert database.first.await_args.kwargs == {
        "source_id": "source-a",
        "candidate_dataset_id": _candidate().dataset_id,
    }

    database.first.return_value = _admission_row(alternative_hash="private-value")
    with pytest.raises(TwinAdmissionError) as invalid_error:
        await repository_admission._read_admission(
            database,
            "source-a",
            _candidate().dataset_id,
        )
    assert invalid_error.value.code == "admission"
    assert "private-value" not in str(invalid_error.value)

    database.first.side_effect = RuntimeError("private database address")
    with pytest.raises(TwinAdmissionError) as storage_error:
        await repository_admission._read_admission(
            database,
            "source-a",
            _candidate().dataset_id,
        )
    assert storage_error.value.code == "storage"
    assert "private database address" not in str(storage_error.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("inserted_count", [0, 1])
async def test_admission_insert_is_bound_and_idempotent(inserted_count):
    database = SimpleNamespace(status=AsyncMock(return_value=inserted_count))

    await repository_admission._insert_admission(
        database,
        _binding().configuration_hash,
        _baseline(),
        _candidate(),
        _evidence(_baseline(), BASELINE_VERIFIED),
        _evidence(_candidate(), CANDIDATE_VERIFIED),
    )

    statement = database.status.await_args.args[0]
    parameters = database.status.await_args.kwargs
    assert statement.startswith('INSERT INTO "mrf"."fhir_formulary_twin_admission"')
    assert "ON CONFLICT DO NOTHING" in statement
    assert "UPDATE" not in statement and "DELETE" not in statement
    assert parameters["source_configuration_hash"] == _binding().configuration_hash
    assert parameters["alternative_hash"] == "f" * 64
    assert _candidate().dataset_id not in statement


@pytest.mark.asyncio
async def test_admission_insert_sanitizes_driver_failure_and_bad_count():
    arguments = (
        SimpleNamespace(status=AsyncMock(return_value=2)),
        _binding().configuration_hash,
        _baseline(),
        _candidate(),
        _evidence(_baseline(), BASELINE_VERIFIED),
        _evidence(_candidate(), CANDIDATE_VERIFIED),
    )
    with pytest.raises(TwinAdmissionError, match="storage failed"):
        await repository_admission._insert_admission(*arguments)

    secret_database = SimpleNamespace(
        status=AsyncMock(side_effect=RuntimeError("private SQL payload"))
    )
    with pytest.raises(TwinAdmissionError) as secret_error:
        await repository_admission._insert_admission(
            secret_database,
            *arguments[1:],
        )
    assert "private SQL payload" not in str(secret_error.value)
    assert secret_error.value.__cause__ is None


@pytest.mark.asyncio
async def test_publication_verifier_is_transaction_neutral_and_returns_locked_row(
    monkeypatch,
):
    admission_result = _admission_result()
    baseline_evidence = _evidence(_baseline(), BASELINE_VERIFIED)
    candidate_evidence = _evidence(_candidate(), CANDIDATE_VERIFIED)
    database = SimpleNamespace()
    monkeypatch.setattr(
        repository_admission,
        "_read_admission",
        AsyncMock(return_value=admission_result),
    )
    monkeypatch.setattr(
        repository_admission,
        "_current_configuration_hash",
        AsyncMock(return_value=_binding().configuration_hash),
    )
    pair_lock = AsyncMock(return_value=(baseline_evidence, candidate_evidence))
    monkeypatch.setattr(repository_admission, "lock_pair_evidence", pair_lock)
    attempt_check = AsyncMock()
    monkeypatch.setattr(
        repository_admission,
        "require_exact_twin_attempt",
        attempt_check,
    )

    observed, candidate_by_field = await verify_twin_admission_for_publication(
        database,
        "source-a",
        _candidate(),
    )

    assert observed == admission_result
    assert candidate_by_field == candidate_evidence.row
    locked_baseline = pair_lock.await_args.args[1]
    assert locked_baseline.dataset_id == _baseline().dataset_id
    assert pair_lock.await_args.kwargs == {
        "candidate_statuses": {"verified", "published"}
    }
    attempt_check.assert_awaited_once()
    assert not hasattr(database, "transaction")


@pytest.mark.asyncio
async def test_publication_verifier_requires_exact_current_admission(monkeypatch):
    monkeypatch.setattr(
        repository_admission,
        "_read_admission",
        AsyncMock(return_value=None),
    )
    with pytest.raises(TwinAdmissionError) as missing_error:
        await verify_twin_admission_for_publication(
            object(),
            "source-a",
            _candidate(),
        )
    assert missing_error.value.code == "missing"

    monkeypatch.setattr(
        repository_admission,
        "_read_admission",
        AsyncMock(return_value=_admission_result()),
    )
    monkeypatch.setattr(
        repository_admission,
        "_current_configuration_hash",
        AsyncMock(return_value="0" * 64),
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
        "require_exact_twin_attempt",
        AsyncMock(),
    )
    with pytest.raises(TwinAdmissionError) as changed_error:
        await verify_twin_admission_for_publication(
            object(),
            "source-a",
            _candidate(status="published"),
        )
    assert changed_error.value.code == "admission"


@pytest.mark.asyncio
async def test_public_admission_sanitizes_unexpected_transaction_failure():
    class _Database:
        def transaction(self):
            raise RuntimeError("private connection")

    with pytest.raises(TwinAdmissionError) as admission_error:
        await admit_verified_twins(
            database=_Database(),
            binding=_binding(),
            baseline=_baseline(),
            candidate=_candidate(),
        )

    assert admission_error.value.code == "storage"
    assert "private connection" not in str(admission_error.value)
    assert admission_error.value.__cause__ is None
