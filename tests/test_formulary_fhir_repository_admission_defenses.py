# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Failure-path proof for formulary twin admission orchestration."""

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.formulary_fhir import repository_admission
from process.formulary_fhir.repository_admission import admit_verified_twins
from process.formulary_fhir.repository_admission import TwinAdmissionError
from process.formulary_fhir.repository_admission import verify_twin_admission_for_publication
from tests.test_formulary_fhir_repository_admission import _admission_result
from tests.test_formulary_fhir_repository_admission import _baseline
from tests.test_formulary_fhir_repository_admission import _binding
from tests.test_formulary_fhir_repository_admission import _candidate
from tests.test_formulary_fhir_repository_admission import _evidence
from tests.test_formulary_fhir_repository_admission import BASELINE_VERIFIED
from tests.test_formulary_fhir_repository_admission import CANDIDATE_VERIFIED


@asynccontextmanager
async def _transaction():
    yield


@pytest.mark.asyncio
async def test_current_pointer_lock_handles_empty_exact_and_invalid_rows():
    database = SimpleNamespace(
        first=AsyncMock(
            side_effect=[
                None,
                {"dataset_id": _candidate().dataset_id},
                {"dataset_id": "private\nvalue"},
            ]
        )
    )

    assert await repository_admission._locked_predecessor(database, "source-a") is None
    assert (
        await repository_admission._locked_predecessor(database, "source-a")
        == _candidate().dataset_id
    )
    with pytest.raises(TwinAdmissionError) as pointer_error:
        await repository_admission._locked_predecessor(database, "source-a")
    assert pointer_error.value.code == "pointer"
    assert "private" not in str(pointer_error.value)
    assert "FOR UPDATE" in database.first.await_args.args[0]


@pytest.mark.asyncio
async def test_source_configuration_hash_is_exact_and_sanitized(monkeypatch):
    monkeypatch.setattr(
        repository_admission,
        "load_enabled_source",
        AsyncMock(return_value=_binding()),
    )
    assert await repository_admission._current_configuration_hash(
        object(),
        "source-a",
    ) == _binding().configuration_hash

    repository_admission.load_enabled_source.side_effect = RuntimeError(
        "private source configuration"
    )
    with pytest.raises(TwinAdmissionError) as source_error:
        await repository_admission._current_configuration_hash(object(), "source-a")
    assert source_error.value.code == "source"
    assert "private source" not in str(source_error.value)


@pytest.mark.asyncio
async def test_exact_admission_replays_or_inserts_once(monkeypatch):
    baseline_evidence = _evidence(_baseline(), BASELINE_VERIFIED)
    candidate_evidence = _evidence(_candidate(), CANDIDATE_VERIFIED)
    existing = _admission_result()
    readback = AsyncMock(return_value=existing)
    insert = AsyncMock()
    monkeypatch.setattr(repository_admission, "_read_admission", readback)
    monkeypatch.setattr(repository_admission, "_insert_admission", insert)

    replayed = await repository_admission._persist_exact_admission(
        object(),
        _binding().configuration_hash,
        _baseline(),
        _candidate(),
        baseline_evidence,
        candidate_evidence,
    )
    assert replayed == existing
    insert.assert_not_awaited()

    readback.side_effect = [None, existing]
    inserted = await repository_admission._persist_exact_admission(
        object(),
        _binding().configuration_hash,
        _baseline(),
        _candidate(),
        baseline_evidence,
        candidate_evidence,
    )
    assert inserted == existing
    insert.assert_awaited_once()


@pytest.mark.asyncio
async def test_exact_admission_rejects_missing_or_drifted_readback(monkeypatch):
    baseline_evidence = _evidence(_baseline(), BASELINE_VERIFIED)
    candidate_evidence = _evidence(_candidate(), CANDIDATE_VERIFIED)
    monkeypatch.setattr(
        repository_admission,
        "_read_admission",
        AsyncMock(side_effect=[None, None]),
    )
    monkeypatch.setattr(repository_admission, "_insert_admission", AsyncMock())

    with pytest.raises(TwinAdmissionError) as missing_error:
        await repository_admission._persist_exact_admission(
            object(),
            _binding().configuration_hash,
            _baseline(),
            _candidate(),
            baseline_evidence,
            candidate_evidence,
        )
    assert missing_error.value.code == "admission"

    repository_admission._read_admission.side_effect = None
    repository_admission._read_admission.return_value = _admission_result()
    with pytest.raises(TwinAdmissionError, match="admission evidence"):
        await repository_admission._persist_exact_admission(
            object(),
            "0" * 64,
            _baseline(),
            _candidate(),
            baseline_evidence,
            candidate_evidence,
        )


@pytest.mark.asyncio
async def test_admission_rejects_invalid_binding_source_lock_and_drift(monkeypatch):
    with pytest.raises(TwinAdmissionError) as request_error:
        await repository_admission._admit_verified_twins(
            SimpleNamespace(transaction=_transaction),
            object(),
            _baseline(),
            _candidate(),
        )
    assert request_error.value.code == "invalid_request"

    monkeypatch.setattr(
        repository_admission,
        "lock_source",
        AsyncMock(side_effect=RuntimeError("private source row")),
    )
    with pytest.raises(TwinAdmissionError) as lock_error:
        await repository_admission._admit_verified_twins(
            SimpleNamespace(transaction=_transaction),
            _binding(),
            _baseline(),
            _candidate(),
        )
    assert lock_error.value.code == "source"
    assert "private source row" not in str(lock_error.value)

    repository_admission.lock_source.side_effect = None
    monkeypatch.setattr(
        repository_admission,
        "_current_configuration_hash",
        AsyncMock(return_value="0" * 64),
    )
    with pytest.raises(TwinAdmissionError, match="source configuration"):
        await repository_admission._admit_verified_twins(
            SimpleNamespace(transaction=_transaction),
            _binding(),
            _baseline(),
            _candidate(),
        )


@pytest.mark.asyncio
async def test_admission_rejects_stale_predecessor(monkeypatch):
    monkeypatch.setattr(repository_admission, "lock_source", AsyncMock())
    monkeypatch.setattr(
        repository_admission,
        "_current_configuration_hash",
        AsyncMock(return_value=_binding().configuration_hash),
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
        AsyncMock(return_value="ffd_" + "p" * 48),
    )

    with pytest.raises(TwinAdmissionError) as pointer_error:
        await repository_admission._admit_verified_twins(
            SimpleNamespace(transaction=_transaction),
            _binding(),
            _baseline(),
            _candidate(),
        )
    assert pointer_error.value.code == "pointer"


@pytest.mark.asyncio
async def test_public_wrappers_preserve_bounded_errors_and_hide_unknowns(monkeypatch):
    monkeypatch.setattr(
        repository_admission,
        "_admit_verified_twins",
        AsyncMock(side_effect=TwinAdmissionError("invalid_request")),
    )
    with pytest.raises(TwinAdmissionError) as request_error:
        await admit_verified_twins(
            database=object(),
            binding=_binding(),
            baseline=_baseline(),
            candidate=_candidate(),
        )
    assert request_error.value.code == "invalid_request"

    with pytest.raises(TwinAdmissionError) as candidate_error:
        await verify_twin_admission_for_publication(object(), "source-a", object())
    assert candidate_error.value.code == "invalid_request"

    monkeypatch.setattr(
        repository_admission,
        "_verify_twin_admission_for_publication",
        AsyncMock(side_effect=RuntimeError("private publication state")),
    )
    with pytest.raises(TwinAdmissionError) as storage_error:
        await verify_twin_admission_for_publication(
            object(),
            "source-a",
            _candidate(),
        )
    assert storage_error.value.code == "storage"
    assert "private publication" not in str(storage_error.value)


@pytest.mark.asyncio
async def test_empty_admission_read_returns_none():
    database = SimpleNamespace(first=AsyncMock(return_value=None))

    assert await repository_admission._read_admission(
        database,
        "source-a",
        _candidate().dataset_id,
    ) is None
