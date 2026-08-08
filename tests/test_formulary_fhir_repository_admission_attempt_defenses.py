# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Failure-path coverage for immutable formulary twin attempts."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.formulary_fhir import repository_admission_attempt
from process.formulary_fhir.repository_admission_attempt import persist_twin_attempt
from process.formulary_fhir.repository_admission_attempt import (
    require_exact_twin_attempt,
)
from process.formulary_fhir.repository_admission_types import TwinAdmissionError
from tests.test_formulary_fhir_repository_admission_attempt import _attempt_row
from tests.test_formulary_fhir_repository_admission_attempt import _baseline
from tests.test_formulary_fhir_repository_admission_attempt import _candidate


@pytest.mark.asyncio
async def test_attempt_lookup_sanitizes_storage_and_row_failures():
    storage_database = SimpleNamespace(
        all=AsyncMock(side_effect=RuntimeError("private database address"))
    )
    with pytest.raises(TwinAdmissionError) as storage_error:
        await repository_admission_attempt._root_attempts(
            storage_database,
            _baseline().dataset_id,
            _candidate().dataset_id,
        )
    assert storage_error.value.code == "storage"
    assert "private database" not in str(storage_error.value)

    invalid_database = SimpleNamespace(
        all=AsyncMock(return_value=[_attempt_row(candidate_evidence_hash="invalid")])
    )
    with pytest.raises(TwinAdmissionError) as attempt_error:
        await repository_admission_attempt._root_attempts(
            invalid_database,
            _baseline().dataset_id,
            _candidate().dataset_id,
        )
    assert attempt_error.value.code == "attempt"
    assert "invalid" not in str(attempt_error.value)


@pytest.mark.asyncio
async def test_attempt_insert_sanitizes_count_hash_and_driver_failures():
    bad_count_database = SimpleNamespace(status=AsyncMock(return_value=2))
    with pytest.raises(TwinAdmissionError) as count_error:
        await repository_admission_attempt._insert_attempt(
            bad_count_database,
            _baseline(),
            _candidate(),
            "9" * 64,
            "1" * 64,
            "1" * 64,
        )
    assert count_error.value.code == "storage"

    invalid_hash_database = SimpleNamespace(status=AsyncMock())
    with pytest.raises(TwinAdmissionError, match="storage failed"):
        await repository_admission_attempt._insert_attempt(
            invalid_hash_database,
            _baseline(),
            _candidate(),
            "invalid",
            "1" * 64,
            "1" * 64,
        )
    invalid_hash_database.status.assert_not_awaited()

    driver_database = SimpleNamespace(
        status=AsyncMock(side_effect=RuntimeError("private SQL payload"))
    )
    with pytest.raises(TwinAdmissionError) as driver_error:
        await repository_admission_attempt._insert_attempt(
            driver_database,
            _baseline(),
            _candidate(),
            "9" * 64,
            "1" * 64,
            "1" * 64,
        )
    assert "private SQL" not in str(driver_error.value)
    assert driver_error.value.__cause__ is None


@pytest.mark.asyncio
async def test_attempt_insert_requires_exact_post_write_readback():
    database = SimpleNamespace(
        all=AsyncMock(side_effect=[[], []]),
        status=AsyncMock(return_value=1),
    )

    with pytest.raises(TwinAdmissionError) as attempt_error:
        await persist_twin_attempt(
            database,
            _baseline(),
            _candidate(),
            "9" * 64,
            "1" * 64,
            "1" * 64,
        )

    assert attempt_error.value.code == "attempt"


@pytest.mark.asyncio
async def test_exact_matched_attempt_is_returned_without_mutation():
    database = SimpleNamespace(all=AsyncMock(return_value=[_attempt_row()]))

    attempt = await require_exact_twin_attempt(
        database,
        _baseline(),
        _candidate(),
        "9" * 64,
        "1" * 64,
        "1" * 64,
    )

    assert attempt.matched is True
