# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from process.formulary_fhir import repository_batch
from process.formulary_fhir.types import MedicationRecord


def _medication(index: int, *, alternatives: tuple[str, ...] = ()):
    medication_id = f"MI-synthetic-{index}"
    return MedicationRecord(
        upstream_medication_id=medication_id,
        upstream_version_id="1",
        upstream_last_updated="2026-08-06T12:00:00Z",
        status="active",
        drug_name=f"Synthetic {index}",
        rxnorm_id=str(index),
        ndc11=None,
        codings=(),
        raw_extensions=(),
        source_plan_identifiers=("SYNTHETIC-PLAN",),
        drug_tier="preferred",
        prior_authorization=False,
        step_therapy=False,
        quantity_limit=False,
        alternative_references=alternatives,
        content_hash=f"{index:064x}",
    )


@pytest.mark.asyncio
async def test_alias_rows_use_bounded_multi_row_statements(monkeypatch):
    monkeypatch.setattr(repository_batch, "WRITE_BATCH_SIZE", 2)
    status = AsyncMock(return_value=1)
    monkeypatch.setattr(repository_batch.db, "status", status)
    medications = tuple(_medication(index) for index in range(5))
    medications_by_id = {
        medication.upstream_medication_id: medication for medication in medications
    }
    variants_by_id = {
        medication.upstream_medication_id: f"{index + 10:064x}"
        for index, medication in enumerate(medications)
    }

    await repository_batch.insert_changed_alias_rows(
        "alias-version",
        medications_by_id,
        variants_by_id,
        apply_california_rule=False,
    )

    medication_calls = [
        call
        for call in status.await_args_list
        if "fhir_formulary_medication" in call.args[0]
    ]
    membership_calls = [
        call
        for call in status.await_args_list
        if "fhir_formulary_alias_membership" in call.args[0]
    ]
    assert len(medication_calls) == 3
    assert len(membership_calls) == 3
    assert all("ON CONFLICT" in call.args[0] for call in status.await_args_list)


@pytest.mark.asyncio
async def test_alternative_batch_preserves_raw_and_corrected_evidence(monkeypatch):
    status = AsyncMock(return_value=1)
    monkeypatch.setattr(repository_batch.db, "status", status)
    source_medication = _medication(
        1,
        alternatives=("MedicationKnowledge/synthetic-2",),
    )
    corrected_target = _medication(2)
    medications_by_id = {
        source_medication.upstream_medication_id: source_medication,
        "MI-synthetic-2": corrected_target,
    }
    variants_by_id = {medication_id: "a" * 64 for medication_id in medications_by_id}

    await repository_batch.insert_changed_alias_rows(
        "alias-version",
        medications_by_id,
        variants_by_id,
        apply_california_rule=True,
    )

    alternative_call = next(
        call
        for call in status.await_args_list
        if "fhir_formulary_alternative" in call.args[0]
    )
    assert alternative_call.kwargs["raw_reference_0"] == (
        "MedicationKnowledge/synthetic-2"
    )
    assert alternative_call.kwargs["corrected_reference_0"] == (
        "MedicationKnowledge/MI-synthetic-2"
    )
    assert alternative_call.kwargs["resolved_medication_id_0"] == ("MI-synthetic-2")


def test_production_batch_size_stays_within_postgresql_parameter_bounds():
    assert 500 <= repository_batch.WRITE_BATCH_SIZE <= 2_000
