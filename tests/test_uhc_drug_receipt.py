# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import replace
import datetime as dt
from unittest.mock import ANY
from unittest.mock import AsyncMock

import pytest

import process.formulary_fhir.uhc_drug_receipt as receipt_module
from process.formulary_fhir.uhc_drug_receipt import UHCDrugAdmissionReceipt
from process.formulary_fhir.uhc_drug_receipt import uhc_drug_receipt_id
from tests.uhc_drug_receipt_test_support import ADMITTED_AT
from tests.uhc_drug_receipt_test_support import admission_receipt
from tests.uhc_drug_receipt_test_support import admitted_twin
from tests.uhc_drug_receipt_test_support import artifact_acquisition_result
from tests.uhc_drug_receipt_test_support import OBSERVATION_SHA256
from tests.uhc_drug_receipt_test_support import source_binding
from tests.uhc_drug_parser_test_support import artifact_set


def _receipt_row(receipt: UHCDrugAdmissionReceipt) -> dict:
    evidence = receipt.evidence
    admission = receipt.admission
    return {
        "receipt_id": receipt.receipt_id,
        "source_id": admission.source_id,
        "source_observation_sha256": receipt.source_observation_sha256,
        "source_file_set_sha256": evidence.source_file_set_sha256,
        "artifact_set_sha256": evidence.artifact_set_sha256,
        "candidate_dataset_id": admission.candidate_dataset_id,
        "spool_content_sha256": evidence.spool_content_sha256,
        "file_count": evidence.file_count,
        "expected_file_count": receipt.expected_file_count,
        "excluded_file_count": receipt.excluded_file_count,
        "selected_source_file_ids": list(receipt.selected_source_file_ids),
        "exclusion_code": receipt.exclusion_code,
        "raw_record_count": evidence.raw_record_count,
        "raw_plan_entry_count": evidence.raw_plan_entry_count,
        "plan_count": evidence.plan_count,
        "medication_membership_count": evidence.medication_membership_count,
        "duplicate_count": evidence.duplicate_count,
        "superseded_count": evidence.superseded_count,
        "max_last_updated_at": evidence.max_last_updated_at,
        "recorded_at": receipt.recorded_at,
        "observed_file_set_sha256": evidence.source_file_set_sha256,
    }


class _Database:
    def __init__(self, receipt: UHCDrugAdmissionReceipt) -> None:
        self.receipt_by_field = _receipt_row(receipt)
        self.insert_values: dict | None = None

    async def first(self, _statement, **_values):
        return self.receipt_by_field

    async def status(self, _statement, **values):
        self.insert_values = values
        return 1

    @asynccontextmanager
    async def transaction(self):
        yield


def test_receipt_identity_is_deterministic_and_binds_every_root() -> None:
    """Stable identity rotates for every retained or normalized root."""

    twin_result, _artifacts = admitted_twin()
    expected = admission_receipt(twin_result)
    evidence = expected.evidence
    admission = expected.admission

    assert uhc_drug_receipt_id(
        admission.source_id,
        admission.candidate_dataset_id,
        OBSERVATION_SHA256,
        evidence.source_file_set_sha256,
        evidence.artifact_set_sha256,
        evidence.spool_content_sha256,
    ) == expected.receipt_id
    assert uhc_drug_receipt_id(
        admission.source_id,
        admission.candidate_dataset_id,
        "1" * 64,
        evidence.source_file_set_sha256,
        evidence.artifact_set_sha256,
        evidence.spool_content_sha256,
    ) != expected.receipt_id


@pytest.mark.asyncio
async def test_load_receipt_reconstructs_admission_and_spool(monkeypatch) -> None:
    """A fresh process can rebuild the complete receipt from durable rows."""

    twin_result, _artifacts = admitted_twin()
    expected = admission_receipt(twin_result)
    database = _Database(expected)
    monkeypatch.setattr(
        receipt_module,
        "load_uhc_receipt_admission",
        AsyncMock(return_value=twin_result.admission),
    )

    observed = await receipt_module.load_uhc_drug_admission_receipt(
        receipt_id=expected.receipt_id,
        database=database,
    )

    assert observed == expected


@pytest.mark.asyncio
async def test_record_receipt_inserts_exact_values_and_replays(monkeypatch) -> None:
    """Admission recording inserts once and requires exact semantic readback."""

    twin_result, artifacts = admitted_twin()
    expected = admission_receipt(twin_result)
    database = _Database(expected)
    monkeypatch.setattr(
        receipt_module,
        "register_uhc_formulary_source",
        AsyncMock(return_value=source_binding()),
    )
    monkeypatch.setattr(
        receipt_module,
        "load_complete_source_artifact_set",
        AsyncMock(return_value=artifacts),
    )
    monkeypatch.setattr(
        receipt_module,
        "load_uhc_receipt_admission",
        AsyncMock(return_value=twin_result.admission),
    )
    monkeypatch.setattr(
        receipt_module,
        "require_source_unchanged",
        AsyncMock(),
    )

    observed = await receipt_module._record_receipt_under_lease(
        acquisition=artifact_acquisition_result(artifacts),
        twin_result=twin_result,
        database=database,
    )

    assert observed == expected
    assert database.insert_values is not None
    assert database.insert_values["receipt_id"] == expected.receipt_id
    assert database.insert_values["file_count"] == 48
    assert database.insert_values["expected_file_count"] == 48
    assert database.insert_values["excluded_file_count"] == 0
    assert len(database.insert_values["selected_source_file_ids"]) == 48
    assert database.insert_values["exclusion_code"] is None
    assert database.insert_values["plan_count"] == 2
    assert database.insert_values["medication_membership_count"] == 5


@pytest.mark.asyncio
async def test_record_receipt_rejects_conflicting_candidate(monkeypatch) -> None:
    """A candidate cannot be rebound to a different retained observation."""

    twin_result, artifacts = admitted_twin()
    expected = admission_receipt(twin_result)
    conflicting = admission_receipt(
        twin_result,
        observation_sha256="1" * 64,
    )
    database = _Database(conflicting)
    monkeypatch.setattr(
        receipt_module,
        "register_uhc_formulary_source",
        AsyncMock(return_value=source_binding()),
    )
    monkeypatch.setattr(
        receipt_module,
        "load_complete_source_artifact_set",
        AsyncMock(return_value=artifacts),
    )
    monkeypatch.setattr(
        receipt_module,
        "load_uhc_receipt_admission",
        AsyncMock(return_value=twin_result.admission),
    )
    monkeypatch.setattr(
        receipt_module,
        "require_source_unchanged",
        AsyncMock(),
    )

    with pytest.raises(RuntimeError, match="receipt changed"):
        await receipt_module._record_receipt_under_lease(
            acquisition=artifact_acquisition_result(artifacts),
            twin_result=twin_result,
            database=database,
        )
    assert expected.receipt_id != conflicting.receipt_id


@pytest.mark.asyncio
async def test_reconstruct_rehashes_artifacts_and_contract(monkeypatch) -> None:
    """Publication inputs require the exact receipt, CAS set, and source hash."""

    twin_result, artifacts = admitted_twin()
    expected = admission_receipt(twin_result)
    artifact_loader = AsyncMock(return_value=artifacts)
    monkeypatch.setattr(
        receipt_module,
        "load_uhc_drug_admission_receipt",
        AsyncMock(return_value=expected),
    )
    monkeypatch.setattr(
        receipt_module,
        "register_uhc_formulary_source",
        AsyncMock(return_value=source_binding()),
    )
    monkeypatch.setattr(
        receipt_module,
        "reopen_source_artifact_set",
        artifact_loader,
    )
    source_fence = AsyncMock()
    monkeypatch.setattr(
        receipt_module,
        "require_source_unchanged",
        source_fence,
    )

    reconstructed = await receipt_module.reconstruct_uhc_drug_publication_inputs(
        receipt_id=expected.receipt_id,
        database=object(),
    )

    assert reconstructed.receipt is expected
    assert reconstructed.artifacts is artifacts
    assert reconstructed.candidate.dataset_id == (
        twin_result.admission.candidate_dataset_id
    )
    artifact_loader.assert_awaited_once_with(
        expected.source_id,
        expected.evidence.source_file_set_sha256,
        expected.artifact_set_sha256,
        database=ANY,
        cancel_check=None,
    )
    source_fence.assert_awaited_once()


@pytest.mark.asyncio
async def test_partial_reconstruct_replays_selected_rows_after_recovery(
    monkeypatch,
) -> None:
    """Later verification of omitted rows cannot redefine a partial receipt."""

    twin_result, artifacts = admitted_twin(selected_file_count=47)
    expected = admission_receipt(twin_result)
    full_artifacts, _bodies_by_name = artifact_set()
    full_identities = tuple(
        artifact.identity for artifact in full_artifacts.artifacts
    )
    full_loader = AsyncMock(return_value=full_identities)
    selected_loader = AsyncMock(return_value=artifacts)
    complete_reopener = AsyncMock()
    monkeypatch.setattr(
        receipt_module,
        "load_uhc_drug_admission_receipt",
        AsyncMock(return_value=expected),
    )
    monkeypatch.setattr(
        receipt_module,
        "register_uhc_formulary_source",
        AsyncMock(return_value=source_binding()),
    )
    monkeypatch.setattr(
        receipt_module,
        "load_source_artifact_identities",
        full_loader,
    )
    monkeypatch.setattr(
        receipt_module,
        "load_selected_source_artifact_set",
        selected_loader,
    )
    monkeypatch.setattr(
        receipt_module,
        "reopen_source_artifact_set",
        complete_reopener,
    )
    monkeypatch.setattr(receipt_module, "require_source_unchanged", AsyncMock())

    reconstructed = await receipt_module.reconstruct_uhc_drug_publication_inputs(
        receipt_id=expected.receipt_id,
        database=object(),
    )

    assert reconstructed.receipt is expected
    assert reconstructed.artifacts is artifacts
    complete_reopener.assert_not_awaited()
    selected_loader.assert_awaited_once_with(
        full_identities,
        selected_source_file_ids=expected.selected_source_file_ids,
        require_unselected_pending=False,
        database=ANY,
        cancel_check=None,
    )


@pytest.mark.asyncio
async def test_reconstruct_blocks_source_or_artifact_drift(monkeypatch) -> None:
    """No candidate authority survives source or retained artifact drift."""

    twin_result, artifacts = admitted_twin()
    expected = admission_receipt(twin_result)
    monkeypatch.setattr(
        receipt_module,
        "load_uhc_drug_admission_receipt",
        AsyncMock(return_value=expected),
    )
    monkeypatch.setattr(
        receipt_module,
        "register_uhc_formulary_source",
        AsyncMock(
            return_value=replace(
                source_binding(),
                configuration_hash="1" * 64,
            )
        ),
    )
    monkeypatch.setattr(
        receipt_module,
        "reopen_source_artifact_set",
        AsyncMock(return_value=artifacts),
    )

    with pytest.raises(RuntimeError, match="contract changed"):
        await receipt_module.reconstruct_uhc_drug_publication_inputs(
            receipt_id=expected.receipt_id,
            database=object(),
        )

    monkeypatch.setattr(
        receipt_module,
        "register_uhc_formulary_source",
        AsyncMock(return_value=source_binding()),
    )
    monkeypatch.setattr(
        receipt_module,
        "reopen_source_artifact_set",
        AsyncMock(side_effect=RuntimeError("retained artifact set changed")),
    )
    with pytest.raises(RuntimeError, match="retained artifact set changed"):
        await receipt_module.reconstruct_uhc_drug_publication_inputs(
            receipt_id=expected.receipt_id,
            database=object(),
        )


def test_receipt_rejects_pre_admission_or_future_source_time() -> None:
    """Stored source and receipt timestamps cannot cross admission bounds."""

    twin_result, _artifacts = admitted_twin()
    expected = admission_receipt(twin_result)
    with pytest.raises(ValueError, match="receipt is inconsistent"):
        replace(
            expected,
            recorded_at=ADMITTED_AT - dt.timedelta(seconds=1),
        )
    with pytest.raises(ValueError, match="receipt is inconsistent"):
        replace(
            expected,
            evidence=replace(
                expected.evidence,
                max_last_updated_at=(
                    expected.admission.cutoff_at + dt.timedelta(seconds=1)
                ),
            ),
        )
