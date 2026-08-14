# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed boundaries for durable UHC drug receipt evidence."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import replace
import datetime as dt
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.formulary_fhir.repository_shared import PublicationResult
import process.formulary_fhir.uhc_drug_operation_evidence as operation_evidence
import process.formulary_fhir.uhc_drug_receipt as receipt_module
import process.formulary_fhir.uhc_drug_receipt_store as receipt_store
import process.formulary_fhir.uhc_drug_release as release_module
from tests.test_uhc_drug_receipt import _receipt_row
from tests.uhc_drug_receipt_test_support import admission_receipt
from tests.uhc_drug_receipt_test_support import admitted_twin
from tests.uhc_drug_receipt_test_support import artifact_acquisition_result
from tests.uhc_drug_receipt_test_support import OBSERVATION_SHA256
from tests.uhc_drug_receipt_test_support import source_binding


@asynccontextmanager
async def _source_lease(*_args, **_kwargs):
    yield


def _valid_operation_evidence():
    twin_result, _artifacts = admitted_twin()
    receipt = admission_receipt(twin_result)
    return operation_evidence.receipt_operation_evidence(receipt)


def _admission_row(admission) -> dict[str, object]:
    verification = admission.verification
    alternative = admission.alternative
    return {
        "source_id": admission.source_id,
        "baseline_dataset_id": admission.baseline_dataset_id,
        "baseline_run_id": admission.baseline_run_id,
        "candidate_dataset_id": admission.candidate_dataset_id,
        "candidate_run_id": admission.candidate_run_id,
        "predecessor_dataset_id": admission.predecessor_dataset_id,
        "cutoff_at": admission.cutoff_at,
        "source_configuration_hash": admission.source_configuration_hash,
        "acquisition_contract_hash": admission.acquisition_contract_hash,
        "list_count": verification.list_count,
        "alias_count": verification.alias_count,
        "medication_count": verification.medication_membership_count,
        "coverage_hash": verification.coverage_hash,
        "membership_hash": verification.membership_hash,
        "alternative_count": alternative.count,
        "alternative_hash": alternative.evidence_hash,
        "baseline_verified_at": admission.baseline_verified_at,
        "candidate_verified_at": admission.candidate_verified_at,
        "admitted_at": admission.admitted_at,
    }


def test_receipt_aggregates_reject_cross_bound_values() -> None:
    """Receipt wrappers cannot bind a changed identity or unrelated objects."""

    twin_result, artifacts = admitted_twin()
    receipt = admission_receipt(twin_result)
    candidate = receipt_module._candidate_from_admission(receipt.admission)

    with pytest.raises(ValueError, match="receipt is inconsistent"):
        replace(receipt, receipt_id="ffur_" + "0" * 48)
    with pytest.raises(ValueError, match="publication inputs"):
        receipt_module.UHCDrugPublicationInputs(
            receipt,
            object(),
            artifacts,
            candidate,
        )
    with pytest.raises(ValueError, match="recorded admission"):
        receipt_module.UHCDrugRecordedAdmission(object(), receipt)


@pytest.mark.asyncio
async def test_receipt_load_preserves_store_failures_and_hides_parse_errors(
    monkeypatch,
) -> None:
    """Bounded store errors pass through while malformed rows are normalized."""

    twin_result, _artifacts = admitted_twin()
    receipt = admission_receipt(twin_result)
    store_failure = RuntimeError("bounded store failure")
    load_row = AsyncMock(side_effect=store_failure)
    monkeypatch.setattr(receipt_module, "load_uhc_receipt_row", load_row)

    with pytest.raises(RuntimeError, match="bounded store failure") as caught:
        await receipt_module.load_uhc_drug_admission_receipt(
            receipt_id=receipt.receipt_id,
            database=object(),
        )
    assert caught.value is store_failure

    load_row.side_effect = ValueError("malformed stored value")
    with pytest.raises(RuntimeError, match="receipt is invalid") as caught:
        await receipt_module.load_uhc_drug_admission_receipt(
            receipt_id=receipt.receipt_id,
            database=object(),
        )
    assert caught.value.__cause__ is None


def test_record_contract_rejects_source_configuration_drift() -> None:
    """A valid twin cannot be recorded beneath a different source binding."""

    twin_result, artifacts = admitted_twin()
    drifted_binding = replace(
        source_binding(),
        configuration_hash="0" * 64,
    )

    with pytest.raises(RuntimeError, match="contract is inconsistent"):
        receipt_module._require_record_contract(
            drifted_binding,
            artifacts,
            twin_result,
        )


@pytest.mark.asyncio
async def test_record_rechecks_artifacts_and_admission_under_lease(
    monkeypatch,
) -> None:
    """Receipt recording rejects retained-set or stored-admission drift."""

    twin_result, artifacts = admitted_twin()
    acquisition = artifact_acquisition_result(artifacts)
    monkeypatch.setattr(
        receipt_module,
        "register_uhc_formulary_source",
        AsyncMock(return_value=source_binding()),
    )
    load_artifacts = AsyncMock(return_value=object())
    monkeypatch.setattr(
        receipt_module,
        "load_complete_source_artifact_set",
        load_artifacts,
    )

    with pytest.raises(RuntimeError, match="artifacts changed"):
        await receipt_module._record_receipt_under_lease(
            acquisition=acquisition,
            twin_result=twin_result,
            database=object(),
        )

    load_artifacts.return_value = artifacts
    monkeypatch.setattr(
        receipt_module,
        "load_uhc_receipt_admission",
        AsyncMock(return_value=object()),
    )
    with pytest.raises(RuntimeError, match="admission changed"):
        await receipt_module._record_receipt_under_lease(
            acquisition=acquisition,
            twin_result=twin_result,
            database=object(),
        )


@pytest.mark.asyncio
async def test_record_wrapper_validates_inputs_and_uses_source_lease(
    monkeypatch,
) -> None:
    """Only exact artifact and twin types reach the leased recorder."""

    twin_result, artifacts = admitted_twin()
    acquisition = artifact_acquisition_result(artifacts)
    expected = admission_receipt(twin_result)
    monkeypatch.setattr(
        receipt_module.manual_lock,
        "manual_source_lease",
        _source_lease,
    )
    recorder = AsyncMock(return_value=expected)
    monkeypatch.setattr(receipt_module, "_record_receipt_under_lease", recorder)

    with pytest.raises(ValueError, match="input is invalid"):
        await receipt_module.record_uhc_drug_admission_receipt(
            acquisition=object(),
            twin_result=twin_result,
            database=object(),
        )
    with pytest.raises(ValueError, match="input is invalid"):
        await receipt_module.record_uhc_drug_admission_receipt(
            acquisition=acquisition,
            twin_result=object(),
            database=object(),
        )

    observed = await receipt_module.record_uhc_drug_admission_receipt(
        acquisition=acquisition,
        twin_result=twin_result,
        database=object(),
    )
    assert observed is expected
    recorder.assert_awaited_once()


def test_operation_evidence_accepts_predecessor_and_rejects_collisions() -> None:
    """Optional predecessors are validated and root identities stay distinct."""

    evidence = _valid_operation_evidence()
    predecessor_id = "ffd_" + "3" * 48

    assert replace(
        evidence,
        predecessor_dataset_id=predecessor_id,
    ).predecessor_dataset_id == predecessor_id
    with pytest.raises(ValueError, match="receipt evidence is invalid"):
        replace(evidence, candidate_run_id=evidence.baseline_run_id)


def test_operation_evidence_rejects_counts_timestamps_and_wrong_types() -> None:
    """Operator payload boundaries revalidate counts, order, and exact types."""

    evidence = _valid_operation_evidence()
    with pytest.raises(ValueError, match="receipt evidence is invalid"):
        replace(evidence, file_count=47)
    with pytest.raises(ValueError, match="receipt evidence is invalid"):
        replace(
            evidence,
            recorded_at=evidence.admitted_at - dt.timedelta(seconds=1),
        )
    with pytest.raises(ValueError, match="receipt evidence is invalid"):
        operation_evidence.receipt_operation_evidence(object())
    with pytest.raises(ValueError, match="receipt evidence is invalid"):
        operation_evidence.receipt_operation_payload(object())


def test_receipt_store_error_codes_are_bounded() -> None:
    """Unexpected internal lookup codes collapse to a public evidence code."""

    missing = receipt_store.UHCDrugReceiptStoreError("missing")
    evidence = receipt_store.UHCDrugReceiptStoreError("unexpected")

    assert (missing.code, str(missing)) == (
        "missing",
        "UHC drug admission receipt missing",
    )
    assert (evidence.code, str(evidence)) == (
        "evidence",
        "UHC drug admission receipt evidence",
    )


@pytest.mark.asyncio
async def test_receipt_store_rejects_missing_or_cross_observation_rows() -> None:
    """A receipt row must exist and join to its exact observed file set."""

    twin_result, _artifacts = admitted_twin()
    receipt = admission_receipt(twin_result)
    database = SimpleNamespace(first=AsyncMock(return_value=None))

    with pytest.raises(receipt_store.UHCDrugReceiptStoreError) as caught:
        await receipt_store.load_uhc_receipt_row(
            receipt.receipt_id,
            database=database,
        )
    assert caught.value.code == "missing"

    drifted_row = _receipt_row(receipt)
    drifted_row["observed_file_set_sha256"] = "0" * 64
    database.first.return_value = drifted_row
    with pytest.raises(RuntimeError, match="receipt is inconsistent"):
        await receipt_store.load_uhc_receipt_row(
            receipt.receipt_id,
            database=database,
        )


@pytest.mark.asyncio
async def test_receipt_store_rejects_missing_twin_admission() -> None:
    """A durable receipt cannot outlive its immutable generic admission."""

    twin_result, _artifacts = admitted_twin()
    admission = twin_result.admission
    database = SimpleNamespace(first=AsyncMock(return_value=None))

    with pytest.raises(RuntimeError, match="twin admission is missing"):
        await receipt_store.load_uhc_receipt_admission(
            admission.source_id,
            admission.candidate_dataset_id,
            database=database,
        )

    database.first.return_value = _admission_row(admission)
    assert await receipt_store.load_uhc_receipt_admission(
        admission.source_id,
        admission.candidate_dataset_id,
        database=database,
    ) == admission


def test_release_rejects_publication_before_admission() -> None:
    """Repository publication time cannot precede durable admission."""

    twin_result, _artifacts = admitted_twin()
    receipt = admission_receipt(twin_result)
    publication = PublicationResult(
        receipt.source_id,
        receipt.candidate_dataset_id,
        1,
        receipt.admission.admitted_at - dt.timedelta(seconds=1),
    )

    with pytest.raises(RuntimeError, match="result is inconsistent"):
        release_module._validated_publication_result(
            receipt.receipt_id,
            receipt.candidate_dataset_id,
            receipt.admission.admitted_at,
            publication,
        )
