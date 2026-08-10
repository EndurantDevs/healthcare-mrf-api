# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from dataclasses import replace
import datetime as dt

from process.formulary_fhir.repository_admission_types import AlternativeProof
from process.formulary_fhir.repository_admission_types import TwinAdmissionResult
from process.formulary_fhir.repository_shared import DatasetRef
from process.formulary_fhir.repository_shared import DatasetVerification
from process.formulary_fhir.source import EnabledSourceBinding
from process.formulary_fhir.source import LIBRARY_ONLY_LAUNCH_MODE
from process.formulary_fhir.uhc_drug_parser_contract import UHCDrugSpoolEvidence
from process.formulary_fhir.uhc_drug_receipt import UHCDrugAdmissionReceipt
from process.formulary_fhir.uhc_drug_receipt import uhc_drug_receipt_id
from process.formulary_fhir.uhc_drug_sync_contract import (
    UHCDrugSynchronizationResult,
)
from process.formulary_fhir.uhc_drug_sync_contract import (
    uhc_drug_sync_contract_hash,
)
from process.formulary_fhir.uhc_drug_twin import UHCDrugTwinResult
from process.formulary_fhir.uhc_source import uhc_formulary_source_manifest
from tests.uhc_drug_parser_test_support import artifact_set


CUTOFF = dt.datetime(2026, 8, 10, 13, 0, tzinfo=dt.UTC)
ADMITTED_AT = CUTOFF + dt.timedelta(minutes=3)
OBSERVATION_SHA256 = "f" * 64


def source_binding() -> EnabledSourceBinding:
    """Build the reviewed exact UHC source binding used by receipt tests."""

    definition = uhc_formulary_source_manifest().definition
    return EnabledSourceBinding(
        source_id=definition.source_id,
        config=definition.config,
        configuration_hash="d" * 64,
        alternative_correction=None,
        launch_mode=LIBRARY_ONLY_LAUNCH_MODE,
    )


def _spool_evidence(artifacts) -> UHCDrugSpoolEvidence:
    return UHCDrugSpoolEvidence(
        source_id=artifacts.source_id,
        source_file_set_sha256=artifacts.source_file_set_sha256,
        artifact_set_sha256=artifacts.artifact_set_sha256,
        spool_content_sha256="9" * 64,
        file_count=48,
        raw_record_count=48,
        raw_plan_entry_count=48,
        plan_count=2,
        medication_membership_count=5,
        duplicate_count=0,
        superseded_count=0,
        max_last_updated_at=CUTOFF,
    )


def _twin_datasets(source_id: str, contract_hash: str):
    baseline_dataset = DatasetRef(
        source_id,
        "ffd_" + "1" * 48,
        "uhc-drug-baseline-20260810",
        None,
        CUTOFF,
        contract_hash,
        "none",
        "verified",
    )
    candidate_dataset = DatasetRef(
        source_id,
        "ffd_" + "2" * 48,
        "uhc-drug-candidate-20260810",
        None,
        CUTOFF,
        contract_hash,
        "requested",
        "verified",
    )
    return baseline_dataset, candidate_dataset


def _twin_admission(
    baseline_dataset: DatasetRef,
    candidate_dataset: DatasetRef,
    verification: DatasetVerification,
) -> TwinAdmissionResult:
    return TwinAdmissionResult(
        source_id=candidate_dataset.source_id,
        baseline_dataset_id=baseline_dataset.dataset_id,
        baseline_run_id=baseline_dataset.run_id,
        candidate_dataset_id=candidate_dataset.dataset_id,
        candidate_run_id=candidate_dataset.run_id,
        predecessor_dataset_id=None,
        cutoff_at=CUTOFF,
        source_configuration_hash="d" * 64,
        acquisition_contract_hash=candidate_dataset.acquisition_contract_hash,
        verification=verification,
        alternative=AlternativeProof(0, "e" * 64),
        baseline_verified_at=CUTOFF + dt.timedelta(minutes=1),
        candidate_verified_at=CUTOFF + dt.timedelta(minutes=2),
        admitted_at=ADMITTED_AT,
    )


def admitted_twin():
    """Build one internally consistent admitted twin and artifact set."""

    artifacts, _bodies_by_name = artifact_set()
    evidence = _spool_evidence(artifacts)
    contract_hash = uhc_drug_sync_contract_hash(
        source_binding(),
        artifacts,
        evidence,
        CUTOFF,
    )
    baseline_dataset, candidate_dataset = _twin_datasets(
        artifacts.source_id,
        contract_hash,
    )
    candidate_verification = DatasetVerification(
        artifacts.source_id,
        candidate_dataset.dataset_id,
        2,
        2,
        5,
        "b" * 64,
        "c" * 64,
    )
    baseline_result = UHCDrugSynchronizationResult(
        baseline_dataset,
        replace(
            candidate_verification,
            dataset_id=baseline_dataset.dataset_id,
        ),
        evidence,
        2,
        0,
    )
    candidate_result = UHCDrugSynchronizationResult(
        candidate_dataset,
        candidate_verification,
        evidence,
        2,
        0,
    )
    admission = _twin_admission(
        baseline_dataset,
        candidate_dataset,
        candidate_verification,
    )
    return UHCDrugTwinResult(admission, baseline_result, candidate_result), artifacts


def admission_receipt(
    twin_result: UHCDrugTwinResult,
    *,
    observation_sha256: str = OBSERVATION_SHA256,
) -> UHCDrugAdmissionReceipt:
    """Build the exact receipt corresponding to one admitted twin."""

    admission = twin_result.admission
    evidence = twin_result.candidate.evidence
    receipt_id = uhc_drug_receipt_id(
        admission.source_id,
        admission.candidate_dataset_id,
        observation_sha256,
        evidence.source_file_set_sha256,
        evidence.artifact_set_sha256,
        evidence.spool_content_sha256,
    )
    return UHCDrugAdmissionReceipt(
        receipt_id=receipt_id,
        source_observation_sha256=observation_sha256,
        artifact_set_sha256=evidence.artifact_set_sha256,
        admission=admission,
        evidence=evidence,
        recorded_at=ADMITTED_AT + dt.timedelta(seconds=1),
    )


__all__ = (
    "ADMITTED_AT",
    "CUTOFF",
    "OBSERVATION_SHA256",
    "admission_receipt",
    "admitted_twin",
    "source_binding",
)
