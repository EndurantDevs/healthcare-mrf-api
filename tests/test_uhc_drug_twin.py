# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import replace
import datetime as dt
import os
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

import process.formulary_fhir.uhc_drug_spool as spool
import process.formulary_fhir.uhc_drug_receipt as receipt_module
import process.formulary_fhir.uhc_drug_twin as twin
from process.formulary_fhir.repository import DatasetRef
from process.formulary_fhir.repository import DatasetVerification
from process.formulary_fhir.repository_admission_types import AlternativeProof
from process.formulary_fhir.repository_admission_types import TwinAdmissionResult
from process.formulary_fhir.uhc_drug_sync_contract import (
    UHCDrugSynchronizationResult,
)
from process.formulary_fhir.uhc_drug_receipt import UHCDrugRecordedAdmission
from tests.uhc_drug_receipt_test_support import admission_receipt
from tests.uhc_drug_receipt_test_support import artifact_acquisition_result
from tests.test_uhc_drug_sync import _binding
from tests.uhc_drug_parser_test_support import artifact_set
from tests.uhc_drug_parser_test_support import install_artifact_reader


CUTOFF = dt.datetime.now(dt.UTC) - dt.timedelta(minutes=5)


def _private_directory(path: Path) -> Path:
    path.mkdir()
    os.chmod(path, 0o700)
    return path


def _spool_evidence(monkeypatch, work_directory):
    artifacts, bodies_by_name = artifact_set()
    artifacts = replace(
        artifacts,
        artifacts=tuple(
            replace(artifact, verified_at=CUTOFF - dt.timedelta(minutes=1))
            for artifact in artifacts.artifacts
        ),
    )
    install_artifact_reader(monkeypatch, spool, bodies_by_name)
    evidence = spool.materialize_uhc_drug_spool(
        artifacts,
        spool_path=work_directory / "source.sqlite",
    )
    return artifacts, evidence


def _synchronization(
    evidence,
    *,
    run_id: str,
    dataset_digit: str,
    intent: str,
) -> UHCDrugSynchronizationResult:
    dataset_id = "ffd_" + dataset_digit * 48
    dataset = DatasetRef(
        source_id=evidence.source_id,
        dataset_id=dataset_id,
        run_id=run_id,
        previous_dataset_id=None,
        cutoff_at=CUTOFF,
        acquisition_contract_hash="c" * 64,
        intent=intent,
        status="verified",
    )
    verification = DatasetVerification(
        source_id=evidence.source_id,
        dataset_id=dataset_id,
        list_count=evidence.plan_count,
        alias_count=evidence.plan_count,
        medication_membership_count=evidence.medication_membership_count,
        coverage_hash="a" * 64,
        membership_hash="b" * 64,
    )
    return UHCDrugSynchronizationResult(
        dataset=dataset,
        verification=verification,
        evidence=evidence,
        full_alias_count=evidence.plan_count,
        resumed_alias_count=0,
    )


def _admission(
    baseline: UHCDrugSynchronizationResult,
    candidate: UHCDrugSynchronizationResult,
) -> TwinAdmissionResult:
    verified_at = candidate.dataset.cutoff_at + dt.timedelta(minutes=1)
    return TwinAdmissionResult(
        source_id=candidate.dataset.source_id,
        baseline_dataset_id=baseline.dataset.dataset_id,
        baseline_run_id=baseline.dataset.run_id,
        candidate_dataset_id=candidate.dataset.dataset_id,
        candidate_run_id=candidate.dataset.run_id,
        predecessor_dataset_id=None,
        cutoff_at=candidate.dataset.cutoff_at,
        source_configuration_hash="d" * 64,
        acquisition_contract_hash=(
            candidate.dataset.acquisition_contract_hash
        ),
        verification=candidate.verification,
        alternative=AlternativeProof(0, "e" * 64),
        baseline_verified_at=verified_at,
        candidate_verified_at=verified_at,
        admitted_at=verified_at,
    )


class _TwinBuildRecorder:
    def __init__(self, work_directory, evidence, baseline, candidate, admission):
        self.work_directory = work_directory
        self.evidence = evidence
        self.baseline = baseline
        self.candidate = candidate
        self.admission = admission
        self.events: list[str] = []

    async def materialize(self, *_args, **_kwargs):
        self.events.append("materialize")
        return (
            self.work_directory / "baseline.sqlite",
            self.evidence,
            self.work_directory / "candidate.sqlite",
            self.evidence,
        )

    async def synchronize_baseline(self, **values):
        self.events.append("baseline")
        assert values["run_id"] == "uhc-baseline"
        return self.baseline

    async def synchronize_candidate(self, **values):
        self.events.append("candidate")
        assert values["run_id"] == "uhc-candidate"
        return self.candidate

    async def artifacts_unchanged(self, _request):
        self.events.append("artifacts")

    async def source_unchanged(self, *_args, **_kwargs):
        self.events.append("source")

    async def admit(self, **values):
        self.events.append("admit")
        assert values["baseline"] == self.baseline.dataset
        assert values["candidate"] == self.candidate.dataset
        return self.admission


def _install_twin_build_recorder(monkeypatch, recorder):
    monkeypatch.setattr(twin, "_materialize_independent_spools", recorder.materialize)
    monkeypatch.setattr(
        twin,
        "synchronize_uhc_drug_dataset",
        recorder.synchronize_baseline,
    )
    monkeypatch.setattr(
        twin,
        "_synchronize_requested_uhc_drug_dataset",
        recorder.synchronize_candidate,
    )
    monkeypatch.setattr(twin, "admit_verified_twins", recorder.admit)
    monkeypatch.setattr(
        twin,
        "_require_artifacts_unchanged",
        recorder.artifacts_unchanged,
    )
    monkeypatch.setattr(twin, "require_source_unchanged", recorder.source_unchanged)


@pytest.mark.asyncio
async def test_recorded_twin_writes_receipt_before_lease_exit(
    monkeypatch,
    tmp_path,
) -> None:
    """The production twin seam cannot release its lease without a receipt."""

    work_directory = _private_directory(tmp_path / "work")
    artifacts, evidence = _spool_evidence(monkeypatch, work_directory)
    baseline = _synchronization(
        evidence,
        run_id="uhc-baseline",
        dataset_digit="1",
        intent="none",
    )
    candidate = _synchronization(
        evidence,
        run_id="uhc-candidate",
        dataset_digit="2",
        intent="requested",
    )
    twin_result = twin.UHCDrugTwinResult(
        _admission(baseline, candidate),
        baseline,
        candidate,
    )
    expected_receipt = admission_receipt(twin_result)
    acquisition = artifact_acquisition_result(artifacts)
    events: list[str] = []

    @asynccontextmanager
    async def source_lease(*_args, **_kwargs):
        events.append("lease-enter")
        yield
        events.append("lease-exit")

    async def build_and_record(_request, observed_acquisition):
        assert observed_acquisition is acquisition
        events.append("receipt-recorded")
        return UHCDrugRecordedAdmission(twin_result, expected_receipt)

    monkeypatch.setattr(twin.manual_lock, "manual_source_lease", source_lease)
    monkeypatch.setattr(twin, "_verify_and_record_under_lease", build_and_record)

    observed = await twin.verify_and_record_uhc_drug_twins(
        acquisition=acquisition,
        baseline_run_id=baseline.dataset.run_id,
        candidate_run_id=candidate.dataset.run_id,
        cutoff=candidate.dataset.cutoff_at,
        work_directory=work_directory,
        database=object(),
        repository=object(),
    )

    assert observed.receipt is expected_receipt
    assert events == ["lease-enter", "receipt-recorded", "lease-exit"]


@pytest.mark.asyncio
async def test_internal_recording_reuses_admission_after_crash(
    monkeypatch,
    tmp_path,
) -> None:
    """An admission without a receipt is repaired by the exact rerun."""

    work_directory = _private_directory(tmp_path / "work")
    artifacts, evidence = _spool_evidence(monkeypatch, work_directory)
    baseline = _synchronization(
        evidence,
        run_id="uhc-baseline",
        dataset_digit="1",
        intent="none",
    )
    candidate = _synchronization(
        evidence,
        run_id="uhc-candidate",
        dataset_digit="2",
        intent="requested",
    )
    twin_result = twin.UHCDrugTwinResult(
        _admission(baseline, candidate),
        baseline,
        candidate,
    )
    expected_receipt = admission_receipt(twin_result)
    acquisition = artifact_acquisition_result(artifacts)
    request = twin._validated_twin_request(
        artifacts,
        baseline.dataset.run_id,
        candidate.dataset.run_id,
        candidate.dataset.cutoff_at,
        work_directory,
        object(),
        object(),
    )
    build = AsyncMock(return_value=twin_result)
    receipt_writer = AsyncMock(return_value=expected_receipt)
    monkeypatch.setattr(twin, "_build_and_admit_twins", build)
    monkeypatch.setattr(
        receipt_module,
        "_record_receipt_under_lease",
        receipt_writer,
    )

    observed = await twin._verify_and_record_under_lease(
        request,
        acquisition,
    )

    assert observed == UHCDrugRecordedAdmission(twin_result, expected_receipt)
    build.assert_awaited_once()
    receipt_writer.assert_awaited_once_with(
        acquisition=acquisition,
        twin_result=twin_result,
        database=request.database,
    )


@pytest.mark.asyncio
async def test_independent_spools_are_distinct_and_identical(
    monkeypatch,
    tmp_path,
) -> None:
    artifacts, bodies_by_name = artifact_set()
    artifacts = replace(
        artifacts,
        artifacts=tuple(
            replace(artifact, verified_at=CUTOFF - dt.timedelta(minutes=1))
            for artifact in artifacts.artifacts
        ),
    )
    install_artifact_reader(monkeypatch, spool, bodies_by_name)
    work_directory = _private_directory(tmp_path / "work")

    baseline_path, baseline, candidate_path, candidate = (
        await twin._materialize_independent_spools(
            artifacts,
            work_directory,
        )
    )

    assert baseline_path != candidate_path
    assert baseline_path.exists() and candidate_path.exists()
    assert baseline == candidate


@pytest.mark.asyncio
async def test_build_orders_baseline_candidate_admission_and_postflight(
    monkeypatch,
    tmp_path,
) -> None:
    work_directory = _private_directory(tmp_path / "work")
    artifacts, evidence = _spool_evidence(monkeypatch, work_directory)
    baseline = _synchronization(
        evidence,
        run_id="uhc-baseline",
        dataset_digit="1",
        intent="none",
    )
    candidate = _synchronization(
        evidence,
        run_id="uhc-candidate",
        dataset_digit="2",
        intent="requested",
    )
    admission = _admission(baseline, candidate)
    recorder = _TwinBuildRecorder(
        work_directory,
        evidence,
        baseline,
        candidate,
        admission,
    )
    monkeypatch.setattr(
        twin,
        "register_uhc_formulary_source",
        AsyncMock(return_value=_binding()),
    )
    _install_twin_build_recorder(monkeypatch, recorder)
    request = twin._validated_twin_request(
        artifacts,
        "uhc-baseline",
        "uhc-candidate",
        candidate.dataset.cutoff_at,
        work_directory,
        object(),
        object(),
    )

    twin_result = await twin._build_and_admit_twins(
        request,
        work_directory,
    )

    assert twin_result.admission is admission
    assert recorder.events == [
        "materialize",
        "baseline",
        "candidate",
        "artifacts",
        "source",
        "admit",
    ]


@pytest.mark.asyncio
async def test_public_twin_verifier_holds_one_source_lease(
    monkeypatch,
    tmp_path,
) -> None:
    work_directory = _private_directory(tmp_path / "work")
    artifacts, evidence = _spool_evidence(monkeypatch, work_directory)
    baseline = _synchronization(
        evidence,
        run_id="uhc-baseline",
        dataset_digit="1",
        intent="none",
    )
    candidate = _synchronization(
        evidence,
        run_id="uhc-candidate",
        dataset_digit="2",
        intent="requested",
    )
    expected = twin.UHCDrugTwinResult(
        _admission(baseline, candidate),
        baseline,
        candidate,
    )
    lease_events: list[str] = []

    @asynccontextmanager
    async def source_lease(*_args, **_kwargs):
        lease_events.append("enter")
        yield
        lease_events.append("exit")

    verify_under_lease = AsyncMock(return_value=expected)
    monkeypatch.setattr(twin.manual_lock, "manual_source_lease", source_lease)
    monkeypatch.setattr(twin, "_verify_twins_under_lease", verify_under_lease)

    observed = await twin.verify_uhc_drug_twins(
        artifacts=artifacts,
        baseline_run_id="uhc-baseline",
        candidate_run_id="uhc-candidate",
        cutoff=candidate.dataset.cutoff_at,
        work_directory=work_directory,
        database=object(),
        repository=object(),
    )

    assert observed is expected
    assert lease_events == ["enter", "exit"]
    verify_under_lease.assert_awaited_once()


def test_twin_request_rejects_shared_identity_and_unsafe_workdir(
    monkeypatch,
    tmp_path,
) -> None:
    work_directory = _private_directory(tmp_path / "work")
    artifacts, _evidence = _spool_evidence(monkeypatch, work_directory)
    cutoff = dt.datetime.now(dt.UTC) - dt.timedelta(minutes=1)

    with pytest.raises(ValueError, match="request is invalid"):
        twin._validated_twin_request(
            artifacts,
            "same-run",
            "same-run",
            cutoff,
            work_directory,
            object(),
            None,
        )

    unsafe_directory = tmp_path / "unsafe"
    unsafe_directory.mkdir()
    os.chmod(unsafe_directory, 0o755)
    with pytest.raises(ValueError, match="work directory is invalid"):
        twin._validated_twin_request(
            artifacts,
            "baseline-run",
            "candidate-run",
            cutoff,
            unsafe_directory,
            object(),
            None,
        )
