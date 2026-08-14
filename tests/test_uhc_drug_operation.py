# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Default-off operation and aggregate evidence contracts for UHC drugs."""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from process.formulary_fhir.repository_shared import PublicationResult
import process.formulary_fhir.uhc_drug_acquire_operation as acquire_operation
import process.formulary_fhir.uhc_drug_operation as operation
from process.formulary_fhir.uhc_drug_receipt import UHCDrugRecordedAdmission
import process.formulary_fhir.uhc_drug_publish_operation as publish_operation
from tests.uhc_drug_receipt_test_support import admission_receipt
from tests.uhc_drug_receipt_test_support import admitted_twin
from tests.uhc_drug_receipt_test_support import artifact_acquisition_result


PAST_CUTOFF = dt.datetime(2026, 8, 9, 12, tzinfo=dt.UTC)


def _private_work_directory(tmp_path: Path, monkeypatch) -> Path:
    work_directory = tmp_path.resolve() / "uhc-work"
    work_directory.mkdir(mode=0o700)
    work_directory.chmod(0o700)
    monkeypatch.setenv(operation.WORK_DIRECTORY_ENV, str(work_directory))
    return work_directory


def _receipt_evidence():
    twin_result, _artifacts = admitted_twin()
    return operation.receipt_operation_evidence(
        admission_receipt(twin_result)
    )


def _acquisition_fixture():
    twin_result, artifacts = admitted_twin()
    receipt = admission_receipt(twin_result)
    admission = twin_result.admission
    acquisition = artifact_acquisition_result(
        artifacts,
        observation_sha256=receipt.source_observation_sha256,
        downloaded_file_count=7,
        downloaded_byte_count=12345,
    )
    identities = operation.UHCDrugRunIdentities(
        admission.baseline_run_id,
        admission.candidate_run_id,
        admission.cutoff_at,
        admission.cutoff_at.isoformat().replace("+00:00", "Z"),
    )
    return acquisition, identities, UHCDrugRecordedAdmission(
        twin_result,
        receipt,
    )


def _install_acquisition_fixture(
    monkeypatch,
    tmp_path: Path,
    acquisition,
    identities,
    recorded,
) -> None:
    monkeypatch.setenv(operation.ACQUISITION_ENABLED_ENV, "true")
    monkeypatch.delenv(operation.PUBLICATION_ENABLED_ENV, raising=False)
    _private_work_directory(tmp_path, monkeypatch)
    monkeypatch.setattr(
        acquire_operation,
        "acquire_current_uhc_drug_artifacts",
        AsyncMock(return_value=acquisition),
    )
    monkeypatch.setattr(
        acquire_operation,
        "uhc_drug_run_identities",
        lambda *_arguments: identities,
    )
    monkeypatch.setattr(
        acquire_operation,
        "verify_and_record_uhc_drug_twins",
        AsyncMock(return_value=recorded),
    )


@pytest.mark.parametrize(
    ("acquisition_value", "publication_value", "expected_code"),
    [
        (None, None, "disabled"),
        ("false", "false", "disabled"),
        ("TRUE", None, "disabled"),
        ("true", "true", "gate_conflict"),
    ],
)
def test_operation_gates_are_exact_default_off_and_exclusive(
    monkeypatch,
    acquisition_value,
    publication_value,
    expected_code,
) -> None:
    """Only one lowercase exact gate may authorize one phase."""

    for variable_name, variable_value in (
        (operation.ACQUISITION_ENABLED_ENV, acquisition_value),
        (operation.PUBLICATION_ENABLED_ENV, publication_value),
    ):
        if variable_value is None:
            monkeypatch.delenv(variable_name, raising=False)
        else:
            monkeypatch.setenv(variable_name, variable_value)

    with pytest.raises(operation.UHCDrugOperationError) as caught:
        operation.require_uhc_acquisition_gate()

    assert caught.value.code == expected_code


def test_phase_specific_gate_accepts_only_its_phase(monkeypatch) -> None:
    """Acquisition and publication gates never authorize each other."""

    monkeypatch.setenv(operation.ACQUISITION_ENABLED_ENV, "true")
    monkeypatch.delenv(operation.PUBLICATION_ENABLED_ENV, raising=False)
    operation.require_uhc_acquisition_gate()
    with pytest.raises(operation.UHCDrugOperationError) as caught:
        operation.require_uhc_publication_gate()
    assert caught.value.code == "disabled"


def test_work_directory_requires_exact_private_owned_path(
    tmp_path,
    monkeypatch,
) -> None:
    """Spools can only use an existing resolved owner-private directory."""

    work_directory = _private_work_directory(tmp_path, monkeypatch)
    assert operation.uhc_drug_work_directory() == work_directory

    work_directory.chmod(0o755)
    with pytest.raises(operation.UHCDrugOperationError) as caught:
        operation.uhc_drug_work_directory()
    assert caught.value.code == "invalid_request"


def test_run_identities_are_stable_distinct_and_rotate_every_root() -> None:
    """Both root IDs bind the observation, file set, content, and cutoff."""

    original = operation.uhc_drug_run_identities(
        "1" * 64,
        "2" * 64,
        "3" * 64,
        PAST_CUTOFF,
    )
    replay = operation.uhc_drug_run_identities(
        "1" * 64,
        "2" * 64,
        "3" * 64,
        PAST_CUTOFF,
    )

    assert original == replay
    assert original.baseline_run_id.startswith("ffua_")
    assert original.candidate_run_id.startswith("ffub_")
    assert original.baseline_run_id != original.candidate_run_id
    for changed_arguments in (
        ("4" * 64, "2" * 64, "3" * 64, PAST_CUTOFF),
        ("1" * 64, "4" * 64, "3" * 64, PAST_CUTOFF),
        ("1" * 64, "2" * 64, "4" * 64, PAST_CUTOFF),
        (
            "1" * 64,
            "2" * 64,
            "3" * 64,
            PAST_CUTOFF + dt.timedelta(seconds=1),
        ),
    ):
        assert operation.uhc_drug_run_identities(*changed_arguments) != original


@pytest.mark.asyncio
async def test_acquisition_gate_and_request_validation_precede_io(
    monkeypatch,
    tmp_path,
) -> None:
    """A disabled or malformed request cannot touch retained state or network."""

    acquire = AsyncMock()
    monkeypatch.setattr(
        acquire_operation,
        "acquire_current_uhc_drug_artifacts",
        acquire,
    )
    monkeypatch.delenv(operation.ACQUISITION_ENABLED_ENV, raising=False)
    with pytest.raises(operation.UHCDrugOperationError) as caught:
        await acquire_operation.acquire_and_admit_uhc_drugs(
            raw_set_sha256="not-a-hash"
        )
    assert caught.value.code == "disabled"
    acquire.assert_not_awaited()

    monkeypatch.setenv(operation.ACQUISITION_ENABLED_ENV, "true")
    _private_work_directory(tmp_path, monkeypatch)
    with pytest.raises(operation.UHCDrugOperationError) as caught:
        await acquire_operation.acquire_and_admit_uhc_drugs(
            raw_set_sha256="not-a-hash"
        )
    assert caught.value.code == "invalid_request"
    acquire.assert_not_awaited()


@pytest.mark.asyncio
async def test_acquisition_returns_full_receipt_bound_evidence(
    monkeypatch,
    tmp_path,
) -> None:
    """The acquire phase emits both root IDs and every durable proof hash."""

    acquisition, identities, recorded = _acquisition_fixture()
    _install_acquisition_fixture(
        monkeypatch,
        tmp_path,
        acquisition,
        identities,
        recorded,
    )
    receipt = recorded.receipt
    admission = recorded.twin_result.admission

    database = object()
    operation_result = await acquire_operation.acquire_and_admit_uhc_drugs(
        raw_set_sha256="a" * 64,
        database=database,
    )
    response_by_field = json.loads(
        operation.admission_result_json(operation_result)
    )

    assert response_by_field["status"] == "admitted"
    assert response_by_field["receipt_id"] == receipt.receipt_id
    assert response_by_field["baseline_run_id"] == admission.baseline_run_id
    assert response_by_field["candidate_run_id"] == admission.candidate_run_id
    assert (
        response_by_field["artifact_set_sha256"]
        == acquisition.artifact_set_sha256
    )
    assert response_by_field["downloaded_file_count"] == 7
    assert response_by_field["reused_file_count"] == 41
    assert response_by_field["downloaded_byte_count"] == 12345
    assert response_by_field["coverage"] == {
        "status": "complete",
        "expected_artifact_count": 48,
        "included_artifact_count": 48,
        "missing_artifact_count": 0,
    }
    acquire_operation.verify_and_record_uhc_drug_twins.assert_awaited_once_with(
        acquisition=acquisition,
        baseline_run_id=identities.baseline_run_id,
        candidate_run_id=identities.candidate_run_id,
        cutoff=identities.cutoff_at,
        work_directory=operation.uhc_drug_work_directory(),
        database=database,
    )


def test_partial_receipt_output_reports_only_aggregate_coverage() -> None:
    """A selected receipt is truthful without exposing omitted identities."""

    twin_result, _artifacts = admitted_twin(selected_file_count=47)
    receipt = admission_receipt(twin_result)
    evidence = operation.receipt_operation_evidence(receipt)
    result = operation.UHCDrugAdmissionOperationResult(
        evidence=evidence,
        downloaded_file_count=1,
        reused_file_count=46,
        downloaded_byte_count=123,
    )

    response_by_field = json.loads(operation.admission_result_json(result))

    assert response_by_field["file_count"] == 47
    assert response_by_field["coverage"] == {
        "status": "partial",
        "expected_artifact_count": 48,
        "included_artifact_count": 47,
        "missing_artifact_count": 1,
    }
    assert "selected_source_file_ids" not in response_by_field
    assert "exclusion_code" not in response_by_field


@pytest.mark.asyncio
async def test_publication_reloads_receipt_and_binds_result(monkeypatch) -> None:
    """Published output is reconstructed from the durable receipt."""

    twin_result, _artifacts = admitted_twin()
    receipt = admission_receipt(twin_result)
    published_at = receipt.recorded_at + dt.timedelta(minutes=1)
    publication = PublicationResult(
        receipt.source_id,
        receipt.candidate_dataset_id,
        1,
        published_at,
    )
    monkeypatch.setenv(operation.PUBLICATION_ENABLED_ENV, "true")
    monkeypatch.delenv(operation.ACQUISITION_ENABLED_ENV, raising=False)
    load = AsyncMock(return_value=receipt)
    publish = AsyncMock(return_value=publication)
    monkeypatch.setattr(
        publish_operation,
        "load_uhc_drug_admission_receipt",
        load,
    )
    monkeypatch.setattr(
        publish_operation,
        "publish_admitted_uhc_drug_candidate",
        publish,
    )
    database = object()

    operation_result = await publish_operation.publish_uhc_drug_receipt(
        receipt_id=receipt.receipt_id,
        database=database,
    )
    response_by_field = json.loads(
        operation.publication_result_json(operation_result)
    )

    assert response_by_field["status"] == "published"
    assert response_by_field["receipt_id"] == receipt.receipt_id
    assert (
        response_by_field["candidate_dataset_id"]
        == receipt.candidate_dataset_id
    )
    assert response_by_field["generation"] == 1
    load.assert_awaited_once_with(
        receipt_id=receipt.receipt_id,
        database=database,
    )
    publish.assert_awaited_once_with(
        receipt_id=receipt.receipt_id,
        database=database,
    )


def test_serializers_revalidate_mutated_frozen_results() -> None:
    """A forged in-memory aggregate cannot bypass constructor validation."""

    receipt_evidence = _receipt_evidence()
    admission_result = operation.UHCDrugAdmissionOperationResult(
        receipt_evidence,
        0,
        48,
        0,
    )
    object.__setattr__(admission_result, "reused_file_count", 47)

    with pytest.raises(operation.UHCDrugOperationError) as caught:
        operation.admission_result_json(admission_result)

    assert caught.value.code == "evidence"
