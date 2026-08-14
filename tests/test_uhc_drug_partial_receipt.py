# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import process.formulary_fhir.uhc_drug_receipt as receipt_module
from process.formulary_fhir.uhc_drug_receipt import uhc_drug_receipt_id
from tests.test_uhc_drug_receipt import _Database
from tests.uhc_drug_parser_test_support import artifact_set
from tests.uhc_drug_receipt_test_support import OBSERVATION_SHA256
from tests.uhc_drug_receipt_test_support import admission_receipt
from tests.uhc_drug_receipt_test_support import admitted_twin
from tests.uhc_drug_receipt_test_support import artifact_acquisition_result
from tests.uhc_drug_receipt_test_support import source_binding


def test_partial_receipt_identity_binds_selection_and_preserves_legacy_full() -> None:
    full_twin, full_artifacts = admitted_twin()
    full_receipt = admission_receipt(full_twin)
    full_evidence = full_receipt.evidence
    full_ids = tuple(artifact.identity.source_file_id for artifact in full_artifacts.artifacts)
    legacy_full_id = uhc_drug_receipt_id(
        full_receipt.source_id,
        full_receipt.candidate_dataset_id,
        OBSERVATION_SHA256,
        full_evidence.source_file_set_sha256,
        full_evidence.artifact_set_sha256,
        full_evidence.spool_content_sha256,
    )
    assert full_receipt.receipt_id == legacy_full_id
    assert uhc_drug_receipt_id(
        full_receipt.source_id,
        full_receipt.candidate_dataset_id,
        OBSERVATION_SHA256,
        full_evidence.source_file_set_sha256,
        full_evidence.artifact_set_sha256,
        full_evidence.spool_content_sha256,
        selected_source_file_ids_value=full_ids,
    ) == legacy_full_id

    partial_twin, partial_artifacts = admitted_twin(selected_file_count=47)
    partial_receipt = admission_receipt(partial_twin)
    partial_ids = tuple(artifact.identity.source_file_id for artifact in partial_artifacts.artifacts)
    assert partial_receipt.is_coverage_complete is False
    assert partial_receipt.expected_file_count == 48
    assert partial_receipt.excluded_file_count == 1
    assert partial_receipt.receipt_id == uhc_drug_receipt_id(
        partial_receipt.source_id,
        partial_receipt.candidate_dataset_id,
        OBSERVATION_SHA256,
        partial_receipt.evidence.source_file_set_sha256,
        partial_receipt.evidence.artifact_set_sha256,
        partial_receipt.evidence.spool_content_sha256,
        selected_source_file_ids_value=partial_ids,
        exclusion_code="not_selected",
    )
    assert partial_receipt.receipt_id != legacy_full_id


@pytest.mark.parametrize("selected_file_count", (1, 46, 47, 48))
def test_receipt_accepts_every_nonempty_validated_subset(selected_file_count: int) -> None:
    twin_result, _artifacts = admitted_twin(selected_file_count=selected_file_count)
    receipt = admission_receipt(twin_result)
    assert receipt.evidence.file_count == selected_file_count
    assert receipt.expected_file_count == 48
    assert receipt.excluded_file_count == 48 - selected_file_count
    assert receipt.is_coverage_complete is (selected_file_count == 48)


def test_partial_receipt_rejects_selection_or_coverage_drift() -> None:
    twin_result, _artifacts = admitted_twin(selected_file_count=47)
    receipt = admission_receipt(twin_result)
    with pytest.raises(ValueError, match="receipt is inconsistent"):
        replace(receipt, selected_source_file_ids=tuple(reversed(receipt.selected_source_file_ids)))
    with pytest.raises(ValueError, match="receipt is inconsistent"):
        replace(receipt, expected_file_count=47)
    with pytest.raises(ValueError, match="receipt is inconsistent"):
        replace(receipt, exclusion_code=None)


def _partial_record_case(monkeypatch):
    twin_result, artifacts = admitted_twin(selected_file_count=47)
    expected = admission_receipt(twin_result)
    acquisition = artifact_acquisition_result(artifacts)
    full_artifacts, _bodies_by_name = artifact_set()
    full_identities = tuple(artifact.identity for artifact in full_artifacts.artifacts)
    full_loader = AsyncMock(return_value=full_identities)
    selected_loader = AsyncMock(return_value=artifacts)
    complete_loader = AsyncMock()
    monkeypatch.setattr(receipt_module, "register_uhc_formulary_source", AsyncMock(return_value=source_binding()))
    monkeypatch.setattr(receipt_module, "load_source_artifact_identities", full_loader)
    monkeypatch.setattr(receipt_module, "load_selected_source_artifact_set", selected_loader)
    monkeypatch.setattr(receipt_module, "load_complete_source_artifact_set", complete_loader)
    monkeypatch.setattr(
        receipt_module, "load_uhc_receipt_admission", AsyncMock(return_value=twin_result.admission)
    )
    monkeypatch.setattr(receipt_module, "require_source_unchanged", AsyncMock())
    return SimpleNamespace(
        twin_result=twin_result,
        artifacts=artifacts,
        acquisition=acquisition,
        expected=expected,
        database=_Database(expected),
        full_identities=full_identities,
        full_loader=full_loader,
        selected_loader=selected_loader,
        complete_loader=complete_loader,
    )


@pytest.mark.asyncio
async def test_record_partial_receipt_persists_canonical_selection(monkeypatch) -> None:
    case = _partial_record_case(monkeypatch)
    observed = await receipt_module._record_receipt_under_lease(
        acquisition=case.acquisition,
        twin_result=case.twin_result,
        database=case.database,
    )
    assert observed == case.expected
    case.complete_loader.assert_not_awaited()
    case.full_loader.assert_awaited_once_with(
        case.artifacts.source_id, case.artifacts.source_file_set_sha256, database=case.database
    )
    case.selected_loader.assert_awaited_once_with(
        case.full_identities,
        selected_source_file_ids=case.expected.selected_source_file_ids,
        require_unselected_pending=False,
        database=case.database,
    )
    assert case.database.insert_values is not None
    assert case.database.insert_values["selected_source_file_ids"] == case.expected.selected_source_file_ids
    assert case.database.insert_values["expected_file_count"] == 48
    assert case.database.insert_values["excluded_file_count"] == 1
    assert case.database.insert_values["exclusion_code"] == "not_selected"


@pytest.mark.asyncio
async def test_record_partial_receipt_rejects_unwitnessed_selection(
    monkeypatch,
) -> None:
    case = _partial_record_case(monkeypatch)
    unwitnessed = replace(
        case.acquisition,
        excluded_source_file_ids=("0" * 64,),
    )

    with pytest.raises(RuntimeError, match="selection changed"):
        await receipt_module._record_receipt_under_lease(
            acquisition=unwitnessed,
            twin_result=case.twin_result,
            database=case.database,
        )

    assert case.database.insert_values is None
