# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Operator-contract boundary proof for the dormant UHC formulary flow."""

from __future__ import annotations

import datetime as dt
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import Mock

import pytest

import process.formulary_fhir.uhc_drug_operation as operation
import process.formulary_fhir.uhc_drug_acquire_operation as acquire_operation
import process.formulary_fhir.uhc_drug_publish_operation as publish_operation
from tests.test_uhc_drug_operation import _acquisition_fixture
from tests.test_uhc_drug_operation import PAST_CUTOFF
from tests.test_uhc_drug_operation import _receipt_evidence


def test_run_identity_contract_rejects_collisions_and_text_drift() -> None:
    with pytest.raises(ValueError, match="run identities"):
        operation.UHCDrugRunIdentities(
            "same-run",
            "same-run",
            PAST_CUTOFF,
            "2026-08-09T12:00:00Z",
        )
    with pytest.raises(ValueError, match="run identities"):
        operation.UHCDrugRunIdentities(
            "baseline-run",
            "candidate-run",
            PAST_CUTOFF,
            "2026-08-09T12:00:01Z",
        )
    identities = operation.uhc_drug_run_identities(
        "1" * 64,
        "2" * 64,
        "3" * 64,
        PAST_CUTOFF,
    )
    assert "roots=<redacted>" in repr(identities)


def test_publication_result_rejects_generation_and_time_drift() -> None:
    evidence = _receipt_evidence()
    with pytest.raises(ValueError, match="publication result"):
        operation.UHCDrugPublicationOperationResult(
            evidence,
            0,
            evidence.admitted_at,
        )
    with pytest.raises(ValueError, match="publication result"):
        operation.UHCDrugPublicationOperationResult(
            evidence,
            1,
            evidence.admitted_at - dt.timedelta(microseconds=1),
        )


def test_internal_gate_name_fails_closed(monkeypatch) -> None:
    monkeypatch.delenv(operation.ACQUISITION_ENABLED_ENV, raising=False)
    monkeypatch.delenv(operation.PUBLICATION_ENABLED_ENV, raising=False)
    with pytest.raises(operation.UHCDrugOperationError) as caught:
        operation._require_gate("unknown")
    assert caught.value.code == "invalid_request"


def test_work_directory_normalizes_path_resolution_failures(monkeypatch) -> None:
    monkeypatch.setenv(operation.WORK_DIRECTORY_ENV, "/synthetic/private")
    monkeypatch.setattr(operation, "Path", Mock(side_effect=ValueError("path")))
    with pytest.raises(operation.UHCDrugOperationError) as caught:
        operation.uhc_drug_work_directory()
    assert caught.value.code == "invalid_request"


def test_run_identities_reject_future_cutoff() -> None:
    with pytest.raises(operation.UHCDrugOperationError) as caught:
        operation.uhc_drug_run_identities(
            "1" * 64,
            "2" * 64,
            "3" * 64,
            dt.datetime.now(dt.UTC) + dt.timedelta(days=1),
        )
    assert caught.value.code == "invalid_request"


@pytest.mark.parametrize(
    ("error", "default_code", "expected_code"),
    (
        (RuntimeError("plain"), "publication", "publication"),
        (operation.UHCDrugOperationError("busy"), "publication", "busy"),
        (operation.UHCDrugOperationError("evidence"), "publication", "publication"),
    ),
)
def test_operation_error_allows_only_public_codes(
    error,
    default_code,
    expected_code,
) -> None:
    assert operation.uhc_operation_error(error, default_code).code == expected_code


def test_serializers_reject_wrong_types_and_forged_publication() -> None:
    with pytest.raises(operation.UHCDrugOperationError) as admission_error:
        operation.admission_result_json(object())
    assert admission_error.value.code == "evidence"

    with pytest.raises(operation.UHCDrugOperationError) as publication_error:
        operation.publication_result_json(object())
    assert publication_error.value.code == "evidence"

    evidence = _receipt_evidence()
    publication_result = operation.UHCDrugPublicationOperationResult(
        evidence,
        1,
        evidence.admitted_at,
    )
    object.__setattr__(publication_result, "generation", 0)
    with pytest.raises(operation.UHCDrugOperationError) as forged_error:
        operation.publication_result_json(publication_result)
    assert forged_error.value.code == "evidence"


def test_acquisition_result_rejects_cross_receipt_drift(monkeypatch) -> None:
    acquisition, identities, recorded = _acquisition_fixture()
    changed_acquisition = replace(
        acquisition,
        source_observation_sha256="e" * 64,
    )
    with pytest.raises(operation.UHCDrugOperationError) as mismatch_error:
        acquire_operation._admission_result(
            changed_acquisition,
            recorded,
            identities,
        )
    assert mismatch_error.value.code == "evidence"

    monkeypatch.setattr(
        acquire_operation,
        "receipt_operation_evidence",
        Mock(side_effect=ValueError("synthetic evidence")),
    )
    with pytest.raises(operation.UHCDrugOperationError) as constructor_error:
        acquire_operation._admission_result(acquisition, recorded, identities)
    assert constructor_error.value.code == "evidence"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "expected_code"),
    (
        (TimeoutError("timeout"), None),
        (operation.UHCDrugOperationError("busy"), "busy"),
        (ValueError("invalid"), "invalid_request"),
        (RuntimeError("failed"), "acquisition"),
    ),
)
async def test_acquisition_operation_normalizes_phase_failures(
    monkeypatch,
    failure,
    expected_code,
) -> None:
    monkeypatch.setattr(acquire_operation, "require_uhc_acquisition_gate", Mock())
    monkeypatch.setattr(
        acquire_operation,
        "uhc_drug_work_directory",
        Mock(side_effect=failure),
    )
    monkeypatch.setattr(
        acquire_operation,
        "uhc_operation_error",
        lambda _error, code: operation.UHCDrugOperationError(code),
    )

    if expected_code is None:
        with pytest.raises(TimeoutError):
            await acquire_operation.acquire_and_admit_uhc_drugs(
                raw_set_sha256="a" * 64
            )
        return
    with pytest.raises(operation.UHCDrugOperationError) as caught:
        await acquire_operation.acquire_and_admit_uhc_drugs(
            raw_set_sha256="a" * 64
        )
    assert caught.value.code == expected_code


@pytest.mark.asyncio
async def test_publication_operation_rejects_evidence_and_dataset_drift(
    monkeypatch,
) -> None:
    monkeypatch.setattr(publish_operation, "require_uhc_publication_gate", Mock())
    monkeypatch.setattr(
        publish_operation,
        "validate_uhc_drug_receipt_id",
        lambda receipt_id: receipt_id,
    )
    monkeypatch.setattr(
        publish_operation,
        "load_uhc_drug_admission_receipt",
        AsyncMock(return_value=object()),
    )
    monkeypatch.setattr(
        publish_operation,
        "publish_admitted_uhc_drug_candidate",
        AsyncMock(return_value=SimpleNamespace(dataset_id="ffd_" + "1" * 48)),
    )
    monkeypatch.setattr(
        publish_operation,
        "receipt_operation_evidence",
        Mock(side_effect=TypeError("synthetic evidence")),
    )
    with pytest.raises(operation.UHCDrugOperationError) as evidence_error:
        await publish_operation.publish_uhc_drug_receipt(receipt_id="ffur_test")
    assert evidence_error.value.code == "evidence"

    monkeypatch.setattr(
        publish_operation,
        "receipt_operation_evidence",
        Mock(return_value=SimpleNamespace(candidate_dataset_id="ffd_" + "2" * 48)),
    )
    with pytest.raises(operation.UHCDrugOperationError) as dataset_error:
        await publish_operation.publish_uhc_drug_receipt(receipt_id="ffur_test")
    assert dataset_error.value.code == "evidence"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "expected_code"),
    (
        (TimeoutError("timeout"), None),
        (operation.UHCDrugOperationError("busy"), "busy"),
        (ValueError("invalid"), "invalid_request"),
        (RuntimeError("failed"), "publication"),
    ),
)
async def test_publication_operation_normalizes_phase_failures(
    monkeypatch,
    failure,
    expected_code,
) -> None:
    monkeypatch.setattr(publish_operation, "require_uhc_publication_gate", Mock())
    monkeypatch.setattr(
        publish_operation,
        "validate_uhc_drug_receipt_id",
        Mock(side_effect=failure),
    )
    monkeypatch.setattr(
        publish_operation,
        "uhc_operation_error",
        lambda _error, code: operation.UHCDrugOperationError(code),
    )

    if expected_code is None:
        with pytest.raises(TimeoutError):
            await publish_operation.publish_uhc_drug_receipt(
                receipt_id="ffur_test"
            )
        return
    with pytest.raises(operation.UHCDrugOperationError) as caught:
        await publish_operation.publish_uhc_drug_receipt(receipt_id="ffur_test")
    assert caught.value.code == expected_code
