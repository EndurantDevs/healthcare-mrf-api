# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed contracts and admission validation for Practitioner acquisition."""

from __future__ import annotations

import copy
from dataclasses import replace

import pytest

from process import uhc_flex_practitioner_acquisition as acquisition
from process import uhc_flex_practitioner_acquisition_contract as contract
from process import uhc_flex_practitioner_acquisition_runtime as runtime
from process.uhc_flex_official_cohort_store import UHCFlexOfficialCohortSyncResult
from process.uhc_flex_practitioner_twin_store_contract import (
    UHCFlexPractitionerTwinStoreError,
)
from tests.uhc_flex_practitioner_acquisition_test_support import (
    acquire_with_harness,
    AcquisitionHarness,
    cohort_fixture,
    enabled_config,
    OPERATION_KEY,
    PROJECTION_DATE,
    registration_fixture,
)


def _mutated(value, **changes):
    changed = copy.copy(value)
    for field_name, field_value in changes.items():
        object.__setattr__(changed, field_name, field_value)
    return changed


async def _admission_validation_fixture():
    harness = AcquisitionHarness(npi_count=1)
    receipt = await acquire_with_harness(harness)
    identity_by_role = {
        identity.acquisition_role: identity for identity in harness.identities.values()
    }
    summary_by_role = {
        role: harness.summaries[identity.acquisition_id]
        for role, identity in identity_by_role.items()
    }
    context = acquisition._AcquisitionContext(
        registration=registration_fixture(),
        cohort=cohort_fixture(npi_count=1),
        dataset_intent_id=receipt.dataset_intent_id,
        identity_by_role=identity_by_role,
        semantic_projection_as_of=PROJECTION_DATE,
        operation_key=OPERATION_KEY,
    )
    admission = next(iter(harness.admissions.values()))
    return harness, context, identity_by_role, summary_by_role, admission


@pytest.mark.parametrize("value", [True, "1", None, float("nan"), -0.1])
def test_duration_and_aggregate_contracts_reject_noncanonical_values(value):
    with pytest.raises(ValueError):
        contract.strict_nonnegative_seconds(value, "timing")

    with pytest.raises(ValueError):
        contract.UHCFlexPractitionerAcquisitionProgress(
            acquisition_role="other",
            phase="unknown",
            worker_count=0,
            claim_count=-1,
            retry_count=0,
            matched_count=0,
            unmatched_count=0,
            error_count=0,
        )


def test_runtime_accepts_the_documented_concurrency_ceiling():
    assert contract.UHC_FLEX_PRACTITIONER_ACQUISITION_MAX_CONCURRENCY == 32
    assert contract.UHCFlexPractitionerAcquisitionConfig(
        enabled=True,
        concurrency=32,
    ).concurrency == 32


@pytest.mark.asyncio
async def test_receipt_contracts_reject_each_closed_identity_boundary():
    receipt = await acquire_with_harness(AcquisitionHarness(npi_count=1))
    root = receipt.baseline
    invalid_root_changes = (
        {"acquisition_role": "other"},
        {"acquisition_id": object()},
        {"acquisition_id": "invalid"},
        {"run_id": object()},
        {"run_id": "invalid"},
        {"matched_count": True},
        {"unmatched_count": -1},
        {"resource_count": -1},
        {"terminal_set_sha256": object()},
        {"terminal_set_sha256": "invalid"},
    )
    for changes in invalid_root_changes:
        with pytest.raises(ValueError):
            replace(root, **changes)

    invalid_receipt_changes = (
        {"source_id": "other"},
        {"endpoint_id": object()},
        {"endpoint_id": "invalid"},
        {"official_dataset_id": object()},
        {"official_dataset_id": ""},
        {"official_dataset_hash": object()},
        {"official_dataset_hash": "invalid"},
        {"official_content_proof_sha256": object()},
        {"official_content_proof_sha256": "invalid"},
        {"dataset_intent_id": "pdufdi_" + "0" * 48},
        {"expected_npi_count": True},
        {"expected_npi_count": 0},
        {"baseline": object()},
        {"baseline": receipt.candidate},
        {"candidate": object()},
        {"candidate": receipt.baseline},
        {"candidate": _mutated(receipt.candidate, acquisition_id=root.acquisition_id)},
        {"baseline": _mutated(root, run_id="pdufpr_" + "0" * 48)},
        {"candidate": _mutated(receipt.candidate, run_id="pdufpr_" + "0" * 48)},
        {"baseline": _mutated(root, matched_count=0, unmatched_count=0)},
        {
            "candidate": _mutated(
                receipt.candidate,
                matched_count=0,
                unmatched_count=0,
            )
        },
        {"twin_attempt_id": object()},
        {"twin_attempt_id": "invalid"},
        {"admission_id": object()},
        {"admission_id": "invalid"},
    )
    for changes in invalid_receipt_changes:
        with pytest.raises((TypeError, ValueError)):
            replace(receipt, **changes)


def test_source_cohort_and_runtime_inputs_fail_closed():
    registration = registration_fixture()
    invalid_registration_values = (
        object(),
        _mutated(registration, source_id="other"),
        _mutated(registration, endpoint_id="0" * 64),
    )
    for invalid in invalid_registration_values:
        with pytest.raises(acquisition.UHCFlexPractitionerAcquisitionError):
            acquisition._validate_registration(invalid)

    malformed_sync = object.__new__(UHCFlexOfficialCohortSyncResult)
    object.__setattr__(malformed_sync, "cohort", object())
    object.__setattr__(malformed_sync, "created", False)
    for invalid in (object(), malformed_sync):
        with pytest.raises(acquisition.UHCFlexPractitionerAcquisitionError):
            acquisition._validated_cohort_sync(invalid)

    with pytest.raises(ValueError):
        acquisition._validated_runtime_inputs(
            OPERATION_KEY,
            PROJECTION_DATE,
            object(),
            None,
        )
    with pytest.raises(ValueError):
        acquisition._validated_runtime_inputs(
            OPERATION_KEY,
            PROJECTION_DATE,
            enabled_config(),
            object(),
        )
    assert runtime.default_dependencies().session_scope is runtime.default_session_scope


@pytest.mark.asyncio
@pytest.mark.parametrize("created_count", [True, 2])
async def test_context_rejects_invalid_initialize_count(created_count):
    harness = AcquisitionHarness(npi_count=1)

    async def invalid_initialize(*_args, **_kwargs):
        return created_count

    dependencies = replace(
        harness.dependencies(),
        initialize_root=invalid_initialize,
    )
    with pytest.raises(acquisition.UHCFlexPractitionerAcquisitionError):
        await acquisition._initialize_context(
            operation_key=OPERATION_KEY,
            projection_date=PROJECTION_DATE,
            dependencies=dependencies,
            database=harness.database,
        )


@pytest.mark.asyncio
async def test_root_and_admission_validation_reject_every_drift():
    _harness, context, identity_by_role, summary_by_role, admission = (
        await _admission_validation_fixture()
    )
    with pytest.raises(acquisition.UHCFlexPractitionerAcquisitionError):
        acquisition._root_receipt(
            identity_by_role["baseline"],
            _mutated(
                summary_by_role["baseline"],
                acquisition_id="pdufpa_" + "0" * 48,
            ),
            0.0,
        )

    partial_summary = replace(
        summary_by_role["candidate"],
        matched_count=0, unmatched_count=0,
        error_count=1, cohort_complete=False,
    )
    partial_receipt = acquisition._root_receipt(
        identity_by_role["candidate"],
        partial_summary,
        0.0,
    )
    assert partial_receipt.error_count == 1
    assert partial_receipt.cohort_complete is False

    invalid_admission_values = (
        object(),
        _mutated(admission, baseline_acquisition_id="pdufpa_" + "0" * 48),
        _mutated(admission, candidate_acquisition_id="pdufpa_" + "0" * 48),
        _mutated(admission, cohort_id="pdufc_" + "0" * 48),
        _mutated(admission, dataset_intent_id="pdufdi_" + "0" * 48),
        _mutated(admission, semantic_projection_as_of="2026-08-11"),
        _mutated(admission, operation_key="0" * 64),
        _mutated(admission, expected_npi_count=2),
        _mutated(admission, terminal_set_sha256="0" * 64),
        _mutated(admission, resource_count=2),
        _mutated(admission, publication_authority=False),
    )
    for invalid in invalid_admission_values:
        with pytest.raises(acquisition.UHCFlexPractitionerAcquisitionError):
            acquisition._validate_admission(
                invalid,
                baseline=summary_by_role["baseline"],
                candidate=summary_by_role["candidate"],
                context=context,
            )
    for baseline in (
        _mutated(summary_by_role["baseline"], terminal_set_sha256="0" * 64),
        _mutated(summary_by_role["baseline"], resource_count=2),
    ):
        with pytest.raises(acquisition.UHCFlexPractitionerAcquisitionError):
            acquisition._validate_admission(
                admission,
                baseline=baseline,
                candidate=summary_by_role["candidate"],
                context=context,
            )


@pytest.mark.asyncio
async def test_admission_surfaces_store_errors_and_impossible_empty_result(monkeypatch):
    harness, context, _identity_by_role, summary_by_role, _admission = (
        await _admission_validation_fixture()
    )
    harness.admission_error = UHCFlexPractitionerTwinStoreError("state")
    with pytest.raises(UHCFlexPractitionerTwinStoreError):
        await acquisition._admit_root_pair(
            context,
            summary_by_role["baseline"],
            summary_by_role["candidate"],
            dependencies=harness.dependencies(),
            database=harness.database,
        )

    harness.admission_error = None
    monkeypatch.setattr(
        acquisition, "_validate_admission", lambda *_args, **_kwargs: None
    )
    with pytest.raises(acquisition.UHCFlexPractitionerAcquisitionError):
        await acquisition._admit_root_pair(
            context,
            summary_by_role["baseline"],
            summary_by_role["candidate"],
            dependencies=harness.dependencies(),
            database=harness.database,
        )
