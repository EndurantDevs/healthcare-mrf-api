# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Contracts for the default-off exact-cohort Practitioner operator."""

from __future__ import annotations

import asyncio
import json
import os
from types import ModuleType, SimpleNamespace

import pytest

from process import uhc_flex_practitioner_operator as operator


OPERATION_KEY = "a" * 64
CANDIDATE_ACQUISITION_ID = "pdufpa_" + "b" * 48
GATE_BY_PHASE = {
    "sync-cohort": operator.COHORT_ENABLED_ENV,
    "acquire-admit": operator.ACQUISITION_ENABLED_ENV,
    "acquire-admit-single-root": operator.SINGLE_ROOT_ACQUISITION_ENABLED_ENV,
    "publish-admitted": operator.PUBLICATION_ENABLED_ENV,
}


def _disable_all_gates(monkeypatch) -> None:
    for gate_name in GATE_BY_PHASE.values():
        monkeypatch.delenv(gate_name, raising=False)


def _allow_retired_operation_internals(monkeypatch) -> None:
    monkeypatch.setattr(
        operator,
        "require_uhc_flex_practitioner_operator_gate",
        lambda _: None,
    )


@pytest.mark.parametrize("phase", tuple(GATE_BY_PHASE))
def test_each_operator_phase_is_default_off(monkeypatch, phase) -> None:
    _disable_all_gates(monkeypatch)

    with pytest.raises(operator.UHCFlexPractitionerOperatorError) as caught:
        operator.require_uhc_flex_practitioner_operator_gate(phase)

    assert caught.value.code == "disabled"


@pytest.mark.parametrize(
    "phase", ("sync-cohort", "acquire-admit-single-root", "publish-admitted")
)
def test_active_phase_requires_only_its_exact_gate(monkeypatch, phase) -> None:
    _disable_all_gates(monkeypatch)
    monkeypatch.setenv(GATE_BY_PHASE[phase], "true")

    operator.require_uhc_flex_practitioner_operator_gate(phase)


@pytest.mark.asyncio
async def test_retired_direct_acquisition_ignores_legacy_enable_gate(
    monkeypatch,
) -> None:
    _disable_all_gates(monkeypatch)
    monkeypatch.setenv(operator.ACQUISITION_ENABLED_ENV, "true")

    with pytest.raises(operator.UHCFlexPractitionerOperatorError) as caught:
        await operator.acquire_admit_uhc_flex_practitioner_operation(
            operation_key=OPERATION_KEY,
            semantic_projection_as_of="2026-08-10",
            concurrency=4,
            max_attempts=3,
            lease_seconds=300,
            retry_base_seconds=1.0,
            max_retry_seconds=60.0,
            database=object(),
        )

    assert caught.value.code == "disabled"


def test_multiple_phase_gates_fail_closed(monkeypatch) -> None:
    _disable_all_gates(monkeypatch)
    monkeypatch.setenv(operator.COHORT_ENABLED_ENV, "true")
    monkeypatch.setenv(operator.ACQUISITION_ENABLED_ENV, "true")

    with pytest.raises(operator.UHCFlexPractitionerOperatorError) as caught:
        operator.require_uhc_flex_practitioner_operator_gate("sync-cohort")

    assert caught.value.code == "gate_conflict"


def test_only_lowercase_true_enables_a_gate(monkeypatch) -> None:
    _disable_all_gates(monkeypatch)
    monkeypatch.setenv(operator.COHORT_ENABLED_ENV, "TRUE")

    with pytest.raises(operator.UHCFlexPractitionerOperatorError) as caught:
        operator.require_uhc_flex_practitioner_operator_gate("sync-cohort")

    assert caught.value.code == "disabled"


@pytest.mark.asyncio
async def test_sync_cohort_returns_only_bounded_status(monkeypatch) -> None:
    """A cohort replay exposes aggregate proof and no member identifiers."""

    _disable_all_gates(monkeypatch)
    monkeypatch.setenv(operator.COHORT_ENABLED_ENV, "true")
    module_name = "process.uhc_flex_official_cohort_store"
    phase_module = ModuleType(module_name)
    database = object()
    database_calls = []

    class SyncResult:
        def __init__(self) -> None:
            self.created = False
            self.cohort = SimpleNamespace(
                cohort_complete=True,
                cohort_id="pdufc_" + "c" * 48,
                endpoint_collection_complete=False,
                endpoint_complete=False,
                npi_count=9,
                official_content_proof_sha256="d" * 64,
                official_dataset_hash="e" * 64,
                official_dataset_id="pdd_" + "f" * 48,
                practitioner_resource_count=10,
            )

    async def sync_cohort(*, database):
        database_calls.append(database)
        return SyncResult()

    phase_module.UHCFlexOfficialCohortSyncResult = SyncResult
    phase_module.sync_uhc_flex_official_cohort = sync_cohort
    monkeypatch.setitem(__import__("sys").modules, module_name, phase_module)

    rendered_receipt = await operator.sync_uhc_flex_practitioner_cohort_operation(
        database=database
    )
    receipt_by_field = json.loads(rendered_receipt)

    assert database_calls == [database]
    assert receipt_by_field == {
        "cohort_complete": True,
        "cohort_created": False,
        "cohort_id": "pdufc_" + "c" * 48,
        "endpoint_collection_complete": False,
        "endpoint_complete": False,
        "npi_count": 9,
        "official_content_proof_sha256": "d" * 64,
        "official_dataset_hash": "e" * 64,
        "official_dataset_id": "pdd_" + "f" * 48,
        "practitioner_resource_count": 10,
        "status": "sealed",
    }


def _root(role: str, character: str) -> SimpleNamespace:
    return SimpleNamespace(
        acquisition_role=role,
        acquisition_id="pdufpa_" + character * 48,
        cohort_complete=True,
        elapsed_seconds=2.5,
        error_count=0,
        matched_count=7,
        resource_count=8,
        run_id="pdufpr_" + character * 48,
        terminal_set_sha256="1" * 64,
        unmatched_count=2,
    )


def _acquisition_phase_module() -> tuple[ModuleType, type, list[dict]]:
    module_name = "process.uhc_flex_practitioner_acquisition"
    phase_module = ModuleType(module_name)
    operation_calls: list[dict] = []

    class Config:
        def __init__(self, **config_by_field) -> None:
            self.config_by_field = config_by_field

    class Receipt:
        def __init__(self) -> None:
            self.operation_key = OPERATION_KEY
            self.semantic_projection_as_of = "2026-08-10"
            self.cohort_id = "pdufc_" + "2" * 48
            self.official_dataset_id = "pdd_" + "3" * 48
            self.official_dataset_hash = "4" * 64
            self.official_content_proof_sha256 = "5" * 64
            self.dataset_intent_id = "pdufdi_" + "6" * 48
            self.expected_npi_count = 9
            self.baseline = _root("baseline", "7")
            self.candidate = _root("candidate", "8")
            self.twin_attempt_id = "pdufpta_" + "9" * 48
            self.admission_id = "pdufpad_" + "a" * 48
            self.elapsed_seconds = 5.0

    async def acquire_twins(**keyword_arguments):
        operation_calls.append(keyword_arguments)
        return Receipt()

    phase_module.UHCFlexPractitionerAcquisitionConfig = Config
    phase_module.UHCFlexPractitionerAcquisitionReceipt = Receipt
    phase_module.acquire_uhc_flex_practitioner_twins = acquire_twins
    return phase_module, Config, operation_calls


@pytest.mark.asyncio
async def test_acquisition_forwards_resume_identity_and_bounds(monkeypatch) -> None:
    """The stable campaign coordinates and every execution bound are forwarded."""

    _disable_all_gates(monkeypatch)
    monkeypatch.setenv(operator.ACQUISITION_ENABLED_ENV, "true")
    _allow_retired_operation_internals(monkeypatch)
    module_name = "process.uhc_flex_practitioner_acquisition"
    phase_module, _config_type, operation_calls = _acquisition_phase_module()
    monkeypatch.setitem(__import__("sys").modules, module_name, phase_module)
    database = object()

    rendered_receipt = await operator.acquire_admit_uhc_flex_practitioner_operation(
        operation_key=OPERATION_KEY,
        semantic_projection_as_of="2026-08-10",
        concurrency=6,
        max_attempts=4,
        lease_seconds=600,
        retry_base_seconds=2.0,
        max_retry_seconds=30.0,
        database=database,
    )
    receipt_by_field = json.loads(rendered_receipt)

    assert len(operation_calls) == 1
    assert operation_calls[0]["operation_key"] == OPERATION_KEY
    assert operation_calls[0]["semantic_projection_as_of"] == "2026-08-10"
    assert operation_calls[0]["database"] is database
    assert operation_calls[0]["config"].config_by_field == {
        "enabled": True,
        "concurrency": 6,
        "max_attempts": 4,
        "lease_seconds": 600,
        "retry_base_seconds": 2.0,
        "max_retry_seconds": 30.0,
    }
    assert receipt_by_field["status"] == "admitted"
    assert receipt_by_field["candidate"]["acquisition_id"] == ("pdufpa_" + "8" * 48)
    assert receipt_by_field["profile_delta_dispatch"] == {
        "operator_command_available": False,
        "status": "not_applicable_before_publication",
    }
    assert "npi" not in rendered_receipt.lower().replace(
        "expected_npi_count",
        "",
    )


def _single_root_phase_modules(acquisition_result=None):
    phase_module = ModuleType("process.uhc_flex_practitioner_acquisition")
    contract_module = ModuleType("process.uhc_flex_practitioner_single_root_contract")
    operation_calls = []

    class Config:
        def __init__(self, **config_by_field) -> None:
            self.config_by_field = config_by_field

    class Receipt:
        operation_key = OPERATION_KEY
        semantic_projection_as_of = "2026-08-10"
        cohort_id = "pdufc_" + "2" * 48
        official_dataset_id = "pdd_" + "3" * 48
        official_dataset_hash = "4" * 64
        official_content_proof_sha256 = "5" * 64
        dataset_intent_id = "pdufdi_" + "6" * 48
        expected_npi_count = 9
        candidate = _root("candidate", "8")
        admission_id = "pdufpad_" + "a" * 48
        reviewed_root_policy_json = {"required_root_count": 1}
        elapsed_seconds = 5.0

    async def acquire_single_root(**keyword_arguments):
        operation_calls.append(keyword_arguments)
        if isinstance(acquisition_result, BaseException):
            raise acquisition_result
        return Receipt() if acquisition_result is None else acquisition_result

    phase_module.UHCFlexPractitionerAcquisitionConfig = Config
    phase_module.acquire_uhc_flex_single_root = acquire_single_root
    contract_module.UHCFlexPractitionerSingleRootReceipt = Receipt
    return phase_module, contract_module, operation_calls


def _install_single_root_phase(monkeypatch, acquisition_result=None):
    phase_module, contract_module, operation_calls = _single_root_phase_modules(
        acquisition_result
    )
    modules = __import__("sys").modules
    monkeypatch.setitem(modules, phase_module.__name__, phase_module)
    monkeypatch.setitem(modules, contract_module.__name__, contract_module)
    return operation_calls


@pytest.mark.asyncio
async def test_single_root_acquisition_forwards_bounds_and_returns_policy(
    monkeypatch,
) -> None:
    _disable_all_gates(monkeypatch)
    monkeypatch.setenv(operator.SINGLE_ROOT_ACQUISITION_ENABLED_ENV, "true")
    calls = _install_single_root_phase(monkeypatch)
    database = object()

    rendered = await operator.acquire_uhc_flex_single_root_operation(
        operation_key=OPERATION_KEY,
        semantic_projection_as_of="2026-08-10",
        concurrency=6,
        max_attempts=4,
        lease_seconds=600,
        retry_base_seconds=2.0,
        max_retry_seconds=30.0,
        database=database,
    )

    assert calls[0]["database"] is database
    assert calls[0]["config"].config_by_field["concurrency"] == 6
    receipt = json.loads(rendered)
    assert receipt["status"] == "admitted"
    assert receipt["candidate"]["acquisition_id"].startswith("pdufpa_")
    assert receipt["provider_directory_reviewed_root_policy_v1"] == {
        "required_root_count": 1
    }


@pytest.mark.asyncio
async def test_single_root_acquisition_rejects_wrong_receipt(monkeypatch) -> None:
    _disable_all_gates(monkeypatch)
    monkeypatch.setenv(operator.SINGLE_ROOT_ACQUISITION_ENABLED_ENV, "true")
    _install_single_root_phase(monkeypatch, object())

    with pytest.raises(operator.UHCFlexPractitionerOperatorError) as caught:
        await operator.acquire_uhc_flex_single_root_operation(
            operation_key=OPERATION_KEY,
            semantic_projection_as_of="2026-08-10",
            concurrency=4,
            max_attempts=3,
            lease_seconds=300,
            retry_base_seconds=1.0,
            max_retry_seconds=60.0,
            database=object(),
        )

    assert caught.value.code == "evidence"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "expected_type", "expected_code"),
    (
        (asyncio.CancelledError(), asyncio.CancelledError, None),
        (TimeoutError(), TimeoutError, None),
        (
            operator.UHCFlexPractitionerOperatorError("state"),
            operator.UHCFlexPractitionerOperatorError,
            "state",
        ),
        (ValueError(), operator.UHCFlexPractitionerOperatorError, "invalid_request"),
        (RuntimeError(), operator.UHCFlexPractitionerOperatorError, "acquisition"),
    ),
)
async def test_single_root_acquisition_normalizes_only_safe_errors(
    monkeypatch, error, expected_type, expected_code
) -> None:
    _disable_all_gates(monkeypatch)
    monkeypatch.setenv(operator.SINGLE_ROOT_ACQUISITION_ENABLED_ENV, "true")
    _install_single_root_phase(monkeypatch, error)

    with pytest.raises(expected_type) as caught:
        await operator.acquire_uhc_flex_single_root_operation(
            operation_key=OPERATION_KEY,
            semantic_projection_as_of="2026-08-10",
            concurrency=4,
            max_attempts=3,
            lease_seconds=300,
            retry_base_seconds=1.0,
            max_retry_seconds=60.0,
            database=object(),
        )

    if expected_code is not None:
        assert caught.value.code == expected_code


@pytest.mark.asyncio
async def test_cancellation_is_not_normalized(monkeypatch) -> None:
    _disable_all_gates(monkeypatch)
    monkeypatch.setenv(operator.COHORT_ENABLED_ENV, "true")
    module_name = "process.uhc_flex_official_cohort_store"
    phase_module = ModuleType(module_name)

    class SyncResult:
        pass

    async def cancel_sync(*, database):
        del database
        raise asyncio.CancelledError()

    phase_module.UHCFlexOfficialCohortSyncResult = SyncResult
    phase_module.sync_uhc_flex_official_cohort = cancel_sync
    monkeypatch.setitem(__import__("sys").modules, module_name, phase_module)

    with pytest.raises(asyncio.CancelledError):
        await operator.sync_uhc_flex_practitioner_cohort_operation(database=object())
