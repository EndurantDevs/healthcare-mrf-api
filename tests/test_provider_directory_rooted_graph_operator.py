# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed-flow tests for the dormant rooted graph operator."""

from __future__ import annotations

import asyncio
from dataclasses import replace
import json
import math
import sys
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from process import provider_directory_rooted_graph_operator as operator
from process import provider_directory_rooted_graph_operator_contract as contract
from process.provider_directory_dataset_scoped_publication_contract import (
    ExactCurrentDataset,
    LEGACY_PRACTITIONER_VARIANT,
    exact_uhc_dataset_pair,
)
from process.provider_directory_rooted_graph_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_LEGACY_ROOT_PUBLICATION_CONTRACT_ID,
)
from process.provider_directory_rooted_graph_source_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_SIGNATURE_SHA256,
)
from process.uhc_flex_official_cohort_contract import (
    UHC_FLEX_OFFICIAL_AUTHORITY_ID,
)


OPERATION_KEY = "a" * 64
SECOND_OPERATION_KEY = "b" * 64
ROOTED_GRAPH_SHA256 = "c" * 64


def _enable_only(monkeypatch: pytest.MonkeyPatch, selected: str) -> None:
    for gate_name in (
        contract.REGISTRATION_ENABLED_ENV,
        contract.ACQUISITION_ENABLED_ENV,
        contract.SINGLE_ROOT_ACQUISITION_ENABLED_ENV,
        contract.PUBLICATION_ENABLED_ENV,
    ):
        monkeypatch.setenv(
            gate_name,
            "true" if gate_name == selected else "false",
        )


def _legacy_current() -> ExactCurrentDataset:
    pair = exact_uhc_dataset_pair()
    return ExactCurrentDataset(
        dataset_id="pdufpd_" + "1" * 48,
        endpoint_id=pair.legacy_endpoint_id,
        source_id=pair.legacy_source_id,
        root_source_id=pair.legacy_source_id,
        root_endpoint_id=pair.legacy_endpoint_id,
        acquisition_source_id=pair.rooted_source_id,
        acquisition_endpoint_id=pair.rooted_endpoint_id,
        practitioner_origin_source_id=pair.legacy_source_id,
        practitioner_origin_endpoint_id=pair.legacy_endpoint_id,
        source_authority_id=UHC_FLEX_OFFICIAL_AUTHORITY_ID,
        endpoint_signature_sha256=(
            PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_SIGNATURE_SHA256
        ),
        dataset_hash="2" * 64,
        resource_count=3,
        practitioner_resource_count=3,
        root_content_proof_sha256="3" * 64,
        root_cohort_id="synthetic-reviewed-cohort",
        semantic_projection_as_of="2026-08-10",
        operation_key="4" * 64,
        acquisition_root_run_id="pdufpar_" + "5" * 48,
        variant=LEGACY_PRACTITIONER_VARIANT,
        root_publication_contract_id=(
            PROVIDER_DIRECTORY_ROOTED_GRAPH_LEGACY_ROOT_PUBLICATION_CONTRACT_ID
        ),
    )


def _allow_retired_operation_internals(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(operator, "require_rooted_graph_operator_gate", lambda _: None)


def test_active_gates_are_exact_and_retired_acquisition_stays_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate_by_phase = {
        "register": contract.REGISTRATION_ENABLED_ENV,
        "acquire": contract.ACQUISITION_ENABLED_ENV,
        contract.SINGLE_ROOT_ACQUISITION_PHASE: (
            contract.SINGLE_ROOT_ACQUISITION_ENABLED_ENV
        ),
        "publish": contract.PUBLICATION_ENABLED_ENV,
    }
    assert contract.OPERATOR_PHASES == tuple(gate_by_phase)
    for phase in ("register", contract.SINGLE_ROOT_ACQUISITION_PHASE, "publish"):
        gate_name = gate_by_phase[phase]
        _enable_only(monkeypatch, gate_name)
        contract.require_rooted_graph_operator_gate(phase)

    _enable_only(monkeypatch, contract.ACQUISITION_ENABLED_ENV)
    with pytest.raises(contract.ProviderDirectoryRootedGraphOperatorError) as disabled:
        contract.require_rooted_graph_operator_gate("acquire")
    assert disabled.value.code == "disabled"

    _enable_only(monkeypatch, contract.REGISTRATION_ENABLED_ENV)
    monkeypatch.setenv(contract.REGISTRATION_ENABLED_ENV, "TRUE")
    with pytest.raises(contract.ProviderDirectoryRootedGraphOperatorError) as disabled:
        contract.require_rooted_graph_operator_gate("register")
    assert disabled.value.code == "disabled"

    monkeypatch.setenv(contract.REGISTRATION_ENABLED_ENV, "true")
    monkeypatch.setenv(contract.ACQUISITION_ENABLED_ENV, "true")
    with pytest.raises(contract.ProviderDirectoryRootedGraphOperatorError) as conflict:
        contract.require_rooted_graph_operator_gate("register")
    assert conflict.value.code == "gate_conflict"


@pytest.mark.asyncio
async def test_retired_direct_acquisition_ignores_legacy_enable_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_only(monkeypatch, contract.ACQUISITION_ENABLED_ENV)

    with pytest.raises(contract.ProviderDirectoryRootedGraphOperatorError) as caught:
        await operator.acquire_admit_rooted_graph_operation(
            operation_key=OPERATION_KEY,
            concurrency=4,
            max_attempts=3,
            lease_seconds=300,
            retry_base_seconds=1.0,
            max_retry_seconds=60.0,
            root_timeout_seconds=604_800.0,
            database=object(),
        )

    assert caught.value.code == "disabled"


def test_operator_contract_is_closed_and_json_serialization_fails_closed() -> None:
    payload = contract.rooted_graph_operator_contract_payload()
    assert payload["scheduling"] == "none"
    assert payload["publication_selector"] == "exact-publication-acquisition-id"
    assert payload["operation_key"].startswith("required-exact")
    assert len(contract.PROVIDER_DIRECTORY_ROOTED_GRAPH_OPERATOR_CONTRACT_SHA256) == 64
    with pytest.raises(contract.ProviderDirectoryRootedGraphOperatorError) as caught:
        contract._canonical_json({"not_json": math.nan})
    assert caught.value.code == "evidence"


def test_same_root_and_operation_key_resume_same_twins() -> None:
    current = _legacy_current()
    first = contract.build_rooted_graph_operator_identities(
        current,
        operation_key=OPERATION_KEY,
    )
    replay = contract.build_rooted_graph_operator_identities(
        current,
        operation_key=OPERATION_KEY,
    )
    restarted = contract.build_rooted_graph_operator_identities(
        current,
        operation_key=SECOND_OPERATION_KEY,
    )
    changed_root = contract.build_rooted_graph_operator_identities(
        replace(current, dataset_hash="6" * 64),
        operation_key=OPERATION_KEY,
    )

    assert replay == first
    assert replay.baseline.acquisition_id == first.baseline.acquisition_id
    assert replay.candidate.acquisition_id == first.candidate.acquisition_id
    assert restarted.dataset_intent_id != first.dataset_intent_id
    assert restarted.baseline.run_id != first.baseline.run_id
    assert changed_root.dataset_intent_id != first.dataset_intent_id
    assert "pdrgi_" not in repr(first)
    with pytest.raises(ValueError, match="operation_key"):
        contract.build_rooted_graph_operator_identities(
            current,
            operation_key="A" * 64,
        )


@pytest.mark.asyncio
async def test_invalid_operation_key_fails_before_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_only(monkeypatch, contract.ACQUISITION_ENABLED_ENV)
    _allow_retired_operation_internals(monkeypatch)

    async def database_reached(_database: Any) -> Any:
        pytest.fail("invalid operation key reached database")

    monkeypatch.setattr(operator, "_select_exact_current_root", database_reached)
    with pytest.raises(contract.ProviderDirectoryRootedGraphOperatorError) as caught:
        await operator.acquire_admit_rooted_graph_operation(
            operation_key="not-a-hash",
            concurrency=4,
            max_attempts=3,
            lease_seconds=300,
            retry_base_seconds=1.0,
            max_retry_seconds=60.0,
            root_timeout_seconds=604_800.0,
            database=object(),
        )
    assert caught.value.code == "invalid_request"


@pytest.mark.asyncio
async def test_invalid_publication_selector_fails_before_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_only(monkeypatch, contract.PUBLICATION_ENABLED_ENV)
    publication_module = ModuleType(
        "process.provider_directory_rooted_graph_publication"
    )

    async def reject_call(*_args: Any, **_kwargs: Any) -> Any:
        pytest.fail("invalid publication selector reached database")

    publication_module.publish_provider_directory_rooted_graph_dataset = reject_call
    publication_module.ProviderDirectoryRootedGraphPublicationResult = object
    monkeypatch.setitem(sys.modules, publication_module.__name__, publication_module)

    with pytest.raises(contract.ProviderDirectoryRootedGraphOperatorError) as caught:
        await operator.publish_admitted_rooted_graph_operation(
            publication_acquisition_id="latest",
            batch_size=64,
            database=object(),
        )
    assert caught.value.code == "invalid_request"


@pytest.mark.asyncio
async def test_invalid_controls_fail_before_current_selection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_only(monkeypatch, contract.ACQUISITION_ENABLED_ENV)
    _allow_retired_operation_internals(monkeypatch)

    async def reject_selection(_database: Any) -> Any:
        pytest.fail("invalid controls reached current-dataset selection")

    monkeypatch.setattr(operator, "_select_exact_current_root", reject_selection)
    with pytest.raises(contract.ProviderDirectoryRootedGraphOperatorError) as caught:
        await operator.acquire_admit_rooted_graph_operation(
            operation_key=OPERATION_KEY,
            concurrency=4,
            max_attempts=3,
            lease_seconds=300,
            retry_base_seconds=10.0,
            max_retry_seconds=1.0,
            root_timeout_seconds=604_800.0,
            database=object(),
        )
    assert caught.value.code == "invalid_request"


class _Transaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *_error: object) -> None:
        return None


@pytest.mark.asyncio
async def test_current_selector_uses_shared_lock_and_exact_pair(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current = _legacy_current()
    calls: list[tuple[str, Any]] = []

    class Database:
        def transaction(self) -> _Transaction:
            calls.append(("transaction", None))
            return _Transaction()

        async def scalar(self, _query: str, **values: Any) -> None:
            calls.append(("lock", values["lock_identity"]))

    selection_module = ModuleType(
        "process.provider_directory_dataset_scoped_publication"
    )

    def exact_pair() -> object:
        calls.append(("pair", None))
        return "exact-pair"

    async def lock_current(database: Any, *, pair: object) -> ExactCurrentDataset:
        assert isinstance(database, Database)
        calls.append(("select", pair))
        return current

    selection_module.exact_uhc_dataset_pair = exact_pair
    selection_module.lock_exact_current_dataset = lock_current
    monkeypatch.setitem(sys.modules, selection_module.__name__, selection_module)

    selected = await operator._select_exact_current_root(Database())

    assert selected is current
    assert calls == [
        ("transaction", None),
        ("lock", contract_module_lock_identity()),
        ("pair", None),
        ("select", "exact-pair"),
    ]


def contract_module_lock_identity() -> str:
    from process.provider_directory_dataset_scoped_publication_contract import (
        EXACT_DATASET_PUBLICATION_LOCK_IDENTITY,
    )

    return EXACT_DATASET_PUBLICATION_LOCK_IDENTITY


def _fake_acquisition_module(calls: list[Any], *, cancel: bool) -> ModuleType:
    """Build a bounded twin-acquisition module for operation composition."""

    acquisition_module = ModuleType(
        "process.provider_directory_rooted_graph_acquisition"
    )

    class Config:
        def __init__(self, **values: Any) -> None:
            self.values = values

    class Receipt:
        pass

    async def acquire(baseline: Any, candidate: Any, **values: Any) -> Any:
        calls.append(("acquire", baseline, candidate, values))
        if cancel:
            raise asyncio.CancelledError()
        baseline_receipt = SimpleNamespace(
            acquisition_id=baseline.acquisition_id,
            completed_count=3,
            edge_count=8,
            resource_count=10,
            rooted_graph_sha256=ROOTED_GRAPH_SHA256,
            run_id=baseline.run_id,
        )
        candidate_receipt = SimpleNamespace(
            acquisition_id=candidate.acquisition_id,
            completed_count=3,
            edge_count=8,
            resource_count=10,
            rooted_graph_sha256=ROOTED_GRAPH_SHA256,
            run_id=candidate.run_id,
        )
        receipt = Receipt()
        receipt.dataset_intent_id = baseline.dataset_intent_id
        receipt.baseline = baseline_receipt
        receipt.candidate = candidate_receipt
        receipt.rooted_graphs_match = True
        return receipt

    acquisition_module.ProviderDirectoryRootedGraphAcquisitionConfig = Config
    acquisition_module.ProviderDirectoryRootedGraphAcquisitionReceipt = Receipt
    acquisition_module.acquire_provider_directory_rooted_graph_twins = acquire
    return acquisition_module


def _fake_twin_module(calls: list[Any]) -> tuple[ModuleType, type[Any]]:
    """Build a bounded admission module tied to the captured twin identities."""

    twin_module = ModuleType("process.provider_directory_rooted_graph_twin_store")

    class Admission:
        pass

    async def admit(first: str, second: str, **values: Any) -> Admission:
        calls.append(("admit", first, second, values))
        admission = Admission()
        admission.admission_id = "pdrgad_" + "7" * 48
        admission.attempt_id = "pdrgat_" + "8" * 48
        admission.publication_acquisition_id = second
        admission.comparison_acquisition_id = first
        admission.dataset_intent_id = calls[0][1].dataset_intent_id
        admission.root_dataset_id = calls[0][1].root_dataset_id
        admission.rooted_graph_sha256 = ROOTED_GRAPH_SHA256
        admission.publication_authority = True
        return admission

    twin_module.ProviderDirectoryRootedGraphTwinAdmission = Admission
    twin_module.admit_provider_directory_rooted_graph_twins = admit
    return twin_module, Admission


def _acquisition_modules(
    monkeypatch: pytest.MonkeyPatch,
    *,
    cancel: bool = False,
) -> tuple[list[Any], type[Any]]:
    """Install isolated acquisition and admission fakes for one test."""

    calls: list[Any] = []
    acquisition_module = _fake_acquisition_module(calls, cancel=cancel)
    twin_module, admission_type = _fake_twin_module(calls)
    monkeypatch.setitem(sys.modules, acquisition_module.__name__, acquisition_module)
    monkeypatch.setitem(sys.modules, twin_module.__name__, twin_module)
    return calls, admission_type


@pytest.mark.asyncio
async def test_acquisition_replays_deterministically_and_never_publishes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_only(monkeypatch, contract.ACQUISITION_ENABLED_ENV)
    _allow_retired_operation_internals(monkeypatch)
    current = _legacy_current()

    async def select(_database: Any) -> ExactCurrentDataset:
        return current

    monkeypatch.setattr(operator, "_select_exact_current_root", select)
    calls, _admission_type = _acquisition_modules(monkeypatch)
    publication_name = "process.provider_directory_rooted_graph_publication"
    monkeypatch.delitem(sys.modules, publication_name, raising=False)

    arguments_by_name = {
        "operation_key": OPERATION_KEY,
        "concurrency": 4,
        "max_attempts": 3,
        "lease_seconds": 300,
        "retry_base_seconds": 1.0,
        "max_retry_seconds": 60.0,
        "root_timeout_seconds": 604_800.0,
        "database": object(),
    }
    first = json.loads(
        await operator.acquire_admit_rooted_graph_operation(**arguments_by_name)
    )
    replay = json.loads(
        await operator.acquire_admit_rooted_graph_operation(**arguments_by_name)
    )

    assert first == replay
    assert first["operation_key"] == OPERATION_KEY
    assert first["status"] == "admitted"
    assert first["publication_acquisition_id"].startswith("pdrga_")
    assert [call[0] for call in calls] == ["acquire", "admit"] * 2
    assert publication_name not in sys.modules


@pytest.mark.asyncio
async def test_acquisition_preserves_cancellation_before_admission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_only(monkeypatch, contract.ACQUISITION_ENABLED_ENV)
    _allow_retired_operation_internals(monkeypatch)

    async def select(_database: Any) -> ExactCurrentDataset:
        return _legacy_current()

    monkeypatch.setattr(operator, "_select_exact_current_root", select)
    calls, _admission_type = _acquisition_modules(monkeypatch, cancel=True)
    with pytest.raises(asyncio.CancelledError):
        await operator.acquire_admit_rooted_graph_operation(
            operation_key=OPERATION_KEY,
            concurrency=4,
            max_attempts=3,
            lease_seconds=300,
            retry_base_seconds=1.0,
            max_retry_seconds=60.0,
            root_timeout_seconds=604_800.0,
            database=object(),
        )
    assert [call[0] for call in calls] == ["acquire"]
