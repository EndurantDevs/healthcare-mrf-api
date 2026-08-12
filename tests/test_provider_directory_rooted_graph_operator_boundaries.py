# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Failure-boundary coverage for the manual rooted graph operator."""

from __future__ import annotations

import asyncio
import sys
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from process import provider_directory_rooted_graph_operator as operator
from process import provider_directory_rooted_graph_operator_contract as contract


OPERATION_KEY = "a" * 64
PUBLICATION_ACQUISITION_ID = "pdrga_" + "b" * 48


def _enable_only(monkeypatch: pytest.MonkeyPatch, selected: str) -> None:
    for gate_name in (
        contract.REGISTRATION_ENABLED_ENV,
        contract.ACQUISITION_ENABLED_ENV,
        contract.PUBLICATION_ENABLED_ENV,
    ):
        monkeypatch.setenv(
            gate_name,
            "true" if gate_name == selected else "false",
        )


def test_contract_rejects_invalid_identity_phase_and_root() -> None:
    shared_scope = SimpleNamespace(scope_id="scope")
    baseline = SimpleNamespace(
        acquisition_id="pdrga_" + "1" * 48,
        acquisition_role="baseline",
        dataset_intent_id="pdrgi_" + "2" * 48,
        run_id="pdrgr_" + "3" * 48,
        scope_id="scope",
    )
    candidate = SimpleNamespace(
        acquisition_id="pdrga_" + "4" * 48,
        acquisition_role="candidate",
        dataset_intent_id=baseline.dataset_intent_id,
        run_id="pdrgr_" + "5" * 48,
        scope_id="scope",
    )

    with pytest.raises(ValueError, match="identity_invalid"):
        contract.ProviderDirectoryRootedGraphOperatorIdentities(
            operation_key=object(),
            dataset_intent_id=baseline.dataset_intent_id,
            scope=shared_scope,
            baseline=baseline,
            candidate=candidate,
        )
    with pytest.raises(contract.ProviderDirectoryRootedGraphOperatorError) as phase:
        contract.require_rooted_graph_operator_gate("unknown")
    assert phase.value.code == "invalid_request"
    with pytest.raises(ValueError, match="root_invalid"):
        contract.build_rooted_graph_operator_identities(
            object(),
            operation_key=OPERATION_KEY,
        )


def test_contract_error_mapping_preserves_only_closed_codes() -> None:
    preserved = contract._operation_error(
        contract.ProviderDirectoryRootedGraphOperatorError("busy"),
        "registration",
    )
    replaced = contract._operation_error(RuntimeError("private"), "publication")

    assert isinstance(preserved, contract.ProviderDirectoryRootedGraphOperatorError)
    assert preserved.code == "busy"
    assert isinstance(replaced, contract.ProviderDirectoryRootedGraphOperatorError)
    assert replaced.code == "publication"


@pytest.mark.asyncio
async def test_registration_rejects_wrong_result_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registration_module = ModuleType(
        "process.provider_directory_rooted_graph_registration"
    )

    class Result:
        pass

    async def register(*, database: Any) -> object:
        assert database is marker_database
        return object()

    registration_module.ProviderDirectoryRootedGraphRegistrationResult = Result
    registration_module.register_provider_directory_rooted_graph_source = register
    monkeypatch.setitem(sys.modules, registration_module.__name__, registration_module)
    marker_database = object()

    with pytest.raises(contract.ProviderDirectoryRootedGraphOperatorError) as caught:
        await operator._register_source(marker_database)
    assert caught.value.code == "evidence"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("raised_error", "expected_code"),
    (
        (asyncio.CancelledError(), None),
        (TimeoutError(), None),
        (contract.ProviderDirectoryRootedGraphOperatorError("busy"), "busy"),
        (RuntimeError("private"), "registration"),
    ),
)
async def test_registration_error_taxonomy(
    monkeypatch: pytest.MonkeyPatch,
    raised_error: BaseException,
    expected_code: str | None,
) -> None:
    _enable_only(monkeypatch, contract.REGISTRATION_ENABLED_ENV)

    async def fail(_database: Any) -> Any:
        raise raised_error

    monkeypatch.setattr(operator, "_register_source", fail)
    with pytest.raises(type(raised_error)) as caught:
        await operator.register_rooted_graph_source_operation(database=object())
    if expected_code is not None:
        assert caught.value.code == expected_code


class _Transaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *_error: object) -> None:
        return None


class _Database:
    def transaction(self) -> _Transaction:
        return _Transaction()

    async def scalar(self, _query: str, **_values: Any) -> None:
        return None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("selected", "expected_code"),
    ((None, "missing"), (object(), "evidence")),
)
async def test_current_selector_rejects_absent_or_wrong_typed_root(
    monkeypatch: pytest.MonkeyPatch,
    selected: object | None,
    expected_code: str,
) -> None:
    selection_module = ModuleType(
        "process.provider_directory_dataset_scoped_publication"
    )
    selection_module.exact_uhc_dataset_pair = lambda: object()

    async def lock_current(_database: Any, *, pair: object) -> object | None:
        assert pair is not None
        return selected

    selection_module.lock_exact_current_dataset = lock_current
    monkeypatch.setitem(sys.modules, selection_module.__name__, selection_module)

    with pytest.raises(contract.ProviderDirectoryRootedGraphOperatorError) as caught:
        await operator._select_exact_current_root(_Database())
    assert caught.value.code == expected_code


def _identities() -> SimpleNamespace:
    return SimpleNamespace(
        dataset_intent_id="pdrgi_" + "1" * 48,
        baseline=SimpleNamespace(acquisition_id="pdrga_" + "2" * 48),
        candidate=SimpleNamespace(acquisition_id="pdrga_" + "3" * 48),
    )


def test_acquisition_and_admission_reject_unbound_evidence() -> None:
    identities = _identities()

    class Receipt:
        pass

    receipt = Receipt()
    receipt.dataset_intent_id = identities.dataset_intent_id
    receipt.baseline = identities.baseline
    receipt.candidate = identities.candidate
    receipt.rooted_graphs_match = False
    with pytest.raises(contract.ProviderDirectoryRootedGraphOperatorError) as mismatch:
        operator._require_acquisition_receipt(receipt, identities, Receipt)
    assert mismatch.value.code == "mismatch"
    with pytest.raises(contract.ProviderDirectoryRootedGraphOperatorError) as evidence:
        operator._require_acquisition_receipt(object(), identities, Receipt)
    assert evidence.value.code == "evidence"

    current = SimpleNamespace(dataset_id="root")
    matching_receipt = SimpleNamespace(
        candidate=SimpleNamespace(rooted_graph_sha256="4" * 64)
    )

    class Admission:
        pass

    with pytest.raises(contract.ProviderDirectoryRootedGraphOperatorError) as admission:
        operator._require_admission(
            object(),
            current,
            identities,
            matching_receipt,
            Admission,
        )
    assert admission.value.code == "admission"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("raised_error", "expected_code"),
    (
        (contract.ProviderDirectoryRootedGraphOperatorError("busy"), "busy"),
        (RuntimeError("private"), "acquisition"),
    ),
)
async def test_acquisition_error_taxonomy(
    monkeypatch: pytest.MonkeyPatch,
    raised_error: Exception,
    expected_code: str,
) -> None:
    _enable_only(monkeypatch, contract.ACQUISITION_ENABLED_ENV)
    monkeypatch.setattr(operator, "require_rooted_graph_operator_gate", lambda _: None)

    async def fail(*_args: Any, **_kwargs: Any) -> str:
        raise raised_error

    monkeypatch.setattr(operator, "_run_acquisition_phase", fail)
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
    assert caught.value.code == expected_code


def _publication_module(raised_error: BaseException | None) -> ModuleType:
    publication_module = ModuleType(
        "process.provider_directory_rooted_graph_publication"
    )

    class Result:
        pass

    async def publish(*_args: Any, **_kwargs: Any) -> object:
        if raised_error is not None:
            raise raised_error
        return object()

    publication_module.ProviderDirectoryRootedGraphPublicationResult = Result
    publication_module.publish_provider_directory_rooted_graph_dataset = publish
    return publication_module


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("raised_error", "expected_type", "expected_code"),
    (
        (None, contract.ProviderDirectoryRootedGraphOperatorError, "evidence"),
        (asyncio.CancelledError(), asyncio.CancelledError, None),
        (TimeoutError(), TimeoutError, None),
        (
            TypeError("private"),
            contract.ProviderDirectoryRootedGraphOperatorError,
            "invalid_request",
        ),
        (
            ValueError("private"),
            contract.ProviderDirectoryRootedGraphOperatorError,
            "invalid_request",
        ),
        (
            RuntimeError("private"),
            contract.ProviderDirectoryRootedGraphOperatorError,
            "publication",
        ),
    ),
)
async def test_publication_error_taxonomy(
    monkeypatch: pytest.MonkeyPatch,
    raised_error: BaseException | None,
    expected_type: type[BaseException],
    expected_code: str | None,
) -> None:
    _enable_only(monkeypatch, contract.PUBLICATION_ENABLED_ENV)
    publication_module = _publication_module(raised_error)
    monkeypatch.setitem(sys.modules, publication_module.__name__, publication_module)

    with pytest.raises(expected_type) as caught:
        await operator.publish_admitted_rooted_graph_operation(
            publication_acquisition_id=PUBLICATION_ACQUISITION_ID,
            batch_size=4096,
            database=object(),
        )
    if expected_code is not None:
        assert caught.value.code == expected_code
