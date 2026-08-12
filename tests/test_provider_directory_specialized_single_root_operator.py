# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused branch proof for the reviewed rooted single-root flow."""

from __future__ import annotations

import asyncio
from dataclasses import fields, replace
from datetime import UTC, datetime
import json
from pathlib import Path
import runpy
import sys
from types import ModuleType
from typing import Any

import pytest

from process import provider_directory_profile_uhc_flex_contract as profile_contract
from process import provider_directory_rooted_graph_acquisition as acquisition
from process import provider_directory_rooted_graph_operator as operator
from process import provider_directory_rooted_graph_operator_contract as contract
from process import provider_directory_rooted_graph_single_root_contract as single_root
from process import provider_directory_rooted_graph_twin_store as twin_store
from process.provider_directory_rooted_graph_acquisition_contract import (
    ProviderDirectoryRootedGraphAcquisitionConfig,
    ProviderDirectoryRootedGraphRootReceipt,
)
from process.provider_directory_rooted_graph_result_contract import (
    ProviderDirectoryRootedGraphAcquisitionSummary,
)
from process.provider_directory_rooted_graph_twin_contract import (
    build_rooted_graph_single_root_admission,
    ProviderDirectoryRootedGraphTwinAdmission,
    ProviderDirectoryRootedGraphTwinError,
)
from tests.provider_directory_rooted_graph_publication_test_support import (
    exact_current,
    sealed_roots,
)


OPERATION_KEY = "f" * 64
RECORDED_AT = datetime(2026, 8, 10, 12, tzinfo=UTC)


class _Transaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *_error: object) -> None:
        return None


class _Database:
    def transaction(self) -> _Transaction:
        return _Transaction()

    async def scalar(self, _statement: str) -> datetime:
        return RECORDED_AT


def _enable_single_root(monkeypatch: pytest.MonkeyPatch) -> None:
    for gate_name in (
        contract.REGISTRATION_ENABLED_ENV,
        contract.ACQUISITION_ENABLED_ENV,
        contract.SINGLE_ROOT_ACQUISITION_ENABLED_ENV,
        contract.PUBLICATION_ENABLED_ENV,
    ):
        monkeypatch.setenv(
            gate_name,
            "true"
            if gate_name == contract.SINGLE_ROOT_ACQUISITION_ENABLED_ENV
            else "false",
        )


def _identity():
    return single_root.derive_single_root_identity(
        exact_current(), operation_key=OPERATION_KEY
    )


def _candidate():
    identity = _identity().candidate
    proof = sealed_roots()[1]
    return type(proof)(
        **{
            field.name: (
                getattr(identity, field.name)
                if hasattr(identity, field.name)
                else getattr(proof, field.name)
            )
            for field in fields(proof)
        }
    )


def _receipt() -> ProviderDirectoryRootedGraphRootReceipt:
    candidate = _identity().candidate
    return ProviderDirectoryRootedGraphRootReceipt(
        acquisition_role="candidate",
        acquisition_id=candidate.acquisition_id,
        run_id=candidate.run_id,
        completed_count=3,
        resource_count=8,
        edge_count=5,
        rooted_graph_sha256="4" * 64,
        elapsed_seconds=1.25,
    )


def _admission() -> ProviderDirectoryRootedGraphTwinAdmission:
    return build_rooted_graph_single_root_admission(
        _candidate(),
        acquisition_operation_key=OPERATION_KEY,
        admitted_at=RECORDED_AT,
    )


def _install_operator_fakes(
    monkeypatch: pytest.MonkeyPatch,
    receipt: object,
    admission: object,
) -> None:
    async def select(_database: Any):
        return exact_current()

    monkeypatch.setattr(operator, "_select_exact_current_root", select)
    acquisition_module = ModuleType(
        "process.provider_directory_rooted_graph_acquisition"
    )

    async def acquire(candidate: Any, **_values: Any) -> object:
        assert candidate == _identity().candidate
        return receipt

    acquisition_module.acquire_rooted_graph_single_root = acquire
    acquisition_module.ProviderDirectoryRootedGraphAcquisitionConfig = (
        ProviderDirectoryRootedGraphAcquisitionConfig
    )
    acquisition_module.ProviderDirectoryRootedGraphRootReceipt = (
        ProviderDirectoryRootedGraphRootReceipt
    )
    store_module = ModuleType("process.provider_directory_rooted_graph_twin_store")

    async def admit(acquisition_id: str, **values: Any) -> object:
        assert acquisition_id == _identity().candidate.acquisition_id
        assert values["acquisition_operation_key"] == OPERATION_KEY
        return admission

    store_module.admit_rooted_graph_single_root = admit
    store_module.ProviderDirectoryRootedGraphTwinAdmission = (
        ProviderDirectoryRootedGraphTwinAdmission
    )
    monkeypatch.setitem(sys.modules, acquisition_module.__name__, acquisition_module)
    monkeypatch.setitem(sys.modules, store_module.__name__, store_module)


def _operator_arguments() -> dict[str, object]:
    return {
        "operation_key": OPERATION_KEY,
        "concurrency": 4,
        "max_attempts": 3,
        "lease_seconds": 300,
        "retry_base_seconds": 1.0,
        "max_retry_seconds": 60.0,
        "root_timeout_seconds": 604_800.0,
        "database": object(),
    }


def test_single_root_contract_guards_and_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = contract.single_root_operator_contract_payload()
    identity = contract.build_rooted_graph_single_root_identity(
        exact_current(), operation_key=OPERATION_KEY
    )
    assert payload["reviewed_root_policy"]["required_root_count"] == 1
    assert identity == _identity()
    assert profile_contract._is_rooted_admission_metadata_valid({}) is False
    with pytest.raises(ValueError):
        single_root.derive_single_root_identity(object(), operation_key=OPERATION_KEY)
    with pytest.raises(ValueError):
        replace(identity, operation_key="invalid")
    with pytest.raises(ValueError):
        build_rooted_graph_single_root_admission(
            object(), acquisition_operation_key=OPERATION_KEY, admitted_at=RECORDED_AT
        )

    class _BadDigest:
        def hexdigest(self) -> str:
            return "0" * 64

    monkeypatch.setattr(contract.hashlib, "sha256", lambda _payload: _BadDigest())
    with pytest.raises(
        RuntimeError, match="provider_directory_rooted_graph_single_root_contract_invalid"
    ):
        runpy.run_path(Path(contract.__file__))


@pytest.mark.asyncio
async def test_single_root_acquisition_validates_role_and_seals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identity = _identity().candidate
    summary = ProviderDirectoryRootedGraphAcquisitionSummary(
        identity.acquisition_id,
        identity.scope_id,
        3,
        0,
        8,
        5,
        "1" * 64,
        "2" * 64,
        "3" * 64,
        "4" * 64,
        True,
        False,
        False,
    )
    sealed_sources: list[tuple[object, ...]] = []

    async def acquire(*_args: Any, **_values: Any):
        return summary, 1.25, ("exact-source",)

    async def require(_identities: Any, **keyword_arguments: Any) -> None:
        sealed_sources.append(keyword_arguments["expected_source"])

    monkeypatch.setattr(acquisition, "_runtime_dependencies", lambda *_args: object())
    monkeypatch.setattr(acquisition, "_acquire_root", acquire)
    monkeypatch.setattr(acquisition, "_require_sealed_roots", require)
    config = ProviderDirectoryRootedGraphAcquisitionConfig(enabled=True)
    root_receipt = await acquisition.acquire_rooted_graph_single_root(
        identity, config=config, database=object()
    )
    assert root_receipt == _receipt()
    assert sealed_sources == [("exact-source",)]
    with pytest.raises(ValueError):
        await acquisition.acquire_rooted_graph_single_root(object(), config=config)
    with pytest.raises(ValueError):
        await acquisition.acquire_rooted_graph_single_root(
            contract.build_rooted_graph_operator_identities(
                exact_current(), operation_key=OPERATION_KEY
            ).baseline,
            config=config,
        )


@pytest.mark.asyncio
async def test_single_root_operator_returns_bounded_admission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_single_root(monkeypatch)
    _install_operator_fakes(monkeypatch, _receipt(), _admission())
    result = json.loads(await operator.acquire_single_root_operation(**_operator_arguments()))
    assert result["status"] == "admitted"
    assert result["operation_key"] == OPERATION_KEY
    assert result["acquisition"]["acquisition_id"] == _receipt().acquisition_id
    assert result["provider_directory_reviewed_root_policy_v1"] == {
        "policy_version": "provider-directory-reviewed-root-policy-v1",
        "required_root_count": 1,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("bad_receipt", "bad_admission", "expected_code"),
    ((True, False, "evidence"), (False, True, "admission")),
)
async def test_single_root_operator_rejects_wrong_evidence_type(
    monkeypatch: pytest.MonkeyPatch,
    bad_receipt: bool,
    bad_admission: bool,
    expected_code: str,
) -> None:
    _enable_single_root(monkeypatch)
    _install_operator_fakes(
        monkeypatch,
        object() if bad_receipt else _receipt(),
        object() if bad_admission else _admission(),
    )
    with pytest.raises(contract.ProviderDirectoryRootedGraphOperatorError) as caught:
        await operator.acquire_single_root_operation(**_operator_arguments())
    assert caught.value.code == expected_code


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "expected_code"),
    (
        (asyncio.CancelledError(), None),
        (contract.ProviderDirectoryRootedGraphOperatorError("state"), "state"),
        (RuntimeError("synthetic"), "acquisition"),
    ),
)
async def test_single_root_operator_maps_runtime_failures(
    monkeypatch: pytest.MonkeyPatch,
    failure: BaseException,
    expected_code: str | None,
) -> None:
    _enable_single_root(monkeypatch)

    async def fail(*_args: Any, **_values: Any) -> str:
        raise failure

    monkeypatch.setattr(operator, "_run_single_root_acquisition_phase", fail)
    if expected_code is None:
        with pytest.raises(asyncio.CancelledError):
            await operator.acquire_single_root_operation(**_operator_arguments())
        return
    with pytest.raises(contract.ProviderDirectoryRootedGraphOperatorError) as caught:
        await operator.acquire_single_root_operation(**_operator_arguments())
    assert caught.value.code == expected_code


@pytest.mark.asyncio
async def test_single_root_operator_rejects_invalid_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_single_root(monkeypatch)
    arguments = _operator_arguments()
    arguments["operation_key"] = "invalid"
    with pytest.raises(contract.ProviderDirectoryRootedGraphOperatorError) as caught:
        await operator.acquire_single_root_operation(**arguments)
    assert caught.value.code == "invalid_request"


@pytest.mark.asyncio
async def test_single_root_store_rejects_invalid_key_and_replays(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = _candidate()
    admission = _admission()
    with pytest.raises(ValueError):
        await twin_store.admit_rooted_graph_single_root(
            candidate.acquisition_id,
            acquisition_operation_key="invalid",
            database=object(),
        )

    async def current(_database: Any):
        return exact_current()

    async def root(_database: Any, _acquisition_id: str):
        return candidate

    async def no_admission(*_args: Any, **_values: Any):
        return None

    monkeypatch.setattr(twin_store, "_lock_logical_current", current)
    monkeypatch.setattr(twin_store, "_lock_single_root", root)
    monkeypatch.setattr(twin_store, "_insert_authority", no_admission)
    monkeypatch.setattr(twin_store, "_read_admission", no_admission)
    monkeypatch.setattr(twin_store, "_require_exact", lambda *_args: None)
    with pytest.raises(ProviderDirectoryRootedGraphTwinError, match="state"):
        await twin_store.admit_rooted_graph_single_root(
            candidate.acquisition_id,
            acquisition_operation_key=OPERATION_KEY,
            database=_Database(),
        )

    async def read_admission(*_args: Any, **_values: Any):
        return admission

    monkeypatch.setattr(twin_store, "_read_admission", read_admission)
    assert (
        await twin_store.require_provider_directory_rooted_graph_admission(
            candidate.acquisition_id, database=_Database()
        )
        is admission
    )
